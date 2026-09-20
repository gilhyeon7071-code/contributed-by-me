from __future__ import annotations

import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER_DIR = ROOT / "paper"

FILLS_PATH = PAPER_DIR / "fills.csv"
TRADES_PATH = PAPER_DIR / "trades.csv"
TRADES_CALC_PATH = PAPER_DIR / "trades_calc.csv"
STATE_PATH = PAPER_DIR / "paper_state.json"
SNAPSHOT_PATH = LOG_DIR / "entry_signal_snapshot_latest.csv"
ROOTB_LEDGER_PATH = Path(r"E:\vibe\buffett\data\ledger\paper_fills_ledger.csv")

OUT_JSON = LOG_DIR / "entry_trace_performance_latest.json"
OUT_CSV = LOG_DIR / "entry_trace_performance_latest.csv"
OUT_SUMMARY_CSV = LOG_DIR / "entry_trace_performance_summary_latest.csv"


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype=str)


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8", "utf-8-sig", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except UnicodeDecodeError:
            continue
        except Exception:
            return {}
    return {}


def _load_rootb_entry_lineage(path: Path) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        return {}
    rows = _read_csv(path)
    if rows.empty:
        return {}
    out: Dict[str, Dict[str, str]] = {}
    for _, row in rows.iterrows():
        side = str(row.get("side", "") or "").upper()
        if side != "BUY":
            continue
        order_id = str(
            row.get("entry_order_id")
            or row.get("source_order_id")
            or row.get("order_id")
            or ""
        ).strip()
        trace = str(row.get("entry_trace_id") or "").strip()
        if not order_id or not trace:
            continue
        item = out.setdefault(order_id, {})
        for src_col, dst_col in (
            ("entry_trace_id", "entry_trace_id"),
            ("entry_intent_id", "entry_intent_id"),
            ("entry_order_id", "entry_order_id"),
            ("source_order_id", "source_order_id"),
            ("lineage_origin", "lineage_origin"),
            ("source", "lineage_source"),
        ):
            value = str(row.get(src_col) or "").strip()
            if value and not item.get(dst_col):
                item[dst_col] = value
    return out


def _note_fields(note: Any) -> Dict[str, str]:
    text = str(note or "")
    out: Dict[str, str] = {}
    # Some calculated trade notes prefix the raw note with "order_id=... |".
    text = text.replace(" | ", ";")
    for part in text.split(";"):
        if "=" not in part:
            continue
        key, val = part.split("=", 1)
        key = str(key or "").strip()
        if not key:
            continue
        out[key] = str(val or "").strip()
    return out


def _extract_ymd(value: Any) -> str:
    digits = re.sub(r"[^0-9]", "", str(value or ""))
    return digits[:8] if len(digits) >= 8 else ""


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        val = float(value)
        if math.isnan(val) or math.isinf(val):
            return default
        return val
    except Exception:
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if pd.isna(value):
            return default
        return int(float(value))
    except Exception:
        return default


def _build_entry_rows(fills: pd.DataFrame, rootb_lineage: Dict[str, Dict[str, str]] | None = None) -> pd.DataFrame:
    if fills.empty:
        return pd.DataFrame()
    work = fills.copy()
    if "side" not in work.columns:
        return pd.DataFrame()
    work = work[work["side"].astype(str).str.upper() == "BUY"].copy()
    if work.empty:
        return pd.DataFrame()

    rows: List[Dict[str, Any]] = []
    for _, row in work.iterrows():
        note = _note_fields(row.get("note", ""))
        order_id = str(row.get("order_id", "") or "").strip()
        lineage = (rootb_lineage or {}).get(order_id, {})
        entry_trace_id = note.get("entry_trace_id", "") or lineage.get("entry_trace_id", "")
        if not entry_trace_id:
            entry_trace_id = f"MISSING_TRACE_{order_id}"
        entry_intent_id = note.get("entry_intent_id", "") or lineage.get("entry_intent_id", "")
        entry_order_id = note.get("entry_order_id", "") or lineage.get("entry_order_id", "") or order_id
        rows.append(
            {
                "entry_trace_id": entry_trace_id,
                "entry_intent_id": entry_intent_id,
                "entry_order_id": entry_order_id,
                "lineage_trace_source": "rootb_ledger" if lineage.get("entry_trace_id") and not note.get("entry_trace_id", "") else "paper_note",
                "lineage_origin": lineage.get("lineage_origin", ""),
                "code": str(row.get("code", "") or "").zfill(6),
                "entry_ts": str(row.get("datetime", "") or ""),
                "entry_ymd": _extract_ymd(row.get("datetime", "")),
                "entry_qty": _to_int(row.get("qty"), 0),
                "entry_price": _to_float(row.get("price"), 0.0),
                "entry_notional": _to_int(row.get("qty"), 0) * _to_float(row.get("price"), 0.0),
                "signal_date": note.get("signal_date", ""),
                "entry_timing": note.get("entry_timing", ""),
                "split_entry": note.get("split_entry", ""),
                "horizon": note.get("horizon", ""),
                "surge_immediate": note.get("surge_immediate", ""),
                "surge_type": note.get("surge_type", ""),
                "surge_score": note.get("surge_score", ""),
                "run_label": note.get("run_label", ""),
                "replay_chain_id": note.get("replay_chain_id", ""),
            }
        )
    return pd.DataFrame(rows)


def _build_closed_rows(trades: pd.DataFrame, trades_calc: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    if not trades.empty:
        for _, row in trades.iterrows():
            note = _note_fields(row.get("note", ""))
            trace = note.get("entry_trace_id") or note.get("source_trace_id") or ""
            if not trace:
                continue
            rows.append(
                {
                    "entry_trace_id": trace,
                    "closed_trade_id": str(row.get("trade_id", "") or ""),
                    "exit_ts": str(row.get("exit_date", "") or ""),
                    "exit_ymd": _extract_ymd(row.get("exit_date", "")),
                    "exit_reason": str(row.get("exit_reason", "") or note.get("exit_reason", "")),
                    "closed_qty": _to_int(row.get("qty") or note.get("sell_qty"), 0),
                    "pnl_pct": _to_float(row.get("pnl_pct"), 0.0),
                    "pnl_krw": _to_float(row.get("pnl_krw"), 0.0),
                    "net_ret": _to_float(row.get("pnl_pct"), 0.0),
                    "source": "trades.csv",
                }
            )
    if not trades_calc.empty:
        for _, row in trades_calc.iterrows():
            note = _note_fields(row.get("note", ""))
            trace = note.get("entry_trace_id") or note.get("source_trace_id") or ""
            if not trace:
                continue
            rows.append(
                {
                    "entry_trace_id": trace,
                    "closed_trade_id": str(row.get("trade_id", "") or ""),
                    "exit_ts": str(row.get("exit_ts", "") or ""),
                    "exit_ymd": _extract_ymd(row.get("exit_ts", "")),
                    "exit_reason": str(note.get("exit_reason", "") or ""),
                    "closed_qty": _to_int(row.get("qty"), 0),
                    "pnl_pct": _to_float(row.get("gross_ret"), 0.0),
                    "pnl_krw": 0.0,
                    "net_ret": _to_float(row.get("net_ret"), 0.0),
                    "source": "trades_calc.csv",
                }
            )
    if not rows:
        return pd.DataFrame()
    closed = pd.DataFrame(rows)
    # Prefer trades_calc rows for net_ret; keep both sources aggregated by trace.
    return closed


def _build_open_rows(state: Dict[str, Any]) -> pd.DataFrame:
    open_pos = state.get("open_positions")
    if not isinstance(open_pos, list):
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    for pos in open_pos:
        if not isinstance(pos, dict):
            continue
        trace = str(pos.get("entry_trace_id") or "").strip()
        if not trace:
            trace = f"MISSING_OPEN_TRACE_{str(pos.get('code', '') or '').zfill(6)}"
        rows.append(
            {
                "entry_trace_id": trace,
                "open_qty": _to_int(pos.get("qty"), 0),
                "open_entry_price": _to_float(pos.get("entry_price"), 0.0),
                "open_entry_date": str(pos.get("entry_date") or pos.get("entry_ts") or ""),
                "open_last_price": _to_float(pos.get("last_price"), 0.0),
            }
        )
    return pd.DataFrame(rows)


def _entry_class(row: pd.Series) -> str:
    split = str(row.get("split_entry") or "").strip().lower()
    surge = str(row.get("surge_immediate") or "").strip().lower()
    surge_type = str(row.get("surge_type") or "").strip()
    timing = str(row.get("entry_timing") or "").strip()
    if split and split not in {"1st", "first"}:
        return "split_entry"
    if surge in {"1", "true", "yes", "y"} or surge_type:
        return "surge_entry"
    if "validation_reduce" in str(row.get("entry_gate_reason") or ""):
        return "validation_reduce_entry"
    if timing:
        return f"normal_{timing}"
    return "normal_entry"


def run() -> Dict[str, Any]:
    fills = _read_csv(FILLS_PATH)
    trades = _read_csv(TRADES_PATH)
    trades_calc = _read_csv(TRADES_CALC_PATH)
    state = _read_json(STATE_PATH)
    rootb_lineage = _load_rootb_entry_lineage(ROOTB_LEDGER_PATH)

    entries = _build_entry_rows(fills, rootb_lineage)
    closed = _build_closed_rows(trades, trades_calc)
    open_rows = _build_open_rows(state)

    if entries.empty:
        out = pd.DataFrame()
    else:
        out = entries.copy()
        if not closed.empty:
            agg = closed.groupby("entry_trace_id", dropna=False).agg(
                closed_rows=("entry_trace_id", "size"),
                closed_qty=("closed_qty", "sum"),
                net_ret_sum=("net_ret", "sum"),
                net_ret_mean=("net_ret", "mean"),
                pnl_krw_sum=("pnl_krw", "sum"),
                exit_reasons=("exit_reason", lambda s: ",".join(sorted({str(x) for x in s if str(x)}))),
                last_exit_ymd=("exit_ymd", "max"),
            ).reset_index()
            out = out.merge(agg, on="entry_trace_id", how="left")
        if not open_rows.empty:
            out = out.merge(open_rows, on="entry_trace_id", how="left")

        for col in ["closed_rows", "closed_qty", "net_ret_sum", "net_ret_mean", "pnl_krw_sum", "open_qty"]:
            if col not in out.columns:
                out[col] = 0
        out["closed_rows"] = pd.to_numeric(out["closed_rows"], errors="coerce").fillna(0).astype(int)
        out["closed_qty"] = pd.to_numeric(out["closed_qty"], errors="coerce").fillna(0).astype(int)
        out["open_qty"] = pd.to_numeric(out["open_qty"], errors="coerce").fillna(0).astype(int)
        out["status"] = out.apply(
            lambda r: "closed" if int(r.get("closed_qty", 0)) >= int(r.get("entry_qty", 0)) and int(r.get("entry_qty", 0)) > 0
            else ("open_partial" if int(r.get("closed_qty", 0)) > 0 or int(r.get("open_qty", 0)) > 0 else "open_or_unmatched"),
            axis=1,
        )
        out["entry_class"] = out.apply(_entry_class, axis=1)

    if out.empty:
        summary = pd.DataFrame(
            columns=[
                "entry_class",
                "entries",
                "closed_entries",
                "realized_entries",
                "open_entries",
                "win_rate_realized",
                "net_ret_mean_realized",
            ]
        )
    else:
        summary_rows: List[Dict[str, Any]] = []
        for entry_class, grp in out.groupby("entry_class", dropna=False):
            realized = grp[pd.to_numeric(grp.get("closed_rows", 0), errors="coerce").fillna(0) > 0].copy()
            realized_ret = pd.to_numeric(realized.get("net_ret_sum", pd.Series(dtype=float)), errors="coerce").dropna()
            summary_rows.append(
                {
                    "entry_class": str(entry_class),
                    "entries": int(len(grp)),
                    "closed_entries": int((grp["status"] == "closed").sum()),
                    "realized_entries": int(len(realized)),
                    "open_entries": int((grp["status"] != "closed").sum()),
                    "win_rate_realized": float((realized_ret > 0).mean()) if len(realized_ret) else 0.0,
                    "net_ret_mean_realized": float(realized_ret.mean()) if len(realized_ret) else 0.0,
                }
            )
        summary = pd.DataFrame(summary_rows)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    summary.to_csv(OUT_SUMMARY_CSV, index=False, encoding="utf-8-sig")

    payload = {
        "generated_at": _now_ts(),
        "status": "OK",
        "inputs": {
            "fills": str(FILLS_PATH),
            "trades": str(TRADES_PATH),
            "trades_calc": str(TRADES_CALC_PATH),
            "state": str(STATE_PATH),
            "snapshot": str(SNAPSHOT_PATH),
            "rootb_ledger": str(ROOTB_LEDGER_PATH),
        },
        "outputs": {
            "detail_csv": str(OUT_CSV),
            "summary_csv": str(OUT_SUMMARY_CSV),
        },
        "counts": {
            "entry_rows": int(len(entries)),
            "closed_rows": int(len(closed)),
            "open_rows": int(len(open_rows)),
            "detail_rows": int(len(out)),
            "summary_rows": int(len(summary)),
            "missing_trace_entries": int(out["entry_trace_id"].astype(str).str.startswith("MISSING_TRACE_").sum()) if not out.empty else 0,
            "rootb_lineage_entries": int((out.get("lineage_trace_source", pd.Series(dtype=str)).astype(str) == "rootb_ledger").sum()) if not out.empty else 0,
        },
        "summary": summary.to_dict(orient="records"),
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> int:
    payload = run()
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
