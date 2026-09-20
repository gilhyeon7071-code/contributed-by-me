from __future__ import annotations

import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PAPER_DIR = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"
FILLS_PATH = PAPER_DIR / "fills.csv"


def _norm_ymd(value: Any) -> str:
    digits = re.sub(r"[^0-9]", "", str(value or ""))
    return digits[:8] if len(digits) >= 8 else ""


def _z6(value: Any) -> str:
    digits = re.sub(r"[^0-9]", "", str(value or ""))
    return digits.zfill(6) if digits else ""


def _parse_note_field(note: Any, key: str) -> str:
    m = re.search(rf"(?:^|[;|]){re.escape(key)}=([^;|]+)", str(note or ""))
    return m.group(1).strip() if m else ""


def _to_bool_value(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "y", "yes"}


def _source_entry_block_from_note(note: Any) -> tuple[bool, str]:
    decision = _parse_note_field(note, "source_entry_decision").strip().upper()
    blocked_text = _parse_note_field(note, "source_entry_blocked")
    reason = _parse_note_field(note, "source_entry_reason")
    exclude_reasons = _parse_note_field(note, "source_exclude_reasons")
    blocked = _to_bool_value(blocked_text) or decision == "ENTRY_BLOCKED"
    if not blocked:
        return False, ""
    parts = []
    if decision:
        parts.append(f"source_entry_decision={decision}")
    if blocked_text:
        parts.append(f"source_entry_blocked={blocked_text}")
    if reason:
        parts.append(f"source_entry_reason={reason}")
    if exclude_reasons:
        parts.append(f"source_exclude_reasons={exclude_reasons}")
    return True, "SOURCE_ENTRY_BLOCKED:" + ";".join(parts)


def _is_stop_row(side: Any, note: Any) -> bool:
    if str(side or "").strip().upper() != "SELL":
        return False
    text = str(note or "").upper()
    return "STOP" in text or "STOP_GAP" in text


def _to_bool_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "y", "yes"})


_NOTE_PRIORITY_KEYS = (
    "source_entry_decision",
    "source_entry_reason",
    "source_entry_allowed",
    "source_entry_blocked",
    "source_exclude_reasons",
    "surge_paper_probe_allowed",
    "surge_paper_probe_block_reasons",
)


def _merge_note_values(note_vals: list[str], limit: int = 500) -> str:
    seen_notes = list(dict.fromkeys(v for v in note_vals if v))
    priority_tokens: list[str] = []
    other_tokens: list[str] = []
    seen_tokens: set[str] = set()

    for note in seen_notes:
        for raw in str(note).split(";"):
            token = raw.strip()
            if not token or token in seen_tokens:
                continue
            seen_tokens.add(token)
            key = token.split("=", 1)[0].strip()
            if key in _NOTE_PRIORITY_KEYS:
                priority_tokens.append(token)
            else:
                other_tokens.append(token)

    return ";".join(priority_tokens + other_tokens)[:limit]


def _merge_duplicate_orders(out_df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    key_cols = ["exec_date", "side", "code"]
    dup_rows = int(out_df.duplicated(subset=key_cols, keep=False).sum())
    if dup_rows == 0:
        return out_df, 0

    merged_rows = []
    for _, g in out_df.groupby(key_cols, sort=False, dropna=False):
        qty_num = pd.to_numeric(g["fill_qty"], errors="coerce").fillna(0.0)
        px_num = pd.to_numeric(g["fill_price"], errors="coerce").fillna(0.0)
        qty_sum = float(qty_num.sum())
        if qty_sum > 0:
            px = float((qty_num * px_num).sum() / qty_sum)
        else:
            px = float(px_num.iloc[-1]) if len(px_num) else 0.0
        side = str(g["side"].iloc[0]).upper().strip()
        stop_level = px * 0.95 if side == "BUY" else None

        signal_date = next((str(v) for v in g["signal_date"].tolist() if str(v).strip()), "")
        signal_ts = next((str(v) for v in g["signal_ts"].tolist() if str(v).strip()), "") if "signal_ts" in g.columns else ""
        note_vals = [str(v).strip() for v in g["note"].tolist() if str(v).strip()]
        note = _merge_note_values(note_vals)
        reason_vals = [str(v).strip() for v in g["reason"].tolist() if str(v).strip()]
        reason = " | ".join(dict.fromkeys(reason_vals).keys())[:300]
        entry_block_reason_vals = [str(v).strip() for v in g["entry_block_reason"].tolist() if str(v).strip()]
        entry_block_reason = " | ".join(dict.fromkeys(entry_block_reason_vals).keys())[:300]

        merged_rows.append(
            {
                "exec_date": str(g["exec_date"].iloc[0]),
                "side": str(g["side"].iloc[0]),
                "code": str(g["code"].iloc[0]),
                "fill_qty": qty_sum,
                "fill_price": px,
                "stop_level": stop_level,
                "signal_date": signal_date,
                "signal_ts": signal_ts,
                "is_stop": bool(_to_bool_series(g["is_stop"]).any()),
                "note": note,
                "reason": reason,
                "entry_blocked": bool(_to_bool_series(g["entry_blocked"]).any()),
                "entry_block_reason": entry_block_reason,
                "posthoc_policy_violation": bool(
                    _to_bool_series(g.get("posthoc_policy_violation", pd.Series(False, index=g.index))).any()
                ),
                "posthoc_policy_reason": " | ".join(
                    dict.fromkeys(
                        str(v).strip()
                        for v in g.get("posthoc_policy_reason", pd.Series("", index=g.index)).tolist()
                        if str(v).strip()
                    ).keys()
                )[:300],
                "execution_blocked": bool(
                    _to_bool_series(g.get("execution_blocked", pd.Series(False, index=g.index))).any()
                ),
                "execution_block_reason": " | ".join(
                    dict.fromkeys(
                        str(v).strip()
                        for v in g.get("execution_block_reason", pd.Series("", index=g.index)).tolist()
                        if str(v).strip()
                    ).keys()
                )[:300],
                "disclosure_policy_available": bool(
                    _to_bool_series(g.get("disclosure_policy_available", pd.Series(False, index=g.index))).any()
                ),
            }
        )

    return pd.DataFrame(merged_rows), dup_rows


def build(as_of: str) -> dict:
    as_of = _norm_ymd(as_of)
    if not as_of:
        raise ValueError("as_of must be YYYYMMDD")
    if not FILLS_PATH.exists():
        raise FileNotFoundError(str(FILLS_PATH))

    PAPER_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    fills = pd.read_csv(FILLS_PATH, dtype=str).fillna("")
    required = ["datetime", "code", "side", "qty", "price", "order_id", "note"]
    missing = [col for col in required if col not in fills.columns]
    if missing:
        raise ValueError(f"fills missing columns: {missing}")

    work = fills.copy()
    work["_ymd"] = work["datetime"].map(_norm_ymd)
    work = work[work["_ymd"].eq(as_of)].copy()

    out_path = PAPER_DIR / f"orders_{as_of}_exec.xlsx"
    status_path = LOG_DIR / f"intraday_orders_exec_from_fills_{as_of}.json"
    latest_path = LOG_DIR / "intraday_orders_exec_from_fills_latest.json"

    if work.empty:
        out = pd.DataFrame(
            columns=[
                "exec_date",
                "side",
                "code",
                "fill_qty",
                "fill_price",
                "stop_level",
                "signal_date",
                "fill_datetime",
                "signal_ts",
                "is_stop",
                "note",
                "reason",
                "entry_blocked",
                "entry_block_reason",
                "posthoc_policy_violation",
                "posthoc_policy_reason",
                "execution_blocked",
                "execution_block_reason",
                "disclosure_policy_available",
            ]
        )
    else:
        side_u = work["side"].astype(str).str.strip().str.upper()
        fill_price = pd.to_numeric(work["price"], errors="coerce")
        source_entry_guard = work["note"].map(_source_entry_block_from_note)
        source_entry_blocked = source_entry_guard.map(lambda x: bool(x[0]))
        source_entry_reason = source_entry_guard.map(lambda x: str(x[1]))
        out = pd.DataFrame(
            {
                "exec_date": as_of,
                "side": side_u,
                "code": work["code"].map(_z6),
                "fill_qty": pd.to_numeric(work["qty"], errors="coerce"),
                "fill_price": fill_price,
                "stop_level": fill_price.where(side_u.eq("BUY")) * 0.95,
                "signal_date": work["note"].map(lambda x: _parse_note_field(x, "signal_date")),
                "fill_datetime": work["datetime"],
                "signal_ts": work["note"].map(lambda x: _parse_note_field(x, "signal_ts")),
                "is_stop": [
                    _is_stop_row(side, note)
                    for side, note in zip(work["side"].tolist(), work["note"].tolist())
                ],
                "note": work["note"],
                "reason": "",
                "entry_blocked": source_entry_blocked,
                "entry_block_reason": source_entry_reason,
                "posthoc_policy_violation": False,
                "posthoc_policy_reason": "",
                "execution_blocked": False,
                "execution_block_reason": "",
                "disclosure_policy_available": False,
            }
        )

    raw_rows = int(len(out))
    out, merged_duplicate_rows = _merge_duplicate_orders(out)
    out.to_excel(out_path, index=False)
    qty = pd.to_numeric(out.get("fill_qty", pd.Series(dtype=float)), errors="coerce")
    side_counts = out["side"].astype(str).str.upper().value_counts(dropna=False).to_dict() if "side" in out else {}
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS",
        "mode": "intraday_runtime_from_fills",
        "as_of": as_of,
        "source": str(FILLS_PATH),
        "output": str(out_path),
        "rows": int(len(out)),
        "raw_rows": raw_rows,
        "merged_duplicate_rows": int(merged_duplicate_rows),
        "side_counts": side_counts,
        "source_entry_blocked_rows": int(_to_bool_series(out.get("entry_blocked", pd.Series(dtype=bool))).sum()) if len(out) else 0,
        "qty_total": float(qty.sum(skipna=True)) if len(qty) else 0.0,
        "policy_change": False,
        "note": "Intraday dispatch contract file generated from same-day paper fills only; does not change gates, sizing, orders, fills, ledger, or risk policy.",
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    status_path.write_text(text, encoding="utf-8")
    latest_path.write_text(text, encoding="utf-8")
    return payload


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv if argv is None else argv)
    as_of = argv[1] if len(argv) > 1 else datetime.now().strftime("%Y%m%d")
    try:
        payload = build(as_of)
    except Exception as exc:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        err = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "status": "FAIL",
            "as_of": _norm_ymd(as_of),
            "error": f"{type(exc).__name__}:{exc}",
            "policy_change": False,
        }
        (LOG_DIR / "intraday_orders_exec_from_fills_latest.json").write_text(
            json.dumps(err, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(json.dumps(err, ensure_ascii=False))
        return 2
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
