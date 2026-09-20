from __future__ import annotations

import importlib.util
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PNL_REPORT = ROOT / "paper_pnl_report.py"
TRADES_CALC = ROOT / "paper" / "trades_calc.csv"
FILLS_NORM = ROOT / "paper" / "fills_norm.csv"
PRICES = ROOT / "paper" / "prices" / "ohlcv_paper.parquet"
ORDERS_DIR = ROOT / "paper"
OUT_JSON = LOG_DIR / "mdd_intraday_stopgap_legacy_shadow_latest.json"
OUT_CSV = LOG_DIR / "mdd_intraday_stopgap_legacy_shadow_latest.csv"


def _load_pnl_module() -> Any:
    spec = importlib.util.spec_from_file_location("paper_pnl_report_runtime", PNL_REPORT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed_to_load:{PNL_REPORT}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _note_value(note: Any, key: str) -> str:
    m = re.search(r"(?:^|[;|\s])" + re.escape(key) + r"=([^;|\s]+)", str(note or ""))
    return m.group(1).strip() if m else ""


def _norm_code(value: Any) -> str:
    s = re.sub(r"\D", "", str(value or ""))
    return s.zfill(6) if s else ""


def _ymd(value: Any) -> str:
    s = re.sub(r"\D", "", str(value or ""))
    return s[:8] if len(s) >= 8 else ""


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        out = float(str(value).replace(",", ""))
        return default if out != out else out
    except Exception:
        return default


def _official_scope(pnl: Any) -> tuple[pd.DataFrame, str, dict[str, Any]]:
    raw = pnl._ensure_exit_date(pnl._load_trades(TRADES_CALC))
    df, op_scope = pnl._apply_operational_scope(raw)
    df, blocked = pnl._filter_blocked_entry_trades(df, ORDERS_DIR)
    df, unauditable = pnl._filter_unauditable_surge_trades(df)
    ret_col = pnl._pick_ret_col(df)
    if not ret_col:
        raise RuntimeError("return_column_missing")
    return df.copy(), ret_col, {"raw": int(len(raw)), "operational_scope": op_scope, "blocked": blocked, "unauditable": unauditable}


def _prepare_trades(df: pd.DataFrame, pnl: Any, ret_col: str) -> pd.DataFrame:
    out = pnl._ensure_exit_date(df.copy())
    out["exit_date"] = out["exit_date"].map(pnl._normalize_ymd)
    if "entry_ts" in out.columns:
        out["entry_date"] = out["entry_ts"].astype(str).map(pnl._normalize_ymd)
    elif "entry_date" in out.columns:
        out["entry_date"] = out["entry_date"].astype(str).map(pnl._normalize_ymd)
    else:
        out["entry_date"] = ""
    out["code_norm"] = out.get("code", pd.Series("", index=out.index)).map(_norm_code)
    out["note_s"] = out.get("note", pd.Series("", index=out.index)).fillna("").astype(str)
    out["entry_order_id_note"] = out["note_s"].map(lambda s: _note_value(s, "entry_order_id") or _note_value(s, "order_id"))
    out["entry_timing_note"] = out["note_s"].map(lambda s: _note_value(s, "entry_timing"))
    for c in [ret_col, "qty", "entry_price", "exit_price"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def _load_fills() -> pd.DataFrame:
    fills = pd.read_csv(FILLS_NORM, dtype=str, encoding="utf-8-sig").fillna("")
    fills = fills.copy()
    fills["code_norm"] = fills["code"].map(_norm_code)
    fills["date_norm"] = fills["date"].map(_ymd)
    fills["qty_n"] = pd.to_numeric(fills["qty"], errors="coerce").fillna(0.0)
    fills["price_n"] = pd.to_numeric(fills["price"], errors="coerce").fillna(0.0)
    fills["note_s"] = fills["note"].astype(str)
    fills["entry_oid"] = fills["note_s"].map(lambda s: _note_value(s, "entry_order_id") or _note_value(s, "source_order_id"))
    fills["exit_reason"] = fills["note_s"].map(lambda s: _note_value(s, "exit_reason"))
    return fills


def _load_prices() -> pd.DataFrame:
    px = pd.read_parquet(PRICES)
    px = px.copy()
    date_col = next((c for c in ["ymd", "date", "trade_date"] if c in px.columns), "")
    if not date_col:
        raise KeyError(f"price date column missing: {list(px.columns)}")
    px["code_norm"] = px["code"].map(_norm_code)
    px["ymd"] = px[date_col].astype(str).str.slice(0, 8)
    for col in ["open", "close"]:
        px[col] = pd.to_numeric(px[col], errors="coerce")
    return px


def _metrics(df: pd.DataFrame, pnl: Any, ret_col: str) -> dict[str, Any]:
    eq = pnl._equity_metrics(df.copy(), ret_col)
    ret = pd.to_numeric(df[ret_col], errors="coerce") if ret_col in df.columns else pd.Series(dtype=float)
    return {
        "rows": int(len(df)),
        "trades_used": int(ret.notna().sum()),
        "sum_ret": float(ret.sum()) if len(ret) else 0.0,
        "avg_ret": float(ret.mean()) if len(ret) else None,
        "loss_n": int((ret < 0).sum()) if len(ret) else 0,
        "max_drawdown_pct": eq.get("max_drawdown_pct"),
        "end_equity": eq.get("end_equity"),
    }


def _find_legacy_candidates(work: pd.DataFrame, fills: pd.DataFrame, prices: pd.DataFrame) -> list[dict[str, Any]]:
    buy_by_oid = fills[fills["side"].str.upper().eq("BUY")].set_index("order_id", drop=False)
    rows: list[dict[str, Any]] = []
    seen_trade_indexes: set[int] = set()
    stop_fills = fills[
        fills["side"].str.upper().eq("SELL")
        & fills["exit_reason"].eq("STOP_GAP")
        & fills["entry_oid"].astype(str).ne("")
    ].copy()
    for _, sf in stop_fills.iterrows():
        entry_oid = str(sf["entry_oid"])
        if entry_oid not in buy_by_oid.index:
            continue
        bf = buy_by_oid.loc[entry_oid]
        if isinstance(bf, pd.DataFrame):
            bf = bf.iloc[0]
        if _note_value(bf.get("note"), "entry_timing") != "intraday_realtime":
            continue
        code = str(sf["code_norm"])
        date = str(sf["date_norm"])
        px = prices[prices["code_norm"].eq(code) & prices["ymd"].eq(date)]
        if px.empty:
            continue
        open_px = _f(px.iloc[0].get("open"), 0.0)
        close_px = _f(px.iloc[0].get("close"), 0.0)
        entry_px = _f(bf.get("price"), 0.0)
        exit_px = _f(sf.get("price"), 0.0)
        if entry_px <= 0 or exit_px <= 0:
            continue
        stop_price = entry_px * 0.95
        current_o_stopgap = entry_px <= stop_price
        legacy_open_stopgap = open_px <= stop_price
        if not (legacy_open_stopgap and not current_o_stopgap and abs(exit_px - open_px) < 0.0001):
            continue
        match = work[
            work["code_norm"].eq(code)
            & work["exit_date"].eq(date)
            & (work["entry_price"].sub(entry_px).abs() < 0.0001)
            & (work["exit_price"].sub(exit_px).abs() < 0.0001)
            & (
                (work.get("qty", pd.Series(0, index=work.index)).sub(_f(sf.get("qty"), 0.0)).abs() < 0.0001)
                | (work["entry_order_id_note"].eq(entry_oid))
            )
        ]
        for idx, tr in match.iterrows():
            if int(idx) in seen_trade_indexes:
                continue
            seen_trade_indexes.add(int(idx))
            actual_ret = _f(tr.get("net_ret", tr.get("pnl_pct")), 0.0)
            gross_actual = (exit_px - entry_px) / entry_px
            cost_drag = gross_actual - actual_ret
            close_net_proxy = ((close_px - entry_px) / entry_px) - cost_drag if close_px > 0 else None
            rows.append(
                {
                    "trade_index": int(idx),
                    "trade_id": str(tr.get("trade_id") or ""),
                    "code": code,
                    "date": date,
                    "entry_order_id": entry_oid,
                    "qty": _f(sf.get("qty"), 0.0),
                    "entry_price": entry_px,
                    "exit_price": exit_px,
                    "day_open": open_px,
                    "day_close": close_px,
                    "actual_ret": actual_ret,
                    "close_net_proxy": close_net_proxy,
                    "delta_ret": (close_net_proxy - actual_ret) if close_net_proxy is not None else None,
                    "sell_order_id": str(sf.get("order_id") or ""),
                }
            )
    return rows


def main() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    pnl = _load_pnl_module()
    scoped, ret_col, meta = _official_scope(pnl)
    work = _prepare_trades(scoped, pnl, ret_col)
    fills = _load_fills()
    prices = _load_prices()
    candidates = _find_legacy_candidates(work, fills, prices)

    idx_to_ret = {
        int(c["trade_index"]): float(c["close_net_proxy"])
        for c in candidates
        if c.get("close_net_proxy") is not None
    }
    sim = work.copy()
    for idx, new_ret in idx_to_ret.items():
        sim.loc[idx, ret_col] = new_ret
    baseline = _metrics(work, pnl, ret_col)
    after = _metrics(sim, pnl, ret_col)
    pd.DataFrame(candidates).to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "status": "FAIL" if candidates else "PASS",
        "policy_change": False,
        "trading_effect": False,
        "source": {
            "trades_calc": str(TRADES_CALC),
            "fills_norm": str(FILLS_NORM),
            "prices": str(PRICES),
        },
        "scope_meta": meta,
        "baseline": baseline,
        "candidate_rows": len(candidates),
        "unique_codes": sorted({str(c["code"]) for c in candidates}),
        "after_replace_with_entryday_close": after,
        "delta_vs_baseline": {
            "sum_ret": after["sum_ret"] - baseline["sum_ret"],
            "max_drawdown_pct": (after.get("max_drawdown_pct") or 0.0) - (baseline.get("max_drawdown_pct") or 0.0),
        },
        "candidates": candidates,
        "interpretation": [
            "Read-only shadow replacement for intraday_realtime STOP_GAP rows that look like legacy entry-date open artifacts.",
            "Rows are replaced with same-day close net-return proxies only for diagnostic MDD impact.",
            "This does not modify historical fills/trades or production policy.",
        ],
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": payload["status"], "candidate_rows": len(candidates), "baseline_mdd": baseline.get("max_drawdown_pct"), "after_mdd": after.get("max_drawdown_pct"), "delta": payload["delta_vs_baseline"], "out_json": str(OUT_JSON), "out_csv": str(OUT_CSV)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
