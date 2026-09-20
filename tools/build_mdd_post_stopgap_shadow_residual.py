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
ORDERS_DIR = ROOT / "paper"
STOPGAP_SHADOW = LOG_DIR / "mdd_intraday_stopgap_legacy_shadow_latest.json"
OUT_JSON = LOG_DIR / "mdd_post_stopgap_shadow_residual_latest.json"
OUT_CSV = LOG_DIR / "mdd_post_stopgap_shadow_residual_latest.csv"


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


def _prepare(df: pd.DataFrame, pnl: Any, ret_col: str) -> pd.DataFrame:
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
    for key in ["exit_reason", "entry_timing", "surge_type", "horizon", "sell_ratio_pct", "partial_exit", "entry_order_id"]:
        out[key] = out["note_s"].map(lambda s, k=key: _note_value(s, k))
    for c in [ret_col, "qty", "entry_price", "exit_price"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    out["_ret"] = pd.to_numeric(out[ret_col], errors="coerce")
    out["_is_stopgap_shadow_candidate"] = False
    return out


def _load_sell_fills() -> pd.DataFrame:
    fills = pd.read_csv(FILLS_NORM, dtype=str, encoding="utf-8-sig").fillna("")
    fills = fills[fills["side"].astype(str).str.upper().eq("SELL")].copy()
    fills["code_norm"] = fills["code"].map(_norm_code)
    fills["date_norm"] = fills["date"].map(lambda v: re.sub(r"\D", "", str(v or ""))[:8])
    fills["entry_order_id"] = fills["note"].map(lambda s: _note_value(s, "entry_order_id") or _note_value(s, "source_order_id"))
    fills["exit_reason_fill"] = fills["note"].map(lambda s: _note_value(s, "exit_reason"))
    fills["sell_ratio_pct_fill"] = fills["note"].map(lambda s: _note_value(s, "sell_ratio_pct"))
    fills["partial_exit_fill"] = fills["note"].map(lambda s: _note_value(s, "partial_exit"))
    fills["qty_n"] = pd.to_numeric(fills["qty"], errors="coerce").fillna(0.0)
    fills["price_n"] = pd.to_numeric(fills["price"], errors="coerce").fillna(0.0)
    return fills


def _restore_exit_meta(work: pd.DataFrame) -> pd.DataFrame:
    sells = _load_sell_fills()
    out = work.copy()
    for idx, row in out.iterrows():
        code = str(row.get("code_norm") or "")
        exit_date = str(row.get("exit_date") or "")
        entry_oid = str(row.get("entry_order_id") or row.get("entry_order_id_note") or "")
        qty = _f(row.get("qty"), 0.0)
        exit_price = _f(row.get("exit_price"), 0.0)
        cand = sells[sells["code_norm"].eq(code) & sells["date_norm"].eq(exit_date)].copy()
        if entry_oid:
            by_oid = cand[cand["entry_order_id"].eq(entry_oid)]
            if not by_oid.empty:
                cand = by_oid
        if qty > 0:
            by_qty = cand[cand["qty_n"].sub(qty).abs() < 0.0001]
            if not by_qty.empty:
                cand = by_qty
        if exit_price > 0:
            by_price = cand[cand["price_n"].sub(exit_price).abs() < 0.0001]
            if not by_price.empty:
                cand = by_price
        if cand.empty:
            continue
        hit = cand.iloc[0]
        out.at[idx, "exit_reason"] = str(hit.get("exit_reason_fill") or out.at[idx, "exit_reason"])
        out.at[idx, "sell_ratio_pct"] = str(hit.get("sell_ratio_pct_fill") or out.at[idx, "sell_ratio_pct"])
        out.at[idx, "partial_exit"] = str(hit.get("partial_exit_fill") or out.at[idx, "partial_exit"])
    return out


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


def _apply_stopgap_shadow(work: pd.DataFrame, ret_col: str) -> pd.DataFrame:
    payload = json.loads(STOPGAP_SHADOW.read_text(encoding="utf-8"))
    out = work.copy()
    for item in payload.get("candidates", []) or []:
        idx = int(item.get("trade_index"))
        if idx in out.index and item.get("close_net_proxy") is not None:
            out.loc[idx, ret_col] = float(item["close_net_proxy"])
            out.loc[idx, "_ret"] = float(item["close_net_proxy"])
            out.loc[idx, "_is_stopgap_shadow_candidate"] = True
    return out


def _breakdown(df: pd.DataFrame, ret_col: str, axis: str, top: int = 20) -> list[dict[str, Any]]:
    if df.empty or axis not in df.columns:
        return []
    g = (
        df.groupby(axis, dropna=False)
        .agg(
            trades=("trade_id", "count"),
            avg_ret=(ret_col, "mean"),
            sum_ret=(ret_col, "sum"),
            loss_n=(ret_col, lambda s: int((pd.to_numeric(s, errors="coerce") < 0).sum())),
            entry_notional=("entry_price", lambda s: 0.0),
        )
        .reset_index()
    )
    rows = []
    for _, r in g.iterrows():
        rows.append(
            {
                "axis": axis,
                "value": str(r[axis]),
                "trades": int(r["trades"]),
                "avg_ret": float(r["avg_ret"]) if pd.notna(r["avg_ret"]) else None,
                "sum_ret": float(r["sum_ret"]) if pd.notna(r["sum_ret"]) else 0.0,
                "loss_n": int(r["loss_n"]),
            }
        )
    return sorted(rows, key=lambda x: x["sum_ret"])[:top]


def _top_losses(df: pd.DataFrame, ret_col: str, n: int = 20) -> list[dict[str, Any]]:
    keep = [c for c in ["trade_id", "exit_date", "code_norm", "qty", "entry_price", "exit_price", ret_col, "exit_reason", "entry_timing", "surge_type", "horizon", "sell_ratio_pct", "partial_exit"] if c in df.columns]
    rows = df[keep].sort_values(ret_col, ascending=True).head(n).copy()
    return json.loads(rows.to_json(orient="records", force_ascii=False))


def main() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    pnl = _load_pnl_module()
    scoped, ret_col, meta = _official_scope(pnl)
    work = _restore_exit_meta(_prepare(scoped, pnl, ret_col))
    shadow = _apply_stopgap_shadow(work, ret_col)
    # Keep the same MDD investigation window used by the cluster-axis diagnostic.
    window = shadow[shadow["exit_date"].astype(str).ge("20260504") & shadow["exit_date"].astype(str).le("20260604")].copy()
    window_non_shadow = window[~window["_is_stopgap_shadow_candidate"].astype(bool)].copy()
    axes = ["exit_reason", "entry_timing", "surge_type", "horizon", "sell_ratio_pct", "partial_exit", "exit_date"]
    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "status": "FAIL",
        "policy_change": False,
        "trading_effect": False,
        "source": {
            "trades_calc": str(TRADES_CALC),
            "stopgap_shadow": str(STOPGAP_SHADOW),
        },
        "scope_meta": meta,
        "baseline_after_stopgap_shadow": _metrics(shadow, pnl, ret_col),
        "window_after_stopgap_shadow": _metrics(window, pnl, ret_col),
        "window_excluding_shadow_candidates": _metrics(window_non_shadow, pnl, ret_col),
        "axis_breakdown": [row for axis in axes for row in _breakdown(window_non_shadow, ret_col, axis)],
        "top_loss_trades": _top_losses(window_non_shadow, ret_col),
        "interpretation": [
            "Read-only residual decomposition after applying the intraday STOP_GAP legacy shadow replacement.",
            "The residual window excludes the seven legacy STOP_GAP candidate rows from the breakdown so remaining loss clusters are visible.",
            "This artifact does not modify historical data or production policy.",
        ],
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    out_rows = pd.DataFrame(payload["axis_breakdown"] + [{"axis": "top_loss_trade", **r} for r in payload["top_loss_trades"]])
    out_rows.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(json.dumps({"status": payload["status"], "baseline_after_shadow_mdd": payload["baseline_after_stopgap_shadow"]["max_drawdown_pct"], "window_rows": payload["window_excluding_shadow_candidates"]["rows"], "out_json": str(OUT_JSON), "out_csv": str(OUT_CSV)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
