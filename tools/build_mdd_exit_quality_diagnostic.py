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
PRICE_PARQUET = ROOT / "paper" / "prices" / "ohlcv_paper.parquet"
MDD_CLUSTER = LOG_DIR / "paper_mdd_cluster_axis_diagnosis_latest.json"
OUT_JSON = LOG_DIR / "mdd_exit_quality_diagnostic_latest.json"
OUT_CSV = LOG_DIR / "mdd_exit_quality_diagnostic_latest.csv"

TARGET_REASONS = {"STOP", "STOP_GAP", "DDM_LIQUIDATE_L3", "DDM_LIQUIDATE_L4"}


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


def _norm_ymd(value: Any) -> str:
    s = re.sub(r"\D", "", str(value or ""))
    return s[:8] if len(s) >= 8 else ""


def _norm_code(value: Any) -> str:
    s = re.sub(r"\D", "", str(value or ""))
    return s.zfill(6) if s else ""


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return default
        out = float(str(value).replace(",", ""))
        if out != out:
            return default
        return out
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
    return df.copy(), ret_col, {
        "raw": int(len(raw)),
        "operational_scope": op_scope,
        "blocked_entry_filter": blocked,
        "unauditable_surge_filter": unauditable,
    }


def _prepare_trades(df: pd.DataFrame, pnl: Any, ret_col: str) -> pd.DataFrame:
    out = pnl._ensure_exit_date(df.copy())
    out["exit_date"] = out["exit_date"].map(pnl._normalize_ymd)
    out["entry_date"] = out["entry_ts"].astype(str).map(pnl._normalize_ymd)
    out["code_norm"] = out.get("code", pd.Series("", index=out.index)).map(_norm_code)
    out["note_s"] = out.get("note", pd.Series("", index=out.index)).fillna("").astype(str)
    out["_ret"] = pd.to_numeric(out[ret_col], errors="coerce")
    out["entry_price_n"] = pd.to_numeric(out.get("entry_price", 0.0), errors="coerce")
    out["exit_price_n"] = pd.to_numeric(out.get("exit_price", 0.0), errors="coerce")
    out["qty_n"] = pd.to_numeric(out.get("qty", 0), errors="coerce")
    out["entry_order_id"] = out["note_s"].map(lambda s: _note_value(s, "entry_order_id"))
    out["exit_reason_matched"] = out["note_s"].map(lambda s: _note_value(s, "exit_reason"))
    out["surge_type"] = out["note_s"].map(lambda s: _note_value(s, "surge_type") or "UNKNOWN")
    out["surge_immediate"] = out["note_s"].map(lambda s: _note_value(s, "surge_immediate"))
    out["entry_timing"] = out["note_s"].map(lambda s: _note_value(s, "entry_timing") or "UNKNOWN")
    out["horizon"] = out["note_s"].map(lambda s: _note_value(s, "horizon") or "UNKNOWN")
    out["sell_ratio_pct"] = pd.to_numeric(out["note_s"].map(lambda s: _note_value(s, "sell_ratio_pct")), errors="coerce")
    out["is_surge_like"] = (
        out["surge_type"].astype(str).ne("UNKNOWN")
        | out["surge_immediate"].astype(str).str.strip().eq("1")
    )
    return out


def _load_sell_notes() -> tuple[dict[tuple[str, str, str, str], dict[str, Any]], dict[tuple[str, str, str], dict[str, Any]]]:
    fills = pd.read_csv(FILLS_NORM, dtype=str, encoding="utf-8-sig").fillna("")
    sells = fills.loc[fills["side"].astype(str).str.upper().eq("SELL")].copy()
    sells["code_norm"] = sells["code"].map(_norm_code)
    sells["exit_date"] = sells["date"].map(_norm_ymd)
    sells["qty_norm"] = pd.to_numeric(sells["qty"], errors="coerce").fillna(0).astype(float).map(lambda x: str(int(x)) if x == int(x) else str(x))
    sells["note_s"] = sells["note"].astype(str)
    sells["entry_order_id"] = sells["note_s"].map(lambda s: _note_value(s, "entry_order_id") or _note_value(s, "source_order_id"))
    sells["exit_reason"] = sells["note_s"].map(lambda s: _note_value(s, "exit_reason"))
    sells["sell_ratio_pct_from_sell"] = sells["note_s"].map(lambda s: _note_value(s, "sell_ratio_pct"))
    by_qty: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    by_oid: dict[tuple[str, str, str], dict[str, Any]] = {}
    for _, row in sells.iterrows():
        item = {
            "exit_reason": str(row.get("exit_reason") or ""),
            "sell_ratio_pct_from_sell": str(row.get("sell_ratio_pct_from_sell") or ""),
            "sell_order_id": str(row.get("order_id") or ""),
            "sell_note": str(row.get("note_s") or ""),
        }
        key4 = (
            str(row.get("code_norm") or ""),
            str(row.get("exit_date") or ""),
            str(row.get("entry_order_id") or ""),
            str(row.get("qty_norm") or ""),
        )
        key3 = key4[:3]
        if key4[0] and key4[1] and key4[2] and key4[3]:
            by_qty.setdefault(key4, item)
        if key3[0] and key3[1] and key3[2]:
            by_oid.setdefault(key3, item)
    return by_qty, by_oid


def _attach_sell_metadata(trades: pd.DataFrame) -> pd.DataFrame:
    by_qty, by_oid = _load_sell_notes()
    out = trades.copy()
    reasons: list[str] = []
    ratios: list[float | None] = []
    sell_order_ids: list[str] = []
    match_statuses: list[str] = []
    for _, row in out.iterrows():
        qty = _to_float(row.get("qty_n"), 0.0) or 0.0
        qty_s = str(int(qty)) if qty == int(qty) else str(qty)
        key4 = (
            str(row.get("code_norm") or ""),
            str(row.get("exit_date") or ""),
            str(row.get("entry_order_id") or ""),
            qty_s,
        )
        key3 = key4[:3]
        item = by_qty.get(key4)
        status = "SELL_NOTE_MATCH_QTY"
        if item is None:
            item = by_oid.get(key3)
            status = "SELL_NOTE_MATCH_OID" if item is not None else "NO_SELL_NOTE_MATCH"
        trade_reason = str(row.get("exit_reason_matched") or "")
        reason = trade_reason or (str(item.get("exit_reason") or "") if item else "")
        ratio = _to_float(row.get("sell_ratio_pct"), None)
        if ratio is None and item:
            ratio = _to_float(item.get("sell_ratio_pct_from_sell"), None)
        reasons.append(reason)
        ratios.append(ratio)
        sell_order_ids.append(str(item.get("sell_order_id") or "") if item else "")
        match_statuses.append(status)
    out["exit_reason_matched"] = reasons
    out["sell_ratio_pct"] = ratios
    out["sell_order_id_matched"] = sell_order_ids
    out["sell_note_match_status"] = match_statuses
    return out


def _load_prices() -> pd.DataFrame:
    px = pd.read_parquet(PRICE_PARQUET)
    px = px.copy()
    px["date_norm"] = px["date"].map(_norm_ymd)
    px["code_norm"] = px["code"].map(_norm_code)
    for col in ["open", "high", "low", "close"]:
        px[col] = pd.to_numeric(px[col], errors="coerce")
    return px.sort_values(["code_norm", "date_norm"])


def _post_exit_markout(row: pd.Series, prices_by_code: dict[str, pd.DataFrame]) -> dict[str, Any]:
    code = str(row.get("code_norm") or "")
    exit_date = str(row.get("exit_date") or "")
    exit_price = _to_float(row.get("exit_price_n"), None)
    if not code or not exit_date or not exit_price or exit_price <= 0:
        return {"post_data_status": "MISSING_EXIT_CONTEXT"}
    px = prices_by_code.get(code)
    if px is None or px.empty:
        return {"post_data_status": "NO_PRICE_HISTORY"}
    after = px.loc[px["date_norm"].astype(str).gt(exit_date)].head(5).copy()
    if after.empty:
        return {"post_data_status": "NO_POST_EXIT_BARS"}

    def close_ret(n: int) -> float | None:
        if len(after) < n:
            return None
        close = _to_float(after.iloc[n - 1].get("close"), None)
        return (close / exit_price - 1.0) if close and exit_price else None

    lows = pd.to_numeric(after["low"], errors="coerce").dropna()
    min_low = float(lows.min()) if not lows.empty else None
    min_low_ret = (min_low / exit_price - 1.0) if min_low and exit_price else None
    return {
        "post_data_status": "PASS",
        "post_bars": int(len(after)),
        "ret_close_1d": close_ret(1),
        "ret_close_3d": close_ret(3),
        "ret_close_5d": close_ret(5),
        "min_low_5d_ret": min_low_ret,
    }


def _classify(row: dict[str, Any]) -> str:
    status = str(row.get("post_data_status") or "")
    if status != "PASS":
        return "UNOBSERVABLE"
    min_low = _to_float(row.get("min_low_5d_ret"), None)
    r3 = _to_float(row.get("ret_close_3d"), None)
    r5 = _to_float(row.get("ret_close_5d"), None)
    if min_low is not None and min_low <= -0.03:
        return "PROTECTIVE_CONTINUED_DOWNSIDE"
    if (r3 is not None and r3 <= -0.02) or (r5 is not None and r5 <= -0.02):
        return "PROTECTIVE_CONTINUED_DOWNSIDE"
    if (r3 is not None and r3 >= 0.02) or (r5 is not None and r5 >= 0.02):
        return "POTENTIALLY_LATE_OR_OVER_EXIT"
    return "AMBIGUOUS_FLAT_AFTER_EXIT"


def _summarize(df: pd.DataFrame, group_cols: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if df.empty:
        return rows
    for keys, g in df.groupby(group_cols, dropna=False, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        item = {col: key for col, key in zip(group_cols, keys)}
        ret_col = "_ret" if "_ret" in g.columns else "net_ret"
        item.update({
            "trades": int(len(g)),
            "sum_net_ret": float(pd.to_numeric(g[ret_col], errors="coerce").sum()),
            "avg_net_ret": float(pd.to_numeric(g[ret_col], errors="coerce").mean()),
            "entry_notional": float(pd.to_numeric(g["entry_notional"], errors="coerce").sum()),
            "loss_trades": int((pd.to_numeric(g[ret_col], errors="coerce") < 0).sum()),
        })
        rows.append(item)
    rows.sort(key=lambda x: (str(x.get(group_cols[0], "")), -abs(float(x.get("sum_net_ret", 0.0)))))
    return rows


def build() -> dict[str, Any]:
    pnl = _load_pnl_module()
    scoped, ret_col, scope_meta = _official_scope(pnl)
    trades = _prepare_trades(scoped, pnl, ret_col)
    trades = _attach_sell_metadata(trades)
    cluster = json.loads(MDD_CLUSTER.read_text(encoding="utf-8-sig")) if MDD_CLUSTER.exists() else {}
    window = cluster.get("mdd_window") if isinstance(cluster.get("mdd_window"), dict) else {}
    peak = str(window.get("peak_date") or "")
    trough = str(window.get("trough_date") or "")
    if not peak or not trough:
        raise RuntimeError("mdd_window_missing")

    win = trades.loc[trades["exit_date"].between(peak, trough)].copy()
    target = win.loc[win["exit_reason_matched"].isin(TARGET_REASONS)].copy()
    prices = _load_prices()
    prices_by_code = {code: g.copy() for code, g in prices.groupby("code_norm", sort=False)}

    detail_rows: list[dict[str, Any]] = []
    for _, row in target.iterrows():
        markout = _post_exit_markout(row, prices_by_code)
        item = {
            "axis": "detail",
            "trade_id": str(row.get("trade_id") or ""),
            "code": str(row.get("code_norm") or ""),
            "exit_date": str(row.get("exit_date") or ""),
            "entry_date": str(row.get("entry_date") or ""),
            "exit_reason": str(row.get("exit_reason_matched") or ""),
            "net_ret": _to_float(row.get("_ret"), None),
            "qty": _to_float(row.get("qty_n"), None),
            "entry_price": _to_float(row.get("entry_price_n"), None),
            "exit_price": _to_float(row.get("exit_price_n"), None),
            "entry_notional": (_to_float(row.get("entry_price_n"), 0.0) or 0.0) * (_to_float(row.get("qty_n"), 0.0) or 0.0),
            "entry_timing": str(row.get("entry_timing") or ""),
            "surge_type": str(row.get("surge_type") or "UNKNOWN"),
            "is_surge_like": bool(row.get("is_surge_like")),
            "horizon": str(row.get("horizon") or ""),
            "sell_ratio_pct": _to_float(row.get("sell_ratio_pct"), None),
            "sell_order_id": str(row.get("sell_order_id_matched") or ""),
            "sell_note_match_status": str(row.get("sell_note_match_status") or ""),
        }
        item.update(markout)
        item["quality_label"] = _classify(item)
        detail_rows.append(item)

    detail = pd.DataFrame(detail_rows)
    summary_rows: list[dict[str, Any]] = []
    if not detail.empty:
        for col in ["exit_reason", "quality_label", "is_surge_like"]:
            for item in _summarize(detail, [col]):
                item["axis"] = col
                summary_rows.append(item)
        for item in _summarize(detail, ["exit_reason", "quality_label"]):
            item["axis"] = "exit_reason_x_quality"
            summary_rows.append(item)

    csv_rows = summary_rows + detail_rows
    out_df = pd.DataFrame(csv_rows)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    quality_counts = detail["quality_label"].value_counts().to_dict() if not detail.empty else {}
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "FAIL",
        "policy_change": False,
        "trading_effect": False,
        "source": {
            "trades_calc": str(TRADES_CALC),
            "price_parquet": str(PRICE_PARQUET),
            "mdd_cluster": str(MDD_CLUSTER),
        },
        "mdd_window": window,
        "filter_rows": {
            "official_scope_rows": int(len(scoped)),
            "window_rows": int(len(win)),
            "target_exit_rows": int(len(target)),
            "detail_rows": int(len(detail_rows)),
        },
        "scope_meta": scope_meta,
        "target_reasons": sorted(TARGET_REASONS),
        "quality_counts": quality_counts,
        "summary": summary_rows,
        "detail_sample": detail_rows[:20],
        "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV)},
        "interpretation": [
            "Read-only post-exit markout diagnostic; no policy value was changed.",
            "PROTECTIVE_CONTINUED_DOWNSIDE means price kept falling after the exit, so the exit likely reduced further downside.",
            "POTENTIALLY_LATE_OR_OVER_EXIT means price recovered after the exit, so the exit may have been late, too aggressive, or preceded by excessive entry exposure.",
            "UNOBSERVABLE means there were not enough post-exit price bars in the local price parquet.",
        ],
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> None:
    payload = build()
    print(json.dumps({
        "status": payload.get("status"),
        "target_exit_rows": payload.get("filter_rows", {}).get("target_exit_rows"),
        "quality_counts": payload.get("quality_counts"),
        "out_json": str(OUT_JSON),
        "out_csv": str(OUT_CSV),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
