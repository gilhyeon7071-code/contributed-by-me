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
STACKING_JSON = LOG_DIR / "mdd_ddm_stop_stacking_diagnostic_latest.json"
OUT_JSON = LOG_DIR / "mdd_ddm_stop_defer_counterfactual_latest.json"
OUT_CSV = LOG_DIR / "mdd_ddm_stop_defer_counterfactual_latest.csv"


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
    return df.copy(), ret_col, {"raw": len(raw), "operational_scope": op_scope, "blocked": blocked, "unauditable": unauditable}


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
    out["exit_reason_note"] = out["note_s"].map(lambda s: _note_value(s, "exit_reason"))
    out["entry_order_id_note"] = out["note_s"].map(lambda s: _note_value(s, "entry_order_id") or _note_value(s, "source_order_id"))
    out["sell_qty_note"] = out["note_s"].map(lambda s: _f(_note_value(s, "sell_qty"), 0.0))
    for c in [ret_col, "qty", "entry_price", "exit_price"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    out["_ret"] = pd.to_numeric(out[ret_col], errors="coerce")
    return out


def _load_prices() -> pd.DataFrame:
    px = pd.read_parquet(PRICES)
    px = px.copy()
    date_col = next((c for c in ["ymd", "date", "trade_date"] if c in px.columns), "")
    if not date_col:
        raise KeyError(f"price date column missing: {list(px.columns)}")
    px["code_norm"] = px["code"].map(_norm_code)
    px["ymd"] = px[date_col].astype(str).str.slice(0, 8)
    px["close_n"] = pd.to_numeric(px["close"], errors="coerce")
    return px.sort_values(["code_norm", "ymd"]).reset_index(drop=True)


def _price_at(px: pd.DataFrame, code: str, exit_date: str, offset: int) -> tuple[str, float | None]:
    code_px = px[px["code_norm"].eq(code)].reset_index(drop=True)
    idx = code_px.index[code_px["ymd"].eq(exit_date)]
    if len(idx) == 0:
        return "", None
    pos = int(idx[0]) + offset
    if pos < 0 or pos >= len(code_px):
        return "", None
    row = code_px.iloc[pos]
    price = _f(row.get("close_n"), -1.0)
    return str(row.get("ymd") or ""), price if price > 0 else None


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


def _match_stop_trade(work: pd.DataFrame, group: dict[str, Any]) -> pd.Index:
    code = _norm_code(group.get("code"))
    date = _ymd(group.get("date"))
    entry_oid = str(group.get("entry_order_id") or "")
    stop_qty = _f(group.get("stop_qty"), 0.0)
    stop_prices = []
    for oid in str(group.get("sell_order_ids") or "").split(","):
        oid = oid.strip()
        if not oid or "_STOP" not in oid:
            continue
        # Exact STOP fill metadata is more reliable than trades_calc notes,
        # because trades_calc carries the entry BUY note.
        fills = pd.read_csv(FILLS_NORM, dtype=str, encoding="utf-8-sig").fillna("")
        fills["code_norm"] = fills["code"].map(_norm_code)
        fills["entry_order_id_note"] = fills["note"].map(lambda s: _note_value(s, "entry_order_id") or _note_value(s, "source_order_id"))
        hit = fills.loc[fills["order_id"].astype(str).eq(oid)]
        if not hit.empty:
            stop_prices.append(_f(hit.iloc[0].get("price"), 0.0))
    mask = (
        work["code_norm"].eq(code)
        & work["exit_date"].eq(date)
        & work["entry_order_id_note"].eq(entry_oid)
    )
    if stop_qty > 0:
        qty_mask = (work["sell_qty_note"].sub(stop_qty).abs() < 0.0001) | (work.get("qty", pd.Series(0, index=work.index)).sub(stop_qty).abs() < 0.0001)
        mask = mask & qty_mask
    if stop_prices:
        price_mask = pd.Series(False, index=work.index)
        for px in stop_prices:
            if px > 0 and "exit_price" in work.columns:
                price_mask = price_mask | (work["exit_price"].sub(px).abs() < 0.0001)
        mask = mask & price_mask
    return work.index[mask]


def main() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    pnl = _load_pnl_module()
    scoped, ret_col, meta = _official_scope(pnl)
    work = _prepare_trades(scoped, pnl, ret_col)
    px = _load_prices()
    stacking = json.loads(STACKING_JSON.read_text(encoding="utf-8"))
    groups = list(stacking.get("groups", []) or [])

    matched: list[dict[str, Any]] = []
    replacement_by_offset: dict[int, dict[int, float]] = {0: {}, 1: {}, 3: {}, 5: {}}
    for group in groups:
        idx = _match_stop_trade(work, group)
        code = _norm_code(group.get("code"))
        exit_date = _ymd(group.get("date"))
        for i in idx:
            row = work.loc[i]
            entry_price = _f(row.get("entry_price"), 0.0)
            exit_price = _f(row.get("exit_price"), 0.0)
            actual_ret = _f(row.get(ret_col), 0.0)
            gross_actual = (exit_price - entry_price) / entry_price if entry_price > 0 else 0.0
            cost_drag = gross_actual - actual_ret
            alternatives: dict[str, Any] = {}
            for offset in [0, 1, 3, 5]:
                alt_date, alt_price = _price_at(px, code, exit_date, offset)
                if alt_price is None or entry_price <= 0:
                    alternatives[f"defer_{offset}d_close"] = {"date": alt_date, "price": None, "net_ret_proxy": None, "delta_ret": None}
                    continue
                gross = (alt_price - entry_price) / entry_price
                net_proxy = gross - cost_drag
                delta = net_proxy - actual_ret
                alternatives[f"defer_{offset}d_close"] = {"date": alt_date, "price": alt_price, "net_ret_proxy": net_proxy, "delta_ret": delta}
                replacement_by_offset[offset][int(i)] = net_proxy
            matched.append(
                {
                    "trade_index": int(i),
                    "trade_id": str(row.get("trade_id") or ""),
                    "code": code,
                    "exit_date": exit_date,
                    "entry_order_id": str(group.get("entry_order_id") or ""),
                    "stop_qty": _f(group.get("stop_qty"), 0.0),
                    "actual_ret": actual_ret,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "alternatives": alternatives,
                }
            )

    variants: list[dict[str, Any]] = []
    for offset, repl in replacement_by_offset.items():
        sim = work.copy()
        for i, new_ret in repl.items():
            sim.loc[i, ret_col] = new_ret
            sim.loc[i, "_ret"] = new_ret
        variants.append(
            {
                "variant": f"defer_stop_after_same_day_ddm_to_{offset}d_close",
                "offset_days": offset,
                "replaced_trades": int(len(repl)),
                "after": _metrics(sim, pnl, ret_col),
                "delta_vs_baseline": {
                    "sum_ret": _metrics(sim, pnl, ret_col)["sum_ret"] - _metrics(work, pnl, ret_col)["sum_ret"],
                    "max_drawdown_pct": (
                        (_metrics(sim, pnl, ret_col).get("max_drawdown_pct") or 0.0)
                        - (_metrics(work, pnl, ret_col).get("max_drawdown_pct") or 0.0)
                    ),
                },
            }
        )

    baseline = _metrics(work, pnl, ret_col)
    flat_rows = []
    for item in matched:
        base = {k: v for k, v in item.items() if k != "alternatives"}
        for name, alt in item["alternatives"].items():
            row = dict(base)
            row["counterfactual"] = name
            row.update({f"alt_{k}": v for k, v in alt.items()})
            flat_rows.append(row)
    pd.DataFrame(flat_rows).to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "status": "FAIL" if matched else "PASS",
        "policy_change": False,
        "trading_effect": False,
        "source": {
            "trades_calc": str(TRADES_CALC),
            "fills_norm": str(FILLS_NORM),
            "prices": str(PRICES),
            "stacking_json": str(STACKING_JSON),
        },
        "scope_meta": meta,
        "baseline": baseline,
        "matched_stop_trades": len(matched),
        "matched": matched,
        "variants": variants,
        "interpretation": [
            "Read-only counterfactual that replaces STOP rows following same-day DDM with deferred close exits.",
            "The calculation is diagnostic only and uses observed cost drag as a net-return proxy.",
            "A favorable result does not authorize production defer/suppress behavior without fail-closed policy review.",
        ],
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": payload["status"], "matched_stop_trades": len(matched), "baseline_mdd": baseline.get("max_drawdown_pct"), "variants": variants, "out_json": str(OUT_JSON), "out_csv": str(OUT_CSV)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
