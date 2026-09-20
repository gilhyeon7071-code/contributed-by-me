from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CASE_CSV = LOG_DIR / "mdd_exit_late_case_diagnostic_latest.csv"
PRICE_PARQUET = ROOT / "paper" / "prices" / "ohlcv_paper.parquet"
OUT_JSON = LOG_DIR / "mdd_exit_counterfactual_diagnostic_latest.json"
OUT_CSV = LOG_DIR / "mdd_exit_counterfactual_diagnostic_latest.csv"


def _float(v: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def _str(v: Any) -> str:
    if pd.isna(v):
        return ""
    return str(v)


def _ymd(v: Any) -> str:
    s = _str(v).strip()
    if not s:
        return ""
    if "." in s:
        s = s.split(".", 1)[0]
    return s.zfill(8)


def _load_prices() -> pd.DataFrame:
    px = pd.read_parquet(PRICE_PARQUET)
    px = px.copy()
    date_col = next((c for c in ["ymd", "date", "trade_date"] if c in px.columns), "")
    if not date_col:
        raise KeyError(f"price parquet date column not found: {list(px.columns)}")
    px["code"] = px["code"].astype(str).str.zfill(6)
    px["ymd"] = px[date_col].astype(str).str.slice(0, 8)
    px = px.sort_values(["code", "ymd"]).reset_index(drop=True)
    return px


def _price_at(code_px: pd.DataFrame, ymd: str, offset: int) -> tuple[str, float | None]:
    idx = code_px.index[code_px["ymd"].eq(ymd)]
    if len(idx) == 0:
        return "", None
    pos = code_px.index.get_loc(idx[0]) + offset
    if pos < 0 or pos >= len(code_px):
        return "", None
    row = code_px.iloc[pos]
    return _str(row["ymd"]), _float(row["close"], default=float("nan"))


def _counterfactual_row(row: pd.Series, px: pd.DataFrame) -> dict[str, Any]:
    code = _str(row.get("code")).zfill(6)
    exit_date = _ymd(row.get("exit_date"))
    entry_price = _float(row.get("entry_price"))
    exit_price = _float(row.get("exit_price"))
    qty = _float(row.get("qty"))
    actual_net_ret = _float(row.get("net_ret"))
    actual_gross_ret = (exit_price - entry_price) / entry_price if entry_price > 0 else 0.0
    cost_drag_proxy = actual_gross_ret - actual_net_ret

    code_px = px[px["code"].eq(code)].reset_index(drop=True)
    alternatives: dict[str, dict[str, Any]] = {}
    for label, offset in [
        ("hold_to_exit_close", 0),
        ("hold_1d_close", 1),
        ("hold_3d_close", 3),
        ("hold_5d_close", 5),
    ]:
        alt_date, alt_price = _price_at(code_px, exit_date, offset)
        if alt_price is None or pd.isna(alt_price) or entry_price <= 0:
            alternatives[label] = {
                "date": alt_date,
                "price": None,
                "gross_ret": None,
                "net_ret_proxy": None,
                "delta_vs_actual_net_ret": None,
                "delta_pnl": None,
            }
            continue
        gross_ret = (alt_price - entry_price) / entry_price
        net_ret_proxy = gross_ret - cost_drag_proxy
        alternatives[label] = {
            "date": alt_date,
            "price": alt_price,
            "gross_ret": gross_ret,
            "net_ret_proxy": net_ret_proxy,
            "delta_vs_actual_net_ret": net_ret_proxy - actual_net_ret,
            "delta_pnl": qty * (alt_price - exit_price),
        }

    same_day = alternatives["hold_to_exit_close"]
    best_label = ""
    best_delta = None
    for label, payload in alternatives.items():
        delta = payload.get("delta_vs_actual_net_ret")
        if delta is None:
            continue
        if best_delta is None or delta > best_delta:
            best_label = label
            best_delta = delta

    return {
        "trade_id": _str(row.get("trade_id")),
        "code": code,
        "entry_date": _ymd(row.get("entry_date")),
        "exit_date": exit_date,
        "exit_reason": _str(row.get("exit_reason")),
        "case_label": _str(row.get("case_label")),
        "is_surge_like": _str(row.get("is_surge_like")),
        "surge_type": _str(row.get("surge_type")),
        "qty": qty,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "actual_net_ret": actual_net_ret,
        "actual_gross_ret": actual_gross_ret,
        "cost_drag_proxy": cost_drag_proxy,
        "same_day_close_delta_pnl": same_day.get("delta_pnl"),
        "same_day_close_delta_ret": same_day.get("delta_vs_actual_net_ret"),
        "best_counterfactual": best_label,
        "best_delta_ret": best_delta,
        "alternatives": alternatives,
    }


def _sum_alt(rows: list[dict[str, Any]], label: str) -> dict[str, Any]:
    deltas = []
    pnl_deltas = []
    for row in rows:
        alt = row["alternatives"].get(label, {})
        if alt.get("delta_vs_actual_net_ret") is not None:
            deltas.append(float(alt["delta_vs_actual_net_ret"]))
        if alt.get("delta_pnl") is not None:
            pnl_deltas.append(float(alt["delta_pnl"]))
    return {
        "observable": len(deltas),
        "sum_delta_ret": sum(deltas),
        "avg_delta_ret": sum(deltas) / len(deltas) if deltas else None,
        "sum_delta_pnl": sum(pnl_deltas),
    }


def main() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    cases = pd.read_csv(CASE_CSV, dtype=str, encoding="utf-8-sig")
    px = _load_prices()
    detail = [_counterfactual_row(row, px) for _, row in cases.iterrows()]
    summary = {
        "hold_to_exit_close": _sum_alt(detail, "hold_to_exit_close"),
        "hold_1d_close": _sum_alt(detail, "hold_1d_close"),
        "hold_3d_close": _sum_alt(detail, "hold_3d_close"),
        "hold_5d_close": _sum_alt(detail, "hold_5d_close"),
    }
    out = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "status": "FAIL" if detail else "PASS",
        "policy_change": False,
        "trading_effect": False,
        "source": {
            "case_csv": str(CASE_CSV),
            "price_parquet": str(PRICE_PARQUET),
        },
        "case_count": len(detail),
        "summary": summary,
        "cases": detail,
        "interpretation": [
            "Read-only counterfactual for the five potentially late or over-exit cases.",
            "net_ret_proxy reuses the observed trade cost drag; it is diagnostic, not an execution fill model.",
            "A positive counterfactual does not authorize threshold changes without rule-axis validation.",
        ],
    }
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    flat_rows = []
    for row in detail:
        base = {k: v for k, v in row.items() if k != "alternatives"}
        for label, alt in row["alternatives"].items():
            r = dict(base)
            r["counterfactual"] = label
            r.update({f"alt_{k}": v for k, v in alt.items()})
            flat_rows.append(r)
    pd.DataFrame(flat_rows).to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(
        json.dumps(
            {
                "status": out["status"],
                "case_count": len(detail),
                "summary": summary,
                "out_json": str(OUT_JSON),
                "out_csv": str(OUT_CSV),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
