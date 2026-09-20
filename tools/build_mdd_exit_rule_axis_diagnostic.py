from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
COUNTERFACTUAL_JSON = LOG_DIR / "mdd_exit_counterfactual_diagnostic_latest.json"
LATE_CASE_JSON = LOG_DIR / "mdd_exit_late_case_diagnostic_latest.json"
FILLS_NORM = ROOT / "paper" / "fills_norm.csv"
OUT_JSON = LOG_DIR / "mdd_exit_rule_axis_diagnostic_latest.json"
OUT_CSV = LOG_DIR / "mdd_exit_rule_axis_diagnostic_latest.csv"


def _s(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or _s(value).strip() == "":
            return default
        out = float(_s(value).replace(",", ""))
        return default if out != out else out
    except Exception:
        return default


def _note_value(note: Any, key: str) -> str:
    m = re.search(r"(?:^|[;|\s])" + re.escape(key) + r"=([^;|\s]+)", _s(note))
    return m.group(1).strip() if m else ""


def _load_fills() -> pd.DataFrame:
    fills = pd.read_csv(FILLS_NORM, dtype=str, encoding="utf-8-sig").fillna("")
    fills = fills.copy()
    fills["code_norm"] = fills["code"].astype(str).str.zfill(6)
    fills["qty_n"] = pd.to_numeric(fills["qty"], errors="coerce").fillna(0.0)
    fills["price_n"] = pd.to_numeric(fills["price"], errors="coerce").fillna(0.0)
    fills["note_s"] = fills["note"].astype(str)
    fills["entry_order_id_note"] = fills["note_s"].map(lambda s: _note_value(s, "entry_order_id") or _note_value(s, "source_order_id"))
    fills["exit_reason_note"] = fills["note_s"].map(lambda s: _note_value(s, "exit_reason"))
    fills["gap_loss_pct_note"] = fills["note_s"].map(lambda s: _f(_note_value(s, "gap_loss_pct"), 0.0))
    fills["severity_reasons_note"] = fills["note_s"].map(lambda s: _note_value(s, "severity_reasons"))
    return fills


def _find_sell_fill(fills: pd.DataFrame, case: dict[str, Any]) -> pd.Series | None:
    code = _s(case.get("code")).zfill(6)
    exit_date = _s(case.get("exit_date"))
    reason = _s(case.get("exit_reason"))
    entry_order_id = _s(case.get("sell_order_id") or "")
    sells = fills.loc[
        fills["code_norm"].eq(code)
        & fills["date"].astype(str).eq(exit_date)
        & fills["side"].astype(str).str.upper().eq("SELL")
    ].copy()
    if sells.empty:
        return None
    by_reason = sells.loc[sells["exit_reason_note"].eq(reason)]
    if not by_reason.empty:
        sells = by_reason
    target_qty = _f(case.get("qty"), -1.0)
    if target_qty >= 0:
        by_qty = sells.loc[(sells["qty_n"] - target_qty).abs() < 0.0001]
        if not by_qty.empty:
            sells = by_qty
    return sells.iloc[0]


def _axis_for(case: dict[str, Any], counter: dict[str, Any], sell: pd.Series | None) -> tuple[str, list[str]]:
    exit_reason = _s(case.get("exit_reason"))
    entry_date = _s(case.get("entry_date"))
    exit_date = _s(case.get("exit_date"))
    is_same_day = entry_date == exit_date
    is_surge = _s(case.get("is_surge_like")).lower() == "true"
    same_day_reasons = case.get("same_day_exit_reasons") or {}
    exit_open = _f(case.get("exit_open"))
    exit_low = _f(case.get("exit_low"))
    exit_close = _f(case.get("exit_close"))
    exit_price = _f(case.get("exit_price"))
    entry_price = _f(case.get("entry_price"))
    best = _s(counter.get("best_counterfactual"))
    best_delta = _f(counter.get("best_delta_ret"))
    same_day_delta = _f(counter.get("same_day_close_delta_ret"))
    gap_loss = _f(sell.get("gap_loss_pct_note") if sell is not None else 0.0)
    severity_reasons = _s(sell.get("severity_reasons_note") if sell is not None else "")

    flags: list[str] = []
    if is_same_day:
        flags.append("SAME_DAY_ENTRY_EXIT")
    if is_surge:
        flags.append("SURGE_LIKE")
    if exit_reason == "STOP_GAP":
        flags.append("STOP_GAP")
    if "DDM_LIQUIDATE" in exit_reason:
        flags.append("DDM_LIQUIDATE")
    if isinstance(same_day_reasons, dict) and len(same_day_reasons) > 1:
        flags.append("STACKED_SAME_DAY_EXITS")
    if exit_price == exit_open and exit_close > exit_price:
        flags.append("EXIT_PRICE_EQUALS_OPEN_WHILE_CLOSE_RECOVERED")
    if gap_loss >= 0.08 or "force_gap" in severity_reasons:
        flags.append("FORCE_GAP_THRESHOLD_PATH")
    if best_delta >= 0.30:
        flags.append("LARGE_COUNTERFACTUAL_RECOVERY")
    elif same_day_delta >= 0.08:
        flags.append("SAME_DAY_RECOVERY")

    if exit_reason == "STOP_GAP" and is_same_day and exit_price == exit_open and exit_close > exit_price:
        return "ENTRY_DAY_INTRADAY_GAP_STOP_OPEN_PRICE_AXIS", flags
    if "DDM_LIQUIDATE" in exit_reason and is_same_day and isinstance(same_day_reasons, dict) and "STOP" in same_day_reasons:
        return "SAME_DAY_DDM_STOP_STACKING_AXIS", flags
    if "DDM_LIQUIDATE" in exit_reason and is_same_day:
        return "SAME_DAY_DDM_LIQUIDATION_AXIS", flags
    if exit_reason == "STOP_GAP":
        return "STOP_GAP_THRESHOLD_AXIS", flags
    return "RULE_AXIS_REVIEW_REQUIRED", flags


def main() -> None:
    counter_payload = json.loads(COUNTERFACTUAL_JSON.read_text(encoding="utf-8"))
    late_payload = json.loads(LATE_CASE_JSON.read_text(encoding="utf-8"))
    fills = _load_fills()
    late_cases = {
        (_s(c.get("trade_id")), _s(c.get("code")), _s(c.get("exit_reason"))): c
        for c in late_payload.get("cases", [])
    }

    rows: list[dict[str, Any]] = []
    for counter in counter_payload.get("cases", []):
        key = (_s(counter.get("trade_id")), _s(counter.get("code")), _s(counter.get("exit_reason")))
        case = dict(late_cases.get(key, {}))
        case.update(counter)
        sell = _find_sell_fill(fills, case)
        axis, flags = _axis_for(case, counter, sell)
        row = {
            "trade_id": _s(case.get("trade_id")),
            "code": _s(case.get("code")).zfill(6),
            "entry_date": _s(case.get("entry_date")),
            "exit_date": _s(case.get("exit_date")),
            "exit_reason": _s(case.get("exit_reason")),
            "case_label": _s(case.get("case_label")),
            "rule_axis": axis,
            "axis_flags": ",".join(flags),
            "entry_price": _f(case.get("entry_price")),
            "exit_price": _f(case.get("exit_price")),
            "exit_open": _f(case.get("exit_open")),
            "exit_low": _f(case.get("exit_low")),
            "exit_close": _f(case.get("exit_close")),
            "same_day_close_delta_ret": _f(counter.get("same_day_close_delta_ret")),
            "best_counterfactual": _s(counter.get("best_counterfactual")),
            "best_delta_ret": _f(counter.get("best_delta_ret")),
            "same_day_exit_reasons": json.dumps(case.get("same_day_exit_reasons") or {}, ensure_ascii=False),
            "sell_order_id": _s(sell.get("order_id") if sell is not None else ""),
            "sell_ts": _s(sell.get("ts") if sell is not None else ""),
            "sell_gap_loss_pct": _f(sell.get("gap_loss_pct_note") if sell is not None else 0.0),
            "sell_severity_reasons": _s(sell.get("severity_reasons_note") if sell is not None else ""),
        }
        rows.append(row)

    out_df = pd.DataFrame(rows)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    axis_counts = out_df["rule_axis"].value_counts().to_dict() if not out_df.empty else {}
    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "status": "FAIL" if rows else "PASS",
        "policy_change": False,
        "trading_effect": False,
        "source": {
            "counterfactual_json": str(COUNTERFACTUAL_JSON),
            "late_case_json": str(LATE_CASE_JSON),
            "fills_norm": str(FILLS_NORM),
        },
        "case_count": len(rows),
        "axis_counts": axis_counts,
        "cases": rows,
        "interpretation": [
            "Read-only rule-axis split for the five MDD late/over-exit cases.",
            "ENTRY_DAY_INTRADAY_GAP_STOP_OPEN_PRICE_AXIS means same-day intraday entries were stopped using entry-date open/low style gap-stop logic.",
            "SAME_DAY_DDM_STOP_STACKING_AXIS means DDM and STOP liquidation both occurred on the entry day for the same position lineage.",
            "This artifact identifies cause axes; it does not change thresholds or execution policy.",
        ],
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": payload["status"], "case_count": len(rows), "axis_counts": axis_counts, "out_json": str(OUT_JSON), "out_csv": str(OUT_CSV)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
