from __future__ import annotations

import json
import math
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SCAN_DIR = LOG_DIR / "research_report_entry_trigger_v2_transition_value2b_ret1_1_relaxation_scope_scan_20260713_132200"
OLD_TRADES = SCAN_DIR / "balanced_original_trades.csv"
SELECTION_CONTRACT_PATH = LOG_DIR / "c3_normal_official_replay_selection_contract_latest.json"
STABLE_PARAMS = ROOT / "12_Risk_Controlled" / "stable_params_v41_1.json"

LATEST_JSON = LOG_DIR / "c3_old27_selection_eligibility_probe_latest.json"
LATEST_DETAIL_CSV = LOG_DIR / "c3_old27_selection_eligibility_probe_rows_latest.csv"
LATEST_RULE_CSV = LOG_DIR / "c3_old27_selection_eligibility_probe_rules_latest.csv"
LATEST_SUMMARY_CSV = LOG_DIR / "c3_old27_selection_eligibility_probe_summary_latest.csv"
LATEST_MD = LOG_DIR / "c3_old27_selection_eligibility_probe_latest.md"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _safe_float(value: Any) -> float:
    try:
        if value is None or value == "":
            return math.nan
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def _norm_code(value: Any) -> str:
    s = str(value).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(6)[-6:]


def _load_report_params() -> tuple[Any, dict[str, Any]]:
    os.environ["REPORT_RESEARCH_MODE"] = "1"
    os.environ["REPORT_RESEARCH_PARAMS_PATH"] = str(STABLE_PARAMS)
    os.environ["REPORT_ALLOW_UNAPPROVED_FALLBACK"] = "1"
    os.environ["REPORT_RESEARCH_OUTPUT_DIR"] = str(LOG_DIR / "c3_old27_selection_eligibility_report_context")
    sys.path.insert(0, str(ROOT))
    import report_backtest_v41_1 as rb  # noqa: WPS433

    params = rb.load_params()
    base_filter = rb._params_to_filter_dict(params)
    ladder = rb._relax_ladder(base_filter) if bool(params.use_relax_ladder) else [("L0", base_filter)]
    return params, {"base_filter": base_filter, "ladder": [{"level": level, "params": p} for level, p in ladder]}


def _eval_op(value: float, op: str, threshold: float) -> bool:
    if not math.isfinite(value):
        return False
    if op == "<=":
        return value <= threshold
    if op == "<":
        return value < threshold
    if op == ">=":
        return value >= threshold
    if op == ">":
        return value > threshold
    if op in {"=", "=="}:
        return value == threshold
    if op == "!=":
        return value != threshold
    raise ValueError(f"unsupported op: {op}")


def _numeric_filter_value(row: pd.Series, field: str) -> tuple[bool, str, float]:
    field_map = {
        "signal_ret1_pct": "signal_ret1_pct",
        "signal_rs": "signal_rs",
        "signal_v_accel": "signal_v_accel",
        "signal_stretch": "signal_stretch",
        "signal_atr_pct": "signal_atr_pct",
        "signal_high_52w_gap": "signal_high_52w_gap",
        "signal_value": "signal_value",
        "mkt_ret20": "mkt_ret20",
        "mkt_ret60": "mkt_ret60",
        "mkt_vol20": "mkt_vol20",
        "mkt_breadth": "mkt_breadth",
    }
    col = field_map.get(field, field)
    if col not in row.index:
        return False, "missing_column", math.nan
    return True, "evaluated", _safe_float(row[col])


def _selection_signal_value(row: pd.Series, signal: str) -> tuple[bool, str, float]:
    col_map = {
        "rs": "signal_rs",
        "v_accel": "signal_v_accel",
        "stretch": "signal_stretch",
        "value": "signal_value",
        "atr": "signal_atr_pct",
        "rsi": "rsi14",
        "volcorr": "vol_close_corr20",
        "high52": "signal_high_52w_gap",
        "listing": "listing_days",
    }
    col = col_map.get(signal, signal)
    if col not in row.index:
        return False, "missing_column", math.nan
    return True, "evaluated", _safe_float(row[col])


def _eval_selection_signal(signal: str, value: float, p: dict[str, float]) -> tuple[bool, str, float]:
    if signal == "rs":
        return value > float(p["rs_lim"]), ">", float(p["rs_lim"])
    if signal == "v_accel":
        return value > float(p["v_accel_lim"]), ">", float(p["v_accel_lim"])
    if signal == "stretch":
        return value < float(p["stretch_max"]), "<", float(p["stretch_max"])
    if signal == "value":
        return value > float(p["value_min"]), ">", float(p["value_min"])
    if signal == "atr":
        return value < float(p["atr_max"]), "<", float(p["atr_max"])
    if signal == "rsi":
        return value < float(p["rsi_max"]), "<", float(p["rsi_max"])
    if signal == "volcorr":
        return value >= float(p["vol_close_corr_min"]), ">=", float(p["vol_close_corr_min"])
    if signal == "high52":
        return value <= float(p["near_52w_high_gap_max"]), "<=", float(p["near_52w_high_gap_max"])
    if signal == "listing":
        return value >= float(p["min_listing_days"]), ">=", float(p["min_listing_days"])
    raise ValueError(f"unknown signal: {signal}")


def main() -> int:
    old = pd.read_csv(OLD_TRADES, dtype={"code": str}, encoding="utf-8-sig")
    old["code"] = old["code"].map(_norm_code)
    contract_payload = _read_json(SELECTION_CONTRACT_PATH)
    contract = contract_payload.get("selection_contract", contract_payload)
    params, params_payload = _load_report_params()
    stable_payload = _read_json(STABLE_PARAMS)
    ladder = params_payload["ladder"]

    rule_rows: list[dict[str, Any]] = []
    detail_rows: list[dict[str, Any]] = []
    required_signals = [str(x) for x in contract.get("required_signals", [])]
    numeric_filters = list(contract.get("numeric_filters") or [])

    for idx, row in old.reset_index(drop=True).iterrows():
        row_result: dict[str, Any] = {
            "old_index": int(idx),
            "code": row["code"],
            "signal_date": row.get("signal_date"),
            "old_relax_level": row.get("relax_level"),
            "old_score": row.get("score"),
        }
        numeric_eval = 0
        numeric_pass = 0
        numeric_missing: list[str] = []
        numeric_failed: list[str] = []
        for item in numeric_filters:
            field = str(item.get("field"))
            op = str(item.get("op"))
            threshold = float(item.get("value"))
            available, status, value = _numeric_filter_value(row, field)
            passed = bool(available and _eval_op(value, op, threshold))
            if available:
                numeric_eval += 1
                numeric_pass += int(passed)
            else:
                numeric_missing.append(field)
            if available and not passed:
                numeric_failed.append(field)
            rule_rows.append(
                {
                    "old_index": int(idx),
                    "code": row["code"],
                    "rule_layer": "numeric_filter",
                    "rule": field,
                    "available": bool(available),
                    "status": status,
                    "op": op,
                    "threshold": threshold,
                    "value": value,
                    "passed": bool(passed) if available else "",
                }
            )

        row_result["numeric_filters_total"] = len(numeric_filters)
        row_result["numeric_filters_evaluable"] = numeric_eval
        row_result["numeric_filters_passed"] = numeric_pass
        row_result["numeric_missing"] = "|".join(numeric_missing)
        row_result["numeric_failed"] = "|".join(numeric_failed)

        best_level = ""
        best_level_all_available_pass = False
        level_summaries: list[str] = []
        for ladder_item in ladder:
            level = str(ladder_item["level"])
            p_try = dict(ladder_item["params"])
            signal_eval = 0
            signal_pass = 0
            signal_missing: list[str] = []
            signal_failed: list[str] = []
            for signal in required_signals:
                available, status, value = _selection_signal_value(row, signal)
                if available:
                    passed, op, threshold = _eval_selection_signal(signal, value, p_try)
                    signal_eval += 1
                    signal_pass += int(passed)
                    if not passed:
                        signal_failed.append(signal)
                else:
                    passed, op, threshold = False, "", math.nan
                    signal_missing.append(signal)
                rule_rows.append(
                    {
                        "old_index": int(idx),
                        "code": row["code"],
                        "rule_layer": f"select_candidates_{level}",
                        "rule": signal,
                        "available": bool(available),
                        "status": status,
                        "op": op,
                        "threshold": threshold,
                        "value": value,
                        "passed": bool(passed) if available else "",
                    }
                )
            all_available = signal_eval == len(required_signals)
            all_pass = all_available and signal_pass == len(required_signals)
            if all_pass and not best_level:
                best_level = level
                best_level_all_available_pass = True
            level_summaries.append(f"{level}:eval={signal_eval},pass={signal_pass},missing={','.join(signal_missing)},failed={','.join(signal_failed)}")

        row_result["selection_best_level_on_old_features"] = best_level
        row_result["selection_all_available_pass"] = bool(best_level_all_available_pass)
        row_result["selection_ladder_summary"] = " | ".join(level_summaries)
        detail_rows.append(row_result)

    detail = pd.DataFrame(detail_rows)
    rules = pd.DataFrame(rule_rows)

    summary_rows = [
        {"metric": "old_rows", "value": "total", "n": int(len(detail))},
        {"metric": "numeric_all_evaluable_filters_passed", "value": "true", "n": int(((detail["numeric_filters_evaluable"] == detail["numeric_filters_total"]) & (detail["numeric_filters_passed"] == detail["numeric_filters_total"])).sum())},
        {"metric": "selection_all_available_pass", "value": "true", "n": int(detail["selection_all_available_pass"].sum())},
        {"metric": "selection_missing_any_required_signal", "value": "true", "n": int(detail["selection_ladder_summary"].str.contains("missing=.*[a-zA-Z]", regex=True).sum())},
    ]
    for field in sorted(set("|".join(detail["numeric_missing"].dropna().astype(str)).split("|")) - {""}):
        summary_rows.append({"metric": "numeric_missing_field", "value": field, "n": int(detail["numeric_missing"].str.contains(field, regex=False).sum())})
    for field in sorted(set("|".join(detail["numeric_failed"].dropna().astype(str)).split("|")) - {""}):
        summary_rows.append({"metric": "numeric_failed_field", "value": field, "n": int(detail["numeric_failed"].str.contains(field, regex=False).sum())})

    summary = pd.DataFrame(summary_rows)
    payload = {
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "READ_ONLY_C3_OLD27_SELECTION_ELIGIBILITY_PROBE_NOT_OPERATIONAL",
        "old_rows": int(len(detail)),
        "required_signals": required_signals,
        "numeric_filter_count": int(len(numeric_filters)),
        "numeric_all_evaluable_filters_passed": int(summary.loc[summary["metric"].eq("numeric_all_evaluable_filters_passed"), "n"].iloc[0]),
        "selection_all_available_pass": int(detail["selection_all_available_pass"].sum()),
        "selection_missing_any_required_signal": int(summary.loc[summary["metric"].eq("selection_missing_any_required_signal"), "n"].iloc[0]),
        "missing_required_signal_columns": sorted(set(["rsi14", "vol_close_corr20", "listing_days"]).difference(old.columns)),
        "params": {
            "promoted": bool(stable_payload.get("promoted", False)),
            "best_score": stable_payload.get("best_score"),
            "use_relax_ladder": bool(getattr(params, "use_relax_ladder", False)),
            "ladder_levels": [x["level"] for x in ladder],
        },
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_live_order_changed": False,
            "policy_changed": False,
        },
        "full_logic_application": "NOT_APPLIED",
        "detail_csv": str(LATEST_DETAIL_CSV),
        "rule_csv": str(LATEST_RULE_CSV),
        "summary_csv": str(LATEST_SUMMARY_CSV),
    }

    detail.to_csv(LATEST_DETAIL_CSV, index=False, encoding="utf-8-sig")
    rules.to_csv(LATEST_RULE_CSV, index=False, encoding="utf-8-sig")
    summary.to_csv(LATEST_SUMMARY_CSV, index=False, encoding="utf-8-sig")
    LATEST_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# C3 old27 selection eligibility probe",
        "",
        f"- status: {payload['status']}",
        f"- old_rows: {payload['old_rows']}",
        f"- numeric_filter_count: {payload['numeric_filter_count']}",
        f"- numeric_all_evaluable_filters_passed: {payload['numeric_all_evaluable_filters_passed']}",
        f"- selection_all_available_pass: {payload['selection_all_available_pass']}",
        f"- selection_missing_any_required_signal: {payload['selection_missing_any_required_signal']}",
        f"- missing_required_signal_columns: {payload['missing_required_signal_columns']}",
        f"- ladder_levels: {payload['params']['ladder_levels']}",
        "",
        "## Summary",
        "",
    ]
    for row in summary.to_dict("records"):
        lines.append(f"- {row['metric']}={row['value']}: {row['n']}")
    LATEST_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
