from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SCENARIO_PATH = LOG_DIR / "capital_sizing_scenario_report_latest.json"
OUT_JSON = LOG_DIR / "capital_sizing_tuning_matrix_latest.json"
OUT_CSV = LOG_DIR / "capital_sizing_tuning_matrix_latest.csv"


def _read_json(path: Path) -> Dict[str, Any]:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except Exception:
            continue
    return {}


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        text = str(value).replace(",", "").strip()
        if not text:
            return default
        out = float(text)
        if out != out:
            return default
        return out
    except Exception:
        return default


def _get(dct: Dict[str, Any], path: Iterable[str], default: Any = None) -> Any:
    cur: Any = dct
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def _scenario_lookup(rows: List[Dict[str, Any]], scenario: str, context: str) -> Dict[str, Any]:
    for row in rows:
        if row.get("scenario") == scenario and row.get("context") == context:
            return row
    return {}


def build_matrix() -> Dict[str, Any]:
    scenario = _read_json(SCENARIO_PATH)
    rows = scenario.get("scenario_rows") if isinstance(scenario.get("scenario_rows"), list) else []
    perf = scenario.get("performance_context") if isinstance(scenario.get("performance_context"), dict) else {}
    cfg = scenario.get("current_config") if isinstance(scenario.get("current_config"), dict) else {}
    alloc = scenario.get("allocation_basis") if isinstance(scenario.get("allocation_basis"), dict) else {}
    runtime = scenario.get("runtime_modifiers") if isinstance(scenario.get("runtime_modifiers"), dict) else {}

    gross_pf = _to_float(perf.get("gross_pf"), 0.0)
    avg_ret = _to_float(perf.get("avg_ret"), 0.0)
    win_rate = _to_float(perf.get("win_rate"), 0.0)
    account_risk_clear = bool(runtime.get("account_risk_clear", False))
    current_gross = _to_float(cfg.get("gross_exposure_pct"), 0.0)
    runtime_basic_effective = _to_float(alloc.get("basic_alloc_pct_runtime_effective"), 0.0)
    config_basic = _to_float(alloc.get("basic_alloc_pct_config"), 0.0)

    perf_ok_for_expansion = gross_pf >= 1.0 and avg_ret > 0.0 and win_rate >= 0.35
    perf_near_watch = gross_pf >= 0.8 or avg_ret >= -0.005

    current_main = _scenario_lookup(rows, "current", "main_macro_only")
    gross_065_main = _scenario_lookup(rows, "gross_0.65", "main_macro_only")
    gross_075_main = _scenario_lookup(rows, "gross_0.75", "main_macro_only")
    gross_080_main = _scenario_lookup(rows, "gross_0.80", "main_macro_only")

    def delta(row: Dict[str, Any]) -> float:
        return _to_float(row.get("gross_cap_delta_vs_current_krw"), 0.0)

    matrix_rows: List[Dict[str, Any]] = [
        {
            "rank": 1,
            "candidate": "gross_exposure_pct_0.55_to_0.65",
            "classification": "CONDITIONAL_FIRST_CANDIDATE",
            "policy_change": True,
            "config_field": "capital_budget_policy.gross_exposure_pct",
            "from_value": current_gross,
            "to_value": 0.65,
            "main_cap_delta_krw": round(delta(gross_065_main), 2),
            "reason": "smallest gross-cap increase; changes real exposure axis instead of inactive cash_per_trade axis",
            "apply_condition": "account_risk_clear=true and recent PF/avg_ret improves; otherwise paper-only validation",
            "current_evidence_status": "INSUFFICIENT_FOR_IMMEDIATE_APPLY" if not perf_ok_for_expansion else "SUPPORTED",
        },
        {
            "rank": 2,
            "candidate": "basic_allocation_runtime_effective_recovery",
            "classification": "SECOND_CANDIDATE_AFTER_SELECTION_IMPROVES",
            "policy_change": True,
            "config_field": "capital_budget_policy allocation caps",
            "from_value": runtime_basic_effective,
            "to_value": config_basic,
            "main_cap_delta_krw": 0.0,
            "reason": "runtime normal/basic budget is lower than configured basic allocation due reserved allocations",
            "apply_condition": "normal/split/surge candidate quality separates; avoid broad budget release while PF below 1",
            "current_evidence_status": "WAIT_FOR_SELECTION_EVIDENCE" if not perf_ok_for_expansion else "SUPPORTED",
        },
        {
            "rank": 3,
            "candidate": "gross_exposure_pct_0.55_to_0.75",
            "classification": "LATER_STAGE_CANDIDATE",
            "policy_change": True,
            "config_field": "capital_budget_policy.gross_exposure_pct",
            "from_value": current_gross,
            "to_value": 0.75,
            "main_cap_delta_krw": round(delta(gross_075_main), 2),
            "reason": "larger exposure jump; only suitable after 0.65 scenario has positive validation",
            "apply_condition": "0.65 validation passes and PF remains above threshold",
            "current_evidence_status": "NOT_SUPPORTED_NOW" if not perf_ok_for_expansion else "SUPPORTED_AFTER_STAGE_1",
        },
        {
            "rank": 4,
            "candidate": "gross_exposure_pct_0.55_to_0.80",
            "classification": "REJECT_FOR_CURRENT_EVIDENCE",
            "policy_change": True,
            "config_field": "capital_budget_policy.gross_exposure_pct",
            "from_value": current_gross,
            "to_value": 0.80,
            "main_cap_delta_krw": round(delta(gross_080_main), 2),
            "reason": "largest exposure jump while latest aggregate and strategy PF are below 1",
            "apply_condition": "requires strong multi-day evidence, not current state",
            "current_evidence_status": "NOT_SUPPORTED_NOW",
        },
        {
            "rank": 5,
            "candidate": "cash_per_trade_2m_to_4m_or_5m",
            "classification": "REJECT_AS_PRIMARY_AXIS",
            "policy_change": True,
            "config_field": "cash_per_trade",
            "from_value": _to_float(cfg.get("cash_per_trade_krw"), 0.0),
            "to_value": "4000000_to_5000000",
            "main_cap_delta_krw": 0.0,
            "reason": "current sizing_mode=capital_slots, so cash_per_trade is not the primary active slot amount",
            "apply_condition": "only relevant if sizing_mode changes to fixed_cash or another path uses cash_per_trade",
            "current_evidence_status": "NOT_PRIMARY",
        },
    ]

    if not account_risk_clear:
        final_status = "NO_CAPITAL_EXPANSION_ACCOUNT_RISK_NOT_CLEAR"
    elif perf_ok_for_expansion:
        final_status = "STAGED_EXPANSION_SUPPORTED_START_WITH_0_65"
    elif perf_near_watch:
        final_status = "WATCHLIST_BUILD_0_65_SCENARIO_ONLY"
    else:
        final_status = "DO_NOT_EXPAND_FIX_SELECTION_EDGE_FIRST"

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z"),
        "schema_version": "capital_sizing_tuning_matrix_v1",
        "scope": "read_only_capital_sizing_tuning_priority",
        "policy_change": False,
        "trading_effect": False,
        "sources": {
            "scenario_report": str(SCENARIO_PATH),
            "config": _get(scenario, ["sources", "config"]),
            "paper_pnl_summary": _get(scenario, ["sources", "paper_pnl_summary"]),
            "run_log": _get(scenario, ["sources", "run_log"]),
        },
        "decision": {
            "status": final_status,
            "first_change_candidate_if_policy_approved": "capital_budget_policy.gross_exposure_pct 0.55 -> 0.65",
            "not_recommended_primary_change": "cash_per_trade 2m -> 4m/5m",
            "reason": "capital_slots mode plus latest PF/avg_ret evidence",
            "requires_policy_approval_for_apply": True,
        },
        "evidence_snapshot": {
            "sizing_mode": cfg.get("sizing_mode"),
            "cash_per_trade_effect": cfg.get("cash_per_trade_effect"),
            "gross_exposure_pct": current_gross,
            "current_main_effective_gross_pct": _to_float(current_main.get("effective_gross_exposure_pct"), 0.0),
            "current_main_gross_cap_krw": _to_float(current_main.get("effective_gross_cap_krw"), 0.0),
            "runtime_basic_alloc_pct": runtime_basic_effective,
            "config_basic_alloc_pct": config_basic,
            "gross_pf": gross_pf,
            "avg_ret": avg_ret,
            "win_rate": win_rate,
            "account_risk_clear": account_risk_clear,
        },
        "matrix_rows": matrix_rows,
    }


def main() -> int:
    payload = build_matrix()
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json_stamped = LOG_DIR / f"capital_sizing_tuning_matrix_{stamp}.json"
    out_csv_stamped = LOG_DIR / f"capital_sizing_tuning_matrix_{stamp}.csv"

    for path in (OUT_JSON, out_json_stamped):
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    rows = payload["matrix_rows"]
    fieldnames = list(rows[0].keys())
    for path in (OUT_CSV, out_csv_stamped):
        with path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    print(
        json.dumps(
            {
                "status": "OK",
                "decision_status": payload["decision"]["status"],
                "out_json": str(OUT_JSON),
                "out_csv": str(OUT_CSV),
                "out_json_stamped": str(out_json_stamped),
                "out_csv_stamped": str(out_csv_stamped),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
