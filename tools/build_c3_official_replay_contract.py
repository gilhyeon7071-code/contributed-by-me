from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(r"E:\1_Data")
LOG_DIR = ROOT / "2_Logs"
SCAN_DIR = LOG_DIR / "research_report_entry_trigger_v2_transition_value2b_ret1_1_relaxation_scope_scan_20260713_132200"

RELAXATION_CONTRACTS = SCAN_DIR / "relaxation_contracts.json"
DESIGN_JSON = LOG_DIR / "c3_normal_strategy_design_checklist_latest.json"
EXACT_SUMMARY = LOG_DIR / "c3_cross_variant_exit_repair_exact_entry_summary_latest.csv"
REPORT_BACKTEST = ROOT / "report_backtest_v41_1.py"

OUT_CONTRACT = LOG_DIR / "c3_normal_official_replay_contract_latest.json"
OUT_NATIVE_SELECTION = LOG_DIR / "c3_normal_official_replay_selection_contract_latest.json"
OUT_MATRIX = LOG_DIR / "c3_normal_official_replay_contract_matrix_latest.csv"
OUT_MD = LOG_DIR / "c3_normal_official_replay_contract_latest.md"

STATUS = "READ_ONLY_C3_OFFICIAL_REPLAY_CONTRACT_NOT_OPERATIONAL"
ALLOWED_NATIVE_KEYS = {
    "required_signals",
    "inverse_signals",
    "regimes",
    "signal_date_start",
    "signal_date_end",
    "numeric_filters",
    "entry_trigger",
    "exit_overrides",
    "research_note",
}


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _summary_row(summary: pd.DataFrame, tier: str, repair_variant: str) -> dict[str, Any]:
    mask = (
        summary["sample_mode"].astype(str).eq("dedup_trade_key")
        & summary["source_variant"].astype(str).eq("__ALL_SOURCE_VARIANTS__")
        & summary["tier"].astype(str).eq(tier)
        & summary["repair_variant"].astype(str).eq(repair_variant)
    )
    rows = summary.loc[mask].to_dict(orient="records")
    return rows[0] if rows else {}


def _validate_native_contract(contract: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    unknown = sorted(set(contract).difference(ALLOWED_NATIVE_KEYS))
    if unknown:
        issues.append(f"unknown_native_keys={unknown}")
    if not isinstance(contract.get("required_signals", []), list):
        issues.append("required_signals_not_list")
    if not isinstance(contract.get("numeric_filters", []), list):
        issues.append("numeric_filters_not_list")
    if not isinstance(contract.get("entry_trigger", {}), dict):
        issues.append("entry_trigger_not_object")
    if not isinstance(contract.get("exit_overrides", {}), dict):
        issues.append("exit_overrides_not_object")
    trigger_type = str((contract.get("entry_trigger") or {}).get("type", "")).strip()
    if trigger_type not in {"breakout_prior_high", "next_open"}:
        issues.append(f"unsupported_entry_trigger_type={trigger_type}")
    return issues


def main() -> int:
    relaxation = _read_json(RELAXATION_CONTRACTS)
    design = _read_json(DESIGN_JSON)
    summary = pd.read_csv(EXACT_SUMMARY)

    if "balanced_original" not in relaxation:
        raise SystemExit("balanced_original contract missing")

    base_native = dict(relaxation["balanced_original"])
    base_native["research_note"] = (
        "C3 normal official replay native report contract. "
        "Post-replay adapter and targeted exit repair are not native report_backtest fields."
    )
    native_issues = _validate_native_contract(base_native)

    exact_c3_base = _summary_row(summary, "exact_c3_after_recheck", "baseline")
    exact_c3_repair = _summary_row(summary, "exact_c3_after_recheck", "profit_then_stop_repair_exact_entry_5pct_50pct")
    c3_like_base = _summary_row(summary, "c3_like_score080_follow1_v080", "baseline")
    c3_like_repair = _summary_row(summary, "c3_like_score080_follow1_v080", "profit_then_stop_repair_exact_entry_5pct_50pct")
    all_base = _summary_row(summary, "all_cross_rows", "baseline")
    all_repair = _summary_row(summary, "all_cross_rows", "profit_then_stop_repair_exact_entry_5pct_50pct")

    post_replay_adapter_contract = {
        "adapter_name": "c3_normal_daily_adapter",
        "strategy_family": "C3_TRANSITION_PULLBACK_REACCEL_NORMAL",
        "route_class": "NORMAL_DAILY_RESEARCH",
        "must_not_use": [
            "surge_inject",
            "realtime_lob",
            "orderflow_l1",
            "surge_budget_slots",
        ],
        "post_replay_filters": [
            {"field": "c3_after_recheck", "op": "==", "value": 1, "source": "cross_variant_recheck"},
            {"field": "score", "op": ">=", "value": 1.0, "source": "report_trade_csv"},
            {"field": "followthrough_1d", "op": "==", "value": 1, "source": "report_trade_csv_after_entry"},
            {"field": "signal_v_accel", "op": ">=", "value": 0.9, "source": "report_trade_csv"},
            {"field": "market_after_recheck", "op": "not_empty", "value": True, "source": "cross_variant_recheck"},
        ],
        "native_report_support": False,
        "native_report_support_reason": "followthrough_1d and c3_after_recheck are post-entry/recheck fields, not native pre-selection filters.",
    }

    targeted_exit_repair_contract = {
        "adapter_name": "targeted_profit_then_stop_repair_only",
        "trigger": [
            {"field": "exit_reason", "op": "contains", "value": "STOP"},
            {"field": "max_high_pct_to_exit", "op": ">=", "value": 5.0},
        ],
        "action": {
            "take_profit_fraction": 0.5,
            "take_profit_pct": 5.0,
            "remaining_fraction": 0.5,
            "remaining_outcome": "baseline_exit_outcome",
        },
        "native_report_support": False,
        "native_report_support_reason": "current report_backtest supports generic TP levels but not conditional only-if-stop-after-profit repair.",
        "rejected_alternative": {
            "name": "partial_tp_5pct_50pct_all",
            "reason": "reduced ret_sum in exact C3, C3-like, and all-cross headline tiers",
        },
    }

    support_matrix = [
        {
            "layer": "native_selection_contract",
            "supported_by_current_report_backtest": True,
            "contract_file": str(OUT_NATIVE_SELECTION),
            "notes": "Uses existing required_signals, numeric_filters, regimes, entry_trigger, exit_overrides fields.",
        },
        {
            "layer": "c3_post_replay_adapter",
            "supported_by_current_report_backtest": False,
            "contract_file": str(OUT_CONTRACT),
            "notes": "Requires post-replay filtering on c3_after_recheck and followthrough_1d.",
        },
        {
            "layer": "targeted_exit_repair",
            "supported_by_current_report_backtest": False,
            "contract_file": str(OUT_CONTRACT),
            "notes": "Requires conditional repair only for STOP rows that first reached +5%.",
        },
        {
            "layer": "broad_partial_take_profit",
            "supported_by_current_report_backtest": True,
            "contract_file": "",
            "notes": "Technically expressible as TP levels, but rejected by evidence for this C3 path.",
        },
        {
            "layer": "paper_or_live_routing",
            "supported_by_current_report_backtest": False,
            "contract_file": "",
            "notes": "Out of scope; no operational route or gate change.",
        },
    ]

    evidence = {
        "exact_c3_after_recheck": {
            "baseline": exact_c3_base,
            "targeted_repair": exact_c3_repair,
        },
        "c3_like_score080_follow1_v080": {
            "baseline": c3_like_base,
            "targeted_repair": c3_like_repair,
        },
        "all_cross_rows": {
            "baseline": all_base,
            "targeted_repair": all_repair,
        },
    }

    native_payload = {
        "selection_contract": base_native,
        "contract_status": "READ_ONLY_NATIVE_REPORT_SELECTION_CONTRACT",
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_live_order_changed": False,
            "policy_changed": False,
        },
    }
    OUT_NATIVE_SELECTION.write_text(json.dumps(native_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    full_contract = {
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": STATUS,
        "purpose": "Define a scoped official replay contract for C3 normal daily adapter and targeted exit repair.",
        "source_files": {
            "relaxation_contracts": str(RELAXATION_CONTRACTS),
            "design_checklist": str(DESIGN_JSON),
            "exact_entry_summary": str(EXACT_SUMMARY),
            "report_backtest": str(REPORT_BACKTEST),
        },
        "native_report_selection_contract_file": str(OUT_NATIVE_SELECTION),
        "native_contract_validation": {
            "allowed_key_check": "PASS" if not native_issues else "FAIL",
            "issues": native_issues,
        },
        "native_report_selection_contract": base_native,
        "post_replay_adapter_contract": post_replay_adapter_contract,
        "targeted_exit_repair_contract": targeted_exit_repair_contract,
        "support_matrix": support_matrix,
        "evidence": evidence,
        "design_source_status": design.get("status"),
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_live_order_changed": False,
            "policy_changed": False,
        },
        "full_logic_application": "NOT_APPLIED",
        "next_replay_boundary": [
            "The native selection contract can be passed to report_backtest research mode.",
            "The C3 adapter and targeted exit repair require post-replay tooling or code support.",
            "Do not route to paper/live or change stable gates from this contract.",
        ],
        "outputs": {
            "full_contract_json": str(OUT_CONTRACT),
            "native_selection_contract_json": str(OUT_NATIVE_SELECTION),
            "support_matrix_csv": str(OUT_MATRIX),
            "markdown": str(OUT_MD),
        },
    }
    OUT_CONTRACT.write_text(json.dumps(full_contract, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(support_matrix).to_csv(OUT_MATRIX, index=False, encoding="utf-8-sig")

    lines = [
        "# C3 normal official replay contract",
        "",
        f"- status: `{STATUS}`",
        "- full logic application: `NOT_APPLIED`",
        f"- native selection contract: `{OUT_NATIVE_SELECTION}`",
        f"- native contract validation: `{full_contract['native_contract_validation']['allowed_key_check']}`",
        "",
        "## Contract split",
        "",
        "- Native report selection contract: supported by current `report_backtest_v41_1.py`.",
        "- C3 post-replay adapter: not native, needs post-replay filtering.",
        "- Targeted exit repair: not native, needs conditional repair tooling/code support.",
        "- Broad partial TP: technically native but rejected by evidence.",
        "",
        "## Evidence",
        "",
    ]
    for tier, vals in evidence.items():
        b = vals["baseline"]
        r = vals["targeted_repair"]
        lines.append(
            "- "
            f"{tier}: baseline ret_sum={b.get('ret_sum')} PF={b.get('profit_factor')} -> "
            f"targeted repair ret_sum={r.get('ret_sum')} PF={r.get('profit_factor')} repair_hit_n={r.get('repair_hit_n')}"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- This contract does not execute a replay.",
            "- This contract does not change candidates, backtests, HPO, paper trading, broker routing, gates, policy, or capital allocation.",
            "- Any official replay must report native report output separately from post-replay adapter output.",
        ]
    )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(
        json.dumps(
            {
                "status": STATUS,
                "native_contract_validation": full_contract["native_contract_validation"]["allowed_key_check"],
                "support_matrix_rows": len(support_matrix),
                "native_selection_contract": str(OUT_NATIVE_SELECTION),
                "full_contract": str(OUT_CONTRACT),
                "matrix": str(OUT_MATRIX),
                "markdown": str(OUT_MD),
                "full_logic_application": "NOT_APPLIED",
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
