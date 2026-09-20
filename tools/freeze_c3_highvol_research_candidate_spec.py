from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

WALK_JSON = LOG_DIR / "c3_highvol_walkforward_grid_latest.json"
GRID_JSON = LOG_DIR / "c3_highvol_entry_grid_hold7_latest.json"
ROW_REVIEW_JSON = LOG_DIR / "c3_highvol_best_grid_row_review_latest.json"
EXIT_JSON = LOG_DIR / "c3_highvol_exit_variant_compare_latest.json"
BROADER_JSON = LOG_DIR / "c3_highvol_broader_source_replay_latest.json"
PROFILE_JSON = LOG_DIR / "c3_highvol_broader_sample_profile_latest.json"

OUT_JSON = LOG_DIR / "c3_highvol_research_candidate_spec_latest.json"
OUT_MD = LOG_DIR / "c3_highvol_research_candidate_spec_latest.md"


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise TypeError(f"{path} did not contain a JSON object")
    return data


def require_operation_effect_clear(name: str, data: dict[str, Any]) -> dict[str, Any]:
    effect = data.get("operation_effect")
    if not isinstance(effect, dict):
        raise ValueError(f"{name} missing operation_effect")
    changed_keys = [
        "candidate_generation_changed",
        "backtest_changed",
        "hpo_changed",
        "paper_or_broker_changed",
        "gate_or_threshold_changed",
        "policy_changed",
    ]
    bad = [k for k in changed_keys if effect.get(k) is not False]
    if bad:
        raise ValueError(f"{name} operation_effect not clear: {bad}")
    if effect.get("full_logic_application") != "NOT_APPLIED":
        raise ValueError(f"{name} full_logic_application is not NOT_APPLIED")
    return effect


def require_not_approved(name: str, data: dict[str, Any]) -> dict[str, Any]:
    classification = data.get("classification")
    if not isinstance(classification, dict):
        raise ValueError(f"{name} missing classification")
    if classification.get("operational_decision") != "NOT_APPROVED":
        raise ValueError(f"{name} is not NOT_APPROVED")
    if classification.get("full_logic_application") != "NOT_APPLIED":
        raise ValueError(f"{name} classification is not NOT_APPLIED")
    return classification


def find_grid_rule(grid: dict[str, Any], rule_id: str) -> dict[str, Any]:
    rows = grid.get("top_grid")
    if not isinstance(rows, list):
        raise ValueError("grid JSON missing top_grid")
    for row in rows:
        if isinstance(row, dict) and row.get("rule_id") == rule_id:
            return row
    raise ValueError(f"best rule not found in top_grid: {rule_id}")


def fmt_pct(value: Any) -> str:
    if value is None:
        return "NA"
    return f"{float(value) * 100:.2f}%"


def fmt_num(value: Any, digits: int = 6) -> str:
    if value is None:
        return "NA"
    return f"{float(value):.{digits}f}"


def build_markdown(spec: dict[str, Any]) -> str:
    evidence = spec["evidence_summary"]
    candidate = spec["candidate_spec"]
    guardrails = spec["guardrails"]
    future = spec["required_future_evidence_before_implementation"]
    validation = spec["validation"]

    lines: list[str] = []
    lines.append("# C3 High-Volatility Research Candidate Spec")
    lines.append("")
    lines.append(f"- candidate_id: {spec['candidate_id']}")
    lines.append(f"- generated_at: {spec['generated_at']}")
    lines.append(f"- status: {spec['status']}")
    lines.append(f"- operational_decision: {spec['operational_decision']}")
    lines.append(f"- full_logic_application: {spec['full_logic_application']}")
    lines.append("")
    lines.append("## Candidate Boundary")
    lines.append("")
    lines.append(f"- family_role: {candidate['family_role']}")
    lines.append(f"- existing_c3_relationship: {candidate['existing_c3_relationship']}")
    lines.append(f"- market_regime: {', '.join(candidate['market_regime'])}")
    lines.append(f"- exit_variant: {candidate['exit_variant']}")
    lines.append(f"- entry_gap_pct: {fmt_pct(candidate['entry_gap_pct']['min_inclusive'])} ~ {fmt_pct(candidate['entry_gap_pct']['max_exclusive'])}")
    lines.append(f"- signal_v_accel: {fmt_num(candidate['signal_v_accel']['min_inclusive'], 3)} ~ {candidate['signal_v_accel']['upper_band_research_only']}")
    lines.append(f"- signal_atr_pct: {fmt_pct(candidate['signal_atr_pct']['min_inclusive'])} ~ {fmt_pct(candidate['signal_atr_pct']['max_exclusive'])}")
    lines.append(f"- value_filter: {candidate['value_filter']}")
    lines.append("")
    lines.append("## Evidence Summary")
    lines.append("")
    lines.append(f"- best_grid_rule: {evidence['best_grid_rule']['rule_id']}")
    lines.append(f"- best_grid_all: n={evidence['best_grid_rule']['n_all']}, ret={fmt_num(evidence['best_grid_rule']['ret_all'])}, PF={fmt_num(evidence['best_grid_rule']['pf_all'])}")
    lines.append(f"- best_grid_2025H2: n={evidence['best_grid_rule']['n_2025H2']}, ret={fmt_num(evidence['best_grid_rule']['ret_2025H2'])}, PF={fmt_num(evidence['best_grid_rule']['pf_2025H2'])}")
    lines.append(f"- best_grid_2026H1: n={evidence['best_grid_rule']['n_2026H1']}, ret={fmt_num(evidence['best_grid_rule']['ret_2026H1'])}, PF={fmt_num(evidence['best_grid_rule']['pf_2026H1'])}")
    lines.append(f"- walk_forward_2025H2: test_n={evidence['walk_forward']['pre_2025H2_to_2025H2']['test_n']}, test_ret={fmt_num(evidence['walk_forward']['pre_2025H2_to_2025H2']['test_ret'])}, test_pf={fmt_num(evidence['walk_forward']['pre_2025H2_to_2025H2']['test_pf'])}")
    lines.append(f"- walk_forward_2026H1: test_n={evidence['walk_forward']['pre_2026_to_2026H1']['test_n']}, test_ret={fmt_num(evidence['walk_forward']['pre_2026_to_2026H1']['test_ret'])}, test_pf={fmt_num(evidence['walk_forward']['pre_2026_to_2026H1']['test_pf'])}")
    lines.append(f"- top5_share_gross_profit: {fmt_pct(evidence['concentration']['top5_share_gross_profit'])}")
    lines.append(f"- ret_without_top5: {fmt_num(evidence['concentration']['ret_without_top5'])}")
    lines.append("")
    lines.append("## Guardrails")
    lines.append("")
    for item in guardrails:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## Required Future Evidence Before Implementation")
    lines.append("")
    for item in future:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## Validation")
    lines.append("")
    for item in validation:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## Source Artifacts")
    lines.append("")
    for name, path in spec["source_artifacts"].items():
        lines.append(f"- {name}: {path}")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    sources = {
        "walk_forward": WALK_JSON,
        "entry_grid": GRID_JSON,
        "row_review": ROW_REVIEW_JSON,
        "exit_variant": EXIT_JSON,
        "broader_source": BROADER_JSON,
        "profile": PROFILE_JSON,
    }
    missing = [str(path) for path in sources.values() if not path.exists()]
    if missing:
        raise FileNotFoundError({"missing_sources": missing})

    walk = load_json(WALK_JSON)
    grid = load_json(GRID_JSON)
    review = load_json(ROW_REVIEW_JSON)
    exit_variant = load_json(EXIT_JSON)
    broader = load_json(BROADER_JSON)
    profile = load_json(PROFILE_JSON)

    for name, data in {
        "walk_forward": walk,
        "entry_grid": grid,
        "row_review": review,
        "exit_variant": exit_variant,
        "broader_source": broader,
        "profile": profile,
    }.items():
        require_not_approved(name, data)
        require_operation_effect_clear(name, data)

    selected = walk.get("selected")
    if not isinstance(selected, list) or len(selected) != 2:
        raise ValueError("walk-forward selected rows must contain exactly two windows")

    selected_by_walk = {row["walk_id"]: row for row in selected if isinstance(row, dict)}
    pre_2025 = selected_by_walk["train_pre_2025H2_test_2025H2"]
    pre_2026 = selected_by_walk["train_pre_2026_test_2026H1"]

    best_rule_id = review.get("best_rule_id")
    if best_rule_id != grid.get("classification", {}).get("best_rule_id"):
        raise ValueError("best rule mismatch between grid and row review")
    best_rule = find_grid_rule(grid, str(best_rule_id))
    concentration = review.get("concentration")
    if not isinstance(concentration, dict):
        raise ValueError("row review missing concentration")

    operation_effect = {
        "candidate_generation_changed": False,
        "backtest_changed": False,
        "hpo_changed": False,
        "paper_or_broker_changed": False,
        "gate_or_threshold_changed": False,
        "policy_changed": False,
        "full_logic_application": "NOT_APPLIED",
    }

    spec: dict[str, Any] = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "candidate_id": "C3_HIGHVOL_FAMILY_RESEARCH_V0",
        "status": "RESEARCH_SPEC_FROZEN_NOT_OPERATIONAL",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "scope": "freeze_read_only_research_candidate_spec_from_existing_c3_highvol_artifacts",
        "source_artifacts": {name: str(path) for name, path in sources.items()},
        "candidate_spec": {
            "family_role": "separate_high_volatility_candidate_family",
            "existing_c3_relationship": "not_a_replacement_for_existing_c3",
            "market_regime": ["BULL", "SIDEWAYS"],
            "excluded_market_regime": ["TRANSITION"],
            "exit_variant": "hold7_stop6",
            "entry_gap_pct": {
                "min_inclusive": best_rule["gap_lo"],
                "max_exclusive": best_rule["gap_hi"],
            },
            "signal_v_accel": {
                "min_inclusive": best_rule["v_accel_lo"],
                "best_same_sample_max_exclusive": best_rule["v_accel_hi"],
                "walkforward_2025H2_selected_max_exclusive": pre_2025["v_accel_hi"],
                "walkforward_2026H1_selected_max_exclusive": pre_2026["v_accel_hi"],
                "upper_band_research_only": "1.75_to_2.00_not_final_policy",
            },
            "signal_atr_pct": {
                "min_inclusive": best_rule["atr_lo"],
                "max_exclusive": best_rule["atr_hi"],
            },
            "value_filter": "none_required_for_research_candidate",
            "value_filter_reason": "value_500m_variant_ranked_high_but_removed_2026H1_rows; no-value rule preserved current-window evidence",
        },
        "evidence_summary": {
            "entry_grid": {
                "base_rows_bull_sideways": grid.get("base_rows_bull_sideways"),
                "grid_rows": grid.get("grid_rows"),
                "viable_grid_count": 38,
                "classification": grid["classification"],
            },
            "best_grid_rule": {
                "rule_id": best_rule["rule_id"],
                "n_all": best_rule["n_all"],
                "ret_all": best_rule["ret_all"],
                "pf_all": best_rule["pf_all"],
                "n_2025H2": best_rule["n_2025H2"],
                "ret_2025H2": best_rule["ret_2025H2"],
                "pf_2025H2": best_rule["pf_2025H2"],
                "n_2026H1": best_rule["n_2026H1"],
                "ret_2026H1": best_rule["ret_2026H1"],
                "pf_2026H1": best_rule["pf_2026H1"],
                "n_pre_2026": best_rule["n_pre_2026"],
                "ret_pre_2026": best_rule["ret_pre_2026"],
                "pf_pre_2026": best_rule["pf_pre_2026"],
            },
            "walk_forward": {
                "classification": walk["classification"],
                "pre_2025H2_to_2025H2": {
                    "rule_id": pre_2025["rule_id"],
                    "train_n": pre_2025["train_n"],
                    "train_ret": pre_2025["train_ret"],
                    "train_pf": pre_2025["train_pf"],
                    "test_n": pre_2025["test_n"],
                    "test_ret": pre_2025["test_ret"],
                    "test_pf": pre_2025["test_pf"],
                },
                "pre_2026_to_2026H1": {
                    "rule_id": pre_2026["rule_id"],
                    "train_n": pre_2026["train_n"],
                    "train_ret": pre_2026["train_ret"],
                    "train_pf": pre_2026["train_pf"],
                    "test_n": pre_2026["test_n"],
                    "test_ret": pre_2026["test_ret"],
                    "test_pf": pre_2026["test_pf"],
                },
            },
            "concentration": concentration,
            "negative_or_weak_points": [
                "2024H1 is negative in best grid sample",
                "2026H1 forward sample is only n=2",
                "top5_share_gross_profit remains high",
                "candidate is same research family only; no live or paper approval",
            ],
        },
        "guardrails": [
            "Do not modify existing C3 policy or thresholds from this spec.",
            "Do not use this spec as stable, HPO-approved, paper-approved, or broker-approved parameters.",
            "Do not relax mkt_vol20 or other existing C3 production defenses because of this spec.",
            "Do not use TRANSITION regime for this high-volatility family based on current evidence.",
            "Do not remove low-RS or 52-week defensive filters broadly; prior no_reversion_caps replay was negative.",
            "Keep this as a read-only shadow/research candidate layer until future evidence passes.",
            "Keep stable gate, paper quality gate, broker gate, risk lock, and FAIL-CLOSED behavior unchanged.",
        ],
        "required_future_evidence_before_implementation": [
            "Forward or non-overlapping validation sample should reach at least 20 trades for the candidate family.",
            "Forward test should remain positive with PF >= 1.2 and ret_sum > 0 after costs.",
            "A 2025H2-like adverse or sideways period should not be meaningfully negative.",
            "Current-regime sample should be expanded beyond n=2 before operational consideration.",
            "Top5 gross-profit concentration should fall below 55%, or ret_without_top5 should remain materially positive.",
            "Price-history integrity contract must remain applied in any replay used for approval.",
            "Implementation, if later approved, should start as a separate shadow candidate family, not as existing C3 replacement.",
            "No stable parameter, official backtest, paper, or broker path may consume this spec without an explicit later approval step.",
        ],
        "operation_effect": operation_effect,
        "validation": [
            "source_artifacts_exist: PASS",
            "source_classifications_not_approved: PASS",
            "source_operation_effect_no_changes: PASS",
            "best_rule_cross_artifact_match: PASS",
            "research_only_guardrails_recorded: PASS",
        ],
    }

    OUT_JSON.write_text(json.dumps(spec, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    OUT_MD.write_text(build_markdown(spec), encoding="utf-8")
    print(json.dumps({
        "status": "ok",
        "out_json": str(OUT_JSON),
        "out_md": str(OUT_MD),
        "candidate_id": spec["candidate_id"],
        "operational_decision": spec["operational_decision"],
        "full_logic_application": spec["full_logic_application"],
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
