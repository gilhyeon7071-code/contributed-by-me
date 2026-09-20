from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

MATRIX_JSON = LOG_DIR / "c3_highvol_candidate_family_decision_matrix_latest.json"

OUT_JSON = LOG_DIR / "c3_highvol_forward_observation_ledger_spec_latest.json"
OUT_CSV = LOG_DIR / "c3_highvol_forward_observation_ledger_spec_latest.csv"
OUT_MD = LOG_DIR / "c3_highvol_forward_observation_ledger_spec_latest.md"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def get_matrix_row(matrix: list[dict[str, Any]], lane: str) -> dict[str, Any]:
    for row in matrix:
        if row.get("family_or_lane") == lane:
            return row
    raise KeyError(f"missing matrix row: {lane}")


def build_required_fields() -> list[dict[str, str]]:
    return [
        {"field": "observation_id", "required": "yes", "definition": "Stable key: family_or_lane + signal_date + code + exit_variant + source_run_id."},
        {"field": "source_run_id", "required": "yes", "definition": "Read-only generation or audit run id that produced the candidate row."},
        {"field": "as_of_date", "required": "yes", "definition": "Date when the observation row is produced. Must not be mixed with paper/broker execution dates."},
        {"field": "signal_date", "required": "yes", "definition": "Candidate signal date at market-data grain."},
        {"field": "code", "required": "yes", "definition": "Stock code."},
        {"field": "name", "required": "no", "definition": "Stock name when available; unknown allowed."},
        {"field": "family_or_lane", "required": "yes", "definition": "One of the decision-matrix lanes."},
        {"field": "lane_assignment_rule", "required": "yes", "definition": "Deterministic rule used to assign the row to the lane."},
        {"field": "candidate_family", "required": "yes", "definition": "C3_HIGHVOL_FAMILY_RESEARCH_V0 for the high-vol research family."},
        {"field": "regime", "required": "yes", "definition": "Market regime label used by the source artifact. BULL/SIDEWAYS are eligible for high-vol research; TRANSITION is excluded."},
        {"field": "entry_gap_pct", "required": "yes", "definition": "Observed entry gap feature; expected high-vol research range is >=0.0147299509 and <0.05."},
        {"field": "signal_v_accel", "required": "yes", "definition": "Volume acceleration feature; expected lower bound is >=1.255833911."},
        {"field": "signal_atr_pct", "required": "yes", "definition": "ATR percent feature; expected range is >=0.03 and <0.04."},
        {"field": "signal_value", "required": "yes", "definition": "Liquidity/value feature used only for lane assignment, not for existing C3 policy relaxation."},
        {"field": "exit_variant", "required": "yes", "definition": "Research exit variant. Current frozen family uses hold7_stop6."},
        {"field": "entry_price", "required": "yes", "definition": "Price used for research return calculation."},
        {"field": "exit_date", "required": "yes", "definition": "Research exit date. Must be based on verified trading-day progression where available."},
        {"field": "exit_price", "required": "yes", "definition": "Price used for research exit calculation."},
        {"field": "ret_after_cost", "required": "yes", "definition": "Forward return after the research cost assumption used by the source audit."},
        {"field": "exit_reason", "required": "yes", "definition": "Research exit reason such as hold or stop."},
        {"field": "data_quality_status", "required": "yes", "definition": "PASS, EXCLUDED_PRICE_HISTORY, EXCLUDED_MISSING_FIELD, or REVIEW_REQUIRED."},
        {"field": "operational_effect", "required": "yes", "definition": "Must be NOT_APPLIED for every observation row in this contract."},
    ]


def build_lane_specs(matrix: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "family_or_lane": "existing_c3_operational_family",
            "source_decision": get_matrix_row(matrix, "existing_c3_operational_family")["current_decision"],
            "ledger_role": "baseline_reference_only",
            "assignment_rule": "Do not create high-vol observation rows from this lane except as a baseline comparison label.",
            "minimum_forward_evidence_before_review": "not_applicable_for_keep_unchanged",
            "promotion_allowed_by_this_spec": "no",
            "stop_condition": "Any attempt to use this spec to relax existing C3 gates must stop.",
        },
        {
            "family_or_lane": "c3_highvol_family_research_container",
            "source_decision": get_matrix_row(matrix, "c3_highvol_family_research_container")["current_decision"],
            "ledger_role": "parent_research_container",
            "assignment_rule": "Rows must first satisfy the frozen high-vol family shape: BULL/SIDEWAYS, gap >=0.0147299509 and <0.05, v_accel >=1.255833911, ATR >=0.03 and <0.04, exit_variant=hold7_stop6.",
            "minimum_forward_evidence_before_review": ">=20 non-overlapping forward observations, PF>=1.2, ret_sum>0 after costs, and no materially negative adverse/sideways split.",
            "promotion_allowed_by_this_spec": "no",
            "stop_condition": "If source matrix is not NOT_APPROVED/NOT_APPLIED, regenerate the spec after explicit policy review.",
        },
        {
            "family_or_lane": "primary_floor500_quality_lane",
            "source_decision": get_matrix_row(matrix, "primary_floor500_quality_lane")["current_decision"],
            "ledger_role": "primary_shadow_lane",
            "assignment_rule": "High-vol family shape plus signal_value >= 500,000,000.",
            "minimum_forward_evidence_before_review": "Prefer >=20 forward observations. ret_sum>0, PF>=1.2, and ret_without_top5 remains positive or concentration is explicitly controlled.",
            "promotion_allowed_by_this_spec": "no",
            "stop_condition": "Do not convert historical floor500 strength into a production value-floor rule without forward evidence.",
        },
        {
            "family_or_lane": "sub500_current_observation_lane",
            "source_decision": get_matrix_row(matrix, "sub500_current_observation_lane")["current_decision"],
            "ledger_role": "current_like_micro_liquidity_observation",
            "assignment_rule": "High-vol family shape plus signal_value < 500,000,000. Keep separate from floor500 because prior evidence was only n=2.",
            "minimum_forward_evidence_before_review": ">=10 current-like observations before judging whether this is a valid separate micro-liquidity lane.",
            "promotion_allowed_by_this_spec": "no",
            "stop_condition": "Do not buy from this lane while it remains OBSERVE_ONLY.",
        },
        {
            "family_or_lane": "sub500_legacy_weak_lane",
            "source_decision": get_matrix_row(matrix, "sub500_legacy_weak_lane")["current_decision"],
            "ledger_role": "true_exclude_tracking",
            "assignment_rule": "Historical weak sub500 rows stay excluded unless future rows form a materially different current-like pattern.",
            "minimum_forward_evidence_before_review": "not_applicable_until_new_pattern_exists",
            "promotion_allowed_by_this_spec": "no",
            "stop_condition": "Do not merge this lane back into the sub500 current observation lane.",
        },
        {
            "family_or_lane": "value_cap_review_group",
            "source_decision": get_matrix_row(matrix, "value_cap_review_group")["current_decision"],
            "ledger_role": "highvol_only_value_cap_review",
            "assignment_rule": "Rows above the existing C3 value cap may be reviewed only inside the high-vol family; this is not a broad C3 value-cap relaxation.",
            "minimum_forward_evidence_before_review": "Forward high-vol-only rows must show positive return without top-trade dependence.",
            "promotion_allowed_by_this_spec": "no",
            "stop_condition": "Stop if a row is used to relax existing C3 value cap outside high-vol research.",
        },
        {
            "family_or_lane": "no_mktvol_membership_exploration_group",
            "source_decision": get_matrix_row(matrix, "no_mktvol_membership_exploration_group")["current_decision"],
            "ledger_role": "membership_gap_shadow_compare",
            "assignment_rule": "Track rows outside existing C3 mkt_vol membership separately; never treat them as current C3 buy candidates.",
            "minimum_forward_evidence_before_review": "Forward shadow comparison must show whether mkt_vol exclusion is harmful or protective.",
            "promotion_allowed_by_this_spec": "no",
            "stop_condition": "Stop if membership-gap rows are mixed into official C3 candidate counts.",
        },
    ]


def build_payload() -> dict[str, Any]:
    matrix_payload = load_json(MATRIX_JSON)
    matrix = matrix_payload["decision_matrix"]

    lane_specs = build_lane_specs(matrix)
    required_fields = build_required_fields()

    validation = [
        "source_matrix_not_approved_not_applied: PASS"
        if matrix_payload.get("operational_decision") == "NOT_APPROVED"
        and matrix_payload.get("full_logic_application") == "NOT_APPLIED"
        else "source_matrix_not_approved_not_applied: FAIL",
        "source_matrix_rows_7: PASS" if len(matrix) == 7 else "source_matrix_rows_7: FAIL",
        "lane_specs_match_matrix_rows: PASS"
        if {row["family_or_lane"] for row in lane_specs} == {row["family_or_lane"] for row in matrix}
        else "lane_specs_match_matrix_rows: FAIL",
        "required_fields_have_observation_id_and_quality_status: PASS"
        if {"observation_id", "data_quality_status"}.issubset({row["field"] for row in required_fields})
        else "required_fields_have_observation_id_and_quality_status: FAIL",
        "all_specs_no_promotion_allowed: PASS"
        if all(row["promotion_allowed_by_this_spec"] == "no" for row in lane_specs)
        else "all_specs_no_promotion_allowed: FAIL",
    ]

    return {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "read_only_c3_highvol_forward_observation_ledger_spec",
        "classification": "C3_HIGHVOL_FORWARD_OBSERVATION_LEDGER_SPEC_RESEARCH_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "source_artifacts": {
            "decision_matrix": str(MATRIX_JSON),
        },
        "ledger_contract": {
            "grain": "one row per code per signal_date per family_or_lane per exit_variant per source_run_id",
            "purpose": "Collect future read-only observations by lane so later policy review can compare actual forward evidence without changing current trading logic.",
            "dedupe_key": ["family_or_lane", "signal_date", "code", "exit_variant", "source_run_id"],
            "forbidden_uses": [
                "production_candidate_generation",
                "official_backtest_claim",
                "hpo_objective",
                "paper_or_live_order",
                "gate_or_threshold_relaxation",
                "existing_c3_contract_replacement",
            ],
        },
        "required_fields": required_fields,
        "lane_specs": lane_specs,
        "review_gates": [
            {
                "gate": "sample_size",
                "pass_condition": "family >=20 forward observations; sub500 current lane >=10 observation rows for first review only",
                "meaning": "Enough evidence to start review, not enough by itself to approve trading.",
            },
            {
                "gate": "return_quality",
                "pass_condition": "ret_sum > 0 and PF >= 1.2 after costs",
                "meaning": "Basic forward return quality.",
            },
            {
                "gate": "concentration",
                "pass_condition": "ret_without_top5 > 0 or explicit concentration control is designed and separately validated",
                "meaning": "Avoid approving a rule driven only by a few outliers.",
            },
            {
                "gate": "regime_split",
                "pass_condition": "BULL/SIDEWAYS results are reported separately; TRANSITION remains excluded unless a new explicit research pass is approved",
                "meaning": "Do not blend incompatible market states.",
            },
            {
                "gate": "data_quality",
                "pass_condition": "Rows with abnormal price history or missing required fields are excluded from return claims and reported separately",
                "meaning": "Return evidence must not be built from bad price data.",
            },
        ],
        "operation_effect": {
            "candidate_generation": False,
            "backtest": False,
            "hpo": False,
            "diagnostics": False,
            "paper_or_live": False,
            "parameter_or_gate_change": False,
        },
        "validation": validation,
    }


def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    fields = [
        "family_or_lane",
        "source_decision",
        "ledger_role",
        "assignment_rule",
        "minimum_forward_evidence_before_review",
        "promotion_allowed_by_this_spec",
        "stop_condition",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_md(payload: dict[str, Any], path: Path) -> None:
    lines = [
        "# C3 High-Vol Forward Observation Ledger Spec",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- operational_decision: {payload['operational_decision']}",
        f"- full_logic_application: {payload['full_logic_application']}",
        f"- grain: {payload['ledger_contract']['grain']}",
        "",
        "## Lane Specs",
        "",
        "| family_or_lane | role | assignment_rule | minimum_evidence | stop_condition |",
        "|---|---|---|---|---|",
    ]
    for row in payload["lane_specs"]:
        safe = {key: str(value).replace("|", "/") for key, value in row.items()}
        lines.append(
            f"| {safe['family_or_lane']} | {safe['ledger_role']} | {safe['assignment_rule']} | {safe['minimum_forward_evidence_before_review']} | {safe['stop_condition']} |"
        )
    lines.extend(
        [
            "",
            "## Required Fields",
            "",
            "| field | required | definition |",
            "|---|---:|---|",
        ]
    )
    for row in payload["required_fields"]:
        safe = {key: str(value).replace("|", "/") for key, value in row.items()}
        lines.append(f"| {safe['field']} | {safe['required']} | {safe['definition']} |")
    lines.extend(
        [
            "",
            "## Review Gates",
            "",
            "| gate | pass_condition | meaning |",
            "|---|---|---|",
        ]
    )
    for row in payload["review_gates"]:
        safe = {key: str(value).replace("|", "/") for key, value in row.items()}
        lines.append(f"| {safe['gate']} | {safe['pass_condition']} | {safe['meaning']} |")
    lines.extend(
        [
            "",
            "## Validation",
            "",
            *[f"- {item}" for item in payload["validation"]],
            "",
            "## Guardrail",
            "",
            "This is a read-only observation ledger specification. It does not create candidate rows, approve trading, change gates, or replace existing C3.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    payload = build_payload()
    if any(item.endswith("FAIL") for item in payload["validation"]):
        raise SystemExit("validation failed: " + "; ".join(payload["validation"]))

    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    write_csv(payload["lane_specs"], OUT_CSV)
    write_md(payload, OUT_MD)
    print(
        json.dumps(
            {
                "status": "ok",
                "json": str(OUT_JSON),
                "csv": str(OUT_CSV),
                "md": str(OUT_MD),
                "lane_specs": len(payload["lane_specs"]),
                "required_fields": len(payload["required_fields"]),
                "review_gates": len(payload["review_gates"]),
                "classification": payload["classification"],
                "operational_decision": payload["operational_decision"],
                "full_logic_application": payload["full_logic_application"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
