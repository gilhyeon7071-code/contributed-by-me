from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SPEC_JSON = LOG_DIR / "c3_highvol_research_candidate_spec_latest.json"
DUAL_JSON = LOG_DIR / "c3_highvol_dual_band_observation_audit_latest.json"
LIQ_JSON = LOG_DIR / "c3_highvol_liquidity_band_compare_latest.json"
AVAIL_JSON = LOG_DIR / "c3_highvol_shadow_availability_audit_latest.json"

OUT_JSON = LOG_DIR / "c3_highvol_candidate_family_decision_matrix_latest.json"
OUT_CSV = LOG_DIR / "c3_highvol_candidate_family_decision_matrix_latest.csv"
OUT_MD = LOG_DIR / "c3_highvol_candidate_family_decision_matrix_latest.md"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def find_summary(rows: list[dict[str, Any]], key: str, value: str) -> dict[str, Any]:
    for row in rows:
        if str(row.get(key)) == value:
            return row
    raise KeyError(f"missing summary row: {key}={value}")


def fmt_float(value: Any, digits: int = 6) -> str:
    if value is None:
        return "NA"
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def evidence_text(row: dict[str, Any]) -> str:
    return (
        f"n={row.get('n')}, ret_sum={fmt_float(row.get('ret_sum'))}, "
        f"PF={fmt_float(row.get('profit_factor'))}, "
        f"ret_without_top5={fmt_float(row.get('ret_without_top5'))}, "
        f"top5_share={fmt_float(row.get('top5_share_gross_profit'))}"
    )


def build_matrix() -> dict[str, Any]:
    spec = load_json(SPEC_JSON)
    dual = load_json(DUAL_JSON)
    liq = load_json(LIQ_JSON)
    avail = load_json(AVAIL_JSON)

    all_shadow = find_summary(dual["summary"], "value", "all_dual_band_source")
    floor500 = find_summary(dual["summary"], "value", "primary_floor500_quality_lane")
    sub500_current = find_summary(dual["summary"], "value", "sub500_current_observation_lane")
    sub500_legacy = find_summary(dual["summary"], "value", "sub500_legacy_weak_lane")
    current_cap = find_summary(liq["summary"], "band_id", "current_cap_le_2b")
    value_gt_2b = find_summary(liq["summary"], "band_id", "value_gt_2b_all")
    no_mktvol = find_summary(
        avail["summary"],
        "value",
        "EXPLORATION_C3_CAPS_NO_MKTVOL_MEMBERSHIP",
    )
    value_cap_review = find_summary(
        avail["summary"],
        "value",
        "EXPLORATION_VALUE_CAP_REVIEW_REQUIRED",
    )

    best_rule = spec["evidence_summary"]["best_grid_rule"]
    candidate_spec = spec["candidate_spec"]

    matrix = [
        {
            "family_or_lane": "existing_c3_operational_family",
            "current_decision": "KEEP_UNCHANGED",
            "operational_effect": "NO_CHANGE",
            "evidence_summary": (
                "Current C3 contract is the baseline, not replaced by the high-vol research family. "
                f"Current value cap slice inside shadow sample: {evidence_text(current_cap)}."
            ),
            "blockers": "High-vol evidence is from a separate research family; it does not justify broad C3 contract relaxation.",
            "forward_evidence_required": "None for keeping current C3 unchanged.",
            "next_readonly_step": "Compare future shadow candidates against existing C3 without changing C3 gates.",
        },
        {
            "family_or_lane": "c3_highvol_family_research_container",
            "current_decision": "KEEP_RESEARCH_ONLY",
            "operational_effect": "NOT_APPLIED",
            "evidence_summary": (
                f"Candidate={spec['candidate_id']}; regime={','.join(candidate_spec['market_regime'])}; "
                f"exit={candidate_spec['exit_variant']}; best_n={best_rule['n_all']}; "
                f"best_ret={fmt_float(best_rule['ret_all'])}; best_pf={fmt_float(best_rule['pf_all'])}; "
                f"2026H1_n={best_rule['n_2026H1']}; 2026H1_ret={fmt_float(best_rule['ret_2026H1'])}."
            ),
            "blockers": "Sample is small, top-profit concentration exists, and current-period evidence is weak.",
            "forward_evidence_required": "At least 20 non-overlapping forward trades, PF>=1.2, ret_sum>0 after costs, and adverse/sideways period not materially negative.",
            "next_readonly_step": "Track as a shadow candidate family only; do not connect to candidate generation, HPO, backtest, or paper/live order flow.",
        },
        {
            "family_or_lane": "primary_floor500_quality_lane",
            "current_decision": "RESEARCH_SHADOW_ONLY",
            "operational_effect": "NOT_APPLIED",
            "evidence_summary": evidence_text(floor500),
            "blockers": "Strong historical result but no 2026H1 rows and still top5 concentrated.",
            "forward_evidence_required": "Require current-like forward sample expansion, preferably >=20 trades; if smaller, explicitly label as observation only. Ret_without_top5 should remain positive or concentration risk must be separately capped.",
            "next_readonly_step": "Keep as the first shadow lane for future observation; no production value-floor rule yet.",
        },
        {
            "family_or_lane": "sub500_current_observation_lane",
            "current_decision": "OBSERVE_ONLY",
            "operational_effect": "NOT_APPLIED",
            "evidence_summary": evidence_text(sub500_current),
            "blockers": "Only 2 rows; one stop; not enough to infer a tradable sub-500M structure.",
            "forward_evidence_required": "Collect at least 10 current-like observation rows before judging whether this is a valid separate micro-liquidity lane.",
            "next_readonly_step": "Keep a separate read-only observation lane so current-period candidates are not lost, but do not buy from it.",
        },
        {
            "family_or_lane": "sub500_legacy_weak_lane",
            "current_decision": "EXCLUDE_FOR_NOW",
            "operational_effect": "NOT_APPLIED",
            "evidence_summary": evidence_text(sub500_legacy),
            "blockers": "Legacy weak lane is negative and too small.",
            "forward_evidence_required": "No implementation evidence; only revisit if future observation creates a materially different current-like pattern.",
            "next_readonly_step": "Treat as true exclude for the current research pass.",
        },
        {
            "family_or_lane": "value_cap_review_group",
            "current_decision": "REVIEW_WITHIN_HIGHVOL_ONLY",
            "operational_effect": "NOT_APPLIED",
            "evidence_summary": (
                f"Above current C3 value cap: {evidence_text(value_gt_2b)}; "
                f"availability bucket: n={value_cap_review.get('n')}, ret_sum={fmt_float(value_cap_review.get('ret_sum'))}, "
                f"PF={fmt_float(value_cap_review.get('profit_factor'))}."
            ),
            "blockers": "This supports a high-vol family review, not a broad relaxation of existing C3 value cap.",
            "forward_evidence_required": "Forward high-vol-only tracking must prove that larger value names work without top-trade dependence.",
            "next_readonly_step": "Keep value-cap relaxation out of existing C3; evaluate only inside the high-vol research family.",
        },
        {
            "family_or_lane": "no_mktvol_membership_exploration_group",
            "current_decision": "RESEARCH_ONLY_NOT_C3_BUY",
            "operational_effect": "NOT_APPLIED",
            "evidence_summary": (
                f"Rows outside existing C3 mkt_vol membership: n={no_mktvol.get('n')}, "
                f"ret_sum={fmt_float(no_mktvol.get('ret_sum'))}, PF={fmt_float(no_mktvol.get('profit_factor'))}."
            ),
            "blockers": "These are not existing C3 buy candidates; membership gap must be treated as separate family exploration.",
            "forward_evidence_required": "Forward shadow comparison must show whether mkt_vol exclusion is harmful or protective for this family.",
            "next_readonly_step": "Keep separate from official C3 until forward evidence exists.",
        },
    ]

    dual_effect = dual.get("operation_effect", {})

    validation = [
        "source_spec_not_approved_not_applied: PASS"
        if spec.get("operational_decision") == "NOT_APPROVED"
        and spec.get("full_logic_application") == "NOT_APPLIED"
        else "source_spec_not_approved_not_applied: FAIL",
        "dual_band_operation_effect_no_changes: PASS"
        if dual_effect.get("candidate_generation_changed") is False
        and dual_effect.get("backtest_changed") is False
        and dual_effect.get("hpo_changed") is False
        and dual_effect.get("paper_or_broker_changed") is False
        and dual_effect.get("gate_or_threshold_changed") is False
        and dual_effect.get("policy_changed") is False
        and dual_effect.get("full_logic_application") == "NOT_APPLIED"
        else "dual_band_operation_effect_no_changes: FAIL",
        "matrix_rows_expected_7: PASS" if len(matrix) == 7 else "matrix_rows_expected_7: FAIL",
        "existing_c3_keep_unchanged_present: PASS"
        if any(row["family_or_lane"] == "existing_c3_operational_family" and row["current_decision"] == "KEEP_UNCHANGED" for row in matrix)
        else "existing_c3_keep_unchanged_present: FAIL",
        "all_rows_not_applied_or_no_change: PASS"
        if all(row["operational_effect"] in {"NOT_APPLIED", "NO_CHANGE"} for row in matrix)
        else "all_rows_not_applied_or_no_change: FAIL",
    ]

    return {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "read_only_c3_highvol_candidate_family_decision_matrix",
        "classification": "C3_HIGHVOL_CANDIDATE_FAMILY_DECISION_MATRIX_RESEARCH_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "source_artifacts": {
            "research_candidate_spec": str(SPEC_JSON),
            "dual_band_observation_audit": str(DUAL_JSON),
            "liquidity_band_compare": str(LIQ_JSON),
            "shadow_availability_audit": str(AVAIL_JSON),
        },
        "decision_matrix": matrix,
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
        "current_decision",
        "operational_effect",
        "evidence_summary",
        "blockers",
        "forward_evidence_required",
        "next_readonly_step",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_md(payload: dict[str, Any], path: Path) -> None:
    rows = payload["decision_matrix"]
    lines = [
        "# C3 High-Vol Candidate Family Decision Matrix",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- operational_decision: {payload['operational_decision']}",
        f"- full_logic_application: {payload['full_logic_application']}",
        "",
        "## Decision Matrix",
        "",
        "| family_or_lane | decision | operational_effect | evidence | blocker | next |",
        "|---|---:|---:|---|---|---|",
    ]
    for row in rows:
        lines.append(
            "| {family_or_lane} | {current_decision} | {operational_effect} | {evidence_summary} | {blockers} | {next_readonly_step} |".format(
                **{key: str(value).replace("|", "/") for key, value in row.items()}
            )
        )
    lines.extend(
        [
            "",
            "## Validation",
            "",
            *[f"- {item}" for item in payload["validation"]],
            "",
            "## Guardrail",
            "",
            "This artifact is read-only research evidence. It does not approve, relax, or connect any production candidate generation, backtest, HPO, paper, or live order path.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    payload = build_matrix()
    if any(item.endswith("FAIL") for item in payload["validation"]):
        raise SystemExit("validation failed: " + "; ".join(payload["validation"]))

    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    write_csv(payload["decision_matrix"], OUT_CSV)
    write_md(payload, OUT_MD)

    print(json.dumps({
        "status": "ok",
        "json": str(OUT_JSON),
        "csv": str(OUT_CSV),
        "md": str(OUT_MD),
        "rows": len(payload["decision_matrix"]),
        "classification": payload["classification"],
        "operational_decision": payload["operational_decision"],
        "full_logic_application": payload["full_logic_application"],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
