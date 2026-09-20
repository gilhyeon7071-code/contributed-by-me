from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

OUT_JSON = LOG_DIR / "promotion_rule_design_latest.json"
OUT_CSV = LOG_DIR / "promotion_rule_design_latest.csv"
OUT_MD = LOG_DIR / "promotion_rule_design_latest.md"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _summary(path: Path) -> dict[str, Any]:
    return _read_json(path).get("summary", {})


def _rule(
    rule_id: str,
    signal_context: str,
    current_layer: str,
    default_state: str,
    promote_when: str,
    remain_observe_when: str,
    hard_block_when: str,
    required_evidence: str,
    output_state: str,
    order_connection: str = "NONE",
) -> dict[str, Any]:
    return {
        "rule_id": rule_id,
        "signal_context": signal_context,
        "current_layer": current_layer,
        "default_state": default_state,
        "promote_when": promote_when,
        "remain_observe_when": remain_observe_when,
        "hard_block_when": hard_block_when,
        "required_evidence": required_evidence,
        "output_state": output_state,
        "order_connection": order_connection,
        "policy_change_applied": False,
        "trading_logic_changed": False,
    }


def build() -> dict[str, Any]:
    design = _summary(LOG_DIR / "active_policy_design_map_latest.json")
    defense = _summary(LOG_DIR / "defense_signal_policy_path_audit_latest.json")
    reclaim = _summary(LOG_DIR / "reclaim_outcome_review_latest.json")
    surge = _summary(LOG_DIR / "surge_active_response_policy_audit_latest.json")
    production = _summary(LOG_DIR / "production_risk_policy_audit_latest.json")

    rules = [
        _rule(
            "PROMO-01",
            "normal_candidate_without_surge_or_defense_conflict",
            "core_always_on_policy",
            "KEEP_EXISTING_PATH",
            "existing candidate path already allows it",
            "not applicable",
            "core gate or hard safety gate blocks",
            "existing candidate decision artifacts",
            "NO_NEW_PROMOTION_RULE",
        ),
        _rule(
            "PROMO-02",
            "defense_signal_potential_block",
            "defense_signal_entry_policy",
            "OBSERVE_ONLY",
            "future sample shows reclaim succeeds after defense block with predefined success metric",
            "current real candidate block is 0 or sample remains too small",
            "follow-through failure, late-chase pattern, or hard safety gate block",
            "defense audit rows plus reclaim outcome samples",
            "RECLAIM_REVIEW_CANDIDATE_ONLY",
        ),
        _rule(
            "PROMO-03",
            "surge_wait_reclaim_allowed",
            "surge_active_response_layer",
            "WAIT_RECLAIM",
            "price reclaims defined level and follow-through sample passes after observation window",
            "active response remains WAIT_RECLAIM without enough follow-through evidence",
            "HARD_EXCLUDE label or entry decision remains blocked",
            "surge active response rows plus reclaim observe sampler history",
            "PROMOTION_REVIEW_OBSERVE_ONLY",
        ),
        _rule(
            "PROMO-04",
            "surge_wait_reclaim_blocked",
            "surge_active_response_layer",
            "BLOCKED_OBSERVE_ONLY",
            "not promoted directly; must first move to WAIT_RECLAIM_ALLOWED in source layer",
            "blocked but still useful for later outcome review",
            "entry block reason persists or liquidity/order-book evidence fails",
            "surge active response row with entry decision and LOB fields",
            "OBSERVE_ONLY_NO_PROMOTION",
        ),
        _rule(
            "PROMO-05",
            "surge_hard_exclude",
            "surge_active_response_layer",
            "HARD_BLOCK",
            "not promoted by this design",
            "not applicable",
            "HARD_EXCLUDE label remains present",
            "surge active response hard-exclude reason",
            "KEEP_BLOCK",
        ),
        _rule(
            "PROMO-06",
            "production_risk_normal_with_measurement_gap",
            "production_risk_playbook",
            "NO_CURRENT_ACTIVE_EFFECT",
            "not a candidate-promotion input until measurement gaps are closed",
            "risk artifact is normal but slippage/reject evidence incomplete",
            "risk action becomes BLOCK/SOFT_PAUSE or kill switch triggers",
            "production risk playbook plus slippage/reject usable samples",
            "EVIDENCE_GAP_ONLY",
        ),
        _rule(
            "PROMO-07",
            "fail_closed_structural_stop",
            "fail_closed_and_validation_guards",
            "KEEP_FAIL_CLOSED",
            "not promoted; only scoped if repeated stop is unrelated to trading risk",
            "guard triggers are rare or trading-risk-related",
            "orders_exec missing, date mismatch, paper/broker date mixed, or true risk lock",
            "batch logs, latest JSON, D-rule and SSOT checks",
            "GUARD_SCOPE_REVIEW_ONLY",
        ),
    ]

    status_counts: dict[str, int] = {}
    for rule in rules:
        status_counts[rule["output_state"]] = status_counts.get(rule["output_state"], 0) + 1

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS",
        "schema_version": "promotion_rule_design_v1",
        "source_files": {
            "active_policy_design_map": str(LOG_DIR / "active_policy_design_map_latest.json"),
            "defense_signal_policy_path_audit": str(LOG_DIR / "defense_signal_policy_path_audit_latest.json"),
            "reclaim_outcome_review": str(LOG_DIR / "reclaim_outcome_review_latest.json"),
            "surge_active_response_policy_audit": str(LOG_DIR / "surge_active_response_policy_audit_latest.json"),
            "production_risk_policy_audit": str(LOG_DIR / "production_risk_policy_audit_latest.json"),
        },
        "source_snapshot": {
            "design_rows": design.get("rows", "unknown"),
            "defense_current_active_block_rows": defense.get("current_active_block_rows", "unknown"),
            "reclaim_unique_policy_rows": reclaim.get("unique_policy_rows", "unknown"),
            "surge_immediate_or_probe_ready_rows": surge.get("immediate_or_probe_ready_rows", "unknown"),
            "production_measurement_gap_count": production.get("measurement_gap_count", "unknown"),
        },
        "outputs": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
            "md": str(OUT_MD),
        },
        "scope": {
            "policy_effect": "read_only_promotion_rule_design_only",
            "full_logic_application": "NOT_APPLIED",
            "order_connection": "NONE",
        },
        "summary": {
            "rules": len(rules),
            "output_state_counts": status_counts,
            "policy_change_applied_rows": 0,
            "trading_logic_changed_rows": 0,
            "order_connected_rows": 0,
            "next_real_work": "define metrics and sample thresholds before implementing any promotion rule",
        },
        "rules": rules,
    }


def write_outputs(payload: dict[str, Any]) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    rules = payload["rules"]
    with OUT_CSV.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(rules[0].keys()))
        writer.writeheader()
        writer.writerows(rules)

    lines = [
        "# Promotion Rule Design",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- rules: {payload['summary']['rules']}",
        f"- policy_effect: {payload['scope']['policy_effect']}",
        f"- full_logic_application: {payload['scope']['full_logic_application']}",
        f"- order_connection: {payload['scope']['order_connection']}",
        f"- policy_change_applied_rows: {payload['summary']['policy_change_applied_rows']}",
        f"- trading_logic_changed_rows: {payload['summary']['trading_logic_changed_rows']}",
        f"- order_connected_rows: {payload['summary']['order_connected_rows']}",
        f"- next_real_work: {payload['summary']['next_real_work']}",
        "",
        "## Rules",
    ]
    for rule in rules:
        lines.extend(
            [
                "",
                f"### {rule['rule_id']} {rule['signal_context']}",
                f"- current_layer: {rule['current_layer']}",
                f"- default_state: {rule['default_state']}",
                f"- promote_when: {rule['promote_when']}",
                f"- remain_observe_when: {rule['remain_observe_when']}",
                f"- hard_block_when: {rule['hard_block_when']}",
                f"- required_evidence: {rule['required_evidence']}",
                f"- output_state: {rule['output_state']}",
                f"- order_connection: {rule['order_connection']}",
            ]
        )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    payload = build()
    write_outputs(payload)
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
