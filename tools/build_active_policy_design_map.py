from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

OUT_JSON = LOG_DIR / "active_policy_design_map_latest.json"
OUT_CSV = LOG_DIR / "active_policy_design_map_latest.csv"
OUT_MD = LOG_DIR / "active_policy_design_map_latest.md"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _summary(path: Path) -> dict[str, Any]:
    return _read_json(path).get("summary", {})


def _row(
    layer: str,
    current_effect: str,
    current_status: str,
    evidence: str,
    decision_needed: str,
    next_action: str,
) -> dict[str, Any]:
    return {
        "layer": layer,
        "current_effect": current_effect,
        "current_status": current_status,
        "evidence": evidence,
        "decision_needed": decision_needed,
        "next_action": next_action,
        "trading_logic_changed": False,
        "order_route_changed": False,
        "policy_change_applied": False,
    }


def build() -> dict[str, Any]:
    active = _summary(LOG_DIR / "active_policy_layer_audit_latest.json")
    defense = _summary(LOG_DIR / "defense_signal_policy_path_audit_latest.json")
    production = _summary(LOG_DIR / "production_risk_policy_audit_latest.json")
    surge = _summary(LOG_DIR / "surge_active_response_policy_audit_latest.json")
    reclaim = _summary(LOG_DIR / "reclaim_outcome_review_latest.json")

    rows = [
        _row(
            "core_always_on_policy",
            "actual_policy",
            "KEEP_ALWAYS_ON",
            "keep_always_on_count=3; active_policy_grouped_items=%s"
            % active.get("grouped_items", "unknown"),
            "none_now",
            "do_not_change_without_specific failure evidence",
        ),
        _row(
            "defense_signal_entry_policy",
            "candidate_pool_filter",
            defense.get("actual_policy_classification", "unknown"),
            "current_active_block_rows=%s; potential_block_rows=%s; real_retained_log_block_estimate=%s"
            % (
                defense.get("current_active_block_rows", "unknown"),
                defense.get("potential_block_rows", "unknown"),
                defense.get("real_retained_log_block_estimate", "unknown"),
            ),
            "always_on_vs_signal_specific",
            "keep current behavior; accumulate reclaim observe samples before policy proposal",
        ),
        _row(
            "reclaim_observe_sampler",
            "observe_only_evidence",
            "SAMPLE_TOO_SMALL",
            "unique_policy_rows=%s; unique_observed_rows=%s; unique_watch_reclaim_rows=%s"
            % (
                reclaim.get("unique_policy_rows", "unknown"),
                reclaim.get("unique_observed_rows", "unknown"),
                reclaim.get("unique_watch_reclaim_rows", "unknown"),
            ),
            "sample_window_and_success_metric",
            "continue observe-only accumulation; do not connect to order route",
        ),
        _row(
            "production_risk_playbook",
            "risk_policy_possible",
            "NO_CURRENT_ACTIVE_EFFECT",
            "action=%s; active_policy_effect=%s; measurement_gap_count=%s"
            % (
                production.get("action", "unknown"),
                production.get("active_policy_effect", "unknown"),
                production.get("measurement_gap_count", "unknown"),
            ),
            "blocking_vs_sizing_vs_evidence_bundle",
            "do not change policy now; close measurement gaps before sizing/blocking changes",
        ),
        _row(
            "surge_active_response_layer",
            "read_only_classification",
            "WAIT_RECLAIM_OR_HARD_EXCLUDE_ONLY",
            "detected_rows=%s; immediate_or_probe_ready_rows=%s; route_enabled_rows=%s"
            % (
                surge.get("detected_rows", "unknown"),
                surge.get("immediate_or_probe_ready_rows", "unknown"),
                surge.get("route_enabled_rows", "unknown"),
            ),
            "how_wait_reclaim_becomes_candidate_review",
            "define evidence accumulation; no bypass from WAIT_RECLAIM to buy",
        ),
        _row(
            "fail_closed_and_validation_guards",
            "safety_guard",
            "KEEP_BUT_SCOPE_FAIL_CLOSED",
            "validation_guard_count=%s; dependency_review_required=%s"
            % (
                active.get("policy_layer_counts", {}).get("VALIDATION_GUARD", "unknown"),
                active.get("review_action_counts", {}).get("DEPENDENCY_REVIEW_REQUIRED", "unknown"),
            ),
            "scope hard-fail to true operational risk",
            "review only when a guard creates repeated structural stop unrelated to trading risk",
        ),
    ]

    counts: dict[str, int] = {}
    for row in rows:
        counts[row["current_status"]] = counts.get(row["current_status"], 0) + 1

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS",
        "schema_version": "active_policy_design_map_v1",
        "source_files": {
            "active_policy_layer_audit": str(LOG_DIR / "active_policy_layer_audit_latest.json"),
            "defense_signal_policy_path_audit": str(LOG_DIR / "defense_signal_policy_path_audit_latest.json"),
            "production_risk_policy_audit": str(LOG_DIR / "production_risk_policy_audit_latest.json"),
            "surge_active_response_policy_audit": str(LOG_DIR / "surge_active_response_policy_audit_latest.json"),
            "reclaim_outcome_review": str(LOG_DIR / "reclaim_outcome_review_latest.json"),
        },
        "outputs": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
            "md": str(OUT_MD),
        },
        "scope": {
            "policy_effect": "read_only_design_map_only",
            "full_logic_application": "NOT_APPLIED",
            "order_connection": "NONE",
        },
        "summary": {
            "rows": len(rows),
            "status_counts": counts,
            "trading_logic_changed_rows": 0,
            "order_route_changed_rows": 0,
            "policy_change_applied_rows": 0,
            "next_real_work": "define promotion/evidence rules before any policy change",
        },
        "rows": rows,
    }


def write_outputs(payload: dict[str, Any]) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    rows = payload["rows"]
    with OUT_CSV.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    lines = [
        "# Active Policy Design Map",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- rows: {payload['summary']['rows']}",
        f"- policy_effect: {payload['scope']['policy_effect']}",
        f"- full_logic_application: {payload['scope']['full_logic_application']}",
        f"- order_connection: {payload['scope']['order_connection']}",
        f"- trading_logic_changed_rows: {payload['summary']['trading_logic_changed_rows']}",
        f"- order_route_changed_rows: {payload['summary']['order_route_changed_rows']}",
        f"- policy_change_applied_rows: {payload['summary']['policy_change_applied_rows']}",
        f"- next_real_work: {payload['summary']['next_real_work']}",
        "",
        "## Rows",
    ]
    for row in rows:
        lines.extend(
            [
                "",
                f"### {row['layer']}",
                f"- current_effect: {row['current_effect']}",
                f"- current_status: {row['current_status']}",
                f"- evidence: {row['evidence']}",
                f"- decision_needed: {row['decision_needed']}",
                f"- next_action: {row['next_action']}",
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
