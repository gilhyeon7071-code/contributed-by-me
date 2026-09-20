from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

OUT_JSON = LOG_DIR / "promotion_metric_threshold_design_latest.json"
OUT_CSV = LOG_DIR / "promotion_metric_threshold_design_latest.csv"
OUT_MD = LOG_DIR / "promotion_metric_threshold_design_latest.md"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _rule(
    metric_id: str,
    applies_to: str,
    metric_name: str,
    proposed_basis: str,
    minimum_sample_rule: str,
    pass_direction: str,
    fail_direction: str,
    source_needed: str,
    implementation_status: str = "DESIGN_ONLY",
) -> dict[str, Any]:
    return {
        "metric_id": metric_id,
        "applies_to": applies_to,
        "metric_name": metric_name,
        "proposed_basis": proposed_basis,
        "minimum_sample_rule": minimum_sample_rule,
        "pass_direction": pass_direction,
        "fail_direction": fail_direction,
        "source_needed": source_needed,
        "implementation_status": implementation_status,
        "policy_change_applied": False,
        "trading_logic_changed": False,
        "order_connection": "NONE",
    }


def build() -> dict[str, Any]:
    promotion_design = _read_json(LOG_DIR / "promotion_rule_design_latest.json")
    reclaim_review = _read_json(LOG_DIR / "reclaim_outcome_review_latest.json")
    surge_audit = _read_json(LOG_DIR / "surge_active_response_policy_audit_latest.json")

    promotion_summary = promotion_design.get("summary", {})
    reclaim_summary = reclaim_review.get("summary", {})
    surge_summary = surge_audit.get("summary", {})

    metrics = [
        _rule(
            "METRIC-01",
            "RECLAIM_REVIEW_CANDIDATE_ONLY",
            "minimum_unique_samples",
            "unique observed policy samples, not repeated manual sampler rows",
            "current unique_policy_rows=%s; threshold must be decided before policy implementation"
            % reclaim_summary.get("unique_policy_rows", "unknown"),
            "enough unique rows to reduce single-day/noise bias",
            "sample too small or duplicate-heavy",
            "reclaim_outcome_review_latest.json",
        ),
        _rule(
            "METRIC-02",
            "RECLAIM_REVIEW_CANDIDATE_ONLY",
            "followthrough_return",
            "first-to-last observed return after reclaim watch state",
            "current unique_observed_rows=%s; compare by signal class, not pooled blindly"
            % reclaim_summary.get("unique_observed_rows", "unknown"),
            "positive or loss-limited follow-through versus blocked baseline",
            "continued negative drift after observe signal",
            "reclaim_outcome_review_latest.csv/history",
        ),
        _rule(
            "METRIC-03",
            "PROMOTION_REVIEW_OBSERVE_ONLY",
            "wait_reclaim_success_rate",
            "share of WAIT_RECLAIM rows that reclaim and hold defined level",
            "current immediate_or_probe_ready_rows=%s; no ready row yet"
            % surge_summary.get("immediate_or_probe_ready_rows", "unknown"),
            "reclaim holds beyond observation window",
            "reclaim fails or immediately fades",
            "surge_active_response_layer plus intraday follow-through samples",
        ),
        _rule(
            "METRIC-04",
            "PROMOTION_REVIEW_OBSERVE_ONLY",
            "max_adverse_excursion",
            "worst drawdown during observation window before any review promotion",
            "must be measured on observed candidates before defining a live threshold",
            "drawdown stays inside acceptable probe-risk budget",
            "drawdown exceeds expected-value budget",
            "intraday price path joined to observe sampler",
        ),
        _rule(
            "METRIC-05",
            "OBSERVE_ONLY_NO_PROMOTION",
            "blocked_to_allowed_transition_rate",
            "how often blocked WAIT_RECLAIM later becomes allowed in source layer",
            "requires repeated days/sessions, not one snapshot",
            "transition happens with improved LOB/liquidity evidence",
            "blocked condition persists or worsens",
            "surge_active_response_layer history",
        ),
        _rule(
            "METRIC-06",
            "EVIDENCE_GAP_ONLY",
            "risk_measurement_completeness",
            "slippage/reject/liquidity samples usable for production risk decision",
            "current measurement_gap_count=%s; must be 0 before sizing/blocking policy change"
            % _read_json(LOG_DIR / "production_risk_policy_audit_latest.json")
            .get("summary", {})
            .get("measurement_gap_count", "unknown"),
            "all required risk measures have current usable samples",
            "missing columns, stale mock sample, or unusable reject/slippage data",
            "production_risk_policy_audit_latest.json",
        ),
        _rule(
            "METRIC-07",
            "GUARD_SCOPE_REVIEW_ONLY",
            "structural_stop_recurrence",
            "count repeated hard stops unrelated to true trading risk",
            "requires batch/log recurrence with D-rule and SSOT checks",
            "same non-trading-risk stop repeats and can be scoped without weakening safety",
            "orders/fills/date/risk-lock integrity issue remains true",
            "run_paper_daily logs, latest state JSON, D-rule evidence",
        ),
    ]

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS",
        "schema_version": "promotion_metric_threshold_design_v1",
        "source_files": {
            "promotion_rule_design": str(LOG_DIR / "promotion_rule_design_latest.json"),
            "reclaim_outcome_review": str(LOG_DIR / "reclaim_outcome_review_latest.json"),
            "surge_active_response_policy_audit": str(LOG_DIR / "surge_active_response_policy_audit_latest.json"),
            "production_risk_policy_audit": str(LOG_DIR / "production_risk_policy_audit_latest.json"),
        },
        "source_snapshot": {
            "promotion_rules": promotion_summary.get("rules", "unknown"),
            "reclaim_unique_policy_rows": reclaim_summary.get("unique_policy_rows", "unknown"),
            "reclaim_unique_observed_rows": reclaim_summary.get("unique_observed_rows", "unknown"),
            "surge_immediate_or_probe_ready_rows": surge_summary.get("immediate_or_probe_ready_rows", "unknown"),
        },
        "outputs": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
            "md": str(OUT_MD),
        },
        "scope": {
            "policy_effect": "read_only_metric_threshold_design_only",
            "full_logic_application": "NOT_APPLIED",
            "order_connection": "NONE",
        },
        "summary": {
            "metrics": len(metrics),
            "policy_change_applied_rows": 0,
            "trading_logic_changed_rows": 0,
            "order_connected_rows": 0,
            "thresholds_implemented": 0,
            "next_real_work": "collect enough unique samples and then freeze numeric pass/fail thresholds",
        },
        "metrics": metrics,
    }


def write_outputs(payload: dict[str, Any]) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    metrics = payload["metrics"]
    with OUT_CSV.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(metrics[0].keys()))
        writer.writeheader()
        writer.writerows(metrics)

    lines = [
        "# Promotion Metric Threshold Design",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- metrics: {payload['summary']['metrics']}",
        f"- policy_effect: {payload['scope']['policy_effect']}",
        f"- full_logic_application: {payload['scope']['full_logic_application']}",
        f"- order_connection: {payload['scope']['order_connection']}",
        f"- thresholds_implemented: {payload['summary']['thresholds_implemented']}",
        f"- policy_change_applied_rows: {payload['summary']['policy_change_applied_rows']}",
        f"- trading_logic_changed_rows: {payload['summary']['trading_logic_changed_rows']}",
        f"- order_connected_rows: {payload['summary']['order_connected_rows']}",
        f"- next_real_work: {payload['summary']['next_real_work']}",
        "",
        "## Metrics",
    ]
    for metric in metrics:
        lines.extend(
            [
                "",
                f"### {metric['metric_id']} {metric['metric_name']}",
                f"- applies_to: {metric['applies_to']}",
                f"- proposed_basis: {metric['proposed_basis']}",
                f"- minimum_sample_rule: {metric['minimum_sample_rule']}",
                f"- pass_direction: {metric['pass_direction']}",
                f"- fail_direction: {metric['fail_direction']}",
                f"- source_needed: {metric['source_needed']}",
                f"- implementation_status: {metric['implementation_status']}",
                f"- order_connection: {metric['order_connection']}",
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
