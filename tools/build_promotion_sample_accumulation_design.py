from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

OUT_JSON = LOG_DIR / "promotion_sample_accumulation_design_latest.json"
OUT_CSV = LOG_DIR / "promotion_sample_accumulation_design_latest.csv"
OUT_MD = LOG_DIR / "promotion_sample_accumulation_design_latest.md"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _plan(
    sample_id: str,
    sample_family: str,
    source_event: str,
    source_files: str,
    unique_key: str,
    accumulation_timing: str,
    duplicate_rule: str,
    outcome_join: str,
    ready_for_threshold_when: str,
    blocked_from_policy_when: str,
) -> dict[str, Any]:
    return {
        "sample_id": sample_id,
        "sample_family": sample_family,
        "source_event": source_event,
        "source_files": source_files,
        "unique_key": unique_key,
        "accumulation_timing": accumulation_timing,
        "duplicate_rule": duplicate_rule,
        "outcome_join": outcome_join,
        "ready_for_threshold_when": ready_for_threshold_when,
        "blocked_from_policy_when": blocked_from_policy_when,
        "implementation_status": "DESIGN_ONLY",
        "policy_change_applied": False,
        "trading_logic_changed": False,
        "order_connection": "NONE",
    }


def build() -> dict[str, Any]:
    metric_design = _read_json(LOG_DIR / "promotion_metric_threshold_design_latest.json")
    reclaim_review = _read_json(LOG_DIR / "reclaim_outcome_review_latest.json")
    surge_audit = _read_json(LOG_DIR / "surge_active_response_policy_audit_latest.json")
    production_audit = _read_json(LOG_DIR / "production_risk_policy_audit_latest.json")

    metric_summary = metric_design.get("summary", {})
    reclaim_summary = reclaim_review.get("summary", {})
    surge_summary = surge_audit.get("summary", {})
    production_summary = production_audit.get("summary", {})

    plans = [
        _plan(
            "SAMPLE-01",
            "defense_reclaim",
            "defense signal potential block or reclaim watch row",
            "defense_signal_policy_path_audit_latest.*, reclaim_observe_sampler_history.csv",
            "trade_date + code + source_layer + checklist_status",
            "daily after candidate/surge audit artifacts are refreshed",
            "keep first row per unique key; repeated manual runs must not increase unique evidence",
            "join intraday price path from event time/date to end-of-window",
            "minimum unique sample rule is met and followthrough metric is available",
            "unique sample count remains too small or followthrough join is missing",
        ),
        _plan(
            "SAMPLE-02",
            "surge_wait_reclaim",
            "WAIT_RECLAIM_ALLOWED row in surge active response layer",
            "surge_active_response_layer_latest.*, reclaim_observe_sampler_history.csv",
            "trade_date + code + active_response_label + entry_decision",
            "daily after surge active response layer generation",
            "one source snapshot per code/date/label/decision; later duplicate snapshots are audit-only",
            "join reclaim level, hold result, and adverse excursion during observation window",
            "success/fade result exists for enough unique WAIT_RECLAIM rows",
            "immediate_or_probe_ready_rows is 0 or reclaim/hold result is absent",
        ),
        _plan(
            "SAMPLE-03",
            "surge_blocked_transition",
            "WAIT_RECLAIM_BLOCKED row that later becomes WAIT_RECLAIM_ALLOWED",
            "surge_active_response_layer history",
            "code + first_block_date + transition_date",
            "daily across sessions, not single snapshot only",
            "count only first blocked-to-allowed transition per code per event cycle",
            "join liquidity/LOB fields before and after transition",
            "transition rate and post-transition outcome are both measurable",
            "blocked reason persists or transition cannot be reconstructed",
        ),
        _plan(
            "SAMPLE-04",
            "risk_measurement",
            "production risk signal row with slippage/reject/liquidity measurement",
            "production_risk_policy_audit_latest.json and underlying dispatch/risk logs",
            "trade_date + signal_name + source",
            "daily after production risk playbook generation",
            "latest usable sample per signal/date; stale mock samples remain evidence gaps",
            "no price join; validate completeness and freshness only",
            "measurement_gap_count reaches 0 over current usable sources",
            "missing column, stale mock source, or unusable sample remains",
        ),
        _plan(
            "SAMPLE-05",
            "structural_stop",
            "hard stop that may be unrelated to true trading risk",
            "run_paper_daily_last.txt, latest state JSON, D-rule/SSOT evidence",
            "trade_date + stop_code + source_artifact",
            "daily after batch or loop stop event",
            "same stop counted once per date/source unless root cause changes",
            "join to D-rule, orders_exec, run_id/as_of, paper/broker date checks",
            "same non-trading-risk stop repeats with clean SSOT/risk checks",
            "orders_exec missing, exec_date mismatch, paper/broker mixed date, or true risk lock",
        ),
    ]

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS",
        "schema_version": "promotion_sample_accumulation_design_v1",
        "source_files": {
            "promotion_metric_threshold_design": str(LOG_DIR / "promotion_metric_threshold_design_latest.json"),
            "reclaim_outcome_review": str(LOG_DIR / "reclaim_outcome_review_latest.json"),
            "surge_active_response_policy_audit": str(LOG_DIR / "surge_active_response_policy_audit_latest.json"),
            "production_risk_policy_audit": str(LOG_DIR / "production_risk_policy_audit_latest.json"),
        },
        "source_snapshot": {
            "metric_design_count": metric_summary.get("metrics", "unknown"),
            "reclaim_unique_policy_rows": reclaim_summary.get("unique_policy_rows", "unknown"),
            "reclaim_unique_observed_rows": reclaim_summary.get("unique_observed_rows", "unknown"),
            "surge_detected_rows": surge_summary.get("detected_rows", "unknown"),
            "surge_immediate_or_probe_ready_rows": surge_summary.get("immediate_or_probe_ready_rows", "unknown"),
            "production_measurement_gap_count": production_summary.get("measurement_gap_count", "unknown"),
        },
        "outputs": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
            "md": str(OUT_MD),
        },
        "scope": {
            "policy_effect": "read_only_sample_accumulation_design_only",
            "full_logic_application": "NOT_APPLIED",
            "order_connection": "NONE",
        },
        "summary": {
            "sample_plans": len(plans),
            "policy_change_applied_rows": 0,
            "trading_logic_changed_rows": 0,
            "order_connected_rows": 0,
            "history_writer_implemented": 0,
            "next_real_work": "implement or schedule observe-only history writers after scope approval",
        },
        "plans": plans,
    }


def write_outputs(payload: dict[str, Any]) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    plans = payload["plans"]
    with OUT_CSV.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(plans[0].keys()))
        writer.writeheader()
        writer.writerows(plans)

    lines = [
        "# Promotion Sample Accumulation Design",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- sample_plans: {payload['summary']['sample_plans']}",
        f"- policy_effect: {payload['scope']['policy_effect']}",
        f"- full_logic_application: {payload['scope']['full_logic_application']}",
        f"- order_connection: {payload['scope']['order_connection']}",
        f"- history_writer_implemented: {payload['summary']['history_writer_implemented']}",
        f"- policy_change_applied_rows: {payload['summary']['policy_change_applied_rows']}",
        f"- trading_logic_changed_rows: {payload['summary']['trading_logic_changed_rows']}",
        f"- order_connected_rows: {payload['summary']['order_connected_rows']}",
        f"- next_real_work: {payload['summary']['next_real_work']}",
        "",
        "## Sample Plans",
    ]
    for plan in plans:
        lines.extend(
            [
                "",
                f"### {plan['sample_id']} {plan['sample_family']}",
                f"- source_event: {plan['source_event']}",
                f"- source_files: {plan['source_files']}",
                f"- unique_key: {plan['unique_key']}",
                f"- accumulation_timing: {plan['accumulation_timing']}",
                f"- duplicate_rule: {plan['duplicate_rule']}",
                f"- outcome_join: {plan['outcome_join']}",
                f"- ready_for_threshold_when: {plan['ready_for_threshold_when']}",
                f"- blocked_from_policy_when: {plan['blocked_from_policy_when']}",
                f"- implementation_status: {plan['implementation_status']}",
                f"- order_connection: {plan['order_connection']}",
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
