from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SOURCE_CSV = LOG_DIR / "logic_role_inventory_audit_latest.csv"
OUT_JSON = LOG_DIR / "logic_role_batch_focus_audit_latest.json"
OUT_CSV = LOG_DIR / "logic_role_batch_focus_audit_latest.csv"
OUT_MD = LOG_DIR / "logic_role_batch_focus_audit_latest.md"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _refine(row: dict[str, str]) -> tuple[str, str, str]:
    name = str(row.get("name") or "").replace("\\\\", "\\").lower()
    role = str(row.get("role") or "")
    fail_mode = str(row.get("fail_mode") or "")

    if role == "ACTIVE_POLICY":
        if "paper_engine.py" in name:
            return "ACTIVE_POLICY_CORE_ENGINE", "paper_engine_entry_order_decision", "keep_fail_closed"
        if "gate_daily" in name or "p0_daily_check" in name:
            return "ACTIVE_POLICY_GATE", "p0_gate_controls_entry_permission", "keep_fail_closed_or_explicitly_audited"
        if "production_risk_playbook" in name:
            return "ACTIVE_POLICY_RISK", "risk_policy_affects_sizing_or_blocks", "keep_with_policy_metadata"
        if "defense_signal" in name:
            return "ACTIVE_POLICY_ENTRY_FILTER", "defense_signal_can_filter_entry_pool", "keep_but_track_block_delta"
        if "surge_active_response_layer" in name:
            return "ACTIVE_POLICY_SURGE_RESPONSE", "surge_response_may_affect_surge_handling", "manual_verify_effect_scope"
        return "ACTIVE_POLICY_REVIEW", "active_policy_name_requires_manual_scope_check", "manual_review"

    if "kis_order_dispatch" in name or "kis_cancel_open_orders" in name or "kis_sync_fills" in name:
        return "BROKER_CORE", "broker_order_or_fill_sync_path", "keep_fail_closed"
    if "ledger_append" in name or "repair_rootb" in name or "paper_sync.py" in name or "reconcile" in name:
        return "SSOT_CORE", "orders_fills_ledger_stats_chain", "keep_fail_closed_or_post_chain_controlled"
    if "ssot_" in name or "vibe_generate_stats" in name or "dashboard_point_today" in name:
        return "ROOTB_REPORT_SYNC", "rootb_dashboard_or_final_snapshot_sync", "keep_but_separate_from_trading_core"
    if "surge_detector_realtime" in name or "surge_lob_ingest" in name or "surge_ml_score_realtime" in name or "intraday_price_snapshot" in name or "fast_surge_sanity" in name:
        return "REALTIME_SURGE_CORE", "surge_realtime_detection_or_microstructure_input", "keep_if_intraday_consumer_depends_on_latest"
    if "news_" in name or "run_news_pipeline" in name or "signal_integration" in name or "final_score_merge" in name or "sector_score" in name:
        return "SIGNAL_ENRICHMENT", "news_sector_signal_enrichment", "prefer_fail_soft_unless_contract_breaks_core"
    if "check_signal_contract" in name or "validate_" in name or "integrity" in name or "resilience" in name or "calibration" in name:
        return "VALIDATION_GUARD", "validation_or_contract_guard", "fail_closed_only_for_core_contracts"
    if "report" in name or "summary" in name or "audit" in name or "monitor" in name or "diag" in name or "recommend" in name:
        return "REPORT_ONLY", "diagnostic_or_reporting_step", "manual_or_daily_summary_preferred"
    if "candidate_action" in name or "recheck" in name or "promoted" in name:
        return "OBSERVE_DECISION_SUPPORT", "candidate_review_or_recheck_support", "must_not_silently_change_entry"
    if "build_surge_universe" in name or "sync_candidates_meta" in name or "macro_signal" in name or "build_rate_series" in name:
        return "CORE_INPUT_PREP", "candidate_or_macro_input_preparation", "keep_required_if_downstream_depends"
    if fail_mode == "FAIL_CLOSED":
        return "FAIL_CLOSED_UNCLASSIFIED", "unclassified_but_fail_closed_in_batch", "manual_review_priority_high"
    return "LOW_PRIORITY_UNCLASSIFIED", "unclassified_fail_soft_or_unknown", "manual_review_priority_normal"


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "source_type",
        "name",
        "line",
        "original_role",
        "refined_role",
        "refined_reason",
        "recommended_handling",
        "fail_mode",
        "trading_effect",
        "entry_pool_filter_effect",
        "order_dispatch_effect",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Logic Role Batch Focus Audit",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- rows: `{payload['summary']['rows']}`",
        "",
        "## Refined Role Counts",
    ]
    for role, count in sorted(payload["summary"]["refined_role_counts"].items()):
        lines.append(f"- {role}: `{count}`")
    lines.extend(["", "## Priority Notes"])
    for note in payload["summary"]["priority_notes"]:
        lines.append(f"- {note}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    source_rows = _read_csv(SOURCE_CSV)
    target_rows = [
        row for row in source_rows
        if row.get("source_type") == "batch_step"
        and row.get("role") in {"ACTIVE_POLICY", "UNCLASSIFIED_REVIEW"}
    ]
    out_rows: list[dict[str, Any]] = []
    for row in target_rows:
        refined_role, reason, handling = _refine(row)
        out_rows.append(
            {
                "source_type": row.get("source_type"),
                "name": row.get("name"),
                "line": row.get("line"),
                "original_role": row.get("role"),
                "refined_role": refined_role,
                "refined_reason": reason,
                "recommended_handling": handling,
                "fail_mode": row.get("fail_mode"),
                "trading_effect": row.get("trading_effect"),
                "entry_pool_filter_effect": row.get("entry_pool_filter_effect"),
                "order_dispatch_effect": row.get("order_dispatch_effect"),
            }
        )

    counts = Counter(row["refined_role"] for row in out_rows)
    priority_notes = [
        "Do not retire BROKER_CORE, SSOT_CORE, ACTIVE_POLICY_CORE_ENGINE, ACTIVE_POLICY_GATE, or CORE_INPUT_PREP without separate E2E proof.",
        "REPORT_ONLY and LOW_PRIORITY_UNCLASSIFIED are candidates for manual or daily-summary execution, not immediate removal.",
        "SIGNAL_ENRICHMENT should stay fail-soft unless a downstream contract explicitly requires it.",
        "REALTIME_SURGE_CORE should be judged by intraday consumer dependency, not by dashboard usefulness.",
    ]
    payload = {
        "generated_at": _now(),
        "status": "PASS",
        "schema_version": "logic_role_batch_focus_audit_v1",
        "source_files": {"inventory_csv": str(SOURCE_CSV)},
        "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV), "md": str(OUT_MD)},
        "summary": {
            "rows": len(out_rows),
            "refined_role_counts": dict(counts),
            "priority_notes": priority_notes,
        },
        "rows_sample": out_rows[:50],
    }
    _write_csv(OUT_CSV, out_rows)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_md(OUT_MD, payload)
    print(json.dumps({"status": "PASS", "rows": len(out_rows), "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
