from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

PATH_AUDIT_CSV = LOG_DIR / "surge_event_money_pullback_path_audit_latest.csv"
BRIDGE_REVIEW_CSV = LOG_DIR / "surge_event_money_pullback_bridge_review_latest.csv"
REACTION_CHECK_CSV = LOG_DIR / "surge_event_money_pullback_bridge_reaction_check_latest.csv"
PENDING_STATUS_JSON = LOG_DIR / "pending_entry_status_latest.json"

OUT_JSON = LOG_DIR / "surge_event_money_pullback_overlay_impact_latest.json"
OUT_CSV = LOG_DIR / "surge_event_money_pullback_overlay_impact_latest.csv"


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def _code(row: dict[str, Any]) -> str:
    value = str(row.get("code") or "").strip()
    return value.zfill(6) if value.isdigit() else value


def _index(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {_code(row): row for row in rows if _code(row)}


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = "" if value is None else str(value).strip()
        if text and text.lower() not in {"nan", "<na>", "none"}:
            return text
    return ""


def _impact(path_row: dict[str, str], bridge_row: dict[str, str], reaction_row: dict[str, str]) -> tuple[str, str, str]:
    path_status = path_row.get("path_status", "")
    bridge_decision = bridge_row.get("bridge_decision", "")
    reaction_label = reaction_row.get("reaction_label", "")
    entry_signal = _first_nonempty(path_row.get("entry_signal"))

    if entry_signal:
        return "NO_OVERLAY_CHANGE_ENTRY_LAYER_PRESENT", "current_logic_already_reached_entry_layer", "no_change"

    if path_status == "VALID_RISK_OR_EXECUTION_BLOCK" or bridge_decision == "KEEP_BLOCKED":
        return "NO_OVERLAY_CHANGE_KEEP_BLOCKED", "hard_or_execution_block_remains", "keep_blocked"

    if path_status == "SURGE_ALLOWED_NOT_IN_ENTRY_LAYER":
        return "CURRENT_LOGIC_GAP_SURGE_ALLOWED", "current_surge_allowed_but_entry_layer_absent", "audit_current_entry_path"

    if bridge_decision == "BRIDGE_SHADOW_CANDIDATE":
        if reaction_label == "BRIDGE_CONFIRM":
            return "OVERLAY_POLICY_REVIEW_CANDIDATE", "bridge_confirmed_but_not_auto_trade", "policy_review_only"
        if reaction_label == "BRIDGE_RECOVERING":
            return "OVERLAY_OBSERVATION_UPGRADE", "bridge_recovering_changes_watch_priority_only", "continue_intraday_observation"
        if reaction_label == "BRIDGE_REJECT":
            return "OVERLAY_REJECTS_SHADOW", "bridge_reaction_failed", "drop_shadow_watch"
        return "OVERLAY_WAIT_ONLY", "bridge_shadow_needs_confirmation", "wait_for_confirmation"

    if bridge_decision == "KEEP_WATCH_ONLY":
        return "NO_OVERLAY_CHANGE_KEEP_WATCH", "watch_only_remains_watch_only", "no_trade_change"

    return "NO_OVERLAY_CHANGE_UNCLASSIFIED", "no_overlay_rule_matched", "manual_review"


def _audit_quality(
    impact_label: str,
    overlay_action: str,
    path_row: dict[str, str],
    reaction_row: dict[str, str],
) -> tuple[str, str, str, str]:
    if impact_label == "CURRENT_LOGIC_GAP_SURGE_ALLOWED":
        return (
            "P1",
            "ENTRY_PATH_GAP",
            "surge_allowed_but_missing_entry_layer_row",
            "verify why surge allowed row did not become entry decision row",
        )
    if impact_label == "OVERLAY_POLICY_REVIEW_CANDIDATE":
        return (
            "P1",
            "BRIDGE_CONFIRMED_SHADOW",
            "confirmed bridge shadow without trade route change",
            "policy review only after repeated evidence; do not auto route",
        )
    if impact_label == "OVERLAY_OBSERVATION_UPGRADE":
        missing = _first_nonempty(reaction_row.get("missing_confirmations"), "missing_confirmation_detail")
        return (
            "P2",
            "OBSERVATION_PRIORITY",
            f"bridge recovering but still missing {missing}",
            "continue intraday observation until confirm or reject",
        )
    if impact_label == "OVERLAY_REJECTS_SHADOW":
        return (
            "P2",
            "SHADOW_REJECTED",
            "bridge shadow reaction failed",
            "drop from bridge watch unless new criteria-first signal appears",
        )
    if impact_label == "NO_OVERLAY_CHANGE_KEEP_BLOCKED":
        return (
            "P3",
            "BLOCK_VALIDATED",
            "hard or execution block remains valid",
            "keep blocked; no overlay escalation",
        )
    if impact_label == "NO_OVERLAY_CHANGE_KEEP_WATCH":
        return (
            "P4",
            "WATCH_UNCHANGED",
            "watch-only status unchanged",
            "no immediate audit action",
        )
    return (
        "P4",
        "NO_MATERIAL_CHANGE",
        f"overlay action={overlay_action}",
        "manual review only if repeated",
    )


def main() -> int:
    path_rows = _read_csv(PATH_AUDIT_CSV)
    bridge_by_code = _index(_read_csv(BRIDGE_REVIEW_CSV))
    reaction_by_code = _index(_read_csv(REACTION_CHECK_CSV))
    pending_status = _read_json(PENDING_STATUS_JSON)

    out_rows: list[dict[str, Any]] = []
    for path_row in path_rows:
        code = _code(path_row)
        bridge_row = bridge_by_code.get(code, {})
        reaction_row = reaction_by_code.get(code, {})
        impact_label, impact_reason, overlay_action = _impact(path_row, bridge_row, reaction_row)
        audit_priority, audit_gap_type, audit_quality_reason, audit_next_check = _audit_quality(
            impact_label,
            overlay_action,
            path_row,
            reaction_row,
        )
        out_rows.append(
            {
                "code": code,
                "name": path_row.get("name", ""),
                "audit_priority": audit_priority,
                "audit_gap_type": audit_gap_type,
                "audit_quality_reason": audit_quality_reason,
                "audit_next_check": audit_next_check,
                "current_path_status": path_row.get("path_status", ""),
                "current_path_reason": path_row.get("path_reason", ""),
                "current_entry_signal": path_row.get("entry_signal", ""),
                "current_entry_reason": path_row.get("entry_reason", ""),
                "bridge_decision": bridge_row.get("bridge_decision", ""),
                "reaction_label": reaction_row.get("reaction_label", ""),
                "missing_confirmations": reaction_row.get("missing_confirmations", ""),
                "overlay_impact": impact_label,
                "overlay_impact_reason": impact_reason,
                "overlay_action": overlay_action,
                "trade_route_change": False,
                "watch_priority_change": impact_label in {
                    "OVERLAY_OBSERVATION_UPGRADE",
                    "OVERLAY_POLICY_REVIEW_CANDIDATE",
                    "OVERLAY_REJECTS_SHADOW",
                    "CURRENT_LOGIC_GAP_SURGE_ALLOWED",
                },
                "criteria_first_bucket": path_row.get("criteria_first_bucket", ""),
                "criteria_first_bucket_display": path_row.get("criteria_first_bucket_display", ""),
                "bucket_contract": path_row.get("bucket_contract", ""),
                "total_structure_score": path_row.get("total_structure_score", ""),
                "event_score": path_row.get("event_score", ""),
                "money_score": path_row.get("money_score", ""),
                "pullback_score": path_row.get("pullback_score", ""),
                "risk_score": path_row.get("risk_score", ""),
                "blocker_detail": path_row.get("blocker_detail", ""),
                "policy_change": False,
                "entry_approval_changed": False,
                "paper_order_route": False,
                "broker_order_route": False,
                "trading_route": False,
                "connection_level": "SIGNAL_QUALITY_ONLY",
                "trading_connection": False,
                "signal_connection": False,
                "execution_connection": False,
                "connection_note": "Signal quality or observation only; not entry approval, order, fill, or ledger.",
                "research_only": True,
            }
        )

    impact_counts = Counter(str(row["overlay_impact"]) for row in out_rows)
    action_counts = Counter(str(row["overlay_action"]) for row in out_rows)
    priority_counts = Counter(str(row["audit_priority"]) for row in out_rows)
    gap_type_counts = Counter(str(row["audit_gap_type"]) for row in out_rows)
    trade_route_changes = sum(1 for row in out_rows if row["trade_route_change"])
    watch_priority_changes = sum(1 for row in out_rows if row["watch_priority_change"])

    summary = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "research_only": True,
        "policy_change": False,
        "entry_approval_changed": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "trading_route": False,
        "connection_level": "SIGNAL_QUALITY_ONLY",
        "trading_connection": False,
        "signal_connection": False,
        "execution_connection": False,
        "connection_note": "Signal quality or observation only; not entry approval, order, fill, or ledger.",
        "inputs": {
            "path_audit_csv": str(PATH_AUDIT_CSV),
            "bridge_review_csv": str(BRIDGE_REVIEW_CSV),
            "reaction_check_csv": str(REACTION_CHECK_CSV),
            "pending_status_json": str(PENDING_STATUS_JSON),
        },
        "input_rows": {
            "path_audit_rows": len(path_rows),
            "bridge_review_rows": len(bridge_by_code),
            "reaction_check_rows": len(reaction_by_code),
        },
        "pending_status_snapshot": {
            "generated_at": pending_status.get("generated_at", ""),
            "status": pending_status.get("status", ""),
            "entry_ready": pending_status.get("entry_ready", ""),
            "filled": pending_status.get("filled", ""),
            "candidates_after_caps": pending_status.get("candidates_after_caps", ""),
            "pending_queue_len_raw": pending_status.get("pending_queue_len_raw", ""),
        },
        "overlay_impact_counts": dict(impact_counts),
        "overlay_action_counts": dict(action_counts),
        "audit_priority_counts": dict(priority_counts),
        "audit_gap_type_counts": dict(gap_type_counts),
        "trade_route_changes": trade_route_changes,
        "watch_priority_changes": watch_priority_changes,
        "rows": out_rows,
    }

    OUT_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    fieldnames = list(out_rows[0].keys()) if out_rows else ["code", "name", "overlay_impact", "overlay_action"]
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    print(
        json.dumps(
            {
                "overlay_impact_counts": dict(impact_counts),
                "audit_priority_counts": dict(priority_counts),
                "trade_route_changes": trade_route_changes,
                "watch_priority_changes": watch_priority_changes,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
