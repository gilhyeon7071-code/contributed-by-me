from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

OVERLAY_IMPACT_CSV = LOG_DIR / "surge_event_money_pullback_overlay_impact_latest.csv"
PENDING_STATUS_JSON = LOG_DIR / "pending_entry_status_latest.json"

OUT_JSON = LOG_DIR / "surge_event_money_pullback_combined_shadow_latest.json"
OUT_CSV = LOG_DIR / "surge_event_money_pullback_combined_shadow_latest.csv"


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


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = "" if value is None else str(value).strip()
        if text and text.lower() not in {"nan", "<na>", "none"}:
            return text
    return ""


def _combined_state(row: dict[str, str]) -> tuple[str, str, str]:
    current_path = row.get("current_path_status", "")
    bridge_decision = row.get("bridge_decision", "")
    reaction_label = row.get("reaction_label", "")
    overlay_impact = row.get("overlay_impact", "")
    audit_priority = row.get("audit_priority", "")

    if current_path == "REACHED_ENTRY_LAYER":
        return "CURRENT_ENTRY_LAYER", "existing_logic_reached_entry_layer", "current_logic_controls"

    if overlay_impact == "NO_OVERLAY_CHANGE_KEEP_BLOCKED":
        return "COMBINED_BLOCKED", "existing_risk_or_execution_block_preserved", "keep_blocked"

    if bridge_decision == "BRIDGE_SHADOW_CANDIDATE":
        if reaction_label == "BRIDGE_CONFIRM":
            return "COMBINED_POLICY_REVIEW", "new_overlay_confirmed_shadow_but_no_auto_trade", "policy_review_only"
        if reaction_label == "BRIDGE_RECOVERING":
            return "COMBINED_RECOVERY_WATCH", "new_overlay_upgrades_observation_priority", "watch_until_confirm_or_reject"
        if reaction_label == "BRIDGE_REJECT":
            return "COMBINED_DROP_WATCH", "new_overlay_rejects_shadow_candidate", "drop_shadow_watch"
        return "COMBINED_WAIT_WATCH", "new_overlay_shadow_waiting_confirmation", "wait"

    if audit_priority == "P1":
        return "COMBINED_ROUTE_AUDIT", "overlay_found_current_logic_route_gap", "audit_route"

    if overlay_impact == "NO_OVERLAY_CHANGE_KEEP_WATCH":
        return "COMBINED_WATCH_UNCHANGED", "new_overlay_does_not_change_watch_state", "no_change"

    return "COMBINED_NO_MATERIAL_CHANGE", "no_combined_shadow_change", "no_change"


def main() -> int:
    overlay_rows = _read_csv(OVERLAY_IMPACT_CSV)
    pending_status = _read_json(PENDING_STATUS_JSON)

    out_rows: list[dict[str, Any]] = []
    for row in overlay_rows:
        state, reason, action = _combined_state(row)
        out_rows.append(
            {
                "code": row.get("code", ""),
                "name": row.get("name", ""),
                "combined_shadow_state": state,
                "combined_shadow_reason": reason,
                "combined_shadow_action": action,
                "current_path_status": row.get("current_path_status", ""),
                "current_path_reason": row.get("current_path_reason", ""),
                "bridge_decision": row.get("bridge_decision", ""),
                "reaction_label": row.get("reaction_label", ""),
                "overlay_impact": row.get("overlay_impact", ""),
                "audit_priority": row.get("audit_priority", ""),
                "audit_gap_type": row.get("audit_gap_type", ""),
                "audit_next_check": row.get("audit_next_check", ""),
                "current_entry_signal": row.get("current_entry_signal", ""),
                "current_entry_reason": row.get("current_entry_reason", ""),
                "criteria_first_bucket": row.get("criteria_first_bucket", ""),
                "criteria_first_bucket_display": row.get("criteria_first_bucket_display", ""),
                "bucket_contract": row.get("bucket_contract", ""),
                "event_score": row.get("event_score", ""),
                "money_score": row.get("money_score", ""),
                "pullback_score": row.get("pullback_score", ""),
                "risk_score": row.get("risk_score", ""),
                "trade_route_change": False,
                "entry_approval_changed": False,
                "policy_change": False,
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

    state_counts = Counter(str(row["combined_shadow_state"]) for row in out_rows)
    action_counts = Counter(str(row["combined_shadow_action"]) for row in out_rows)
    current_entry_rows = sum(1 for row in out_rows if row["combined_shadow_state"] == "CURRENT_ENTRY_LAYER")
    new_recovery_watch_rows = sum(1 for row in out_rows if row["combined_shadow_state"] == "COMBINED_RECOVERY_WATCH")
    combined_trade_route_changes = sum(1 for row in out_rows if row["trade_route_change"])

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
            "overlay_impact_csv": str(OVERLAY_IMPACT_CSV),
            "pending_status_json": str(PENDING_STATUS_JSON),
        },
        "input_rows": {
            "overlay_rows": len(overlay_rows),
        },
        "pending_status_snapshot": {
            "generated_at": pending_status.get("generated_at", ""),
            "status": pending_status.get("status", ""),
            "entry_ready": pending_status.get("entry_ready", ""),
            "filled": pending_status.get("filled", ""),
            "candidates_after_caps": pending_status.get("candidates_after_caps", ""),
            "pending_queue_len_raw": pending_status.get("pending_queue_len_raw", ""),
        },
        "combined_shadow_state_counts": dict(state_counts),
        "combined_shadow_action_counts": dict(action_counts),
        "current_entry_rows": current_entry_rows,
        "new_recovery_watch_rows": new_recovery_watch_rows,
        "combined_trade_route_changes": combined_trade_route_changes,
        "rows": out_rows,
    }

    OUT_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    fieldnames = list(out_rows[0].keys()) if out_rows else ["code", "name", "combined_shadow_state"]
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    print(
        json.dumps(
            {
                "combined_shadow_state_counts": dict(state_counts),
                "current_entry_rows": current_entry_rows,
                "new_recovery_watch_rows": new_recovery_watch_rows,
                "combined_trade_route_changes": combined_trade_route_changes,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
