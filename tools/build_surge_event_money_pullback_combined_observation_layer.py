from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

COMBINED_SHADOW_CSV = LOG_DIR / "surge_event_money_pullback_combined_shadow_latest.csv"
OVERLAY_IMPACT_CSV = LOG_DIR / "surge_event_money_pullback_overlay_impact_latest.csv"
PENDING_STATUS_JSON = LOG_DIR / "pending_entry_status_latest.json"

OUT_JSON = LOG_DIR / "surge_event_money_pullback_combined_observation_layer_latest.json"
OUT_CSV = LOG_DIR / "surge_event_money_pullback_combined_observation_layer_latest.csv"


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


def _priority(state: str, audit_priority: str) -> tuple[int, str]:
    if state == "COMBINED_POLICY_REVIEW":
        return 1, "POLICY_REVIEW_ONLY"
    if state == "COMBINED_RECOVERY_WATCH":
        return 2, "HIGH_OBSERVATION"
    if audit_priority == "P1" or state == "COMBINED_ROUTE_AUDIT":
        return 2, "ROUTE_AUDIT"
    if state == "CURRENT_ENTRY_LAYER":
        return 3, "CURRENT_LOGIC_HANDLES"
    if state == "COMBINED_BLOCKED":
        return 4, "BLOCKED"
    return 5, "UNCHANGED_WATCH"


def _human_status(state: str) -> str:
    return {
        "COMBINED_POLICY_REVIEW": "보강 확인 후보: 정책 검토 전용",
        "COMBINED_RECOVERY_WATCH": "회복 관찰 후보",
        "COMBINED_ROUTE_AUDIT": "경로 감사 필요",
        "CURRENT_ENTRY_LAYER": "현재 로직 처리 중",
        "COMBINED_BLOCKED": "차단 유지",
        "COMBINED_WATCH_UNCHANGED": "관찰 유지",
        "COMBINED_WAIT_WATCH": "확인 대기",
        "COMBINED_DROP_WATCH": "관찰 제외",
    }.get(state, "변화 없음")


def main() -> int:
    combined_rows = _read_csv(COMBINED_SHADOW_CSV)
    overlay_by_code = _index(_read_csv(OVERLAY_IMPACT_CSV))
    pending_status = _read_json(PENDING_STATUS_JSON)

    out_rows: list[dict[str, Any]] = []
    for row in combined_rows:
        code = _code(row)
        overlay = overlay_by_code.get(code, {})
        state = row.get("combined_shadow_state", "")
        audit_priority = row.get("audit_priority", "")
        priority_rank, priority_label = _priority(state, audit_priority)
        out_rows.append(
            {
                "code": code,
                "name": row.get("name", ""),
                "combined_status": _human_status(state),
                "combined_shadow_state": state,
                "observation_priority_rank": priority_rank,
                "observation_priority_label": priority_label,
                "combined_shadow_action": row.get("combined_shadow_action", ""),
                "combined_shadow_reason": row.get("combined_shadow_reason", ""),
                "audit_priority": audit_priority,
                "audit_gap_type": row.get("audit_gap_type", ""),
                "audit_next_check": row.get("audit_next_check", ""),
                "overlay_impact": row.get("overlay_impact", ""),
                "current_path_status": row.get("current_path_status", ""),
                "current_path_reason": row.get("current_path_reason", ""),
                "bridge_decision": row.get("bridge_decision", ""),
                "reaction_label": row.get("reaction_label", ""),
                "missing_confirmations": overlay.get("missing_confirmations", ""),
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

    out_rows.sort(key=lambda r: (int(r["observation_priority_rank"]), str(r["code"])))

    state_counts = Counter(str(row["combined_shadow_state"]) for row in out_rows)
    priority_counts = Counter(str(row["observation_priority_label"]) for row in out_rows)
    trade_route_changes = sum(1 for row in out_rows if row["trade_route_change"])

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
            "combined_shadow_csv": str(COMBINED_SHADOW_CSV),
            "overlay_impact_csv": str(OVERLAY_IMPACT_CSV),
            "pending_status_json": str(PENDING_STATUS_JSON),
        },
        "input_rows": {
            "combined_shadow_rows": len(combined_rows),
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
        "observation_priority_counts": dict(priority_counts),
        "trade_route_changes": trade_route_changes,
        "rows": out_rows,
    }

    OUT_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    fieldnames = list(out_rows[0].keys()) if out_rows else ["code", "name", "combined_status"]
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    print(
        json.dumps(
            {
                "combined_shadow_state_counts": dict(state_counts),
                "observation_priority_counts": dict(priority_counts),
                "trade_route_changes": trade_route_changes,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
