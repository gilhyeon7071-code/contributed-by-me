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
OUT_JSON = LOG_DIR / "surge_event_money_pullback_bridge_review_latest.json"
OUT_CSV = LOG_DIR / "surge_event_money_pullback_bridge_review_latest.csv"


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _float(value: Any) -> float:
    try:
        text = str(value).strip()
        if not text or text.lower() in {"nan", "<na>", "none"}:
            return 0.0
        return float(text)
    except Exception:
        return 0.0


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = "" if value is None else str(value).strip()
        if text and text.lower() not in {"nan", "<na>", "none"}:
            return text
    return ""


def _token_summary(text: str) -> tuple[str, str]:
    lower = text.lower()
    tokens = []
    if "adx" in lower:
        tokens.append("trend_strength_filter")
    if "disparity" in lower:
        tokens.append("extended_price_filter")
    if "high52" in lower:
        tokens.append("near_high_filter")
    if "stoch" in lower:
        tokens.append("stoch_filter")
    if "atr(" in lower:
        tokens.append("atr_volatility_filter")
    if "rs(" in lower:
        tokens.append("relative_strength_filter")
    if "v_accel" in lower:
        tokens.append("volume_acceleration_filter")
    return "|".join(tokens), "|".join(sorted(set(tokens)))


def _review_decision(row: dict[str, str]) -> tuple[str, str, str]:
    bucket = row.get("criteria_first_bucket", "")
    status = row.get("path_status", "")
    risk_score = _float(row.get("risk_score"))
    event_score = _float(row.get("event_score"))
    money_score = _float(row.get("money_score"))
    pullback_score = _float(row.get("pullback_score"))
    blocker = _first_nonempty(row.get("blocker_detail"), row.get("failed_filters"))
    _, token_set = _token_summary(blocker)

    if status == "VALID_RISK_OR_EXECUTION_BLOCK":
        return "KEEP_BLOCKED", "current_realtime_surge_hard_block_remains", "do_not_bridge"

    if risk_score > 0:
        return "KEEP_WATCH_ONLY", "risk_score_positive_requires_manual_observation_only", "do_not_bridge"

    if bucket == "A_BUYABLE_WATCH" and status == "BRIDGE_REVIEW_NEEDED":
        if event_score >= 2.0 and money_score >= 3.0 and pullback_score >= 2.0 and risk_score <= 0.75:
            return (
                "BRIDGE_SHADOW_CANDIDATE",
                "criteria_a_strong_but_general_filters_block_execution_pool",
                f"review_general_filters:{token_set}",
            )
        return "KEEP_WATCH_ONLY", "criteria_a_missing_second_stage_strength", "do_not_bridge"

    if status == "GENERAL_CANDIDATE_FILTER_BLOCK":
        return "KEEP_WATCH_ONLY", "not_a_criteria_a_bridge_case", f"general_filters:{token_set}"

    return "KEEP_WATCH_ONLY", "not_bridge_case", "do_not_bridge"


def main() -> int:
    rows = _read_csv(PATH_AUDIT_CSV)
    out_rows: list[dict[str, Any]] = []

    for row in rows:
        decision, reason, review_focus = _review_decision(row)
        token_text, token_set = _token_summary(_first_nonempty(row.get("blocker_detail"), row.get("failed_filters")))
        out_rows.append(
            {
                "code": row.get("code", ""),
                "name": row.get("name", ""),
                "bridge_decision": decision,
                "bridge_reason": reason,
                "review_focus": review_focus,
                "criteria_first_bucket": row.get("criteria_first_bucket", ""),
                "criteria_first_bucket_display": row.get("criteria_first_bucket_display", ""),
                "bucket_contract": row.get("bucket_contract", ""),
                "path_status": row.get("path_status", ""),
                "path_reason": row.get("path_reason", ""),
                "total_structure_score": row.get("total_structure_score", ""),
                "event_score": row.get("event_score", ""),
                "money_score": row.get("money_score", ""),
                "pullback_score": row.get("pullback_score", ""),
                "risk_score": row.get("risk_score", ""),
                "risk_evidence": row.get("risk_evidence", ""),
                "source_hits": row.get("source_hits", ""),
                "final_score": row.get("final_score", ""),
                "execution_pool": row.get("execution_pool", ""),
                "natural_pass": row.get("natural_pass", ""),
                "surge_entry_decision": row.get("surge_entry_decision", ""),
                "surge_entry_allowed": row.get("surge_entry_allowed", ""),
                "surge_entry_blocked": row.get("surge_entry_blocked", ""),
                "blocker_tokens": token_text,
                "blocker_token_set": token_set,
                "blocker_detail": _first_nonempty(row.get("blocker_detail"), row.get("failed_filters")),
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
            }
        )

    decision_counts = Counter(str(row["bridge_decision"]) for row in out_rows)
    focus_counts = Counter(str(row["review_focus"]) for row in out_rows)
    bridge_rows = [row for row in out_rows if row["bridge_decision"] == "BRIDGE_SHADOW_CANDIDATE"]

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
        },
        "input_rows": {
            "path_audit_rows": len(rows),
        },
        "decision_counts": dict(decision_counts),
        "review_focus_counts": dict(focus_counts),
        "bridge_shadow_codes": [row["code"] for row in bridge_rows],
        "rows": out_rows,
    }

    OUT_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    fieldnames = list(out_rows[0].keys()) if out_rows else [
        "code",
        "name",
        "bridge_decision",
        "bridge_reason",
        "review_focus",
    ]
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    print(json.dumps({"decision_counts": dict(decision_counts), "bridge_shadow_codes": summary["bridge_shadow_codes"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
