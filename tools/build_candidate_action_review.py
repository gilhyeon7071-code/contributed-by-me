"""Build a read-only review report from candidate follow-up observations.

The report highlights queued names that may deserve later policy review. It is
advisory only: no score, gate, order, fill, ledger, or trading policy is changed.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

FOLLOWUP_JSON = LOG_DIR / "candidate_action_followup_latest.json"
REVIEW_JSON = LOG_DIR / "candidate_action_review_latest.json"
REVIEW_CSV = LOG_DIR / "candidate_action_review_latest.csv"

MIN_MATURED_MOVE_PCT = 1.5
MIN_EARLY_MOVE_PCT = 3.0
MIN_OBSERVED_ROWS_FOR_ACTIONABLE = 1

SIGNAL_VALIDATION_FIELDS = [
    "event_validation_present",
    "event_validation_sources",
    "event_theme_decision",
    "event_theme_reason",
    "global_event_label",
    "beneficiary_grade",
    "smart_money_quality",
    "price_zone_label",
    "four_question_decision",
    "four_question_reason",
    "event_money_bucket",
    "event_money_bucket_display",
    "event_money_watch_subtype",
    "event_money_quality_label",
    "event_money_reason",
    "event_money_event_score",
    "event_money_money_score",
    "event_money_pullback_score",
    "event_money_risk_score",
    "pullback_methodology_status",
    "pullback_axis_score",
    "pullback_refined_watch",
    "fourq_role_fit_label",
    "fourq_forward_status",
    "fourq_forward_outcome",
    "fourq_forward_reason",
    "fourq_intraday_return",
    "shadow_news_score",
    "shadow_score_reason",
    "final_score_delta_sim",
    "final_score_shadow_sim",
    "rank_delta",
    "score_only_entry_effect",
    "news_multisource_signal_date8",
    "news_multisource_outcome_status",
    "news_multisource_fwd_1d_ret",
    "news_multisource_fwd_3d_ret",
    "news_multisource_fwd_5d_ret",
    "defense_action_shadow",
    "general_action_shadow",
    "surge_action_shadow",
    "would_block_general",
    "would_block_surge",
    "defense_signal_score",
    "defense_reasons",
]


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value).strip()
        if text == "":
            return float(default)
        return float(text)
    except Exception:
        return float(default)


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _review_bucket(row: Dict[str, Any]) -> str:
    state = str(row.get("action_state") or "")
    ret_pct = _f(row.get("return_pct_since_first_seen"), 0.0)
    matured = _bool(row.get("matured"))
    has_price = _bool(row.get("current_price_available"))

    if not has_price:
        return "NO_PRICE_OBSERVED"
    if state == "BLOCKED":
        if matured and ret_pct >= MIN_MATURED_MOVE_PCT:
            return "BLOCKED_MOVED_REVIEW"
        if (not matured) and ret_pct >= MIN_EARLY_MOVE_PCT:
            return "EARLY_BLOCKED_MOVE_WATCH"
        return "BLOCKED_NO_REVIEW"
    if state == "RECHECK":
        if matured and ret_pct >= MIN_MATURED_MOVE_PCT:
            return "RECHECK_MISSED_MOVE_CANDIDATE"
        if (not matured) and ret_pct >= MIN_EARLY_MOVE_PCT:
            return "EARLY_RECHECK_MOVE_WATCH"
        return "RECHECK_WAIT"
    if state == "WATCH":
        if matured and ret_pct >= MIN_MATURED_MOVE_PCT:
            return "WATCH_MISSED_MOVE_CANDIDATE"
        if (not matured) and ret_pct >= MIN_EARLY_MOVE_PCT:
            return "EARLY_WATCH_MOVE_WATCH"
        return "WATCH_WAIT"
    if state == "TRADABLE":
        return "TRADABLE_OBSERVED"
    return "UNKNOWN_STATE"


def _priority(row: Dict[str, Any], bucket: str) -> float:
    ret_pct = _f(row.get("return_pct_since_first_seen"), 0.0)
    queue_priority = _f(row.get("queue_priority"), 0.0)
    trading_value = _f(row.get("current_trading_value"), 0.0)
    score = ret_pct * 10.0 + queue_priority
    if "MISSED_MOVE" in bucket:
        score += 50.0
    if "BLOCKED_MOVED" in bucket:
        score += 30.0
    if "EARLY_" in bucket:
        score += 15.0
    if trading_value >= 1_000_000_000:
        score += 5.0
    return round(score, 6)


def _summarize(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    bucket_counts: Dict[str, int] = {}
    state_counts: Dict[str, int] = {}
    actionable = 0
    validation_present = 0
    validation_source_counts: Dict[str, int] = {}
    for row in rows:
        bucket = str(row.get("review_bucket") or "")
        state = str(row.get("action_state") or "")
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
        state_counts[state] = state_counts.get(state, 0) + 1
        if _bool(row.get("policy_review_candidate")):
            actionable += 1
        if _bool(row.get("event_validation_present")):
            validation_present += 1
        for src in str(row.get("event_validation_sources") or "").split("|"):
            src = src.strip()
            if src:
                validation_source_counts[src] = validation_source_counts.get(src, 0) + 1
    return {
        "bucket_counts": bucket_counts,
        "state_counts": state_counts,
        "policy_review_candidates": actionable,
        "event_validation_present_rows": validation_present,
        "event_validation_source_counts": validation_source_counts,
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "review_bucket",
        "policy_review_candidate",
        "review_priority",
        "source",
        "action_state",
        "code",
        "name",
        "return_pct_since_first_seen",
        "elapsed_minutes",
        "recheck_after_minutes",
        "matured",
        "current_price_available",
        "baseline_price",
        "current_price",
        "current_change_pct",
        "current_trading_value",
        "reason",
        "review_note",
        *SIGNAL_VALIDATION_FIELDS,
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    followup_doc = _read_json(FOLLOWUP_JSON)
    followup_rows = followup_doc.get("rows") if isinstance(followup_doc.get("rows"), list) else []
    review_rows: List[Dict[str, Any]] = []

    for row in followup_rows:
        if not isinstance(row, dict):
            continue
        bucket = _review_bucket(row)
        policy_candidate = bucket in {
            "RECHECK_MISSED_MOVE_CANDIDATE",
            "WATCH_MISSED_MOVE_CANDIDATE",
            "BLOCKED_MOVED_REVIEW",
        }
        early_watch = bucket.startswith("EARLY_")
        out = dict(row)
        out["review_bucket"] = bucket
        out["policy_review_candidate"] = bool(policy_candidate)
        out["early_move_watch"] = bool(early_watch)
        out["review_priority"] = _priority(row, bucket)
        out["review_note"] = (
            "policy_review_only_no_trading_effect"
            if policy_candidate
            else "observe_only_no_trading_effect"
        )
        out["trading_allowed"] = False
        review_rows.append(out)

    review_rows.sort(key=lambda r: _f(r.get("review_priority"), 0.0), reverse=True)
    summary = _summarize(review_rows)
    actionable_ready = summary["policy_review_candidates"] >= MIN_OBSERVED_ROWS_FOR_ACTIONABLE

    payload = {
        "generated_from": str(FOLLOWUP_JSON),
        "schema_version": "candidate_action_review_v1",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "thresholds": {
            "min_matured_move_pct": MIN_MATURED_MOVE_PCT,
            "min_early_move_pct": MIN_EARLY_MOVE_PCT,
        },
        "summary": {
            "followup_rows": len(followup_rows),
            "review_rows": len(review_rows),
            "actionable_ready": actionable_ready,
            **summary,
        },
        "artifacts": {
            "followup": str(FOLLOWUP_JSON),
            "json": str(REVIEW_JSON),
            "csv": str(REVIEW_CSV),
        },
        "rows": review_rows,
    }
    _write_json(REVIEW_JSON, payload)
    _write_csv(REVIEW_CSV, review_rows)
    print(json.dumps({
        "status": "OK",
        "rows": len(review_rows),
        "policy_review_candidates": summary["policy_review_candidates"],
        "actionable_ready": actionable_ready,
        "out_json": str(REVIEW_JSON),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
