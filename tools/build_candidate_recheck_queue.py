"""Persist next-action recheck items for the next active-response cycle.

This script turns `RECHECK_15M` and review-promotion actions into a durable
queue. It does not place orders or change trading gates. The next action queue
reader can bring due items back into the candidate action queue for re-evaluation.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

PLAN_JSON = LOG_DIR / "candidate_action_plan_latest.json"
RECHECK_JSON = LOG_DIR / "candidate_action_recheck_queue_latest.json"
RECHECK_CSV = LOG_DIR / "candidate_action_recheck_queue_latest.csv"

KST = dt.timezone(dt.timedelta(hours=9))
RECHECK_ACTIONS = {"RECHECK_15M", "PROMOTE_TO_RECHECK"}

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

NEWS_TOPIC_IMPACT_FIELDS = [
    "news_topic_present",
    "news_topic_candidate_name",
    "news_topic_key",
    "news_topic_label",
    "news_topic_state",
    "news_topic_direction",
    "news_topic_risk_state",
    "news_topic_confirm_level",
    "news_topic_candidate_effect",
    "news_topic_effect_reason",
    "news_topic_article_count",
    "news_topic_avg_strength",
    "news_topic_avg_confidence",
    "news_topic_representative_titles",
    "news_topic_representative_evidence",
]


def _now() -> dt.datetime:
    return dt.datetime.now(tz=KST)


def _ts(value: dt.datetime | None = None) -> str:
    return (value or _now()).isoformat(timespec="seconds")


def _parse_dt(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=KST)
        return parsed.astimezone(KST)
    except Exception:
        return None


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _code(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    if not text:
        return ""
    return text.zfill(6)[-6:]


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value).strip()
        if text == "":
            return float(default)
        return float(text)
    except Exception:
        return float(default)


def _row_key(row: Dict[str, Any]) -> str:
    return "|".join([
        str(row.get("next_action") or ""),
        _code(row.get("code")),
        str(row.get("action_reason") or row.get("reason") or "")[:120],
    ])


def _due_minutes(row: Dict[str, Any]) -> int:
    action = str(row.get("next_action") or "")
    if action == "PROMOTE_TO_RECHECK":
        return 0
    minutes = int(_f(row.get("recheck_after_minutes"), 0.0))
    return minutes if minutes > 0 else 15


def _merge_existing(existing_rows: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in existing_rows:
        if not isinstance(row, dict):
            continue
        key = _row_key(row)
        if key:
            row = dict(row)
            row["queue_key"] = key
            out[key] = dict(row)
    return out


def _status(row: Dict[str, Any], now: dt.datetime) -> str:
    if str(row.get("next_action") or "") not in RECHECK_ACTIONS:
        return "IGNORED"
    due_at = _parse_dt(row.get("due_at"))
    if due_at is None:
        return "SCHEDULED"
    if due_at <= now:
        return "DUE"
    return "SCHEDULED"


def _summarize(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    status_counts: Dict[str, int] = {}
    action_counts: Dict[str, int] = {}
    validation_present = 0
    validation_source_counts: Dict[str, int] = {}
    news_topic_present = 0
    news_topic_effect_counts: Dict[str, int] = {}
    for row in rows:
        status = str(row.get("recheck_status") or "")
        action = str(row.get("next_action") or "")
        status_counts[status] = status_counts.get(status, 0) + 1
        action_counts[action] = action_counts.get(action, 0) + 1
        if str(row.get("event_validation_present") or "").strip().lower() in {"1", "true", "yes", "y"}:
            validation_present += 1
        if str(row.get("news_topic_present") or "").strip().lower() in {"1", "true", "yes", "y"}:
            news_topic_present += 1
            for effect in str(row.get("news_topic_candidate_effect") or "").split("|"):
                effect = effect.strip()
                if effect:
                    news_topic_effect_counts[effect] = news_topic_effect_counts.get(effect, 0) + 1
        for src in str(row.get("event_validation_sources") or "").split("|"):
            src = src.strip()
            if src:
                validation_source_counts[src] = validation_source_counts.get(src, 0) + 1
    return {
        "status_counts": status_counts,
        "action_counts": action_counts,
        "event_validation_present_rows": validation_present,
        "event_validation_source_counts": validation_source_counts,
        "news_topic_present_rows": news_topic_present,
        "news_topic_effect_counts": news_topic_effect_counts,
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "recheck_status",
        "due_at",
        "scheduled_at",
        "next_action",
        "action_reason",
        "source",
        "action_state",
        "code",
        "name",
        "priority",
        "return_pct_since_first_seen",
        "review_bucket",
        "reason",
        "queue_key",
        *NEWS_TOPIC_IMPACT_FIELDS,
        *SIGNAL_VALIDATION_FIELDS,
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    now = _now()
    plan_doc = _read_json(PLAN_JSON)
    existing_doc = _read_json(RECHECK_JSON)
    plan_rows = plan_doc.get("rows") if isinstance(plan_doc.get("rows"), list) else []
    existing_rows = existing_doc.get("rows") if isinstance(existing_doc.get("rows"), list) else []
    merged = _merge_existing(existing_rows)

    for row in plan_rows:
        if not isinstance(row, dict):
            continue
        action = str(row.get("next_action") or "")
        if action not in RECHECK_ACTIONS:
            continue
        if str(row.get("source") or "") == "action_recheck_due":
            continue
        code = _code(row.get("code"))
        if not code:
            continue
        key = _row_key(row)
        base = merged.get(key, {})
        scheduled_at = str(base.get("scheduled_at") or _ts(now))
        scheduled_dt = _parse_dt(scheduled_at) or now
        due_at = str(base.get("due_at") or _ts(scheduled_dt + dt.timedelta(minutes=_due_minutes(row))))
        out = dict(row)
        out["queue_key"] = key
        out["scheduled_at"] = scheduled_at
        out["due_at"] = due_at
        out["recheck_status"] = _status(out, now)
        out["trading_allowed"] = False
        merged[key] = out

    rows = sorted(
        merged.values(),
        key=lambda r: (
            0 if str(r.get("recheck_status") or "") == "DUE" else 1,
            str(r.get("due_at") or ""),
            -_f(r.get("priority"), 0.0),
            str(r.get("code") or ""),
        ),
    )
    summary = _summarize(rows)
    payload = {
        "schema_version": "candidate_action_recheck_queue_v1",
        "generated_at": _ts(now),
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "source_plan": str(PLAN_JSON),
        "summary": {
            "rows": len(rows),
            **summary,
        },
        "artifacts": {
            "plan": str(PLAN_JSON),
            "json": str(RECHECK_JSON),
            "csv": str(RECHECK_CSV),
        },
        "rows": rows,
    }
    _write_json(RECHECK_JSON, payload)
    _write_csv(RECHECK_CSV, rows)
    print(json.dumps({
        "status": "OK",
        "rows": len(rows),
        "status_counts": summary["status_counts"],
        "out_json": str(RECHECK_JSON),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
