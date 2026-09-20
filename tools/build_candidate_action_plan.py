"""Build the candidate next-action contract for the active response flow.

This script does not place orders. It records the selected next action for each
candidate after discovery, context, review, and paper-engine execution artifacts
have been refreshed.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER_DIR = ROOT / "paper"

QUEUE_JSON = LOG_DIR / "candidate_action_queue_latest.json"
FOLLOWUP_JSON = LOG_DIR / "candidate_action_followup_latest.json"
REVIEW_JSON = LOG_DIR / "candidate_action_review_latest.json"
PENDING_JSON = LOG_DIR / "pending_entry_status_latest.json"
POST_ENTRY_JSON = LOG_DIR / "post_entry_learning_latest.json"
PLAN_JSON = LOG_DIR / "candidate_action_plan_latest.json"
PLAN_CSV = LOG_DIR / "candidate_action_plan_latest.csv"
FILLS_CSV = PAPER_DIR / "fills.csv"
CONFIG_PATH = PAPER_DIR / "paper_engine_config.json"
CANDIDATES_FINAL = LOG_DIR / "candidates_latest_data.with_final_score.csv"

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


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return [{str(k): str(v) for k, v in row.items()} for row in csv.DictReader(f)]
    except Exception:
        return []


def _code(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    if not text:
        return ""
    return text.zfill(6)[-6:]


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value).strip()
        if text == "":
            return float(default)
        return float(text)
    except Exception:
        return float(default)


def _note_map(note: Any) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for part in str(note or "").split(";"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def _current_buy_fills() -> Dict[str, Dict[str, Any]]:
    rows = _read_csv(FILLS_CSV)
    if not rows:
        return {}
    max_ymd = ""
    for row in rows:
        side = str(row.get("side") or row.get("action") or "").strip().upper()
        if side != "BUY":
            continue
        raw_dt = str(row.get("datetime") or row.get("ts") or row.get("date") or "")
        ymd = "".join(ch for ch in raw_dt if ch.isdigit())[:8]
        if ymd > max_ymd:
            max_ymd = ymd
    if not max_ymd:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        side = str(row.get("side") or row.get("action") or "").strip().upper()
        raw_dt = str(row.get("datetime") or row.get("ts") or row.get("date") or "")
        ymd = "".join(ch for ch in raw_dt if ch.isdigit())[:8]
        code = _code(row.get("code"))
        if side == "BUY" and ymd == max_ymd and code:
            note = _note_map(row.get("note"))
            out[code] = {
                "datetime": raw_dt,
                "order_id": str(row.get("order_id") or ""),
                "note": note,
                "surge_immediate": _bool(note.get("surge_immediate")),
                "entry_timing": str(note.get("entry_timing") or ""),
                "signal_ts": str(note.get("signal_ts") or ""),
            }
    return out


def _current_buy_codes() -> Set[str]:
    return set(_current_buy_fills())


def _load_candidate_map() -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in _read_csv(CANDIDATES_FINAL):
        code = _code(row.get("code"))
        if code:
            out[code] = row
    return out


def _sector_union_ok(candidate: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    pol = cfg.get("positive_entry_criteria", {}) if isinstance(cfg.get("positive_entry_criteria"), dict) else {}
    try:
        union_strength_min = float(cfg.get("union_entry_strength_min", 0.65) or 0.65)
    except Exception:
        union_strength_min = 0.65
    return bool(
        _f(candidate.get("final_score"), _f(candidate.get("score"), 0.0)) > _f(pol.get("min_score"), 0.0)
        and bool(pol.get("allow_sector_union", True))
        and str(candidate.get("candidate_origin") or "").strip().upper() == "SECTOR_PREFILTER_UNION"
        and str(candidate.get("sector_action") or "").strip().upper() == "BUY"
        and _bool(candidate.get("sector_entry_allowed"))
        and _f(candidate.get("sector_strength"), 0.0) >= union_strength_min
    )


def _fresh_sector_fallback_selected(
    code: str,
    candidates: Dict[str, Dict[str, Any]],
    cfg: Dict[str, Any],
    prior_buy_codes: Set[str],
) -> bool:
    pol = cfg.get("positive_entry_criteria", {}) if isinstance(cfg.get("positive_entry_criteria"), dict) else {}
    fallback = pol.get("fresh_sector_allowed_fallback", {}) if isinstance(pol.get("fresh_sector_allowed_fallback"), dict) else {}
    if not bool(fallback.get("enabled", False)):
        return False
    active = {c: r for c, r in candidates.items() if c not in prior_buy_codes}
    if any(_sector_union_ok(row, cfg) for row in active.values()):
        return False
    origin_required = str(fallback.get("candidate_origin", "SECTOR_PREFILTER_UNION") or "").strip().upper()
    actions_raw = fallback.get("allowed_sector_actions", ["BUY", "WAIT"])
    if not isinstance(actions_raw, list):
        actions_raw = ["BUY", "WAIT"]
    allowed_actions = {str(x).strip().upper() for x in actions_raw if str(x).strip()}
    min_score = _f(fallback.get("min_final_score"), 0.10)
    min_strength = _f(fallback.get("min_sector_strength"), 0.40)
    max_candidates = max(1, int(_f(fallback.get("max_candidates"), 1.0)))
    eligible: List[Tuple[str, float]] = []
    for cand_code, row in active.items():
        origin_ok = not origin_required or str(row.get("candidate_origin") or "").strip().upper() == origin_required
        action_ok = str(row.get("sector_action") or "").strip().upper() in allowed_actions
        if (
            origin_ok
            and action_ok
            and _bool(row.get("sector_entry_allowed"))
            and _f(row.get("sector_strength"), 0.0) >= min_strength
            and _f(row.get("final_score"), _f(row.get("score"), 0.0)) >= min_score
        ):
            eligible.append((cand_code, _f(row.get("final_score"), _f(row.get("score"), 0.0))))
    eligible.sort(key=lambda x: (-x[1], x[0]))
    return code in {cand_code for cand_code, _ in eligible[:max_candidates]}


def _fill_path_origins(buy_fills: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    cfg = _read_json(CONFIG_PATH)
    candidates = _load_candidate_map()
    prior_buy_codes: Set[str] = set()
    out: Dict[str, str] = {}
    ordered = sorted(buy_fills.items(), key=lambda item: str(item[1].get("datetime") or ""))
    for code, fill in ordered:
        if fill.get("surge_immediate"):
            out[code] = "SURGE_IMMEDIATE"
        elif code in candidates and _sector_union_ok(candidates[code], cfg):
            out[code] = "SECTOR_UNION_CONDITIONAL"
        elif _fresh_sector_fallback_selected(code, candidates, cfg, prior_buy_codes):
            out[code] = "FRESH_SECTOR_ALLOWED_FALLBACK"
        prior_buy_codes.add(code)
    return out


def _row_key(row: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        _code(row.get("code")),
        str(row.get("source") or ""),
        str(row.get("action_state") or ""),
    )


def _execution_match_rank(row: Dict[str, Any], fill: Dict[str, Any]) -> int:
    source = str(row.get("source") or "")
    state = str(row.get("action_state") or "")
    if fill.get("surge_immediate"):
        if source == "surge_realtime":
            return 0
        if source == "action_recheck_due":
            return 1
        if source == "market_rising":
            return 2
        if state == "TRADABLE":
            return 3
        return 4
    if source == "daily_candidate" and state == "TRADABLE":
        return 0
    if state == "TRADABLE":
        return 1
    if source == "daily_candidate":
        return 2
    if source == "action_recheck_due":
        return 3
    return 4


def _fill_provenance(row: Dict[str, Any], fill: Dict[str, Any], fill_path_origin: str = "") -> str:
    source = str(row.get("source") or "")
    state = str(row.get("action_state") or "")
    if fill.get("surge_immediate"):
        if source == "surge_realtime":
            return "SURGE_IMMEDIATE_SOURCE_MATCH"
        return "SURGE_IMMEDIATE_FILL_ROW_FALLBACK"
    if fill_path_origin == "SECTOR_UNION_CONDITIONAL":
        return "SECTOR_UNION_CONDITIONAL_MATCH"
    if fill_path_origin == "FRESH_SECTOR_ALLOWED_FALLBACK":
        return "FRESH_SECTOR_ALLOWED_FALLBACK_MATCH"
    if state == "TRADABLE":
        return "TRADABLE_ROW_MATCH"
    return "FILL_OBSERVED_NO_CURRENT_TRADABLE_ROW"


def _select_executed_buy_keys(
    queue_rows: Iterable[Dict[str, Any]],
    buy_fills: Dict[str, Dict[str, Any]],
) -> Set[Tuple[str, str, str]]:
    best: Dict[str, Tuple[int, Tuple[str, str, str]]] = {}
    for row in queue_rows:
        if not isinstance(row, dict):
            continue
        code = _code(row.get("code"))
        fill = buy_fills.get(code)
        if not fill:
            continue
        key = _row_key(row)
        rank = _execution_match_rank(row, fill)
        old = best.get(code)
        if old is None or rank < old[0]:
            best[code] = (rank, key)
    return {key for _, key in best.values()}


def _index_by_code(rows: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = _code(row.get("code"))
        if code and code not in out:
            out[code] = row
    return out


def _index_by_row_key(rows: Iterable[Dict[str, Any]]) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
    out: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = _row_key(row)
        if key[0] and key not in out:
            out[key] = row
    return out


def _load_post_entry_feedback() -> tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    doc = _read_json(POST_ENTRY_JSON)
    rows = doc.get("rows") if isinstance(doc.get("rows"), list) else []
    return _index_by_code(rows), doc.get("summary") if isinstance(doc.get("summary"), dict) else {}


def _post_entry_feedback(
    code: str,
    next_action: str,
    post_by_code: Dict[str, Dict[str, Any]],
    post_summary: Dict[str, Any],
) -> Dict[str, Any]:
    row = post_by_code.get(code, {})
    bucket = str(row.get("learning_bucket") or "")
    origin = str(row.get("entry_origin") or "")
    ret_pct = _f(row.get("unrealized_return_pct"), 0.0)
    open_qty = int(_f(row.get("open_qty"), 0.0))

    out = {
        "learning_feedback": "NONE",
        "learning_feedback_reason": "",
        "learning_feedback_strength": "NONE",
        "learning_feedback_action": "",
        "learning_feedback_score_multiplier": 1.0,
        "post_entry_bucket": bucket,
        "post_entry_learning_cause": str(row.get("learning_cause") or ""),
        "post_entry_learning_policy_action": str(row.get("learning_policy_action") or ""),
        "post_entry_origin": origin,
        "post_entry_return_pct": ret_pct,
        "post_entry_open_qty": open_qty,
    }

    if "FULL_DDM_LIQUIDATED" in bucket or "FULL_EXIT" in bucket:
        out.update({
            "learning_feedback": "SAME_DAY_FULL_EXIT_FEEDBACK",
            "learning_feedback_reason": str(row.get("learning_cause") or "post_entry_full_exit"),
            "learning_feedback_strength": "REVIEW",
            "learning_feedback_action": "NO_SAME_DAY_REENTRY_WITHOUT_NEW_SIGNAL",
            "learning_feedback_score_multiplier": 0.70,
        })
        return out

    if "PARTIAL_DDM_LIQUIDATED" in bucket or "PARTIAL_EXIT" in bucket:
        out.update({
            "learning_feedback": "PARTIAL_EXIT_FEEDBACK",
            "learning_feedback_reason": str(row.get("learning_cause") or "post_entry_partial_exit"),
            "learning_feedback_strength": "TIGHTEN",
            "learning_feedback_action": "TIGHTEN_CONTINUATION_RECHECK",
            "learning_feedback_score_multiplier": 0.85,
        })
        return out

    if bucket.endswith("_NEGATIVE"):
        out.update({
            "learning_feedback": "ENTRY_NEGATIVE_FEEDBACK",
            "learning_feedback_reason": f"post_entry_return_pct={ret_pct:.4f}",
            "learning_feedback_strength": "TIGHTEN",
            "learning_feedback_action": "TIGHTEN_RECHECK_PROMOTION",
            "learning_feedback_score_multiplier": 0.80,
        })
        return out

    if bucket.endswith("_POSITIVE"):
        out.update({
            "learning_feedback": "ENTRY_POSITIVE_FEEDBACK",
            "learning_feedback_reason": f"post_entry_return_pct={ret_pct:.4f}",
            "learning_feedback_strength": "SUPPORT",
            "learning_feedback_action": "ALLOW_CONTINUATION_RECHECK",
            "learning_feedback_score_multiplier": 1.10,
        })
        return out

    if next_action == "PROMOTE_TO_RECHECK":
        origin_counts = post_summary.get("origin_counts") if isinstance(post_summary.get("origin_counts"), dict) else {}
        bucket_counts = post_summary.get("bucket_counts") if isinstance(post_summary.get("bucket_counts"), dict) else {}
        promoted_entries = int(_f(origin_counts.get("ACTIVE_RECHECK_PROMOTION"), 0.0))
        promoted_negative = sum(
            int(_f(count, 0.0))
            for key, count in bucket_counts.items()
            if str(key).startswith("PROMOTED_") and str(key).endswith("_NEGATIVE")
        )
        promoted_partial_exit = sum(
            int(_f(count, 0.0))
            for key, count in bucket_counts.items()
            if str(key).startswith("PROMOTED_") and "PARTIAL" in str(key)
        )
        promoted_risk = promoted_negative + promoted_partial_exit
        if promoted_entries > 0 and promoted_risk > 0:
            out.update({
                "learning_feedback": "PROMOTION_SESSION_RISK_FEEDBACK",
                "learning_feedback_reason": f"promoted_risk={promoted_risk}/{promoted_entries}",
                "learning_feedback_strength": "TIGHTEN",
                "learning_feedback_action": "PROMOTE_WITH_SCORE_HAIRCUT",
                "learning_feedback_score_multiplier": 0.90,
            })
    return out


def _next_action(
    row: Dict[str, Any],
    review: Dict[str, Any],
    buy_fills: Dict[str, Dict[str, Any]],
    executed_buy_keys: Set[Tuple[str, str, str]],
    fill_path_origins: Dict[str, str],
    pending: Dict[str, Any],
) -> Dict[str, Any]:
    code = _code(row.get("code"))
    state = str(row.get("action_state") or "")
    reason = str(row.get("reason") or "")
    review_bucket = str(review.get("review_bucket") or "")

    fill = buy_fills.get(code)
    if fill and _row_key(row) in executed_buy_keys:
        return {
            "next_action": "EXECUTED_BUY",
            "action_reason": "buy_fill_observed_today",
            "action_strength": "DONE",
            "fill_observed_today": True,
            "fill_order_id": fill.get("order_id", ""),
            "fill_entry_timing": fill.get("entry_timing", ""),
            "fill_signal_ts": fill.get("signal_ts", ""),
            "fill_surge_immediate": bool(fill.get("surge_immediate")),
            "fill_path_origin": fill_path_origins.get(code, ""),
            "fill_provenance": _fill_provenance(row, fill, fill_path_origins.get(code, "")),
        }
    if state == "TRADABLE":
        max_new = int(_f(pending.get("max_new"), 0.0))
        if max_new > 0:
            return {
                "next_action": "BUY_CANDIDATE",
                "action_reason": "tradable_candidate_waiting_execution",
                "action_strength": "ACTIVE",
            }
        return {
            "next_action": "QUEUE_UNTIL_CAP_OPENS",
            "action_reason": "tradable_but_max_new_zero_or_cap_closed",
            "action_strength": "WAIT",
        }
    if review_bucket in {"RECHECK_MISSED_MOVE_CANDIDATE", "WATCH_MISSED_MOVE_CANDIDATE"}:
        return {
            "next_action": "PROMOTE_TO_RECHECK",
            "action_reason": review_bucket,
            "action_strength": "REVIEW",
        }
    if review_bucket == "BLOCKED_MOVED_REVIEW":
        return {
            "next_action": "REVIEW_BLOCK_RULE",
            "action_reason": review_bucket,
            "action_strength": "REVIEW",
        }
    if str(review_bucket).startswith("EARLY_"):
        return {
            "next_action": "RECHECK_15M",
            "action_reason": review_bucket,
            "action_strength": "WATCH",
        }
    if state == "RECHECK":
        return {
            "next_action": "RECHECK_15M",
            "action_reason": reason or "queued_recheck",
            "action_strength": "WATCH",
        }
    if state == "WATCH":
        return {
            "next_action": "WATCH_ONLY",
            "action_reason": reason or "queued_watch",
            "action_strength": "WATCH",
        }
    if state == "BLOCKED":
        return {
            "next_action": "BLOCK_HARD_OR_POLICY",
            "action_reason": reason or "queued_blocked",
            "action_strength": "BLOCK",
        }
    return {
        "next_action": "LEARN_ONLY",
        "action_reason": "unknown_state",
        "action_strength": "LEARN",
    }


def _summarize(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    action_counts: Dict[str, int] = {}
    strength_counts: Dict[str, int] = {}
    active_response_label_counts: Dict[str, int] = {}
    surge_after_state_counts: Dict[str, int] = {}
    validation_present = 0
    validation_source_counts: Dict[str, int] = {}
    event_theme_decision_counts: Dict[str, int] = {}
    event_money_bucket_counts: Dict[str, int] = {}
    defense_action_counts: Dict[str, int] = {}
    news_topic_present = 0
    news_topic_effect_counts: Dict[str, int] = {}
    for row in rows:
        action = str(row.get("next_action") or "")
        strength = str(row.get("action_strength") or "")
        action_counts[action] = action_counts.get(action, 0) + 1
        strength_counts[strength] = strength_counts.get(strength, 0) + 1
        active_label = str(row.get("active_response_label") or "").strip()
        if active_label:
            active_response_label_counts[active_label] = active_response_label_counts.get(active_label, 0) + 1
        surge_after_state = str(row.get("surge_after_state") or "").strip()
        if surge_after_state:
            surge_after_state_counts[surge_after_state] = surge_after_state_counts.get(surge_after_state, 0) + 1
        if _bool(row.get("event_validation_present")):
            validation_present += 1
        if _bool(row.get("news_topic_present")):
            news_topic_present += 1
            for effect in str(row.get("news_topic_candidate_effect") or "").split("|"):
                effect = effect.strip()
                if effect:
                    news_topic_effect_counts[effect] = news_topic_effect_counts.get(effect, 0) + 1
        for src in str(row.get("event_validation_sources") or "").split("|"):
            src = src.strip()
            if src:
                validation_source_counts[src] = validation_source_counts.get(src, 0) + 1
        event_decision = str(row.get("event_theme_decision") or "").strip()
        if event_decision:
            event_theme_decision_counts[event_decision] = event_theme_decision_counts.get(event_decision, 0) + 1
        event_bucket = str(row.get("event_money_bucket") or "").strip()
        if event_bucket:
            event_money_bucket_counts[event_bucket] = event_money_bucket_counts.get(event_bucket, 0) + 1
        defense_action = str(row.get("defense_action_shadow") or "").strip()
        if defense_action:
            defense_action_counts[defense_action] = defense_action_counts.get(defense_action, 0) + 1
    return {
        "action_counts": action_counts,
        "strength_counts": strength_counts,
        "active_response_label_counts": active_response_label_counts,
        "surge_after_state_counts": surge_after_state_counts,
        "event_validation_present_rows": validation_present,
        "event_validation_source_counts": validation_source_counts,
        "event_theme_decision_counts": event_theme_decision_counts,
        "event_money_bucket_counts": event_money_bucket_counts,
        "defense_action_counts": defense_action_counts,
        "news_topic_present_rows": news_topic_present,
        "news_topic_effect_counts": news_topic_effect_counts,
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "next_action",
        "action_strength",
        "action_reason",
        "source",
        "action_state",
        "code",
        "name",
        "active_response_label",
        "active_response_reason",
        "active_response_next_check",
        "surge_after_state",
        "review_bucket",
        "return_pct_since_first_seen",
        "current_price_available",
        "post_entry_bucket",
        "post_entry_learning_cause",
        "post_entry_learning_policy_action",
        "learning_feedback",
        "learning_feedback_action",
        "learning_feedback_score_multiplier",
        "reason",
        "trading_allowed",
        "fill_observed_today",
        "fill_order_id",
        "fill_entry_timing",
        "fill_signal_ts",
        "fill_surge_immediate",
        "fill_path_origin",
        "fill_provenance",
        *NEWS_TOPIC_IMPACT_FIELDS,
        *SIGNAL_VALIDATION_FIELDS,
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    queue_doc = _read_json(QUEUE_JSON)
    followup_doc = _read_json(FOLLOWUP_JSON)
    review_doc = _read_json(REVIEW_JSON)
    pending_doc = _read_json(PENDING_JSON)

    queue_rows = queue_doc.get("rows") if isinstance(queue_doc.get("rows"), list) else []
    followup_rows = followup_doc.get("rows") if isinstance(followup_doc.get("rows"), list) else []
    review_rows = review_doc.get("rows") if isinstance(review_doc.get("rows"), list) else []

    followup_by_code = _index_by_code(followup_rows)
    review_by_code = _index_by_code(review_rows)
    review_by_row_key = _index_by_row_key(review_rows)
    post_by_code, post_summary = _load_post_entry_feedback()
    buy_fills = _current_buy_fills()
    bought_today = set(buy_fills)
    fill_path_origins = _fill_path_origins(buy_fills)
    executed_buy_keys = _select_executed_buy_keys(queue_rows, buy_fills)

    rows: List[Dict[str, Any]] = []
    for qrow in queue_rows:
        if not isinstance(qrow, dict):
            continue
        code = _code(qrow.get("code"))
        if not code:
            continue
        merged = dict(qrow)
        if code in followup_by_code:
            merged.update({
                "return_pct_since_first_seen": followup_by_code[code].get("return_pct_since_first_seen"),
                "current_price_available": followup_by_code[code].get("current_price_available"),
                "current_price": followup_by_code[code].get("current_price"),
                "current_change_pct": followup_by_code[code].get("current_change_pct"),
                "current_trading_value": followup_by_code[code].get("current_trading_value"),
            })
        review = review_by_row_key.get(_row_key(qrow)) or review_by_code.get(code, {})
        if review:
            merged["review_bucket"] = review.get("review_bucket")
        merged.update(_next_action(merged, review, buy_fills, executed_buy_keys, fill_path_origins, pending_doc))
        if code in buy_fills and "fill_observed_today" not in merged:
            merged["fill_observed_today"] = True
            merged["fill_order_id"] = buy_fills[code].get("order_id", "")
            merged["fill_entry_timing"] = buy_fills[code].get("entry_timing", "")
            merged["fill_signal_ts"] = buy_fills[code].get("signal_ts", "")
            merged["fill_surge_immediate"] = bool(buy_fills[code].get("surge_immediate"))
            merged["fill_path_origin"] = fill_path_origins.get(code, "")
            merged["fill_provenance"] = "SAME_CODE_NON_EXECUTION_ROW"
        merged.update(_post_entry_feedback(
            code,
            str(merged.get("next_action") or ""),
            post_by_code,
            post_summary,
        ))
        merged["trading_allowed"] = bool(_bool(qrow.get("trading_allowed")) or merged.get("next_action") == "BUY_CANDIDATE")
        rows.append(merged)

    summary = _summarize(rows)
    payload = {
        "schema_version": "candidate_action_plan_v1",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "execution_bridge": "paper_engine",
        "summary": {
            "queue_rows": len(queue_rows),
            "plan_rows": len(rows),
            "bought_today_codes": sorted(bought_today),
            **summary,
        },
        "artifacts": {
            "queue": str(QUEUE_JSON),
            "followup": str(FOLLOWUP_JSON),
            "review": str(REVIEW_JSON),
            "pending": str(PENDING_JSON),
            "post_entry_learning": str(POST_ENTRY_JSON),
            "fills": str(FILLS_CSV),
            "json": str(PLAN_JSON),
            "csv": str(PLAN_CSV),
        },
        "rows": rows,
    }
    _write_json(PLAN_JSON, payload)
    _write_csv(PLAN_CSV, rows)
    print(json.dumps({
        "status": "OK",
        "rows": len(rows),
        "action_counts": summary["action_counts"],
        "bought_today_codes": sorted(bought_today),
        "out_json": str(PLAN_JSON),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
