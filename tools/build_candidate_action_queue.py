"""Build a read-only candidate action queue from current RootA artifacts.

This observer does not create orders and does not change scores, gates, or
trading policy. It keeps blocked or filtered names visible as watch/recheck
evidence so a no-buy day is still analyzable.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CONFIG_PATH = ROOT / "paper" / "paper_engine_config.json"

OUT_JSON = LOG_DIR / "candidate_action_queue_latest.json"
OUT_CSV = LOG_DIR / "candidate_action_queue_latest.csv"
RECHECK_QUEUE_JSON = LOG_DIR / "candidate_action_recheck_queue_latest.json"
SCORE_RVOL_CONDITIONAL_JSON = LOG_DIR / "surge_score_rvol_conditional_recheck_queue_latest.json"
NO_LOB_RECHECK_CSV = LOG_DIR / "surge_no_lob_recheck_queue_latest.csv"
SURGE_INTENT_SPLIT_CSV = LOG_DIR / "surge_probe_intent_split_latest.csv"
SURGE_ACTIVE_RESPONSE_CSV = LOG_DIR / "surge_active_response_layer_latest.csv"
SURGE_ACTIVE_RESPONSE_JSON = LOG_DIR / "surge_active_response_layer_latest.json"
SURGE_EVENT_MONEY_PULLBACK_CSV = LOG_DIR / "surge_event_money_pullback_screener_latest.csv"
EVENT_THEME_CSV = LOG_DIR / "event_theme_candidate_layer_latest.csv"
PULLBACK_METHODOLOGY_CSV = LOG_DIR / "pullback_methodology_first_screener_latest.csv"
DEFENSE_SIGNAL_CSV = LOG_DIR / "defense_signal_shadow_latest.csv"
FOUR_Q_FORWARD_CSV = LOG_DIR / "four_question_role_fit_forward_validation_latest.csv"
NEWS_MULTISOURCE_IMPACT_CSV = LOG_DIR / "news_multisource_shadow_score_impact_sim_latest.csv"
NEWS_MULTISOURCE_HISTORY_CSV = LOG_DIR / "news_multisource_shadow_score_history.csv"
NEWS_TOPIC_CANDIDATE_IMPACT_CSV = LOG_DIR / "news_topic_candidate_impact_latest.csv"

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


def _now_kst() -> str:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).isoformat(timespec="seconds")


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _read_csv(path: Path, limit: int | None = None) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    rows: List[Dict[str, str]] = []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                rows.append({str(k): str(v) for k, v in row.items()})
                if limit is not None and len(rows) >= int(limit):
                    break
    except Exception:
        return []
    return rows


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value).strip()
        if text == "":
            return float(default)
        return float(text)
    except Exception:
        return float(default)


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _clean_code(row: Dict[str, Any]) -> str:
    return str(row.get("code") or row.get("stck_shrn_iscd") or "").strip().zfill(6)[-6:]


def _name(row: Dict[str, Any]) -> str:
    return str(row.get("name") or row.get("hts_kor_isnm") or "").strip()


def _intent_by_code(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        code = _clean_code(row)
        if code:
            out[code] = row
    return out


def _active_response_by_code(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        code = _clean_code(row)
        if code:
            out[code] = row
    return out


def _active_response_counts(rows: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows:
        label = str(row.get("active_response_label") or "").strip()
        if not label:
            continue
        counts[label] = int(counts.get(label, 0)) + 1
    return counts


def _index_by_code(rows: Iterable[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        code = _clean_code(row)
        if code and code not in out:
            out[code] = row
    return out


def _copy_signal_fields(dst: Dict[str, Any], src: Dict[str, str], mapping: Dict[str, str], sources: List[str], source_name: str) -> None:
    if not src:
        return
    sources.append(source_name)
    for out_key, in_key in mapping.items():
        value = src.get(in_key, "")
        if str(value).strip() != "":
            dst[out_key] = value


def _load_signal_validation_by_code() -> tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    event_theme = _index_by_code(_read_csv(EVENT_THEME_CSV))
    event_money = _index_by_code(_read_csv(SURGE_EVENT_MONEY_PULLBACK_CSV))
    pullback = _index_by_code(_read_csv(PULLBACK_METHODOLOGY_CSV))
    defense = _index_by_code(_read_csv(DEFENSE_SIGNAL_CSV))
    fourq_forward = _index_by_code(_read_csv(FOUR_Q_FORWARD_CSV))
    news_impact = _index_by_code(_read_csv(NEWS_MULTISOURCE_IMPACT_CSV))
    news_history = _index_by_code(_read_csv(NEWS_MULTISOURCE_HISTORY_CSV))
    codes = set(event_theme) | set(event_money) | set(pullback) | set(defense) | set(fourq_forward) | set(news_impact) | set(news_history)
    out: Dict[str, Dict[str, Any]] = {}
    for code in sorted(codes):
        sources: List[str] = []
        row: Dict[str, Any] = {}
        _copy_signal_fields(
            row,
            event_theme.get(code, {}),
            {
                "event_theme_decision": "event_theme_decision",
                "event_theme_reason": "decision_reason",
                "global_event_label": "global_event_label",
                "beneficiary_grade": "beneficiary_grade",
                "smart_money_quality": "smart_money_quality",
                "price_zone_label": "price_zone_label",
                "four_question_decision": "four_question_decision",
                "four_question_reason": "four_question_reason",
            },
            sources,
            "event_theme",
        )
        _copy_signal_fields(
            row,
            event_money.get(code, {}),
            {
                "event_money_bucket": "criteria_first_bucket",
                "event_money_bucket_display": "criteria_first_bucket_display",
                "event_money_watch_subtype": "watch_subtype",
                "event_money_quality_label": "quality_label",
                "event_money_reason": "bucket_reason",
                "event_money_event_score": "event_score",
                "event_money_money_score": "money_score",
                "event_money_pullback_score": "pullback_score",
                "event_money_risk_score": "risk_score",
            },
            sources,
            "event_money_pullback",
        )
        _copy_signal_fields(
            row,
            pullback.get(code, {}),
            {
                "pullback_methodology_status": "methodology_status",
                "pullback_axis_score": "axis_score",
                "pullback_refined_watch": "refined_pullback_watch",
            },
            sources,
            "pullback_methodology",
        )
        _copy_signal_fields(
            row,
            fourq_forward.get(code, {}),
            {
                "fourq_role_fit_label": "role_fit_label",
                "fourq_forward_status": "forward_status",
                "fourq_forward_outcome": "role_forward_outcome",
                "fourq_forward_reason": "role_forward_reason",
                "fourq_intraday_return": "intraday_return",
            },
            sources,
            "four_question_forward",
        )
        _copy_signal_fields(
            row,
            news_impact.get(code, {}),
            {
                "shadow_news_score": "shadow_news_score",
                "shadow_score_reason": "shadow_score_reason",
                "final_score_delta_sim": "final_score_delta_sim",
                "final_score_shadow_sim": "final_score_shadow_sim",
                "rank_delta": "rank_delta",
                "score_only_entry_effect": "score_only_entry_effect",
            },
            sources,
            "news_multisource_shadow",
        )
        _copy_signal_fields(
            row,
            news_history.get(code, {}),
            {
                "news_multisource_signal_date8": "signal_date8",
                "news_multisource_outcome_status": "outcome_status",
                "news_multisource_fwd_1d_ret": "fwd_1d_ret",
                "news_multisource_fwd_3d_ret": "fwd_3d_ret",
                "news_multisource_fwd_5d_ret": "fwd_5d_ret",
            },
            sources,
            "news_multisource_forward",
        )
        _copy_signal_fields(
            row,
            defense.get(code, {}),
            {
                "defense_action_shadow": "defense_action_shadow",
                "general_action_shadow": "general_action_shadow",
                "surge_action_shadow": "surge_action_shadow",
                "would_block_general": "would_block_general",
                "would_block_surge": "would_block_surge",
                "defense_signal_score": "defense_signal_score",
                "defense_reasons": "defense_reasons",
                "shadow_news_score": "shadow_news_score",
                "score_only_entry_effect": "score_only_entry_effect",
                "fourq_intraday_return": "fourq_intraday_return",
            },
            sources,
            "defense_signal",
        )
        row["event_validation_present"] = bool(sources)
        row["event_validation_sources"] = "|".join(sources)
        out[code] = row
    meta = {
        "event_theme_rows": len(event_theme),
        "event_money_pullback_rows": len(event_money),
        "pullback_methodology_rows": len(pullback),
        "four_question_forward_rows": len(fourq_forward),
        "news_multisource_shadow_rows": len(news_impact),
        "news_multisource_history_rows": len(news_history),
        "defense_signal_rows": len(defense),
        "validated_codes": len(out),
    }
    return out, meta


def _load_news_topic_impact_by_code() -> tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    rows = _read_csv(NEWS_TOPIC_CANDIDATE_IMPACT_CSV)
    by_code: Dict[str, List[Dict[str, str]]] = {}
    skipped_direct_candidate = 0
    skipped_trading_effect = 0
    for row in rows:
        code = _clean_code(row)
        if not code:
            continue
        if _truthy(row.get("direct_candidate_allowed")):
            skipped_direct_candidate += 1
            continue
        if _truthy(row.get("trading_effect")):
            skipped_trading_effect += 1
            continue
        by_code.setdefault(code, []).append(row)

    out: Dict[str, Dict[str, Any]] = {}
    for code, items in by_code.items():
        items = sorted(
            items,
            key=lambda r: (
                -_f(r.get("confirm_level"), 0.0),
                -_f(r.get("candidate_article_count"), 0.0),
                -_f(r.get("avg_confidence"), 0.0),
                str(r.get("topic_key") or ""),
            ),
        )
        first = items[0]
        out[code] = {
            "news_topic_present": True,
            "news_topic_candidate_name": str(first.get("name") or "").strip(),
            "news_topic_key": "|".join(str(r.get("topic_key") or "").strip() for r in items if str(r.get("topic_key") or "").strip()),
            "news_topic_label": "|".join(str(r.get("topic_label") or "").strip() for r in items if str(r.get("topic_label") or "").strip()),
            "news_topic_state": str(first.get("topic_state") or "").strip(),
            "news_topic_direction": str(first.get("direction") or "").strip(),
            "news_topic_risk_state": str(first.get("risk_state") or "").strip(),
            "news_topic_confirm_level": str(first.get("confirm_level") or "").strip(),
            "news_topic_candidate_effect": "|".join(str(r.get("candidate_effect") or "").strip() for r in items if str(r.get("candidate_effect") or "").strip()),
            "news_topic_effect_reason": str(first.get("effect_reason") or "").strip(),
            "news_topic_article_count": str(first.get("candidate_article_count") or first.get("topic_article_count") or "").strip(),
            "news_topic_avg_strength": str(first.get("avg_strength") or "").strip(),
            "news_topic_avg_confidence": str(first.get("avg_confidence") or "").strip(),
            "news_topic_representative_titles": str(first.get("representative_titles") or "").strip(),
            "news_topic_representative_evidence": str(first.get("representative_evidence") or "").strip(),
        }
    return out, {
        "input": str(NEWS_TOPIC_CANDIDATE_IMPACT_CSV),
        "rows": len(rows),
        "matched_codes": len(out),
        "skipped_direct_candidate_allowed": skipped_direct_candidate,
        "skipped_trading_effect": skipped_trading_effect,
        "trading_effect": False,
        "policy_effect": False,
    }


def _apply_signal_validation(rows: List[Dict[str, Any]], validation_by_code: Dict[str, Dict[str, Any]]) -> None:
    for row in rows:
        code = _clean_code(row)
        validation = validation_by_code.get(code, {})
        if validation:
            row.update(validation)
        else:
            row["event_validation_present"] = False
            row["event_validation_sources"] = ""


def _apply_news_topic_impact(rows: List[Dict[str, Any]], impact_by_code: Dict[str, Dict[str, Any]]) -> None:
    for row in rows:
        code = _clean_code(row)
        impact = impact_by_code.get(code, {})
        if impact:
            row.update(impact)
            if not str(row.get("name") or "").strip() and str(impact.get("news_topic_candidate_name") or "").strip():
                row["name"] = str(impact.get("news_topic_candidate_name") or "").strip()
        else:
            row["news_topic_present"] = False


def _signal_validation_counts(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    source_counts: Dict[str, int] = {}
    event_decision_counts: Dict[str, int] = {}
    bucket_counts: Dict[str, int] = {}
    defense_counts: Dict[str, int] = {}
    fourq_forward_counts: Dict[str, int] = {}
    news_outcome_counts: Dict[str, int] = {}
    present = 0
    for row in rows:
        if _truthy(row.get("event_validation_present")):
            present += 1
        for src in str(row.get("event_validation_sources") or "").split("|"):
            src = src.strip()
            if src:
                source_counts[src] = source_counts.get(src, 0) + 1
        event_decision = str(row.get("event_theme_decision") or "").strip()
        if event_decision:
            event_decision_counts[event_decision] = event_decision_counts.get(event_decision, 0) + 1
        bucket = str(row.get("event_money_bucket") or "").strip()
        if bucket:
            bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
        defense = str(row.get("defense_action_shadow") or "").strip()
        if defense:
            defense_counts[defense] = defense_counts.get(defense, 0) + 1
        fourq_forward = str(row.get("fourq_forward_status") or "").strip()
        if fourq_forward:
            fourq_forward_counts[fourq_forward] = fourq_forward_counts.get(fourq_forward, 0) + 1
        news_outcome = str(row.get("news_multisource_outcome_status") or "").strip()
        if news_outcome:
            news_outcome_counts[news_outcome] = news_outcome_counts.get(news_outcome, 0) + 1
    return {
        "event_validation_present_rows": present,
        "event_validation_source_counts": source_counts,
        "event_theme_decision_counts": event_decision_counts,
        "event_money_bucket_counts": bucket_counts,
        "defense_action_counts": defense_counts,
        "fourq_forward_status_counts": fourq_forward_counts,
        "news_multisource_outcome_counts": news_outcome_counts,
    }


def _surge_after_state(label: Any) -> str:
    label_text = str(label or "").strip().upper()
    if label_text in {"ACTIVE_ENTRY_READY", "PROBE_READY"}:
        return "ENTRY_REVIEW"
    if label_text == "WAIT_RECLAIM":
        return "WAIT_RECLAIM"
    if label_text == "WAIT_LOB":
        return "WAIT_LOB"
    if label_text == "HARD_EXCLUDE":
        return "HARD_EXCLUDE"
    return ""


def _positive_entry_eval(row: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    final_score = _f(row.get("final_score"), _f(row.get("final_score_base"), 0.0))
    sector_allowed = _truthy(row.get("sector_entry_allowed"))
    execution_pool = _truthy(row.get("execution_pool"))
    sector_strength = _f(row.get("sector_strength"), 0.0)
    pol = cfg.get("positive_entry_criteria", {}) if isinstance(cfg.get("positive_entry_criteria"), dict) else {}
    score_ok = final_score > _f(pol.get("min_score"), 0.0)
    try:
        union_strength_min = float(cfg.get("union_entry_strength_min", 0.65) or 0.65)
    except Exception:
        union_strength_min = 0.65
    union_ok = (
        bool(pol.get("allow_sector_union", True))
        and str(row.get("candidate_origin") or "").strip().upper() == "SECTOR_PREFILTER_UNION"
        and str(row.get("sector_action") or "").strip().upper() == "BUY"
        and sector_allowed
        and sector_strength >= union_strength_min
    )
    execution_ok = execution_pool or union_ok
    ok = bool(score_ok and sector_allowed and execution_ok)
    if ok and union_ok and not execution_pool:
        return {
            "ok": True,
            "reason": f"sector_union_conditional:strength>={union_strength_min:.2f}",
            "path": "SECTOR_UNION_CONDITIONAL",
        }
    if ok:
        return {"ok": True, "reason": "passes_daily_candidate_and_execution_pool", "path": "EXECUTION_POOL"}
    reasons = []
    if not score_ok:
        reasons.append("final_score_nonpositive")
    if not sector_allowed:
        reasons.append("sector_entry_not_allowed")
    if not execution_ok:
        reasons.append("not_execution_pool")
    return {"ok": False, "reason": ";".join(reasons) or "daily_candidate_not_tradable", "path": ""}


def _queue_row(
    *,
    source: str,
    action_state: str,
    row: Dict[str, Any],
    reason: str,
    priority: float = 0.0,
    recheck_after_minutes: int = 0,
    trading_allowed: bool = False,
) -> Dict[str, Any]:
    return {
        "source": source,
        "action_state": action_state,
        "code": _clean_code(row),
        "name": _name(row),
        "reason": reason,
        "priority": round(float(priority), 6),
        "recheck_after_minutes": int(recheck_after_minutes),
        "trading_allowed": bool(trading_allowed),
        "final_score": _f(row.get("final_score"), _f(row.get("final_score_base"), 0.0)),
        "score": _f(row.get("score"), 0.0),
        "change_pct": _f(row.get("change_pct"), _f(row.get("ret1_pct"), 0.0)),
        "trading_value": _f(row.get("trading_value"), _f(row.get("value"), 0.0)),
        "sector_strength": _f(row.get("sector_strength"), 0.0),
        "sector_entry_allowed": _truthy(row.get("sector_entry_allowed")),
        "execution_pool": _truthy(row.get("execution_pool")),
        "candidate_origin": str(row.get("candidate_origin") or "").strip(),
        "exclude_reasons": str(row.get("exclude_reasons") or "").strip(),
    }


def _apply_surge_review_policy(qrow: Dict[str, Any], *, entry_allowed: bool) -> None:
    if not entry_allowed:
        return
    active_label = str(qrow.get("active_response_label") or "").strip().upper()
    after_state = str(qrow.get("surge_after_state") or "").strip().upper()
    intent_class = str(qrow.get("surge_intent_class") or "").strip().upper()
    expectancy_ready = _truthy(qrow.get("surge_expectancy_buy_ready"))

    if active_label == "WAIT_RECLAIM" or after_state == "WAIT_RECLAIM":
        qrow["action_state"] = "RECHECK"
        qrow["reason"] = "surge_entry_allowed_but_reclaim_required"
        qrow["recheck_after_minutes"] = 5
        qrow["trading_allowed"] = False
        return
    if active_label == "WAIT_LOB" or after_state == "WAIT_LOB":
        qrow["action_state"] = "RECHECK"
        qrow["reason"] = "surge_entry_allowed_but_lob_recheck_required"
        qrow["recheck_after_minutes"] = 5
        qrow["trading_allowed"] = False
        return
    if active_label == "HARD_EXCLUDE" or after_state == "HARD_EXCLUDE":
        qrow["action_state"] = "BLOCKED"
        qrow["reason"] = "surge_entry_allowed_but_active_response_hard_exclude"
        qrow["recheck_after_minutes"] = 0
        qrow["trading_allowed"] = False
        return
    if intent_class and not expectancy_ready:
        qrow["action_state"] = "RECHECK" if intent_class == "PATH_VALIDATION_PROBE" else "WATCH"
        qrow["reason"] = "surge_entry_allowed_but_expectancy_not_ready"
        qrow["recheck_after_minutes"] = 5
        qrow["trading_allowed"] = False


def _dedupe(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    priority_order = {
        "TRADABLE": 0,
        "RECHECK": 1,
        "WATCH": 2,
        "BLOCKED": 3,
    }
    best: Dict[tuple[str, str, str], Dict[str, Any]] = {}
    for row in rows:
        key = (str(row.get("code") or ""), str(row.get("source") or ""), str(row.get("action_state") or ""))
        if not key[0]:
            continue
        prev = best.get(key)
        if prev is None:
            best[key] = row
            continue
        old_rank = priority_order.get(str(prev.get("action_state")), 9)
        new_rank = priority_order.get(str(row.get("action_state")), 9)
        if (new_rank, -float(row.get("priority") or 0.0)) < (old_rank, -float(prev.get("priority") or 0.0)):
            best[key] = row
    return sorted(
        best.values(),
        key=lambda x: (
            priority_order.get(str(x.get("action_state")), 9),
            -float(x.get("priority") or 0.0),
            str(x.get("code") or ""),
        ),
    )


def main() -> int:
    cfg = _read_json(CONFIG_PATH)
    p1 = _read_json(LOG_DIR / "p1_entry_gate_status_latest.json")
    pending = _read_json(LOG_DIR / "pending_entry_status_latest.json")
    final_status = _read_json(LOG_DIR / "final_score_merge_status_latest.json")
    liquidity = _read_json(LOG_DIR / "liquidity_filter_daily_last.json")
    surge_status = _read_json(LOG_DIR / "surge_realtime_latest.json")
    recheck_queue = _read_json(RECHECK_QUEUE_JSON)
    score_rvol_conditional = _read_json(SCORE_RVOL_CONDITIONAL_JSON)
    no_lob_recheck_rows = _read_csv(NO_LOB_RECHECK_CSV)
    surge_intent_by_code = _intent_by_code(_read_csv(SURGE_INTENT_SPLIT_CSV))
    active_response_status = _read_json(SURGE_ACTIVE_RESPONSE_JSON)
    active_response_by_code = _active_response_by_code(_read_csv(SURGE_ACTIVE_RESPONSE_CSV))
    signal_validation_by_code, signal_validation_meta = _load_signal_validation_by_code()
    news_topic_impact_by_code, news_topic_impact_meta = _load_news_topic_impact_by_code()

    candidates = _read_csv(LOG_DIR / "candidates_latest_data.with_final_score.csv")
    market_rising = _read_csv(LOG_DIR / "market_rising_latest.csv", limit=30)
    surge_rows = _read_csv(LOG_DIR / "surge_realtime_latest.csv")

    intraday_policy = (((cfg.get("p1_entry_policy") or {}).get("intraday")) or {}) if isinstance(cfg, dict) else {}
    morning_strength_min = _f(intraday_policy.get("morning_sector_strength_min"), 0.80)

    queue: List[Dict[str, Any]] = []

    for row in candidates:
        final_score = _f(row.get("final_score"), _f(row.get("final_score_base"), 0.0))
        sector_allowed = _truthy(row.get("sector_entry_allowed"))
        sector_strength = _f(row.get("sector_strength"), 0.0)
        entry_eval = _positive_entry_eval(row, cfg)
        positive = bool(entry_eval.get("ok"))

        if positive:
            if sector_strength >= morning_strength_min:
                reason = str(entry_eval.get("reason") or "passes_daily_candidate")
            else:
                reason = f"{entry_eval.get('reason')};morning_strength_observe_only:{sector_strength:.4f}<{morning_strength_min:.4f}"
            queue.append(_queue_row(source="daily_candidate", action_state="TRADABLE", row=row, reason=reason, priority=final_score, trading_allowed=True))
        else:
            queue.append(_queue_row(source="daily_candidate", action_state="BLOCKED", row=row, reason=str(entry_eval.get("reason") or "daily_candidate_not_tradable"), priority=final_score))

    for row in liquidity.get("removed") or []:
        if isinstance(row, dict):
            queue.append(_queue_row(source="liquidity_removed", action_state="WATCH", row=row, reason=";".join(str(x) for x in row.get("reasons") or ["liquidity_rule_removed"]), priority=_f(row.get("day_ret_pct")), recheck_after_minutes=30))

    for row in market_rising[:20]:
        queue.append(_queue_row(source="market_rising", action_state="WATCH", row=row, reason="market_rising_rank", priority=_f(row.get("change_pct")) + (_f(row.get("trading_value")) / 1_000_000_000_000.0), recheck_after_minutes=15))

    for row in surge_rows:
        score_final = _f(row.get("surge_score_final"), _f(row.get("surge_score"), 0.0))
        if score_final < 78.0:
            continue
        entry_allowed = _truthy(row.get("entry_allowed"))
        excluded = _truthy(row.get("excluded_by_policy"))
        if entry_allowed:
            state = "TRADABLE"
            reason = "surge_entry_allowed"
            recheck_after_minutes = 0
        else:
            state = "BLOCKED" if excluded else "RECHECK"
            reason = str(row.get("exclude_reasons") or "surge_candidate_no_policy_exclusion").strip()
            recheck_after_minutes = 0 if excluded else 5
        qrow = _queue_row(
                source="surge_realtime",
                action_state=state,
                row=row,
                reason=reason,
                priority=score_final,
                recheck_after_minutes=recheck_after_minutes,
                trading_allowed=entry_allowed,
            )
        active = active_response_by_code.get(_clean_code(row), {})
        if active:
            active_label = str(active.get("active_response_label") or "").strip()
            qrow.update(
                {
                    "active_response_label": active_label,
                    "active_response_reason": str(active.get("active_response_reason") or "").strip(),
                    "active_response_next_check": str(active.get("active_response_next_check") or "").strip(),
                    "surge_after_state": str(active.get("surge_after_state") or "").strip() or _surge_after_state(active_label),
                }
            )
        intent = surge_intent_by_code.get(_clean_code(row), {})
        if intent:
            qrow.update(
                {
                    "surge_intent_class": intent.get("intent_class", ""),
                    "surge_intent_reason": intent.get("intent_reason", ""),
                    "surge_expectancy_buy_ready": _truthy(intent.get("expectancy_buy_ready")),
                }
            )
        _apply_surge_review_policy(qrow, entry_allowed=entry_allowed)
        queue.append(qrow)

    for row in recheck_queue.get("rows") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("recheck_status") or "").upper() != "DUE":
            continue
        reason = str(row.get("action_reason") or row.get("reason") or "recheck_due").strip()
        queue.append(_queue_row(source="action_recheck_due", action_state="RECHECK", row=row, reason=reason, priority=_f(row.get("priority")), recheck_after_minutes=15))

    for row in no_lob_recheck_rows:
        reason = str(row.get("recheck_reason") or "NO_LOB_BLOCK_LOB_COLLECTED").strip()
        priority = _f(row.get("surge_score_final"), _f(row.get("surge_score"), 0.0))
        queue.append(
            _queue_row(
                source="no_lob_recheck",
                action_state="RECHECK",
                row=row,
                reason=reason,
                priority=priority,
                recheck_after_minutes=5,
                trading_allowed=False,
            )
        )

    score_rvol_conditional_rows = score_rvol_conditional.get("items")
    if not isinstance(score_rvol_conditional_rows, list):
        score_rvol_conditional_rows = []
    for row in score_rvol_conditional_rows:
        if not isinstance(row, dict):
            continue
        reason = str(row.get("recheck_reason") or "SCORE_RVOL_CONDITIONAL_RECHECK_ONLY").strip()
        priority = _f(row.get("score_rvol_rule_score"), _f(row.get("surge_score_final"), 0.0))
        queue.append(
            _queue_row(
                source="score_rvol_conditional_recheck",
                action_state="RECHECK",
                row=row,
                reason=reason,
                priority=priority,
                recheck_after_minutes=5,
                trading_allowed=False,
            )
        )

    queue = _dedupe(queue)
    _apply_signal_validation(queue, signal_validation_by_code)
    _apply_news_topic_impact(queue, news_topic_impact_by_code)
    counts: Dict[str, int] = {}
    for row in queue:
        counts[str(row.get("action_state"))] = counts.get(str(row.get("action_state")), 0) + 1
    active_response_label_counts = _active_response_counts(queue)
    signal_validation_counts = _signal_validation_counts(queue)

    payload = {
        "generated_at": _now_kst(),
        "schema_version": "candidate_action_queue_v1",
        "trading_effect": False,
        "policy_effect": False,
        "as_of_ymd": str(p1.get("as_of_ymd") or final_status.get("asof_ymd") or pending.get("runtime_ymd") or ""),
        "summary": {
            "queue_rows": len(queue),
            "counts": counts,
            "daily_candidate_rows": len(candidates),
            "market_rising_rows": len(market_rising),
            "surge_evaluated_rows": int(surge_status.get("evaluated_rows") or len(surge_rows) or 0),
            "surge_alerts_count": int(surge_status.get("alerts_count") or 0),
            "surge_active_response_source_ts": active_response_status.get("source_ts", ""),
            "surge_active_response_label_counts": active_response_label_counts,
            "signal_validation_inputs": signal_validation_meta,
            "news_topic_candidate_impact_inputs": news_topic_impact_meta,
            "news_topic_present_rows": sum(1 for row in queue if _truthy(row.get("news_topic_present"))),
            **signal_validation_counts,
            "entry_candidates_before_p1": p1.get("entry_candidates_before"),
            "entry_candidates_after_p1": p1.get("entry_candidates_after"),
            "pending_entry_ready": pending.get("entry_ready"),
            "pending_filled": pending.get("filled"),
        },
        "state_inputs": {
            "p1_actions": p1.get("actions") or [],
            "p1_market_regime": p1.get("market_regime"),
            "pending_market_regime": pending.get("market_regime"),
            "pending_max_new": pending.get("max_new"),
            "pending_max_new_surge": pending.get("max_new_surge"),
            "pending_max_new_zero_reason": pending.get("max_new_zero_reason"),
            "news_gate": (final_status.get("news_gate") or {}).get("gate") if isinstance(final_status.get("news_gate"), dict) else None,
        },
        "artifacts": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
            "candidates": str(LOG_DIR / "candidates_latest_data.with_final_score.csv"),
            "market_rising": str(LOG_DIR / "market_rising_latest.csv"),
            "surge_realtime": str(LOG_DIR / "surge_realtime_latest.csv"),
            "p1_entry_gate": str(LOG_DIR / "p1_entry_gate_status_latest.json"),
            "pending_entry": str(LOG_DIR / "pending_entry_status_latest.json"),
            "recheck_queue": str(RECHECK_QUEUE_JSON),
            "no_lob_recheck": str(NO_LOB_RECHECK_CSV),
            "score_rvol_conditional_recheck": str(SCORE_RVOL_CONDITIONAL_JSON),
            "surge_probe_intent_split": str(SURGE_INTENT_SPLIT_CSV),
            "surge_active_response": str(SURGE_ACTIVE_RESPONSE_CSV),
            "surge_event_money_pullback": str(SURGE_EVENT_MONEY_PULLBACK_CSV),
            "event_theme": str(EVENT_THEME_CSV),
            "pullback_methodology": str(PULLBACK_METHODOLOGY_CSV),
            "four_question_forward": str(FOUR_Q_FORWARD_CSV),
            "news_multisource_shadow": str(NEWS_MULTISOURCE_IMPACT_CSV),
            "news_multisource_forward": str(NEWS_MULTISOURCE_HISTORY_CSV),
            "news_topic_candidate_impact": str(NEWS_TOPIC_CANDIDATE_IMPACT_CSV),
            "defense_signal": str(DEFENSE_SIGNAL_CSV),
        },
        "rows": queue,
    }

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        fields = [
            "source",
            "action_state",
            "code",
            "name",
            "reason",
            "priority",
            "recheck_after_minutes",
            "trading_allowed",
            "final_score",
            "score",
            "change_pct",
            "trading_value",
            "sector_strength",
            "sector_entry_allowed",
            "execution_pool",
            "candidate_origin",
            "exclude_reasons",
            "active_response_label",
            "active_response_reason",
            "active_response_next_check",
            "surge_after_state",
            "surge_intent_class",
            "surge_intent_reason",
            "surge_expectancy_buy_ready",
            *NEWS_TOPIC_IMPACT_FIELDS,
            *SIGNAL_VALIDATION_FIELDS,
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in queue:
            writer.writerow({k: row.get(k, "") for k in fields})

    print(json.dumps({"status": "OK", "rows": len(queue), "counts": counts, "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
