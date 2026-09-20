from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

CANDIDATES_CSV = LOG_DIR / "candidates_latest_data.with_final_score.csv"
EVENT_THEME_CSV = LOG_DIR / "event_theme_candidate_layer_latest.csv"
MULTISOURCE_SHADOW_CSV = LOG_DIR / "news_multisource_shadow_score_impact_sim_latest.csv"
SURGE_REALTIME_CSV = LOG_DIR / "surge_realtime_latest.csv"
SURGE_PULLBACK_CSV = LOG_DIR / "surge_event_money_pullback_screener_latest.csv"
FOUR_Q_VALIDATION_CSV = LOG_DIR / "four_question_role_fit_forward_validation_latest.csv"
TRADES_CSV = ROOT / "paper" / "trades_calc.csv"

OUT_JSON = LOG_DIR / "defense_signal_shadow_latest.json"
OUT_CSV = LOG_DIR / "defense_signal_shadow_latest.csv"

KST = timezone(timedelta(hours=9))


def _now_kst() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as fh:
                return [{str(k): str(v or "") for k, v in row.items()} for row in csv.DictReader(fh)]
        except UnicodeDecodeError:
            continue
    return []


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fields})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _code(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6)[-6:] if digits else ""


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value or "").strip()
        if text == "":
            return float(default)
        return float(text)
    except Exception:
        return float(default)


def _b(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "y", "yes"}


def _index(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code and code not in out:
            out[code] = row
    return out


def _aggregate_trades(rows: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[float]] = {}
    for row in rows:
        code = _code(row.get("code") or row.get("ticker"))
        if not code:
            continue
        text = str(row.get("net_ret") or "").strip()
        if not text:
            continue
        try:
            net_ret = float(text)
        except Exception:
            continue
        grouped.setdefault(code, []).append(net_ret)

    out: dict[str, dict[str, Any]] = {}
    for code, vals in grouped.items():
        out[code] = {
            "historical_trade_rows": len(vals),
            "historical_trade_avg_net_ret": sum(vals) / len(vals) if vals else 0.0,
        }
    return out


def _pick_name(*rows: dict[str, str]) -> str:
    for row in rows:
        name = str(row.get("name") or "").strip()
        if name:
            return name
    return ""


def _add(points: list[tuple[str, int]], label: str, score: int) -> None:
    if score != 0:
        points.append((label, score))


def _classify_action(score: int, route: str) -> str:
    if route == "surge":
        if score >= 55:
            return "SURGE_SHADOW_BLOCK"
        if score >= 35:
            return "SURGE_SHADOW_LOB_RECHECK"
        if score >= 20:
            return "SURGE_SHADOW_WATCH"
        return "SURGE_SHADOW_ALLOW"
    if score >= 65:
        return "GENERAL_SHADOW_WAIT"
    if score >= 35:
        return "GENERAL_SHADOW_REVIEW"
    if score >= 20:
        return "GENERAL_SHADOW_WATCH"
    return "GENERAL_SHADOW_ALLOW"


def _defense_scores(
    candidate: dict[str, str],
    event: dict[str, str],
    shadow: dict[str, str],
    surge_rt: dict[str, str],
    surge_pb: dict[str, str],
    fourq_validation: dict[str, str],
    trade_ref: dict[str, Any],
) -> dict[str, Any]:
    late = str(event.get("late_buy_risk") or "").strip().upper()
    prepriced = str(event.get("news_prepriced_risk") or "").strip().upper()
    four_q = str(event.get("four_question_decision") or "").strip().upper()
    smart = str(event.get("smart_money_quality") or "").strip().upper()
    price_zone = str(event.get("price_zone_label") or "").strip().upper()
    event_decision = str(event.get("event_theme_decision") or "").strip().upper()
    shadow_news_score = _f(shadow.get("shadow_news_score"), 0.0)
    score_only_effect = str(shadow.get("score_only_entry_effect") or "").strip().upper()
    orderflow_tag = str(surge_rt.get("orderflow_tag") or "").strip().upper()
    lob_status = str(surge_rt.get("lob_status") or "").strip().upper()
    surge_entry_decision = str(surge_pb.get("entry_decision") or "").strip().upper()
    surge_risk_score = _f(surge_pb.get("risk_score"), 0.0)
    news_block_rows = _f(candidate.get("news_implication_block_rows"), 0.0)
    news_risk_score = _f(candidate.get("news_implication_risk_score"), 0.0)
    publisher_state = str(candidate.get("publisher_gate_state") or "").strip().upper()
    intraday_return = _f(fourq_validation.get("intraday_return"), 0.0)
    historical_trade_avg = _f(trade_ref.get("historical_trade_avg_net_ret"), 0.0)

    common: list[tuple[str, int]] = []
    general: list[tuple[str, int]] = []
    surge: list[tuple[str, int]] = []

    if late == "HIGH":
        _add(general, "late_buy_risk_high", 25)
        _add(surge, "late_buy_risk_high", 35)
    elif late == "MEDIUM":
        _add(general, "late_buy_risk_medium", 10)
        _add(surge, "late_buy_risk_medium", 15)

    if prepriced == "HIGH":
        _add(general, "news_prepriced_high", 25)
        _add(surge, "news_prepriced_high", 35)
    elif prepriced == "MEDIUM":
        _add(general, "news_prepriced_medium", 10)
        _add(surge, "news_prepriced_medium", 15)

    if four_q == "READONLY_CHASE_BLOCK":
        _add(general, "four_question_chase_block", 20)
        _add(surge, "four_question_chase_block", 35)
    elif four_q in {"READONLY_PRIORITY_WATCH", "READONLY_MONEY_PULLBACK_WATCH"}:
        _add(common, "four_question_watch_support", -5)

    if smart in {"ABSENT", "WEAK"}:
        _add(general, "smart_money_not_confirmed", 8)
        _add(surge, "smart_money_not_confirmed", 10)

    if price_zone == "TOP_RISK":
        _add(general, "price_zone_top_risk", 20)
        _add(surge, "price_zone_top_risk", 35)
    elif price_zone == "PULLBACK_OR_TURNING_WATCH":
        _add(common, "price_zone_pullback_watch", -5)

    if intraday_return <= -0.005:
        _add(general, "intraday_negative_recheck", 20)
        _add(surge, "intraday_negative_recheck", 25)
    elif intraday_return >= 0.005:
        _add(common, "intraday_positive_block_downgrade", -35)

    if historical_trade_avg <= -0.005:
        _add(general, "historical_trade_loss_reference", 8)
        _add(surge, "historical_trade_loss_reference", 10)
    elif historical_trade_avg >= 0.005:
        _add(common, "historical_trade_positive_reference", -8)

    if event_decision == "WATCH_BLOCK_CHASE":
        _add(general, "event_theme_watch_block_chase", 15)
        _add(surge, "event_theme_watch_block_chase", 25)

    if news_block_rows > 0:
        _add(common, "news_implication_block_rows", 20)
    if news_risk_score >= 0.75:
        _add(common, "news_implication_risk_high", 15)
    elif news_risk_score >= 0.4:
        _add(common, "news_implication_risk_medium", 8)

    if publisher_state in {"SHADOW_ONLY_MISSING_PUBLISHER_METRICS", "MISSING_SOURCE_EVIDENCE"}:
        _add(common, "publisher_or_source_evidence_weak", 10)

    if shadow_news_score > 0 and score_only_effect == "SCORE_ONLY_ALREADY_ABOVE_MIN":
        _add(common, "multisource_shadow_confirms_without_new_entry", -5)

    if orderflow_tag == "PAUSE":
        _add(surge, "orderflow_pause", 50)
    elif orderflow_tag == "CAUTION":
        _add(surge, "orderflow_caution", 25)
    if lob_status == "NO_LOB":
        _add(surge, "no_lob_execution_risk", 30)

    if surge_entry_decision in {"BLOCK", "WAIT", "WATCH"}:
        _add(surge, f"surge_pullback_entry_{surge_entry_decision.lower()}", 20)
    if surge_risk_score >= 60:
        _add(surge, "surge_pullback_risk_high", 20)
    elif surge_risk_score >= 35:
        _add(surge, "surge_pullback_risk_medium", 10)

    common_score = sum(score for _, score in common)
    general_score = max(0, common_score + sum(score for _, score in general))
    surge_score = max(0, common_score + sum(score for _, score in surge))
    max_score = max(general_score, surge_score)

    general_action = _classify_action(general_score, "general")
    surge_action = _classify_action(surge_score, "surge")
    negative_confirmation = bool(
        intraday_return <= -0.005
        or historical_trade_avg <= -0.005
        or news_block_rows > 0
        or orderflow_tag in {"PAUSE", "CAUTION"}
        or surge_entry_decision in {"BLOCK", "WAIT"}
    )
    if surge_action == "SURGE_SHADOW_BLOCK" and intraday_return >= 0.005 and not negative_confirmation:
        surge_action = "SURGE_SHADOW_LOB_RECHECK"
    if general_action == "GENERAL_SHADOW_WAIT" and intraday_return >= 0.005 and not negative_confirmation:
        general_action = "GENERAL_SHADOW_REVIEW"
    reasons = common + general + surge
    positive_reasons = [f"{label}:{score}" for label, score in reasons if score > 0]
    support_reasons = [f"{label}:{score}" for label, score in reasons if score < 0]

    return {
        "defense_signal_score": max_score,
        "defense_score_general": general_score,
        "defense_score_surge": surge_score,
        "defense_action_shadow": (
            surge_action
            if surge_score >= general_score and surge_action != "SURGE_SHADOW_ALLOW"
            else general_action
        ),
        "general_action_shadow": general_action,
        "surge_action_shadow": surge_action,
        "would_block_general": general_action == "GENERAL_SHADOW_WAIT",
        "would_block_surge": surge_action == "SURGE_SHADOW_BLOCK",
        "defense_reasons": "|".join(positive_reasons),
        "defense_support_reasons": "|".join(support_reasons),
    }


def main() -> int:
    status: dict[str, Any] = {
        "generated_at": _now_kst(),
        "mode": "read_only_defense_signal_shadow",
        "score_effect": False,
        "trading_effect": False,
        "policy_effect": False,
        "entry_approval_changed": False,
        "entry_pool_filter_effect": True,
        "paper_order_route": False,
        "broker_order_route": False,
        "no_order_effect": True,
        "downstream_consumer": "paper_engine.defense_signal_entry_policy",
        "downstream_effect": {
            "score_effect": False,
            "order_dispatch_effect": False,
            "entry_pool_filter_effect": True,
            "general_block_action": "GENERAL_SHADOW_WAIT",
            "surge_block_action": "SURGE_SHADOW_BLOCK",
        },
        "inputs": {
            "candidates": str(CANDIDATES_CSV),
            "event_theme": str(EVENT_THEME_CSV),
            "multisource_shadow": str(MULTISOURCE_SHADOW_CSV),
            "surge_realtime": str(SURGE_REALTIME_CSV),
            "surge_pullback": str(SURGE_PULLBACK_CSV),
            "four_question_validation": str(FOUR_Q_VALIDATION_CSV),
            "trades": str(TRADES_CSV),
        },
        "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV)},
        "quality": "FAIL",
        "reason": "",
    }

    candidates = _read_csv(CANDIDATES_CSV)
    events = _read_csv(EVENT_THEME_CSV)
    shadows = _read_csv(MULTISOURCE_SHADOW_CSV)
    surge_rt_rows = _read_csv(SURGE_REALTIME_CSV)
    surge_pb_rows = _read_csv(SURGE_PULLBACK_CSV)
    fourq_rows = _read_csv(FOUR_Q_VALIDATION_CSV)
    trade_rows = _read_csv(TRADES_CSV)

    if not candidates:
        status["reason"] = "candidate_input_missing_or_empty"
        _write_json(OUT_JSON, status)
        return 1
    if not events:
        status["reason"] = "event_theme_input_missing_or_empty"
        _write_json(OUT_JSON, status)
        return 1

    cand_by_code = _index(candidates)
    event_by_code = _index(events)
    shadow_by_code = _index(shadows)
    surge_rt_by_code = _index(surge_rt_rows)
    surge_pb_by_code = _index(surge_pb_rows)
    fourq_by_code = _index(fourq_rows)
    trade_ref_by_code = _aggregate_trades(trade_rows)

    codes = sorted(set(cand_by_code) | set(event_by_code) | set(shadow_by_code) | set(surge_rt_by_code) | set(surge_pb_by_code))
    out_rows: list[dict[str, Any]] = []

    for code in codes:
        cand = cand_by_code.get(code, {})
        event = event_by_code.get(code, {})
        shadow = shadow_by_code.get(code, {})
        surge_rt = surge_rt_by_code.get(code, {})
        surge_pb = surge_pb_by_code.get(code, {})
        fourq_validation = fourq_by_code.get(code, {})
        trade_ref = trade_ref_by_code.get(code, {})
        in_general = code in cand_by_code
        in_surge = code in surge_rt_by_code or code in surge_pb_by_code
        if in_general and in_surge:
            route_scope = "BOTH"
        elif in_surge:
            route_scope = "SURGE_ONLY"
        elif in_general:
            route_scope = "GENERAL_ONLY"
        else:
            route_scope = "WATCH_ONLY"

        scores = _defense_scores(cand, event, shadow, surge_rt, surge_pb, fourq_validation, trade_ref)
        scores["would_block_general"] = bool(in_general and scores.get("would_block_general"))
        scores["would_block_surge"] = bool(in_surge and scores.get("would_block_surge"))
        row = {
            "code": code,
            "name": _pick_name(cand, event, shadow, surge_rt, surge_pb),
            "route_scope": route_scope,
            "in_general_candidates": in_general,
            "in_surge_candidates": in_surge,
            "final_score": cand.get("final_score", ""),
            "news_score": cand.get("news_score", event.get("news_score", "")),
            "event_score": event.get("event_score", ""),
            "global_event_label": event.get("global_event_label", ""),
            "event_theme_decision": event.get("event_theme_decision", ""),
            "four_question_decision": event.get("four_question_decision", ""),
            "late_buy_risk": event.get("late_buy_risk", ""),
            "news_prepriced_risk": event.get("news_prepriced_risk", ""),
            "smart_money_quality": event.get("smart_money_quality", ""),
            "price_zone_label": event.get("price_zone_label", ""),
            "shadow_news_score": shadow.get("shadow_news_score", ""),
            "score_only_entry_effect": shadow.get("score_only_entry_effect", ""),
            "surge_score_final": surge_rt.get("surge_score_final", ""),
            "surge_type": surge_rt.get("surge_type", surge_pb.get("detected_surge_type", "")),
            "surge_entry_decision": surge_pb.get("entry_decision", ""),
            "orderflow_tag": surge_rt.get("orderflow_tag", ""),
            "lob_status": surge_rt.get("lob_status", ""),
            "fourq_intraday_return": fourq_validation.get("intraday_return", ""),
            "historical_trade_rows": trade_ref.get("historical_trade_rows", 0),
            "historical_trade_avg_net_ret": trade_ref.get("historical_trade_avg_net_ret", ""),
            "no_order_effect": True,
            "score_effect": False,
            "trading_effect": False,
            **scores,
        }
        out_rows.append(row)

    out_rows.sort(
        key=lambda r: (
            -int(r.get("defense_signal_score") or 0),
            str(r.get("route_scope") or ""),
            str(r.get("code") or ""),
        )
    )

    fields = [
        "code",
        "name",
        "route_scope",
        "in_general_candidates",
        "in_surge_candidates",
        "final_score",
        "news_score",
        "event_score",
        "global_event_label",
        "event_theme_decision",
        "four_question_decision",
        "late_buy_risk",
        "news_prepriced_risk",
        "smart_money_quality",
        "price_zone_label",
        "shadow_news_score",
        "score_only_entry_effect",
        "surge_score_final",
        "surge_type",
        "surge_entry_decision",
        "orderflow_tag",
        "lob_status",
        "fourq_intraday_return",
        "historical_trade_rows",
        "historical_trade_avg_net_ret",
        "defense_signal_score",
        "defense_score_general",
        "defense_score_surge",
        "defense_action_shadow",
        "general_action_shadow",
        "surge_action_shadow",
        "would_block_general",
        "would_block_surge",
        "defense_reasons",
        "defense_support_reasons",
        "no_order_effect",
        "score_effect",
        "trading_effect",
    ]
    _write_csv(OUT_CSV, out_rows, fields)

    action_counts = Counter(str(row["defense_action_shadow"]) for row in out_rows)
    route_counts = Counter(str(row["route_scope"]) for row in out_rows)
    general_block = sum(1 for row in out_rows if row["would_block_general"])
    surge_block = sum(1 for row in out_rows if row["would_block_surge"])
    status.update(
        {
            "quality": "PASS",
            "reason": "ok",
            "rows": len(out_rows),
            "route_scope_counts": dict(route_counts),
            "defense_action_shadow_counts": dict(action_counts),
            "would_block_general_rows": general_block,
            "would_block_surge_rows": surge_block,
            "max_defense_signal_score": max((int(row["defense_signal_score"]) for row in out_rows), default=0),
            "top_rows": out_rows[:20],
            "notes": [
                "producer is read-only for score, order route, fills, ledger, and policy files",
                "paper_engine.defense_signal_entry_policy can consume would_block_* fields as an entry-pool filter",
                "general and surge scores are separated because the same defense signal has different meaning by route",
                "would_block_* fields are not order dispatch decisions, but they can affect paper_engine candidate eligibility",
            ],
        }
    )
    _write_json(OUT_JSON, status)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
