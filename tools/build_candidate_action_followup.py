"""Track follow-up performance for the read-only candidate action queue.

This script only observes queued names. It does not create orders, modify
scores, or change policy. It records whether WATCH/RECHECK/BLOCKED names later
move after being placed in the queue.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

QUEUE_JSON = LOG_DIR / "candidate_action_queue_latest.json"
FOLLOWUP_JSON = LOG_DIR / "candidate_action_followup_latest.json"
FOLLOWUP_CSV = LOG_DIR / "candidate_action_followup_latest.csv"
BASELINE_JSON = LOG_DIR / "candidate_action_followup_baseline.json"


KST = dt.timezone(dt.timedelta(hours=9))

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


def _now() -> dt.datetime:
    return dt.datetime.now(tz=KST)


def _ts() -> str:
    return _now().isoformat(timespec="seconds")


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


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value).strip()
        if text == "":
            return float(default)
        return float(text)
    except Exception:
        return float(default)


def _code(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    if not text:
        return ""
    return text.zfill(6)[-6:]


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


def _load_price_map() -> Dict[str, Dict[str, Any]]:
    price_map: Dict[str, Dict[str, Any]] = {}

    for row in _read_csv(LOG_DIR / "intraday_prices_latest.csv"):
        code = _code(row.get("code"))
        price = _f(row.get("current_price"), 0.0)
        if code and price > 0:
            price_map[code] = {
                "price": price,
                "source": "intraday_prices",
                "price_ts": str(row.get("ts") or ""),
                "change_pct": None,
                "trading_value": _f(row.get("trading_value"), 0.0),
            }

    for row in _read_csv(LOG_DIR / "market_rising_latest.csv"):
        code = _code(row.get("code"))
        price = _f(row.get("current_price"), 0.0)
        if code and price > 0:
            price_map[code] = {
                "price": price,
                "source": "market_rising",
                "price_ts": "",
                "change_pct": _f(row.get("change_pct"), 0.0),
                "trading_value": _f(row.get("trading_value"), 0.0),
            }

    for row in _read_csv(LOG_DIR / "surge_realtime_latest.csv"):
        code = _code(row.get("code"))
        price = _f(row.get("current_price"), 0.0)
        if code and price > 0:
            price_map[code] = {
                "price": price,
                "source": "surge_realtime",
                "price_ts": str(row.get("ts") or ""),
                "change_pct": _f(row.get("change_pct"), 0.0) * 100.0,
                "trading_value": _f(row.get("trading_value"), 0.0),
            }

    return price_map


def _baseline_key(row: Dict[str, Any]) -> str:
    return "|".join([
        str(row.get("source") or ""),
        str(row.get("action_state") or ""),
        str(row.get("code") or ""),
        str(row.get("reason") or "")[:120],
    ])


def _summarize(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    counts: Dict[str, int] = {}
    observed = 0
    matured = 0
    positive = 0
    negative = 0
    validation_present = 0
    validation_source_counts: Dict[str, int] = {}
    best: Dict[str, Any] | None = None
    worst: Dict[str, Any] | None = None
    for row in rows:
        state = str(row.get("action_state") or "")
        counts[state] = counts.get(state, 0) + 1
        if row.get("current_price_available"):
            observed += 1
        if row.get("matured"):
            matured += 1
        ret = _f(row.get("return_pct_since_first_seen"), 0.0)
        if ret > 0:
            positive += 1
        elif ret < 0:
            negative += 1
        if row.get("event_validation_present"):
            validation_present += 1
        for src in str(row.get("event_validation_sources") or "").split("|"):
            src = src.strip()
            if src:
                validation_source_counts[src] = validation_source_counts.get(src, 0) + 1
        if row.get("current_price_available"):
            if best is None or ret > _f(best.get("return_pct_since_first_seen"), 0.0):
                best = row
            if worst is None or ret < _f(worst.get("return_pct_since_first_seen"), 0.0):
                worst = row
    return {
        "counts": counts,
        "observed_price_rows": observed,
        "matured_rows": matured,
        "positive_rows": positive,
        "negative_rows": negative,
        "event_validation_present_rows": validation_present,
        "event_validation_source_counts": validation_source_counts,
        "best": best,
        "worst": worst,
    }


def main() -> int:
    queue_doc = _read_json(QUEUE_JSON)
    queue_rows = queue_doc.get("rows") if isinstance(queue_doc.get("rows"), list) else []
    baseline = _read_json(BASELINE_JSON)
    items = baseline.get("items") if isinstance(baseline.get("items"), dict) else {}
    price_map = _load_price_map()
    now = _now()
    rows: List[Dict[str, Any]] = []

    for qrow in queue_rows:
        if not isinstance(qrow, dict):
            continue
        code = _code(qrow.get("code"))
        if not code:
            continue
        key = _baseline_key(qrow)
        price_info = price_map.get(code, {})
        current_price = _f(price_info.get("price"), 0.0)
        fallback_price = _f(qrow.get("trading_value"), 0.0)
        item = items.get(key) if isinstance(items.get(key), dict) else {}
        first_seen_at = str(item.get("first_seen_at") or _ts())
        first_seen_dt = _parse_dt(first_seen_at) or now
        baseline_price = _f(item.get("baseline_price"), 0.0)
        baseline_source = str(item.get("baseline_price_source") or "")

        if baseline_price <= 0.0 and current_price > 0.0:
            baseline_price = current_price
            baseline_source = str(price_info.get("source") or "current_price")

        elapsed_min = max(0.0, (now - first_seen_dt).total_seconds() / 60.0)
        recheck_after = int(_f(qrow.get("recheck_after_minutes"), 0.0))
        matured = bool(recheck_after > 0 and elapsed_min >= float(recheck_after))
        ret_pct = 0.0
        if baseline_price > 0.0 and current_price > 0.0:
            ret_pct = (current_price / baseline_price - 1.0) * 100.0

        items[key] = {
            "first_seen_at": first_seen_at,
            "baseline_price": baseline_price,
            "baseline_price_source": baseline_source,
            "source": str(qrow.get("source") or ""),
            "action_state": str(qrow.get("action_state") or ""),
            "code": code,
            "name": str(qrow.get("name") or ""),
            "reason": str(qrow.get("reason") or ""),
        }

        out_row = {
            "source": str(qrow.get("source") or ""),
            "action_state": str(qrow.get("action_state") or ""),
            "code": code,
            "name": str(qrow.get("name") or ""),
            "reason": str(qrow.get("reason") or ""),
            "first_seen_at": first_seen_at,
            "elapsed_minutes": round(elapsed_min, 2),
            "recheck_after_minutes": recheck_after,
            "matured": matured,
            "baseline_price": baseline_price,
            "baseline_price_source": baseline_source,
            "current_price": current_price,
            "current_price_source": str(price_info.get("source") or ""),
            "current_price_available": current_price > 0.0,
            "return_pct_since_first_seen": round(ret_pct, 4),
            "queue_priority": _f(qrow.get("priority"), 0.0),
            "queue_change_pct": _f(qrow.get("change_pct"), 0.0),
            "current_change_pct": price_info.get("change_pct"),
            "current_trading_value": _f(price_info.get("trading_value"), 0.0),
            "trading_allowed": False,
        }
        for field in SIGNAL_VALIDATION_FIELDS:
            out_row[field] = qrow.get(field, "")
        rows.append(out_row)

    rows.sort(key=lambda r: (
        str(r.get("action_state") or ""),
        -abs(_f(r.get("return_pct_since_first_seen"), 0.0)),
        str(r.get("code") or ""),
    ))

    summary = _summarize(rows)
    payload = {
        "generated_at": _ts(),
        "schema_version": "candidate_action_followup_v1",
        "trading_effect": False,
        "policy_effect": False,
        "queue_generated_at": queue_doc.get("generated_at"),
        "summary": {
            "queue_rows": len(queue_rows),
            "followup_rows": len(rows),
            **summary,
        },
        "artifacts": {
            "queue": str(QUEUE_JSON),
            "json": str(FOLLOWUP_JSON),
            "csv": str(FOLLOWUP_CSV),
            "baseline": str(BASELINE_JSON),
        },
        "rows": rows,
    }

    _write_json(BASELINE_JSON, {"generated_at": _ts(), "items": items})
    _write_json(FOLLOWUP_JSON, payload)
    with FOLLOWUP_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        fields = [
            "source",
            "action_state",
            "code",
            "name",
            "reason",
            "first_seen_at",
            "elapsed_minutes",
            "recheck_after_minutes",
            "matured",
            "baseline_price",
            "baseline_price_source",
            "current_price",
            "current_price_source",
            "current_price_available",
            "return_pct_since_first_seen",
            "queue_priority",
            "queue_change_pct",
            "current_change_pct",
            "current_trading_value",
            "trading_allowed",
            *SIGNAL_VALIDATION_FIELDS,
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fields})

    print(json.dumps({
        "status": "OK",
        "rows": len(rows),
        "observed": summary["observed_price_rows"],
        "matured": summary["matured_rows"],
        "out_json": str(FOLLOWUP_JSON),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
