"""Build promoted recheck candidates for paper-engine candidate review.

Only due, non-blocked recheck promotions are emitted. This is a candidate input
bridge, not an order sender; paper_engine still applies its normal entry checks.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
RECHECK_JSON = LOG_DIR / "candidate_action_recheck_queue_latest.json"
POST_ENTRY_JSON = LOG_DIR / "post_entry_learning_latest.json"
OUT_JSON = LOG_DIR / "promoted_recheck_candidates_latest.json"
OUT_CSV = LOG_DIR / "promoted_recheck_candidates_latest.csv"
KST = dt.timezone(dt.timedelta(hours=9))


def _now() -> dt.datetime:
    return dt.datetime.now(tz=KST)


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


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def _blocked(row: Dict[str, Any]) -> bool:
    if str(row.get("action_state") or "").upper() == "BLOCKED":
        return True
    reason = str(row.get("reason") or row.get("action_reason") or "")
    hard_tokens = [
        "NO_LOB_BLOCK",
        "KRX_WARNING",
        "KRX_CAUTION",
        "ENTRY_CHANGE_BLOCK",
        "ENTRY_ATR_CAP",
        "RVOL_OVERHEAT_BLOCK",
        "TRADING_VALUE_FLOOR",
    ]
    return any(token in reason for token in hard_tokens)


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return [{str(k): str(v) for k, v in row.items()} for row in csv.DictReader(f)]
    except Exception:
        return []


def _price_map() -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    for row in _read_csv(LOG_DIR / "market_rising_latest.csv"):
        code = _code(row.get("code"))
        price = _f(row.get("current_price"), 0.0)
        if code and price > 0.0:
            out[code] = {
                "price": price,
                "change_pct": _f(row.get("change_pct"), 0.0),
                "trading_value": _f(row.get("trading_value"), 0.0),
            }
    for row in _read_csv(LOG_DIR / "surge_realtime_latest.csv"):
        code = _code(row.get("code"))
        price = _f(row.get("current_price"), 0.0)
        if code and price > 0.0:
            out[code] = {
                "price": price,
                "change_pct": _f(row.get("change_pct"), 0.0) * 100.0,
                "trading_value": _f(row.get("trading_value"), 0.0),
            }
    return out


def _post_entry_map() -> tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    doc = _read_json(POST_ENTRY_JSON)
    rows = doc.get("rows") if isinstance(doc.get("rows"), list) else []
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = _code(row.get("code"))
        if code:
            out[code] = row
    summary = doc.get("summary") if isinstance(doc.get("summary"), dict) else {}
    return out, summary


def _session_feedback(post_summary: Dict[str, Any]) -> Dict[str, Any]:
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
        return {
            "learning_feedback": "PROMOTION_SESSION_RISK_FEEDBACK",
            "learning_feedback_action": "PROMOTE_WITH_SCORE_HAIRCUT",
            "learning_feedback_score_multiplier": 0.90,
            "learning_feedback_reason": f"promoted_risk={promoted_risk}/{promoted_entries}",
        }
    return {}


def _blocked_by_post_entry(code: str, post_by_code: Dict[str, Dict[str, Any]]) -> str:
    row = post_by_code.get(code, {})
    if not row:
        return ""
    open_qty = int(_f(row.get("open_qty"), 0.0))
    bucket = str(row.get("learning_bucket") or "")
    if open_qty > 0:
        return "same_day_position_already_open"
    if bucket == "ENTRY_CLOSED_OR_FLAT":
        return "same_day_entry_closed_or_flat"
    if bucket.endswith("_NEGATIVE"):
        return "same_day_negative_entry_feedback"
    return ""


def _candidate_row(row: Dict[str, Any], ymd: str, price_info: Dict[str, float]) -> Dict[str, Any]:
    priority = _f(row.get("priority"), 0.0)
    ret_since = _f(row.get("return_pct_since_first_seen"), 0.0)
    feedback_multiplier = _f(row.get("learning_feedback_score_multiplier"), 1.0)
    feedback_multiplier = max(0.50, min(1.25, feedback_multiplier))
    score = max(0.03, min(0.95, priority / 100.0))
    final_score_base = max(0.03, min(0.35, 0.12 + max(ret_since, 0.0) / 100.0))
    final_score = max(0.03, min(0.35, final_score_base * feedback_multiplier))
    trading_value = _f(row.get("current_trading_value"), _f(price_info.get("trading_value"), _f(row.get("trading_value"), 0.0)))
    current_price = _f(row.get("current_price"), _f(price_info.get("price"), 0.0))
    current_change = _f(row.get("current_change_pct"), _f(price_info.get("change_pct"), _f(row.get("change_pct"), 0.0)))
    return {
        "date": ymd,
        "date_yyyymmdd": ymd,
        "signal_date": ymd,
        "code": _code(row.get("code")),
        "name": str(row.get("name") or ""),
        "market": "UNKNOWN",
        "close": current_price,
        "value": trading_value,
        "trading_value": trading_value,
        "ret1_pct": current_change,
        "score": score,
        "final_score": final_score,
        "final_score_base": final_score_base,
        "learning_feedback": str(row.get("learning_feedback") or "NONE"),
        "learning_feedback_action": str(row.get("learning_feedback_action") or ""),
        "learning_feedback_score_multiplier": feedback_multiplier,
        "candidate_origin": "ACTIVE_RECHECK_PROMOTION",
        "execution_pool": True,
        "natural_pass": False,
        "horizon_label": "SHORT",
        "sector_entry_allowed": True,
        "sector_action": "BUY",
        "sector_strength": max(0.0, _f(row.get("sector_strength"), 0.0)),
        "sector_entry_weight": 1.0,
        "news_implication_block_rows": 0,
        "news_implication_reduce_size_rows": 0,
        "news_implication_watch_rows": 0,
        "promoted_recheck": True,
        "promoted_recheck_reason": str(row.get("action_reason") or row.get("reason") or ""),
        "promoted_recheck_source": str(row.get("source") or ""),
        "promoted_recheck_due_at": str(row.get("due_at") or ""),
        "promoted_recheck_return_pct": ret_since,
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "date",
        "date_yyyymmdd",
        "signal_date",
        "code",
        "name",
        "market",
        "close",
        "value",
        "trading_value",
        "ret1_pct",
        "score",
        "final_score",
        "final_score_base",
        "learning_feedback",
        "learning_feedback_action",
        "learning_feedback_score_multiplier",
        "candidate_origin",
        "execution_pool",
        "natural_pass",
        "horizon_label",
        "sector_entry_allowed",
        "sector_action",
        "sector_strength",
        "sector_entry_weight",
        "news_implication_block_rows",
        "news_implication_reduce_size_rows",
        "news_implication_watch_rows",
        "promoted_recheck",
        "promoted_recheck_reason",
        "promoted_recheck_source",
        "promoted_recheck_due_at",
        "promoted_recheck_return_pct",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    now = _now()
    ymd = now.strftime("%Y%m%d")
    doc = _read_json(RECHECK_JSON)
    prices = _price_map()
    post_by_code, post_summary = _post_entry_map()
    session_feedback = _session_feedback(post_summary)
    input_rows = doc.get("rows") if isinstance(doc.get("rows"), list) else []
    rows: List[Dict[str, Any]] = []
    skipped_blocked = 0
    skipped_not_tradable = 0
    skipped_learning_feedback = 0
    for row in input_rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("recheck_status") or "").upper() != "DUE":
            continue
        if str(row.get("next_action") or "") != "PROMOTE_TO_RECHECK":
            continue
        if _blocked(row):
            skipped_blocked += 1
            continue
        if not (
            _truthy(row.get("trading_allowed"))
            and _truthy(row.get("execution_pool"))
            and _truthy(row.get("sector_entry_allowed"))
        ):
            skipped_not_tradable += 1
            continue
        code = _code(row.get("code"))
        if not code:
            continue
        learning_block_reason = _blocked_by_post_entry(code, post_by_code)
        if learning_block_reason:
            skipped_learning_feedback += 1
            continue
        if session_feedback and str(row.get("learning_feedback") or "NONE") == "NONE":
            row = dict(row)
            row.update(session_feedback)
        price_info = prices.get(code, {})
        if _f(row.get("current_price"), _f(price_info.get("price"), 0.0)) <= 0.0:
            continue
        rows.append(_candidate_row(row, ymd, price_info))

    seen: set[str] = set()
    deduped: List[Dict[str, Any]] = []
    for row in sorted(rows, key=lambda r: (-_f(r.get("final_score"), 0.0), str(r.get("code") or ""))):
        code = str(row.get("code") or "")
        if code in seen:
            continue
        seen.add(code)
        deduped.append(row)

    payload = {
        "schema_version": "promoted_recheck_candidates_v1",
        "generated_at": now.isoformat(timespec="seconds"),
        "trading_effect": False,
        "policy_effect": True,
        "source_recheck_queue": str(RECHECK_JSON),
        "summary": {
            "rows": len(deduped),
            "skipped_blocked": skipped_blocked,
            "skipped_not_tradable": skipped_not_tradable,
            "skipped_learning_feedback": skipped_learning_feedback,
        },
        "artifacts": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
        },
        "rows": deduped,
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, deduped)
    print(json.dumps({
        "status": "OK",
        "rows": len(deduped),
        "skipped_blocked": skipped_blocked,
        "skipped_not_tradable": skipped_not_tradable,
        "out_csv": str(OUT_CSV),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
