"""Build a read-only quality report for recheck promotion candidates.

The report evaluates whether RECHECK_15M and PROMOTE_TO_RECHECK candidates have
fresh price evidence, positive follow-up, and remaining hard-block reasons. It
does not create orders or change entry policy.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from statistics import median
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

PLAN_CSV = LOG_DIR / "candidate_action_plan_latest.csv"
REVIEW_CSV = LOG_DIR / "candidate_action_review_latest.csv"
FOLLOWUP_CSV = LOG_DIR / "candidate_action_followup_latest.csv"
RECHECK_CSV = LOG_DIR / "candidate_action_recheck_queue_latest.csv"
PENDING_JSON = LOG_DIR / "pending_entry_status_latest.json"
P1_JSON = LOG_DIR / "p1_entry_gate_status_latest.json"

OUT_JSON = LOG_DIR / "recheck_promotion_quality_report_latest.json"
OUT_CSV = LOG_DIR / "recheck_promotion_quality_report_latest.csv"

KST = dt.timezone(dt.timedelta(hours=9))
HARD_BLOCK_TOKENS = {
    "NO_LOB_BLOCK",
    "ENTRY_CHANGE_BLOCK",
    "ENTRY_ATR_CAP",
    "HIGH_REJECTION_ENTRY_BLOCK",
    "RVOL_OVERHEAT_BLOCK",
    "SCORE_RVOL_OVERHEAT_BLOCK",
    "ORDER_IMBALANCE_EXTREME",
    "MARKOUT_NEGATIVE_BLOCK",
    "SPREAD_BLOCK",
    "KRX_ADMIN",
    "KRX_WARNING",
    "KRX_CAUTION",
    "TRADING_VALUE_FLOOR",
}


def _now() -> str:
    return dt.datetime.now(tz=KST).isoformat(timespec="seconds")


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return [{str(k): str(v) for k, v in row.items()} for row in csv.DictReader(f)]
    except Exception:
        return []


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


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _code(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    if not text:
        return ""
    return text.zfill(6)[-6:]


def _reason_keys(reason: Any) -> List[str]:
    keys: List[str] = []
    for part in str(reason or "").split("|"):
        token = part.strip().split(":", 1)[0].strip().upper()
        if token:
            keys.append(token)
    return keys


def _return_bucket(ret: float) -> str:
    if ret >= 3.0:
        return "STRONG_UP_3P"
    if ret >= 1.5:
        return "UP_1P5_TO_3P"
    if ret > 0.0:
        return "SMALL_UP"
    if ret == 0.0:
        return "FLAT_OR_NO_MOVE"
    if ret > -3.0:
        return "DOWN_0_TO_3P"
    return "DOWN_GT_3P"


def _quality_bucket(row: Dict[str, Any]) -> str:
    ret = _f(row.get("return_pct_since_first_seen"), 0.0)
    has_price = _truthy(row.get("current_price_available"))
    hard = _truthy(row.get("has_hard_block"))
    if not has_price:
        return "NO_PRICE_EVIDENCE"
    if hard:
        return "HAS_REMAINING_HARD_BLOCK"
    if ret >= 1.5:
        return "CLEAN_POSITIVE_FOLLOWUP"
    if ret > 0.0:
        return "CLEAN_SMALL_POSITIVE"
    if ret == 0.0:
        return "CLEAN_FLAT"
    return "CLEAN_NEGATIVE_FOLLOWUP"


def _stats(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    observed = [r for r in rows if _truthy(r.get("current_price_available"))]
    vals = [_f(r.get("return_pct_since_first_seen"), 0.0) for r in observed]
    pos = sum(1 for v in vals if v > 0)
    neg = sum(1 for v in vals if v < 0)
    zero = sum(1 for v in vals if v == 0)
    return {
        "n": len(rows),
        "observed_n": len(observed),
        "avg_return_pct": round(sum(vals) / len(vals), 6) if vals else None,
        "median_return_pct": round(float(median(vals)), 6) if vals else None,
        "win_rate": round(pos / len(vals), 6) if vals else None,
        "positive_n": pos,
        "negative_n": neg,
        "zero_n": zero,
        "min_return_pct": round(min(vals), 6) if vals else None,
        "max_return_pct": round(max(vals), 6) if vals else None,
    }


def _summaries(rows: List[Dict[str, Any]], group_field: str) -> List[Dict[str, Any]]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        raw = row.get(group_field)
        key = "UNKNOWN" if raw is None or str(raw) == "" else str(raw)
        groups.setdefault(key, []).append(row)
    return [{"group": k, **_stats(v)} for k, v in sorted(groups.items())]


def _load_review_index() -> Dict[tuple[str, str, str], Dict[str, str]]:
    out: Dict[tuple[str, str, str], Dict[str, str]] = {}
    for row in _read_csv(REVIEW_CSV):
        key = (_code(row.get("code")), str(row.get("source") or ""), str(row.get("reason") or "")[:120])
        out[key] = row
    return out


def _load_followup_index() -> Dict[tuple[str, str, str], Dict[str, str]]:
    out: Dict[tuple[str, str, str], Dict[str, str]] = {}
    for row in _read_csv(FOLLOWUP_CSV):
        key = (_code(row.get("code")), str(row.get("source") or ""), str(row.get("reason") or "")[:120])
        out[key] = row
    return out


def _plan_quality_rows() -> List[Dict[str, Any]]:
    review_index = _load_review_index()
    follow_index = _load_followup_index()
    rows: List[Dict[str, Any]] = []
    for row in _read_csv(PLAN_CSV):
        next_action = str(row.get("next_action") or "")
        if next_action not in {"RECHECK_15M", "PROMOTE_TO_RECHECK"}:
            continue
        code = _code(row.get("code"))
        source = str(row.get("source") or "")
        reason = str(row.get("reason") or "")
        key = (code, source, reason[:120])
        review = review_index.get(key, {})
        follow = follow_index.get(key, {})
        ret = _f(row.get("return_pct_since_first_seen"), _f(follow.get("return_pct_since_first_seen"), 0.0))
        reason_keys = _reason_keys(reason)
        hard = sorted(k for k in reason_keys if k in HARD_BLOCK_TOKENS)
        out: Dict[str, Any] = {
            "source": source,
            "code": code,
            "next_action": next_action,
            "action_state": str(row.get("action_state") or ""),
            "review_bucket": str(row.get("review_bucket") or review.get("review_bucket") or ""),
            "reason": reason,
            "reason_keys": "|".join(reason_keys),
            "has_hard_block": bool(hard),
            "hard_block_keys": "|".join(hard),
            "current_price_available": _truthy(row.get("current_price_available") or follow.get("current_price_available")),
            "return_pct_since_first_seen": ret,
            "return_bucket": _return_bucket(ret),
            "current_change_pct": _f(follow.get("current_change_pct"), 0.0),
            "current_trading_value": _f(follow.get("current_trading_value"), 0.0),
            "trading_allowed": _truthy(row.get("trading_allowed")),
            "policy_effect": False,
            "trading_effect": False,
        }
        out["quality_bucket"] = _quality_bucket(out)
        rows.append(out)
    return rows


def _recheck_queue_quality_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in _read_csv(RECHECK_CSV):
        next_action = str(row.get("next_action") or "")
        if next_action not in {"RECHECK_15M", "PROMOTE_TO_RECHECK"}:
            continue
        reason = str(row.get("reason") or "")
        reason_keys = _reason_keys(reason)
        hard = sorted(k for k in reason_keys if k in HARD_BLOCK_TOKENS)
        ret = _f(row.get("return_pct_since_first_seen"), 0.0)
        out: Dict[str, Any] = {
            "source": str(row.get("source") or ""),
            "code": _code(row.get("code")),
            "next_action": next_action,
            "action_state": str(row.get("action_state") or ""),
            "review_bucket": str(row.get("review_bucket") or ""),
            "reason": reason,
            "reason_keys": "|".join(reason_keys),
            "has_hard_block": bool(hard),
            "hard_block_keys": "|".join(hard),
            "current_price_available": True,
            "return_pct_since_first_seen": ret,
            "return_bucket": _return_bucket(ret),
            "current_change_pct": 0.0,
            "current_trading_value": 0.0,
            "trading_allowed": False,
            "policy_effect": False,
            "trading_effect": False,
        }
        out["quality_bucket"] = _quality_bucket(out)
        rows.append(out)
    return rows


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "section",
        "group",
        "n",
        "observed_n",
        "avg_return_pct",
        "median_return_pct",
        "win_rate",
        "positive_n",
        "negative_n",
        "zero_n",
        "min_return_pct",
        "max_return_pct",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    pending = _read_json(PENDING_JSON)
    p1 = _read_json(P1_JSON)
    plan_rows = _plan_quality_rows()
    queue_rows = _recheck_queue_quality_rows()
    sections = {
        "plan_by_next_action": _summaries(plan_rows, "next_action"),
        "plan_by_quality_bucket": _summaries(plan_rows, "quality_bucket"),
        "plan_by_review_bucket": _summaries(plan_rows, "review_bucket"),
        "plan_by_hard_block": _summaries(plan_rows, "has_hard_block"),
        "queue_by_next_action": _summaries(queue_rows, "next_action"),
        "queue_by_quality_bucket": _summaries(queue_rows, "quality_bucket"),
    }
    flat: List[Dict[str, Any]] = []
    for section, rows in sections.items():
        for row in rows:
            flat.append({"section": section, **row})
    payload = {
        "generated_at": _now(),
        "schema_version": "recheck_promotion_quality_report_v1",
        "trading_effect": False,
        "policy_effect": False,
        "current_gate": {
            "market_regime": pending.get("market_regime") or p1.get("market_regime"),
            "entry_gate_decision_before_p1": p1.get("entry_gate_decision_before_p1"),
            "entry_gate_reason_before_p1": p1.get("entry_gate_reason_before_p1"),
            "max_new": pending.get("max_new"),
            "max_new_surge": pending.get("max_new_surge"),
            "entry_ready": pending.get("entry_ready"),
        },
        "summary_sections": sections,
        "artifacts": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
            "candidate_action_plan": str(PLAN_CSV),
            "candidate_action_review": str(REVIEW_CSV),
            "candidate_action_followup": str(FOLLOWUP_CSV),
            "candidate_action_recheck_queue": str(RECHECK_CSV),
        },
        "interpretation_hint": {
            "clean_positive_followup": "price observed, no remaining hard block, return since first seen >= 1.5%",
            "has_remaining_hard_block": "still observe/recheck only; not an entry permission",
        },
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, flat)
    print(json.dumps({"status": "OK", "plan_rows": len(plan_rows), "queue_rows": len(queue_rows), "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
