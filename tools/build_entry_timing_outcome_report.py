"""Build a read-only entry timing outcome report.

This report compares candidate timing buckets such as RECHECK_15M,
PROMOTE_TO_RECHECK, NOW, and BLOCKED. It does not create orders, change gates,
or modify trading policy.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

RECHECK_CSV = LOG_DIR / "candidate_action_recheck_queue_latest.csv"
PLAN_CSV = LOG_DIR / "candidate_action_plan_latest.csv"
FOLLOWUP_CSV = LOG_DIR / "candidate_action_followup_latest.csv"
SURGE_CSV = LOG_DIR / "surge_realtime_latest.csv"
POST_ENTRY_JSON = LOG_DIR / "post_entry_learning_latest.json"
PENDING_JSON = LOG_DIR / "pending_entry_status_latest.json"
P1_JSON = LOG_DIR / "p1_entry_gate_status_latest.json"

OUT_JSON = LOG_DIR / "entry_timing_outcome_report_latest.json"
OUT_CSV = LOG_DIR / "entry_timing_outcome_report_latest.csv"

KST = dt.timezone(dt.timedelta(hours=9))


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


def _code(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    if not text:
        return ""
    return text.zfill(6)[-6:]


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _return_stats(rows: Iterable[Dict[str, Any]], ret_key: str = "return_pct") -> Dict[str, Any]:
    vals: List[float] = []
    positive = 0
    negative = 0
    zero = 0
    observed = 0
    for row in rows:
        if not bool(row.get("observed", True)):
            continue
        observed += 1
        ret = _f(row.get(ret_key), 0.0)
        vals.append(ret)
        if ret > 0:
            positive += 1
        elif ret < 0:
            negative += 1
        else:
            zero += 1
    n = len(vals)
    return {
        "n": int(len(list(rows))) if not isinstance(rows, list) else len(rows),
        "observed_n": observed,
        "avg_return_pct": round(sum(vals) / n, 6) if n else None,
        "median_return_pct": round(float(median(vals)), 6) if n else None,
        "win_rate": round(positive / n, 6) if n else None,
        "positive_n": positive,
        "negative_n": negative,
        "zero_n": zero,
        "min_return_pct": round(min(vals), 6) if n else None,
        "max_return_pct": round(max(vals), 6) if n else None,
    }


def _summarize(rows: List[Dict[str, Any]], group_key: str, ret_key: str = "return_pct") -> List[Dict[str, Any]]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        key = str(row.get(group_key) or "UNKNOWN")
        groups.setdefault(key, []).append(row)
    out: List[Dict[str, Any]] = []
    for key, items in sorted(groups.items()):
        stat = _return_stats(items, ret_key=ret_key)
        out.append({"group": key, **stat})
    return out


def _plan_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in _read_csv(PLAN_CSV):
        ret = _f(row.get("return_pct_since_first_seen"), 0.0)
        rows.append(
            {
                "source": "candidate_action_plan",
                "timing_group": str(row.get("next_action") or row.get("action_state") or "UNKNOWN"),
                "action_state": str(row.get("action_state") or ""),
                "code": _code(row.get("code")),
                "reason": str(row.get("action_reason") or row.get("reason") or ""),
                "return_pct": ret,
                "observed": _truthy(row.get("current_price_available")),
                "review_bucket": str(row.get("review_bucket") or ""),
                "trading_allowed": _truthy(row.get("trading_allowed")),
            }
        )
    return rows


def _followup_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in _read_csv(FOLLOWUP_CSV):
        ret = _f(row.get("return_pct_since_first_seen"), 0.0)
        rows.append(
            {
                "source": "candidate_action_followup",
                "timing_group": str(row.get("action_state") or "UNKNOWN"),
                "action_state": str(row.get("action_state") or ""),
                "code": _code(row.get("code")),
                "reason": str(row.get("reason") or ""),
                "return_pct": ret,
                "observed": _truthy(row.get("current_price_available")),
                "review_bucket": "",
                "trading_allowed": _truthy(row.get("trading_allowed")),
            }
        )
    return rows


def _recheck_queue_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in _read_csv(RECHECK_CSV):
        rows.append(
            {
                "source": "candidate_action_recheck_queue",
                "timing_group": str(row.get("next_action") or "UNKNOWN"),
                "action_state": str(row.get("action_state") or ""),
                "code": _code(row.get("code")),
                "reason": str(row.get("action_reason") or row.get("reason") or ""),
                "return_pct": _f(row.get("return_pct_since_first_seen"), 0.0),
                "observed": True,
                "review_bucket": str(row.get("review_bucket") or ""),
                "recheck_status": str(row.get("recheck_status") or ""),
                "trading_allowed": False,
            }
        )
    return rows


def _surge_timing_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in _read_csv(SURGE_CSV):
        timing = str(row.get("entry_timing") or "UNKNOWN").upper()
        rows.append(
            {
                "source": "surge_realtime",
                "timing_group": timing,
                "action_state": str(row.get("entry_decision") or ""),
                "code": _code(row.get("code")),
                "reason": str(row.get("entry_reason") or row.get("exclude_reasons") or ""),
                "return_pct": _f(row.get("change_pct"), 0.0) * 100.0,
                "observed": _f(row.get("current_price"), 0.0) > 0,
                "review_bucket": "",
                "trading_allowed": _truthy(row.get("entry_allowed")),
            }
        )
    return rows


def _post_entry_rows() -> List[Dict[str, Any]]:
    doc = _read_json(POST_ENTRY_JSON)
    src = doc.get("rows") if isinstance(doc.get("rows"), list) else []
    rows: List[Dict[str, Any]] = []
    for row in src:
        if not isinstance(row, dict):
            continue
        rows.append(
            {
                "source": "post_entry_learning",
                "timing_group": str(row.get("entry_origin") or "UNKNOWN"),
                "action_state": str(row.get("entry_action") or ""),
                "code": _code(row.get("code")),
                "reason": str(row.get("learning_bucket") or row.get("learning_policy_action") or ""),
                "return_pct": _f(row.get("unrealized_return_pct"), 0.0),
                "observed": _truthy(row.get("current_price_available")),
                "review_bucket": str(row.get("learning_bucket") or ""),
                "trading_allowed": False,
            }
        )
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
    plan_rows = _plan_rows()
    follow_rows = _followup_rows()
    recheck_rows = _recheck_queue_rows()
    surge_rows = _surge_timing_rows()
    post_rows = _post_entry_rows()

    sections = {
        "plan_next_action": _summarize(plan_rows, "timing_group"),
        "followup_action_state": _summarize(follow_rows, "timing_group"),
        "recheck_queue_next_action": _summarize(recheck_rows, "timing_group"),
        "surge_entry_timing_current_change": _summarize(surge_rows, "timing_group"),
        "post_entry_origin": _summarize(post_rows, "timing_group"),
    }
    flat_rows: List[Dict[str, Any]] = []
    for section, rows in sections.items():
        for row in rows:
            flat_rows.append({"section": section, **row})

    payload = {
        "generated_at": _now(),
        "schema_version": "entry_timing_outcome_report_v1",
        "trading_effect": False,
        "policy_effect": False,
        "current_gate": {
            "market_regime": pending.get("market_regime") or p1.get("market_regime"),
            "entry_gate_decision_before_p1": p1.get("entry_gate_decision_before_p1"),
            "entry_gate_reason_before_p1": p1.get("entry_gate_reason_before_p1"),
            "max_new": pending.get("max_new"),
            "max_new_surge": pending.get("max_new_surge"),
            "max_new_zero_reason": pending.get("max_new_zero_reason"),
            "entry_ready": pending.get("entry_ready"),
            "candidates_after_caps": pending.get("candidates_after_caps"),
        },
        "summary_sections": sections,
        "artifacts": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
            "candidate_action_recheck_queue": str(RECHECK_CSV),
            "candidate_action_plan": str(PLAN_CSV),
            "candidate_action_followup": str(FOLLOWUP_CSV),
            "surge_realtime": str(SURGE_CSV),
            "post_entry_learning": str(POST_ENTRY_JSON),
        },
        "interpretation_hint": {
            "return_pct_basis": "candidate queues use return_pct_since_first_seen; surge NOW/BLOCKED uses current day change_pct, not forward return",
            "trading_policy": "read-only; no timing bucket is promoted to entry approval by this report",
        },
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, flat_rows)
    print(json.dumps({
        "status": "OK",
        "sections": list(sections.keys()),
        "out_json": str(OUT_JSON),
        "out_csv": str(OUT_CSV),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
