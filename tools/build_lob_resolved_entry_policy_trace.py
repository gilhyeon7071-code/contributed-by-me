"""Trace entry-policy status for LOB-resolved MICROSTRUCTURE_WAIT candidates.

This read-only report answers one narrow question: if a missed-move candidate's
LOB blocker is resolved in the observation queue, what still prevents it from
being treated as an entry-policy candidate?
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

MICRO_CSV = LOG_DIR / "microstructure_wait_lob_resolution_report_latest.csv"
NO_LOB_RECHECK_CSV = LOG_DIR / "surge_no_lob_recheck_queue_latest.csv"
SURGE_CSV = LOG_DIR / "surge_realtime_latest.csv"
PLAN_CSV = LOG_DIR / "candidate_action_plan_latest.csv"
PARAMS_JSON = ROOT / "paper" / "surge_params.json"

OUT_JSON = LOG_DIR / "lob_resolved_entry_policy_trace_latest.json"
OUT_CSV = LOG_DIR / "lob_resolved_entry_policy_trace_latest.csv"

KST = dt.timezone(dt.timedelta(hours=9))


def _now() -> str:
    return dt.datetime.now(tz=KST).isoformat(timespec="seconds")


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return [{str(k): str(v) for k, v in row.items()} for row in csv.DictReader(f)]
    except Exception:
        return []


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _code(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text.zfill(6)[-6:] if text else ""


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value).strip()
        if not text:
            return float(default)
        return float(text)
    except Exception:
        return float(default)


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on", "t"}


def _by_code(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code:
            out[code] = row
    return out


def _plan_rows_by_code(rows: List[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    out: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code:
            out.setdefault(code, []).append(row)
    return out


def _tokens(text: Any) -> List[str]:
    return [part.strip() for part in str(text or "").replace(";", "|").split("|") if part.strip()]


def _hard_tokens(*values: Any) -> List[str]:
    joined = "|".join(str(v or "") for v in values)
    out: List[str] = []
    for token in [
        "NO_LOB_BLOCK",
        "HIGH_REJECTION_ENTRY_BLOCK",
        "SCORE_RVOL_OVERHEAT_BLOCK",
        "ENTRY_CHANGE_BLOCK",
        "ENTRY_ATR_CAP",
        "SPREAD_BLOCK",
        "ORDERFLOW_RISK_BLOCK",
        "ORDERFLOW_PAUSE",
        "ORDER_IMBALANCE_EXTREME",
        "KRX_CAUTION",
        "KRX_WARNING",
        "KRX_RISK",
        "KRX_ADMIN",
        "NO_REFERENCE",
        "NO_REALTIME_SURGE",
        "NEWS_NEGATIVE",
        "NEWS_IMPLICATION_BLOCK",
    ]:
        if token in joined:
            out.append(token)
    return out


def _is_clean_lob_resolved(row: Dict[str, str]) -> bool:
    return str(row.get("resolution_class") or "") == "LOB_RESOLVED_RECHECK_QUEUE"


def _status_for(
    micro: Dict[str, str],
    queue: Dict[str, str],
    surge: Dict[str, str],
    plans: List[Dict[str, str]],
    params: Dict[str, Any],
) -> Dict[str, Any]:
    queue_lob_ok = _truthy(queue.get("lob_available")) and str(queue.get("lob_status") or "").upper() == "OK"
    latest_still_no_lob = "NO_LOB_BLOCK" in str(surge.get("exclude_reasons") or surge.get("entry_reason") or "")
    queue_entry_approval_changed = _truthy(queue.get("entry_approval_changed"))
    queue_policy_change = _truthy(queue.get("policy_change"))
    plan_sources = sorted({str(row.get("source") or "") for row in plans if row})
    plan_states = sorted({str(row.get("action_state") or "") for row in plans if row})
    plan_reasons = sorted({str(row.get("reason") or row.get("action_reason") or "") for row in plans if row})

    queue_hards = _hard_tokens(queue.get("exclude_reasons"), queue.get("entry_reason"))
    latest_hards = _hard_tokens(surge.get("exclude_reasons"), surge.get("entry_reason"))
    policy_relaxations = _hard_tokens(surge.get("paper_policy_relaxations"))
    resolved_by_queue = "NO_LOB_BLOCK" in queue_hards and queue_lob_ok
    remaining_excluding_no_lob = [x for x in latest_hards if x != "NO_LOB_BLOCK"]

    change_pct = _f(queue.get("change_pct"), _f(surge.get("change_pct")))
    rvol20 = _f(queue.get("rvol20"), _f(surge.get("rvol20")))
    spread_bps = _f(queue.get("spread_bps"), _f(surge.get("spread_bps")))
    trading_value = _f(queue.get("trading_value"), _f(surge.get("trading_value")))
    score_final = _f(queue.get("surge_score_final"), _f(surge.get("surge_score_final")))

    max_entry_change = _f(params.get("max_entry_change_pct"), 0.16)
    max_rvol20 = _f(params.get("max_rvol20"), 5.0)
    max_spread_bps = _f(params.get("max_spread_bps"), 40.0)
    min_trading_value = _f(params.get("trading_value_floor"), 1_000_000_000.0)

    checks = {
        "queue_lob_ok": queue_lob_ok,
        "resolved_no_lob_in_queue": resolved_by_queue,
        "queue_entry_approval_changed": queue_entry_approval_changed,
        "queue_policy_change": queue_policy_change,
        "change_under_static_cap": change_pct <= max_entry_change,
        "rvol_under_static_max": rvol20 <= max_rvol20,
        "spread_under_static_max": spread_bps <= max_spread_bps,
        "trading_value_above_floor": trading_value >= min_trading_value,
        "latest_still_no_lob": latest_still_no_lob,
        "remaining_hard_excluding_no_lob": bool(remaining_excluding_no_lob),
    }

    if not queue:
        trace_class = "NO_CURRENT_RECHECK_QUEUE_ROW"
        action_hint = "WAIT_FOR_LOB_RECHECK_QUEUE"
    elif not queue_lob_ok:
        trace_class = "QUEUE_LOB_NOT_CLEAN"
        action_hint = "WAIT_FOR_CLEAN_LOB"
    elif remaining_excluding_no_lob:
        trace_class = "LOB_RESOLVED_BUT_POLICY_BLOCKED"
        action_hint = "DO_NOT_PROMOTE_UNTIL_POLICY_BLOCKERS_CLEAR"
    elif not queue_entry_approval_changed and not queue_policy_change:
        trace_class = "LOB_RESOLVED_OBSERVE_ONLY_GAP"
        action_hint = "POLICY_REVIEW_REQUIRED_FOR_ANY_ENTRY_LINK"
    else:
        trace_class = "LOB_RESOLVED_POLICY_CHANGED"
        action_hint = "VERIFY_OFFICIAL_ENTRY_CHAIN_BEFORE_USE"

    return {
        "trace_class": trace_class,
        "action_hint": action_hint,
        "queue_lob_ok": queue_lob_ok,
        "resolved_no_lob_in_queue": resolved_by_queue,
        "latest_still_no_lob": latest_still_no_lob,
        "queue_entry_approval_changed": queue_entry_approval_changed,
        "queue_policy_change": queue_policy_change,
        "remaining_hard_excluding_no_lob": "|".join(remaining_excluding_no_lob),
        "queue_hard_tokens": "|".join(queue_hards),
        "latest_hard_tokens": "|".join(latest_hards),
        "latest_policy_relaxation_tokens": "|".join(policy_relaxations),
        "plan_sources": "|".join(plan_sources),
        "plan_states": "|".join(plan_states),
        "plan_reasons": "|".join(plan_reasons),
        "change_pct": round(change_pct, 6),
        "max_entry_change_pct": round(max_entry_change, 6),
        "rvol20": round(rvol20, 6),
        "max_rvol20": round(max_rvol20, 6),
        "spread_bps": round(spread_bps, 6),
        "max_spread_bps": round(max_spread_bps, 6),
        "trading_value": round(trading_value, 2),
        "trading_value_floor": round(min_trading_value, 2),
        "surge_score_final": round(score_final, 6),
        "checks_json": json.dumps(checks, ensure_ascii=False, sort_keys=True),
    }


def _build_rows() -> List[Dict[str, Any]]:
    micro_rows = [row for row in _read_csv(MICRO_CSV) if _is_clean_lob_resolved(row)]
    queue_by_code = _by_code(_read_csv(NO_LOB_RECHECK_CSV))
    surge_by_code = _by_code(_read_csv(SURGE_CSV))
    plans_by_code = _plan_rows_by_code(_read_csv(PLAN_CSV))
    params = _read_json(PARAMS_JSON)

    rows: List[Dict[str, Any]] = []
    for micro in micro_rows:
        code = _code(micro.get("code"))
        queue = queue_by_code.get(code, {})
        surge = surge_by_code.get(code, {})
        plans = plans_by_code.get(code, [])
        status = _status_for(micro, queue, surge, plans, params)
        rows.append(
            {
                "code": code,
                "name": micro.get("name", ""),
                "micro_resolution_class": micro.get("resolution_class", ""),
                "micro_action_hint": micro.get("action_hint", ""),
                "return_pct_since_first_seen": micro.get("return_pct_since_first_seen", ""),
                "queue_ts": queue.get("ts", ""),
                "queue_entry_reason": queue.get("entry_reason", ""),
                "queue_recheck_reason": queue.get("recheck_reason", ""),
                "latest_ts": surge.get("ts", ""),
                "latest_entry_reason": surge.get("entry_reason", ""),
                "latest_exclude_reasons": surge.get("exclude_reasons", ""),
                "latest_paper_policy_relaxations": surge.get("paper_policy_relaxations", ""),
                **status,
                "entry_approval_changed": False,
                "trading_allowed": False,
                "policy_effect": False,
                "policy_change_applied": False,
            }
        )
    return rows


def _summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    class_counts = Counter(str(row.get("trace_class") or "") for row in rows)
    blockers = Counter()
    for row in rows:
        for token in _tokens(row.get("remaining_hard_excluding_no_lob")):
            blockers[token] += 1
    return {
        "rows": len(rows),
        "trace_class_counts": dict(sorted(class_counts.items())),
        "remaining_hard_excluding_no_lob_counts": dict(sorted(blockers.items())),
        "observe_only_gap_rows": sum(1 for row in rows if row.get("trace_class") == "LOB_RESOLVED_OBSERVE_ONLY_GAP"),
        "policy_blocked_rows": sum(1 for row in rows if row.get("trace_class") == "LOB_RESOLVED_BUT_POLICY_BLOCKED"),
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys()) if rows else ["code"]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    rows = _build_rows()
    payload = {
        "generated_at": _now(),
        "scope": "read_only_lob_resolved_entry_policy_trace",
        "policy_note": "Observation-only trace. No entry approval, order route, threshold, or gate behavior is changed.",
        "source_files": {
            "micro_csv": str(MICRO_CSV),
            "no_lob_recheck_csv": str(NO_LOB_RECHECK_CSV),
            "surge_csv": str(SURGE_CSV),
            "plan_csv": str(PLAN_CSV),
            "params_json": str(PARAMS_JSON),
        },
        "summary": _summary(rows),
        "rows": rows,
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"out_json": str(OUT_JSON), "out_csv": str(OUT_CSV), "summary": payload["summary"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
