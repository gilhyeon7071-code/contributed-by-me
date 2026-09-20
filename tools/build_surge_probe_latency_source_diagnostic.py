"""Diagnose timing and source quality for surge shadow probe failures.

This report is read-only. It separates late selection, late follow-up, and LOB
source mismatch so shadow probe failures do not get explained by guesswork.
"""
from __future__ import annotations

import csv
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SHADOW_CSV = LOG_DIR / "surge_shadow_probe_candidate_report_latest.csv"
NO_LOB_REVIEW_CSV = LOG_DIR / "no_lob_recheck_review_report_latest.csv"
FOLLOWUP_CSV = LOG_DIR / "candidate_action_followup_latest.csv"
TRACKER_CSV = LOG_DIR / "surge_shadow_probe_markout_tracker_latest.csv"
TIMEPOINT_CSV = LOG_DIR / "surge_shadow_probe_timepoint_markout_latest.csv"
LOB_CSV = LOG_DIR / "surge_lob_latest.csv"
WAIT_LOB_CSV = LOG_DIR / "surge_wait_lob_hoga_observe_latest.csv"

OUT_JSON = LOG_DIR / "surge_probe_latency_source_diagnostic_latest.json"
OUT_CSV = LOG_DIR / "surge_probe_latency_source_diagnostic_latest.csv"

KST = timezone(timedelta(hours=9))


def _now() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: List[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _code(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text.isdigit() else text


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _parse_dt(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=KST)
    return dt.astimezone(KST)


def _minutes_between(start: Any, end: Any) -> Optional[float]:
    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end)
    if not start_dt or not end_dt:
        return None
    return round((end_dt - start_dt).total_seconds() / 60.0, 4)


def _by_code(rows: Iterable[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code:
            out[code] = row
    return out


def _first_by_code(rows: Iterable[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code and code not in out:
            out[code] = row
    return out


def _timepoint_status(rows: List[Dict[str, str]], code: str) -> str:
    items = [row for row in rows if _code(row.get("code")) == code]
    if not items:
        return ""
    return "|".join(f"{row.get('target_minutes')}m:{row.get('timepoint_status')}" for row in items)


def _row(candidate: Dict[str, str], refs: Dict[str, Dict[str, Dict[str, str]]], timepoints: List[Dict[str, str]]) -> Dict[str, Any]:
    code = _code(candidate.get("code"))
    review = refs["review"].get(code, {})
    follow = refs["follow"].get(code, {})
    tracker = refs["tracker"].get(code, {})
    lob = refs["lob"].get(code, {})
    wait = refs["wait"].get(code, {})

    queue_ts = review.get("queue_ts", "")
    first_seen = follow.get("first_seen_at", "")
    tracker_observed_at = tracker.get("observed_at", "")
    lob_ts = lob.get("ts", "")
    wait_ts = wait.get("ts", "")

    first_seen_to_recheck_min = _minutes_between(first_seen, queue_ts)
    recheck_to_tracker_min = _minutes_between(queue_ts, tracker_observed_at)
    first_seen_to_tracker_min = _minutes_between(first_seen, tracker_observed_at)
    tracker_to_latest_lob_min = _minutes_between(tracker_observed_at, lob_ts)

    review_lob_ok = _truthy(review.get("lob_ok"))
    wait_lob_recheckable = str(wait.get("lob_recheck_class", "") or wait.get(None, "")).strip()
    latest_lob_available = _truthy(lob.get("lob_available"))
    latest_lob_status = lob.get("lob_status", "")
    latest_hoga_fetch_status = lob.get("hoga_fetch_status", "")

    flags: List[str] = []
    if first_seen_to_recheck_min is not None and first_seen_to_recheck_min > 30:
        flags.append("LATE_RECHECK_AFTER_FIRST_SEEN")
    if recheck_to_tracker_min is not None and recheck_to_tracker_min > 10:
        flags.append("LATE_MARKOUT_AFTER_RECHECK")
    if first_seen_to_tracker_min is not None and first_seen_to_tracker_min > 60:
        flags.append("NOT_EARLY_SURGE_ENTRY_SAMPLE")
    if review_lob_ok and not latest_lob_available:
        flags.append("LOB_REVIEW_VS_LATEST_LOB_MISMATCH")
    if latest_hoga_fetch_status == "SKIP_MAX_FETCH":
        flags.append("LOB_FETCH_SKIPPED_BY_MAX_FETCH")
    if tracker.get("tracker_status") == "SHADOW_KILL_CONDITION_TRIGGERED":
        flags.append("FORWARD_MARKOUT_KILL")

    if "LOB_REVIEW_VS_LATEST_LOB_MISMATCH" in flags or "LOB_FETCH_SKIPPED_BY_MAX_FETCH" in flags:
        primary_issue = "LOB_SOURCE_QUALITY_MISMATCH"
    elif "LATE_RECHECK_AFTER_FIRST_SEEN" in flags or "NOT_EARLY_SURGE_ENTRY_SAMPLE" in flags:
        primary_issue = "LATE_RECHECK_NOT_EARLY_ENTRY"
    elif "LATE_MARKOUT_AFTER_RECHECK" in flags:
        primary_issue = "LATE_FOLLOWUP_SAMPLE"
    elif "FORWARD_MARKOUT_KILL" in flags:
        primary_issue = "FORWARD_MARKOUT_FAILED"
    else:
        primary_issue = "NO_OBVIOUS_LATENCY_SOURCE_ISSUE"

    return {
        "code": code,
        "primary_issue": primary_issue,
        "diagnostic_flags": "|".join(flags),
        "detected_surge_type": candidate.get("detected_surge_type", ""),
        "shadow_probe_candidate": _truthy(candidate.get("shadow_probe_candidate")),
        "source_review_class": candidate.get("source_review_class", ""),
        "candidate_return_pct": round(_f(candidate.get("return_pct_since_first_seen")), 6),
        "followup_return_pct": round(_f(follow.get("return_pct_since_first_seen")), 6),
        "tracker_current_return_pct": round(_f(tracker.get("current_return_pct")), 6),
        "tracker_markout_delta_pct_points": round(_f(tracker.get("markout_delta_pct_points")), 6),
        "first_seen_at": first_seen,
        "recheck_queue_ts": queue_ts,
        "tracker_observed_at": tracker_observed_at,
        "latest_lob_ts": lob_ts,
        "wait_lob_ts": wait_ts,
        "first_seen_to_recheck_minutes": first_seen_to_recheck_min,
        "recheck_to_tracker_minutes": recheck_to_tracker_min,
        "first_seen_to_tracker_minutes": first_seen_to_tracker_min,
        "tracker_to_latest_lob_minutes": tracker_to_latest_lob_min,
        "review_lob_ok": review_lob_ok,
        "review_spread_bps": round(_f(review.get("spread_bps")), 6),
        "latest_lob_available": latest_lob_available,
        "latest_lob_status": latest_lob_status,
        "latest_hoga_fetch_status": latest_hoga_fetch_status,
        "wait_lob_recheck_class": wait_lob_recheckable,
        "timepoint_statuses": _timepoint_status(timepoints, code),
        "live_order_route_enabled": False,
        "entry_approval_changed": False,
        "trading_allowed": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }


def main() -> int:
    generated_at = _now()
    candidates = [row for row in _read_csv(SHADOW_CSV) if _truthy(row.get("shadow_probe_candidate"))]
    refs = {
        "review": _by_code(_read_csv(NO_LOB_REVIEW_CSV)),
        "follow": _first_by_code(_read_csv(FOLLOWUP_CSV)),
        "tracker": _by_code(_read_csv(TRACKER_CSV)),
        "lob": _by_code(_read_csv(LOB_CSV)),
        "wait": _by_code(_read_csv(WAIT_LOB_CSV)),
    }
    timepoints = _read_csv(TIMEPOINT_CSV)
    rows = [_row(candidate, refs, timepoints) for candidate in candidates]
    payload = {
        "generated_at": generated_at,
        "scope": "read_only_surge_probe_latency_source_diagnostic",
        "policy_note": "Diagnostic only. No live entry, order route, threshold, or gate behavior is changed.",
        "source_files": {
            "shadow_csv": str(SHADOW_CSV),
            "no_lob_review_csv": str(NO_LOB_REVIEW_CSV),
            "followup_csv": str(FOLLOWUP_CSV),
            "tracker_csv": str(TRACKER_CSV),
            "timepoint_csv": str(TIMEPOINT_CSV),
            "lob_csv": str(LOB_CSV),
            "wait_lob_csv": str(WAIT_LOB_CSV),
        },
        "summary": {
            "rows": len(rows),
            "primary_issue_counts": {key: sum(1 for row in rows if row.get("primary_issue") == key) for key in sorted({str(row.get("primary_issue")) for row in rows})},
            "live_order_route_enabled": False,
            "trading_effect": False,
            "policy_effect": False,
            "policy_change_applied": False,
        },
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"out_json": str(OUT_JSON), "out_csv": str(OUT_CSV), "summary": payload["summary"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
