"""Build a read-only LOB resolution report for MICROSTRUCTURE_WAIT rows.

The report connects the promote-probe review output with observation-only LOB
queues. It does not approve entries or change production policy.
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

PROMOTE_REVIEW_CSV = LOG_DIR / "promote_probe_review_report_latest.csv"
WAIT_OBSERVE_CSV = LOG_DIR / "surge_wait_lob_hoga_observe_latest.csv"
NO_LOB_RECHECK_CSV = LOG_DIR / "surge_no_lob_recheck_queue_latest.csv"
SCORE_RVOL_RECHECK_CSV = LOG_DIR / "surge_score_rvol_conditional_recheck_queue_latest.csv"
SURGE_CSV = LOG_DIR / "surge_realtime_latest.csv"

OUT_JSON = LOG_DIR / "microstructure_wait_lob_resolution_report_latest.json"
OUT_CSV = LOG_DIR / "microstructure_wait_lob_resolution_report_latest.csv"

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


def _tokens(text: Any) -> List[str]:
    return [part.strip() for part in str(text or "").replace(";", "|").split("|") if part.strip()]


def _remaining_blockers(*values: Any) -> List[str]:
    joined = "|".join(str(v or "") for v in values)
    blockers: List[str] = []
    for token in [
        "HIGH_REJECTION_ENTRY_BLOCK",
        "SCORE_RVOL_OVERHEAT_BLOCK",
        "ENTRY_CHANGE_BLOCK",
        "ENTRY_ATR_CAP",
        "NO_REFERENCE",
        "NO_REALTIME_SURGE",
        "ORDER_IMBALANCE_EXTREME",
        "SPREAD",
        "KRX_CAUTION",
    ]:
        if token in joined:
            blockers.append(token)
    return blockers


def _is_lob_ok(row: Dict[str, str]) -> bool:
    status = str(row.get("lob_status") or "").strip().upper()
    return _truthy(row.get("lob_available")) and status in {"OK", "LOB_OK", "AVAILABLE"}


def _classify(
    review: Dict[str, str],
    observe: Dict[str, str],
    no_lob: Dict[str, str],
    score_rvol: Dict[str, str],
    surge: Dict[str, str],
) -> Dict[str, Any]:
    blockers = _remaining_blockers(
        review.get("entry_reason"),
        review.get("reason"),
        review.get("failed_checks"),
        observe.get("entry_reason"),
        observe.get("exclude_reasons"),
        no_lob.get("entry_reason"),
        no_lob.get("exclude_reasons"),
        no_lob.get("remaining_hard_blockers"),
        score_rvol.get("entry_reason"),
        score_rvol.get("exclude_reasons"),
        score_rvol.get("remaining_hard_blockers"),
        surge.get("entry_reason"),
        surge.get("exclude_reasons"),
    )

    in_no_lob_queue = bool(no_lob)
    in_score_rvol_queue = bool(score_rvol)
    in_observe = bool(observe)
    latest_lob_ok = _is_lob_ok(surge)
    observe_lob_ok = _is_lob_ok(observe)
    queue_lob_ok = _is_lob_ok(no_lob) or _is_lob_ok(score_rvol)

    if in_no_lob_queue and queue_lob_ok and blockers:
        resolution_class = "LOB_RESOLVED_RECHECK_QUEUE_WITH_CURRENT_BLOCKER"
        action_hint = "RECHECK_AFTER_CURRENT_BLOCKER_CLEARS"
    elif in_no_lob_queue and queue_lob_ok:
        resolution_class = "LOB_RESOLVED_RECHECK_QUEUE"
        action_hint = "REVIEW_RECHECK_ONLY_NOT_ENTRY_APPROVAL"
    elif in_score_rvol_queue and queue_lob_ok and blockers:
        resolution_class = "LOB_RESOLVED_SCORE_RVOL_WITH_CURRENT_BLOCKER"
        action_hint = "KEEP_CONDITIONAL_REVIEW_UNTIL_BLOCKER_CLEARS"
    elif in_score_rvol_queue and queue_lob_ok:
        resolution_class = "LOB_RESOLVED_SCORE_RVOL_CONDITIONAL"
        action_hint = "KEEP_CONDITIONAL_SCORE_RVOL_REVIEW_ONLY"
    elif (latest_lob_ok or observe_lob_ok) and blockers:
        resolution_class = "LOB_RESOLVED_BUT_RISKY"
        action_hint = "DO_NOT_PROMOTE_UNTIL_REMAINING_BLOCKERS_CLEAR"
    elif in_observe:
        resolution_class = "OBSERVED_LOB_NOT_CLEAN"
        action_hint = "WAIT_FOR_NEXT_OBSERVATION"
    else:
        resolution_class = "NOT_PROBED_OR_STALE_LOB"
        action_hint = "IMPROVE_OBSERVATION_COVERAGE_FIRST"

    if "NO_REFERENCE" in blockers:
        resolution_class = "NO_REFERENCE_OR_NOT_REALTIME"
        action_hint = "NOT_A_RUNTIME_ENTRY_CANDIDATE"

    return {
        "resolution_class": resolution_class,
        "action_hint": action_hint,
        "remaining_blockers": "|".join(blockers),
        "in_wait_observe": in_observe,
        "in_no_lob_recheck_queue": in_no_lob_queue,
        "in_score_rvol_recheck_queue": in_score_rvol_queue,
        "latest_lob_ok": latest_lob_ok,
        "observe_lob_ok": observe_lob_ok,
        "queue_lob_ok": queue_lob_ok,
    }


def _build_rows() -> List[Dict[str, Any]]:
    reviews = [
        row
        for row in _read_csv(PROMOTE_REVIEW_CSV)
        if str(row.get("review_class") or "") == "MICROSTRUCTURE_WAIT"
    ]
    observe_by_code = _by_code(_read_csv(WAIT_OBSERVE_CSV))
    no_lob_by_code = _by_code(_read_csv(NO_LOB_RECHECK_CSV))
    score_rvol_by_code = _by_code(_read_csv(SCORE_RVOL_RECHECK_CSV))
    surge_by_code = _by_code(_read_csv(SURGE_CSV))

    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for review in reviews:
        code = _code(review.get("code"))
        if not code or code in seen:
            continue
        seen.add(code)
        observe = observe_by_code.get(code, {})
        no_lob = no_lob_by_code.get(code, {})
        score_rvol = score_rvol_by_code.get(code, {})
        surge = surge_by_code.get(code, {})
        cls = _classify(review, observe, no_lob, score_rvol, surge)
        out.append(
            {
                "code": code,
                "name": review.get("name", ""),
                "return_pct_since_first_seen": round(_f(review.get("return_pct_since_first_seen")), 6),
                "review_entry_reason": review.get("entry_reason", ""),
                "review_failed_checks": review.get("failed_checks", ""),
                "latest_entry_reason": surge.get("entry_reason", ""),
                "latest_exclude_reasons": surge.get("exclude_reasons", ""),
                "latest_lob_status": surge.get("lob_status", ""),
                "latest_spread_bps": round(_f(surge.get("spread_bps")), 6),
                "observe_probe_class": observe.get("probe_class", ""),
                "observe_hoga_fetch_status": observe.get("hoga_fetch_status", ""),
                "observe_lob_status": observe.get("lob_status", ""),
                "no_lob_recheck_reason": no_lob.get("recheck_reason", ""),
                "score_rvol_recheck_reason": score_rvol.get("recheck_reason", ""),
                **cls,
                "entry_approval_changed": False,
                "trading_allowed": False,
                "policy_effect": False,
                "policy_change_applied": False,
            }
        )
    return out


def _summarize(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    class_counts = Counter(str(row.get("resolution_class") or "") for row in rows)
    blockers = Counter()
    for row in rows:
        for token in _tokens(row.get("remaining_blockers")):
            blockers[token] += 1
    return {
        "rows": len(rows),
        "resolution_class_counts": dict(sorted(class_counts.items())),
        "remaining_blocker_counts": dict(sorted(blockers.items())),
        "recheck_queue_rows": sum(1 for row in rows if row.get("in_no_lob_recheck_queue")),
        "score_rvol_queue_rows": sum(1 for row in rows if row.get("in_score_rvol_recheck_queue")),
        "wait_observe_rows": sum(1 for row in rows if row.get("in_wait_observe")),
        "latest_lob_ok_rows": sum(1 for row in rows if row.get("latest_lob_ok")),
        "queue_lob_ok_rows": sum(1 for row in rows if row.get("queue_lob_ok")),
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
        "source_files": {
            "promote_review_csv": str(PROMOTE_REVIEW_CSV),
            "wait_observe_csv": str(WAIT_OBSERVE_CSV),
            "no_lob_recheck_csv": str(NO_LOB_RECHECK_CSV),
            "score_rvol_recheck_csv": str(SCORE_RVOL_RECHECK_CSV),
            "surge_csv": str(SURGE_CSV),
        },
        "scope": "read_only_microstructure_wait_lob_resolution",
        "policy_note": "Observation-only report. No entry approval, order route, threshold, or gate behavior is changed.",
        "summary": _summarize(rows),
        "rows": rows,
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"out_json": str(OUT_JSON), "out_csv": str(OUT_CSV), "summary": payload["summary"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
