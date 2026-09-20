from __future__ import annotations

import csv
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
OUT_JSON = LOG_DIR / "surge_no_entry_hard_blocker_policy_summary_latest.json"
OUT_CSV = LOG_DIR / "surge_no_entry_hard_blocker_policy_summary_latest.csv"
KST = timezone(timedelta(hours=9))
FIELDS = [
    "blocker", "status", "evidence", "sample_rows", "candidate_rows",
    "possible_over_suppression_rows", "recommended_action", "policy_change",
    "entry_approval_changed", "research_only",
]


def _now() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def _load_json(name: str) -> Dict[str, Any]:
    path = LOG_DIR / name
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in FIELDS})


def _bucket_count(summary: Dict[str, Any], bucket: str) -> int:
    for row in summary.get("rvol_bucket_counts", []) or []:
        if row.get("bucket") == bucket:
            return int(row.get("count") or 0)
    return 0


def _add(rows: List[Dict[str, Any]], blocker: str, status: str, evidence: str, sample_rows: Any, candidate_rows: Any, possible_over: Any, action: str) -> None:
    rows.append({
        "blocker": blocker,
        "status": status,
        "evidence": evidence,
        "sample_rows": int(sample_rows or 0),
        "candidate_rows": int(candidate_rows or 0),
        "possible_over_suppression_rows": int(possible_over or 0),
        "recommended_action": action,
        "policy_change": False,
        "entry_approval_changed": False,
        "research_only": True,
    })


def build() -> Dict[str, Any]:
    ev = _load_json("surge_ev_candidate_zero_diagnostic_latest.json").get("summary", {})
    hr = _load_json("surge_high_rejection_policy_fitness_latest.json").get("summary", {})
    hrg = _load_json("surge_high_rejection_gray_zone_markout_watch_latest.json").get("summary", {})
    sr = _load_json("surge_score_rvol_overheat_policy_fitness_latest.json").get("summary", {})
    rv = _load_json("surge_rvol_overheat_policy_fitness_latest.json").get("summary", {})
    ec = _load_json("surge_entry_change_policy_fitness_latest.json").get("summary", {})
    ecw = _load_json("surge_entry_change_clean_continuation_watch_latest.json").get("summary", {})

    rows: List[Dict[str, Any]] = []
    _add(
        rows,
        "HIGH_REJECTION_ENTRY_BLOCK",
        "WATCH_GRAY_ZONE_ONLY",
        f"fitness_rows={hr.get('rows')}; gray_zone_rows={hrg.get('gray_zone_rows')}; watch_only_rows={hrg.get('watch_only_rows')}; history_rows={hrg.get('history_rows')}; evaluable_rows={hrg.get('evaluable_rows')}",
        hr.get("rows", 0),
        0,
        0,
        "Keep production block; accumulate gray-zone markout only.",
    )
    _add(
        rows,
        "SCORE_RVOL_OVERHEAT_BLOCK",
        "SUPPORTED_FAIL_CLOSED",
        f"rows={sr.get('rows')}; rvol_3_5={_bucket_count(sr, '3-5')}; rvol_ge5={_bucket_count(sr, '>=5')}; conditional_queue_actual={sr.get('conditional_queue_rows_actual')}; eligible_by_recalc={sr.get('conditional_queue_eligible_by_recalc')}; possible_over={sr.get('possible_over_suppression_rows')}",
        sr.get("rows", 0),
        sr.get("conditional_queue_eligible_by_recalc", 0),
        sr.get("possible_over_suppression_rows", 0),
        "No relaxation support; conditional queue eligibility remains zero.",
    )
    _add(
        rows,
        "RVOL_OVERHEAT_BLOCK",
        "SUPPORTED_FAIL_CLOSED",
        f"rows={rv.get('rows')}; unique={rv.get('unique_codes')}; policy_max_rvol20={rv.get('max_rvol20')}; avg_rvol20={rv.get('avg_rvol20')}; max_seen_rvol20={rv.get('max_seen_rvol20')}; possible_over={rv.get('possible_over_suppression_rows')}",
        rv.get("rows", 0),
        0,
        rv.get("possible_over_suppression_rows", 0),
        "No relaxation support; exact reason-key diagnostic avoids SCORE_RVOL substring false positives.",
    )
    _add(
        rows,
        "ENTRY_CHANGE_BLOCK_16PCT",
        "NO_RELAXATION_SUPPORT_CURRENT_SAMPLE",
        f"review_rows={ec.get('review_rows')}; policy_candidate_rows={ec.get('candidate_rows')}; possible_over={ec.get('possible_over_suppression_rows')}; clean_watch_rows={ecw.get('review_rows')}; clean_candidate_rows={ecw.get('clean_candidate_rows')}; clean_evaluable_rows={ecw.get('evaluable_rows')}",
        ec.get("review_rows", 0),
        ecw.get("clean_candidate_rows", 0),
        ec.get("possible_over_suppression_rows", 0),
        "Keep 16pct block for now; clean 16-20pct continuation sample is absent.",
    )
    _add(
        rows,
        "EV_PROBE_CANDIDATE_ROWS_ZERO",
        "EXPLAINED_BY_HARD_BLOCKERS",
        f"candidate_rows={ev.get('candidate_rows')}; no_lob_recheck_rows={ev.get('no_lob_recheck_rows')}; clean_after_no_lob_removed_rows={ev.get('clean_after_no_lob_removed_rows')}; diagnostic_rows={ev.get('diagnostic_rows')}",
        ev.get("diagnostic_rows", ev.get("no_lob_recheck_rows", 0)),
        ev.get("candidate_rows", 0),
        0,
        "Not a generator outage; fail-closed blockers are preventing EV candidate promotion.",
    )
    approved_relaxation = [r for r in rows if str(r.get("recommended_action", "")).lower().startswith("relax")]
    watch_only = [r for r in rows if "WATCH" in str(r.get("status", "")) or "sample" in str(r.get("recommended_action", "")).lower() or "markout" in str(r.get("recommended_action", "")).lower()]
    payload = {
        "ts": _now(),
        "status": "OK",
        "scope": "surge_no_entry_hard_blocker_policy_summary",
        "source_files": {
            "ev_candidate_zero": str(LOG_DIR / "surge_ev_candidate_zero_diagnostic_latest.json"),
            "high_rejection": str(LOG_DIR / "surge_high_rejection_policy_fitness_latest.json"),
            "high_rejection_gray_zone_markout_watch": str(LOG_DIR / "surge_high_rejection_gray_zone_markout_watch_latest.json"),
            "score_rvol_overheat": str(LOG_DIR / "surge_score_rvol_overheat_policy_fitness_latest.json"),
            "rvol_overheat": str(LOG_DIR / "surge_rvol_overheat_policy_fitness_latest.json"),
            "entry_change": str(LOG_DIR / "surge_entry_change_policy_fitness_latest.json"),
            "entry_change_clean_watch": str(LOG_DIR / "surge_entry_change_clean_continuation_watch_latest.json"),
        },
        "summary": {
            "blocker_rows": len(rows),
            "approved_relaxation_count": len(approved_relaxation),
            "watch_only_count": len(watch_only),
            "entry_approval_changed": False,
            "policy_change": False,
            "decision": "KEEP_FAIL_CLOSED_AND_ACCUMULATE_WATCH_MARKOUTS",
            "reason": "Current no-entry state is explained by hard blockers; no blocker has clean evidence for production relaxation.",
        },
        "risk_contract": {
            "policy_change": False,
            "entry_approval_changed": False,
            "entry_signal": False,
            "live_order_allowed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_route": False,
            "research_only": True,
            "must_not_dispatch": True,
        },
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    stamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    (LOG_DIR / f"surge_no_entry_hard_blocker_policy_summary_{stamp}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(LOG_DIR / f"surge_no_entry_hard_blocker_policy_summary_{stamp}.csv", rows)
    return payload


def main() -> int:
    payload = build()
    print(json.dumps({"status": payload["status"], "summary": payload["summary"], "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
