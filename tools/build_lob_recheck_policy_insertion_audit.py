"""Audit insertion points for LOB-resolved recheck candidates.

This is a read-only code/artifact audit. It identifies where the current
NO_LOB recheck queue is intentionally observation-only, where it is not merged
into the candidate action queue, and what would be the minimal future policy
review points. It does not change trading behavior.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SURGE_WAIT_PROBE = ROOT / "tools" / "surge_wait_lob_hoga_probe.py"
ACTION_QUEUE = ROOT / "tools" / "build_candidate_action_queue.py"
ACTION_PLAN = ROOT / "tools" / "build_candidate_action_plan.py"
NO_LOB_RECHECK_CSV = LOG_DIR / "surge_no_lob_recheck_queue_latest.csv"
SCORE_RVOL_RECHECK_JSON = LOG_DIR / "surge_score_rvol_conditional_recheck_queue_latest.json"
ACTION_QUEUE_JSON = LOG_DIR / "candidate_action_queue_latest.json"
ACTION_PLAN_CSV = LOG_DIR / "candidate_action_plan_latest.csv"
TRACE_JSON = LOG_DIR / "lob_resolved_entry_policy_trace_latest.json"

OUT_JSON = LOG_DIR / "lob_recheck_policy_insertion_audit_latest.json"
OUT_CSV = LOG_DIR / "lob_recheck_policy_insertion_audit_latest.csv"

KST = dt.timezone(dt.timedelta(hours=9))


def _now() -> str:
    return dt.datetime.now(tz=KST).isoformat(timespec="seconds")


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError:
        return path.read_text(encoding="cp949", errors="replace")
    except Exception:
        return ""


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
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


def _code(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text.zfill(6)[-6:] if text else ""


def _find_line(path: Path, needle: str) -> Dict[str, Any]:
    lines = _read_text(path).splitlines()
    for idx, line in enumerate(lines, start=1):
        if needle in line:
            return {"file": str(path), "line": idx, "needle": needle, "text": line.strip()}
    return {"file": str(path), "line": None, "needle": needle, "text": ""}


def _codes_from_rows(rows: List[Dict[str, str]]) -> List[str]:
    return sorted({_code(row.get("code")) for row in rows if _code(row.get("code"))})


def _queue_sources() -> List[str]:
    doc = _read_json(ACTION_QUEUE_JSON)
    rows = doc.get("rows") if isinstance(doc.get("rows"), list) else []
    return sorted({str(row.get("source") or "") for row in rows if isinstance(row, dict)})


def _plan_codes_for_source(source: str) -> List[str]:
    return sorted(
        {
            _code(row.get("code"))
            for row in _read_csv(ACTION_PLAN_CSV)
            if _code(row.get("code")) and str(row.get("source") or "") == source
        }
    )


def _build_rows() -> List[Dict[str, Any]]:
    no_lob_rows = _read_csv(NO_LOB_RECHECK_CSV)
    no_lob_codes = _codes_from_rows(no_lob_rows)
    action_sources = _queue_sources()
    plan_no_lob_codes = _plan_codes_for_source("no_lob_recheck")
    trace = _read_json(TRACE_JSON)
    trace_rows = trace.get("rows") if isinstance(trace.get("rows"), list) else []
    trace_codes = _codes_from_rows([r for r in trace_rows if isinstance(r, dict)])

    evidence = {
        "probe_sets_recheck_only": _find_line(SURGE_WAIT_PROBE, 'q["recheck_action"] = "RECHECK_ONLY"'),
        "probe_disables_entry_approval": _find_line(SURGE_WAIT_PROBE, 'q["entry_approval_changed"] = False'),
        "probe_marks_observe_scope": _find_line(SURGE_WAIT_PROBE, 'q["queue_scope"] = "observe_only_no_lob_recheck"'),
        "action_queue_reads_score_rvol_only": _find_line(ACTION_QUEUE, "SCORE_RVOL_CONDITIONAL_JSON"),
        "action_queue_reads_no_lob_recheck": _find_line(ACTION_QUEUE, "NO_LOB_RECHECK_CSV"),
        "action_queue_no_lob_loop": _find_line(ACTION_QUEUE, 'source="no_lob_recheck"'),
        "action_queue_score_rvol_loop": _find_line(ACTION_QUEUE, 'source="score_rvol_conditional_recheck"'),
        "action_plan_buy_candidate_only_from_tradable": _find_line(ACTION_PLAN, 'if state == "TRADABLE":'),
        "action_plan_trading_allowed_rule": _find_line(ACTION_PLAN, 'merged["trading_allowed"] = bool'),
    }

    no_lob_queue_merged = "no_lob_recheck" in action_sources
    clean_trace_exists = bool(trace_codes)

    rows = [
        {
            "audit_item": "no_lob_probe_output_scope",
            "status": "OBSERVE_ONLY_CONFIRMED",
            "evidence_file": evidence["probe_sets_recheck_only"]["file"],
            "evidence_line": evidence["probe_sets_recheck_only"]["line"],
            "evidence_text": evidence["probe_sets_recheck_only"]["text"],
            "impact": "LOB-resolved rows are intentionally marked recheck-only.",
            "policy_change_required": False,
            "trading_effect": False,
        },
        {
            "audit_item": "entry_approval_flag",
            "status": "ENTRY_APPROVAL_DISABLED",
            "evidence_file": evidence["probe_disables_entry_approval"]["file"],
            "evidence_line": evidence["probe_disables_entry_approval"]["line"],
            "evidence_text": evidence["probe_disables_entry_approval"]["text"],
            "impact": "The LOB probe cannot change entry approval by design.",
            "policy_change_required": True,
            "trading_effect": False,
        },
        {
            "audit_item": "candidate_action_queue_merge",
            "status": "NO_LOB_RECHECK_NOT_MERGED" if not no_lob_queue_merged else "NO_LOB_RECHECK_MERGED",
            "evidence_file": str(ACTION_QUEUE),
            "evidence_line": (
                evidence["action_queue_no_lob_loop"]["line"]
                if no_lob_queue_merged
                else evidence["action_queue_reads_score_rvol_only"]["line"]
            ),
            "evidence_text": (
                evidence["action_queue_no_lob_loop"]["text"]
                if no_lob_queue_merged
                else evidence["action_queue_reads_score_rvol_only"]["text"]
            ),
            "impact": (
                "General NO_LOB recheck queue is surfaced as review-only in candidate_action_queue."
                if no_lob_queue_merged
                else "General NO_LOB recheck queue is not a current source in candidate_action_queue."
            ),
            "policy_change_required": False,
            "trading_effect": False,
        },
        {
            "audit_item": "buy_candidate_gate",
            "status": "BUY_ONLY_FROM_TRADABLE",
            "evidence_file": evidence["action_plan_buy_candidate_only_from_tradable"]["file"],
            "evidence_line": evidence["action_plan_buy_candidate_only_from_tradable"]["line"],
            "evidence_text": evidence["action_plan_buy_candidate_only_from_tradable"]["text"],
            "impact": "Even if a NO_LOB recheck row is surfaced, it will not become BUY_CANDIDATE unless action_state becomes TRADABLE.",
            "policy_change_required": True,
            "trading_effect": False,
        },
        {
            "audit_item": "minimal_safe_insertion_point",
            "status": "SURFACE_AS_REVIEW_ONLY_FIRST",
            "evidence_file": str(ACTION_QUEUE),
            "evidence_line": evidence["action_queue_score_rvol_loop"]["line"],
            "evidence_text": evidence["action_queue_score_rvol_loop"]["text"],
            "impact": "The least risky future insertion is to ingest NO_LOB recheck rows into candidate_action_queue as RECHECK/trading_allowed=false, parallel to score_rvol_conditional_recheck.",
            "policy_change_required": False,
            "trading_effect": False,
        },
        {
            "audit_item": "entry_policy_insertion_point",
            "status": "REQUIRES_EXPLICIT_POLICY_REVIEW",
            "evidence_file": evidence["action_plan_trading_allowed_rule"]["file"],
            "evidence_line": evidence["action_plan_trading_allowed_rule"]["line"],
            "evidence_text": evidence["action_plan_trading_allowed_rule"]["text"],
            "impact": "Any path from LOB-resolved review rows to BUY_CANDIDATE requires changing action_state/trading_allowed semantics and must be treated as policy change.",
            "policy_change_required": True,
            "trading_effect": False,
        },
    ]

    return rows, {
        "no_lob_recheck_codes": no_lob_codes,
        "no_lob_recheck_rows": len(no_lob_rows),
        "action_queue_sources": action_sources,
        "no_lob_recheck_merged_in_action_queue": no_lob_queue_merged,
        "plan_no_lob_recheck_codes": plan_no_lob_codes,
        "clean_trace_codes": trace_codes,
        "clean_trace_exists": clean_trace_exists,
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys()) if rows else ["audit_item"]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    rows, artifact_summary = _build_rows()
    payload = {
        "generated_at": _now(),
        "scope": "read_only_lob_recheck_policy_insertion_audit",
        "policy_note": "No behavior is changed. Entry-policy activation requires explicit future approval.",
        "source_files": {
            "surge_wait_probe": str(SURGE_WAIT_PROBE),
            "action_queue": str(ACTION_QUEUE),
            "action_plan": str(ACTION_PLAN),
            "no_lob_recheck_csv": str(NO_LOB_RECHECK_CSV),
            "score_rvol_recheck_json": str(SCORE_RVOL_RECHECK_JSON),
            "action_queue_json": str(ACTION_QUEUE_JSON),
            "action_plan_csv": str(ACTION_PLAN_CSV),
            "trace_json": str(TRACE_JSON),
        },
        "artifact_summary": artifact_summary,
        "summary": {
            "audit_rows": len(rows),
            "policy_change_required_rows": sum(1 for row in rows if row.get("policy_change_required")),
            "trading_effect": False,
            "policy_effect": False,
            "policy_change_applied": False,
        },
        "rows": rows,
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"out_json": str(OUT_JSON), "out_csv": str(OUT_CSV), "summary": payload["summary"], "artifact_summary": artifact_summary}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
