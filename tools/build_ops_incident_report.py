from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
TEMPLATE = ROOT / "docs" / "references" / "ROOTA_INCIDENT_REPORT_TEMPLATE.md"
KST = timezone(timedelta(hours=9))


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            continue
    return {}


def _safe_slug(text: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(text or "").strip()).strip("_")
    return slug[:80] or "incident"


def _get_path(summary: Dict[str, Any], key: str) -> str:
    ev = summary.get("evidence") if isinstance(summary.get("evidence"), dict) else {}
    return str(ev.get(key) or "")


def _status_row(status: str, evidence: str, fail_reason: str = "") -> str:
    if str(status).upper() == "PASS":
        return f"PASS - {evidence}"
    if str(status).upper() == "FAIL":
        return f"FAIL - {fail_reason or evidence}"
    return f"NA - {evidence}"


def build_report(
    sanity_json: Path,
    template_path: Path,
    output_dir: Path,
    command: str,
    exit_code: int | None,
    title: str,
) -> Path:
    summary = _read_json(sanity_json)
    replay = _read_json(Path(_get_path(summary, "replay_compare")))
    fills = _read_json(Path(_get_path(summary, "fills_summary")))
    ssot = _read_json(Path(_get_path(summary, "ssot_health_json")))
    idem = _read_json(Path(_get_path(summary, "idempotency_smoke")))

    now_kst = datetime.now(timezone.utc).astimezone(KST)
    incident_id = f"ROOTA-{now_kst.strftime('%Y%m%d-%H%M%S')}"
    status = str(summary.get("status") or "UNKNOWN").upper()
    reason = str(summary.get("reason") or "unknown")
    d = str(summary.get("D") or replay.get("D") or fills.get("D") or "unknown")
    rc = int(exit_code if exit_code is not None else summary.get("exit_code") or 0)

    # [2026-08-24] status 를 evidence JSON 에서만 읽어서, 실패로 이 리포트를 만들면서도
    #   "RootA ops sanity PASS; exit_code=5" 라고 적혀 나왔다(파일명까지 all_checks_passed).
    #   run_ops_sanity_quick.bat 이 실패 경로에서 JSON 갱신에 실패하면 낡은 PASS 가 남는다.
    #   실행 결과(exit_code)가 판정의 주(主)이고 evidence JSON 은 보조다.
    _json_status = status
    _json_rc = summary.get("exit_code")
    _json_at = str(summary.get("generated_at_utc") or "unknown")
    if rc != 0:
        status = "FAIL"
        _stale = (_json_status == "PASS") or (str(_json_rc) == "0")
        if _stale:
            reason = "exit_code_%d_stale_evidence" % rc
        elif reason == "unknown":
            reason = "exit_code_%d" % rc
        evidence_note = (
            "실행 결과 exit_code=%d 로 FAIL 이다. evidence JSON 은 status=%s exit_code=%s "
            "generated_at_utc=%s 로, 이 실행분이 아닐 수 있다." % (rc, _json_status, _json_rc, _json_at)
        )
    else:
        evidence_note = ""
    short_title = title or f"ops sanity {status.lower()} - {reason}"
    run_log = str(summary.get("run_log") or "")
    primary_evidence = str(sanity_json)

    replay_counts = replay.get("counts") if isinstance(replay.get("counts"), dict) else {}
    replay_checks = replay.get("checks") if isinstance(replay.get("checks"), dict) else {}
    ssot_overall = ssot.get("overall") if isinstance(ssot.get("overall"), dict) else {}

    context = {
        "INCIDENT_ID": incident_id,
        "short_title": short_title,
        "timestamp_kst": now_kst.strftime("%Y-%m-%d %H:%M:%S%z"),
        "component_name": "RootA ops sanity",
        "paper|broker|dashboard|batch": "batch",
        "D_or_unknown": d,
        "commit_or_unknown": "unknown",
        "one_line_observed_failure_and_impact": (
            f"RootA ops sanity {status}; reason={reason}; exit_code={rc}; D={d}."
        ),
        "ci_failure|batch_failure|manual_detection|dashboard_alert": "batch_failure" if status != "PASS" else "manual_detection",
        "command": command or r"E:\1_Data\run_ops_sanity_quick.bat",
        "exit_code": str(rc),
        "primary_evidence_path": primary_evidence,
        "run_log_path": run_log,
        "what_the_facts_mean_without_expanding_policy": (
            "Quick sanity evidence is PASS." if status == "PASS"
            else ("Quick sanity stopped before PASS; inspect evidence paths before any policy change. "
                  + evidence_note)
        ),
        "strategies_or_unknown": "unknown",
        "orders_or_unknown": "unknown",
        "fills_or_unknown": str(replay_counts.get("source_events") or fills.get("rows_for_D") or "unknown"),
        "ledger_stats_or_unknown": "see canonical_replay_compare_latest.json supporting_reports",
        "pnl_or_unknown": "unknown",
        "additional_logs_or_na": "NA",
        "action": "generated incident report from ops sanity evidence",
        "mitigation_or_na": "NA",
        "rollback_target_or_na": "NA",
        "approver_or_na": "NA",
        "what_was_tested": (
            f"ops sanity summary status={status}; ssot={ssot_overall.get('status', 'unknown')}; "
            f"fills_rows_for_D={fills.get('rows_for_D', 'unknown')}; "
            f"idempotency={idem.get('status', 'unknown')}; "
            f"replay_status={replay.get('status', 'unknown')}"
        ),
        "what_was_not_tested": "CI remote runner, Windows Scheduled Task registration, trading policy changes",
        "name": "unknown",
        "date": "unknown",
        "complete|partial|incomplete": "complete" if status == "PASS" else "partial",
        "short_reason": reason,
    }

    verification = {
        "Functional verification: PASS / FAIL / NA - {{evidence_or_reason}}": _status_row(
            status, primary_evidence, reason
        ),
        "Consistency verification: PASS / FAIL / NA - {{evidence_or_reason}}": _status_row(
            str(replay.get("status") or "NA"),
            f"source_events={replay_counts.get('source_events')}, canonical_events={replay_counts.get('canonical_events')}",
            reason,
        ),
        "Operating reflection verification: PASS / FAIL / NA - {{evidence_or_reason}}": _status_row(
            "PASS" if sanity_json.exists() else "FAIL",
            str(sanity_json),
            "missing sanity summary",
        ),
        "Policy verification: PASS / FAIL / NA - {{evidence_or_reason}}": "PASS - no policy or state-changing endpoint is executed by the incident builder",
        "FAIL-CLOSED verification: PASS / FAIL / NA - {{evidence_or_reason}}": (
            "PASS - non-zero sanity exit is preserved by run_ops_sanity_ci.bat"
        ),
        "Regression verification: PASS / FAIL / NA - {{evidence_or_reason}}": _status_row(
            str(replay.get("status") or "NA"),
            f"state_hash_equal={replay_checks.get('state_hash_equal')}, chain_hash_equal={replay_checks.get('chain_hash_equal')}",
            reason,
        ),
    }

    text = template_path.read_text(encoding="utf-8")
    for key, value in context.items():
        text = text.replace("{{" + key + "}}", str(value))
    for needle, replacement in verification.items():
        text = text.replace(needle, needle.split(" - ", 1)[0] + " - " + replacement)

    output_dir.mkdir(parents=True, exist_ok=True)
    out = output_dir / f"{incident_id}_{_safe_slug(reason)}.md"
    out.write_text(text, encoding="utf-8")
    latest = output_dir / "incident_latest.md"
    latest.write_text(text, encoding="utf-8")
    print(f"[INCIDENT] wrote {out}")
    print(f"[INCIDENT] latest {latest}")
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sanity-json", default=str(LOG_DIR / "ops_sanity_quick_latest.json"))
    parser.add_argument("--template", default=str(TEMPLATE))
    parser.add_argument("--output-dir", default=str(LOG_DIR / "incidents"))
    parser.add_argument("--command", default=r"E:\1_Data\run_ops_sanity_quick.bat")
    parser.add_argument("--exit-code", type=int, default=None)
    parser.add_argument("--title", default="")
    args = parser.parse_args()

    sanity_json = Path(args.sanity_json)
    template_path = Path(args.template)
    if not sanity_json.exists():
        print(f"[INCIDENT] missing sanity json: {sanity_json}")
        return 2
    if not template_path.exists():
        print(f"[INCIDENT] missing template: {template_path}")
        return 3
    build_report(sanity_json, template_path, Path(args.output_dir), args.command, args.exit_code, args.title)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
