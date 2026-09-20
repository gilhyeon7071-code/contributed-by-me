from __future__ import annotations

import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"

OUT_JSON = LOGS / "surge_timephase_validation_matrix_latest.json"
OUT_CSV = LOGS / "surge_timephase_validation_matrix_latest.csv"


def _now() -> datetime:
    return datetime.now().replace(microsecond=0)


def _today_ymd() -> str:
    return _now().strftime("%Y%m%d")


def _read_json(path: Path) -> tuple[dict[str, Any] | None, str]:
    if not path.exists():
        return None, "MISSING"
    try:
        return json.loads(path.read_text(encoding="utf-8-sig")), "OK"
    except Exception as exc:
        return None, f"PARSE_ERROR:{type(exc).__name__}:{exc}"


def _mtime(path: Path) -> str:
    if not path.exists():
        return ""
    return datetime.fromtimestamp(path.stat().st_mtime).replace(microsecond=0).isoformat()


def _age_minutes(path: Path) -> float | None:
    if not path.exists():
        return None
    return round((_now() - datetime.fromtimestamp(path.stat().st_mtime)).total_seconds() / 60.0, 2)


def _find_last(pattern: str, path: Path) -> str:
    if not path.exists():
        return ""
    rx = re.compile(pattern)
    last = ""
    try:
        with path.open("r", encoding="utf-8-sig", errors="replace") as fh:
            for line in fh:
                if rx.search(line):
                    last = line.strip()
    except OSError:
        return ""
    return last


def _count_log(pattern: str, path: Path) -> int:
    if not path.exists():
        return 0
    rx = re.compile(pattern)
    count = 0
    try:
        with path.open("r", encoding="utf-8-sig", errors="replace") as fh:
            for line in fh:
                if rx.search(line):
                    count += 1
    except OSError:
        return 0
    return count


def _status_by_freshness(path: Path, expected_ymd: str, observed_ymd: str | None) -> str:
    if not path.exists():
        return "FAIL"
    if observed_ymd and observed_ymd != expected_ymd:
        return "FAIL"
    if _age_minutes(path) is not None and _age_minutes(path) > 24 * 60:
        return "FAIL"
    return "PASS"


def _preopen_phase(today: str) -> dict[str, Any]:
    path = LOGS / "preopen_5min_check_latest.json"
    preopen, state = _read_json(path)
    readiness, readiness_state = _read_json(LOGS / "surge_preopen_precursor_readiness_latest.json")
    summary = (readiness or {}).get("summary") if isinstance((readiness or {}).get("summary"), dict) else {}
    as_of = str((preopen or {}).get("as_of_ymd") or (readiness or {}).get("calendar", {}).get("date") or "")
    fresh_status = _status_by_freshness(path, today, as_of or None)
    status = "PASS" if state == "OK" and readiness_state == "OK" and fresh_status == "PASS" else "FAIL"
    return {
        "phase": "장시작전",
        "status": status,
        "time_window": "장전/개장 직전",
        "entrypoint": "run_preopen_5min_check.bat",
        "primary_artifact": str(path),
        "artifact_mtime": _mtime(path),
        "artifact_age_min": _age_minutes(path),
        "as_of": as_of,
        "key_values": {
            "preopen_ready_items": summary.get("preopen_ready_items"),
            "preopen_total_items": summary.get("total_items"),
            "preopen_ready_ratio": summary.get("preopen_ready_ratio"),
            "preopen_quality_score": summary.get("preopen_quality_score"),
            "preopen_source_not_implemented_items": summary.get("preopen_source_not_implemented_items"),
            "intraday_required_items": summary.get("intraday_required_items"),
            "trading_allowed": ((readiness or {}).get("risk_contract") or {}).get("trading_allowed"),
        },
        "tested": "장전 체크 배치 존재, 최신 장전 readiness/5분 체크 산출물 존재와 날짜를 확인",
        "not_tested": "장전 배치를 새로 실행하지 않음",
        "gap": "" if status == "PASS" else "오늘 기준 장전 산출물 최신성 부족",
        "order_impact": "none",
    }


def _market_open_phase(today: str) -> dict[str, Any]:
    market_path = LOGS / "market_rising_status_latest.json"
    surge_path = LOGS / "surge_realtime_latest.json"
    loop_log = LOGS / "run_intraday_paper_last.txt"
    market, market_state = _read_json(market_path)
    surge, surge_state = _read_json(surge_path)
    intraday_date = str((surge or {}).get("intraday_date") or "")
    market_ok = bool((market or {}).get("ok"))
    surge_date_ok = intraday_date == today
    log_start = _find_last(r"\[LOOP\] cycle 1 done:|\[LOOP\] cycle [0-9]+ start", loop_log)
    status = "PASS" if market_state == "OK" and surge_state == "OK" and market_ok and surge_date_ok else "FAIL"
    return {
        "phase": "장시작후",
        "status": status,
        "time_window": "개장 직후/초기 루프",
        "entrypoint": "run_intraday_paper.bat -> intraday_paper_loop.py",
        "primary_artifact": str(market_path),
        "artifact_mtime": _mtime(market_path),
        "artifact_age_min": _age_minutes(market_path),
        "as_of": intraday_date,
        "key_values": {
            "market_rising_ok": market_ok,
            "market_rising_selected_rows": (market or {}).get("selected_rows"),
            "surge_input_rows": (surge or {}).get("input_rows"),
            "surge_evaluated_rows": (surge or {}).get("evaluated_rows"),
            "surge_detected_count": (surge or {}).get("detected_count"),
            "entry_wait_execution": ((surge or {}).get("entry_decision_counts") or {}).get("WAIT_EXECUTION"),
            "latest_loop_marker": log_start,
        },
        "tested": "시장상승 스냅샷과 급등 감지 최신 날짜/행 수를 확인",
        "not_tested": "장시작후 전용 별도 판정 배치는 없음",
        "gap": "" if status == "PASS" else "개장 직후 최신 입력 또는 날짜 정합성 부족",
        "order_impact": "none",
    }


def _intraday_phase(today: str) -> dict[str, Any]:
    surge_path = LOGS / "surge_realtime_latest.json"
    lob_path = LOGS / "surge_lob_latest.json"
    exec_path = LOGS / "surge_intraday_execution_validation_latest.json"
    loop_log = LOGS / "run_intraday_paper_last.txt"
    surge, surge_state = _read_json(surge_path)
    lob, lob_state = _read_json(lob_path)
    execution, execution_state = _read_json(exec_path)
    intraday_date = str((surge or {}).get("intraday_date") or "")
    loop_done = _find_last(r"\[LOOP\] cycle [0-9]+ done: [0-9]+/[0-9]+ ok", loop_log)
    status = (
        "PASS"
        if surge_state == "OK"
        and lob_state == "OK"
        and execution_state == "OK"
        and intraday_date == today
        and bool(loop_done)
        else "FAIL"
    )
    return {
        "phase": "장중",
        "status": status,
        "time_window": "정규장 반복 루프",
        "entrypoint": "intraday_paper_loop.py",
        "primary_artifact": str(surge_path),
        "artifact_mtime": _mtime(surge_path),
        "artifact_age_min": _age_minutes(surge_path),
        "as_of": intraday_date,
        "key_values": {
            "surge_status": (surge or {}).get("status"),
            "alerts_count": (surge or {}).get("alerts_count"),
            "detected_count": (surge or {}).get("detected_count"),
            "lob_rows": (lob or {}).get("rows"),
            "lob_coverage_pct": (surge or {}).get("lob_coverage_pct"),
            "execution_validation_status": (execution or {}).get("status"),
            "execution_validation_rows": (execution or {}).get("rows"),
            "latest_loop_done": loop_done,
        },
        "tested": "급등 감지, LOB, 장중 체결 검증, 루프 완료 로그를 교차 확인",
        "not_tested": "실시간 주문 체결을 새로 발생시키지 않음",
        "gap": "" if status == "PASS" else "장중 루프/LOB/실행검증 중 하나의 최신 증거 부족",
        "order_impact": "none",
    }


def _followthrough_phase(today: str) -> dict[str, Any]:
    path = LOGS / "surge_followthrough_validation_latest.json"
    loop_log = LOGS / "run_intraday_paper_last.txt"
    loop_status_path = LOGS / "intraday_loop_status_latest.json"
    data, state = _read_json(path)
    loop_status, loop_state = _read_json(loop_status_path)
    validation_d = str((data or {}).get("validation_D") or "")
    timeout_count = _count_log(r"\[surge_followthrough_validation\] TIMEOUT", loop_log)
    today_dash = f"{today[:4]}-{today[4:6]}-{today[6:8]}"
    today_timeout = _count_log(rf"{today_dash} .* \[surge_followthrough_validation\] TIMEOUT", loop_log)
    latest_loop_followthrough: dict[str, Any] = {}
    if loop_state == "OK":
        for step in (loop_status or {}).get("steps", []):
            if step.get("label") == "surge_followthrough_validation":
                latest_loop_followthrough = step
                break
    fresh_status = _status_by_freshness(path, today, validation_d or None)
    if state == "OK" and fresh_status == "PASS" and today_timeout == 0:
        status = "PASS"
    elif state == "OK" and fresh_status == "PASS":
        status = "PARTIAL"
    else:
        status = "FAIL"
    return {
        "phase": "장중 후속검증",
        "status": status,
        "time_window": "감지 후 30분 이상 성숙 구간",
        "entrypoint": "validate_surge_followthrough.py",
        "primary_artifact": str(path),
        "artifact_mtime": _mtime(path),
        "artifact_age_min": _age_minutes(path),
        "as_of": validation_d,
        "key_values": {
            "validation_state": (data or {}).get("validation_state"),
            "rows": ((data or {}).get("summary") or {}).get("rows"),
            "success_like_rows": ((data or {}).get("summary") or {}).get("success_like_rows"),
            "risk_like_rows": ((data or {}).get("summary") or {}).get("risk_like_rows"),
            "all_timeout_count_in_log": timeout_count,
            "today_timeout_count_in_log": today_timeout,
            "latest_loop_followthrough_skipped": latest_loop_followthrough.get("skipped"),
            "latest_loop_followthrough_blocked_by": latest_loop_followthrough.get("blocked_by"),
        },
        "tested": "최신 후속검증 산출물 날짜와 오늘 루프 timeout 반복 여부를 확인",
        "not_tested": "후속검증을 새로 재실행하지 않음",
        "gap": "" if status == "PASS" else (
            f"최신 공식 루프 후속검증 스킵: {latest_loop_followthrough.get('blocked_by')}"
            if latest_loop_followthrough.get("skipped") and latest_loop_followthrough.get("blocked_by")
            else "오늘 후속검증 산출물은 최신이나 이전 timeout 이력 존재"
            if status == "PARTIAL"
            else "오늘 후속검증 최신 산출물 부재 또는 timeout 반복"
        ),
        "order_impact": "none",
    }


def build() -> dict[str, Any]:
    today = _today_ymd()
    phases = [
        _preopen_phase(today),
        _market_open_phase(today),
        _intraday_phase(today),
        _followthrough_phase(today),
    ]
    counts: dict[str, int] = {}
    for row in phases:
        counts[row["status"]] = counts.get(row["status"], 0) + 1
    overall = "PASS" if counts.get("FAIL", 0) == 0 and counts.get("PARTIAL", 0) == 0 else "PARTIAL"
    completed = [row["phase"] for row in phases if row["status"] == "PASS"]
    remaining = [f"{row['phase']}:{row['gap']}" for row in phases if row["status"] != "PASS"]
    return {
        "generated_at": _now().isoformat(),
        "as_of": today,
        "scope": "surge_timephase_validation_matrix",
        "status": overall,
        "status_counts": counts,
        "risk_contract": {
            "read_only_diagnostic": True,
            "policy_change": False,
            "entry_approval_changed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_allowed": False,
        },
        "phases": phases,
        "summary": {
            "completed": ", ".join(completed),
            "remaining_problem": "; ".join(remaining) if remaining else "",
            "next_step": "남은 PARTIAL/FAIL 항목의 공식 루프 반영 증거 확인",
        },
    }


def write_outputs(payload: dict[str, Any]) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    stamp = payload["generated_at"].replace("-", "").replace(":", "").replace("T", "_")
    stamped_json = LOGS / f"surge_timephase_validation_matrix_{stamp}.json"
    stamped_csv = LOGS / f"surge_timephase_validation_matrix_{stamp}.csv"
    for path in (OUT_JSON, stamped_json):
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    fields = [
        "phase",
        "status",
        "time_window",
        "as_of",
        "artifact_mtime",
        "artifact_age_min",
        "entrypoint",
        "primary_artifact",
        "gap",
        "tested",
        "not_tested",
        "order_impact",
        "key_values_json",
    ]
    rows = []
    for row in payload["phases"]:
        flat = {k: row.get(k, "") for k in fields if k != "key_values_json"}
        flat["key_values_json"] = json.dumps(row.get("key_values", {}), ensure_ascii=False, sort_keys=True)
        rows.append(flat)
    for path in (OUT_CSV, stamped_csv):
        with path.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)


def main() -> int:
    payload = build()
    write_outputs(payload)
    print(json.dumps({
        "status": payload["status"],
        "as_of": payload["as_of"],
        "status_counts": payload["status_counts"],
        "out_json": str(OUT_JSON),
        "out_csv": str(OUT_CSV),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
