from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOTA = Path(r"E:\1_Data")
ROOTB = Path(r"E:\vibe\buffett")
LOGS = ROOTA / "2_Logs"
RUNS = ROOTB / "runs"

if str(ROOTA) not in sys.path:
    sys.path.insert(0, str(ROOTA))
from holiday_manager import HolidayManager

DASHBOARD_STATE = RUNS / "dashboard_state_latest.json"
SSOT_HEALTH = LOGS / "ssot_health_card_latest.json"
P0_CHECK = ROOTA / "p0_daily_check.py"
BUILD_SSOT_HEALTH = ROOTA / "tools" / "build_ssot_health_card.py"
BUILD_DASHBOARD_V2 = ROOTB / "tools" / "build_dashboard_state_v2.py"
BASELINE_LOCK = ROOTB / "tools" / "ssot_baseline_lock.py"
SSOT_PTR = RUNS / "SSOT_TODAY_FINAL.json"
CONFIG_YAML = ROOTB / "config.yaml"

PENDING_STATUS = LOGS / "pending_entry_status_latest.json"
PAPER_RECOVERY = LOGS / "paper_recovery_status_latest.json"
CAND_META = LOGS / "candidates_latest_meta.json"
CAND_CSV = LOGS / "candidates_latest_data.csv"

REPORT_LATEST = LOGS / "oneclick_ops_runbook_latest.json"


def _now_ts() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def _now_iso() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8", "utf-8-sig", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            continue
    return {}


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _mtime_age_seconds(path: Path) -> Optional[float]:
    if not path.exists():
        return None
    return max(0.0, dt.datetime.now().timestamp() - path.stat().st_mtime)


def _norm8(v: Any) -> str:
    s = re.sub(r"\D", "", str(v or ""))
    return s[:8] if len(s) >= 8 else ""


def _today_ymd() -> str:
    return dt.datetime.now().strftime("%Y%m%d")


def _prev_bday(ymd: str) -> str:
    try:
        return HolidayManager().previous_trading_day(ymd)
    except Exception:
        return ""


def _pick_latest(glob_pat: str) -> Optional[Path]:
    files = sorted(LOGS.glob(glob_pat), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _run(cmd: List[str], cwd: Path, timeout_sec: int = 180) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "cmd": cmd,
        "cwd": str(cwd),
        "ok": False,
        "returncode": None,
        "stdout_tail": "",
        "stderr_tail": "",
    }
    try:
        p = subprocess.run(
            cmd,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_sec,
        )
        out["returncode"] = int(p.returncode)
        out["stdout_tail"] = (p.stdout or "")[-4000:]
        out["stderr_tail"] = (p.stderr or "")[-4000:]
        out["ok"] = p.returncode == 0
    except subprocess.TimeoutExpired as e:
        out["returncode"] = None
        out["stdout_tail"] = (e.stdout or "")[-4000:] if isinstance(e.stdout, str) else ""
        out["stderr_tail"] = (e.stderr or "")[-4000:] if isinstance(e.stderr, str) else ""
        out["error"] = "timeout"
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
    return out


def _run_dashboard_build() -> Dict[str, Any]:
    code = (
        "import sys,runpy;"
        "sys.path.insert(0,r'E:\\vibe\\buffett\\tools');"
        "runpy.run_path(r'E:\\vibe\\buffett\\tools\\build_dashboard_state_v2.py',run_name='__main__')"
    )
    return _run([sys.executable, "-c", code], cwd=ROOTA, timeout_sec=240)


def _sync_stats_dir_from_ptr() -> Dict[str, Any]:
    out: Dict[str, Any] = {"ok": False, "updated": False}
    ptr = _read_json(SSOT_PTR)

    # [2026-09-03] 이 함수는 포인터를 읽어 **config.yaml 의 stats_dir 를 다시 쓴다.**
    #   SSOT_TODAY_FINAL.json 의 소비 지점 중 **유일하게 쓰기 부작용을 남기는 곳**이다.
    #   상류(ssot_today_final_update)가 매매 없는 날 낡은 스냅샷으로 폴백할 수 있게 바뀌었다
    #   (stale_fallback=true). 그 상태로 config 를 갈아끼우면 **대시보드가 낡은 통계를
    #   현재 값으로 보여준다.** 포인터가 스스로 낡았다고 말하면 여기서는 쓰지 않는다.
    #   강제하려면 ONECLICK_ALLOW_STALE_PTR=1 을 명시해야 한다.
    if bool(ptr.get("stale_fallback")):
        allow = str(os.environ.get("ONECLICK_ALLOW_STALE_PTR", "")).strip().lower() in {"1", "true", "y", "yes"}
        if not allow:
            out["reason"] = "ptr_stale_fallback"
            out["expected_D"] = ptr.get("expected_D")
            out["snapshot_D"] = ptr.get("snapshot_D") or ptr.get("D")
            out["gap_days"] = ptr.get("gap_days")
            return out
        out["forced_stale"] = True

    snap = Path(str(ptr.get("latest_snapshot") or "").strip()) if ptr.get("latest_snapshot") else None
    if not snap or not snap.exists():
        out["reason"] = "latest_snapshot_missing"
        return out
    target_stats = snap / "stats"
    if not target_stats.exists():
        out["reason"] = "snapshot_stats_missing"
        return out

    text = CONFIG_YAML.read_text(encoding="utf-8") if CONFIG_YAML.exists() else ""
    pat = re.compile(r"^\s*stats_dir\s*:\s*(.+?)\s*$", re.M)
    replacement = f"stats_dir: '{str(target_stats)}'"
    if pat.search(text):
        new_text = pat.sub(lambda _m: replacement, text)
    else:
        new_text = (text.rstrip() + "\n" + replacement + "\n") if text else (replacement + "\n")

    if new_text != text:
        CONFIG_YAML.write_text(new_text, encoding="utf-8")
        out["updated"] = True
    out["ok"] = True
    out["stats_dir"] = str(target_stats)
    return out


def _date_max_from_candidates_csv(path: Path) -> str:
    if not path.exists():
        return ""
    best = ""
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                for k in ("date", "dt", "ymd", "date_yyyymmdd", "as_of_ymd", "date8"):
                    if k in row and row[k]:
                        n = _norm8(row[k])
                        if n and n > best:
                            best = n
    except Exception:
        return ""
    return best


def _check_hard_freshness() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    card = _read_json(SSOT_HEALTH)
    ds = card.get("datasets") if isinstance(card.get("datasets"), list) else []
    today = _today_ymd()
    prev = _prev_bday(today)
    allowed = {today, prev}
    wanted = {"krx_clean", "prices_paper"}
    map_ds = {str(d.get("name") or ""): d for d in ds if isinstance(d, dict)}
    for name in sorted(wanted):
        d = map_ds.get(name, {})
        max_date = _norm8(d.get("max_date") or d.get("as_of_ymd") or "")
        ok = bool(max_date and max_date in allowed)
        rows.append(
            {
                "item": name,
                "level": "BLOCK",
                "status": "PASS" if ok else "FAIL",
                "expected": f"{prev} or {today}",
                "actual": max_date or "missing",
                "reason": "" if ok else f"daily_asof_mismatch:{max_date}",
            }
        )
    return rows


def _check_soft_items() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    # loop state
    rc = _read_json(LOGS / "runtime_chain_status_latest.json")
    rc_overall = str(rc.get("overall") or "").upper()
    rows.append(
        {
            "item": "loop_state",
            "level": "WARN",
            "status": "PASS" if rc_overall == "OK" else "WARN",
            "expected": "runtime_chain overall=OK",
            "actual": rc_overall or "missing",
            "reason": "" if rc_overall == "OK" else "runtime_chain_not_ok",
        }
    )

    # paper state
    pend = _read_json(PENDING_STATUS)
    rec = _read_json(PAPER_RECOVERY)
    pend_age = _mtime_age_seconds(PENDING_STATUS)
    rec_age = _mtime_age_seconds(PAPER_RECOVERY)
    paper_warn = (pend_age is None or pend_age > 3600) or (rec_age is None or rec_age > 3600)
    rows.append(
        {
            "item": "paper_state",
            "level": "WARN",
            "status": "WARN" if paper_warn else "PASS",
            "expected": "pending/recovery <=60m",
            "actual": f"pending={None if pend_age is None else round(pend_age)}s,recovery={None if rec_age is None else round(rec_age)}s",
            "reason": "paper_state_stale" if paper_warn else "",
        }
    )

    # pnl summary
    pnl_latest = _pick_latest("paper_pnl_summary_*.json")
    pnl_age = _mtime_age_seconds(pnl_latest) if pnl_latest else None
    pnl_warn = pnl_age is None or pnl_age > 86400
    rows.append(
        {
            "item": "pnl_summary",
            "level": "WARN",
            "status": "WARN" if pnl_warn else "PASS",
            "expected": "latest <=24h",
            "actual": str(pnl_latest) if pnl_latest else "missing",
            "reason": "pnl_summary_missing_or_stale" if pnl_warn else "",
        }
    )

    # candidates
    cand_meta = _read_json(CAND_META)
    cand_meta_date = _norm8(cand_meta.get("latest_date") or cand_meta.get("as_of_ymd") or "")
    cand_csv_date = _date_max_from_candidates_csv(CAND_CSV)
    cand_actual = max(cand_meta_date, cand_csv_date)
    today = _today_ymd()
    prev = _prev_bday(today)
    cand_warn = not cand_actual or cand_actual not in {today, prev}
    rows.append(
        {
            "item": "candidates",
            "level": "WARN",
            "status": "WARN" if cand_warn else "PASS",
            "expected": f"{prev} or {today}",
            "actual": cand_actual or "missing",
            "reason": "candidates_stale" if cand_warn else "",
        }
    )

    # signals
    sig = _pick_latest("signal_integration_status_*.json")
    sig_obj = _read_json(sig) if sig else {}
    sig_asof = _norm8(
        sig_obj.get("as_of_ymd")
        or sig_obj.get("signal_date")
        or sig_obj.get("latest_date")
        or sig_obj.get("date")
        or ""
    )
    if not sig_asof and sig is not None:
        m = re.search(r"(\d{8})", sig.name)
        sig_asof = m.group(1) if m else ""
    sig_warn = not sig_asof or sig_asof not in {today, prev}
    rows.append(
        {
            "item": "signals",
            "level": "WARN",
            "status": "WARN" if sig_warn else "PASS",
            "expected": f"{prev} or {today}",
            "actual": sig_asof or (str(sig) if sig else "missing"),
            "reason": "signals_stale_or_missing" if sig_warn else "",
        }
    )

    return rows


def _verify_state() -> Dict[str, Any]:
    state = _read_json(DASHBOARD_STATE)
    alerts = state.get("alerts") if isinstance(state.get("alerts"), list) else []
    gates = state.get("gates") if isinstance(state.get("gates"), dict) else {}
    baseline = state.get("ssot_baseline_guard") if isinstance(state.get("ssot_baseline_guard"), dict) else {}
    freshness = ((state.get("gate_summary") or {}).get("freshness_guard") if isinstance(state.get("gate_summary"), dict) else {})
    return {
        "dashboard_state_exists": DASHBOARD_STATE.exists(),
        "status_overall": state.get("status_overall"),
        "alerts_count": len(alerts),
        "config_snapshot_match": gates.get("config_snapshot_match"),
        "baseline_status": baseline.get("status"),
        "freshness_blocked": freshness.get("blocked"),
        "freshness_reasons": freshness.get("reasons") if isinstance(freshness.get("reasons"), list) else [],
    }


def _final_verdict(rows: List[Dict[str, Any]]) -> str:
    if any(r.get("level") == "BLOCK" and r.get("status") != "PASS" for r in rows):
        return "BLOCK"
    if any(r.get("status") == "WARN" for r in rows):
        return "WARN"
    return "PASS"


def runbook(mode: str, do_recover: bool, do_baseline_relock: bool) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "generated_at": _now_iso(),
        "mode": mode,
        "steps": [],
        "checks": [],
        "verify": {},
    }

    result["checks"] = _check_hard_freshness() + _check_soft_items()

    if do_recover:
        steps = []
        steps.append({"name": "p0_daily_check", **_run([sys.executable, str(P0_CHECK)], cwd=ROOTA, timeout_sec=180)})
        steps.append({"name": "build_ssot_health_card", **_run([sys.executable, str(BUILD_SSOT_HEALTH)], cwd=ROOTA, timeout_sec=180)})
        steps.append({"name": "sync_stats_dir", **_sync_stats_dir_from_ptr()})
        if do_baseline_relock:
            steps.append({"name": "baseline_lock_force", **_run([sys.executable, str(BASELINE_LOCK), "init", "--force"], cwd=ROOTB, timeout_sec=120)})
        steps.append({"name": "build_dashboard_state_v2", **_run_dashboard_build()})
        result["steps"] = steps
        result["checks_after_recover"] = _check_hard_freshness() + _check_soft_items()

    verify = _verify_state()
    result["verify"] = verify

    final_checks = result.get("checks_after_recover") if isinstance(result.get("checks_after_recover"), list) else result["checks"]
    verdict = _final_verdict(final_checks)

    if not verify.get("dashboard_state_exists"):
        verdict = "BLOCK"
    if str(verify.get("status_overall") or "").upper() == "FAIL":
        verdict = "BLOCK"

    result["summary"] = {
        "verdict": verdict,
        "block_count": sum(1 for r in final_checks if r.get("level") == "BLOCK" and r.get("status") != "PASS"),
        "warn_count": sum(1 for r in final_checks if r.get("status") == "WARN"),
        "pass_count": sum(1 for r in final_checks if r.get("status") == "PASS"),
        "status_overall": verify.get("status_overall"),
        "alerts_count": verify.get("alerts_count"),
    }
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description="One-click ops runbook: diagnose + self-heal + verify + summary")
    ap.add_argument("--mode", choices=["preopen", "recover", "diagnose"], default="preopen")
    ap.add_argument("--baseline-relock", action="store_true", help="include ssot baseline lock --force during recover")
    args = ap.parse_args()

    do_recover = args.mode == "recover"
    report = runbook(mode=args.mode, do_recover=do_recover, do_baseline_relock=bool(args.baseline_relock))

    ts_path = LOGS / f"oneclick_ops_runbook_{_now_ts()}.json"
    _write_json(ts_path, report)
    _write_json(REPORT_LATEST, report)

    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    print(f"[RUNBOOK] wrote: {ts_path}")
    print(f"[RUNBOOK] verdict={summary.get('verdict')} block={summary.get('block_count')} warn={summary.get('warn_count')} pass={summary.get('pass_count')}")
    print(f"[RUNBOOK] dashboard_status={summary.get('status_overall')} alerts={summary.get('alerts_count')}")

    return 0 if summary.get("verdict") in {"PASS", "WARN"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
