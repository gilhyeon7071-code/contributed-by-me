from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
LOOP_STATUS_PATH = LOG_DIR / "intraday_loop_status_latest.json"
WATCHDOG_STATUS_PATH = LOG_DIR / "intraday_watchdog_status_latest.json"
RUN_BAT_PATH = ROOT / "run_intraday_paper.bat"
DAILY_BATCH_LOCK_PATH = LOG_DIR / "run_paper_daily.lock"


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _loop_age_seconds(path: Path) -> float | None:
    if not path.exists():
        return None
    return max(0.0, time.time() - path.stat().st_mtime)


def _find_loop_pids() -> list[int]:
    cmd = [
        "powershell",
        "-NoProfile",
        "-Command",
        "Get-CimInstance Win32_Process | "
        "Where-Object { ($_.CommandLine -like '*intraday_paper_loop.py*') -or ($_.CommandLine -like '*run_intraday_paper.bat*') } | "
        "Select-Object -ExpandProperty ProcessId",
    ]
    try:
        cp = subprocess.run(cmd, text=True, capture_output=True, encoding="utf-8", errors="replace", check=False)
    except Exception:
        return []
    pids: list[int] = []
    for line in (cp.stdout or "").splitlines():
        s = line.strip()
        if not s:
            continue
        try:
            pid = int(s)
        except ValueError:
            continue
        if pid > 0 and pid not in pids:
            pids.append(pid)
    return pids


def _kill_pids(pids: list[int]) -> list[int]:
    killed: list[int] = []
    for pid in pids:
        if pid == os.getpid():
            continue
        cp = subprocess.run(["taskkill", "/F", "/PID", str(pid)], text=True, capture_output=True, check=False)
        if cp.returncode == 0:
            killed.append(pid)
    return killed


def _start_loop() -> tuple[bool, str]:
    if not RUN_BAT_PATH.exists():
        return False, f"run bat missing: {RUN_BAT_PATH}"
    try:
        subprocess.Popen(["cmd", "/c", "start", "", "/min", str(RUN_BAT_PATH)], cwd=str(ROOT))
        return True, "started"
    except Exception as e:
        return False, f"start failed: {type(e).__name__}: {e}"


def main() -> int:
    try:
        if str(ROOT) not in sys.path:
            sys.path.insert(0, str(ROOT))
        from holiday_manager import HolidayManager
        current_ymd = datetime.now().strftime("%Y%m%d")
        if not HolidayManager().is_market_open(current_ymd):
            doc = {
                "ts": _now_iso(),
                "status": "STANDBY",
                "action": "none",
                "reason": f"market closed (holiday/weekend) for {current_ymd}"
            }
            WATCHDOG_STATUS_PATH.write_text(json.dumps(doc, indent=4), encoding="utf-8")
            return 0
    except Exception as e:
        print(f"Failed to check holiday status: {e}")

    ap = argparse.ArgumentParser(description="Intraday loop external watchdog")
    ap.add_argument("--stale-min", type=float, default=float(os.environ.get("INTRADAY_WATCHDOG_STALE_MIN", "12") or 12.0))
    ap.add_argument("--restart-cooldown-sec", type=int, default=int(os.environ.get("INTRADAY_WATCHDOG_COOLDOWN_SEC", "180") or 180))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    now_iso = _now_iso()
    stale_threshold_sec = max(60.0, float(args.stale_min) * 60.0)
    age_sec = _loop_age_seconds(LOOP_STATUS_PATH)
    stale = age_sec is None or age_sec > stale_threshold_sec
    daily_batch_lock_exists = DAILY_BATCH_LOCK_PATH.exists()
    daily_batch_lock_age_sec = _loop_age_seconds(DAILY_BATCH_LOCK_PATH)

    prev = _read_json(WATCHDOG_STATUS_PATH)
    last_restart_at = str(prev.get("last_restart_at") or "")
    cooldown_ok = True
    if last_restart_at:
        try:
            elapsed = (datetime.now() - datetime.fromisoformat(last_restart_at)).total_seconds()
            cooldown_ok = elapsed >= int(args.restart_cooldown_sec)
        except Exception:
            cooldown_ok = True

    pids_before = _find_loop_pids()
    action = "none"
    started = False
    start_reason = ""
    killed: list[int] = []

    if stale:
        if daily_batch_lock_exists:
            action = "daily_batch_skip"
            start_reason = "run_paper_daily_lock_active"
        elif not cooldown_ok:
            action = "cooldown_skip"
        else:
            action = "restart"
            if not args.dry_run:
                killed = _kill_pids(pids_before)
                started, start_reason = _start_loop()
            else:
                start_reason = "dry_run"

    out = {
        "ts": now_iso,
        "status": "STALE" if stale else "FRESH",
        "loop_status_path": str(LOOP_STATUS_PATH),
        "loop_status_exists": LOOP_STATUS_PATH.exists(),
        "loop_status_age_sec": None if age_sec is None else round(age_sec, 1),
        "daily_batch_lock_exists": bool(daily_batch_lock_exists),
        "daily_batch_lock_age_sec": None if daily_batch_lock_age_sec is None else round(daily_batch_lock_age_sec, 1),
        "stale_threshold_sec": round(stale_threshold_sec, 1),
        "restart_cooldown_sec": int(args.restart_cooldown_sec),
        "cooldown_ok": bool(cooldown_ok),
        "action": action,
        "dry_run": bool(args.dry_run),
        "loop_pids_before": pids_before,
        "killed_pids": killed,
        "started": started,
        "start_reason": start_reason,
        "last_restart_at": now_iso if (action == "restart" and (args.dry_run or started)) else last_restart_at,
    }
    _write_json(WATCHDOG_STATUS_PATH, out)

    msg = f"[WATCHDOG] status={out['status']} age={out['loop_status_age_sec']}s action={action} started={started}"
    print(msg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
