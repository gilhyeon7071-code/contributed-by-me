from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"


def _now_ts() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def _run(cmd: List[str]) -> Dict[str, Any]:
    cp = subprocess.run(
        cmd,
        cwd=str(ROOT),
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )
    return {
        "ok": cp.returncode == 0,
        "returncode": int(cp.returncode),
        "stdout": str(cp.stdout or "").strip(),
        "stderr": str(cp.stderr or "").strip(),
        "cmd": cmd,
    }


def _detect_python() -> str:
    candidates = [
        ROOT / ".venv" / "Scripts" / "python.exe",
        Path(r"E:\vibe\buffett\.venv\Scripts\python.exe"),
        Path(r"C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"),
    ]
    for p in candidates:
        if p.exists():
            return str(p)
    return sys.executable


def _query_task(task_name: str) -> Dict[str, Any]:
    return _run(["schtasks", "/Query", "/TN", str(task_name), "/FO", "LIST"])


def main() -> int:
    ap = argparse.ArgumentParser(description="Register weekly performance-review task in Windows Task Scheduler")
    ap.add_argument("--task-name", default="Buffett_Perf_Weekly")
    ap.add_argument("--day", default="MON", help="MON,TUE,WED,THU,FRI,SAT,SUN")
    ap.add_argument("--time", default="18:10", help="HH:MM 24h")
    ap.add_argument("--lookback-days", type=int, default=7)
    ap.add_argument("--python-path", default="")
    ap.add_argument("--workdir", default=str(ROOT))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true", help="Delete and recreate if exists")
    ap.add_argument("--run-now", action="store_true")
    ap.add_argument("--summary-json", default="")
    args = ap.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = Path(args.summary_json) if args.summary_json else (LOG_DIR / f"perf_task_register_{ts}.json")
    out_latest = LOG_DIR / "perf_task_register_latest.json"

    task_name = str(args.task_name).strip()
    day = str(args.day).strip().upper()
    st = str(args.time).strip()
    lookback_days = max(1, int(args.lookback_days))
    workdir = Path(str(args.workdir)).resolve()
    py = str(args.python_path).strip() or _detect_python()

    perf_cmd = f'"{py}" "{workdir}\\tools\\perf_review_weekly.py" --lookback-days {lookback_days}'
    compare_cmd = f'"{py}" "{workdir}\\tools\\kis_mode_compare_report.py"'
    tr = f'cmd /c "cd /d {workdir} && {perf_cmd} && {compare_cmd}"'

    query_before = _query_task(task_name)

    actions: List[Dict[str, Any]] = []
    create_cmd = [
        "schtasks",
        "/Create",
        "/TN",
        task_name,
        "/SC",
        "WEEKLY",
        "/D",
        day,
        "/ST",
        st,
        "/TR",
        tr,
        "/RL",
        "LIMITED",
        "/F",
    ]

    if args.dry_run:
        actions.append({"name": "create_task", "ok": True, "dry_run": True, "cmd": create_cmd})
    else:
        if args.force and query_before.get("ok", False):
            del_cmd = ["schtasks", "/Delete", "/TN", task_name, "/F"]
            actions.append({"name": "delete_existing", **_run(del_cmd)})

        actions.append({"name": "create_task", **_run(create_cmd)})

    query_after = _query_task(task_name)

    if args.run_now and (not args.dry_run):
        run_cmd = ["schtasks", "/Run", "/TN", task_name]
        actions.append({"name": "run_now", **_run(run_cmd)})

    ok = all(bool(a.get("ok", False)) for a in actions) if actions else bool(args.dry_run)
    if (not args.dry_run) and (not query_after.get("ok", False)):
        ok = False

    payload = {
        "generated_at": _now_ts(),
        "ok": bool(ok),
        "dry_run": bool(args.dry_run),
        "task_name": task_name,
        "schedule": {"day": day, "time": st},
        "workdir": str(workdir),
        "python_path": py,
        "task_command": tr,
        "query_before": query_before,
        "actions": actions,
        "query_after": query_after,
    }

    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    out_latest.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] summary={out_json}")
    print(f"[OK] task={task_name} dry_run={args.dry_run} ok={ok}")
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
