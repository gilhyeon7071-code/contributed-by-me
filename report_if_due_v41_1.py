# -*- coding: utf-8 -*-
"""
주 1회(기본 7일) 과거 성과 리포트 자동 갱신

실행:
  python E:\\1_Data\\report_if_due_v41_1.py
  python E:\\1_Data\\report_if_due_v41_1.py --force
"""

from datetime import datetime
from pathlib import Path
import subprocess
import sys

BASE_DIR = Path(__file__).resolve().parent
RC_DIR = BASE_DIR / "12_Risk_Controlled"
OUT_SUMMARY = RC_DIR / "report_backtest_summary_v41_1.json"

FRESH_DAYS = 7

def age_days(p: Path) -> float:
    if not p.exists():
        return 1e9
    dt = datetime.now() - datetime.fromtimestamp(p.stat().st_mtime)
    return dt.total_seconds() / 86400.0

def main() -> int:
    force = ("--force" in sys.argv) or ("-f" in sys.argv)

    RC_DIR.mkdir(parents=True, exist_ok=True)
    a = age_days(OUT_SUMMARY)
    if (not force) and a < FRESH_DAYS:
        print(f"[REPORT] skip: fresh ({a:.2f}d < {FRESH_DAYS}d) -> {OUT_SUMMARY}")
        return 0

    script = BASE_DIR / "report_backtest_v41_1.py"
    if not script.exists():
        print(f"[ERROR] missing: {script}")
        return 1

    if force:
        print("[REPORT] force run requested")
    else:
        print(f"[REPORT] run: stale ({a:.2f}d >= {FRESH_DAYS}d)")

    r = subprocess.run([sys.executable, str(script)], cwd=str(BASE_DIR))
    return r.returncode

if __name__ == "__main__":
    raise SystemExit(main())
