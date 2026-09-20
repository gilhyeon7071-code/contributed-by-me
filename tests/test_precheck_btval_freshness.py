# -*- coding: utf-8 -*-
"""진입 전 신선도 검사가 **양쪽 방향으로** 정확히 동작하는지 고정한다.

이 검사는 fail-closed 다 - rc!=0 이면 run_paper_daily.bat 이 `goto :FAILED` 로
진입 전에 배치를 멈춘다. 잘못 발동하면 **그날 매매가 통째로 멈춘다.**
그래서 "안 울린다"만이 아니라 "정확히 언제 울리는가"를 같이 고정한다.

2026-09-11 PLANS (358)(359).
"""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "tools" / "precheck_btval_freshness.py"

RC_OK, RC_MISSING, RC_STALE = 0, 65, 67


def _run(path: Path, max_age_days: float):
    r = subprocess.run(
        [sys.executable, str(SCRIPT), "--path", str(path), "--max-age-days", str(max_age_days)],
        capture_output=True, text=True,
    )
    return r.returncode, (r.stdout or "") + (r.stderr or "")


def test_fresh_artifact_passes(tmp_path):
    f = tmp_path / "backtest_validation_latest.json"
    f.write_text("{}", encoding="utf-8")
    rc, out = _run(f, 3.0)
    assert rc == RC_OK, out
    assert "OK" in out


def test_missing_artifact_blocks(tmp_path):
    rc, out = _run(tmp_path / "nope.json", 3.0)
    assert rc == RC_MISSING, out
    assert "MISSING" in out


def test_stale_artifact_blocks(tmp_path):
    f = tmp_path / "backtest_validation_latest.json"
    f.write_text("{}", encoding="utf-8")
    old = time.time() - 4 * 86400          # 4일 전
    import os
    os.utime(f, (old, old))
    rc, out = _run(f, 3.0)
    assert rc == RC_STALE, out
    assert "STALE" in out


def test_boundary_just_inside_threshold_passes(tmp_path):
    """경계값은 어느 쪽인지 정해져 있어야 한다 - 2.9일은 통과."""
    f = tmp_path / "backtest_validation_latest.json"
    f.write_text("{}", encoding="utf-8")
    old = time.time() - 2.9 * 86400
    import os
    os.utime(f, (old, old))
    rc, out = _run(f, 3.0)
    assert rc == RC_OK, out


def test_batch_calls_the_script_before_the_engine():
    """배치가 이 검사를 **진입 앞에** 두고 있는지 고정한다."""
    bat = ROOT / "run_paper_daily.bat"
    with bat.open("r", encoding="utf-8", newline="") as fh:
        t = fh.read()
    i_check = t.index("precheck_btval_freshness.py")
    i_engine = t.index('set "LAST_STEP_LABEL=[7/9] paper_engine.py main"')
    i_btval = t.index('call :LOG_STEP_BEGIN "[6.96/9] run_backtest_validation_real.bat"')
    assert i_check < i_engine, "신선도 검사가 진입 뒤로 가면 의미가 없다"
    assert i_engine < i_btval, "[6.96/9] 는 진입 뒤에 있어야 한다 (PLANS 358)"
