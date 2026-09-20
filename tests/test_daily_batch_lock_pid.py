# -*- coding: utf-8 -*-
"""장중 루프가 **죽은 배치의 락**을 존중하지 않는지 고정한다.

[2026-09-13] 실측 사고:
  배치가 메모리 부족으로 죽으면서 `2_Logs/run_paper_daily.lock` 만 남았다(pid 13708 종료).
  루프의 `_daily_batch_lock_state` 는 **나이만** 봤다 - `age > 7200초` 가 아니면 active.
  그래서 죽은 주인의 락을 **2시간 동안** 존중하고 사이클을 건너뛴다.
  실측 판정: `{'active': True, 'age_sec': 192.7, 'reason': 'run_paper_daily_lock_active'}`

  배치 자신은 pid 로 판정한다(run_paper_daily.bat:2286 "stale lock with dead pid").
  **루프만 달랐다.**

이 경로는 아침 진입 창을 통째로 먹는다 - 2026-09-11 에 94사이클(09:01~10:36)을 잃었다.

판정 불가일 때는 **살아 있는 것으로 본다**(fail-closed). 틀리는 방향이 중요하다:
죽은 것을 살았다고 보면 사이클 몇 개를 잃고 끝나지만, 살아 있는 배치를 죽었다고 보면
루프가 배치와 같이 돌아 충돌한다.
"""
from __future__ import annotations

import os

import pytest

L = pytest.importorskip("intraday_paper_loop")


def _lock(tmp_path, info: str):
    d = tmp_path / "run_paper_daily.lock"
    d.mkdir(parents=True, exist_ok=True)
    (d / "run.info").write_text(info, encoding="utf-8")
    return d


def test_dead_pid_lock_is_ignored(tmp_path):
    """**핵심.** 주인이 죽었으면 락은 없는 것이다."""
    d = _lock(tmp_path, "started=x\npid=999999\n")
    st = L._daily_batch_lock_state(d)
    assert st["active"] is False, st
    assert "dead_pid" in st["reason"], st


def test_live_pid_lock_is_respected(tmp_path):
    """살아 있는 배치와 같이 돌면 안 된다."""
    d = _lock(tmp_path, "started=x\npid=%d\n" % os.getpid())
    st = L._daily_batch_lock_state(d)
    assert st["active"] is True, st


def test_missing_pid_is_treated_as_alive(tmp_path):
    """pid 를 못 읽으면 판정 불가다 - fail-closed."""
    d = _lock(tmp_path, "started=x\n")
    assert L._daily_batch_lock_state(d)["active"] is True


def test_no_lock_is_inactive(tmp_path):
    assert L._daily_batch_lock_state(tmp_path / "nolock")["active"] is False


def test_age_rule_still_applies(tmp_path, monkeypatch):
    """나이 규칙을 없앤 게 아니다. pid 판정을 **더한** 것이다."""
    import time
    d = _lock(tmp_path, "started=x\npid=%d\n" % os.getpid())
    old = time.time() - 99999
    os.utime(d, (old, old))
    st = L._daily_batch_lock_state(d)
    assert st["active"] is False and st["reason"] == "stale_lock_ignored", st
