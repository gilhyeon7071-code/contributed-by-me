# -*- coding: utf-8 -*-
"""topn 사이클 상태 판정을 고정한다.

[2026-09-12] 미결 대장 D6 / PLANS RootA (207) 보류 1.

(207) 이 지적한 핵심은 **창 밖을 "멈췄다" 로 오독하는 것**이다.
1회 사이클이 5~11초인데 주기가 10분이라, 상태를 구분하지 않으면 화면이 거의 항상
거짓말을 한다. 그래서 상태 다섯 가지를 각각 고정한다.
"""
from __future__ import annotations

import datetime as dt
import io
import json
import pytest

tcs = pytest.importorskip("tools.topn_cycle_status", reason="repo root not importable")


REAL_CYCLE = """[START] intraday 2026-09-11 10:35:02.30 TOPN_APPLY=1
[OK] 10:35 는 장 시간(0900-1530) 안이다
[OK] 20260911 는 거래일이다
[DATE] signal=20260910  exec=20260911
[OK] topn_build_orders
[OK] topn_dispatch
[OK] topn_reconcile
[END] intraday rc=0 2026-09-11 10:35:13.53
"""

SKIPPED_CYCLE = """[START] intraday 2026-09-12 15:15:02.74 TOPN_APPLY=1
[WAIT] 20260912 는 주말(토)이다. 거래일이 아니다
[WAIT] not a trading day - skipping
[END] skipped 2026-09-12 15:15:02.87
"""

FAILED_CYCLE = """[START] intraday 2026-09-11 11:05:02.10 TOPN_APPLY=1
[OK] topn_build_orders
[FAIL] topn_dispatch rc=3
[END] intraday rc=3 2026-09-11 11:05:09.40
"""


def _load(monkeypatch, tmp_path, text):
    p = tmp_path / "run_topn_intraday.log"
    io.open(str(p), "w", encoding="utf-8").write(text)
    monkeypatch.setattr(tcs, "TOPN_LOG", p, raising=False)


def test_parses_a_real_cycle(monkeypatch, tmp_path):
    _load(monkeypatch, tmp_path, REAL_CYCLE)
    c = tcs.parse_cycles()[-1]
    assert [s["stage"] for s in c["stages"]] == tcs.STAGES_INTRADAY
    assert c["rc"] == 0 and c["skipped"] is False
    assert c["elapsed_sec"] == pytest.approx(11.23, abs=0.01)


def test_guard_lines_are_not_mistaken_for_stages(monkeypatch, tmp_path):
    """`[OK] 10:35 는 장 시간...` 은 단계가 아니다. 섞이면 단계 수가 거짓이 된다."""
    _load(monkeypatch, tmp_path, REAL_CYCLE)
    stages = [s["stage"] for s in tcs.parse_cycles()[-1]["stages"]]
    assert all(s in tcs.KNOWN_STAGES for s in stages)
    assert len(stages) == 3


def test_skipped_cycle_is_marked(monkeypatch, tmp_path):
    _load(monkeypatch, tmp_path, SKIPPED_CYCLE)
    c = tcs.parse_cycles()[-1]
    assert c["skipped"] is True and c["stages"] == []


def test_failed_stage_is_captured(monkeypatch, tmp_path):
    _load(monkeypatch, tmp_path, FAILED_CYCLE)
    c = tcs.parse_cycles()[-1]
    assert c["rc"] == 3 and c["fail"] == ["topn_dispatch"]


# --- 상태 판정 -------------------------------------------------------------

def _win(monkeypatch):
    monkeypatch.setattr(tcs, "read_window", lambda: {
        "start_hhmm": 905, "interval_min": 10, "end_hhmm": 1520, "source": "test"})


def test_window_closed_is_not_reported_as_stalled(monkeypatch, tmp_path):
    """**(207) 의 핵심.** 15:20 이후는 '오늘 종료' 이지 '멈췄다' 가 아니다."""
    _load(monkeypatch, tmp_path, REAL_CYCLE)
    _win(monkeypatch)
    out = tcs.build(now=dt.datetime(2026, 9, 11, 16, 30))
    assert out["state"] == "WINDOW_CLOSED"
    assert "종료" in out["note"]


def test_before_window(monkeypatch, tmp_path):
    _load(monkeypatch, tmp_path, REAL_CYCLE)
    _win(monkeypatch)
    assert tcs.build(now=dt.datetime(2026, 9, 11, 8, 40))["state"] == "BEFORE_WINDOW"


def test_idle_reports_when_the_next_run_is_due(monkeypatch, tmp_path):
    _load(monkeypatch, tmp_path, REAL_CYCLE)
    _win(monkeypatch)
    out = tcs.build(now=dt.datetime(2026, 9, 11, 10, 41))
    assert out["state"] == "IDLE"
    assert "다음 ~4분" in out["note"], out["note"]


def test_running_reports_the_current_stage(monkeypatch, tmp_path):
    """END 가 아직 없는 사이클은 실행 중이다."""
    _load(monkeypatch, tmp_path,
          "[START] intraday 2026-09-11 10:35:02.30 TOPN_APPLY=1\n[OK] topn_build_orders\n")
    _win(monkeypatch)
    out = tcs.build(now=dt.datetime(2026, 9, 11, 10, 35, 5))
    assert out["state"] == "RUNNING"
    assert "topn_build_orders" in out["note"]


def test_non_trading_day_beats_every_other_state(monkeypatch, tmp_path):
    """2026-09-12 는 토요일. 창 안 시각이어도 거래일 판정이 먼저다."""
    _load(monkeypatch, tmp_path, SKIPPED_CYCLE)
    _win(monkeypatch)
    assert tcs.build(now=dt.datetime(2026, 9, 12, 11, 0))["state"] == "NON_TRADING_DAY"


def test_missing_stage_is_visible(monkeypatch, tmp_path):
    _load(monkeypatch, tmp_path, FAILED_CYCLE)
    _win(monkeypatch)
    out = tcs.build(now=dt.datetime(2026, 9, 11, 11, 10))
    assert out["last_real_cycle"]["stages"] == ["topn_build_orders", "topn_dispatch"]
    assert "topn_reconcile" not in out["last_real_cycle"]["stages"]
    assert out["recent_fail_count"] >= 1


def test_window_fallback_is_labelled(monkeypatch):
    """예약을 못 읽으면 그 사실이 보여야 한다. 조용히 대비값을 쓰면 안 된다."""
    def boom(*a, **k):
        raise OSError("no scheduler")
    monkeypatch.setattr(tcs.subprocess, "run", boom)
    w = tcs.read_window()
    assert w["source"].startswith("fallback"), w
    assert w["start_hhmm"] == 905
