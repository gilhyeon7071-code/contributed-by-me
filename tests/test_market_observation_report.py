# -*- coding: utf-8 -*-
"""관측 실패율 집계를 고정한다.

[2026-09-13] 왜 재나: 2026-09-12 에 진입 게이트를 "관측 못 하면 막는다" 로 바꿨다.
사용자 승인 근거는 *"그 기간에는 매매가 이루어지지 않으니까"* 였는데, 그 말은
**실제 발동 구간**에 대한 것이었다. WS 가 끊긴 경우는 시장이 정상이다.
둘을 구별할 방법이 없으므로 **선택은 빈도에 달려 있다.** 그래서 빈도를 센다.

집계에서 틀리기 쉬운 둘을 고정한다.
  (1) 장 밖 실패를 섞으면 안 된다 - 틱이 없는 게 정상이라 빈도가 무의미해진다
  (2) 성공도 세야 한다 - 실패만 세면 분모가 없어 비율을 못 낸다
"""
from __future__ import annotations

import json

import pytest

mor = pytest.importorskip("tools.market_observation_report")


def _write(tmp_path, monkeypatch, rows):
    p = tmp_path / "obs.jsonl"
    p.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n",
                 encoding="utf-8")
    monkeypatch.setattr(mor, "LEDGER", p, raising=False)
    return p


def _row(ts, valid, why=""):
    return {"ts": ts, "ymd": ts[:10].replace("-", ""), "valid": valid,
            "action": "" if valid else "no_observation",
            "n_observed": 2 if valid else 0,
            "rejected": {} if valid else {"0001": why or "stale_source:age=900s"}}


def test_offhours_failures_are_excluded(tmp_path, monkeypatch):
    """**장 밖 실패를 섞지 않는다.** 밤에는 틱이 없는 게 정상이다."""
    _write(tmp_path, monkeypatch, [
        _row("2026-09-14T10:00:00", True),
        _row("2026-09-14T22:00:00", False),   # 장 밖 - 세면 안 됨
        _row("2026-09-14T07:00:00", False),   # 장 전 - 세면 안 됨
    ])
    r = mor.report(days=30)
    assert r["cycles"] == 1 and r["fail"] == 0, r


def test_success_is_counted_as_denominator(tmp_path, monkeypatch):
    _write(tmp_path, monkeypatch, [
        _row("2026-09-14T10:00:00", True),
        _row("2026-09-14T10:02:00", True),
        _row("2026-09-14T10:04:00", False),
    ])
    r = mor.report(days=30)
    assert r["cycles"] == 3 and r["fail"] == 1
    assert r["fail_pct"] == pytest.approx(33.3, abs=0.1), r


def test_non_trading_day_rows_are_excluded(tmp_path, monkeypatch):
    """2026-09-13 은 일요일. 휴장일 실패는 비용이 아니다."""
    _write(tmp_path, monkeypatch, [
        _row("2026-09-13T10:00:00", False),
        _row("2026-09-14T10:00:00", True),
    ])
    r = mor.report(days=30)
    assert r["trading_days_seen"] == 1 and r["cycles"] == 1 and r["fail"] == 0, r


def test_failure_window_is_reported(tmp_path, monkeypatch):
    """언제 끊겼는지가 중요하다 - 장 시작 직후만이면 성격이 다르다."""
    _write(tmp_path, monkeypatch, [
        _row("2026-09-14T09:01:00", False),
        _row("2026-09-14T09:03:00", False),
        _row("2026-09-14T14:00:00", True),
    ])
    d = mor.report(days=30)["days"][0]
    assert d["first_fail_hhmm"] == 901 and d["last_fail_hhmm"] == 903, d


def test_empty_ledger_says_so(tmp_path, monkeypatch):
    """표본이 없는 것을 '실패 0' 으로 흡수하지 않는다."""
    _write(tmp_path, monkeypatch, [])
    r = mor.report(days=30)
    assert r["days"] == [] and r["fail_pct"] is None, r
