# -*- coding: utf-8 -*-
"""전진 원장 평가의 셈법을 고정한다.

[2026-09-13] 사용자 지적 *"보이는 것은 없고"* 에서 나왔다.
원장에 10거래일 600행이 쌓여 있는데 읽는 코드 넷이 **전부 일수만 셌다.**
선택을 가격에 붙이는 코드가 없어서 아무것도 안 보였다.

여기서 고정하는 것은 **집계 방식**이다. 특히 일 클러스터 -
종목-일 단위로 바로 평균내면 같은 날 함께 움직인 종목을 독립 표본으로 세어
유의성을 과대평가한다. 신호일별로 먼저 접고 그 평균을 평균해야 한다.
"""
from __future__ import annotations

import pandas as pd
import pytest

ev = pytest.importorskip("tools.topn_forward_eval")


def _led(rows):
    return pd.DataFrame(rows, columns=["date", "arm", "rank", "code", "score"]).astype(str)


def _pan(rows):
    return pd.DataFrame(rows, columns=["date", "code", "close"])


def test_day_clustering_not_stock_day_average(monkeypatch):
    """**핵심.** 한 날에 종목이 몰려 있어도 그 날은 표본 1이다.

    D1: SCORE 종목 3개가 전부 +10%  / D2: SCORE 종목 1개가 -10%
    종목-일 평균이면 (10+10+10-10)/4 = +5%.
    신호일 평균이면 (+10 + -10)/2 = 0%. 후자가 맞다.
    """
    led = _led([("20260101", "ARM_SCORE", "1", "000001", "1"),
                ("20260101", "ARM_SCORE", "2", "000002", "1"),
                ("20260101", "ARM_SCORE", "3", "000003", "1"),
                ("20260102", "ARM_SCORE", "1", "000004", "1")])
    pan = _pan([("20260101", "000001", 100.0), ("20260102", "000001", 110.0),
                ("20260101", "000002", 100.0), ("20260102", "000002", 110.0),
                ("20260101", "000003", 100.0), ("20260102", "000003", 110.0),
                ("20260102", "000004", 100.0), ("20260103", "000004", 90.0)])
    monkeypatch.setattr(ev, "_ledger", lambda: led)
    monkeypatch.setattr(ev, "_panel", lambda: pan.assign(
        code=pan["code"].astype(str), date=pan["date"].astype(str)))
    out = ev.forward_returns([1])
    arms = out["horizons"]["1"]["arms"]
    assert arms["ARM_SCORE"]["signal_days"] == 2
    assert arms["ARM_SCORE"]["mean_ret_pct"] == pytest.approx(0.0, abs=1e-6), arms


def test_arm_difference_is_reported(monkeypatch):
    led = _led([("20260101", "ARM_SCORE", "1", "000001", "1"),
                ("20260101", "ARM_RANDOM", "1", "000002", "1")])
    pan = _pan([("20260101", "000001", 100.0), ("20260102", "000001", 105.0),
                ("20260101", "000002", 100.0), ("20260102", "000002", 101.0)])
    monkeypatch.setattr(ev, "_ledger", lambda: led)
    monkeypatch.setattr(ev, "_panel", lambda: pan)
    blk = ev.forward_returns([1])["horizons"]["1"]
    assert blk["score_minus_random_pct"] == pytest.approx(4.0, abs=1e-6), blk


def test_unmatured_horizon_says_so(monkeypatch):
    """만기 미도달을 '수익 0' 으로 흡수하지 않는다."""
    led = _led([("20260101", "ARM_SCORE", "1", "000001", "1")])
    pan = _pan([("20260101", "000001", 100.0)])
    monkeypatch.setattr(ev, "_ledger", lambda: led)
    monkeypatch.setattr(ev, "_panel", lambda: pan)
    blk = ev.forward_returns([10])["horizons"]["10"]
    assert "note" in blk and "만기" in blk["note"], blk


def test_cost_is_reported_not_assumed():
    """비용은 표기만 한다 - 차감해 버리면 원수치를 잃는다."""
    import inspect
    src = inspect.getsource(ev.forward_returns)
    assert "roundtrip_cost_pct" in src
    assert "ret" in src and "- ROUNDTRIP" not in src, "수익률에서 비용을 빼고 있다"
