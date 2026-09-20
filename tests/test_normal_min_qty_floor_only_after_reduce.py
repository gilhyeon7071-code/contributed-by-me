# -*- coding: utf-8 -*-
"""최소수량 바닥은 **축소가 실제로 일어났을 때만** 걸려야 한다.

[2026-09-12] 실측 회귀. 2026-09-11 NORMAL 후보 6종목 중 4종목이
`NORMAL_INTRADAY_MOMENTUM_REDUCED_MIN_QTY_BLOCK(qty_after<2)` 로 차단됐고
4종목 전부 qty_initial=1 이었다. 주가가 비싸 1주가 된 것이지 작은 주문이 아니다.

수리 전 코드는 `qty = max(1, floor(1 * 0.5)) = 1` 로 **아무것도 축소하지 않고**
그 1주를 min_reduced_qty(2) 미만이라고 차단했다. 이 시험은 그 경로를 고정한다.
"""
from __future__ import annotations

import math
import pytest

entry = pytest.importorskip("paper_engine.entry")


def _floor_reason(qty_before: int, mult: float, price: float, cfg: dict):
    """엔진의 축소 + 바닥 판정을 그대로 재현한다 (entry.py `_apply_normal_entry_qty_reductions`)."""
    qty_after = max(1, int(math.floor(float(qty_before) * mult)))
    if int(qty_after) < int(qty_before):
        reason, notional = entry._normal_reduced_min_qty_block_reason(qty_after, price, cfg)
    else:
        reason, notional = "", float(qty_after) * float(price)
    return qty_after, reason, notional


CFG = {"normal_realtime_gap_policy": {"intraday_momentum_recheck": {
    "min_reduced_qty": 2, "min_reduced_notional_krw": 0}}}


def test_qty1_is_not_blocked_because_nothing_was_reduced():
    """1주는 반토막 나지 않는다. 바닥이 걸리면 안 된다 (2026-09-11 실측 4종목)."""
    qty_after, reason, notional = _floor_reason(1, 0.5, 221000.0, CFG)
    assert qty_after == 1, "축소가 일어나지 않아야 한다"
    assert reason == "", "축소되지 않은 주문에 최소수량 바닥이 걸렸다: %s" % reason
    assert notional == pytest.approx(221000.0)


def test_real_reduction_to_one_share_is_still_blocked():
    """3주 -> 1주 는 실제 축소다. 바닥은 살아 있어야 한다."""
    qty_after, reason, _ = _floor_reason(3, 0.5, 20000.0, CFG)
    assert qty_after == 1
    assert "qty_after<2" in reason, "실제 축소된 먼지 주문이 통과했다"


def test_notional_floor_still_applies_on_real_reduction():
    """금액 바닥은 그대로다."""
    cfg = {"normal_realtime_gap_policy": {"intraday_momentum_recheck": {
        "min_reduced_qty": 1, "min_reduced_notional_krw": 100000.0}}}
    qty_after, reason, _ = _floor_reason(4, 0.5, 10000.0, cfg)
    assert qty_after == 2
    assert "notional<" in reason


def test_helper_itself_is_unchanged():
    """헬퍼의 의미는 건드리지 않았다 - 호출 조건만 좁혔다."""
    reason, notional = entry._normal_reduced_min_qty_block_reason(1, 221000.0, CFG)
    assert "qty_after<2" in reason
    assert notional == pytest.approx(221000.0)
