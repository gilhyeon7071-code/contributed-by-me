"""분기 시총(V2) E1 주문 생성 — 정상, 그리고 C9 사전 차단마다 멈추는지."""
from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import pytest

from paper.strategies.kospi_mcap_quarterly_v2.src import orders as E

NOW = datetime(2026, 10, 1, 9, 10, 0)
CFG = {
    "strategy_id": "KOSPI_MCAP_QUARTERLY_V2", "weight_cap": 0.20, "cost_reserve_pct": 0.0,
    "quote_max_age_seconds": 5, "execution_coverage_fail_pct": 0.10, "buy_cost_estimate_pct": 0.0,
}


def _target(rows):
    # rows: (code, market_cap, basket_weight)
    df = pd.DataFrame(rows, columns=["code", "market_cap", "basket_weight"])
    df["account_target_weight"] = df["basket_weight"]
    return df


TARGET = _target([("000001", 500, 0.2), ("000002", 400, 0.2), ("000003", 300, 0.2), ("000004", 200, 0.2), ("000005", 100, 0.2)])


def _q(price, age=1, qty=100, bid=None):
    return {"ask1": price, "bid1": bid if bid is not None else price, "ask1_qty": qty, "bid1_qty": qty,
            "quote_ts": NOW - timedelta(seconds=age)}


def _quotes(price=1000, **over):
    q = {c: _q(price) for c in ["000001", "000002", "000003", "000004", "000005", "999999"]}
    q.update(over)
    return q


def _plan(phase, holdings=None, cash=10_000_000, quotes=None, target=TARGET, equity=1_000_000, exposure=1.0):
    return E.plan_orders(target, selection_date="20260930", execution_date="20261001", exposure=exposure,
                         strategy_equity=equity, holdings=holdings or {}, cash_available=cash,
                         quotes=quotes or _quotes(), now=NOW, cfg=CFG, phase=phase)


def test_first_rebalance_buys_only():
    orders, s = _plan("BUY")
    assert s["status"] == "OK" and len(orders) == 5
    assert all(o["side"] == "BUY" and o["requested_qty"] == 200 and o["limit_price"] == 1000 for o in orders)
    sells, s2 = _plan("SELL")
    assert sells == [] and s2["status"] == "OK"


def test_limit_prices_round_to_tick():
    orders, _ = _plan("BUY", quotes=_quotes(price=10_003))  # 5천~2만 구간 10원 단위
    assert {o["limit_price"] for o in orders} == {10_010}
    sells, _ = _plan("SELL", holdings={"999999": 10}, quotes=_quotes(price=10_003))
    assert sells[0]["limit_price"] == 10_000 and sells[0]["reason"] == "NOT_IN_TARGET"


def test_ids_are_deterministic_and_change_with_target():
    a, _ = _plan("BUY")
    b, _ = _plan("BUY")
    assert [o["order_id"] for o in a] == [o["order_id"] for o in b]
    t2 = _target([("000001", 500, 0.3), ("000002", 400, 0.1), ("000003", 300, 0.2), ("000004", 200, 0.2), ("000005", 100, 0.2)])
    c, _ = _plan("BUY", target=t2)
    assert a[0]["target_version"] != c[0]["target_version"]


def test_sell_above_target_and_buy_below():
    holdings = {"000001": 300, "000002": 100}
    sells, _ = _plan("SELL", holdings=holdings)
    assert [(o["code"], o["requested_qty"], o["reason"]) for o in sells] == [("000001", 100, "ABOVE_TARGET")]
    buys, _ = _plan("BUY", holdings={"000001": 200, "000002": 100})
    assert ("000002", 100) in [(o["code"], o["requested_qty"]) for o in buys]
    assert "000001" not in {o["code"] for o in buys}


def test_stale_quote_under_ten_pct_skips_and_records_shortfall():
    t = _target([("000001", 500, 0.2), ("000002", 400, 0.2), ("000003", 300, 0.2), ("000004", 200, 0.35), ("000005", 100, 0.05)])
    orders, s = _plan("BUY", target=t, quotes=_quotes(**{"000005": _q(1000, age=30)}))
    assert s["status"] == "OK" and "000005" not in {o["code"] for o in orders}
    assert s["execution_shortfall_weight"] == pytest.approx(0.05) and s["unorderable"]["000005"].startswith("QUOTE_STALE")


def test_coverage_fail_at_ten_pct_or_more():
    orders, s = _plan("BUY", quotes=_quotes(**{"000005": _q(1000, age=30)}))  # 20% 못 삼
    assert orders == [] and s["reasons"][0].startswith("FAIL_EXECUTION_COVERAGE")


def test_missing_and_non_positive_quotes():
    q = _quotes()
    del q["000004"]
    q["000005"] = _q(1000, qty=0)
    _, s = _plan("BUY", quotes=q)
    assert s["unorderable"] == {"000004": "QUOTE_MISSING", "000005": "QUOTE_NON_POSITIVE"}


def test_buy_cash_short_stops_instead_of_shrinking():
    orders, s = _plan("BUY", cash=500_000)
    assert orders == [] and s["status"] == "STOP" and s["reasons"][0].startswith("BUY_CASH_SHORT")


def test_sell_non_target_without_quote_is_skipped_not_guessed():
    q = _quotes()
    del q["999999"]
    sells, s = _plan("SELL", holdings={"999999": 5}, quotes=q)
    assert sells == [] and s["skipped"] == [{"code": "999999", "side": "SELL", "reason": "QUOTE_MISSING"}]


def test_exposure_half_halves_buys():
    orders, _ = _plan("BUY", exposure=0.5, target=_target([(c, m, w) for c, m, w in
                     [("000001", 500, 0.2), ("000002", 400, 0.2), ("000003", 300, 0.2), ("000004", 200, 0.2), ("000005", 100, 0.2)]]
                     ).assign(account_target_weight=lambda d: d["basket_weight"] * 0.5))
    assert all(o["requested_qty"] == 100 for o in orders)


def test_phase_must_be_valid():
    with pytest.raises(ValueError):
        _plan("BOTH")
