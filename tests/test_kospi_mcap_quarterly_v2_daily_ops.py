"""분기 시총(V2) 일일 운용 — 노출 변경 목표(scale_current_target) · 주문 ID 분리 · 저녁/아침/오후 작업.
exec plan 4-2 검증 지점 a·b·c·g. 가짜 한투만 쓴다, 실제 발주 없음."""
from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from paper.strategies.kospi_mcap_quarterly_v2.src import daily_ops as D
from paper.strategies.kospi_mcap_quarterly_v2.src import kis_adapter as K
from paper.strategies.kospi_mcap_quarterly_v2.src import orders as O
from paper.strategies.kospi_mcap_quarterly_v2.src import scale_target as S

_spec = importlib.util.spec_from_file_location("v2_runner_fake", Path(__file__).parent / "test_kospi_mcap_quarterly_v2_runner.py")
_RT = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_RT)
SimKIS, Clock, CFG = _RT.SimKIS, _RT.Clock, _RT.CFG

HOL = {"20260922": "20260923", "20260923": "20260928", "20260928": "20260929", "20260929": "20260930", "20260930": "20261001", "20261001": "20261002", "20261002": "20261005",
       "20261005": "20261006", "20261006": "20261007", "20261007": "20261008"}
OPEN = lambda d: d in HOL  # noqa: E731
THR = {"index_code": "2001", "index_window": 200}
SID = CFG["strategy_id"]
CODES = [f"{i:06d}" for i in range(100, 110)]


def _target(weights=None, codes=None):
    codes = codes or CODES
    w = weights or [1.0 / len(codes)] * len(codes)
    return pd.DataFrame({"code": codes, "market_cap": [1e14 - i for i in range(len(codes))], "basket_weight": w,
                         "account_target_weight": w, "target_qty": [9] * len(codes), "price": [1] * len(codes)})


def _summary(exposure=1.0, status="OK"):
    return {"status": status, "stage": "D7", "selection_date": "20260930",
            "d3": {"status": "OK", "exposure": exposure, "regime": "above"}}


def _quotes(prices, now):
    return {c: K.fetch_quote(SimKIS(ask=a, bid=a - 100), c, lambda: now) for c, a in prices.items()}


def _plan(target, exposure, now, prices, equity=60_000_000, event_tag=None, holdings=None):
    return O.plan_orders(target, selection_date="20260930", execution_date="20261001", exposure=exposure,
                         strategy_equity=equity, holdings=holdings or {}, cash_available=1e12,
                         quotes=_quotes(prices, now), now=now, cfg=CFG, phase="BUY", event_tag=event_tag)


NOW = datetime(2026, 10, 1, 10, 0, 0)


# ---------------------------------------------------------------- 주문 ID (검증 b·c)

def test_quarterly_order_id_unchanged_without_tag():
    t = _target()
    orders, s = _plan(t, 1.0, NOW, {c: 70100 for c in CODES})
    tv = O.target_version(t)
    assert s["status"] == "OK" and s["rebalance_id"] == f"{SID}|20260930"
    assert orders[0]["order_id"] == f"{SID}|20260930|{orders[0]['code']}|BUY|{tv}|1"


def test_scale_order_ids_are_separate_from_quarterly():
    t = _target()
    q, _ = _plan(t, 1.0, NOW, {c: 70100 for c in CODES})
    tag = S.scale_event_tag(0.5, "20261005")
    s_orders, s = _plan(t, 1.0, NOW, {c: 70100 for c in CODES}, event_tag=tag)
    assert s["rebalance_id"] == f"{SID}|20260930|SCALE|0.50|20261005"
    assert all("|SCALE|0.50|20261005|" in o["order_id"] for o in s_orders)
    assert not ({o["order_id"] for o in q} & {o["order_id"] for o in s_orders})


# ---------------------------------------------------------------- 노출 변경 목표

def test_scale_multiplies_basket_weight_and_drops_stale_columns():
    t, s = S.scale_current_target(_target(), _summary(), regime={"status": "OK", "exposure": 0.5, "regime": "below"},
                                  effective_date="20261005")
    assert s["status"] == "OK" and s["event_tag"] == "SCALE|0.50|20261005" and s["selection_date"] == "20260930"
    assert s["d3"]["exposure"] == 0.5 and s["base_exposure"] == 1.0
    assert t["account_target_weight"].tolist() == pytest.approx([0.05] * 10)
    assert "target_qty" not in t.columns and "price" not in t.columns


@pytest.mark.parametrize("summary,regime,weights,reason", [
    (_summary(status="STOP"), {"status": "OK", "exposure": 0.5}, None, "BASE_TARGET_NOT_OK"),
    (_summary(), {"status": "STOP", "reasons": ["x"]}, None, "REGIME_NOT_OK"),
    (_summary(), {"status": "OK", "exposure": 0.0}, None, "EXPOSURE_INVALID"),
    (_summary(), {"status": "OK", "exposure": 0.5}, [0.2] * 10, "BASE_WEIGHTS_INVALID"),
])
def test_scale_refuses_bad_inputs(summary, regime, weights, reason):
    t, s = S.scale_current_target(_target(weights), summary, regime=regime, effective_date="20261005")
    assert t is None and s["status"] == "STOP" and any(r.startswith(reason) for r in s["reasons"])


def test_round_trip_half_then_full_restores_high_price_name():
    """보유 수량 x0.5/x2 였다면 1주 종목이 0주로 빠져 못 돌아온다. 비중 스케일링은 NAV 로 다시 계산해 돌아온다."""
    codes = ["000100", "000200", "000300"]
    prices = {"000100": 70100, "000200": 70100, "000300": 2_861_000}
    base = _target([0.6, 0.3, 0.1], codes)
    full, _ = _plan(base, 1.0, NOW, prices, equity=30_000_000)
    half_t, _ = S.scale_current_target(base, _summary(), regime={"status": "OK", "exposure": 0.5},
                                       effective_date="20261005")
    back_t, _ = S.scale_current_target(half_t, _summary(), regime={"status": "OK", "exposure": 1.0},
                                       effective_date="20261012")
    back, _ = _plan(back_t, 1.0, NOW, prices, equity=30_000_000)
    qty = lambda os: {o["code"]: o["requested_qty"] for o in os}  # noqa: E731
    assert qty(full)["000300"] == 1 and qty(back) == qty(full)


# ---------------------------------------------------------------- 판정 (순수)

LED_OK = {"status": "COMPLETE", "positions": [{"code": "000100", "qty": 5}], "missing_prices": []}
REG_ABOVE = {"status": "OK", "regime": "above", "exposure": 1.0}
REG_BELOW = {"status": "OK", "regime": "below", "exposure": 0.5}
CUR_FULL = {"exposure": 1.0}


@pytest.mark.parametrize("ledger,latch,regime,current,action,reason", [
    (LED_OK, {"active": True, "triggered_as_of": "20261001"}, REG_ABOVE, CUR_FULL, "LIQUIDATE", "LATCH_ACTIVE"),
    ({**LED_OK, "positions": []}, {"active": True}, REG_ABOVE, CUR_FULL, "NONE", "LATCH_ACTIVE_FLAT"),
    (LED_OK, {"active": False}, REG_BELOW, None, "NONE", "NO_ACTIVE_TARGET"),
    ({**LED_OK, "status": "INCOMPLETE"}, {"active": False}, REG_BELOW, CUR_FULL, "UNDETERMINED", "NAV_INCOMPLETE"),
    (LED_OK, {"active": False}, {"status": "STOP", "reasons": ["INDEX_NOT_READY"]}, CUR_FULL, "UNDETERMINED",
     "REGIME_NOT_OK"),
    (LED_OK, {"active": False}, REG_BELOW, CUR_FULL, "SCALE", "REGIME_BELOW:1.00->0.50"),
    (LED_OK, {"active": False}, REG_ABOVE, {"exposure": 0.5}, "SCALE", "REGIME_ABOVE:0.50->1.00"),
    (LED_OK, {"active": False}, REG_ABOVE, CUR_FULL, "NONE", "EXPOSURE_MATCHES_REGIME"),
])
def test_decide_next_action(ledger, latch, regime, current, action, reason):
    a = D.decide_next_action(ledger=ledger, latch=latch, regime=regime, current=current, for_date="20261002")
    assert a["action"] == action and a["reasons"][0].startswith(reason) and a["for_date"] == "20261002"


def test_missing_evening_runs():
    rows = [{"job": "evening", "date": "20260929", "status": "OK"}]
    assert D.missing_evening_runs(rows, "20261002", HOL.get) == ["20260930", "20261001"]
    assert D.missing_evening_runs(rows + [{"job": "evening", "date": "20261001", "status": "OK"}], "20261002",
                                  HOL.get) == []
    assert D.missing_evening_runs([], "20261002", HOL.get) == []


# ---------------------------------------------------------------- 저녁 작업

def _index_csv(tmp_path, today, falling=False):
    dates = [d.strftime("%Y%m%d") for d in pd.bdate_range(end=pd.Timestamp(today), periods=230)]
    closes = [1000 - i if falling else 800 + i for i in range(len(dates))]
    p = tmp_path / "index.csv"
    pd.DataFrame({"date": dates, "index_code": "2001", "close": closes, "fetched_at": "x"}).to_csv(p, index=False)
    return p


def _evening(tmp_path, kis, when, falling=False, alerts=None):
    alerts = [] if alerts is None else alerts
    clk = Clock(when)
    return D.evening(kis, state_dir=tmp_path / "state", cfg=CFG, thresholds=THR,
                     index_csv=_index_csv(tmp_path, when.strftime("%Y%m%d"), falling), clock=clk,
                     is_market_open=OPEN, next_trading_day=HOL.get, alert=lambda t, lv: alerts.append((t, lv)))


def test_evening_before_first_rebalance_is_none_with_full_nav(tmp_path):
    alerts = []
    rep = _evening(tmp_path, SimKIS(), datetime(2026, 10, 1, 20, 20), alerts=alerts)
    assert rep["status"] == "OK" and rep["action"] == "NONE" and rep["action_reasons"] == ["NO_ACTIVE_TARGET"]
    assert rep["ledger"]["nav"] == CFG["strategy_capital_krw"] and rep["latch"]["active"] is False
    na = json.loads((tmp_path / "state" / D.NEXT_ACTION).read_text(encoding="utf-8"))
    assert na["for_date"] == "20261002" and alerts == []


def test_evening_regime_drop_schedules_scale_and_alerts(tmp_path):
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / D.CURRENT).write_text(json.dumps({"exposure": 1.0}), encoding="utf-8")
    alerts = []
    rep = _evening(tmp_path, SimKIS(), datetime(2026, 10, 1, 20, 20), falling=True, alerts=alerts)
    assert rep["action"] == "SCALE" and rep["regime"]["regime"] == "below" and len(alerts) == 1


def test_evening_index_not_ready_is_undetermined(tmp_path):
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / D.CURRENT).write_text(json.dumps({"exposure": 1.0}), encoding="utf-8")
    clk = Clock(datetime(2026, 10, 2, 20, 20))
    rep = D.evening(SimKIS(), state_dir=tmp_path / "state", cfg=CFG, thresholds=THR,
                    index_csv=_index_csv(tmp_path, "20261001"), clock=clk, is_market_open=OPEN,
                    next_trading_day=HOL.get, alert=lambda t, lv: None)
    assert rep["status"] == "STOP" and rep["action"] == "UNDETERMINED"
    assert "INDEX_NOT_READY" in str(rep["reasons"])


def test_evening_standby_on_holiday_and_gap_is_incident(tmp_path):
    rep = _evening(tmp_path, SimKIS(), datetime(2026, 10, 3, 20, 20))
    assert rep["status"] == "STANDBY"
    (tmp_path / "state" / D.DAILY_LOG).write_text(json.dumps({"job": "evening", "date": "20260929",
                                                              "status": "OK"}) + "\n", encoding="utf-8")
    rep = _evening(tmp_path, SimKIS(), datetime(2026, 10, 1, 20, 20))
    assert rep["status"] == "STOP" and rep["incidents"][0]["type"] == "3_UNAUTHORIZED_HALT"


# ---------------------------------------------------------------- 아침 · 오후 (검증 g)

def _prepare(tmp_path, action, regime=None, for_date="20261002", auto_submit=False):
    st = tmp_path / "state"
    st.mkdir(exist_ok=True)
    _target().to_csv(tmp_path / "t.csv", index=False)
    (tmp_path / "s.json").write_text(json.dumps(_summary()), encoding="utf-8")
    D.set_current(state_dir=st, target_csv=tmp_path / "t.csv", summary_json=tmp_path / "s.json")
    na = {"for_date": for_date, "action": action, "reasons": []}
    if regime:
        na["regime"] = regime
    (st / D.NEXT_ACTION).write_text(json.dumps(na), encoding="utf-8")
    return st, {"auto_submit": auto_submit, "sell_wait_seconds": 30, "sell_poll_seconds": 10}


def _morning(st, kis, ops, alerts=None):
    clk = Clock(datetime(2026, 10, 2, 10, 0, 0))
    return D.morning(kis, state_dir=st, cfg=CFG, ops=ops, clock=clk, sleep=clk.sleep, is_market_open=OPEN,
                     next_trading_day=HOL.get, alert=lambda t, lv: (alerts if alerts is not None else []).append(t)), clk


def test_morning_none_sends_nothing(tmp_path):
    st, ops = _prepare(tmp_path, "NONE")
    kis = SimKIS()
    rep, _ = _morning(st, kis, ops)
    assert rep["status"] == "OK" and kis.orders == []


def test_morning_scale_shadow_plans_but_sends_nothing(tmp_path):
    st, ops = _prepare(tmp_path, "SCALE", regime={"status": "OK", "exposure": 0.5, "regime": "below"})
    kis = SimKIS()
    rep, _ = _morning(st, kis, ops)
    assert rep["status"] == "OK", rep["reasons"]
    assert kis.orders == [] and rep["buy"]["orders"] == 10 and "NO_SUBMIT_SHADOW" in rep["reasons"]
    assert not (st / D.PENDING).exists()  # 그림자면 현재 목표를 올리지 않는다
    assert json.loads((st / D.CURRENT).read_text(encoding="utf-8"))["exposure"] == 1.0


def test_morning_liquidate_shadow_sends_nothing(tmp_path):
    st, ops = _prepare(tmp_path, "LIQUIDATE")
    (st / "latch.json").write_text(json.dumps({"triggered_as_of": "20261001"}), encoding="utf-8")
    kis = SimKIS()
    rep, _ = _morning(st, kis, ops)
    assert rep["action"] == "LIQUIDATE" and kis.orders == [] and "NO_SUBMIT_SHADOW" in rep["reasons"]


def test_morning_stale_action_is_not_executed(tmp_path):
    st, ops = _prepare(tmp_path, "SCALE", regime={"status": "OK", "exposure": 0.5}, for_date="20261001",
                       auto_submit=True)
    kis, alerts = SimKIS(), []
    rep, _ = _morning(st, kis, ops, alerts)
    assert rep["status"] == "STOP" and kis.orders == [] and len(alerts) == 1


def test_armed_scale_then_afternoon_promotes_current(tmp_path):
    st, ops = _prepare(tmp_path, "SCALE", regime={"status": "OK", "exposure": 0.5, "regime": "below"},
                       auto_submit=True)
    kis = SimKIS()
    rep, clk = _morning(st, kis, ops)
    assert rep["status"] == "OK", rep["reasons"]
    assert len(kis.orders) == 10 and all("|SCALE|0.50|20261002|" in o for o in
                                         [json.loads(x)["order_id"] for x in
                                          (st / "orders.jsonl").read_text(encoding="utf-8").splitlines()])
    assert (st / D.PENDING).exists()
    clk.t = datetime(2026, 10, 2, 15, 25, 0)
    aft = D.afternoon(kis, state_dir=st, cfg=CFG, clock=clk, is_market_open=OPEN, next_trading_day=HOL.get,
                      alert=lambda t, lv: None)
    assert aft["status"] == "OK", aft["reasons"]
    cur = json.loads((st / D.CURRENT).read_text(encoding="utf-8"))
    assert cur["exposure"] == 0.5 and cur["source"] == "SCALE" and not (st / D.PENDING).exists()


def test_afternoon_without_pending_does_nothing(tmp_path):
    st, _ = _prepare(tmp_path, "NONE")
    kis = SimKIS()
    clk = Clock(datetime(2026, 10, 2, 15, 25, 0))
    rep = D.afternoon(kis, state_dir=st, cfg=CFG, clock=clk, is_market_open=OPEN, next_trading_day=HOL.get,
                      alert=lambda t, lv: None)
    assert rep["action"] == "NONE" and kis.orders == [] and kis.cancels == []
