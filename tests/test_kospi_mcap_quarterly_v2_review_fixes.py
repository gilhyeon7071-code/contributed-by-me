"""분기 시총(V2) — 2026-09-19 독립 검토 결함 수리 시험. 가짜 한투만, 실제 발주 없음.
1 체결 조회 전체(00)·페이지 잘림 / 2 노출 미달성이면 현재 목표로 안 올림 / 3 오후 동기화→취소 순서 /
4 사고 드러내기 / 5 복구 전이 기록 / 6 다음 거래일 1회 재주문 / 8 노출 출처 일치 / 10 주문마다 발주 창."""
from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from paper.strategies.kospi_mcap_quarterly_v2.src import daily_ops as D
from paper.strategies.kospi_mcap_quarterly_v2.src import execution as E
from paper.strategies.kospi_mcap_quarterly_v2.src import kis_adapter as K
from paper.strategies.kospi_mcap_quarterly_v2.src import orders as O

_spec = importlib.util.spec_from_file_location("v2_runner_fake2", Path(__file__).parent / "test_kospi_mcap_quarterly_v2_runner.py")
_RT = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_RT)
SimKIS, Clock, CFG = _RT.SimKIS, _RT.Clock, _RT.CFG

HOL = {"20260922": "20260923", "20260923": "20260928", "20260928": "20260929", "20260929": "20260930",
       "20260930": "20261001", "20261001": "20261002", "20261002": "20261005", "20261005": "20261006",
       "20261006": "20261007", "20261007": "20261008"}
OPEN = lambda d: d in HOL  # noqa: E731
CODES = [f"{i:06d}" for i in range(100, 110)]
NOALERT = lambda t, lv: None  # noqa: E731


def _target():
    w = [0.1] * 10
    return pd.DataFrame({"code": CODES, "market_cap": [1e14 - i for i in range(10)], "basket_weight": w,
                         "account_target_weight": w})


def _state(tmp_path, action="NONE", regime=None, for_date="20261002", auto_submit=False):
    st = tmp_path / "state"
    st.mkdir(exist_ok=True)
    _target().to_csv(tmp_path / "t.csv", index=False)
    (tmp_path / "s.json").write_text(json.dumps({"status": "OK", "stage": "D7", "selection_date": "20260930",
                                                 "d3": {"status": "OK", "exposure": 1.0}}), encoding="utf-8")
    D.set_current(state_dir=st, target_csv=tmp_path / "t.csv", summary_json=tmp_path / "s.json")
    na = {"for_date": for_date, "action": action, "reasons": []}
    if regime:
        na["regime"] = regime
    (st / D.NEXT_ACTION).write_text(json.dumps(na), encoding="utf-8")
    return st, {"auto_submit": auto_submit, "sell_wait_seconds": 30, "sell_poll_seconds": 10}


def _morning(st, kis, ops, t=datetime(2026, 10, 2, 10, 0, 0)):
    clk = Clock(t)
    rep = D.morning(kis, state_dir=st, cfg=CFG, ops=ops, clock=clk, sleep=clk.sleep, is_market_open=OPEN,
                    next_trading_day=HOL.get, alert=NOALERT)
    return rep, clk


def _afternoon(st, kis, clk):
    clk.t = clk.t.replace(hour=15, minute=25, second=0)
    return D.afternoon(kis, state_dir=st, cfg=CFG, clock=clk, is_market_open=OPEN, next_trading_day=HOL.get,
                       alert=NOALERT)


HALF = {"status": "OK", "exposure": 0.5, "regime": "below"}


# ---------------- 1. 체결 조회
def test_day_orders_asks_for_all_orders_including_unfilled():
    kis = SimKIS(fill_mode="none")
    kis.place_order_cash("BUY", "005930", 1, "limit", 70100)
    rows = K.fetch_day_orders(kis, "20260922")
    assert kis.last_ccld_dvsn == "00" and len(rows) == 1 and rows[0]["tot_ccld_qty"] == "0"


def test_day_orders_truncated_is_loud():
    class Many:
        def inquire_daily_ccld(self, **kw):
            return {"rows": [], "pages": K.DAY_ORDERS_MAX_PAGES + 1}
    with pytest.raises(RuntimeError, match="DAY_ORDERS_TRUNCATED"):
        K.fetch_day_orders(Many(), "20260922")


# ---------------- 5. 복구 전이
def test_recovery_from_submitting_is_persisted_and_reloadable(tmp_path):
    log = tmp_path / "orders.jsonl"
    o = {"order_id": "X|1", "intent_id": "X", "attempt_no": 1, "code": "005930", "side": "BUY", "limit_price": 70100,
         "requested_qty": 1, "rebalance_id": "R"}
    E.OrderBook(log).record(E._ev(o, "SUBMITTING", datetime(2026, 10, 1, 10)))  # 여기서 프로세스가 죽었다고 치자
    book = E.OrderBook(log)
    assert book.state("X|1") == "UNKNOWN_PENDING" and len(log.read_text(encoding="utf-8").splitlines()) == 2
    E.resolve_unknown(book, [], datetime(2026, 10, 1, 10, 5), query_complete=True)
    assert E.OrderBook(log).state("X|1") == "NOT_RECEIVED"  # 예전엔 여기서 INVALID_TRANSITION


# ---------------- 10. 주문마다 발주 창
def test_window_rechecked_per_order(tmp_path):
    kis = SimKIS()
    clk = Clock(datetime(2026, 10, 1, 15, 19, 0))
    orders = [{"order_id": f"R|{c}|BUY|v|1", "intent_id": f"R|{c}|BUY|v", "attempt_no": 1, "code": c, "side": "BUY",
               "limit_price": 70100, "requested_qty": 1, "rebalance_id": "R"} for c in CODES[:4]]

    def slow_hook(o):  # 건마다 호가를 받는 데 40초 걸린다
        clk.sleep(40)
        return o, None
    res = E.submit_orders(orders, kis, E.OrderBook(tmp_path / "o.jsonl"), clk(), CFG, clock=clk, pre_submit=slow_hook)
    assert res["status"] == "STOP" and res["reasons"][0].startswith("WINDOW_CLOSED_MID_RUN")
    assert len(kis.orders) == 1  # 15:19:40 한 건만. 15:20:20 이후는 안 나감


# ---------------- 8. 노출 출처 일치
def test_target_weights_must_match_summary_exposure():
    now = datetime(2026, 10, 1, 10)
    quotes = {c: K.fetch_quote(SimKIS(), c, lambda: now) for c in CODES}
    orders, s = O.plan_orders(_target(), selection_date="20260930", execution_date="20261001", exposure=0.5,
                              strategy_equity=60e6, holdings={}, cash_available=1e12, quotes=quotes, now=now,
                              cfg=CFG, phase="BUY")
    assert orders == [] and s["reasons"][0].startswith("TARGET_EXPOSURE_MISMATCH")


# ---------------- 2·3. 노출 변경 실패는 현재 목표로 안 올림 / 오후 순서
def test_scale_buy_failure_is_not_promoted(tmp_path):
    st, ops = _state(tmp_path, "SCALE", regime={"status": "OK", "exposure": 1.0, "regime": "above"}, auto_submit=True)
    (st / D.CURRENT).write_text(json.dumps({**json.loads((st / D.CURRENT).read_text(encoding="utf-8")),
                                            "exposure": 0.5}), encoding="utf-8")
    kis = SimKIS()
    kis.prvs = 1_000_000  # 매수가능 100만원 -> BUY_CASH_SHORT
    rep, clk = _morning(st, kis, ops)
    assert rep["status"] == "STOP" and any("BUY_CASH_SHORT" in r for r in rep["reasons"]) and kis.orders == []
    aft = _afternoon(st, kis, clk)
    assert aft["status"] == "STOP" and aft["promoted"] is False
    assert json.loads((st / D.CURRENT).read_text(encoding="utf-8"))["exposure"] == 0.5


def test_partial_fill_does_not_reach_exposure_and_is_not_promoted(tmp_path):
    st, ops = _state(tmp_path, "SCALE", regime=HALF, auto_submit=True)
    kis = SimKIS(fill_mode="part")
    rep, clk = _morning(st, kis, ops)
    aft = _afternoon(st, kis, clk)
    assert aft["promoted"] is False and any(r.startswith("EXPOSURE_NOT_ACHIEVED") for r in aft["reasons"])


def test_afternoon_syncs_before_cancel(tmp_path):
    st, ops = _state(tmp_path, "SCALE", regime=HALF, auto_submit=True)
    kis = SimKIS()  # 즉시 전량 체결. 아침엔 동기화가 없어 장부는 ACCEPTED 로 남는다
    rep, clk = _morning(st, kis, ops)
    assert rep["status"] == "OK", rep["reasons"]
    aft = _afternoon(st, kis, clk)
    assert kis.cancels == []  # 예전엔 체결된 주문을 취소하러 가서 CANCELLED_UNFILLED 로 적었다
    assert aft["status"] == "OK" and aft["promoted"] is True and aft["incidents"] == []


# ---------------- 4. 사고 드러내기
def test_evening_surfaces_order_missing_from_day_query(tmp_path):
    st = tmp_path / "state"
    book = E.OrderBook(st / "orders.jsonl")
    o = {"order_id": "R|005930|BUY|v|1", "intent_id": "R|005930|BUY|v", "attempt_no": 1, "code": "005930",
         "side": "BUY", "limit_price": 70100, "requested_qty": 1, "rebalance_id": "R"}
    book.record(E._ev(o, "SUBMITTING", datetime(2026, 9, 30, 10)))
    book.record(E._ev(o, "ACCEPTED", datetime(2026, 9, 30, 10), broker_order_no="9999999999"))
    idx = tmp_path / "idx.csv"
    dates = [d.strftime("%Y%m%d") for d in pd.bdate_range(end=pd.Timestamp("20261001"), periods=230)]
    pd.DataFrame({"date": dates, "index_code": "2001", "close": range(800, 1030), "fetched_at": "x"}).to_csv(idx, index=False)
    rep = D.evening(SimKIS(), state_dir=st, cfg=CFG, thresholds={"index_code": "2001", "index_window": 200},
                    index_csv=idx, clock=Clock(datetime(2026, 10, 1, 20, 20)), is_market_open=OPEN,
                    next_trading_day=HOL.get, alert=NOALERT)
    assert rep["status"] == "STOP" and any(i["type"] == "ORDER_NOT_IN_DAY_QUERY" for i in rep["incidents"])


# ---------------- 6. 다음 거래일 1회 재주문
def _cancelled_first_attempt(st, day="2026-10-01", side="BUY", filled=0):
    book = E.OrderBook(st / "orders.jsonl")
    o = {"order_id": f"KOSPI_MCAP_QUARTERLY_V2|20260930|000100|{side}|v|1", "intent_id": f"KOSPI_MCAP_QUARTERLY_V2|20260930|000100|{side}|v",
         "attempt_no": 1, "code": "000100", "side": side, "limit_price": 70100, "requested_qty": 10,
         "rebalance_id": "KOSPI_MCAP_QUARTERLY_V2|20260930"}
    t = datetime.fromisoformat(day + "T10:00:00")
    book.record(E._ev(o, "SUBMITTING", t))
    book.record(E._ev(o, "ACCEPTED", t, broker_order_no="0000000077"))
    if filled:
        book.record(E._ev(o, "PARTIALLY_FILLED", t, filled_qty=filled))
    book.record(E._ev(o, "PARTIAL_CANCELLED" if filled else "CANCELLED_UNFILLED", t.replace(hour=15, minute=25)))
    return o


def test_replay_shadow_plans_remaining_without_sending(tmp_path):
    st, ops = _state(tmp_path)
    _cancelled_first_attempt(st, filled=4)
    kis = SimKIS()
    rep, _ = _morning(st, kis, ops)
    r = rep["replay"]
    assert r["planned"] == 1 and r["orders"][0]["requested_qty"] == 6 and r["orders"][0]["order_id"].endswith("|2")
    assert kis.orders == [] and "NO_SUBMIT_SHADOW" in rep["reasons"]


def test_replay_armed_sends_attempt_two_once(tmp_path):
    st, ops = _state(tmp_path, auto_submit=True)
    _cancelled_first_attempt(st)
    kis = SimKIS()
    rep, _ = _morning(st, kis, ops)
    assert rep["replay"]["submitted"] == 1 and len(kis.orders) == 1 and (st / D.PENDING).exists()
    assert D.plan_replays(E.OrderBook(st / "orders.jsonl"), "20261002", HOL.get) == []  # 2차는 다시 안 함


def test_replay_skips_stale_days_and_latch_blocks_buys(tmp_path):
    st, ops = _state(tmp_path, auto_submit=True)
    _cancelled_first_attempt(st, day="2026-09-30")  # 이틀 전 것 -> 대상 아님
    assert D.plan_replays(E.OrderBook(st / "orders.jsonl"), "20261002", HOL.get) == []
    (tmp_path / "b").mkdir()
    st2, ops2 = _state(tmp_path / "b", auto_submit=True)
    _cancelled_first_attempt(st2)
    (st2 / "latch.json").write_text("{}", encoding="utf-8")
    kis = SimKIS()
    rep, _ = _morning(st2, kis, ops2)
    assert kis.orders == [] and any(r.startswith("LATCH_ACTIVE_NO_BUY_REPLAY") for r in rep["replay"]["reasons"])


# ---------------- 17. 래치 청산: 한 종목 불일치가 전체를 막지 않는다 (사용자 결정 (나), 2026-09-19)
from paper.strategies.kospi_mcap_quarterly_v2.src import run_execution_day as R  # noqa: E402


def _latched_state(tmp_path, ledger, broker):
    sd = tmp_path / "state"
    sd.mkdir()
    lines = [json.dumps({"fill_id": f"{c}|{q}", "order_id": c, "code": c, "side": "BUY", "qty": q, "amount": q * 70000,
                         "trade_date": "20261001", "observed_at": "2026-10-01T10:00:00"}) for c, q in ledger.items()]
    (sd / "fills.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    (sd / "latch.json").write_text(json.dumps({"triggered_as_of": "20261001"}), encoding="utf-8")
    kis = SimKIS()
    kis.hold = dict(broker)
    return sd, kis


def test_liquidate_sells_the_rest_when_one_code_mismatches(tmp_path):
    sd, kis = _latched_state(tmp_path, {"005930": 10, "000660": 5}, {"005930": 9, "000660": 5})
    r = R.liquidate_after_latch(kis, state_dir=sd, cfg=CFG, no_submit=False, clock=Clock(datetime(2026, 10, 2, 9, 30)))
    sent = {o["pdno"]: o["ord_qty"] for o in kis.orders}
    assert sent == {"005930": "9", "000660": "5"}  # 예전엔 둘 다 0건
    assert r["status"] == "STOP" and "005930(ledger=10,broker=9)" in r["reasons"][-1]


def test_liquidate_skips_code_missing_in_account_but_sells_others(tmp_path):
    sd, kis = _latched_state(tmp_path, {"005930": 10, "000660": 5}, {"000660": 5})
    r = R.liquidate_after_latch(kis, state_dir=sd, cfg=CFG, no_submit=False, clock=Clock(datetime(2026, 10, 2, 9, 30)))
    assert [(o["pdno"], o["ord_qty"]) for o in kis.orders] == [("000660", "5")] and r["status"] == "STOP"


# ---------------- 목표 변경 이벤트 로그 (DDD 도메인 이벤트, 2026-09-19)
def _events(st):
    p = st / D.TARGET_EVENTS
    return [json.loads(x) for x in p.read_text(encoding="utf-8").splitlines()] if p.exists() else []


def test_target_events_record_set_promote_and_failed_attempt(tmp_path):
    st, ops = _state(tmp_path, "SCALE", regime=HALF, auto_submit=True)
    ev = _events(st)
    assert [(e["event"], e["exposure_from"], e["exposure_to"]) for e in ev] == [("QUARTERLY_SET", None, 1.0)]
    kis = SimKIS()
    _, clk = _morning(st, kis, ops)
    _afternoon(st, kis, clk)
    ev = _events(st)
    assert (ev[-1]["event"], ev[-1]["exposure_from"], ev[-1]["exposure_to"]) == ("SCALE_PROMOTED", 1.0, 0.5)


def test_target_events_record_not_promoted(tmp_path):
    st, ops = _state(tmp_path, "SCALE", regime=HALF, auto_submit=True)
    kis = SimKIS(fill_mode="part")
    _, clk = _morning(st, kis, ops)
    _afternoon(st, kis, clk)
    last = _events(st)[-1]
    assert last["event"] == "SCALE_NOT_PROMOTED" and last["exposure_from"] == 1.0 and last["exposure_to"] == 0.5
    assert any(r.startswith("EXPOSURE_NOT_ACHIEVED") for r in last["reasons"])
