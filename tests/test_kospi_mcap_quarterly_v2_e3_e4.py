"""분기 시총(V2) E3 체결 확인 · E4 원장/래치 · 사고 4종 — 합성 입력. 각 감지기가 이상에서 우는지 본다."""
from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest

from paper.strategies.kospi_mcap_quarterly_v2.src import execution as X
from paper.strategies.kospi_mcap_quarterly_v2.src import fills as F
from paper.strategies.kospi_mcap_quarterly_v2.src import incidents as I
from paper.strategies.kospi_mcap_quarterly_v2.src import ledger as L

T0 = datetime(2026, 10, 1, 9, 10, 0)
CFG = {"fee_pct": 0.00015, "sell_tax_pct": 0.002, "settlement_trading_days": 2, "latch_nav_ratio": 0.75}
HOL = {"20261001": "20261002", "20261002": "20261005", "20261005": "20261006", "20261006": "20261007",
       "20261007": "20261008"}


def ntd(d):
    return HOL[d]


def _accepted(book, code="005930", side="BUY", qty=10, no="0000000001"):
    intent = f"R|{code}|{side}|v1"
    o = {"order_id": f"{intent}|1", "intent_id": intent, "attempt_no": 1, "code": code, "side": side,
         "limit_price": 1000, "requested_qty": qty, "rebalance_id": "R"}
    book.record(X._ev(o, "SUBMITTING", T0))
    book.record(X._ev(o, "ACCEPTED", T0, broker_order_no=no, broker_org_no="00950"))
    return o


def _row(no="0000000001", code="005930", side="02", qty=10, cum=0, amt=0, rmn=None, cncl="N"):
    return {"odno": no, "pdno": code, "sll_buy_dvsn_cd": side, "ord_qty": str(qty), "ord_unpr": "1000",
            "tot_ccld_qty": str(cum), "tot_ccld_amt": str(amt), "rmn_qty": str(qty - cum if rmn is None else rmn),
            "cncl_yn": cncl, "cncl_cfrm_qty": "0", "ord_dt": "20261001"}


# ---------------- E3

def test_incremental_fills_and_idempotent(tmp_path):
    book = X.OrderBook(tmp_path / "o.jsonl")
    o = _accepted(book)
    fp = tmp_path / "f.jsonl"
    r1 = F.sync_fills(book, [_row(cum=4, amt=4000)], fp, T0)
    assert r1["status"] == "OK" and len(r1["new_fills"]) == 1 and book.state(o["order_id"]) == "PARTIALLY_FILLED"
    r2 = F.sync_fills(book, [_row(cum=4, amt=4000)], fp, T0)
    assert r2["new_fills"] == []
    r3 = F.sync_fills(X.OrderBook(tmp_path / "o.jsonl"), [_row(cum=10, amt=10030)], fp, T0)
    fills = F.load_fills(fp)
    assert [f["qty"] for f in fills] == [4, 6] and fills[1]["amount"] == 6030
    assert X.OrderBook(tmp_path / "o.jsonl").state(o["order_id"]) == "FILLED" and r3["status"] == "OK"


def test_amount_uses_tot_ccld_amt_not_avg(tmp_path):
    book = X.OrderBook(tmp_path / "o.jsonl")
    _accepted(book, qty=976)
    F.sync_fills(book, [_row(qty=976, cum=976, amt=15532930)], tmp_path / "f.jsonl", T0)
    assert F.load_fills(tmp_path / "f.jsonl")[0]["amount"] == 15532930


@pytest.mark.parametrize("row,kind", [
    (_row(cum=11, amt=11000), "OVERFILL"),
    (_row(code="000660", cum=1, amt=1000), "BROKER_ROW_MISMATCH"),
    (_row(side="01", cum=1, amt=1000), "BROKER_ROW_MISMATCH"),
])
def test_e3_alarms(tmp_path, row, kind):
    book = X.OrderBook(tmp_path / "o.jsonl")
    _accepted(book)
    r = F.sync_fills(book, [row], tmp_path / "f.jsonl", T0)
    assert r["status"] == "STOP" and r["incidents"][0]["type"] == kind


def test_fill_decreased_alarm(tmp_path):
    book = X.OrderBook(tmp_path / "o.jsonl")
    _accepted(book)
    F.sync_fills(book, [_row(cum=5, amt=5000)], tmp_path / "f.jsonl", T0)
    r = F.sync_fills(book, [_row(cum=3, amt=3000)], tmp_path / "f.jsonl", T0)
    assert r["incidents"][0]["type"] == "FILL_DECREASED"


def test_fill_after_terminal_alarm_keeps_fill(tmp_path):
    book = X.OrderBook(tmp_path / "o.jsonl")
    o = _accepted(book)
    book.record(X._ev(book.orders[o["order_id"]], "CANCELLED_UNFILLED", T0))
    r = F.sync_fills(book, [_row(cum=2, amt=2000)], tmp_path / "f.jsonl", T0)
    assert r["incidents"][0]["type"] == "FILL_AFTER_TERMINAL" and len(F.load_fills(tmp_path / "f.jsonl")) == 1


def test_cancel_not_confirmed_alarm_and_orphans(tmp_path):
    book = X.OrderBook(tmp_path / "o.jsonl")
    o = _accepted(book)
    book.record(X._ev(book.orders[o["order_id"]], "CANCELLED_UNFILLED", T0))
    r = F.sync_fills(book, [_row(), _row(no="9999999999", code="000660")], tmp_path / "f.jsonl", T0)
    assert r["incidents"][0]["type"] == "CANCEL_NOT_CONFIRMED" and r["orphans"] == 1


def test_missing_row_listed_not_filled(tmp_path):
    book = X.OrderBook(tmp_path / "o.jsonl")
    o = _accepted(book)
    r = F.sync_fills(book, [], tmp_path / "f.jsonl", T0)
    assert r["missing"] == [o["order_id"]] and r["status"] == "OK"


# ---------------- E4

def _fill(side, qty, amount, day="20261001", code="005930", n=1):
    return {"fill_id": f"x|{side}|{n}", "code": code, "side": side, "qty": qty, "amount": amount, "trade_date": day,
            "observed_at": f"{day}T10:00:{n:02d}"}


def test_ledger_cash_settlement_split_and_costs():
    fl = [_fill("BUY", 10, 100000, n=1), _fill("SELL", 4, 44000, n=2)]
    led = L.build_ledger(1_000_000, fl, as_of="20261001", closes={"005930": 11000}, cfg=CFG, next_trading_day=ntd)
    buy_cost = 100000 * 1.00015
    sell_net = 44000 - 44000 * 0.00015 - 44000 * 0.002
    assert led["cash_economic"] == pytest.approx(1_000_000 - buy_cost + sell_net)
    assert led["cash_settled"] == 1_000_000  # 둘 다 D+2 전
    assert led["payable"] == pytest.approx(buy_cost) and led["receivable"] == pytest.approx(sell_net)
    assert led["positions"][0]["qty"] == 6 and led["nav"] == pytest.approx(led["cash_economic"] + 66000)
    assert led["realized_pnl"] == pytest.approx(sell_net - buy_cost * 4 / 10)
    later = L.build_ledger(1_000_000, fl, as_of="20261005", closes={"005930": 11000}, cfg=CFG, next_trading_day=ntd)
    assert later["cash_settled"] == pytest.approx(later["cash_economic"]) and later["payable"] == 0


def test_ledger_rebuild_is_order_independent():
    fl = [_fill("BUY", 10, 100000, n=1), _fill("SELL", 4, 44000, n=2)]
    a = L.build_ledger(1_000_000, fl, as_of="20261001", closes={"005930": 11000}, cfg=CFG, next_trading_day=ntd)
    b = L.build_ledger(1_000_000, fl[::-1], as_of="20261001", closes={"005930": 11000}, cfg=CFG, next_trading_day=ntd)
    assert a == b


def test_missing_close_makes_nav_incomplete_and_latch_not_evaluated(tmp_path):
    led = L.build_ledger(1_000_000, [_fill("BUY", 10, 900000)], as_of="20261001", closes={}, cfg=CFG,
                         next_trading_day=ntd)
    assert led["status"] == "INCOMPLETE" and led["nav"] is None
    assert L.evaluate_latch(led, CFG, tmp_path / "latch.json")["active"] is False
    assert not (tmp_path / "latch.json").exists()


def test_negative_position_alarm():
    led = L.build_ledger(1_000_000, [_fill("SELL", 1, 1000)], as_of="20261001", closes={}, cfg=CFG, next_trading_day=ntd)
    assert led["incidents"][0]["type"] == "NEGATIVE_POSITION" and led["status"] == "INCOMPLETE"


@pytest.mark.parametrize("close,active", [(75000 - 1, True), (75000, True), (75000 + 20, False)])
def test_latch_boundary_and_persistence(tmp_path, close, active):
    fl = [_fill("BUY", 10, 1_000_000)]
    led = L.build_ledger(1_000_150, fl, as_of="20261001", closes={"005930": close}, cfg=CFG, next_trading_day=ntd)
    # 자본 1,000,150 - (1,000,000 + 수수료 150) = 현금 0 -> NAV = 10*close, 기준 750,112.5
    assert led["nav"] == pytest.approx(10 * close)
    lat = L.evaluate_latch(led, CFG, tmp_path / "latch.json")
    assert lat["active"] is active
    if lat["active"]:
        led2 = L.build_ledger(1_000_150, fl, as_of="20261002", closes={"005930": 999999}, cfg=CFG, next_trading_day=ntd)
        assert L.evaluate_latch(led2, CFG, tmp_path / "latch.json")["active"] is True  # 자동 해제 없음


# ---------------- 사고 4종

def test_incident_1_intent_result():
    assert I.intent_vs_result({"A": 10}, {"A": 10}, {}) == []
    assert I.intent_vs_result({"A": 10}, {"A": 7}, {})[0]["type"] == "1_INTENT_RESULT_MISMATCH"
    assert I.intent_vs_result({"A": 10}, {"A": 7}, {"A": "PARTIAL_CANCELLED"}) == []
    assert I.intent_vs_result({}, {"B": 1}, {})[0]["code"] == "B"


def test_incident_2_silent_failure(tmp_path):
    p = tmp_path / "a.json"
    assert I.silent_failures([(p, datetime.now())])[0]["detail"] == "MISSING"
    p.write_text("{}")
    assert I.silent_failures([(p, datetime.now() + timedelta(hours=1))])[0]["detail"] == "STALE"
    assert I.silent_failures([(p, datetime.now() - timedelta(hours=1))]) == []


def test_incident_3_unauthorized_halt():
    assert I.unauthorized_halts(["20261001"], ["20261001"]) == []
    assert I.unauthorized_halts(["20261001", "20261002"], ["20261001"])[0]["date"] == "20261002"


def test_incident_4_definition_violations():
    ok = [{"order_id": "a", "side": "BUY", "execution_date": "20261001"}]
    assert I.definition_violations(ok, allowed_execution_dates=["20261001"], latch_active=False) == []
    bad_date = I.definition_violations(ok, allowed_execution_dates=["20261002"], latch_active=False)
    assert bad_date[0]["detail"] == "DATE_NOT_ALLOWED"
    assert I.definition_violations(ok, allowed_execution_dates=["20261001"], latch_active=True)[0]["detail"] == "BUY_AFTER_LATCH"
    over = I.definition_violations([], allowed_execution_dates=[], latch_active=False,
                                   positions_value={"A": 215, "B": 210}, nav=1000, cap=0.20, exposure=1.0)
    assert [v["code"] for v in over] == ["A"]
