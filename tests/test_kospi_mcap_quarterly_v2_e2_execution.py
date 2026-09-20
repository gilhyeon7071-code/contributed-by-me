"""분기 시총(V2) E2 발주·상태기계 — 가짜 브로커로. 실제 발주 없음."""
from __future__ import annotations

import json
from datetime import datetime
from types import SimpleNamespace

import pytest

from paper.strategies.kospi_mcap_quarterly_v2.src import execution as X

CFG = {"submit_start": "09:05:00", "submit_end": "15:20:00",
       "submit_blackouts": [["09:19:30", "09:21:30"]], "mock_only": True}
T0 = datetime(2026, 10, 1, 9, 10, 0)


def _o(code="005930", side="BUY", qty=10, price=250000, attempt=1):
    intent = f"R|{code}|{side}|v1"
    return {"order_id": f"{intent}|{attempt}", "intent_id": intent, "attempt_no": attempt, "code": code,
            "side": side, "limit_price": price, "requested_qty": qty, "rebalance_id": "R"}


class Fake:
    def __init__(self, responses=None, mock=True):
        self.cfg = SimpleNamespace(mock=mock)
        self.responses = list(responses or [])
        self.calls = []
        self.cancels = []

    def place_order_cash(self, **kw):
        self.calls.append(kw)
        r = self.responses.pop(0) if self.responses else {"ok": True, "ord_no": f"{len(self.calls):010d}", "org_no": "00950"}
        if isinstance(r, Exception):
            raise r
        return r

    def cancel_order(self, **kw):
        self.cancels.append(kw)
        return {"ok": True, "msg1": "ok"}


def test_accept_and_log_replay(tmp_path):
    log = tmp_path / "ev.jsonl"
    book = X.OrderBook(log)
    r = X.submit_orders([_o(), _o("000660")], Fake(), book, T0, CFG)
    assert r["accepted"] == 2 and r["status"] == "OK"
    again = X.OrderBook(log)
    assert again.state(_o()["order_id"]) == "ACCEPTED" and again.orders[_o()["order_id"]]["broker_order_no"]


def test_rerun_does_not_resubmit(tmp_path):
    log = tmp_path / "ev.jsonl"
    f = Fake()
    X.submit_orders([_o()], f, X.OrderBook(log), T0, CFG)
    r = X.submit_orders([_o()], f, X.OrderBook(log), T0, CFG)
    assert len(f.calls) == 1 and r["skipped_existing"] == 1


@pytest.mark.parametrize("hhmm", ["09:04:59", "15:20:01", "09:20:00"])
def test_window_and_blackout_block_everything(tmp_path, hhmm):
    f = Fake()
    now = datetime.strptime(f"2026-10-01 {hhmm}", "%Y-%m-%d %H:%M:%S")
    r = X.submit_orders([_o()], f, X.OrderBook(tmp_path / "ev.jsonl"), now, CFG)
    assert r["status"] == "STOP" and f.calls == []


def test_prod_client_refused(tmp_path):
    f = Fake(mock=False)
    r = X.submit_orders([_o()], f, X.OrderBook(tmp_path / "ev.jsonl"), T0, CFG)
    assert r["reasons"] == ["CLIENT_NOT_MOCK"] and f.calls == []


def test_network_error_is_unknown_and_stops_rest(tmp_path):
    f = Fake([ConnectionError("Connection aborted")])
    book = X.OrderBook(tmp_path / "ev.jsonl")
    r = X.submit_orders([_o(), _o("000660")], f, book, T0, CFG)
    assert r["status"] == "STOP" and r["unknown"] == 1 and len(f.calls) == 1
    assert book.state(_o()["order_id"]) == "UNKNOWN_PENDING"
    # 다시 돌려도 모르는 주문은 재전송하지 않는다
    r2 = X.submit_orders([_o()], f, X.OrderBook(tmp_path / "ev.jsonl"), T0, CFG)
    assert len(f.calls) == 1 and r2["skipped_existing"] == 1


def test_local_validation_error_is_rejected_local(tmp_path):
    f = Fake([ValueError("Invalid stock code: 0120G0")])
    book = X.OrderBook(tmp_path / "ev.jsonl")
    r = X.submit_orders([_o("0120G0"), _o("000660")], f, book, T0, CFG)
    assert book.state(_o("0120G0")["order_id"]) == "REJECTED_LOCAL" and r["accepted"] == 1


def test_broker_reject_and_ok_without_order_no(tmp_path):
    f = Fake([{"ok": False, "rt_cd": "1", "msg1": "호가단위 오류"}, {"ok": True, "ord_no": ""}])
    book = X.OrderBook(tmp_path / "ev.jsonl")
    r = X.submit_orders([_o(), _o("000660"), _o("402340")], f, book, T0, CFG)
    assert book.state(_o()["order_id"]) == "REJECTED"
    assert book.state(_o("000660")["order_id"]) == "UNKNOWN_PENDING" and r["status"] == "STOP"
    assert len(f.calls) == 2  # 세 번째는 멈춤


def test_crash_after_submitting_record_becomes_unknown(tmp_path):
    log = tmp_path / "ev.jsonl"
    book = X.OrderBook(log)
    book.record(X._ev(_o(), "SUBMITTING", T0))
    assert X.OrderBook(log).state(_o()["order_id"]) == "UNKNOWN_PENDING"


def test_invalid_transition_and_overfill(tmp_path):
    book = X.OrderBook(tmp_path / "ev.jsonl")
    with pytest.raises(ValueError, match="INVALID_TRANSITION"):
        book.record(X._ev(_o(), "FILLED", T0))
    X.submit_orders([_o()], Fake(), book, T0, CFG)
    with pytest.raises(ValueError, match="OVERFILL"):
        book.record(X._ev(_o(), "PARTIALLY_FILLED", T0, filled_qty=11))
    book.record(X._ev(_o(), "PARTIALLY_FILLED", T0, filled_qty=5))
    with pytest.raises(ValueError, match="FILL_DECREASED"):
        book.record(X._ev(_o(), "PARTIALLY_FILLED", T0, filled_qty=4))


def test_resolve_unknown_unique_match_none_and_ambiguous(tmp_path):
    f = Fake([ConnectionError("x")])
    book = X.OrderBook(tmp_path / "ev.jsonl")
    X.submit_orders([_o()], f, book, T0, CFG)
    row = {"odno": "0000009999", "ord_gno_brno": "00950", "pdno": "005930", "sll_buy_dvsn_cd": "02",
           "ord_qty": "10", "ord_unpr": "250000"}
    amb = X.resolve_unknown(book, [row, dict(row, odno="0000009998")], T0, query_complete=True)
    assert amb["ambiguous"] == [_o()["order_id"]] and book.state(_o()["order_id"]) == "UNKNOWN_PENDING"
    none = X.resolve_unknown(book, [], T0, query_complete=False)
    assert none["not_received"] == [] and book.state(_o()["order_id"]) == "UNKNOWN_PENDING"
    ok = X.resolve_unknown(book, [row], T0, query_complete=True)
    assert ok["accepted"] == [_o()["order_id"]] and book.orders[_o()["order_id"]]["broker_order_no"] == "0000009999"


def test_resolve_unknown_not_received_when_query_complete(tmp_path):
    book = X.OrderBook(tmp_path / "ev.jsonl")
    X.submit_orders([_o()], Fake([ConnectionError("x")]), book, T0, CFG)
    r = X.resolve_unknown(book, [], T0, query_complete=True)
    assert r["not_received"] == [_o()["order_id"]] and book.state(_o()["order_id"]) == "NOT_RECEIVED"


def test_cancel_remaining_after_1520_only(tmp_path):
    f = Fake()
    book = X.OrderBook(tmp_path / "ev.jsonl")
    X.submit_orders([_o(), _o("000660")], f, book, T0, CFG)
    book.record(X._ev(_o(), "PARTIALLY_FILLED", T0, filled_qty=4))
    early = X.cancel_remaining(book, f, datetime(2026, 10, 1, 15, 0), CFG)
    assert early["status"] == "STOP" and f.cancels == []
    r = X.cancel_remaining(book, f, datetime(2026, 10, 1, 15, 20, 30), CFG)
    assert book.state(_o()["order_id"]) == "PARTIAL_CANCELLED"
    assert book.state(_o("000660")["order_id"]) == "CANCELLED_UNFILLED" and len(r["cancelled"]) == 2


def test_cancel_failure_is_loud(tmp_path):
    class Bad(Fake):
        def cancel_order(self, **kw):
            raise TimeoutError("t")
    f = Bad()
    book = X.OrderBook(tmp_path / "ev.jsonl")
    X.submit_orders([_o()], f, book, T0, CFG)
    r = X.cancel_remaining(book, f, datetime(2026, 10, 1, 15, 21), CFG)
    assert r["status"] == "STOP" and book.state(_o()["order_id"]) == "CANCEL_FAILED"


def test_replay_once_only(tmp_path):
    f = Fake()
    book = X.OrderBook(tmp_path / "ev.jsonl")
    X.submit_orders([_o()], f, book, T0, CFG)
    book.record(X._ev(_o(), "PARTIALLY_FILLED", T0, filled_qty=3))
    X.cancel_remaining(book, f, datetime(2026, 10, 1, 15, 21), CFG)
    c = X.replay_candidates(book)
    assert c == [{"intent_id": _o()["intent_id"], "code": "005930", "side": "BUY", "remaining_qty": 7, "next_attempt_no": 2}]
    X.submit_orders([_o(qty=7, attempt=2)], f, book, datetime(2026, 10, 2, 9, 10), CFG)
    assert X.replay_candidates(book) == []
    r = X.submit_orders([_o(qty=7, attempt=3)], f, book, datetime(2026, 10, 2, 9, 10), CFG)
    assert any(x.startswith("ATTEMPT_LIMIT") for x in r["reasons"])
