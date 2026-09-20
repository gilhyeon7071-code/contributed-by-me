"""order_capability_canary — 보유가 있어도 매도하지 않고 하한가 매수 1주만 낸다(2026-09-19). 가짜 한투만."""
from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import order_capability_canary as C  # noqa: E402


class FakeClient:
    def __init__(self, rt_cd="0", cancel_rt="0"):
        self.orders, self.cancels = [], []
        self.rt_cd, self.cancel_rt = rt_cd, cancel_rt

    def inquire_balance_positions(self, max_pages=10):  # 다른 전략의 보유가 있는 계좌
        return {"ok": True, "rows": [{"pdno": "000660", "hldg_qty": "5", "evlu_amt": "1000000"}]}

    def inquire_price(self, code):
        return {"output": {"stck_mxpr": "91000", "stck_prpr": "70000", "stck_llam": "49000"}}

    def place_order_cash(self, side, code, qty, order_type, price):
        self.orders.append((side, code, qty, order_type, price))
        return {"ok": self.rt_cd == "0", "rt_cd": self.rt_cd, "msg_cd": "X", "msg1": "m",
                "ord_no": "0000000001", "org_no": "00950"}

    def cancel_order(self, org_order_no, org_order_branch_no, cancel_all=True, order_dvsn="00"):
        self.cancels.append(org_order_no)
        return {"rt_cd": self.cancel_rt, "msg_cd": "Y", "msg1": "c"}


@pytest.fixture
def session(monkeypatch):
    monkeypatch.setattr(C, "_is_trading_day", lambda now: True)
    monkeypatch.setattr(C, "_in_session", lambda now: True)
    monkeypatch.setattr(C.time, "sleep", lambda s: None)

    def install(client):
        mod = types.ModuleType("kis_order_client")
        mod.KISOrderClient = types.SimpleNamespace(from_env=lambda mock: client)
        monkeypatch.setitem(sys.modules, "kis_order_client", mod)
        return client
    return install


def test_never_sells_holdings_buys_at_lower_limit(session):
    cl = session(FakeClient())
    out = C.run_canary(mock=True, dry_run=False)
    assert cl.orders == [("BUY", "005930", 1, "limit", 49000)]
    assert out["status"] == "ORDER_OK" and C.build_alert_text(out) is None and cl.cancels == ["0000000001"]


def test_rejected_order_alarms(session):
    session(FakeClient(rt_cd="1"))
    out = C.run_canary(mock=True, dry_run=False)
    assert out["status"] == "ORDER_REJECTED" and C.build_alert_text(out) is not None


def test_cancel_failure_alarms_loudly(session):
    session(FakeClient(cancel_rt="1"))
    out = C.run_canary(mock=True, dry_run=False)
    txt = C.build_alert_text(out)
    assert out["status"] == "CANCEL_FAILED" and "BUY 1주 @ 49,000" in out["reason"] and "수동 취소" in txt


def test_dry_run_sends_nothing(session):
    cl = session(FakeClient())
    out = C.run_canary(mock=True, dry_run=True)
    assert out["status"] == "DRY_RUN" and cl.orders == [] and out["side"] == "BUY"
