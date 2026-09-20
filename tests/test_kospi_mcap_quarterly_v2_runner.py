"""분기 시총(V2) 집행일 실행기 — 가짜 한투(모의)로 test-one 전 경로와 재구성 리허설. 실제 발주 없음."""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest

from paper.strategies.kospi_mcap_quarterly_v2.src import execution as X
from paper.strategies.kospi_mcap_quarterly_v2.src import run_execution_day as R

CFG = json.loads((R.DEFAULT_STRATEGY_CFG).read_text(encoding="utf-8"))
HOL = {"20260922": "20260923", "20260923": "20260928", "20260928": "20260929", "20261001": "20261002",
       "20261002": "20261005"}


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += timedelta(seconds=s)


class SimKIS:
    """모의 서버 흉내: 지정가가 호가를 건너면 즉시 체결(fill_mode='full'), 'none' 이면 미체결, 'part' 면 일부."""

    def __init__(self, fill_mode="full", ask=70100, bid=70000, raise_on_submit=None, fee=0.00015, tax=0.002):
        self.cfg = SimpleNamespace(mock=True)
        self.fill_mode, self.ask, self.bid = fill_mode, ask, bid
        self.raise_on_submit = raise_on_submit
        self.orders = []
        self.hold = {}
        self.prvs = 100_000_000
        self.ruse = 0
        self.fee, self.tax = fee, tax
        self.cancels = []

    def inquire_hoga(self, code):
        return {"ok": True, "output": {"askp1": str(self.ask), "bidp1": str(self.bid), "askp_rsqn1": "100",
                                       "bidp_rsqn1": "100", "aspr_acpt_hour": "100000"}}

    def inquire_psbl_order(self, code, order_price, ord_dvsn="01"):
        return {"ok": True, "output": {"nrcvb_buy_amt": str(self.prvs), "ruse_psbl_amt": str(self.ruse),
                                       "ord_psbl_cash": str(self.prvs), "max_buy_amt": str(self.prvs * 5)}}

    def inquire_balance_positions(self, max_pages=10):
        rows = [{"pdno": c, "hldg_qty": str(q), "prpr": str(self.bid)} for c, q in self.hold.items() if q]
        return {"ok": True, "rows": rows, "summary_rows": [{"dnca_tot_amt": "0", "prvs_rcdl_excc_amt": str(self.prvs),
                                                             "nass_amt": "0"}]}

    def place_order_cash(self, side, code, qty, order_type, price):
        if self.raise_on_submit:
            e, self.raise_on_submit = self.raise_on_submit, None
            no = f"{len(self.orders) + 1:010d}"
            self.orders.append(self._row(no, side, code, qty, price, 0))
            raise e
        no = f"{len(self.orders) + 1:010d}"
        crossed = (side == "BUY" and price >= self.ask) or (side == "SELL" and price <= self.bid)
        n = {"full": qty, "none": 0, "part": qty // 2}[self.fill_mode] if crossed else 0
        self.orders.append(self._row(no, side, code, qty, price, n))
        return {"ok": True, "ord_no": no, "org_no": "00950", "msg1": "ok", "rt_cd": "0"}

    def _row(self, no, side, code, qty, price, n):
        amt = n * price
        if side == "BUY":
            self.prvs -= int(amt * (1 + self.fee))
            self.hold[code] = self.hold.get(code, 0) + n
        else:
            net = int(amt * (1 - self.fee - self.tax))
            self.prvs += net
            self.ruse += net
            self.hold[code] = self.hold.get(code, 0) - n
        return {"odno": no, "pdno": code, "sll_buy_dvsn_cd": "02" if side == "BUY" else "01", "ord_qty": str(qty),
                "ord_unpr": str(price), "tot_ccld_qty": str(n), "tot_ccld_amt": str(amt), "rmn_qty": str(qty - n),
                "cncl_yn": "N", "cncl_cfrm_qty": "0", "ord_dt": "20260922", "ord_gno_brno": "00950"}

    def inquire_daily_ccld(self, start_ymd, end_ymd, ccld_dvsn="01", max_pages=30, **kw):
        # 실물 KIS 처럼: '01' = 체결분만, '00' = 전체 (2026-09-19 — 전체를 돌려주던 가짜가 결함을 가렸다)
        self.last_ccld_dvsn = ccld_dvsn
        rows = [dict(r) for r in self.orders]
        if ccld_dvsn == "01":
            rows = [r for r in rows if int(r["tot_ccld_qty"]) > 0]
        return {"ok": True, "rows": rows, "pages": 1}

    def cancel_order(self, org_order_no, org_order_branch_no, cancel_all=True):
        self.cancels.append(org_order_no)
        for r in self.orders:
            if r["odno"] == org_order_no:
                r["cncl_yn"] = "Y"
                r["cncl_cfrm_qty"] = r["rmn_qty"]
        return {"ok": True, "msg1": "cancel ok"}


def _run(tmp_path, kis, hhmmss=(10, 0, 0), no_submit=False, open_=True):
    clk = Clock(datetime(2026, 9, 22, *hhmmss))
    return R.test_one(kis, code="005930", qty=1, out_dir=tmp_path, cfg=CFG, no_submit=no_submit, clock=clk,
                      sleep=clk.sleep, next_trading_day=HOL.get, is_market_open=lambda d: open_), clk


def test_test_one_full_round_trip(tmp_path):
    kis = SimKIS()
    rep, _ = _run(tmp_path, kis)
    assert rep["status"] == "OK", rep["reasons"]
    assert rep["buy"]["final_state"] == "FILLED" and rep["sell"]["final_state"] == "FILLED"
    assert [o["ord_unpr"] for o in kis.orders] == ["70100", "70000"]  # (가) 매수 매도호가1, 매도 매수호가1
    c = rep["checks"]
    assert c["order_no_returned"] and c["ccld_row_matched"] and c["broker_holding_back_to_start"]
    assert c["buy_fee_inferred"] == 10  # int(70100*0.00015)
    assert c["same_day_reuse_rise"] > 0
    led = rep["after_sell"]["ledger"]
    assert led["positions"] == [] and led["status"] == "COMPLETE" and rep["incidents"] == []
    assert (tmp_path / "test_one_log.jsonl").exists()


def test_no_submit_sends_nothing_even_outside_window(tmp_path):
    kis = SimKIS()
    rep, _ = _run(tmp_path, kis, hhmmss=(20, 0, 0), no_submit=True)
    assert kis.orders == [] and "NO_SUBMIT_REHEARSAL" in rep["reasons"]
    assert rep["planned_buy"]["limit_price"] == 70100 and rep["window"].startswith("OUTSIDE")


@pytest.mark.parametrize("hhmmss,open_,reason", [((9, 0, 0), True, "OUTSIDE_SUBMIT_WINDOW"),
                                                  ((9, 20, 0), True, "SUBMIT_BLACKOUT"),
                                                  ((10, 0, 0), False, "MARKET_CLOSED")])
def test_blocked_times_send_nothing(tmp_path, hhmmss, open_, reason):
    kis = SimKIS()
    rep, _ = _run(tmp_path, kis, hhmmss=hhmmss, open_=open_)
    assert rep["status"] == "STOP" and rep["reasons"][0].startswith(reason) and kis.orders == []


def test_unfilled_buy_is_cancelled_and_sell_skipped(tmp_path):
    kis = SimKIS(fill_mode="none")
    rep, clk = _run(tmp_path, kis)
    assert kis.cancels == ["0000000001"] and rep["buy"]["final_state"] == "CANCELLED_UNFILLED"
    assert "BUY_NOT_FILLED_SELL_SKIPPED" in rep["reasons"] and len(kis.orders) == 1
    assert rep["incidents"] == [] and rep["status"] == "OK"
    assert clk.t >= datetime(2026, 9, 22, 10, 1, 0)  # 60초 기다린 뒤 취소


def test_submit_exception_becomes_unknown_then_resolved_by_query(tmp_path):
    kis = SimKIS(raise_on_submit=TimeoutError("read timeout"))
    rep, _ = _run(tmp_path, kis)
    ev = [json.loads(x) for x in (tmp_path / "orders.jsonl").read_text(encoding="utf-8").splitlines()]
    states = [e["state"] for e in ev if e["side"] == "BUY"]
    assert "UNKNOWN_PENDING" in states and "ACCEPTED" in states
    assert len([o for o in kis.orders if o["sll_buy_dvsn_cd"] == "02"]) == 1  # 재전송 없음
    assert rep["buy"]["final_state"] == "CANCELLED_UNFILLED"  # 가짜는 예외 주문을 미체결로 남긴다


def test_rerun_same_second_does_not_resend(tmp_path):
    kis = SimKIS()
    _run(tmp_path, kis)
    n = len(kis.orders)
    _run(tmp_path, kis)
    assert len(kis.orders) == n


def test_refuses_production_state_dir():
    with pytest.raises(SystemExit):
        R._refuse_production(R.PRODUCTION_STATE_DIR / "x")


def test_rebalance_buy_rehearsal_plans_without_sending(tmp_path):
    kis = SimKIS()
    target = pd.DataFrame({"code": ["005930", "000660"], "market_cap": [5e14, 3e14], "basket_weight": [0.5, 0.5],
                           "account_target_weight": [0.5, 0.5]})
    target.to_csv(tmp_path / "t.csv", index=False)
    (tmp_path / "s.json").write_text(json.dumps({"status": "OK", "selection_date": "20260930",
                                                 "d3": {"exposure": 1.0}}), encoding="utf-8")
    clk = Clock(datetime(2026, 10, 1, 9, 10, 0))
    rep = R.rebalance_phase(kis, phase="BUY", target_csv=tmp_path / "t.csv", summary_json=tmp_path / "s.json",
                            state_dir=tmp_path / "state", cfg=CFG, no_submit=True, clock=clk, next_trading_day=HOL.get)
    assert rep["status"] == "OK", rep["reasons"]
    assert kis.orders == [] and len(rep["orders"]) == 2
    assert sum(o["notional"] for o in rep["orders"]) <= CFG["strategy_capital_krw"]


def test_rebalance_buy_blocked_by_latch(tmp_path):
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "latch.json").write_text("{}")
    (tmp_path / "s.json").write_text(json.dumps({"status": "OK", "selection_date": "20260930", "d3": {"exposure": 1.0}}))
    pd.DataFrame({"code": ["005930"]}).to_csv(tmp_path / "t.csv", index=False)
    rep = R.rebalance_phase(SimKIS(), phase="BUY", target_csv=tmp_path / "t.csv", summary_json=tmp_path / "s.json",
                            state_dir=tmp_path / "state", cfg=CFG, no_submit=True, next_trading_day=HOL.get)
    assert rep["reasons"] == ["LATCH_ACTIVE_NO_BUY"]


# ---------------- 계좌 점검 (읽기 전용)

def test_check_account_ok_warn_stop(tmp_path):
    from paper.strategies.kospi_mcap_quarterly_v2.src import check_account as C
    now = datetime(2026, 9, 21, 20, 0, 0)
    kis = SimKIS()
    assert C.check(kis, CFG, state_dir=None, probe_code="005930", now=now)["status"] == "OK"
    kis.hold = {"000660": 3}
    assert C.check(kis, CFG, state_dir=None, probe_code="005930", now=now)["status"] == "WARN"
    kis.prvs = 1_000_000
    r = C.check(kis, CFG, state_dir=None, probe_code="005930", now=now)
    assert r["status"] == "STOP" and r["findings"][0].startswith("BUYING_POWER_BELOW_CAPITAL")
    assert kis.orders == [] and kis.cancels == []


# ---------------- 09-17 사용자 승인 3건: 취소 시험 / 래치 뒤 전량 매도 / 집행 직후 비중 판정

def test_capital_is_60m_and_cancel_test_keys():
    assert CFG["strategy_capital_krw"] == 60_000_000
    assert CFG["cancel_test_price_discount_pct"] == 0.10 and CFG["weight_violation_tolerance"] == 0.01


def test_cancel_test_confirms_cancel(tmp_path):
    kis = SimKIS()
    clk = Clock(datetime(2026, 9, 22, 10, 0, 0))
    rep = R.test_one(kis, code="005930", qty=1, out_dir=tmp_path, cfg=CFG, no_submit=False, clock=clk, sleep=clk.sleep,
                     next_trading_day=HOL.get, is_market_open=lambda d: True, cancel_test=True)
    ct = rep["cancel_test"]
    assert kis.orders[-1]["ord_unpr"] == "63000"  # 70000*0.9 내림
    assert kis.cancels == [kis.orders[-1]["odno"]] and ct["final_state"] == "CANCELLED_UNFILLED"
    assert rep["checks"]["cancel_confirmed_by_query"] is True and rep["status"] == "OK", rep["reasons"]


def test_cancel_test_alarms_when_query_does_not_show_cancel(tmp_path):
    class NoCancelMark(SimKIS):
        def cancel_order(self, org_order_no, org_order_branch_no, cancel_all=True):
            self.cancels.append(org_order_no)
            return {"ok": True, "msg1": "cancel ok"}  # 응답은 성공인데 조회에는 안 찍힘
    kis = NoCancelMark()
    clk = Clock(datetime(2026, 9, 22, 10, 0, 0))
    rep = R.test_one(kis, code="005930", qty=1, out_dir=tmp_path, cfg=CFG, no_submit=False, clock=clk, sleep=clk.sleep,
                     next_trading_day=HOL.get, is_market_open=lambda d: True, cancel_test=True)
    assert rep["checks"]["cancel_confirmed_by_query"] is False
    assert rep["status"] == "STOP" and any(i["type"] == "CANCEL_NOT_CONFIRMED" for i in rep["incidents"])


def _state_with_holding(tmp_path, kis, qty=5):
    sd = tmp_path / "state"
    sd.mkdir()
    (sd / "fills.jsonl").write_text(json.dumps({"fill_id": "a|5", "order_id": "a", "code": "005930", "side": "BUY",
                                                "qty": qty, "amount": qty * 70000, "trade_date": "20261001",
                                                "observed_at": "2026-10-01T10:00:00"}) + "\n", encoding="utf-8")
    kis.hold = {"005930": qty}
    return sd


def test_liquidate_requires_latch_and_next_day(tmp_path):
    kis = SimKIS()
    sd = _state_with_holding(tmp_path, kis)
    clk = Clock(datetime(2026, 10, 2, 9, 30, 0))
    assert R.liquidate_after_latch(kis, state_dir=sd, cfg=CFG, no_submit=False, clock=clk)["reasons"] == ["LATCH_NOT_ACTIVE"]
    (sd / "latch.json").write_text(json.dumps({"triggered_as_of": "20261002"}))
    r = R.liquidate_after_latch(kis, state_dir=sd, cfg=CFG, no_submit=False, clock=clk)
    assert r["status"] == "STOP" and r["reasons"][0].startswith("LIQUIDATE_FROM_NEXT_TRADING_DAY") and kis.orders == []


def test_liquidate_sells_all_once_per_day_and_keeps_latch(tmp_path):
    kis = SimKIS()
    sd = _state_with_holding(tmp_path, kis)
    (sd / "latch.json").write_text(json.dumps({"triggered_as_of": "20261001"}))
    clk = Clock(datetime(2026, 10, 2, 9, 30, 0))
    r = R.liquidate_after_latch(kis, state_dir=sd, cfg=CFG, no_submit=False, clock=clk)
    assert r["status"] == "OK", r["reasons"]
    assert [(o["sll_buy_dvsn_cd"], o["ord_qty"], o["ord_unpr"]) for o in kis.orders] == [("01", "5", "70000")]
    R.liquidate_after_latch(kis, state_dir=sd, cfg=CFG, no_submit=False, clock=clk)
    assert len(kis.orders) == 1 and (sd / "latch.json").exists()


def test_liquidate_rehearsal_sends_nothing(tmp_path):
    kis = SimKIS()
    sd = _state_with_holding(tmp_path, kis)
    (sd / "latch.json").write_text(json.dumps({"triggered_as_of": "20261001"}))
    r = R.liquidate_after_latch(kis, state_dir=sd, cfg=CFG, no_submit=True, clock=Clock(datetime(2026, 10, 2, 9, 30)))
    assert kis.orders == [] and len(r["orders"]) == 1 and "NO_SUBMIT_REHEARSAL" in r["reasons"]


@pytest.mark.parametrize("qty,exposure,expect", [(5, 1.0, "OK"), (5, 0.5, "STOP")])
def test_post_execution_weight_check(tmp_path, qty, exposure, expect):
    # 원장: 자본 6,000만 중 005930 5주 x 70,000 = 35만 -> 비중 약 0.6%. 상한을 작게 만들어 판정을 건드린다
    kis = SimKIS()
    sd = _state_with_holding(tmp_path, kis, qty)
    (tmp_path / "s.json").write_text(json.dumps({"d3": {"exposure": exposure}}))
    cfg = dict(CFG, weight_cap=0.01, weight_violation_tolerance=0.0005)  # 비중 0.583% vs 한도 1.05% / 0.55%
    r = R.post_execution_check(kis, state_dir=sd, summary_json=tmp_path / "s.json", cfg=cfg,
                               clock=Clock(datetime(2026, 10, 1, 15, 40)), next_trading_day=HOL.get)
    assert r["status"] == expect, r
