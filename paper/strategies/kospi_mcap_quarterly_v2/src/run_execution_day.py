"""집행일 실행기 — E1 주문 → E2 발주 → E3 체결 확인 → E4 원장 · 사고 판정을 명령 하나로.

    # 09-22 장중 1건 시험 (매수 1주 → 체결 → 매도 1주 → 체결, 전부 시험 폴더에)
    python -m paper.strategies.kospi_mcap_quarterly_v2.src.run_execution_day test-one --code 005930 --out-dir <시험폴더>
    # 발주 없이 리허설 (장 밖에서도 됨: 호가·잔고·매수가능 조회만)
    ... test-one --code 005930 --out-dir <시험폴더> --no-submit

    # 재구성일 (10-01)
    ... rebalance --phase SELL --target <target_portfolio_YYYYMMDD.csv> --summary <target_YYYYMMDD_summary.json> --state-dir <상태폴더> [--no-submit]
    ... sync   --state-dir <상태폴더>
    ... rebalance --phase BUY ...
    ... cancel --state-dir <상태폴더>       (15:20 이후)
    ... ledger --state-dir <상태폴더>
    ... post-exec-check --state-dir <상태폴더> --summary <target_YYYYMMDD_summary.json>   (집행일 cancel·sync 뒤 한 번)
    # 한도(-25%) 래치가 켜진 다음 거래일부터
    ... liquidate --state-dir <상태폴더> [--no-submit]

쓰기 지점: --out-dir / --state-dir 안 파일뿐. orders.jsonl · fills.jsonl · latch.json 은 날짜 없는 append-only.
시험(test-one)은 전략 상태 폴더(data/state)를 거부한다 — 시험 체결이 전략 원장에 섞이지 않게.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from paper.strategies.kospi_mcap_quarterly_v2.src import execution as E
from paper.strategies.kospi_mcap_quarterly_v2.src import fills as F
from paper.strategies.kospi_mcap_quarterly_v2.src import incidents as I
from paper.strategies.kospi_mcap_quarterly_v2.src import kis_adapter as K
from paper.strategies.kospi_mcap_quarterly_v2.src import ledger as L
from paper.strategies.kospi_mcap_quarterly_v2.src.orders import plan_orders
from paper.strategies.kospi_mcap_quarterly_v2.src.run_d1_d2 import STRATEGY_ROOT
from utils.krx_tick import round_tick

DEFAULT_STRATEGY_CFG = STRATEGY_ROOT / "config" / "strategy_v1.json"
PRODUCTION_STATE_DIR = STRATEGY_ROOT / "data" / "state"


def _next_trading_day_fn() -> Callable[[str], str]:
    from holiday_manager import HolidayManager
    h = HolidayManager()
    return lambda d: h.next_trading_day(d)


def _write(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _append(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(obj, ensure_ascii=False, default=str) + "\n")


def refresh_price_hook(client: Any, cfg: Dict[str, Any], clock: Callable[[], datetime], log: List[Dict[str, Any]]):
    """보내기 직전 호가를 다시 받아 지정가를 (가) 규칙으로 갱신한다. 매수는 매도호가1 올림, 매도는 매수호가1 내림."""
    def hook(o: Dict[str, Any]):
        q = K.fetch_quote(client, o["code"], clock)
        if not q.get("ok") or q.get("ask1", 0) <= 0 or q.get("bid1", 0) <= 0:
            return o, f"QUOTE_UNAVAILABLE:{o['code']}:{q.get('error')}"
        age = (clock() - q["quote_ts"]).total_seconds()
        if age > float(cfg["quote_max_age_seconds"]):
            return o, f"QUOTE_STALE:{o['code']}:{age:.1f}s"
        new_px = round_tick(float(q["ask1"]), up=True) if o["side"] == "BUY" else round_tick(float(q["bid1"]), up=False)
        o2 = {**o, "planned_limit_price": o["limit_price"], "limit_price": int(new_px),
              "notional": int(new_px) * int(o["requested_qty"]), "ask1": q["ask1"], "bid1": q["bid1"],
              "quote_ts": q["quote_ts"].isoformat(timespec="seconds")}
        log.append({"order_id": o["order_id"], "planned": o["limit_price"], "sent": int(new_px),
                    "quote_elapsed_sec": q.get("elapsed_sec")})
        return o2, None
    return hook


def poll_until_done(client: Any, book: E.OrderBook, fills_path: Path, order_ids: List[str], ymd: str, cfg: Dict[str, Any],
                    clock: Callable[[], datetime], sleep: Callable[[float], None] = time.sleep) -> Dict[str, Any]:
    """체결 조회를 주기적으로 돌려 주문이 끝나거나 시간이 다 될 때까지. 모르는 상태는 여기서 조회로 푼다."""
    interval = float(cfg["fill_poll_interval_seconds"])
    deadline = clock() + timedelta(seconds=float(cfg["fill_poll_max_seconds"]))  # 주입 시계 기준(시험 가능)
    rounds: List[Dict[str, Any]] = []
    while True:
        try:
            rows = K.fetch_day_orders(client, ymd)
        except Exception as e:
            rounds.append({"ts": clock().isoformat(timespec="seconds"), "error": f"{type(e).__name__}:{e}"})
            rows = None
        if rows is not None:
            last = clock() >= deadline
            res_u = E.resolve_unknown(book, rows, clock(), query_complete=last)
            res_f = F.sync_fills(book, rows, fills_path, clock())
            rounds.append({"ts": clock().isoformat(timespec="seconds"), "rows": len(rows), "resolve": res_u,
                           "new_fills": len(res_f["new_fills"]), "incidents": res_f["incidents"],
                           "missing": res_f["missing"], "orphans": res_f["orphans"]})
            if res_f["incidents"]:
                break
        states = {oid: book.state(oid) for oid in order_ids}
        if all(s in E.TERMINAL for s in states.values()) or clock() >= deadline:
            break
        sleep(interval)
    return {"states": {oid: book.state(oid) for oid in order_ids}, "rounds": rounds}


def _prvs(bal: Optional[Dict[str, Any]]) -> Optional[int]:
    return bal.get("prvs_rcdl_excc_amt") if bal else None


def test_one(client: Any, *, code: str, qty: int, out_dir: Path, cfg: Dict[str, Any], no_submit: bool,
             clock: Callable[[], datetime] = datetime.now, sleep: Callable[[float], None] = time.sleep,
             next_trading_day: Optional[Callable[[str], str]] = None, is_market_open: Optional[Callable[[str], bool]] = None,
             cancel_test: bool = False) -> Dict[str, Any]:
    """매수 qty → 체결 확인 → 원장 → 매수가능 변화 → 매도 qty → 체결 확인 → 원장 → 사고 판정. 가정 검증 항목을 보고에 남긴다."""
    now = clock()
    ymd = now.strftime("%Y%m%d")
    run_tag = now.strftime("%H%M%S")
    out_dir = Path(out_dir)
    book = E.OrderBook(out_dir / "orders.jsonl")
    fills_path = out_dir / "fills.jsonl"
    rep: Dict[str, Any] = {"mode": "test-one", "code": code, "qty": qty, "run_at": now.isoformat(timespec="seconds"),
                           "no_submit": no_submit, "status": "OK", "reasons": [], "checks": {}}
    rep["window"] = E.window_reason(now, cfg) or "IN_WINDOW"
    if is_market_open is not None:
        rep["market_open_today"] = bool(is_market_open(ymd))
    if not no_submit:
        if rep["window"] != "IN_WINDOW":
            rep.update(status="STOP", reasons=[rep["window"]])
        elif is_market_open is not None and not rep["market_open_today"]:
            rep.update(status="STOP", reasons=[f"MARKET_CLOSED:{ymd}"])
        if rep["status"] == "STOP":
            _write(out_dir / f"test_one_{ymd}_{run_tag}.json", rep)
            return rep

    bal0 = K.fetch_balance(client)
    q0 = K.fetch_quote(client, code, clock)
    rep["before"] = {"balance": bal0, "quote": q0}
    if not q0.get("ok") or q0.get("ask1", 0) <= 0:
        rep.update(status="STOP", reasons=[f"QUOTE_UNAVAILABLE:{q0.get('error')}"])
        _write(out_dir / f"test_one_{ymd}_{run_tag}.json", rep)
        return rep
    buy_px = round_tick(float(q0["ask1"]), up=True)
    rep["before"]["buying_power"] = K.fetch_buying_power(client, code, buy_px)

    def mk(side: str, px: int) -> Dict[str, Any]:
        intent = f"V2TEST|{ymd}|{run_tag}|{code}|{side}"
        return {"order_id": f"{intent}|1", "intent_id": intent, "attempt_no": 1, "code": code, "side": side,
                "limit_price": int(px), "requested_qty": int(qty), "rebalance_id": f"V2TEST|{ymd}|{run_tag}",
                "execution_date": ymd}

    buy = mk("BUY", buy_px)
    rep["planned_buy"] = buy
    if no_submit:
        rep["reasons"].append("NO_SUBMIT_REHEARSAL")
        _write(out_dir / f"test_one_{ymd}_{run_tag}.json", rep)
        return rep

    ntd = next_trading_day or _next_trading_day_fn()
    hook_log: List[Dict[str, Any]] = []
    hook = refresh_price_hook(client, cfg, clock, hook_log)

    def leg(order: Dict[str, Any]) -> Dict[str, Any]:
        sub = E.submit_orders([order], client, book, clock(), cfg, clock=clock, pre_submit=hook)
        out = {"submit": sub}
        if sub["skipped_pre_submit"] or not (sub["accepted"] or sub["unknown"]):
            out["final_state"] = book.state(order["order_id"])
            return out
        poll = poll_until_done(client, book, fills_path, [order["order_id"]], ymd, cfg, clock, sleep)
        out["poll"] = poll
        st = book.state(order["order_id"])
        if st in {"ACCEPTED", "PARTIALLY_FILLED"}:
            out["cancel"] = E.cancel_one_now(book, client, order["order_id"], clock(), "TEST_ONE_POLL_TIMEOUT")
            # 취소 직전 체결이 들어왔을 수 있다 — 한 번 더 맞춘다
            out["post_cancel_poll"] = poll_until_done(client, book, fills_path, [order["order_id"]], ymd,
                                                      {**cfg, "fill_poll_max_seconds": 0}, clock, sleep)
        out["final_state"] = book.state(order["order_id"])
        return out

    def ledger_now(mark: int) -> Dict[str, Any]:
        fl = [f for f in F.load_fills(fills_path) if f["rebalance_id"] == buy["rebalance_id"]]
        return L.build_ledger(float(cfg["strategy_capital_krw"]), fl, as_of=ymd, closes={code: mark}, cfg=cfg,
                              next_trading_day=ntd)

    rep["buy"] = leg(buy)
    rep["submit_price_refresh"] = hook_log
    bought = int(book.orders.get(buy["order_id"], {}).get("filled_qty") or 0)
    bal1 = K.fetch_balance(client)
    q1 = K.fetch_quote(client, code, clock)
    rep["after_buy"] = {"balance": bal1, "buying_power": K.fetch_buying_power(client, code, buy_px),
                        "ledger": ledger_now(q1.get("bid1") or buy_px), "filled_qty": bought}

    if bought > 0:
        sell = {**mk("SELL", round_tick(float(q1.get("bid1") or buy_px), up=False)), "requested_qty": bought}
        rep["sell"] = leg(sell)
        sold = int(book.orders.get(sell["order_id"], {}).get("filled_qty") or 0)
        bal2 = K.fetch_balance(client)
        q2 = K.fetch_quote(client, code, clock)
        rep["after_sell"] = {"balance": bal2, "buying_power": K.fetch_buying_power(client, code, buy_px),
                             "ledger": ledger_now(q2.get("bid1") or buy_px), "filled_qty": sold}
    else:
        rep["reasons"].append("BUY_NOT_FILLED_SELL_SKIPPED")

    # 가정 검증 — 값을 남기고 판정한다. 모르면 None
    fl = [f for f in F.load_fills(fills_path) if f["rebalance_id"] == buy["rebalance_id"]]
    buy_amt = sum(f["amount"] for f in fl if f["side"] == "BUY")
    sell_amt = sum(f["amount"] for f in fl if f["side"] == "SELL")
    c = rep["checks"]
    c["order_no_returned"] = bool(rep["buy"]["submit"].get("accepted"))
    c["balance_prpr_present"] = code in rep["after_buy"]["balance"].get("prices", {}) if bought > 0 else None
    c["ccld_row_matched"] = bool(fl)
    if bought > 0 and _prvs(bal0) is not None and _prvs(bal1) is not None:
        c["buy_d2_cash_drop"] = _prvs(bal0) - _prvs(bal1)
        c["buy_fee_inferred"] = c["buy_d2_cash_drop"] - buy_amt
        c["buy_fee_pct_inferred"] = c["buy_fee_inferred"] / buy_amt if buy_amt else None
    if "after_sell" in rep and sell_amt:
        bal2 = rep["after_sell"]["balance"]
        c["sell_d2_cash_rise"] = _prvs(bal2) - _prvs(bal1)
        c["sell_fee_tax_inferred"] = sell_amt - c["sell_d2_cash_rise"]
        c["sell_fee_tax_pct_inferred"] = c["sell_fee_tax_inferred"] / sell_amt
        bp1, bp2 = rep["after_buy"]["buying_power"], rep["after_sell"]["buying_power"]
        c["same_day_reuse_rise"] = bp2["ruse_psbl_amt"] - bp1["ruse_psbl_amt"]
        c["nrcvb_rise_after_sell"] = bp2["nrcvb_buy_amt"] - bp1["nrcvb_buy_amt"]
        c["broker_holding_back_to_start"] = bal2["holdings"].get(code, 0) == bal0["holdings"].get(code, 0)
    cancel_legs = [x for x in (rep.get("buy"), rep.get("sell")) if x and x.get("cancel")]
    c["cancel_exercised"] = [x["cancel"] for x in cancel_legs]

    if cancel_test:
        rep["cancel_test"] = run_cancel_test(client, book, fills_path, code=code, ymd=ymd, run_tag=run_tag, cfg=cfg,
                                             clock=clock, sleep=sleep)
        ct = rep["cancel_test"]
        c["cancel_response_ok"] = (ct.get("cancel") or {}).get("status") == "OK"
        c["cancel_final_state"] = ct.get("final_state")
        c["cancel_row_after"] = ct.get("row_after")
        c["cancel_confirmed_by_query"] = ct.get("confirmed_by_query")

    # 사고 판정 (시험 범위): 의도=시작 보유로 복귀, 정의 위반=허용 날짜
    final_led = (rep.get("after_sell") or rep["after_buy"])["ledger"]
    led_pos = {p["code"]: p["qty"] for p in final_led["positions"]}
    explained = {code: "SELL_NOT_FILLED"} if "after_sell" in rep and rep["after_sell"]["filled_qty"] < bought else {}
    orders_sent = [o for o in book.orders.values() if o.get("rebalance_id") == buy["rebalance_id"]]
    inc = I.intent_vs_result({code: 0}, led_pos, explained)
    inc += I.definition_violations([{**o, "execution_date": ymd} for o in orders_sent], allowed_execution_dates=[ymd],
                                   latch_active=False)
    for leg in (rep.get("buy"), rep.get("sell"), rep.get("cancel_test")):
        for key in ("poll", "post_cancel_poll"):
            for r in ((leg or {}).get(key) or {}).get("rounds", []):
                inc += r.get("incidents", [])
    if bought > 0 and rep["after_buy"]["balance"]["holdings"].get(code, 0) - bal0["holdings"].get(code, 0) != bought:
        inc.append({"type": "1_INTENT_RESULT_MISMATCH", "code": code, "detail": "BROKER_HOLDING_DELTA_NE_FILL"})
    rep["incidents"] = inc
    if inc:
        rep["status"] = "STOP"
        rep["reasons"].append(f"INCIDENTS:{len(inc)}")
    if cancel_test and not rep["cancel_test"].get("skipped") and rep["cancel_test"].get("final_state") not in E.TERMINAL:
        rep["status"] = "STOP"
        rep["reasons"].append(f"CANCEL_TEST_NOT_TERMINAL:{rep['cancel_test'].get('final_state')}")
    if rep["buy"]["final_state"] not in E.TERMINAL or (rep.get("sell") and rep["sell"]["final_state"] not in E.TERMINAL):
        rep["status"] = "STOP"
        rep["reasons"].append("ORDER_NOT_TERMINAL")
    _write(out_dir / f"test_one_{ymd}_{run_tag}.json", rep)
    _append(out_dir / "test_one_log.jsonl", {"run_at": rep["run_at"], "code": code, "status": rep["status"],
                                             "reasons": rep["reasons"], "checks": c})
    return rep


def run_cancel_test(client: Any, book: E.OrderBook, fills_path: Path, *, code: str, ymd: str, run_tag: str,
                    cfg: Dict[str, Any], clock: Callable[[], datetime], sleep: Callable[[float], None]) -> Dict[str, Any]:
    """체결되지 않을 가격(매수호가1 x (1-10%), 내림)으로 1주 매수 -> 기다림 -> 취소 -> 조회로 취소 확인.
    확인하는 가정: 취소 응답, 취소 뒤 원 주문 행의 cncl_yn / cncl_cfrm_qty / rmn_qty (E3 CANCEL_NOT_CONFIRMED 근거).
    호가 재조회 훅은 쓰지 않는다 - 훅은 지정가를 매도호가1 로 올려 체결시킨다."""
    out: Dict[str, Any] = {}
    blocked = E.window_reason(clock(), cfg)
    if blocked:
        out["skipped"] = blocked
        return out
    q = K.fetch_quote(client, code, clock)
    if not q.get("ok") or q.get("bid1", 0) <= 0:
        out["skipped"] = f"QUOTE_UNAVAILABLE:{q.get('error')}"
        return out
    px = round_tick(float(q["bid1"]) * (1 - float(cfg["cancel_test_price_discount_pct"])), up=False)
    intent = f"V2TEST|{ymd}|{run_tag}|{code}|BUY_CANCELTEST"
    o = {"order_id": f"{intent}|1", "intent_id": intent, "attempt_no": 1, "code": code, "side": "BUY",
         "limit_price": int(px), "requested_qty": 1, "rebalance_id": f"V2TEST|{ymd}|{run_tag}|CANCELTEST",
         "execution_date": ymd}
    out["order"] = o
    out["submit"] = E.submit_orders([o], client, book, clock(), cfg, clock=clock)
    if not out["submit"].get("accepted") and not out["submit"].get("unknown"):
        out["final_state"] = book.state(o["order_id"])
        return out
    sleep(float(cfg["cancel_test_wait_seconds"]))
    once = {**cfg, "fill_poll_max_seconds": 0}
    out["poll"] = poll_until_done(client, book, fills_path, [o["order_id"]], ymd, once, clock, sleep)
    if book.state(o["order_id"]) in {"ACCEPTED", "PARTIALLY_FILLED"}:
        out["cancel"] = E.cancel_one_now(book, client, o["order_id"], clock(), "CANCEL_TEST")
        sleep(float(cfg["fill_poll_interval_seconds"]))
        out["post_cancel_poll"] = poll_until_done(client, book, fills_path, [o["order_id"]], ymd, once, clock, sleep)
        no = str(book.orders[o["order_id"]].get("broker_order_no"))
        try:
            day_rows = K.fetch_day_orders(client, ymd)
        except Exception as e:
            day_rows, out["row_error"] = [], f"{type(e).__name__}:{e}"
        rows = [r for r in day_rows if str(r.get("odno")) == no]
        keys = ("odno", "orgn_odno", "ord_qty", "tot_ccld_qty", "rmn_qty", "cncl_yn", "cncl_cfrm_qty", "rjct_qty",
                "sll_buy_dvsn_cd")
        out["row_after"] = {k: rows[0].get(k) for k in keys} if rows else None
        # 취소가 새 주문번호 행으로 따로 찍히는지도 본다
        out["same_code_rows_after"] = [{k: r.get(k) for k in keys} for r in day_rows if str(r.get("pdno")) == code]
        incs = [i for r in out["post_cancel_poll"]["rounds"] for i in r.get("incidents", [])]
        out["confirmed_by_query"] = bool(rows) and not any(i["type"] == "CANCEL_NOT_CONFIRMED" for i in incs)
    out["final_state"] = book.state(o["order_id"])
    return out


# ---------------------------------------------------------------- 한도(-25%) 도달 뒤 전량 매도

def liquidate_after_latch(client: Any, *, state_dir: Path, cfg: Dict[str, Any], no_submit: bool,
                          clock: Callable[[], datetime] = datetime.now) -> Dict[str, Any]:
    """래치가 켜진 **다음 거래일부터** 전략 원장 보유 전량을 지정가(보내기 직전 매수호가1 내림)로 판다
    (사용자 2026-09-17 승인). 자동 재개 없음, 래치 파일은 그대로 둔다.
    주문 ID 에 집행일을 넣어 하루 한 번씩 남은 수량을 다시 낸다(같은 날 재실행은 재전송 안 함).
    전날 주문은 15:20 cancel 로 끝나 있어야 한다 - 살아 있는 주문이 있으면 멈춘다."""
    now = clock()
    ymd = now.strftime("%Y%m%d")
    state_dir = Path(state_dir)
    rep: Dict[str, Any] = {"mode": "liquidate", "run_at": now.isoformat(timespec="seconds"), "no_submit": no_submit,
                           "status": "OK", "reasons": [], "orders": []}
    latch_path = state_dir / "latch.json"
    if not latch_path.exists():
        rep.update(status="STOP", reasons=["LATCH_NOT_ACTIVE"])
        return rep
    trig = str(json.loads(latch_path.read_text(encoding="utf-8")).get("triggered_as_of") or "")
    if not trig or ymd <= trig:
        rep.update(status="STOP", reasons=[f"LIQUIDATE_FROM_NEXT_TRADING_DAY:{trig}"])
        return rep
    book = E.OrderBook(state_dir / "orders.jsonl")
    live = [o["order_id"] for o in book.orders.values()
            if o["state"] not in E.TERMINAL and not str(o["order_id"]).endswith(f"|{ymd}|1")]
    if live:
        rep.update(status="STOP", reasons=[f"OPEN_ORDERS_FROM_EARLIER:{live}"])
        return rep
    holdings: Dict[str, int] = {}
    for f in F.load_fills(state_dir / "fills.jsonl"):
        holdings[f["code"]] = holdings.get(f["code"], 0) + (int(f["qty"]) if f["side"] == "BUY" else -int(f["qty"]))
    bal = K.fetch_balance(client)
    rebalance_id = f"{cfg['strategy_id']}|LATCH|{trig}"
    # [2026-09-19 사용자 결정 (나)] 한 종목이 원장 > 계좌여도 **나머지는 판다**. 예전엔 그 한 종목 때문에
    #   전체 청산이 멈췄다 — 래치는 탈출이 목적이라 한 종목이 탈출구를 잠그면 안 된다(독립 검토 17번).
    #   어긋난 종목은 원장·계좌 중 **적은 쪽**만 판다(V2 몫이 아니거나 없는 주식을 팔지 않는다). 크게 남긴다.
    rep["mismatches"] = []
    for code, qty in sorted(holdings.items()):
        if qty <= 0:
            continue
        broker_q = int(bal["holdings"].get(code, 0))
        if broker_q < qty:
            rep["mismatches"].append({"code": code, "ledger_qty": qty, "broker_qty": broker_q})
            qty = broker_q
            if qty <= 0:
                continue
        intent = f"{rebalance_id}|{code}|SELL|{ymd}"
        rep["orders"].append({"order_id": f"{intent}|1", "intent_id": intent, "attempt_no": 1, "code": code,
                              "side": "SELL", "limit_price": 0, "requested_qty": qty, "rebalance_id": rebalance_id,
                              "execution_date": ymd})
    if rep["reasons"]:
        rep["status"] = "STOP"
    if no_submit:
        rep["reasons"].append("NO_SUBMIT_REHEARSAL")
    elif rep["status"] == "OK" and rep["orders"]:
        log: List[Dict[str, Any]] = []
        rep["submit"] = E.submit_orders(rep["orders"], client, book, clock(), cfg, clock=clock,
                                        pre_submit=refresh_price_hook(client, cfg, clock, log))
        rep["submit_price_refresh"] = log
        if rep["submit"]["status"] != "OK" or rep["submit"]["skipped_pre_submit"]:
            rep.update(status="STOP", reasons=rep["submit"]["reasons"] +
                       [f"SKIPPED:{x['order_id']}:{x['reason']}" for x in rep["submit"]["skipped_pre_submit"]])
    if rep["mismatches"]:  # 보내고 난 뒤에도 사람이 봐야 한다 — STOP 으로 올려 알림이 가게
        rep["status"] = "STOP"
        rep["reasons"].append("LEDGER_EXCEEDS_BROKER_HOLDING:" + ",".join(
            f"{m['code']}(ledger={m['ledger_qty']},broker={m['broker_qty']})" for m in rep["mismatches"]))
    _write(state_dir / f"liquidate_{ymd}_{now:%H%M%S}.json", rep)
    return rep


def post_execution_check(client: Any, *, state_dir: Path, summary_json: Path, cfg: Dict[str, Any],
                         clock: Callable[[], datetime] = datetime.now, next_trading_day=None) -> Dict[str, Any]:
    """집행일 15:20 취소 + 동기화 뒤 한 번만: 종목 비중 > 상한 x 노출 + 허용폭 이면 정의 위반. 가격은 계좌 현재가.
    매일 돌리지 않는다(분기 사이 가격 상승으로 20% 초과는 정상)."""
    now = clock()
    ymd = now.strftime("%Y%m%d")
    state_dir = Path(state_dir)
    summary = json.loads(Path(summary_json).read_text(encoding="utf-8"))
    exposure = float(summary["d3"]["exposure"])
    bal = K.fetch_balance(client)
    led = _state_ledger(state_dir, cfg, ymd, bal.get("prices", {}), next_trading_day or _next_trading_day_fn())
    rep: Dict[str, Any] = {"mode": "post-execution-check", "run_at": now.isoformat(timespec="seconds"),
                           "ledger_status": led["status"], "nav": led["nav"], "exposure": exposure,
                           "status": "OK", "reasons": []}
    if led["status"] != "COMPLETE":
        rep.update(status="STOP", reasons=[f"NAV_INCOMPLETE:{led['missing_prices']}"])
    else:
        rep["incidents"] = I.definition_violations(
            [], allowed_execution_dates=[], latch_active=False,
            positions_value={p["code"]: p["value"] for p in led["positions"]}, nav=led["nav"],
            cap=float(cfg["weight_cap"]), exposure=exposure, tolerance=float(cfg["weight_violation_tolerance"]))
        rep["max_weight"] = max((p["value"] / led["nav"] for p in led["positions"]), default=0.0)
        if rep["incidents"]:
            rep.update(status="STOP", reasons=[f"INCIDENTS:{len(rep['incidents'])}"])
    _write(state_dir / f"post_execution_check_{ymd}.json", rep)
    return rep


# ---------------------------------------------------------------- 재구성일 단계

def _state_ledger(state_dir: Path, cfg: Dict[str, Any], as_of: str, closes: Dict[str, float], ntd) -> Dict[str, Any]:
    return L.build_ledger(float(cfg["strategy_capital_krw"]), F.load_fills(state_dir / "fills.jsonl"), as_of=as_of,
                          closes=closes, cfg=cfg, next_trading_day=ntd)


def rebalance_phase(client: Any, *, phase: str, target_csv: Path, summary_json: Path, state_dir: Path,
                    cfg: Dict[str, Any], no_submit: bool, clock: Callable[[], datetime] = datetime.now,
                    next_trading_day: Optional[Callable[[str], str]] = None) -> Dict[str, Any]:
    now = clock()
    ymd = now.strftime("%Y%m%d")
    ntd = next_trading_day or _next_trading_day_fn()
    summary = json.loads(Path(summary_json).read_text(encoding="utf-8"))
    target = pd.read_csv(target_csv, dtype={"code": str})
    rep: Dict[str, Any] = {"mode": "rebalance", "phase": phase, "run_at": now.isoformat(timespec="seconds"),
                           "no_submit": no_submit, "status": "OK", "reasons": []}
    if summary.get("status") != "OK":
        rep.update(status="STOP", reasons=[f"TARGET_NOT_OK:{summary.get('stage')}"])
        return rep
    exposure = float(summary["d3"]["exposure"])
    selection_date = str(summary["selection_date"])
    latch = state_dir / "latch.json"
    if phase == "BUY" and latch.exists():
        rep.update(status="STOP", reasons=["LATCH_ACTIVE_NO_BUY"])
        return rep

    bal = K.fetch_balance(client)
    fills = F.load_fills(state_dir / "fills.jsonl")
    pre = L.build_ledger(float(cfg["strategy_capital_krw"]), fills, as_of=ymd, closes=bal.get("prices", {}), cfg=cfg,
                         next_trading_day=ntd)
    holdings = {p["code"]: p["qty"] for p in pre["positions"]}
    # 전략 원장 보유 ≠ 계좌 보유면 멈춘다(계좌에 다른 보유가 있는 건 허용: 전략 종목만 대조)
    diff = {c: (q, bal["holdings"].get(c, 0)) for c, q in holdings.items() if bal["holdings"].get(c, 0) < q}
    if diff:
        rep.update(status="STOP", reasons=[f"LEDGER_EXCEEDS_BROKER_HOLDING:{diff}"])
        return rep
    equity = pre["nav"] if pre["nav"] is not None else (float(cfg["strategy_capital_krw"]) if not fills else None)
    if equity is None:
        rep.update(status="STOP", reasons=[f"NAV_INCOMPLETE:{pre['missing_prices']}"])
        return rep

    codes = sorted(set(target["code"]) | set(holdings))
    quotes = {c: K.fetch_quote(client, c, clock) for c in codes}
    quotes = {c: q for c, q in quotes.items() if q.get("ok")}
    cash_available = pre["cash_economic"]
    if phase == "BUY" and codes:
        bp = K.fetch_buying_power(client, target["code"].iloc[0], 0)
        rep["buying_power"] = bp
        cash_available = min(cash_available, float(bp["nrcvb_buy_amt"]))
    # 호가는 순서대로 받아 앞 종목이 낡는다 — 계획은 가장 오래된 호가 시각 기준으로 평가하지 않고
    # 수량만 정한다. 신선도는 보내기 직전 한 건마다 다시 본다(refresh_price_hook)
    plan_now = max((q["quote_ts"] for q in quotes.values()), default=now)
    for q in quotes.values():
        q["quote_ts"] = plan_now
    orders, s = plan_orders(target, selection_date=selection_date, execution_date=ymd, exposure=exposure,
                            strategy_equity=equity, holdings=holdings, cash_available=cash_available, quotes=quotes,
                            now=plan_now, cfg=cfg, phase=phase, event_tag=summary.get("event_tag"))
    rep["plan"] = s
    rep["orders"] = orders
    if s["status"] != "OK":
        rep.update(status="STOP", reasons=s["reasons"])
    elif not no_submit and orders:
        book = E.OrderBook(state_dir / "orders.jsonl")
        log: List[Dict[str, Any]] = []
        rep["submit"] = E.submit_orders(orders, client, book, clock(), cfg, clock=clock,
                                        pre_submit=refresh_price_hook(client, cfg, clock, log))
        rep["submit_price_refresh"] = log
        if rep["submit"]["status"] != "OK":
            rep.update(status="STOP", reasons=rep["submit"]["reasons"])
    elif no_submit:
        rep["reasons"].append("NO_SUBMIT_REHEARSAL")
    _write(state_dir / f"rebalance_{phase}_{ymd}_{now:%H%M%S}.json", rep)
    return rep


def sync_state(client: Any, state_dir: Path, clock=datetime.now) -> Dict[str, Any]:
    now = clock()
    rows = K.fetch_day_orders(client, now.strftime("%Y%m%d"))
    book = E.OrderBook(state_dir / "orders.jsonl")
    res = {"resolve": E.resolve_unknown(book, rows, now, query_complete=False),
           "fills": F.sync_fills(book, rows, state_dir / "fills.jsonl", now),
           "states": {oid: o["state"] for oid, o in book.orders.items()}}
    _append(state_dir / "sync_log.jsonl", {"ts": now.isoformat(timespec="seconds"), "new_fills": len(res["fills"]["new_fills"]),
                                           "incidents": res["fills"]["incidents"], "resolve": res["resolve"]})
    return res


def ledger_state(client: Any, state_dir: Path, cfg: Dict[str, Any], clock=datetime.now, next_trading_day=None) -> Dict[str, Any]:
    now = clock()
    bal = K.fetch_balance(client)
    led = _state_ledger(state_dir, cfg, now.strftime("%Y%m%d"), bal.get("prices", {}), next_trading_day or _next_trading_day_fn())
    led["latch"] = L.evaluate_latch(led, cfg, state_dir / "latch.json")
    led["valuation_basis"] = "broker_balance_prpr"
    _write(state_dir / f"ledger_{now:%Y%m%d}.json", led)
    _append(state_dir / "nav_log.jsonl", {k: led[k] for k in ("as_of", "status", "nav", "cash_economic", "cash_settled",
                                                              "market_value", "fees", "taxes")})
    return led


def _refuse_production(path: Path) -> None:
    p = Path(path).resolve()
    if p == PRODUCTION_STATE_DIR.resolve() or PRODUCTION_STATE_DIR.resolve() in p.parents:
        raise SystemExit(f"test output must not be the strategy state dir: {p}")


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    t = sub.add_parser("test-one")
    t.add_argument("--code", required=True)
    t.add_argument("--qty", type=int, default=1)
    t.add_argument("--out-dir", required=True, type=Path)
    t.add_argument("--no-submit", action="store_true")
    t.add_argument("--cancel-test", action="store_true")
    lq = sub.add_parser("liquidate")
    lq.add_argument("--state-dir", required=True, type=Path)
    lq.add_argument("--no-submit", action="store_true")
    pc = sub.add_parser("post-exec-check")
    pc.add_argument("--state-dir", required=True, type=Path)
    pc.add_argument("--summary", required=True, type=Path)
    r = sub.add_parser("rebalance")
    r.add_argument("--phase", required=True, choices=["SELL", "BUY"])
    r.add_argument("--target", required=True, type=Path)
    r.add_argument("--summary", required=True, type=Path)
    r.add_argument("--state-dir", required=True, type=Path)
    r.add_argument("--no-submit", action="store_true")
    for name in ("sync", "cancel", "ledger"):
        p = sub.add_parser(name)
        p.add_argument("--state-dir", required=True, type=Path)
    ap.add_argument("--strategy-cfg", type=Path, default=DEFAULT_STRATEGY_CFG)
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    cfg = json.loads(args.strategy_cfg.read_text(encoding="utf-8"))
    client = K.make_client()
    if args.cmd == "test-one":
        _refuse_production(args.out_dir)
        from holiday_manager import HolidayManager
        h = HolidayManager()
        res = test_one(client, code=args.code, qty=args.qty, out_dir=args.out_dir, cfg=cfg, no_submit=args.no_submit,
                       next_trading_day=lambda d: h.next_trading_day(d), is_market_open=h.is_market_open,
                       cancel_test=args.cancel_test)
    elif args.cmd == "liquidate":
        res = liquidate_after_latch(client, state_dir=args.state_dir, cfg=cfg, no_submit=args.no_submit)
    elif args.cmd == "post-exec-check":
        res = post_execution_check(client, state_dir=args.state_dir, summary_json=args.summary, cfg=cfg)
    elif args.cmd == "rebalance":
        res = rebalance_phase(client, phase=args.phase, target_csv=args.target, summary_json=args.summary,
                              state_dir=args.state_dir, cfg=cfg, no_submit=args.no_submit)
    elif args.cmd == "sync":
        res = sync_state(client, args.state_dir)
    elif args.cmd == "cancel":
        book = E.OrderBook(args.state_dir / "orders.jsonl")
        res = E.cancel_remaining(book, client, datetime.now(), cfg)
    else:
        res = ledger_state(client, args.state_dir, cfg)
    print(json.dumps(res, ensure_ascii=False, indent=2, default=str))
    return 0 if res.get("status", "OK") in ("OK", "COMPLETE") else 3


if __name__ == "__main__":
    raise SystemExit(main())
