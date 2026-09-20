"""KIS 연결부 — 모의계좌 전용. tools/kis_order_client.py 를 고치지 않고 감싸기만 한다.

응답 필드(2026-09-17 실물 확인):
  호가 inquire_hoga().output: askp1, bidp1, askp_rsqn1, bidp_rsqn1, aspr_acpt_hour(HHMMSS, 거래소 호가 접수 시각)
  매수가능 inquire_psbl_order().output: nrcvb_buy_amt(미수 없는 매수금액) — **이것만 쓴다**
          (max_buy_amt 는 증거금·신용 포함 5억대로 부풀어 있어 쓰면 미수가 생긴다), ruse_psbl_amt(재사용가능금액)
  잔고 inquire_balance_positions().summary_rows[0]: dnca_tot_amt(예수금 총액, 매도 미결제 제외),
          prvs_rcdl_excc_amt(D+2 예수금), nass_amt(순자산)
"""
from __future__ import annotations

import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Optional

ROOT = Path(__file__).resolve().parents[4]
TOOLS = ROOT / "tools"


def make_client():
    if str(TOOLS) not in sys.path:
        sys.path.insert(0, str(TOOLS))
    from kis_order_client import KISOrderClient  # noqa: E402
    client = KISOrderClient.from_env(mock=True)
    if getattr(client.cfg, "mock", False) is not True:
        raise RuntimeError("KIS client is not mock — V2 is mock-only")
    return client


def _i(v: Any) -> int:
    try:
        return int(float(str(v).replace(",", "").strip() or 0))
    except ValueError:
        return 0


def fetch_quote(client: Any, code: str, now_fn: Callable[[], datetime] = datetime.now) -> Dict[str, Any]:
    t0 = time.time()
    try:
        rsp = client.inquire_hoga(code=code)
    except Exception as e:
        return {"ok": False, "code": code, "error": f"{type(e).__name__}:{e}", "elapsed_sec": round(time.time() - t0, 3)}
    o = rsp.get("output") or {}
    return {"ok": bool(rsp.get("ok")), "code": code, "ask1": _i(o.get("askp1")), "bid1": _i(o.get("bidp1")),
            "ask1_qty": _i(o.get("askp_rsqn1")), "bid1_qty": _i(o.get("bidp_rsqn1")),
            "exchange_hhmmss": str(o.get("aspr_acpt_hour") or ""), "quote_ts": now_fn(),
            "elapsed_sec": round(time.time() - t0, 3)}


def fetch_buying_power(client: Any, code: str, price: int) -> Dict[str, Any]:
    rsp = client.inquire_psbl_order(code=code, order_price=int(price), ord_dvsn="00")
    o = rsp.get("output") or {}
    return {"nrcvb_buy_amt": _i(o.get("nrcvb_buy_amt")), "ruse_psbl_amt": _i(o.get("ruse_psbl_amt")),
            "ord_psbl_cash": _i(o.get("ord_psbl_cash")), "max_buy_amt_do_not_use": _i(o.get("max_buy_amt"))}


DAY_ORDERS_MAX_PAGES = 30


def fetch_day_orders(client: Any, ymd: str):
    """당일 주문 **전체**(체결·미체결·취소). kis_order_client 기본값 ccld_dvsn='01' 은 체결분만이라
    미체결·취소 행이 빠진다 -> 접수된 미체결 주문이 NOT_RECEIVED 로 확정되고(중복 발주 경로)
    취소 확인이 영원히 안 된다(2026-09-19 독립 검토 1번). 저장소 다른 곳도 미체결을 보려면 '00' 을 쓴다.
    페이지 한도에서 잘리면 조용히 넘기지 않는다(kis_order_client 는 pages 를 한도+1 로 돌려준다)."""
    r = client.inquire_daily_ccld(start_ymd=ymd, end_ymd=ymd, ccld_dvsn="00", max_pages=DAY_ORDERS_MAX_PAGES)
    if int(r.get("pages") or 0) > DAY_ORDERS_MAX_PAGES:
        raise RuntimeError(f"DAY_ORDERS_TRUNCATED:pages>{DAY_ORDERS_MAX_PAGES}")
    return r.get("rows", []) or []


def fetch_balance(client: Any) -> Dict[str, Any]:
    b = client.inquire_balance_positions(max_pages=10)
    s = (b.get("summary_rows") or [{}])[0]
    rows = [r for r in (b.get("rows") or []) if _i(r.get("hldg_qty")) > 0]
    holdings = {str(r.get("pdno")): _i(r.get("hldg_qty")) for r in rows}
    prices = {str(r.get("pdno")): _i(r.get("prpr")) for r in rows if _i(r.get("prpr")) > 0}  # prpr 현재가
    return {"holdings": holdings, "prices": prices, "dnca_tot_amt": _i(s.get("dnca_tot_amt")),
            "prvs_rcdl_excc_amt": _i(s.get("prvs_rcdl_excc_amt")), "nass_amt": _i(s.get("nass_amt")),
            "thdt_buy_amt": _i(s.get("thdt_buy_amt")), "thdt_sll_amt": _i(s.get("thdt_sll_amt"))}
