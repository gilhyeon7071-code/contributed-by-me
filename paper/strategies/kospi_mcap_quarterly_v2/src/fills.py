"""E3 체결 확인 — KIS 당일 주문·체결 조회 행을 주문 상태기계와 체결 로그에 반영한다. 판정만, 발주 안 함.

조회 필드(2026-09-17 실물 확인, inquire_daily_ccld rows):
  odno 주문번호 / pdno 종목 / sll_buy_dvsn_cd 01 매도 02 매수 / ord_qty / ord_unpr /
  tot_ccld_qty 누적 체결 / tot_ccld_amt 누적 체결금액 / avg_prvs 평균가 / rmn_qty 잔량 / cncl_yn / ord_dt
체결 금액은 평균가×수량이 아니라 tot_ccld_amt 를 쓴다(377450: 15,914×976=15,532,064 vs 실제 15,532,930).
spec/E3_fill_confirmation.md 참조.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

from paper.strategies.kospi_mcap_quarterly_v2.src.execution import OrderBook, _ev

SIDE_CODE = {"SELL": "01", "BUY": "02"}


def load_fills(path: Path) -> List[Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    return [json.loads(x) for x in p.read_text(encoding="utf-8").splitlines() if x.strip()]


def _append(path: Path, row: Dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def _int(v: Any) -> int:
    try:
        return int(float(str(v).replace(",", "").strip() or 0))
    except ValueError:
        return 0


def sync_fills(book: OrderBook, broker_rows: Iterable[Dict[str, Any]], fills_path: Path, now: datetime) -> Dict[str, Any]:
    """누적 체결의 증가분만 새 체결로 쓴다(fill_id = order_id|누적수량 → 다시 돌려도 같은 결과)."""
    rows = {str(r.get("odno")): r for r in broker_rows}
    fills = load_fills(fills_path)
    seen = {f["fill_id"] for f in fills}
    prev: Dict[str, Dict[str, int]] = {}
    for f in fills:
        p = prev.setdefault(f["order_id"], {"qty": 0, "amount": 0})
        p["qty"] = max(p["qty"], int(f["cum_qty"]))
        p["amount"] = max(p["amount"], int(f["cum_amount"]))

    out = {"status": "OK", "new_fills": [], "incidents": [], "missing": [], "orphans": 0}
    ours = set()
    for o in list(book.orders.values()):
        no = o.get("broker_order_no")
        if not no:
            continue
        ours.add(str(no))
        r = rows.get(str(no))
        if r is None:
            if o["state"] in {"ACCEPTED", "PARTIALLY_FILLED"}:
                out["missing"].append(o["order_id"])
            continue
        if str(r.get("pdno")) != o["code"] or str(r.get("sll_buy_dvsn_cd")) != SIDE_CODE[o["side"]] \
                or _int(r.get("ord_qty")) != int(o["requested_qty"]):
            out["incidents"].append({"type": "BROKER_ROW_MISMATCH", "order_id": o["order_id"],
                                     "detail": {k: r.get(k) for k in ("pdno", "sll_buy_dvsn_cd", "ord_qty")}})
            continue
        cum_qty, cum_amt = _int(r.get("tot_ccld_qty")), _int(r.get("tot_ccld_amt"))
        p = prev.get(o["order_id"], {"qty": 0, "amount": 0})
        if cum_qty > int(o["requested_qty"]):
            out["incidents"].append({"type": "OVERFILL", "order_id": o["order_id"], "detail": cum_qty})
            continue
        if cum_qty < p["qty"]:
            out["incidents"].append({"type": "FILL_DECREASED", "order_id": o["order_id"], "detail": [p["qty"], cum_qty]})
            continue
        if cum_qty > p["qty"]:
            qty, amount = cum_qty - p["qty"], cum_amt - p["amount"]
            fill = {"fill_id": f"{o['order_id']}|{cum_qty}", "order_id": o["order_id"], "intent_id": o["intent_id"],
                    "rebalance_id": o.get("rebalance_id"), "code": o["code"], "side": o["side"], "qty": qty,
                    "amount": amount, "price": amount / qty, "cum_qty": cum_qty, "cum_amount": cum_amt,
                    "broker_order_no": str(no), "trade_date": str(r.get("ord_dt") or now.strftime("%Y%m%d")),
                    "observed_at": now.isoformat(timespec="seconds")}
            if fill["fill_id"] not in seen:
                _append(fills_path, fill)
                seen.add(fill["fill_id"])
                out["new_fills"].append(fill)
            if o["state"] in {"FILLED", "CANCELLED_UNFILLED", "PARTIAL_CANCELLED"}:
                # 돈은 움직였는데 상태는 끝났다고 적혀 있다 — 체결은 남기고 사고로 올린다
                out["incidents"].append({"type": "FILL_AFTER_TERMINAL", "order_id": o["order_id"],
                                         "detail": [o["state"], cum_qty]})
            else:
                new_state = "FILLED" if cum_qty == int(o["requested_qty"]) else "PARTIALLY_FILLED"
                book.record(_ev(o, new_state, now, filled_qty=cum_qty, cum_amount=cum_amt))
        if o["state"] in {"CANCELLED_UNFILLED", "PARTIAL_CANCELLED"} and _int(r.get("rmn_qty")) > 0 \
                and str(r.get("cncl_yn")) != "Y" and _int(r.get("cncl_cfrm_qty")) == 0:
            out["incidents"].append({"type": "CANCEL_NOT_CONFIRMED", "order_id": o["order_id"],
                                     "detail": {"rmn_qty": r.get("rmn_qty"), "cncl_yn": r.get("cncl_yn")}})
    out["orphans"] = len([n for n in rows if n not in ours])
    if out["incidents"]:
        out["status"] = "STOP"
    return out
