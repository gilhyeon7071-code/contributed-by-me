"""E2 발주 · 주문 상태기계 — E1 주문을 KIS 로 보내고 상태를 날짜 없는 append-only 이벤트 로그로 남긴다.

블록 출처: C9 A(집행 시간 09:05~15:20), C9 C(15:20 취소, 다음 거래일 1회 재주문, UNKNOWN_PENDING 은
          조회로 미접수 확인 전 재전송 금지, 같은 order_id 중복 실행 건너뜀, 강제 재전송 금지).
2026-09-17 확인: tools/kis_order_client.py 는 POST 타임아웃은 UNKNOWN_PENDING 으로 올리지만
연결 오류는 '재시도 가능 NETWORK' 로 올린다. 응답 도중 끊겨도 주문은 접수됐을 수 있으므로
**발송 중 난 모든 예외를 UNKNOWN_PENDING 으로** 다룬다. 발송 전 입력 검사(ValueError)만 REJECTED_LOCAL.
spec/E2_submission_state_machine.md 참조.
"""
from __future__ import annotations

import json
from datetime import datetime, time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

TERMINAL = {"FILLED", "REJECTED", "REJECTED_LOCAL", "CANCELLED_UNFILLED", "PARTIAL_CANCELLED", "NOT_RECEIVED"}
TRANSITIONS = {
    None: {"SUBMITTING"},
    "SUBMITTING": {"ACCEPTED", "REJECTED", "REJECTED_LOCAL", "UNKNOWN_PENDING"},
    "UNKNOWN_PENDING": {"ACCEPTED", "NOT_RECEIVED"},
    "ACCEPTED": {"PARTIALLY_FILLED", "FILLED", "CANCELLED_UNFILLED", "PARTIAL_CANCELLED", "CANCEL_FAILED"},
    "PARTIALLY_FILLED": {"PARTIALLY_FILLED", "FILLED", "PARTIAL_CANCELLED", "CANCEL_FAILED"},
    "CANCEL_FAILED": {"PARTIALLY_FILLED", "FILLED", "CANCELLED_UNFILLED", "PARTIAL_CANCELLED", "CANCEL_FAILED"},
}
MAX_ATTEMPT = 2  # C9: 다음 거래일 1회만 다시


class OrderBook:
    """이벤트 로그를 다시 읽어 주문별 현재 상태를 만든다. 로그가 유일한 진실이다."""

    def __init__(self, log_path: Path):
        self.log_path = Path(log_path)
        self.orders: Dict[str, Dict[str, Any]] = {}
        if self.log_path.exists():
            for line in self.log_path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    self._apply(json.loads(line), persist=False)
        # 발송 중이던 기록만 있고 결과가 없으면(프로세스가 죽음) 모르는 상태다.
        # [2026-09-19] 이 전이를 **로그에도** 남긴다. 메모리에서만 바꾸면 뒤에 붙는 NOT_RECEIVED 가
        #   다음 로드 때 SUBMITTING->NOT_RECEIVED(표에 없음)로 읽혀 장부 전체가 안 열린다(독립 검토 5번).
        for o in list(self.orders.values()):
            if o["state"] == "SUBMITTING":
                self._apply(_ev(o, "UNKNOWN_PENDING", datetime.now(), error="RECOVERED_FROM_SUBMITTING"), persist=True)
                o["recovered_from_submitting"] = True

    def state(self, order_id: str) -> Optional[str]:
        o = self.orders.get(order_id)
        return o["state"] if o else None

    def record(self, event: Dict[str, Any]) -> None:
        self._apply(event, persist=True)

    def _apply(self, event: Dict[str, Any], persist: bool) -> None:
        oid = event["order_id"]
        cur = self.orders.get(oid)
        prev = cur["state"] if cur else None
        new = event["state"]
        if new not in TRANSITIONS.get(prev, set()):
            raise ValueError(f"INVALID_TRANSITION:{oid}:{prev}->{new}")
        if cur is None:
            cur = {k: event.get(k) for k in ("order_id", "intent_id", "attempt_no", "code", "side",
                                             "limit_price", "requested_qty", "rebalance_id")}
            cur.update({"filled_qty": 0, "events": 0})
            self.orders[oid] = cur
        filled = event.get("filled_qty")
        if filled is not None:
            if int(filled) > int(cur["requested_qty"]):
                raise ValueError(f"OVERFILL:{oid}:{filled}>{cur['requested_qty']}")
            if int(filled) < int(cur["filled_qty"]):
                raise ValueError(f"FILL_DECREASED:{oid}:{filled}<{cur['filled_qty']}")
            cur["filled_qty"] = int(filled)
        for k in ("broker_order_no", "broker_org_no"):
            if event.get(k):
                cur[k] = event[k]
        cur["state"] = new
        cur["events"] += 1
        cur["last_ts"] = event.get("ts")
        if persist:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(event, ensure_ascii=False) + "\n")


def _hhmmss(s: str) -> time:
    return datetime.strptime(s, "%H:%M:%S").time()


def window_reason(now: datetime, cfg: Dict[str, Any]) -> Optional[str]:
    t = now.time()
    if not (_hhmmss(cfg["submit_start"]) <= t <= _hhmmss(cfg["submit_end"])):
        return f"OUTSIDE_SUBMIT_WINDOW:{t:%H:%M:%S}"
    for start, end in cfg.get("submit_blackouts", []):
        if _hhmmss(start) <= t <= _hhmmss(end):
            return f"SUBMIT_BLACKOUT:{start}-{end}"
    return None


def _ev(order: Dict[str, Any], state: str, now: datetime, **extra) -> Dict[str, Any]:
    base = {k: order.get(k) for k in ("order_id", "intent_id", "attempt_no", "code", "side",
                                      "limit_price", "requested_qty", "rebalance_id")}
    return {**base, "state": state, "ts": now.isoformat(timespec="seconds"), **extra}


def submit_orders(orders: List[Dict[str, Any]], client: Any, book: OrderBook, now: datetime,
                  cfg: Dict[str, Any], clock=None, pre_submit=None) -> Dict[str, Any]:
    """주문을 보낸다. 창 밖이면 하나도 안 보낸다. 모르는 상태가 한 건이라도 나오면 나머지를 멈춘다.

    pre_submit(order) -> (order, skip_reason): 보내기 직전 호가를 다시 받아 지정가를 갱신하는 자리.
    50종목을 차례로 조회하면 앞 종목 호가가 5초를 넘기므로 호가 신선도는 **주문 한 건마다** 본다."""
    clock = clock or (lambda: now)
    reason = window_reason(now, cfg)
    if reason:
        return {"status": "STOP", "reasons": [reason], "submitted": 0}
    if cfg.get("mock_only", True) is not True:
        return {"status": "STOP", "reasons": ["MOCK_ONLY_REQUIRED"], "submitted": 0}
    if getattr(getattr(client, "cfg", None), "mock", True) is not True:
        return {"status": "STOP", "reasons": ["CLIENT_NOT_MOCK"], "submitted": 0}

    result = {"status": "OK", "reasons": [], "submitted": 0, "accepted": 0, "rejected": 0,
              "unknown": 0, "skipped_existing": 0, "skipped_pre_submit": [], "rows": []}
    for o in orders:
        if int(o["attempt_no"]) > MAX_ATTEMPT:
            result["reasons"].append(f"ATTEMPT_LIMIT:{o['order_id']}")
            continue
        if book.state(o["order_id"]) is not None:
            result["skipped_existing"] += 1
            continue
        if pre_submit is not None:
            o, skip = pre_submit(o)
            if skip:
                result["skipped_pre_submit"].append({"order_id": o["order_id"], "reason": skip})
                continue
        # [2026-09-19] 창은 주문마다, **보내기 직전**(호가 갱신 뒤)에 다시 본다. 건마다 호가를 받아 50건이면
        #   1분 가까이 걸려, 시작 때만 보면 15:20 이후나 09:19:30~09:21:30 금지 구간에도 나간다(독립 검토 10번).
        late = window_reason(clock(), cfg)
        if late:
            result["status"] = "STOP"
            result["reasons"].append(f"WINDOW_CLOSED_MID_RUN:{late}:{o['order_id']}")
            break
        book.record(_ev(o, "SUBMITTING", clock()))
        try:
            rsp = client.place_order_cash(side=o["side"], code=o["code"], qty=int(o["requested_qty"]),
                                          order_type="limit", price=int(o["limit_price"]))
        except ValueError as e:  # 발송 전 입력 검사 — 확실히 안 나갔다
            book.record(_ev(o, "REJECTED_LOCAL", clock(), error=str(e)))
            result["rejected"] += 1
            continue
        except Exception as e:  # 발송 중 — 나갔는지 모른다
            book.record(_ev(o, "UNKNOWN_PENDING", clock(), error=f"{type(e).__name__}:{e}"))
            result["unknown"] += 1
            result["status"] = "STOP"
            result["reasons"].append(f"UNKNOWN_PENDING_STOPS_REMAINING:{o['order_id']}")
            break
        result["submitted"] += 1
        if rsp.get("ok") and rsp.get("ord_no"):
            book.record(_ev(o, "ACCEPTED", clock(), broker_order_no=rsp["ord_no"], broker_org_no=rsp.get("org_no"),
                            rt_cd=rsp.get("rt_cd"), msg=rsp.get("msg1")))
            result["accepted"] += 1
        elif rsp.get("ok"):  # 성공인데 주문번호가 없다 — 귀속 불가라 모르는 상태로
            book.record(_ev(o, "UNKNOWN_PENDING", clock(), error="OK_WITHOUT_ORDER_NO", msg=rsp.get("msg1")))
            result["unknown"] += 1
            result["status"] = "STOP"
            result["reasons"].append(f"OK_WITHOUT_ORDER_NO:{o['order_id']}")
            break
        else:
            book.record(_ev(o, "REJECTED", clock(), rt_cd=rsp.get("rt_cd"), msg=rsp.get("msg1"),
                            error_code=rsp.get("error_code")))
            result["rejected"] += 1
    if result["rejected"] and result["status"] == "OK":
        result["reasons"].append(f"REJECTED:{result['rejected']}")
    return result


def resolve_unknown(book: OrderBook, broker_rows: Iterable[Dict[str, Any]], now: datetime,
                    query_complete: bool) -> Dict[str, Any]:
    """모르는 상태 주문을 브로커 당일 주문 조회와 맞춘다. 종목·방향·수량·가격이 정확히 한 건 맞으면 접수로,
    조회가 완전하고 맞는 건이 없으면 미접수로. 두 건 이상이면 그대로 둔다(추정 금지)."""
    side_code = {"SELL": "01", "BUY": "02"}
    known_nos = {o.get("broker_order_no") for o in book.orders.values() if o.get("broker_order_no")}
    rows = [r for r in broker_rows if str(r.get("odno")) not in known_nos]
    out = {"accepted": [], "not_received": [], "ambiguous": []}
    for o in list(book.orders.values()):
        if o["state"] != "UNKNOWN_PENDING":
            continue
        matches = [r for r in rows if str(r.get("pdno")) == o["code"]
                   and str(r.get("sll_buy_dvsn_cd")) == side_code[o["side"]]
                   and int(r.get("ord_qty", 0)) == int(o["requested_qty"])
                   and int(float(r.get("ord_unpr", 0))) == int(o["limit_price"])]
        if len(matches) == 1:
            m = matches[0]
            book.record(_ev(o, "ACCEPTED", now, broker_order_no=str(m.get("odno")),
                            broker_org_no=str(m.get("ord_gno_brno", "")), resolved_by="broker_query"))
            rows = [r for r in rows if r is not m]
            out["accepted"].append(o["order_id"])
        elif not matches and query_complete:
            book.record(_ev(o, "NOT_RECEIVED", now, resolved_by="broker_query"))
            out["not_received"].append(o["order_id"])
        elif len(matches) > 1:
            out["ambiguous"].append(o["order_id"])
    return out


def cancel_remaining(book: OrderBook, client: Any, now: datetime, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """15:20 이후 남은 수량 취소. 취소 성공은 체결 수로 CANCELLED_UNFILLED / PARTIAL_CANCELLED, 실패는 크게 남긴다."""
    if now.time() < _hhmmss(cfg["submit_end"]):
        return {"status": "STOP", "reasons": [f"CANCEL_TOO_EARLY:{now:%H:%M:%S}"]}
    out = {"status": "OK", "reasons": [], "cancelled": [], "failed": []}
    for o in list(book.orders.values()):
        if o["state"] not in {"ACCEPTED", "PARTIALLY_FILLED", "CANCEL_FAILED"}:
            continue
        try:
            rsp = client.cancel_order(org_order_no=o["broker_order_no"], org_order_branch_no=o.get("broker_org_no") or "",
                                      cancel_all=True)
            ok = bool(rsp.get("ok"))
            msg = rsp.get("msg1")
        except Exception as e:
            ok, msg = False, f"{type(e).__name__}:{e}"
        if ok:
            state = "CANCELLED_UNFILLED" if int(o["filled_qty"]) == 0 else "PARTIAL_CANCELLED"
            book.record(_ev(o, state, now, msg=msg))
            out["cancelled"].append(o["order_id"])
        else:
            book.record(_ev(o, "CANCEL_FAILED", now, msg=msg))
            out["failed"].append(o["order_id"])
    if out["failed"]:
        out["status"] = "STOP"
        out["reasons"].append(f"CANCEL_FAILED:{len(out['failed'])}")
    return out


def cancel_one_now(book: OrderBook, client: Any, order_id: str, now: datetime, reason: str) -> Dict[str, Any]:
    """시험용·수동 취소: 한 주문을 지금 취소한다(15:20 규칙은 정기 취소에만). 사유를 반드시 남긴다."""
    o = book.orders.get(order_id)
    if not o or o["state"] not in {"ACCEPTED", "PARTIALLY_FILLED", "CANCEL_FAILED"}:
        return {"status": "STOP", "reasons": [f"NOT_CANCELLABLE:{order_id}:{o['state'] if o else None}"]}
    try:
        rsp = client.cancel_order(org_order_no=o["broker_order_no"], org_order_branch_no=o.get("broker_org_no") or "",
                                  cancel_all=True)
        ok, msg = bool(rsp.get("ok")), rsp.get("msg1")
    except Exception as e:
        ok, msg = False, f"{type(e).__name__}:{e}"
    if ok:
        state = "CANCELLED_UNFILLED" if int(o["filled_qty"]) == 0 else "PARTIAL_CANCELLED"
        book.record(_ev(o, state, now, msg=msg, cancel_reason=reason))
        return {"status": "OK", "reasons": [], "state": state}
    book.record(_ev(o, "CANCEL_FAILED", now, msg=msg, cancel_reason=reason))
    return {"status": "STOP", "reasons": [f"CANCEL_FAILED:{order_id}"]}


def replay_candidates(book: OrderBook) -> List[Dict[str, Any]]:
    """다음 거래일 1회 재주문 대상: 1차 시도가 취소·거절로 끝나 남은 수량이 있는 것. 2차는 대상이 아니다."""
    attempts: Dict[str, List[Dict[str, Any]]] = {}
    for o in book.orders.values():
        attempts.setdefault(o["intent_id"], []).append(o)
    out = []
    for intent, os_ in attempts.items():
        if any(int(o["attempt_no"]) >= MAX_ATTEMPT for o in os_):
            continue
        first = os_[0]
        if first["state"] in {"CANCELLED_UNFILLED", "PARTIAL_CANCELLED", "REJECTED", "NOT_RECEIVED"}:
            remaining = int(first["requested_qty"]) - int(first["filled_qty"])
            if remaining > 0:
                out.append({"intent_id": intent, "code": first["code"], "side": first["side"],
                            "remaining_qty": remaining, "next_attempt_no": int(first["attempt_no"]) + 1})
    return out
