"""긴급 전량 청산 — **요청과 집행을 분리**한다 (2026-09-21 신설).

왜 분리하나: 화면(브라우저)에 실주문 권한을 주지 않기 위해서다.
버튼은 **요청 파일만** 남기고, 집행은 이 전용 경로가 한다.
그래야 오조작·원격 접근이 곧바로 주문이 되지 않는다.

왜 기존 경로를 안 쓰나:
  `run_execution_day.liquidate_after_latch` 는 **래치 전용**이고 **발동 다음 거래일부터**만 판다.
  수동 긴급 청산은 사유도 다르고(원장이 섞이면 안 된다) 즉시여야 한다.
  그리고 그 함수는 09-22 실발주 시험과 09-28 가부의 **검증 대상 코드**라 건드리지 않는다.
  대신 같은 모듈(execution·fills·kis_adapter)과 같은 주문 ID 계약을 **재사용**한다.

안전 규칙 셋:
  1. **무장 스위치** — `daily_ops_v1.json` 의 `kill_switch_armed` 가 true 일 때만 보낸다.
     꺼져 있으면 계획만 하고 남긴다(그림자). auto_submit 무장과 같은 급의 승인 사항이다.
  2. **멱등** — 요청 하나당 한 번만 집행한다(request_id 기준). 두 번 눌러도 주문이 두 번 나가지 않는다.
  3. **사유 태그 분리** — `rebalance_id = <전략>|KILL|<요청ID>`. 래치 청산·분기 재구성과 원장에서 섞이지 않는다.

계좌 몫이 아닌 주식을 팔지 않는다: 종목마다 **원장과 계좌 중 적은 쪽**만 판다(래치 청산과 같은 규칙).
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List

from paper.strategies.kospi_mcap_quarterly_v2.src import execution as E
from paper.strategies.kospi_mcap_quarterly_v2.src import fills as F
from paper.strategies.kospi_mcap_quarterly_v2.src import kis_adapter as K
from paper.strategies.kospi_mcap_quarterly_v2.src import run_execution_day as R

REQUEST = "kill_request.json"


def _write(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def request(*, state_dir: Path, reason: str, by: str,
            clock: Callable[[], datetime] = datetime.now) -> Dict[str, Any]:
    """요청만 남긴다. **주문은 나가지 않는다.**"""
    now = clock()
    state_dir = Path(state_dir)
    rec = {"request_id": f"KILL-{now:%Y%m%d-%H%M%S}", "requested_at": now.isoformat(timespec="seconds"),
           "by": by, "reason": reason, "status": "REQUESTED"}
    _write(state_dir / REQUEST, rec)
    return rec


def read_request(state_dir: Path) -> Dict[str, Any]:
    p = Path(state_dir) / REQUEST
    if not p.is_file():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as exc:                       # 못 읽으면 '없음' 이 아니라 '모름' 이다
        return {"status": "UNREADABLE", "error": f"{type(exc).__name__}: {exc}"}


def execute(client: Any, *, state_dir: Path, cfg: Dict[str, Any], ops: Dict[str, Any],
            no_submit: bool = False, clock: Callable[[], datetime] = datetime.now) -> Dict[str, Any]:
    now = clock()
    ymd = now.strftime("%Y%m%d")
    state_dir = Path(state_dir)
    rep: Dict[str, Any] = {"mode": "kill", "run_at": now.isoformat(timespec="seconds"),
                           "status": "OK", "reasons": [], "orders": [], "mismatches": [],
                           "submitted": 0}

    req = read_request(state_dir)
    if not req:
        rep.update(status="STANDBY", reasons=["NO_REQUEST"])
        return rep
    if req.get("status") == "UNREADABLE":
        rep.update(status="STOP", reasons=["REQUEST_UNREADABLE:" + str(req.get("error"))])
        return rep
    rid = str(req.get("request_id") or "")
    rep["request_id"] = rid
    if req.get("status") in ("EXECUTED", "DONE"):   # 멱등 — 같은 요청을 두 번 집행하지 않는다
        rep.update(status="STANDBY", reasons=[f"ALREADY_EXECUTED:{req.get('executed_at')}"])
        return rep

    armed = bool(ops.get("kill_switch_armed", False))
    rep["armed"] = armed

    book = E.OrderBook(state_dir / "orders.jsonl")
    live = [o["order_id"] for o in book.orders.values()
            if o["state"] not in E.TERMINAL and not str(o["order_id"]).startswith(f"{cfg['strategy_id']}|KILL|")]
    if live:
        rep.update(status="STOP", reasons=[f"OPEN_ORDERS_FROM_EARLIER:{live}"])
        _write(state_dir / f"kill_{ymd}_{now:%H%M%S}.json", rep)
        return rep

    holdings: Dict[str, int] = {}
    for f in F.load_fills(state_dir / "fills.jsonl"):
        holdings[f["code"]] = holdings.get(f["code"], 0) + (int(f["qty"]) if f["side"] == "BUY" else -int(f["qty"]))
    bal = K.fetch_balance(client)
    rebalance_id = f"{cfg['strategy_id']}|KILL|{rid}"
    for code, qty in sorted(holdings.items()):
        if qty <= 0:
            continue
        broker_q = int(bal["holdings"].get(code, 0))
        if broker_q < qty:                          # 래치 청산과 같은 규칙: 적은 쪽만 판다
            rep["mismatches"].append({"code": code, "ledger_qty": qty, "broker_qty": broker_q})
            qty = broker_q
            if qty <= 0:
                continue
        intent = f"{rebalance_id}|{code}|SELL|{ymd}"
        rep["orders"].append({"order_id": f"{intent}|1", "intent_id": intent, "attempt_no": 1, "code": code,
                              "side": "SELL", "limit_price": 0, "requested_qty": qty,
                              "rebalance_id": rebalance_id, "execution_date": ymd})

    if not rep["orders"]:
        rep["reasons"].append("NOTHING_TO_LIQUIDATE")

    if not armed:
        rep["reasons"].append("NOT_ARMED_SHADOW")   # 계획만 남긴다
    elif no_submit:
        rep["reasons"].append("NO_SUBMIT_REHEARSAL")
    elif rep["orders"]:
        log: List[Dict[str, Any]] = []
        rep["submit"] = E.submit_orders(rep["orders"], client, book, clock(), cfg, clock=clock,
                                        pre_submit=R.refresh_price_hook(client, cfg, clock, log))
        rep["submit_price_refresh"] = log
        rep["submitted"] = int(rep["submit"].get("submitted", 0) or 0)
        if rep["submit"]["status"] != "OK" or rep["submit"]["skipped_pre_submit"]:
            rep.update(status="STOP", reasons=rep["reasons"] + rep["submit"]["reasons"] +
                       [f"SKIPPED:{x['order_id']}:{x['reason']}" for x in rep["submit"]["skipped_pre_submit"]])

    if rep["mismatches"]:                           # 보낸 뒤에도 사람이 봐야 한다
        rep["status"] = "STOP"
        rep["reasons"].append("LEDGER_EXCEEDS_BROKER_HOLDING:" + ",".join(
            f"{m['code']}(ledger={m['ledger_qty']},broker={m['broker_qty']})" for m in rep["mismatches"]))

    # 요청 파일에 결과를 되돌려 화면이 '요청됨 / 집행됨 / 무장 안 됨' 을 구분할 수 있게 한다
    if armed and not no_submit:
        req.update(status="EXECUTED", executed_at=now.isoformat(timespec="seconds"),
                   result_status=rep["status"], submitted=rep["submitted"], orders=len(rep["orders"]))
    else:
        req.update(status="REQUESTED", last_shadow_at=now.isoformat(timespec="seconds"),
                   last_shadow_orders=len(rep["orders"]),
                   last_shadow_reason=("NOT_ARMED" if not armed else "NO_SUBMIT"))
    _write(state_dir / REQUEST, req)
    _write(state_dir / f"kill_{ymd}_{now:%H%M%S}.json", rep)
    return rep


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="긴급 전량 청산 — 요청/집행 분리")
    sub = ap.add_subparsers(dest="cmd", required=True)
    rq = sub.add_parser("request")
    rq.add_argument("--state-dir", required=True, type=Path)
    rq.add_argument("--reason")
    # [2026-09-21] 한글 사유가 Node->Python 명령줄에서 깨졌다(파일 저장값까지 깨짐 = 인자 층 문제).
    #   PYTHONIOENCODING 은 출력 층이라 소용없었다. **인코딩 문제 자체를 없앤다** — base64 로 받는다.
    rq.add_argument("--reason-b64", help="사유를 base64(utf-8)로. 명령줄 인코딩을 타지 않는다")
    rq.add_argument("--by", default="cli")
    st = sub.add_parser("status")
    st.add_argument("--state-dir", required=True, type=Path)
    ex = sub.add_parser("execute")
    ex.add_argument("--state-dir", required=True, type=Path)
    ex.add_argument("--no-submit", action="store_true")
    ex.add_argument("--strategy-cfg", type=Path, default=R.DEFAULT_STRATEGY_CFG)
    ex.add_argument("--ops-cfg", type=Path,
                    default=Path(__file__).resolve().parents[1] / "config" / "daily_ops_v1.json")
    a = ap.parse_args(argv)

    if a.cmd == "request":
        if a.reason_b64:
            import base64
            reason = base64.b64decode(a.reason_b64).decode("utf-8", errors="replace")
        elif a.reason:
            reason = a.reason
        else:
            print(json.dumps({"status": "ERROR", "error": "--reason 또는 --reason-b64 가 필요하다"},
                             ensure_ascii=False))
            return 2
        print(json.dumps(request(state_dir=a.state_dir, reason=reason, by=a.by),
                         ensure_ascii=False, indent=2))
        return 0
    if a.cmd == "status":
        print(json.dumps(read_request(a.state_dir) or {"status": "NO_REQUEST"},
                         ensure_ascii=False, indent=2))
        return 0
    cfg = json.loads(a.strategy_cfg.read_text(encoding="utf-8"))
    ops = json.loads(a.ops_cfg.read_text(encoding="utf-8"))
    res = execute(K.make_client(), state_dir=a.state_dir, cfg=cfg, ops=ops, no_submit=a.no_submit)
    print(json.dumps(res, ensure_ascii=False, indent=2, default=str))
    return 0 if res["status"] in ("OK", "STANDBY") else 3


if __name__ == "__main__":
    raise SystemExit(main())
