# -*- coding: utf-8 -*-
"""**주문이 실제로 나가는지**를 매일 확인한다. 조회로는 알 수 없기 때문이다.

2026-09-07 신규. PLANS (224)(225) 에서 드러난 구조적 결함:

```
잔고조회        정상 (39종목)
주문가능조회     rt_cd=0 "주문가능조회가 완료되었습니다"
                ord_psbl_cash=31,334,757 / max_buy_qty=474 까지 돌려준다
실제 주문       rt_cd=1 msg_cd=40910000 "모의투자 주문이 불가능한 상태입니다"
```

**읽기 전용 API 는 전부 통과하는데 주문만 거부된다.** 그래서 08-25 이후 13일간
계좌가 막힌 것을 아무도 몰랐다 — 내부 폭락 가드가 먼저 막아 브로커에 닿은 주문이
0건이었고, 조회는 계속 정상이었기 때문이다.

**설계 — 체결되지 않는 주문을 낸다.**

```
방식     005930 1주를 **하한가**에 매수 지정가로 낸다
왜 안전  하한가 매수는 주가가 하한가를 쳐야만 체결된다. 그 확률은 사실상 0 이다.
         지정가라 증거금도 1주분만 잡히고, 즉시 취소하므로 잠깐이다
왜 매수  [2026-09-19 변경] 예전엔 보유 종목 1주를 상한가에 매도했다. 그런데 10-01 부터
         이 모의계좌를 새 로직(분기 시총 V2)도 쓴다 — 보유 매도 프로브는 **다른 전략의
         보유를 시험 재료로 쓴다**(체결되면 그 전략 원장과 계좌가 1주 어긋난다).
         매수 프로브는 어떤 보유도 건드리지 않는다. PLANS 533
직후     즉시 취소한다. 취소 실패는 **큰 소리로** 알린다
판정     접수(rt_cd=0) = 주문 가능 / 거부 = 주문 불가
```

**장중에만 의미가 있다.** 장 시간 밖 거부는 다른 이유이므로 SKIP 으로 남기고
PASS 로 기록하지 않는다(모르는 것을 통과시키지 않는다).

    python tools/order_capability_canary.py              # 관측 + 경보
    python tools/order_capability_canary.py --dry-run    # 주문 없이 조건만 점검
    python tools/order_capability_canary.py --strict     # 주문 불가면 rc=2
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# 배치 콘솔이 cp949 라 em-dash 같은 문자에서 print 가 죽는다.
# 도구가 일을 다 끝내고 **출력에서** 죽으면 rc!=0 이 되어 성공이 실패로 기록된다.
def _force_utf8_stdout() -> None:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


_force_utf8_stdout()

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
TOOLS_DIR = ROOT / "tools"

# 매수 프로브 종목. 유동성이 커야 가격제한 조회가 안정적이다.
NEWLINE = chr(10)

PROBE_CODE = "005930"   # 삼성전자

SESSION_START = 9 * 60 + 5      # 09:05 - 개장 직후 혼잡을 피한다
SESSION_END = 15 * 60 + 10      # 15:10 - 동시호가 전에 끝낸다
CANCEL_RETRIES = 3
CANCEL_SLEEP_SEC = 1.0


def _now() -> dt.datetime:
    return dt.datetime.now()


def _in_session(now: dt.datetime) -> bool:
    m = now.hour * 60 + now.minute
    return SESSION_START <= m <= SESSION_END


def _is_trading_day(now: dt.datetime) -> Optional[bool]:
    try:
        sys.path.insert(0, str(ROOT))
        from holiday_manager import HolidayManager  # type: ignore

        return bool(HolidayManager().is_market_open(now.strftime("%Y%m%d")))
    except Exception:
        return None


def run_canary(mock: bool, dry_run: bool) -> Dict[str, Any]:
    now = _now()
    out: Dict[str, Any] = {
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "account_mode": "mock" if mock else "prod",
        "dry_run": bool(dry_run),
    }

    trading = _is_trading_day(now)
    out["trading_day"] = trading
    out["in_session"] = _in_session(now)
    if trading is None:
        out["status"] = "SKIP"
        out["reason"] = "휴장 달력을 못 읽어 거래일 판정 불가"
        return out
    if not trading or not out["in_session"]:
        out["status"] = "SKIP"
        out["reason"] = ("휴장일" if not trading else
                         f"장 시간 밖 ({now:%H:%M}, 유효 09:05~15:10)")
        return out

    try:
        sys.path.insert(0, str(TOOLS_DIR))
        from kis_order_client import KISOrderClient  # type: ignore

        client = KISOrderClient.from_env(mock=mock)
    except Exception as exc:
        out["status"] = "NO_CLIENT"
        out["reason"] = f"{type(exc).__name__}: {exc}"
        return out

    # [2026-09-19] 보유와 무관하게 항상 **하한가 매수**. 보유 매도 프로브는 같은 계좌를 쓰는
    #   다른 전략(V2)의 보유를 건드린다. 하한가 매수는 주가가 하한가를 쳐야만 체결된다.
    #   지정가라 증거금도 1주분만 잡힌다(시장가는 가용현금의 77%를 잡는다 - PLANS 참조).
    side = "BUY"
    holding = {"code": PROBE_CODE, "qty": 0, "evlu_amt": 0.0, "name": "(매수 프로브)"}
    out["side"] = side
    out["probe"] = holding

    # 가격제한을 얻는다. 하한가 매수는 사실상 체결되지 않는다.
    try:
        p = client.inquire_price(code=holding["code"])
        o = (p or {}).get("output") or {}
        upper = int(float(o.get("stck_mxpr") or 0))
        cur = int(float(o.get("stck_prpr") or 0))
    except Exception as exc:
        out["status"] = "NO_PRICE"
        out["reason"] = f"시세 조회 실패: {type(exc).__name__}: {exc}"
        return out
    lower = int(float(o.get("stck_llam") or 0))
    if upper <= 0 or cur <= 0 or upper <= cur or lower <= 0 or lower >= cur:
        out["status"] = "NO_PRICE"
        out["reason"] = f"가격제한이 유효하지 않다 (상한 {upper}, 하한 {lower}, 현재 {cur})"
        return out
    probe_price = upper if side == "SELL" else lower
    out["probe"].update({"upper_limit": upper, "lower_limit": lower, "current": cur,
                         "probe_price": probe_price,
                         "distance_pct": round(100.0 * (probe_price / cur - 1.0), 2)})

    if dry_run:
        out["status"] = "DRY_RUN"
        out["reason"] = (f"조건 충족. 실제로 냈다면 {holding['code']} 1주 @ {probe_price:,} {side}")
        return out

    # ── 실주문 (체결되지 않도록 하한가 매수) ─────────────────────────────
    try:
        r = client.place_order_cash(side=side, code=holding["code"], qty=1,
                                    order_type="limit", price=probe_price)
    except Exception as exc:
        out["status"] = "ORDER_EXCEPTION"
        out["reason"] = f"{type(exc).__name__}: {exc}"
        return out

    out["order_response"] = {"rt_cd": r.get("rt_cd"), "msg_cd": r.get("msg_cd"),
                             "msg1": r.get("msg1"), "ok": r.get("ok")}
    ord_no = str(r.get("ord_no") or "").strip()
    org_no = str(r.get("org_no") or "").strip()

    if str(r.get("rt_cd")) != "0":
        out["status"] = "ORDER_REJECTED"
        out["reason"] = f"주문 거부 msg_cd={r.get('msg_cd')} {r.get('msg1')}"
        return out

    out["ord_no"] = ord_no
    out["org_no"] = org_no

    # ── 즉시 취소. 실패는 조용히 넘기지 않는다 ────────────────────────────
    cancels: List[Dict[str, Any]] = []
    cancelled = False
    for i in range(CANCEL_RETRIES):
        try:
            cr = client.cancel_order(org_order_no=ord_no, org_order_branch_no=org_no,
                                     cancel_all=True, order_dvsn="00")
            cancels.append({"try": i + 1, "rt_cd": cr.get("rt_cd"),
                            "msg_cd": cr.get("msg_cd"), "msg1": cr.get("msg1")})
            if str(cr.get("rt_cd")) == "0":
                cancelled = True
                break
        except Exception as exc:
            cancels.append({"try": i + 1, "error": f"{type(exc).__name__}: {exc}"})
        time.sleep(CANCEL_SLEEP_SEC)
    out["cancel_attempts"] = cancels
    out["cancelled"] = cancelled

    if cancelled:
        out["status"] = "ORDER_OK"
        out["reason"] = "주문 접수 후 취소 완료 — 계좌는 주문 가능 상태다"
    else:
        out["status"] = "CANCEL_FAILED"
        out["reason"] = (f"주문은 접수됐으나(ord_no={ord_no}) 취소에 {CANCEL_RETRIES}회 실패했다. "
                         f"{side} 1주 @ {probe_price:,} 주문이 남아 있다 — 손으로 취소할 것")
    return out


def build_alert_text(out: Dict[str, Any]) -> Optional[str]:
    st = out.get("status")
    if st in ("ORDER_OK", "DRY_RUN"):
        return None
    head = f"[주문 카나리아] {out['generated_at']} 계좌={out.get('account_mode')} 상태={st}"
    lines = [head, f"  {out.get('reason')}"]
    if st == "SKIP":
        # 휴장일 SKIP 은 사건이 아니다. 그러나 **거래일인데 SKIP** 이면 판정을 못 낸 것이다.
        # 배치 시각이 밀리거나 세션 창 판정이 틀어지면 매일 조용히 SKIP 이 되고,
        # 그 침묵이 정상으로 보인다. 2026-09-07 에 이 구멍을 확인했다.
        if out.get("trading_day"):
            return NEWLINE.join([head, f"  {out.get('reason')}",
                                 "  거래일인데 판정을 못 냈다. 실행 시각·세션 창을 확인할 것"])
        return None
    if st == "CANCEL_FAILED":
        lines.append(f"  ord_no={out.get('ord_no')} org_no={out.get('org_no')} — 즉시 수동 취소 필요")
    pr = out.get("probe") or {}
    if pr:
        lines.append(f"  probe={out.get('side')} {pr.get('code')} @ {pr.get('probe_price')}")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prod", action="store_true", help="실계좌에 낸다 (기본은 모의)")
    ap.add_argument("--dry-run", action="store_true", help="주문 없이 조건만 점검")
    ap.add_argument("--no-alert", action="store_true")
    ap.add_argument("--strict", action="store_true", help="주문 불가면 rc=2")
    ap.add_argument("--out-dir", default=str(LOG_DIR))
    args = ap.parse_args()

    if args.prod:
        # 실계좌 카나리아는 실화폐다. 명시적으로 막아 둔다.
        print("[CANARY] --prod 는 실화폐 주문이다. 이 도구는 모의 전용으로 둔다.")
        return 2

    out = run_canary(mock=True, dry_run=args.dry_run)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = _now().strftime("%Y%m%d_%H%M%S")
    body = json.dumps(out, ensure_ascii=False, indent=2)
    (out_dir / f"order_capability_canary_{ts}.json").write_text(body, encoding="utf-8")
    (out_dir / "order_capability_canary_latest.json").write_text(body, encoding="utf-8")

    print(f"[CANARY] status={out['status']} 계좌={out.get('account_mode')} "
          f"거래일={out.get('trading_day')} 장중={out.get('in_session')}")
    print(f"         {out.get('reason')}")
    if out.get("probe"):
        p = out["probe"]
        print(f"         probe={out.get('side')} {p.get('code')} {p.get('name')} "
              f"현재={p.get('current')} 지시가={p.get('probe_price')} ({p.get('distance_pct')}%)")

    text = build_alert_text(out)
    if text and not args.no_alert:
        try:
            sys.path.insert(0, str(TOOLS_DIR))
            from notify_channels import send_alert  # type: ignore

            lvl = "error" if out["status"] in ("ORDER_REJECTED", "CANCEL_FAILED") else "warn"
            res = send_alert(text, level=lvl, extra={"source": "order_capability_canary"},
                             cooldown_sec=6 * 3600)
            print(f"[CANARY] alert ok={res.get('ok')} suppressed={res.get('suppressed')}")
        except Exception as exc:
            print(f"[CANARY] alert_failed {type(exc).__name__}: {exc}")

    if args.strict and out["status"] not in ("ORDER_OK", "SKIP", "DRY_RUN"):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
