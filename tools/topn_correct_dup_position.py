# -*- coding: utf-8 -*-
"""[일회성 교정] 2026-09-08 중복 발주로 생긴 초과 보유분을 의도 수량으로 되돌린다.

경위  PLANS (245). 2026-09-08 에 003230 이 12:41 / 13:05 / 13:15 세 번 발주돼
      의도 12주 대신 27주가 체결됐다. dedup 이 죽은 상태였고 그 방아쇠는
      생산 경로로 돌린 시험 실행이었다. 사용자 결정: **초과분을 매도해 되돌린다.**

성격  전략 판단이 아니라 **교정**이다. 그래서 주문 note 에 CORRECTION 을 박아
      나중에 이 거래를 전략 성과로 오독하지 않게 한다.

안전  1) 실행 시점에 브로커 보유를 다시 조회한다. 눈으로 본 27 을 믿지 않는다
      2) 초과분이 0 이하면 **아무것도 하지 않는다**(이미 정리됐거나 상황이 바뀐 것)
      3) 초과분이 기대치(15)보다 크면 멈춘다. 모르는 일이 더 있었다는 뜻이다
      4) 매도 지정가는 저장소 규칙(직전가 x 0.995, 호가단위 내림)을 그대로 쓴다
      5) --apply 없이는 계획만 출력한다

일회성  목적을 다하면 지운다. 예약작업도 함께 지운다(VIBE_TopN_DupFix_Once).
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tools"))

from kis_order_client import KISOrderClient  # noqa: E402
from utils.krx_tick import round_tick  # noqa: E402

CODE = "003230"
WANT_QTY = 12          # 의도 수량 (자본/6 / 지정가 1,337,000)
MAX_EXCESS = 15        # 2026-09-08 시점 실측 초과분. 이보다 크면 멈춘다
TAG = "RD_TOPN_STAGE1"
OUT_DIR = ROOT / "2_Logs" / "topn"


def _held_qty(client: KISOrderClient, code: str) -> int:
    pos = client.inquire_balance_positions()
    for r in (pos.get("rows") or pos.get("output1") or []):
        if str(r.get("pdno", "")).zfill(6) == code:
            raw = str(r.get("hldg_qty") or r.get("qty") or 0).replace(",", "")
            return int(float(raw))
    return 0


def _append_submit_row(d: str, mode: str, row: dict) -> Path:
    """교정 매도를 우리 발주 이력에 남긴다.

    topn_reconcile 이 브로커 체결을 **주문번호로만** 귀속하므로(PLANS (235)),
    여기에 남기지 않으면 원장은 27주로 남고 실제는 12주가 되어 어긋난다.
    note 에 CORRECTION 을 박아 전략 거래와 구분한다.
    """
    p = OUT_DIR / ("topn_orders_%s_broker_submit_%s.csv" % (d, mode))
    cols = ["dispatch_ts", "exec_date", "side", "code", "qty", "price", "signal_date",
            "is_stop", "note", "entry_blocked", "entry_block_reason", "source_row",
            "dispatch_status", "ok", "rt_cd", "msg1", "ord_no", "org_no", "tr_id", "error"]
    exists = p.exists()
    with p.open("a", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        if not exists:
            w.writeheader()
        w.writerow(row)
    return p


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="없으면 계획만 출력한다")
    ap.add_argument("--mock", default="true", choices=["true", "false"])
    args = ap.parse_args()

    if args.mock != "true":
        print("[STOP] 이 교정 도구는 모의계좌 전용이다")
        return 2

    c = KISOrderClient.from_env(mock="true")
    held = _held_qty(c, CODE)
    excess = held - WANT_QTY
    print("[HOLD] %s 보유 %d주 / 의도 %d주 -> 초과 %d주" % (CODE, held, WANT_QTY, excess), flush=True)

    if excess <= 0:
        print("[SKIP] 초과분이 없다. 아무것도 하지 않는다 (이미 정리됐거나 상황이 바뀌었다)")
        return 0
    if excess > MAX_EXCESS:
        print("[STOP] 초과분 %d주 가 기대치 %d주 보다 크다. 모르는 일이 더 있다는 뜻이므로 멈춘다"
              % (excess, MAX_EXCESS))
        return 2

    px = c.inquire_price(code=CODE)
    last = float(str((px.get("output") or {}).get("stck_prpr") or 0).replace(",", ""))
    if last <= 0:
        print("[STOP] 현재가를 못 구했다. 가격 없이 팔지 않는다")
        return 2
    limit = int(round_tick(last * 0.995, up=False))
    print("[PLAN] SELL %s %d주 @ %s  (직전가 %s x 0.995, 호가단위 내림)"
          % (CODE, excess, format(limit, ","), format(int(last), ",")), flush=True)

    if not args.apply:
        print("[DRY] --apply 가 없어 발주하지 않았다")
        return 0

    res = c.place_order_cash(side="SELL", code=CODE, qty=excess, order_type="limit", price=limit)
    ok = bool(res.get("ok"))
    print("[ORDER] ok=%s ord_no=%s msg=%s" % (ok, res.get("ord_no"), str(res.get("msg1"))[:80]), flush=True)

    now = dt.datetime.now()
    d = now.strftime("%Y%m%d")
    p = _append_submit_row(d, "mock", {
        "dispatch_ts": now.isoformat(timespec="seconds"),
        "exec_date": d, "side": "SELL", "code": CODE, "qty": excess, "price": limit,
        "signal_date": "", "is_stop": "False",
        "note": "%s CORRECTION dup_dispatch_20260908 (PLANS 245)" % TAG,
        "entry_blocked": "False", "entry_block_reason": "",
        "dispatch_status": "ACCEPTED" if ok else "REJECTED",
        "ok": ok, "rt_cd": res.get("rt_cd"), "msg1": str(res.get("msg1"))[:120],
        "ord_no": res.get("ord_no"), "org_no": res.get("org_no"), "tr_id": res.get("tr_id"),
        "error": str(res.get("error_code") or ""),
    })
    print("[LOG] 발주 이력에 기록: %s" % p, flush=True)

    (OUT_DIR / "dup_position_fix_20260908.json").write_text(
        json.dumps({"ran_at": now.isoformat(timespec="seconds"), "code": CODE,
                    "held_before": held, "want": WANT_QTY, "sold_qty": excess,
                    "limit": limit, "ok": ok, "ord_no": res.get("ord_no"),
                    "msg1": str(res.get("msg1"))[:120]}, ensure_ascii=False, indent=2),
        encoding="utf-8")
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
