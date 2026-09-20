# -*- coding: utf-8 -*-
"""브로커가 실제로 청구한 수수료·제세금을 매일 받아 기록하고, 설정 상수와 대조한다.

왜 필요한가 (2026-08-22, PLANS 56):
  이 시스템의 비용 상수는 실매매 이력이 없어 추측으로 박혔고 두 곳이 서로 달랐다.
      paper_engine_config.json   fee 0.004%  tax 0.15%
      paper_fills_ledger.csv     fee 0.02%   tax 0.20%
  실계좌 왕복 1건(2026-07-29 카카오 2주)을 브로커 API 로 조회하니
      fee = 0 원,  tl_tax = 144 원 (매도 72,350 의 0.19903%)
  둘 다 틀렸고 방향도 반대였다. 상수는 2026-08-22 에 fee 0 / tax 0.20% 로 고쳤다.

  그런데 **수수료 0 은 증권사 무료 혜택일 수 있다.** 끝나면 비용이 과소계상되는데,
  지금 구조로는 그걸 알아챌 방법이 없다. 이 도구가 그 눈이다.

무엇을 하나
  1. 기간별매매손익(TTTC8715R)을 조회한다 - 읽기 전용, 주문 안 낸다
  2. 거래별 실청구액을 `2_Logs/broker_realized_costs.csv` 에 누적한다(중복 방지)
  3. 실측 요율을 역산해 `paper_engine_config.json` 의 상수와 대조한다
  4. 어긋나면 **크게 남긴다**. 조용히 지나가지 않는 것이 이 도구의 존재 이유다

사용
  python tools/kis_sync_broker_costs.py                      # 최근 7일
  python tools/kis_sync_broker_costs.py --start 20260701 --end 20260822
  python tools/kis_sync_broker_costs.py --days 30
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "tools") not in sys.path:
    sys.path.insert(0, str(ROOT / "tools"))

LOG_DIR = ROOT / "2_Logs"
LEDGER = LOG_DIR / "broker_realized_costs.csv"
REPORT = LOG_DIR / "broker_cost_rates_latest.json"
ENGINE_CFG = ROOT / "paper" / "paper_engine_config.json"

# 실측으로 확정된 필드명 (2026-08-22, kis_probe_period_trade_profit.py 로 역탐색)
F_DATE, F_CODE, F_NAME = "trad_dt", "pdno", "prdt_name"
F_BUY_QTY, F_BUY_AMT = "buy_qty", "buy_amt"
F_SELL_QTY, F_SELL_AMT = "sll_qty", "sll_amt"
F_PNL, F_PNL_RT, F_FEE, F_TAX = "rlzt_pfls", "pfls_rt", "fee", "tl_tax"

LEDGER_COLS = [
    "trad_dt", "code", "name", "buy_qty", "buy_amt", "sell_qty", "sell_amt",
    "realized_pnl", "pnl_rate", "fee_krw", "tax_krw",
    "fee_rate_implied", "tax_rate_implied", "synced_at", "tr_id",
]


def _num(value: Any, default: float = 0.0) -> float:
    txt = str(value if value is not None else "").replace(",", "").strip()
    if not txt:
        return default
    try:
        return float(txt)
    except Exception:
        return default


def _load_existing_keys() -> set:
    if not LEDGER.exists():
        return set()
    keys = set()
    try:
        with LEDGER.open("r", encoding="utf-8-sig", newline="") as fh:
            for row in csv.DictReader(fh):
                keys.add((row.get("trad_dt", ""), row.get("code", ""),
                          row.get("buy_qty", ""), row.get("sell_qty", "")))
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] 기존 원장 읽기 실패({exc}) - 중복 방지가 동작하지 않는다")
    return keys


def _append(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    new_file = not LEDGER.exists()
    with LEDGER.open("a", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=LEDGER_COLS)
        if new_file:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in LEDGER_COLS})
    return len(rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="", help="YYYYMMDD")
    ap.add_argument("--end", default="", help="YYYYMMDD")
    ap.add_argument("--days", type=int, default=7, help="start 미지정 시 최근 N일")
    ap.add_argument("--mock", action="store_true")
    ap.add_argument("--tolerance-bps", type=float, default=1.0,
                    help="설정 상수와 실측의 허용 오차(bps)")
    ap.add_argument("--once-per-day", action="store_true",
                    help="오늘 이미 돌았으면 즉시 종료. 인트라데이 루프에서 매 사이클 불러도 안전하다")
    ap.add_argument("--after-hhmm", type=int, default=0,
                    help="이 시각 이후에만 실행(HHMM). 실현손익은 장 마감 뒤 확정된다")
    args = ap.parse_args()

    # 하루 1회 가드. 브로커 API 를 60초마다 두드리지 않기 위한 것이다.
    stamp = LOG_DIR / "broker_cost_sync_stamp.txt"
    today_ymd = datetime.now().strftime("%Y%m%d")
    if args.once_per_day:
        if int(args.after_hhmm) > 0 and int(datetime.now().strftime("%H%M")) < int(args.after_hhmm):
            print(f"[SKIP] {args.after_hhmm} 이전이라 건너뛴다")
            return 0
        try:
            if stamp.exists() and stamp.read_text(encoding="utf-8").strip() == today_ymd:
                print(f"[SKIP] 오늘({today_ymd}) 이미 동기화했다")
                return 0
        except Exception:
            pass

    today = datetime.now()
    end = args.end or today.strftime("%Y%m%d")
    start = args.start or (today - timedelta(days=max(1, args.days))).strftime("%Y%m%d")

    from kis_order_client import KISOrderClient  # noqa: E402

    client = KISOrderClient.from_env(mock=bool(args.mock))
    try:
        rsp = client.inquire_period_trade_profit(start_ymd=start, end_ymd=end)
    except Exception as exc:  # noqa: BLE001
        print(f"[FAIL] 조회 실패: {type(exc).__name__}: {exc}")
        return 2

    rows = rsp.get("rows") or []
    summary = rsp.get("summary") or {}
    print(f"[OK] {start}~{end}  거래 {len(rows)}건  tr_id={rsp.get('tr_id')}")

    existing = _load_existing_keys()
    synced_at = datetime.now().isoformat(timespec="seconds")
    new_rows: List[Dict[str, Any]] = []
    for r in rows:
        buy_amt, sell_amt = _num(r.get(F_BUY_AMT)), _num(r.get(F_SELL_AMT))
        fee, tax = _num(r.get(F_FEE)), _num(r.get(F_TAX))
        turnover = buy_amt + sell_amt
        rec = {
            "trad_dt": str(r.get(F_DATE, "")).strip(),
            "code": str(r.get(F_CODE, "")).strip().zfill(6),
            "name": str(r.get(F_NAME, "")).strip(),
            "buy_qty": str(r.get(F_BUY_QTY, "")).strip(),
            "buy_amt": buy_amt,
            "sell_qty": str(r.get(F_SELL_QTY, "")).strip(),
            "sell_amt": sell_amt,
            "realized_pnl": _num(r.get(F_PNL)),
            "pnl_rate": _num(r.get(F_PNL_RT)),
            "fee_krw": fee,
            "tax_krw": tax,
            # 수수료는 양방향 거래대금 기준, 세금은 매도 기준(매수엔 안 붙는다)
            "fee_rate_implied": (fee / turnover) if turnover > 0 else "",
            "tax_rate_implied": (tax / sell_amt) if sell_amt > 0 else "",
            "synced_at": synced_at,
            "tr_id": rsp.get("tr_id", ""),
        }
        key = (rec["trad_dt"], rec["code"], rec["buy_qty"], rec["sell_qty"])
        if key in existing:
            continue
        new_rows.append(rec)

    added = _append(new_rows)
    print(f"[LEDGER] 신규 {added}건 추가 -> {LEDGER}")

    # ---- 설정 상수와 대조 -------------------------------------------------
    cfg = {}
    try:
        cfg = json.loads(ENGINE_CFG.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] 엔진 설정을 읽지 못했다: {exc}")

    cfg_fee = float(cfg.get("fee_pct", 0.0) or 0.0)
    cfg_tax = float(cfg.get("sell_tax_pct", 0.0) or 0.0)

    tot_fee = _num(summary.get("tot_fee"))
    tot_amt = _num(summary.get("tot_tr_amt"))
    sell_tax = _num(summary.get("sll_tltx_smtl"))
    sell_amt_tot = _num(summary.get("sll_tr_amt_smtl"))

    act_fee = (tot_fee / tot_amt) if tot_amt > 0 else None
    act_tax = (sell_tax / sell_amt_tot) if sell_amt_tot > 0 else None

    tol = float(args.tolerance_bps) / 10000.0
    mismatches: List[str] = []
    print("\n=== 설정 상수 vs 브로커 실청구")
    for label, actual, configured in (("수수료", act_fee, cfg_fee), ("거래세", act_tax, cfg_tax)):
        if actual is None:
            print(f"   {label}: 거래 없음 - 대조 불가")
            continue
        gap = actual - configured
        flag = ""
        if abs(gap) > tol:
            flag = "  <== 불일치"
            mismatches.append(
                f"{label} 설정 {configured:.5%} vs 실측 {actual:.5%} (차 {gap*10000:+.2f}bps)"
            )
        print(f"   {label}: 설정 {configured:.5%}  실측 {actual:.5%}  차 {gap*10000:+.2f}bps{flag}")

    if mismatches:
        print("\n" + "!" * 66)
        print("[MISMATCH] 브로커 실청구가 설정 상수와 다르다. 손익 계산이 틀어진다.")
        for m in mismatches:
            print("   " + m)
        print("   수수료 무료 혜택 종료 가능성부터 확인할 것 (PLANS 2026-08-22 56)")
        print("!" * 66)

    report = {
        "generated_at": synced_at,
        "range": {"start": start, "end": end},
        "tr_id": rsp.get("tr_id"),
        "trade_count": len(rows),
        "new_ledger_rows": added,
        "actual": {"fee_rate": act_fee, "tax_rate": act_tax,
                   "tot_fee_krw": tot_fee, "tot_turnover_krw": tot_amt,
                   "sell_tax_krw": sell_tax, "sell_turnover_krw": sell_amt_tot},
        "configured": {"fee_pct": cfg_fee, "sell_tax_pct": cfg_tax},
        "tolerance_bps": args.tolerance_bps,
        "mismatches": mismatches,
        "status": "MISMATCH" if mismatches else ("NO_TRADES" if act_tax is None else "MATCH"),
    }
    REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.once_per_day:
        # 불일치여도 스탬프는 찍는다. 안 찍으면 매 사이클 API 를 다시 두드린다.
        # 불일치 사실은 REPORT 와 rc=3 으로 남는다.
        try:
            stamp.write_text(today_ymd, encoding="utf-8")
        except Exception as exc:
            print(f"[WARN] 스탬프 기록 실패: {exc}")
    print(f"\n[REPORT] {REPORT}  status={report['status']}")
    # 불일치는 rc 로도 드러낸다. 배치가 이걸 잡는다.
    return 3 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
