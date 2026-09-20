# -*- coding: utf-8 -*-
"""절대 모멘텀(레짐 필터)을 지수에 얹어 검정한다 - 시계열 결정 축.

2026-08-27. (130) 에서 드러난 것 - **지금까지 세운 모든 시험이 횡단면("어떤 종목")이었고
시계열("들고 있을까 말까")은 한 번도 안 봤다.** 이 도구가 그 축이다.

## 검정할 주장

"KOSPI 는 순환형이라 폭락 국면에서 주식 비중을 0 으로 비우는 타임 트랙만 추가해도
CAGR 은 비약적으로 상승하고 MDD 는 1/3 이하로 줄어든다"

**정량적 주장이라 참/거짓이 갈린다.** 반증 경로가 있다.

## 반증 가능성 - 무엇이 나오면 틀린 것인가

```
MDD 가 1/3 로 안 준다                     -> 주장의 후반부 거짓
CAGR 이 매수보유보다 낮다                  -> 주장의 전반부 거짓
휩쏘(연 4회 이상 헛스위치)가 잦다            -> 박스권에서 비용만 나간다
룩백 길이를 바꾸면 결과가 뒤집힌다            -> 과적합. 한 값에서만 되는 것은 신호가 아니다
```
**마지막이 핵심이다.** 12개월 하나만 재고 좋다고 말하면 그건 고른 것이다.
그래서 **여러 룩백을 전부 낸다.**

## 비용

스위치 1회 = 전량 매매 = 왕복 0.358%. 진입/이탈 각각에 절반씩(편도 0.179%) 적용한다.
지수를 사고파는 것이므로 개별종목 스프레드가 아니라 ETF 수준이 맞으나,
**보수적으로 개별종목 기준 비용을 그대로 쓴다.**

읽기 전용이다.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(r"E:\1_Data")
LOGS = ROOT / "2_Logs"


def stats(eq: pd.Series, per_year: float = 247.0) -> dict:
    r = eq.pct_change().dropna()
    yrs = len(eq) / per_year
    tot = float(eq.iloc[-1] / eq.iloc[0])
    return {"total": tot, "cagr": (tot ** (1 / yrs) - 1) * 100,
            "vol": float(r.std() * np.sqrt(per_year) * 100),
            "mdd": float((eq / eq.cummax() - 1).min() * 100),
            "sharpe": float(r.mean() / r.std() * np.sqrt(per_year)) if r.std() > 0 else np.nan}


def main() -> int:
    ap = argparse.ArgumentParser(description="절대 모멘텀 레짐 필터 검정")
    ap.add_argument("--index", default="0001", help="0001 KOSPI / 1001 KOSDAQ / 2001 KOSPI200")
    ap.add_argument("--cost", type=float, default=0.00179, help="편도 비용(스위치 1회당)")
    ap.add_argument("--cash-yield", type=float, default=0.0, help="현금 연수익률(0=무이자, 보수적)")
    ap.add_argument("--out", default=str(LOGS / "design" / "abs_momentum_latest.csv"))
    args = ap.parse_args()

    ix = pd.read_csv(LOGS / "index_daily_history.csv", dtype=str, encoding="utf-8-sig")
    ix["close"] = pd.to_numeric(ix["close"], errors="coerce")
    s = ix[ix["index_code"] == args.index].sort_values("date")
    px = s.set_index("date")["close"].astype(float)
    px.index = px.index.astype(str)
    px = px.dropna()
    name = {"0001": "KOSPI", "1001": "KOSDAQ", "2001": "KOSPI200"}.get(args.index, args.index)
    print("[절대모멘텀] %s  %s ~ %s  %d거래일" % (name, px.index[0], px.index[-1], len(px)))
    print("  비용: 스위치 1회당 편도 %.3f%%   현금 수익률 연 %.1f%%"
          % (100 * args.cost, 100 * args.cash_yield))

    r = px.pct_change().fillna(0.0)
    cash_d = args.cash_yield / 247.0
    rows = []
    curves = {}

    bh = (1 + r).cumprod()
    st = stats(bh)
    rows.append({"rule": "매수보유", "lookback": "-", "switches": 0, "in_market_pct": 100.0, **st})
    curves["매수보유"] = bh

    for lb in (20, 60, 120, 200, 247):          # 1개월 ~ 12개월
        ma = px.rolling(lb, min_periods=lb).mean()
        sig = (px > ma).shift(1)                 # 전일 종가로 판단 -> 당일 반영 (선견 없음)
        sig = sig.fillna(False)
        sw = sig.astype(int).diff().abs().fillna(0)
        rr = r.where(sig, cash_d) - sw * args.cost
        eq = (1 + rr).cumprod()
        st = stats(eq)
        rows.append({"rule": "MA%d 위" % lb, "lookback": lb, "switches": int(sw.sum()),
                     "in_market_pct": 100 * sig.mean(), **st})
        curves["MA%d" % lb] = eq

    for lb in (60, 120, 247):                    # 절대수익률 룰
        pastret = px / px.shift(lb) - 1.0
        sig = (pastret > 0).shift(1).fillna(False)
        sw = sig.astype(int).diff().abs().fillna(0)
        rr = r.where(sig, cash_d) - sw * args.cost
        eq = (1 + rr).cumprod()
        st = stats(eq)
        rows.append({"rule": "%d일수익>0" % lb, "lookback": lb, "switches": int(sw.sum()),
                     "in_market_pct": 100 * sig.mean(), **st})
        curves["R%d" % lb] = eq

    res = pd.DataFrame(rows)
    print()
    print("%-14s %8s %9s %9s %9s %8s %8s %9s"
          % ("규칙", "누적", "CAGR", "연변동성", "MDD", "샤프", "스위치", "주식보유%"))
    for x in res.itertuples():
        print("%-14s %7.2f배 %8.2f%% %8.1f%% %8.1f%% %8.2f %8d %8.1f%%"
              % (x.rule, x.total, x.cagr, x.vol, x.mdd, x.sharpe, x.switches, x.in_market_pct))

    bhr = res.iloc[0]
    print()
    print("  매수보유 대비 (주장: CAGR 상승 + MDD 1/3 이하)")
    for x in res.iloc[1:].itertuples():
        mdd_ratio = x.mdd / bhr["mdd"] if bhr["mdd"] else np.nan
        ok_cagr = "O" if x.cagr > bhr["cagr"] else "X"
        ok_mdd = "O" if mdd_ratio <= 1 / 3 else "X"
        print("    %-14s CAGR %+6.2f%%p [%s]   MDD %5.1f%% -> %5.1f%% (%.2f배) [%s]   연 스위치 %.1f회"
              % (x.rule, x.cagr - bhr["cagr"], ok_cagr, bhr["mdd"], x.mdd, mdd_ratio, ok_mdd,
                 x.switches / (len(px) / 247.0)))

    out = pd.DataFrame({"date": px.index})
    for k, v in curves.items():
        out[k] = v.to_numpy()
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False, encoding="utf-8-sig")
    res.to_csv(str(args.out).replace(".csv", "_summary.csv"), index=False, encoding="utf-8-sig")
    print("\n  wrote %s" % args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
