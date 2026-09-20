# -*- coding: utf-8 -*-
"""보통주-우선주 괴리율 페어 트레이딩을 검정한다 (대안3).

2026-08-27. (130) 이후 브레인스토밍에서 나온 대안 중 **지금 데이터로 검증 가능한 두 번째**.
지주사-자회사는 지배구조 매핑이 없어 못 하지만, **보통주-우선주는 종목코드로 쌍을 만들 수 있다**
(앞 5자리 동일, 우선주는 끝자리 5/7/9 등). 패널에서 121쌍 후보가 나왔다.

## 왜 이게 다른가

지금까지 잰 것은 전부 **횡단면 랭킹**이었다(어떤 종목이 더 오를까).
페어는 **두 자산의 구조적 관계**를 쓴다 - 같은 기업의 두 주식이므로
괴리율이 벌어지면 좁혀지는 힘이 있다. 시장 요인은 구조적으로 상쇄된다(베타 0).

## 검정 설계 (결과 보기 전에 적는다)

```
쌍          앞5자리 동일 + 둘 다 거래대금 하한 통과 + 관측일 500일 이상
괴리율      ratio = 우선주가 / 보통주가.  z = (ratio - MA_lb) / SD_lb
진입        |z| >= z_in  (싼 쪽 롱, 비싼 쪽 숏)
청산        |z| <= z_out 또는 보유 h_max 일 초과
비용        진입·청산 각각 양쪽 다리 -> 왕복 0.358% x 2다리 = 0.716% / 라운드
합격선      비용 뺀 연환산이 KOSPI(11.34%)를 넘는가
```

## 반증 경로

```
비용 뺀 수익이 음수                    -> 괴리가 안 좁혀지거나 비용이 먹는다
z_in / lb 를 바꾸면 결과가 뒤집힌다      -> 과적합
거래 가능 쌍이 너무 적다                -> 자본을 못 담는다(실행 불가)
숏 가능성 미검증                       -> 우선주/보통주 공매도 제약은 별도 확인 필요(관점 D)
```

읽기 전용이다.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(r"E:\1_Data")
sys.path.insert(0, str(ROOT / "tools"))
from load_merged_panel import load_merged      # noqa: E402

LOGS = ROOT / "2_Logs"
CA_HI, CA_LO = 1.305, 0.695


def main() -> int:
    ap = argparse.ArgumentParser(description="보통주-우선주 페어 차익거래")
    ap.add_argument("--lookback", type=int, default=120)
    ap.add_argument("--z-in", type=float, default=2.0)
    ap.add_argument("--z-out", type=float, default=0.5)
    ap.add_argument("--h-max", type=int, default=60)
    ap.add_argument("--min-value", type=float, default=3e8, help="양쪽 다리 거래대금 하한")
    ap.add_argument("--cost", type=float, default=0.00400, help="다리당 왕복. 2026-09-10: 0.00358 -> 0.00400, 브로커 실측")
    ap.add_argument("--start", default="20150101")
    ap.add_argument("--out", default=str(LOGS / "design" / "pref_pair_latest.csv"))
    args = ap.parse_args()

    print("[페어] 보통주-우선주  lb=%d  진입 |z|>=%.1f  청산 |z|<=%.1f  최대보유 %d일"
          % (args.lookback, args.z_in, args.z_out, args.h_max))
    print("  비용: 라운드당 %.3f%% (양쪽 다리 왕복)" % (100 * 2 * args.cost))

    d = load_merged()
    d = d[d["date"] >= args.start]
    px = d.pivot_table(index="date", columns="code", values="close", aggfunc="last")
    val = d.pivot_table(index="date", columns="code", values="value", aggfunc="last")
    px.index = px.index.astype(str)
    val = val.reindex(index=px.index, columns=px.columns)

    # 기업행위/결함 구간의 가격은 쓰지 않는다
    r1 = px / px.shift(1) - 1.0
    okpx = px.where(((r1 <= CA_HI - 1) & (r1 >= CA_LO - 1)) | r1.isna())

    base = {}
    for c in px.columns:
        base.setdefault(str(c)[:5], []).append(str(c))
    pairs = []
    for k, v in base.items():
        if len(v) < 2:
            continue
        v = sorted(v)
        com = v[0]                       # 끝자리 0 = 보통주
        for pref in v[1:]:
            pairs.append((com, pref))
    print("  쌍 후보 %d개" % len(pairs))

    trades = []
    used = 0
    for com, pref in pairs:
        a, b = okpx[com], okpx[pref]
        m = a.notna() & b.notna() & (val[com] >= args.min_value) & (val[pref] >= args.min_value)
        if m.sum() < 500:
            continue
        used += 1
        ratio = (b / a).where(m)
        mu = ratio.rolling(args.lookback, min_periods=args.lookback).mean()
        sd = ratio.rolling(args.lookback, min_periods=args.lookback).std()
        z = (ratio - mu) / sd
        idx = list(px.index)
        pos = 0          # +1 = 우선주 롱/보통주 숏, -1 = 반대
        ent_i = -1
        ra, rb = a.pct_change(), b.pct_change()
        for i in range(args.lookback, len(idx)):
            t = idx[i]
            zz = z.iloc[i]
            if not m.iloc[i] or pd.isna(zz):
                continue
            if pos == 0:
                if zz <= -args.z_in:
                    pos, ent_i = +1, i        # 우선주가 싸다 -> 우선주 롱
                elif zz >= args.z_in:
                    pos, ent_i = -1, i
            else:
                held = i - ent_i
                if abs(zz) <= args.z_out or held >= args.h_max:
                    seg = slice(ent_i + 1, i + 1)
                    leg = (rb.iloc[seg] - ra.iloc[seg]) * pos
                    if leg.notna().all() and len(leg):
                        gross = float((1 + leg).prod() - 1)
                        trades.append({"pair": "%s/%s" % (com, pref), "entry": idx[ent_i],
                                       "exit": t, "held": held, "z_in": float(z.iloc[ent_i]),
                                       "gross": gross, "net": gross - 2 * args.cost})
                    pos, ent_i = 0, -1

    if not trades:
        print("  [STOP] 거래가 하나도 없다")
        return 2
    T = pd.DataFrame(trades)
    print("  실제 사용 쌍 %d개 / 거래 %d건" % (used, len(T)))
    print()
    yrs = (pd.to_datetime(T["exit"].max()) - pd.to_datetime(T["entry"].min())).days / 365.25
    print("  기간 %s ~ %s (%.1f년)   연 %.1f건" % (T["entry"].min(), T["exit"].max(), yrs, len(T) / yrs))
    print("  건당 총수익 평균 %+.2f%%  중앙 %+.2f%%   승률 %.1f%%"
          % (100 * T["gross"].mean(), 100 * T["gross"].median(), 100 * (T["gross"] > 0).mean()))
    print("  건당 순수익 평균 %+.2f%%  중앙 %+.2f%%   승률 %.1f%%"
          % (100 * T["net"].mean(), 100 * T["net"].median(), 100 * (T["net"] > 0).mean()))
    print("  평균 보유 %.0f일  (최대 %d일 제한)" % (T["held"].mean(), args.h_max))

    se = T["net"].std(ddof=1) / np.sqrt(len(T))
    tstat = T["net"].mean() / se if se else np.nan
    print("  순수익 t = %.2f  (건 단위 - 같은 날 여러 쌍이 겹치면 과대평가)" % tstat)

    # 자본 회전 기준 연환산: 동시에 N개 쌍을 굴린다고 보면
    for n_slots in (5, 10, 20):
        cap_ret = T["net"].mean() * (len(T) / yrs) / n_slots
        print("    동시 %2d쌍 운용 가정 -> 연 %+.1f%%   (KOSPI 11.34%% 대비 %+.1f%%p)"
              % (n_slots, 100 * cap_ret, 100 * cap_ret - 11.34))

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    T.to_csv(args.out, index=False, encoding="utf-8-sig")
    print("\n  wrote %s (%d행)" % (args.out, len(T)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
