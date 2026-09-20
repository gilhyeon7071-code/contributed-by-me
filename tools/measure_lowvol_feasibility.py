# -*- coding: utf-8 -*-
"""저변동성 포트폴리오를 **실제로 채우고 굴릴 수 있는지** 잰다. (알파 주장 아님)

## 왜 만드나

2026-09-10 H007 로 `vol_20` 저변동성 단조성이 봉인 구간에서 부분 재현됐고,
집행 비용도 실측(왕복 0.343%)했다. 남은 질문은 `feedback_three_verification_questions`
의 두 번째다 - **"매매가 되느냐"**.

## 먼저 짚을 불일치

H007 이 잰 것은 "D0~D3 평균" 인데 그것은 **십분위마다 50종목씩 뽑은 네 바구니의 평균**이다.
실제로 살 수 있는 것은 하나의 포트폴리오다.
**표본 외 증거는 "십분위 단조 구조" 에 대한 것이지 특정 50종목 구성에 대한 것이 아니다.**

그래서 이 도구는 알파를 재지 않는다. 재는 것은 사실뿐이다:

```
1. 풀 크기      매 리밸런싱 시점에 D0~D3 에 몇 종목이 있나. 50을 채울 수 있나
2. 회전율       한 번 리밸런싱에 몇 종목이 바뀌나
                -> 비용 계산의 "연 12.35회전" 가정이 맞는지. 덜 바뀌면 비용이 준다
3. 존속         고른 종목이 보유 20일 동안 살아 있나 (상폐·거래정지·유동성 이탈)
4. 유동성 지속  매수 시점에 20억이던 종목이 매도 시점에도 20억인가
```

전부 패널로 재는 사실이라 어느 구간에서 재도 된다(홀드아웃 소진과 무관).

## 읽기 전용

주문을 넣지 않는다. 파일도 지정한 출력만 쓴다.

[2026-09-10] 신설.
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tools"))
from design_signal_inventory import load_merged, CA_HI, CA_LO  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="저변동성 포트 구성 가능성 (알파 아님)")
    ap.add_argument("--start", default="2015-01-01")
    ap.add_argument("--end", default=None)
    ap.add_argument("--min-value", type=float, default=2e9)
    ap.add_argument("--vol-win", type=int, default=20)
    ap.add_argument("--hold", type=int, default=20)
    ap.add_argument("--positions", type=int, default=50)
    ap.add_argument("--pool-deciles", type=int, default=4,
                    help="상위 몇 개 십분위를 풀로 쓰나. 4 = D0~D3")
    ap.add_argument("--capital", type=float, default=100_000_000)
    ap.add_argument("--out", default="2_Logs/design/lowvol_feasibility_latest.csv")
    a = ap.parse_args()

    d = load_merged()
    d = d[d["date"] >= a.start.replace("-", "")]
    if a.end:
        d = d[d["date"] <= a.end.replace("-", "")]
    px = d.pivot_table(index="date", columns="code", values="close", aggfunc="last")
    val = d.pivot_table(index="date", columns="code", values="value", aggfunc="last")
    px.index = px.index.astype(str)
    val = val.reindex(index=px.index, columns=px.columns)
    print("패널 %d거래일 x %d종목  (%s ~ %s)" % (px.shape[0], px.shape[1], px.index[0], px.index[-1]))

    r1 = px / px.shift(1) - 1.0
    r1 = r1.where((r1 <= CA_HI - 1) & (r1 >= CA_LO - 1))
    vol = r1.rolling(a.vol_win, min_periods=max(5, a.vol_win * 3 // 4)).std()
    tradable = (val >= a.min_value) & px.notna() & vol.notna()

    dates = list(px.index)
    reb = dates[120::a.hold]
    print("리밸런싱 시점 %d개 (%d거래일마다)" % (len(reb), a.hold))
    print("풀 = vol_%d 하위 %d십분위, 그중 vol 오름차순 상위 %d종목"
          % (a.vol_win, a.pool_deciles, a.positions))
    print()

    rows = []
    prev: set[str] = set()
    for i, t in enumerate(reb):
        m = tradable.loc[t]
        x = vol.loc[t][m]
        if len(x) < 100:
            continue
        n_tradable = int(len(x))
        cut = int(np.ceil(n_tradable * a.pool_deciles / 10.0))
        pool = x.nsmallest(cut)
        pick = list(pool.nsmallest(min(a.positions, len(pool))).index)
        cur = set(pick)

        # 존속: 보유 기간 끝에 가격이 있나 / 그때도 유동성이 있나
        j = dates.index(t) + a.hold
        alive = liq_ok = np.nan
        if j < len(dates):
            t2 = dates[j]
            alive = float(px.loc[t2, pick].notna().mean())
            liq_ok = float((val.loc[t2, pick] >= a.min_value).mean())

        turn = np.nan if not prev else len(cur - prev) / max(1, len(cur))
        rows.append({
            "date": t, "n_tradable": n_tradable, "pool_size": int(len(pool)),
            "filled": len(pick), "short_of_target": max(0, a.positions - len(pick)),
            "turnover": turn, "alive_at_exit": alive, "liquid_at_exit": liq_ok,
            "slot_krw": a.capital / a.positions,
        })
        prev = cur

    res = pd.DataFrame(rows)
    if res.empty:
        print("[STOP] 리밸런싱 시점이 없어요")
        return 2

    print("=== 1. 풀 크기 / 채움 ===")
    print("  거래가능 종목  중앙 %d (최소 %d / 최대 %d)"
          % (res.n_tradable.median(), res.n_tradable.min(), res.n_tradable.max()))
    print("  풀(D0~D%d) 크기 중앙 %d (최소 %d)"
          % (a.pool_deciles - 1, res.pool_size.median(), res.pool_size.min()))
    short = int((res.short_of_target > 0).sum())
    print("  목표 %d종목을 못 채운 시점  **%d회 / %d회** (%.1f%%)"
          % (a.positions, short, len(res), short / len(res) * 100))
    if short:
        w = res[res.short_of_target > 0]
        print("     최악 %s 에 %d종목 부족" % (w.loc[w.short_of_target.idxmax(), "date"],
                                          int(w.short_of_target.max())))

    print()
    print("=== 2. 회전율 ===")
    tv = res.turnover.dropna()
    print("  한 번 리밸런싱에 바뀌는 비율  중앙 %.1f%% / 평균 %.1f%% (최소 %.0f%% 최대 %.0f%%)"
          % (tv.median() * 100, tv.mean() * 100, tv.min() * 100, tv.max() * 100))
    eff = 247.0 / a.hold * tv.mean()
    print("  -> 실효 연 회전  %.2f회  (전량 교체 가정은 %.2f회)"
          % (eff, 247.0 / a.hold))
    for rt in (0.343, 0.400):
        print("     왕복 %.3f%% 기준 연 비용  실효 **%.2f%%**  (가정으로는 %.2f%%)"
              % (rt, rt * eff, rt * 247.0 / a.hold))

    print()
    print("=== 3. 존속 / 유동성 지속 (보유 %d거래일) ===" % a.hold)
    print("  매도 시점에 가격이 있는 비율   중앙 %.2f%% (최소 %.2f%%)"
          % (res.alive_at_exit.median() * 100, res.alive_at_exit.min() * 100))
    print("  매도 시점에도 %.0f억 이상인 비율 중앙 %.1f%% (최소 %.1f%%)"
          % (a.min_value / 1e8, res.liquid_at_exit.median() * 100, res.liquid_at_exit.min() * 100))

    out = ROOT / a.out
    out.parent.mkdir(parents=True, exist_ok=True)
    res.to_csv(out, index=False, encoding="utf-8-sig")
    stamp = out.with_name(out.name.replace("_latest", "_%s" % dt.datetime.now().strftime("%Y%m%d_%H%M%S")))
    res.to_csv(stamp, index=False, encoding="utf-8-sig")
    print()
    print("  wrote %s" % out)
    print()
    print("  주의: 이 도구는 **알파를 재지 않아요.** 구성 가능성만 봐요.")
    print("        H007 의 표본 외 증거는 '십분위 단조 구조' 에 대한 것이지")
    print("        여기서 만든 50종목 구성에 대한 것이 아니에요.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
