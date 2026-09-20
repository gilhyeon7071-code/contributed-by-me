# -*- coding: utf-8 -*-
"""보통주-우선주 페어를 **실제 포트폴리오로 굴려** 지수와 대본다 - 페어를 닫는 도구.

2026-08-27. 앞선 `design_pref_pair_arb.py` 는 **건당 수익의 평균**을 냈고
"동시 5쌍 운용 가정 -> 연 X%" 로 거칠게 연환산했다. 그건 추정이지 측정이 아니다.
- 슬롯이 비어야 진입할 수 있는 제약이 반영 안 됐다
- 복리·변동성·낙폭이 안 보인다
- 자본이 놀고 있는 기간(신호 없음)이 무시됐다

여기서는 **날짜를 하루씩 밟으며 슬롯을 채우고 비우고, 매일 평가**한다.

## 두 판본을 같이 낸다

```
LS  롱숏      싼 다리 롱 + 비싼 다리 숏.  시장중립(베타 0). **공매도 필요**
LO  롱온리    싼 다리만 산다.             공매도 불필요, 시장 노출 있음
              z<0(우선주 쌈) -> 우선주 매수 / z>0(우선주 비쌈) -> 보통주 매수
```
**LO 는 공매도가 막혀도 실행 가능한 유일한 형태**다. 한국은 전면 금지 구간이 반복됐고
(2020~2021, 2023~2024 등) 개인 대주는 종목이 제한적이다. 그 제약을 우회한다.

## 비용

진입·청산 각각 편도 0.179%. LS 는 다리가 둘이라 라운드당 0.716%, LO 는 0.358%.
슬롯이 비어 있는 동안의 현금 수익률은 0 으로 둔다(보수적).

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


def stats(eq: pd.Series, per_year: float = 247.0) -> dict:
    r = eq.pct_change().dropna()
    yrs = len(eq) / per_year
    tot = float(eq.iloc[-1] / eq.iloc[0])
    return {"total": tot, "cagr": (tot ** (1 / yrs) - 1) * 100 if tot > 0 else np.nan,
            "vol": float(r.std() * np.sqrt(per_year) * 100),
            "mdd": float((eq / eq.cummax() - 1).min() * 100),
            "sharpe": float(r.mean() / r.std() * np.sqrt(per_year)) if r.std() > 0 else np.nan}


def main() -> int:
    ap = argparse.ArgumentParser(description="페어 포트폴리오 (LS/LO) vs 지수")
    ap.add_argument("--lookback", type=int, default=120)
    ap.add_argument("--z-in", type=float, default=2.0)
    ap.add_argument("--z-out", type=float, default=0.5)
    ap.add_argument("--h-max", type=int, default=60)
    ap.add_argument("--slots", type=int, default=5)
    ap.add_argument("--min-value", type=float, default=3e8)
    ap.add_argument("--cost", type=float, default=0.00179, help="편도")
    ap.add_argument("--start", default="20150101")
    ap.add_argument("--out", default=str(LOGS / "design" / "pref_pair_portfolio_latest.csv"))
    args = ap.parse_args()

    print("[페어 포트폴리오] lb=%d z_in=%.1f z_out=%.1f h_max=%d 슬롯 %d개  편도비용 %.3f%%"
          % (args.lookback, args.z_in, args.z_out, args.h_max, args.slots, 100 * args.cost))

    d = load_merged()
    d = d[d["date"] >= args.start]
    px = d.pivot_table(index="date", columns="code", values="close", aggfunc="last")
    val = d.pivot_table(index="date", columns="code", values="value", aggfunc="last")
    px.index = px.index.astype(str)
    val = val.reindex(index=px.index, columns=px.columns)
    r1raw = px / px.shift(1) - 1.0
    good = (r1raw <= CA_HI - 1) & (r1raw >= CA_LO - 1)
    okpx = px.where(good | r1raw.isna())
    ret = okpx.pct_change()

    base = {}
    for c in px.columns:
        base.setdefault(str(c)[:5], []).append(str(c))
    pairs = []
    for k, v in base.items():
        if len(v) < 2:
            continue
        v = sorted(v)
        for pref in v[1:]:
            com = v[0]
            m = okpx[com].notna() & okpx[pref].notna() & (val[com] >= args.min_value) & (val[pref] >= args.min_value)
            if m.sum() < 500:
                continue
            pairs.append((com, pref, m))
    print("  사용 쌍 %d개" % len(pairs))

    Z = {}
    for com, pref, m in pairs:
        ratio = (okpx[pref] / okpx[com]).where(m)
        mu = ratio.rolling(args.lookback, min_periods=args.lookback).mean()
        sd = ratio.rolling(args.lookback, min_periods=args.lookback).std()
        Z[(com, pref)] = ((ratio - mu) / sd)

    dates = list(px.index)
    results = {}
    for mode in ("LS", "LO"):
        eq = 1.0
        curve, open_pos = [], {}       # key -> dict(dir, entry_i)
        n_trades = 0
        for i in range(args.lookback, len(dates)):
            t = dates[i]
            # 1) 보유분 손익 (슬롯당 자본 = eq/슬롯수, 미사용 슬롯은 현금)
            day = 0.0
            for key, p in list(open_pos.items()):
                com, pref = key
                rc, rp = ret.at[t, com], ret.at[t, pref]
                if pd.isna(rc) or pd.isna(rp):
                    continue
                if mode == "LS":
                    leg = (rp - rc) * p["dir"]
                else:
                    leg = rp if p["dir"] > 0 else rc      # 싼 쪽만 보유
                day += leg / args.slots
            # 2) 청산
            for key, p in list(open_pos.items()):
                z = Z[key].iloc[i]
                if pd.isna(z):
                    continue
                if abs(z) <= args.z_out or (i - p["entry_i"]) >= args.h_max:
                    day -= (args.cost * (2 if mode == "LS" else 1)) / args.slots
                    del open_pos[key]
            # 3) 진입 (|z| 큰 순서)
            if len(open_pos) < args.slots:
                cand = []
                for key in Z:
                    if key in open_pos:
                        continue
                    z = Z[key].iloc[i]
                    if pd.notna(z) and abs(z) >= args.z_in:
                        cand.append((abs(z), key, 1 if z < 0 else -1))
                cand.sort(reverse=True)
                for _, key, dr in cand:
                    if len(open_pos) >= args.slots:
                        break
                    open_pos[key] = {"dir": dr, "entry_i": i}
                    day -= (args.cost * (2 if mode == "LS" else 1)) / args.slots
                    n_trades += 1
            eq *= (1 + day)
            curve.append({"date": t, "eq": eq, "open": len(open_pos)})
        c = pd.DataFrame(curve).set_index("date")["eq"]
        results[mode] = (c, n_trades, pd.DataFrame(curve)["open"].mean())

    ix = pd.read_csv(LOGS / "index_daily_history.csv", dtype=str, encoding="utf-8-sig")
    ix["close"] = pd.to_numeric(ix["close"], errors="coerce")
    kp = ix[ix["index_code"] == "0001"].set_index("date")["close"].astype(float)
    kp.index = kp.index.astype(str)
    common = [t for t in results["LS"][0].index if t in kp.index]
    k = kp.reindex(common); k = k / k.iloc[0]

    print()
    print("%-20s %8s %9s %9s %9s %8s %8s %9s"
          % ("", "누적", "CAGR", "연변동성", "MDD", "샤프", "거래수", "평균보유쌍"))
    rows = {}
    for mode, label in (("LS", "롱숏(공매도 필요)"), ("LO", "롱온리(공매도 불필요)")):
        c, nt, avgopen = results[mode]
        c = c.reindex(common)
        s = stats(c)
        rows[mode] = s
        print("%-20s %7.2f배 %8.2f%% %8.1f%% %8.1f%% %8.2f %8d %8.1f"
              % (label, s["total"], s["cagr"], s["vol"], s["mdd"], s["sharpe"], nt, avgopen))
    sk = stats(k)
    rows["KOSPI"] = sk
    print("%-20s %7.2f배 %8.2f%% %8.1f%% %8.1f%% %8.2f %8s %8s"
          % ("KOSPI", sk["total"], sk["cagr"], sk["vol"], sk["mdd"], sk["sharpe"], "-", "-"))

    print()
    for mode, label in (("LS", "롱숏"), ("LO", "롱온리")):
        s = rows[mode]
        print("  %-6s vs KOSPI:  CAGR %+6.2f%%p   MDD %+5.1f%%p   샤프 %+.2f"
              % (label, s["cagr"] - sk["cagr"], s["mdd"] - sk["mdd"], s["sharpe"] - sk["sharpe"]))

    out = pd.DataFrame({"date": common})
    out["롱숏"] = results["LS"][0].reindex(common).to_numpy()
    out["롱온리"] = results["LO"][0].reindex(common).to_numpy()
    out["KOSPI"] = k.to_numpy()
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False, encoding="utf-8-sig")
    print("\n  wrote %s (%d행)" % (args.out, len(out)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
