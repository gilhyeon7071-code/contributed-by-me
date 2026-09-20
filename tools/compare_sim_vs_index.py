# -*- coding: utf-8 -*-
"""리밸런싱 시뮬레이션을 지수와 대조한다.

2026-08-27 신규. (112) 에서 시뮬을 재실행해 CAGR 5.07% / MDD -51.5% 를 얻었고
"벤치마크를 +36%p 이겼다" 고 했는데, **그 벤치마크는 지수가 아니라 동일가중 매수보유**였다.
그건 "저변동 배제 필터가 값을 하는가" 를 재는 잣대이지 "시장을 이기는가" 가 아니다.

사용자 질문("오늘 2% 올랐는데 왜 우리는")에서 드러난 것도 같은 축이다 -
지수는 시총가중, 이 장부는 동일가중이라 대형주 장세에서 구조적으로 뒤진다(2026-08-26 실측).

**굴릴 이유가 있는지 없는지는 지수와 대봐야 나온다.** 그게 이 도구다.

지수는 (120) 에서 배선하고 (127) 에서 페이지네이션으로 11.4년을 채웠다.
`2_Logs/index_daily_history.csv` KOSPI/KOSDAQ 각 2,890행.

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


def stats(idx: pd.Series, name: str, days_per_year: float = 247.0) -> dict:
    x = idx.dropna()
    if len(x) < 3:
        return {}
    r = x.pct_change().dropna()
    yrs = len(x) / days_per_year
    total = float(x.iloc[-1] / x.iloc[0])
    return {
        "name": name,
        "n": len(x),
        "total": total,
        "cagr": (total ** (1 / yrs) - 1) * 100 if yrs > 0 else np.nan,
        "vol": float(r.std() * np.sqrt(252) * 100),
        "mdd": float((x / x.cummax() - 1).min() * 100),
        "sharpe": float(r.mean() / r.std() * np.sqrt(252)) if r.std() > 0 else np.nan,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="시뮬 vs 지수")
    ap.add_argument("--sim", default=str(LOGS / "rebalance" / "sim_equity.csv"))
    ap.add_argument("--index", default=str(LOGS / "index_daily_history.csv"))
    ap.add_argument("--out", default=str(LOGS / "rebalance" / "sim_vs_index.csv"))
    args = ap.parse_args()

    sim = pd.read_csv(args.sim, dtype={"date": str})
    sim = sim[["date", "equity"]].dropna().sort_values("date")

    ix = pd.read_csv(args.index, dtype=str, encoding="utf-8-sig")
    ix["close"] = pd.to_numeric(ix["close"], errors="coerce")
    piv = ix.pivot_table(index="date", columns="index_code", values="close", aggfunc="last")
    piv.index = piv.index.astype(str)

    # 지수 이력이 있는 구간으로 자른다. 시뮬이 더 길면 그 부분은 비교 불가다.
    common = sorted(set(sim["date"]) & set(piv.index))
    if len(common) < 100:
        print("[STOP] 공통 거래일이 %d일뿐이다" % len(common))
        return 2
    lo, hi = common[0], common[-1]
    sim_c = sim[sim["date"].between(lo, hi)].set_index("date")["equity"]
    sim_c = sim_c.reindex(common).ffill()
    print("[대조] 공통 구간 %s ~ %s  (%d 거래일, 약 %.1f년)"
          % (lo, hi, len(common), len(common) / 247.0))
    dropped = len(sim) - len(sim_c)
    if dropped > 0:
        print("  시뮬 %d일 중 %d일은 지수 이력이 없어 제외" % (len(sim), dropped))

    rows = [stats(sim_c / sim_c.iloc[0], "전략(시뮬)")]
    for code, name in (("0001", "KOSPI"), ("2001", "KOSDAQ")):
        if code in piv.columns:
            s = piv[code].reindex(common).ffill()
            rows.append(stats(s / s.iloc[0], name))
    # 코스피/코스닥 단순평균 - 이 장부가 두 시장에 걸쳐 있으므로 참고용
    if {"0001", "2001"}.issubset(piv.columns):
        a = (piv["0001"].reindex(common).ffill() / piv["0001"].reindex(common).ffill().iloc[0])
        b = (piv["2001"].reindex(common).ffill() / piv["2001"].reindex(common).ffill().iloc[0])
        rows.append(stats((a + b) / 2, "KOSPI/KOSDAQ 평균"))

    print()
    print("%-20s %8s %9s %9s %9s %8s" % ("", "누적", "CAGR", "연변동성", "MDD", "샤프"))
    for r in rows:
        if not r:
            continue
        print("%-20s %7.2f배 %8.2f%% %8.1f%% %8.1f%% %8.2f"
              % (r["name"], r["total"], r["cagr"], r["vol"], r["mdd"], r["sharpe"]))

    base = rows[0]
    print()
    print("전략 대비:")
    for r in rows[1:]:
        if not r:
            continue
        print("  vs %-18s 누적 %+.2f배  CAGR %+.2f%%p  MDD %+.1f%%p  샤프 %+.2f"
              % (r["name"], base["total"] - r["total"], base["cagr"] - r["cagr"],
                 base["mdd"] - r["mdd"], base["sharpe"] - r["sharpe"]))

    out = pd.DataFrame({"date": common, "strategy": (sim_c / sim_c.iloc[0]).to_numpy()})
    for code, name in (("0001", "KOSPI"), ("2001", "KOSDAQ")):
        if code in piv.columns:
            s = piv[code].reindex(common).ffill()
            out[name] = (s / s.iloc[0]).to_numpy()
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False, encoding="utf-8-sig")
    print("\n  wrote %s (%d행)" % (args.out, len(out)))

    # 연도별로도 본다 - 전 구간 하나로는 어느 국면에서 갈리는지 안 보인다
    y = out.copy()
    y["yr"] = y["date"].str[:4]
    print()
    print("연도별 수익률")
    print("  %-6s %10s %10s %10s" % ("연도", "전략", "KOSPI", "차이"))
    for yr, g in y.groupby("yr"):
        if len(g) < 20:
            continue
        sr = g["strategy"].iloc[-1] / g["strategy"].iloc[0] - 1
        kr = g["KOSPI"].iloc[-1] / g["KOSPI"].iloc[0] - 1 if "KOSPI" in g else np.nan
        mark = "" if sr >= kr else "   <"
        print("  %-6s %9.1f%% %9.1f%% %9.1f%%p%s" % (yr, 100 * sr, 100 * kr, 100 * (sr - kr), mark))
    return 0


if __name__ == "__main__":
    sys.exit(main())
