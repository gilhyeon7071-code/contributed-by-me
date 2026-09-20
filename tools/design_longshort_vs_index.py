# -*- coding: utf-8 -*-
"""롱숏 구성을 실제로 굴려 지수와 대본다 (관점 B: 시장 초과).

2026-08-27 신규. (129) 에서 롱숏 스프레드가 5지평 전부 합격선을 넘는 것을 확인했으나,
**스프레드 숫자와 실제 수익곡선은 다르다.**
- 스프레드는 기간별 평균이고, 복리·변동성·낙폭이 안 보인다
- 비용이 **양쪽 다리 모두**에 붙는다
- 관점 B 의 잣대는 "통계적 유의" 가 아니라 **지수 초과**다

## 왜 D0 vs D9 인가 (십분위 선택을 안 한다)

(128) 에서 `vol_60` 의 최선 롱 다리는 D4(+0.43%p)였다. 그런데 **그 D4 는 같은 데이터로
고른 것**이라 그대로 쓰면 in-sample 선택이다. 여기서는 **양 끝(D0/D9)** 만 쓴다 -
"저변동을 사고 고변동을 판다" 는 사전에 말할 수 있는 형태이고, 고를 여지가 없다.

## 비용

왕복 0.358%(2026-08-26 호가로 독립 확인). 매 리밸런싱마다 **각 다리 전량 교체**로 본다.
숏 다리의 대차 수수료는 **넣지 않았다** - 그건 관점 D(실행 가능성)에서 실측할 값이다.
따라서 이 결과는 **롱숏에 후한 쪽**이다.

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


def perf(curve: pd.Series, name: str, per_year: float) -> dict:
    """per_year = 1년에 몇 구간인가. h=10 이면 24.7, h=60 이면 4.1.

    [2026-08-27] 처음에 연환산 계수를 252 로 하드코딩해서 변동성·샤프가
    sqrt(252/per_year) 배로 부풀었다(h=60 에서 샤프 11.87 이 나왔다 - 불가능한 값).
    구간 표집이면 구간수/년으로 연환산해야 한다.
    """
    x = curve.dropna()
    r = x.pct_change().dropna()
    yrs = len(x) / per_year
    tot = float(x.iloc[-1] / x.iloc[0])
    return {"name": name, "total": tot,
            "cagr": (tot ** (1 / yrs) - 1) * 100 if tot > 0 and yrs > 0 else np.nan,
            "vol": float(r.std() * np.sqrt(per_year) * 100),
            "mdd": float((x / x.cummax() - 1).min() * 100),
            "sharpe": float(r.mean() / r.std() * np.sqrt(per_year)) if r.std() > 0 else np.nan}


def main() -> int:
    ap = argparse.ArgumentParser(description="롱숏 vs 지수 (관점 B)")
    ap.add_argument("--h", type=int, default=10)
    ap.add_argument("--feature", default="vol_60", choices=["vol_60", "vol_20", "mom_5"])
    ap.add_argument("--min-value", type=float, default=1e9)
    ap.add_argument("--cost", type=float, default=0.00400, help="왕복 비용(다리당). 2026-09-10: 0.00358 -> 0.00400, 브로커 실측")
    ap.add_argument("--deciles", type=int, default=10)
    ap.add_argument("--start", default="20150101")
    ap.add_argument("--out", default=str(LOGS / "design" / "longshort_vs_index_latest.csv"))
    args = ap.parse_args()

    print("[관점 B] 롱숏 vs 지수   h=%d  축=%s  다리당 왕복비용 %.3f%%"
          % (args.h, args.feature, 100 * args.cost))
    print("  구성: D0(하위) 롱 / D9(상위) 숏, 십분위 선택 없음")
    print("  주의: 숏 대차 수수료 미포함 -> 롱숏에 후한 값")

    d = load_merged()
    d = d[d["date"] >= args.start]
    px = d.pivot_table(index="date", columns="code", values="close", aggfunc="last")
    val = d.pivot_table(index="date", columns="code", values="value", aggfunc="last")
    px.index = px.index.astype(str)
    val = val.reindex(index=px.index, columns=px.columns)

    r1 = px / px.shift(1) - 1.0
    r1c = r1.where((r1 <= CA_HI - 1) & (r1 >= CA_LO - 1))
    if args.feature == "vol_60":
        F = r1c.rolling(60, min_periods=45).std()
    elif args.feature == "vol_20":
        F = r1c.rolling(20, min_periods=15).std()
    else:
        F = px / px.shift(5) - 1.0

    bad = ~((r1 <= CA_HI - 1) & (r1 >= CA_LO - 1)) & r1.notna()
    fwd = px.shift(-args.h) / px - 1.0
    fwd = fwd.where(~bad.shift(-args.h).rolling(args.h, min_periods=1).max().astype(bool))
    trad = (val >= args.min_value) & px.notna() & fwd.notna()

    dates = list(px.index)
    reb = dates[120::args.h]
    rows = []
    for t in reb:
        m = trad.loc[t]
        x, y = F.loc[t][m], fwd.loc[t][m]
        ok = x.notna() & y.notna()
        if ok.sum() < 100:
            continue
        x, y = x[ok], y[ok]
        q = pd.qcut(x.rank(method="first"), args.deciles, labels=False, duplicates="drop")
        if q.nunique() < args.deciles:
            continue
        g = y.groupby(q).mean()
        rows.append({"date": t, "lo": g.iloc[0], "hi": g.iloc[-1], "uni": y.mean(), "n": int(ok.sum())})
    p = pd.DataFrame(rows)
    print("  리밸런싱 %d회 (%s ~ %s)" % (len(p), p["date"].iloc[0], p["date"].iloc[-1]))

    c = args.cost
    p["ls_gross"] = p["lo"] - p["hi"]                 # 롱 D0 + 숏 D9
    p["ls_net"] = p["ls_gross"] - 2 * c               # 양쪽 다리 비용
    p["lo_net"] = p["lo"] - c                         # 롱온리 D0
    p["uni_net"] = p["uni"]                           # 유니버스 매수보유(교체 없음 가정)

    ix = pd.read_csv(LOGS / "index_daily_history.csv", dtype=str, encoding="utf-8-sig")
    ix["close"] = pd.to_numeric(ix["close"], errors="coerce")
    kp = ix[ix["index_code"] == "0001"].set_index("date")["close"]
    kp.index = kp.index.astype(str)

    # 지수는 같은 리밸런싱 날짜 기준으로 구간수익을 만든다
    kdates = [t for t in p["date"] if t in kp.index]
    if len(kdates) < len(p) * 0.9:
        print("  [주의] 지수 이력이 %d/%d 구간만 덮는다" % (len(kdates), len(p)))
    # [2026-08-27] 지수를 리밸런싱 날짜에 맞출 때 **결측을 dropna 로 버리면 안 된다** -
    #   인접하지 않은 두 구간이 이어붙어 수익이 왜곡된다(실측: 같은 KOSPI 인데
    #   h=10 9.49% / h=20 13.71% 로 갈렸다. 참값은 11.35%).
    #   전 거래일 종가로 ffill 해서 구간을 끊지 않는다.
    kfull = kp.reindex(sorted(set(kp.index) | set(p["date"]))).ffill()
    kser = kfull.reindex(p["date"]).astype(float)
    miss = int(kser.isna().sum())
    if miss:
        print("  [주의] 지수 결측 %d구간 (ffill 로도 못 채운 앞쪽)" % miss)
    p["kospi"] = kser.pct_change().shift(-1).to_numpy()

    q = p.dropna(subset=["kospi"]).reset_index(drop=True)
    curves = {
        "롱숏 D0-D9 (비용후)": (1 + q["ls_net"]).cumprod(),
        "롱온리 D0 (비용후)": (1 + q["lo_net"]).cumprod(),
        "유니버스 동일가중": (1 + q["uni_net"]).cumprod(),
        "KOSPI": (1 + q["kospi"]).cumprod(),
    }
    per_year = 247.0 / args.h          # 1년에 몇 구간인가
    res = [perf(cv, name, per_year) for name, cv in curves.items()]

    print()
    print("%-22s %8s %9s %9s %9s %8s" % ("", "누적", "CAGR", "연변동성", "MDD", "샤프"))
    for r in res:
        print("%-22s %7.2f배 %8.2f%% %8.1f%% %8.1f%% %8.2f"
              % (r["name"], r["total"], r["cagr"], r["vol"], r["mdd"], r["sharpe"]))

    ls, ks = res[0], res[3]
    print()
    print("  롱숏 vs KOSPI:  CAGR %+.2f%%p   MDD %+.1f%%p   샤프 %+.2f"
          % (ls["cagr"] - ks["cagr"], ls["mdd"] - ks["mdd"], ls["sharpe"] - ks["sharpe"]))
    print("  구간별 승률: 롱숏 %.1f%%  (KOSPI 대비 우세 %.1f%%)"
          % (100 * (q["ls_net"] > 0).mean(), 100 * (q["ls_net"] > q["kospi"]).mean()))
    print("  [주의] MDD 는 %d거래일 간격 표집이라 구간 안의 낙폭을 못 본다 - "
          "지평이 길수록 실제보다 얕게 나온다" % args.h)

    out = pd.DataFrame({"date": q["date"]})
    for name, cv in curves.items():
        out[name] = cv.to_numpy()
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False, encoding="utf-8-sig")
    print("\n  wrote %s (%d행)" % (args.out, len(out)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
