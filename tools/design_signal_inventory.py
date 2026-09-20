# -*- coding: utf-8 -*-
"""신호 재고 조사 - **무엇을 만들지 정하기 위한 설계 입력**을 만든다.

2026-08-27 신규. 검증 도구가 아니다. 검증은 이미 만든 것을 판정하는 일이고,
이 시스템이 지금까지 그것만 해왔다 - 만들고, 돌리고, 사후 부검했다(PLANS 127).

**여기서는 아무것도 만들지 않는다. 재료를 잰다.**
어떤 축도 합격선을 못 넘으면 그 축들로 만든 어떤 구성도 못 넘는다.
그러면 "이 데이터로는 안 된다" 가 유효한 결론이 되고, 헛되이 만들지 않게 된다.

## 설계 규격 (결과를 보기 전에 적는다)

```
합격선(MES)   h10 상위십분위 알파 >= 1.0%p        D트랙이 정한 값
검정력        MDE > MES 이면 그 축은 "판정 불가" 로 적는다. 통과로도 실패로도 안 센다
표집          h10 이면 10거래일 간격 비중첩. 겹침 보정 배수를 쓰지 않고 겹침 자체를 없앤다
기준          같은 날 유니버스 동일가중 (횡단면이므로 시장 요인이 상쇄된다)
정제          연속일 종가비 ±30.5% 밖은 기업행위/결함으로 보고 그 종목-날짜를 뺀다
유니버스      거래대금 >= 10억 (실제로 살 수 있는 것만)
```

## 왜 재무·뉴스 축을 안 넣나

`_cache/dart_fundamental_latest.csv` 는 **현재 스냅샷**이라 과거 시점에 그 값을 알 수
없었다. 그대로 쓰면 look-ahead 다(이 시스템에서 이미 재무 IC 78% 가 그것으로 무효가 됐다).
뉴스도 같다. **점 시점(point-in-time) 이 보장되는 축만** 넣는다 - 가격·거래대금 파생.

읽기 전용이다.
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(r"E:\1_Data")
sys.path.insert(0, str(ROOT / "tools"))
from load_merged_panel import load_merged          # noqa: E402

OUT_DIR = ROOT / "2_Logs" / "design"
CA_HI, CA_LO = 1.305, 0.695          # 일간 가격제한 밖 = 정상 등락이 아니다



def index_cagr(start_ymd: str, end_ymd: str, index_code: str = "0001") -> "tuple":
    """측정 구간의 지수 CAGR. **상수를 쓰지 않는다.**

    [2026-09-10] `11.35%` 상수는 2015~2026 전구간 KOSPI 값이다.
    설계구간(2015~2024)만 재면서 그 상수와 대면 구간이 어긋난다 -
    KOSPI 는 2015~2024 CAGR 이 +2.23% 뿐이고 2025~2026 이 수익의 거의 전부다.
    돌려주는 값: (cagr_pct, 실제시작, 실제끝, 거래일수). 자료가 없으면 (None, ...).
    """
    import pandas as _pd
    from pathlib import Path as _Path
    f = _Path(__file__).resolve().parents[1] / "2_Logs" / "index_daily_history.csv"
    if not f.exists():
        return (None, None, None, 0)
    try:
        ix = _pd.read_csv(f, dtype=str, encoding="utf-8-sig")
        ix["close"] = _pd.to_numeric(ix["close"], errors="coerce")
        k = ix[ix["index_code"] == index_code].dropna(subset=["close"]).copy()
        k["date"] = k["date"].astype(str)
        k = k.sort_values("date")
        lo = str(start_ymd).replace("-", "")
        hi = str(end_ymd).replace("-", "") if end_ymd else "99999999"
        w = k[(k["date"] >= lo) & (k["date"] <= hi)]
        if len(w) < 100:
            return (None, None, None, len(w))
        yrs = len(w) / 247.0
        cagr = (w["close"].iloc[-1] / w["close"].iloc[0]) ** (1.0 / yrs) - 1.0
        return (cagr * 100.0, w["date"].iloc[0], w["date"].iloc[-1], len(w))
    except Exception:
        return (None, None, None, 0)


def build_features(px: pd.DataFrame, val: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """모두 t 시점까지의 정보만 쓴다. shift 로 미래를 막지 않고, 애초에 과거만 참조한다."""
    r1 = px / px.shift(1) - 1.0
    r1 = r1.where((r1 <= CA_HI - 1) & (r1 >= CA_LO - 1))     # 기업행위/결함 제거
    logv = np.log(val.replace(0, np.nan))

    f: dict[str, pd.DataFrame] = {}
    f["mom_5"] = px / px.shift(5) - 1.0
    f["mom_20"] = px / px.shift(20) - 1.0
    f["mom_60"] = px / px.shift(60) - 1.0
    f["mom_120"] = px / px.shift(120) - 1.0
    f["mom_20_ex1"] = px.shift(1) / px.shift(21) - 1.0        # 최근 1일 제외(반전 분리)
    f["rev_1"] = -r1                                          # 단기 반전: 어제 많이 내린 것
    f["vol_20"] = r1.rolling(20, min_periods=15).std()
    f["vol_60"] = r1.rolling(60, min_periods=45).std()
    f["liq_logvalue"] = logv.rolling(20, min_periods=15).mean()
    f["liq_trend"] = (logv.rolling(5, min_periods=4).mean()
                      - logv.rolling(60, min_periods=45).mean())   # 거래대금 급증
    f["price_level"] = np.log(px)
    ma20 = px.rolling(20, min_periods=15).mean()
    ma60 = px.rolling(60, min_periods=45).mean()
    f["ma20_ratio"] = px / ma20 - 1.0
    f["ma60_ratio"] = px / ma60 - 1.0
    hi60 = px.rolling(60, min_periods=45).max()
    f["dist_high60"] = px / hi60 - 1.0
    # rs: 시장 대비 초과 (차이 형태 - 비율 형태는 하락장에서 부호가 뒤집힌다)
    mkt = r1.mean(axis=1)
    f["rs_20"] = (r1.rolling(20, min_periods=15).mean().sub(
        mkt.rolling(20, min_periods=15).mean(), axis=0))
    return f


def main() -> int:
    ap = argparse.ArgumentParser(description="신호 재고 조사 (설계 입력)")
    ap.add_argument("--h", type=int, default=10, help="예측 지평(거래일)")
    ap.add_argument("--mes", type=float, default=1.0,
                    help="합격선 %%p (h=10 기준). 다른 지평에서는 h/10 배로 비례한다 - "
                         "알파는 표류항이라 지평에 선형으로 는다")
    ap.add_argument("--cost", type=float, default=0.358,
                    help="왕복 비용 %%%%. 2026-08-26 호가로 독립 확인된 값")
    ap.add_argument("--min-value", type=float, default=1e9)
    ap.add_argument("--deciles", type=int, default=10)
    ap.add_argument("--start", default="20150101")
    ap.add_argument("--out", default=str(OUT_DIR / "signal_inventory_latest.csv"))
    args = ap.parse_args()

    print("[설계] 신호 재고 조사")
    mes_eff = args.mes * args.h / 10.0
    print("  규격: h=%d거래일  합격선(MES)=%.2f%%p (h=10 기준 %.1f%%p 의 %.1f배)  "
          "유니버스=거래대금 %.0f억 이상"
          % (args.h, mes_eff, args.mes, args.h / 10.0, args.min_value / 1e8))
    print("  비용: 왕복 %.3f%%  ->  연 %.1f회 교체 시 연 %.1f%% 부담"
          % (args.cost, 247.0 / args.h, args.cost * 247.0 / args.h))
    print("  표집: %d거래일 간격 비중첩 (겹침 보정이 아니라 겹침 제거)" % args.h)

    d = load_merged()
    d = d[d["date"] >= args.start]
    px = d.pivot_table(index="date", columns="code", values="close", aggfunc="last")
    val = d.pivot_table(index="date", columns="code", values="value", aggfunc="last")
    px.index = px.index.astype(str)
    val = val.reindex(index=px.index, columns=px.columns)
    print("  패널 %d거래일 x %d종목" % px.shape)

    feats = build_features(px, val)

    # 미래수익 (h거래일). 기업행위/결함 구간은 제거한다.
    r1 = px / px.shift(1) - 1.0
    bad = ~((r1 <= CA_HI - 1) & (r1 >= CA_LO - 1)) & r1.notna()
    fwd = px.shift(-args.h) / px - 1.0
    bad_fwd = bad.shift(-args.h).rolling(args.h, min_periods=1).max().astype(bool)
    fwd = fwd.where(~bad_fwd)

    tradable = (val >= args.min_value) & px.notna() & fwd.notna()
    dates = list(px.index)
    sample_dates = dates[120::args.h]          # 워밍업 120일 후, h간격 비중첩
    sample_dates = [x for x in sample_dates if x in px.index]
    print("  표집일 %d개 (비중첩)" % len(sample_dates))

    rows = []
    for name, F in feats.items():
        per = []
        for dt_ in sample_dates:
            if dt_ not in F.index:
                continue
            m = tradable.loc[dt_]
            x = F.loc[dt_][m]
            y = fwd.loc[dt_][m]
            ok = x.notna() & y.notna()
            if ok.sum() < 100:
                continue
            x, y = x[ok], y[ok]
            q = pd.qcut(x.rank(method="first"), args.deciles, labels=False, duplicates="drop")
            g = y.groupby(q).mean()
            uni = y.mean()
            if len(g) < args.deciles:
                continue
            rec = {"date": dt_, "top": g.iloc[-1], "bot": g.iloc[0], "uni": uni,
                   "n": int(ok.sum())}
            for i in range(args.deciles):
                rec["d%d" % i] = g.iloc[i]
            per.append(rec)
        if len(per) < 30:
            rows.append({"feature": name, "periods": len(per), "verdict": "표본 부족"})
            continue
        p = pd.DataFrame(per)
        n = len(p)
        # [2026-08-27] **최선의 십분위**로 판정한다.
        #   처음엔 상위십분위(D9) 알파만 봤는데 그건 "높을수록 좋다" 를 가정한 것이다.
        #   실측하니 vol_60/mom_5 는 D9 가 최악이고 중간(D2~D4)이 최선이었다 -
        #   방향이 반대인 축을 전부 "미달" 로 버릴 뻔했다.
        #   롱온리 구성이 실제로 얻을 수 있는 최대치는 "가장 좋은 십분위" 다.
        alphas = {i: (p["d%d" % i] - p["uni"]) * 100 for i in range(args.deciles)}
        best_i = max(alphas, key=lambda i: alphas[i].mean())
        best = alphas[best_i]
        se_best = best.std(ddof=1) / np.sqrt(n)
        spread = (p["top"] - p["bot"]) * 100
        se_spr = spread.std(ddof=1) / np.sqrt(n)
        mde = 1.96 * se_best
        verdict = ("판정 불가(MDE>MES)" if mde > mes_eff
                   else ("합격" if best.mean() >= mes_eff else "미달"))
        # 비용 뺀 연환산 **초과분**. 지평마다 교체 횟수가 다르므로 이걸로 비교한다.
        # [2026-09-09 정정] 이 값은 유니버스 대비 **초과**다. 지수 절대수익과 직접 대면 안 된다 -
        #   종전 출력이 그렇게 비교하고 있었다(초과 vs 절대). 유니버스 자체 수익을 더해야 절대가 된다.
        net_ann = (best.mean() - args.cost) * (247.0 / args.h)
        uni_ann = p["uni"].mean() * 100 * (247.0 / args.h)
        rows.append({
            "feature": name, "periods": n, "n_median": int(p["n"].median()),
            "best_decile": best_i,
            "best_alpha_pp": best.mean(), "best_t": best.mean() / se_best if se_best else np.nan,
            "top_alpha_pp": ((p["top"] - p["uni"]) * 100).mean(),
            "bot_alpha_pp": ((p["bot"] - p["uni"]) * 100).mean(),
            "spread_pp": spread.mean(), "spread_t": spread.mean() / se_spr if se_spr else np.nan,
            "mde_pp": mde, "mes_eff_pp": mes_eff, "net_ann_pct": net_ann,
            "uni_ann_pct": uni_ann, "abs_ann_pct": uni_ann + net_ann, "verdict": verdict,
        })

    res = pd.DataFrame(rows)
    res = res.sort_values("best_alpha_pp", ascending=False, na_position="last")
    print()
    print("%-16s %6s %5s %9s %7s %9s %8s %10s  %s"
          % ("축", "표집", "최선", "최선알파", "t", "D9알파", "MDE", "비용뺀연환산", "판정"))
    for r in res.itertuples():
        if not hasattr(r, "best_alpha_pp") or pd.isna(getattr(r, "best_alpha_pp", np.nan)):
            print("%-16s %7s  %s" % (r.feature, getattr(r, "periods", "-"), r.verdict))
            continue
        print("%-16s %6d %5s %8.2f%%p %7.2f %8.2f%%p %7.2f%%p %9.1f%%  %s"
              % (r.feature, r.periods, "D%d" % r.best_decile, r.best_alpha_pp, r.best_t,
                 r.top_alpha_pp, r.mde_pp, r.net_ann_pct, r.verdict))

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    res.to_csv(out, index=False, encoding="utf-8-sig")
    stamped = out.with_name(out.name.replace("_latest", "_%s" % dt.datetime.now().strftime("%Y%m%d_%H%M%S")))
    res.to_csv(stamped, index=False, encoding="utf-8-sig")
    print()
    n_pass = int((res["verdict"] == "합격").sum()) if "verdict" in res else 0
    n_undec = int((res["verdict"] == "판정 불가(MDE>MES)").sum()) if "verdict" in res else 0
    print("  합격 %d축 / 판정불가 %d축 / 전체 %d축   (h=%d, MES=%.2f%%p)"
          % (n_pass, n_undec, len(res), args.h, mes_eff))
    if "net_ann_pct" in res and len(res):
        bestrow = res.iloc[0]
        print("  최선 축 비용 뺀 연환산 **초과** %.1f%%p" % bestrow["net_ann_pct"])
        bm, bm_lo, bm_hi, bm_n = index_cagr(args.start, getattr(args, "end", None))
        if bm is None:
            print("  동일가중 유니버스 자체 %.1f%%  ->  절대 합계 %.1f%%  "
                  "(**지수 비교 불가** - index_daily_history.csv 에서 구간 자료를 못 읽었어요)"
                  % (bestrow["uni_ann_pct"], bestrow["abs_ann_pct"]))
        else:
            print("  동일가중 유니버스 자체 %.1f%%  ->  절대 합계 %.1f%%"
                  % (bestrow["uni_ann_pct"], bestrow["abs_ann_pct"]))
            print("  **측정 구간의** KOSPI CAGR %.2f%% (%s~%s, %d일) 대비 %+.1f%%p"
                  % (bm, bm_lo, bm_hi, bm_n, bestrow["abs_ann_pct"] - bm))
            print("  (상수 11.35%% 는 2015~2026 전구간 값이에요. 구간을 안 맞추면 결론이 뒤집혀요)")
        print("  주의: 이 알파는 **십분위(약 300종목)** 폭이다. 좁은 바스켓에서는 sd 가 커져")
        print("        N=6 이면 MDE 가 알파보다 크다(H002 실측). 폭을 빼고 인용하지 말 것.")
    if n_pass == 0:
        print("  -> 합격선을 넘는 축이 없다. 이 축들로 만든 어떤 구성도 넘지 못한다.")
    print("  wrote %s" % out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
