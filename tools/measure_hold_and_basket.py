# -*- coding: utf-8 -*-
"""보유기간(h)과 바스켓 폭(N)이 알파와 **판정 가능성**에 미치는 영향을 잰다.

## 왜 만드나

`design_signal_inventory.py` 는 **십분위(약 300종목)** 폭으로만 잰다.
실제 하네스는 `max_pos=6` 이다. 폭이 50배 다르다.

2026-09-09 H002 측정에서 이게 결정적인 것으로 드러났다 -
N=6 에서는 MDE(최소검출효과)가 알파보다 커서 **알파가 있어도 없어도 같은 그림**이 나온다.
지난 1년의 "안 된다" 결론 상당수가 신호 탓인지 폭 탓인지 구분되지 않는다.

처음엔 이 측정을 heredoc 으로 즉석 실행했고 **스크립트도 출력도 남지 않았다.**
사용자 지적으로 도구화한다. 결과를 인용하려면 재현이 돼야 한다.

## 초판에서 고친 것 (같은 날)

- 밴드를 20~40% 로 잡았다 -> 최선 십분위 D2 는 **20~30%** 다. 정확히 잡는다
- `random_state=hash(t)` 를 썼다 -> 파이썬 문자열 해시는 프로세스마다 다르다.
  **재현이 안 되는 난수**였다. `default_rng(seed)` 로 바꾸고 시드를 여러 개 돌린다

## 쓰는 법

    python tools/measure_hold_and_basket.py --axis vol_60
    python tools/measure_hold_and_basket.py --axis vol_60 --min-value 2e9   # 사전등록 하한
    python tools/measure_hold_and_basket.py --h 40 --n 50 --seeds 50

[2026-09-09] 신설. H002 근거 도구.
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
from design_signal_inventory import load_merged, build_features, CA_HI, CA_LO  # noqa: E402


PROD_AXES = ("ARM_SCORE", "rs", "v_accel", "stretch", "atr14_pct")


def build_prod_axis(axis: str, start: str, end: str | None = None) -> pd.DataFrame:
    """생산 팩터 하나를 (거래일 x 종목) 넓은 표로 만든다.

    [2026-09-10] H004 로 신설. `ARM_SCORE` 는 네 축의 동일가중 z-합이라
    **합이 0이어도 항이 0이라는 뜻은 아니다.** 구성요소를 같은 자로 재려면 이게 필요하다.
    """
    if axis == "ARM_SCORE":
        return build_arm_score(start, end)
    if axis not in PROD_AXES:
        raise ValueError("생산 축이 아니에요: %s" % axis)
    import optimize_params_v41_1 as O
    from utils.price_history_contract import apply_price_history_contract

    d = load_merged(cols=["date", "code", "open", "high", "low", "close", "value"],
                    with_market=True)
    d = d[d["date"] >= start.replace("-", "")]
    if end:
        d = d[d["date"] <= end.replace("-", "")]
    print("  통합 패널 %d행" % len(d))
    d, meta = apply_price_history_contract(d)
    print("  가격이력 계약 통과 %d행" % len(d))
    d = O.compute_factors(d)
    if axis not in d.columns:
        raise RuntimeError("compute_factors 가 %s 를 안 냈어요" % axis)
    d = d[["date", "code", axis]].dropna()
    import gc
    gc.collect()
    w = d.pivot_table(index="date", columns="code", values=axis, aggfunc="last")
    w.index = pd.to_datetime(w.index).strftime("%Y%m%d")
    print("  %s  %d거래일 x %d종목" % (axis, w.shape[0], w.shape[1]))
    return w


def build_arm_score(start: str, end: str | None = None) -> pd.DataFrame:
    """하네스가 실제로 쓰는 `ARM_SCORE` 를 패널 전체에 대해 만든다.

    `z(rs)+z(v_accel)-z(stretch)-z(atr14_pct)`, 가중치 전부 1 (튜닝 금지).

    **재구현하지 않는다.** 생산 함수 `optimize_params_v41_1.compute_factors` 를 그대로 쓴다.
    다만 로더는 생산의 `load_data` 가 아니라 **통합 패널**(archive + Raw)을 먹인다 -
    생산 로더는 `Raw/` 를 안 읽어 27개월 유니버스 62.8% 결손이 있다(2026-08-28 실측).

    생산과 다른 점 하나: 생산은 그날의 **후보 유니버스 안에서** z 를 낸다.
    여기서는 거래가능 전체에서 낸다. 순위는 대체로 같지만 같다고 단정하지 말 것.
    """
    import optimize_params_v41_1 as O
    from utils.price_history_contract import apply_price_history_contract

    d = load_merged(cols=["date", "code", "open", "high", "low", "close", "value"],
                    with_market=True)
    d = d[d["date"] >= start.replace("-", "")]
    if end:
        d = d[d["date"] <= end.replace("-", "")]
    print("  통합 패널 %d행" % len(d))
    d, meta = apply_price_history_contract(d)
    print("  가격이력 계약 통과 %d행" % len(d))
    # [2026-09-09] compute_factors 는 컬럼 50여 개를 붙인다. 770만 행이면 16GB 에서 죽는다.
    #   (실측: 사용가능 3.5GB 에서 OOM kill). 필요한 4개만 남기고 즉시 버린다.
    d = O.compute_factors(d)
    need = ["rs", "v_accel", "stretch", "atr14_pct"]
    d = d[["date", "code"] + [c for c in need if c in d.columns]].copy()
    import gc
    gc.collect()
    miss = [c for c in need if c not in d.columns]
    if miss:
        raise RuntimeError("compute_factors 가 안 낸 컬럼: %s" % miss)
    d = d.dropna(subset=need)

    def _z(v):
        m, sd = v.mean(), v.std(ddof=0)
        if not np.isfinite(sd) or sd == 0:
            return pd.Series(0.0, index=v.index)
        return (v.fillna(m) - m) / sd

    g = d.groupby("date", sort=True)
    d["_arm"] = (g["rs"].transform(_z) + g["v_accel"].transform(_z)
                 - g["stretch"].transform(_z) - g["atr14_pct"].transform(_z))
    w = d.pivot_table(index="date", columns="code", values="_arm", aggfunc="last")
    w.index = pd.to_datetime(w.index).strftime("%Y%m%d")
    print("  ARM_SCORE %d거래일 x %d종목" % w.shape)
    return w


def main() -> int:
    ap = argparse.ArgumentParser(description="보유기간 x 바스켓 폭 측정 (H002)")
    ap.add_argument("--axis", default="vol_60", help="build_features 의 축 이름")
    ap.add_argument("--h", type=int, nargs="+", default=[20, 40, 60])
    ap.add_argument("--n", type=int, nargs="+", default=[6, 20, 50])
    ap.add_argument("--seeds", type=int, default=20)
    ap.add_argument("--cost", type=float, default=0.400, help="왕복 비용 %%. 브로커 정산 실측")
    ap.add_argument("--min-value", type=float, default=1e9,
                    help="유동성 하한. **사전등록 하네스는 20억(2e9)이다** - 기본값 10억과 다르다")
    ap.add_argument("--band", type=float, nargs=2, default=[0.20, 0.30],
                    help="최선 십분위 대역. vol_60 은 D2 = 20~30%%")
    ap.add_argument("--min-blocks", type=int, default=20,
                    help="이 블록 수 미만인 칸은 버린다. [2026-09-10] 기본 20 때문에 "
                         "5년 하위구간 x h60(블록 18)이 통째로 사라졌다. 낮출 때는 "
                         "**검정력이 없다는 것을 결과에 함께 적을 것**")
    ap.add_argument("--scan-deciles", action="store_true",
                    help="십분위 10개를 전부 훑어 각각의 알파/t/MDE 를 낸다. "
                         "부호를 예측하지 않는 가설에 쓴다. **사후 선택이므로 10칸을 원장에 계상할 것**")
    ap.add_argument("--select", choices=["band", "top"], default="band",
                    help="band=대역에서 난수 표집(시드 여러 개). "
                         "top=점수 상위 N 을 그대로(하네스와 동일, 난수 없음)")
    ap.add_argument("--start", default="2015-01-01",
                    help="ARM_SCORE 는 메모리 때문에 전구간이 안 돈다. "
                         "사전등록 설계 구간 2020-01-01 을 쓸 것 (2025+ 는 봉인)")
    ap.add_argument("--end", default=None, help="봉인 구간을 막는다. 2단계는 2024-12-31")
    ap.add_argument("--eval-start", default=None,
                    help="**평가 시점**의 하한(YYYY-MM-DD). 지표 워밍업은 --start 부터의 자료를 쓰되 "
                         "알파 계산은 이 날짜 이후 시점만 쓴다. 짧은 구간(봉인)을 검정할 때 "
                         "워밍업으로 블록을 잃지 않게 한다. 미래수익은 전부 평가 구간 안에 있다")
    ap.add_argument("--out", default="2_Logs/design/hold_basket_latest.csv")
    a = ap.parse_args()

    print("축 %s / 비용 %.3f%% / 유동성하한 %.0f억 / 밴드 %.0f~%.0f%% / 시드 %d회"
          % (a.axis, a.cost, a.min_value / 1e8, a.band[0] * 100, a.band[1] * 100, a.seeds))

    d = load_merged()
    d = d[d["date"] >= a.start]
    if a.end:
        d = d[d["date"] <= a.end.replace("-", "")]
        print("상한 적용: %s 까지만 봐요" % a.end)
    if a.eval_start:
        print("**평가 시점은 %s 부터**예요. 그 앞 자료는 지표 워밍업에만 씁니다 "
              "(미래수익은 전부 평가 구간 안)" % a.eval_start)
    px = d.pivot_table(index="date", columns="code", values="close", aggfunc="last")
    val = d.pivot_table(index="date", columns="code", values="value", aggfunc="last")
    px.index = px.index.astype(str)
    val = val.reindex(index=px.index, columns=px.columns)
    print("패널 %d거래일 x %d종목" % px.shape)

    if a.axis in PROD_AXES:
        F = build_prod_axis(a.axis, a.start, a.end).reindex(index=px.index, columns=px.columns)
        feats = {a.axis: F}
    else:
        feats = build_features(px, val)
    if a.axis == "all":
        axes = dict(feats)
        print("  축 %d개를 한 번에 돕니다: %s" % (len(axes), ", ".join(sorted(axes))))
    else:
        if a.axis not in feats:
            print("[STOP] 없는 축이에요: %s" % a.axis)
            print("       있는 축: %s" % ", ".join(sorted(feats)))
            return 2
        axes = {a.axis: feats[a.axis]}

    r1 = px / px.shift(1) - 1.0
    bad = ~((r1 <= CA_HI - 1) & (r1 >= CA_LO - 1)) & r1.notna()

    rows = []
    for _axname, F in axes.items():
      if a.axis == "all":
        print("  [축] %s" % _axname, flush=True)
      for h in a.h:
          fwd = px.shift(-h) / px - 1.0
          bf = bad.shift(-h).rolling(h, min_periods=1).max().astype(bool)
          fwd = fwd.where(~bf)                      # 기업행위/결함 구간 제거
          tr = (val >= a.min_value) & px.notna() & fwd.notna()
          ctx = []
          _dates = list(px.index)
          if a.eval_start:
              _lo = str(a.eval_start).replace("-", "")
              _from = next((i for i, x in enumerate(_dates) if str(x) >= _lo), len(_dates))
              _sample = _dates[_from::h]        # 워밍업은 이미 앞 자료로 채워졌다
          else:
              _sample = _dates[120::h]          # 워밍업 120일, h간격 **비중첩**
          for t in _sample:
              if t not in F.index:
                  continue
              m = tr.loc[t]
              x, y = F.loc[t][m], fwd.loc[t][m]
              ok = x.notna() & y.notna()
              if ok.sum() < 100:
                  continue
              x, y = x[ok], y[ok]
              if a.scan_deciles:
                  # 십분위 10개를 전부 보관한다. 부호를 예측하지 않는 가설용
                  q = pd.qcut(x.rank(method="first"), 10, labels=False, duplicates="drop")
                  if getattr(q, "nunique", lambda: 0)() < 10:
                      continue
                  ctx.append(({i: x.index.values[(q.values == i)] for i in range(10)}, y, y.mean()))
              elif a.select == "top":
                  # 하네스와 같은 방식: 점수 상위를 그대로 뽑는다. 난수가 없다
                  ctx.append((x.sort_values(ascending=False).index.values, y, y.mean()))
              else:
                  lo, hi = x.quantile(a.band[0]), x.quantile(a.band[1])
                  band = x[(x >= lo) & (x <= hi)]
                  ctx.append((band.index.values, y, y.mean()))
          if a.scan_deciles:
              for N in a.n:
                  for dec in range(10):
                      al = []
                      for buckets, y, um in ctx:
                          idx = buckets.get(dec)
                          if idx is None or len(idx) < N:
                              continue
                          pick = idx[:N] if len(idx) == N else np.random.default_rng(dec).choice(idx, N, replace=False)
                          al.append((y[pick].mean() - um) * 100)
                      al = np.array(al)
                      if len(al) < a.min_blocks:
                          continue
                      se = al.std(ddof=1) / np.sqrt(len(al))
                      mde = 1.96 * se
                      rows.append({
                          "axis": _axname, "h": h, "N": N, "decile": dec, "blocks": len(al),
                          "alpha_pp": float(al.mean()), "alpha_sd_across_seeds": 0.0,
                          "t_median": float(al.mean() / se) if se else np.nan,
                          "mde_median_pp": float(mde),
                          "seeds_t_gt2": int(abs(al.mean() / se) > 2) if se else 0, "seeds": 1,
                          "net_excess_ann_pct": (float(al.mean()) - a.cost) * 247.0 / h,
                          "measurable": bool(mde < abs(float(al.mean()))),
                      })
              continue
          for N in a.n:
              ts, mdes, mus = [], [], []
              n_seeds = 1 if a.select == "top" else a.seeds
              for seed in range(n_seeds):
                  rng = np.random.default_rng(seed)
                  al = []
                  for idx, y, um in ctx:
                      if len(idx) < N:
                          continue
                      pick = idx[:N] if a.select == "top" else rng.choice(idx, N, replace=False)
                      al.append((y[pick].mean() - um) * 100)
                  al = np.array(al)
                  if len(al) < a.min_blocks:
                      continue
                  se = al.std(ddof=1) / np.sqrt(len(al))
                  ts.append(al.mean() / se)
                  mdes.append(1.96 * se)
                  mus.append(al.mean())
              if not ts:
                  print("  h=%d N=%d 표본 부족" % (h, N))
                  continue
              rows.append({
                  "axis": _axname, "h": h, "N": N, "blocks": len(ctx),
                  "alpha_pp": float(np.mean(mus)), "alpha_sd_across_seeds": float(np.std(mus)),
                  "t_median": float(np.median(ts)), "mde_median_pp": float(np.median(mdes)),
                  "seeds_t_gt2": int(sum(1 for v in ts if v > 2)), "seeds": len(ts),
                  # 순초과 연환산. 알파에서 왕복비용을 빼고 연 회전수를 곱한다
                  "net_excess_ann_pct": (float(np.mean(mus)) - a.cost) * 247.0 / h,
                  "measurable": bool(np.median(mdes) < np.mean(mus)),
              })

    res = pd.DataFrame(rows)
    print()
    if a.scan_deciles:
        print("%-13s %3s %4s %4s %7s %9s %8s %9s %9s  %s"
              % ("축", "h", "N", "십분", "블록", "알파%p", "t", "MDE", "순초과/년", "판정"))
        for r in res.sort_values(["axis", "h", "decile"]).itertuples():
            print("%-13s %3d %4d  D%-2d %7d %9.2f %8.2f %9.2f %8.1f%%  %s"
                  % (r.axis, r.h, r.N, r.decile, r.blocks, r.alpha_pp, r.t_median,
                     r.mde_median_pp, r.net_excess_ann_pct,
                     "**가능**" if r.measurable else "불가"))
    else:
        print("%3s %4s %7s %8s %8s %9s %8s  %s"
              % ("h", "N", "블록", "알파%p", "t중앙", "MDE중앙", "순초과/년", "t>2 시드"))
        for r in res.itertuples():
            print("%3d %4d %7d %8.2f %8.2f %9.2f %7.1f%%  %d/%d%s"
                  % (r.h, r.N, r.blocks, r.alpha_pp, r.t_median, r.mde_median_pp,
                     r.net_excess_ann_pct, r.seeds_t_gt2, r.seeds,
                     "" if r.measurable else "   <- MDE>알파, 판정 불가"))

    out = ROOT / a.out
    out.parent.mkdir(parents=True, exist_ok=True)
    res.to_csv(out, index=False, encoding="utf-8-sig")
    stamp = out.with_name(out.name.replace("_latest", "_%s" % dt.datetime.now().strftime("%Y%m%d_%H%M%S")))
    res.to_csv(stamp, index=False, encoding="utf-8-sig")
    print()
    print("  판정 가능 %d칸 / 전체 %d칸" % (int(res["measurable"].sum()), len(res)))
    print("  wrote %s" % out)
    print("  wrote %s" % stamp)
    print()
    print("  주의: 패널 측정이에요. 집행 슬리피지·호가 충격이 안 들어갔어요.")
    print("        '최선 십분위' 는 사후 선택이에요 - 다중검정 원장에 라운드를 추가하세요.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
