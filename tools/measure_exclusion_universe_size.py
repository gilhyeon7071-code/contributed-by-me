# -*- coding: utf-8 -*-
"""배제형 게이트만 남겼을 때 **유니버스가 몇 개나 남는가**. 읽기 전용, 주문 없음.

사용자 설계서(2026-09-10) 검토용.
  "배제형 6개(listing/stretch/atr/rsi/rs/volcorr) + 동일가중" 을 제안하면서
  value(거래대금) 게이트를 1차 검증에서 제외했다.
  그런데 **listing_pass 는 상장 경과일수만 본다**(listing_days >= min_listing_days).
  즉 value 를 빼면 **유동성 하한이 아예 없다.**

그래서 잰다:
  - 게이트를 하나씩 누적 적용할 때 남는 종목 수 (Phase 2 ablation 의 1차 자료)
  - volcorr 를 원래 방향(>=)과 반전(<=)으로 각각
  - 동일가중 시 1종목당 금액 (1억 기준) -> 체결 가능성
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import generate_candidates_v41_1 as gen  # noqa: E402

OUT = ROOT / "2_Logs" / "exclusion_universe_size_latest.json"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback-days", type=int, default=120)
    ap.add_argument("--min-universe", type=int, default=2000)
    ap.add_argument("--capital", type=float, default=1e8)
    a = ap.parse_args()

    raw = gen._load_data()
    df, _l, _ = gen._compute_factors(raw)
    max_d = pd.to_datetime(df["date"].max())
    dx = df[df["date"] >= max_d - pd.Timedelta(days=a.lookback_days)].copy()
    uni = dx.groupby("date")["code"].nunique()
    dx = dx[dx["date"].isin(uni[uni >= a.min_universe].index)].copy()

    p = gen._normalize_params(gen.read_json(ROOT / "12_Risk_Controlled" / "stable_params_v41_1.json") or {})
    # 생산이 실제로 쓰는 단계(L6 중앙값)의 값으로 잰다
    lad = dict(gen._relax_ladder(dict(p)))
    p6 = lad.get("L6", p)

    G = [
        ("listing", dx["listing_days"] >= float(p6["min_listing_days"])),
        ("stretch", dx["stretch"] < float(p6["stretch_max"])),
        ("atr", dx["atr14_pct"] < float(p6["atr_max"])),
        ("rsi", dx["rsi14"] < float(p6["rsi_max"])),
        ("rs", dx["rs"] > float(p6["rs_lim"])),
    ]
    VC_ORIG = ("volcorr(>=원래)", dx["vol_close_corr20"] >= float(p6["vol_close_corr_min"]))
    VC_FLIP = ("volcorr(<=반전)", dx["vol_close_corr20"] <= float(p6["vol_close_corr_min"]))

    dates = sorted(dx["date"].unique())
    n_days = len(dates)
    rows = []

    def _report(seq, tag):
        m = pd.Series(True, index=dx.index)
        for name, cond in seq:
            m = m & cond.fillna(False)
            per_day = m.groupby(dx["date"]).sum()
            v = pd.to_numeric(dx.loc[m, "value"], errors="coerce")
            rows.append({
                "variant": tag,
                "gate_added": name,
                "median_n_per_day": float(per_day.median()),
                "min_n_per_day": int(per_day.min()),
                "days_under_10": int((per_day < 10).sum()),
                "total_days": n_days,
                "median_value_krw": (float(v.median()) if len(v) else None),
                "p10_value_krw": (float(v.quantile(0.10)) if len(v) else None),
            })
        return rows[-1]

    last_o = _report(G + [VC_ORIG], "volcorr_원래")
    last_f = _report(G + [VC_FLIP], "volcorr_반전")

    print("창 %s ~ %s (%d거래일)  L6 파라미터" % (
        pd.Timestamp(dates[0]).date(), pd.Timestamp(dates[-1]).date(), n_days))
    print()
    print("%-14s %-16s %10s %8s %10s %14s" % (
        "변형", "게이트 누적", "중앙 종목수", "최소", "10미만 일", "종목당 금액"))
    for r in rows:
        per = (a.capital / r["median_n_per_day"]) if r["median_n_per_day"] else float("nan")
        print(f'{r["variant"]:<14} {r["gate_added"]:<16} '
              f'{r["median_n_per_day"]:>10.0f} {r["min_n_per_day"]:>8d} '
              f'{r["days_under_10"]:>10d} {per:>13,.0f}원')
    print()
    for lbl, r in (("volcorr 원래", last_o), ("volcorr 반전", last_f)):
        per = a.capital / r["median_n_per_day"] if r["median_n_per_day"] else float("nan")
        print(f'{lbl} -> 중앙 {r["median_n_per_day"]:.0f}종목 / '
              f'1억 동일가중이면 종목당 {per:,.0f}원 / '
              f'잔존 종목 거래대금 중앙 {(r["median_value_krw"] or 0):,.0f}원 '
              f'(하위10% {(r["p10_value_krw"] or 0):,.0f}원)')

    OUT.write_text(json.dumps({
        "measured_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "window": {"start": str(pd.Timestamp(dates[0]).date()),
                   "end": str(pd.Timestamp(dates[-1]).date()), "days": n_days},
        "params_level": "L6",
        "capital_krw": a.capital,
        "rows": rows,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print()
    print("[OK] %s" % OUT.name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
