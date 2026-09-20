# -*- coding: utf-8 -*-
"""Gate 0(유동성 하한) 을 얼마로 잡아야 하는가. 읽기 전용, 주문 없음.

사용자 제안(2026-09-10): 20일 평균 거래대금 >= 5억~10억 을 **0단계**로 꽂는다.

확인할 것:
  - 하한별 잔존 종목 수와 **하위 10% 거래대금** (체결 가능성)
  - 1억 동일가중 시 종목당 금액이 **일 거래대금의 몇 %** 인가
  - `value`(당일) 와 `value20`(20일 평균) 이 얼마나 다른가
    -> 20일 평균은 지금 패널에 **없다.** 여기서 만들어 비교한다
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

OUT = ROOT / "2_Logs" / "liquidity_floor_sizing_latest.json"
EOK = 1e8  # 1억


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback-days", type=int, default=120)
    ap.add_argument("--min-universe", type=int, default=2000)
    ap.add_argument("--capital", type=float, default=1e8)
    a = ap.parse_args()

    raw = gen._load_data()
    df, _l, _ = gen._compute_factors(raw)
    df = df.sort_values(["code", "date"])
    # 20일 평균 거래대금 - 패널에 없어서 여기서 만든다
    df["value20"] = df.groupby("code")["value"].transform(
        lambda s: pd.to_numeric(s, errors="coerce").rolling(20, min_periods=10).mean())

    max_d = pd.to_datetime(df["date"].max())
    dx = df[df["date"] >= max_d - pd.Timedelta(days=a.lookback_days)].copy()
    uni = dx.groupby("date")["code"].nunique()
    dx = dx[dx["date"].isin(uni[uni >= a.min_universe].index)].copy()
    dates = sorted(dx["date"].unique())

    p = gen._normalize_params(gen.read_json(ROOT / "12_Risk_Controlled" / "stable_params_v41_1.json") or {})
    p6 = dict(gen._relax_ladder(dict(p))).get("L6", p)

    # 안전망 4개 + rs (volcorr 는 제안대로 통과율 ~80% 로 완화한 형태를 따로 본다)
    safety = (
        (dx["listing_days"] >= float(p6["min_listing_days"]))
        & (dx["stretch"] < float(p6["stretch_max"]))
        & (dx["atr14_pct"] < float(p6["atr_max"]))
        & (dx["rsi14"] < float(p6["rsi_max"]))
    ).fillna(False)
    with_rs = safety & (dx["rs"] > float(p6["rs_lim"])).fillna(False)

    rows = []
    for floor_eok in (0, 5, 10, 20, 50):
        floor = floor_eok * EOK
        for label, base in (("안전망4", safety), ("안전망4+rs", with_rs)):
            m = base & (pd.to_numeric(dx["value20"], errors="coerce") >= floor)
            per_day = m.groupby(dx["date"]).sum()
            n_med = float(per_day.median())
            v = pd.to_numeric(dx.loc[m, "value20"], errors="coerce").dropna()
            p10 = float(v.quantile(0.10)) if len(v) else float("nan")
            per_name = a.capital / n_med if n_med else float("nan")
            rows.append({
                "floor_eok": floor_eok, "stage": label,
                "median_n": n_med, "min_n": int(per_day.min()),
                "p10_value20": p10,
                "per_name_krw": per_name,
                "order_pct_of_p10": (per_name / p10 * 100.0) if p10 and p10 == p10 else None,
            })

    print(f"창 {pd.Timestamp(dates[0]).date()} ~ {pd.Timestamp(dates[-1]).date()} "
          f"({len(dates)}거래일)  자본 {a.capital:,.0f}원")
    print()
    print(f'{"하한":>6} {"단계":<12} {"중앙 종목":>9} {"최소":>6} '
          f'{"하위10% 거래대금":>18} {"종목당":>12} {"주문/거래대금":>13}')
    for r in rows:
        pct = r["order_pct_of_p10"]
        print(f'{r["floor_eok"]:>4}억 {r["stage"]:<12} {r["median_n"]:>9.0f} {r["min_n"]:>6d} '
              f'{r["p10_value20"]:>18,.0f} {r["per_name_krw"]:>12,.0f} '
              f'{(f"{pct:.2f}%" if pct is not None else "-"):>13}')

    OUT.write_text(json.dumps({
        "measured_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "window": {"start": str(pd.Timestamp(dates[0]).date()),
                   "end": str(pd.Timestamp(dates[-1]).date()), "days": len(dates)},
        "capital_krw": a.capital,
        "note": "value20 = 20일 평균 거래대금. 패널에 없어 이 도구가 계산했다",
        "rows": rows,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print()
    print(f"[OK] {OUT.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
