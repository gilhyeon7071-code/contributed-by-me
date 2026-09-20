# -*- coding: utf-8 -*-
"""근거 없는 파라미터를 **코드 선언 기본값으로 되돌리면** 후보가 얼마나 바뀌나.
읽기 전용, 주문 없음, 생산 산출물 미변경.

## 왜

`stable_params_v41_1.json` 은 `promoted: False` 이고 `as_of: 2026-08-14` 다.
그 시점은 섹터 유니온 폴백이 KeyError 로 죽은 채 튜닝된 때이고,
그 위에서 완화 사다리 rs 가 역방향으로 돌았다(D2).
**아무도 이 값으로 하자고 결정한 기록이 없다.**

```
              코드 선언(DEFAULT_PARAMS)   현재 라이브     배수
value_min            10억                  1,550억       155배
v_accel_lim           2.5                     6.6         2.6배
```

되돌리는 것은 정당해 보이지만, **얼마나 바뀌는지 모르고 라이브를 바꾸면 안 된다.**
그래서 먼저 잰다.

## 무엇을 재나

생산과 같은 규칙으로 잰다 — 9개 게이트를 완화 사다리로 훑고
**후보가 나오는 첫 단계에서 멈춘다**(generate_candidates_v41_1.py:2186).

변형 4개:
```
현행          value 1,550억 / v_accel 6.6
value 되돌림  value    10억 / v_accel 6.6
v_accel 되돌림 value 1,550억 / v_accel 2.5
둘 다 되돌림  value    10억 / v_accel 2.5
```
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

OUT = ROOT / "2_Logs" / "param_revert_impact_latest.json"


def _mask(dx: pd.DataFrame, p: dict) -> pd.Series:
    m = (
        (dx["rs"] > float(p["rs_lim"]))
        & (dx["v_accel"] > float(p["v_accel_lim"]))
        & (dx["stretch"] < float(p["stretch_max"]))
        & (dx["value"] > float(p["value_min"]))
        & (dx["atr14_pct"] < float(p["atr_max"]))
        & (dx["rsi14"] < float(p["rsi_max"]))
        & (dx["vol_close_corr20"] >= float(p["vol_close_corr_min"]))
        & (dx["high_52w_gap"] <= float(p["near_52w_high_gap_max"]))
        & (dx["listing_days"] >= float(p["min_listing_days"]))
    )
    if float(p.get("require_macd_golden", 0.0) or 0.0) >= 0.5:
        m = m & (dx["macd_golden"] == True)  # noqa: E712
    return m.fillna(False)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback-days", type=int, default=120)
    ap.add_argument("--min-universe", type=int, default=2000)
    a = ap.parse_args()

    raw = gen._load_data()
    df, _l, _ = gen._compute_factors(raw)
    max_d = pd.to_datetime(df["date"].max())
    dx = df[df["date"] >= max_d - pd.Timedelta(days=a.lookback_days)].copy()
    uni = dx.groupby("date")["code"].nunique()
    dx = dx[dx["date"].isin(uni[uni >= a.min_universe].index)].copy()
    dates = sorted(dx["date"].unique())

    base = gen._normalize_params(
        gen.read_json(ROOT / "12_Risk_Controlled" / "stable_params_v41_1.json") or {})
    DEF = gen.DEFAULT_PARAMS

    variants = {
        "현행": {},
        "value되돌림": {"value_min": float(DEF["value_min"])},
        "v_accel되돌림": {"v_accel_lim": float(DEF["v_accel_lim"])},
        "둘다되돌림": {"value_min": float(DEF["value_min"]),
                       "v_accel_lim": float(DEF["v_accel_lim"])},
    }

    rows = []
    for name, override in variants.items():
        p0 = dict(base)
        p0.update(override)
        p0 = gen._normalize_params(p0)          # 정규화 클램프도 그대로 태운다
        ladder = gen._relax_ladder(dict(p0))
        per_level = [(lv, _mask(dx, pp)) for lv, pp in ladder]

        chosen, counts = {}, {}
        for d in dates:
            sel = dx["date"].to_numpy() == d
            for lv, m in per_level:
                c = int(m.to_numpy()[sel].sum())
                if c > 0:
                    chosen[d] = lv
                    counts[d] = c
                    break
            else:
                chosen[d] = "NONE"
                counts[d] = 0
        cnt = pd.Series(counts)
        lv_counts = pd.Series(chosen).value_counts().to_dict()
        rows.append({
            "variant": name,
            "value_min_eok": round(float(p0["value_min"]) / 1e8, 1),
            "v_accel_lim": float(p0["v_accel_lim"]),
            "median_candidates": float(cnt.median()),
            "mean_candidates": float(cnt.mean()),
            "max_candidates": int(cnt.max()),
            "zero_days": int((cnt == 0).sum()),
            "total_days": len(dates),
            "levels": lv_counts,
        })

    print(f"창 {pd.Timestamp(dates[0]).date()} ~ {pd.Timestamp(dates[-1]).date()} ({len(dates)}거래일)")
    print("생산 규칙: 9개 게이트 + 완화 사다리, 첫 통과 단계에서 멈춤")
    print()
    print(f'{"변형":<14} {"value_min":>10} {"v_accel":>8} {"중앙":>6} {"평균":>7} {"최대":>6} {"0후보일":>8}')
    for r in rows:
        print(f'{r["variant"]:<14} {r["value_min_eok"]:>9,.0f}억 {r["v_accel_lim"]:>8.2f} '
              f'{r["median_candidates"]:>6.0f} {r["mean_candidates"]:>7.1f} '
              f'{r["max_candidates"]:>6d} {r["zero_days"]:>4d}/{r["total_days"]:<3d}')
    print()
    print("선택된 완화 단계 분포:")
    for r in rows:
        top = sorted(r["levels"].items(), key=lambda kv: -kv[1])
        print(f'  {r["variant"]:<14} ' + "  ".join(f"{k}:{v}" for k, v in top))

    OUT.write_text(json.dumps({
        "measured_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "window": {"start": str(pd.Timestamp(dates[0]).date()),
                   "end": str(pd.Timestamp(dates[-1]).date()), "days": len(dates)},
        "defaults": {"value_min": float(DEF["value_min"]), "v_accel_lim": float(DEF["v_accel_lim"])},
        "live": {"value_min": float(base["value_min"]), "v_accel_lim": float(base["v_accel_lim"])},
        "rows": rows,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print()
    print(f"[OK] {OUT.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
