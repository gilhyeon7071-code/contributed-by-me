# -*- coding: utf-8 -*-
"""패널로 시총가중 지수를 직접 만들어 API 지수와 대조한다.

2026-08-27. (127) 에서 "리밸런싱 전략이 지수에 CAGR -6.28%p 로 졌다" 고 결론냈는데,
그 뒤 **지수 시계열 자체를 신뢰할 수 없다는 것**이 드러났다.
```
API 지수 일간 |변화| 중앙   종합 3.12% / KOSPI200 3.85% / KRX100 3.82% / KOSDAQ 1.99%
패널 동일가중 일간 표준편차  2.57%
```
**분산된 지수가 개별종목 동일가중보다 더 흔들릴 수는 없다.**
그런데 close 가 항상 [low,high] 안에 있어 행 단위로는 일관된다 - API 가 주는 값이 그렇다.

**남의 지수를 믿는 대신 우리 패널로 만든다.** 그러면
- 둘이 맞으면 API 가 옳고 (127) 결론이 산다
- 안 맞으면 API 를 잣대로 쓸 수 없고 (127) 을 다시 해야 한다

## 만드는 법

`시총가중 지수` = sum(shares_i * price_i) 의 변화율.
상장주식수는 `_cache/pykrx_fundamental_latest.csv`(생산이 쓰는 소스, 2,873종목 100% 채움).
**과거 주식수 이력이 없으므로 현재 주식수를 고정으로 쓴다** - 이건 근사다.
분할·증자가 있으면 과거 시총이 틀어진다. 그래서 **수익률은 일별로 계산**하고
(수량 고정이면 일별 수익률은 가중평균이 되어 수량 오차의 영향이 작다), 누적한다.

기업행위/데이터 결함은 ±30.5% 로 거른다([[project_1data_panel_methodology]]).

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
    ap = argparse.ArgumentParser(description="자체 시총가중 지수 vs API 지수")
    ap.add_argument("--start", default="20150101")
    ap.add_argument("--min-value", type=float, default=0.0, help="0 이면 전 종목")
    ap.add_argument("--out", default=str(LOGS / "design" / "own_index_vs_api.csv"))
    args = ap.parse_args()

    d = load_merged()
    d = d[d["date"] >= args.start]
    px = d.pivot_table(index="date", columns="code", values="close", aggfunc="last")
    val = d.pivot_table(index="date", columns="code", values="value", aggfunc="last")
    px.index = px.index.astype(str)
    val = val.reindex(index=px.index, columns=px.columns)
    print("[자체지수] 패널 %d거래일 x %d종목" % px.shape)

    sh = pd.read_csv(ROOT / "_cache" / "pykrx_fundamental_latest.csv", dtype=str, encoding="utf-8-sig")
    sh["code"] = sh["code"].astype(str).str.zfill(6)
    sh["listed_shares"] = pd.to_numeric(sh["listed_shares"], errors="coerce")
    shares = sh.dropna(subset=["listed_shares"]).drop_duplicates("code").set_index("code")["listed_shares"]
    cover = px.columns.isin(shares.index)
    print("  상장주식수 확보 %d / %d 종목 (%.1f%%)"
          % (cover.sum(), len(px.columns), 100 * cover.mean()))

    r1 = px / px.shift(1) - 1.0
    r1 = r1.where((r1 <= CA_HI - 1) & (r1 >= CA_LO - 1))

    w_cap = px.shift(1).mul(shares.reindex(px.columns).to_numpy(), axis=1)   # 전일 시총
    if args.min_value > 0:
        m = val.shift(1) >= args.min_value
        w_cap = w_cap.where(m)
    w_cap = w_cap.where(r1.notna())
    w = w_cap.div(w_cap.sum(axis=1), axis=0)

    cap_ret = (r1 * w).sum(axis=1, min_count=1)          # 시총가중
    eq_ret = r1.mean(axis=1)                              # 동일가중
    n_used = w.notna().sum(axis=1)

    ix = pd.read_csv(LOGS / "index_daily_history.csv", dtype=str, encoding="utf-8-sig")
    ix["close"] = pd.to_numeric(ix["close"], errors="coerce")
    api = {}
    for code, name in (("0001", "KOSPI"), ("1001", "KOSDAQ"), ("2001", "KOSPI200")):
        s = ix[ix["index_code"] == code].set_index("date")["close"]
        s.index = s.index.astype(str)
        api[name] = s.reindex(px.index).astype(float).pct_change()

    cmp_ = pd.DataFrame({"자체_시총가중": cap_ret, "자체_동일가중": eq_ret, **api}).dropna(how="all")
    cmp_ = cmp_[cmp_.index >= cmp_.index[70]]             # 워밍업

    print()
    print("%-16s %10s %10s %12s" % ("계열", "일간표준편차", "연환산", "자체시총가중과 상관"))
    base = cmp_["자체_시총가중"]
    for c in cmp_.columns:
        s = cmp_[c].dropna()
        if len(s) < 100:
            continue
        corr = cmp_[[c, "자체_시총가중"]].dropna().corr().iloc[0, 1]
        print("%-16s %9.2f%% %9.1f%% %12.3f"
              % (c, 100 * s.std(), 100 * s.std() * np.sqrt(252), corr))

    print()
    print("누적 (%s ~ %s)" % (cmp_.index[0], cmp_.index[-1]))
    for c in cmp_.columns:
        s = cmp_[c].fillna(0)
        cum = float((1 + s).prod())
        yrs = len(s) / 247.0
        print("  %-16s %7.2f배   CAGR %6.2f%%   MDD %6.1f%%"
              % (c, cum, 100 * (cum ** (1 / yrs) - 1),
                 100 * (((1 + s).cumprod()) / ((1 + s).cumprod()).cummax() - 1).min()))

    print()
    print("  자체 지수 편입 종목수: 중앙 %d  최소 %d  최대 %d"
          % (n_used.median(), n_used.min(), n_used.max()))

    out = cmp_.copy()
    out.insert(0, "date", out.index)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False, encoding="utf-8-sig")
    print("  wrote %s (%d행)" % (args.out, len(out)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
