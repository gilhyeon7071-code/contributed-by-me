"""H1 재검증 — 공시일이 아니라 **실제로 사는 기간**을 잰다.

    python -m paper.strategies.kospi_mcap_quarterly_v2.src.measure_buyback_period --events-dir <폴더>

왜 다시 재나
  가설의 핵심은 공시가 아니라 **압력**이에요. 자기주식 취득 공시에는 취득 기간이 적혀 있어요
  (예: 07-10~10-09, 400만주, 40억원, 장내매수). 회사는 그동안 매일 시장에서 사요 — 가격과 무관하게.
  공시일 5일 창은 정보 효과에 가깝고, 이 측정은 **압력 효과**를 봐요.

  가설이 맞다면: 취득 기간 중에는 같은 종목의 평소보다 낫고, 기간이 끝나면 사라져야 해요.
  틀렸다면: 기간 안팎에 차이가 없어요. **둘 다 검증 가능한 예측이에요.**

재는 법 (같은 날 비교 — 시장 방향을 지운다)
  1. 종목별 하루 초과수익 = 그 종목 수익률 − 짝지은 대조 종목들의 중앙값 (H1·H2 와 같은 대조군)
  2. 날짜마다  기간 안 종목들의 평균 초과  −  기간 밖 종목들의 평균 초과  = 그날의 스프레드
  3. 스프레드의 부호를 날짜 단위로 센다(이항검정). **날짜가 관측 단위** — 종목은 서로 겹치니까

기록: `buyback_period_effect.json`
"""
from __future__ import annotations

import argparse
import json
import math
import statistics as st
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


def load_periods(events_dir: Path, types: List[str]) -> List[Dict[str, Any]]:
    rows = [json.loads(x) for x in (events_dir / "event_details.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    out = []
    for r in rows:
        if r.get("status") != "OK" or r.get("event_type") not in types:
            continue
        if not r.get("period_start") or not r.get("period_end"):
            continue
        out.append(r)
    return out


def daily_excess(prices: pd.DataFrame, pairs: Dict[str, List[str]], codes: List[str]) -> Dict[str, Dict[str, float]]:
    """{종목: {날짜: 초과수익}} — 대조군 중앙값을 뺀 값. 대조 가격이 없는 날은 만들지 않는다."""
    px: Dict[str, Dict[str, float]] = {}
    for code, d, c in zip(prices["code"].astype(str), prices["date"].astype(str), prices["close"]):
        if c and c > 0:
            px.setdefault(code, {})[d] = float(c)

    def rets(code: str) -> Dict[str, float]:
        s = px.get(code, {})
        days = sorted(s)
        return {days[i]: s[days[i]] / s[days[i - 1]] - 1 for i in range(1, len(days))}

    cache: Dict[str, Dict[str, float]] = {}
    out: Dict[str, Dict[str, float]] = {}
    for code in codes:
        r_s = rets(code)
        ctrl = [c for c in pairs.get(code, []) if c in px]
        if not r_s or not ctrl:
            continue
        for c in ctrl:
            cache.setdefault(c, rets(c))
        ex = {}
        for d, v in r_s.items():
            cv = [cache[c][d] for c in ctrl if d in cache[c]]
            if cv:
                ex[d] = v - st.median(cv)
        if ex:
            out[code] = ex
    return out


def binom_p(pos: int, n: int) -> Optional[float]:
    if n == 0:
        return None
    tail = sum(math.comb(n, i) for i in range(max(pos, n - pos), n + 1)) + \
        sum(math.comb(n, i) for i in range(0, min(pos, n - pos) + 1))
    return min(1.0, tail / (2 ** n))


def measure(periods: List[Dict[str, Any]], excess: Dict[str, Dict[str, float]]) -> Dict[str, Any]:
    by_code: Dict[str, List[Tuple[str, str]]] = {}
    for p in periods:
        by_code.setdefault(p["stock_code"], []).append((p["period_start"], p["period_end"]))
    days = sorted({d for s in excess.values() for d in s})
    rows = []
    for d in days:
        inside, outside = [], []
        for code, ex in excess.items():
            if d not in ex:
                continue
            spans = by_code.get(code)
            if not spans:
                continue
            active = any(s <= d <= e for s, e in spans)
            (inside if active else outside).append(ex[d])
        if inside and outside:
            rows.append({"date": d, "n_in": len(inside), "n_out": len(outside),
                         "in_mean": st.mean(inside), "out_mean": st.mean(outside),
                         "spread": st.mean(inside) - st.mean(outside)})
    spr = [r["spread"] for r in rows]
    pos = sum(1 for x in spr if x > 0)
    res = {"days": len(rows), "codes_with_period": len(by_code), "codes_measured": len(excess),
           "spread_mean_daily": st.mean(spr) if spr else None,
           "spread_median_daily": st.median(spr) if spr else None,
           "positive_days": pos, "positive_ratio": pos / len(spr) if spr else None,
           "binom_p": binom_p(pos, len(spr)),
           "annualized_spread": (st.mean(spr) * 252 * 100) if spr else None,
           "mdе_daily_pct": (2 * st.pstdev(spr) / math.sqrt(len(spr)) * 100) if len(spr) > 1 else None,
           "avg_in_per_day": st.mean([r["n_in"] for r in rows]) if rows else None,
           "avg_out_per_day": st.mean([r["n_out"] for r in rows]) if rows else None}
    return {"summary": res, "rows": rows}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events-dir", required=True, type=Path)
    ap.add_argument("--types", default="BUYBACK_DIRECT,BUYBACK_TRUST")
    ap.add_argument("--control-set", default="control_set_v2.json")
    ap.add_argument("--prices", default="prices_krx.parquet")
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    ed = args.events_dir
    types = [t.strip() for t in args.types.split(",") if t.strip()]
    periods = load_periods(ed, types)
    pairs = json.loads((ed / args.control_set).read_text(encoding="utf-8"))["pairs"]
    prices = pd.read_parquet(ed / args.prices, columns=["code", "date", "close"])
    codes = sorted({p["stock_code"] for p in periods})
    ex = daily_excess(prices, pairs, codes)
    res = measure(periods, ex)
    out = {"measured_at": datetime.now().isoformat(timespec="seconds"), "types": types,
           "periods": len(periods), **res["summary"]}
    (ed / "buyback_period_effect.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
