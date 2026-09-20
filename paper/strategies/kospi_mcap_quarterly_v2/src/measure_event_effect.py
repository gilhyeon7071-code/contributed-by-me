"""H1 검증 2 — 사건 효과를 **대조군 기준**으로 잰다. 지수 대비가 아니라 크기가 비슷한 비사건 종목 대비.

    python -m paper.strategies.kospi_mcap_quarterly_v2.src.measure_event_effect --events-dir <폴더> [--types ...] [--k 5]

왜 지수 대비로는 안 되나 (2026-09-18 실측)
  2026-07-28 KOSPI200 −10.8% 인데 KOSPI 종목 수익률 중앙값은 −3.5% 였다.
  소형주는 가만히 있어도 지수 대비 +7%p 가 찍힌다. 사건 종목은 대부분 소형주다.

재는 값 (전부 **시가 진입 기준** — 공시 다음 거래일 t0 시가에 사서 t0+k 종가에 판다)
  사건 수익  = 종목 t0 시가 → t0+k 종가
  대조 수익  = 짝지은 대조 종목들의 **같은 날짜** 같은 방식 수익의 중앙값
  사건 효과  = 사건 수익 − 대조 수익      ← 이게 판정 대상
  부호 검정  = 사건 효과가 양인 비율, 이항검정 p (효과 없음이면 50%)

  **날짜 기준이 정본이에요.** 같은 날 여러 종목이 함께 사건을 맞으면(투자주의 230건이 51일에 몰림)
  같은 시장 흐름을 타니 독립 표본이 아니에요. 종목 단위로 세면 표본이 실제보다 커 보여요
  (2026-09-18 실측: 투자주의 1일 종목 p 0.002 → 날짜 p 0.161).
  그래서 날짜별 중앙값을 하나의 관측으로 보고 다시 센 `day_*` 값을 판정에 씁니다.

기록: `event_effect_<k>d.json` (요약) + `event_effect_rows.jsonl` (사건별 값, append-only)
"""
from __future__ import annotations

import argparse
import json
import math
import statistics as st
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


def load_prices(path: Path) -> Dict[str, Dict[str, Dict[str, float]]]:
    df = pd.read_parquet(path, columns=["code", "date", "open", "close"])
    out: Dict[str, Dict[str, Dict[str, float]]] = {}
    for code, d, o, c in zip(df["code"].astype(str), df["date"].astype(str), df["open"], df["close"]):
        if c and c > 0:
            out.setdefault(code, {})[d] = {"open": float(o) if o and o > 0 else None, "close": float(c)}
    return out


def trading_days_of(px: Dict[str, Dict[str, Dict[str, float]]]) -> List[str]:
    days = set()
    for series in px.values():
        days.update(series)
    return sorted(days)


def open_to_close(px, code: str, t0: str, k: int, days: List[str]) -> Optional[float]:
    """t0 시가에 사서 t0+k 종가에 판 수익률. 값이 없으면 None(0 으로 채우지 않는다)."""
    if t0 not in days:
        return None
    i = days.index(t0)
    if i + k >= len(days):
        return None
    o = (px.get(code, {}).get(t0) or {}).get("open")
    c = (px.get(code, {}).get(days[i + k]) or {}).get("close")
    if not o or not c:
        return None
    return c / o - 1


def binom_p_two_sided(pos: int, n: int) -> Optional[float]:
    """효과가 없으면 부호는 반반. 그 가정에서 이만큼 치우칠 확률."""
    if n == 0:
        return None
    def comb_sum(lo, hi):
        return sum(math.comb(n, i) for i in range(lo, hi + 1))
    tail = comb_sum(max(pos, n - pos), n) + comb_sum(0, min(pos, n - pos))
    return min(1.0, tail / (2 ** n))


def measure(events: List[Dict[str, Any]], pairs: Dict[str, List[str]], px, k: int,
            next_trading_day) -> Dict[str, Any]:
    days = trading_days_of(px)
    rows = []
    for e in events:
        t0 = next_trading_day(e["rcept_dt"])
        if t0 is None:
            continue
        ev = open_to_close(px, e["stock_code"], t0, k, days)
        if ev is None:
            continue
        ctrl = [open_to_close(px, c, t0, k, days) for c in pairs.get(e["stock_code"], [])]
        ctrl = [c for c in ctrl if c is not None]
        if not ctrl:
            continue
        cm = st.median(ctrl)
        rows.append({"event_id": e["event_id"], "event_type": e["event_type"], "stock_code": e["stock_code"],
                     "corp_name": e.get("corp_name"), "t0": t0, "k": k, "event_ret": ev, "control_ret": cm,
                     "controls_used": len(ctrl), "effect": ev - cm})
    out: Dict[str, Any] = {"k": k, "rows": len(rows), "by_type": {}}
    for t in sorted({r["event_type"] for r in rows}):
        v = [r["effect"] for r in rows if r["event_type"] == t]
        pos = sum(1 for x in v if x > 0)
        byday: Dict[str, List[float]] = {}
        for r in rows:
            if r["event_type"] == t:
                byday.setdefault(r["t0"], []).append(r["effect"])
        day_eff = [st.median(x) for x in byday.values()]
        day_pos = sum(1 for x in day_eff if x > 0)
        out["by_type"][t] = {
            "n": len(v), "median_effect": st.median(v), "mean_effect": st.mean(v),
            "positive_ratio": pos / len(v), "binom_p": binom_p_two_sided(pos, len(v)),
            "day_n": len(day_eff), "day_median_effect": st.median(day_eff),
            "day_positive_ratio": day_pos / len(day_eff) if day_eff else None,
            "day_binom_p": binom_p_two_sided(day_pos, len(day_eff)),
            "median_event_ret": st.median([r["event_ret"] for r in rows if r["event_type"] == t]),
            "median_control_ret": st.median([r["control_ret"] for r in rows if r["event_type"] == t]),
            "stdev_effect": st.pstdev(v) if len(v) > 1 else None,
        }
    return {"summary": out, "rows": rows}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events-dir", required=True, type=Path)
    ap.add_argument("--types", default="BUYBACK_DIRECT,BUYBACK_TRUST")
    ap.add_argument("--k", type=int, default=5)
    ap.add_argument("--prices", default="prices_krx.parquet")
    ap.add_argument("--control-set", default="control_set.json")
    ap.add_argument("--exclude-amendments", action="store_true", default=True)
    ap.add_argument("--events-file", default="h1_events.jsonl")
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    ed = args.events_dir
    events = [json.loads(x) for x in (ed / args.events_file).read_text(encoding="utf-8").splitlines() if x.strip()]
    types = {t.strip() for t in args.types.split(",") if t.strip()}
    events = [e for e in events if e["event_type"] in types and e["stock_code"]]
    if args.exclude_amendments:
        events = [e for e in events if not e.get("is_amendment")]
    seen = set()
    uniq = []
    for e in sorted(events, key=lambda x: (x["rcept_dt"], x["event_id"])):
        key = (e["stock_code"], e["rcept_dt"], e["event_type"])
        if key in seen:
            continue
        seen.add(key)
        uniq.append(e)
    cs = json.loads((ed / args.control_set).read_text(encoding="utf-8"))
    pairs = cs["pairs"]
    px = load_prices(ed / args.prices)
    days = trading_days_of(px)

    def ntd(ymd: str):
        later = [d for d in days if d > ymd]
        return later[0] if later else None

    res = measure(uniq, pairs, px, args.k, ntd)
    lv = cs.get("match_level", {})
    for r in res["rows"]:
        r["match_level"] = lv.get(r["stock_code"])
    res["summary"]["control_set"] = args.control_set
    res["summary"]["match_level_counts"] = {k: sum(1 for r in res["rows"] if r.get("match_level") == k)
                                            for k in sorted(set(lv.values()))}
    (ed / f"event_effect_{args.k}d.json").write_text(
        json.dumps({"measured_at": datetime.now().isoformat(timespec="seconds"), "types": sorted(types),
                    "events_considered": len(uniq), **res["summary"]}, ensure_ascii=False, indent=2), encoding="utf-8")
    with (ed / "event_effect_rows.jsonl").open("a", encoding="utf-8") as fh:
        for r in res["rows"]:
            fh.write(json.dumps({**r, "measured_at": datetime.now().isoformat(timespec="seconds")},
                                ensure_ascii=False) + "\n")
    print(json.dumps({"events_considered": len(uniq), **res["summary"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
