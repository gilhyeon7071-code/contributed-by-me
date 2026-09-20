"""H1 검증 1 — 대조군 만들기. 사건 종목과 **시총이 비슷한 비사건 종목**을 골라 같은 잣대로 잰다.

    python -m paper.strategies.kospi_mcap_quarterly_v2.src.build_control_set --events-dir <폴더> --universe <input_YYYYMMDD.csv> [--per-event 2]

왜 필요한가 (2026-09-18 실측)
  2026-07-28 KOSPI200 은 −10.8% 인데 KOSPI 종목 수익률 중앙값은 −3.5% 였다.
  대형주가 지수를 끌어내린 날이라, **소형주는 가만히 있어도 지수 대비 +7%p** 가 찍힌다.
  사건 종목은 대부분 소형주다 → 지수 대비 초과수익만 보면 사건 효과가 아니라 크기 효과를 본다.

방법 (v2, 2026-09-18 오후 — 업종·유동성까지 맞춤)
  후보 = 비사건 KOSPI 종목 중  ① **같은 업종**  ② 일 거래대금이 사건 종목의 [1/밴드, 밴드] 배 안
  그 후보들 중 **시총이 가장 가까운** N 개를 짝짓는다. 후보가 없으면 순서대로 느슨하게 풀고,
  어디까지 풀었는지를 `match_level` 로 남긴다(모르면 모른다고 적는다).
    match_level  SECTOR_LIQ_CAP → LIQ_CAP(업종 풂) → CAP(둘 다 풂)
  같은 종목이 여러 번 뽑히면 한 번만. 사건 종목은 대조군에서 뺀다.

기록: `control_set.json` (사건 종목 → 짝지은 대조 종목, 시총·업종·유동성·match_level 포함)
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


def liquidity_from_archive(archive_dir: Path, start: str, end: str) -> Dict[str, float]:
    """종목별 일 거래대금 중앙값. 아카이브에 빠진 날이 있어도 중앙값이라 쓸 수 있다(연속 계열이 아니어도 됨)."""
    import re
    vals: Dict[str, List[float]] = {}
    for f in sorted(Path(archive_dir).glob("*clean.parquet")):
        m = re.search(r"(\d{8})_(\d{8})", f.name)
        if not m or m.group(1) != m.group(2) or not (start <= m.group(1) <= end):
            continue
        df = pd.read_parquet(f, columns=["code", "close", "volume", "market"])
        df = df[(df["market"] == "KOSPI") & (df["close"] > 0)]
        for code, c, v in zip(df["code"].astype(str), df["close"], df["volume"]):
            vals.setdefault(code, []).append(float(c) * float(v or 0))
    return {k: float(pd.Series(v).median()) for k, v in vals.items() if v}


def build(events: List[Dict[str, Any]], universe: pd.DataFrame, per_event: int = 2,
          sectors: Optional[Dict[str, str]] = None, liquidity: Optional[Dict[str, float]] = None,
          band: float = 3.0) -> Dict[str, Any]:
    uni = universe[(universe["market"] == "KOSPI") & (universe["market_cap"] > 0)].copy()
    uni["code"] = uni["code"].astype(str)
    event_codes = {e["stock_code"] for e in events}
    pool = uni[~uni["code"].isin(event_codes)].copy().reset_index(drop=True)
    caps = dict(zip(uni["code"], uni["market_cap"]))
    sectors = sectors or {}
    liquidity = liquidity or {}
    pool["sector"] = pool["code"].map(sectors)
    pool["liq"] = pool["code"].map(liquidity)

    pairs: Dict[str, List[str]] = {}
    levels: Dict[str, str] = {}
    missing_cap = []
    for code in sorted(event_codes):
        cap = caps.get(code)
        if not cap or cap <= 0:
            missing_cap.append(code)
            continue
        sec, liq = sectors.get(code), liquidity.get(code)
        tries = []
        if sec and liq:
            tries.append(("SECTOR_LIQ_CAP", (pool["sector"] == sec) & pool["liq"].notna()
                          & (pool["liq"] >= liq / band) & (pool["liq"] <= liq * band)))
        if liq:
            tries.append(("LIQ_CAP", pool["liq"].notna() & (pool["liq"] >= liq / band) & (pool["liq"] <= liq * band)))
        tries.append(("CAP", pd.Series(True, index=pool.index)))
        for level, mask in tries:
            cand = pool[mask]
            if len(cand) >= per_event:
                near = (cand["market_cap"] - cap).abs().nsmallest(per_event).index
                pairs[code] = [str(pool.loc[i, "code"]) for i in near]
                levels[code] = level
                break
    controls = sorted({c for v in pairs.values() for c in v})
    return {"pairs": pairs, "match_level": levels, "controls": controls, "event_codes": sorted(event_codes),
            "missing_market_cap": missing_cap, "per_event": per_event, "liquidity_band": band,
            "caps": {c: float(caps[c]) for c in list(event_codes) + controls if c in caps},
            "sectors": {c: sectors.get(c) for c in list(event_codes) + controls if sectors.get(c)},
            "liquidity": {c: liquidity.get(c) for c in list(event_codes) + controls if liquidity.get(c)}}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events-dir", required=True, type=Path)
    ap.add_argument("--universe", required=True, type=Path, help="D1 산출 input_YYYYMMDD.csv (code·market·market_cap)")
    ap.add_argument("--types", default="BUYBACK_DIRECT,BUYBACK_TRUST")
    ap.add_argument("--per-event", type=int, default=2)
    ap.add_argument("--sector-master", type=Path,
                    default=Path(r"E:\1_Data\_cache\krx_current_industry_master_20260715_partial.csv"))
    ap.add_argument("--archive", type=Path, default=Path(r"E:\1_Data\krx_daily_archive"))
    ap.add_argument("--liq-start", default="20260801")
    ap.add_argument("--liq-end", default="20260917")
    ap.add_argument("--band", type=float, default=3.0)
    ap.add_argument("--out-name", default="control_set.json")
    ap.add_argument("--events-file", default="h1_events.jsonl")
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    events = [json.loads(x) for x in (args.events_dir / args.events_file).read_text(encoding="utf-8").splitlines() if x.strip()]
    types = {t.strip() for t in args.types.split(",") if t.strip()}
    events = [e for e in events if e["event_type"] in types and e["stock_code"]]
    uni = pd.read_csv(args.universe, dtype={"code": str})
    sectors = {}
    if args.sector_master and args.sector_master.exists():
        sm = pd.read_csv(args.sector_master, dtype={"code": str})
        sectors = {str(c): str(s) for c, s in zip(sm["code"], sm["industry_name"]) if isinstance(s, str)}
    liq = liquidity_from_archive(args.archive, args.liq_start, args.liq_end) if args.archive.exists() else {}
    out = build(events, uni, args.per_event, sectors, liq, args.band)
    out["built_at"] = datetime.now().isoformat(timespec="seconds")
    out["sector_master"] = str(args.sector_master)
    out["liquidity_window"] = [args.liq_start, args.liq_end]
    (args.events_dir / args.out_name).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    from collections import Counter
    print(json.dumps({"events": len(events), "event_codes": len(out["event_codes"]),
                      "controls": len(out["controls"]), "match_level": Counter(out["match_level"].values()),
                      "sectors_known": len(out["sectors"]), "liquidity_known": len(out["liquidity"]),
                      "missing_market_cap": out["missing_market_cap"]},
                     ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
