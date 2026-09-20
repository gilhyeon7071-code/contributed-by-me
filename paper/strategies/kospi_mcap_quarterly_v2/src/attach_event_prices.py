"""H1 수집기 3 — 사건 뒤 가격을 붙인다. "사건이 몇 번 있나" 를 "가격을 미나" 로 바꾸는 단계.

    python -m paper.strategies.kospi_mcap_quarterly_v2.src.attach_event_prices --events-dir <폴더> [--types BUYBACK_DIRECT,BUYBACK_TRUST]

무엇을 / 어디서
  종목 일별 시세  **`prices_adj.parquet` (한투 수정주가, 기본)** — 없으면 `--price-source archive`
                 아카이브는 원주가·수정주가가 섞여 있어 사건 분석에 그대로 쓰면 없는 −50% 가 생겨요(09-18 실측)
  시장 일별 시세  `2_Logs/index_daily_history.csv` 의 KOSPI200(2001) — 종가와 시가 둘 다 (읽기만)

재는 값 — **두 가지 기준**
  기준일 t0 = 접수일의 **다음 거래일**(접수 시각이 장중·장후로 갈리니 하루 늦춰 잡는다. 보수적)
  1) 종가 기준  car_k    = t0 당일(t0-1 종가 대비)부터 k일까지 초과수익 합 — "값이 움직였나"
  2) **시가 진입 기준** car_open_k = t0 시가에 사서 t0+k 종가에 판 초과수익 — "우리가 먹을 수 있나"
     gap = t0 시가 ÷ t0-1 종가 − 1 (지수 대비). 공시가 장후면 이 갭은 **우리가 못 먹는 부분**이에요
  중복: 같은 종목·같은 t0·같은 사건은 한 번만 남겨요(기재정정 등으로 겹침)
  기업행위 의심: 창 안에 일간 초과수익 |31%| 초과가 있으면 `ca_suspect=true` 로 표시해요

**아직 판정이 아니에요.** 이 값은 "사건 뒤에 값이 어떻게 움직였나" 이고,
"그래서 사도 되나" 는 비용·체결 가정과 표본 수를 갖춘 뒤에 별도로 판정해요.

기록: `event_price_windows.jsonl` — event_id 로 한 줄, 창이 아직 안 찬 건 status=INCOMPLETE 로 남기고
나중에 다시 돌리면 채워요(append-only, 마지막 줄이 최신).
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
ARCHIVE = ROOT / "krx_daily_archive"
INDEX_CSV = ROOT / "2_Logs" / "index_daily_history.csv"
PRE, POST = 5, 20
INDEX_CODE = "2001"  # KOSPI200
PRICE_LIMIT = 0.31  # 가격제한 ±30% 밖 움직임 = 기업행위 의심


def load_index(path: Path = INDEX_CSV, index_code: str = INDEX_CODE) -> Dict[str, Dict[str, float]]:
    """{날짜: {close, open}} — 열 순서는 date, code, name, close, open, high, low, volume, ts"""
    out: Dict[str, Dict[str, float]] = {}
    with Path(path).open(encoding="utf-8") as fh:
        for row in csv.reader(fh):
            if len(row) > 4 and row[1] == index_code:
                try:
                    c, o = float(row[3]), float(row[4])
                except ValueError:
                    continue
                if c > 0:
                    out[row[0]] = {"close": c, "open": o if o > 0 else c}
    return out


def _files_for(start: str, end: str) -> List[Path]:
    """파일명 범위가 [start, end] 와 겹치는 것만. 전체를 읽지 않는다."""
    out = []
    for p in sorted(ARCHIVE.glob("*clean.parquet")):
        m = re.search(r"(\d{8})_(\d{8})", p.name)
        if not m:
            continue
        if m.group(2) >= start and m.group(1) <= end:
            out.append(p)
    return out


def load_prices(codes: set, start: str, end: str, source: str = "adj",
                adj_path: Optional[Path] = None) -> pd.DataFrame:
    """수정주가 파일이 기본. archive 는 기준이 섞여 있어 대조용으로만."""
    if source == "adj":
        if adj_path is None or not Path(adj_path).exists():
            raise SystemExit(f"수정주가 파일이 없어요: {adj_path} (fetch_adjusted_prices 먼저)")
        out = pd.read_parquet(adj_path, columns=["code", "date", "open", "close"])
        out = out[out["code"].isin(codes)].copy()
    else:
        frames = []
        for p in _files_for(start, end):
            df = pd.read_parquet(p, columns=["date", "code", "open", "close"])
            df = df[df["code"].isin(codes)]
            if not df.empty:
                frames.append(df)
        if not frames:
            return pd.DataFrame(columns=["date", "code", "open", "close"])
        out = pd.concat(frames, ignore_index=True)
    out["date"] = out["date"].astype(str)
    out = out[(out["date"] >= start) & (out["date"] <= end) & (out["close"] > 0)]  # 휴장일 0 패딩 제거
    return out.drop_duplicates(["date", "code"], keep="last").sort_values(["code", "date"])


def window_for(code: str, t0: str, px: Dict[str, Dict[str, Dict[str, float]]], idx: Dict[str, Dict[str, float]],
               trading_days: List[str]) -> Dict[str, Any]:
    """t0 앞뒤 오프셋별 초과수익(종가 기준)과, t0 시가에 산 경우(시가 진입 기준) 둘 다."""
    if t0 not in trading_days:
        return {"status": "T0_NOT_TRADING_DAY"}
    i0 = trading_days.index(t0)
    lo, hi = i0 - PRE - 1, i0 + POST
    if lo < 0:
        return {"status": "NO_PRE_WINDOW"}
    days = trading_days[lo:hi + 1]
    rows = []
    car = 0.0
    prev_s = prev_m = None
    complete = i0 + POST < len(trading_days)
    for d in days:
        bar = px.get(code, {}).get(d) or {}
        s, so = bar.get("close"), bar.get("open")
        m = idx.get(d, {}).get("close")
        off = trading_days.index(d) - i0
        rec = {"offset": off, "date": d, "close": s, "open": so, "stock_ret": None, "index_ret": None,
               "excess": None, "car": None}
        if s and m and prev_s and prev_m:
            sr, mr = s / prev_s - 1, m / prev_m - 1
            rec.update({"stock_ret": sr, "index_ret": mr, "excess": sr - mr})
            if off >= 0:
                car += sr - mr
                rec["car"] = car
        if s:
            prev_s = s
        if m:
            prev_m = m
        rows.append(rec)
    car_at = {f"car_{k}": next((r["car"] for r in rows if r["offset"] == k and r["car"] is not None), None)
              for k in (0, 1, 5, 10, 20)}

    # 시가 진입 기준 — t0 시가에 사서 t0+k 종가에 판다. 지수도 같은 방식(시가→종가)
    o0 = (px.get(code, {}).get(t0) or {}).get("open")
    i_o0 = idx.get(t0, {}).get("open")
    prev_close = (px.get(code, {}).get(trading_days[i0 - 1]) or {}).get("close")
    i_prev_close = idx.get(trading_days[i0 - 1], {}).get("close")
    open_at: Dict[str, Any] = {f"car_open_{k}": None for k in (0, 1, 5, 10, 20)}
    gap = None
    if o0 and i_o0 and prev_close and i_prev_close:
        gap = (o0 / prev_close - 1) - (i_o0 / i_prev_close - 1)
        for k in (0, 1, 5, 10, 20):
            j = i0 + k
            if j >= len(trading_days):
                continue
            d = trading_days[j]
            s = (px.get(code, {}).get(d) or {}).get("close")
            m = idx.get(d, {}).get("close")
            if s and m:
                open_at[f"car_open_{k}"] = (s / o0 - 1) - (m / i_o0 - 1)

    missing = [r["offset"] for r in rows if r["offset"] >= 0 and r["close"] is None]
    ca = any(r["excess"] is not None and abs(r["excess"]) > PRICE_LIMIT for r in rows)
    return {"status": "COMPLETE" if complete and not missing else "INCOMPLETE",
            "missing_offsets": missing, "ca_suspect": ca, "gap_excess": gap, "rows": rows, **car_at, **open_at}


def dedupe(events: List[Dict[str, Any]], t0_of) -> Tuple[List[Dict[str, Any]], int]:
    """같은 종목·같은 기준일·같은 사건은 한 번만. 기재정정 등으로 겹친 건 독립 표본이 아니에요."""
    seen = set()
    out, dropped = [], 0
    for e in sorted(events, key=lambda x: (x["rcept_dt"], x["event_id"])):
        k = (e["stock_code"], t0_of(e["rcept_dt"]), e["event_type"])
        if k in seen:
            dropped += 1
            continue
        seen.add(k)
        out.append(e)
    return out, dropped


def run(events_dir: Path, types: List[str], now: datetime, index_csv: Path = INDEX_CSV,
        price_source: str = "adj") -> Dict[str, Any]:
    events_dir = Path(events_dir)
    events = [json.loads(x) for x in (events_dir / "h1_events.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    events = [e for e in events if e["event_type"] in types and e["stock_code"]]
    out_path = events_dir / "event_price_windows.jsonl"
    done: Dict[str, str] = {}
    if out_path.exists():
        for line in out_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                d = json.loads(line)
                if d.get("price_source", "archive") == price_source:  # 출처가 다르면 다시 잰다
                    done[d["event_id"]] = d["status"]
    todo = [e for e in events if done.get(e["event_id"]) != "COMPLETE"]
    res = {"run_at": now.isoformat(timespec="seconds"), "events": len(events), "todo": len(todo),
           "complete": 0, "incomplete": 0, "skipped": 0, "status": "OK", "reasons": []}
    if not todo:
        return res
    idx = load_index(index_csv)
    trading_days = sorted(idx)
    todo, dropped = dedupe(todo, lambda ymd: _next_trading_day(trading_days, ymd))
    res["duplicates_dropped"] = dropped
    res["todo"] = len(todo)
    start = min(e["rcept_dt"] for e in todo)
    end = max(trading_days[-1], max(e["rcept_dt"] for e in todo))
    codes = {e["stock_code"] for e in todo}
    prices = load_prices(codes, _shift(trading_days, start, -PRE - 2), end, price_source,
                         events_dir / "prices_adj.parquet")
    px: Dict[str, Dict[str, Dict[str, float]]] = {}
    for code, d, o, c in zip(prices["code"], prices["date"], prices["open"], prices["close"]):
        px.setdefault(str(code), {})[str(d)] = {"open": float(o) if o and o > 0 else None, "close": float(c)}
    rows_out = []
    for e in todo:
        t0 = _next_trading_day(trading_days, e["rcept_dt"])
        if t0 is None:
            res["skipped"] += 1
            continue
        w = window_for(e["stock_code"], t0, px, idx, trading_days)
        if w["status"] in ("T0_NOT_TRADING_DAY", "NO_PRE_WINDOW"):
            res["skipped"] += 1
            continue
        rows_out.append({"event_id": e["event_id"], "event_type": e["event_type"], "stock_code": e["stock_code"],
                         "corp_name": e.get("corp_name"), "rcept_dt": e["rcept_dt"], "t0": t0,
                         "direction": e.get("direction"), "price_source": price_source,
                         "observed_at": now.isoformat(timespec="seconds"), **w})
        res["complete" if w["status"] == "COMPLETE" else "incomplete"] += 1
    if rows_out:
        with out_path.open("a", encoding="utf-8") as fh:
            for r in rows_out:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    with (events_dir / "collect_log.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"tool": "attach_event_prices", **res}, ensure_ascii=False) + "\n")
    return res


def _next_trading_day(trading_days: List[str], ymd: str) -> Optional[str]:
    for d in trading_days:
        if d > ymd:
            return d
    return None


def _shift(trading_days: List[str], ymd: str, n: int) -> str:
    cand = [d for d in trading_days if d <= ymd]
    if not cand:
        return ymd
    i = max(0, trading_days.index(cand[-1]) + n)
    return trading_days[i]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events-dir", required=True, type=Path)
    ap.add_argument("--types", default="BUYBACK_DIRECT,BUYBACK_TRUST,BUYBACK_DISPOSAL,RIGHTS_OFFERING")
    ap.add_argument("--price-source", choices=["adj", "archive"], default="adj")
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    res = run(args.events_dir, [t.strip() for t in args.types.split(",") if t.strip()], datetime.now(),
              price_source=args.price_source)
    print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0 if res["status"] == "OK" else 3


if __name__ == "__main__":
    raise SystemExit(main())
