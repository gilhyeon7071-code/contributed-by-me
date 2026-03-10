from __future__ import annotations

from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Optional, List

import pandas as pd

try:
    from pykrx import stock
except Exception as e:
    raise SystemExit(f"[FATAL] pykrx import failed: {type(e).__name__}: {e}")

BASE = Path(r"E:\1_Data")
OUT = BASE / "paper" / "prices" / "ohlcv_paper.parquet"
OUT.parent.mkdir(parents=True, exist_ok=True)

def prev_weekday_lag1(d: date) -> date:
    x = d - timedelta(days=1)
    while x.weekday() >= 5:
        x -= timedelta(days=1)
    return x

def norm8(d: date) -> str:
    return d.strftime("%Y%m%d")

def parquet_date_max(p: Path) -> Optional[str]:
    if not p.exists() or p.stat().st_size == 0:
        return None
    try:
        df = pd.read_parquet(p, columns=["date"])
        if df.empty:
            return None
        v = df["date"].dropna().max()
        if hasattr(v, "strftime"):
            return v.strftime("%Y%m%d")
        s = str(v)
        return s.replace("-", "")[:8]
    except Exception:
        return None

def main():
    today = date.today()
    end = prev_weekday_lag1(today)
    end8 = norm8(end)

    last8 = parquet_date_max(OUT)
    if last8:
        start = datetime.strptime(last8, "%Y%m%d").date() + timedelta(days=1)
        while start.weekday() >= 5:
            start += timedelta(days=1)
    else:
        start = end - timedelta(days=30)

    if start > end:
        print(f"[PRICES] up-to-date: last={last8} end={end8}")
        return 0

    days: List[str] = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(norm8(d))
        d += timedelta(days=1)

    print(f"[PRICES] target dates: {days[0]}..{days[-1]} n={len(days)} out={OUT}")

    parts = []
    for ymd in days:
        df = stock.get_market_ohlcv_by_ticker(ymd)
        if df is None or df.empty:
            print(f"[PRICES] skip(no data): {ymd}")
            continue
        df = df.reset_index()
        first = df.columns[0]
        df = df.rename(columns={first: "code"})
        df["date"] = ymd

        ren = {}
        for c in df.columns:
            if c in ("code", "date"):
                continue
            cl = str(c).lower()
            if cl in ("시가", "open"): ren[c] = "open"
            elif cl in ("고가", "high"): ren[c] = "high"
            elif cl in ("저가", "low"): ren[c] = "low"
            elif cl in ("종가", "close"): ren[c] = "close"
            elif cl in ("거래량", "volume"): ren[c] = "volume"
            elif cl in ("거래대금", "value"): ren[c] = "value"
        df = df.rename(columns=ren)

        keep = [c for c in ["date","code","open","high","low","close","volume","value"] if c in df.columns]
        parts.append(df[keep])

    if not parts:
        raise SystemExit("[FATAL] no price parts fetched (all holidays?)")

    new = pd.concat(parts, ignore_index=True)

    if OUT.exists() and OUT.stat().st_size > 0:
        old = pd.read_parquet(OUT)
        merged = pd.concat([old, new], ignore_index=True).drop_duplicates(subset=["date","code"], keep="last")
    else:
        merged = new

    merged.to_parquet(OUT, index=False)
    dm = merged["date"].astype(str).max()
    print(f"[PRICES] wrote: {OUT} rows={len(merged)} date_max={dm}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
