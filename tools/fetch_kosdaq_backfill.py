# -*- coding: utf-8 -*-
"""
fetch_kosdaq_backfill.py -- one-shot backfill of KOSDAQ 2020-01 ~ 2022-09.

Why this exists
---------------
krx_daily_archive is KOSPI-only before 2025, and Raw/krx_daily_20221001_20251224
.parquet (RAW_WIDE) only starts 2022-10. That leaves KOSDAQ missing for the first
2.75 years, which is the binding constraint on effective sample size for the
research loop (see .agent/PLANS.md 2026-08-19 (6)).

Roster
------
pykrx cannot supply a historical roster -- get_market_ticker_list returns 0 rows
and the by-ticker endpoints are dead as of 2026-08-19. Using today's roster would
make the backfill survivor-only, which defeats the point. Instead the roster is
taken from RAW_WIDE's 2022 KOSDAQ membership: 1,632 codes, contemporaneous with
the target window and including names delisted since.

Known limitations, deliberately accepted
----------------------------------------
- The per-code endpoint does not return 거래대금, so `value` is close * volume.
  Checked against RAW_WIDE where both exist: median ratio 1.0003, and the
  1e8 liquidity floor agrees on 99.888% of rows (eligible set +0.035%).
- Codes that listed AND delisted entirely between 2020-01 and 2022-09 are absent
  from the 2022 roster and cannot be recovered this way. The panel remains
  slightly survivor-biased for that window.

Writes one parquet in krx_daily_archive with an explicit market column, so
research_loop._market_map() will not mistake it for a KOSPI-only archive file.

Read-only with respect to every existing file.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_WIDE = BASE_DIR / "Raw" / "krx_daily_20221001_20251224.parquet"
OUT_DIR = BASE_DIR / "krx_daily_archive"
START, END = "20200102", "20220930"
OUT_NAME = f"krx_daily_{START}_{END}_kosdaq_clean.parquet"

COL_MAP = {"시가": "open", "고가": "high", "저가": "low", "종가": "close",
           "거래량": "volume", "등락률": "change_rate"}


PANEL_CACHE = BASE_DIR / "2_Logs" / "research_loop_panel_cache.parquet"


def roster(source: str) -> tuple[list[str], str]:
    """Contemporaneous code list for the window being fetched.

    'raw2022'  -- RAW_WIDE's 2022 KOSDAQ membership, for the 2020-2022 backfill.
    'panel2020'-- every code the panel carries during 2020, for a pre-2020 fetch.
                  Both markets. Names delisted before 2020 are unrecoverable and
                  that residual survivorship is recorded with the result.
    """
    if source == "raw2022":
        raw = pd.read_parquet(RAW_WIDE, columns=["code", "date", "market"])
        raw["code"] = raw["code"].astype(str).str.zfill(6)
        raw["date"] = raw["date"].astype(str).str.replace("-", "", regex=False).str[:8]
        codes = raw.loc[(raw["market"] == "KOSDAQ") & (raw["date"] < "20230101"), "code"]
        return sorted(set(codes)), "RAW_WIDE 2022 KOSDAQ membership"
    if source == "panel2020":
        c = pd.read_parquet(PANEL_CACHE, columns=["code", "date"])
        c["code"] = c["code"].astype(str).str.zfill(6)
        return sorted(set(c.loc[c["date"] < "20210101", "code"])), "panel 2020 membership"
    raise ValueError(f"unknown roster source: {source}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="fetch only the first N codes (smoke test)")
    ap.add_argument("--out", default="")
    ap.add_argument("--sleep", type=float, default=0.0, help="seconds between calls")
    ap.add_argument("--start", default=START)
    ap.add_argument("--end", default=END)
    ap.add_argument("--roster", choices=["raw2022", "panel2020"], default="raw2022")
    ap.add_argument("--market-label", default="KOSDAQ",
                    help="value written to the market column; use '' to resolve per code later")
    args = ap.parse_args()

    from pykrx import stock

    start, end = args.start, args.end
    codes, roster_desc = roster(args.roster)
    if args.limit:
        codes = codes[: args.limit]
    print(f"[roster] {len(codes):,} codes from {roster_desc}")
    print(f"[window] {start} ~ {end}")
    if not args.out:
        args.out = str(OUT_DIR / f"krx_daily_{start}_{end}_backfill_clean.parquet")

    out_path = Path(args.out)
    if out_path.exists():
        print(f"[abort] {out_path} already exists -- refusing to overwrite")
        return 1

    frames, failed, empty = [], [], []
    t0 = time.time()
    for i, code in enumerate(codes, 1):
        try:
            df = stock.get_market_ohlcv_by_date(start, end, code)
        except Exception as exc:
            failed.append((code, type(exc).__name__))
            continue
        if df is None or df.empty:
            empty.append(code)
            continue
        df = df.rename(columns=COL_MAP)
        keep = [c for c in ("open", "high", "low", "close", "volume", "change_rate") if c in df.columns]
        df = df[keep].reset_index()
        df = df.rename(columns={df.columns[0]: "date"})
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y%m%d")
        df["code"] = code
        frames.append(df)
        if i % 200 == 0:
            el = time.time() - t0
            print(f"  {i:5d}/{len(codes)}  ok={len(frames)} empty={len(empty)} fail={len(failed)}  "
                  f"{el:.0f}s elapsed, ~{el / i * (len(codes) - i):.0f}s left")
        if args.sleep:
            time.sleep(args.sleep)

    if not frames:
        print("[abort] nothing fetched")
        return 1

    panel = pd.concat(frames, ignore_index=True)
    if args.market_label:
        panel["market"] = args.market_label
    else:
        # resolve each code against what the panel already knows
        known = pd.read_parquet(PANEL_CACHE, columns=["code", "market"]).drop_duplicates("code")
        known["code"] = known["code"].astype(str).str.zfill(6)
        m = dict(zip(known["code"], known["market"]))
        panel["market"] = panel["code"].map(m).fillna("UNKNOWN")
        print(f"[market] resolved: {panel.drop_duplicates('code')['market'].value_counts().to_dict()}")
    # the per-code endpoint has no 거래대금; close*volume tracks it to a median
    # ratio of 1.0003 and agrees with the 1e8 floor on 99.888% of RAW_WIDE rows
    panel["value"] = panel["close"].astype("float64") * panel["volume"].astype("float64")
    panel = panel[["date", "code", "market", "open", "high", "low",
                   "close", "volume", "value", "change_rate"]]
    panel = panel.drop_duplicates(subset=["date", "code"], keep="last")
    panel = panel.sort_values(["code", "date"]).reset_index(drop=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(out_path, index=False)

    el = time.time() - t0
    print()
    print(f"[done] {el:.0f}s")
    print(f"  rows          : {len(panel):,}")
    print(f"  codes         : {panel['code'].nunique():,}")
    print(f"  dates         : {panel['date'].nunique():,}  {panel['date'].min()} ~ {panel['date'].max()}")
    print(f"  nonzero close : {int((panel['close'] > 0).sum()):,} "
          f"({100 * (panel['close'] > 0).mean():.1f}%)")
    print(f"  empty codes   : {len(empty)}")
    print(f"  failed codes  : {len(failed)}")
    if failed[:5]:
        print(f"    sample: {failed[:5]}")
    print(f"  wrote         : {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
