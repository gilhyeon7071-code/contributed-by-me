# -*- coding: utf-8 -*-
"""
research_loop.py -- the "smallest complete loop" harness.

One signal. All stocks. No gates. One holding period. Costs applied. One number.

Read-only: reads price parquets only, writes nothing under E:\\1_Data except an
optional panel cache. Never touches Gate / LOCK / orders / fills / ledger.

Design and first result: .agent\\PLANS.md 2026-08-18 blocks (9) and (10).

Contract (do not change casually -- changing these makes rounds incomparable):
  - return source : change_rate when present, else close-to-close.
                    change_rate is 100% NaN for 2025 and 99.3% for 2026, so the
                    fallback is mandatory or the panel silently ends 2024-12-20.
  - data error    : |daily return| > 31% dropped (outside KRX daily limit).
  - entry / exit  : signal at close(t) -> enter close(t+1) -> exit close(t+1+hold).
  - cost          : production contract, 0.358% round trip.
  - baseline      : same day, same eligible universe, equal weight, same cost.
  - metric        : per-signal-day basket net return minus baseline, bootstrap CI.

Usage:
    python tools/research_loop.py --lookback 120
    python tools/research_loop.py --lookback 20 --hold 20 --topk 30
    python tools/research_loop.py --lookback 120 --direction bottom
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
PRICE_DIRS = [BASE_DIR / "krx_daily_archive", BASE_DIR / "_krx_manual"]
DEFAULT_CACHE = BASE_DIR / "2_Logs" / "research_loop_panel_cache.parquet"

COST_ROUNDTRIP = 0.00358      # production contract, see PLANS 2026-08-18 (2)
DAILY_LIMIT_PCT = 31.0        # KRX daily move limit; beyond this is a data error


def build_panel(cache: Path, rebuild: bool) -> pd.DataFrame:
    if cache.exists() and not rebuild:
        panel = pd.read_parquet(cache)
        print(f"[panel] cache hit: {cache}  rows={len(panel):,}")
        return panel

    files = []
    for d in PRICE_DIRS:
        for p in glob.glob(str(d / "krx_daily_*_clean.parquet")):
            if ".bak" in os.path.basename(p):
                continue
            files.append(p)
    print(f"[panel] reading {len(files)} parquet files")

    frames = []
    for p in files:
        try:
            frames.append(pd.read_parquet(p))
        except Exception as exc:
            print(f"[panel]   SKIP {os.path.basename(p)}: {type(exc).__name__}")
    panel = pd.concat(frames, ignore_index=True)
    print(f"[panel] raw rows: {len(panel):,}")

    panel["date"] = panel["date"].astype(str).str.replace("-", "", regex=False).str[:8]
    panel["code"] = panel["code"].astype(str).str.zfill(6)
    for col in ("close", "value", "change_rate"):
        panel[col] = pd.to_numeric(panel[col], errors="coerce")

    before = len(panel)
    panel = panel[panel["close"] > 0]
    print(f"[panel] close>0 filter removed {before - len(panel):,} "
          f"({100 * (before - len(panel)) / before:.1f}%) -- weekend zero padding")

    panel = panel.drop_duplicates(subset=["date", "code"], keep="last")
    panel = panel[["date", "code", "close", "value", "change_rate"]].sort_values(["code", "date"])

    prev = panel.groupby("code", sort=False)["close"].shift(1)
    implied = (panel["close"] / prev - 1.0) * 100.0
    from_cr = panel["change_rate"].notna().sum()
    panel["ret_pct"] = panel["change_rate"].where(panel["change_rate"].notna(), implied)
    print(f"[panel] returns: {from_cr:,} from change_rate, "
          f"{panel['ret_pct'].notna().sum() - from_cr:,} reconstructed from close")

    bad = (panel["ret_pct"].abs() > DAILY_LIMIT_PCT).sum()
    panel.loc[panel["ret_pct"].abs() > DAILY_LIMIT_PCT, "ret_pct"] = np.nan
    print(f"[panel] dropped {bad:,} rows with |return| > {DAILY_LIMIT_PCT}% as data errors")

    panel["market"] = panel["code"].map(_market_map()).fillna("UNKNOWN")
    mix = panel.drop_duplicates("code")["market"].value_counts().to_dict()
    print(f"[panel] market resolution (distinct codes): {mix}")

    panel = panel[["date", "code", "market", "value", "ret_pct"]]
    cache.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(cache, index=False)
    print(f"[panel] cached -> {cache}")
    return panel


def _market_map() -> dict:
    """code -> KOSPI / KOSDAQ.

    The pre-2025 archive is KOSPI-only by construction (verified 2026-08-19:
    0 KOSDAQ codes in 2020-2024, while 939/984 of today's KOSPI names are
    present). So a code appearing there was KOSPI at that time, including names
    that have since delisted -- filtering on "KOSPI today" alone would drop them
    and reintroduce survivorship. The map is therefore the union of:
      (a) every code in a pre-2025 KOSPI-only archive file, and
      (b) every code labelled KOSPI in a file that populates the market column.
    """
    kospi: set[str] = set()
    kosdaq: set[str] = set()

    for d in PRICE_DIRS:
        for p in glob.glob(str(d / "krx_daily_*_clean.parquet")):
            base = os.path.basename(p)
            if ".bak" in base:
                continue
            try:
                cols = pd.read_parquet(p, columns=["code", "market"])
            except Exception:
                # no market column -> only usable as a KOSPI-era membership hint
                try:
                    cols = pd.read_parquet(p, columns=["code"])
                except Exception:
                    continue
                cols["market"] = pd.NA
            cols["code"] = cols["code"].astype(str).str.zfill(6)
            labelled = cols.dropna(subset=["market"])
            labelled = labelled[labelled["market"].astype(str).isin(("KOSPI", "KOSDAQ"))]
            for code, market in labelled.drop_duplicates("code")[["code", "market"]].itertuples(index=False):
                (kospi if market == "KOSPI" else kosdaq).add(code)

            # pre-2025 wide archive files are KOSPI-only by construction
            digits = [t for t in base.replace(".parquet", "").split("_") if t.isdigit() and len(t) == 8]
            if digits and max(digits) < "20250101":
                kospi.update(cols["code"].unique().tolist())

    kospi -= kosdaq  # a code explicitly labelled KOSDAQ wins
    out = {c: "KOSPI" for c in kospi}
    out.update({c: "KOSDAQ" for c in kosdaq})
    return out


def bootstrap_ci(values: np.ndarray, n: int, rng: np.random.Generator) -> tuple[float, float]:
    v = np.asarray(values, dtype=float)
    v = v[~np.isnan(v)]
    if len(v) < 10:
        return float("nan"), float("nan")
    idx = rng.integers(0, len(v), size=(n, len(v)))
    means = np.sort(v[idx].mean(axis=1))
    return float(means[int(0.025 * n)]), float(means[int(0.975 * n)])


def run(args: argparse.Namespace) -> int:
    rng = np.random.default_rng(args.seed)
    panel = build_panel(Path(args.panel_cache), args.rebuild_panel)

    if args.market != "all":
        want = args.market.upper()
        before_codes = panel["code"].nunique()
        panel = panel[panel["market"] == want]
        print(f"[panel] market filter {want}: {before_codes:,} -> {panel['code'].nunique():,} codes")
        yearly = panel.assign(yr=panel["date"].str[:4]).groupby("yr")["code"].nunique()
        print("[panel] distinct codes per year after filter: "
              + ", ".join(f"{y}:{v}" for y, v in yearly.items()))

    rets = panel.pivot(index="date", columns="code", values="ret_pct") / 100.0
    vals = panel.pivot(index="date", columns="code", values="value").reindex_like(rets)
    rets = rets.sort_index()
    vals = vals.reindex_like(rets)
    print(f"[panel] {rets.shape[0]} dates x {rets.shape[1]} codes  "
          f"{rets.index.min()} ~ {rets.index.max()}")

    growth = 1.0 + rets
    signal = growth.rolling(args.lookback, min_periods=args.lookback).apply(np.prod, raw=True) - 1.0
    forward = (growth.rolling(args.hold, min_periods=args.hold).apply(np.prod, raw=True) - 1.0
               ).shift(-(1 + args.hold))
    eligible = (vals >= args.min_value) & signal.notna() & forward.notna()

    rows = []
    for day in rets.index:
        mask = eligible.loc[day]
        if mask.sum() < args.topk * 2:
            continue
        codes = mask[mask].index
        sig = signal.loc[day, codes]
        fwd = forward.loc[day, codes]
        picks = sig.nsmallest(args.topk).index if args.direction == "bottom" else sig.nlargest(args.topk).index
        rows.append({
            "date": day,
            "n_univ": int(mask.sum()),
            "basket_net": float(fwd[picks].mean()) - COST_ROUNDTRIP,
            "base_net": float(fwd.mean()) - COST_ROUNDTRIP,
        })

    res = pd.DataFrame(rows)
    if res.empty:
        print("no signal days produced -- check lookback/hold vs panel length")
        return 1
    res["excess"] = res["basket_net"] - res["base_net"]
    excess = res["excess"].dropna().to_numpy()
    mean = float(excess.mean())
    sd = float(excess.std(ddof=1))
    lo, hi = bootstrap_ci(excess, args.bootstrap, rng)
    periods_per_year = 252.0 / args.hold

    bar = "=" * 84
    print()
    print(bar)
    print(f"ROUND: lookback={args.lookback}d  hold={args.hold}d  top{args.topk} "
          f"({args.direction})  market={args.market}  min_value={args.min_value:,.0f}  "
          f"cost={COST_ROUNDTRIP:.5f} RT")
    print(bar)
    print(f"  signal days        : {len(excess)}   {res['date'].min()} ~ {res['date'].max()}")
    print(f"  median universe/day: {res['n_univ'].median():.0f}")
    print()
    print(f"  MEAN EXCESS        : {100 * mean:+.4f}%   per {args.hold}-day holding period")
    print(f"  bootstrap 95% CI   : [{100 * lo:+.4f}%, {100 * hi:+.4f}%]")
    print(f"  CI excludes zero   : {(lo > 0) or (hi < 0)}")
    print(f"  naive annualized   : {100 * mean * periods_per_year:+.2f}%")
    verdict = "POSITIVE EDGE" if lo > 0 else "NEGATIVE" if hi < 0 else "NO DETECTABLE EDGE"
    print(f"  >>> {verdict}")

    print()
    print("  supporting")
    print(f"    basket net mean  : {100 * res['basket_net'].mean():+.4f}%")
    print(f"    baseline net mean: {100 * res['base_net'].mean():+.4f}%")
    print(f"    win rate         : {100 * (res['excess'] > 0).mean():.1f}%")
    print(f"    excess sd        : {100 * sd:.3f}%")
    print(f"    info ratio (ann) : {mean / sd * np.sqrt(periods_per_year):.3f}")

    # concentration control -- Track C ret_without_top5 precedent
    ordered = np.sort(excess)[::-1]
    wo5 = ordered[5:]
    l5, h5 = bootstrap_ci(wo5, args.bootstrap, rng)
    print()
    print("  concentration control (drop the 5 best signal days)")
    print(f"    mean excess      : {100 * wo5.mean():+.4f}%   CI [{100 * l5:+.4f}%, {100 * h5:+.4f}%]")
    print(f"    depends on a few days: {abs(wo5.mean() - mean) > abs(mean) * 0.5}")

    print()
    print("  by year (diagnostic only, not a verdict)")
    res["yr"] = res["date"].str[:4]
    for year, grp in res.groupby("yr"):
        print(f"    {year}  days={len(grp):4d}  excess={100 * grp['excess'].mean():+7.4f}%  "
              f"basket={100 * grp['basket_net'].mean():+7.4f}%  "
              f"base={100 * grp['base_net'].mean():+7.4f}%")

    mde = 1.96 * sd / np.sqrt(len(excess))
    print()
    print("  power check")
    print(f"    n={len(excess)}  sd={100 * sd:.3f}%")
    print(f"    min detectable effect (95%): {100 * mde:.4f}% per period "
          f"= {100 * mde * periods_per_year:.2f}% annualized")
    print(f"    observed |effect| / MDE    : {abs(mean) / mde:.2f}x")
    if abs(mean) < mde:
        print("    -> DEFERRED_INSUFFICIENT_SAMPLE: effect is below what this sample can detect")

    print(bar)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="smallest complete loop")
    ap.add_argument("--lookback", type=int, default=20, help="signal window in trading days")
    ap.add_argument("--hold", type=int, default=5, help="holding period in trading days")
    ap.add_argument("--topk", type=int, default=20, help="basket size")
    ap.add_argument("--direction", choices=["top", "bottom"], default="top",
                    help="top = highest signal (momentum), bottom = lowest (reversal)")
    ap.add_argument("--min-value", type=float, default=1e8,
                    help="minimum daily traded value in KRW (tradability floor, not a signal gate)")
    ap.add_argument("--market", choices=["all", "kospi", "kosdaq"], default="all",
                    help="universe. 'all' mixes populations: the pre-2025 archive is KOSPI-only "
                         "and KOSDAQ only starts in 2025, so 'all' is NOT one population over time")
    ap.add_argument("--bootstrap", type=int, default=10000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--panel-cache", default=str(DEFAULT_CACHE))
    ap.add_argument("--rebuild-panel", action="store_true", help="ignore the cache and rebuild")
    ap.add_argument("--deciles", type=int, default=0,
                    help="if > 0, report all N deciles and monotonicity instead of a single top/bottom basket")
    return run(ap.parse_args())


if __name__ == "__main__":
    sys.exit(main())
