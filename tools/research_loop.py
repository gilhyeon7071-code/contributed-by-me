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
# Wide KOSPI+KOSDAQ file covering 2022-10 ~ 2025-12. The krx_daily_archive is
# KOSPI-only before 2025, so this is the only source of pre-2025 KOSDAQ, and it
# retains names delisted since (2022 KOSDAQ 1,993 codes vs 1,493 still listed).
RAW_WIDE = BASE_DIR / "Raw" / "krx_daily_20221001_20251224.parquet"
DEFAULT_CACHE = BASE_DIR / "2_Logs" / "research_loop_panel_cache.parquet"

COST_ROUNDTRIP = 0.00358      # production contract, see PLANS 2026-08-18 (2)
DAILY_LIMIT_PCT = 31.0        # KRX daily move limit; beyond this is a data error


def build_panel(cache: Path, rebuild: bool) -> pd.DataFrame:
    if cache.exists() and not rebuild:
        panel = pd.read_parquet(cache)
        print(f"[panel] cache hit: {cache}  rows={len(panel):,}")
        return panel

    # RAW_WIDE first, then the archive. The archive carries change_rate and the
    # raw file does not, and the dedupe below keeps the last occurrence, so the
    # archive's richer rows win wherever the two overlap.
    files = [str(RAW_WIDE)] if RAW_WIDE.exists() else []
    for d in PRICE_DIRS:
        for p in glob.glob(str(d / "krx_daily_*_clean.parquet")):
            if ".bak" in os.path.basename(p):
                continue
            files.append(p)
    print(f"[panel] reading {len(files)} parquet files"
          f"{' (incl. RAW_WIDE)' if RAW_WIDE.exists() else ''}")

    frames = []
    for p in files:
        try:
            frames.append(pd.read_parquet(p))
        except Exception as exc:
            print(f"[panel]   SKIP {os.path.basename(p)}: {type(exc).__name__}")
    panel = pd.concat(frames, ignore_index=True)
    if "change_rate" not in panel.columns:
        panel["change_rate"] = pd.NA
    print(f"[panel] raw rows: {len(panel):,}")

    panel["date"] = panel["date"].astype(str).str.replace("-", "", regex=False).str[:8]
    panel["code"] = panel["code"].astype(str).str.zfill(6)
    for col in ("close", "value", "change_rate"):
        panel[col] = pd.to_numeric(panel[col], errors="coerce")

    before = len(panel)
    panel = panel[panel["close"] > 0]
    print(f"[panel] close>0 filter removed {before - len(panel):,} "
          f"({100 * (before - len(panel)) / before:.1f}%) -- weekend zero padding")

    # stable sort so rows carrying change_rate land last and survive the dedupe
    panel["_has_cr"] = panel["change_rate"].notna().astype(int)
    panel = panel.sort_values(["date", "code", "_has_cr"], kind="mergesort")
    panel = panel.drop_duplicates(subset=["date", "code"], keep="last").drop(columns=["_has_cr"])
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

    panel = panel[["date", "code", "market", "close", "value", "ret_pct"]]
    cache.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(cache, index=False)
    print(f"[panel] cached -> {cache}")
    return panel


def _market_map() -> dict:
    """code -> KOSPI / KOSDAQ.

    The krx_daily_archive files for 2020-2024 are KOSPI-only by construction
    (verified 2026-08-19: 0 KOSDAQ codes, while 939/984 of today's KOSPI names
    are present). So a code appearing there was KOSPI at the time, including
    names since delisted -- filtering on "KOSPI today" would drop them and
    reintroduce survivorship.

    RAW_WIDE breaks that shortcut: it is also pre-2025 and wide, but it DOES
    carry KOSDAQ and its own market column. Explicit labels therefore win, and
    the KOSPI-only inference is applied only to unlabelled archive files -- never
    to RAW_WIDE.
    """
    kospi: set[str] = set()
    kosdaq: set[str] = set()

    sources = [str(RAW_WIDE)] if RAW_WIDE.exists() else []
    for d in PRICE_DIRS:
        sources.extend(glob.glob(str(d / "krx_daily_*_clean.parquet")))

    for p in sources:
        if True:
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

            # pre-2025 UNLABELLED archive files are KOSPI-only by construction.
            # RAW_WIDE is excluded: it is pre-2025 but carries KOSDAQ explicitly.
            if p == str(RAW_WIDE):
                continue
            digits = [t for t in base.replace(".parquet", "").split("_") if t.isdigit() and len(t) == 8]
            if digits and max(digits) < "20250101" and labelled.empty:
                kospi.update(cols["code"].unique().tolist())

    kospi -= kosdaq  # a code explicitly labelled KOSDAQ wins
    out = {c: "KOSPI" for c in kospi}
    out.update({c: "KOSDAQ" for c in kosdaq})
    return out


def bootstrap_ci(values: np.ndarray, n: int, rng: np.random.Generator) -> tuple[float, float]:
    """i.i.d. bootstrap. OPTIMISTIC when holding windows overlap -- see block_bootstrap_ci."""
    v = np.asarray(values, dtype=float)
    v = v[~np.isnan(v)]
    if len(v) < 10:
        return float("nan"), float("nan")
    idx = rng.integers(0, len(v), size=(n, len(v)))
    means = np.sort(v[idx].mean(axis=1))
    return float(means[int(0.025 * n)]), float(means[int(0.975 * n)])


def block_bootstrap_ci(values: np.ndarray, block: int, n: int,
                       rng: np.random.Generator) -> tuple[float, float]:
    """Moving-block bootstrap. This is the honest interval for this design.

    A signal is emitted every day but held for `hold` days, so consecutive
    observations share (hold-1)/hold of their forward window. Treating days as
    independent understated the CI width by roughly 3-4x when this was first
    measured on 2026-08-19, which was enough to flip several 'significant'
    findings back to undecided. Effective independent sample is about n/hold.
    """
    v = np.asarray(values, dtype=float)
    v = v[~np.isnan(v)]
    L = len(v)
    if L < max(40, 2 * block) or block < 1:
        return float("nan"), float("nan")
    n_blocks = int(np.ceil(L / block))
    max_start = L - block
    means = np.empty(n)
    for i in range(n):
        starts = rng.integers(0, max_start + 1, size=n_blocks)
        means[i] = np.concatenate([v[s:s + block] for s in starts])[:L].mean()
    means.sort()
    return float(means[int(0.025 * n)]), float(means[int(0.975 * n)])


def run_ic_stability(args, rets, signal, forward, eligible, rng) -> int:
    """Signal-validity layer (V3 methodology section 3.1), as a confirmation round.

    Primary metric is the daily cross-sectional rank IC, not a basket return.
    A basket of `topk` names is dominated by the dispersion of a handful of
    stocks; the IC uses every eligible name every day, so it is far better
    powered at the sample sizes available here.

    Pre-registered pass rule (do not change after seeing a result):
      1. every segment reaches min_eff_n effective independent observations
      2. every segment's BLOCK bootstrap CI excludes zero
      3. every segment shares the same sign
      4. 1-3 hold at EVERY segment count in --segments, not just one
      5. the decile structure is MONOTONE, not U-shaped

    Rules 4 and 5 were added 2026-08-19 after bollinger passed the earlier
    three-rule version and turned out to be untradeable. It only passed at two
    segments -- three or four broke it -- and its deciles were U-shaped, with the
    most compressed decile the worst performer. A signal whose two tails both
    lose has a non-zero IC without having a usable ranking, so monotonicity is
    now part of the pass condition rather than a diagnostic. See PLANS (7) F.
    """
    ic_by_day: list[float] = []
    days: list[str] = []
    bucket_rows: list[np.ndarray] = []
    nb = max(2, args.deciles or 10)
    for day in rets.index:
        if args.signal_start_date and day < args.signal_start_date:
            continue
        if args.signal_end_date and day > args.signal_end_date:
            continue
        mask = eligible.loc[day]
        if mask.sum() < args.min_names:
            continue
        codes = mask[mask].index
        sig = signal.loc[day, codes]
        fwd = forward.loc[day, codes]
        ic_by_day.append(float(sig.rank().corr(fwd.rank())))
        days.append(day)
        buckets = np.ceil(sig.rank(method="first") / (len(codes) / nb)).clip(1, nb).astype(int)
        means = fwd.groupby(buckets).mean().reindex(range(1, nb + 1))
        bucket_rows.append(means.to_numpy(dtype=float) - float(fwd.mean()))

    if len(ic_by_day) < args.hold * 4:
        print(f"only {len(ic_by_day)} signal days -- not enough for block inference")
        return 1

    ic = np.asarray(ic_by_day)
    idx = pd.Index(days)
    bar = "=" * 84

    print()
    print(bar)
    print(f"IC STABILITY ROUND (confirmation)  lookback={args.lookback}d  hold={args.hold}d  "
          f"market={args.market}")
    print(bar)
    seg_counts = [int(x) for x in str(args.segments).replace(" ", "").split(",") if x]
    print("  pre-registered pass rule (all five must hold):")
    print("    1. every segment reaches the minimum effective n")
    print("    2. every segment's BLOCK CI excludes zero")
    print("    3. every segment shares one sign")
    print(f"    4. rules 1-3 hold at EVERY segment count in {seg_counts}")
    print(f"    5. the {nb}-bucket structure is monotone, not U-shaped")
    print(f"  min effective n per segment = {args.min_eff_n}   block length = {args.hold}")
    print()

    def report(label: str, v: np.ndarray) -> tuple[bool, bool, float]:
        eff = len(v) // max(args.hold, 1)
        m = float(v.mean())
        lo, hi = block_bootstrap_ci(v, args.hold, args.bootstrap, rng)
        excl = bool((lo > 0) or (hi < 0)) if lo == lo else False
        enough = eff >= args.min_eff_n
        flag = "OK " if (excl and enough) else ("n<min" if not enough else "spans0")
        ci = f"[{lo:+.5f}, {hi:+.5f}]" if lo == lo else "[   n/a   ]"
        print(f"  {label:24s} n={len(v):5d} eff={eff:4d}  IC={m:+.5f}  block95={ci:24s} {flag}")
        return excl, enough, m

    full_excl, _, full_m = report("FULL PERIOD", ic)

    # rule 4: the same three checks at every requested segment count
    seg_results = {}
    for n_seg in seg_counts:
        if n_seg < 2:
            continue
        print()
        excl_l, enough_l, signs_l = [], [], []
        bounds = np.linspace(0, len(ic), n_seg + 1).astype(int)
        for s in range(n_seg):
            a, b = bounds[s], bounds[s + 1]
            lbl = f"[{n_seg}seg] s{s+1} {idx[a][:6]}~{idx[b-1][:6]}"
            e, n_ok, m = report(lbl, ic[a:b])
            excl_l.append(e); enough_l.append(n_ok); signs_l.append(np.sign(m))
        seg_results[n_seg] = {
            "excl": all(excl_l),
            "enough": all(enough_l),
            "same_sign": len(set(signs_l)) == 1,
            "sign": signs_l[0] if signs_l else 0.0,
        }

    # rule 5: monotone vs U-shaped
    B = np.vstack(bucket_rows)
    bmeans = B.mean(axis=0)
    order = pd.Series(np.arange(1, nb + 1))
    rho = float(order.corr(pd.Series(bmeans), method="spearman"))
    opposite_tails = bool(np.sign(bmeans[0]) != np.sign(bmeans[-1]))
    monotone = (abs(rho) >= args.min_monotone_rho) and opposite_tails
    print()
    print(f"  bucket structure ({nb} buckets, B1 = lowest signal)")
    print("    " + "  ".join(f"B{i+1}:{100*bmeans[i]:+.3f}%" for i in range(nb)))
    print(f"    spearman(bucket, excess) = {rho:+.3f}   (need |rho| >= {args.min_monotone_rho})")
    print(f"    tails on opposite sides  = {opposite_tails}   "
          f"(B1 {100*bmeans[0]:+.3f}% vs B{nb} {100*bmeans[-1]:+.3f}%)")
    print(f"    -> {'MONOTONE' if monotone else 'NOT MONOTONE (U-shaped or flat)'}")

    if args.by_year:
        print()
        print("  by year (diagnostic only, not part of the pass rule)")
        yr = np.asarray([d[:4] for d in idx])
        for y in sorted(set(yr.tolist())):
            v = ic[yr == y]
            if len(v) < args.hold * 2:
                print(f"    {y}: n={len(v):4d}  too short for block inference")
                continue
            lo, hi = block_bootstrap_ci(v, args.hold, max(2000, args.bootstrap // 5), rng)
            print(f"    {y}: n={len(v):4d} eff={len(v)//args.hold:3d}  IC={v.mean():+.5f}  "
                  f"block95=[{lo:+.5f}, {hi:+.5f}]")

    print()
    print("  rule check")
    fails = []
    if not seg_results:
        fails.append("no segment counts requested")
    for n_seg, r in sorted(seg_results.items()):
        marks = []
        if not r["enough"]:
            marks.append("n<min")
        if not r["excl"]:
            marks.append("spans0")
        if not r["same_sign"]:
            marks.append("sign split")
        ok = not marks
        print(f"    {n_seg} segments : {'PASS' if ok else 'FAIL (' + ', '.join(marks) + ')'}")
        if not ok:
            fails.append(f"{n_seg}-segment split")
    print(f"    monotonicity: {'PASS' if monotone else 'FAIL'}")
    if not monotone:
        fails.append("bucket structure not monotone")

    any_short = any(not r["enough"] for r in seg_results.values())
    print()
    if not fails:
        sign_word = "negative" if full_m < 0 else "positive"
        verdict = (f"CONFIRMED -- stable across {seg_counts} segment splits, consistent "
                   f"{sign_word} sign, and a monotone bucket structure")
    elif any_short and len(fails) == 1:
        verdict = "DEFERRED_INSUFFICIENT_SAMPLE -- a segment is below the minimum effective n"
    else:
        verdict = "NOT_CONFIRMED -- " + "; ".join(fails)
    print(f"  >>> {verdict}")
    print()
    print("  note: a non-zero IC only says the signal carries cross-sectional information.")
    print("  Rule 5 is what separates that from a usable ranking -- a U-shaped signal has")
    print("  a non-zero IC because both tails lose, and cannot be turned into a basket.")
    print(bar)
    return 0


def run_deciles(args, rets, signal, forward, eligible, rng) -> int:
    """Quantile monotonicity (V3 methodology section 3.1).

    top/bottom baskets alone cannot tell three very different structures apart:
      monotone   -> the signal carries information; the sign may simply be inverted
      U-shaped   -> only the extremes are bad; this is an "avoid extremes" rule
      flat       -> the signal carries nothing
    So report every bucket, plus a daily rank IC as the summary statistic.
    """
    n_buckets = args.deciles
    bucket_rows = []      # per day: mean net forward return of each bucket
    base_rows = []
    ic_rows = []          # per day: Spearman rank corr(signal, forward) across the universe
    dates_used = []

    for day in rets.index:
        if args.signal_start_date and day < args.signal_start_date:
            continue
        if args.signal_end_date and day > args.signal_end_date:
            continue
        mask = eligible.loc[day]
        if mask.sum() < n_buckets * 5:
            continue
        codes = mask[mask].index
        sig = signal.loc[day, codes]
        fwd = forward.loc[day, codes]

        sig_rank = sig.rank(method="first")
        # bucket 1 = lowest signal ... bucket N = highest signal
        buckets = np.ceil(sig_rank / (len(sig_rank) / n_buckets)).clip(1, n_buckets).astype(int)
        means = fwd.groupby(buckets).mean()
        if len(means) != n_buckets:
            continue
        bucket_rows.append(means.reindex(range(1, n_buckets + 1)).to_numpy() - COST_ROUNDTRIP)
        base_rows.append(float(fwd.mean()) - COST_ROUNDTRIP)
        ic_rows.append(float(sig.rank().corr(fwd.rank())))
        dates_used.append(day)

    if not bucket_rows:
        print("no usable days -- universe too small for the requested bucket count")
        return 1

    B = np.vstack(bucket_rows)                 # days x buckets
    base = np.asarray(base_rows)
    excess = B - base[:, None]
    ic = np.asarray(ic_rows)
    periods_per_year = 252.0 / args.hold

    bar = "=" * 84
    print()
    print(bar)
    print(f"QUANTILE ROUND: lookback={args.lookback}d  hold={args.hold}d  buckets={n_buckets}  "
          f"market={args.market}  min_value={args.min_value:,.0f}  cost={COST_ROUNDTRIP:.5f} RT")
    print(bar)
    print(f"  signal days : {len(B)}   {dates_used[0]} ~ {dates_used[-1]}")
    print(f"  bucket 1 = LOWEST signal, bucket {n_buckets} = HIGHEST signal")
    print()
    print(f"  {'bucket':>7s} {'excess/period':>14s} {'95% CI':>24s} {'annualized':>12s}")
    print("  " + "-" * 62)
    means = []
    for b in range(n_buckets):
        col = excess[:, b]
        m = float(col.mean())
        lo, hi = bootstrap_ci(col, args.bootstrap, rng)
        blo, bhi = block_bootstrap_ci(col, args.hold, args.bootstrap, rng)
        means.append(m)
        star = " *" if (blo > 0 or bhi < 0) else ""
        print(f"  {b + 1:7d} {100 * m:13.4f}% "
              f"[{100 * blo:+8.4f}%,{100 * bhi:+8.4f}%] {100 * m * periods_per_year:11.2f}%{star}")
    print(f"  (* = BLOCK bootstrap CI excludes zero; block={args.hold}, "
          f"effective indep. n ~{len(B) // max(args.hold, 1)})")

    means = np.asarray(means)
    order = np.arange(1, n_buckets + 1)
    # Spearman between bucket index and its mean excess
    rank_corr = float(pd.Series(order).corr(pd.Series(means), method="spearman"))
    ic_mean = float(ic.mean())
    # daily ICs overlap for the same reason bucket returns do -- block it too
    ic_lo, ic_hi = block_bootstrap_ci(ic, args.hold, args.bootstrap, rng)

    print()
    print("  monotonicity")
    print(f"    spearman(bucket index, mean excess) : {rank_corr:+.3f}")
    print(f"    daily rank IC mean                  : {ic_mean:+.5f}   "
          f"CI [{ic_lo:+.5f}, {ic_hi:+.5f}]")
    print(f"    rank IC CI excludes zero            : {(ic_lo > 0) or (ic_hi < 0)}")

    extremes = (means[0] + means[-1]) / 2.0
    middle = means[1:-1].mean()
    print()
    print("  shape test")
    print(f"    mean of the two extreme buckets : {100 * extremes:+.4f}%")
    print(f"    mean of the middle buckets      : {100 * middle:+.4f}%")
    print(f"    extremes - middle               : {100 * (extremes - middle):+.4f}%")

    if abs(rank_corr) >= 0.7 and ((ic_lo > 0) or (ic_hi < 0)):
        shape = ("MONOTONE (%s). The signal carries information; a negative slope means the "
                 "usable trade is the inverted one." % ("increasing" if rank_corr > 0 else "decreasing"))
    elif extremes < middle - abs(middle) * 0.25:
        shape = "U-SHAPED. Only the extremes underperform -- this is an avoid-extremes rule, not a ranking signal."
    elif not ((ic_lo > 0) or (ic_hi < 0)):
        shape = "FLAT. Rank IC is indistinguishable from zero -- the signal carries no cross-sectional information."
    else:
        shape = "MIXED. Neither cleanly monotone nor cleanly U-shaped; do not build on this yet."
    print()
    print(f"  >>> {shape}")
    print(bar)
    return 0


def compute_signal(args: argparse.Namespace, rets: pd.DataFrame, panel: pd.DataFrame) -> pd.DataFrame:
    """Build the signal matrix for the chosen grammar."""
    stype = args.signal_type
    if stype == "momentum":
        growth = 1.0 + rets
        return growth.rolling(args.lookback, min_periods=args.lookback).apply(np.prod, raw=True) - 1.0

    # Level-based grammars must NOT use the raw close: the panel is not adjusted
    # for corporate actions, so a split inside the lookback window breaks the
    # price level, not just one day's return. Measured 2026-08-19 on KOSPI: 303
    # split-like jumps across 175 codes contaminate 1.45% of eligible cells, and
    # those cells make up 10.16% of the bottom breakout decile against a 1.45%
    # base rate -- i.e. splits are pushed systematically into the extreme bucket.
    # Rebuild an adjusted series by cumulating the split-safe return instead.
    # Both grammars below are scale-invariant, so the arbitrary starting level
    # does not matter.
    adj = adjusted_price(rets)

    if stype == "breakout":
        return adj / adj.rolling(args.lookback, min_periods=args.lookback).max() - 1.0
    if stype == "bollinger":
        ma = adj.rolling(args.lookback, min_periods=args.lookback).mean()
        sd = adj.rolling(args.lookback, min_periods=args.lookback).std()
        return (adj - ma) / sd
    raise ValueError(f"unknown signal_type: {stype}")


def adjusted_price(rets: pd.DataFrame) -> pd.DataFrame:
    """Split-adjusted price level rebuilt from returns.

    `rets` already prefers change_rate and drops |r| > 31% as a data error, so
    cumulating it yields a series with no corporate-action discontinuities. Days
    with no return (not listed, or a dropped data-error day) are carried flat and
    then masked back out, so a code's untraded periods stay untradeable rather
    than becoming a flat price that the rolling window would treat as real.
    """
    level = (1.0 + rets.fillna(0.0)).cumprod()
    return level.where(rets.notna())


def run(args: argparse.Namespace) -> int:
    rng = np.random.default_rng(args.seed)
    panel = build_panel(Path(args.panel_cache), args.rebuild_panel)

    if args.start_date:
        panel = panel[panel["date"] >= args.start_date]
        print(f"[panel] start_date filter >= {args.start_date}: {len(panel):,} rows")
    if args.end_date:
        panel = panel[panel["date"] <= args.end_date]
        print(f"[panel] end_date filter <= {args.end_date}: {len(panel):,} rows")

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

    signal = compute_signal(args, rets, panel)
    growth = 1.0 + rets
    forward = (growth.rolling(args.hold, min_periods=args.hold).apply(np.prod, raw=True) - 1.0
               ).shift(-(1 + args.hold))
    eligible = (vals >= args.min_value) & signal.notna() & forward.notna()

    mode = args.mode
    if mode == "auto":
        mode = "deciles" if args.deciles > 0 else "basket"
    if mode == "ic":
        return run_ic_stability(args, rets, signal, forward, eligible, rng)
    if mode == "deciles":
        if args.deciles <= 0:
            args.deciles = 10
        return run_deciles(args, rets, signal, forward, eligible, rng)

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
    if args.signal_start_date:
        res = res[res["date"] >= args.signal_start_date]
        print(f"[signal] signal_start_date filter >= {args.signal_start_date}: {len(res)} days")
    if args.signal_end_date:
        res = res[res["date"] <= args.signal_end_date]
        print(f"[signal] signal_end_date filter <= {args.signal_end_date}: {len(res)} days")
    if res.empty:
        print("no signal days produced -- check lookback/hold vs panel length and date filters")
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
    blo, bhi = block_bootstrap_ci(excess, args.hold, args.bootstrap, rng)
    print(f"  MEAN EXCESS        : {100 * mean:+.4f}%   per {args.hold}-day holding period")
    print(f"  iid 95% CI         : [{100 * lo:+.4f}%, {100 * hi:+.4f}%]   (optimistic)")
    print(f"  BLOCK 95% CI       : [{100 * blo:+.4f}%, {100 * bhi:+.4f}%]   (honest, block={args.hold})")
    print(f"  effective indep. n : ~{len(excess) // max(args.hold, 1)}  (nominal {len(excess)})")
    print(f"  block CI excl zero : {(blo > 0) or (bhi < 0)}")
    print(f"  naive annualized   : {100 * mean * periods_per_year:+.2f}%")
    if blo > 0:
        verdict = "POSITIVE EDGE"
    elif bhi < 0:
        verdict = "NEGATIVE"
    else:
        verdict = "UNDECIDED -- block CI spans zero (iid CI would have said otherwise)" \
            if (lo > 0 or hi < 0) else "NO DETECTABLE EDGE"
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

    eff_n = max(1, len(excess) // max(args.hold, 1))
    mde = 1.96 * sd / np.sqrt(eff_n)
    print()
    print("  power check")
    print(f"    nominal n={len(excess)}  effective indep. n={eff_n}  sd={100 * sd:.3f}%")
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
    ap.add_argument("--signal-type", choices=["momentum", "breakout", "bollinger"], default="momentum",
                    help="momentum = past return; breakout = close / rolling high - 1; "
                         "bollinger = (close - MA) / SD")
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
    ap.add_argument("--start-date", default="",
                    help="include panel rows on or after this YYYYMMDD (e.g. 20220101). "
                         "Use this when you do NOT want pre-period data used for lookback.")
    ap.add_argument("--end-date", default="",
                    help="include panel rows on or before this YYYYMMDD (e.g. 20241231)")
    ap.add_argument("--signal-start-date", default="",
                    help="emit signals only on or after this YYYYMMDD, while still using earlier "
                         "panel rows for lookback (e.g. 20220101 with 120d lookback needs 2021 data)")
    ap.add_argument("--signal-end-date", default="",
                    help="emit signals only on or before this YYYYMMDD")
    ap.add_argument("--mode", choices=["auto", "basket", "deciles", "ic"], default="auto",
                    help="basket = one top/bottom basket; deciles = quantile monotonicity; "
                         "ic = rank-IC stability confirmation round. 'auto' picks deciles when "
                         "--deciles is given, else basket")
    ap.add_argument("--deciles", type=int, default=0,
                    help="bucket count for --mode deciles (default 10 when the mode is selected)")
    ap.add_argument("--segments", default="2,3,4",
                    help="--mode ic: comma-separated segment counts. The pass rule must hold at "
                         "EVERY one of them -- a signal that only survives a single split is not "
                         "stable (bollinger, 2026-08-19)")
    ap.add_argument("--min-monotone-rho", type=float, default=0.7,
                    help="--mode ic: minimum |spearman(bucket, excess)| for the structure to count "
                         "as monotone rather than U-shaped")
    ap.add_argument("--min-eff-n", type=int, default=20,
                    help="--mode ic: minimum effective independent observations per segment")
    ap.add_argument("--min-names", type=int, default=50,
                    help="--mode ic: minimum eligible names on a day for its IC to count")
    ap.add_argument("--by-year", action="store_true",
                    help="--mode ic: also print a per-year IC diagnostic")
    return run(ap.parse_args())


if __name__ == "__main__":
    sys.exit(main())
