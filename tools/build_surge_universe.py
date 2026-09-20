from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PRICES = ROOT / "paper" / "prices" / "ohlcv_paper.parquet"
DEFAULT_OUT = ROOT / "paper" / "surge_universe.csv"


def _code6(value: object) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def build_universe(prices_path: Path, out_path: Path, top_n: int, lookback_days: int) -> dict:
    df = pd.read_parquet(prices_path)
    required = {"date", "code", "close", "volume"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise SystemExit(f"missing_required_columns:{','.join(missing)}")

    work = df[["date", "code", "close", "volume"]].copy()
    work["code"] = work["code"].map(_code6)
    work = work[work["code"].str.len().eq(6)].copy()
    work["date"] = work["date"].astype(str)
    work["close"] = pd.to_numeric(work["close"], errors="coerce")
    work["volume"] = pd.to_numeric(work["volume"], errors="coerce").fillna(0.0)
    work = work.dropna(subset=["close"])
    work = work[work["close"] > 0].copy()

    dates = sorted(work["date"].dropna().unique())
    if not dates:
        raise SystemExit("no_price_dates")
    recent_dates = set(dates[-max(5, int(lookback_days)) :])
    work = work[work["date"].isin(recent_dates)].copy()
    work = work.sort_values(["code", "date"])
    work["ret"] = work.groupby("code")["close"].pct_change()

    grouped = work.groupby("code", as_index=False).agg(
        avg_volume_20d=("volume", "mean"),
        ret_std_20d=("ret", "std"),
        bars=("date", "nunique"),
    )
    grouped = grouped[grouped["bars"] >= min(10, max(5, int(lookback_days) // 2))].copy()
    if grouped.empty:
        raise SystemExit("no_universe_rows")

    vol_rank = grouped["avg_volume_20d"].rank(pct=True)
    std_rank = grouped["ret_std_20d"].fillna(0.0).rank(pct=True)
    grouped["universe_score"] = (0.60 * vol_rank + 0.40 * std_rank).round(4)
    grouped = grouped.sort_values(
        ["universe_score", "avg_volume_20d", "ret_std_20d"],
        ascending=[False, False, False],
    ).head(max(1, int(top_n)))

    out = grouped[["code", "avg_volume_20d", "ret_std_20d", "universe_score"]].copy()
    out["avg_volume_20d"] = out["avg_volume_20d"].round(0).astype("int64")
    out["ret_std_20d"] = out["ret_std_20d"].fillna(0.0).round(6)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False, encoding="utf-8-sig")
    return {
        "prices_path": str(prices_path),
        "out_path": str(out_path),
        "rows": int(len(out)),
        "top_n": int(top_n),
        "lookback_days": int(lookback_days),
        "date_min": min(recent_dates),
        "date_max": max(recent_dates),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Build intraday surge universe from recent paper OHLCV data.")
    ap.add_argument("--prices", default=str(DEFAULT_PRICES))
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--top-n", type=int, default=150)
    ap.add_argument("--lookback-days", type=int, default=20)
    args = ap.parse_args()

    report = build_universe(
        prices_path=Path(args.prices),
        out_path=Path(args.out),
        top_n=int(args.top_n),
        lookback_days=int(args.lookback_days),
    )
    print(
        "[SURGE_UNIVERSE] "
        f"rows={report['rows']} top_n={report['top_n']} "
        f"date_range={report['date_min']}..{report['date_max']} out={report['out_path']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
