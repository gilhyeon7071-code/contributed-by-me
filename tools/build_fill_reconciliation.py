from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
DEFAULT_LEDGER = Path(r"E:\vibe\buffett\data\ledger\paper_fills_ledger.csv")


def _hhmmss_to_seconds(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.replace(":", "", regex=False).str.zfill(6).str.slice(0, 6)
    h = pd.to_numeric(s.str.slice(0, 2), errors="coerce")
    m = pd.to_numeric(s.str.slice(2, 4), errors="coerce")
    sec = pd.to_numeric(s.str.slice(4, 6), errors="coerce")
    return h * 3600 + m * 60 + sec


def load_ledger(path: Path, since_ymd: str) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig")
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["date"] = df["date"].astype(str).str.slice(0, 8)
    df = df[df["date"] >= since_ymd].copy()
    dtstr = df["datetime"].astype(str)
    df["fill_time_hhmmss"] = dtstr.str.split("T").str[1].fillna("090000")
    df["fill_seconds"] = _hhmmss_to_seconds(df["fill_time_hhmmss"])
    df["side"] = df["side"].astype(str).str.upper().str.strip()
    df["fill_price"] = pd.to_numeric(df["fill_price"], errors="coerce")
    df = df.dropna(subset=["fill_price", "fill_seconds"])
    df = df[df["side"].isin(["BUY", "SELL"])]
    return df.reset_index(drop=True)


def _asof_ref(fills: pd.DataFrame, market: pd.DataFrame, price_cols: dict, tolerance_sec: int) -> pd.DataFrame:
    """asof-join fills against a sorted (code, seconds) market table; return ref price per side."""
    if market.empty:
        out = fills.copy()
        out["_ref_seconds"] = np.nan
        for col in price_cols.values():
            out[f"_{col}"] = np.nan
        return out

    # merge_asof requires the "on" column sorted globally (not just within each `by` group).
    m = market.sort_values("seconds").reset_index(drop=True)
    left = fills.sort_values("fill_seconds").reset_index()  # keep original index in 'index'
    merged = pd.merge_asof(
        left, m, left_on="fill_seconds", right_on="seconds", by="code",
        direction="backward", tolerance=tolerance_sec,
    )
    merged = merged.set_index("index").reindex(fills.index)
    merged["_ref_seconds"] = merged["seconds"]
    return merged


def reconcile_day(fills_day: pd.DataFrame, log_dir: Path, ymd: str, tolerance_sec: int) -> pd.DataFrame:
    quotes_path = log_dir / f"intraday_quote_snapshots_{ymd}.parquet"
    trades_path = log_dir / f"intraday_trade_ticks_{ymd}.parquet"

    out = fills_day.copy()
    out["ref_price"] = np.nan
    out["ref_source"] = "no_market_data"
    out["ref_seconds"] = np.nan

    if quotes_path.exists():
        q = pd.read_parquet(quotes_path, columns=["code", "hhmmss", "ask1", "bid1"])
        q["seconds"] = _hhmmss_to_seconds(q["hhmmss"])
        m = _asof_ref(out, q, {}, tolerance_sec)
        buy = out["side"].eq("BUY") & m["ask1"].notna()
        sell = out["side"].eq("SELL") & m["bid1"].notna()
        out.loc[buy, "ref_price"] = m.loc[buy, "ask1"]
        out.loc[sell, "ref_price"] = m.loc[sell, "bid1"]
        out.loc[buy | sell, "ref_source"] = "quote_top_of_book"
        out.loc[buy | sell, "ref_seconds"] = m.loc[buy | sell, "_ref_seconds"]

    still_missing = out["ref_price"].isna()
    if still_missing.any() and trades_path.exists():
        # rename to avoid colliding with the ledger's own unrelated "price" column during merge
        t = pd.read_parquet(trades_path, columns=["code", "hhmmss", "price"]).rename(columns={"price": "market_trade_price"})
        t["seconds"] = _hhmmss_to_seconds(t["hhmmss"])
        m2 = _asof_ref(out.loc[still_missing], t, {}, tolerance_sec)
        found = m2["market_trade_price"].notna()
        idx = m2.index[found]
        out.loc[idx, "ref_price"] = m2.loc[idx, "market_trade_price"]
        out.loc[idx, "ref_source"] = "last_trade_fallback"
        out.loc[idx, "ref_seconds"] = m2.loc[idx, "_ref_seconds"]

    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Reconcile paper-engine fills against real KIS tick/quote data (real execution slippage)")
    ap.add_argument("--ledger-csv", default=str(DEFAULT_LEDGER))
    ap.add_argument("--log-dir", default=str(LOG_DIR))
    ap.add_argument("--since", default="20260409", help="YYYYMMDD, only reconcile fills on/after this date (Track B tick coverage start)")
    ap.add_argument("--tolerance-sec", type=int, default=300, help="max staleness for asof match before treating as no_market_data")
    ap.add_argument("--out-dir", default=str(LOG_DIR))
    args = ap.parse_args()

    ledger_path = Path(args.ledger_csv)
    log_dir = Path(args.log_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    fills = load_ledger(ledger_path, args.since)
    print(f"[FILL_RECON] ledger={ledger_path} fills_since_{args.since}={len(fills)}")

    results = []
    for ymd, day_fills in fills.groupby("date", sort=True):
        day_out = reconcile_day(day_fills, log_dir, ymd, args.tolerance_sec)
        results.append(day_out)
        matched = int(day_out["ref_price"].notna().sum())
        print(f"[FILL_RECON] {ymd} fills={len(day_out)} matched={matched}")

    if not results:
        print("[FILL_RECON] no fills in scope")
        return 0

    out = pd.concat(results, ignore_index=True)

    is_buy = out["side"].eq("BUY")
    is_sell = out["side"].eq("SELL")
    has_ref = out["ref_price"].notna() & (out["ref_price"] > 0)

    out["real_slippage_bps"] = np.nan
    out.loc[has_ref & is_buy, "real_slippage_bps"] = (
        (out.loc[has_ref & is_buy, "fill_price"] - out.loc[has_ref & is_buy, "ref_price"])
        / out.loc[has_ref & is_buy, "ref_price"] * 10000.0
    )
    out.loc[has_ref & is_sell, "real_slippage_bps"] = (
        (out.loc[has_ref & is_sell, "ref_price"] - out.loc[has_ref & is_sell, "fill_price"])
        / out.loc[has_ref & is_sell, "ref_price"] * 10000.0
    )
    out["fill_to_ref_lag_sec"] = out["fill_seconds"] - out["ref_seconds"]

    cols = [
        "date", "fill_time_hhmmss", "code", "side", "fill_price", "fill_qty",
        "ref_price", "ref_source", "ref_seconds", "fill_to_ref_lag_sec",
        "real_slippage_bps", "slippage_actual_bps", "slippage_ref_source",
    ]
    cols = [c for c in cols if c in out.columns]
    detail = out[cols].copy()

    detail_path = out_dir / "fill_reconciliation_detail_latest.csv"
    detail.to_csv(detail_path, index=False, encoding="utf-8-sig")

    matched = detail["real_slippage_bps"].notna()
    s = detail.loc[matched, "real_slippage_bps"]
    old = pd.to_numeric(detail.loc[matched, "slippage_actual_bps"], errors="coerce").dropna()

    summary = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "ledger_csv": str(ledger_path),
        "since": args.since,
        "tolerance_sec": args.tolerance_sec,
        "total_fills_in_scope": int(len(detail)),
        "matched_to_market_data": int(matched.sum()),
        "coverage_pct": float(matched.mean() * 100.0) if len(detail) else 0.0,
        "by_ref_source": detail["ref_source"].value_counts().to_dict(),
        "real_slippage_bps": {
            "n": int(len(s)),
            "median": float(s.median()) if len(s) else None,
            "mean": float(s.mean()) if len(s) else None,
            "std": float(s.std()) if len(s) else None,
            "p5": float(s.quantile(0.05)) if len(s) else None,
            "p95": float(s.quantile(0.95)) if len(s) else None,
        },
        "old_ledger_slippage_actual_bps_same_rows": {
            "n": int(len(old)),
            "median": float(old.median()) if len(old) else None,
            "mean": float(old.mean()) if len(old) else None,
            "std": float(old.std()) if len(old) else None,
        },
    }
    summary_path = out_dir / "fill_reconciliation_summary_latest.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(f"[FILL_RECON] coverage={summary['coverage_pct']:.1f}% matched={summary['matched_to_market_data']}/{summary['total_fills_in_scope']}")
    print(f"[FILL_RECON] real_slippage_bps median={summary['real_slippage_bps']['median']} mean={summary['real_slippage_bps']['mean']}")
    print(f"[FILL_RECON] old ledger slippage_actual_bps (same rows) median={summary['old_ledger_slippage_actual_bps_same_rows']['median']} mean={summary['old_ledger_slippage_actual_bps_same_rows']['mean']}")
    print(f"[FILL_RECON] detail={detail_path}")
    print(f"[FILL_RECON] summary={summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
