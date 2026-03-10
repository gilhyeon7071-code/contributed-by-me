from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import List, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SOURCE_NAME = "bok_policy_rate_manual_seed_v1"


def _manual_bok_seed() -> List[Tuple[str, float]]:
    # Bank of Korea base-rate change points (manual seed).
    return [
        ("2020-01-01", 1.25),
        ("2020-03-17", 0.75),
        ("2020-05-28", 0.50),
        ("2021-08-26", 0.75),
        ("2021-11-25", 1.00),
        ("2022-01-14", 1.25),
        ("2022-04-14", 1.50),
        ("2022-05-26", 1.75),
        ("2022-07-13", 2.25),
        ("2022-08-25", 2.50),
        ("2022-10-12", 3.00),
        ("2022-11-24", 3.25),
        ("2023-01-13", 3.50),
    ]


def _market_range_default() -> Tuple[pd.Timestamp, pd.Timestamp]:
    p = LOG_DIR / "backtest_market_ohlc_latest.csv"
    if p.exists():
        try:
            df = pd.read_csv(p)
            if "date" in df.columns and len(df):
                d = pd.to_datetime(df["date"], errors="coerce").dropna().sort_values()
                if len(d):
                    return pd.Timestamp(d.iloc[0].date()), pd.Timestamp(d.iloc[-1].date())
        except Exception:
            pass
    end = pd.Timestamp(dt.date.today())
    start = end - pd.Timedelta(days=365 * 6)
    return pd.Timestamp(start.date()), pd.Timestamp(end.date())


def build_rate_series(start_date: str = "", end_date: str = "") -> Tuple[pd.DataFrame, dict]:
    d0, d1 = _market_range_default()
    if start_date:
        d0 = pd.Timestamp(pd.to_datetime(start_date).date())
    if end_date:
        d1 = pd.Timestamp(pd.to_datetime(end_date).date())
    if d1 < d0:
        d0, d1 = d1, d0

    seed = pd.DataFrame(_manual_bok_seed(), columns=["effective_date", "rate"])
    seed["effective_date"] = pd.to_datetime(seed["effective_date"], errors="coerce")
    seed = seed.dropna(subset=["effective_date", "rate"]).sort_values("effective_date")

    cal = pd.DataFrame({"date": pd.date_range(d0, d1, freq="B")})
    out = pd.merge_asof(
        cal.sort_values("date"),
        seed.rename(columns={"effective_date": "date"}).sort_values("date"),
        on="date",
        direction="backward",
    )
    out["rate"] = pd.to_numeric(out["rate"], errors="coerce").ffill().bfill()
    out["source"] = SOURCE_NAME
    out = out[["date", "rate", "source"]]

    meta = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "source": SOURCE_NAME,
        "seed_points": len(seed),
        "rows": int(len(out)),
        "date_min": out["date"].min().strftime("%Y-%m-%d") if len(out) else None,
        "date_max": out["date"].max().strftime("%Y-%m-%d") if len(out) else None,
    }
    return out, meta


def main() -> int:
    ap = argparse.ArgumentParser(description="Build local rate_series_latest.csv from manual BOK seed")
    ap.add_argument("--out-csv", default="")
    ap.add_argument("--out-json", default="")
    ap.add_argument("--start-date", default="")
    ap.add_argument("--end-date", default="")
    args = ap.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    df, meta = build_rate_series(args.start_date, args.end_date)

    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = Path(args.out_csv) if args.out_csv else (LOG_DIR / f"rate_series_{stamp}.csv")
    out_json = Path(args.out_json) if args.out_json else (LOG_DIR / f"rate_series_{stamp}.json")
    latest_csv = LOG_DIR / "rate_series_latest.csv"
    latest_json = LOG_DIR / "rate_series_latest.json"

    z = df.copy()
    z["date"] = z["date"].dt.strftime("%Y-%m-%d")
    z.to_csv(out_csv, index=False, encoding="utf-8-sig")
    z.to_csv(latest_csv, index=False, encoding="utf-8-sig")
    out_json.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_json.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[RATE] csv={out_csv}")
    print(f"[RATE] latest_csv={latest_csv}")
    print(f"[RATE] json={out_json}")
    print(f"[RATE] latest_json={latest_json}")
    print(f"[RATE] rows={meta['rows']} range={meta['date_min']}..{meta['date_max']} source={meta['source']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
