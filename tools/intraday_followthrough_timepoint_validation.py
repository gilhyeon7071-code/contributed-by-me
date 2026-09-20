from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import pandas as pd


DEFAULT_TIMEPOINTS = ("10:00", "11:00", "14:00", "15:00", "15:20", "15:29")


def _date8(value: object) -> str:
    text = str(value).strip().replace("-", "")
    return text[:8]


def _next_session_map(ohlcv: pd.DataFrame) -> dict[str, str]:
    dates = sorted(ohlcv["date"].astype(str).unique())
    return {d: dates[i + 1] for i, d in enumerate(dates[:-1])}


def _read_candidates(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={"code": str})
    df["code"] = df["code"].astype(str).str.zfill(6)
    return df


def _read_intraday(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={"code": str})
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["ts_dt"] = pd.to_datetime(df["ts"])
    return df


def _profit_factor(ret: pd.Series) -> float | None:
    gross_pos = float(ret[ret > 0].sum())
    gross_neg = float(-ret[ret < 0].sum())
    if gross_neg == 0:
        return None
    return gross_pos / gross_neg


def _scenario_stats(rows: pd.DataFrame) -> dict[str, object]:
    n = int(len(rows))
    if n == 0:
        return {
            "evaluable_count": 0,
            "win_rate": None,
            "expectancy": None,
            "pf": None,
            "avg_next_open_to_high_ret": None,
            "avg_next_open_to_low_ret": None,
        }
    ret = rows["next_open_to_close_ret"].astype(float)
    return {
        "evaluable_count": n,
        "win_rate": float((ret > 0).mean()),
        "expectancy": float(ret.mean()),
        "pf": _profit_factor(ret),
        "avg_next_open_to_high_ret": float(rows["next_open_to_high_ret"].mean()),
        "avg_next_open_to_low_ret": float(rows["next_open_to_low_ret"].mean()),
    }


def build_validation(root: Path, out_dir: Path, timepoints: tuple[str, ...]) -> dict[str, object]:
    log_dir = root / "2_Logs"
    ohlcv_path = root / "paper" / "prices" / "ohlcv_paper.parquet"
    ohlcv = pd.read_parquet(ohlcv_path)
    ohlcv["code"] = ohlcv["code"].astype(str).str.zfill(6)
    ohlcv["date"] = ohlcv["date"].astype(str)
    next_by_date = _next_session_map(ohlcv)

    all_rows: list[pd.DataFrame] = []
    date_summaries: list[dict[str, object]] = []
    skipped_dates: list[dict[str, str]] = []

    for cand_path in sorted(log_dir.glob("candidates_v41_1_*.csv")):
        signal_date = _date8(cand_path.stem.rsplit("_", 1)[-1])
        intraday_path = log_dir / f"intraday_prices_history_{signal_date}.csv"
        if not intraday_path.exists():
            skipped_dates.append({"signal_date": signal_date, "reason": "missing_intraday_history"})
            continue
        next_date = next_by_date.get(signal_date)
        if not next_date:
            skipped_dates.append({"signal_date": signal_date, "reason": "missing_next_session_ohlcv"})
            continue
        prev_dates = [d for d in next_by_date if next_by_date[d] == signal_date]
        if not prev_dates:
            skipped_dates.append({"signal_date": signal_date, "reason": "missing_prev_session_ohlcv"})
            continue
        prev_date = prev_dates[-1]

        candidates = _read_candidates(cand_path)
        intraday = _read_intraday(intraday_path)
        prev_close = (
            ohlcv[ohlcv["date"].eq(prev_date)][["code", "close"]]
            .rename(columns={"close": "prev_close"})
        )
        next_ohlc = (
            ohlcv[ohlcv["date"].eq(next_date)][["code", "open", "close", "high", "low"]]
            .rename(
                columns={
                    "open": "next_open",
                    "close": "next_close",
                    "high": "next_high",
                    "low": "next_low",
                }
            )
        )

        keep = [
            "code",
            "name",
            "score",
            "relax_level",
            "candidate_origin",
            "execution_pool",
            "natural_pass",
            "horizon_label",
            "close",
            "value",
        ]
        base = candidates[[c for c in keep if c in candidates.columns]].copy()
        base = base.merge(prev_close, on="code", how="left").merge(next_ohlc, on="code", how="left")
        base["signal_date"] = signal_date
        base["prev_session"] = prev_date
        base["next_session"] = next_date

        date_info = {
            "signal_date": signal_date,
            "prev_session": prev_date,
            "next_session": next_date,
            "candidate_rows": int(len(base)),
            "candidate_with_prev_close": int(base["prev_close"].notna().sum()),
            "candidate_with_next_ohlc": int(base["next_open"].notna().sum()),
            "timepoints": [],
        }

        for hhmm in timepoints:
            cutoff = pd.Timestamp(f"{signal_date[:4]}-{signal_date[4:6]}-{signal_date[6:8]} {hhmm}:59")
            snap = (
                intraday[intraday["ts_dt"] <= cutoff]
                .sort_values("ts_dt")
                .groupby("code", as_index=False)
                .tail(1)
            )
            snap = snap[["code", "ts", "current_price", "open", "high", "low", "volume", "trading_value"]]
            df = base.merge(snap, on="code", how="left", suffixes=("", "_snap"))
            df["timepoint"] = hhmm
            df["has_snapshot"] = df["current_price"].notna()
            df["confirm_at_T"] = (
                df["has_snapshot"]
                & df["prev_close"].notna()
                & (df["current_price"] > df["open"])
                & (df["current_price"] > df["prev_close"])
            )
            df["next_open_to_close_ret"] = df["next_close"] / df["next_open"] - 1
            df["next_open_to_high_ret"] = df["next_high"] / df["next_open"] - 1
            df["next_open_to_low_ret"] = df["next_low"] / df["next_open"] - 1
            selected = df[df["confirm_at_T"] & df["next_open_to_close_ret"].notna()].copy()
            stats = _scenario_stats(selected)
            date_info["timepoints"].append(
                {
                    "timepoint": hhmm,
                    "snapshot_coverage": int(df["has_snapshot"].sum()),
                    "confirmed_count": int(df["confirm_at_T"].sum()),
                    **stats,
                }
            )
            all_rows.append(df)

        date_summaries.append(date_info)

    if all_rows:
        rows = pd.concat(all_rows, ignore_index=True)
    else:
        rows = pd.DataFrame()

    aggregate_results: list[dict[str, object]] = []
    for hhmm in timepoints:
        if rows.empty:
            selected = pd.DataFrame()
            snapshot_coverage = 0
            confirmed_count = 0
        else:
            subset = rows[rows["timepoint"].eq(hhmm)]
            snapshot_coverage = int(subset["has_snapshot"].sum())
            confirmed_count = int(subset["confirm_at_T"].sum())
            selected = subset[subset["confirm_at_T"] & subset["next_open_to_close_ret"].notna()]
        aggregate_results.append(
            {
                "timepoint": hhmm,
                "snapshot_coverage": snapshot_coverage,
                "confirmed_count": confirmed_count,
                **_scenario_stats(selected),
            }
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    rows_path = out_dir / "intraday_followthrough_timepoint_rows.csv"
    summary_path = out_dir / "intraday_followthrough_timepoint_summary.json"
    rows.to_csv(rows_path, index=False, encoding="utf-8-sig")
    summary = {
        "method": "intraday_followthrough_timepoint_validation",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "definition": "confirm_at_T = current_price > intraday open and current_price > previous session close; next-session simple return = next close / next open - 1",
        "inputs": {
            "candidates_glob": str(log_dir / "candidates_v41_1_*.csv"),
            "intraday_glob": str(log_dir / "intraday_prices_history_*.csv"),
            "ohlcv": str(ohlcv_path),
        },
        "rows_csv": str(rows_path),
        "date_summaries": date_summaries,
        "skipped_dates": skipped_dates,
        "aggregate_results": aggregate_results,
        "limits": [
            "This is validation-only and does not change operating buy policy.",
            "Only dates with candidates, same-day intraday history, previous-session close, and next-session OHLC are included.",
            "This is not an orders/fills/ledger/stats end-to-end portfolio simulation.",
        ],
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=r"E:\1_Data")
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--timepoints", default=",".join(DEFAULT_TIMEPOINTS))
    args = parser.parse_args()

    root = Path(args.root)
    out_dir = Path(args.out_dir) if args.out_dir else root / "tmp" / f"intraday_followthrough_validation_{datetime.now():%Y%m%d}"
    timepoints = tuple(x.strip() for x in args.timepoints.split(",") if x.strip())
    summary = build_validation(root, out_dir, timepoints)
    print(json.dumps({"summary": str(out_dir / "intraday_followthrough_timepoint_summary.json"), "aggregate_results": summary["aggregate_results"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
