#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Technical indicator diagnostic for STOC v41.1 factors.

Outputs (E:\1_Data\2_Logs):
- indicator_diag_summary[_<tag>]_YYYYMMDD_HHMMSS.json (+ latest pointer)
- indicator_diag_continuous[_<tag>]_YYYYMMDD_HHMMSS.csv (+ latest pointer)
- indicator_diag_binary[_<tag>]_YYYYMMDD_HHMMSS.csv (+ latest pointer)
- indicator_diag_daily_combo[_<tag>]_YYYYMMDD_HHMMSS.csv (+ latest pointer)
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import sys
from typing import Dict, List

import numpy as np
import pandas as pd


ROOT = Path(r"E:\1_Data")
LOG_DIR = ROOT / "2_Logs"
RISK_DIR = ROOT / "12_Risk_Controlled"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import generate_candidates_v41_1 as gen  # noqa: E402


def _ts_now() -> str:
    return pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")


def _safe_qcut_ranked(s: pd.Series, q: int = 5) -> pd.Series:
    r = s.rank(method="first")
    return pd.qcut(r, q=q, labels=False) + 1


def _build_pass_columns(df: pd.DataFrame, p: Dict[str, float]) -> pd.DataFrame:
    out = df.copy()
    out["rs_pass"] = out["rs"] > float(p["rs_lim"])
    out["v_accel_pass"] = out["v_accel"] > float(p["v_accel_lim"])
    out["stretch_pass"] = out["stretch"] < float(p["stretch_max"])
    out["value_pass"] = out["value"] > float(p["value_min"])
    out["atr_pass"] = out["atr14_pct"] < float(p["atr_max"])
    out["rsi_pass"] = out["rsi14"] < float(p["rsi_max"])
    out["volcorr_pass"] = out["vol_close_corr20"] >= float(p["vol_close_corr_min"])
    out["high52_pass"] = out["high_52w_gap"] <= float(p["near_52w_high_gap_max"])
    out["listing_pass"] = out["listing_days"] >= float(p["min_listing_days"])

    require_macd = float(p.get("require_macd_golden", 0.0) or 0.0) >= 0.5
    if require_macd:
        out["all_pass"] = (
            out["rs_pass"]
            & out["v_accel_pass"]
            & out["stretch_pass"]
            & out["value_pass"]
            & out["atr_pass"]
            & out["rsi_pass"]
            & out["volcorr_pass"]
            & out["high52_pass"]
            & out["listing_pass"]
            & (out["macd_golden"] == True)
        )
    else:
        out["all_pass"] = (
            out["rs_pass"]
            & out["v_accel_pass"]
            & out["stretch_pass"]
            & out["value_pass"]
            & out["atr_pass"]
            & out["rsi_pass"]
            & out["volcorr_pass"]
            & out["high52_pass"]
            & out["listing_pass"]
        )
    return out


def _continuous_diag(df: pd.DataFrame, factors: List[str], target: str) -> pd.DataFrame:
    rows = []
    for col in factors:
        x = df[[col, target]].dropna()
        n = len(x)
        if n < 200:
            rows.append(
                {
                    "factor": col,
                    "n": n,
                    "spearman": np.nan,
                    "q1_mean_bps": np.nan,
                    "q5_mean_bps": np.nan,
                    "q5_minus_q1_bps": np.nan,
                    "q1_winrate": np.nan,
                    "q5_winrate": np.nan,
                }
            )
            continue

        # Spearman without scipy dependency (rank correlation via Pearson on ranks)
        spearman = float(x[col].rank(method="average").corr(x[target].rank(method="average")))

        try:
            q = _safe_qcut_ranked(x[col], q=5)
            q1 = x.loc[q == 1, target]
            q5 = x.loc[q == 5, target]
            q1_mean = float(q1.mean() * 10000.0)
            q5_mean = float(q5.mean() * 10000.0)
            q1_win = float((q1 > 0).mean())
            q5_win = float((q5 > 0).mean())
            delta = q5_mean - q1_mean
        except Exception:
            q1_mean = np.nan
            q5_mean = np.nan
            q1_win = np.nan
            q5_win = np.nan
            delta = np.nan

        rows.append(
            {
                "factor": col,
                "n": n,
                "spearman": spearman,
                "q1_mean_bps": q1_mean,
                "q5_mean_bps": q5_mean,
                "q5_minus_q1_bps": delta,
                "q1_winrate": q1_win,
                "q5_winrate": q5_win,
            }
        )

    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values("q5_minus_q1_bps", ascending=False, na_position="last")
    return out


def _binary_diag(df: pd.DataFrame, cols: List[str], target: str) -> pd.DataFrame:
    rows = []
    for col in cols:
        x = df[[col, target]].dropna()
        if x.empty:
            continue
        x = x.copy()
        x[col] = x[col].astype(bool)
        pass_n = int(x[col].sum())
        fail_n = int((~x[col]).sum())
        n = int(len(x))

        pass_rate = float(pass_n / n) if n else np.nan
        pass_ret = float(x.loc[x[col], target].mean() * 10000.0) if pass_n else np.nan
        fail_ret = float(x.loc[~x[col], target].mean() * 10000.0) if fail_n else np.nan
        delta = pass_ret - fail_ret if (not np.isnan(pass_ret) and not np.isnan(fail_ret)) else np.nan
        pass_win = float((x.loc[x[col], target] > 0).mean()) if pass_n else np.nan
        fail_win = float((x.loc[~x[col], target] > 0).mean()) if fail_n else np.nan

        rows.append(
            {
                "filter": col,
                "n": n,
                "pass_n": pass_n,
                "fail_n": fail_n,
                "pass_rate": pass_rate,
                "pass_mean_bps": pass_ret,
                "fail_mean_bps": fail_ret,
                "pass_minus_fail_bps": delta,
                "pass_winrate": pass_win,
                "fail_winrate": fail_win,
            }
        )

    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values("pass_minus_fail_bps", ascending=False, na_position="last")
    return out


def _daily_combo_diag(df: pd.DataFrame, target: str) -> pd.DataFrame:
    rows = []
    for d, x in df.groupby("date", sort=True):
        t = pd.to_numeric(x[target], errors="coerce")
        sel = x["all_pass"].astype(bool)
        t_sel = pd.to_numeric(x.loc[sel, target], errors="coerce")
        rows.append(
            {
                "date": d,
                "universe_n": int(len(x)),
                "selected_n": int(sel.sum()),
                "universe_mean_bps": float(t.mean() * 10000.0) if t.notna().any() else np.nan,
                "selected_mean_bps": float(t_sel.mean() * 10000.0) if t_sel.notna().any() else np.nan,
            }
        )
    g = pd.DataFrame(rows)
    g["alpha_bps"] = g["selected_mean_bps"] - g["universe_mean_bps"]
    return g


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback-days", type=int, default=120)
    ap.add_argument("--min-universe", type=int, default=2000)
    ap.add_argument("--target-horizon", type=int, default=1, choices=[1, 2, 5])
    ap.add_argument("--tag", type=str, default="")
    args = ap.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    raw = gen._load_data()
    if raw.empty:
        print("[ERR] no raw data")
        return 2

    df, latest_dt, _ = gen._compute_factors(raw)
    if df.empty:
        print("[ERR] no factor data")
        return 2

    df = df.sort_values(["code", "date"]).reset_index(drop=True)
    for h in [1, 2, 5]:
        df[f"fwd_ret_{h}d"] = df.groupby("code")["close"].shift(-h) / (df["close"] + 1e-12) - 1.0

    max_d = pd.to_datetime(df["date"].max())
    start_d = max_d - pd.Timedelta(days=int(args.lookback_days))
    dx = df[df["date"] >= start_d].copy()

    uni = dx.groupby("date")["code"].nunique()
    valid_dates = uni[uni >= int(args.min_universe)].index
    dx = dx[dx["date"].isin(valid_dates)].copy()
    if dx.empty:
        print("[ERR] no rows after lookback/min-universe filter")
        return 3

    stable_path = RISK_DIR / "stable_params_v41_1.json"
    params_raw = gen.read_json(stable_path) or {}
    params = gen._normalize_params(params_raw)
    dx = _build_pass_columns(dx, params)

    target = f"fwd_ret_{int(args.target_horizon)}d"

    cont_cols = [
        "rs",
        "rs_slope",
        "v_accel",
        "stretch",
        "atr14_pct",
        "rsi14",
        "vol_close_corr20",
        "high_52w_gap",
        "listing_days",
    ]
    bin_cols = [
        "rs_pass",
        "v_accel_pass",
        "stretch_pass",
        "value_pass",
        "atr_pass",
        "rsi_pass",
        "volcorr_pass",
        "high52_pass",
        "listing_pass",
        "macd_golden",
        "all_pass",
    ]

    cont = _continuous_diag(dx, cont_cols, target=target)
    binary = _binary_diag(dx, bin_cols, target=target)
    daily_combo = _daily_combo_diag(dx, target=target)

    ts = _ts_now()
    tag = (args.tag or "").strip()
    tag_suffix = f"_{tag}" if tag else ""

    cont_path = LOG_DIR / f"indicator_diag_continuous{tag_suffix}_{ts}.csv"
    bin_path = LOG_DIR / f"indicator_diag_binary{tag_suffix}_{ts}.csv"
    combo_path = LOG_DIR / f"indicator_diag_daily_combo{tag_suffix}_{ts}.csv"
    summary_path = LOG_DIR / f"indicator_diag_summary{tag_suffix}_{ts}.json"

    cont.to_csv(cont_path, index=False, encoding="utf-8-sig")
    binary.to_csv(bin_path, index=False, encoding="utf-8-sig")
    daily_combo.to_csv(combo_path, index=False, encoding="utf-8-sig")

    selected_days = int((daily_combo["selected_n"] > 0).sum())
    summary = {
        "as_of": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "latest_date": str(pd.to_datetime(latest_dt).date()),
        "window": {
            "start_date": str(pd.to_datetime(dx["date"].min()).date()),
            "end_date": str(pd.to_datetime(dx["date"].max()).date()),
            "lookback_days": int(args.lookback_days),
            "min_universe": int(args.min_universe),
            "rows": int(len(dx)),
            "dates": int(dx["date"].nunique()),
            "codes": int(dx["code"].nunique()),
        },
        "target": target,
        "target_horizon_days": int(args.target_horizon),
        "tag": tag,
        "params": {
            "rs_lim": float(params["rs_lim"]),
            "v_accel_lim": float(params["v_accel_lim"]),
            "stretch_max": float(params["stretch_max"]),
            "value_min": float(params["value_min"]),
            "atr_max": float(params["atr_max"]),
            "rsi_max": float(params["rsi_max"]),
            "require_macd_golden": float(params["require_macd_golden"]),
            "vol_close_corr_min": float(params["vol_close_corr_min"]),
            "near_52w_high_gap_max": float(params["near_52w_high_gap_max"]),
            "min_listing_days": float(params["min_listing_days"]),
        },
        "combo": {
            "selected_days": selected_days,
            "total_days": int(len(daily_combo)),
            "selected_day_ratio": float(selected_days / len(daily_combo)) if len(daily_combo) else 0.0,
            "avg_selected_n": float(daily_combo["selected_n"].mean()) if len(daily_combo) else 0.0,
            "avg_alpha_bps": float(daily_combo["alpha_bps"].mean(skipna=True)) if len(daily_combo) else np.nan,
            "positive_alpha_day_ratio": float((daily_combo["alpha_bps"] > 0).mean(skipna=True)) if len(daily_combo) else np.nan,
        },
        "top_continuous": cont.head(5).to_dict(orient="records"),
        "top_binary": binary.head(5).to_dict(orient="records"),
        "paths": {
            "continuous_csv": str(cont_path),
            "binary_csv": str(bin_path),
            "daily_combo_csv": str(combo_path),
            "summary_json": str(summary_path),
        },
    }

    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    if tag:
        shutil.copyfile(cont_path, LOG_DIR / f"indicator_diag_continuous_latest_{tag}.csv")
        shutil.copyfile(bin_path, LOG_DIR / f"indicator_diag_binary_latest_{tag}.csv")
        shutil.copyfile(combo_path, LOG_DIR / f"indicator_diag_daily_combo_latest_{tag}.csv")
        shutil.copyfile(summary_path, LOG_DIR / f"indicator_diag_summary_latest_{tag}.json")
    else:
        shutil.copyfile(cont_path, LOG_DIR / "indicator_diag_continuous_latest.csv")
        shutil.copyfile(bin_path, LOG_DIR / "indicator_diag_binary_latest.csv")
        shutil.copyfile(combo_path, LOG_DIR / "indicator_diag_daily_combo_latest.csv")
        shutil.copyfile(summary_path, LOG_DIR / "indicator_diag_summary_latest.json")

    print(f"[OK] continuous={cont_path}")
    print(f"[OK] binary={bin_path}")
    print(f"[OK] combo={combo_path}")
    print(f"[OK] summary={summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

