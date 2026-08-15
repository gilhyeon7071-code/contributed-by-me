"""Shared candidate-selection core for v41.1.

This module is the single source of truth for the per-day candidate filter
used by both the optimizer (optimize_params_v41_1.py) and production
(generate_candidates_v41_1.py). Any change here affects both paths, which is
intentional: the optimizer must simulate exactly what production will do.
"""

from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd


def select_candidates_core(day_df: pd.DataFrame, p: dict) -> pd.DataFrame:
    """Apply the v41.1 candidate filter.

    Parameters
    ----------
    day_df : pd.DataFrame
        A single trading day's rows with factor columns.
    p : dict
        Normalized parameters.

    Returns
    -------
    pd.DataFrame
        Rows that pass all filters.
    """
    if day_df.empty:
        return day_df.copy()

    cond = (
        (day_df["rs"] > float(p["rs_lim"]))
        & (day_df["v_accel"] > float(p["v_accel_lim"]))
        & (day_df["v_accel"] <= float(p.get("v_accel_max", 5.0)))
        & (day_df["stretch"] < float(p["stretch_max"]))
        & (day_df["value"] > float(p["value_min"]))
        & (day_df["atr14_pct"] < float(p["atr_max"]))
        & (day_df["rsi14"] < float(p["rsi_max"]))
        & (day_df["vol_close_corr20"] >= float(p["vol_close_corr_min"]))
        & (day_df["high_52w_gap"] <= float(p["near_52w_high_gap_max"]))
        & (day_df["listing_days"] >= float(p["min_listing_days"]))
    )

    if float(p.get("require_macd_golden", 0.0) or 0.0) >= 0.5:
        cond = cond & (day_df["macd_golden"] == True)

    # Defense: skip or require positive rs_slope in bear/crash regime.
    if "market_regime" in day_df.columns:
        is_bear = day_df["market_regime"].astype(str).str.upper().isin({"BEAR", "CRASH"})
        if float(p.get("defense_bear_disable_entry") or 0.0) >= 0.5:
            cond = cond & (~is_bear)
        else:
            bear_defense = float(p.get("defense_bear_rs_slope_min") or 0.0)
            if bear_defense != 0.0 and "rs_slope" in day_df.columns:
                cond = cond & (~is_bear | (day_df["rs_slope"] >= bear_defense))

    # Sector blacklist.
    blacklist_raw = str(p.get("sector_blacklist", "")).strip()
    if blacklist_raw and "sector_code" in day_df.columns:
        blacklist = {int(float(x.strip())) for x in blacklist_raw.split(",") if x.strip()}
        if blacklist:
            sc_num = pd.to_numeric(day_df["sector_code"], errors="coerce")
            cond = cond & (~sc_num.isin(blacklist))

    # Market-cap floor.
    min_mcap = float(p.get("min_market_cap", 0.0) or 0.0)
    if min_mcap > 0.0 and "market_cap" in day_df.columns:
        cond = cond & (day_df["market_cap"] >= min_mcap)

    # MA200 requirement.
    if (
        float(p.get("require_above_ma200", 0.0) or 0.0) >= 0.5
        and "ma200" in day_df.columns
        and "close" in day_df.columns
    ):
        cond = cond & (day_df["close"] > day_df["ma200"])

    # Rule-e market/sector defense gates.
    for col, key in (
        ("mkt_ret20", "mkt_ret20_min"),
        ("mkt_ret60", "mkt_ret60_min"),
        ("sector_rs", "sector_rs_min"),
    ):
        thr = p.get(key, -1.0)
        if thr is None:
            continue
        try:
            thr_val = float(thr)
        except (TypeError, ValueError):
            continue
        if np.isfinite(thr_val) and thr_val > -1.0 and col in day_df.columns:
            cond = cond & (day_df[col] > thr_val)

    return day_df[cond].copy()
