"""Shared candidate-selection core for v41.1.

의도: 옵티마이저(optimize_params_v41_1.py)와 생산(generate_candidates_v41_1.py)이
같은 후보 필터를 쓰게 하는 것.

[2026-08-21 실측 정정] **현재 그 의도는 지켜지지 않는다.**
이 파일은 옵티마이저/백테스트만 쓴다. 생산 `generate_candidates_v41_1.py` 는
이 모듈을 import 하지 않고 자체 필터를 갖고 있으며, 아래 게이트들이 서로 다르다.

이 모듈에만 있고 생산에는 없는 게이트 (stable_params_v41_1.json 기준):
    v_accel_max=5.0 / defense_bear_disable_entry=1.0 / defense_bear_rs_slope_min=-0.015
    sector_blacklist="005,024" / min_market_cap=1e11 / require_above_ma200=1.0
생산은 2026-08-15 에 v_accel_max 와 bear 게이트를 뺐고(그 경위는
generate_candidates_v41_1.py 의 rule_e 주석 참조) 이쪽은 그대로 남았다.

그 결과 현재 파라미터로 이 필터는 **논리적 공집합**이다:
    (v_accel > v_accel_lim 6.6) AND (v_accel <= v_accel_max 5.0)
실측(2026-08-21): 생산 후보 21종목을 이 필터에 넣으면 0종목 통과.
6개 게이트를 다 풀어도 0종목.

진입 시점과 갭 필터도 다르다:
    옵티마이저 same_close(신호일 종가, 갭 필터 미적용)
    백테스트   next_open(기본값)
    생산 실제  intraday_realtime 54% / same_close 8%, 갭 필터 적용

**따라서 이 모듈을 고쳐도 생산 동작은 바뀌지 않는다.** 양쪽을 실제로 일치시키려면
생산이 이 모듈을 쓰도록 배선하는 별도 작업이 필요하다.
상세: .agent/PLANS.md 2026-08-21 (17)(19)
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
