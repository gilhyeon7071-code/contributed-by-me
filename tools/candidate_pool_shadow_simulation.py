#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Shadow simulation for candidate pool threshold changes.

Read-only with respect to live trading artifacts. Writes diagnostics only:
- 2_Logs/candidate_pool_shadow_simulation_YYYYMMDD_HHMMSS.json
- 2_Logs/candidate_pool_shadow_simulation_YYYYMMDD_HHMMSS.csv
- latest pointers for both files
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import generate_candidates_v41_1 as gen  # noqa: E402


def _ts() -> str:
    return pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def _load_chosen_params() -> dict[str, Any]:
    meta_path = LOG_DIR / "candidates_latest_meta.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        p = meta.get("chosen_params")
        if isinstance(p, dict) and p:
            return gen._normalize_params(p)
    stable_path = ROOT / "12_Risk_Controlled" / "stable_params_v41_1.json"
    return gen._normalize_params(gen.read_json(stable_path) or {})


def _scenario_params(base: dict[str, Any]) -> list[tuple[str, dict[str, Any], str]]:
    scenarios: list[tuple[str, dict[str, Any], str]] = []

    def add(name: str, updates: dict[str, float], note: str) -> None:
        p = dict(base)
        p.update(updates)
        scenarios.append((name, gen._normalize_params(p), note))

    scenarios.append(("baseline_current_l7", gen._normalize_params(dict(base)), "current chosen params from latest candidate meta"))
    add("disparity_l8_like", {"disparity20_max": 1.25, "disparity60_max": 1.36}, "relax only disparity caps to L8-like values")
    add("disparity_l9_like", {"disparity20_max": 1.30, "disparity60_max": 1.42}, "relax only disparity caps to L9-like values")
    add("v_accel_0_85", {"v_accel_lim": 0.85}, "relax only v_accel lower bound to 0.85")
    add("v_accel_0_65", {"v_accel_lim": 0.65}, "relax only v_accel lower bound to 0.65")
    add(
        "disparity_l8_v_accel_0_85",
        {"disparity20_max": 1.25, "disparity60_max": 1.36, "v_accel_lim": 0.85},
        "combined moderate disparity and v_accel relaxation",
    )
    return scenarios


def _natural_mask(today: pd.DataFrame, p: dict[str, Any]) -> pd.Series:
    idx = today.index
    adx_min = _f(p.get("adx_trend_min"), 20.0)
    stoch_min = _f(p.get("stoch_k_min"), 20.0)
    stoch_max = _f(p.get("stoch_k_max"), 85.0)

    adx = pd.to_numeric(today.get("adx14"), errors="coerce")
    stoch = pd.to_numeric(today.get("stoch_k"), errors="coerce")
    ret1_pct = pd.to_numeric(today.get("ret1_pct"), errors="coerce").fillna(0.0)
    limit_up_mask = ret1_pct >= _f(p.get("limit_up_entry_ret_pct"), 29.0)

    stretch_max = pd.Series(_f(p.get("stretch_max"), 1.19), index=idx, dtype=float)
    rsi_max = pd.Series(_f(p.get("rsi_max"), 70.0), index=idx, dtype=float)
    disp20_max = pd.Series(_f(p.get("disparity20_max"), 1.06), index=idx, dtype=float)
    disp60_max = pd.Series(_f(p.get("disparity60_max"), 1.12), index=idx, dtype=float)
    high52_max = pd.Series(_f(p.get("near_52w_high_gap_max"), 0.05), index=idx, dtype=float)
    if bool(limit_up_mask.any()):
        stretch_max.loc[limit_up_mask] = max(_f(p.get("stretch_max"), 1.19), _f(p.get("limit_up_stretch_max"), 1.25))
        rsi_max.loc[limit_up_mask] = max(_f(p.get("rsi_max"), 70.0), _f(p.get("limit_up_rsi_max"), 85.0))
        disp20_max.loc[limit_up_mask] = max(_f(p.get("disparity20_max"), 1.06), _f(p.get("limit_up_disparity20_max"), 1.35))
        disp60_max.loc[limit_up_mask] = max(_f(p.get("disparity60_max"), 1.12), _f(p.get("limit_up_disparity60_max"), 1.45))
        high52_max.loc[limit_up_mask] = max(_f(p.get("near_52w_high_gap_max"), 0.05), _f(p.get("limit_up_high52_gap_max"), 0.4))

    adx_cond = (adx >= adx_min).fillna(False)
    stoch_range = ((stoch >= stoch_min) & (stoch <= stoch_max)).fillna(False)
    stoch_trend = (stoch >= 50.0).fillna(False)
    stoch_cond = ((adx_cond & stoch_trend) | ((~adx_cond) & stoch_range)).fillna(False)

    rs_legacy = pd.to_numeric(today["rs"], errors="coerce") > _f(p.get("rs_lim"), 1.7)
    rs_excess = pd.to_numeric(today.get("rs_excess"), errors="coerce") > _f(p.get("rs_excess_min"), 0.02)
    rs_cond = (rs_legacy | rs_excess).fillna(False)

    return (
        rs_cond
        & (pd.to_numeric(today["stretch"], errors="coerce") < stretch_max).fillna(False)
        & (pd.to_numeric(today["atr14_pct"], errors="coerce") < _f(p.get("atr_max"), 0.12)).fillna(False)
        & (pd.to_numeric(today["rsi14"], errors="coerce") < rsi_max).fillna(False)
        & adx_cond
        & stoch_cond
        & (pd.to_numeric(today.get("bb_width"), errors="coerce") >= _f(p.get("bb_width_min"), 0.0)).fillna(False)
        & (pd.to_numeric(today["vol_close_corr20"], errors="coerce") >= _f(p.get("vol_close_corr_min"), 0.0)).fillna(False)
        & (pd.to_numeric(today["listing_days"], errors="coerce") >= _f(p.get("min_listing_days"), 126.0)).fillna(False)
        & (pd.to_numeric(today["disparity20"], errors="coerce") <= disp20_max).fillna(False)
        & (pd.to_numeric(today["disparity60"], errors="coerce") <= disp60_max).fillna(False)
        & (pd.to_numeric(today["value"], errors="coerce") > _f(p.get("value_min"), 1_000_000_000.0)).fillna(False)
        & (pd.to_numeric(today["v_accel"], errors="coerce") > _f(p.get("v_accel_lim"), 2.5)).fillna(False)
        & (pd.to_numeric(today["high_52w_gap"], errors="coerce") <= high52_max).fillna(False)
    ).fillna(False)


def _summarize_selection(df: pd.DataFrame, mask: pd.Series, target: str) -> dict[str, Any]:
    selected = df.loc[mask].copy()
    t_all = pd.to_numeric(df[target], errors="coerce")
    t_sel = pd.to_numeric(selected[target], errors="coerce")
    return {
        "selected_n": int(mask.sum()),
        "selected_mean_bps": float(t_sel.mean() * 10000.0) if t_sel.notna().any() else None,
        "universe_mean_bps": float(t_all.mean() * 10000.0) if t_all.notna().any() else None,
        "alpha_bps": float((t_sel.mean() - t_all.mean()) * 10000.0) if t_sel.notna().any() and t_all.notna().any() else None,
        "win_rate": float((t_sel > 0).mean()) if t_sel.notna().any() else None,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback-days", type=int, default=120)
    ap.add_argument("--min-universe", type=int, default=1000)
    ap.add_argument("--target-horizon", type=int, default=5, choices=[1, 2, 5])
    args = ap.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    raw = gen._load_data()
    if raw.empty:
        print("[ERR] raw data empty")
        return 2

    df, latest_dt, _ = gen._compute_factors(raw)
    if df.empty:
        print("[ERR] factor data empty")
        return 3

    df = df.sort_values(["code", "date"]).reset_index(drop=True)
    target = f"fwd_ret_{int(args.target_horizon)}d"
    df[target] = df.groupby("code")["close"].shift(-int(args.target_horizon)) / (df["close"] + 1e-12) - 1.0
    df["date8"] = pd.to_datetime(df["date"]).dt.strftime("%Y%m%d")

    max_d = pd.to_datetime(df["date"].max())
    start_d = max_d - pd.Timedelta(days=int(args.lookback_days))
    dx = df[df["date"] >= start_d].copy()
    uni = dx.groupby("date")["code"].nunique()
    valid_dates = uni[uni >= int(args.min_universe)].index
    hist = dx[dx["date"].isin(valid_dates)].copy()
    latest_date8 = str(pd.to_datetime(latest_dt).strftime("%Y%m%d"))
    latest = df[df["date8"] == latest_date8].copy()
    if hist.empty or latest.empty:
        print("[ERR] no simulation rows")
        return 4

    base = _load_chosen_params()
    rows: list[dict[str, Any]] = []
    daily_rows: list[dict[str, Any]] = []
    for name, params, note in _scenario_params(base):
        hist_masks = []
        for d, day in hist.groupby("date", sort=True):
            mask = _natural_mask(day, params)
            hist_masks.append(pd.Series(mask.values, index=day.index))
            day_summary = _summarize_selection(day, mask, target)
            daily_rows.append({"scenario": name, "date": str(pd.to_datetime(d).date()), **day_summary})
        hist_mask = pd.concat(hist_masks).reindex(hist.index).fillna(False).astype(bool)
        hist_summary = _summarize_selection(hist, hist_mask, target)

        latest_mask = _natural_mask(latest, params)
        latest_summary = _summarize_selection(latest, latest_mask, target)
        selected_days = int(sum(1 for r in daily_rows if r["scenario"] == name and int(r["selected_n"]) > 0))
        scenario_daily = [r for r in daily_rows if r["scenario"] == name]
        avg_selected_n = float(np.mean([float(r["selected_n"]) for r in scenario_daily])) if scenario_daily else 0.0
        avg_alpha = float(np.nanmean([r["alpha_bps"] for r in scenario_daily if r["alpha_bps"] is not None])) if scenario_daily else None
        rows.append(
            {
                "scenario": name,
                "note": note,
                "latest_selected_n": int(latest_summary["selected_n"]),
                "hist_selected_n": int(hist_summary["selected_n"]),
                "hist_dates": int(hist["date"].nunique()),
                "hist_selected_days": selected_days,
                "hist_selected_day_ratio": float(selected_days / max(1, hist["date"].nunique())),
                "hist_avg_selected_n": avg_selected_n,
                "hist_avg_alpha_bps": avg_alpha,
                "hist_win_rate": hist_summary["win_rate"],
                "hist_selected_mean_bps": hist_summary["selected_mean_bps"],
                "params": {
                    "v_accel_lim": float(params["v_accel_lim"]),
                    "disparity20_max": float(params["disparity20_max"]),
                    "disparity60_max": float(params["disparity60_max"]),
                    "value_min": float(params["value_min"]),
                    "near_52w_high_gap_max": float(params["near_52w_high_gap_max"]),
                },
            }
        )

    out = pd.DataFrame(rows).sort_values(["latest_selected_n", "hist_avg_alpha_bps"], ascending=[False, False])
    ts = _ts()
    csv_path = LOG_DIR / f"candidate_pool_shadow_simulation_{ts}.csv"
    json_path = LOG_DIR / f"candidate_pool_shadow_simulation_{ts}.json"
    latest_csv = LOG_DIR / "candidate_pool_shadow_simulation_latest.csv"
    latest_json = LOG_DIR / "candidate_pool_shadow_simulation_latest.json"
    out_for_csv = out.drop(columns=["params"], errors="ignore")
    out_for_csv.to_csv(csv_path, index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "PASS",
        "mode": "shadow_simulation_only_no_policy_change",
        "scope_note": "technical natural-pass filters only; excludes fundamental overlay, balanced rescue, sector union, order/fill/ledger/stats effects",
        "latest_date": latest_date8,
        "target": target,
        "lookback_days": int(args.lookback_days),
        "min_universe": int(args.min_universe),
        "hist_rows": int(len(hist)),
        "hist_dates": int(hist["date"].nunique()),
        "latest_rows": int(len(latest)),
        "scenarios": rows,
        "paths": {"csv": str(csv_path), "json": str(json_path), "latest_csv": str(latest_csv), "latest_json": str(latest_json)},
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    shutil.copyfile(csv_path, latest_csv)
    shutil.copyfile(json_path, latest_json)
    print(json.dumps({"json": str(json_path), "csv": str(csv_path), "scenarios": len(rows)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
