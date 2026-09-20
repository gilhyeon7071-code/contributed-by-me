# -*- coding: utf-8 -*-
"""Quick diagnostic: candidate frequency under constraint variations."""
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import generate_candidates_v41_1 as gc

BASE = Path(__file__).resolve().parent
DATA_DIR = BASE / "krx_daily_archive"
STABLE = BASE / "12_Risk_Controlled" / "stable_params_v41_1.json"




def load_stable_params() -> dict:
    raw = json.loads(STABLE.read_text(encoding="utf-8-sig"))
    if isinstance(raw, dict) and "params" in raw:
        return dict(raw["params"])
    return dict(raw)


def count_candidates_by_year(df_factored: pd.DataFrame, params: dict):
    """Return candidate counts aggregated by calendar year and by month."""
    results = []
    for dt, grp in df_factored.groupby("date"):
        sel = gc._select_candidates(grp.copy(), params)
        results.append({"date": dt, "n_candidates": len(sel), "n_universe": len(grp)})
    res = pd.DataFrame(results).sort_values("date")
    res["year"] = res["date"].dt.year
    res["month"] = res["date"].dt.to_period("M").astype(str)
    return res


def scenario(name: str, base: dict, overrides: dict):
    p = {**base, **overrides}
    p = gc._normalize_params(p)
    return name, p


def main():
    df = gc._load_data(max_gap_sessions=0)
    if df.empty:
        print("[ERR] no data loaded")
        return
    base = load_stable_params()
    print(f"Loaded {len(df):,} rows from {df['date'].min().date()} to {df['date'].max().date()}")

    print("Computing factors (one-time)...")
    df_factored, latest_dt, is_bull = gc._compute_factors(df.copy())
    print(f"Factored {len(df_factored):,} rows, latest={latest_dt.date()}, bull={is_bull}")

    scenarios = [
        scenario("current", base, {}),
        scenario("no_bear_block", base, {"defense_bear_disable_entry": 0.0}),
        scenario("rule_e_off", base, {
            "mkt_ret20_min": -1.0,
            "mkt_ret60_min": -1.0,
            "sector_rs_min": -1.0,
        }),
        scenario("no_bear_no_rule_e", base, {
            "defense_bear_disable_entry": 0.0,
            "mkt_ret20_min": -1.0,
            "mkt_ret60_min": -1.0,
            "sector_rs_min": -1.0,
        }),
        scenario("value_min_1000e", base, {"value_min": 100_000_000_000.0}),
        scenario("value_min_500e", base, {"value_min": 50_000_000_000.0}),
        scenario("value_min_100e", base, {"value_min": 10_000_000_000.0}),
        scenario("rule_e_off_value_1000e", base, {
            "mkt_ret20_min": -1.0,
            "mkt_ret60_min": -1.0,
            "sector_rs_min": -1.0,
            "value_min": 100_000_000_000.0,
        }),
        scenario("rule_e_off_value_500e", base, {
            "mkt_ret20_min": -1.0,
            "mkt_ret60_min": -1.0,
            "sector_rs_min": -1.0,
            "value_min": 50_000_000_000.0,
        }),
        scenario("rule_e_off_value_100e", base, {
            "mkt_ret20_min": -1.0,
            "mkt_ret60_min": -1.0,
            "sector_rs_min": -1.0,
            "value_min": 10_000_000_000.0,
        }),
    ]

    summary = []
    for name, params in scenarios:
        res = count_candidates_by_year(df_factored, params)
        if res.empty:
            continue
        yearly = res.groupby("year")["n_candidates"].sum().reset_index()
        total = yearly["n_candidates"].sum()
        active_days = int((res["n_candidates"] > 0).sum())
        all_days = len(res)
        summary.append({
            "scenario": name,
            "total_candidates": total,
            "active_days": active_days,
            "all_days": all_days,
            "active_day_rate": active_days / all_days if all_days else 0,
            "annual_mean": total / max(1, res["year"].nunique()),
            "yearly": yearly.to_dict("records"),
        })
        print(f"\n{name}: total={total:,}, active_days={active_days}/{all_days} ({active_days/all_days:.1%}), annual_mean={total/max(1, res['year'].nunique()):.0f}")
        print(yearly.to_string(index=False))

    # Convert numpy/pandas ints to native Python for JSON serialization
    def _convert(obj):
        if isinstance(obj, dict):
            return {k: _convert(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_convert(v) for v in obj]
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        return obj

    out_path = BASE / "2_Logs" / "frequency_constraint_diag_latest.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(_convert(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nSaved summary to {out_path}")


if __name__ == "__main__":
    main()
