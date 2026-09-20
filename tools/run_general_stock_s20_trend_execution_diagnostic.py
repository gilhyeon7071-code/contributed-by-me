"""Read-only cost-aware execution diagnostic for S20 Trend Following cohort."""
from __future__ import annotations

import hashlib
import importlib.util
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
BASE_RUNNER_PATH = ROOT / "tools" / "run_general_stock_structure_outcome_map.py"
EXPLORER_PATH = ROOT / "tools" / "run_general_stock_s20_trend_exploration.py"
OUT_PREFIX = "general_stock_s20_trend_execution_diagnostic_20260723_costfix"
# 2026-07-23: replaced the original unfounded 1.4%/0.6% guesses with the
# verified cost model from PLANS.md same date (fee_pct 0.5%->0.004%,
# sell_tax_pct 0.2%->0.15%, real online/API brokerage rates; slippage_pct
# left unchanged at flat 0.1%, no verified basis to alter it).
# [2026-09-10] 0.00358 -> 0.00400. 2026-07-23 모델(fee 0.00004*2 + slip 0.001*2 + tax 0.0015)의
#   역산이었는데, 2026-08-24 브로커 실측(tr_id TTTC8715R: 수수료 0 / 제세금 0.19723%)으로
#   fee 0.0 / tax 0.002 가 확정됐다. optimize_params_v41_1.DEFAULT_FEE 와 같은 값이다.
#   차이는 0.042%p 로 작지만, 비용 세계가 여럿이면 어느 숫자가 맞는지 알 수 없게 된다.
# REALISTIC: round-trip fee(2x0.0) + slippage(2x0.001) + tax(0.002) = 0.00400
# FLOOR (zero-slippage sensitivity): round-trip fee(2x0.0) + tax(0.002) = 0.00200
COST_BASELINE = 0.00400
COST_SENSITIVITY = 0.00200


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_module(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def safe_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def add_path_dependent_returns(universe: pd.DataFrame, horizons: tuple[int, ...]) -> pd.DataFrame:
    """Calculates forward returns considering trailing stops."""
    out = universe.copy()
    
    calendar = pd.DatetimeIndex(sorted(out["date"].dropna().unique()))
    positions = {day: position for position, day in enumerate(calendar)}
    
    # Pre-calculate future prices by shifting
    # For a real robust vectorized approach in Pandas, we can use rolling on the reverse sorted data, 
    # but it's simpler to just calculate for the exact signals since they are sparse.
    
    # We will build a lookup table for prices
    price_lookup = out.set_index(["_entity_key", "date"])[["open", "high", "low", "close", "atr20"]].sort_index()
    
    signal_rows = out.loc[out["is_s20_trend_candidate"]].copy()
    
    results = []
    
    for idx, row in signal_rows.iterrows():
        entity = row["_entity_key"]
        start_date = row["entry_date"]
        start_pos_idx = positions.get(start_date)
        if start_pos_idx is None:
            continue
            
        # Entry price is open of the next day
        entry_price = row["entry_open"]
        if pd.isna(entry_price) or entry_price <= 0:
            continue
            
        atr20 = row["atr20"]
        
        # Get forward horizon slice
        for h in horizons:
            end_pos_idx = min(start_pos_idx + h, len(calendar) - 1)
            end_date = calendar[end_pos_idx]
            
            try:
                # Get the path from entry to horizon
                path = price_lookup.loc[(entity, start_date):(entity, end_date)]
                if path.empty:
                    continue
                
                path = path.copy()
                path["cummax_high"] = path["high"].cummax()
                
                # Exit A: -20% from cummax high
                path["exit_a_trigger"] = path["low"] <= path["cummax_high"] * 0.80
                
                # Exit B: -3x ATR from cummax high
                path["exit_b_trigger"] = path["low"] <= (path["cummax_high"] - 3 * atr20)
                
                # Determine exits
                exit_a_idx = path["exit_a_trigger"].idxmax() if path["exit_a_trigger"].any() else None
                exit_b_idx = path["exit_b_trigger"].idxmax() if path["exit_b_trigger"].any() else None
                
                # Calculate Returns
                def calc_return(exit_idx, trigger_col, stop_price_calc):
                    if exit_idx:
                        # hit stop loss
                        stop_price = stop_price_calc(path.loc[exit_idx, "cummax_high"])
                        # If open is already below stop price, we get stopped out at open
                        actual_exit = min(path.loc[exit_idx, "open"], stop_price)
                    else:
                        # hit horizon end
                        actual_exit = path.iloc[-1]["close"]
                    return actual_exit / entry_price - 1.0
                
                ret_a = calc_return(exit_a_idx, "exit_a_trigger", lambda h_max: h_max * 0.80)
                ret_b = calc_return(exit_b_idx, "exit_b_trigger", lambda h_max: h_max - 3 * atr20)
                
                results.append({
                    "index": idx,
                    "horizon": h,
                    "gross_return_A": ret_a,
                    "gross_return_B": ret_b,
                    "net_return_A_base": (ret_a + 1.0) * (1 - COST_BASELINE) - 1.0,
                    "net_return_A_sens": (ret_a + 1.0) * (1 - COST_SENSITIVITY) - 1.0,
                    "net_return_B_base": (ret_b + 1.0) * (1 - COST_BASELINE) - 1.0,
                    "net_return_B_sens": (ret_b + 1.0) * (1 - COST_SENSITIVITY) - 1.0,
                })
            except KeyError:
                continue
                
    res_df = pd.DataFrame(results)
    if not res_df.empty:
        for h in horizons:
            h_df = res_df[res_df["horizon"] == h].set_index("index")
            out[f"net_return_A_base_h{h}"] = h_df["net_return_A_base"]
            out[f"net_return_A_sens_h{h}"] = h_df["net_return_A_sens"]
            out[f"net_return_B_base_h{h}"] = h_df["net_return_B_base"]
            out[f"net_return_B_sens_h{h}"] = h_df["net_return_B_sens"]
            
    return out


def daily_basket(frame: pd.DataFrame, return_column: str) -> pd.DataFrame:
    return frame.groupby("date", as_index=False).agg(
        net_return=(return_column, "mean"),
        constituent_count=(return_column, "size"),
    )


def summarize(valid: pd.DataFrame, return_column: str, partition: str) -> dict[str, object]:
    if valid.empty:
        return {
            "partition": partition, "horizon": return_column, "signal_dates": 0,
            "candidate_rows": 0, "avg_constituents": None, "mean_daily_net_return_bps": None,
            "positive_daily_net_rate": None, "status": "DEFERRED_INSUFFICIENT_SAMPLE",
        }
    daily = daily_basket(valid, return_column)
    return {
        "partition": partition,
        "horizon": return_column,
        "signal_dates": int(len(daily)),
        "candidate_rows": int(daily["constituent_count"].sum()),
        "avg_constituents": round(float(daily["constituent_count"].mean()), 4),
        "mean_daily_net_return_bps": round(float(daily["net_return"].mean() * 10_000), 4),
        "positive_daily_net_rate": round(float(daily["net_return"].gt(0).mean()), 6),
        "status": "DESCRIPTIVE" if len(daily) >= 30 else "DEFERRED_INSUFFICIENT_SAMPLE",
    }


def main() -> int:
    base = load_module("base", BASE_RUNNER_PATH)
    explorer = load_module("explorer", EXPLORER_PATH)
    
    print("[1/5] loading integrity-filtered price history")
    report = base.load_report()
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})
    
    print("[2/5] building S20 Trend candidates")
    universe = base.build_forward_panel(raw)
    universe = base.attach_point_in_time_universe(universe)
    universe = base.add_observable_states(universe)
    universe["in_structural_cohort"] = universe["stock_trend_state"].eq("UPTREND") & universe["liquidity_tier"].eq("HIGH_LIQUID")
    universe = explorer.add_trend_signals(universe)
    
    print("[3/5] calculating path-dependent trailing stops and costs")
    universe = add_path_dependent_returns(universe, base.HORIZONS)
    
    print("[4/5] building fixed execution diagnostic summary")
    rows: list[dict[str, object]] = []
    
    s20_signals = universe.loc[universe["is_s20_trend_candidate"]].copy()
    
    for partition, start, end in base.PARTITIONS:
        part = s20_signals.loc[s20_signals["date"].between(pd.Timestamp(start), pd.Timestamp(end))].copy()
        for horizon in base.HORIZONS:
            for exit_type in ["A", "B"]:
                for cost_type in ["base", "sens"]:
                    return_column = f"net_return_{exit_type}_{cost_type}_h{horizon}"
                    if return_column in part.columns:
                        valid = part.loc[part[return_column].notna()].copy()
                        rows.append(summarize(valid, return_column, partition))
                        
    summary = pd.DataFrame(rows).sort_values(["partition", "horizon"], kind="mergesort").reset_index(drop=True)
    
    print("[5/5] writing research-only evidence")
    summary_path = LOG_DIR / f"{OUT_PREFIX}_summary.csv"
    json_path = LOG_DIR / f"{OUT_PREFIX}.json"
    md_path = LOG_DIR / f"{OUT_PREFIX}.md"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "read_only_s20_trend_execution_diagnostic",
        "exploratory_only": True,
        "price_history_contract": integrity,
        "selection": "S20 Trend Candidate (Breakout)",
        "execution": "next actual global-session open entry; h20/h60/h120 with path-dependent trailing stops",
        "costs": {"baseline": COST_BASELINE, "sensitivity": COST_SENSITIVITY},
        "input": {"universe_rows": int(len(universe)), "universe_signal_dates": int(universe["date"].nunique())},
        "summary": safe_records(summary),
        "outputs": {"summary_csv": str(summary_path), "markdown": str(md_path)},
        "operational_change": False,
        "promotion": "FORBIDDEN_EXPLORATORY_ONLY",
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    
    overall = summary.loc[summary["partition"] == "ALL_AVAILABLE"]
    lines = [
        "# S20 Trend Execution Diagnostic — Exploratory",
        "",
        f"- generated_at: {payload['generated_at']}",
        "- primary: daily equal-weight net-return basket with Trailing Stops",
        "- Exit A: -20% from peak. Exit B: -3x ATR from peak.",
        f"- Costs (2026-07-23 corrected): base={COST_BASELINE*100:.3f}% (fee+slippage+tax), sens={COST_SENSITIVITY*100:.3f}% (fee+tax floor, zero slippage)",
        "",
        "## Overall Results",
        "",
        "```csv",
        overall.to_csv(index=False),
        "```",
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "summary": str(summary_path), "universe_rows": int(len(universe))}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
