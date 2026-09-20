"""Read-only exploratory S20 Trend Following signal generator."""
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
OUT_PREFIX = "general_stock_s20_trend_exploration_20260720"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_base() -> Any:
    spec = importlib.util.spec_from_file_location("structure_outcome_map_base", BASE_RUNNER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {BASE_RUNNER_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def safe_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def add_trend_signals(universe: pd.DataFrame) -> pd.DataFrame:
    out = universe.sort_values(["_entity_key", "date"], kind="mergesort").copy()
    grouped = out.groupby("_entity_key", sort=False)
    
    # 1. Long-term MAs
    out["ma120"] = grouped["close"].transform(lambda value: value.rolling(120, min_periods=120).mean())
    out["ma200"] = grouped["close"].transform(lambda value: value.rolling(200, min_periods=200).mean())
    
    # 2. 52-week High (approx 250 trading days)
    out["high52w"] = grouped["high"].transform(lambda value: value.rolling(250, min_periods=120).max())
    
    # 3. ATR 20
    prev_close = grouped["close"].shift(1)
    tr1 = out["high"] - out["low"]
    tr2 = (out["high"] - prev_close).abs()
    tr3 = (out["low"] - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    out["tr"] = tr
    out["atr20"] = grouped["tr"].transform(lambda value: value.rolling(20, min_periods=20).mean())
    
    # 4. Entry Condition (Breakout)
    # Price is above MA120, MA120 is above MA200 (Uptrend Alignment)
    # Price is within 10% of 52-week high or breaking it
    # Volume is at least 2x of 20-day average
    uptrend_aligned = out["close"].gt(out["ma120"]) & out["ma120"].gt(out["ma200"])
    near_high = out["close"].ge(out["high52w"] * 0.90)
    volume_surge = out["value"].gt(out["prior_avg_value20"] * 2.0)
    
    out["is_s20_trend_candidate"] = uptrend_aligned & near_high & volume_surge & out["in_structural_cohort"]
    
    return out


def daily_aggregate(frame: pd.DataFrame, return_name: str) -> pd.DataFrame:
    work = frame.loc[frame[return_name].notna(), ["date", return_name, "is_leader"]].copy()
    return work.groupby("date", as_index=False).agg(
        avg_gross_return=(return_name, "mean"),
        leader_rate=("is_leader", "mean"),
        row_count=(return_name, "size"),
    )


def summarize_condition(frame: pd.DataFrame, return_name: str, partition: str, axis: str, condition: str, baseline: pd.DataFrame) -> dict[str, object]:
    daily = daily_aggregate(frame, return_name)
    if daily.empty:
        return {"partition": partition, "horizon": return_name.replace("gross_forward_", ""), "axis": axis, "condition": condition, "candidate_rows": 0, "signal_dates": 0, "avg_rows_per_signal_date": None, "leader_rate": None, "leader_lift_vs_baseline": None, "mean_daily_gross_return_bps": None, "positive_daily_gross_rate": None, "status": "DEFERRED_INSUFFICIENT_SAMPLE"}
    baseline_daily = baseline[["date", "leader_rate"]].rename(columns={"leader_rate": "baseline_leader_rate"})
    daily = daily.merge(baseline_daily, on="date", how="left", validate="one_to_one")
    leader_rate = float(daily["leader_rate"].mean())
    baseline_rate = float(daily["baseline_leader_rate"].mean())
    return {
        "partition": partition,
        "horizon": return_name.replace("gross_forward_", ""),
        "axis": axis,
        "condition": condition,
        "candidate_rows": int(daily["row_count"].sum()),
        "signal_dates": int(len(daily)),
        "avg_rows_per_signal_date": round(float(daily["row_count"].mean()), 4),
        "leader_rate": round(leader_rate, 6),
        "leader_lift_vs_baseline": round(leader_rate / baseline_rate, 4) if baseline_rate > 0 else None,
        "mean_daily_gross_return_bps": round(float(daily["avg_gross_return"].mean() * 10_000), 4),
        "positive_daily_gross_rate": round(float(daily["avg_gross_return"].gt(0).mean()), 6),
        "status": "DESCRIPTIVE" if len(daily) >= 30 else "DEFERRED_INSUFFICIENT_SAMPLE",
    }


def main() -> int:
    base = load_base()
    print("[1/5] loading integrity-filtered price history")
    report = base.load_report()
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})
    print("[2/5] building actual-session h20/h60/h120 outcomes")
    universe = base.build_forward_panel(raw)
    universe = base.attach_point_in_time_universe(universe)
    print("[3/5] computing signal-date broad states and S20 trend signals")
    universe = base.add_observable_states(universe)
    universe["in_structural_cohort"] = universe["stock_trend_state"].eq("UPTREND") & universe["liquidity_tier"].eq("HIGH_LIQUID")
    universe = add_trend_signals(universe)
    print("[4/5] mapping same-date future leader rates for S20")
    
    rows: list[dict[str, object]] = []
    for partition, start, end in base.PARTITIONS:
        part = universe.loc[universe["date"].between(pd.Timestamp(start), pd.Timestamp(end))].copy()
        for horizon in base.HORIZONS:
            return_name = f"gross_forward_h{horizon}"
            valid = part.loc[part[return_name].notna()].copy()
            valid["forward_rank"] = valid.groupby(["date", "market"], sort=False)[return_name].rank(method="first", pct=True, ascending=True)
            valid["is_leader"] = valid["forward_rank"].ge(0.90)
            baseline = daily_aggregate(valid.loc[valid["in_structural_cohort"]], return_name)
            
            # S20 Trend Condition
            s20_valid = valid.loc[valid["is_s20_trend_candidate"]]
            rows.append(summarize_condition(s20_valid, return_name, partition, "STRATEGY", "S20_TREND", baseline))
            
    print("[5/5] writing research-only outputs")
    summary = pd.DataFrame(rows).sort_values(["partition", "horizon", "condition"], kind="mergesort").reset_index(drop=True)
    summary_path = LOG_DIR / f"{OUT_PREFIX}_summary.csv"
    json_path = LOG_DIR / f"{OUT_PREFIX}.json"
    md_path = LOG_DIR / f"{OUT_PREFIX}.md"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "read_only_general_stock_s20_trend_exploration",
        "exploratory_only": True,
        "price_history_contract": integrity,
        "cohort": "S20 Trend Candidate (Breakout) in UPTREND x HIGH_LIQUID",
        "input": {
            "universe_rows": int(len(universe)),
            "universe_signal_dates": int(universe["date"].nunique()),
            "s20_candidates_count": int(universe["is_s20_trend_candidate"].sum()),
            "base_runner_sha256": sha256(BASE_RUNNER_PATH),
            "runner_sha256": sha256(Path(__file__)),
        },
        "summary": safe_records(summary),
        "outputs": {"summary_csv": str(summary_path), "markdown": str(md_path)},
        "operational_change": False,
        "promotion": "FORBIDDEN_EXPLORATORY_ONLY",
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    
    overall_h60 = summary.loc[(summary["partition"] == "ALL_AVAILABLE") & (summary["horizon"] == "h60")]
    lines = [
        "# General Stock S20 Trend Exploration",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- S20 candidates found: {payload['input']['s20_candidates_count']}",
        "- This is exploratory only to validate the S20 trend universe.",
        "",
        "## Overall h60 S20 Trend",
        "",
        "```csv",
        overall_h60.to_csv(index=False),
        "```",
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "summary": str(summary_path), "json": str(json_path), "markdown": str(md_path), "universe_rows": int(len(universe))}, ensure_ascii=False))
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
