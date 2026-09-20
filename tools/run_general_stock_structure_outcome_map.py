"""Read-only broad-state map for ordinary-stock candidate quality."""
from __future__ import annotations

import hashlib
import importlib.util
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
REPORT_PATH = ROOT / "report_backtest_v41_1.py"
MEMBERSHIP_PATH = ROOT / "_cache" / "krx_population_static_membership_latest.parquet"
OUT_PREFIX = "general_stock_structure_outcome_map_20260720"
HORIZONS = (20, 60, 120)
MIN_PRICE = 1_000.0
MIN_DAILY_VALUE = 50_000_000.0
HIGH_LIQUID_VALUE = 2_000_000_000.0
MID_LIQUID_VALUE = 300_000_000.0
PARTITIONS = (
    ("ALL_AVAILABLE", "2020-01-02", "2025-12-31"),
    ("PERIOD_2020_2021", "2020-01-02", "2021-12-31"),
    ("PERIOD_2022_2023", "2022-01-01", "2023-12-31"),
    ("PERIOD_2024", "2024-01-01", "2024-12-31"),
    ("PERIOD_2025", "2025-01-01", "2025-12-31"),
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_report() -> Any:
    spec = importlib.util.spec_from_file_location("general_stock_structure_report", REPORT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {REPORT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def safe_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def build_forward_panel(raw: pd.DataFrame) -> pd.DataFrame:
    out = raw.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    out["code"] = out["code"].astype(str).str.zfill(6)
    out["_entity_key"] = out["price_history_key"].astype(str)
    out = out.sort_values(["_entity_key", "date"], kind="mergesort").reset_index(drop=True)
    calendar = pd.DatetimeIndex(sorted(out["date"].dropna().unique()))
    positions = {day: position for position, day in enumerate(calendar)}
    out["_global_index"] = out["date"].map(positions)
    out["entry_date"] = out["_global_index"].map(
        lambda position: calendar[int(position) + 1]
        if pd.notna(position) and int(position) + 1 < len(calendar)
        else pd.NaT
    )
    entry = out[["_entity_key", "date", "open"]].rename(columns={"date": "entry_date", "open": "entry_open"})
    out = out.merge(entry, on=["_entity_key", "entry_date"], how="left", validate="many_to_one")
    for horizon in HORIZONS:
        exit_date = f"exit_date_h{horizon}"
        exit_close = f"exit_close_h{horizon}"
        return_name = f"gross_forward_h{horizon}"
        out[exit_date] = out["_global_index"].map(
            lambda position: calendar[int(position) + horizon]
            if pd.notna(position) and int(position) + horizon < len(calendar)
            else pd.NaT
        )
        close_lookup = out[["_entity_key", "date", "close"]].rename(columns={"date": exit_date, "close": exit_close})
        out = out.merge(close_lookup, on=["_entity_key", exit_date], how="left", validate="many_to_one")
        out[return_name] = (out[exit_close] / out["entry_open"] - 1.0).where(out[exit_close].gt(0) & out["entry_open"].gt(0))
    return out


def attach_point_in_time_universe(panel: pd.DataFrame) -> pd.DataFrame:
    membership = pd.read_parquet(MEMBERSHIP_PATH, columns=["as_of_date", "code", "market", "security_type"])
    membership["date"] = pd.to_datetime(membership["as_of_date"].astype(str), format="%Y%m%d", errors="coerce")
    membership["code"] = membership["code"].astype(str).str.zfill(6)
    membership["market"] = membership["market"].fillna("").astype(str).str.upper().str.strip()
    membership = membership.loc[
        membership["market"].isin(["KOSPI", "KOSDAQ"])
        & membership["security_type"].fillna("").astype(str).str.upper().eq("COMMON"),
        ["date", "code", "market"],
    ].drop_duplicates(["date", "code"])
    if membership.duplicated(["date", "code"]).any():
        raise RuntimeError("duplicate point-in-time membership key")
    result = panel.merge(membership, on=["date", "code"], how="inner", validate="many_to_one", suffixes=("", "_membership"))
    if "market_membership" not in result.columns:
        raise RuntimeError("point-in-time membership market column missing")
    result["market"] = result["market_membership"].fillna("").astype(str).str.upper().str.strip()
    if not result["market"].isin(["KOSPI", "KOSDAQ"]).all():
        raise RuntimeError("invalid point-in-time market after membership join")
    base = (
        pd.to_numeric(result["close"], errors="coerce").ge(MIN_PRICE)
        & pd.to_numeric(result["value"], errors="coerce").ge(MIN_DAILY_VALUE)
    )
    return result.loc[base].copy()


def add_observable_states(universe: pd.DataFrame) -> pd.DataFrame:
    out = universe.sort_values(["_entity_key", "date"], kind="mergesort").copy()
    grouped = out.groupby("_entity_key", sort=False)
    out["ma60"] = grouped["close"].transform(lambda value: value.rolling(60, min_periods=60).mean())
    out["ma60_lag10"] = grouped["close"].transform(lambda value: value.rolling(60, min_periods=60).mean().shift(10))
    out["prior_avg_value20"] = grouped["value"].transform(lambda value: value.rolling(20, min_periods=20).mean().shift(1))
    above_ma60 = out["close"].gt(out["ma60"])
    out["market_breadth"] = above_ma60.groupby([out["date"], out["market"]], sort=False).transform("mean")
    out["market_breadth_state"] = "UNAVAILABLE"
    out.loc[out["market_breadth"].ge(0.60), "market_breadth_state"] = "BROAD_UP"
    out.loc[out["market_breadth"].lt(0.40), "market_breadth_state"] = "BROAD_WEAK"
    out.loc[out["market_breadth"].between(0.40, 0.60, inclusive="left"), "market_breadth_state"] = "MIXED"
    out["stock_trend_state"] = "TRANSITION"
    out.loc[out["close"].gt(out["ma60"]) & out["ma60"].gt(out["ma60_lag10"]), "stock_trend_state"] = "UPTREND"
    out.loc[out["close"].lt(out["ma60"]) & out["ma60"].le(out["ma60_lag10"]), "stock_trend_state"] = "WEAK_TREND"
    out.loc[out["ma60"].isna() | out["ma60_lag10"].isna(), "stock_trend_state"] = "UNAVAILABLE"
    out["liquidity_tier"] = "UNAVAILABLE"
    out.loc[out["prior_avg_value20"].ge(HIGH_LIQUID_VALUE), "liquidity_tier"] = "HIGH_LIQUID"
    out.loc[out["prior_avg_value20"].ge(MID_LIQUID_VALUE) & out["prior_avg_value20"].lt(HIGH_LIQUID_VALUE), "liquidity_tier"] = "MID_LIQUID"
    out.loc[out["prior_avg_value20"].ge(MIN_DAILY_VALUE) & out["prior_avg_value20"].lt(MID_LIQUID_VALUE), "liquidity_tier"] = "LOW_LIQUID"
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


def summarize(universe: pd.DataFrame) -> pd.DataFrame:
    output: list[dict[str, object]] = []
    axes = {
        "MARKET_BREADTH": ("market_breadth_state", ["BROAD_UP", "MIXED", "BROAD_WEAK"]),
        "STOCK_TREND": ("stock_trend_state", ["UPTREND", "TRANSITION", "WEAK_TREND"]),
        "STRUCTURAL_LIQUIDITY": ("liquidity_tier", ["HIGH_LIQUID", "MID_LIQUID", "LOW_LIQUID"]),
    }
    for partition, start, end in PARTITIONS:
        part = universe.loc[universe["date"].between(pd.Timestamp(start), pd.Timestamp(end))].copy()
        for horizon in HORIZONS:
            return_name = f"gross_forward_h{horizon}"
            valid = part.loc[part[return_name].notna()].copy()
            valid["forward_rank"] = valid.groupby(["date", "market"], sort=False)[return_name].rank(method="first", pct=True, ascending=True)
            valid["is_leader"] = valid["forward_rank"].ge(0.90)
            baseline = daily_aggregate(valid, return_name)
            output.append(summarize_condition(valid, return_name, partition, "BASELINE", "ALL_ELIGIBLE", baseline))
            for axis, (column, conditions) in axes.items():
                for condition in conditions:
                    output.append(summarize_condition(valid.loc[valid[column].eq(condition)], return_name, partition, axis, condition, baseline))
    return pd.DataFrame(output).sort_values(["partition", "horizon", "axis", "condition"], kind="mergesort").reset_index(drop=True)


def main() -> int:
    if not MEMBERSHIP_PATH.exists():
        raise RuntimeError(f"missing membership file: {MEMBERSHIP_PATH}")
    report = load_report()
    print("[1/5] loading PRICE_HISTORY_INTEGRITY_V1 data")
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})
    print("[2/5] building actual-session h20/h60/h120 outcomes")
    universe = build_forward_panel(raw)
    universe = attach_point_in_time_universe(universe)
    print("[3/5] computing signal-date broad states")
    universe = add_observable_states(universe)
    print("[4/5] mapping same-date future leader rates")
    summary = summarize(universe)
    print("[5/5] writing research-only outputs")
    summary_path = LOG_DIR / f"{OUT_PREFIX}_summary.csv"
    json_path = LOG_DIR / f"{OUT_PREFIX}.json"
    md_path = LOG_DIR / f"{OUT_PREFIX}.md"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "read_only_general_stock_structural_outcome_map",
        "exploratory_only": True,
        "price_history_contract": integrity,
        "states": {
            "market_breadth": "within-market share above MA60: BROAD_UP>=0.60; MIXED 0.40~0.60; BROAD_WEAK<0.40",
            "stock_trend": "UPTREND close>MA60 and rising MA60; WEAK_TREND close<MA60 and non-rising MA60; TRANSITION otherwise",
            "structural_liquidity": "prior avg value20: HIGH>=2bn; MID 300m~2bn; LOW 50m~300m",
        },
        "outcome": "next actual session open to h20/h60/h120 actual-session close gross return; same-date same-market top decile is leader",
        "partitions": [{"label": label, "start": start, "end": end} for label, start, end in PARTITIONS],
        "input": {
            "universe_rows": int(len(universe)),
            "universe_signal_dates": int(universe["date"].nunique()),
            "membership_sha256": sha256(MEMBERSHIP_PATH),
            "runner_sha256": sha256(Path(__file__)),
            "report_backtest_sha256": sha256(REPORT_PATH),
        },
        "summary": safe_records(summary),
        "outputs": {"summary_csv": str(summary_path), "markdown": str(md_path)},
        "operational_change": False,
        "promotion": "FORBIDDEN_EXPLORATORY_ONLY",
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    overall_h60 = summary.loc[(summary["partition"] == "ALL_AVAILABLE") & (summary["horizon"] == "h60")]
    lines = [
        "# General Stock Structural Outcome Map — Exploratory",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- universe_rows: {payload['input']['universe_rows']}",
        f"- universe_signal_dates: {payload['input']['universe_signal_dates']}",
        "- candidate-quality map only; no entry/exit strategy and no operating change",
        "",
        "## Overall h60 Map",
        "",
        "```csv",
        overall_h60.to_csv(index=False),
        "```",
        "",
        "## Interpretation Limit",
        "",
        "- Leader labels are future research outcomes. States use only signal-date or prior data.",
        "- This output does not choose a conjunction, entry rule, holding period, or trading policy.",
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "summary": str(summary_path), "json": str(json_path), "markdown": str(md_path), "universe_rows": int(len(universe))}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
