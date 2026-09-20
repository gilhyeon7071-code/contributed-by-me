"""Read-only incremental candidate-quality test: UPTREND × liquidity tier."""
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
BASE_RUNNER_PATH = ROOT / "tools" / "run_general_stock_structure_outcome_map.py"
OUT_PREFIX = "general_stock_trend_liquidity_interaction_20260720"
CONDITIONS = (
    ("UPTREND", None),
    ("UPTREND_HIGH_LIQUID", "HIGH_LIQUID"),
    ("UPTREND_MID_LIQUID", "MID_LIQUID"),
    ("UPTREND_LOW_LIQUID", "LOW_LIQUID"),
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_base() -> Any:
    spec = importlib.util.spec_from_file_location("trend_liquidity_base", BASE_RUNNER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {BASE_RUNNER_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def safe_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def daily_aggregate(frame: pd.DataFrame, return_name: str) -> pd.DataFrame:
    return frame.groupby("date", as_index=False).agg(
        leader_rate=("is_leader", "mean"),
        mean_return=(return_name, "mean"),
        row_count=(return_name, "size"),
    )


def summarize_condition(valid: pd.DataFrame, return_name: str, partition: str, condition_name: str, tier: str | None) -> dict[str, object]:
    uptrend = valid.loc[valid["stock_trend_state"].eq("UPTREND")].copy()
    if tier is None:
        selected = uptrend
    else:
        selected = uptrend.loc[uptrend["liquidity_tier"].eq(tier)].copy()
    if selected.empty:
        return {"partition": partition, "horizon": return_name.replace("gross_forward_", ""), "condition": condition_name, "candidate_rows": 0, "signal_dates": 0, "avg_rows_per_signal_date": None, "leader_rate": None, "lift_vs_full_baseline": None, "incremental_lift_vs_uptrend": None, "mean_daily_gross_return_bps": None, "status": "DEFERRED_INSUFFICIENT_SAMPLE"}
    selected_daily = daily_aggregate(selected, return_name)
    dates = selected_daily[["date"]]
    full_daily = daily_aggregate(valid, return_name).rename(columns={"leader_rate": "full_leader_rate"})
    uptrend_daily = daily_aggregate(uptrend, return_name).rename(columns={"leader_rate": "uptrend_leader_rate"})
    joined = selected_daily.merge(full_daily[["date", "full_leader_rate"]], on="date", how="left", validate="one_to_one")
    joined = joined.merge(uptrend_daily[["date", "uptrend_leader_rate"]], on="date", how="left", validate="one_to_one")
    leader_rate = float(joined["leader_rate"].mean())
    full_rate = float(joined["full_leader_rate"].mean())
    uptrend_rate = float(joined["uptrend_leader_rate"].mean())
    return {
        "partition": partition,
        "horizon": return_name.replace("gross_forward_", ""),
        "condition": condition_name,
        "candidate_rows": int(joined["row_count"].sum()),
        "signal_dates": int(len(joined)),
        "avg_rows_per_signal_date": round(float(joined["row_count"].mean()), 4),
        "leader_rate": round(leader_rate, 6),
        "lift_vs_full_baseline": round(leader_rate / full_rate, 4) if full_rate > 0 else None,
        "incremental_lift_vs_uptrend": round(leader_rate / uptrend_rate, 4) if uptrend_rate > 0 else None,
        "mean_daily_gross_return_bps": round(float(joined["mean_return"].mean() * 10_000), 4),
        "status": "DESCRIPTIVE" if len(joined) >= 30 else "DEFERRED_INSUFFICIENT_SAMPLE",
    }


def main() -> int:
    base = load_base()
    print("[1/5] loading integrity-filtered price history")
    report = base.load_report()
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})
    print("[2/5] building shared actual-session outcomes and broad states")
    universe = base.build_forward_panel(raw)
    universe = base.attach_point_in_time_universe(universe)
    universe = base.add_observable_states(universe)
    print("[3/5] ranking future leaders within date and market")
    rows: list[dict[str, object]] = []
    for partition, start, end in base.PARTITIONS:
        part = universe.loc[universe["date"].between(pd.Timestamp(start), pd.Timestamp(end))].copy()
        for horizon in base.HORIZONS:
            return_name = f"gross_forward_h{horizon}"
            valid = part.loc[part[return_name].notna()].copy()
            valid["forward_rank"] = valid.groupby(["date", "market"], sort=False)[return_name].rank(method="first", pct=True, ascending=True)
            valid["is_leader"] = valid["forward_rank"].ge(0.90)
            for condition_name, tier in CONDITIONS:
                rows.append(summarize_condition(valid, return_name, partition, condition_name, tier))
    print("[4/5] building fixed comparison summary")
    summary = pd.DataFrame(rows).sort_values(["partition", "horizon", "condition"], kind="mergesort").reset_index(drop=True)
    print("[5/5] writing research-only evidence")
    summary_path = LOG_DIR / f"{OUT_PREFIX}_summary.csv"
    json_path = LOG_DIR / f"{OUT_PREFIX}.json"
    md_path = LOG_DIR / f"{OUT_PREFIX}.md"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "read_only_general_stock_trend_liquidity_incremental_test",
        "exploratory_only": True,
        "price_history_contract": integrity,
        "comparison": {
            "reference": "UPTREND",
            "interactions": [condition for condition, tier in CONDITIONS if tier is not None],
            "primary": "h60 same-date/same-market top-decile leader rate; interaction compared to UPTREND on same interaction dates",
        },
        "input": {
            "universe_rows": int(len(universe)),
            "universe_signal_dates": int(universe["date"].nunique()),
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
        "# General Stock UPTREND × Liquidity Incremental Test",
        "",
        f"- generated_at: {payload['generated_at']}",
        "- primary: same-date h60 future leader rate; interaction compared to UPTREND on the same dates",
        "- candidate-quality only; no entry/exit/cost/operating change",
        "",
        "## Overall h60",
        "",
        "```csv",
        overall_h60.to_csv(index=False),
        "```",
        "",
        "## Interpretation Limit",
        "",
        "- This compares a single pre-registered two-axis interaction against its UPTREND reference.",
        "- It does not select a condition, create a trade rule, or change operations.",
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "summary": str(summary_path), "json": str(json_path), "markdown": str(md_path), "universe_rows": int(len(universe))}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
