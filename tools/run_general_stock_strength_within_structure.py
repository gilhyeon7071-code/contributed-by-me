"""Read-only price-strength map inside the UPTREND × HIGH_LIQUID cohort."""
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
OUT_PREFIX = "general_stock_strength_within_structure_20260720"
STRENGTH_WINDOW = 20
DECILES = tuple(range(1, 11))


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_base() -> Any:
    spec = importlib.util.spec_from_file_location("strength_within_structure_base", BASE_RUNNER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {BASE_RUNNER_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def safe_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def add_strength_signal(universe: pd.DataFrame) -> pd.DataFrame:
    out = universe.sort_values(["_entity_key", "date"], kind="mergesort").copy()
    grouped = out.groupby("_entity_key", sort=False)
    out["strength_return20"] = grouped["close"].transform(lambda value: value / value.shift(STRENGTH_WINDOW) - 1.0)
    out["in_structural_cohort"] = out["stock_trend_state"].eq("UPTREND") & out["liquidity_tier"].eq("HIGH_LIQUID")
    eligible = out["in_structural_cohort"] & out["strength_return20"].notna()
    rank = out.loc[eligible].groupby(["date", "market"], sort=False)["strength_return20"].rank(method="first", pct=True)
    out["strength_decile"] = pd.Series(pd.NA, index=out.index, dtype="Int64")
    out.loc[eligible, "strength_decile"] = np.ceil(rank * 10).astype("int64")
    valid_deciles = out.loc[eligible, "strength_decile"].dropna()
    if valid_deciles.empty or not valid_deciles.between(1, 10).all():
        raise RuntimeError("invalid structural-cohort strength decile")
    return out


def daily_aggregate(frame: pd.DataFrame, return_name: str) -> pd.DataFrame:
    return frame.groupby("date", as_index=False).agg(
        leader_rate=("is_leader", "mean"),
        mean_return=(return_name, "mean"),
        row_count=(return_name, "size"),
    )


def summarize_condition(valid: pd.DataFrame, return_name: str, partition: str, decile: int | None) -> dict[str, object]:
    cohort = valid.loc[valid["in_structural_cohort"] & valid["strength_decile"].notna()].copy()
    condition = "COHORT_ALL" if decile is None else f"S{decile}"
    selected = cohort if decile is None else cohort.loc[cohort["strength_decile"].eq(decile)].copy()
    if selected.empty:
        return {
            "partition": partition, "horizon": return_name.replace("gross_forward_", ""), "condition": condition,
            "candidate_rows": 0, "signal_dates": 0, "avg_rows_per_signal_date": None, "leader_rate": None,
            "lift_vs_cohort": None, "mean_daily_gross_return_bps": None, "status": "DEFERRED_INSUFFICIENT_SAMPLE",
        }
    selected_daily = daily_aggregate(selected, return_name)
    cohort_daily = daily_aggregate(cohort, return_name).rename(columns={"leader_rate": "cohort_leader_rate"})
    joined = selected_daily.merge(cohort_daily[["date", "cohort_leader_rate"]], on="date", how="left", validate="one_to_one")
    leader_rate = float(joined["leader_rate"].mean())
    cohort_rate = float(joined["cohort_leader_rate"].mean())
    return {
        "partition": partition,
        "horizon": return_name.replace("gross_forward_", ""),
        "condition": condition,
        "candidate_rows": int(joined["row_count"].sum()),
        "signal_dates": int(len(joined)),
        "avg_rows_per_signal_date": round(float(joined["row_count"].mean()), 4),
        "leader_rate": round(leader_rate, 6),
        "lift_vs_cohort": round(leader_rate / cohort_rate, 4) if cohort_rate > 0 else None,
        "mean_daily_gross_return_bps": round(float(joined["mean_return"].mean() * 10_000), 4),
        "status": "DESCRIPTIVE" if len(joined) >= 30 else "DEFERRED_INSUFFICIENT_SAMPLE",
    }


def main() -> int:
    base = load_base()
    print("[1/5] loading integrity-filtered price history")
    report = base.load_report()
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})
    print("[2/5] building observable structural cohort and 20-session strength")
    universe = base.build_forward_panel(raw)
    universe = base.attach_point_in_time_universe(universe)
    universe = base.add_observable_states(universe)
    universe = add_strength_signal(universe)
    print("[3/5] ranking future leaders within same date and market")
    rows: list[dict[str, object]] = []
    for partition, start, end in base.PARTITIONS:
        part = universe.loc[universe["date"].between(pd.Timestamp(start), pd.Timestamp(end))].copy()
        for horizon in base.HORIZONS:
            return_name = f"gross_forward_h{horizon}"
            valid = part.loc[part[return_name].notna()].copy()
            valid["forward_rank"] = valid.groupby(["date", "market"], sort=False)[return_name].rank(method="first", pct=True, ascending=True)
            valid["is_leader"] = valid["forward_rank"].ge(0.90)
            rows.append(summarize_condition(valid, return_name, partition, None))
            for decile in DECILES:
                rows.append(summarize_condition(valid, return_name, partition, decile))
    print("[4/5] building fixed decile summary")
    summary = pd.DataFrame(rows).sort_values(["partition", "horizon", "condition"], kind="mergesort").reset_index(drop=True)
    print("[5/5] writing research-only evidence")
    summary_path = LOG_DIR / f"{OUT_PREFIX}_summary.csv"
    json_path = LOG_DIR / f"{OUT_PREFIX}.json"
    md_path = LOG_DIR / f"{OUT_PREFIX}.md"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "read_only_general_stock_strength_within_structural_cohort",
        "exploratory_only": True,
        "price_history_contract": integrity,
        "cohort": "signal-date UPTREND and HIGH_LIQUID",
        "signal": "20 valid-session close return, ranked within cohort by same date and market into S1 weakest through S10 strongest",
        "outcome": "next actual session open to h20/h60/h120 close gross return; leader is same-date/same-market full-universe future top decile",
        "comparison": "each strength decile compared to COHORT_ALL on the same decile dates; h60 primary, h20/h120 diagnostic",
        "input": {
            "universe_rows": int(len(universe)),
            "universe_signal_dates": int(universe["date"].nunique()),
            "strength_window_sessions": STRENGTH_WINDOW,
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
        "# General Stock Strength within Structural Cohort — Exploratory",
        "",
        f"- generated_at: {payload['generated_at']}",
        "- primary: h60 future leader rate by fixed 20-session price-strength decile",
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
        "- The ten pre-registered deciles are descriptive; this output does not choose a threshold or create a trading rule.",
        "- Price strength uses only signal-date and prior prices; leader labels are future research outcomes.",
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "summary": str(summary_path), "json": str(json_path), "markdown": str(md_path), "universe_rows": int(len(universe))}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
