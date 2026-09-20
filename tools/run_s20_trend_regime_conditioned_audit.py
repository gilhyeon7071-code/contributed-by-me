"""Descriptive audit: does the already-fixed 5-state research regime classifier
explain the S20 Trend (-20% peak stop, Exit A) execution diagnostic's per-period
variance (2024 uniformly negative, 2020-21/2022-23/2025 mostly positive at h60/h120,
see PLANS.md 2026-07-23 "S20 execution diagnostic — new cost model")?

This is the guard-prescribed next step (section 6.5: after a single strategy-grammar
axis - Exit A vs Exit B - shows an effect, test the incremental effect of combining
a second, already-fixed axis - regime - rather than inventing a new grammar).

Reuses, unmodified:
  - S20 Trend candidate definition + path-dependent Exit A trailing-stop simulation
    (tools/run_general_stock_s20_trend_execution_diagnostic.py, 2026-07-23 cost-
    corrected version)
  - The 5-state research regime classifier fixed 2026-07-20
    (report_backtest_v41_1.py:_assign_report_research_regime)
Exit B is out of scope here - already rejected (negative in every period/horizon
regardless of cost model, see PLANS.md 2026-07-23).

Primary horizon: h120 (showed the most cross-period directional consistency in the
prior diagnostic: 3 of 4 fixed partitions positive, vs h60's 2 of 4). h60/h20 are
secondary diagnostics. Episode-level reproducibility (contiguous regime runs) is
checked from the start, per the same-day lesson from the NORMAL-candidate and
sector-leader regime audits (full-period aggregates can be dominated by one episode).

Descriptive only. No operational change.
"""
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
DIAG_PATH = ROOT / "tools" / "run_general_stock_s20_trend_execution_diagnostic.py"
BASE_RUNNER_PATH = ROOT / "tools" / "run_general_stock_structure_outcome_map.py"
EXPLORER_PATH = ROOT / "tools" / "run_general_stock_s20_trend_exploration.py"
REPORT_PATH = ROOT / "report_backtest_v41_1.py"
OUT_PREFIX = "s20_trend_regime_conditioned_audit_20260723"

HORIZONS = (20, 60, 120)
PRIMARY_HORIZON = 120
MIN_UNIQUE_SIGNAL_DATES = 30


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


def find_episodes(series: pd.Series, label: str) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    is_target = series == label
    episodes: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    start = None
    prev_date = None
    for date, val in is_target.items():
        if val:
            if start is None:
                start = date
        else:
            if start is not None:
                episodes.append((start, prev_date))
                start = None
        prev_date = date
    if start is not None:
        episodes.append((start, prev_date))
    return episodes


def summarize_by_regime(frame: pd.DataFrame) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for regime_val, part in frame.groupby("research_regime", sort=False):
        for horizon in HORIZONS:
            net_column = f"net_return_A_base_h{horizon}"
            rows = part.loc[part[net_column].notna(), ["date", net_column]]
            if rows.empty:
                records.append({
                    "regime": regime_val, "horizon": f"h{horizon}", "is_primary": horizon == PRIMARY_HORIZON,
                    "candidate_rows": 0, "signal_dates": 0, "mean_daily_net_bps": None,
                    "positive_daily_net_rate": None, "status": "DEFERRED_INSUFFICIENT_SAMPLE",
                })
                continue
            daily = rows.groupby("date", as_index=False)[net_column].mean()
            n_dates = int(len(daily))
            records.append({
                "regime": regime_val, "horizon": f"h{horizon}", "is_primary": horizon == PRIMARY_HORIZON,
                "candidate_rows": int(len(rows)), "signal_dates": n_dates,
                "mean_daily_net_bps": round(float(daily[net_column].mean() * 10_000), 4),
                "positive_daily_net_rate": round(float(daily[net_column].gt(0).mean()), 6),
                "status": "DESCRIPTIVE" if n_dates >= MIN_UNIQUE_SIGNAL_DATES else "DEFERRED_INSUFFICIENT_SAMPLE",
            })
    return records


def regime_composition_by_partition(frame: pd.DataFrame, partitions: tuple) -> list[dict[str, object]]:
    """How much of each fixed partition's S20 signal dates fall in each regime -
    the direct test of "was 2024 just regime-composition-different"."""
    records: list[dict[str, object]] = []
    for label, start, end in partitions:
        part = frame.loc[frame["date"].between(pd.Timestamp(start), pd.Timestamp(end))]
        if part.empty:
            continue
        total_dates = part["date"].nunique()
        for regime_val, sub in part.groupby("research_regime", sort=False):
            n = sub["date"].nunique()
            records.append({
                "partition": label, "regime": regime_val,
                "signal_dates": int(n), "share_of_partition": round(float(n / total_dates), 4) if total_dates else None,
            })
    return records


def episode_reproducibility(frame: pd.DataFrame, regime_series: pd.Series, regime_label: str) -> list[dict[str, object]]:
    episodes = find_episodes(regime_series.sort_index(), regime_label)
    records: list[dict[str, object]] = []
    net_column = f"net_return_A_base_h{PRIMARY_HORIZON}"
    for i, (start, end) in enumerate(episodes):
        part = frame.loc[(frame["date"] >= start) & (frame["date"] <= end)]
        rows = part.dropna(subset=[net_column])
        n_dates = rows["date"].nunique()
        mean_bps = None
        if n_dates > 0:
            mean_bps = round(float(rows.groupby("date")[net_column].mean().mean() * 10_000), 4)
        if n_dates > 0:
            records.append({
                "regime": regime_label, "episode_index": i + 1,
                "start": str(start.date()), "end": str(end.date()), "calendar_days": int((end - start).days) + 1,
                "signal_dates": int(n_dates), "mean_daily_net_bps": mean_bps,
            })
    return records


def main() -> int:
    diag = load_module("diag", DIAG_PATH)
    base = load_module("base", BASE_RUNNER_PATH)
    explorer = load_module("explorer", EXPLORER_PATH)
    report = load_module("report", REPORT_PATH)

    print("[1/6] loading integrity-filtered price history")
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})

    print("[2/6] building S20 Trend candidates (unmodified)")
    universe = base.build_forward_panel(raw)
    universe = base.attach_point_in_time_universe(universe)
    universe = base.add_observable_states(universe)
    universe["in_structural_cohort"] = universe["stock_trend_state"].eq("UPTREND") & universe["liquidity_tier"].eq("HIGH_LIQUID")
    universe = explorer.add_trend_signals(universe)

    print("[3/6] path-dependent Exit A/B returns (2026-07-23 corrected cost model, unmodified)")
    universe = diag.add_path_dependent_returns(universe, base.HORIZONS)

    print("[4/6] assigning fixed (2026-07-20, unmodified) research regime")
    factors_for_regime = report.compute_factors(raw)
    regime_series = report._assign_report_research_regime(factors_for_regime)
    regime_df = regime_series.reset_index()
    regime_df.columns = ["date", "research_regime"]

    s20 = universe.loc[universe["is_s20_trend_candidate"]].copy()
    s20["date"] = pd.to_datetime(s20["date"])
    s20 = s20.merge(regime_df, on="date", how="left")
    s20["research_regime"] = s20["research_regime"].fillna("UNKNOWN")

    print("[5/6] summarizing by regime + regime composition per fixed partition + episode reproducibility")
    by_regime = summarize_by_regime(s20)
    partitions = (
        ("PERIOD_2020_2021", "2020-01-01", "2021-12-31"),
        ("PERIOD_2022_2023", "2022-01-01", "2023-12-31"),
        ("PERIOD_2024", "2024-01-01", "2024-12-31"),
        ("PERIOD_2025", "2025-01-01", "2025-12-31"),
    )
    composition = regime_composition_by_partition(s20, partitions)

    episode_records: list[dict[str, object]] = []
    for label in ["BULL", "SIDEWAYS", "STRESS", "BEAR", "TRANSITION"]:
        episode_records.extend(episode_reproducibility(s20, regime_series, label))

    print("[6/6] writing research-only evidence")
    regime_df_out = pd.DataFrame(by_regime)
    comp_df = pd.DataFrame(composition)
    ep_df = pd.DataFrame(episode_records)

    regime_path = LOG_DIR / f"{OUT_PREFIX}_by_regime.csv"
    comp_path = LOG_DIR / f"{OUT_PREFIX}_regime_composition.csv"
    ep_path = LOG_DIR / f"{OUT_PREFIX}_episodes.csv"
    json_path = LOG_DIR / f"{OUT_PREFIX}.json"

    regime_df_out.to_csv(regime_path, index=False, encoding="utf-8-sig")
    comp_df.to_csv(comp_path, index=False, encoding="utf-8-sig")
    ep_df.to_csv(ep_path, index=False, encoding="utf-8-sig")

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "read_only_s20_trend_regime_conditioned_audit",
        "descriptive_only": True,
        "note": (
            "Guard 6.5 second-axis test: S20 Trend Exit A (already fixed strategy grammar) "
            "combined with the already-fixed 2026-07-20 research regime classifier. "
            "Tests whether 2024's uniform negative result (PLANS.md 2026-07-23) is "
            "explained by regime composition rather than being an unexplained anomaly."
        ),
        "price_history_contract": integrity,
        "primary_horizon": f"h{PRIMARY_HORIZON}",
        "min_unique_signal_dates": MIN_UNIQUE_SIGNAL_DATES,
        "input": {
            "s20_candidate_rows": int(len(s20)),
            "s20_signal_dates": int(s20["date"].nunique()),
            "runner_sha256": sha256(Path(__file__)),
            "diag_sha256": sha256(DIAG_PATH),
            "report_backtest_sha256": sha256(REPORT_PATH),
        },
        "by_regime": safe_records(regime_df_out),
        "regime_composition_by_partition": safe_records(comp_df),
        "episodes": safe_records(ep_df),
        "outputs": {"by_regime_csv": str(regime_path), "regime_composition_csv": str(comp_path), "episodes_csv": str(ep_path)},
        "operational_change": False,
        "promotion": "FORBIDDEN_DESCRIPTIVE_ONLY",
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({
        "status": "OK", "s20_candidate_rows": int(len(s20)),
        "by_regime_csv": str(regime_path), "regime_composition_csv": str(comp_path), "episodes_csv": str(ep_path),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
