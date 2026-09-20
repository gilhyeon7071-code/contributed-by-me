"""Descriptive audit: does the already-fixed 5-state research regime classifier
(report_backtest_v41_1.py:_assign_report_research_regime, defined and truncate-replay
validated on 2026-07-20, unmodified here) explain performance differences in the
live NORMAL candidate population (generate_candidates_v41_1.py:_select_candidates()
DEFAULT_PARAMS, unmodified, same population as the 2026-07-23 GAPUP_BLOCK audit)?

This is the "2nd priority" item from the 2026-07-23 cost-model/existence-review
discussion: a clean regime-conditioning test that avoids the contamination found
in the 2026-07-21 "Structural Cohort x Market-Regime Gate" exploration (that
hypothesis was born from observing the S10 diagnostic's volatile results). Here:
  - the regime classifier's definition/thresholds were fixed on 2026-07-20, before
    this specific question ("does regime explain NORMAL-candidate performance?")
    was ever asked;
  - the candidate population (NORMAL, not S10) has never been regime-segmented
    before.
Still labeled DESCRIPTIVE, not confirmatory: cell minimum (30 unique signal dates)
enforced, no threshold tuned on these results, no operational change.

Cost model uses the 2026-07-23 correction (fee 0.005->0.00004, sell_tax
0.002->0.0015); slippage_pct left at the existing flat 0.001 (tiered_slippage
has no verified basis, per PLANS.md 2026-07-23, and is not applied in this
research panel).

No operational change: does not touch orders/fills/ledger/Gate/LOCK/candidate
generation/paper runtime.
"""
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
REPORT_PATH = ROOT / "report_backtest_v41_1.py"
MEMBERSHIP_PATH = ROOT / "_cache" / "krx_population_static_membership_latest.parquet"
OUT_PREFIX = "normal_candidates_regime_conditioned_audit_20260723"

SELECT_PARAMS = {
    "rs_lim": 1.70,
    "v_accel_lim": 2.50,
    "stretch_max": 1.19,
    "value_min": 1_000_000_000.0,
    "atr_max": 0.12,
    "rsi_max": 70.0,
    "vol_close_corr_min": 0.0,
    "near_52w_high_gap_max": 0.05,
    "min_listing_days": 126.0,
}

# 2026-07-23 corrected cost model (see PLANS.md same date). slippage_pct kept at
# the existing flat default - tiered_slippage has no verified basis and is not
# reproducible here without historical market-cap data.
FEE_PCT = 0.00004
SLIPPAGE_PCT = 0.001
SELL_TAX_PCT = 0.0015

HORIZONS = (1, 2, 5)
PRIMARY_HORIZON = 5  # matches paper_engine_config.json min_hold_days=2/max_hold_days=8 midpoint
PARTITIONS = (
    ("TRAIN_2020_2023", "2020-01-02", "2023-12-29"),
    ("VALIDATION_2024", "2024-01-01", "2024-12-31"),
    ("ASSESSMENT_2025", "2025-01-01", "2025-12-31"),
    ("RECENT_2026_H1", "2026-01-01", "2026-06-30"),
)
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


def build_execution_panel(factors: pd.DataFrame) -> pd.DataFrame:
    work = factors.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.normalize()
    work["code"] = work["code"].astype(str).str.zfill(6)
    entity_key = "price_history_key" if "price_history_key" in work.columns else "code"
    work["_entity_key"] = work[entity_key].astype(str)
    work = work.sort_values(["_entity_key", "date"], kind="mergesort").reset_index(drop=True)
    calendar = pd.DatetimeIndex(sorted(work["date"].dropna().unique()))
    calendar_pos = {day: index for index, day in enumerate(calendar)}
    work["_global_index"] = work["date"].map(calendar_pos)
    for horizon in HORIZONS:
        exit_date_column = f"exit_date_h{horizon}"
        exit_close_column = f"exit_close_h{horizon}"
        net_column = f"net_return_h{horizon}"
        work[exit_date_column] = work["_global_index"].map(
            lambda index: calendar[int(index) + horizon]
            if pd.notna(index) and int(index) + horizon < len(calendar)
            else pd.NaT
        )
        lookup = work[["_entity_key", "date", "close"]].rename(
            columns={"date": exit_date_column, "close": exit_close_column}
        )
        work = work.merge(lookup, on=["_entity_key", exit_date_column], how="left", validate="many_to_one")
    work["entry_date"] = work["_global_index"].map(
        lambda index: calendar[int(index) + 1]
        if pd.notna(index) and int(index) + 1 < len(calendar)
        else pd.NaT
    )
    entry_lookup = work[["_entity_key", "date", "open"]].rename(columns={"date": "entry_date", "open": "entry_open"})
    work = work.merge(entry_lookup, on=["_entity_key", "entry_date"], how="left", validate="many_to_one")
    for horizon in HORIZONS:
        exit_close_column = f"exit_close_h{horizon}"
        net_column = f"net_return_h{horizon}"
        valid = work["entry_open"].gt(0) & work[exit_close_column].gt(0)
        gross = (work[exit_close_column] / work["entry_open"]) - 1.0
        price_ratio = (work["entry_open"] + work[exit_close_column]) / work["entry_open"]
        cost_rate = price_ratio * (FEE_PCT + SLIPPAGE_PCT)
        work[net_column] = (gross - cost_rate - SELL_TAX_PCT).where(valid)
    return work


def build_research_universe(panel: pd.DataFrame) -> pd.DataFrame:
    membership = pd.read_parquet(MEMBERSHIP_PATH, columns=["as_of_date", "code", "market", "security_type"])
    membership["date"] = pd.to_datetime(membership["as_of_date"].astype(str), format="%Y%m%d", errors="coerce")
    membership["code"] = membership["code"].astype(str).str.zfill(6)
    membership["market"] = membership["market"].fillna("").astype(str).str.upper().str.strip()
    membership = membership.loc[
        membership["market"].isin(["KOSPI", "KOSDAQ"])
        & membership["security_type"].fillna("").astype(str).str.upper().eq("COMMON"),
        ["date", "code", "market"],
    ].drop_duplicates(["date", "code"])
    merged = panel.merge(membership, on=["date", "code"], how="inner", validate="many_to_one", suffixes=("", "_membership"))
    return merged.loc[merged["entry_open"].gt(0)].copy()


def select_candidates(universe: pd.DataFrame) -> pd.DataFrame:
    p = SELECT_PARAMS
    cond = (
        (universe["rs"] > p["rs_lim"])
        & (universe["v_accel"] > p["v_accel_lim"])
        & (universe["stretch"] < p["stretch_max"])
        & (universe["value"] > p["value_min"])
        & (universe["atr_pct"] < p["atr_max"])
        & (universe["rsi14"] < p["rsi_max"])
        & (universe["vol_close_corr20"] >= p["vol_close_corr_min"])
        & (universe["high_52w_gap"] <= p["near_52w_high_gap_max"])
        & (universe["listing_days"] >= p["min_listing_days"])
    )
    return universe.loc[cond].copy()


def summarize(candidates: pd.DataFrame, group_col: str, label_prefix: str) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for group_val, part in candidates.groupby(group_col, sort=False):
        for horizon in HORIZONS:
            net_column = f"net_return_h{horizon}"
            rows = part.loc[part[net_column].notna(), ["date", net_column]]
            if rows.empty:
                records.append({
                    label_prefix: group_val, "horizon": f"h{horizon}", "is_primary": horizon == PRIMARY_HORIZON,
                    "candidate_rows": 0, "signal_dates": 0,
                    "mean_daily_net_bps": None, "positive_daily_net_rate": None, "status": "DEFERRED_INSUFFICIENT_SAMPLE",
                })
                continue
            daily = rows.groupby("date", as_index=False)[net_column].mean()
            n_dates = int(len(daily))
            records.append({
                label_prefix: group_val, "horizon": f"h{horizon}", "is_primary": horizon == PRIMARY_HORIZON,
                "candidate_rows": int(len(rows)), "signal_dates": n_dates,
                "mean_daily_net_bps": round(float(daily[net_column].mean() * 10_000), 4),
                "positive_daily_net_rate": round(float(daily[net_column].gt(0).mean()), 6),
                "status": "DESCRIPTIVE" if n_dates >= MIN_UNIQUE_SIGNAL_DATES else "DEFERRED_INSUFFICIENT_SAMPLE",
            })
    return records


def main() -> int:
    if not MEMBERSHIP_PATH.exists():
        raise RuntimeError(f"missing membership file: {MEMBERSHIP_PATH}")
    report = load_module("regime_audit_report_backtest", REPORT_PATH)
    print("[1/6] loading integrity-filtered price history")
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})
    print("[2/6] computing factors identical to generate_candidates_v41_1.py")
    factors = report.compute_factors(raw)

    print("[3/6] assigning fixed (2026-07-20, unmodified) research regime over full universe")
    regime_series = report._assign_report_research_regime(factors)
    regime_df = regime_series.reset_index()
    regime_df.columns = ["date", "research_regime"]

    print("[4/6] building execution panel + NORMAL candidate selection")
    panel = build_execution_panel(factors)
    panel = build_research_universe(panel)
    candidates = select_candidates(panel)
    candidates = candidates.merge(regime_df, on="date", how="left")
    candidates["research_regime"] = candidates["research_regime"].fillna("UNKNOWN")

    print("[5/6] summarizing by regime (full period) and by fixed partition (robustness)")
    by_regime = summarize(candidates, "research_regime", "regime")

    partition_records: list[dict[str, object]] = []
    for label, start, end in PARTITIONS:
        start_ts, end_ts = pd.Timestamp(start), pd.Timestamp(end)
        part = candidates.loc[candidates["date"].between(start_ts, end_ts)].copy()
        if part.empty:
            continue
        part_summary = summarize(part, "research_regime", "regime")
        for row in part_summary:
            row["partition"] = label
        partition_records.extend(part_summary)

    print("[6/6] writing research-only evidence")
    overall_df = pd.DataFrame(by_regime)
    partition_df = pd.DataFrame(partition_records)
    overall_path = LOG_DIR / f"{OUT_PREFIX}_overall.csv"
    partition_path = LOG_DIR / f"{OUT_PREFIX}_by_partition.csv"
    candidates_path = LOG_DIR / f"{OUT_PREFIX}_candidates.csv"
    json_path = LOG_DIR / f"{OUT_PREFIX}.json"
    md_path = LOG_DIR / f"{OUT_PREFIX}.md"

    overall_df.to_csv(overall_path, index=False, encoding="utf-8-sig")
    partition_df.to_csv(partition_path, index=False, encoding="utf-8-sig")
    keep_cols = ["date", "code", "market", "research_regime", "rs", "v_accel", "stretch",
                 *[f"net_return_h{h}" for h in HORIZONS]]
    candidates.loc[:, keep_cols].sort_values(["date", "code"], kind="mergesort").to_csv(
        candidates_path, index=False, encoding="utf-8-sig"
    )

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "read_only_normal_candidates_regime_conditioned_audit",
        "descriptive_only": True,
        "round_type": "exploration_clean_2nd_attempt",
        "note": (
            "Regime classifier (definition fixed 2026-07-20, truncate-replay validated, "
            "unmodified here) applied for the first time to the live NORMAL candidate "
            "population (unmodified _select_candidates DEFAULT_PARAMS). No threshold "
            "tuned on these results. Corrected 2026-07-23 cost model applied "
            f"(fee_pct={FEE_PCT}, sell_tax_pct={SELL_TAX_PCT}, slippage_pct={SLIPPAGE_PCT} unchanged flat)."
        ),
        "price_history_contract": integrity,
        "select_params": SELECT_PARAMS,
        "primary_horizon": f"h{PRIMARY_HORIZON}",
        "min_unique_signal_dates": MIN_UNIQUE_SIGNAL_DATES,
        "cost_model": {"fee_pct": FEE_PCT, "slippage_pct": SLIPPAGE_PCT, "sell_tax_pct": SELL_TAX_PCT},
        "partitions": [{"label": label, "start": start, "end": end} for label, start, end in PARTITIONS],
        "input": {
            "universe_rows": int(len(panel)),
            "candidate_rows": int(len(candidates)),
            "candidate_signal_dates": int(candidates["date"].nunique()),
            "membership_sha256": sha256(MEMBERSHIP_PATH),
            "runner_sha256": sha256(Path(__file__)),
            "report_backtest_sha256": sha256(REPORT_PATH),
        },
        "overall_by_regime": safe_records(overall_df),
        "by_partition": safe_records(partition_df),
        "outputs": {
            "overall_csv": str(overall_path), "by_partition_csv": str(partition_path),
            "candidates_csv": str(candidates_path), "markdown": str(md_path),
        },
        "operational_change": False,
        "promotion": "FORBIDDEN_DESCRIPTIVE_ONLY",
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# NORMAL Candidates x Research Regime — Clean 2nd-Attempt Descriptive Audit",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- candidate_rows: {payload['input']['candidate_rows']}",
        f"- candidate_signal_dates: {payload['input']['candidate_signal_dates']}",
        f"- primary_horizon: h{PRIMARY_HORIZON} (matches paper_engine_config min/max_hold_days)",
        "- promotion: forbidden; operational_change: false",
        "",
        "## Overall by regime (full period)",
        "```csv",
        overall_df.to_csv(index=False),
        "```",
        "",
        "## By fixed partition (robustness)",
        "```csv",
        partition_df.to_csv(index=False),
        "```",
        "",
        "## Interpretation Limit",
        "- Descriptive only. Regime classifier and candidate selection rule both unmodified from prior fixed definitions.",
        "- Not a confirmation round; DEFERRED_INSUFFICIENT_SAMPLE cells below 30 unique signal dates are not interpretable.",
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(json.dumps({
        "status": "OK", "overall_csv": str(overall_path), "by_partition_csv": str(partition_path),
        "candidate_rows": int(len(candidates)),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
