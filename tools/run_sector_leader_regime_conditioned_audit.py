"""Descriptive audit: does the leading stock within a currently-STRONG sector show
better regime-conditioned forward returns than the leading stock of an arbitrary
sector (i.e., does sector-strength filtering add incremental value over plain
within-sector RS ranking)?

Pre-registered design (fixed before looking at any result, per this session's
2026-07-23 discussion):
  - Sector mapping: sector_ssot.csv (current snapshot only, no historical dates
    available in this project - applied retroactively to 2020-2026H1. This is a
    known limitation: sector reclassification/relisting drift is not modeled.
  - Sector strength: median ret_20 (already-computed factor, unmodified) across
    that sector's point-in-time constituents on a given date.
  - Strong sector: top 20th percentile of sectors that day by this median.
  - Sector leader: top 1-3 stocks by `rs` (already-computed factor, unmodified)
    WITHIN a sector.
  - Baseline A: sector leaders of ANY sector (no strength filter).
  - Candidate B (the hypothesis): sector leaders of STRONG sectors only.
  - Regime: the already-fixed 5-state classifier (_assign_report_research_regime,
    unmodified, truncate-replay validated 2026-07-20).
  - Primary horizon: h20 (sector rotation is assumed to be a slower phenomenon
    than single-stock swing entries). h5/h60 are secondary diagnostics.
  - Cost model: 2026-07-23 corrected (fee_pct=0.00004, slippage_pct=0.001 flat,
    sell_tax_pct=0.0015).
  - Cell minimum: 30 unique signal dates for full-period/regime cells.
  - Episode-level reproducibility (contiguous regime runs) is checked from the
    start this time (lesson from the same-day regime-conditioned NORMAL-candidate
    audit, where a full-period aggregate was dominated by a single episode).

Descriptive only. No operational change. Does not touch orders/fills/ledger/
Gate/LOCK/candidate generation/paper runtime.
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
SECTOR_PATH = ROOT / "_cache" / "sector_ssot.csv"
OUT_PREFIX = "sector_leader_regime_conditioned_audit_20260723"

FEE_PCT = 0.00004
SLIPPAGE_PCT = 0.001
SELL_TAX_PCT = 0.0015

HORIZONS = (5, 20, 60)
PRIMARY_HORIZON = 20
MIN_VALUE_KRW = 1_000_000_000.0
STRONG_SECTOR_PCTILE = 0.80  # top 20th percentile
SECTOR_LEADER_TOP_N = 3
MIN_UNIQUE_SIGNAL_DATES = 30
PARTITIONS = (
    ("TRAIN_2020_2023", "2020-01-02", "2023-12-29"),
    ("VALIDATION_2024", "2024-01-01", "2024-12-31"),
    ("ASSESSMENT_2025", "2025-01-01", "2025-12-31"),
    ("RECENT_2026_H1", "2026-01-01", "2026-06-30"),
)


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


def build_universe(panel: pd.DataFrame) -> pd.DataFrame:
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
    base_mask = merged["entry_open"].gt(0) & pd.to_numeric(merged["value"], errors="coerce").ge(MIN_VALUE_KRW)
    universe = merged.loc[base_mask].copy()

    sector = pd.read_csv(SECTOR_PATH, encoding="utf-8-sig", dtype={"code": str})
    sector["code"] = sector["code"].astype(str).str.zfill(6)
    sector = sector[["code", "krx_sector"]].drop_duplicates("code")
    universe = universe.merge(sector, on="code", how="inner")
    universe = universe.dropna(subset=["ret_20", "rs"])
    return universe


def add_sector_strength_and_leaders(universe: pd.DataFrame) -> pd.DataFrame:
    out = universe.copy()
    sector_daily_strength = out.groupby(["date", "krx_sector"])["ret_20"].median().rename("sector_median_ret20")
    out = out.join(sector_daily_strength, on=["date", "krx_sector"])
    out["sector_strength_pctile"] = out.groupby("date")["sector_median_ret20"].rank(pct=True, method="average")
    out["is_strong_sector_day"] = out["sector_strength_pctile"] >= STRONG_SECTOR_PCTILE

    out["sector_rs_rank"] = out.groupby(["date", "krx_sector"])["rs"].rank(ascending=False, method="first")
    out["is_sector_leader"] = out["sector_rs_rank"] <= SECTOR_LEADER_TOP_N

    out["baseline_a_any_sector_leader"] = out["is_sector_leader"]
    out["candidate_b_strong_sector_leader"] = out["is_sector_leader"] & out["is_strong_sector_day"]
    return out


def summarize_by_regime(frame: pd.DataFrame, flag_col: str, regime_col: str) -> list[dict[str, object]]:
    sel = frame.loc[frame[flag_col]]
    records: list[dict[str, object]] = []
    for regime_val, part in sel.groupby(regime_col, sort=False):
        for horizon in HORIZONS:
            net_column = f"net_return_h{horizon}"
            rows = part.loc[part[net_column].notna(), ["date", net_column]]
            if rows.empty:
                records.append({
                    "group": flag_col, "regime": regime_val, "horizon": f"h{horizon}", "is_primary": horizon == PRIMARY_HORIZON,
                    "candidate_rows": 0, "signal_dates": 0, "mean_daily_net_bps": None,
                    "positive_daily_net_rate": None, "status": "DEFERRED_INSUFFICIENT_SAMPLE",
                })
                continue
            daily = rows.groupby("date", as_index=False)[net_column].mean()
            n_dates = int(len(daily))
            records.append({
                "group": flag_col, "regime": regime_val, "horizon": f"h{horizon}", "is_primary": horizon == PRIMARY_HORIZON,
                "candidate_rows": int(len(rows)), "signal_dates": n_dates,
                "mean_daily_net_bps": round(float(daily[net_column].mean() * 10_000), 4),
                "positive_daily_net_rate": round(float(daily[net_column].gt(0).mean()), 6),
                "status": "DESCRIPTIVE" if n_dates >= MIN_UNIQUE_SIGNAL_DATES else "DEFERRED_INSUFFICIENT_SAMPLE",
            })
    return records


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


def episode_reproducibility(frame: pd.DataFrame, flag_col: str, regime_series: pd.Series, regime_label: str) -> list[dict[str, object]]:
    episodes = find_episodes(regime_series.sort_index(), regime_label)
    sel = frame.loc[frame[flag_col]]
    records: list[dict[str, object]] = []
    net_column = f"net_return_h{PRIMARY_HORIZON}"
    for i, (start, end) in enumerate(episodes):
        part = sel.loc[(sel["date"] >= start) & (sel["date"] <= end)]
        rows = part.dropna(subset=[net_column])
        n_dates = rows["date"].nunique()
        mean_bps = None
        if n_dates > 0:
            mean_bps = round(float(rows.groupby("date")[net_column].mean().mean() * 10_000), 4)
        records.append({
            "regime": regime_label, "episode_index": i + 1,
            "start": str(start.date()), "end": str(end.date()), "calendar_days": int((end - start).days) + 1,
            "signal_dates": int(n_dates), "mean_daily_net_h20_bps": mean_bps,
        })
    return records


def main() -> int:
    if not MEMBERSHIP_PATH.exists():
        raise RuntimeError(f"missing membership file: {MEMBERSHIP_PATH}")
    if not SECTOR_PATH.exists():
        raise RuntimeError(f"missing sector file: {SECTOR_PATH}")
    report = load_module("sector_leader_report_backtest", REPORT_PATH)
    print("[1/7] loading integrity-filtered price history")
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})
    print("[2/7] computing factors (ret_20, rs unmodified)")
    factors = report.compute_factors(raw)

    print("[3/7] assigning fixed (2026-07-20, unmodified) research regime over full universe")
    regime_series = report._assign_report_research_regime(factors)
    regime_df = regime_series.reset_index()
    regime_df.columns = ["date", "research_regime"]

    print("[4/7] building execution panel + universe + sector join")
    panel = build_execution_panel(factors)
    universe = build_universe(panel)
    universe = add_sector_strength_and_leaders(universe)
    universe = universe.merge(regime_df, on="date", how="left")
    universe["research_regime"] = universe["research_regime"].fillna("UNKNOWN")

    print("[5/7] summarizing baseline A (any-sector leader) vs candidate B (strong-sector leader) by regime")
    records_a = summarize_by_regime(universe, "baseline_a_any_sector_leader", "research_regime")
    records_b = summarize_by_regime(universe, "candidate_b_strong_sector_leader", "research_regime")

    print("[6/7] episode-level reproducibility for candidate B (strong-sector leader)")
    episode_records: list[dict[str, object]] = []
    for label in ["BULL", "SIDEWAYS", "STRESS", "BEAR"]:
        episode_records.extend(episode_reproducibility(universe, "candidate_b_strong_sector_leader", regime_series, label))

    print("[7/7] writing research-only evidence")
    a_df = pd.DataFrame(records_a)
    b_df = pd.DataFrame(records_b)
    ep_df = pd.DataFrame(episode_records)

    a_path = LOG_DIR / f"{OUT_PREFIX}_baseline_a.csv"
    b_path = LOG_DIR / f"{OUT_PREFIX}_candidate_b.csv"
    ep_path = LOG_DIR / f"{OUT_PREFIX}_episodes.csv"
    json_path = LOG_DIR / f"{OUT_PREFIX}.json"

    a_df.to_csv(a_path, index=False, encoding="utf-8-sig")
    b_df.to_csv(b_path, index=False, encoding="utf-8-sig")
    ep_df.to_csv(ep_path, index=False, encoding="utf-8-sig")

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "read_only_sector_leader_regime_conditioned_audit",
        "descriptive_only": True,
        "note": (
            "Sector mapping is a current-snapshot CSV applied retroactively (no historical "
            "sector-membership dates available) - a stated limitation, not a bug. "
            "Sector strength = median ret_20 within sector; strong sector = top 20th "
            "percentile that day. Sector leader = top-3 by rs within sector. Baseline A = "
            "any-sector leader; Candidate B = strong-sector leader only. Both use the "
            "already-fixed (2026-07-20) regime classifier, unmodified."
        ),
        "price_history_contract": integrity,
        "params": {
            "strong_sector_pctile": STRONG_SECTOR_PCTILE,
            "sector_leader_top_n": SECTOR_LEADER_TOP_N,
            "min_value_krw": MIN_VALUE_KRW,
            "primary_horizon": f"h{PRIMARY_HORIZON}",
            "min_unique_signal_dates": MIN_UNIQUE_SIGNAL_DATES,
        },
        "cost_model": {"fee_pct": FEE_PCT, "slippage_pct": SLIPPAGE_PCT, "sell_tax_pct": SELL_TAX_PCT},
        "input": {
            "universe_rows": int(len(universe)),
            "sector_count": int(universe["krx_sector"].nunique()),
            "membership_sha256": sha256(MEMBERSHIP_PATH),
            "sector_sha256": sha256(SECTOR_PATH),
            "runner_sha256": sha256(Path(__file__)),
            "report_backtest_sha256": sha256(REPORT_PATH),
        },
        "baseline_a_any_sector_leader": safe_records(a_df),
        "candidate_b_strong_sector_leader": safe_records(b_df),
        "candidate_b_episodes": safe_records(ep_df),
        "outputs": {"baseline_a_csv": str(a_path), "candidate_b_csv": str(b_path), "episodes_csv": str(ep_path)},
        "operational_change": False,
        "promotion": "FORBIDDEN_DESCRIPTIVE_ONLY",
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({
        "status": "OK", "universe_rows": int(len(universe)), "sector_count": int(universe["krx_sector"].nunique()),
        "baseline_a_csv": str(a_path), "candidate_b_csv": str(b_path), "episodes_csv": str(ep_path),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
