#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Reproduce one single-axis research grammar per regime in July OOS data.

The hypotheses are selected mechanically from the saved historical excess-return
matrix.  This is a separate research candidate layer: it never reads or writes
the operating candidate, buy, block, score, HPO, gate, paper, or order paths.
"""

from __future__ import annotations

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
INPUT_REPEAT = LOG_DIR / "new_method_historical_axis_matrix_repeat_latest.csv"

OOS_START = pd.Timestamp("2026-07-01")
ALLOWED_BINS = {"Q1_LOWEST", "Q5_HIGHEST", "TRUE"}
HORIZON_MAP = {"h1": 1, "h2": 2, "h5": 5}

OUT_SELECTION = LOG_DIR / "new_method_representative_hypothesis_selection_latest.csv"
OUT_CANDIDATES = LOG_DIR / "new_method_representative_oos_candidates_latest.csv"
OUT_SUMMARY = LOG_DIR / "new_method_representative_oos_candidate_summary_latest.csv"
OUT_JSON = LOG_DIR / "new_method_representative_oos_candidates_latest.json"
OUT_MD = LOG_DIR / "new_method_representative_oos_candidates_latest.md"

BANNED_EXISTING_LOGIC_FIELDS = (
    "entry_allowed_validation",
    "block_reasons",
    "blocked_by_risk",
    "buy_signal",
    "score",
    "final_score",
    "candidate_score",
    "promotion_blocker",
    "forward_return",
)


def _load_report_module() -> Any:
    spec = importlib.util.spec_from_file_location("report_for_representative_oos_candidates", REPORT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load report module: {REPORT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _exact_returns(factors: pd.DataFrame) -> pd.DataFrame:
    out = factors.sort_values(["price_history_key", "price_session_index"], kind="mergesort").copy()
    group = out.groupby("price_history_key", sort=False)
    for label, horizon in HORIZON_MAP.items():
        future_close = group["close"].shift(-horizon)
        future_session = group["price_session_index"].shift(-horizon)
        exact = future_session.sub(out["price_session_index"]).eq(horizon)
        out[f"path_return_{label}"] = future_close.div(out["close"]).sub(1.0).where(exact)
        out[f"path_exit_date_{label}"] = group["date"].shift(-horizon).where(exact)
    return out


def _select_hypotheses(repeat: pd.DataFrame) -> pd.DataFrame:
    required = {
        "research_regime", "axis_family", "axis_field", "axis_bin", "horizon",
        "enough_sample_periods", "mean_of_period_excess_returns", "repeat_state",
    }
    missing = sorted(required.difference(repeat.columns))
    if missing:
        raise ValueError(f"repeat matrix missing fields: {missing}")
    eligible = repeat[
        repeat["repeat_state"].astype(str).eq("REPEATED_POSITIVE_EXCESS_DESCRIPTIVE")
        & repeat["axis_bin"].astype(str).isin(ALLOWED_BINS)
        & pd.to_numeric(repeat["enough_sample_periods"], errors="coerce").ge(3)
    ].copy()
    eligible["mean_of_period_excess_returns"] = pd.to_numeric(eligible["mean_of_period_excess_returns"], errors="coerce")
    eligible["horizon_days"] = eligible["horizon"].map(HORIZON_MAP)
    eligible = eligible.dropna(subset=["mean_of_period_excess_returns", "horizon_days"])
    selected = (
        eligible.sort_values(
            ["research_regime", "mean_of_period_excess_returns", "enough_sample_periods", "horizon_days", "axis_field"],
            ascending=[True, False, False, False, True],
        )
        .groupby("research_regime", as_index=False, sort=True)
        .head(1)
        .reset_index(drop=True)
    )
    selected.insert(0, "research_hypothesis_id", [f"REP_{regime}_{idx + 1:02d}" for idx, regime in enumerate(selected["research_regime"].astype(str))])
    selected["selection_rule"] = "repeated_positive_excess + extreme_bin(Q1/Q5/TRUE) + max_mean_excess_per_regime"
    return selected


def _axis_bin(frame: pd.DataFrame, field: str) -> tuple[pd.Series, pd.Series]:
    if field == "macd_golden":
        return np.where(frame[field].fillna(False).astype(bool), "TRUE", "FALSE"), pd.Series(np.nan, index=frame.index)
    values = pd.to_numeric(frame[field], errors="coerce")
    percentile = values.groupby(frame["date"], sort=False).rank(method="first", pct=True)
    bins = np.select(
        [percentile.le(0.20), percentile.le(0.40), percentile.le(0.60), percentile.le(0.80)],
        ["Q1_LOWEST", "Q2", "Q3", "Q4"],
        default="Q5_HIGHEST",
    )
    return bins, percentile


def _safe_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return df.astype(object).where(pd.notna(df), None).to_dict(orient="records")


def _oos_baselines(current: pd.DataFrame) -> pd.DataFrame:
    """Same-date, same-research-regime OOS baselines from the full research universe."""
    records: list[dict[str, Any]] = []
    for horizon in HORIZON_MAP:
        column = f"path_return_{horizon}"
        for (date, regime), group in current.groupby(["date", "research_regime"], dropna=False):
            returns = pd.to_numeric(group[column], errors="coerce").dropna()
            records.append({
                "date": date,
                "research_regime": regime,
                "horizon": horizon,
                "oos_regime_baseline_closed_rows": int(len(returns)),
                "oos_regime_baseline_return": float(returns.mean()) if len(returns) else np.nan,
            })
    return pd.DataFrame(records)


def main() -> int:
    if not INPUT_REPEAT.exists():
        raise SystemExit(f"missing historical repeat matrix: {INPUT_REPEAT}")
    repeat = pd.read_csv(INPUT_REPEAT, encoding="utf-8-sig")
    selected = _select_hypotheses(repeat)
    if selected.empty:
        raise SystemExit("no representative hypotheses meet the saved historical selection contract")

    report = _load_report_module()
    raw = report.load_data()
    price_integrity = raw.attrs.get("price_history_integrity", {})
    factors = report.compute_factors(raw)
    factors["date"] = pd.to_datetime(factors["date"], errors="coerce").dt.normalize()
    factors["research_regime"] = factors["date"].map(report._assign_report_research_regime(factors))
    factors = _exact_returns(factors)
    data_max = pd.to_datetime(factors["date"], errors="coerce").max()
    current = factors[
        factors["date"].between(OOS_START, data_max)
        & factors["market"].astype(str).str.upper().isin(["KOSPI", "KOSDAQ"])
        & pd.to_numeric(factors["close"], errors="coerce").gt(0)
        & pd.to_numeric(factors["value"], errors="coerce").gt(0)
    ].copy()
    if current.empty:
        raise SystemExit("no OOS research universe rows after 2026-07-01")
    baselines = _oos_baselines(current)

    candidates: list[pd.DataFrame] = []
    for row in selected.itertuples(index=False):
        field = str(row.axis_field)
        if field not in current.columns:
            continue
        bins, percentiles = _axis_bin(current, field)
        picked = current[(current["research_regime"].astype(str) == str(row.research_regime)) & (pd.Series(bins, index=current.index).astype(str) == str(row.axis_bin))].copy()
        if picked.empty:
            continue
        horizon = str(row.horizon)
        picked["research_hypothesis_id"] = row.research_hypothesis_id
        picked["axis_family"] = row.axis_family
        picked["axis_field"] = field
        picked["axis_bin"] = row.axis_bin
        picked["horizon"] = horizon
        picked["horizon_days"] = int(HORIZON_MAP[horizon])
        picked["historical_mean_excess_return"] = float(row.mean_of_period_excess_returns)
        picked["historical_enough_sample_periods"] = int(row.enough_sample_periods)
        picked["signal_percentile"] = percentiles.loc[picked.index].to_numpy()
        picked["path_forward_return"] = pd.to_numeric(picked[f"path_return_{horizon}"], errors="coerce")
        picked["path_exit_date"] = pd.to_datetime(picked[f"path_exit_date_{horizon}"], errors="coerce").dt.strftime("%Y-%m-%d")
        picked["path_status"] = np.where(picked["path_forward_return"].notna(), "CLOSED_EXACT", "PENDING_OR_SEGMENT_BREAK")
        picked = picked.merge(
            baselines[baselines["horizon"].eq(horizon)],
            on=["date", "research_regime", "horizon"],
            how="left",
            validate="many_to_one",
        )
        picked["oos_excess_return"] = picked["path_forward_return"] - picked["oos_regime_baseline_return"]
        candidates.append(picked)

    detail = pd.concat(candidates, ignore_index=True) if candidates else pd.DataFrame()
    detail_cols = [
        "date", "code", "market", "research_regime", "research_hypothesis_id", "axis_family", "axis_field", "axis_bin", "horizon", "horizon_days",
        "close", "value", "rs", "rs_slope", "ret1_pct", "stretch", "v_accel", "atr_pct", "rsi14", "macd_golden", "high_52w_gap",
        "signal_percentile", "historical_mean_excess_return", "historical_enough_sample_periods", "path_status", "path_exit_date", "path_forward_return",
        "oos_regime_baseline_closed_rows", "oos_regime_baseline_return", "oos_excess_return",
    ]
    for col in detail_cols:
        if col not in detail.columns:
            detail[col] = np.nan
    detail = detail[detail_cols].sort_values(["date", "research_hypothesis_id", "code"]).reset_index(drop=True)
    if detail.duplicated(["date", "code", "research_hypothesis_id"]).any():
        raise SystemExit("duplicate OOS research candidate keys")

    summary_rows: list[dict[str, Any]] = []
    if not detail.empty:
        for key, group in detail.groupby(["research_hypothesis_id", "research_regime", "axis_field", "axis_bin", "horizon"], dropna=False):
            closed = group[group["path_status"].eq("CLOSED_EXACT")]
            ret = pd.to_numeric(closed["path_forward_return"], errors="coerce").dropna()
            excess = pd.to_numeric(closed["oos_excess_return"], errors="coerce").dropna()
            summary_rows.append({
                "research_hypothesis_id": key[0], "research_regime": key[1], "axis_field": key[2], "axis_bin": key[3], "horizon": key[4],
                "candidate_rows": int(len(group)), "unique_dates": int(group["date"].nunique()), "unique_codes": int(group["code"].nunique()),
                "closed_rows": int(len(ret)), "pending_rows": int(len(group) - len(ret)),
                "win_rate": float((ret > 0).mean()) if len(ret) else None,
                "avg_path_forward_return": float(ret.mean()) if len(ret) else None,
                "median_path_forward_return": float(ret.median()) if len(ret) else None,
                "positive_excess_rows": int((excess > 0).sum()),
                "avg_oos_excess_return": float(excess.mean()) if len(excess) else None,
                "median_oos_excess_return": float(excess.median()) if len(excess) else None,
            })
    summary = pd.DataFrame(summary_rows)
    if not summary.empty:
        summary = summary.sort_values("research_hypothesis_id").reset_index(drop=True)

    selected.to_csv(OUT_SELECTION, index=False, encoding="utf-8-sig")
    detail.to_csv(OUT_CANDIDATES, index=False, encoding="utf-8-sig")
    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_new_method_representative_oos_candidates",
        "selection_input": str(INPUT_REPEAT),
        "selection_rule": "repeated_positive_excess + extreme_bin(Q1/Q5/TRUE) + max_mean_excess_per_regime",
        "oos_window": {"start": OOS_START.strftime("%Y-%m-%d"), "data_max": data_max.strftime("%Y-%m-%d")},
        "price_history_contract": price_integrity,
        "excluded_existing_logic_fields": list(BANNED_EXISTING_LOGIC_FIELDS),
        "oos_baseline": "same signal date, same research regime, same horizon across the full OOS research universe",
        "selected_hypotheses": _safe_records(selected),
        "row_counts": {
            "oos_research_universe_rows": int(len(current)),
            "oos_research_dates": int(current["date"].nunique()),
            "selected_hypotheses": int(len(selected)),
            "candidate_rows": int(len(detail)),
            "candidate_unique_dates": int(detail["date"].nunique()) if not detail.empty else 0,
            "closed_rows": int((detail["path_status"] == "CLOSED_EXACT").sum()) if not detail.empty else 0,
            "pending_rows": int((detail["path_status"] != "CLOSED_EXACT").sum()) if not detail.empty else 0,
        },
        "limitations": [
            "Representative selection is exploratory and derives from the historical matrix; it is not an operating approval rule.",
            "Each hypothesis uses one signal axis only. No signal intersection or candidate score is applied.",
            "The July window is independent from the 2025-06 through 2026-06 selection window, but may be too short for a performance conclusion.",
            "OOS interpretation uses same-date, same-research-regime excess return; raw return is descriptive only.",
            "No intraday fill, transaction cost, slippage, or concentration model is included.",
        ],
        "operational_change": False,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    lines = [
        "# New Method Representative OOS Candidates",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- oos_window: {payload['oos_window']['start']} to {payload['oos_window']['data_max']}",
        f"- selected_hypotheses: {payload['row_counts']['selected_hypotheses']}",
        f"- candidate_rows: {payload['row_counts']['candidate_rows']}",
        f"- closed_rows: {payload['row_counts']['closed_rows']}",
        f"- pending_rows: {payload['row_counts']['pending_rows']}",
        "- operating logic fields: excluded",
        "",
        "## Selected hypotheses",
    ]
    for _, row in selected.iterrows():
        lines.append(
            f"- {row['research_hypothesis_id']}: {row['research_regime']} / {row['axis_field']} / {row['axis_bin']} / {row['horizon']} "
            f"(historical_excess={float(row['mean_of_period_excess_returns']):.6f}, periods={int(row['enough_sample_periods'])})"
        )
    lines.extend(["", "## OOS candidate summary"])
    if summary.empty:
        lines.append("- no current OOS candidates for the selected research regimes")
    else:
        for _, row in summary.iterrows():
            avg = row["avg_path_forward_return"]
            avg_excess = row["avg_oos_excess_return"]
            lines.append(
                f"- {row['research_hypothesis_id']}: candidates={int(row['candidate_rows'])}, dates={int(row['unique_dates'])}, "
                f"closed={int(row['closed_rows'])}, pending={int(row['pending_rows'])}, "
                f"avg_return={'-' if pd.isna(avg) else f'{float(avg):.6f}'}, "
                f"avg_excess={'-' if pd.isna(avg_excess) else f'{float(avg_excess):.6f}'}"
            )
    lines.extend(["", "## Caveats", *[f"- {item}" for item in payload["limitations"]]])
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "selection": str(OUT_SELECTION), "candidates": str(OUT_CANDIDATES), "summary": str(OUT_SUMMARY), "json": str(OUT_JSON), "md": str(OUT_MD)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
