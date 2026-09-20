"""Read-only OOS comparison of the original STRESS axis and fold-derived alternatives."""

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
INPUT_SELECTION = LOG_DIR / "new_method_representative_hypothesis_selection_latest.csv"
INPUT_FOLDS = LOG_DIR / "new_method_representative_selection_stability_folds_latest.csv"
OOS_START = pd.Timestamp("2026-07-01")
HORIZON_MAP = {"h1": 1, "h2": 2, "h5": 5}

OUT_SELECTION = LOG_DIR / "new_method_stress_competing_axes_selection_latest.csv"
OUT_DETAIL = LOG_DIR / "new_method_stress_competing_axes_oos_candidates_latest.csv"
OUT_SUMMARY = LOG_DIR / "new_method_stress_competing_axes_oos_summary_latest.csv"
OUT_OVERLAP = LOG_DIR / "new_method_stress_competing_axes_oos_overlap_latest.csv"
OUT_JSON = LOG_DIR / "new_method_stress_competing_axes_oos_latest.json"
OUT_MD = LOG_DIR / "new_method_stress_competing_axes_oos_latest.md"

BANNED_EXISTING_LOGIC_FIELDS = (
    "entry_allowed_validation", "block_reasons", "blocked_by_risk", "buy_signal", "score",
    "final_score", "candidate_score", "promotion_blocker", "forward_return",
)


def _load_report_module() -> Any:
    spec = importlib.util.spec_from_file_location("report_for_stress_competing_axes_oos", REPORT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load report module: {REPORT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _exact_returns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.sort_values(["price_history_key", "price_session_index"], kind="mergesort").copy()
    group = out.groupby("price_history_key", sort=False)
    for label, days in HORIZON_MAP.items():
        future_close = group["close"].shift(-days)
        future_session = group["price_session_index"].shift(-days)
        exact = future_session.sub(out["price_session_index"]).eq(days)
        out[f"path_return_{label}"] = future_close.div(out["close"]).sub(1.0).where(exact)
        out[f"path_exit_date_{label}"] = group["date"].shift(-days).where(exact)
    return out


def _axis_bin(frame: pd.DataFrame, field: str) -> tuple[pd.Series, pd.Series]:
    if field == "macd_golden":
        return pd.Series(np.where(frame[field].fillna(False).astype(bool), "TRUE", "FALSE"), index=frame.index), pd.Series(np.nan, index=frame.index)
    values = pd.to_numeric(frame[field], errors="coerce")
    percentile = values.groupby(frame["date"], sort=False).rank(method="first", pct=True)
    bins = np.select(
        [percentile.le(0.20), percentile.le(0.40), percentile.le(0.60), percentile.le(0.80)],
        ["Q1_LOWEST", "Q2", "Q3", "Q4"],
        default="Q5_HIGHEST",
    )
    return pd.Series(bins, index=frame.index), percentile


def _oos_baselines(current: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, object]] = []
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


def _stress_selections(selection: pd.DataFrame, folds: pd.DataFrame) -> pd.DataFrame:
    original = selection[selection["research_regime"].astype(str).eq("STRESS")].copy()
    if len(original) != 1:
        raise SystemExit(f"expected one original STRESS representative, found {len(original)}")
    original = original[["axis_family", "axis_field", "axis_bin", "horizon"]].copy()
    original["selection_source"] = "ORIGINAL_REPRESENTATIVE"
    original["fold_winner_count"] = 0
    winners = folds[folds["research_regime"].astype(str).eq("STRESS")].copy()
    winner_columns = ["fold_winner_axis_family", "fold_winner_axis_field", "fold_winner_axis_bin", "fold_winner_horizon"]
    winners = winners.dropna(subset=winner_columns)
    alternatives = (
        winners.groupby(winner_columns, as_index=False)
        .agg(
            fold_winner_count=("held_out_period", "nunique"),
            mean_fold_winner_excess=("fold_winner_mean_remaining_excess", "mean"),
        )
        .rename(columns={
            "fold_winner_axis_family": "axis_family",
            "fold_winner_axis_field": "axis_field",
            "fold_winner_axis_bin": "axis_bin",
            "fold_winner_horizon": "horizon",
        })
    )
    alternatives["selection_source"] = "FOLD_WINNER_ALTERNATIVE"
    alternatives = alternatives[["axis_family", "axis_field", "axis_bin", "horizon", "selection_source", "fold_winner_count"]]
    combined = pd.concat([original, alternatives], ignore_index=True, sort=False)
    combined = combined.drop_duplicates(["axis_family", "axis_field", "axis_bin", "horizon"], keep="first")
    combined = combined.sort_values(["selection_source", "axis_field", "axis_bin", "horizon"]).reset_index(drop=True)
    combined.insert(0, "research_hypothesis_id", [f"STRESS_COMPETE_{idx + 1:02d}" for idx in range(len(combined))])
    combined.insert(1, "research_regime", "STRESS")
    combined["horizon_days"] = combined["horizon"].map(HORIZON_MAP)
    return combined


def _safe_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def main() -> int:
    if not INPUT_SELECTION.exists() or not INPUT_FOLDS.exists():
        raise SystemExit("missing representative selection or stability fold input")
    selection = pd.read_csv(INPUT_SELECTION, encoding="utf-8-sig")
    folds = pd.read_csv(INPUT_FOLDS, encoding="utf-8-sig")
    selected = _stress_selections(selection, folds)

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
        raise SystemExit("no OOS research universe rows")
    baselines = _oos_baselines(current)

    candidates: list[pd.DataFrame] = []
    for row in selected.itertuples(index=False):
        field = str(row.axis_field)
        if field not in current.columns:
            continue
        bins, percentiles = _axis_bin(current, field)
        picked = current[
            current["research_regime"].astype(str).eq("STRESS")
            & bins.astype(str).eq(str(row.axis_bin))
        ].copy()
        if picked.empty:
            continue
        horizon = str(row.horizon)
        picked["research_hypothesis_id"] = row.research_hypothesis_id
        picked["axis_family"] = row.axis_family
        picked["axis_field"] = field
        picked["axis_bin"] = row.axis_bin
        picked["horizon"] = horizon
        picked["horizon_days"] = int(row.horizon_days)
        picked["selection_source"] = row.selection_source
        picked["fold_winner_count"] = int(row.fold_winner_count)
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
        "date", "code", "market", "research_regime", "research_hypothesis_id", "selection_source", "fold_winner_count",
        "axis_family", "axis_field", "axis_bin", "horizon", "horizon_days", "close", "value", "rs", "rs_slope", "stretch",
        "v_accel", "atr_pct", "rsi14", "signal_percentile", "path_status", "path_exit_date", "path_forward_return",
        "oos_regime_baseline_closed_rows", "oos_regime_baseline_return", "oos_excess_return",
    ]
    for column in detail_cols:
        if column not in detail.columns:
            detail[column] = np.nan
    detail = detail[detail_cols].sort_values(["date", "research_hypothesis_id", "code"]).reset_index(drop=True)
    if detail.duplicated(["date", "code", "research_hypothesis_id"]).any():
        raise SystemExit("duplicate competing-axis candidate keys")

    summary_rows: list[dict[str, object]] = []
    for key, group in detail.groupby(["research_hypothesis_id", "selection_source", "axis_field", "axis_bin", "horizon"], dropna=False):
        closed = group[group["path_status"].eq("CLOSED_EXACT")].copy()
        returns = pd.to_numeric(closed["path_forward_return"], errors="coerce").dropna()
        excess = pd.to_numeric(closed["oos_excess_return"], errors="coerce").dropna()
        daily = closed.groupby("date", as_index=False)["oos_excess_return"].mean()
        summary_rows.append({
            "research_hypothesis_id": key[0], "selection_source": key[1], "axis_field": key[2], "axis_bin": key[3], "horizon": key[4],
            "candidate_rows": int(len(group)), "candidate_dates": int(group["date"].nunique()), "closed_rows": int(len(returns)),
            "closed_dates": int(daily["date"].nunique()), "pending_rows": int(len(group) - len(returns)),
            "avg_path_forward_return": float(returns.mean()) if len(returns) else None,
            "avg_oos_excess_return": float(excess.mean()) if len(excess) else None,
            "avg_daily_equal_weight_excess_return": float(daily["oos_excess_return"].mean()) if len(daily) else None,
            "positive_excess_days": int((daily["oos_excess_return"] > 0).sum()),
            "assessment": "INSUFFICIENT_DATES" if len(daily) < 5 else "DESCRIPTIVE_ONLY",
        })
    summary = pd.DataFrame(summary_rows).sort_values("research_hypothesis_id").reset_index(drop=True) if summary_rows else pd.DataFrame()

    overlap_rows: list[dict[str, object]] = []
    ids = selected["research_hypothesis_id"].tolist()
    for left_index, left_id in enumerate(ids):
        left = set(map(tuple, detail.loc[detail["research_hypothesis_id"].eq(left_id), ["date", "code"]].to_numpy()))
        for right_id in ids[left_index + 1:]:
            right = set(map(tuple, detail.loc[detail["research_hypothesis_id"].eq(right_id), ["date", "code"]].to_numpy()))
            union = left | right
            overlap_rows.append({
                "left_hypothesis_id": left_id,
                "right_hypothesis_id": right_id,
                "intersection_rows": int(len(left & right)),
                "union_rows": int(len(union)),
                "jaccard_overlap": float(len(left & right) / len(union)) if union else np.nan,
            })
    overlap = pd.DataFrame(overlap_rows)

    selected.to_csv(OUT_SELECTION, index=False, encoding="utf-8-sig")
    detail.to_csv(OUT_DETAIL, index=False, encoding="utf-8-sig")
    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    overlap.to_csv(OUT_OVERLAP, index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_new_method_stress_competing_single_axes_oos",
        "oos_window": {"start": OOS_START.strftime("%Y-%m-%d"), "data_max": data_max.strftime("%Y-%m-%d")},
        "inputs": {"original_selection": str(INPUT_SELECTION), "stability_folds": str(INPUT_FOLDS)},
        "selection_contract": "original STRESS representative plus each unique STRESS leave-one-period-out fold winner; no operating logic field is used",
        "price_history_contract": price_integrity,
        "excluded_existing_logic_fields": list(BANNED_EXISTING_LOGIC_FIELDS),
        "row_counts": {"selected_axes": int(len(selected)), "candidate_rows": int(len(detail)), "closed_rows": int((detail["path_status"] == "CLOSED_EXACT").sum()) if not detail.empty else 0},
        "summary": _safe_records(summary),
        "overlap": _safe_records(overlap),
        "limitations": [
            "Alternatives were discovered from historical fold winners, so this comparison remains exploratory and subject to selection multiplicity.",
            "All OOS returns are close-to-close paths with no transaction-cost, slippage, capacity, or overlap portfolio model.",
            "The result compares single axes only and neither forms a combined rule nor approves an operating signal.",
        ],
        "operational_change": False,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    lines = [
        "# New Method STRESS Competing Single-Axis OOS Comparison", "",
        f"- generated_at: {payload['generated_at']}",
        f"- oos_window: {payload['oos_window']['start']} to {payload['oos_window']['data_max']}",
        f"- selected_axes: {payload['row_counts']['selected_axes']}",
        f"- candidate_rows: {payload['row_counts']['candidate_rows']}",
        f"- closed_rows: {payload['row_counts']['closed_rows']}",
        "- scope: read-only; existing operating logic fields excluded", "", "## OOS summary",
    ]
    for _, row in summary.iterrows():
        lines.append(
            f"- {row['research_hypothesis_id']}: {row['selection_source']} / {row['axis_field']} / {row['axis_bin']} / {row['horizon']}, "
            f"closed_dates={int(row['closed_dates'])}, avg_daily_excess={'-' if pd.isna(row['avg_daily_equal_weight_excess_return']) else f'{float(row['avg_daily_equal_weight_excess_return']):.6f}'}, "
            f"assessment={row['assessment']}"
        )
    lines.extend(["", "## Caveats", *[f"- {item}" for item in payload["limitations"]]])
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "selection": str(OUT_SELECTION), "summary": str(OUT_SUMMARY), "overlap": str(OUT_OVERLAP), "json": str(OUT_JSON), "md": str(OUT_MD)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
