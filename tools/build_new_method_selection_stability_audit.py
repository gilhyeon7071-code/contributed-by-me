"""Read-only leave-one-period-out stability audit for representative hypotheses."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
INPUT_PERIOD = LOG_DIR / "new_method_historical_axis_matrix_summary_latest.csv"
INPUT_SELECTION = LOG_DIR / "new_method_representative_hypothesis_selection_latest.csv"
OUT_FOLDS = LOG_DIR / "new_method_representative_selection_stability_folds_latest.csv"
OUT_SUMMARY = LOG_DIR / "new_method_representative_selection_stability_summary_latest.csv"
OUT_JSON = LOG_DIR / "new_method_representative_selection_stability_latest.json"
OUT_MD = LOG_DIR / "new_method_representative_selection_stability_latest.md"
ALLOWED_BINS = {"Q1_LOWEST", "Q5_HIGHEST", "TRUE"}
HORIZON_DAYS = {"h1": 1, "h2": 2, "h5": 5}


def _safe_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def _representative_for_fold(frame: pd.DataFrame, regime: str) -> pd.Series | None:
    """Diagnostic only: two positive remaining periods are required after one holdout."""
    subset = frame[frame["research_regime"].astype(str).eq(str(regime))].copy()
    grouped = (
        subset.groupby(["axis_family", "axis_field", "axis_bin", "horizon"], as_index=False)
        .agg(
            adequate_periods=("period", "nunique"),
            positive_periods=("is_positive_excess", "sum"),
            mean_remaining_excess=("excess_avg_return", "mean"),
        )
    )
    eligible = grouped[
        grouped["axis_bin"].astype(str).isin(ALLOWED_BINS)
        & grouped["adequate_periods"].ge(2)
        & grouped["positive_periods"].eq(grouped["adequate_periods"])
    ].copy()
    if eligible.empty:
        return None
    eligible["horizon_days"] = eligible["horizon"].map(HORIZON_DAYS).fillna(0)
    return eligible.sort_values(
        ["mean_remaining_excess", "adequate_periods", "horizon_days", "axis_field"],
        ascending=[False, False, False, True],
    ).iloc[0]


def main() -> int:
    if not INPUT_PERIOD.exists() or not INPUT_SELECTION.exists():
        raise SystemExit("missing historical axis matrix or representative selection")
    period = pd.read_csv(INPUT_PERIOD, encoding="utf-8-sig")
    selected = pd.read_csv(INPUT_SELECTION, encoding="utf-8-sig")
    required_period = {
        "period", "research_regime", "axis_family", "axis_field", "axis_bin", "horizon",
        "evidence_state", "excess_avg_return",
    }
    required_selected = {"research_hypothesis_id", "research_regime", "axis_family", "axis_field", "axis_bin", "horizon"}
    missing = sorted(required_period.difference(period.columns)) + sorted(required_selected.difference(selected.columns))
    if missing:
        raise SystemExit(f"missing required fields: {missing}")
    period["excess_avg_return"] = pd.to_numeric(period["excess_avg_return"], errors="coerce")
    adequate = period[
        period["evidence_state"].astype(str).eq("ENOUGH_DESCRIPTIVE_SAMPLE")
        & period["excess_avg_return"].notna()
    ].copy()
    adequate["is_positive_excess"] = adequate["excess_avg_return"].gt(0)

    fold_rows: list[dict[str, object]] = []
    for original in selected.itertuples(index=False):
        key_mask = (
            adequate["research_regime"].astype(str).eq(str(original.research_regime))
            & adequate["axis_family"].astype(str).eq(str(original.axis_family))
            & adequate["axis_field"].astype(str).eq(str(original.axis_field))
            & adequate["axis_bin"].astype(str).eq(str(original.axis_bin))
            & adequate["horizon"].astype(str).eq(str(original.horizon))
        )
        original_periods = sorted(adequate.loc[key_mask, "period"].dropna().astype(str).unique())
        for held_out in original_periods:
            remaining = adequate[adequate["period"].astype(str).ne(held_out)].copy()
            original_remaining = remaining[
                (remaining["research_regime"].astype(str).eq(str(original.research_regime)))
                & (remaining["axis_family"].astype(str).eq(str(original.axis_family)))
                & (remaining["axis_field"].astype(str).eq(str(original.axis_field)))
                & (remaining["axis_bin"].astype(str).eq(str(original.axis_bin)))
                & (remaining["horizon"].astype(str).eq(str(original.horizon)))
            ]
            winner = _representative_for_fold(remaining, str(original.research_regime))
            remaining_periods = int(original_remaining["period"].nunique())
            positive_periods = int(original_remaining["is_positive_excess"].sum())
            original_remains_positive = remaining_periods >= 2 and positive_periods == remaining_periods
            same_winner = bool(
                winner is not None
                and str(winner["axis_family"]) == str(original.axis_family)
                and str(winner["axis_field"]) == str(original.axis_field)
                and str(winner["axis_bin"]) == str(original.axis_bin)
                and str(winner["horizon"]) == str(original.horizon)
            )
            fold_rows.append({
                "research_hypothesis_id": original.research_hypothesis_id,
                "research_regime": original.research_regime,
                "axis_family": original.axis_family,
                "axis_field": original.axis_field,
                "axis_bin": original.axis_bin,
                "horizon": original.horizon,
                "held_out_period": held_out,
                "original_remaining_periods": remaining_periods,
                "original_positive_remaining_periods": positive_periods,
                "original_remains_positive": original_remains_positive,
                "original_mean_remaining_excess": float(original_remaining["excess_avg_return"].mean()) if remaining_periods else np.nan,
                "fold_winner_axis_family": None if winner is None else winner["axis_family"],
                "fold_winner_axis_field": None if winner is None else winner["axis_field"],
                "fold_winner_axis_bin": None if winner is None else winner["axis_bin"],
                "fold_winner_horizon": None if winner is None else winner["horizon"],
                "fold_winner_mean_remaining_excess": None if winner is None else float(winner["mean_remaining_excess"]),
                "same_representative_winner": same_winner,
            })
    folds = pd.DataFrame(fold_rows)
    if folds.empty:
        raise SystemExit("no representative folds could be constructed")
    summary = (
        folds.groupby(["research_hypothesis_id", "research_regime", "axis_family", "axis_field", "axis_bin", "horizon"], as_index=False)
        .agg(
            holdout_folds=("held_out_period", "nunique"),
            all_folds_original_positive=("original_remains_positive", "all"),
            original_positive_fold_count=("original_remains_positive", "sum"),
            same_representative_winner_count=("same_representative_winner", "sum"),
            mean_original_remaining_excess=("original_mean_remaining_excess", "mean"),
        )
    )
    summary["winner_stability_state"] = np.where(
        summary["same_representative_winner_count"].eq(summary["holdout_folds"]),
        "STABLE_WITHIN_HISTORICAL_FOLDS",
        "ALTERNATIVE_WINNER_IN_SOME_FOLDS",
    )
    if folds.duplicated(["research_hypothesis_id", "held_out_period"]).any():
        raise SystemExit("duplicate leave-one-period-out folds")

    folds.to_csv(OUT_FOLDS, index=False, encoding="utf-8-sig")
    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_new_method_representative_selection_stability",
        "inputs": {"historical_period_matrix": str(INPUT_PERIOD), "representative_selection": str(INPUT_SELECTION)},
        "fold_contract": {
            "holdout": "each period where the original selected hypothesis has an adequate sample",
            "original_selection_contract_changed": False,
            "diagnostic_fold_rule": "at least two remaining adequate periods, all with positive excess return, then max mean remaining excess per regime",
        },
        "row_counts": {"selected_hypotheses": int(len(selected)), "fold_rows": int(len(folds)), "summary_rows": int(len(summary))},
        "summary": _safe_records(summary),
        "limitations": [
            "This tests historical selection stability only; it does not use July OOS results or approve a trading rule.",
            "The diagnostic two-period threshold is required because holding out one historical period reduces the original three-period cases to two periods.",
            "A stable fold winner can still fail later OOS validation.",
        ],
        "operational_change": False,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    lines = [
        "# New Method Representative Selection Stability Audit",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- selected_hypotheses: {payload['row_counts']['selected_hypotheses']}",
        f"- leave_one_period_out_folds: {payload['row_counts']['fold_rows']}",
        "- scope: read-only; original representative selection contract unchanged",
        "",
        "## Summary",
    ]
    for _, row in summary.iterrows():
        lines.append(
            f"- {row['research_hypothesis_id']}: folds={int(row['holdout_folds'])}, "
            f"original_positive={int(row['original_positive_fold_count'])}/{int(row['holdout_folds'])}, "
            f"same_winner={int(row['same_representative_winner_count'])}/{int(row['holdout_folds'])}, "
            f"state={row['winner_stability_state']}"
        )
    lines.extend(["", "## Caveats", *[f"- {item}" for item in payload["limitations"]]])
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "folds": str(OUT_FOLDS), "summary": str(OUT_SUMMARY), "json": str(OUT_JSON), "md": str(OUT_MD)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
