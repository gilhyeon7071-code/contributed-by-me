"""Read-only historical matrix for strategy, regime, and atomic-signal effects."""

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
START = pd.Timestamp("2025-06-01")
END = pd.Timestamp("2026-06-30")
HORIZONS = {"h1": 1, "h2": 2, "h5": 5}
PERIODS = {
    "P1_202506_202509": (pd.Timestamp("2025-06-01"), pd.Timestamp("2025-09-30")),
    "P2_202510_202512": (pd.Timestamp("2025-10-01"), pd.Timestamp("2025-12-31")),
    "P3_202601_202603": (pd.Timestamp("2026-01-01"), pd.Timestamp("2026-03-31")),
    "P4_202604_202606": (pd.Timestamp("2026-04-01"), pd.Timestamp("2026-06-30")),
}
OUT_DEFINITIONS = LOG_DIR / "new_method_strategy_axis_definitions_latest.csv"
OUT_PERIOD = LOG_DIR / "new_method_strategy_axis_matrix_period_latest.csv"
OUT_REPEAT = LOG_DIR / "new_method_strategy_axis_matrix_repeat_latest.csv"
OUT_JSON = LOG_DIR / "new_method_strategy_axis_matrix_latest.json"
OUT_MD = LOG_DIR / "new_method_strategy_axis_matrix_latest.md"


def _load_report_module() -> Any:
    spec = importlib.util.spec_from_file_location("report_for_strategy_axis_matrix", REPORT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load report module: {REPORT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _exact_returns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.sort_values(["price_history_key", "price_session_index"], kind="mergesort").copy()
    grouped = out.groupby("price_history_key", sort=False)
    for label, days in HORIZONS.items():
        future_close = grouped["close"].shift(-days)
        future_session = grouped["price_session_index"].shift(-days)
        exact = future_session.sub(out["price_session_index"]).eq(days)
        out[f"path_return_{label}"] = future_close.div(out["close"]).sub(1.0).where(exact)
    return out


def _strategy_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.sort_values(["price_history_key", "price_session_index"], kind="mergesort").copy()
    grouped = out.groupby("price_history_key", sort=False)
    out["strategy_ma20"] = grouped["close"].transform(lambda x: x.rolling(20, min_periods=20).mean())
    out["strategy_ma60"] = grouped["close"].transform(lambda x: x.rolling(60, min_periods=60).mean())
    std20 = grouped["close"].transform(lambda x: x.rolling(20, min_periods=20).std(ddof=0))
    out["strategy_z20"] = (out["close"] - out["strategy_ma20"]) / (std20 + 1e-9)
    previous_ma20 = grouped["strategy_ma20"].shift(1)
    previous_ma60 = grouped["strategy_ma60"].shift(1)
    previous_high252 = grouped["close"].transform(lambda x: x.rolling(252, min_periods=60).max().shift(1))
    out["MR_BOLLINGER"] = out["strategy_z20"].le(-1.5) & out["rsi14"].le(30.0)
    out["MA_CROSS_UP"] = out["strategy_ma20"].gt(out["strategy_ma60"]) & previous_ma20.le(previous_ma60)
    out["BREAKOUT_252D"] = out["close"].gt(previous_high252)
    return out


def _atomic_signals(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for field, name, ascending in (
        ("stretch", "STRETCH_Q1", True),
        ("rs", "RS_Q5", False),
        ("rs_slope", "RS_SLOPE_Q5", False),
        ("v_accel", "V_ACCEL_Q5", False),
    ):
        rank = pd.to_numeric(out[field], errors="coerce").groupby(out["date"], sort=False).rank(method="first", pct=True, ascending=ascending)
        out[name] = rank.le(0.20)
    return out


def _add_periods(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["period"] = pd.NA
    for label, (start, end) in PERIODS.items():
        out.loc[out["date"].between(start, end), "period"] = label
    return out


def _summarize_candidate(
    frame: pd.DataFrame,
    candidate_mask: pd.Series,
    baseline_mask: pd.Series,
    baseline_keys: list[str],
    group_keys: list[str],
    scope: str,
    horizon: str,
) -> pd.DataFrame:
    ret_col = f"path_return_{horizon}"
    required = list(dict.fromkeys(["date", "code", "period", *baseline_keys, *group_keys, ret_col]))
    candidates = frame.loc[candidate_mask & frame[ret_col].notna(), required].copy()
    baseline = frame.loc[baseline_mask & frame[ret_col].notna(), ["date", *baseline_keys, ret_col]].copy()
    if candidates.empty or baseline.empty:
        return pd.DataFrame()
    baseline_group = ["date", *baseline_keys]
    baseline = baseline.groupby(baseline_group, as_index=False)[ret_col].agg(["mean", "size"]).reset_index()
    baseline = baseline.rename(columns={"mean": "baseline_return", "size": "baseline_closed_rows"})
    merged = candidates.merge(baseline, on=baseline_group, how="left", validate="many_to_one")
    merged["excess_return"] = merged[ret_col] - merged["baseline_return"]
    if merged["baseline_return"].isna().any():
        raise RuntimeError(f"missing baseline in {scope}/{horizon}")
    summary_keys = ["period", *group_keys]
    grouped = merged.groupby(summary_keys, as_index=False)
    result = grouped.agg(
        candidate_rows=(ret_col, "size"),
        unique_dates=("date", "nunique"),
        unique_codes=("code", "nunique"),
        avg_return=(ret_col, "mean"),
        median_return=(ret_col, "median"),
        avg_excess_return=("excess_return", "mean"),
        median_excess_return=("excess_return", "median"),
        baseline_closed_rows=("baseline_closed_rows", "mean"),
    )
    daily = merged.groupby([*summary_keys, "date"], as_index=False)["excess_return"].mean()
    daily = daily.groupby(summary_keys, as_index=False)["excess_return"].agg(["mean", "median", lambda x: int((x > 0).sum())]).reset_index()
    daily = daily.rename(columns={"mean": "avg_daily_equal_weight_excess", "median": "median_daily_equal_weight_excess", "<lambda_0>": "positive_excess_dates"})
    result = result.merge(daily, on=summary_keys, how="left", validate="one_to_one")
    result.insert(1, "scope", scope)
    result.insert(2, "horizon", horizon)
    result["baseline_type"] = "FULL_UNIVERSE_SAME_DATE" if not baseline_keys else "SAME_DATE_CONDITIONAL_BASELINE"
    return result


def _safe_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def main() -> int:
    report = _load_report_module()
    raw = report.load_data()
    price_integrity = raw.attrs.get("price_history_integrity", {})
    factors = report.compute_factors(raw)
    factors["date"] = pd.to_datetime(factors["date"], errors="coerce").dt.normalize()
    factors["research_regime"] = factors["date"].map(report._assign_report_research_regime(factors))
    factors = _strategy_features(factors)
    factors = _atomic_signals(factors)
    factors = _exact_returns(factors)
    factors = _add_periods(factors)
    panel = factors[
        factors["date"].between(START, END)
        & factors["period"].notna()
        & factors["market"].astype(str).str.upper().isin(["KOSPI", "KOSDAQ"])
        & pd.to_numeric(factors["close"], errors="coerce").gt(0)
        & pd.to_numeric(factors["value"], errors="coerce").gt(0)
    ].copy()
    if panel.empty:
        raise SystemExit("empty strategy research panel")
    strategies = ["MR_BOLLINGER", "MA_CROSS_UP", "BREAKOUT_252D"]
    signals = ["STRETCH_Q1", "RS_Q5", "RS_SLOPE_Q5", "V_ACCEL_Q5"]
    for column in [*strategies, *signals]:
        panel[column] = panel[column].fillna(False).astype(bool)

    result_frames: list[pd.DataFrame] = []
    full_mask = pd.Series(True, index=panel.index)
    for horizon in HORIZONS:
        result_frames.append(_summarize_candidate(panel, full_mask, full_mask, [], ["research_regime"], "REGIME_ONLY", horizon))
        for strategy in strategies:
            st_mask = panel[strategy]
            result_frames.append(_summarize_candidate(panel.assign(strategy_family=strategy), st_mask, full_mask, [], ["strategy_family"], "STRATEGY_ONLY", horizon))
            result_frames.append(_summarize_candidate(panel.assign(strategy_family=strategy), st_mask, full_mask, ["research_regime"], ["research_regime", "strategy_family"], "REGIME_STRATEGY", horizon))
            for signal in signals:
                both_mask = st_mask & panel[signal]
                result_frames.append(_summarize_candidate(panel.assign(strategy_family=strategy, signal_name=signal), both_mask, st_mask, [], ["strategy_family", "signal_name"], "STRATEGY_SIGNAL", horizon))
                result_frames.append(_summarize_candidate(panel.assign(strategy_family=strategy, signal_name=signal), both_mask, st_mask, ["research_regime"], ["research_regime", "strategy_family", "signal_name"], "REGIME_STRATEGY_SIGNAL", horizon))
        for signal in signals:
            sig_mask = panel[signal]
            result_frames.append(_summarize_candidate(panel.assign(signal_name=signal), sig_mask, full_mask, [], ["signal_name"], "SIGNAL_ONLY", horizon))
            result_frames.append(_summarize_candidate(panel.assign(signal_name=signal), sig_mask, full_mask, ["research_regime"], ["research_regime", "signal_name"], "REGIME_SIGNAL", horizon))
    period_summary = pd.concat([x for x in result_frames if not x.empty], ignore_index=True, sort=False)
    id_cols = ["scope", "horizon", "research_regime", "strategy_family", "signal_name", "baseline_type"]
    for column in id_cols:
        if column not in period_summary.columns:
            period_summary[column] = pd.NA
    period_summary = period_summary[["period", *id_cols, "candidate_rows", "unique_dates", "unique_codes", "avg_return", "median_return", "avg_excess_return", "median_excess_return", "avg_daily_equal_weight_excess", "median_daily_equal_weight_excess", "positive_excess_dates", "baseline_closed_rows"]]
    period_summary = period_summary.sort_values(["scope", "horizon", "research_regime", "strategy_family", "signal_name", "period"], na_position="first").reset_index(drop=True)
    if period_summary.duplicated(["period", *id_cols]).any():
        raise SystemExit("duplicate strategy-axis period keys")

    repeat_rows: list[dict[str, object]] = []
    for key, group in period_summary.groupby(id_cols, dropna=False):
        train = group[group["period"].isin(["P1_202506_202509", "P2_202510_202512", "P3_202601_202603"])]
        holdout = group[group["period"].eq("P4_202604_202606")]
        train_excess = pd.to_numeric(train["avg_daily_equal_weight_excess"], errors="coerce").dropna()
        holdout_excess = pd.to_numeric(holdout["avg_daily_equal_weight_excess"], errors="coerce").dropna()
        train_mean = float(train_excess.mean()) if len(train_excess) else np.nan
        holdout_mean = float(holdout_excess.mean()) if len(holdout_excess) else np.nan
        repeat_rows.append({
            **dict(zip(id_cols, key)),
            "available_periods": int(group["period"].nunique()),
            "train_periods": int(len(train_excess)),
            "train_positive_periods": int((train_excess > 0).sum()),
            "train_mean_daily_excess": train_mean,
            "holdout_p4_daily_excess": holdout_mean,
            "holdout_direction_matches_train": bool(np.sign(train_mean) == np.sign(holdout_mean)) if np.isfinite(train_mean) and np.isfinite(holdout_mean) and train_mean != 0 and holdout_mean != 0 else None,
            "evidence_state": "TIME_ORDERED_REPEAT_POSITIVE" if len(train_excess) >= 2 and (train_excess > 0).all() and holdout_mean > 0 else "MIXED_OR_INSUFFICIENT",
        })
    repeat = pd.DataFrame(repeat_rows).sort_values(["scope", "horizon", "research_regime", "strategy_family", "signal_name"], na_position="first").reset_index(drop=True)

    definitions = pd.DataFrame([
        {"strategy_family": "MR_BOLLINGER", "rule": "20-day close z-score <= -1.5 AND RSI14 <= 30", "category": "MEAN_REVERSION", "status": "VALIDATED_HYPOTHESIS"},
        {"strategy_family": "MA_CROSS_UP", "rule": "MA20 > MA60 AND previous MA20 <= previous MA60", "category": "TREND_MA_CROSSOVER", "status": "VALIDATED_HYPOTHESIS"},
        {"strategy_family": "BREAKOUT_252D", "rule": "close > previous 252-session rolling high (minimum 60 sessions)", "category": "BREAKOUT", "status": "VALIDATED_HYPOTHESIS"},
        {"strategy_family": "STATISTICAL_ARBITRAGE", "rule": "pair/spread/cointegration data required", "category": "PAIR_TRADING", "status": "SEPARATE_DATA_REQUIRED"},
    ])
    definitions.to_csv(OUT_DEFINITIONS, index=False, encoding="utf-8-sig")
    period_summary.to_csv(OUT_PERIOD, index=False, encoding="utf-8-sig")
    repeat.to_csv(OUT_REPEAT, index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_new_method_strategy_axis_matrix",
        "window": {"start": START.strftime("%Y-%m-%d"), "end": END.strftime("%Y-%m-%d")},
        "price_history_contract": price_integrity,
        "method": {
            "strategy_definitions": _safe_records(definitions),
            "atomic_signals": signals,
            "comparison_scopes": ["REGIME_ONLY", "STRATEGY_ONLY", "SIGNAL_ONLY", "REGIME_STRATEGY", "REGIME_SIGNAL", "STRATEGY_SIGNAL", "REGIME_STRATEGY_SIGNAL"],
            "time_order": "P1-P3 train-direction versus P4 holdout direction; no operating selection or approval",
        },
        "row_counts": {"panel_rows": int(len(panel)), "panel_dates": int(panel["date"].nunique()), "period_rows": int(len(period_summary)), "repeat_rows": int(len(repeat))},
        "limitations": [
            "Strategies are fixed research hypotheses, not an assumed regime-switching architecture.",
            "Statistical arbitrage cannot be tested from a single-stock OHLCV panel and requires separate pair/spread data.",
            "Returns are close-to-close research paths; no fills, costs, slippage, capacity, or operating gate is modeled.",
        ],
        "operational_change": False,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    positive = repeat[repeat["evidence_state"].eq("TIME_ORDERED_REPEAT_POSITIVE")]
    lines = [
        "# New Method Strategy-Axis Matrix", "",
        f"- generated_at: {payload['generated_at']}",
        f"- window: {payload['window']['start']} to {payload['window']['end']}",
        f"- panel_rows: {payload['row_counts']['panel_rows']}",
        f"- period_rows: {payload['row_counts']['period_rows']}",
        f"- time_ordered_repeat_positive_rows: {len(positive)}",
        "- scope: read-only; existing operating logic fields are not used", "", "## Strategy definitions",
    ]
    for _, row in definitions.iterrows():
        lines.append(f"- {row['strategy_family']}: {row['rule']} ({row['status']})")
    lines.extend(["", "## Time-ordered positive examples"])
    if positive.empty:
        lines.append("- none")
    else:
        for _, row in positive.sort_values("holdout_p4_daily_excess", ascending=False).head(20).iterrows():
            labels = [row["scope"], row["research_regime"], row["strategy_family"], row["signal_name"], row["horizon"]]
            labels = [str(x) for x in labels if pd.notna(x)]
            lines.append(f"- {' / '.join(labels)}: train={float(row['train_mean_daily_excess']):.6f}, P4={float(row['holdout_p4_daily_excess']):.6f}")
    lines.extend(["", "## Caveats", *[f"- {item}" for item in payload["limitations"]]])
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "definitions": str(OUT_DEFINITIONS), "period": str(OUT_PERIOD), "repeat": str(OUT_REPEAT), "json": str(OUT_JSON), "md": str(OUT_MD)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
