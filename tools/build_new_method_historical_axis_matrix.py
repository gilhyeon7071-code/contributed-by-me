#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Build a broad historical, read-only signal-axis matrix for new-method research.

This module does not use the existing operating candidate, buy, block, score,
HPO, or replay verdict fields.  It starts with the valid KRX research universe
and compares each observable signal's cross-sectional quintiles by research
regime and exact h1/h2/h5 price outcome.
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

OUT_SUMMARY = LOG_DIR / "new_method_historical_axis_matrix_summary_latest.csv"
OUT_REPEAT = LOG_DIR / "new_method_historical_axis_matrix_repeat_latest.csv"
OUT_PROFILE = LOG_DIR / "new_method_historical_axis_matrix_profile_latest.csv"
OUT_JSON = LOG_DIR / "new_method_historical_axis_matrix_latest.json"
OUT_MD = LOG_DIR / "new_method_historical_axis_matrix_latest.md"

WINDOW_START = pd.Timestamp("2025-06-01")
WINDOW_END = pd.Timestamp("2026-06-30")
HORIZONS = (1, 2, 5)
MIN_CLOSED_PER_PERIOD = 50

NUMERIC_AXES: tuple[tuple[str, str], ...] = (
    ("rs", "RELATIVE_STRENGTH"),
    ("rs_slope", "MOMENTUM_CHANGE"),
    ("ret1_pct", "SHORT_TERM_MOVE"),
    ("stretch", "DISTANCE_FROM_MA5"),
    ("v_accel", "VOLUME_ACCELERATION"),
    ("atr_pct", "VOLATILITY"),
    ("rsi14", "OSCILLATOR"),
    ("high_52w_gap", "DISTANCE_FROM_52W_HIGH"),
    ("value", "LIQUIDITY"),
)
BOOLEAN_AXES: tuple[tuple[str, str], ...] = (("macd_golden", "TREND_TURN"),)
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
    spec = importlib.util.spec_from_file_location("report_for_new_method_historical_axis", REPORT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load report module: {REPORT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _period(value: pd.Timestamp) -> str:
    date = pd.Timestamp(value)
    if date <= pd.Timestamp("2025-09-30"):
        return "P1_202506_202509"
    if date <= pd.Timestamp("2025-12-31"):
        return "P2_202510_202512"
    if date <= pd.Timestamp("2026-03-31"):
        return "P3_202601_202603"
    return "P4_202604_202606"


def _profit_factor(values: pd.Series) -> float | None:
    ret = pd.to_numeric(values, errors="coerce").dropna()
    gains = float(ret[ret > 0].sum())
    losses = float(-ret[ret < 0].sum())
    if losses > 0:
        return gains / losses
    if gains > 0:
        return None
    return 0.0


def _exact_forward_returns(factors: pd.DataFrame) -> pd.DataFrame:
    out = factors.sort_values(["price_history_key", "price_session_index"], kind="mergesort").copy()
    group = out.groupby("price_history_key", sort=False)
    for horizon in HORIZONS:
        future_close = group["close"].shift(-horizon)
        future_session = group["price_session_index"].shift(-horizon)
        exact = future_session.sub(out["price_session_index"]).eq(horizon)
        out[f"ret_h{horizon}"] = future_close.div(out["close"]).sub(1.0).where(exact)
    return out


def _numeric_axis_rows(base: pd.DataFrame, field: str, family: str) -> pd.DataFrame:
    work = base[["date", "code", "period", "research_regime", field, "ret_h1", "ret_h2", "ret_h5"]].copy().reset_index(drop=True)
    work[field] = pd.to_numeric(work[field], errors="coerce")
    work = work.dropna(subset=[field])
    percentile = work.groupby("date", sort=False)[field].rank(method="first", pct=True)
    work["axis_bin"] = np.select(
        [percentile.le(0.20), percentile.le(0.40), percentile.le(0.60), percentile.le(0.80)],
        ["Q1_LOWEST", "Q2", "Q3", "Q4"],
        default="Q5_HIGHEST",
    )
    work["axis_field"] = field
    work["axis_family"] = family
    return work


def _boolean_axis_rows(base: pd.DataFrame, field: str, family: str) -> pd.DataFrame:
    work = base[["date", "code", "period", "research_regime", field, "ret_h1", "ret_h2", "ret_h5"]].copy().reset_index(drop=True)
    work["axis_bin"] = np.where(work[field].fillna(False).astype(bool), "TRUE", "FALSE")
    work["axis_field"] = field
    work["axis_family"] = family
    return work.drop(columns=[field])


def _baseline_returns(base: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for (period, regime), group in base.groupby(["period", "research_regime"], sort=False, dropna=False):
        for horizon in HORIZONS:
            ret = pd.to_numeric(group[f"ret_h{horizon}"], errors="coerce").dropna()
            records.append({
                "period": period,
                "research_regime": regime,
                "horizon": f"h{horizon}",
                "baseline_closed_rows": int(len(ret)),
                "baseline_avg_return": float(ret.mean()) if len(ret) else None,
                "baseline_median_return": float(ret.median()) if len(ret) else None,
            })
    return pd.DataFrame(records)


def _summarize_axis_rows(rows: pd.DataFrame, baseline: pd.DataFrame) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    keys = ["period", "research_regime", "axis_family", "axis_field", "axis_bin"]
    for key, group in rows.groupby(keys, sort=False, dropna=False):
        for horizon in HORIZONS:
            closed = group[pd.to_numeric(group[f"ret_h{horizon}"], errors="coerce").notna()].copy()
            ret = pd.to_numeric(closed[f"ret_h{horizon}"], errors="coerce").dropna()
            results.append({
                "period": key[0], "research_regime": key[1], "axis_family": key[2], "axis_field": key[3], "axis_bin": key[4], "horizon": f"h{horizon}",
                "universe_rows": int(len(group)), "closed_rows": int(len(ret)), "pending_or_invalid_rows": int(len(group) - len(ret)),
                "unique_dates": int(closed["date"].nunique()) if len(ret) else 0, "unique_codes": int(closed["code"].nunique()) if len(ret) else 0,
                "win_rate": float((ret > 0).mean()) if len(ret) else None, "avg_return": float(ret.mean()) if len(ret) else None,
                "median_return": float(ret.median()) if len(ret) else None, "profit_factor": _profit_factor(ret),
                "evidence_state": "ENOUGH_DESCRIPTIVE_SAMPLE" if len(ret) >= MIN_CLOSED_PER_PERIOD else "SMALL_DESCRIPTIVE_SAMPLE",
            })
    out = pd.DataFrame(results)
    out = out.merge(baseline, on=["period", "research_regime", "horizon"], how="left", validate="many_to_one")
    out["excess_avg_return"] = out["avg_return"] - out["baseline_avg_return"]
    return _safe_records(out)

def _repeat_summary(summary: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    keys = ["research_regime", "axis_family", "axis_field", "axis_bin", "horizon"]
    for key, group in summary.groupby(keys, sort=False, dropna=False):
        usable = group[pd.to_numeric(group["closed_rows"], errors="coerce").ge(MIN_CLOSED_PER_PERIOD)].copy()
        excess = pd.to_numeric(usable["excess_avg_return"], errors="coerce")
        positive = int(excess.gt(0).sum())
        negative = int(excess.lt(0).sum())
        if len(usable) >= 3 and positive == len(usable):
            state = "REPEATED_POSITIVE_EXCESS_DESCRIPTIVE"
        elif len(usable) >= 3 and negative == len(usable):
            state = "REPEATED_NEGATIVE_EXCESS_DESCRIPTIVE"
        else:
            state = "MIXED_OR_INSUFFICIENT"
        rows.append({
            "research_regime": key[0], "axis_family": key[1], "axis_field": key[2], "axis_bin": key[3], "horizon": key[4],
            "available_periods": int(len(group)), "enough_sample_periods": int(len(usable)),
            "positive_excess_periods": positive, "negative_excess_periods": negative,
            "mean_of_period_excess_returns": float(excess.mean()) if len(excess) else None,
            "median_of_period_excess_returns": float(excess.median()) if len(excess) else None, "repeat_state": state,
        })
    return pd.DataFrame(rows).sort_values(keys).reset_index(drop=True)

def _safe_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return df.astype(object).where(pd.notna(df), None).to_dict(orient="records")


def main() -> int:
    report = _load_report_module()
    raw = report.load_data()
    price_integrity = raw.attrs.get("price_history_integrity", {})
    factors = report.compute_factors(raw)
    factors["date"] = pd.to_datetime(factors["date"], errors="coerce").dt.normalize()
    regime_map = report._assign_report_research_regime(factors)
    factors["research_regime"] = factors["date"].map(regime_map)
    factors = _exact_forward_returns(factors)

    base = factors[
        factors["date"].between(WINDOW_START, WINDOW_END)
        & factors["market"].astype(str).str.upper().isin(["KOSPI", "KOSDAQ"])
        & pd.to_numeric(factors["close"], errors="coerce").gt(0)
        & pd.to_numeric(factors["value"], errors="coerce").gt(0)
    ].copy()
    base["period"] = base["date"].map(_period)
    base = base.set_index(["date", "code"], drop=False)
    if base.empty:
        raise SystemExit("historical research universe is empty")

    profile_rows: list[dict[str, Any]] = []
    for period, group in base.groupby("period", dropna=False):
        profile_rows.append({
            "profile_level": "period_regime",
            "period": period,
            "research_regime": "ALL",
            "rows": int(len(group)),
            "unique_dates": int(group["date"].nunique()),
            "unique_codes": int(group["code"].nunique()),
        })
        for regime, segment in group.groupby("research_regime", dropna=False):
            profile_rows.append({
                "profile_level": "period_regime",
                "period": period,
                "research_regime": str(regime),
                "rows": int(len(segment)),
                "unique_dates": int(segment["date"].nunique()),
                "unique_codes": int(segment["code"].nunique()),
            })
    profile = pd.DataFrame(profile_rows).sort_values(["period", "research_regime"]).reset_index(drop=True)

    axis_frames: list[pd.DataFrame] = []
    for field, family in NUMERIC_AXES:
        if field in base.columns:
            axis_frames.append(_numeric_axis_rows(base, field, family))
    for field, family in BOOLEAN_AXES:
        if field in base.columns:
            axis_frames.append(_boolean_axis_rows(base, field, family))
    if not axis_frames:
        raise SystemExit("no configured axis fields found in factor panel")
    axis_rows = pd.concat(axis_frames, ignore_index=True)
    baseline = _baseline_returns(base)
    summary = pd.DataFrame(_summarize_axis_rows(axis_rows, baseline))
    summary = summary.sort_values(["period", "research_regime", "axis_family", "axis_field", "axis_bin", "horizon"]).reset_index(drop=True)
    repeat = _repeat_summary(summary)

    required_summary_rows = int(len(summary))
    if required_summary_rows == 0:
        raise SystemExit("axis summary has no rows")
    if int(summary["closed_rows"].sum()) <= 0:
        raise SystemExit("axis summary has no exact closed returns")

    OUT_SUMMARY.write_text(summary.to_csv(index=False), encoding="utf-8-sig")
    OUT_REPEAT.write_text(repeat.to_csv(index=False), encoding="utf-8-sig")
    OUT_PROFILE.write_text(profile.to_csv(index=False), encoding="utf-8-sig")

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_new_method_historical_axis_matrix",
        "question": "Across a broad research universe, which signal-axis quintiles show repeatable descriptive outcome direction by research regime and h1/h2/h5?",
        "window": {"start": WINDOW_START.strftime("%Y-%m-%d"), "end": WINDOW_END.strftime("%Y-%m-%d")},
        "periods": ["P1_202506_202509", "P2_202510_202512", "P3_202601_202603", "P4_202604_202606"],
        "population": "valid KOSPI/KOSDAQ factor rows with positive close and daily value; no candidate, buy, block, or score filter",
        "excluded_existing_logic_fields": list(BANNED_EXISTING_LOGIC_FIELDS),
        "price_history_contract": price_integrity,
        "row_counts": {
            "raw_price_rows": int(len(raw)),
            "factor_rows": int(len(factors)),
            "analysis_universe_rows": int(len(base)),
            "analysis_unique_dates": int(base["date"].nunique()),
            "analysis_unique_codes": int(base["code"].nunique()),
            "summary_rows": required_summary_rows,
            "repeat_rows": int(len(repeat)),
            "repeated_positive_excess_descriptive_rows": int((repeat["repeat_state"] == "REPEATED_POSITIVE_EXCESS_DESCRIPTIVE").sum()),
            "repeated_negative_excess_descriptive_rows": int((repeat["repeat_state"] == "REPEATED_NEGATIVE_EXCESS_DESCRIPTIVE").sum()),
        },
        "methodology": {
            "signal_bins": "numeric signals are same-date cross-sectional quintiles; macd_golden is TRUE/FALSE",
            "outcome": "close at exact h-th following global trading session in the same price-history segment",
            "horizons": ["h1", "h2", "h5"],
            "repeat_rule": f"A descriptive repeat requires at least 3 periods with at least {MIN_CLOSED_PER_PERIOD} closed rows and the same excess-average-return sign versus the same period/regime universe.",
            "not_an_operational_rule": True,
        },
        "profile_records": _safe_records(profile),
        "repeat_records": _safe_records(repeat),
        "limitations": [
            "This tests signal-axis associations in the research universe, not a final entry/exit strategy or a filled-trade simulation.",
            "Research regimes are ex-ante labels computed only from information available on each date, but they remain one regime definition among possible definitions.",
            "No transaction cost, intraday execution, slippage, or position-concentration model is included.",
            "Repeated descriptive direction is a screening result, not a promotion or operational decision.",
        ],
        "operational_change": False,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")

    lines = [
        "# New Method Historical Signal-Axis Matrix",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- window: {payload['window']['start']} to {payload['window']['end']}",
        f"- research_universe_rows: {payload['row_counts']['analysis_universe_rows']}",
        f"- unique_dates: {payload['row_counts']['analysis_unique_dates']}",
        f"- unique_codes: {payload['row_counts']['analysis_unique_codes']}",
        f"- repeated_positive_excess_descriptive_rows: {payload['row_counts']['repeated_positive_excess_descriptive_rows']}",
        f"- repeated_negative_excess_descriptive_rows: {payload['row_counts']['repeated_negative_excess_descriptive_rows']}",
        "- existing operating-logic fields: excluded",
        "",
        "## Data profile",
    ]
    for _, row in profile.iterrows():
        lines.append(f"- {row['period']} / {row['research_regime']}: rows={int(row['rows'])}, dates={int(row['unique_dates'])}, codes={int(row['unique_codes'])}")
    lines.extend(["", "## Repeated descriptive directions"])
    repeated = repeat[repeat["repeat_state"].ne("MIXED_OR_INSUFFICIENT")]
    for _, row in repeated.iterrows():
        lines.append(
            f"- {row['research_regime']} / {row['axis_field']} / {row['axis_bin']} / {row['horizon']}: "
            f"state={row['repeat_state']}, periods={int(row['enough_sample_periods'])}, "
            f"positive={int(row['positive_excess_periods'])}, negative={int(row['negative_excess_periods'])}, "
            f"mean_of_period_excess={float(row['mean_of_period_excess_returns']):.6f}"
        )
    if repeated.empty:
        lines.append("- none")
    lines.extend(["", "## Caveats", *[f"- {item}" for item in payload["limitations"]]])
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(json.dumps({"status": "OK", "summary": str(OUT_SUMMARY), "repeat": str(OUT_REPEAT), "profile": str(OUT_PROFILE), "json": str(OUT_JSON), "md": str(OUT_MD)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
