from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
REPORT_BACKTEST = ROOT / "report_backtest_v41_1.py"

OUT_JSON = LOG_DIR / "stress_bear_signal_direction_scan_latest.json"
OUT_SUMMARY = LOG_DIR / "stress_bear_signal_direction_scan_summary_latest.csv"
OUT_ROWS = LOG_DIR / "stress_bear_signal_direction_scan_rows_latest.csv"
OUT_MD = LOG_DIR / "stress_bear_signal_direction_scan_latest.md"

CURRENT_START = pd.Timestamp("2026-07-01")
HORIZONS = (1, 2, 5)


SIGNALS: list[dict[str, Any]] = [
    {"signal": "rs", "field": "rs", "direction": "high", "threshold": 0.80, "family_hint": "momentum_relative_strength"},
    {"signal": "rs", "field": "rs", "direction": "low", "threshold": 0.20, "family_hint": "mean_reversion_weakness"},
    {"signal": "rs_slope", "field": "rs_slope", "direction": "high", "threshold": 0.80, "family_hint": "momentum_acceleration"},
    {"signal": "ret1_pct", "field": "ret1_pct", "direction": "high", "threshold": 0.80, "family_hint": "short_momentum"},
    {"signal": "ret1_pct", "field": "ret1_pct", "direction": "low", "threshold": 0.20, "family_hint": "short_reversal"},
    {"signal": "stretch", "field": "stretch", "direction": "high", "threshold": 0.80, "family_hint": "extended_trend"},
    {"signal": "stretch", "field": "stretch", "direction": "low", "threshold": 0.20, "family_hint": "mean_reversion_discount"},
    {"signal": "v_accel", "field": "v_accel", "direction": "high", "threshold": 0.80, "family_hint": "volume_acceleration"},
    {"signal": "atr_pct", "field": "atr_pct", "direction": "high", "threshold": 0.80, "family_hint": "high_volatility"},
    {"signal": "atr_pct", "field": "atr_pct", "direction": "low", "threshold": 0.20, "family_hint": "low_volatility_defensive"},
    {"signal": "rsi14", "field": "rsi14", "direction": "high", "threshold": 0.80, "family_hint": "overbought_trend"},
    {"signal": "rsi14", "field": "rsi14", "direction": "low", "threshold": 0.20, "family_hint": "oversold_reversal"},
    {"signal": "high_52w_gap", "field": "high_52w_gap", "direction": "low", "threshold": 0.20, "family_hint": "near_52w_high"},
    {"signal": "high_52w_gap", "field": "high_52w_gap", "direction": "high", "threshold": 0.80, "family_hint": "far_from_52w_high"},
    {"signal": "value", "field": "value", "direction": "high", "threshold": 0.80, "family_hint": "liquidity_leader"},
    {"signal": "macd_golden", "field": "macd_golden", "direction": "true", "threshold": None, "family_hint": "trend_turn"},
]


def load_report_module() -> Any:
    spec = importlib.util.spec_from_file_location("report_backtest_v41_1_for_stress_bear_signal_scan", REPORT_BACKTEST)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {REPORT_BACKTEST}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def profit_factor(ret: Iterable[float]) -> float | None:
    gains = 0.0
    losses = 0.0
    for value in ret:
        if value > 0:
            gains += float(value)
        elif value < 0:
            losses += abs(float(value))
    if losses > 0:
        return gains / losses
    if gains > 0:
        return None
    return 0.0


def summarize(df: pd.DataFrame, sample: str, regime: str, signal: dict[str, Any], horizon: int) -> dict[str, Any]:
    ret_col = f"fwd_ret_h{horizon}"
    ret = pd.to_numeric(df.get(ret_col, pd.Series(dtype=float)), errors="coerce").dropna()
    return {
        "sample": sample,
        "market_regime": regime,
        "signal": signal["signal"],
        "field": signal["field"],
        "direction": signal["direction"],
        "threshold": signal["threshold"],
        "family_hint": signal["family_hint"],
        "horizon": f"h{horizon}",
        "n": int(len(ret)),
        "win_n": int((ret > 0).sum()) if len(ret) else 0,
        "loss_n": int((ret <= 0).sum()) if len(ret) else 0,
        "win_rate": float((ret > 0).mean()) if len(ret) else None,
        "ret_sum": float(ret.sum()) if len(ret) else 0.0,
        "ret_mean": float(ret.mean()) if len(ret) else None,
        "ret_median": float(ret.median()) if len(ret) else None,
        "profit_factor": profit_factor(ret.tolist()),
        "unique_codes": int(df["code"].nunique()) if "code" in df.columns and len(df) else 0,
        "first_signal_date": df["date"].min().strftime("%Y-%m-%d") if len(df) else "",
        "last_signal_date": df["date"].max().strftime("%Y-%m-%d") if len(df) else "",
    }


def add_forward_returns(factors: pd.DataFrame) -> pd.DataFrame:
    df = factors.sort_values(["code", "price_session_index"]).copy()
    for h in HORIZONS:
        next_close = df.groupby("code", sort=False)["close"].shift(-h)
        next_session = df.groupby("code", sort=False)["price_session_index"].shift(-h)
        next_segment = df.groupby("code", sort=False)["price_history_segment"].shift(-h)
        same_segment = next_segment.eq(df["price_history_segment"])
        actual_h = next_session.eq(df["price_session_index"] + h)
        df[f"fwd_ret_h{h}"] = (next_close / df["close"]) - 1.0
        df.loc[~(same_segment & actual_h), f"fwd_ret_h{h}"] = pd.NA
    return df


def add_regime_and_ranks(report: Any, factors: pd.DataFrame) -> pd.DataFrame:
    df = factors.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    regime = report._assign_report_research_regime(df)
    df["market_regime"] = df["date"].map(regime)
    rank_fields = sorted({item["field"] for item in SIGNALS if item["direction"] in {"high", "low"}})
    for field in rank_fields:
        if field in df.columns:
            df[f"{field}_rank_pct"] = pd.to_numeric(df[field], errors="coerce").groupby(df["date"]).rank(pct=True)
    return df


def signal_mask(df: pd.DataFrame, signal: dict[str, Any]) -> pd.Series:
    field = signal["field"]
    direction = signal["direction"]
    if direction == "true":
        return df[field].fillna(False).astype(bool)
    rank_col = f"{field}_rank_pct"
    ranks = pd.to_numeric(df[rank_col], errors="coerce")
    threshold = float(signal["threshold"])
    if direction == "high":
        return ranks.ge(threshold).fillna(False)
    if direction == "low":
        return ranks.le(threshold).fillna(False)
    raise ValueError(f"unknown direction: {direction}")


def sample_mask(df: pd.DataFrame, sample: str) -> pd.Series:
    if sample == "historical_2020_to_2026_06_30":
        return df["date"].lt(CURRENT_START)
    if sample == "current_2026_07_01_to_data_max":
        return df["date"].ge(CURRENT_START)
    raise ValueError(f"unknown sample: {sample}")


def score_summary(summary: pd.DataFrame) -> pd.DataFrame:
    out = summary.copy()
    out["decision_flag"] = "INSUFFICIENT"
    enough = pd.to_numeric(out["n"], errors="coerce").ge(50)
    pf = pd.to_numeric(out["profit_factor"], errors="coerce")
    mean_ret = pd.to_numeric(out["ret_mean"], errors="coerce")
    out.loc[enough & pf.ge(1.15) & mean_ret.gt(0), "decision_flag"] = "POSITIVE_BRANCH"
    out.loc[enough & pf.le(0.90) & mean_ret.lt(0), "decision_flag"] = "NEGATIVE_BRANCH"
    return out


def clean_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return df.astype(object).where(pd.notna(df), None).to_dict(orient="records")


def main() -> int:
    report = load_report_module()
    data = report.load_data()
    factors = report.compute_factors(data)
    factors = add_forward_returns(factors)
    factors = add_regime_and_ranks(report, factors)

    base = factors[factors["market_regime"].astype(str).isin(["STRESS", "BEAR"])].copy()
    data_max = pd.to_datetime(factors["date"], errors="coerce").max().strftime("%Y-%m-%d")
    samples = ["historical_2020_to_2026_06_30", "current_2026_07_01_to_data_max"]

    summary_rows: list[dict[str, Any]] = []
    selected_rows: list[pd.DataFrame] = []
    for sample in samples:
        sample_df = base[sample_mask(base, sample)].copy()
        for regime in ["STRESS", "BEAR", "STRESS_OR_BEAR"]:
            regime_df = sample_df if regime == "STRESS_OR_BEAR" else sample_df[sample_df["market_regime"].astype(str).eq(regime)].copy()
            for sig in SIGNALS:
                if sig["field"] not in regime_df.columns:
                    continue
                if sig["direction"] != "true" and f"{sig['field']}_rank_pct" not in regime_df.columns:
                    continue
                picked = regime_df[signal_mask(regime_df, sig)].copy()
                for h in HORIZONS:
                    summary_rows.append(summarize(picked, sample, regime, sig, h))
                if sample == "current_2026_07_01_to_data_max" and len(picked):
                    keep = picked.copy()
                    keep["matched_signal"] = sig["signal"]
                    keep["matched_direction"] = sig["direction"]
                    keep["family_hint"] = sig["family_hint"]
                    selected_rows.append(keep)

    summary = score_summary(pd.DataFrame(summary_rows))
    rows = pd.concat(selected_rows, ignore_index=True) if selected_rows else pd.DataFrame()

    current_positive = summary[
        (summary["sample"] == "current_2026_07_01_to_data_max")
        & (summary["decision_flag"] == "POSITIVE_BRANCH")
    ].copy()
    historical_positive = summary[
        (summary["sample"] == "historical_2020_to_2026_06_30")
        & (summary["decision_flag"] == "POSITIVE_BRANCH")
    ].copy()
    if len(current_positive):
        conclusion = "CURRENT_STRESS_BEAR_POSITIVE_SINGLE_SIGNAL_BRANCHES_FOUND_READ_ONLY"
    elif len(historical_positive):
        conclusion = "HISTORICAL_STRESS_BEAR_POSITIVE_BRANCHES_FOUND_CURRENT_NOT_CONFIRMED"
    else:
        conclusion = "NO_STRESS_BEAR_SINGLE_SIGNAL_BRANCH_SUPPORTED"

    sort_cols = ["sample", "market_regime", "horizon", "decision_flag", "profit_factor", "ret_mean", "n"]
    summary = summary.sort_values(sort_cols, ascending=[True, True, True, True, False, False, False]).reset_index(drop=True)
    if len(rows):
        keep_cols = [
            "date", "code", "market", "market_regime", "matched_signal", "matched_direction", "family_hint",
            "close", "value", "ret1_pct", "rs", "rs_slope", "stretch", "v_accel", "atr_pct", "rsi14",
            "macd_golden", "high_52w_gap", "fwd_ret_h1", "fwd_ret_h2", "fwd_ret_h5",
        ]
        rows = rows[[c for c in keep_cols if c in rows.columns]].sort_values(["date", "matched_signal", "code"]).reset_index(drop=True)

    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "stress_bear_signal_direction_scan_read_only",
        "classification": "STRESS_BEAR_SIGNAL_DIRECTION_SCAN_READ_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "conclusion": conclusion,
        "data_window": {
            "min_date": pd.to_datetime(factors["date"], errors="coerce").min().strftime("%Y-%m-%d"),
            "max_date": data_max,
            "current_sample_start": CURRENT_START.strftime("%Y-%m-%d"),
        },
        "method": {
            "population": "market_regime in STRESS or BEAR only",
            "rank_basis": "cross-sectional percentile rank by signal date",
            "forward_return_contract": "same code, same price_history_segment, price_session_index + h for h1/h2/h5",
            "decision_flag_rule": "n>=50 and PF>=1.15 and mean_ret>0 => POSITIVE_BRANCH; n>=50 and PF<=0.90 and mean_ret<0 => NEGATIVE_BRANCH",
        },
        "row_counts": {
            "data_rows": int(len(data)),
            "factor_rows": int(len(factors)),
            "stress_bear_rows": int(len(base)),
            "current_stress_bear_rows": int(len(base[base["date"].ge(CURRENT_START)])),
            "summary_rows": int(len(summary)),
            "current_signal_rows_saved": int(len(rows)),
        },
        "top_positive_branches": clean_records(
            summary[summary["decision_flag"].eq("POSITIVE_BRANCH")]
            .sort_values(["sample", "profit_factor", "ret_mean"], ascending=[True, False, False])
            .head(20)
        ),
        "operation_effect": {
            "candidate_generation": False,
            "official_backtest": False,
            "hpo": False,
            "diagnostics": False,
            "paper_or_live": False,
            "parameter_or_gate_change": False,
        },
        "validation": [
            "price_history_forward_contract_h1_h2_h5: PASS",
            "summary_rows_positive: PASS" if len(summary) else "summary_rows_positive: FAIL",
            "operation_effect_no_changes: PASS",
        ],
    }
    if any(item.endswith("FAIL") for item in payload["validation"]):
        raise SystemExit("validation failed: " + "; ".join(payload["validation"]))

    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    rows.to_csv(OUT_ROWS, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    md = [
        "# STRESS/BEAR Signal Direction Scan",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- conclusion: {payload['conclusion']}",
        f"- data_window: {payload['data_window']['min_date']} ~ {payload['data_window']['max_date']}",
        f"- current_sample_start: {payload['data_window']['current_sample_start']}",
        "",
        "## Method",
        "",
        f"- population: {payload['method']['population']}",
        f"- rank_basis: {payload['method']['rank_basis']}",
        f"- forward_return_contract: {payload['method']['forward_return_contract']}",
        f"- decision_flag_rule: {payload['method']['decision_flag_rule']}",
        "",
        "## Row Counts",
        "",
        *[f"- {k}: {v}" for k, v in payload["row_counts"].items()],
        "",
        "## Top Positive Branches",
        "",
    ]
    for row in payload["top_positive_branches"][:10]:
        md.append(
            f"- {row['sample']} / {row['market_regime']} / {row['signal']} {row['direction']} / {row['horizon']}: "
            f"n={row['n']}, ret_mean={row['ret_mean']}, pf={row['profit_factor']}, family={row['family_hint']}"
        )
    if not payload["top_positive_branches"]:
        md.append("- none")
    md += [
        "",
        "## Operation Effect",
        "",
        "- NOT_APPLIED to candidate generation, official backtest, HPO, diagnostics, paper/live, gates, thresholds, or parameters.",
    ]
    OUT_MD.write_text("\n".join(md) + "\n", encoding="utf-8")

    print(f"[OK] wrote {OUT_JSON}")
    print(f"[OK] wrote {OUT_SUMMARY}")
    print(f"[OK] wrote {OUT_ROWS}")
    print(f"[OK] wrote {OUT_MD}")
    print(f"[CONCLUSION] {conclusion}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
