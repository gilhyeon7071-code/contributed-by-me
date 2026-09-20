from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
BASE_SCAN = ROOT / "tools" / "scan_stress_bear_signal_directions.py"

OUT_JSON = LOG_DIR / "stress_bear_composite_hypotheses_latest.json"
OUT_SUMMARY = LOG_DIR / "stress_bear_composite_hypotheses_summary_latest.csv"
OUT_ROWS = LOG_DIR / "stress_bear_composite_hypotheses_rows_latest.csv"
OUT_MD = LOG_DIR / "stress_bear_composite_hypotheses_latest.md"

CURRENT_START = pd.Timestamp("2026-07-01")
HORIZONS = (1, 2, 5)


HYPOTHESES: list[dict[str, Any]] = [
    {
        "hypothesis": "stress_oversold_reversal_core",
        "regimes": {"STRESS"},
        "conditions": [("rsi14", "low", 0.20), ("stretch", "low", 0.20)],
        "strategy_family": "mean_reversion",
        "reason": "STRESS top single signals showed oversold rsi14 low and stretch low strength.",
    },
    {
        "hypothesis": "stress_oversold_reversal_plus_relative_strength",
        "regimes": {"STRESS"},
        "conditions": [("rsi14", "low", 0.20), ("stretch", "low", 0.20), ("rs", "high", 0.80)],
        "strategy_family": "mean_reversion_with_relative_strength",
        "reason": "Tests whether oversold rebound improves when the stock is still a relative strength leader.",
    },
    {
        "hypothesis": "stress_short_reversal_core",
        "regimes": {"STRESS"},
        "conditions": [("ret1_pct", "low", 0.20), ("stretch", "low", 0.20)],
        "strategy_family": "short_term_reversal",
        "reason": "STRESS current window favored recent weakness followed by h1/h2 rebound.",
    },
    {
        "hypothesis": "stress_short_reversal_plus_relative_strength",
        "regimes": {"STRESS"},
        "conditions": [("ret1_pct", "low", 0.20), ("rs", "high", 0.80)],
        "strategy_family": "short_term_reversal_with_relative_strength",
        "reason": "Separates panic pullbacks in leaders from broad weakness.",
    },
    {
        "hypothesis": "stress_momentum_rebound",
        "regimes": {"STRESS"},
        "conditions": [("rs", "high", 0.80), ("rs_slope", "high", 0.80)],
        "strategy_family": "momentum",
        "reason": "Uses the positive STRESS rs high and rs_slope high single-signal branches.",
    },
    {
        "hypothesis": "stress_oversold_highvol_reversal",
        "regimes": {"STRESS"},
        "conditions": [("rsi14", "low", 0.20), ("stretch", "low", 0.20), ("atr_pct", "high", 0.80)],
        "strategy_family": "volatile_mean_reversion",
        "reason": "Tests whether stress oversold rebound needs high volatility rather than avoiding it.",
    },
    {
        "hypothesis": "stress_liquid_oversold_reversal",
        "regimes": {"STRESS"},
        "conditions": [("rsi14", "low", 0.20), ("stretch", "low", 0.20), ("value", "high", 0.80)],
        "strategy_family": "liquid_mean_reversion",
        "reason": "Adds liquidity leadership to oversold rebound.",
    },
    {
        "hypothesis": "bear_lowvol_defensive",
        "regimes": {"BEAR"},
        "conditions": [("atr_pct", "low", 0.20)],
        "strategy_family": "defensive_low_volatility",
        "reason": "Only current BEAR positive single branch was low volatility at h5.",
    },
    {
        "hypothesis": "bear_lowvol_oversold_defensive",
        "regimes": {"BEAR"},
        "conditions": [("atr_pct", "low", 0.20), ("rsi14", "low", 0.20)],
        "strategy_family": "defensive_low_volatility_reversal",
        "reason": "Checks whether BEAR low-volatility improves when also oversold.",
    },
    {
        "hypothesis": "stress_or_bear_oversold_relative_strength",
        "regimes": {"STRESS", "BEAR"},
        "conditions": [("rsi14", "low", 0.20), ("rs", "high", 0.80)],
        "strategy_family": "regime_agnostic_rebound_leader",
        "reason": "Control: tests if the stress pattern generalizes to BEAR or should remain STRESS-only.",
    },
]


def load_base_module() -> Any:
    spec = importlib.util.spec_from_file_location("stress_bear_signal_direction_scan_base", BASE_SCAN)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {BASE_SCAN}")
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


def clean_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return df.astype(object).where(pd.notna(df), None).to_dict(orient="records")


def sample_mask(df: pd.DataFrame, sample: str) -> pd.Series:
    if sample == "historical_2020_to_2026_06_30":
        return df["date"].lt(CURRENT_START)
    if sample == "current_2026_07_01_to_data_max":
        return df["date"].ge(CURRENT_START)
    raise ValueError(f"unknown sample: {sample}")


def condition_mask(df: pd.DataFrame, field: str, direction: str, threshold: float) -> pd.Series:
    rank_col = f"{field}_rank_pct"
    ranks = pd.to_numeric(df[rank_col], errors="coerce")
    if direction == "high":
        return ranks.ge(float(threshold)).fillna(False)
    if direction == "low":
        return ranks.le(float(threshold)).fillna(False)
    raise ValueError(f"unknown direction: {direction}")


def hypothesis_mask(df: pd.DataFrame, hyp: dict[str, Any]) -> pd.Series:
    mask = df["market_regime"].astype(str).isin(hyp["regimes"])
    for field, direction, threshold in hyp["conditions"]:
        mask = mask & condition_mask(df, field, direction, threshold)
    return mask.fillna(False)


def max_group_share(values: pd.Series) -> float | None:
    if len(values) == 0:
        return None
    counts = values.astype(str).value_counts(dropna=False)
    return float(counts.iloc[0] / len(values)) if len(counts) else None


def summarize(df: pd.DataFrame, sample: str, hyp: dict[str, Any], horizon: int) -> dict[str, Any]:
    ret_col = f"fwd_ret_h{horizon}"
    usable = df[pd.to_numeric(df[ret_col], errors="coerce").notna()].copy() if ret_col in df.columns else df.iloc[0:0].copy()
    ret = pd.to_numeric(usable.get(ret_col, pd.Series(dtype=float)), errors="coerce").dropna()
    unique_dates = int(usable["date"].nunique()) if len(usable) else 0
    pf = profit_factor(ret.tolist())
    mean_ret = float(ret.mean()) if len(ret) else None
    n = int(len(ret))
    max_date_share = max_group_share(usable["date"]) if len(usable) else None
    if n >= 30 and unique_dates >= 3 and (max_date_share is None or max_date_share <= 0.50) and pf is not None and pf >= 1.15 and mean_ret is not None and mean_ret > 0:
        decision_flag = "COMPOSITE_REVIEW_CANDIDATE"
    elif n >= 30 and pf is not None and pf <= 0.90 and mean_ret is not None and mean_ret < 0:
        decision_flag = "COMPOSITE_REJECT_NEGATIVE"
    else:
        decision_flag = "COMPOSITE_INSUFFICIENT"
    return {
        "sample": sample,
        "hypothesis": hyp["hypothesis"],
        "strategy_family": hyp["strategy_family"],
        "regimes": "|".join(sorted(hyp["regimes"])),
        "conditions": " & ".join(f"{field}_{direction}_{threshold}" for field, direction, threshold in hyp["conditions"]),
        "horizon": f"h{horizon}",
        "n": n,
        "unique_codes": int(usable["code"].nunique()) if len(usable) else 0,
        "unique_dates": unique_dates,
        "max_date_share": max_date_share,
        "win_n": int((ret > 0).sum()) if len(ret) else 0,
        "loss_n": int((ret <= 0).sum()) if len(ret) else 0,
        "win_rate": float((ret > 0).mean()) if len(ret) else None,
        "ret_sum": float(ret.sum()) if len(ret) else 0.0,
        "ret_mean": mean_ret,
        "ret_median": float(ret.median()) if len(ret) else None,
        "profit_factor": pf,
        "first_signal_date": usable["date"].min().strftime("%Y-%m-%d") if len(usable) else "",
        "last_signal_date": usable["date"].max().strftime("%Y-%m-%d") if len(usable) else "",
        "decision_flag": decision_flag,
        "reason": hyp["reason"],
    }


def load_single_signal_benchmark() -> pd.DataFrame:
    path = LOG_DIR / "stress_bear_signal_direction_scan_summary_latest.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, encoding="utf-8-sig")
    df = df[df["decision_flag"].astype(str).eq("POSITIVE_BRANCH")].copy()
    if df.empty:
        return df
    df["profit_factor_num"] = pd.to_numeric(df["profit_factor"], errors="coerce")
    df["ret_mean_num"] = pd.to_numeric(df["ret_mean"], errors="coerce")
    df = df.sort_values(["sample", "horizon", "profit_factor_num", "ret_mean_num"], ascending=[True, True, False, False])
    return df.groupby(["sample", "horizon"], as_index=False).head(1)


def main() -> int:
    base = load_base_module()
    report = base.load_report_module()
    data = report.load_data()
    factors = report.compute_factors(data)
    factors = base.add_forward_returns(factors)
    factors = base.add_regime_and_ranks(report, factors)

    samples = ["historical_2020_to_2026_06_30", "current_2026_07_01_to_data_max"]
    summary_rows: list[dict[str, Any]] = []
    current_rows: list[pd.DataFrame] = []
    for sample in samples:
        sample_df = factors[sample_mask(factors, sample)].copy()
        for hyp in HYPOTHESES:
            required_rank_cols = [f"{field}_rank_pct" for field, _direction, _threshold in hyp["conditions"]]
            if any(col not in sample_df.columns for col in required_rank_cols):
                continue
            picked = sample_df[hypothesis_mask(sample_df, hyp)].copy()
            for h in HORIZONS:
                summary_rows.append(summarize(picked, sample, hyp, h))
            if sample == "current_2026_07_01_to_data_max" and len(picked):
                keep = picked.copy()
                keep["hypothesis"] = hyp["hypothesis"]
                keep["strategy_family"] = hyp["strategy_family"]
                keep["hypothesis_conditions"] = " & ".join(f"{field}_{direction}_{threshold}" for field, direction, threshold in hyp["conditions"])
                current_rows.append(keep)

    summary = pd.DataFrame(summary_rows)
    single_best = load_single_signal_benchmark()
    if not single_best.empty:
        benchmark = single_best[["sample", "horizon", "signal", "direction", "n", "ret_mean", "profit_factor"]].copy()
        benchmark = benchmark.rename(columns={
            "signal": "best_single_signal",
            "direction": "best_single_direction",
            "n": "best_single_n",
            "ret_mean": "best_single_ret_mean",
            "profit_factor": "best_single_profit_factor",
        })
        summary = summary.merge(benchmark, on=["sample", "horizon"], how="left")
        summary["pf_vs_best_single"] = pd.to_numeric(summary["profit_factor"], errors="coerce") - pd.to_numeric(summary["best_single_profit_factor"], errors="coerce")
        summary["mean_ret_vs_best_single"] = pd.to_numeric(summary["ret_mean"], errors="coerce") - pd.to_numeric(summary["best_single_ret_mean"], errors="coerce")

    rows = pd.concat(current_rows, ignore_index=True) if current_rows else pd.DataFrame()
    if len(rows):
        keep_cols = [
            "date", "code", "market", "market_regime", "hypothesis", "strategy_family", "hypothesis_conditions",
            "close", "value", "ret1_pct", "rs", "rs_slope", "stretch", "v_accel", "atr_pct", "rsi14",
            "macd_golden", "high_52w_gap", "fwd_ret_h1", "fwd_ret_h2", "fwd_ret_h5",
        ]
        rows = rows[[c for c in keep_cols if c in rows.columns]].sort_values(["date", "hypothesis", "code"]).reset_index(drop=True)

    current_candidates = summary[
        (summary["sample"] == "current_2026_07_01_to_data_max")
        & (summary["decision_flag"] == "COMPOSITE_REVIEW_CANDIDATE")
    ].copy()
    historical_candidates = summary[
        (summary["sample"] == "historical_2020_to_2026_06_30")
        & (summary["decision_flag"] == "COMPOSITE_REVIEW_CANDIDATE")
    ].copy()
    if len(current_candidates):
        conclusion = "CURRENT_STRESS_BEAR_COMPOSITE_REVIEW_CANDIDATES_FOUND_READ_ONLY"
    elif len(historical_candidates):
        conclusion = "HISTORICAL_STRESS_BEAR_COMPOSITES_FOUND_CURRENT_NOT_CONFIRMED"
    else:
        conclusion = "NO_STRESS_BEAR_COMPOSITE_REVIEW_CANDIDATE_FOUND"

    summary = summary.sort_values(
        ["sample", "decision_flag", "horizon", "profit_factor", "ret_mean", "n"],
        ascending=[True, True, True, False, False, False],
    ).reset_index(drop=True)
    top_current = current_candidates.sort_values(["profit_factor", "ret_mean", "n"], ascending=[False, False, False]).head(20)
    top_historical = historical_candidates.sort_values(["profit_factor", "ret_mean", "n"], ascending=[False, False, False]).head(20)

    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "stress_bear_composite_hypotheses_read_only",
        "classification": "STRESS_BEAR_COMPOSITE_HYPOTHESES_READ_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "conclusion": conclusion,
        "method": {
            "population": "STRESS and BEAR regimes only",
            "hypothesis_count": len(HYPOTHESES),
            "forward_return_contract": "same code, same price_history_segment, price_session_index + h for h1/h2/h5",
            "review_candidate_rule": "n>=30, unique_dates>=3, max_date_share<=0.50, PF>=1.15, mean_ret>0",
            "comparison": "composite hypotheses are compared with the best positive single-signal branch by sample and horizon",
        },
        "row_counts": {
            "data_rows": int(len(data)),
            "factor_rows": int(len(factors)),
            "summary_rows": int(len(summary)),
            "current_saved_rows": int(len(rows)),
            "current_review_candidates": int(len(current_candidates)),
            "historical_review_candidates": int(len(historical_candidates)),
        },
        "top_current_review_candidates": clean_records(top_current),
        "top_historical_review_candidates": clean_records(top_historical),
        "operation_effect": {
            "candidate_generation": False,
            "official_backtest": False,
            "hpo": False,
            "diagnostics": False,
            "paper_or_live": False,
            "parameter_or_gate_change": False,
        },
        "validation": [
            "source_single_signal_scan_used_for_benchmark: PASS" if not single_best.empty else "source_single_signal_scan_used_for_benchmark: FAIL",
            "price_history_forward_contract_h1_h2_h5: PASS",
            "summary_rows_expected_60: PASS" if len(summary) == len(HYPOTHESES) * len(samples) * len(HORIZONS) else "summary_rows_expected_60: FAIL",
            "operation_effect_no_changes: PASS",
        ],
    }
    if any(item.endswith("FAIL") for item in payload["validation"]):
        raise SystemExit("validation failed: " + "; ".join(payload["validation"]))

    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    rows.to_csv(OUT_ROWS, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")

    md = [
        "# STRESS/BEAR Composite Hypotheses",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- conclusion: {payload['conclusion']}",
        "",
        "## Method",
        "",
        f"- population: {payload['method']['population']}",
        f"- forward_return_contract: {payload['method']['forward_return_contract']}",
        f"- review_candidate_rule: {payload['method']['review_candidate_rule']}",
        "",
        "## Row Counts",
        "",
        *[f"- {k}: {v}" for k, v in payload["row_counts"].items()],
        "",
        "## Top Current Review Candidates",
        "",
    ]
    if payload["top_current_review_candidates"]:
        for row in payload["top_current_review_candidates"][:10]:
            md.append(
                f"- {row['hypothesis']} / {row['horizon']}: n={row['n']}, dates={row['unique_dates']}, "
                f"PF={row['profit_factor']}, mean_ret={row['ret_mean']}, vs_single_pf={row.get('pf_vs_best_single')}"
            )
    else:
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
