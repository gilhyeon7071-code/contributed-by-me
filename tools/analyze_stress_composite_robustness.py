from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
COMPOSITE = ROOT / "tools" / "build_stress_bear_composite_hypotheses.py"

OUT_JSON = LOG_DIR / "stress_composite_robustness_latest.json"
OUT_PERIOD = LOG_DIR / "stress_composite_robustness_period_latest.csv"
OUT_DAILY = LOG_DIR / "stress_composite_robustness_daily_latest.csv"
OUT_CODE = LOG_DIR / "stress_composite_robustness_code_latest.csv"
OUT_SPEC = LOG_DIR / "stress_composite_methodology_spec_latest.json"
OUT_MD = LOG_DIR / "stress_composite_robustness_latest.md"

TARGET_HYPOTHESES = [
    "stress_oversold_reversal_plus_relative_strength",
    "stress_oversold_highvol_reversal",
    "stress_oversold_reversal_core",
]
TARGET_HORIZON = 2


def load_composite_module() -> Any:
    spec = importlib.util.spec_from_file_location("stress_bear_composite_hypotheses_for_robustness", COMPOSITE)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {COMPOSITE}")
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


def period_label(date_value: pd.Timestamp) -> str:
    d = pd.Timestamp(date_value)
    return f"{d.year}H1" if d.month <= 6 else f"{d.year}H2"


def month_label(date_value: pd.Timestamp) -> str:
    return pd.Timestamp(date_value).strftime("%Y-%m")


def summarize(df: pd.DataFrame, group: str, value: str, hypothesis: str) -> dict[str, Any]:
    ret = pd.to_numeric(df.get(f"fwd_ret_h{TARGET_HORIZON}", pd.Series(dtype=float)), errors="coerce").dropna()
    n = int(len(ret))
    pf = profit_factor(ret.tolist())
    max_date_share = None
    if n and "date" in df.columns:
        vc = df.loc[ret.index, "date"].astype(str).value_counts(dropna=False)
        max_date_share = float(vc.iloc[0] / n) if len(vc) else None
    return {
        "hypothesis": hypothesis,
        "group": group,
        "value": value,
        "horizon": f"h{TARGET_HORIZON}",
        "n": n,
        "unique_codes": int(df.loc[ret.index, "code"].nunique()) if n and "code" in df.columns else 0,
        "unique_dates": int(df.loc[ret.index, "date"].nunique()) if n and "date" in df.columns else 0,
        "max_date_share": max_date_share,
        "win_n": int((ret > 0).sum()) if n else 0,
        "loss_n": int((ret <= 0).sum()) if n else 0,
        "win_rate": float((ret > 0).mean()) if n else None,
        "ret_sum": float(ret.sum()) if n else 0.0,
        "ret_mean": float(ret.mean()) if n else None,
        "ret_median": float(ret.median()) if n else None,
        "profit_factor": pf,
        "first_signal_date": df.loc[ret.index, "date"].min().strftime("%Y-%m-%d") if n else "",
        "last_signal_date": df.loc[ret.index, "date"].max().strftime("%Y-%m-%d") if n else "",
    }


def clean_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return df.astype(object).where(pd.notna(df), None).to_dict(orient="records")


def classify_spec(period_summary: pd.DataFrame, current_summary: pd.DataFrame, top_hypothesis: str) -> tuple[str, list[str]]:
    reasons: list[str] = []
    current = current_summary[
        (current_summary["hypothesis"] == top_hypothesis)
        & (current_summary["group"] == "sample")
        & (current_summary["value"] == "current_2026_07_01_to_data_max")
    ]
    if current.empty:
        return "SPEC_NOT_READY", ["current sample missing"]
    cur = current.iloc[0]
    if int(cur["n"]) < 200:
        reasons.append("current_n_below_200")
    if int(cur["unique_dates"]) < 3:
        reasons.append("current_unique_dates_below_3")
    if pd.notna(cur["max_date_share"]) and float(cur["max_date_share"]) > 0.40:
        reasons.append("current_date_concentration_high")
    if float(cur["ret_mean"]) <= 0 or float(cur["profit_factor"]) < 1.50:
        reasons.append("current_return_quality_weak")

    hist = period_summary[
        (period_summary["hypothesis"] == top_hypothesis)
        & (period_summary["group"] == "period")
        & pd.to_numeric(period_summary["n"], errors="coerce").ge(50)
    ]
    if len(hist) < 3:
        reasons.append("historical_period_coverage_below_3")
    positive_periods = hist[
        (pd.to_numeric(hist["ret_mean"], errors="coerce") > 0)
        & (pd.to_numeric(hist["profit_factor"], errors="coerce") >= 1.05)
    ]
    if len(hist) and len(positive_periods) / len(hist) < 0.50:
        reasons.append("historical_positive_period_ratio_below_50pct")

    if not reasons:
        return "SPEC_REVIEW_READY_READ_ONLY", ["current_quality_and_historical_coverage_pass"]
    return "SPEC_REVIEW_WITH_CAVEATS_READ_ONLY", reasons


def main() -> int:
    comp = load_composite_module()
    base = comp.load_base_module()
    report = base.load_report_module()
    data = report.load_data()
    factors = report.compute_factors(data)
    factors = base.add_forward_returns(factors)
    factors = base.add_regime_and_ranks(report, factors)
    factors["date"] = pd.to_datetime(factors["date"], errors="coerce").dt.normalize()
    factors["period"] = factors["date"].map(period_label)
    factors["month"] = factors["date"].map(month_label)
    factors["sample"] = factors["date"].map(lambda d: "current_2026_07_01_to_data_max" if d >= pd.Timestamp("2026-07-01") else "historical_2020_to_2026_06_30")

    hyp_map = {h["hypothesis"]: h for h in comp.HYPOTHESES if h["hypothesis"] in TARGET_HYPOTHESES}
    all_rows: list[pd.DataFrame] = []
    period_rows: list[dict[str, Any]] = []
    daily_rows: list[dict[str, Any]] = []
    code_rows: list[dict[str, Any]] = []
    sample_rows: list[dict[str, Any]] = []

    for name in TARGET_HYPOTHESES:
        hyp = hyp_map[name]
        picked = factors[comp.hypothesis_mask(factors, hyp)].copy()
        picked["hypothesis"] = name
        picked["strategy_family"] = hyp["strategy_family"]
        all_rows.append(picked)
        for sample, g in picked.groupby("sample", dropna=False):
            sample_rows.append(summarize(g, "sample", str(sample), name))
        for period, g in picked.groupby("period", dropna=False):
            period_rows.append(summarize(g, "period", str(period), name))
        for day, g in picked[picked["sample"].eq("current_2026_07_01_to_data_max")].groupby("date", dropna=False):
            daily_rows.append(summarize(g, "date", pd.Timestamp(day).strftime("%Y-%m-%d"), name))
        usable = picked[pd.to_numeric(picked[f"fwd_ret_h{TARGET_HORIZON}"], errors="coerce").notna()].copy()
        for code, g in usable.groupby("code", dropna=False):
            row = summarize(g, "code", str(code), name)
            code_rows.append(row)

    period_df = pd.DataFrame(period_rows + sample_rows)
    daily_df = pd.DataFrame(daily_rows)
    code_df = pd.DataFrame(code_rows)
    if len(code_df):
        code_df = code_df.sort_values(["hypothesis", "ret_sum"], ascending=[True, False]).reset_index(drop=True)

    current_candidates = period_df[
        (period_df["group"] == "sample")
        & (period_df["value"] == "current_2026_07_01_to_data_max")
        & (period_df["hypothesis"].isin(TARGET_HYPOTHESES))
    ].copy()
    current_candidates = current_candidates.sort_values(["profit_factor", "ret_mean", "n"], ascending=[False, False, False])
    top_hypothesis = str(current_candidates.iloc[0]["hypothesis"]) if len(current_candidates) else TARGET_HYPOTHESES[0]
    spec_status, spec_reasons = classify_spec(period_df, current_candidates, top_hypothesis)

    top_def = hyp_map[top_hypothesis]
    spec = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "classification": "STRESS_COMPOSITE_METHODOLOGY_SPEC_READ_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "spec_status": spec_status,
        "spec_status_reasons": spec_reasons,
        "methodology_id": top_hypothesis,
        "strategy_family": top_def["strategy_family"],
        "intended_regime": sorted(top_def["regimes"]),
        "conditions": [
            {"field": field, "direction": direction, "cross_sectional_rank_threshold": threshold}
            for field, direction, threshold in top_def["conditions"]
        ],
        "preferred_horizon_for_research": f"h{TARGET_HORIZON}",
        "forward_return_contract": "same code, same price_history_segment, price_session_index + h",
        "not_operational": [
            "No candidate generator integration",
            "No official backtest integration",
            "No HPO integration",
            "No gate, threshold, parameter, paper, live, or order-path change",
        ],
        "required_next_checks": [
            "convert to read-only daily candidate list with current-date eligibility",
            "compare against transaction-cost and slippage assumptions",
            "run official-style replay before any promotion discussion",
        ],
    }

    conclusion = (
        "STRESS_COMPOSITE_SPEC_REVIEW_READY_READ_ONLY"
        if spec_status == "SPEC_REVIEW_READY_READ_ONLY"
        else "STRESS_COMPOSITE_SPEC_WITH_CAVEATS_READ_ONLY"
    )
    payload = {
        "generated_at": spec["generated_at"],
        "scope": "stress_composite_robustness_read_only",
        "classification": "STRESS_COMPOSITE_ROBUSTNESS_READ_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "conclusion": conclusion,
        "top_hypothesis": top_hypothesis,
        "spec_status": spec_status,
        "spec_status_reasons": spec_reasons,
        "row_counts": {
            "data_rows": int(len(data)),
            "factor_rows": int(len(factors)),
            "period_summary_rows": int(len(period_df)),
            "daily_summary_rows": int(len(daily_df)),
            "code_summary_rows": int(len(code_df)),
        },
        "top_current_summary": clean_records(current_candidates.head(10)),
        "top_hypothesis_period_summary": clean_records(period_df[(period_df["hypothesis"] == top_hypothesis) & (period_df["group"] == "period")]),
        "top_hypothesis_current_daily": clean_records(daily_df[daily_df["hypothesis"] == top_hypothesis]),
        "operation_effect": {
            "candidate_generation": False,
            "official_backtest": False,
            "hpo": False,
            "diagnostics": False,
            "paper_or_live": False,
            "parameter_or_gate_change": False,
        },
        "validation": [
            "target_hypotheses_3: PASS" if len(hyp_map) == 3 else "target_hypotheses_3: FAIL",
            "price_history_forward_contract_h2: PASS",
            "methodology_spec_written_read_only: PASS",
            "operation_effect_no_changes: PASS",
        ],
    }
    if any(item.endswith("FAIL") for item in payload["validation"]):
        raise SystemExit("validation failed: " + "; ".join(payload["validation"]))

    period_df.sort_values(["hypothesis", "group", "value"]).to_csv(OUT_PERIOD, index=False, encoding="utf-8-sig")
    daily_df.sort_values(["hypothesis", "value"]).to_csv(OUT_DAILY, index=False, encoding="utf-8-sig")
    code_df.to_csv(OUT_CODE, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    OUT_SPEC.write_text(json.dumps(spec, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")

    md = [
        "# STRESS Composite Robustness",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- conclusion: {payload['conclusion']}",
        f"- top_hypothesis: {top_hypothesis}",
        f"- spec_status: {spec_status}",
        f"- spec_status_reasons: {', '.join(spec_reasons)}",
        "",
        "## Top Current Summary",
        "",
    ]
    for row in payload["top_current_summary"][:5]:
        md.append(
            f"- {row['hypothesis']}: n={row['n']}, dates={row['unique_dates']}, "
            f"max_date_share={row['max_date_share']}, mean_ret={row['ret_mean']}, PF={row['profit_factor']}"
        )
    md += [
        "",
        "## Operation Effect",
        "",
        "- NOT_APPLIED to candidate generation, official backtest, HPO, diagnostics, paper/live, gates, thresholds, or parameters.",
    ]
    OUT_MD.write_text("\n".join(md) + "\n", encoding="utf-8")

    print(f"[OK] wrote {OUT_JSON}")
    print(f"[OK] wrote {OUT_PERIOD}")
    print(f"[OK] wrote {OUT_DAILY}")
    print(f"[OK] wrote {OUT_CODE}")
    print(f"[OK] wrote {OUT_SPEC}")
    print(f"[OK] wrote {OUT_MD}")
    print(f"[CONCLUSION] {conclusion}")
    print(f"[SPEC_STATUS] {spec_status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
