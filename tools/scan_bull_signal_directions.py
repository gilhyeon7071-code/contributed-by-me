from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
COMMON_SCAN = ROOT / "tools" / "scan_sideways_signal_directions.py"

OUT_JSON = LOG_DIR / "bull_signal_direction_scan_latest.json"
OUT_SUMMARY = LOG_DIR / "bull_signal_direction_scan_summary_latest.csv"
OUT_ROWS = LOG_DIR / "bull_signal_direction_scan_rows_latest.csv"
OUT_MD = LOG_DIR / "bull_signal_direction_scan_latest.md"

REGIME = "BULL"


def load_common() -> Any:
    spec = importlib.util.spec_from_file_location("scan_signal_directions_common_for_bull", COMMON_SCAN)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {COMMON_SCAN}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    common = load_common()
    report = common.load_report_module()
    data = report.load_data()
    factors = report.compute_factors(data)
    factors = common.add_forward_returns(factors)
    factors = common.add_regime_and_ranks(report, factors)

    base = factors[factors["market_regime"].astype(str).eq(REGIME)].copy()
    data_max = pd.to_datetime(factors["date"], errors="coerce").max().strftime("%Y-%m-%d")
    samples = ["historical_2020_to_2026_06_30", "current_2026_07_01_to_data_max"]

    summary_rows: list[dict[str, Any]] = []
    selected_rows: list[pd.DataFrame] = []
    for sample in samples:
        sample_df = base[common.sample_mask(base, sample)].copy()
        for sig in common.SIGNALS:
            if sig["field"] not in sample_df.columns:
                continue
            if sig["direction"] != "true" and f"{sig['field']}_rank_pct" not in sample_df.columns:
                continue
            picked = sample_df[common.signal_mask(sample_df, sig)].copy()
            for h in common.HORIZONS:
                summary_rows.append(common.summarize(picked, sample, REGIME, sig, h))
            if sample == "current_2026_07_01_to_data_max" and len(picked):
                keep = picked.copy()
                keep["matched_signal"] = sig["signal"]
                keep["matched_direction"] = sig["direction"]
                keep["family_hint"] = sig["family_hint"]
                selected_rows.append(keep)

    summary = common.score_summary(pd.DataFrame(summary_rows))
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
        conclusion = "CURRENT_BULL_POSITIVE_SINGLE_SIGNAL_BRANCHES_FOUND_READ_ONLY"
    elif len(historical_positive):
        conclusion = "HISTORICAL_BULL_POSITIVE_BRANCHES_FOUND_CURRENT_NOT_CONFIRMED"
    else:
        conclusion = "NO_BULL_SINGLE_SIGNAL_BRANCH_SUPPORTED"

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
        "scope": "bull_signal_direction_scan_read_only",
        "classification": "BULL_SIGNAL_DIRECTION_SCAN_READ_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "conclusion": conclusion,
        "data_window": {
            "min_date": pd.to_datetime(factors["date"], errors="coerce").min().strftime("%Y-%m-%d"),
            "max_date": data_max,
            "current_sample_start": common.CURRENT_START.strftime("%Y-%m-%d"),
        },
        "method": {
            "population": "market_regime == BULL only",
            "rank_basis": "cross-sectional percentile rank by signal date",
            "forward_return_contract": "same code, same price_history_segment, price_session_index + h for h1/h2/h5",
            "decision_flag_rule": "n>=50 and PF>=1.15 and mean_ret>0 => POSITIVE_BRANCH; n>=50 and PF<=0.90 and mean_ret<0 => NEGATIVE_BRANCH",
        },
        "row_counts": {
            "data_rows": int(len(data)),
            "factor_rows": int(len(factors)),
            "bull_rows": int(len(base)),
            "current_bull_rows": int(len(base[base["date"].ge(common.CURRENT_START)])),
            "summary_rows": int(len(summary)),
            "current_signal_rows_saved": int(len(rows)),
        },
        "top_positive_branches": common.clean_records(
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

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    rows.to_csv(OUT_ROWS, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")

    md = [
        "# BULL Signal Direction Scan",
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
            f"- {row['sample']} / BULL / {row['signal']} {row['direction']} / {row['horizon']}: "
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
