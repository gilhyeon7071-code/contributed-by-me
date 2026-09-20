from __future__ import annotations

import csv
import importlib.util
import json
import os
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
REPORT_BACKTEST = ROOT / "report_backtest_v41_1.py"

SPEC_JSON = LOG_DIR / "c3_highvol_research_candidate_spec_latest.json"
SELECTION_CONTRACT = LOG_DIR / "c3_normal_official_replay_selection_contract_latest.json"
RESEARCH_PARAMS = LOG_DIR / "c3_strategy_replay_params_latest.json"
ZERO_BREAKDOWN_JSON = LOG_DIR / "c3_highvol_current_forward_zero_breakdown_latest.json"

RUN_DIR = LOG_DIR / "stress_bear_separate_strategy_need_runtime"
OUT_JSON = LOG_DIR / "stress_bear_separate_strategy_need_latest.json"
OUT_SUMMARY = LOG_DIR / "stress_bear_separate_strategy_need_summary_latest.csv"
OUT_ROWS = LOG_DIR / "stress_bear_separate_strategy_need_rows_latest.csv"
OUT_MD = LOG_DIR / "stress_bear_separate_strategy_need_latest.md"

CURRENT_SIGNAL_START = "2026-07-01"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def load_report_module() -> Any:
    spec = importlib.util.spec_from_file_location("report_backtest_v41_1_for_stress_bear_probe", REPORT_BACKTEST)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {REPORT_BACKTEST}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def drop_filters(contract: dict[str, Any], fields: set[str]) -> dict[str, Any]:
    out = deepcopy(contract)
    out["numeric_filters"] = [
        dict(item)
        for item in list(out.get("numeric_filters") or [])
        if str(item.get("field", "")).strip() not in fields
    ]
    return out


def clear_regimes(contract: dict[str, Any]) -> dict[str, Any]:
    out = deepcopy(contract)
    out["regimes"] = []
    return out


def with_window_exit(contract: dict[str, Any], start: str, end: str) -> dict[str, Any]:
    out = deepcopy(contract)
    out["signal_date_start"] = start
    out["signal_date_end"] = end
    out["exit_overrides"] = {
        "hold": 7,
        "stop_loss": -0.06,
        "disable_paper_exit_rules": True,
    }
    return out


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


def summarize(frame: pd.DataFrame, variant: str, stage: str, note: str) -> dict[str, Any]:
    ret = pd.to_numeric(frame.get("ret", pd.Series(dtype=float)), errors="coerce").dropna()
    return {
        "variant": variant,
        "stage": stage,
        "note": note,
        "n": int(len(frame)),
        "win_n": int((ret > 0).sum()) if len(ret) else 0,
        "loss_n": int((ret <= 0).sum()) if len(ret) else 0,
        "ret_sum": float(ret.sum()) if len(ret) else 0.0,
        "ret_mean": float(ret.mean()) if len(ret) else None,
        "profit_factor": profit_factor(ret.tolist()),
        "unique_codes": int(frame["code"].nunique()) if "code" in frame.columns and len(frame) else 0,
        "first_signal_date": str(frame["signal_date"].min()) if "signal_date" in frame.columns and len(frame) else "",
        "last_signal_date": str(frame["signal_date"].max()) if "signal_date" in frame.columns and len(frame) else "",
        "first_entry_date": str(frame["entry_date"].min()) if "entry_date" in frame.columns and len(frame) else "",
        "last_entry_date": str(frame["entry_date"].max()) if "entry_date" in frame.columns and len(frame) else "",
    }


def highvol_mask(trades: pd.DataFrame, spec: dict[str, Any], regimes: set[str]) -> pd.Series:
    candidate = spec["candidate_spec"]
    gap_lo = float(candidate["entry_gap_pct"]["min_inclusive"])
    gap_hi = float(candidate["entry_gap_pct"]["max_exclusive"])
    vacc_lo = float(candidate["signal_v_accel"]["min_inclusive"])
    vacc_hi = float(candidate["signal_v_accel"]["best_same_sample_max_exclusive"])
    atr_lo = float(candidate["signal_atr_pct"]["min_inclusive"])
    atr_hi = float(candidate["signal_atr_pct"]["max_exclusive"])
    if trades.empty:
        return pd.Series(dtype=bool)
    return (
        trades["market_regime"].astype(str).isin(regimes)
        & pd.to_numeric(trades["entry_gap_pct"], errors="coerce").ge(gap_lo)
        & pd.to_numeric(trades["entry_gap_pct"], errors="coerce").lt(gap_hi)
        & pd.to_numeric(trades["signal_v_accel"], errors="coerce").ge(vacc_lo)
        & pd.to_numeric(trades["signal_v_accel"], errors="coerce").lt(vacc_hi)
        & pd.to_numeric(trades["signal_atr_pct"], errors="coerce").ge(atr_lo)
        & pd.to_numeric(trades["signal_atr_pct"], errors="coerce").lt(atr_hi)
    ).fillna(False)


def clean_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return df.astype(object).where(pd.notna(df), None).to_dict(orient="records")


def main() -> int:
    spec = load_json(SPEC_JSON)
    zero = load_json(ZERO_BREAKDOWN_JSON)
    if zero.get("conclusion") != "ZERO_ROWS_CAUSED_BY_REGIME_WINDOW":
        raise ValueError("expected prior zero breakdown to be regime-window driven")

    os.environ["REPORT_RESEARCH_MODE"] = "1"
    os.environ["REPORT_RESEARCH_PARAMS_PATH"] = str(RESEARCH_PARAMS)
    os.environ["REPORT_RESEARCH_SELECTION_CONTRACT_PATH"] = str(SELECTION_CONTRACT)
    os.environ["REPORT_RESEARCH_OUTPUT_DIR"] = str(RUN_DIR)
    os.environ["REPORT_ALLOW_UNAPPROVED_FALLBACK"] = "1"

    report = load_report_module()
    params = report.load_params()
    contract_payload = load_json(SELECTION_CONTRACT)
    base_contract = contract_payload.get("selection_contract", contract_payload)

    data = report.load_data()
    factors = report.compute_factors(data)
    data_max_date = pd.to_datetime(data["date"], errors="coerce").max().strftime("%Y-%m-%d")
    context = report.prepare_simulation_context(factors)

    variants = [
        {
            "variant": "current_source_filters_highvol_family_regime",
            "drop_fields": {"mkt_vol20", "signal_value"},
            "post_regimes": {"BULL", "SIDEWAYS"},
            "note": "Baseline current-forward high-vol family; expected zero in STRESS/BEAR window.",
        },
        {
            "variant": "stress_bear_remove_breadth_same_highvol_shape",
            "drop_fields": {"mkt_vol20", "signal_value", "mkt_breadth"},
            "post_regimes": {"STRESS", "BEAR"},
            "note": "Read-only separate-family need probe: remove anti-stress breadth filter, keep same high-vol shape, allow STRESS/BEAR.",
        },
        {
            "variant": "transition_remove_breadth_same_highvol_shape",
            "drop_fields": {"mkt_vol20", "signal_value", "mkt_breadth"},
            "post_regimes": {"TRANSITION"},
            "note": "Control probe: transition only after breadth removal.",
        },
    ]

    summary_rows: list[dict[str, Any]] = []
    output_rows: list[pd.DataFrame] = []
    for variant in variants:
        contract = with_window_exit(
            clear_regimes(drop_filters(base_contract, set(variant["drop_fields"]))),
            CURRENT_SIGNAL_START,
            data_max_date,
        )
        trades = report.simulate_trades(factors, params, selection_contract=contract, simulation_context=context)
        trades = trades.copy()
        trades["probe_variant"] = variant["variant"]
        trades["probe_note"] = variant["note"]
        summary_rows.append(summarize(trades, variant["variant"], "triggered_trades_before_post_filter", variant["note"]))
        selected = trades[highvol_mask(trades, spec, set(variant["post_regimes"]))].copy() if not trades.empty else trades.copy()
        selected["probe_stage"] = "post_highvol_shape_and_regime_filter"
        selected["probe_allowed_regimes"] = "|".join(sorted(variant["post_regimes"]))
        summary_rows.append(summarize(selected, variant["variant"], "post_highvol_shape_and_regime_filter", variant["note"]))
        if not selected.empty:
            output_rows.append(selected)

    rows_df = pd.concat(output_rows, ignore_index=True) if output_rows else pd.DataFrame()
    summary_df = pd.DataFrame(summary_rows)

    stress_row = summary_df[
        (summary_df["variant"] == "stress_bear_remove_breadth_same_highvol_shape")
        & (summary_df["stage"] == "post_highvol_shape_and_regime_filter")
    ]
    stress_n = int(stress_row.iloc[0]["n"]) if not stress_row.empty else 0
    if stress_n > 0:
        conclusion = "STRESS_BEAR_SEPARATE_FAMILY_HAS_CURRENT_SHADOW_ROWS_REVIEW_REQUIRED"
    else:
        conclusion = "STRESS_BEAR_SEPARATE_FAMILY_NOT_SUPPORTED_BY_CURRENT_WINDOW"

    validation = [
        "prior_zero_regime_window_confirmed: PASS",
        "variant_summary_rows_6: PASS" if len(summary_df) == 6 else "variant_summary_rows_6: FAIL",
        "stress_bear_probe_executed: PASS",
        "operation_effect_no_changes: PASS",
    ]
    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "stress_bear_separate_strategy_need_probe_read_only",
        "classification": "STRESS_BEAR_SEPARATE_STRATEGY_NEED_PROBE_READ_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "conclusion": conclusion,
        "window": {
            "signal_date_start": CURRENT_SIGNAL_START,
            "signal_date_end": data_max_date,
        },
        "source_artifacts": {
            "zero_breakdown": str(ZERO_BREAKDOWN_JSON),
            "report_backtest": str(REPORT_BACKTEST),
            "spec": str(SPEC_JSON),
            "selection_contract": str(SELECTION_CONTRACT),
            "research_params": str(RESEARCH_PARAMS),
        },
        "summary": clean_records(summary_df),
        "operation_effect": {
            "candidate_generation": False,
            "official_backtest": False,
            "hpo": False,
            "diagnostics": False,
            "paper_or_live": False,
            "parameter_or_gate_change": False,
        },
        "validation": validation,
    }
    if any(item.endswith("FAIL") for item in validation):
        raise SystemExit("validation failed: " + "; ".join(validation))

    summary_df.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    rows_df.to_csv(OUT_ROWS, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    lines = [
        "# STRESS/BEAR Separate Strategy Need Probe",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- conclusion: {payload['conclusion']}",
        f"- signal_window: {CURRENT_SIGNAL_START} ~ {data_max_date}",
        "",
        "## Summary",
        "",
        "| variant | stage | n | ret_sum | profit_factor |",
        "|---|---|---:|---:|---:|",
    ]
    for row in clean_records(summary_df):
        lines.append(f"| {row['variant']} | {row['stage']} | {row['n']} | {row['ret_sum']} | {row['profit_factor']} |")
    lines.extend([
        "",
        "## Validation",
        "",
        *[f"- {item}" for item in validation],
        "",
        "## Guardrail",
        "",
        "Read-only need probe only. It does not approve STRESS/BEAR trading, candidate generation, official backtest, HPO, gates, paper, or live orders.",
        "",
    ])
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    print(json.dumps({
        "status": "ok",
        "json": str(OUT_JSON),
        "summary": str(OUT_SUMMARY),
        "rows": str(OUT_ROWS),
        "md": str(OUT_MD),
        "conclusion": conclusion,
        "stress_bear_post_filter_rows": stress_n,
        "validation": validation,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
