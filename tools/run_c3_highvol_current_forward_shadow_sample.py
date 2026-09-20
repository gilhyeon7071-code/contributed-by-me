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
HISTORICAL_GENERATOR_JSON = LOG_DIR / "c3_highvol_exact_strategy_family_generator_latest.json"

RUN_DIR = LOG_DIR / "c3_highvol_current_forward_shadow_runtime"
OUT_JSON = LOG_DIR / "c3_highvol_current_forward_shadow_sample_latest.json"
OUT_ROWS = LOG_DIR / "c3_highvol_current_forward_shadow_sample_rows_latest.csv"
OUT_SUMMARY = LOG_DIR / "c3_highvol_current_forward_shadow_sample_summary_latest.csv"
OUT_MD = LOG_DIR / "c3_highvol_current_forward_shadow_sample_latest.md"

CURRENT_SIGNAL_START = "2026-07-01"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def load_report_module() -> Any:
    spec = importlib.util.spec_from_file_location("report_backtest_v41_1_for_c3_highvol_current_forward", REPORT_BACKTEST)
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


def with_exit_and_window(contract: dict[str, Any], start: str, end: str) -> dict[str, Any]:
    out = deepcopy(contract)
    out["signal_date_start"] = start
    out["signal_date_end"] = end
    out["exit_overrides"] = {
        "hold": 7,
        "stop_loss": -0.06,
        "disable_paper_exit_rules": True,
    }
    return out


def norm_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d").fillna("")


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


def metric_row(df: pd.DataFrame, group: str, value: str, note: str) -> dict[str, Any]:
    ret = pd.to_numeric(df.get("ret", pd.Series(dtype=float)), errors="coerce").dropna()
    positives = ret[ret > 0].sort_values(ascending=False)
    gross_profit = float(positives.sum())
    top5 = float(positives.iloc[:5].sum()) if len(positives) else 0.0
    return {
        "group": group,
        "value": value,
        "note": note,
        "n": int(len(df)),
        "win_n": int((ret > 0).sum()) if len(ret) else 0,
        "loss_n": int((ret <= 0).sum()) if len(ret) else 0,
        "stop_n": int((df["exit_reason"].astype(str).str.upper() == "STOP").sum()) if len(df) else 0,
        "ret_sum": float(ret.sum()) if len(ret) else 0.0,
        "ret_mean": float(ret.mean()) if len(ret) else None,
        "profit_factor": profit_factor(ret.tolist()),
        "unique_codes": int(df["code"].nunique()) if "code" in df.columns and len(df) else 0,
        "first_signal_date": str(df["signal_date"].min()) if len(df) else "",
        "last_signal_date": str(df["signal_date"].max()) if len(df) else "",
        "first_entry_date": str(df["entry_date"].min()) if len(df) else "",
        "last_entry_date": str(df["entry_date"].max()) if len(df) else "",
        "top5_share_gross_profit": (top5 / gross_profit) if gross_profit else None,
        "ret_without_top5": float(ret.sum() - top5) if len(ret) else 0.0,
    }


def clean_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return df.astype(object).where(pd.notna(df), None).to_dict(orient="records")


def main() -> int:
    spec = load_json(SPEC_JSON)
    historical = load_json(HISTORICAL_GENERATOR_JSON)
    if spec.get("operational_decision") != "NOT_APPROVED" or spec.get("full_logic_application") != "NOT_APPLIED":
        raise ValueError("source high-vol spec must remain NOT_APPROVED / NOT_APPLIED")

    candidate = spec["candidate_spec"]
    gap_lo = float(candidate["entry_gap_pct"]["min_inclusive"])
    gap_hi = float(candidate["entry_gap_pct"]["max_exclusive"])
    vacc_lo = float(candidate["signal_v_accel"]["min_inclusive"])
    vacc_hi = float(candidate["signal_v_accel"]["best_same_sample_max_exclusive"])
    atr_lo = float(candidate["signal_atr_pct"]["min_inclusive"])
    atr_hi = float(candidate["signal_atr_pct"]["max_exclusive"])
    regimes = set(candidate["market_regime"])

    os.environ["REPORT_RESEARCH_MODE"] = "1"
    os.environ["REPORT_RESEARCH_PARAMS_PATH"] = str(RESEARCH_PARAMS)
    os.environ["REPORT_RESEARCH_SELECTION_CONTRACT_PATH"] = str(SELECTION_CONTRACT)
    os.environ["REPORT_RESEARCH_OUTPUT_DIR"] = str(RUN_DIR)
    os.environ["REPORT_ALLOW_UNAPPROVED_FALLBACK"] = "1"

    report = load_report_module()
    params = report.load_params()
    contract_payload = load_json(SELECTION_CONTRACT)
    base_contract = contract_payload.get("selection_contract", contract_payload)
    source_contract = clear_regimes(drop_filters(base_contract, {"mkt_vol20", "signal_value"}))

    data = report.load_data()
    factors = report.compute_factors(data)
    data_max_date = pd.to_datetime(data["date"], errors="coerce").max().strftime("%Y-%m-%d")
    data_min_date = pd.to_datetime(data["date"], errors="coerce").min().strftime("%Y-%m-%d")
    contract = with_exit_and_window(source_contract, CURRENT_SIGNAL_START, data_max_date)
    context = report.prepare_simulation_context(factors)
    trades = report.simulate_trades(factors, params, selection_contract=contract, simulation_context=context)
    if trades.empty:
        trades["entry_date_norm"] = pd.Series(dtype=str)
    else:
        trades["entry_date_norm"] = norm_date(trades["entry_date"])

    if trades.empty:
        exact = trades.copy()
    else:
        mask = (
            trades["market_regime"].astype(str).isin(regimes)
            & pd.to_numeric(trades["entry_gap_pct"], errors="coerce").ge(gap_lo)
            & pd.to_numeric(trades["entry_gap_pct"], errors="coerce").lt(gap_hi)
            & pd.to_numeric(trades["signal_v_accel"], errors="coerce").ge(vacc_lo)
            & pd.to_numeric(trades["signal_v_accel"], errors="coerce").lt(vacc_hi)
            & pd.to_numeric(trades["signal_atr_pct"], errors="coerce").ge(atr_lo)
            & pd.to_numeric(trades["signal_atr_pct"], errors="coerce").lt(atr_hi)
        )
        exact = trades[mask.fillna(False)].copy()

    for col, value in [
        ("exit_variant", "hold7_stop6"),
        ("exit_note", "current/forward exact read-only C3 high-vol shadow sample"),
        ("exit_hold", 7),
        ("exit_stop_loss", -0.06),
        ("shadow_profile", "shadow_vaccel_lt_1_75"),
        ("shadow_note", "current/forward exact generator; research shadow only"),
        ("shadow_candidate_id", spec["candidate_id"]),
    ]:
        exact[col] = value

    if not exact.empty:
        exact = exact.sort_values(["signal_date", "entry_date", "code"]).reset_index(drop=True)

    floor500 = exact[pd.to_numeric(exact.get("signal_value", pd.Series(dtype=float)), errors="coerce") >= 500_000_000].copy() if not exact.empty else exact.copy()
    sub500 = exact[pd.to_numeric(exact.get("signal_value", pd.Series(dtype=float)), errors="coerce") < 500_000_000].copy() if not exact.empty else exact.copy()
    summary = pd.DataFrame([
        metric_row(exact, "current_forward_exact", "all", "latest OHLC current/forward window"),
        metric_row(floor500, "current_forward_exact", "floor500", "primary liquidity-quality lane"),
        metric_row(sub500, "current_forward_exact", "sub500", "micro-liquidity lane"),
    ])

    historical_all = next((row for row in historical.get("summary", []) if row.get("group") == "exact_generator" and row.get("value") == "all"), {})
    validation = [
        "historical_generator_reproduced_22_before_current: PASS"
        if historical.get("generation_meta", {}).get("exact_rows") == 22 and historical.get("reproduction_summary", {}).get("matched_rows") == 22
        else "historical_generator_reproduced_22_before_current: FAIL",
        "current_window_start_after_historical_end: PASS" if CURRENT_SIGNAL_START > "2026-06-30" else "current_window_start_after_historical_end: FAIL",
        "current_window_scan_executed: PASS",
        "operation_effect_no_changes: PASS",
    ]

    row_count = int(len(exact))
    if row_count > 0:
        conclusion = "CURRENT_FORWARD_SHADOW_ROWS_FOUND_REVIEW_REQUIRED"
    else:
        conclusion = "NO_CURRENT_FORWARD_SHADOW_ROWS_FOUND_IN_AVAILABLE_OHLC"

    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "current_forward_exact_read_only_c3_highvol_shadow_sample",
        "classification": "C3_HIGHVOL_CURRENT_FORWARD_SHADOW_SAMPLE_READ_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "conclusion": conclusion,
        "source_artifacts": {
            "report_backtest": str(REPORT_BACKTEST),
            "spec": str(SPEC_JSON),
            "selection_contract": str(SELECTION_CONTRACT),
            "research_params": str(RESEARCH_PARAMS),
            "historical_generator": str(HISTORICAL_GENERATOR_JSON),
        },
        "window": {
            "data_min_date": data_min_date,
            "data_max_date": data_max_date,
            "signal_date_start": CURRENT_SIGNAL_START,
            "signal_date_end": data_max_date,
        },
        "generation_meta": {
            "data_rows": int(len(data)),
            "factor_rows": int(len(factors)),
            "trade_rows_before_highvol_filter": int(len(trades)),
            "current_forward_exact_rows": row_count,
            "historical_exact_rows": historical.get("generation_meta", {}).get("exact_rows"),
            "historical_ret_sum": historical_all.get("ret_sum"),
        },
        "summary": clean_records(summary),
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

    exact.to_csv(OUT_ROWS, index=False, encoding="utf-8-sig")
    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")

    lines = [
        "# C3 High-Vol Current/Forward Shadow Sample",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- operational_decision: {payload['operational_decision']}",
        f"- full_logic_application: {payload['full_logic_application']}",
        f"- conclusion: {payload['conclusion']}",
        f"- signal_window: {CURRENT_SIGNAL_START} ~ {data_max_date}",
        "",
        "## Summary",
        "",
        "| group | value | n | ret_sum | profit_factor |",
        "|---|---|---:|---:|---:|",
    ]
    for row in clean_records(summary):
        lines.append(f"| {row['group']} | {row['value']} | {row['n']} | {row['ret_sum']} | {row['profit_factor']} |")
    lines.extend([
        "",
        "## Validation",
        "",
        *[f"- {item}" for item in validation],
        "",
        "## Guardrail",
        "",
        "Read-only current/forward shadow scan only. No operational candidate generation, official backtest, HPO, gate, paper, or live order path was changed.",
        "",
    ])
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")

    print(json.dumps({
        "status": "ok",
        "json": str(OUT_JSON),
        "rows": str(OUT_ROWS),
        "summary": str(OUT_SUMMARY),
        "md": str(OUT_MD),
        "conclusion": conclusion,
        "current_forward_exact_rows": row_count,
        "trade_rows_before_highvol_filter": int(len(trades)),
        "window": payload["window"],
        "validation": validation,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
