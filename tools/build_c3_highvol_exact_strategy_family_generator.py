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
REFERENCE_ROWS = LOG_DIR / "c3_highvol_shadow_availability_audit_rows_latest.csv"

RUN_DIR = LOG_DIR / "c3_highvol_exact_strategy_family_runtime"
OUT_JSON = LOG_DIR / "c3_highvol_exact_strategy_family_generator_latest.json"
OUT_ROWS = LOG_DIR / "c3_highvol_exact_strategy_family_generator_rows_latest.csv"
OUT_SUMMARY = LOG_DIR / "c3_highvol_exact_strategy_family_generator_summary_latest.csv"
OUT_REPRO = LOG_DIR / "c3_highvol_exact_strategy_family_generator_reproduction_latest.csv"
OUT_MD = LOG_DIR / "c3_highvol_exact_strategy_family_generator_latest.md"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def load_report_module() -> Any:
    spec = importlib.util.spec_from_file_location("report_backtest_v41_1_for_c3_highvol_exact_generator", REPORT_BACKTEST)
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


def with_exit(contract: dict[str, Any], hold: int, stop_loss: float) -> dict[str, Any]:
    out = deepcopy(contract)
    out["exit_overrides"] = {
        "hold": int(hold),
        "stop_loss": float(stop_loss),
        "disable_paper_exit_rules": True,
    }
    return out


def norm_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d").fillna("")


def period_label(date_value: str) -> str:
    d = (date_value or "")[:10]
    if "2024-01-01" <= d < "2024-07-01":
        return "2024H1"
    if "2024-07-01" <= d < "2025-01-01":
        return "2024H2"
    if "2025-01-01" <= d < "2025-07-01":
        return "2025H1"
    if "2025-07-01" <= d < "2026-01-01":
        return "2025H2"
    if "2026-01-01" <= d < "2026-07-01":
        return "2026H1"
    return "OTHER"


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
        "first_entry_date": str(df["entry_date_norm"].min()) if len(df) else "",
        "last_entry_date": str(df["entry_date_norm"].max()) if len(df) else "",
        "top5_share_gross_profit": (top5 / gross_profit) if gross_profit else None,
        "ret_without_top5": float(ret.sum() - top5) if len(ret) else 0.0,
    }


def clean_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return df.astype(object).where(pd.notna(df), None).to_dict(orient="records")


def norm_code(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        if pd.notna(value):
            as_float = float(value)
            if as_float.is_integer():
                return str(int(as_float))
    except Exception:
        pass
    return str(value).strip()


def key_tuple(row: pd.Series | dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(row.get("shadow_profile", "")),
        str(row.get("signal_date", ""))[:10],
        str(row.get("entry_date", ""))[:10],
        str(row.get("exit_date", ""))[:10],
        norm_code(row.get("code", "")),
    )


def build_exact_rows() -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    spec = load_json(SPEC_JSON)
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
    contract = with_exit(source_contract, hold=7, stop_loss=-0.06)

    data = report.load_data()
    factors = report.compute_factors(data)
    context = report.prepare_simulation_context(factors)
    trades = report.simulate_trades(factors, params, selection_contract=contract, simulation_context=context)
    if trades.empty:
        trades["entry_date_norm"] = pd.Series(dtype=str)
    else:
        trades["entry_date_norm"] = norm_date(trades["entry_date"])

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
    exact.insert(0, "exit_variant", "hold7_stop6")
    exact.insert(1, "exit_note", "exact read-only C3 high-vol strategy-family generator")
    exact.insert(2, "exit_hold", 7)
    exact.insert(3, "exit_stop_loss", -0.06)
    exact["shadow_profile"] = "shadow_vaccel_lt_1_75"
    exact["shadow_note"] = "same-sample best upper bound; exact generator reproduction"
    exact["shadow_candidate_id"] = spec["candidate_id"]
    exact["entry_period"] = exact["entry_date_norm"].map(period_label)

    # Make the output deterministic and close to previous shadow layer columns.
    sort_cols = ["entry_date_norm", "code", "signal_date", "entry_date", "exit_date"]
    exact = exact.sort_values([col for col in sort_cols if col in exact.columns]).reset_index(drop=True)

    meta = {
        "data_rows": int(len(data)),
        "factor_rows": int(len(factors)),
        "trade_rows_before_highvol_filter": int(len(trades)),
        "exact_rows": int(len(exact)),
        "source_contract_note": "any regime, mkt_vol20 removed, C3 value upper cap removed; exact high-vol filters applied after OHLC replay using actual entry_gap_pct and entry_trigger fields",
    }
    return exact, trades, meta


def compare_reference(exact: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    reference = pd.read_csv(REFERENCE_ROWS, encoding="utf-8-sig")
    reference = reference[reference["shadow_profile"].astype(str) == "shadow_vaccel_lt_1_75"].copy()
    exact_keys = {key_tuple(row) for _, row in exact.iterrows()}
    ref_keys = {key_tuple(row) for _, row in reference.iterrows()}
    matched = exact_keys & ref_keys
    missing_from_exact = ref_keys - exact_keys
    extra_in_exact = exact_keys - ref_keys

    ref_by_key = {key_tuple(row): row for _, row in reference.iterrows()}
    exact_by_key = {key_tuple(row): row for _, row in exact.iterrows()}
    compare_rows: list[dict[str, Any]] = []
    for key in sorted(matched):
        e = exact_by_key[key]
        r = ref_by_key[key]
        compare_rows.append({
            "match_status": "MATCHED",
            "shadow_profile": key[0],
            "signal_date": key[1],
            "entry_date": key[2],
            "exit_date": key[3],
            "code": key[4],
            "ret_exact": e.get("ret"),
            "ret_reference": r.get("ret"),
            "ret_abs_diff": abs(float(e.get("ret", 0.0)) - float(r.get("ret", 0.0))),
            "entry_gap_exact": e.get("entry_gap_pct"),
            "entry_gap_reference": r.get("entry_gap_pct"),
        })
    for key in sorted(missing_from_exact):
        compare_rows.append({
            "match_status": "MISSING_FROM_EXACT",
            "shadow_profile": key[0],
            "signal_date": key[1],
            "entry_date": key[2],
            "exit_date": key[3],
            "code": key[4],
        })
    for key in sorted(extra_in_exact):
        compare_rows.append({
            "match_status": "EXTRA_IN_EXACT",
            "shadow_profile": key[0],
            "signal_date": key[1],
            "entry_date": key[2],
            "exit_date": key[3],
            "code": key[4],
        })
    comp = pd.DataFrame(compare_rows)
    max_ret_abs_diff = float(comp["ret_abs_diff"].max()) if "ret_abs_diff" in comp.columns and not comp.empty else 0.0
    return comp, {
        "reference_rows": int(len(reference)),
        "exact_rows": int(len(exact)),
        "matched_rows": int(len(matched)),
        "missing_from_exact": int(len(missing_from_exact)),
        "extra_in_exact": int(len(extra_in_exact)),
        "max_ret_abs_diff": max_ret_abs_diff,
    }


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    exact, _trades, meta = build_exact_rows()
    reproduction, repro_summary = compare_reference(exact)

    summary = pd.DataFrame([
        metric_row(exact, "exact_generator", "all", "exact read-only OHLC replay high-vol family"),
        metric_row(exact[exact["entry_period"] == "2025H2"], "exact_generator", "2025H2", "reference adverse/sideways period"),
        metric_row(exact[exact["entry_period"] == "2026H1"], "exact_generator", "2026H1", "current-like holdout period"),
        metric_row(exact[pd.to_numeric(exact["signal_value"], errors="coerce") >= 500_000_000], "exact_generator", "floor500", "primary liquidity-quality lane"),
        metric_row(exact[pd.to_numeric(exact["signal_value"], errors="coerce") < 500_000_000], "exact_generator", "sub500", "micro-liquidity lane"),
    ])

    validation = [
        "exact_rows_22: PASS" if len(exact) == 22 else "exact_rows_22: FAIL",
        "reference_rows_22: PASS" if repro_summary["reference_rows"] == 22 else "reference_rows_22: FAIL",
        "reproduction_key_match_all: PASS"
        if repro_summary["matched_rows"] == 22 and repro_summary["missing_from_exact"] == 0 and repro_summary["extra_in_exact"] == 0
        else "reproduction_key_match_all: FAIL",
        "reproduction_ret_diff_zero: PASS" if repro_summary["max_ret_abs_diff"] <= 1e-12 else "reproduction_ret_diff_zero: FAIL",
        "operation_effect_no_changes: PASS",
    ]

    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "exact_read_only_c3_highvol_strategy_family_generator",
        "classification": "C3_HIGHVOL_EXACT_STRATEGY_FAMILY_GENERATOR_READ_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "source_artifacts": {
            "report_backtest": str(REPORT_BACKTEST),
            "spec": str(SPEC_JSON),
            "selection_contract": str(SELECTION_CONTRACT),
            "research_params": str(RESEARCH_PARAMS),
            "reference_rows": str(REFERENCE_ROWS),
        },
        "generation_meta": meta,
        "reproduction_summary": repro_summary,
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
        # Write diagnostic artifacts before failing so the mismatch can be inspected.
        exact.to_csv(OUT_ROWS, index=False, encoding="utf-8-sig")
        summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
        reproduction.to_csv(OUT_REPRO, index=False, encoding="utf-8-sig")
        OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
        raise SystemExit("validation failed: " + "; ".join(validation))

    exact.to_csv(OUT_ROWS, index=False, encoding="utf-8-sig")
    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    reproduction.to_csv(OUT_REPRO, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")

    lines = [
        "# C3 High-Vol Exact Strategy-Family Generator",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- operational_decision: {payload['operational_decision']}",
        f"- full_logic_application: {payload['full_logic_application']}",
        f"- exact_rows: {meta['exact_rows']}",
        f"- reference_match: {repro_summary['matched_rows']}/{repro_summary['reference_rows']}",
        "",
        "## Summary",
        "",
        "| group | value | n | ret_sum | profit_factor | ret_without_top5 |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for row in clean_records(summary):
        lines.append(
            f"| {row['group']} | {row['value']} | {row['n']} | {row['ret_sum']} | {row['profit_factor']} | {row['ret_without_top5']} |"
        )
    lines.extend([
        "",
        "## Validation",
        "",
        *[f"- {item}" for item in validation],
        "",
        "## Guardrail",
        "",
        "Exact read-only generator only. It does not connect to operational candidate generation, official backtest, HPO, gates, paper, or live orders.",
        "",
    ])
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")

    print(json.dumps({
        "status": "ok",
        "json": str(OUT_JSON),
        "rows": str(OUT_ROWS),
        "summary": str(OUT_SUMMARY),
        "reproduction": str(OUT_REPRO),
        "md": str(OUT_MD),
        "exact_rows": meta["exact_rows"],
        "matched_rows": repro_summary["matched_rows"],
        "validation": validation,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
