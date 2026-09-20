from __future__ import annotations

import csv
import importlib.util
import json
import math
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(r"E:\1_Data")
LOG_DIR = ROOT / "2_Logs"
REPORT_BACKTEST = ROOT / "report_backtest_v41_1.py"
SELECTION_CONTRACT = LOG_DIR / "c3_normal_official_replay_selection_contract_latest.json"
RESEARCH_PARAMS = LOG_DIR / "c3_strategy_replay_params_latest.json"

LATEST_JSON = LOG_DIR / "c3_2026h1_zero_rows_diagnosis_latest.json"
LATEST_CSV = LOG_DIR / "c3_2026h1_zero_rows_diagnosis_latest.csv"
LATEST_MD = LOG_DIR / "c3_2026h1_zero_rows_diagnosis_latest.md"
RUN_DIR = LOG_DIR / "c3_2026h1_zero_rows_diagnosis_runtime"

STATUS = "READ_ONLY_C3_2026H1_ZERO_ROWS_DIAGNOSIS_NOT_OPERATIONAL"


def _load_report_module() -> Any:
    spec = importlib.util.spec_from_file_location("report_backtest_v41_1_for_c3_zero_diag", REPORT_BACKTEST)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {REPORT_BACKTEST}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _date_mask(df: pd.DataFrame, start: str, end: str) -> pd.Series:
    return (df["date"] >= pd.Timestamp(start)) & (df["date"] <= pd.Timestamp(end))


def _count_row(stage: str, df: pd.DataFrame, period: str, note: str = "") -> dict[str, Any]:
    dates = int(df["date"].nunique()) if "date" in df.columns and not df.empty else 0
    codes = int(df["code"].nunique()) if "code" in df.columns and not df.empty else 0
    return {"period": period, "stage": stage, "rows": int(len(df)), "dates": dates, "codes": codes, "note": note}


def _apply_numeric_filter(sig: pd.DataFrame, item: dict[str, Any]) -> tuple[pd.DataFrame, str]:
    field_map = {
        "signal_rs": "rs",
        "signal_v_accel": "v_accel",
        "signal_stretch": "stretch",
        "signal_atr_pct": "atr_pct",
        "signal_rsi14": "rsi14",
        "signal_vol_close_corr20": "vol_close_corr20",
        "signal_high_52w_gap": "high_52w_gap",
        "signal_listing_days": "listing_days",
        "signal_ret1_pct": "ret1_pct",
        "signal_value": "value",
        "mkt_ret20": "mkt_ret20",
        "mkt_ret60": "mkt_ret60",
        "mkt_vol20": "mkt_vol20",
        "mkt_breadth": "mkt_breadth",
    }
    raw_field = str(item.get("field", "")).strip()
    op = str(item.get("op", "")).strip()
    value = float(item.get("value"))
    if raw_field == "entry_gap_pct":
        series = (pd.to_numeric(sig["n_open"], errors="coerce") - pd.to_numeric(sig["close"], errors="coerce")) / (
            pd.to_numeric(sig["close"], errors="coerce") + 1e-9
        )
    else:
        field = field_map.get(raw_field, raw_field)
        if field not in sig.columns:
            raise ValueError(f"unknown numeric filter field: {raw_field}")
        series = pd.to_numeric(sig[field], errors="coerce")
    if op == "<=":
        mask = series <= value
    elif op == ">=":
        mask = series >= value
    elif op == "<":
        mask = series < value
    elif op == ">":
        mask = series > value
    elif op == "==":
        mask = series == value
    elif op == "!=":
        mask = series != value
    else:
        raise ValueError(f"unsupported op: {op}")
    before = len(sig)
    out = sig[mask.fillna(False)].copy()
    return out, f"{raw_field} {op} {value}; removed={before - len(out)}"


def _trigger_hit_count(report: Any, selected: pd.DataFrame, context: dict[str, Any], contract: dict[str, Any]) -> tuple[int, int]:
    if selected.empty:
        return 0, 0
    entry_trigger = contract.get("entry_trigger") if isinstance(contract.get("entry_trigger"), dict) else {}
    trigger_window_days = max(1, int(entry_trigger.get("lookahead_days", 5) or 5))
    trigger_basis = str(entry_trigger.get("price_basis", "signal_high")).strip().lower()
    buffer_pct = float(entry_trigger.get("buffer_pct", 0.0) or 0.0)
    df_code = context["df_code"]
    evaluated = 0
    hits = 0
    for _, row in selected.iterrows():
        history_key = str(row.get("price_history_key", ""))
        cdf = df_code.get(history_key)
        if cdf is None or cdf.empty:
            continue
        d = pd.Timestamp(row["date"])
        if trigger_basis in {"signal_close", "close"}:
            basis_px = float(row["close"])
        elif trigger_basis in {"signal_open", "open"}:
            basis_px = float(row["open"])
        else:
            basis_px = float(row["high"])
        trigger_px = basis_px * (1.0 + buffer_pct)
        trigger_window = cdf[cdf["date"] > d].head(trigger_window_days).copy()
        if trigger_window.empty:
            continue
        evaluated += 1
        high = pd.to_numeric(trigger_window["high"], errors="coerce")
        if bool((high >= trigger_px).fillna(False).any()):
            hits += 1
    return evaluated, hits


def _diagnose_period(
    report: Any,
    sig_all: pd.DataFrame,
    context: dict[str, Any],
    params: Any,
    contract: dict[str, Any],
    period_name: str,
    start: str,
    end: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    sig = sig_all[_date_mask(sig_all, start, end)].copy()
    rows.append(_count_row("01_period_raw_sig", sig, period_name, f"{start}..{end}"))

    allowed_regimes = {str(x).upper() for x in contract.get("regimes", []) if str(x).strip()}
    if allowed_regimes:
        before = len(sig)
        sig = sig[sig["market_regime"].astype(str).str.upper().isin(allowed_regimes)].copy()
        rows.append(_count_row("02_regime_filter", sig, period_name, f"allowed={sorted(allowed_regimes)}; removed={before-len(sig)}"))

    numeric_filters = list(contract.get("numeric_filters") or [])
    for idx, item in enumerate(numeric_filters, start=1):
        sig, note = _apply_numeric_filter(sig, item)
        rows.append(_count_row(f"03_numeric_{idx:02d}", sig, period_name, note))
        if sig.empty:
            break

    base_filter = report._params_to_filter_dict(params)
    required = tuple(str(x) for x in contract.get("required_signals") or [])
    selected_parts: list[pd.DataFrame] = []
    if not sig.empty:
        for _, group in sig.groupby("date", sort=True):
            cand = report._select_candidates(group, base_filter, required_signals=required, inverse_signals=tuple())
            if not cand.empty:
                selected_parts.append(cand)
    selected = pd.concat(selected_parts, ignore_index=False) if selected_parts else pd.DataFrame(columns=sig.columns)
    rows.append(_count_row("04_core_signal_select", selected, period_name, f"required={list(required)}"))

    evaluated, hits = _trigger_hit_count(report, selected, context, contract)
    trigger_df = selected.head(hits).copy() if hits else selected.iloc[0:0].copy()
    rows.append(_count_row("05_breakout_trigger_hit_proxy", trigger_df, period_name, f"evaluated={evaluated}; hits={hits}"))

    summary = {
        "period": period_name,
        "start": start,
        "end": end,
        "raw_sig_rows": int(rows[0]["rows"]),
        "post_regime_rows": int(rows[1]["rows"]) if len(rows) > 1 else int(rows[0]["rows"]),
        "post_numeric_rows": int(rows[-3]["rows"]) if len(rows) >= 3 and str(rows[-3]["stage"]).startswith("03_") else int(len(sig)),
        "post_core_signal_rows": int(len(selected)),
        "trigger_evaluated_rows": int(evaluated),
        "trigger_hit_rows": int(hits),
        "zero_cause": "",
    }
    if summary["raw_sig_rows"] <= 0:
        summary["zero_cause"] = "no_signal_rows_in_period"
    elif summary["post_regime_rows"] <= 0:
        summary["zero_cause"] = "regime_filter_removed_all"
    elif summary["post_numeric_rows"] <= 0:
        last_zero = next((r for r in reversed(rows) if str(r["stage"]).startswith("03_") and int(r["rows"]) == 0), None)
        summary["zero_cause"] = f"numeric_filter_removed_all:{last_zero['note'] if last_zero else ''}"
    elif summary["post_core_signal_rows"] <= 0:
        summary["zero_cause"] = "core_signal_selection_removed_all"
    elif summary["trigger_hit_rows"] <= 0:
        summary["zero_cause"] = "breakout_trigger_not_hit"
    else:
        summary["zero_cause"] = "not_zero"
    return rows, summary


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = ["period", "stage", "rows", "dates", "codes", "note"]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# C3 2026H1 Zero Rows Diagnosis",
        "",
        f"- status: `{payload['status']}`",
        f"- created_at: `{payload['created_at']}`",
        f"- conclusion: `{payload['conclusion']}`",
        f"- full_logic_application: `{payload['operation_effect']['full_logic_application']}`",
        "",
        "## Period Summary",
        "",
        "| period | raw | post_regime | post_numeric | core_signal | trigger_hits | zero_cause |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in payload["period_summaries"]:
        lines.append(
            "| {period} | {raw_sig_rows} | {post_regime_rows} | {post_numeric_rows} | {post_core_signal_rows} | {trigger_hit_rows} | {zero_cause} |".format(
                **row
            )
        )
    lines.extend(
        [
            "",
            "## Stage Counts",
            "",
            "| period | stage | rows | dates | codes | note |",
            "|---|---|---:|---:|---:|---|",
        ]
    )
    for row in payload["stage_counts"]:
        lines.append(
            "| {period} | {stage} | {rows} | {dates} | {codes} | {note} |".format(**row)
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Read-only diagnosis only.",
            "- Reuses report_backtest_v41_1.py data loading, factor computation, regime assignment, numeric filters, and core signal selection.",
            "- Does not change candidate generation, official backtest, HPO, paper/live orders, gate, or stable parameters.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    os.environ["REPORT_RESEARCH_MODE"] = "1"
    os.environ["REPORT_RESEARCH_PARAMS_PATH"] = str(RESEARCH_PARAMS)
    os.environ["REPORT_RESEARCH_SELECTION_CONTRACT_PATH"] = str(SELECTION_CONTRACT)
    os.environ["REPORT_RESEARCH_OUTPUT_DIR"] = str(RUN_DIR)
    os.environ["REPORT_ALLOW_UNAPPROVED_FALLBACK"] = "1"

    report = _load_report_module()
    params = report.load_params()
    contract = report._load_research_selection_contract()
    if not isinstance(contract, dict):
        raise RuntimeError("selection contract was not loaded")

    data = report.load_data()
    factors = report.compute_factors(data)
    context = report.prepare_simulation_context(factors)
    sig_all = context["sig"].copy()

    periods = [
        ("observed_all_contract_window", "2024-01-01", "2026-06-30"),
        ("reference_2025H2", "2025-07-01", "2025-12-31"),
        ("target_2026H1", "2026-01-01", "2026-06-30"),
    ]
    stage_counts: list[dict[str, Any]] = []
    period_summaries: list[dict[str, Any]] = []
    for name, start, end in periods:
        rows, summary = _diagnose_period(report, sig_all, context, params, contract, name, start, end)
        stage_counts.extend(rows)
        period_summaries.append(summary)

    target = next(x for x in period_summaries if x["period"] == "target_2026H1")
    if target["raw_sig_rows"] > 0 and target["post_regime_rows"] == 0:
        conclusion = "2026H1 has input data, but the C3 TRANSITION regime filter removes all rows before numeric filters or entry trigger are reached."
    elif str(target["zero_cause"]).startswith("numeric_filter_removed_all"):
        conclusion = f"2026H1 reaches the numeric filters but is removed by {target['zero_cause']}."
    elif target["trigger_hit_rows"] == 0 and target["post_core_signal_rows"] > 0:
        conclusion = "2026H1 has selected signals but no breakout trigger hit."
    else:
        conclusion = f"2026H1 zero-row cause: {target['zero_cause']}."

    payload = {
        "status": STATUS,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_artifacts": {
            "report_backtest": str(REPORT_BACKTEST),
            "selection_contract": str(SELECTION_CONTRACT),
            "research_params": str(RESEARCH_PARAMS),
        },
        "selection_contract": contract,
        "data_rows": int(len(data)),
        "factor_rows": int(len(factors)),
        "signal_rows": int(len(sig_all)),
        "period_summaries": period_summaries,
        "stage_counts": stage_counts,
        "conclusion": conclusion,
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_live_order_changed": False,
            "policy_changed": False,
            "full_logic_application": "NOT_APPLIED",
        },
        "next_required": [
            "If C3 remains tied to TRANSITION only, 2026H1 should be treated as out-of-scope rather than failed performance.",
            "If 2026H1 trading is required, test whether the C3 proxy should allow another pre-entry regime bucket without using post-replay labels.",
        ],
    }

    LATEST_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_csv(LATEST_CSV, stage_counts)
    _write_md(LATEST_MD, payload)
    print(json.dumps({"status": STATUS, "json": str(LATEST_JSON), "csv": str(LATEST_CSV), "md": str(LATEST_MD)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
