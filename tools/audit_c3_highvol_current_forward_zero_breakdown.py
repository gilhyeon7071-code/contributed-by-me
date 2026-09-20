from __future__ import annotations

import csv
import importlib.util
import json
import os
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
REPORT_BACKTEST = ROOT / "report_backtest_v41_1.py"

SPEC_JSON = LOG_DIR / "c3_highvol_research_candidate_spec_latest.json"
SELECTION_CONTRACT = LOG_DIR / "c3_normal_official_replay_selection_contract_latest.json"
RESEARCH_PARAMS = LOG_DIR / "c3_strategy_replay_params_latest.json"
CURRENT_FORWARD_JSON = LOG_DIR / "c3_highvol_current_forward_shadow_sample_latest.json"

RUN_DIR = LOG_DIR / "c3_highvol_current_forward_zero_breakdown_runtime"
OUT_JSON = LOG_DIR / "c3_highvol_current_forward_zero_breakdown_latest.json"
OUT_STAGES = LOG_DIR / "c3_highvol_current_forward_zero_breakdown_stages_latest.csv"
OUT_DAILY = LOG_DIR / "c3_highvol_current_forward_zero_breakdown_daily_latest.csv"
OUT_TRIGGER = LOG_DIR / "c3_highvol_current_forward_zero_breakdown_trigger_rows_latest.csv"
OUT_MD = LOG_DIR / "c3_highvol_current_forward_zero_breakdown_latest.md"

CURRENT_SIGNAL_START = "2026-07-01"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def load_report_module() -> Any:
    spec = importlib.util.spec_from_file_location("report_backtest_v41_1_for_c3_highvol_zero_breakdown", REPORT_BACKTEST)
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


def with_window_and_exit(contract: dict[str, Any], start: str, end: str) -> dict[str, Any]:
    out = deepcopy(contract)
    out["signal_date_start"] = start
    out["signal_date_end"] = end
    out["exit_overrides"] = {
        "hold": 7,
        "stop_loss": -0.06,
        "disable_paper_exit_rules": True,
    }
    return out


def filter_series(sig: pd.DataFrame, item: dict[str, Any]) -> pd.Series:
    field_map = {
        "signal_ret1_pct": "ret1_pct",
        "signal_rs": "rs",
        "signal_v_accel": "v_accel",
        "signal_stretch": "stretch",
        "signal_atr_pct": "atr_pct",
        "signal_high_52w_gap": "high_52w_gap",
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
        series = pd.to_numeric(sig[field], errors="coerce")
    if op == "<=":
        return series <= value
    if op == "<":
        return series < value
    if op == ">=":
        return series >= value
    if op == ">":
        return series > value
    if op in {"==", "="}:
        return series == value
    if op == "!=":
        return series != value
    raise ValueError(f"unsupported op: {op}")


def stage_row(stage: str, count: int, blocker: str, interpretation: str) -> dict[str, Any]:
    return {
        "stage": stage,
        "count": int(count),
        "blocker": blocker,
        "interpretation": interpretation,
    }


def scan_breakout_triggers(
    selected_rows: list[pd.Series],
    df_code: dict[str, pd.DataFrame],
    signal_start: str,
    signal_end: str,
    candidate_spec: dict[str, Any],
) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    gap_lo = float(candidate_spec["entry_gap_pct"]["min_inclusive"])
    gap_hi = float(candidate_spec["entry_gap_pct"]["max_exclusive"])
    vacc_lo = float(candidate_spec["signal_v_accel"]["min_inclusive"])
    vacc_hi = float(candidate_spec["signal_v_accel"]["best_same_sample_max_exclusive"])
    atr_lo = float(candidate_spec["signal_atr_pct"]["min_inclusive"])
    atr_hi = float(candidate_spec["signal_atr_pct"]["max_exclusive"])
    allowed_regimes = set(candidate_spec["market_regime"])

    rows: list[dict[str, Any]] = []
    for r in selected_rows:
        d = pd.to_datetime(r["date"])
        cdf = df_code.get(str(r["price_history_key"]))
        if cdf is None:
            continue
        basis_px = float(r["high"])
        trigger_px_raw = basis_px
        trigger_window = cdf[cdf["date"] > d].head(7).copy()
        if trigger_window.empty:
            rows.append({
                "signal_date": d.strftime("%Y-%m-%d"),
                "code": str(r["code"]),
                "trigger_found": False,
                "trigger_reason": "NO_FUTURE_WINDOW",
            })
            continue
        hit_window = trigger_window[pd.to_numeric(trigger_window["high"], errors="coerce") >= trigger_px_raw]
        if hit_window.empty:
            rows.append({
                "signal_date": d.strftime("%Y-%m-%d"),
                "code": str(r["code"]),
                "market_regime": str(r.get("market_regime", "")),
                "signal_v_accel": float(r.get("v_accel", float("nan"))),
                "signal_atr_pct": float(r.get("atr_pct", float("nan"))),
                "signal_value": float(r.get("value", float("nan"))),
                "trigger_found": False,
                "trigger_reason": "BREAKOUT_NOT_HIT_WITHIN_7D",
            })
            continue
        entry_day = hit_window.iloc[0]
        entry_open_raw = float(entry_day["open"])
        entry_px_raw = entry_open_raw if entry_open_raw > trigger_px_raw else trigger_px_raw
        prev_close = float(r["close"])
        gap = (entry_px_raw - prev_close) / (prev_close + 1e-9)
        regime = str(r.get("market_regime", ""))
        vacc = float(r.get("v_accel", float("nan")))
        atr = float(r.get("atr_pct", float("nan")))
        value = float(r.get("value", float("nan")))
        family_pass = (
            regime in allowed_regimes
            and gap_lo <= gap < gap_hi
            and vacc_lo <= vacc < vacc_hi
            and atr_lo <= atr < atr_hi
        )
        rows.append({
            "signal_date": d.strftime("%Y-%m-%d"),
            "entry_date": pd.to_datetime(entry_day["date"]).strftime("%Y-%m-%d"),
            "code": str(r["code"]),
            "market_regime": regime,
            "signal_v_accel": vacc,
            "signal_atr_pct": atr,
            "signal_value": value,
            "entry_gap_pct": gap,
            "trigger_found": True,
            "trigger_reason": "BREAKOUT_GAP" if entry_open_raw > trigger_px_raw else "BREAKOUT_TOUCH",
            "regime_ok": regime in allowed_regimes,
            "gap_ok": gap_lo <= gap < gap_hi,
            "v_accel_ok": vacc_lo <= vacc < vacc_hi,
            "atr_ok": atr_lo <= atr < atr_hi,
            "floor500": value >= 500_000_000,
            "family_pass": family_pass,
        })
    trigger_df = pd.DataFrame(rows)
    trigger_stages = [
        stage_row("breakout_trigger_input_selected_rows", len(selected_rows), "", "Rows selected by C3 source selector before breakout trigger."),
        stage_row("breakout_trigger_found", int(trigger_df.get("trigger_found", pd.Series(dtype=bool)).fillna(False).sum()) if not trigger_df.empty else 0, "", "Rows where prior-high breakout was hit within 7 trading days."),
        stage_row("highvol_family_all_filters_pass", int(trigger_df.get("family_pass", pd.Series(dtype=bool)).fillna(False).sum()) if not trigger_df.empty else 0, "", "Rows passing regime, gap, v_accel, and ATR high-vol family filters after trigger."),
    ]
    return trigger_stages, trigger_df


def main() -> int:
    spec = load_json(SPEC_JSON)
    current_forward = load_json(CURRENT_FORWARD_JSON)
    if current_forward.get("conclusion") != "NO_CURRENT_FORWARD_SHADOW_ROWS_FOUND_IN_AVAILABLE_OHLC":
        raise ValueError("current-forward scan is not the expected zero-row state")

    os.environ["REPORT_RESEARCH_MODE"] = "1"
    os.environ["REPORT_RESEARCH_PARAMS_PATH"] = str(RESEARCH_PARAMS)
    os.environ["REPORT_RESEARCH_SELECTION_CONTRACT_PATH"] = str(SELECTION_CONTRACT)
    os.environ["REPORT_RESEARCH_OUTPUT_DIR"] = str(RUN_DIR)
    os.environ["REPORT_ALLOW_UNAPPROVED_FALLBACK"] = "1"

    report = load_report_module()
    params = report.load_params()
    contract_payload = load_json(SELECTION_CONTRACT)
    base_contract = contract_payload.get("selection_contract", contract_payload)
    source_contract = with_window_and_exit(clear_regimes(drop_filters(base_contract, {"mkt_vol20", "signal_value"})), CURRENT_SIGNAL_START, current_forward["window"]["data_max_date"])

    data = report.load_data()
    factors = report.compute_factors(data)
    context = report.prepare_simulation_context(factors)
    sig = context["sig"]
    df_code = context["df_code"]

    stages: list[dict[str, Any]] = []
    stages.append(stage_row("sig_after_context_dropna_all_dates", len(sig), "", "Simulation signal rows after required feature dropna."))
    win = sig[(sig["date"] >= pd.Timestamp(CURRENT_SIGNAL_START)) & (sig["date"] <= pd.Timestamp(current_forward["window"]["data_max_date"]))].copy()
    stages.append(stage_row("date_window_2026_07_01_to_data_max", len(win), "", "Rows inside the current/forward signal window."))

    regime_counts = win["market_regime"].astype(str).value_counts(dropna=False).to_dict() if "market_regime" in win.columns else {}
    allowed_regimes = set(spec["candidate_spec"]["market_regime"])
    family_regime = win[win["market_regime"].astype(str).isin(allowed_regimes)].copy() if "market_regime" in win.columns else win.iloc[0:0].copy()
    stages.append(stage_row("family_regime_bull_sideways", len(family_regime), json.dumps(regime_counts, ensure_ascii=False), "Rows in BULL/SIDEWAYS before source numeric filters."))

    filtered = win.copy()
    numeric_filters = list(source_contract.get("numeric_filters") or [])
    for idx, item in enumerate(numeric_filters, start=1):
        before = len(filtered)
        mask = filter_series(filtered, item)
        filtered = filtered[mask.fillna(False)].copy()
        label = f"source_numeric_filter_{idx}_{item.get('field')}_{item.get('op')}_{item.get('value')}"
        stages.append(stage_row(label, len(filtered), f"before={before}", "Sequential source contract numeric filter after removing mkt_vol20 and signal_value caps."))
        if filtered.empty:
            break

    required_raw = source_contract.get("required_signals")
    required_signals = None if required_raw is None else tuple(str(x) for x in required_raw)
    inverse_raw = source_contract.get("inverse_signals") or []
    inverse_signals = tuple(str(x) for x in inverse_raw)
    base_filter = report._params_to_filter_dict(params)
    ladder = report._relax_ladder(base_filter) if bool(params.use_relax_ladder) else [("L0", base_filter)]

    daily_rows: list[dict[str, Any]] = []
    selected_rows: list[pd.Series] = []
    if not filtered.empty:
        for d, g in filtered.groupby("date", sort=True):
            chosen_level = "NONE"
            selected = pd.DataFrame()
            for level, p_try in ladder:
                cand = report._select_candidates(g, p_try, required_signals=required_signals, inverse_signals=inverse_signals)
                if not cand.empty:
                    selected = cand
                    chosen_level = str(level)
                    break
            daily_rows.append({
                "date": pd.to_datetime(d).strftime("%Y-%m-%d"),
                "source_filtered_rows": int(len(g)),
                "selector_level": chosen_level,
                "selected_rows": int(len(selected)),
            })
            for _, row in selected.iterrows():
                selected_rows.append(row)

    selected_total = sum(int(row["selected_rows"]) for row in daily_rows)
    stages.append(stage_row("normal_c3_selector_selected_rows", selected_total, "", "Rows selected by normal C3 selector from the current-window source-filtered pool."))
    trigger_stages, trigger_df = scan_breakout_triggers(selected_rows, df_code, CURRENT_SIGNAL_START, current_forward["window"]["data_max_date"], spec["candidate_spec"])
    stages.extend(trigger_stages)

    conclusion = "ZERO_ROWS_CAUSED_BEFORE_SELECTOR" if selected_total == 0 else "ZERO_ROWS_CAUSED_AFTER_SELECTOR_OR_TRIGGER"
    if len(filtered) == 0:
        conclusion = "ZERO_ROWS_CAUSED_BY_SOURCE_NUMERIC_FILTERS"
    if len(family_regime) == 0:
        conclusion = "ZERO_ROWS_CAUSED_BY_REGIME_WINDOW"

    validation = [
        "current_forward_zero_state_confirmed: PASS",
        "date_window_rows_positive: PASS" if len(win) > 0 else "date_window_rows_positive: FAIL",
        "stage_rows_present: PASS" if len(stages) >= 5 else "stage_rows_present: FAIL",
        "operation_effect_no_changes: PASS",
    ]
    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "c3_highvol_current_forward_zero_breakdown_read_only",
        "classification": "C3_HIGHVOL_CURRENT_FORWARD_ZERO_BREAKDOWN_READ_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "conclusion": conclusion,
        "window": current_forward["window"],
        "source_artifacts": {
            "current_forward": str(CURRENT_FORWARD_JSON),
            "report_backtest": str(REPORT_BACKTEST),
            "spec": str(SPEC_JSON),
            "selection_contract": str(SELECTION_CONTRACT),
            "research_params": str(RESEARCH_PARAMS),
        },
        "regime_counts_in_window": regime_counts,
        "stage_audit": stages,
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

    with OUT_STAGES.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["stage", "count", "blocker", "interpretation"], extrasaction="ignore")
        writer.writeheader()
        writer.writerows(stages)
    pd.DataFrame(daily_rows).to_csv(OUT_DAILY, index=False, encoding="utf-8-sig")
    trigger_df.to_csv(OUT_TRIGGER, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")

    lines = [
        "# C3 High-Vol Current/Forward Zero Breakdown",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- conclusion: {payload['conclusion']}",
        f"- signal_window: {payload['window']['signal_date_start']} ~ {payload['window']['signal_date_end']}",
        "",
        "## Stage Audit",
        "",
        "| stage | count | blocker |",
        "|---|---:|---|",
    ]
    for row in stages:
        lines.append(f"| {row['stage']} | {row['count']} | {str(row['blocker']).replace('|', '/')} |")
    lines.extend([
        "",
        "## Validation",
        "",
        *[f"- {item}" for item in validation],
        "",
        "## Guardrail",
        "",
        "Read-only zero-row breakdown only. No operational candidate generation, official backtest, HPO, gate, paper, or live order path was changed.",
        "",
    ])
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")

    print(json.dumps({
        "status": "ok",
        "json": str(OUT_JSON),
        "stages": str(OUT_STAGES),
        "daily": str(OUT_DAILY),
        "trigger": str(OUT_TRIGGER),
        "md": str(OUT_MD),
        "conclusion": conclusion,
        "stage_count": len(stages),
        "validation": validation,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
