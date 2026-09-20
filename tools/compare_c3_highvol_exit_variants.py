from __future__ import annotations

import importlib.util
import json
import os
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable

import pandas as pd


ROOT = Path(r"E:\1_Data")
LOG_DIR = ROOT / "2_Logs"
REPORT_BACKTEST = ROOT / "report_backtest_v41_1.py"
SELECTION_CONTRACT = LOG_DIR / "c3_normal_official_replay_selection_contract_latest.json"
RESEARCH_PARAMS = LOG_DIR / "c3_strategy_replay_params_latest.json"

OUT_JSON = LOG_DIR / "c3_highvol_exit_variant_compare_latest.json"
OUT_SUMMARY = LOG_DIR / "c3_highvol_exit_variant_compare_summary_latest.csv"
OUT_TRADES = LOG_DIR / "c3_highvol_exit_variant_compare_trades_latest.csv"
OUT_MD = LOG_DIR / "c3_highvol_exit_variant_compare_latest.md"
RUN_DIR = LOG_DIR / "c3_highvol_exit_variant_runtime"

STATUS = "READ_ONLY_C3_HIGHVOL_EXIT_VARIANT_COMPARE_NOT_OPERATIONAL"


def _load_report_module() -> Any:
    spec = importlib.util.spec_from_file_location("report_backtest_v41_1_for_c3_highvol_exit", REPORT_BACKTEST)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {REPORT_BACKTEST}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _drop_filters(contract: dict[str, Any], fields: set[str]) -> dict[str, Any]:
    out = deepcopy(contract)
    out["numeric_filters"] = [
        dict(item)
        for item in list(out.get("numeric_filters") or [])
        if str(item.get("field", "")).strip() not in fields
    ]
    return out


def _clear_regimes(contract: dict[str, Any]) -> dict[str, Any]:
    out = deepcopy(contract)
    out["regimes"] = []
    return out


def _with_exit(contract: dict[str, Any], hold: int, stop_loss: float | None) -> dict[str, Any]:
    out = deepcopy(contract)
    out["exit_overrides"] = {
        "hold": int(hold),
        "stop_loss": stop_loss,
        "disable_paper_exit_rules": True,
    }
    return out


def _norm_trade_dates(trades: pd.DataFrame) -> pd.DataFrame:
    out = trades.copy()
    if out.empty:
        out["entry_date_norm"] = pd.Series(dtype=str)
        return out
    out["entry_date_norm"] = pd.to_datetime(out["entry_date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    return out


def _profit_factor(ret: Iterable[float]) -> float | None:
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


def _segment_mask(df: pd.DataFrame, segment: str) -> pd.Series:
    dt = pd.to_datetime(df["entry_date_norm"], errors="coerce")
    if segment == "all":
        return pd.Series(True, index=df.index)
    if segment == "2025H1":
        return (dt >= "2025-01-01") & (dt <= "2025-06-30")
    if segment == "2025H2":
        return (dt >= "2025-07-01") & (dt <= "2025-12-31")
    if segment == "pre_2026":
        return dt < "2026-01-01"
    if segment == "2026H1":
        return (dt >= "2026-01-01") & (dt <= "2026-06-30")
    if segment == "exclude_2026H1":
        return ~((dt >= "2026-01-01") & (dt <= "2026-06-30"))
    raise ValueError(segment)


def _rule_mask(df: pd.DataFrame, rule: str) -> pd.Series:
    regime = df["market_regime"].astype(str).isin(["BULL", "SIDEWAYS"])
    gap = pd.to_numeric(df["entry_gap_pct"], errors="coerce")
    vaccel = pd.to_numeric(df["signal_v_accel"], errors="coerce")
    atr = pd.to_numeric(df["signal_atr_pct"], errors="coerce")
    if rule == "hv_profile_atr_3_4":
        return (
            regime
            & gap.ge(0.0147299509)
            & gap.lt(0.03)
            & vaccel.ge(1.255833911)
            & vaccel.lt(1.5)
            & atr.ge(0.03)
            & atr.lt(0.04)
        )
    if rule == "hv_profile_mid":
        return regime & gap.ge(0.0147299509) & gap.lt(0.03) & vaccel.ge(1.255833911) & vaccel.lt(1.5)
    raise ValueError(rule)


def _summarize(frame: pd.DataFrame, exit_variant: str, rule: str, segment: str, note: str) -> dict[str, Any]:
    ret = pd.to_numeric(frame.get("ret", pd.Series(dtype=float)), errors="coerce").dropna()
    return {
        "exit_variant": exit_variant,
        "rule": rule,
        "segment": segment,
        "n": int(len(frame)),
        "win_n": int((ret > 0).sum()) if len(ret) else 0,
        "loss_n": int((ret < 0).sum()) if len(ret) else 0,
        "stop_n": int((frame["exit_reason"].astype(str).str.upper() == "STOP").sum()) if len(frame) else 0,
        "stop_gap_n": int((frame["exit_reason"].astype(str).str.upper() == "STOP_GAP").sum()) if len(frame) else 0,
        "time_n": int((frame["exit_reason"].astype(str).str.upper() == "TIME").sum()) if len(frame) else 0,
        "ret_sum": float(ret.sum()) if len(ret) else 0.0,
        "ret_mean": float(ret.mean()) if len(ret) else None,
        "profit_factor": _profit_factor(ret.tolist()),
        "first_entry_date": str(frame["entry_date_norm"].min()) if len(frame) else None,
        "last_entry_date": str(frame["entry_date_norm"].max()) if len(frame) else None,
        "unique_codes": int(frame["code"].nunique()) if len(frame) else 0,
        "note": note,
    }


def _clean_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return df.astype(object).where(pd.notna(df), None).to_dict(orient="records")


def _classify(summary: pd.DataFrame) -> dict[str, Any]:
    focus = summary[
        (summary["rule"] == "hv_profile_mid")
        & (summary["segment"].isin(["all", "2025H2", "2026H1"]))
    ].copy()
    all_rows = focus[focus["segment"] == "all"].sort_values("ret_sum", ascending=False)
    reasons: list[str] = []
    best = {}
    if not all_rows.empty:
        best = all_rows.iloc[0].to_dict()
        reasons.append(f"best_all_variant:{best['exit_variant']}")
        if int(best["n"]) < 20:
            reasons.append("best_variant_sample_under_20")
        if float(best["ret_sum"]) > 0 and (best["profit_factor"] or 0) > 1:
            reasons.append("best_variant_all_positive")
    for _, row in focus[focus["segment"] == "2025H2"].iterrows():
        if float(row["ret_sum"]) < 0:
            reasons.append(f"{row['exit_variant']}_2025H2_negative")
    h1 = focus[focus["segment"] == "2026H1"]
    if not h1.empty and int(h1["n"].max()) < 5:
        reasons.append("2026H1_sample_too_small")
    return {
        "operational_decision": "NOT_APPROVED",
        "research_classification": "C3_HIGHVOL_EXIT_VARIANT_NO_APPROVED_FIX",
        "confidence": "LOW_TO_MEDIUM_RESEARCH_ONLY",
        "full_logic_application": "NOT_APPLIED",
        "best_all_variant": best.get("exit_variant"),
        "reason": reasons or ["insufficient_evidence"],
    }


def main() -> int:
    os.environ["REPORT_RESEARCH_MODE"] = "1"
    os.environ["REPORT_RESEARCH_PARAMS_PATH"] = str(RESEARCH_PARAMS)
    os.environ["REPORT_RESEARCH_SELECTION_CONTRACT_PATH"] = str(SELECTION_CONTRACT)
    os.environ["REPORT_RESEARCH_OUTPUT_DIR"] = str(RUN_DIR)
    os.environ["REPORT_ALLOW_UNAPPROVED_FALLBACK"] = "1"

    report = _load_report_module()
    params = report.load_params()
    payload = _read_json(SELECTION_CONTRACT)
    base_contract = payload.get("selection_contract", payload)
    source_contract = _clear_regimes(_drop_filters(base_contract, {"mkt_vol20", "signal_value"}))

    exit_specs = [
        {"exit_variant": "hold5_stop6_base", "hold": 5, "stop_loss": -0.06, "note": "current research high-vol baseline"},
        {"exit_variant": "hold3_stop6", "hold": 3, "stop_loss": -0.06, "note": "shorter time exit, same stop"},
        {"exit_variant": "hold7_stop6", "hold": 7, "stop_loss": -0.06, "note": "longer time exit, same stop"},
        {"exit_variant": "hold5_stop4", "hold": 5, "stop_loss": -0.04, "note": "tighter stop, same hold"},
        {"exit_variant": "hold5_stop8", "hold": 5, "stop_loss": -0.08, "note": "looser stop, same hold"},
    ]
    rules = ["hv_profile_atr_3_4", "hv_profile_mid"]
    segments = ["all", "2025H1", "2025H2", "pre_2026", "2026H1", "exclude_2026H1"]

    data = report.load_data()
    factors = report.compute_factors(data)
    context = report.prepare_simulation_context(factors)

    all_trades: list[pd.DataFrame] = []
    summary_rows: list[dict[str, Any]] = []
    for spec in exit_specs:
        contract = _with_exit(source_contract, int(spec["hold"]), spec["stop_loss"])
        trades = report.simulate_trades(factors, params, selection_contract=contract, simulation_context=context)
        trades = _norm_trade_dates(trades)
        trades.insert(0, "exit_variant", spec["exit_variant"])
        trades.insert(1, "exit_note", spec["note"])
        trades.insert(2, "exit_hold", int(spec["hold"]))
        trades.insert(3, "exit_stop_loss", spec["stop_loss"])
        all_trades.append(trades)
        for rule in rules:
            selected = trades[_rule_mask(trades, rule).fillna(False)].copy() if len(trades) else trades.copy()
            for segment in segments:
                part = selected[_segment_mask(selected, segment).fillna(False)].copy() if len(selected) else selected.copy()
                summary_rows.append(_summarize(part, spec["exit_variant"], rule, segment, spec["note"]))

    combined = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()
    summary = pd.DataFrame(summary_rows)
    classification = _classify(summary)
    out_payload = {
        "status": STATUS,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_highvol_exit_variant_replay",
        "source_artifacts": {
            "report_backtest": str(REPORT_BACKTEST),
            "selection_contract": str(SELECTION_CONTRACT),
            "research_params": str(RESEARCH_PARAMS),
        },
        "source_contract_note": "any regime, mkt_vol20 removed, C3 value upper cap removed; high-vol rules applied after replay using actual entry gap",
        "exit_specs": exit_specs,
        "classification": classification,
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_broker_changed": False,
            "gate_or_threshold_changed": False,
            "policy_changed": False,
            "full_logic_application": "NOT_APPLIED",
        },
        "data_rows": int(len(data)),
        "factor_rows": int(len(factors)),
        "summary": _clean_records(summary),
    }
    OUT_JSON.write_text(json.dumps(out_payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    combined.to_csv(OUT_TRADES, index=False, encoding="utf-8-sig")

    focus = summary[
        (summary["rule"].isin(rules))
        & (summary["segment"].isin(["all", "2025H2", "2026H1"]))
    ].copy()
    lines = [
        "# C3 high-volatility exit variant comparison",
        "",
        f"- status: `{STATUS}`",
        f"- generated_at: `{out_payload['generated_at']}`",
        "- Scope: read-only exit variant replay; no operating rule or gate changed.",
        "",
        "## Focus comparison",
        "",
        "| exit_variant | rule | segment | n | win | loss | stop | stop_gap | time | ret_sum | PF |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in focus.to_dict(orient="records"):
        pf = row["profit_factor"]
        if pd.isna(pf):
            pf_text = "inf" if row["ret_sum"] > 0 and row["loss_n"] == 0 else ""
        else:
            pf_text = f"{pf:.6f}"
        lines.append(
            f"| {row['exit_variant']} | {row['rule']} | {row['segment']} | {row['n']} | {row['win_n']} | "
            f"{row['loss_n']} | {row['stop_n']} | {row['stop_gap_n']} | {row['time_n']} | {row['ret_sum']:.6f} | {pf_text} |"
        )
    lines.extend(
        [
            "",
            "## Classification",
            "",
            f"- Research classification: `{classification['research_classification']}`",
            f"- Operational decision: `{classification['operational_decision']}`",
            f"- Full logic application: `{classification['full_logic_application']}`",
            f"- Confidence: `{classification['confidence']}`",
            f"- Best all variant: `{classification['best_all_variant']}`",
            f"- Reasons: {', '.join(classification['reason'])}",
        ]
    )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "status": STATUS,
                "json": str(OUT_JSON),
                "summary": str(OUT_SUMMARY),
                "trades": str(OUT_TRADES),
                "md": str(OUT_MD),
                "classification": classification,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
