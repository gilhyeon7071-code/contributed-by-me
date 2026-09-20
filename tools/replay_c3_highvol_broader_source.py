from __future__ import annotations

import importlib.util
import json
import os
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


ROOT = Path(r"E:\1_Data")
LOG_DIR = ROOT / "2_Logs"
REPORT_BACKTEST = ROOT / "report_backtest_v41_1.py"
SELECTION_CONTRACT = LOG_DIR / "c3_normal_official_replay_selection_contract_latest.json"
RESEARCH_PARAMS = LOG_DIR / "c3_strategy_replay_params_latest.json"

OUT_JSON = LOG_DIR / "c3_highvol_broader_source_replay_latest.json"
OUT_SUMMARY = LOG_DIR / "c3_highvol_broader_source_replay_summary_latest.csv"
OUT_TRADES = LOG_DIR / "c3_highvol_broader_source_replay_trades_latest.csv"
OUT_MD = LOG_DIR / "c3_highvol_broader_source_replay_latest.md"
RUN_DIR = LOG_DIR / "c3_highvol_broader_source_runtime"

STATUS = "READ_ONLY_C3_HIGHVOL_BROADER_SOURCE_REPLAY_NOT_OPERATIONAL"

THRESHOLDS = {
    "entry_gap_pct_prior_top": 0.0147299509,
    "signal_v_accel_prior_top": 1.255833911,
    "signal_value_round_500m": 500_000_000.0,
}


def _load_report_module() -> Any:
    spec = importlib.util.spec_from_file_location("report_backtest_v41_1_for_c3_highvol_broader", REPORT_BACKTEST)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {REPORT_BACKTEST}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _field(raw_field: str) -> str:
    return {
        "signal_ret1_pct": "ret1_pct",
        "signal_rs": "rs",
        "signal_v_accel": "v_accel",
        "signal_stretch": "stretch",
        "signal_atr_pct": "atr_pct",
        "signal_high_52w_gap": "high_52w_gap",
        "signal_value": "value",
    }.get(raw_field, raw_field)


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


def _filter_stage_count(sig: pd.DataFrame, contract: dict[str, Any]) -> dict[str, Any]:
    out = sig.copy()
    stage_rows: list[dict[str, Any]] = [{"stage": "start", "rows": int(len(out))}]
    if contract.get("signal_date_start") is not None:
        out = out[out["date"] >= pd.Timestamp(contract["signal_date_start"])].copy()
        stage_rows.append({"stage": "signal_date_start", "rows": int(len(out))})
    if contract.get("signal_date_end") is not None:
        out = out[out["date"] <= pd.Timestamp(contract["signal_date_end"])].copy()
        stage_rows.append({"stage": "signal_date_end", "rows": int(len(out))})
    allowed_regimes = {str(x).upper() for x in contract.get("regimes", []) if str(x).strip()}
    if allowed_regimes:
        out = out[out["market_regime"].astype(str).str.upper().isin(allowed_regimes)].copy()
        stage_rows.append({"stage": "regimes", "rows": int(len(out))})

    for item in list(contract.get("numeric_filters") or []):
        raw_field = str(item.get("field", "")).strip()
        op = str(item.get("op", "")).strip()
        value = float(item.get("value"))
        if raw_field == "entry_gap_pct":
            series = (pd.to_numeric(out["n_open"], errors="coerce") - pd.to_numeric(out["close"], errors="coerce")) / (
                pd.to_numeric(out["close"], errors="coerce") + 1e-9
            )
        else:
            field = _field(raw_field)
            series = pd.to_numeric(out[field], errors="coerce")
        if op == "<=":
            mask = series <= value
        elif op == "<":
            mask = series < value
        elif op == ">=":
            mask = series >= value
        elif op == ">":
            mask = series > value
        elif op in {"==", "="}:
            mask = series == value
        elif op == "!=":
            mask = series != value
        else:
            raise ValueError(f"unsupported op: {op}")
        out = out[mask.fillna(False)].copy()
        stage_rows.append({"stage": f"{raw_field} {op} {value}", "rows": int(len(out))})
        if out.empty:
            break
    return {
        "stage_rows": stage_rows,
        "final_rows": int(len(out)),
        "final_dates": int(out["date"].nunique()) if len(out) else 0,
        "final_codes": int(out["code"].nunique()) if len(out) else 0,
    }


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
    dates = pd.to_datetime(df["entry_date_norm"], errors="coerce")
    if segment == "all":
        return pd.Series(True, index=df.index)
    if segment == "2024":
        return dates.dt.year == 2024
    if segment == "2025H1":
        return (dates >= "2025-01-01") & (dates <= "2025-06-30")
    if segment == "2025H2":
        return (dates >= "2025-07-01") & (dates <= "2025-12-31")
    if segment == "2026H1":
        return (dates >= "2026-01-01") & (dates <= "2026-06-30")
    if segment == "pre_2026":
        return dates < "2026-01-01"
    if segment == "exclude_2026H1":
        return ~((dates >= "2026-01-01") & (dates <= "2026-06-30"))
    raise ValueError(segment)


def _post_mask(df: pd.DataFrame, post_filter: str) -> pd.Series:
    if post_filter == "all_replayed_trades":
        return pd.Series(True, index=df.index)
    gap = pd.to_numeric(df["entry_gap_pct"], errors="coerce")
    vaccel = pd.to_numeric(df["signal_v_accel"], errors="coerce")
    value = pd.to_numeric(df["signal_value"], errors="coerce")
    if post_filter == "actual_gap_vaccel":
        return (gap >= THRESHOLDS["entry_gap_pct_prior_top"]) & (vaccel >= THRESHOLDS["signal_v_accel_prior_top"])
    if post_filter == "actual_gap_vaccel_value500m":
        return (
            (gap >= THRESHOLDS["entry_gap_pct_prior_top"])
            & (vaccel >= THRESHOLDS["signal_v_accel_prior_top"])
            & (value >= THRESHOLDS["signal_value_round_500m"])
        )
    raise ValueError(post_filter)


def _summarize(frame: pd.DataFrame, variant: str, post_filter: str, segment: str) -> dict[str, Any]:
    ret = pd.to_numeric(frame.get("ret", pd.Series(dtype=float)), errors="coerce").dropna()
    return {
        "variant": variant,
        "post_filter": post_filter,
        "segment": segment,
        "n": int(len(frame)),
        "win_n": int((ret > 0).sum()) if len(ret) else 0,
        "loss_n": int((ret < 0).sum()) if len(ret) else 0,
        "stop_n": int((frame["exit_reason"].astype(str).str.upper() == "STOP").sum()) if len(frame) else 0,
        "ret_sum": float(ret.sum()) if len(ret) else 0.0,
        "ret_mean": float(ret.mean()) if len(ret) else None,
        "profit_factor": _profit_factor(ret.tolist()),
        "first_entry_date": str(frame["entry_date_norm"].min()) if len(frame) else None,
        "last_entry_date": str(frame["entry_date_norm"].max()) if len(frame) else None,
        "unique_codes": int(frame["code"].nunique()) if len(frame) else 0,
    }


def _clean_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return df.astype(object).where(pd.notna(df), None).to_dict(orient="records")


def _classify(summary: pd.DataFrame) -> dict[str, Any]:
    target = summary[
        (summary["variant"] == "any_regime_no_c3_value_cap")
        & (summary["post_filter"] == "actual_gap_vaccel_value500m")
        & (summary["segment"].isin(["all", "pre_2026", "2026H1"]))
    ].set_index("segment")
    reasons: list[str] = []
    if "all" in target.index:
        all_row = target.loc["all"]
        if int(all_row["n"]) >= 20:
            reasons.append("broader_source_created_more_than_20_filtered_trades")
        else:
            reasons.append("broader_source_still_small")
        if float(all_row["ret_sum"]) > 0:
            reasons.append("all_period_positive")
        else:
            reasons.append("all_period_non_positive")
    if "pre_2026" in target.index:
        pre = target.loc["pre_2026"]
        if int(pre["n"]) >= 10 and float(pre["ret_sum"]) > 0:
            reasons.append("pre_2026_positive_with_some_sample")
        else:
            reasons.append("pre_2026_not_sufficient")
    if "2026H1" in target.index:
        h1 = target.loc["2026H1"]
        if int(h1["n"]) < 5:
            reasons.append("2026H1_sample_too_small")
        elif float(h1["ret_sum"]) <= 0:
            reasons.append("2026H1_non_positive")
    decision = "NOT_APPROVED"
    research_classification = "C3_HIGHVOL_BROADER_SOURCE_EXPLORATORY"
    confidence = "LOW_RESEARCH_ONLY"
    if (
        "all" in target.index
        and "pre_2026" in target.index
        and int(target.loc["all", "n"]) >= 20
        and int(target.loc["pre_2026", "n"]) >= 10
        and float(target.loc["all", "ret_sum"]) > 0
        and float(target.loc["pre_2026", "ret_sum"]) > 0
    ):
        confidence = "MEDIUM_RESEARCH_ONLY"
    return {
        "operational_decision": decision,
        "research_classification": research_classification,
        "confidence": confidence,
        "full_logic_application": "NOT_APPLIED",
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
    contract_payload = _read_json(SELECTION_CONTRACT)
    base_contract = contract_payload.get("selection_contract", contract_payload)

    data = report.load_data()
    factors = report.compute_factors(data)
    context = report.prepare_simulation_context(factors)
    sig_all = context["sig"].copy()

    variants = [
        {
            "variant": "transition_c3_no_mktvol",
            "contract": _drop_filters(base_contract, {"mkt_vol20"}),
            "note": "same as prior remove_mkt_vol20 sensitivity; transition regime and C3 caps remain",
        },
        {
            "variant": "any_regime_c3_no_mktvol",
            "contract": _clear_regimes(_drop_filters(base_contract, {"mkt_vol20"})),
            "note": "remove regime restriction and mkt_vol20; keep other C3 filters",
        },
        {
            "variant": "any_regime_no_c3_value_cap",
            "contract": _clear_regimes(_drop_filters(base_contract, {"mkt_vol20", "signal_value"})),
            "note": "remove regime restriction, mkt_vol20, and C3 value upper cap; post-filter can require value >= 500M",
        },
        {
            "variant": "any_regime_no_reversion_caps",
            "contract": _clear_regimes(_drop_filters(base_contract, {"mkt_vol20", "signal_value", "signal_rs", "signal_high_52w_gap"})),
            "note": "remove regime restriction, mkt_vol20, value upper cap, low-RS cap, and 52w-high cap; exploratory only",
        },
    ]
    post_filters = ["all_replayed_trades", "actual_gap_vaccel", "actual_gap_vaccel_value500m"]
    segments = ["all", "2024", "2025H1", "2025H2", "pre_2026", "2026H1", "exclude_2026H1"]

    all_trades: list[pd.DataFrame] = []
    stage_counts: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    for spec in variants:
        stage = _filter_stage_count(sig_all, spec["contract"])
        stage_counts.append({"variant": spec["variant"], "note": spec["note"], **stage})
        trades = report.simulate_trades(factors, params, selection_contract=spec["contract"], simulation_context=context)
        trades = _norm_trade_dates(trades)
        trades.insert(0, "variant", spec["variant"])
        trades.insert(1, "variant_note", spec["note"])
        all_trades.append(trades)

        for post_filter in post_filters:
            post = trades[_post_mask(trades, post_filter).fillna(False)].copy() if len(trades) else trades.copy()
            for segment in segments:
                seg = post[_segment_mask(post, segment).fillna(False)].copy() if len(post) else post.copy()
                summary_rows.append(_summarize(seg, spec["variant"], post_filter, segment))

    combined = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()
    summary = pd.DataFrame(summary_rows)
    classification = _classify(summary)

    payload = {
        "status": STATUS,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "source_artifacts": {
            "report_backtest": str(REPORT_BACKTEST),
            "selection_contract": str(SELECTION_CONTRACT),
            "research_params": str(RESEARCH_PARAMS),
        },
        "thresholds": THRESHOLDS,
        "method_boundary": [
            "actual entry_gap_pct is evaluated after replay because it is based on the actual triggered entry day",
            "broader contracts are exploratory and do not change production candidate generation or official backtest policy",
        ],
        "data_rows": int(len(data)),
        "factor_rows": int(len(factors)),
        "signal_rows": int(len(sig_all)),
        "stage_counts": stage_counts,
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
        "summary": _clean_records(summary),
    }

    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    combined.to_csv(OUT_TRADES, index=False, encoding="utf-8-sig")

    focus = summary[
        (summary["post_filter"].isin(["actual_gap_vaccel", "actual_gap_vaccel_value500m"]))
        & (summary["segment"].isin(["all", "pre_2026", "2026H1"]))
    ].copy()
    lines = [
        "# C3 high-volatility broader source replay",
        "",
        f"- status: `{STATUS}`",
        f"- created_at: `{payload['created_at']}`",
        "- Scope: read-only broader-source replay; no operating rule or gate changed.",
        "- Boundary: actual entry_gap_pct is post-replay because it is based on the triggered entry day.",
        "",
        "## Focus summary",
        "",
        "| variant | post_filter | segment | n | win | loss | stop | ret_sum | PF |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in focus.to_dict(orient="records"):
        pf = row["profit_factor"]
        if pd.isna(pf):
            pf_text = "inf" if row["ret_sum"] > 0 and row["loss_n"] == 0 else ""
        else:
            pf_text = f"{pf:.6f}"
        lines.append(
            f"| {row['variant']} | {row['post_filter']} | {row['segment']} | {row['n']} | {row['win_n']} | "
            f"{row['loss_n']} | {row['stop_n']} | {row['ret_sum']:.6f} | {pf_text} |"
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
            f"- Reasons: {', '.join(classification['reason'])}",
        ]
    )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(
        json.dumps(
            {
                "status": STATUS,
                "json": str(OUT_JSON),
                "summary_csv": str(OUT_SUMMARY),
                "trades_csv": str(OUT_TRADES),
                "md": str(OUT_MD),
                "classification": classification,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
