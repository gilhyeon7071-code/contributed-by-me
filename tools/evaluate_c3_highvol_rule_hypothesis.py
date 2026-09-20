from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
BROADER_TRADES = LOG_DIR / "c3_highvol_broader_source_replay_trades_latest.csv"
C3_SUMMARY = LOG_DIR / "c3_mkt_vol_variant_compare_summary_latest.csv"

OUT_JSON = LOG_DIR / "c3_highvol_rule_hypothesis_compare_latest.json"
OUT_SUMMARY = LOG_DIR / "c3_highvol_rule_hypothesis_compare_summary_latest.csv"
OUT_ROWS = LOG_DIR / "c3_highvol_rule_hypothesis_compare_rows_latest.csv"
OUT_MD = LOG_DIR / "c3_highvol_rule_hypothesis_compare_latest.md"

TARGET_VARIANT = "any_regime_no_c3_value_cap"


def _num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _profit_factor(ret: pd.Series) -> float | None:
    gains = float(ret[ret > 0].sum())
    losses = float(-ret[ret < 0].sum())
    if losses == 0:
        return None if gains > 0 else 0.0
    return gains / losses


def _segment_mask(df: pd.DataFrame, segment: str) -> pd.Series:
    dt = pd.to_datetime(df["entry_date_norm"], errors="coerce")
    if segment == "all":
        return pd.Series(True, index=df.index)
    if segment == "2024H1":
        return (dt >= "2024-01-01") & (dt <= "2024-06-30")
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


def _summarize(frame: pd.DataFrame, rule: str, segment: str, source: str, note: str) -> dict[str, Any]:
    ret = _num(frame["ret"]) if len(frame) else pd.Series(dtype=float)
    return {
        "source": source,
        "rule": rule,
        "segment": segment,
        "n": int(len(frame)),
        "win_n": int((ret > 0).sum()) if len(ret) else 0,
        "loss_n": int((ret < 0).sum()) if len(ret) else 0,
        "stop_n": int((frame["exit_reason"].astype(str).str.upper() == "STOP").sum()) if len(frame) and "exit_reason" in frame.columns else 0,
        "ret_sum": float(ret.sum()) if len(ret) else 0.0,
        "ret_mean": float(ret.mean()) if len(ret) else None,
        "profit_factor": _profit_factor(ret),
        "first_entry_date": str(frame["entry_date_norm"].min()) if len(frame) else None,
        "last_entry_date": str(frame["entry_date_norm"].max()) if len(frame) else None,
        "unique_codes": int(frame["code"].nunique()) if len(frame) and "code" in frame.columns else 0,
        "note": note,
    }


def _clean_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def _load_c3_baselines() -> list[dict[str, Any]]:
    if not C3_SUMMARY.exists():
        return []
    raw = pd.read_csv(C3_SUMMARY)
    map_segment = {
        "all_contract_window": "all",
        "reference_2025H2": "2025H2",
        "target_2026H1": "2026H1",
    }
    rows: list[dict[str, Any]] = []
    for _, row in raw.iterrows():
        variant = str(row.get("variant", ""))
        if variant not in {"hard_current_mkt_vol20", "remove_mkt_vol20_only"}:
            continue
        seg = map_segment.get(str(row.get("segment", "")))
        if seg is None:
            continue
        pf_raw = row.get("profit_factor")
        try:
            pf = None if str(pf_raw).lower() in {"inf", "nan", "na"} else float(pf_raw)
        except Exception:
            pf = None
        rows.append(
            {
                "source": "existing_c3_summary",
                "rule": variant,
                "segment": seg,
                "n": int(float(row.get("n", 0))),
                "win_n": int(float(row.get("win_n", 0))),
                "loss_n": int(float(row.get("loss_n", 0))),
                "stop_n": None,
                "ret_sum": float(row.get("ret_sum", 0.0)),
                "ret_mean": float(row.get("ret_mean", 0.0)) if str(row.get("ret_mean", "")).upper() != "NA" else None,
                "profit_factor": pf,
                "first_entry_date": row.get("first_entry_date", ""),
                "last_entry_date": row.get("last_entry_date", ""),
                "unique_codes": int(float(row.get("unique_codes", 0))),
                "note": "existing C3 replay summary; stop_n unavailable in summary csv",
            }
        )
    return rows


def _classify(summary: pd.DataFrame) -> dict[str, Any]:
    focus = summary[
        (summary["source"] == "highvol_rule_hypothesis")
        & (summary["rule"] == "hv_profile_atr_3_4")
    ].set_index("segment")
    reasons: list[str] = []
    if "all" in focus.index:
        row = focus.loc["all"]
        if int(row["n"]) < 20:
            reasons.append("sample_too_small_under_20")
        if float(row["ret_sum"]) > 0 and (row["profit_factor"] or 0) > 1:
            reasons.append("all_period_positive")
    if "2025H2" in focus.index and float(focus.loc["2025H2", "ret_sum"]) < 0:
        reasons.append("2025H2_negative")
    if "2026H1" in focus.index and int(focus.loc["2026H1", "n"]) < 5:
        reasons.append("2026H1_sample_too_small")
    c3 = summary[(summary["source"] == "existing_c3_summary") & (summary["rule"] == "hard_current_mkt_vol20") & (summary["segment"] == "all")]
    if len(c3) and "all" in focus.index and float(focus.loc["all", "ret_sum"]) < float(c3.iloc[0]["ret_sum"]):
        reasons.append("lower_total_return_than_existing_c3_baseline")
    return {
        "operational_decision": "NOT_APPROVED",
        "research_classification": "C3_HIGHVOL_RULE_HYPOTHESIS_PROMISING_BUT_SMALL",
        "confidence": "LOW_TO_MEDIUM_RESEARCH_ONLY",
        "full_logic_application": "NOT_APPLIED",
        "recommended_boundary": "separate_highvol_family_only_keep_existing_c3_unchanged",
        "reason": reasons or ["insufficient_evidence"],
    }


def main() -> int:
    if not BROADER_TRADES.exists():
        raise FileNotFoundError(BROADER_TRADES)
    trades = pd.read_csv(BROADER_TRADES, dtype={"code": str})
    target = trades[trades["variant"].astype(str).eq(TARGET_VARIANT)].copy()
    for col in ["entry_gap_pct", "signal_v_accel", "signal_atr_pct", "signal_value", "ret"]:
        target[col] = _num(target[col])

    base_regime = target[target["market_regime"].astype(str).isin(["BULL", "SIDEWAYS"])].copy()
    rule_specs: list[tuple[str, str, Callable[[pd.DataFrame], pd.Series]]] = [
        (
            "hv_profile_strict",
            "BULL/SIDEWAYS, gap 1.47~2%, v_accel 1.26~1.5, ATR 3.5~4%",
            lambda df: (
                df["entry_gap_pct"].ge(0.0147299509)
                & df["entry_gap_pct"].lt(0.02)
                & df["signal_v_accel"].ge(1.255833911)
                & df["signal_v_accel"].lt(1.5)
                & df["signal_atr_pct"].ge(0.035)
                & df["signal_atr_pct"].lt(0.04)
            ),
        ),
        (
            "hv_profile_atr_3_4",
            "BULL/SIDEWAYS, gap 1.47~3%, v_accel 1.26~1.5, ATR 3~4%",
            lambda df: (
                df["entry_gap_pct"].ge(0.0147299509)
                & df["entry_gap_pct"].lt(0.03)
                & df["signal_v_accel"].ge(1.255833911)
                & df["signal_v_accel"].lt(1.5)
                & df["signal_atr_pct"].ge(0.03)
                & df["signal_atr_pct"].lt(0.04)
            ),
        ),
        (
            "hv_profile_mid",
            "BULL/SIDEWAYS, gap 1.47~3%, v_accel 1.26~1.5",
            lambda df: (
                df["entry_gap_pct"].ge(0.0147299509)
                & df["entry_gap_pct"].lt(0.03)
                & df["signal_v_accel"].ge(1.255833911)
                & df["signal_v_accel"].lt(1.5)
            ),
        ),
        (
            "hv_profile_mid_value500m",
            "BULL/SIDEWAYS, gap 1.47~3%, v_accel 1.26~1.5, value >= 500M",
            lambda df: (
                df["entry_gap_pct"].ge(0.0147299509)
                & df["entry_gap_pct"].lt(0.03)
                & df["signal_v_accel"].ge(1.255833911)
                & df["signal_v_accel"].lt(1.5)
                & df["signal_value"].ge(500_000_000)
            ),
        ),
    ]
    segments = ["all", "2024H1", "2025H1", "2025H2", "pre_2026", "2026H1", "exclude_2026H1"]
    summary_rows: list[dict[str, Any]] = _load_c3_baselines()
    selected_parts: list[pd.DataFrame] = []
    rule_definitions: list[dict[str, Any]] = []

    for rule, note, condition in rule_specs:
        selected = base_regime[condition(base_regime).fillna(False)].copy()
        selected.insert(0, "hypothesis_rule", rule)
        selected.insert(1, "hypothesis_note", note)
        selected_parts.append(selected)
        rule_definitions.append({"rule": rule, "note": note, "rows": int(len(selected))})
        for segment in segments:
            part = selected[_segment_mask(selected, segment).fillna(False)].copy() if len(selected) else selected.copy()
            summary_rows.append(_summarize(part, rule, segment, "highvol_rule_hypothesis", note))

    summary = pd.DataFrame(summary_rows)
    rows = pd.concat(selected_parts, ignore_index=True) if selected_parts else pd.DataFrame()
    classification = _classify(summary)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_compare_highvol_rule_hypothesis_to_existing_c3",
        "input": {
            "broader_trades": str(BROADER_TRADES),
            "existing_c3_summary": str(C3_SUMMARY),
        },
        "target_variant": TARGET_VARIANT,
        "rule_definitions": rule_definitions,
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
    rows.to_csv(OUT_ROWS, index=False, encoding="utf-8-sig")

    focus = summary[
        summary["rule"].isin(["hard_current_mkt_vol20", "remove_mkt_vol20_only", "hv_profile_atr_3_4", "hv_profile_mid"])
        & summary["segment"].isin(["all", "2025H1", "2025H2", "2026H1"])
    ].copy()
    lines = [
        "# C3 high-volatility rule hypothesis comparison",
        "",
        f"- Generated at: {payload['generated_at']}",
        "- Scope: read-only hypothesis comparison; no operating rule or gate changed.",
        f"- Target broader-source variant: `{TARGET_VARIANT}`",
        "",
        "## Focus comparison",
        "",
        "| source | rule | segment | n | win | loss | stop | ret_sum | PF |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in focus.to_dict(orient="records"):
        pf = row["profit_factor"]
        if pd.isna(pf):
            pf_text = "inf" if row["ret_sum"] > 0 and row["loss_n"] == 0 else ""
        else:
            pf_text = f"{pf:.6f}"
        stop = "" if pd.isna(row["stop_n"]) else str(int(row["stop_n"]))
        lines.append(
            f"| {row['source']} | {row['rule']} | {row['segment']} | {row['n']} | {row['win_n']} | "
            f"{row['loss_n']} | {stop} | {row['ret_sum']:.6f} | {pf_text} |"
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
            f"- Boundary: `{classification['recommended_boundary']}`",
            f"- Reasons: {', '.join(classification['reason'])}",
        ]
    )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "json": str(OUT_JSON),
                "summary": str(OUT_SUMMARY),
                "rows": str(OUT_ROWS),
                "md": str(OUT_MD),
                "classification": classification,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
