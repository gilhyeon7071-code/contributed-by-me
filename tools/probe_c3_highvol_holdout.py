from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Callable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
INPUT_ROWS = LOG_DIR / "c3_highvol_family_probe_rows_latest.csv"
OUT_JSON = LOG_DIR / "c3_highvol_holdout_probe_latest.json"
OUT_SUMMARY = LOG_DIR / "c3_highvol_holdout_probe_summary_latest.csv"
OUT_SENSITIVITY = LOG_DIR / "c3_highvol_holdout_probe_sensitivity_latest.csv"
OUT_MD = LOG_DIR / "c3_highvol_holdout_probe_latest.md"

CORE_GUARDS = ["baseline_remove_mkt_vol20", "gap_vaccel_prior_top", "gap_vaccel_value_500m"]


def _num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _profit_factor(ret: pd.Series) -> float | None:
    pos = float(ret[ret > 0].sum())
    neg = float(-ret[ret < 0].sum())
    if neg == 0:
        if pos > 0:
            return None
        return 0.0
    return pos / neg


def _summary(frame: pd.DataFrame, guard: str, split: str) -> dict:
    ret = _num(frame["ret"]) if len(frame) else pd.Series(dtype=float)
    gross_profit = float(ret[ret > 0].sum()) if len(ret) else 0.0
    gross_loss_abs = float(-ret[ret < 0].sum()) if len(ret) else 0.0
    return {
        "guard": guard,
        "split": split,
        "n": int(len(frame)),
        "win_n": int((ret > 0).sum()) if len(ret) else 0,
        "loss_n": int((ret < 0).sum()) if len(ret) else 0,
        "stop_n": int((frame["exit_reason"].astype(str).str.upper() == "STOP").sum()) if len(frame) else 0,
        "ret_sum": float(ret.sum()) if len(ret) else 0.0,
        "ret_mean": float(ret.mean()) if len(ret) else None,
        "profit_factor": _profit_factor(ret),
        "gross_profit": gross_profit,
        "gross_loss_abs": gross_loss_abs,
        "first_entry_date": str(frame["entry_date_norm"].min()) if len(frame) else None,
        "last_entry_date": str(frame["entry_date_norm"].max()) if len(frame) else None,
        "code_n": int(frame["code"].nunique()) if len(frame) else 0,
    }


def _splits(frame: pd.DataFrame) -> list[tuple[str, pd.Series]]:
    dates = pd.to_datetime(frame["entry_date_norm"], errors="coerce")
    return [
        ("all", pd.Series(True, index=frame.index)),
        ("pre_2025H2", dates < pd.Timestamp("2025-07-01")),
        ("2025H2", (dates >= pd.Timestamp("2025-07-01")) & (dates <= pd.Timestamp("2025-12-31"))),
        ("pre_2026", dates < pd.Timestamp("2026-01-01")),
        ("2026H1", (dates >= pd.Timestamp("2026-01-01")) & (dates <= pd.Timestamp("2026-06-30"))),
        ("exclude_2026H1", ~((dates >= pd.Timestamp("2026-01-01")) & (dates <= pd.Timestamp("2026-06-30")))),
    ]


def _concentration(frame: pd.DataFrame, guard: str) -> dict:
    ret = _num(frame["ret"]).dropna()
    positive = ret[ret > 0].sort_values(ascending=False)
    total = float(ret.sum())
    gross_profit = float(positive.sum())
    return {
        "guard": guard,
        "n": int(len(frame)),
        "ret_sum": total,
        "gross_profit": gross_profit,
        "top1_positive_ret": float(positive.iloc[0]) if len(positive) else 0.0,
        "top2_positive_ret": float(positive.iloc[:2].sum()) if len(positive) else 0.0,
        "top1_share_of_gross_profit": float(positive.iloc[0] / gross_profit) if gross_profit > 0 and len(positive) else None,
        "top2_share_of_gross_profit": float(positive.iloc[:2].sum() / gross_profit) if gross_profit > 0 and len(positive) else None,
        "ret_sum_without_top1": float(total - positive.iloc[0]) if len(positive) else total,
        "ret_sum_without_top2": float(total - positive.iloc[:2].sum()) if len(positive) else total,
    }


def _json_records(frame: pd.DataFrame) -> list[dict]:
    clean = frame.astype(object).where(pd.notna(frame), None)
    return clean.to_dict(orient="records")


def _classify(summary: pd.DataFrame, sensitivity: pd.DataFrame) -> dict:
    core = summary[(summary["guard"] == "gap_vaccel_prior_top")].set_index("split")
    sens = sensitivity[sensitivity["guard"] == "gap_vaccel_prior_top"].iloc[0].to_dict()
    reasons: list[str] = []
    if "all" in core.index and core.loc["all", "ret_sum"] > 0 and (core.loc["all", "profit_factor"] or 0) > 1:
        reasons.append("all_period_positive")
    if "pre_2026" in core.index and core.loc["pre_2026", "ret_sum"] <= 0.02:
        reasons.append("pre_2026_edge_is_weak")
    if "2026H1" in core.index and core.loc["2026H1", "n"] < 5:
        reasons.append("2026H1_sample_too_small")
    if float(sens.get("top2_share_of_gross_profit") or 0.0) >= 0.5:
        reasons.append("profit_concentrated_in_top_two_winners")
    if "2025H2" in core.index and core.loc["2025H2", "loss_n"] > 0:
        reasons.append("2025H2_has_mixed_outcomes")
    return {
        "operational_decision": "NOT_APPROVED",
        "research_classification": "C3_HIGHVOL_GAP_ACCEL_EXPLORATORY_HOLDOUT_WEAK",
        "full_logic_application": "NOT_APPLIED",
        "confidence": "LOW_TO_MEDIUM_RESEARCH_ONLY",
        "reason": reasons or ["insufficient_evidence"],
    }


def main() -> None:
    if not INPUT_ROWS.exists():
        raise FileNotFoundError(INPUT_ROWS)

    rows = pd.read_csv(INPUT_ROWS, dtype={"code": str})
    rows = rows[rows["guard"].isin(CORE_GUARDS)].copy()
    for col in ["ret", "entry_gap_pct", "signal_v_accel", "signal_value"]:
        if col in rows.columns:
            rows[col] = _num(rows[col])

    summary_rows: list[dict] = []
    sensitivity_rows: list[dict] = []
    for guard in CORE_GUARDS:
        part = rows[rows["guard"] == guard].copy()
        for split, mask in _splits(part):
            summary_rows.append(_summary(part[mask.fillna(False)].copy(), guard, split))
        sensitivity_rows.append(_concentration(part, guard))

    summary = pd.DataFrame(summary_rows)
    sensitivity = pd.DataFrame(sensitivity_rows)
    classification = _classify(summary, sensitivity)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_temporal_holdout_and_concentration_probe",
        "input": str(INPUT_ROWS),
        "guard_scope": CORE_GUARDS,
        "method_note": "Thresholds were discovered before this check; this is temporal stability and concentration validation, not a clean train/test proof.",
        "classification": classification,
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_broker_changed": False,
            "gate_or_threshold_changed": False,
        },
        "summary": _json_records(summary),
        "sensitivity": _json_records(sensitivity),
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    sensitivity.to_csv(OUT_SENSITIVITY, index=False, encoding="utf-8-sig")

    core = summary[
        (summary["guard"] == "gap_vaccel_prior_top")
        & summary["split"].isin(["all", "pre_2025H2", "2025H2", "pre_2026", "2026H1", "exclude_2026H1"])
    ].copy()
    lines = [
        "# C3 high-volatility holdout probe",
        "",
        f"- Generated at: {payload['generated_at']}",
        f"- Input: `{INPUT_ROWS}`",
        "- Scope: read-only temporal stability and concentration validation.",
        "- Method caveat: this is not a clean train/test proof because the thresholds were discovered before this check.",
        "",
        "## gap_vaccel_prior_top split summary",
        "",
        "| split | n | win | loss | stop | ret_sum | PF | first | last |",
        "|---|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in core.to_dict(orient="records"):
        pf = row["profit_factor"]
        if pd.isna(pf):
            pf_text = "inf" if row["gross_profit"] > 0 and row["gross_loss_abs"] == 0 else ""
        else:
            pf_text = f"{pf:.6f}"
        lines.append(
            f"| {row['split']} | {row['n']} | {row['win_n']} | {row['loss_n']} | {row['stop_n']} | "
            f"{row['ret_sum']:.6f} | {pf_text} | {row['first_entry_date']} | {row['last_entry_date']} |"
        )

    sens = sensitivity[sensitivity["guard"] == "gap_vaccel_prior_top"].iloc[0].to_dict()
    lines.extend(
        [
            "",
            "## Concentration",
            "",
            f"- top1_positive_ret: {sens['top1_positive_ret']:.6f}",
            f"- top2_positive_ret: {sens['top2_positive_ret']:.6f}",
            f"- top2_share_of_gross_profit: {sens['top2_share_of_gross_profit']:.6f}",
            f"- ret_sum_without_top2: {sens['ret_sum_without_top2']:.6f}",
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


if __name__ == "__main__":
    main()
