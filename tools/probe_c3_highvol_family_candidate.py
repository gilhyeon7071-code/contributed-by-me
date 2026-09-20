from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Callable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
INPUT_TRADES = LOG_DIR / "c3_mkt_vol_variant_compare_trades_latest.csv"
OUT_JSON = LOG_DIR / "c3_highvol_family_probe_latest.json"
OUT_SUMMARY = LOG_DIR / "c3_highvol_family_probe_summary_latest.csv"
OUT_ROWS = LOG_DIR / "c3_highvol_family_probe_rows_latest.csv"
OUT_MD = LOG_DIR / "c3_highvol_family_probe_latest.md"

SOURCE_VARIANT = "remove_mkt_vol20_only"

# Thresholds below are copied from the prior read-only 2026H1 pre-entry guard
# search. They are not policy values and are not used by candidate generation.
THRESHOLDS = {
    "entry_gap_pct_prior_top": 0.0147299509,
    "signal_v_accel_prior_top": 1.255833911,
    "signal_value_prior_top": 541_667_860.0,
    "signal_value_round_500m": 500_000_000.0,
}


def _num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _profit_factor(ret: pd.Series) -> float | None:
    pos = ret[ret > 0].sum()
    neg = -ret[ret < 0].sum()
    if neg == 0:
        if pos > 0:
            return None
        return 0.0
    return float(pos / neg)


def _summarize(frame: pd.DataFrame, guard: str, period: str) -> dict:
    ret = _num(frame["ret"])
    pos = ret[ret > 0].sum()
    neg = -ret[ret < 0].sum()
    pf = _profit_factor(ret)
    return {
        "guard": guard,
        "period": period,
        "n": int(len(frame)),
        "win_n": int((ret > 0).sum()),
        "loss_n": int((ret < 0).sum()),
        "flat_n": int((ret == 0).sum()),
        "stop_n": int((frame["exit_reason"].astype(str).str.upper() == "STOP").sum())
        if "exit_reason" in frame.columns
        else 0,
        "ret_sum": float(ret.sum()) if len(frame) else 0.0,
        "ret_mean": float(ret.mean()) if len(frame) else None,
        "ret_median": float(ret.median()) if len(frame) else None,
        "gross_profit": float(pos),
        "gross_loss_abs": float(neg),
        "profit_factor": pf,
        "entry_date_min": str(frame["entry_date_norm"].min()) if len(frame) else None,
        "entry_date_max": str(frame["entry_date_norm"].max()) if len(frame) else None,
        "code_n": int(frame["code"].nunique()) if "code" in frame.columns else 0,
    }


def _period_frames(frame: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    dates = pd.to_datetime(frame["entry_date_norm"], errors="coerce")
    return [
        ("all", frame),
        ("2024", frame[dates.dt.year == 2024]),
        ("2025", frame[dates.dt.year == 2025]),
        ("2025H1", frame[(dates >= "2025-01-01") & (dates <= "2025-06-30")]),
        ("2025H2", frame[(dates >= "2025-07-01") & (dates <= "2025-12-31")]),
        ("2026H1", frame[(dates >= "2026-01-01") & (dates <= "2026-06-30")]),
    ]


def _classify(summary: pd.DataFrame) -> dict:
    core = summary[
        (summary["guard"] == "gap_vaccel_prior_top")
        & (summary["period"].isin(["all", "2025H2", "2026H1"]))
    ].set_index("period")

    result = {
        "operational_decision": "NOT_APPROVED",
        "research_classification": "C3_HIGHVOL_GAP_ACCEL_EXPLORATORY",
        "full_logic_application": "NOT_APPLIED",
        "reason": [],
    }
    if "all" in core.index:
        all_row = core.loc["all"]
        if all_row["n"] >= 8 and all_row["ret_sum"] > 0 and (all_row["profit_factor"] or 0) > 1:
            result["reason"].append("all_period_positive_for_prior_high_gap_vaccel_guard")
        else:
            result["reason"].append("all_period_not_robust")
    if "2026H1" in core.index:
        h1 = core.loc["2026H1"]
        if h1["n"] < 5:
            result["reason"].append("2026H1_sample_too_small")
        if h1["stop_n"] > 0:
            result["reason"].append("2026H1_stops_remain")
    if "2025H2" in core.index:
        h2 = core.loc["2025H2"]
        if h2["loss_n"] > 0:
            result["reason"].append("2025H2_mixed_outcomes")

    if not result["reason"]:
        result["reason"].append("insufficient_evidence")
    return result


def _json_records(frame: pd.DataFrame) -> list[dict]:
    clean = frame.astype(object).where(pd.notna(frame), None)
    return clean.to_dict(orient="records")


def main() -> None:
    if not INPUT_TRADES.exists():
        raise FileNotFoundError(INPUT_TRADES)

    trades = pd.read_csv(INPUT_TRADES, dtype={"code": str})
    source = trades[trades["variant"].astype(str) == SOURCE_VARIANT].copy()
    if source.empty:
        raise RuntimeError(f"no rows for variant={SOURCE_VARIANT}")

    required = [
        "code",
        "signal_date",
        "entry_date_norm",
        "ret",
        "exit_reason",
        "entry_gap_pct",
        "signal_v_accel",
        "signal_value",
        "signal_atr_pct",
    ]
    missing = [col for col in required if col not in source.columns]
    if missing:
        raise RuntimeError(f"missing required columns: {missing}")

    for col in ["ret", "entry_gap_pct", "signal_v_accel", "signal_value", "signal_atr_pct"]:
        source[col] = _num(source[col])

    guards: list[tuple[str, str, Callable[[pd.DataFrame], pd.Series]]] = [
        ("baseline_remove_mkt_vol20", "all source rows after removing only mkt_vol20", lambda df: pd.Series(True, index=df.index)),
        (
            "high_gap_prior_top",
            f"entry_gap_pct >= {THRESHOLDS['entry_gap_pct_prior_top']}",
            lambda df: df["entry_gap_pct"] >= THRESHOLDS["entry_gap_pct_prior_top"],
        ),
        (
            "high_vaccel_prior_top",
            f"signal_v_accel >= {THRESHOLDS['signal_v_accel_prior_top']}",
            lambda df: df["signal_v_accel"] >= THRESHOLDS["signal_v_accel_prior_top"],
        ),
        (
            "high_value_500m",
            f"signal_value >= {THRESHOLDS['signal_value_round_500m']}",
            lambda df: df["signal_value"] >= THRESHOLDS["signal_value_round_500m"],
        ),
        (
            "gap_vaccel_prior_top",
            "entry_gap_pct prior-top threshold AND signal_v_accel prior-top threshold",
            lambda df: (df["entry_gap_pct"] >= THRESHOLDS["entry_gap_pct_prior_top"])
            & (df["signal_v_accel"] >= THRESHOLDS["signal_v_accel_prior_top"]),
        ),
        (
            "gap_value_prior_top",
            "entry_gap_pct prior-top threshold AND signal_value prior-top threshold",
            lambda df: (df["entry_gap_pct"] >= THRESHOLDS["entry_gap_pct_prior_top"])
            & (df["signal_value"] >= THRESHOLDS["signal_value_prior_top"]),
        ),
        (
            "vaccel_value_500m",
            "signal_v_accel prior-top threshold AND signal_value >= 500M",
            lambda df: (df["signal_v_accel"] >= THRESHOLDS["signal_v_accel_prior_top"])
            & (df["signal_value"] >= THRESHOLDS["signal_value_round_500m"]),
        ),
        (
            "gap_vaccel_value_500m",
            "entry_gap_pct prior-top threshold AND signal_v_accel prior-top threshold AND signal_value >= 500M",
            lambda df: (df["entry_gap_pct"] >= THRESHOLDS["entry_gap_pct_prior_top"])
            & (df["signal_v_accel"] >= THRESHOLDS["signal_v_accel_prior_top"])
            & (df["signal_value"] >= THRESHOLDS["signal_value_round_500m"]),
        ),
    ]

    summary_rows: list[dict] = []
    selected_rows: list[pd.DataFrame] = []
    guard_defs: list[dict] = []

    for name, description, condition in guards:
        mask = condition(source).fillna(False)
        guarded = source[mask].copy()
        guarded.insert(0, "guard", name)
        guarded.insert(1, "guard_description", description)
        selected_rows.append(guarded)
        guard_defs.append({"guard": name, "description": description, "selected_rows": int(mask.sum())})
        for period, period_frame in _period_frames(guarded):
            summary_rows.append(_summarize(period_frame, name, period))

    summary = pd.DataFrame(summary_rows)
    rows = pd.concat(selected_rows, ignore_index=True)
    rows = rows.sort_values(["guard", "entry_date_norm", "code"])
    summary = summary.sort_values(["guard", "period"])

    classification = _classify(summary)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_probe_of_c3_highvol_family_candidate",
        "input": str(INPUT_TRADES),
        "source_variant": SOURCE_VARIANT,
        "threshold_source": "prior 2026H1 pre-entry guard search; research-only, not policy",
        "thresholds": THRESHOLDS,
        "source_rows": int(len(source)),
        "source_entry_date_min": str(source["entry_date_norm"].min()),
        "source_entry_date_max": str(source["entry_date_norm"].max()),
        "guard_definitions": guard_defs,
        "classification": classification,
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_broker_changed": False,
            "gate_or_threshold_changed": False,
        },
        "summary": _json_records(summary),
    }

    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    rows.to_csv(OUT_ROWS, index=False, encoding="utf-8-sig")

    core = summary[
        summary["guard"].isin(["baseline_remove_mkt_vol20", "gap_vaccel_prior_top", "gap_vaccel_value_500m"])
        & summary["period"].isin(["all", "2025H2", "2026H1"])
    ].copy()

    lines = [
        "# C3 high-volatility family candidate probe",
        "",
        f"- Generated at: {payload['generated_at']}",
        f"- Input: `{INPUT_TRADES}`",
        f"- Source variant: `{SOURCE_VARIANT}`",
        f"- Source rows: {len(source)}",
        f"- Source entry date range: {payload['source_entry_date_min']} to {payload['source_entry_date_max']}",
        "- Scope: read-only research probe; no production candidate/backtest/HPO/paper/broker/gate changes.",
        "",
        "## Core comparison",
        "",
        "| guard | period | n | win | loss | stop | ret_sum | PF |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in core.to_dict(orient="records"):
        pf = row["profit_factor"]
        if pd.isna(pf):
            pf_text = "inf" if row["gross_profit"] > 0 and row["gross_loss_abs"] == 0 else ""
        else:
            pf_text = f"{pf:.6f}"
        lines.append(
            f"| {row['guard']} | {row['period']} | {row['n']} | {row['win_n']} | {row['loss_n']} | "
            f"{row['stop_n']} | {row['ret_sum']:.6f} | {pf_text} |"
        )

    lines.extend(
        [
            "",
            "## Classification",
            "",
            f"- Research classification: `{classification['research_classification']}`",
            f"- Operational decision: `{classification['operational_decision']}`",
            f"- Full logic application: `{classification['full_logic_application']}`",
            f"- Reasons: {', '.join(classification['reason'])}",
            "",
            "## Boundary",
            "",
            "- This probe checks whether the high-gap/high-v_accel profile deserves a separate read-only strategy-family study.",
            "- It does not approve weakening the existing C3 mkt_vol20 exclusion.",
        ]
    )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
