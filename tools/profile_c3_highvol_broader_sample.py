from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
INPUT_TRADES = LOG_DIR / "c3_highvol_broader_source_replay_trades_latest.csv"

OUT_JSON = LOG_DIR / "c3_highvol_broader_sample_profile_latest.json"
OUT_GROUPS = LOG_DIR / "c3_highvol_broader_sample_profile_groups_latest.csv"
OUT_FEATURES = LOG_DIR / "c3_highvol_broader_sample_profile_features_latest.csv"
OUT_ROWS = LOG_DIR / "c3_highvol_broader_sample_profile_rows_latest.csv"
OUT_MD = LOG_DIR / "c3_highvol_broader_sample_profile_latest.md"

TARGET_VARIANT = "any_regime_no_c3_value_cap"
THRESHOLDS = {
    "entry_gap_pct": 0.0147299509,
    "signal_v_accel": 1.255833911,
    "signal_value_500m": 500_000_000.0,
}


def _num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _profit_factor(ret: pd.Series) -> float | None:
    gains = float(ret[ret > 0].sum())
    losses = float(-ret[ret < 0].sum())
    if losses == 0:
        return None if gains > 0 else 0.0
    return gains / losses


def _summarize(frame: pd.DataFrame, group_type: str, group_value: str, sample: str) -> dict[str, Any]:
    ret = _num(frame["ret"]) if len(frame) else pd.Series(dtype=float)
    return {
        "sample": sample,
        "group_type": group_type,
        "group_value": group_value,
        "n": int(len(frame)),
        "win_n": int((ret > 0).sum()) if len(ret) else 0,
        "loss_n": int((ret < 0).sum()) if len(ret) else 0,
        "stop_n": int((frame["exit_reason"].astype(str).str.upper() == "STOP").sum()) if len(frame) else 0,
        "ret_sum": float(ret.sum()) if len(ret) else 0.0,
        "ret_mean": float(ret.mean()) if len(ret) else None,
        "ret_median": float(ret.median()) if len(ret) else None,
        "profit_factor": _profit_factor(ret),
        "first_entry_date": str(frame["entry_date_norm"].min()) if len(frame) else None,
        "last_entry_date": str(frame["entry_date_norm"].max()) if len(frame) else None,
        "unique_codes": int(frame["code"].nunique()) if len(frame) and "code" in frame.columns else 0,
    }


def _add_time_fields(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    dt = pd.to_datetime(out["entry_date_norm"], errors="coerce")
    out["entry_year"] = dt.dt.year.astype("Int64").astype(str)
    out["entry_half"] = dt.dt.year.astype("Int64").astype(str) + "H" + (((dt.dt.month - 1) // 6) + 1).astype("Int64").astype(str)
    out["entry_month"] = dt.dt.strftime("%Y-%m")
    return out


def _assign_sample(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    gap = _num(out["entry_gap_pct"])
    vaccel = _num(out["signal_v_accel"])
    value = _num(out["signal_value"])
    out["sample_gap_vaccel"] = (gap >= THRESHOLDS["entry_gap_pct"]) & (vaccel >= THRESHOLDS["signal_v_accel"])
    out["sample_gap_vaccel_value500m"] = out["sample_gap_vaccel"] & (value >= THRESHOLDS["signal_value_500m"])
    out["sample"] = "outside"
    out.loc[out["sample_gap_vaccel"], "sample"] = "gap_vaccel"
    out.loc[out["sample_gap_vaccel_value500m"], "sample"] = "gap_vaccel_value500m"
    return out


def _bucket_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["entry_gap_bucket"] = pd.cut(
        _num(out["entry_gap_pct"]),
        bins=[-999, 0.0147299509, 0.02, 0.03, 999],
        labels=["lt_guard", "1.47_to_2pct", "2_to_3pct", "gte_3pct"],
        right=False,
    ).astype(str)
    out["v_accel_bucket"] = pd.cut(
        _num(out["signal_v_accel"]),
        bins=[-999, 1.255833911, 1.5, 2.0, 999],
        labels=["lt_guard", "1.26_to_1.5", "1.5_to_2", "gte_2"],
        right=False,
    ).astype(str)
    out["value_bucket"] = pd.cut(
        _num(out["signal_value"]),
        bins=[-999, 500_000_000, 1_000_000_000, 2_000_000_000, 999_999_999_999],
        labels=["lt_500m", "500m_to_1b", "1b_to_2b", "gte_2b"],
        right=False,
    ).astype(str)
    out["atr_bucket"] = pd.cut(
        _num(out["signal_atr_pct"]),
        bins=[-999, 0.03, 0.035, 0.04, 999],
        labels=["lt_3pct", "3_to_3.5pct", "3.5_to_4pct", "gte_4pct"],
        right=False,
    ).astype(str)
    return out


def _group_summaries(sample_df: pd.DataFrame, sample_name: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rows.append(_summarize(sample_df, "all", "all", sample_name))
    group_cols = [
        "entry_year",
        "entry_half",
        "entry_month",
        "market_regime",
        "exit_reason",
        "entry_trigger_reason",
        "entry_gap_bucket",
        "v_accel_bucket",
        "value_bucket",
        "atr_bucket",
    ]
    for col in group_cols:
        if col not in sample_df.columns:
            continue
        for value, part in sample_df.groupby(col, dropna=False, sort=True):
            rows.append(_summarize(part.copy(), col, str(value), sample_name))
    return rows


def _feature_profile(sample_df: pd.DataFrame, sample_name: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    fields = [
        "entry_gap_pct",
        "signal_v_accel",
        "signal_value",
        "signal_atr_pct",
        "signal_rs",
        "signal_high_52w_gap",
        "signal_stretch",
        "score",
        "mfe_1d",
        "mae_1d",
        "entry_day_open_to_close_ret",
    ]
    outcome_masks = {
        "all": pd.Series(True, index=sample_df.index),
        "win": _num(sample_df["ret"]) > 0,
        "loss": _num(sample_df["ret"]) < 0,
        "stop": sample_df["exit_reason"].astype(str).str.upper() == "STOP",
    }
    for outcome, mask in outcome_masks.items():
        part = sample_df[mask.fillna(False)].copy()
        for field in fields:
            if field not in part.columns:
                continue
            values = _num(part[field]).dropna()
            rows.append(
                {
                    "sample": sample_name,
                    "outcome": outcome,
                    "field": field,
                    "n": int(len(values)),
                    "mean": float(values.mean()) if len(values) else None,
                    "median": float(values.median()) if len(values) else None,
                    "q25": float(values.quantile(0.25)) if len(values) else None,
                    "q75": float(values.quantile(0.75)) if len(values) else None,
                    "min": float(values.min()) if len(values) else None,
                    "max": float(values.max()) if len(values) else None,
                }
            )
    return rows


def _concentration(sample_df: pd.DataFrame, sample_name: str) -> dict[str, Any]:
    ret = _num(sample_df["ret"]).dropna()
    wins = ret[ret > 0].sort_values(ascending=False)
    gross_profit = float(wins.sum())
    ret_sum = float(ret.sum())
    return {
        "sample": sample_name,
        "n": int(len(sample_df)),
        "ret_sum": ret_sum,
        "gross_profit": gross_profit,
        "top1_positive_ret": float(wins.iloc[0]) if len(wins) else 0.0,
        "top2_positive_ret": float(wins.iloc[:2].sum()) if len(wins) else 0.0,
        "top5_positive_ret": float(wins.iloc[:5].sum()) if len(wins) else 0.0,
        "top2_share_of_gross_profit": float(wins.iloc[:2].sum() / gross_profit) if gross_profit > 0 and len(wins) else None,
        "top5_share_of_gross_profit": float(wins.iloc[:5].sum() / gross_profit) if gross_profit > 0 and len(wins) else None,
        "ret_sum_without_top2": float(ret_sum - wins.iloc[:2].sum()) if len(wins) else ret_sum,
        "ret_sum_without_top5": float(ret_sum - wins.iloc[:5].sum()) if len(wins) else ret_sum,
    }


def _classify(groups: pd.DataFrame, concentration: list[dict[str, Any]]) -> dict[str, Any]:
    gvv = groups[(groups["sample"] == "gap_vaccel_value500m") & (groups["group_type"] == "entry_half")].copy()
    gv = groups[(groups["sample"] == "gap_vaccel") & (groups["group_type"] == "entry_half")].copy()
    reasons: list[str] = []
    for sample_name, part in [("gap_vaccel_value500m", gvv), ("gap_vaccel", gv)]:
        positive = part[part["ret_sum"] > 0]
        negative = part[part["ret_sum"] < 0]
        if len(positive):
            reasons.append(f"{sample_name}_has_positive_halves:{','.join(positive['group_value'].astype(str).tolist())}")
        if len(negative):
            reasons.append(f"{sample_name}_has_negative_halves:{','.join(negative['group_value'].astype(str).tolist())}")
    conc = {row["sample"]: row for row in concentration}
    for sample_name in ["gap_vaccel_value500m", "gap_vaccel"]:
        row = conc.get(sample_name, {})
        if float(row.get("top5_share_of_gross_profit") or 0.0) > 0.45:
            reasons.append(f"{sample_name}_profit_concentrated_top5")
    return {
        "operational_decision": "NOT_APPROVED",
        "research_classification": "C3_HIGHVOL_BROADER_PROFILE_MIXED_BY_PERIOD",
        "confidence": "MEDIUM_RESEARCH_ONLY",
        "full_logic_application": "NOT_APPLIED",
        "reason": reasons or ["insufficient_evidence"],
    }


def _clean_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def main() -> int:
    if not INPUT_TRADES.exists():
        raise FileNotFoundError(INPUT_TRADES)
    df = pd.read_csv(INPUT_TRADES, dtype={"code": str})
    target = df[df["variant"].astype(str).eq(TARGET_VARIANT)].copy()
    target = _add_time_fields(_assign_sample(_bucket_features(target)))

    samples = {
        "gap_vaccel": target[target["sample_gap_vaccel"]].copy(),
        "gap_vaccel_value500m": target[target["sample_gap_vaccel_value500m"]].copy(),
    }
    group_rows: list[dict[str, Any]] = []
    feature_rows: list[dict[str, Any]] = []
    concentration_rows: list[dict[str, Any]] = []
    row_parts: list[pd.DataFrame] = []
    for sample_name, sample_df in samples.items():
        sample_df = sample_df.copy()
        sample_df.insert(0, "profile_sample", sample_name)
        row_parts.append(sample_df)
        group_rows.extend(_group_summaries(sample_df, sample_name))
        feature_rows.extend(_feature_profile(sample_df, sample_name))
        concentration_rows.append(_concentration(sample_df, sample_name))

    groups = pd.DataFrame(group_rows)
    features = pd.DataFrame(feature_rows)
    rows = pd.concat(row_parts, ignore_index=True) if row_parts else pd.DataFrame()
    classification = _classify(groups, concentration_rows)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_profile_of_broader_c3_highvol_sample",
        "input": str(INPUT_TRADES),
        "target_variant": TARGET_VARIANT,
        "thresholds": THRESHOLDS,
        "target_variant_rows": int(len(target)),
        "sample_rows": {name: int(len(part)) for name, part in samples.items()},
        "classification": classification,
        "concentration": concentration_rows,
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_broker_changed": False,
            "gate_or_threshold_changed": False,
            "policy_changed": False,
            "full_logic_application": "NOT_APPLIED",
        },
        "groups": _clean_records(groups),
        "features": _clean_records(features),
    }

    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    groups.to_csv(OUT_GROUPS, index=False, encoding="utf-8-sig")
    features.to_csv(OUT_FEATURES, index=False, encoding="utf-8-sig")
    rows.to_csv(OUT_ROWS, index=False, encoding="utf-8-sig")

    focus = groups[
        (groups["sample"].isin(["gap_vaccel", "gap_vaccel_value500m"]))
        & (groups["group_type"].isin(["all", "entry_half", "market_regime", "exit_reason"]))
    ].copy()
    lines = [
        "# C3 high-volatility broader sample profile",
        "",
        f"- Generated at: {payload['generated_at']}",
        f"- Input: `{INPUT_TRADES}`",
        f"- Target variant: `{TARGET_VARIANT}`",
        f"- Target variant rows: {len(target)}",
        f"- gap_vaccel rows: {len(samples['gap_vaccel'])}",
        f"- gap_vaccel_value500m rows: {len(samples['gap_vaccel_value500m'])}",
        "- Scope: read-only profile; no operating rule or gate changed.",
        "",
        "## Focus groups",
        "",
        "| sample | group_type | group_value | n | win | loss | stop | ret_sum | PF |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in focus.to_dict(orient="records"):
        pf = row["profit_factor"]
        if pd.isna(pf):
            pf_text = "inf" if row["ret_sum"] > 0 and row["loss_n"] == 0 else ""
        else:
            pf_text = f"{pf:.6f}"
        lines.append(
            f"| {row['sample']} | {row['group_type']} | {row['group_value']} | {row['n']} | {row['win_n']} | "
            f"{row['loss_n']} | {row['stop_n']} | {row['ret_sum']:.6f} | {pf_text} |"
        )
    lines.extend(
        [
            "",
            "## Concentration",
            "",
        ]
    )
    for row in concentration_rows:
        lines.append(
            f"- {row['sample']}: ret_sum={row['ret_sum']:.6f}, top5_share_of_gross_profit="
            f"{row['top5_share_of_gross_profit']:.6f}, ret_sum_without_top5={row['ret_sum_without_top5']:.6f}"
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
                "json": str(OUT_JSON),
                "groups": str(OUT_GROUPS),
                "features": str(OUT_FEATURES),
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
