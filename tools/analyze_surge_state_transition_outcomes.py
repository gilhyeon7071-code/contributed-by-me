from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, encoding="utf-8-sig")


def _code_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)


def _status_counts(series: pd.Series) -> dict[str, int]:
    return {str(k): int(v) for k, v in series.fillna("UNKNOWN").value_counts().sort_index().items()}


def _group_summary(df: pd.DataFrame, keys: list[str]) -> list[dict[str, Any]]:
    if df.empty:
        return []
    rows: list[dict[str, Any]] = []
    grouped = df.groupby(keys, dropna=False)
    for group_key, g in grouped:
        if not isinstance(group_key, tuple):
            group_key = (group_key,)
        row: dict[str, Any] = {key: (None if pd.isna(value) else str(value)) for key, value in zip(keys, group_key)}
        row.update(
            {
                "n": int(len(g)),
                "codes": int(g["code"].nunique()),
                "avg_ret_pct": float(g["ret_pct"].mean()),
                "median_ret_pct": float(g["ret_pct"].median()),
                "min_ret_pct": float(g["ret_pct"].min()),
                "max_ret_pct": float(g["ret_pct"].max()),
                "positive": int((g["outcome_status"] == "POSITIVE_MARKOUT").sum()),
                "negative": int((g["outcome_status"] == "NEGATIVE_MARKOUT").sum()),
                "flat": int((g["outcome_status"] == "FLAT_MARKOUT").sum()),
                "late_sample": int((g["sample_quality"] == "LATE_SAMPLE").sum()),
                "on_time_sample": int((g["sample_quality"] == "ON_TIME_SAMPLE").sum()),
            }
        )
        rows.append(row)
    rows.sort(key=lambda item: (item["avg_ret_pct"], item["n"]), reverse=True)
    return rows


def _top_rows(df: pd.DataFrame, ascending: bool, limit: int) -> list[dict[str, Any]]:
    cols = [
        "code",
        "name",
        "state",
        "precursor_stage",
        "intraday_stage",
        "ret_pct",
        "change_pct",
        "rvol20",
        "surge_score_final",
        "sample_quality",
        "precursor_risk_flags",
        "intraday_risk_flags",
    ]
    existing = [col for col in cols if col in df.columns]
    out = df.sort_values("ret_pct", ascending=ascending).head(limit)[existing].copy()
    return json.loads(out.where(pd.notna(out), None).to_json(orient="records", force_ascii=False))


def _flag_impact(df: pd.DataFrame, flags: list[str]) -> list[dict[str, Any]]:
    text_cols = [
        col
        for col in ["precursor_reasons", "precursor_risk_flags", "intraday_reasons", "intraday_risk_flags"]
        if col in df.columns
    ]
    rows: list[dict[str, Any]] = []
    for flag in flags:
        mask = pd.Series(False, index=df.index)
        for col in text_cols:
            mask = mask | df[col].astype(str).str.contains(flag, na=False)
        if not mask.any() or not (~mask).any():
            continue
        with_df = df.loc[mask]
        without_df = df.loc[~mask]
        rows.append(
            {
                "flag": flag,
                "with_n": int(len(with_df)),
                "with_avg_ret_pct": float(with_df["ret_pct"].mean()),
                "with_positive": int((with_df["outcome_status"] == "POSITIVE_MARKOUT").sum()),
                "with_negative": int((with_df["outcome_status"] == "NEGATIVE_MARKOUT").sum()),
                "without_n": int(len(without_df)),
                "without_avg_ret_pct": float(without_df["ret_pct"].mean()),
                "without_positive": int((without_df["outcome_status"] == "POSITIVE_MARKOUT").sum()),
                "without_negative": int((without_df["outcome_status"] == "NEGATIVE_MARKOUT").sum()),
            }
        )
    rows.sort(key=lambda item: item["with_avg_ret_pct"] - item["without_avg_ret_pct"])
    return rows


def build_report(outcome_path: Path, shadow_path: Path) -> dict[str, Any]:
    outcome = _read_csv(outcome_path)
    shadow = _read_csv(shadow_path)
    if outcome.empty:
        return {"status": "NO_OUTCOME_ROWS", "outcome_path": str(outcome_path), "shadow_path": str(shadow_path)}

    outcome["code"] = _code_series(outcome["code"])
    if not shadow.empty and "code" in shadow.columns:
        shadow["code"] = _code_series(shadow["code"])
        merge_cols = [
            "code",
            "precursor_stage",
            "intraday_stage",
            "precursor_reasons",
            "precursor_risk_flags",
            "intraday_reasons",
            "intraday_risk_flags",
        ]
        merge_cols = [col for col in merge_cols if col in shadow.columns]
        outcome = outcome.merge(
            shadow[merge_cols].drop_duplicates("code"),
            on="code",
            how="left",
            suffixes=("", "_shadow"),
        )
        for col in ["precursor_stage", "intraday_stage"]:
            shadow_col = f"{col}_shadow"
            if shadow_col in outcome.columns:
                outcome[col] = outcome[shadow_col].combine_first(outcome.get(col))

    flags = [
        "DAILY_MOVE_ALREADY_EXTENDED",
        "PRICE_UP_WITH_WEAK_RVOL",
        "SHALLOW_ASK_DEPTH",
        "CURRENT_NEAR_INTRADAY_HIGH",
        "CHANGE_GE_12PCT",
        "OPEN_TO_CURRENT_GE_4PCT",
        "DAY_RANGE_EXPANSION_GE_12PCT",
        "SPREAD_LE_15BPS",
    ]
    by_target: dict[str, Any] = {}
    for target in sorted(outcome["target_minutes"].dropna().unique()):
        target_df = outcome[outcome["target_minutes"] == target].copy()
        target_key = str(int(target))
        by_target[target_key] = {
            "rows": int(len(target_df)),
            "codes": int(target_df["code"].nunique()),
            "sample_quality_counts": _status_counts(target_df["sample_quality"]),
            "outcome_status_counts": _status_counts(target_df["outcome_status"]),
            "by_state": _group_summary(target_df, ["state"]),
            "by_stage_pair": _group_summary(target_df, ["precursor_stage", "intraday_stage"]),
        }
        if int(target) == 10:
            by_target[target_key]["top_positive"] = _top_rows(target_df, ascending=False, limit=12)
            by_target[target_key]["top_negative"] = _top_rows(target_df, ascending=True, limit=12)
            by_target[target_key]["flag_impact"] = _flag_impact(target_df, flags)

    target_10 = outcome[outcome["target_minutes"] == 10].copy()
    notes: list[str] = []
    if not target_10.empty and (target_10["sample_quality"] == "ON_TIME_SAMPLE").all():
        notes.append("10m samples are all ON_TIME_SAMPLE; use 10m as the primary live read.")
    target_5 = outcome[outcome["target_minutes"] == 5].copy()
    if not target_5.empty and (target_5["sample_quality"] == "LATE_SAMPLE").all():
        notes.append("5m samples are all LATE_SAMPLE; treat 5m returns as delayed-observation evidence.")

    return {
        "status": "PASS",
        "outcome_path": str(outcome_path),
        "shadow_path": str(shadow_path),
        "rows": int(len(outcome)),
        "codes": int(outcome["code"].nunique()),
        "targets": [int(x) for x in sorted(outcome["target_minutes"].dropna().unique())],
        "trading_allowed": False,
        "dispatch_enabled": False,
        "policy_change": False,
        "notes": notes,
        "by_target": by_target,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outcome", default=str(LOG_DIR / "surge_state_transition_outcome_latest.csv"))
    parser.add_argument("--shadow", default=str(LOG_DIR / "surge_state_machine_shadow_latest.csv"))
    parser.add_argument("--output-json", default=str(LOG_DIR / "surge_state_transition_outcome_analysis_latest.json"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    report = build_report(Path(args.outcome), Path(args.shadow))
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.dry_run:
        print(text)
        return 0 if report.get("status") == "PASS" else 2

    output_path = Path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text + "\n", encoding="utf-8")
    print(f"[SURGE_TRANSITION_ANALYSIS] status={report.get('status')} path={output_path}")
    return 0 if report.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
