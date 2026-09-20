from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

HIGH_LOCATION_FLAGS = {
    "OPEN_TO_CURRENT_GE_4PCT",
    "CURRENT_NEAR_INTRADAY_HIGH",
    "DAY_RANGE_EXPANSION_GE_12PCT",
    "DAILY_MOVE_ALREADY_EXTENDED",
}

REVIEW_STAGE_PAIRS = {
    ("WATCH", "WATCH"),
    ("REJECT_PRECURSOR", "WATCH"),
}


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, encoding="utf-8-sig")


def _code_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)


def _flag_text(row: pd.Series) -> str:
    parts = []
    for col in ["precursor_reasons", "precursor_risk_flags", "intraday_reasons", "intraday_risk_flags"]:
        val = row.get(col)
        if pd.notna(val):
            parts.append(str(val))
    return "|".join(parts)


def _has_any_flag(text: str, flags: set[str]) -> bool:
    return any(flag in text for flag in flags)


def _classification(row: pd.Series) -> tuple[str, str]:
    sample_quality = str(row.get("sample_quality") or "")
    if sample_quality == "NO_SAMPLE":
        return "WAIT_10M_SAMPLE", "10m markout sample is not ready yet"
    if sample_quality == "LATE_SAMPLE":
        return "EXCLUDE_LATE_10M_SAMPLE", "10m markout sample exists but arrived outside on-time quality window"
    if sample_quality != "ON_TIME_SAMPLE":
        return "EXCLUDE_UNKNOWN_SAMPLE_QUALITY", f"unsupported 10m sample_quality={sample_quality}"

    precursor_stage = str(row.get("precursor_stage") or "UNKNOWN")
    intraday_stage = str(row.get("intraday_stage") or "UNKNOWN")
    state = str(row.get("state") or "UNKNOWN")
    flag_text = _flag_text(row)

    if _has_any_flag(flag_text, HIGH_LOCATION_FLAGS):
        return "EXCLUDE_HIGH_LOCATION_CHASE", "high-location chase or extended move flag present"

    if intraday_stage == "REJECT_INTRADAY" or state == "REJECT_INTRADAY":
        return "EXCLUDE_INTRADAY_REJECT", "intraday layer rejected the candidate"

    if (precursor_stage, intraday_stage) in REVIEW_STAGE_PAIRS:
        return "REVIEW_PROBE_CANDIDATE", "stage pair is selected for read-only probe review and has no high-location exclusion flag"

    return "OBSERVE_ONLY_OTHER", "not in current review stage-pair set"


def _summarize(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"rows": 0}
    by_class = (
        df.groupby("review_class", dropna=False)
        .agg(
            rows=("code", "count"),
            codes=("code", "nunique"),
            avg_ret_10m_pct=("ret_10m_pct", "mean"),
            median_ret_10m_pct=("ret_10m_pct", "median"),
            positive=("outcome_status_10m", lambda x: int((x == "POSITIVE_MARKOUT").sum())),
            negative=("outcome_status_10m", lambda x: int((x == "NEGATIVE_MARKOUT").sum())),
            flat=("outcome_status_10m", lambda x: int((x == "FLAT_MARKOUT").sum())),
        )
        .reset_index()
        .sort_values(["avg_ret_10m_pct", "rows"], ascending=[False, False])
    )
    by_pair = (
        df.groupby(["precursor_stage", "intraday_stage", "review_class"], dropna=False)
        .agg(
            rows=("code", "count"),
            avg_ret_10m_pct=("ret_10m_pct", "mean"),
            positive=("outcome_status_10m", lambda x: int((x == "POSITIVE_MARKOUT").sum())),
            negative=("outcome_status_10m", lambda x: int((x == "NEGATIVE_MARKOUT").sum())),
            flat=("outcome_status_10m", lambda x: int((x == "FLAT_MARKOUT").sum())),
        )
        .reset_index()
        .sort_values(["avg_ret_10m_pct", "rows"], ascending=[False, False])
    )
    return {
        "rows": int(len(df)),
        "codes": int(df["code"].nunique()),
        "review_class_counts": {str(k): int(v) for k, v in df["review_class"].value_counts().sort_index().items()},
        "by_review_class": json.loads(by_class.to_json(orient="records", force_ascii=False)),
        "by_stage_pair": json.loads(by_pair.to_json(orient="records", force_ascii=False)),
    }


def build_candidates(outcome_path: Path, shadow_path: Path) -> tuple[dict[str, Any], pd.DataFrame]:
    outcome = _read_csv(outcome_path)
    shadow = _read_csv(shadow_path)
    if outcome.empty:
        return {"status": "NO_OUTCOME_ROWS", "rows": 0}, pd.DataFrame()

    outcome["code"] = _code_series(outcome["code"])
    outcome_10 = outcome[outcome["target_minutes"] == 10].copy()
    if outcome_10.empty:
        return {"status": "NO_10M_OUTCOME_ROWS", "rows": 0}, pd.DataFrame()

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
        outcome_10 = outcome_10.merge(
            shadow[merge_cols].drop_duplicates("code"),
            on="code",
            how="left",
            suffixes=("", "_shadow"),
        )
        for col in ["precursor_stage", "intraday_stage"]:
            shadow_col = f"{col}_shadow"
            if shadow_col in outcome_10.columns:
                outcome_10[col] = outcome_10[shadow_col].combine_first(outcome_10.get(col))

    outcome_10["ret_10m_pct"] = outcome_10["ret_pct"]
    outcome_10["outcome_status_10m"] = outcome_10["outcome_status"]
    classified = outcome_10.apply(_classification, axis=1, result_type="expand")
    outcome_10["review_class"] = classified[0]
    outcome_10["review_reason"] = classified[1]
    outcome_10["paper_order_route"] = False
    outcome_10["broker_order_route"] = False
    outcome_10["dispatch_enabled"] = False
    outcome_10["trading_allowed"] = False
    outcome_10["policy_change"] = False

    keep_cols = [
        "code",
        "name",
        "state",
        "precursor_stage",
        "intraday_stage",
        "review_class",
        "review_reason",
        "ret_10m_pct",
        "outcome_status_10m",
        "sample_quality",
        "change_pct",
        "rvol20",
        "surge_score_final",
        "spread_bps",
        "ask_depth_levels",
        "precursor_risk_flags",
        "intraday_risk_flags",
        "paper_order_route",
        "broker_order_route",
        "dispatch_enabled",
        "trading_allowed",
        "policy_change",
    ]
    keep_cols = [col for col in keep_cols if col in outcome_10.columns]
    rows = outcome_10[keep_cols].copy()
    rows = rows.sort_values(["review_class", "ret_10m_pct"], ascending=[True, False])

    report = {
        "status": "PASS",
        "scope": "surge_transition_probe_review_candidates",
        "outcome_path": str(outcome_path),
        "shadow_path": str(shadow_path),
        "target_minutes": 10,
        "trading_allowed": False,
        "dispatch_enabled": False,
        "policy_change": False,
        "high_location_exclusion_flags": sorted(HIGH_LOCATION_FLAGS),
        "review_stage_pairs": [list(pair) for pair in sorted(REVIEW_STAGE_PAIRS)],
        "summary": _summarize(rows),
    }
    return report, rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outcome", default=str(LOG_DIR / "surge_state_transition_outcome_latest.csv"))
    parser.add_argument("--shadow", default=str(LOG_DIR / "surge_state_machine_shadow_latest.csv"))
    parser.add_argument("--output-json", default=str(LOG_DIR / "surge_transition_probe_review_candidates_latest.json"))
    parser.add_argument("--output-csv", default=str(LOG_DIR / "surge_transition_probe_review_candidates_latest.csv"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    report, rows = build_candidates(Path(args.outcome), Path(args.shadow))
    if args.dry_run:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        if not rows.empty:
            print(rows.head(20).to_string(index=False))
        return 0 if report.get("status") == "PASS" else 2

    out_json = Path(args.output_json)
    out_csv = Path(args.output_csv)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    rows.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(
        "[SURGE_TRANSITION_PROBE_REVIEW] "
        f"status={report.get('status')} rows={report.get('summary', {}).get('rows', 0)} "
        f"json={out_json} csv={out_csv}"
    )
    return 0 if report.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
