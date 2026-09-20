from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

INPUT_CSV = LOG_DIR / "surge_transition_probe_review_candidates_latest.csv"
OUT_JSON = LOG_DIR / "surge_probe_review_cohort_diagnostic_latest.json"
OUT_CSV = LOG_DIR / "surge_probe_review_cohort_diagnostic_latest.csv"
OUT_COLUMNS = [
    "code",
    "name",
    "state",
    "precursor_stage",
    "intraday_stage",
    "ret_10m_pct",
    "outcome_status_10m",
    "risk_tags",
    "risk_bucket",
    "suggested_review_action",
]

NEGATIVE_RISK_FLAGS = [
    "ENTRY_BLOCKED_OR_POLICY_EXCLUDED",
    "HIGH_REJECTION_DRAWDOWN",
    "OPEN_TO_CURRENT_FADE",
    "PRICE_UP_WITH_WEAK_RVOL",
    "SHALLOW_ASK_DEPTH",
    "UPPER_WICK_HEAVY",
    "WIDE_SPREAD",
]


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _flag_text(row: pd.Series) -> str:
    return "|".join(
        str(row.get(col) or "")
        for col in ["precursor_risk_flags", "intraday_risk_flags"]
        if pd.notna(row.get(col))
    )


def _risk_tags(row: pd.Series) -> list[str]:
    text = _flag_text(row)
    tags = [flag for flag in NEGATIVE_RISK_FLAGS if flag in text]
    try:
        spread = float(row.get("spread_bps") or 0.0)
    except Exception:
        spread = 0.0
    try:
        ask_depth = float(row.get("ask_depth_levels") or 0.0)
    except Exception:
        ask_depth = 0.0
    try:
        rvol = float(row.get("rvol20") or 0.0)
    except Exception:
        rvol = 0.0
    if spread > 15 and "WIDE_SPREAD" not in tags:
        tags.append("WIDE_SPREAD")
    if ask_depth < 3 and "SHALLOW_ASK_DEPTH" not in tags:
        tags.append("SHALLOW_ASK_DEPTH")
    if rvol < 0.2:
        tags.append("LOW_RVOL_CONTEXT")
    return tags


def _risk_bucket(tags: list[str]) -> str:
    severe = {"ENTRY_BLOCKED_OR_POLICY_EXCLUDED", "HIGH_REJECTION_DRAWDOWN", "WIDE_SPREAD"}
    if any(tag in severe for tag in tags):
        return "HIGH_RISK_REVIEW_ONLY"
    if len(tags) >= 3:
        return "MEDIUM_RISK_MULTI_FLAG"
    if "SHALLOW_ASK_DEPTH" in tags or "PRICE_UP_WITH_WEAK_RVOL" in tags:
        return "MICROSTRUCTURE_RISK"
    if tags:
        return "LIGHT_RISK_FLAG"
    return "CLEAN_REVIEW"


def _to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return json.loads(df.where(pd.notna(df), None).to_json(orient="records", force_ascii=False))


def _group(df: pd.DataFrame, keys: list[str]) -> list[dict[str, Any]]:
    if df.empty:
        return []
    grouped = (
        df.groupby(keys, dropna=False)
        .agg(
            rows=("code", "count"),
            codes=("code", "nunique"),
            avg_ret_10m_pct=("ret_10m_pct", "mean"),
            median_ret_10m_pct=("ret_10m_pct", "median"),
            min_ret_10m_pct=("ret_10m_pct", "min"),
            max_ret_10m_pct=("ret_10m_pct", "max"),
            positive=("outcome_status_10m", lambda x: int((x == "POSITIVE_MARKOUT").sum())),
            negative=("outcome_status_10m", lambda x: int((x == "NEGATIVE_MARKOUT").sum())),
            flat=("outcome_status_10m", lambda x: int((x == "FLAT_MARKOUT").sum())),
        )
        .reset_index()
        .sort_values(["avg_ret_10m_pct", "rows"], ascending=[False, False])
    )
    return _to_records(grouped)


def build_report(input_csv: Path) -> tuple[dict[str, Any], pd.DataFrame]:
    df = _read_csv(input_csv)
    if df.empty:
        return {"status": "NO_INPUT_ROWS", "input_csv": str(input_csv)}, pd.DataFrame()

    for col in ["paper_order_route", "broker_order_route", "dispatch_enabled", "trading_allowed", "policy_change"]:
        if col not in df.columns:
            df[col] = False

    review = df[df["review_class"] == "REVIEW_PROBE_CANDIDATE"].copy()
    if review.empty:
        return {
            "status": "NO_REVIEW_PROBE_CANDIDATES",
            "input_csv": str(input_csv),
            "total_rows": int(len(df)),
            "review_rows": 0,
            "trading_allowed": False,
            "dispatch_enabled": False,
            "policy_change": False,
        }, pd.DataFrame()

    review["risk_tags"] = review.apply(lambda row: "|".join(_risk_tags(row)), axis=1)
    review["risk_bucket"] = review["risk_tags"].apply(lambda value: _risk_bucket(str(value).split("|") if value else []))
    review["suggested_review_action"] = review["risk_bucket"].map(
        {
            "CLEAN_REVIEW": "KEEP_FOR_REVIEW",
            "LIGHT_RISK_FLAG": "KEEP_FOR_REVIEW_SMALL_SAMPLE",
            "MICROSTRUCTURE_RISK": "REVIEW_LOB_QUALITY_FIRST",
            "MEDIUM_RISK_MULTI_FLAG": "OBSERVE_MORE_BEFORE_PROBE",
            "HIGH_RISK_REVIEW_ONLY": "EXCLUDE_FROM_PROBE_FOR_NOW",
        }
    ).fillna("OBSERVE_MORE_BEFORE_PROBE")

    counts = {str(k): int(v) for k, v in df["review_class"].value_counts().sort_index().items()}
    report = {
        "status": "PASS",
        "scope": "surge_probe_review_cohort_diagnostic",
        "input_csv": str(input_csv),
        "total_rows": int(len(df)),
        "review_rows": int(len(review)),
        "review_codes": int(review["code"].astype(str).nunique()),
        "review_class_counts": counts,
        "trading_allowed": False,
        "dispatch_enabled": False,
        "policy_change": False,
        "route_flags_unique": {
            col: sorted(str(v) for v in df[col].dropna().unique().tolist())
            for col in ["paper_order_route", "broker_order_route", "dispatch_enabled", "trading_allowed", "policy_change"]
        },
        "by_risk_bucket": _group(review, ["risk_bucket"]),
        "by_stage_pair": _group(review, ["precursor_stage", "intraday_stage"]),
        "by_action": _group(review, ["suggested_review_action"]),
        "top_positive": _to_records(review.sort_values("ret_10m_pct", ascending=False).head(10)),
        "top_negative": _to_records(review.sort_values("ret_10m_pct", ascending=True).head(10)),
        "notes": [
            "This is a read-only diagnostic for expected-value review, not a trading approval.",
            "Late or waiting 10m samples are not included in REVIEW_PROBE_CANDIDATE diagnostics.",
        ],
    }
    return report, review


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", default=str(INPUT_CSV))
    parser.add_argument("--output-json", default=str(OUT_JSON))
    parser.add_argument("--output-csv", default=str(OUT_CSV))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    report, rows = build_report(Path(args.input_csv))
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
    if rows.empty:
        rows = pd.DataFrame(columns=OUT_COLUMNS)
    rows.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(
        "[SURGE_PROBE_REVIEW_COHORT_DIAGNOSTIC] "
        f"status={report.get('status')} review_rows={report.get('review_rows', 0)} "
        f"json={out_json} csv={out_csv}"
    )
    return 0 if str(report.get("status") or "").startswith("NO_") or report.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
