from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CANDIDATES = LOG_DIR / "new_method_candidates_for_replay_latest.csv"
INTRADAY = LOG_DIR / "intraday_prices_latest.csv"
BRANCH_REPLAY_SUMMARY = LOG_DIR / "new_method_followthrough_branch_replay_summary_latest.csv"

OUT_JSON = LOG_DIR / "new_method_intraday_coverage_latest.json"
OUT_DETAIL = LOG_DIR / "new_method_intraday_coverage_detail_latest.csv"
OUT_BRANCH = LOG_DIR / "new_method_intraday_coverage_branch_latest.csv"
OUT_MISSING = LOG_DIR / "new_method_intraday_coverage_missing_latest.csv"
OUT_MD = LOG_DIR / "new_method_intraday_coverage_latest.md"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _parse_candidate_date(value: Any) -> pd.Timestamp:
    return pd.to_datetime(str(value), errors="coerce")


def _parse_intraday_date(value: Any) -> pd.Timestamp:
    raw = str(value)
    if len(raw) == 8 and raw.isdigit():
        return pd.to_datetime(raw, format="%Y%m%d", errors="coerce")
    return pd.to_datetime(raw, errors="coerce")


def _coverage_reason(row: pd.Series, latest_candidate_date: pd.Timestamp) -> str:
    if bool(row["in_intraday"]):
        return "JOINED_TO_INTRADAY_SNAPSHOT"
    if row["candidate_date_ts"] < latest_candidate_date:
        return "HISTORICAL_BRANCH_NOT_CURRENT_INTRADAY_TARGET"
    return "CURRENT_BRANCH_CODE_NOT_IN_INTRADAY_UNIVERSE"


def _coverage_class(row: pd.Series) -> str:
    if bool(row["in_intraday"]):
        return "JOINED"
    if row["coverage_reason"] == "HISTORICAL_BRANCH_NOT_CURRENT_INTRADAY_TARGET":
        return "CANDIDATE_TIMING_MISMATCH"
    return "INTRADAY_UNIVERSE_GAP"


def main() -> int:
    if not CANDIDATES.exists():
        raise FileNotFoundError(CANDIDATES)
    if not INTRADAY.exists():
        raise FileNotFoundError(INTRADAY)

    cand = pd.read_csv(CANDIDATES, dtype={"code": "string"})
    rt = pd.read_csv(INTRADAY, dtype={"code": "string"})
    cand["code"] = cand["code"].astype("string").str.zfill(6)
    rt["code"] = rt["code"].astype("string").str.zfill(6)
    cand["candidate_date_ts"] = cand["date"].map(_parse_candidate_date)
    rt["intraday_date_ts"] = rt["date"].map(_parse_intraday_date) if "date" in rt.columns else pd.NaT

    latest_candidate_date = cand["candidate_date_ts"].max()
    latest_intraday_date = rt["intraday_date_ts"].max()
    intraday_codes = set(rt["code"].dropna().astype(str))

    detail = cand.copy()
    detail["branch_key"] = (
        detail["date"].astype(str)
        + "|"
        + detail["market_regime"].astype(str)
        + "|"
        + detail["method_branch"].astype(str)
        + "|"
        + detail["horizon"].astype(str)
    )
    detail["in_intraday"] = detail["code"].astype(str).isin(intraday_codes)
    detail["intraday_date"] = latest_intraday_date.strftime("%Y-%m-%d") if pd.notna(latest_intraday_date) else ""
    detail["candidate_vs_intraday_lag_days"] = (
        latest_intraday_date - detail["candidate_date_ts"]
    ).dt.days
    detail["coverage_reason"] = detail.apply(_coverage_reason, axis=1, latest_candidate_date=latest_candidate_date)
    detail["coverage_class"] = detail.apply(_coverage_class, axis=1)

    rt_keep = [
        c
        for c in [
            "code",
            "current_price",
            "open",
            "high",
            "low",
            "trading_value",
            "ts",
        ]
        if c in rt.columns
    ]
    detail = detail.merge(rt[rt_keep], on="code", how="left", suffixes=("", "_intraday"))

    keep_detail = [
        "branch_key",
        "date",
        "intraday_date",
        "candidate_vs_intraday_lag_days",
        "code",
        "name",
        "market",
        "market_regime",
        "method_branch",
        "horizon",
        "in_intraday",
        "coverage_class",
        "coverage_reason",
        "forward_return_status",
        "promotion_blocker",
        "candidate_origin",
        "close",
        "value",
        "score",
        "final_score",
        "current_price",
        "open",
        "high",
        "low",
        "trading_value",
        "ts",
    ]
    keep_detail = [c for c in keep_detail if c in detail.columns]
    detail_out = detail[keep_detail].sort_values(["date", "method_branch", "in_intraday", "code"], ascending=[True, True, False, True])

    branch = (
        detail.groupby(["date", "market_regime", "method_branch", "horizon"], dropna=False)
        .agg(
            candidate_rows=("code", "size"),
            unique_codes=("code", "nunique"),
            joined_rows=("in_intraday", "sum"),
            missing_rows=("in_intraday", lambda s: int((~s).sum())),
            timing_mismatch_rows=("coverage_class", lambda s: int((s == "CANDIDATE_TIMING_MISMATCH").sum())),
            intraday_universe_gap_rows=("coverage_class", lambda s: int((s == "INTRADAY_UNIVERSE_GAP").sum())),
        )
        .reset_index()
    )
    branch["coverage_rate"] = branch["joined_rows"] / branch["candidate_rows"]

    if BRANCH_REPLAY_SUMMARY.exists():
        replay = pd.read_csv(BRANCH_REPLAY_SUMMARY)
        replay_cols = [
            c
            for c in [
                "date",
                "market_regime",
                "method_branch",
                "horizon",
                "followthrough_status",
                "alerts_count",
                "entry_allowed_validation_count",
                "raw_pseudo_intraday_candidate_count",
            ]
            if c in replay.columns
        ]
        if replay_cols:
            branch = branch.merge(replay[replay_cols], on=["date", "market_regime", "method_branch", "horizon"], how="left")

    missing = detail_out.loc[~detail_out["in_intraday"]].copy()

    detail_out.to_csv(OUT_DETAIL, index=False, encoding="utf-8-sig")
    branch.to_csv(OUT_BRANCH, index=False, encoding="utf-8-sig")
    missing.to_csv(OUT_MISSING, index=False, encoding="utf-8-sig")

    reason_counts = detail["coverage_reason"].value_counts(dropna=False).to_dict()
    class_counts = detail["coverage_class"].value_counts(dropna=False).to_dict()
    payload: dict[str, Any] = {
        "generated_at": _now(),
        "classification": "NEW_METHOD_INTRADAY_COVERAGE_ANALYSIS",
        "operation_effect": "READ_ONLY_COVERAGE_ANALYSIS_ONLY",
        "candidates": str(CANDIDATES),
        "intraday": str(INTRADAY),
        "candidate_rows": int(len(cand)),
        "candidate_unique_codes": int(cand["code"].nunique()),
        "intraday_rows": int(len(rt)),
        "intraday_unique_codes": int(rt["code"].nunique()),
        "candidate_dates": sorted(str(v) for v in cand["date"].dropna().unique()),
        "intraday_dates": sorted(str(v) for v in rt["date"].dropna().unique()) if "date" in rt.columns else [],
        "latest_candidate_date": latest_candidate_date.strftime("%Y-%m-%d") if pd.notna(latest_candidate_date) else "",
        "latest_intraday_date": latest_intraday_date.strftime("%Y-%m-%d") if pd.notna(latest_intraday_date) else "",
        "joined_rows": int(detail["in_intraday"].sum()),
        "missing_rows": int((~detail["in_intraday"]).sum()),
        "joined_unique_codes": int(detail.loc[detail["in_intraday"], "code"].nunique()),
        "missing_unique_codes": int(detail.loc[~detail["in_intraday"], "code"].nunique()),
        "coverage_rate": float(detail["in_intraday"].mean()) if len(detail) else 0.0,
        "coverage_class_counts": {str(k): int(v) for k, v in class_counts.items()},
        "coverage_reason_counts": {str(k): int(v) for k, v in reason_counts.items()},
        "operational_candidate_files_written": False,
        "detail_csv": str(OUT_DETAIL),
        "branch_csv": str(OUT_BRANCH),
        "missing_csv": str(OUT_MISSING),
        "branch_summary": branch.to_dict(orient="records"),
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# New Method Intraday Coverage",
        "",
        f"- classification: {payload['classification']}",
        f"- operation_effect: {payload['operation_effect']}",
        f"- candidate_rows: {payload['candidate_rows']}",
        f"- candidate_unique_codes: {payload['candidate_unique_codes']}",
        f"- intraday_rows: {payload['intraday_rows']}",
        f"- intraday_unique_codes: {payload['intraday_unique_codes']}",
        f"- candidate_dates: {payload['candidate_dates']}",
        f"- intraday_dates: {payload['intraday_dates']}",
        f"- joined_rows: {payload['joined_rows']}",
        f"- missing_rows: {payload['missing_rows']}",
        f"- coverage_rate: {payload['coverage_rate']:.6f}",
        f"- coverage_class_counts: {payload['coverage_class_counts']}",
        f"- operational_candidate_files_written: {payload['operational_candidate_files_written']}",
        "",
        "## Branch Coverage",
    ]
    for row in branch.to_dict(orient="records"):
        lines.append(
            "- {date} / {market_regime} / {method_branch} / {horizon}: "
            "rows={candidate_rows}, joined={joined_rows}, missing={missing_rows}, "
            "timing_mismatch={timing_mismatch_rows}, intraday_universe_gap={intraday_universe_gap_rows}, "
            "coverage_rate={coverage_rate:.6f}".format(**row)
        )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
