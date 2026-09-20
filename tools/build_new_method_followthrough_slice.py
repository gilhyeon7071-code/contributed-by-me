from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
INPUT_CSV = LOG_DIR / "new_method_candidates_for_replay_latest.csv"
OUT_CSV = LOG_DIR / "new_method_candidates_for_followthrough_latest.csv"
OUT_META = LOG_DIR / "new_method_candidates_for_followthrough_meta.json"
OUT_MD = LOG_DIR / "new_method_candidates_for_followthrough_latest.md"

FOLLOWTHROUGH_COLUMNS = [
    "date",
    "code",
    "name",
    "market",
    "market_regime",
    "close",
    "value",
    "score",
    "final_score",
    "stretch",
    "atr14_pct",
    "rsi14",
    "high_52w_gap",
    "relax_level",
    "candidate_origin",
    "method_branch",
    "signal_reason",
    "horizon",
    "expected_holding_days",
    "observe_status",
    "promotion_blocker",
    "forward_return_status",
    "source",
    "new_method_rank_in_branch",
    "new_method_forward_return",
]

NUMERIC_COLUMNS = [
    "close",
    "value",
    "score",
    "final_score",
    "stretch",
    "atr14_pct",
    "rsi14",
    "high_52w_gap",
    "expected_holding_days",
    "new_method_rank_in_branch",
]

OPTIONAL_NUMERIC_COLUMNS = [
    "new_method_forward_return",
]


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _fail(message: str) -> int:
    payload = {
        "generated_at": _now(),
        "classification": "NEW_METHOD_FOLLOWTHROUGH_SLICE",
        "status": "FAILED",
        "message": message,
        "input": str(INPUT_CSV),
        "output": str(OUT_CSV),
        "operation_effect": "NO_OPERATIONAL_CANDIDATE_WRITE",
    }
    OUT_META.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_MD.write_text(f"# New Method Followthrough Slice\n\n- status: FAILED\n- message: {message}\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 1


def main() -> int:
    if not INPUT_CSV.exists():
        return _fail(f"missing input: {INPUT_CSV}")

    df = pd.read_csv(INPUT_CSV, dtype={"code": "string"})
    if df.empty:
        return _fail("input has no rows")

    missing = [c for c in FOLLOWTHROUGH_COLUMNS if c not in df.columns]
    if missing:
        return _fail("missing columns: " + ", ".join(missing))

    work = df.copy()
    work["date_key"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.dropna(subset=["date_key"])
    if work.empty:
        return _fail("no valid date rows")

    latest_date = work["date_key"].max()
    latest_rows = work.loc[work["date_key"].eq(latest_date)].copy()

    branch_counts = (
        latest_rows.groupby(["date", "market_regime", "method_branch", "horizon"], dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values(["rows", "method_branch"], ascending=[False, True])
    )
    selected_branch = branch_counts.iloc[0].to_dict()
    slice_df = latest_rows.loc[
        (latest_rows["date"].astype(str) == str(selected_branch["date"]))
        & (latest_rows["method_branch"].astype(str) == str(selected_branch["method_branch"]))
        & (latest_rows["horizon"].astype(str) == str(selected_branch["horizon"]))
    ].copy()

    for col in NUMERIC_COLUMNS + OPTIONAL_NUMERIC_COLUMNS:
        if col in slice_df.columns:
            slice_df[col] = pd.to_numeric(slice_df[col], errors="coerce")

    slice_df["code"] = slice_df["code"].astype("string").str.zfill(6)
    slice_df = slice_df.sort_values(["new_method_rank_in_branch", "code"], na_position="last")
    slice_df = slice_df[FOLLOWTHROUGH_COLUMNS]

    date_count = int(slice_df["date"].nunique())
    branch_count = int(slice_df["method_branch"].nunique())
    duplicate_code_rows = int(slice_df.duplicated(["code"], keep=False).sum())
    invalid_code_rows = int((slice_df["code"].astype("string").str.len() != 6).sum())
    non_numeric: dict[str, int] = {}
    for col in NUMERIC_COLUMNS:
        if col in slice_df.columns:
            non_numeric[col] = int(pd.to_numeric(slice_df[col], errors="coerce").isna().sum())
    non_numeric = {k: v for k, v in non_numeric.items() if v}
    optional_non_numeric: dict[str, int] = {}
    for col in OPTIONAL_NUMERIC_COLUMNS:
        if col in slice_df.columns:
            optional_non_numeric[col] = int(pd.to_numeric(slice_df[col], errors="coerce").isna().sum())
    optional_non_numeric = {k: v for k, v in optional_non_numeric.items() if v}

    ready_for_followthrough = (
        date_count == 1
        and branch_count == 1
        and duplicate_code_rows == 0
        and invalid_code_rows == 0
        and not non_numeric
    )

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    slice_df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    payload: dict[str, Any] = {
        "generated_at": _now(),
        "classification": "NEW_METHOD_FOLLOWTHROUGH_SLICE",
        "status": "READY_FOR_FOLLOWTHROUGH_REPLAY" if ready_for_followthrough else "NOT_READY",
        "input": str(INPUT_CSV),
        "output": str(OUT_CSV),
        "operation_effect": "NO_OPERATIONAL_CANDIDATE_WRITE",
        "selection_rule": "latest_date_then_largest_branch",
        "selected": {
            "date": str(selected_branch["date"]),
            "market_regime": str(selected_branch["market_regime"]),
            "method_branch": str(selected_branch["method_branch"]),
            "horizon": str(selected_branch["horizon"]),
        },
        "row_count": int(len(slice_df)),
        "date_count": date_count,
        "branch_count": branch_count,
        "duplicate_code_rows": duplicate_code_rows,
        "invalid_code_rows": invalid_code_rows,
        "non_numeric_counts": non_numeric,
        "optional_non_numeric_counts": optional_non_numeric,
        "closed_rows": int((slice_df["forward_return_status"].astype(str) == "CLOSED").sum()),
        "pending_rows": int((slice_df["forward_return_status"].astype(str) != "CLOSED").sum()),
        "promotion_blockers": sorted(str(v) for v in slice_df["promotion_blocker"].dropna().unique()),
        "candidate_origin_values": sorted(str(v) for v in slice_df["candidate_origin"].dropna().unique()),
        "operational_candidate_files_written": False,
    }
    OUT_META.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# New Method Followthrough Slice",
        "",
        f"- classification: {payload['classification']}",
        f"- status: {payload['status']}",
        f"- operation_effect: {payload['operation_effect']}",
        f"- selected_date: {payload['selected']['date']}",
        f"- selected_market_regime: {payload['selected']['market_regime']}",
        f"- selected_method_branch: {payload['selected']['method_branch']}",
        f"- selected_horizon: {payload['selected']['horizon']}",
        f"- row_count: {payload['row_count']}",
        f"- date_count: {payload['date_count']}",
        f"- branch_count: {payload['branch_count']}",
        f"- duplicate_code_rows: {payload['duplicate_code_rows']}",
        f"- invalid_code_rows: {payload['invalid_code_rows']}",
        f"- non_numeric_counts: {payload['non_numeric_counts']}",
        f"- optional_non_numeric_counts: {payload['optional_non_numeric_counts']}",
        f"- closed_rows: {payload['closed_rows']}",
        f"- pending_rows: {payload['pending_rows']}",
        f"- promotion_blockers: {payload['promotion_blockers']}",
        f"- operational_candidate_files_written: {payload['operational_candidate_files_written']}",
    ]
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if ready_for_followthrough else 2


if __name__ == "__main__":
    raise SystemExit(main())
