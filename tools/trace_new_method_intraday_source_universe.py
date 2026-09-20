from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

NEW_METHOD_CANDIDATES = LOG_DIR / "new_method_candidates_for_replay_latest.csv"
NEW_METHOD_COVERAGE_MISSING = LOG_DIR / "new_method_intraday_coverage_missing_latest.csv"
INTRADAY_STATUS = LOG_DIR / "intraday_prices_status_latest.json"
INTRADAY_PRICES = LOG_DIR / "intraday_prices_latest.csv"
OP_CANDIDATES = LOG_DIR / "candidates_latest_data.csv"
OP_CANDIDATES_WITH_FINAL = LOG_DIR / "candidates_latest_data.with_final_score.csv"
FUTURE_SIGNAL_PREVIEW = LOG_DIR / "future_signal_preview_latest.json"
INTRADAY_SNAPSHOT_SCRIPT = ROOT / "tools" / "intraday_price_snapshot.py"
RUN_INTRADAY_PAPER_BAT = ROOT / "run_intraday_paper.bat"
RUN_PAPER_DAILY_BAT = ROOT / "run_paper_daily.bat"

OUT_JSON = LOG_DIR / "new_method_intraday_source_trace_latest.json"
OUT_DETAIL = LOG_DIR / "new_method_intraday_source_trace_detail_latest.csv"
OUT_SOURCE = LOG_DIR / "new_method_intraday_source_trace_source_summary_latest.csv"
OUT_MD = LOG_DIR / "new_method_intraday_source_trace_latest.md"


SOURCE_FIELDS = [
    "codes_requested_list",
    "future_signal_preview_codes",
    "realtime_surge_codes",
    "recheck_observation_codes",
    "ev_source_queue_observation_codes",
    "ev_pending_observation_codes",
]


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _codes_from_csv(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        df = pd.read_csv(path, dtype={"code": "string"})
    except Exception:
        return set()
    if "code" not in df.columns:
        return set()
    return set(df["code"].dropna().astype("string").str.zfill(6).astype(str))


def _future_intraday_codes(path: Path) -> set[str]:
    payload = _read_json(path)
    rows = payload.get("preview_rows")
    if rows is None:
        rows = payload.get("rows")
    if not isinstance(rows, list):
        return set()
    df = pd.DataFrame(rows)
    if df.empty or "code" not in df.columns:
        return set()
    if "horizon_type" in df.columns:
        df = df[df["horizon_type"].astype(str).str.upper() == "INTRADAY"].copy()
    return set(df["code"].dropna().astype("string").str.zfill(6).astype(str))


def _line_hits(path: Path, patterns: list[str]) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    hits: list[dict[str, Any]] = []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    for idx, line in enumerate(lines, start=1):
        if any(pattern in line for pattern in patterns):
            hits.append({"path": str(path), "line": idx, "text": line.strip()})
    return hits


def _has_status_isolation_gap() -> bool:
    text = INTRADAY_SNAPSHOT_SCRIPT.read_text(encoding="utf-8", errors="replace")
    return 'paths = {"status": LOG_DIR / "intraday_prices_status.json"}' in text


def main() -> int:
    if not NEW_METHOD_CANDIDATES.exists():
        raise FileNotFoundError(NEW_METHOD_CANDIDATES)
    if not INTRADAY_STATUS.exists():
        raise FileNotFoundError(INTRADAY_STATUS)

    status = _read_json(INTRADAY_STATUS)
    nm = pd.read_csv(NEW_METHOD_CANDIDATES, dtype={"code": "string"})
    nm["code"] = nm["code"].astype("string").str.zfill(6)
    if NEW_METHOD_COVERAGE_MISSING.exists():
        missing_df = pd.read_csv(NEW_METHOD_COVERAGE_MISSING, dtype={"code": "string"})
        missing_df["code"] = missing_df["code"].astype("string").str.zfill(6)
    else:
        missing_df = pd.DataFrame(columns=["code"])

    source_sets: dict[str, set[str]] = {
        field: set(str(v).zfill(6) for v in status.get(field, []) if str(v).strip())
        for field in SOURCE_FIELDS
    }
    source_sets["operational_candidates_latest_data"] = _codes_from_csv(OP_CANDIDATES)
    source_sets["operational_candidates_with_final_score"] = _codes_from_csv(OP_CANDIDATES_WITH_FINAL)
    source_sets["future_signal_preview_intraday_rows"] = _future_intraday_codes(FUTURE_SIGNAL_PREVIEW)
    source_sets["intraday_prices_latest"] = _codes_from_csv(INTRADAY_PRICES)

    latest_date = str(nm["date"].max())
    target = nm.copy()
    target["is_latest_new_method_date"] = target["date"].astype(str).eq(latest_date)
    target["branch_key"] = (
        target["date"].astype(str)
        + "|"
        + target["market_regime"].astype(str)
        + "|"
        + target["method_branch"].astype(str)
        + "|"
        + target["horizon"].astype(str)
    )
    missing_codes = set(missing_df["code"].dropna().astype(str)) if "code" in missing_df.columns else set()
    target["was_missing_from_intraday_coverage"] = target["code"].astype(str).isin(missing_codes)

    rows: list[dict[str, Any]] = []
    for _, row in target.iterrows():
        code = str(row["code"])
        membership = {f"in_{name}": code in codes for name, codes in source_sets.items()}
        source_names = [name for name, codes in source_sets.items() if code in codes]
        if code in source_sets["codes_requested_list"]:
            exclusion_reason = "REQUESTED_AND_FETCHED"
        elif source_names:
            exclusion_reason = "SOURCE_PRESENT_BUT_CAPPED_OR_NOT_REQUESTED"
        elif bool(row["is_latest_new_method_date"]):
            exclusion_reason = "NEW_METHOD_SOURCE_NOT_WIRED_TO_INTRADAY_UNIVERSE"
        else:
            exclusion_reason = "HISTORICAL_NEW_METHOD_BRANCH_NOT_CURRENT_TARGET"
        rows.append(
            {
                "date": row.get("date", ""),
                "branch_key": row.get("branch_key", ""),
                "market_regime": row.get("market_regime", ""),
                "method_branch": row.get("method_branch", ""),
                "horizon": row.get("horizon", ""),
                "code": code,
                "is_latest_new_method_date": bool(row["is_latest_new_method_date"]),
                "was_missing_from_intraday_coverage": bool(row["was_missing_from_intraday_coverage"]),
                "source_memberships": "|".join(source_names),
                "exclusion_reason": exclusion_reason,
                **membership,
            }
        )
    detail = pd.DataFrame(rows).sort_values(["date", "method_branch", "code"])
    detail.to_csv(OUT_DETAIL, index=False, encoding="utf-8-sig")

    source_summary_rows = []
    for name, codes in source_sets.items():
        source_summary_rows.append(
            {
                "source": name,
                "source_codes": len(codes),
                "new_method_overlap_codes": len(set(nm["code"].astype(str)) & codes),
                "latest_new_method_overlap_codes": len(
                    set(nm.loc[nm["date"].astype(str).eq(latest_date), "code"].astype(str)) & codes
                ),
                "new_method_missing_overlap_codes": len(missing_codes & codes),
            }
        )
    source_summary = pd.DataFrame(source_summary_rows)
    source_summary.to_csv(OUT_SOURCE, index=False, encoding="utf-8-sig")

    latest_detail = detail[detail["is_latest_new_method_date"]].copy()
    latest_missing = latest_detail[latest_detail["was_missing_from_intraday_coverage"]].copy()
    requested = set(status.get("codes_requested_list", []))
    all_source_union = set().union(*source_sets.values()) if source_sets else set()
    latest_missing_codes = sorted(latest_missing["code"].astype(str).unique())
    latest_missing_in_any_source = sorted(set(latest_missing_codes) & all_source_union)
    latest_missing_in_requested = sorted(set(latest_missing_codes) & requested)

    code_evidence = {
        "intraday_snapshot_argparse_and_status_lines": _line_hits(
            INTRADAY_SNAPSHOT_SCRIPT,
            [
                'ap.add_argument("--codes"',
                'ap.add_argument("--from-candidates"',
                'ap.add_argument("--max-total-codes"',
                'paths = {"status": LOG_DIR / "intraday_prices_status.json"}',
            ],
        ),
        "run_intraday_paper_cap_lines": _line_hits(
            RUN_INTRADAY_PAPER_BAT,
            ["INTRADAY_PRICE_MAX_TOTAL_CODES"],
        ),
        "run_paper_daily_snapshot_lines": _line_hits(
            RUN_PAPER_DAILY_BAT,
            ["intraday_price_snapshot.py", "--max-total-codes"],
        ),
    }

    isolated_probe_supported = True
    isolated_probe_status_safe_without_change = not _has_status_isolation_gap()
    payload: dict[str, Any] = {
        "generated_at": _now(),
        "classification": "NEW_METHOD_INTRADAY_SOURCE_TRACE",
        "operation_effect": "READ_ONLY_SOURCE_TRACE_ONLY",
        "intraday_status": str(INTRADAY_STATUS),
        "intraday_prices": str(INTRADAY_PRICES),
        "new_method_candidates": str(NEW_METHOD_CANDIDATES),
        "status_ts": status.get("ts"),
        "codes_requested": int(status.get("codes_requested", 0) or 0),
        "codes_ok": int(status.get("codes_ok", 0) or 0),
        "max_total_codes": int(status.get("max_total_codes", 0) or 0),
        "max_total_codes_before": int(status.get("max_total_codes_before", 0) or 0),
        "max_total_codes_applied": bool(status.get("max_total_codes_applied", False)),
        "max_total_codes_priority_kept": int(status.get("max_total_codes_priority_kept", 0) or 0),
        "new_method_rows": int(len(nm)),
        "new_method_unique_codes": int(nm["code"].nunique()),
        "latest_new_method_date": latest_date,
        "latest_new_method_rows": int(len(latest_detail)),
        "latest_new_method_missing_rows": int(len(latest_missing)),
        "latest_new_method_missing_codes": latest_missing_codes,
        "latest_missing_in_any_current_intraday_source": latest_missing_in_any_source,
        "latest_missing_in_requested_codes": latest_missing_in_requested,
        "latest_missing_exclusion_counts": latest_missing["exclusion_reason"].value_counts().to_dict(),
        "requested_new_method_overlap_codes": sorted(set(nm["code"].astype(str)) & requested),
        "source_summary_csv": str(OUT_SOURCE),
        "detail_csv": str(OUT_DETAIL),
        "source_summary": source_summary.to_dict(orient="records"),
        "isolated_probe_supported_by_cli": isolated_probe_supported,
        "isolated_probe_status_safe_without_code_change": isolated_probe_status_safe_without_change,
        "isolated_probe_status_caveat": (
            "custom --out-csv still writes intraday_prices_status.json"
            if not isolated_probe_status_safe_without_change
            else "custom --out-csv status fully isolated"
        ),
        "code_evidence": code_evidence,
        "operational_candidate_files_written": False,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# New Method Intraday Source Trace",
        "",
        f"- classification: {payload['classification']}",
        f"- operation_effect: {payload['operation_effect']}",
        f"- status_ts: {payload['status_ts']}",
        f"- codes_requested: {payload['codes_requested']}",
        f"- max_total_codes: {payload['max_total_codes']}",
        f"- max_total_codes_before: {payload['max_total_codes_before']}",
        f"- max_total_codes_applied: {payload['max_total_codes_applied']}",
        f"- latest_new_method_rows: {payload['latest_new_method_rows']}",
        f"- latest_new_method_missing_rows: {payload['latest_new_method_missing_rows']}",
        f"- latest_new_method_missing_codes: {payload['latest_new_method_missing_codes']}",
        f"- latest_missing_in_any_current_intraday_source: {payload['latest_missing_in_any_current_intraday_source']}",
        f"- latest_missing_in_requested_codes: {payload['latest_missing_in_requested_codes']}",
        f"- latest_missing_exclusion_counts: {payload['latest_missing_exclusion_counts']}",
        f"- isolated_probe_supported_by_cli: {payload['isolated_probe_supported_by_cli']}",
        f"- isolated_probe_status_safe_without_code_change: {payload['isolated_probe_status_safe_without_code_change']}",
        f"- isolated_probe_status_caveat: {payload['isolated_probe_status_caveat']}",
        f"- operational_candidate_files_written: {payload['operational_candidate_files_written']}",
        "",
        "## Source Summary",
    ]
    for row in source_summary.to_dict(orient="records"):
        lines.append(
            "- {source}: source_codes={source_codes}, new_method_overlap={new_method_overlap_codes}, "
            "latest_overlap={latest_new_method_overlap_codes}, missing_overlap={new_method_missing_overlap_codes}".format(
                **row
            )
        )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
