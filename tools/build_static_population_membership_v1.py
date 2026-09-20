from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from validate_static_population_input_v1 import validate


ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / "_cache"
LOGS = ROOT / "2_Logs"

DEFAULT_INPUT = CACHE / "krx_population_static_input.csv"
OUT_PARQUET = CACHE / "krx_population_static_membership_latest.parquet"
OUT_CSV = CACHE / "krx_population_static_membership_latest.csv"
OUT_STATUS = LOGS / "static_population_membership_v1_build_latest.json"
OUT_MD = LOGS / "static_population_membership_v1_build_latest.md"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _norm_ymd(value: object) -> str:
    raw = str(value or "").replace("-", "").replace(".", "").replace("/", "").strip()
    return raw[:8]


def _sha256(path: Path) -> str:
    if not path.exists():
        return ""
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _bounded_price_files() -> list[Path]:
    files: list[Path] = []
    seen: set[str] = set()
    for base in (ROOT / "_krx_manual", ROOT / "krx_daily_archive", ROOT):
        if not base.exists() or not base.is_dir():
            continue
        for path in base.glob("krx_daily_*_clean.parquet"):
            key = str(path.resolve())
            if key in seen:
                continue
            seen.add(key)
            files.append(path)
    return sorted(files, key=lambda p: str(p))


def _load_trading_calendar(start: str, end: str) -> tuple[list[str], dict[str, Any]]:
    report_path = ROOT / "report_backtest_v41_1.py"
    spec = importlib.util.spec_from_file_location("report_backtest_v41_1_for_static_membership", report_path)
    if spec is None or spec.loader is None:
        return [], {"source": str(report_path), "status": "IMPORT_SPEC_FAIL"}
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    try:
        panel = module.load_data()
    except Exception as exc:
        return [], {"source": str(report_path), "status": "LOAD_FAIL", "error": f"{type(exc).__name__}: {exc}"}
    dates = pd.to_datetime(panel["date"], errors="coerce").dropna().dt.strftime("%Y%m%d")
    if start:
        dates = dates[dates >= start]
    if end:
        dates = dates[dates <= end]
    calendar = sorted(set(dates.astype(str)))
    integrity = panel.attrs.get("price_history_integrity", {})
    return calendar, {
        "source": str(report_path),
        "status": "OK" if calendar else "EMPTY_AFTER_FILTER",
        "calendar_contract": "report_backtest_v41_1.load_data + PRICE_HISTORY_INTEGRITY_V1",
        "date_min": calendar[0] if calendar else "",
        "date_max": calendar[-1] if calendar else "",
        "trading_dates": len(calendar),
        "panel_rows": int(len(panel)),
        "panel_codes": int(panel["code"].nunique()) if "code" in panel.columns else 0,
        "price_history_integrity": integrity,
    }

def _read_static_input(path: Path) -> pd.DataFrame:
    for encoding in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, dtype=str, encoding=encoding).fillna("")
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype=str).fillna("")


def _expand_membership(static_df: pd.DataFrame, calendar: list[str], include_security_types: set[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    cal = pd.Series(calendar, dtype=str)
    work = static_df.copy()
    for col in ["code", "name", "market", "security_type", "listed_date", "delisted_date", "source", "evidence_source"]:
        work[col] = work[col].astype(str).str.strip()
    work["market"] = work["market"].str.upper()
    work["security_type"] = work["security_type"].str.upper()
    work["listed_date"] = work["listed_date"].map(_norm_ymd)
    work["delisted_date"] = work["delisted_date"].map(_norm_ymd)

    for _, rec in work.iterrows():
        if rec["security_type"] not in include_security_types:
            continue
        start = rec["listed_date"]
        end = rec["delisted_date"] or "99991231"
        active_dates = cal[(cal >= start) & (cal <= end)]
        for as_of_date in active_dates.tolist():
            rows.append(
                {
                    "as_of_date": as_of_date,
                    "code": rec["code"],
                    "name": rec["name"],
                    "market": rec["market"],
                    "security_type": rec["security_type"],
                    "listed_date": rec["listed_date"],
                    "delisted_date": rec["delisted_date"],
                    "source": rec["source"],
                    "evidence_source": rec["evidence_source"],
                    "population_contract": "STATIC_POPULATION_INPUT_CONTRACT_V1",
                }
            )
    if not rows:
        return pd.DataFrame(
            columns=[
                "as_of_date",
                "code",
                "name",
                "market",
                "security_type",
                "listed_date",
                "delisted_date",
                "source",
                "evidence_source",
                "population_contract",
            ]
        )
    out = pd.DataFrame(rows)
    out = out.drop_duplicates(["as_of_date", "code", "market"], keep="last")
    return out.sort_values(["as_of_date", "market", "code"]).reset_index(drop=True)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = ["issue_code", "severity", "message"]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def build(args: argparse.Namespace) -> dict[str, Any]:
    input_path = Path(args.input)
    validation_payload, validation_issues = validate(input_path, check_price_coverage=False)
    status = "PASS"
    issues: list[dict[str, Any]] = []
    if validation_payload["status"] != "PASS":
        status = "FAIL"
        issues.append({"issue_code": "STATIC_INPUT_VALIDATION_FAIL", "severity": "FAIL", "message": "static input did not pass V1 validation"})

    start = _norm_ymd(args.start)
    end = _norm_ymd(args.end)
    calendar, calendar_meta = _load_trading_calendar(start, end)
    if not calendar:
        status = "FAIL"
        issues.append({"issue_code": "NO_TRADING_CALENDAR", "severity": "FAIL", "message": "no trading dates were found from bounded price parquet files"})

    include_security_types = {x.strip().upper() for x in str(args.include_security_types).split(",") if x.strip()}
    membership = pd.DataFrame()
    if status == "PASS":
        static_df = _read_static_input(input_path)
        membership = _expand_membership(static_df, calendar, include_security_types)
        if membership.empty:
            status = "FAIL"
            issues.append({"issue_code": "NO_MEMBERSHIP_ROWS", "severity": "FAIL", "message": "validated input produced no as_of_date x code rows"})

    output_parquet = Path(args.output_parquet)
    output_csv = Path(args.output_csv)
    if not membership.empty:
        output_parquet.parent.mkdir(parents=True, exist_ok=True)
        membership.to_parquet(output_parquet, index=False)
        if args.write_csv:
            membership.to_csv(output_csv, index=False, encoding="utf-8-sig")
    else:
        output_parquet.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=["as_of_date", "code", "name", "market", "security_type"]).to_parquet(output_parquet, index=False)
        if args.write_csv:
            pd.DataFrame(columns=["as_of_date", "code", "name", "market", "security_type"]).to_csv(output_csv, index=False, encoding="utf-8-sig")

    payload = {
        "generated_at": _now(),
        "contract_version": "STATIC_POPULATION_MEMBERSHIP_V1",
        "round_type": "PREREGISTRATION_SOURCE_RECONSTRUCTION_ONLY",
        "performance_calculated": False,
        "candidate_selection_calculated": False,
        "operational_change": False,
        "broker_order": False,
        "status": status,
        "input": str(input_path),
        "include_security_types": sorted(include_security_types),
        "calendar": calendar_meta,
        "rows": int(len(membership)),
        "unique_dates": int(membership["as_of_date"].nunique()) if not membership.empty else 0,
        "unique_codes": int(membership["code"].nunique()) if not membership.empty else 0,
        "date_min": str(membership["as_of_date"].min()) if not membership.empty else "",
        "date_max": str(membership["as_of_date"].max()) if not membership.empty else "",
        "markets": sorted(membership["market"].dropna().unique().tolist()) if not membership.empty else [],
        "validation_status": validation_payload["status"],
        "validation_fail_count": validation_payload["fail_count"],
        "validation_warn_count": validation_payload["warn_count"],
        "issues": issues,
        "output_parquet": str(output_parquet),
        "output_csv": str(output_csv) if args.write_csv else "",
        "output_parquet_sha256": _sha256(output_parquet),
        "output_csv_sha256": _sha256(output_csv) if args.write_csv else "",
    }
    return payload


def write_status(payload: dict[str, Any]) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    OUT_STATUS.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# Static Population Membership V1 Build",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- status: {payload['status']}",
        f"- input: {payload['input']}",
        f"- rows: {payload['rows']}",
        f"- unique_dates: {payload['unique_dates']}",
        f"- unique_codes: {payload['unique_codes']}",
        f"- date_min: {payload['date_min']}",
        f"- date_max: {payload['date_max']}",
        "- performance_calculated: false",
        "- candidate_selection_calculated: false",
        "- operational_change: false",
        "- broker_order: false",
        "",
        f"output_parquet: {payload['output_parquet']}",
    ]
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build as_of_date x code membership from Static Population Input V1.")
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--start", default="")
    parser.add_argument("--end", default="")
    parser.add_argument("--include-security-types", default="COMMON")
    parser.add_argument("--output-parquet", default=str(OUT_PARQUET))
    parser.add_argument("--output-csv", default=str(OUT_CSV))
    parser.add_argument("--write-csv", action="store_true")
    args = parser.parse_args()

    payload = build(args)
    write_status(payload)
    print(json.dumps({"status": payload["status"], "rows": payload["rows"], "unique_dates": payload["unique_dates"], "unique_codes": payload["unique_codes"]}, ensure_ascii=False))
    return 0 if payload["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
