from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / "_cache"
LOGS = ROOT / "2_Logs"

DEFAULT_INPUT = CACHE / "krx_population_static_input.csv"
TEMPLATE_INPUT = CACHE / "krx_population_static_input_template.csv"
OUT_JSON = LOGS / "static_population_input_v1_validation_latest.json"
OUT_CSV = LOGS / "static_population_input_v1_validation_issues_latest.csv"
OUT_MD = LOGS / "static_population_input_v1_validation_latest.md"

REQUIRED_COLUMNS = [
    "code",
    "name",
    "market",
    "security_type",
    "listed_date",
    "delisted_date",
    "source",
    "evidence_source",
]
ALLOWED_MARKETS = {"KOSPI", "KOSDAQ"}
ALLOWED_SECURITY_TYPES = {"COMMON", "PREFERRED", "REIT", "SPAC", "ETF", "ETN", "OTHER"}
CODE_RE = re.compile(r"^[0-9A-Z]{6,8}$")


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _norm_code(value: object) -> str:
    raw = "".join(ch for ch in str(value or "").upper().strip() if ch.isalnum())
    if raw.isdigit() and len(raw) < 6:
        return raw.zfill(6)
    return raw


def _norm_ymd(value: object) -> str:
    raw = str(value or "").replace("-", "").replace(".", "").replace("/", "").strip()
    if raw.lower() in {"", "nan", "none", "nat", "null"}:
        return ""
    return raw[:8]


def _valid_ymd(value: str) -> bool:
    if not value:
        return False
    try:
        datetime.strptime(value, "%Y%m%d")
        return True
    except Exception:
        return False


def _read_csv_any(path: Path) -> pd.DataFrame:
    for encoding in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, dtype=str, encoding=encoding).fillna("")
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype=str).fillna("")


def _issue(row_num: int | None, severity: str, code: str, message: str, row_key: str = "") -> dict[str, Any]:
    return {
        "row_num": "" if row_num is None else int(row_num),
        "severity": severity,
        "issue_code": code,
        "message": message,
        "row_key": row_key,
    }


def _load_price_code_coverage() -> dict[str, Any]:
    files = []
    for base in (ROOT / "_krx_manual", ROOT / "krx_daily_archive", ROOT):
        if base.exists():
            files.extend(base.glob("krx_daily_*_clean.parquet"))
    if not files:
        return {"available": False, "reason": "NO_PRICE_PARQUET"}
    files = sorted(files, key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
    path = files[0]
    try:
        df = pd.read_parquet(path, columns=["date", "code"])
    except Exception as exc:
        return {"available": False, "reason": f"{type(exc).__name__}: {exc}", "path": str(path)}
    if df.empty:
        return {"available": False, "reason": "EMPTY_PRICE_PARQUET", "path": str(path)}
    df["code"] = df["code"].map(_norm_code)
    return {
        "available": True,
        "path": str(path),
        "rows": int(len(df)),
        "codes": int(df["code"].nunique()),
        "date_min": str(df["date"].astype(str).str[:10].min()),
        "date_max": str(df["date"].astype(str).str[:10].max()),
        "code_set": set(df["code"].dropna().astype(str)),
    }


def validate(path: Path, check_price_coverage: bool) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    issues: list[dict[str, Any]] = []
    if not path.exists():
        issues.append(_issue(None, "FAIL", "INPUT_MISSING", f"input file not found: {path}"))
        return _payload(path, pd.DataFrame(), issues, None), issues

    df = _read_csv_any(path)
    original_columns = [str(c) for c in df.columns]
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    extra = [c for c in df.columns if c not in REQUIRED_COLUMNS]
    for col in missing:
        issues.append(_issue(None, "FAIL", "MISSING_COLUMN", f"required column missing: {col}"))
    for col in extra:
        issues.append(_issue(None, "WARN", "EXTRA_COLUMN", f"extra column ignored by V1 contract: {col}"))
    if missing:
        return _payload(path, df, issues, None, original_columns), issues

    work = df[REQUIRED_COLUMNS].copy()
    for col in REQUIRED_COLUMNS:
        work[col] = work[col].astype(str).str.strip()
    work["code"] = work["code"].map(_norm_code)
    work["market"] = work["market"].str.upper()
    work["security_type"] = work["security_type"].str.upper()
    work["listed_date"] = work["listed_date"].map(_norm_ymd)
    work["delisted_date"] = work["delisted_date"].map(_norm_ymd)

    if work.empty:
        issues.append(_issue(None, "FAIL", "NO_ROWS", "input has header but no data rows"))

    for idx, row in work.iterrows():
        row_num = int(idx) + 2
        row_key = f"{row['code']}|{row['market']}|{row['listed_date']}"
        if not CODE_RE.match(row["code"]):
            issues.append(_issue(row_num, "FAIL", "INVALID_CODE", "code must be a 6-8 character KRX short code using digits/uppercase letters", row_key))
        if row["market"] not in ALLOWED_MARKETS:
            issues.append(_issue(row_num, "FAIL", "INVALID_MARKET", "market must be KOSPI or KOSDAQ", row_key))
        if row["security_type"] not in ALLOWED_SECURITY_TYPES:
            issues.append(_issue(row_num, "FAIL", "INVALID_SECURITY_TYPE", "security_type is outside V1 allowed values", row_key))
        if not _valid_ymd(row["listed_date"]):
            issues.append(_issue(row_num, "FAIL", "INVALID_LISTED_DATE", "listed_date must be YYYYMMDD", row_key))
        if row["delisted_date"] and not _valid_ymd(row["delisted_date"]):
            issues.append(_issue(row_num, "FAIL", "INVALID_DELISTED_DATE", "delisted_date must be blank or YYYYMMDD", row_key))
        if _valid_ymd(row["listed_date"]) and row["delisted_date"] and _valid_ymd(row["delisted_date"]):
            if row["delisted_date"] < row["listed_date"]:
                issues.append(_issue(row_num, "FAIL", "DATE_ORDER", "delisted_date is earlier than listed_date", row_key))
        for col in ("name", "source", "evidence_source"):
            if not row[col]:
                issues.append(_issue(row_num, "FAIL", f"MISSING_{col.upper()}", f"{col} must be populated", row_key))

    dup_mask = work.duplicated(["code", "market", "listed_date"], keep=False)
    for idx, row in work.loc[dup_mask].iterrows():
        issues.append(_issue(int(idx) + 2, "FAIL", "DUPLICATE_INTERVAL_KEY", "duplicate code x market x listed_date", f"{row['code']}|{row['market']}|{row['listed_date']}"))

    for (code, market), grp in work.sort_values(["code", "market", "listed_date"]).groupby(["code", "market"], dropna=False):
        intervals = []
        for idx, row in grp.iterrows():
            start = row["listed_date"]
            end = row["delisted_date"] or "99991231"
            if not (_valid_ymd(start) and _valid_ymd(end)):
                continue
            intervals.append((start, end, int(idx) + 2))
        intervals.sort()
        for prev, cur in zip(intervals, intervals[1:]):
            if cur[0] <= prev[1]:
                issues.append(_issue(cur[2], "FAIL", "OVERLAPPING_INTERVAL", "overlapping listing intervals for same code and market", f"{code}|{market}"))

    coverage = None
    if check_price_coverage and len(work):
        coverage = _load_price_code_coverage()
        if coverage.get("available"):
            price_codes = coverage.pop("code_set")
            input_codes = set(work["code"].dropna().astype(str))
            unmatched_input = sorted(input_codes - price_codes)
            unmatched_price = sorted(price_codes - input_codes)
            coverage["input_codes"] = len(input_codes)
            coverage["input_codes_not_in_price"] = len(unmatched_input)
            coverage["price_codes_not_in_input"] = len(unmatched_price)
            coverage["input_codes_not_in_price_sample"] = unmatched_input[:20]
            coverage["price_codes_not_in_input_sample"] = unmatched_price[:20]
            if unmatched_input:
                issues.append(_issue(None, "WARN", "INPUT_CODES_NOT_IN_PRICE", f"{len(unmatched_input)} input codes are not present in the newest price parquet"))
            if unmatched_price:
                issues.append(_issue(None, "WARN", "PRICE_CODES_NOT_IN_INPUT", f"{len(unmatched_price)} price codes are not covered by the static input"))

    return _payload(path, work, issues, coverage, original_columns), issues


def _payload(path: Path, df: pd.DataFrame, issues: list[dict[str, Any]], coverage: dict[str, Any] | None, original_columns: list[str] | None = None) -> dict[str, Any]:
    fail_count = sum(1 for x in issues if x["severity"] == "FAIL")
    warn_count = sum(1 for x in issues if x["severity"] == "WARN")
    return {
        "generated_at": _now(),
        "contract_version": "STATIC_POPULATION_INPUT_CONTRACT_V1",
        "round_type": "PREREGISTRATION_SOURCE_VALIDATION_ONLY",
        "performance_calculated": False,
        "candidate_selection_calculated": False,
        "operational_change": False,
        "broker_order": False,
        "input": str(path),
        "status": "PASS" if fail_count == 0 else "FAIL",
        "rows": int(len(df)),
        "columns": original_columns or [str(c) for c in df.columns],
        "required_columns": REQUIRED_COLUMNS,
        "fail_count": fail_count,
        "warn_count": warn_count,
        "issue_count": len(issues),
        "markets": sorted(df["market"].dropna().unique().tolist()) if "market" in df.columns and len(df) else [],
        "security_types": sorted(df["security_type"].dropna().unique().tolist()) if "security_type" in df.columns and len(df) else [],
        "unique_codes": int(df["code"].nunique()) if "code" in df.columns and len(df) else 0,
        "date_min": str(df["listed_date"].min()) if "listed_date" in df.columns and len(df) else "",
        "date_max": str(df["listed_date"].max()) if "listed_date" in df.columns and len(df) else "",
        "price_coverage": coverage or {},
        "outputs": {"json": str(OUT_JSON), "issues_csv": str(OUT_CSV), "md": str(OUT_MD)},
    }


def write_outputs(payload: dict[str, Any], issues: list[dict[str, Any]]) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as handle:
        fields = ["row_num", "severity", "issue_code", "message", "row_key"]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for issue in issues:
            writer.writerow(issue)
    lines = [
        "# Static Population Input V1 Validation",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- status: {payload['status']}",
        f"- input: {payload['input']}",
        f"- rows: {payload['rows']}",
        f"- fail_count: {payload['fail_count']}",
        f"- warn_count: {payload['warn_count']}",
        "- performance_calculated: false",
        "- candidate_selection_calculated: false",
        "- operational_change: false",
        "- broker_order: false",
        "",
        "## Issues",
        "",
    ]
    if issues:
        for issue in issues[:50]:
            lines.append(f"- {issue['severity']} {issue['issue_code']}: {issue['message']} {issue.get('row_key', '')}")
    else:
        lines.append("- none")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Static Population Input Contract V1.")
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--check-price-coverage", action="store_true")
    args = parser.parse_args()
    payload, issues = validate(Path(args.input), bool(args.check_price_coverage))
    write_outputs(payload, issues)
    print(json.dumps({"status": payload["status"], "rows": payload["rows"], "fail_count": payload["fail_count"], "warn_count": payload["warn_count"]}, ensure_ascii=False))
    return 0 if payload["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
