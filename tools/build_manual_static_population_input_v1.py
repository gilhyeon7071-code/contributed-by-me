from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from validate_static_population_input_v1 import REQUIRED_COLUMNS, validate, write_outputs


ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / "_cache"
LOGS = ROOT / "2_Logs"
DEFAULT_INPUT_DIR = CACHE / "manual_static_population_source"
DEFAULT_OUTPUT = CACHE / "krx_population_static_input_manual_v1.csv"

OUT_JSON = LOGS / "manual_static_population_build_v1_latest.json"
OUT_CSV = LOGS / "manual_static_population_build_v1_issues_latest.csv"
OUT_MD = LOGS / "manual_static_population_build_v1_latest.md"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc).fillna("")
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype=str).fillna("")


def _norm_code(value: object) -> str:
    raw = "".join(ch for ch in str(value or "").upper().strip() if ch.isalnum())
    if raw.isdigit() and len(raw) < 6:
        return raw.zfill(6)
    return raw


def _norm_date(value: object) -> str:
    digits = "".join(ch for ch in str(value or "").strip() if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _norm_market(value: object) -> str:
    raw = str(value or "").strip().upper()
    if "KOSPI" in raw:
        return "KOSPI"
    if "KOSDAQ" in raw:
        return "KOSDAQ"
    return raw


def _security_type(row: pd.Series) -> str:
    stock_type = str(row.get("주식종류", "")).strip().upper()
    security_kind = str(row.get("증권구분", "")).strip().upper()
    name = f"{row.get('종목명', '')} {row.get('한글 종목명', '')} {row.get('한글 종목약명', '')} {row.get('소속부', '')}".upper()
    if "우선" in stock_type or "종류" in stock_type:
        return "PREFERRED"
    if "SPAC" in name or "스팩" in name or "기업인수목적" in name:
        return "SPAC"
    if "부동산투자회사" in security_kind or "REIT" in name or "리츠" in name:
        return "REIT"
    if "보통" in stock_type:
        return "COMMON"
    return "OTHER"


def _current_listing_files(files: list[Path]) -> list[Path]:
    out: list[Path] = []
    needed = {"단축코드", "한글 종목명", "상장일", "시장구분", "증권구분", "주식종류"}
    for path in files:
        try:
            cols = set(_read_csv(path).columns)
        except Exception:
            continue
        if needed <= cols and "폐지일" not in cols:
            out.append(path)
    return out


def _delisting_files(files: list[Path]) -> list[Path]:
    out: list[Path] = []
    needed = {"종목코드", "종목명", "상장일", "폐지일", "시장구분", "증권구분", "주식종류"}
    for path in files:
        try:
            cols = set(_read_csv(path).columns)
        except Exception:
            continue
        if needed <= cols:
            out.append(path)
    return out


def _issue(severity: str, code: str, message: str, path: str = "", row_key: str = "") -> dict[str, Any]:
    return {"severity": severity, "issue_code": code, "message": message, "path": path, "row_key": row_key}


def _allow_security_kind(value: object) -> bool:
    raw = str(value or "").strip()
    return raw in {"주권", "외국주권", "주식예탁증권", "부동산투자회사"}


def _to_standard_current(df: pd.DataFrame, source_path: Path, include_markets: set[str]) -> tuple[list[dict[str, str]], list[dict[str, Any]], dict[str, int]]:
    rows: list[dict[str, str]] = []
    issues: list[dict[str, Any]] = []
    stats = {"rows_in": int(len(df)), "rows_out": 0, "excluded_market": 0, "excluded_security_kind": 0, "invalid_core": 0}
    for _, rec in df.iterrows():
        market = _norm_market(rec.get("시장구분", ""))
        if market not in include_markets:
            stats["excluded_market"] += 1
            continue
        if not _allow_security_kind(rec.get("증권구분", "")):
            stats["excluded_security_kind"] += 1
            continue
        out = {
            "code": _norm_code(rec.get("단축코드", "")),
            "name": str(rec.get("한글 종목약명", "") or rec.get("한글 종목명", "")).strip(),
            "market": market,
            "security_type": _security_type(rec),
            "listed_date": _norm_date(rec.get("상장일", "")),
            "delisted_date": "",
            "source": "KRX_MANUAL_CURRENT_LISTING",
            "evidence_source": str(source_path),
        }
        if not (out["code"] and out["name"] and out["listed_date"]):
            stats["invalid_core"] += 1
            issues.append(_issue("FAIL", "CURRENT_INVALID_CORE", "current listing row lacks code/name/listed_date", str(source_path), f"{out['code']}|{out['name']}"))
            continue
        rows.append(out)
    stats["rows_out"] = len(rows)
    return rows, issues, stats


def _to_standard_delisted(df: pd.DataFrame, source_path: Path, include_markets: set[str]) -> tuple[list[dict[str, str]], list[dict[str, Any]], dict[str, int]]:
    rows: list[dict[str, str]] = []
    issues: list[dict[str, Any]] = []
    stats = {"rows_in": int(len(df)), "rows_out": 0, "excluded_market": 0, "excluded_security_kind": 0, "invalid_core": 0}
    for _, rec in df.iterrows():
        market = _norm_market(rec.get("시장구분", ""))
        if market not in include_markets:
            stats["excluded_market"] += 1
            continue
        if not _allow_security_kind(rec.get("증권구분", "")):
            stats["excluded_security_kind"] += 1
            continue
        out = {
            "code": _norm_code(rec.get("종목코드", "")),
            "name": str(rec.get("종목명", "")).strip(),
            "market": market,
            "security_type": _security_type(rec),
            "listed_date": _norm_date(rec.get("상장일", "")),
            "delisted_date": _norm_date(rec.get("폐지일", "")),
            "source": "KRX_MANUAL_DELISTING",
            "evidence_source": str(source_path),
        }
        if not (out["code"] and out["name"] and out["listed_date"] and out["delisted_date"]):
            stats["invalid_core"] += 1
            issues.append(_issue("FAIL", "DELISTED_INVALID_CORE", "delisting row lacks code/name/listed_date/delisted_date", str(source_path), f"{out['code']}|{out['name']}"))
            continue
        rows.append(out)
    stats["rows_out"] = len(rows)
    return rows, issues, stats


def build(args: argparse.Namespace) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    input_dir = Path(args.input_dir)
    output = Path(args.output)
    include_markets = {x.strip().upper() for x in str(args.include_markets).split(",") if x.strip()}
    issues: list[dict[str, Any]] = []

    files = sorted(input_dir.glob("*.csv")) if input_dir.exists() else []
    current_files = _current_listing_files(files)
    delisting_files = _delisting_files(files)
    if not current_files:
        issues.append(_issue("FAIL", "CURRENT_LISTING_SOURCE_MISSING", "no current listing source file found", str(input_dir)))
    if not delisting_files:
        issues.append(_issue("FAIL", "DELISTING_SOURCE_MISSING", "no delisting source files found", str(input_dir)))

    rows: list[dict[str, str]] = []
    source_stats: list[dict[str, Any]] = []
    for path in current_files:
        file_rows, file_issues, stats = _to_standard_current(_read_csv(path), path, include_markets)
        rows.extend(file_rows)
        issues.extend(file_issues)
        source_stats.append({"path": str(path), "source_kind": "current_listing", **stats})
    for path in delisting_files:
        file_rows, file_issues, stats = _to_standard_delisted(_read_csv(path), path, include_markets)
        rows.extend(file_rows)
        issues.extend(file_issues)
        source_stats.append({"path": str(path), "source_kind": "delisting", **stats})

    out_df = pd.DataFrame(rows, columns=REQUIRED_COLUMNS)
    before_dedupe = len(out_df)
    if not out_df.empty:
        out_df = out_df.drop_duplicates(["code", "market", "listed_date", "delisted_date"], keep="last")
        out_df = out_df.sort_values(["market", "code", "listed_date", "delisted_date"]).reset_index(drop=True)
    output.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(output, index=False, encoding="utf-8-sig")

    validation_payload, validation_issues = validate(output, check_price_coverage=False)
    write_outputs(validation_payload, validation_issues)
    if validation_payload["status"] != "PASS":
        issues.append(_issue("FAIL", "STATIC_INPUT_VALIDATION_FAIL", "manual merged static population input did not pass V1 validation", str(output)))

    fail_count = sum(1 for x in issues if x["severity"] == "FAIL")
    warn_count = sum(1 for x in issues if x["severity"] == "WARN")
    payload = {
        "generated_at": _now(),
        "contract_version": "MANUAL_STATIC_POPULATION_BUILD_V1",
        "round_type": "PREREGISTRATION_SOURCE_BUILD_ONLY",
        "performance_calculated": False,
        "candidate_selection_calculated": False,
        "operational_change": False,
        "broker_order": False,
        "status": "PASS" if fail_count == 0 and validation_payload["status"] == "PASS" else "FAIL",
        "input_dir": str(input_dir),
        "output": str(output),
        "include_markets": sorted(include_markets),
        "source_files": [str(p) for p in files],
        "current_listing_files": [str(p) for p in current_files],
        "delisting_files": [str(p) for p in delisting_files],
        "source_stats": source_stats,
        "rows_before_dedupe": int(before_dedupe),
        "rows": int(len(out_df)),
        "unique_codes": int(out_df["code"].nunique()) if not out_df.empty else 0,
        "markets": sorted(out_df["market"].dropna().unique().tolist()) if not out_df.empty else [],
        "security_types": sorted(out_df["security_type"].dropna().unique().tolist()) if not out_df.empty else [],
        "listed_date_min": str(out_df["listed_date"].min()) if not out_df.empty else "",
        "listed_date_max": str(out_df["listed_date"].max()) if not out_df.empty else "",
        "delisted_rows": int((out_df["delisted_date"].astype(str) != "").sum()) if not out_df.empty else 0,
        "active_rows": int((out_df["delisted_date"].astype(str) == "").sum()) if not out_df.empty else 0,
        "fail_count": fail_count,
        "warn_count": warn_count,
        "issue_count": len(issues),
        "validation_status": validation_payload["status"],
        "validation_fail_count": validation_payload["fail_count"],
        "validation_warn_count": validation_payload["warn_count"],
        "outputs": {"json": str(OUT_JSON), "issues_csv": str(OUT_CSV), "md": str(OUT_MD)},
    }
    return payload, issues


def write_build_outputs(payload: dict[str, Any], issues: list[dict[str, Any]]) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as handle:
        fields = ["severity", "issue_code", "message", "path", "row_key"]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for issue in issues:
            writer.writerow(issue)
    lines = [
        "# Manual Static Population Build V1",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- status: {payload['status']}",
        f"- input_dir: {payload['input_dir']}",
        f"- output: {payload['output']}",
        f"- rows: {payload['rows']}",
        f"- unique_codes: {payload['unique_codes']}",
        f"- active_rows: {payload['active_rows']}",
        f"- delisted_rows: {payload['delisted_rows']}",
        f"- validation_status: {payload['validation_status']}",
        "- performance_calculated: false",
        "- candidate_selection_calculated: false",
        "- operational_change: false",
        "- broker_order: false",
        "",
        "## Source stats",
        "",
    ]
    for stat in payload["source_stats"]:
        lines.append(f"- {stat['source_kind']} rows_in={stat['rows_in']} rows_out={stat['rows_out']} excluded_market={stat['excluded_market']} excluded_security_kind={stat['excluded_security_kind']} path={stat['path']}")
    lines.extend(["", "## Issues", ""])
    if issues:
        for issue in issues[:50]:
            lines.append(f"- {issue['severity']} {issue['issue_code']}: {issue['message']} {issue.get('path', '')}")
    else:
        lines.append("- none")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Static Population Input V1 from manually downloaded KRX listing/delisting CSVs.")
    parser.add_argument("--input-dir", default=str(DEFAULT_INPUT_DIR))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--include-markets", default="KOSPI,KOSDAQ")
    args = parser.parse_args()
    payload, issues = build(args)
    write_build_outputs(payload, issues)
    print(json.dumps({k: payload[k] for k in ["status", "rows", "unique_codes", "active_rows", "delisted_rows", "validation_status", "fail_count"]}, ensure_ascii=False))
    return 0 if payload["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
