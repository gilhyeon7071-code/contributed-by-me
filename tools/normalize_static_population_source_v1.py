from __future__ import annotations

import argparse
import csv
import json
import re
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

DEFAULT_OUTPUT = CACHE / "krx_population_static_input.csv"
OUT_JSON = LOGS / "static_population_source_normalization_v1_latest.json"
OUT_CSV = LOGS / "static_population_source_normalization_v1_issues_latest.csv"
OUT_MD = LOGS / "static_population_source_normalization_v1_latest.md"


COLUMN_ALIASES: dict[str, list[str]] = {
    "code": ["code", "ticker", "symbol", "종목코드", "단축코드", "단축code", "shortcode", "isu_srt_cd"],
    "name": ["name", "종목명", "한글종목명", "종목약명", "회사명", "isu_abbrv", "isu_nm"],
    "market": ["market", "시장", "시장구분", "시장명", "mkt_nm", "market_name"],
    "security_type": ["security_type", "종목유형", "주식종류", "증권구분", "보통주구분", "stock_type", "isu_type"],
    "listed_date": ["listed_date", "상장일", "상장일자", "신규상장일", "list_dd", "listing_date"],
    "delisted_date": ["delisted_date", "상장폐지일", "폐지일", "상폐일", "상폐일자", "delist_dd", "delisting_date"],
    "source": ["source", "출처", "자료출처", "source_id"],
    "evidence_source": ["evidence_source", "근거", "근거자료", "파일출처", "url", "evidence"],
}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _header_key(value: object) -> str:
    return re.sub(r"[\s_\-./()]+", "", str(value or "").strip().lower())


def _read_csv_any(path: Path) -> pd.DataFrame:
    for encoding in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, dtype=str, encoding=encoding).fillna("")
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype=str).fillna("")


def _norm_code(value: object) -> str:
    raw = "".join(ch for ch in str(value or "").upper().strip() if ch.isalnum())
    if raw.isdigit() and len(raw) < 6:
        return raw.zfill(6)
    return raw


def _norm_date(value: object) -> str:
    raw = str(value or "").strip()
    if raw.lower() in {"", "nan", "none", "nat", "null"}:
        return ""
    digits = "".join(ch for ch in raw if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _norm_market(value: object) -> str:
    raw = str(value or "").strip().upper()
    low = raw.lower()
    if "KOSPI" in raw or "유가" in raw or "거래소" in raw:
        return "KOSPI"
    if "KOSDAQ" in raw or "코스닥" in raw:
        return "KOSDAQ"
    return raw


def _norm_security_type(value: object) -> str:
    raw = str(value or "").strip().upper()
    if not raw or raw.lower() in {"nan", "none", "null"}:
        return ""
    if "PREFERRED" in raw or "우선" in raw:
        return "PREFERRED"
    if "COMMON" in raw or "보통" in raw:
        return "COMMON"
    if "REIT" in raw or "리츠" in raw:
        return "REIT"
    if "SPAC" in raw or "스팩" in raw:
        return "SPAC"
    if raw == "ETF" or "ETF" in raw:
        return "ETF"
    if raw == "ETN" or "ETN" in raw:
        return "ETN"
    if "OTHER" in raw or "기타" in raw:
        return "OTHER"
    return raw


def _find_columns(columns: list[str]) -> dict[str, str]:
    keyed = {_header_key(col): col for col in columns}
    found: dict[str, str] = {}
    for target, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            key = _header_key(alias)
            if key in keyed:
                found[target] = keyed[key]
                break
    return found


def _issue(row_num: int | None, severity: str, code: str, message: str, row_key: str = "") -> dict[str, Any]:
    return {
        "row_num": "" if row_num is None else int(row_num),
        "severity": severity,
        "issue_code": code,
        "message": message,
        "row_key": row_key,
    }


def normalize(args: argparse.Namespace) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    input_path = Path(args.input)
    output_path = Path(args.output)
    issues: list[dict[str, Any]] = []

    if not input_path.exists():
        issues.append(_issue(None, "FAIL", "INPUT_MISSING", f"input file not found: {input_path}"))
        payload = _payload(args, input_path, output_path, {}, 0, issues, None)
        return payload, issues

    source = _read_csv_any(input_path)
    source_columns = [str(c) for c in source.columns]
    found = _find_columns(source_columns)

    required_source_fields = ["code", "name", "market", "security_type", "listed_date"]
    for field in required_source_fields:
        if field not in found:
            issues.append(_issue(None, "FAIL", f"SOURCE_{field.upper()}_COLUMN_MISSING", f"source column for {field} was not found"))

    rows: list[dict[str, str]] = []
    source_id = str(args.source_id or input_path.name)
    evidence_default = str(args.evidence_source or input_path)
    for idx, row in source.iterrows():
        out = {col: "" for col in REQUIRED_COLUMNS}
        for target, source_col in found.items():
            out[target] = str(row.get(source_col, "")).strip()
        out["code"] = _norm_code(out["code"])
        out["market"] = _norm_market(out["market"])
        out["security_type"] = _norm_security_type(out["security_type"])
        out["listed_date"] = _norm_date(out["listed_date"])
        out["delisted_date"] = _norm_date(out["delisted_date"])
        out["source"] = out["source"] or source_id
        out["evidence_source"] = out["evidence_source"] or evidence_default
        rows.append(out)

        if not out["security_type"]:
            issues.append(_issue(int(idx) + 2, "FAIL", "SECURITY_TYPE_NOT_PROVIDED", "security_type must be explicit; name-based inference is not used", out["code"]))

    normalized = pd.DataFrame(rows, columns=REQUIRED_COLUMNS)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    normalized.to_csv(output_path, index=False, encoding="utf-8-sig")

    validation_payload, validation_issues = validate(output_path, check_price_coverage=bool(args.check_price_coverage))
    write_outputs(validation_payload, validation_issues)
    if validation_payload["status"] != "PASS":
        issues.append(_issue(None, "FAIL", "STATIC_INPUT_VALIDATION_FAIL", "normalized output did not pass Static Population Input V1 validation"))

    payload = _payload(args, input_path, output_path, found, len(normalized), issues, validation_payload)
    return payload, issues


def _payload(
    args: argparse.Namespace,
    input_path: Path,
    output_path: Path,
    found: dict[str, str],
    rows: int,
    issues: list[dict[str, Any]],
    validation_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    fail_count = sum(1 for x in issues if x["severity"] == "FAIL")
    warn_count = sum(1 for x in issues if x["severity"] == "WARN")
    return {
        "generated_at": _now(),
        "contract_version": "STATIC_POPULATION_SOURCE_NORMALIZATION_V1",
        "round_type": "PREREGISTRATION_SOURCE_NORMALIZATION_ONLY",
        "performance_calculated": False,
        "candidate_selection_calculated": False,
        "operational_change": False,
        "broker_order": False,
        "status": "PASS" if fail_count == 0 else "FAIL",
        "input": str(input_path),
        "output": str(output_path),
        "rows": int(rows),
        "source_column_map": found,
        "fail_count": fail_count,
        "warn_count": warn_count,
        "issue_count": len(issues),
        "validation_status": (validation_payload or {}).get("status", "NA"),
        "validation_fail_count": (validation_payload or {}).get("fail_count", 0),
        "validation_warn_count": (validation_payload or {}).get("warn_count", 0),
        "check_price_coverage": bool(args.check_price_coverage),
        "outputs": {"json": str(OUT_JSON), "issues_csv": str(OUT_CSV), "md": str(OUT_MD)},
    }


def write_normalization_outputs(payload: dict[str, Any], issues: list[dict[str, Any]]) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as handle:
        fields = ["row_num", "severity", "issue_code", "message", "row_key"]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for issue in issues:
            writer.writerow(issue)
    lines = [
        "# Static Population Source Normalization V1",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- status: {payload['status']}",
        f"- input: {payload['input']}",
        f"- output: {payload['output']}",
        f"- rows: {payload['rows']}",
        f"- validation_status: {payload['validation_status']}",
        f"- fail_count: {payload['fail_count']}",
        f"- warn_count: {payload['warn_count']}",
        "- performance_calculated: false",
        "- candidate_selection_calculated: false",
        "- operational_change: false",
        "- broker_order: false",
        "",
        "## Source column map",
        "",
    ]
    if payload["source_column_map"]:
        for target, source in payload["source_column_map"].items():
            lines.append(f"- {target}: {source}")
    else:
        lines.append("- none")
    lines.extend(["", "## Issues", ""])
    if issues:
        for issue in issues[:50]:
            lines.append(f"- {issue['severity']} {issue['issue_code']}: {issue['message']} {issue.get('row_key', '')}")
    else:
        lines.append("- none")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize a static listing/delisting source CSV into Static Population Input V1.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--source-id", default="")
    parser.add_argument("--evidence-source", default="")
    parser.add_argument("--check-price-coverage", action="store_true")
    args = parser.parse_args()

    payload, issues = normalize(args)
    write_normalization_outputs(payload, issues)
    print(json.dumps({"status": payload["status"], "rows": payload["rows"], "validation_status": payload["validation_status"], "fail_count": payload["fail_count"]}, ensure_ascii=False))
    return 0 if payload["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
