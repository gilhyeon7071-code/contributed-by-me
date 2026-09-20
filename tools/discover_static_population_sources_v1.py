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
LOGS = ROOT / "2_Logs"

OUT_JSON = LOGS / "static_population_source_discovery_v1_latest.json"
OUT_CSV = LOGS / "static_population_source_discovery_v1_candidates_latest.csv"
OUT_MD = LOGS / "static_population_source_discovery_v1_latest.md"

DEFAULT_ROOTS = [
    ROOT / "_cache",
    ROOT / "data",
    ROOT / "Raw",
    ROOT / "research",
    ROOT / "_krx_manual",
    ROOT / "krx_daily_archive",
    ROOT / "_krx_seed_full",
]

EXTENSIONS = {".csv", ".tsv", ".xlsx", ".xls", ".parquet", ".json", ".jsonl", ".txt"}
EXCLUDE_PARTS = {"backup", "__pycache__", ".git", ".venv", "node_modules", "_pytest_tmp"}

COLUMN_ALIASES: dict[str, list[str]] = {
    "code": ["code", "ticker", "symbol", "종목코드", "단축코드", "isu_srt_cd"],
    "name": ["name", "종목명", "한글종목명", "종목약명", "회사명", "isu_abbrv", "isu_nm"],
    "market": ["market", "시장", "시장구분", "시장명", "mkt_nm"],
    "security_type": ["security_type", "종목유형", "주식종류", "증권구분", "보통주구분", "isu_type"],
    "listed_date": ["listed_date", "상장일", "상장일자", "신규상장일", "list_dd", "listing_date"],
    "delisted_date": ["delisted_date", "상장폐지일", "상폐일", "상폐일자", "delist_dd", "delisting_date"],
}

REQUIRED_FOR_DIRECT_NORMALIZATION = {"code", "name", "market", "security_type", "listed_date"}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _header_key(value: object) -> str:
    return re.sub(r"[\s_\-./()]+", "", str(value or "").strip().lower())


def _is_excluded(path: Path) -> bool:
    parts = {p.lower() for p in path.parts}
    return bool(parts & EXCLUDE_PARTS)


def _iter_files(roots: list[Path], max_files: int) -> list[Path]:
    out: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if len(out) >= max_files:
                return out
            if not path.is_file() or _is_excluded(path):
                continue
            if path.suffix.lower() not in EXTENSIONS:
                continue
            key = str(path.resolve())
            if key in seen:
                continue
            seen.add(key)
            out.append(path)
    return out


def _read_columns(path: Path) -> tuple[list[str], int | None, str]:
    suffix = path.suffix.lower()
    try:
        if suffix in {".csv", ".txt", ".tsv"}:
            sep = "\t" if suffix == ".tsv" else None
            for enc in ("utf-8-sig", "utf-8", "cp949"):
                try:
                    df = pd.read_csv(path, dtype=str, encoding=enc, nrows=5, sep=sep, engine="python")
                    return [str(c) for c in df.columns], _count_delimited_rows(path), "OK"
                except UnicodeDecodeError:
                    continue
            df = pd.read_csv(path, dtype=str, nrows=5, sep=sep, engine="python")
            return [str(c) for c in df.columns], _count_delimited_rows(path), "OK"
        if suffix in {".xlsx", ".xls"}:
            df = pd.read_excel(path, dtype=str, nrows=5)
            return [str(c) for c in df.columns], None, "OK"
        if suffix == ".parquet":
            df = pd.read_parquet(path)
            return [str(c) for c in df.columns], int(len(df)), "OK"
        if suffix in {".json", ".jsonl"}:
            text = path.read_text(encoding="utf-8-sig", errors="replace").strip()
            if not text:
                return [], 0, "EMPTY"
            if suffix == ".jsonl":
                first = json.loads(text.splitlines()[0])
                if isinstance(first, dict):
                    return [str(c) for c in first.keys()], None, "OK"
                return [], None, "JSONL_FIRST_NOT_OBJECT"
            data = json.loads(text)
            if isinstance(data, list) and data and isinstance(data[0], dict):
                return [str(c) for c in data[0].keys()], len(data), "OK"
            if isinstance(data, dict):
                if data and all(isinstance(v, dict) for v in data.values()):
                    keys = sorted({str(k) for v in data.values() for k in v.keys()})
                    return keys, len(data), "OK"
                return [str(c) for c in data.keys()], None, "JSON_OBJECT"
            return [], None, "JSON_UNSUPPORTED_SHAPE"
    except Exception as exc:
        return [], None, f"{type(exc).__name__}: {exc}"
    return [], None, "UNSUPPORTED"


def _count_delimited_rows(path: Path) -> int | None:
    try:
        with path.open("rb") as handle:
            lines = sum(1 for _ in handle)
        return max(lines - 1, 0)
    except Exception:
        return None

def _match_columns(columns: list[str]) -> dict[str, str]:
    keyed = {_header_key(c): c for c in columns}
    found: dict[str, str] = {}
    for target, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            key = _header_key(alias)
            if key in keyed:
                found[target] = keyed[key]
                break
    return found


def _candidate_reason(found: dict[str, str], rows: int | None, read_status: str) -> tuple[str, str]:
    missing_direct = sorted(REQUIRED_FOR_DIRECT_NORMALIZATION - set(found))
    if read_status != "OK":
        return "READ_LIMITED", read_status
    if rows == 0:
        return "EMPTY", "file has recognized shape but no rows"
    if not missing_direct:
        return "DIRECT_NORMALIZATION_CANDIDATE", "has direct-normalization required fields"
    if {"code", "name"} <= set(found):
        return "PARTIAL_REFERENCE_ONLY", "has code/name but lacks static listing interval fields"
    return "NOT_STATIC_POPULATION_SOURCE", "missing core static population fields"


def discover(args: argparse.Namespace) -> dict[str, Any]:
    roots = [Path(x) for x in args.roots] if args.roots else DEFAULT_ROOTS
    files = _iter_files(roots, int(args.max_files))
    candidates: list[dict[str, Any]] = []
    for path in files:
        columns, rows, read_status = _read_columns(path)
        found = _match_columns(columns)
        score = len(found)
        reason_code, reason = _candidate_reason(found, rows, read_status)
        if path.name.lower() == "krx_population_static_input_template.csv":
            reason_code = "TEMPLATE_ONLY"
            reason = "contract template is not an actual source dataset"
        if score == 0 and not re.search(r"상장|상폐|listing|listed|delist|population|universe|ticker|stock|krx|종목", str(path), re.I):
            continue
        candidates.append(
            {
                "path": str(path),
                "suffix": path.suffix.lower(),
                "size": int(path.stat().st_size),
                "mtime": datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds"),
                "read_status": read_status,
                "rows": "" if rows is None else int(rows),
                "columns": "|".join(columns[:40]),
                "matched_fields": "|".join(sorted(found)),
                "matched_field_count": score,
                "classification": reason_code,
                "reason": reason,
                "missing_for_direct_normalization": "|".join(sorted(REQUIRED_FOR_DIRECT_NORMALIZATION - set(found))),
            }
        )
    candidates.sort(key=lambda r: (r["classification"] != "DIRECT_NORMALIZATION_CANDIDATE", -int(r["matched_field_count"]), r["path"]))
    direct = [c for c in candidates if c["classification"] == "DIRECT_NORMALIZATION_CANDIDATE"]
    partial = [c for c in candidates if c["classification"] == "PARTIAL_REFERENCE_ONLY"]
    payload = {
        "generated_at": _now(),
        "contract_version": "STATIC_POPULATION_SOURCE_DISCOVERY_V1",
        "round_type": "PREREGISTRATION_SOURCE_DISCOVERY_ONLY",
        "performance_calculated": False,
        "candidate_selection_calculated": False,
        "operational_change": False,
        "broker_order": False,
        "roots": [str(r) for r in roots],
        "files_scanned": len(files),
        "candidates_reported": len(candidates),
        "direct_normalization_candidates": len(direct),
        "partial_reference_candidates": len(partial),
        "status": "PASS" if direct else "NO_DIRECT_STATIC_SOURCE_FOUND",
        "top_candidates": candidates[:20],
        "outputs": {"json": str(OUT_JSON), "candidates_csv": str(OUT_CSV), "md": str(OUT_MD)},
    }
    return {"payload": payload, "candidates": candidates}


def write_outputs(payload: dict[str, Any], candidates: list[dict[str, Any]]) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    fields = [
        "classification",
        "matched_field_count",
        "matched_fields",
        "missing_for_direct_normalization",
        "path",
        "rows",
        "size",
        "mtime",
        "read_status",
        "reason",
        "columns",
    ]
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in candidates:
            writer.writerow(row)
    lines = [
        "# Static Population Source Discovery V1",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- status: {payload['status']}",
        f"- files_scanned: {payload['files_scanned']}",
        f"- candidates_reported: {payload['candidates_reported']}",
        f"- direct_normalization_candidates: {payload['direct_normalization_candidates']}",
        f"- partial_reference_candidates: {payload['partial_reference_candidates']}",
        "- performance_calculated: false",
        "- candidate_selection_calculated: false",
        "- operational_change: false",
        "- broker_order: false",
        "",
        "## Top candidates",
        "",
    ]
    if payload["top_candidates"]:
        for c in payload["top_candidates"][:20]:
            lines.append(f"- {c['classification']} fields={c['matched_fields']} path={c['path']} reason={c['reason']}")
    else:
        lines.append("- none")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Discover local files that can serve as Static Population Input V1 sources.")
    parser.add_argument("--roots", nargs="*", default=[])
    parser.add_argument("--max-files", type=int, default=5000)
    args = parser.parse_args()
    result = discover(args)
    write_outputs(result["payload"], result["candidates"])
    print(json.dumps({k: result["payload"][k] for k in ["status", "files_scanned", "candidates_reported", "direct_normalization_candidates", "partial_reference_candidates"]}, ensure_ascii=False))
    return 0 if result["payload"]["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
