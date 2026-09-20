from __future__ import annotations

import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / "_cache"
LOGS = ROOT / "2_Logs"

OPTIONS_JSON = LOGS / "candidate_generation_v2_population_options_latest.json"
OUT_JSON = LOGS / "candidate_generation_v2_population_source_availability_latest.json"
OUT_MD = LOGS / "candidate_generation_v2_population_source_availability_latest.md"
OUT_CSV = LOGS / "candidate_generation_v2_population_source_availability_sources_latest.csv"


DATE_RE = re.compile(r"(20\d{6})")


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_header(path: Path) -> list[str]:
    if not path.exists() or not path.is_file():
        return []
    for encoding in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=encoding, newline="") as handle:
                reader = csv.reader(handle)
                return [str(x).strip() for x in next(reader, [])]
        except Exception:
            continue
    return []


def _line_count(path: Path) -> int | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        with path.open("rb") as handle:
            count = sum(1 for _ in handle)
        return max(count - 1, 0)
    except Exception:
        return None


def _dated_files(pattern: str) -> dict[str, Any]:
    files = sorted(CACHE.glob(pattern))
    dates: list[str] = []
    for path in files:
        match = DATE_RE.search(path.name)
        if match:
            dates.append(match.group(1))
    return {
        "count": len(files),
        "date_min": min(dates) if dates else None,
        "date_max": max(dates) if dates else None,
        "sample": [str(p) for p in files[:3]],
    }


def _source(
    source_id: str,
    path: Path | str,
    status: str,
    grain: str,
    reason: str,
    has_asof_date: bool,
    has_code: bool,
    has_market: bool,
    has_security_type: bool,
    has_full_population: bool,
    point_in_time: bool,
    notes: str = "",
) -> dict[str, Any]:
    p = Path(path) if isinstance(path, str) else path
    return {
        "source_id": source_id,
        "path": str(p),
        "exists": p.exists(),
        "rows": _line_count(p) if p.suffix.lower() == ".csv" else None,
        "columns": _read_header(p) if p.suffix.lower() == ".csv" else [],
        "status": status,
        "grain": grain,
        "has_asof_date": has_asof_date,
        "has_code": has_code,
        "has_market": has_market,
        "has_security_type": has_security_type,
        "has_full_population": has_full_population,
        "point_in_time": point_in_time,
        "reason": reason,
        "notes": notes,
    }


def _load_options_summary() -> dict[str, Any]:
    if not OPTIONS_JSON.exists():
        return {"exists": False}
    data = json.loads(OPTIONS_JSON.read_text(encoding="utf-8"))
    return {
        "exists": True,
        "conclusion": data.get("conclusion"),
        "raw_rows": data.get("raw_rows"),
        "legacy_rows": data.get("legacy_rows"),
        "legacy_rows_covered_by_later_mapping": data.get("legacy_rows_covered_by_later_mapping"),
        "source_label_coverage": data.get("source_label_coverage", []),
        "label_reference_start": data.get("label_reference_start"),
    }


def build_report() -> dict[str, Any]:
    watchlist = _dated_files("krx_watchlist_*.csv")
    dart_fundamental = _dated_files("dart_fundamental_*.csv")
    pykrx_fundamental = _dated_files("pykrx_fundamental_*.csv")
    pykrx_supply = _dated_files("pykrx_supply_*.csv")
    index_constituents = _dated_files("krx_index_constituents_*.csv")

    sources = [
        _source(
            "CURRENT_KRX_LISTING",
            CACHE / "krx_listing.csv",
            "REJECT",
            "current code-name snapshot",
            "No as-of history, no market field, no security-type field; cannot define 2020-2026 point-in-time KOSPI/KOSDAQ equity eligibility.",
            False,
            True,
            False,
            False,
            False,
            False,
        ),
        _source(
            "CURRENT_KRX_SECTOR_MASTER",
            CACHE / "krx_sector_master_20260109.csv",
            "PARTIAL",
            "single-date sector snapshot",
            "Has code/name/sector/industry for one snapshot but not market membership or security type across history.",
            True,
            True,
            False,
            False,
            False,
            False,
        ),
        _source(
            "SURVIVORSHIP_DELISTED_SEED",
            CACHE / "survivorship_delisted_seed.csv",
            "REJECT",
            "manual delisting seed",
            "Schema is relevant, but current local file has no usable six-digit code rows and cannot supply full historical membership.",
            True,
            True,
            True,
            False,
            False,
            True,
        ),
        _source(
            "KRX_MARKET_SEED",
            CACHE / "krx_market_seed.csv",
            "REJECT",
            "manual market seed",
            "Only a tiny manual seed; not a full point-in-time market-membership source.",
            False,
            True,
            True,
            False,
            False,
            False,
        ),
        _source(
            "KRX_WATCHLIST_DATED_FILES",
            CACHE / "krx_watchlist_latest.csv",
            "REJECT",
            "dated risk/caution watchlist",
            "Dated files are not the exchange population. They contain administration, warning, risk, and caution flags only.",
            True,
            True,
            False,
            False,
            False,
            True,
            notes=json.dumps(watchlist, ensure_ascii=False),
        ),
        _source(
            "KRX_INDEX_CONSTITUENTS",
            CACHE / "krx_index_constituents_latest.csv",
            "REJECT",
            "index constituents",
            "Index membership is not the full KOSPI/KOSDAQ listed equity universe and cannot stand in for market/security eligibility.",
            True,
            True,
            False,
            False,
            False,
            True,
            notes=json.dumps(index_constituents, ensure_ascii=False),
        ),
        _source(
            "DART_FUNDAMENTAL_SNAPSHOTS",
            CACHE / "dart_fundamental_latest.csv",
            "REJECT",
            "fundamental snapshots",
            "Fundamental data can support features, not point-in-time market/security-type population eligibility.",
            True,
            True,
            False,
            False,
            False,
            True,
            notes=json.dumps(dart_fundamental, ensure_ascii=False),
        ),
        _source(
            "PYKRX_FUNDAMENTAL_AND_SUPPLY",
            CACHE / "pykrx_fundamental_latest.csv",
            "REJECT",
            "fundamental/supply snapshots",
            "Feature inputs, not full exchange/security-type membership history.",
            True,
            True,
            False,
            False,
            False,
            True,
            notes=json.dumps({"fundamental": pykrx_fundamental, "supply": pykrx_supply}, ensure_ascii=False),
        ),
        {
            "source_id": "PRICE_OHLCV_MARKET_SOURCE_LABELS",
            "path": str(OPTIONS_JSON),
            "exists": OPTIONS_JSON.exists(),
            "rows": None,
            "columns": [],
            "status": "PARTIAL_RESEARCH_ONLY",
            "grain": "price rows with source labels where present",
            "has_asof_date": True,
            "has_code": True,
            "has_market": True,
            "has_security_type": False,
            "has_full_population": False,
            "point_in_time": False,
            "reason": "Direct labels are absent for large historical sections; later unique mapping is future-observation research coverage only.",
            "notes": json.dumps(_load_options_summary(), ensure_ascii=False),
        },
    ]

    acceptable = [s for s in sources if s["status"] == "ACCEPTABLE"]
    result = {
        "generated_at": _now(),
        "round_type": "PREREGISTRATION_PREPARATION_ONLY",
        "performance_calculated": False,
        "candidate_selection_calculated": False,
        "operational_change": False,
        "broker_order": False,
        "required_contract": {
            "grain": "as_of_date x code",
            "required_fields": ["as_of_date", "code", "market", "security_type"],
            "required_history": "covers the research price window point-in-time, including delisted and relisted names where applicable",
            "eligible_market_values": ["KOSPI", "KOSDAQ"],
            "security_type_rule": "common/equity eligibility must be explicit or reproducibly derived without future observation",
        },
        "inventory_counts": {
            "watchlist_files": watchlist,
            "dart_fundamental_files": dart_fundamental,
            "pykrx_fundamental_files": pykrx_fundamental,
            "pykrx_supply_files": pykrx_supply,
            "index_constituent_files": index_constituents,
        },
        "sources": sources,
        "acceptable_sources": len(acceptable),
        "conclusion": "POINT_IN_TIME_POPULATION_SOURCE_NOT_FOUND_LOCALLY",
        "next_required_action": "Provide or build an approved point-in-time KRX listing/security-type history before V2 performance exploration or confirmation.",
    }
    return result


def write_outputs(result: dict[str, Any]) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    fieldnames = [
        "source_id",
        "status",
        "exists",
        "grain",
        "has_asof_date",
        "has_code",
        "has_market",
        "has_security_type",
        "has_full_population",
        "point_in_time",
        "path",
        "reason",
    ]
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in result["sources"]:
            writer.writerow({key: row.get(key) for key in fieldnames})

    lines = [
        "# Candidate-Generation V2 Population Source Availability",
        "",
        f"- generated_at: {result['generated_at']}",
        f"- round_type: {result['round_type']}",
        f"- conclusion: {result['conclusion']}",
        f"- acceptable_sources: {result['acceptable_sources']}",
        "- performance_calculated: false",
        "- candidate_selection_calculated: false",
        "- operational_change: false",
        "- broker_order: false",
        "",
        "## Required Contract",
        "",
        "- grain: as_of_date x code",
        "- required_fields: as_of_date, code, market, security_type",
        "- required_history: full research price window point-in-time coverage",
        "",
        "## Source Classification",
        "",
    ]
    for source in result["sources"]:
        lines.append(f"- {source['source_id']}: {source['status']} - {source['reason']}")
    lines.append("")
    lines.append(f"Next required action: {result['next_required_action']}")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    result = build_report()
    write_outputs(result)
    print(json.dumps({"status": "OK", "conclusion": result["conclusion"], "acceptable_sources": result["acceptable_sources"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
