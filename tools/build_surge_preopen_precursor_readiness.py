from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"
PAPER = ROOT / "paper"

COVERAGE_JSON = LOGS / "surge_precursor_coverage_matrix_latest.json"
COVERAGE_CSV = LOGS / "surge_precursor_coverage_matrix_latest.csv"
STATE_JSON = LOGS / "surge_state_machine_shadow_latest.json"
CANDIDATES_CSV = LOGS / "candidates_latest_data.with_final_score.csv"
SURGE_UNIVERSE_CSV = PAPER / "surge_universe.csv"
SURGE_PARAMS_JSON = PAPER / "surge_params.json"
OHLCV_PARQUET = PAPER / "prices" / "ohlcv_paper.parquet"
STATE_TRANSITION_HISTORY = LOGS / "surge_state_machine_transition_history.csv"
MARKOUT_SUMMARY_JSON = LOGS / "surge_shadow_probe_markout_summary_latest.json"
TIMEPOINT_MARKOUT_JSON = LOGS / "surge_shadow_probe_timepoint_markout_latest.json"
GRADEBOOK_JSON = LOGS / "surge_ev_shadow_gradebook_latest.json"

OUT_JSON = LOGS / "surge_preopen_precursor_readiness_latest.json"
OUT_CSV = LOGS / "surge_preopen_precursor_readiness_latest.csv"

PREOPEN_SOURCES = {
    "candidate",
    "candidate+ohlcv",
    "ohlcv",
    "flow_history",
    "theme_history",
    "surge_history",
}
INTRADAY_TOKENS = {
    "intraday",
    "surge_realtime",
    "market_rising",
    "ticks",
    "lob",
    "market_rules",
    "disclosure",
    "etf",
    "global",
    "us_sector",
}
IMPLEMENTED = {"REFLECTED", "PARTIAL_REFLECTED"}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _read_csv(path: Path) -> list[dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as fp:
            return list(csv.DictReader(fp))
    except Exception:
        return []


def _file_meta(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "size": 0, "mtime": ""}
    stat = path.stat()
    return {
        "path": str(path),
        "exists": True,
        "size": int(stat.st_size),
        "mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
    }


def _is_preopen_source(source_type: str) -> bool:
    src = str(source_type or "").strip().lower()
    if not src:
        return False
    if any(token in src for token in INTRADAY_TOKENS):
        return False
    return src in PREOPEN_SOURCES or "candidate" in src or "ohlcv" in src


def _top_items(rows: list[dict[str, Any]], status: str, limit: int = 8) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for row in rows:
        if row.get("preopen_status") != status:
            continue
        out.append({
            "category": str(row.get("category") or ""),
            "item": str(row.get("item") or ""),
            "coverage_status": str(row.get("coverage_status") or ""),
            "evidence_fields": str(row.get("evidence_fields") or ""),
        })
        if len(out) >= limit:
            break
    return out


def main() -> int:
    state = _read_json(STATE_JSON)
    rows = _read_csv(COVERAGE_CSV)

    out_rows: list[dict[str, Any]] = []
    for row in rows:
        source_type = str(row.get("source_type") or "")
        coverage_status = str(row.get("coverage_status") or "")
        preopen_source = _is_preopen_source(source_type)
        implemented = coverage_status in IMPLEMENTED
        if preopen_source and implemented:
            preopen_status = "PREOPEN_READY"
            note = "preopen source is reflected or partially reflected"
        elif preopen_source:
            preopen_status = "PREOPEN_SOURCE_NOT_IMPLEMENTED"
            note = "preopen source exists but is not reflected in state scoring"
        elif coverage_status.startswith("INTRADAY"):
            preopen_status = "INTRADAY_REQUIRED"
            note = "cannot be checked preopen only; intraday accumulation is required"
        elif "EXTERNAL" in coverage_status or coverage_status == "SOURCE_MISSING":
            preopen_status = "EXTERNAL_OR_MISSING"
            note = "external source or separate rule is required"
        else:
            preopen_status = "NOT_PREOPEN_SOURCE"
            note = "not suitable for the preopen-only source list"
        out_rows.append({
            "category": row.get("category", ""),
            "item": row.get("item", ""),
            "coverage_status": coverage_status,
            "source_type": source_type,
            "preopen_status": preopen_status,
            "evidence_fields": row.get("evidence_fields", ""),
            "note": note,
        })

    status_counts = Counter(str(row["preopen_status"]) for row in out_rows)
    category_ready = Counter(row["category"] for row in out_rows if row["preopen_status"] == "PREOPEN_READY")
    today = datetime.now()
    is_weekend = today.weekday() >= 5
    files = {
        "coverage_json": _file_meta(COVERAGE_JSON),
        "coverage_csv": _file_meta(COVERAGE_CSV),
        "state_shadow": _file_meta(STATE_JSON),
        "candidates": _file_meta(CANDIDATES_CSV),
        "surge_universe": _file_meta(SURGE_UNIVERSE_CSV),
        "surge_params": _file_meta(SURGE_PARAMS_JSON),
        "ohlcv_parquet": _file_meta(OHLCV_PARQUET),
        "state_transition_history": _file_meta(STATE_TRANSITION_HISTORY),
        "markout_summary": _file_meta(MARKOUT_SUMMARY_JSON),
        "timepoint_markout": _file_meta(TIMEPOINT_MARKOUT_JSON),
        "gradebook": _file_meta(GRADEBOOK_JSON),
    }
    required_ready = all(
        files[key]["exists"]
        for key in ("coverage_json", "coverage_csv", "candidates", "surge_universe", "surge_params", "ohlcv_parquet")
    )

    total_items = len(out_rows)
    preopen_ready = int(status_counts.get("PREOPEN_READY", 0))
    preopen_missing = int(status_counts.get("PREOPEN_SOURCE_NOT_IMPLEMENTED", 0))
    intraday_required = int(status_counts.get("INTRADAY_REQUIRED", 0))
    external_or_missing = int(status_counts.get("EXTERNAL_OR_MISSING", 0))
    ready_ratio = round((preopen_ready / total_items), 4) if total_items else 0.0
    preopen_quality_score = max(
        0.0,
        min(
            100.0,
            round(
                ready_ratio * 100.0
                - min(20.0, preopen_missing * 1.0)
                - min(15.0, external_or_missing * 1.5),
                2,
            ),
        ),
    )
    transition_ready = files["state_transition_history"]["exists"] and files["state_transition_history"]["size"] > 20
    outcome_ready = (
        files["markout_summary"]["exists"]
        and files["timepoint_markout"]["exists"]
        and files["gradebook"]["exists"]
    )

    payload = {
        "ts": today.isoformat(timespec="seconds"),
        "status": "PASS" if required_ready else "WARN",
        "scope": "surge_preopen_precursor_readiness",
        "calendar": {
            "date": today.strftime("%Y%m%d"),
            "weekday": today.strftime("%A"),
            "is_weekend": is_weekend,
            "intraday_validation_available": not is_weekend,
            "note": "if weekend/holiday, intraday LOB, tick, VWAP, and state transition validation is unavailable",
        },
        "summary": {
            "total_items": total_items,
            "preopen_ready_items": preopen_ready,
            "preopen_source_not_implemented_items": preopen_missing,
            "intraday_required_items": intraday_required,
            "external_or_missing_items": external_or_missing,
            "not_preopen_source_items": int(status_counts.get("NOT_PREOPEN_SOURCE", 0)),
            "preopen_ready_ratio": ready_ratio,
            "preopen_quality_score": preopen_quality_score,
            "status_counts": dict(status_counts),
            "category_ready_counts": dict(category_ready),
            "state_shadow_rows": (state.get("summary") or {}).get("rows", 0),
            "state_shadow_counts": (state.get("summary") or {}).get("state_counts", {}),
        },
        "warnings": {
            "preopen_source_not_implemented_top": _top_items(out_rows, "PREOPEN_SOURCE_NOT_IMPLEMENTED"),
            "intraday_required_top": _top_items(out_rows, "INTRADAY_REQUIRED", limit=12),
            "external_or_missing_top": _top_items(out_rows, "EXTERNAL_OR_MISSING"),
        },
        "today_intraday_check": {
            "available_today": not is_weekend,
            "required_items": intraday_required,
            "top_items": _top_items(out_rows, "INTRADAY_REQUIRED", limit=12),
            "note": "intraday checks require an open market session; today is weekend/holiday if available_today=false",
        },
        "transition_outcome_readiness": {
            "state_transition_history_ready": bool(transition_ready),
            "state_transition_history_path": str(STATE_TRANSITION_HISTORY),
            "outcome_markout_ready": bool(outcome_ready),
            "markout_summary_path": str(MARKOUT_SUMMARY_JSON),
            "timepoint_markout_path": str(TIMEPOINT_MARKOUT_JSON),
            "gradebook_path": str(GRADEBOOK_JSON),
            "status": "PASS" if transition_ready and outcome_ready else ("PARTIAL" if outcome_ready else "WARN"),
            "note": "outcome artifacts exist separately; state-machine transition history is required for full readiness",
        },
        "files": files,
        "risk_contract": {
            "read_only_diagnostic": True,
            "policy_change": False,
            "entry_approval_changed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "dispatch_enabled": False,
            "trading_allowed": False,
        },
        "rows": out_rows,
    }

    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as fp:
        fields = ["category", "item", "coverage_status", "source_type", "preopen_status", "evidence_fields", "note"]
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        writer.writerows(out_rows)

    print(json.dumps({"status": payload["status"], **payload["summary"], "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
