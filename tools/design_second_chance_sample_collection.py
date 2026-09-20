"""Design a read-only sample collection plan for second-chance entries.

This does not change trading policy. It converts the current evidence audit
and shadow second-chance evaluation into an explicit readiness judgment for
future policy calibration.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

AUDIT_JSON = LOG_DIR / "existing_second_chance_data_audit_latest.json"
SECOND_CHANCE_JSON = LOG_DIR / "second_chance_shadow_entry_latest.json"
OUT_JSON = LOG_DIR / "second_chance_sample_collection_design_latest.json"
OUT_CSV = LOG_DIR / "second_chance_sample_collection_design_latest.csv"


MIN_SHADOW_HISTORY_ROWS = 40
MIN_REVIEW_READY_ROWS = 10
MIN_UNIQUE_REVIEW_READY_CODES = 5
MIN_TARGET_SERIES_CODES = 5
MIN_SURGE_SNAPSHOT_ROWS = 300


def _now_kst() -> str:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).isoformat(timespec="seconds")


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _i(value: Any, default: int = 0) -> int:
    try:
        if value is None or str(value).strip() == "":
            return int(default)
        return int(float(str(value)))
    except Exception:
        return int(default)


def _code(value: Any) -> str:
    raw = "".join(ch for ch in str(value or "") if ch.isdigit())
    return raw.zfill(6)[-6:] if raw else ""


def _unique_ready_codes(second_doc: Dict[str, Any]) -> List[str]:
    rows = second_doc.get("rows") if isinstance(second_doc.get("rows"), list) else []
    codes = {
        _code(row.get("code"))
        for row in rows
        if isinstance(row, dict)
        and str(row.get("second_chance_verdict") or "") == "SECOND_CHANCE_REVIEW_READY"
        and _code(row.get("code"))
    }
    return sorted(codes)


def _recommendation(
    summary: Dict[str, Any],
    second_summary: Dict[str, Any],
    unique_ready_codes: List[str],
) -> str:
    shadow_rows = _i(summary.get("shadow_history_rows"))
    ready_rows = _i(second_summary.get("review_ready_rows"))
    target_series = _i(summary.get("target_codes_with_surge_series"))
    surge_rows = _i(summary.get("surge_snapshot_rows"))
    if ready_rows <= 0:
        return "KEEP_SHADOW_ONLY"
    if shadow_rows < MIN_SHADOW_HISTORY_ROWS:
        return "KEEP_SHADOW_UNTIL_BASELINE"
    if target_series < MIN_TARGET_SERIES_CODES or surge_rows < MIN_SURGE_SNAPSHOT_ROWS:
        return "KEEP_SHADOW_UNTIL_COVERAGE"
    if ready_rows < MIN_REVIEW_READY_ROWS:
        return "MIN_ENTRY_PROBE_AFTER_APPROVAL"
    if len(unique_ready_codes) < MIN_UNIQUE_REVIEW_READY_CODES:
        return "KEEP_SHADOW_UNTIL_DIVERSIFIED_SAMPLE"
    return "GRADUAL_REDUCTION_AFTER_APPROVAL"


def _readiness_checks(
    summary: Dict[str, Any],
    second_summary: Dict[str, Any],
    unique_ready_codes: List[str],
) -> List[Dict[str, Any]]:
    checks = [
        {
            "check": "shadow_history_baseline",
            "actual": _i(summary.get("shadow_history_rows")),
            "required": MIN_SHADOW_HISTORY_ROWS,
            "status": "PASS" if _i(summary.get("shadow_history_rows")) >= MIN_SHADOW_HISTORY_ROWS else "FAIL",
            "meaning": "enough blocked/review rows exist for threshold calibration",
        },
        {
            "check": "second_chance_ready_rows",
            "actual": _i(second_summary.get("review_ready_rows")),
            "required": MIN_REVIEW_READY_ROWS,
            "status": "PASS" if _i(second_summary.get("review_ready_rows")) >= MIN_REVIEW_READY_ROWS else "FAIL",
            "meaning": "enough cooled candidates exist to evaluate a second-chance rule",
        },
        {
            "check": "second_chance_unique_ready_codes",
            "actual": len(unique_ready_codes),
            "required": MIN_UNIQUE_REVIEW_READY_CODES,
            "status": "PASS" if len(unique_ready_codes) >= MIN_UNIQUE_REVIEW_READY_CODES else "FAIL",
            "meaning": "review-ready rows are spread across enough unique codes to avoid duplicate concentration",
        },
        {
            "check": "target_surge_series_coverage",
            "actual": _i(summary.get("target_codes_with_surge_series")),
            "required": MIN_TARGET_SERIES_CODES,
            "status": "PASS" if _i(summary.get("target_codes_with_surge_series")) >= MIN_TARGET_SERIES_CODES else "FAIL",
            "meaning": "current target candidates have saved surge time-series coverage",
        },
        {
            "check": "surge_snapshot_depth",
            "actual": _i(summary.get("surge_snapshot_rows")),
            "required": MIN_SURGE_SNAPSHOT_ROWS,
            "status": "PASS" if _i(summary.get("surge_snapshot_rows")) >= MIN_SURGE_SNAPSHOT_ROWS else "FAIL",
            "meaning": "enough intraday surge rows exist to inspect overheat and cooldown behavior",
        },
        {
            "check": "real_order_fill_baseline",
            "actual": 0,
            "required": 1,
            "status": "NA",
            "meaning": "requires a separately approved order/fill/ledger sample path",
        },
    ]
    return checks


def _option_rows(recommendation: str) -> List[Dict[str, Any]]:
    rows = [
        {
            "option": "KEEP_SHADOW_ONLY",
            "current_fit": recommendation in {
                "KEEP_SHADOW_ONLY",
                "KEEP_SHADOW_UNTIL_BASELINE",
                "KEEP_SHADOW_UNTIL_COVERAGE",
                "KEEP_SHADOW_UNTIL_DIVERSIFIED_SAMPLE",
            },
            "policy_effect": False,
            "trading_effect": False,
            "when_valid": "when review-ready rows are zero or baseline is too small",
            "validation_method": "collect shadow decisions, blocked reasons, target price paths, and cooldown states",
            "risk": "no execution sample, so final policy values remain unproven",
        },
        {
            "option": "MIN_ENTRY_PROBE_AFTER_APPROVAL",
            "current_fit": recommendation == "MIN_ENTRY_PROBE_AFTER_APPROVAL",
            "policy_effect": True,
            "trading_effect": True,
            "when_valid": "after enough shadow baseline exists but review-ready rows are still limited",
            "validation_method": "one separated minimum-quantity session with strict caps and separate reporting",
            "risk": "small sample can be noisy; must stay separate from production policy",
        },
        {
            "option": "GRADUAL_REDUCTION_AFTER_APPROVAL",
            "current_fit": recommendation == "GRADUAL_REDUCTION_AFTER_APPROVAL",
            "policy_effect": True,
            "trading_effect": True,
            "when_valid": "after shadow baseline and minimum probe both prove order/fill/ledger behavior",
            "validation_method": "reduce only one blocking axis at a time and compare against shadow control rows",
            "risk": "changes policy behavior and can bias future data if applied before baseline",
        },
    ]
    return rows


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "option",
        "current_fit",
        "policy_effect",
        "trading_effect",
        "when_valid",
        "validation_method",
        "risk",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    audit = _read_json(AUDIT_JSON)
    second = _read_json(SECOND_CHANCE_JSON)
    summary = audit.get("summary") if isinstance(audit.get("summary"), dict) else {}
    second_summary = second.get("summary") if isinstance(second.get("summary"), dict) else {}
    unique_ready_codes = _unique_ready_codes(second)
    checks = _readiness_checks(summary, second_summary, unique_ready_codes)
    recommendation = _recommendation(summary, second_summary, unique_ready_codes)
    options = _option_rows(recommendation)
    fail_checks = [r["check"] for r in checks if r.get("status") == "FAIL"]

    payload = {
        "generated_at": _now_kst(),
        "schema_version": "second_chance_sample_collection_design_v1",
        "generated_from": {
            "existing_data_audit": str(AUDIT_JSON),
            "second_chance_shadow_entry": str(SECOND_CHANCE_JSON),
        },
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "recommendation": recommendation,
        "readiness": {
            "status": "NOT_READY_FOR_POLICY_CHANGE" if fail_checks else "READY_FOR_POLICY_REVIEW",
            "failed_checks": fail_checks,
            "checks": checks,
        },
        "sample_design": {
            "recommended_current_structure": (
                "shadow-only sample collection with a later separated minimum-entry probe only after approval"
                if recommendation.startswith("KEEP_SHADOW")
                else recommendation
            ),
            "required_fields_for_future_samples": [
                "candidate_time",
                "code",
                "alpha_reason",
                "blocked_reason",
                "change_pct",
                "atr14_pct",
                "rvol20",
                "drawdown_from_intraday_high",
                "lob_available",
                "spread_bps",
                "orderflow_risk_score",
                "shadow_entry_price",
                "next_5m_return",
                "next_30m_return",
                "close_return",
                "would_order",
                "would_fill",
                "fill_or_block_reason",
            ],
            "policy_value_rule": (
                "Use existing data to reject unsafe relaxations. Do not derive final "
                "thresholds until separated baseline and approved probe samples exist."
            ),
            "duplicate_concentration_rule": (
                "Do not treat row count as sufficient when review-ready rows are "
                "concentrated in fewer than the required number of unique codes."
            ),
            "unique_ready_code_list": unique_ready_codes,
        },
        "options": options,
        "artifacts": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
        },
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, options)
    print(
        json.dumps(
            {
                "status": "OK",
                "recommendation": recommendation,
                "readiness_status": payload["readiness"]["status"],
                "failed_checks": fail_checks,
                "out_json": str(OUT_JSON),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
