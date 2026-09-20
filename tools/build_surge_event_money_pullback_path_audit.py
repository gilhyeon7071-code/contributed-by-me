from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SCREENER_CSV = LOG_DIR / "surge_event_money_pullback_screener_latest.csv"
FINAL_CANDIDATE_CSV = LOG_DIR / "candidates_latest_data.with_final_score.csv"
SURGE_REALTIME_CSV = LOG_DIR / "surge_realtime_latest.csv"
ENTRY_DECISION_CSV = LOG_DIR / "entry_decision_layers_runtime_latest.csv"
PENDING_STATUS_JSON = LOG_DIR / "pending_entry_status_latest.json"

OUT_JSON = LOG_DIR / "surge_event_money_pullback_path_audit_latest.json"
OUT_CSV = LOG_DIR / "surge_event_money_pullback_path_audit_latest.csv"

HARD_SURGE_BLOCKERS = (
    "KRX_WARNING",
    "KRX_RISK",
    "KRX_CAUTION",
    "HIGH_REJECTION_ENTRY_BLOCK",
    "ENTRY_CHANGE_BLOCK",
    "ENTRY_ATR_CAP",
    "NO_LOB_BLOCK",
    "INSUFFICIENT_FEATURES",
)

GENERAL_FILTER_TOKENS = (
    "adx",
    "stoch",
    "disparity",
    "high52",
    "rs(",
    "atr(",
    "v_accel",
)


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def _code(row: dict[str, Any]) -> str:
    value = str(row.get("code") or "").strip()
    return value.zfill(6) if value.isdigit() else value


def _index(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {_code(row): row for row in rows if _code(row)}


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = "" if value is None else str(value).strip()
        if text and text.lower() not in {"nan", "<na>", "none"}:
            return text
    return ""


def _float(value: Any) -> float:
    try:
        text = str(value).strip()
        if not text or text.lower() in {"nan", "<na>", "none"}:
            return 0.0
        return float(text)
    except Exception:
        return 0.0


def _boolish(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _entry_rows_from_status(status: dict[str, Any]) -> dict[str, dict[str, str]]:
    rows = {}
    for item in status.get("entry_decision_rows") or []:
        if isinstance(item, dict):
            row = {str(k): "" if v is None else str(v) for k, v in item.items()}
            code = _code(row)
            if code:
                rows[code] = row
    return rows


def _has_hard_block(text: str) -> bool:
    upper = text.upper()
    return any(token in upper for token in HARD_SURGE_BLOCKERS)


def _has_general_filter(text: str) -> bool:
    lower = text.lower()
    return any(token in lower for token in GENERAL_FILTER_TOKENS)


def _classify_path(
    screener: dict[str, str],
    final_row: dict[str, str],
    surge_row: dict[str, str],
    entry_row: dict[str, str],
    status_entry: dict[str, str],
) -> tuple[str, str, str]:
    bucket = _first_nonempty(screener.get("criteria_first_bucket"))
    risk_score = _float(screener.get("risk_score"))
    risk_evidence = _first_nonempty(screener.get("risk_evidence"))
    failed_filters = _first_nonempty(final_row.get("failed_filters"))
    execution_pool = _boolish(final_row.get("execution_pool"))
    natural_pass = _boolish(final_row.get("natural_pass"))
    surge_decision = _first_nonempty(surge_row.get("entry_decision"))
    surge_reason = _first_nonempty(surge_row.get("entry_reason"), surge_row.get("exclude_reasons"))
    entry_signal = _first_nonempty(entry_row.get("signal"), status_entry.get("signal"))
    entry_reason = _first_nonempty(
        entry_row.get("execution_reason"),
        entry_row.get("pending_reason"),
        status_entry.get("reason"),
    )

    if entry_signal:
        return "REACHED_ENTRY_LAYER", "entry_layer_has_signal", entry_reason

    if surge_decision == "ENTRY_ALLOWED":
        return "SURGE_ALLOWED_NOT_IN_ENTRY_LAYER", "surge_allowed_but_entry_layer_absent", surge_reason

    if surge_decision == "ENTRY_BLOCKED" or _boolish(surge_row.get("entry_blocked")):
        if _has_hard_block(surge_reason):
            return "VALID_RISK_OR_EXECUTION_BLOCK", "surge_path_hard_block", surge_reason
        return "SURGE_PATH_BLOCK_REVIEW", "surge_path_block_without_known_hard_token", surge_reason

    if final_row and (not execution_pool or not natural_pass):
        if bucket == "A_BUYABLE_WATCH" and risk_score <= 0.75 and _has_general_filter(failed_filters):
            return (
                "BRIDGE_REVIEW_NEEDED",
                "criteria_a_blocked_by_general_candidate_filters",
                failed_filters,
            )
        if risk_score > 0:
            return "WATCH_ONLY_RISK_REDUCED", f"screener_risk={risk_score:g}:{risk_evidence}", failed_filters
        return "GENERAL_CANDIDATE_FILTER_BLOCK", "final_candidate_not_execution_pool", failed_filters

    if surge_decision in {"NOT_DETECTED", "NO_REALTIME_SURGE"}:
        if bucket == "A_BUYABLE_WATCH" and risk_score <= 0.75:
            return "BRIDGE_REVIEW_NEEDED", "criteria_a_not_detected_by_realtime_surge", surge_reason
        return "NO_REALTIME_SURGE_PATH", "not_detected_by_realtime_surge", surge_reason

    if not final_row and not surge_row:
        return "NO_CURRENT_PATH", "not_in_final_or_surge_path", ""

    return "PATH_UNCLEAR_REVIEW", "unclassified_path_gap", _first_nonempty(surge_reason, failed_filters)


def main() -> int:
    screener_rows = _read_csv(SCREENER_CSV)
    final_by_code = _index(_read_csv(FINAL_CANDIDATE_CSV))
    surge_by_code = _index(_read_csv(SURGE_REALTIME_CSV))
    entry_by_code = _index(_read_csv(ENTRY_DECISION_CSV))
    status = _read_json(PENDING_STATUS_JSON)
    status_entry_by_code = _entry_rows_from_status(status)

    watch_rows = [row for row in screener_rows if row.get("criteria_first_bucket") != "C_TRUE_EXCLUDE"]

    out_rows: list[dict[str, Any]] = []
    for row in watch_rows:
        code = _code(row)
        final_row = final_by_code.get(code, {})
        surge_row = surge_by_code.get(code, {})
        entry_row = entry_by_code.get(code, {})
        status_entry = status_entry_by_code.get(code, {})

        source_hits = []
        if final_row:
            source_hits.append("final_candidate")
        if surge_row:
            source_hits.append("surge_realtime")
        if entry_row:
            source_hits.append("entry_layer")
        if status_entry:
            source_hits.append("pending_status_entry")

        path_status, path_reason, blocker_detail = _classify_path(
            row,
            final_row,
            surge_row,
            entry_row,
            status_entry,
        )

        out_rows.append(
            {
                "code": code,
                "name": row.get("name", ""),
                "criteria_first_bucket": row.get("criteria_first_bucket", ""),
                "criteria_first_bucket_display": row.get("criteria_first_bucket_display", ""),
                "bucket_contract": row.get("bucket_contract", ""),
                "watch_subtype": row.get("watch_subtype", ""),
                "quality_label": row.get("quality_label", ""),
                "total_structure_score": row.get("total_structure_score", ""),
                "event_score": row.get("event_score", ""),
                "money_score": row.get("money_score", ""),
                "pullback_score": row.get("pullback_score", ""),
                "risk_score": row.get("risk_score", ""),
                "risk_evidence": row.get("risk_evidence", ""),
                "source_hits": "|".join(source_hits),
                "path_status": path_status,
                "path_reason": path_reason,
                "blocker_detail": blocker_detail,
                "final_score": final_row.get("final_score", ""),
                "execution_pool": final_row.get("execution_pool", ""),
                "natural_pass": final_row.get("natural_pass", ""),
                "failed_filters": final_row.get("failed_filters", ""),
                "surge_entry_decision": surge_row.get("entry_decision", ""),
                "surge_entry_allowed": surge_row.get("entry_allowed", ""),
                "surge_entry_blocked": surge_row.get("entry_blocked", ""),
                "surge_entry_reason": surge_row.get("entry_reason", ""),
                "surge_exclude_reasons": surge_row.get("exclude_reasons", ""),
                "entry_signal": _first_nonempty(entry_row.get("signal"), status_entry.get("signal")),
                "entry_reason": _first_nonempty(
                    entry_row.get("execution_reason"),
                    entry_row.get("pending_reason"),
                    status_entry.get("reason"),
                ),
                "research_only": True,
                "policy_change": False,
                "entry_approval_changed": False,
                "paper_order_route": False,
                "broker_order_route": False,
                "trading_route": False,
                "connection_level": "SIGNAL_QUALITY_ONLY",
                "trading_connection": False,
                "signal_connection": False,
                "execution_connection": False,
                "connection_note": "Signal quality or observation only; not entry approval, order, fill, or ledger.",
            }
        )

    status_counts = Counter(str(row["path_status"]) for row in out_rows)
    reason_counts = Counter(str(row["path_reason"]) for row in out_rows)
    bucket_counts = Counter(str(row["criteria_first_bucket"]) for row in out_rows)

    summary = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "research_only": True,
        "policy_change": False,
        "entry_approval_changed": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "trading_route": False,
        "connection_level": "SIGNAL_QUALITY_ONLY",
        "trading_connection": False,
        "signal_connection": False,
        "execution_connection": False,
        "connection_note": "Signal quality or observation only; not entry approval, order, fill, or ledger.",
        "inputs": {
            "screener_csv": str(SCREENER_CSV),
            "final_candidate_csv": str(FINAL_CANDIDATE_CSV),
            "surge_realtime_csv": str(SURGE_REALTIME_CSV),
            "entry_decision_csv": str(ENTRY_DECISION_CSV),
            "pending_status_json": str(PENDING_STATUS_JSON),
        },
        "input_rows": {
            "screener_watch_rows": len(watch_rows),
            "final_candidate_rows": len(final_by_code),
            "surge_realtime_rows": len(surge_by_code),
            "entry_decision_rows": len(entry_by_code),
            "pending_status_entry_rows": len(status_entry_by_code),
        },
        "pending_status_snapshot": {
            "generated_at": status.get("generated_at", ""),
            "status": status.get("status", ""),
            "status_reason": status.get("status_reason", ""),
            "candidates_after_caps": status.get("candidates_after_caps", ""),
            "entry_ready": status.get("entry_ready", ""),
            "filled": status.get("filled", ""),
            "pending_queue_len_raw": status.get("pending_queue_len_raw", ""),
            "max_new": status.get("max_new", ""),
            "max_new_surge": status.get("max_new_surge", ""),
        },
        "bucket_counts": dict(bucket_counts),
        "path_status_counts": dict(status_counts),
        "path_reason_counts": dict(reason_counts),
        "rows": out_rows,
    }

    OUT_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    fieldnames = list(out_rows[0].keys()) if out_rows else [
        "code",
        "name",
        "criteria_first_bucket",
        "criteria_first_bucket_display",
        "bucket_contract",
        "path_status",
        "path_reason",
        "blocker_detail",
    ]
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    print(json.dumps({"path_status_counts": dict(status_counts), "path_reason_counts": dict(reason_counts)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
