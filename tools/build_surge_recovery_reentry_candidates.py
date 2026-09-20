from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

REVIEW_CSV = LOG_DIR / "surge_blocked_path_observation_review_latest.csv"
HISTORY_SUMMARY_JSON = LOG_DIR / "surge_blocked_path_observation_review_history_summary_latest.json"
OUT_JSON = LOG_DIR / "surge_recovery_reentry_candidates_latest.json"
OUT_CSV = LOG_DIR / "surge_recovery_reentry_candidates_latest.csv"
HISTORY_CSV = LOG_DIR / "surge_recovery_reentry_candidates_history.csv"
HISTORY_JSONL = LOG_DIR / "surge_recovery_reentry_candidates_history.jsonl"
HISTORY_SUMMARY_JSON = LOG_DIR / "surge_recovery_reentry_candidates_history_summary_latest.json"

RECOVERY_BLOCKERS = {
    "ENTRY_CHANGE_BLOCK",
    "HIGH_REJECTION_ENTRY_BLOCK",
    "NO_LOB_BLOCK",
    "SCORE_RVOL_OVERHEAT_BLOCK",
}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists() or path.stat().st_size <= 5:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        return list(csv.DictReader(fp))


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def _read_history_csv(path: Path) -> list[dict[str, str]]:
    return _read_csv(path)


def _append_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def _append_jsonl(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps({key: row.get(key, "") for key in fields}, ensure_ascii=False, sort_keys=True) + "\n")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size <= 0:
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _float(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value or "").replace(",", "").strip()
        return float(text) if text else float(default)
    except Exception:
        return float(default)


def _bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _avg(rows: list[dict[str, Any]], key: str) -> Any:
    values: list[float] = []
    for row in rows:
        try:
            text = str(row.get(key) or "").strip()
            if text:
                values.append(float(text))
        except ValueError:
            continue
    if not values:
        return ""
    return round(sum(values) / len(values), 6)


def _blockers(row: dict[str, Any]) -> set[str]:
    return {part for part in str(row.get("blockers") or "").split("|") if part}


def _history_condition_status(summary: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    breakdowns = summary.get("breakdowns") if isinstance(summary, dict) else {}
    for row in (breakdowns or {}).get("conditional_review", []):
        condition = str(row.get("condition") or "")
        status = str(row.get("review_status") or "")
        if condition:
            out[condition] = status
    return out


def _matched_conditions(row: dict[str, Any]) -> list[str]:
    blockers = _blockers(row)
    path_signal = str(row.get("path_signal") or "")
    conditions: list[str] = []
    if path_signal == "POSITIVE_WITH_V_REBOUND":
        if "NO_LOB_BLOCK" in blockers:
            conditions.append("NO_LOB_WITH_POSITIVE_V")
        if "HIGH_REJECTION_ENTRY_BLOCK" in blockers:
            conditions.append("HIGH_REJECTION_WITH_POSITIVE_V")
        if "ENTRY_CHANGE_BLOCK" in blockers:
            conditions.append("ENTRY_CHANGE_WITH_POSITIVE_V")
        if "SCORE_RVOL_OVERHEAT_BLOCK" in blockers:
            conditions.append("SCORE_RVOL_OVERHEAT_WITH_POSITIVE_V")
    if str(row.get("lob_status") or "") == "OK" and blockers:
        conditions.append("LOB_OK_BLOCKED_ANY")
    return conditions


def _classify(row: dict[str, str], condition_status: dict[str, str]) -> tuple[str, str, list[str]]:
    blockers = _blockers(row)
    matched = _matched_conditions(row)
    approved_condition = [
        condition for condition in matched if condition_status.get(condition) == "REVIEW_CONDITIONAL_RELAXATION"
    ]
    if (
        str(row.get("path_signal") or "") == "POSITIVE_WITH_V_REBOUND"
        and blockers.intersection(RECOVERY_BLOCKERS)
        and approved_condition
    ):
        return (
            "RECOVERY_REENTRY_CANDIDATE",
            "blocked_but_positive_v_rebound_matches_review_condition",
            approved_condition,
        )
    if str(row.get("outcome_class") or "") == "PATH_TEST_NEGATIVE":
        return "TRUE_EXCLUDE", "blocked_case_showed_negative_path", matched
    if str(row.get("outcome_class") or "") == "NOT_EVALUABLE":
        return "OBSERVE_MORE", "path_outcome_not_evaluable", matched
    return "OBSERVE_MORE", "recovery_reentry_condition_not_met", matched


def _history_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    class_counts = Counter(str(row.get("candidate_class") or "") for row in rows)
    recovery_rows = [row for row in rows if row.get("candidate_class") == "RECOVERY_REENTRY_CANDIDATE"]
    true_exclude_rows = [row for row in rows if row.get("candidate_class") == "TRUE_EXCLUDE"]
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "surge_recovery_reentry_candidates_history_summary",
        "source_files": {
            "history_csv": str(HISTORY_CSV),
            "history_jsonl": str(HISTORY_JSONL),
        },
        "summary": {
            "history_rows": len(rows),
            "candidate_class_counts": dict(sorted(class_counts.items())),
            "recovery_reentry_rows": len(recovery_rows),
            "true_exclude_rows": len(true_exclude_rows),
            "avg_recovery_ret_3m_pct": _avg(recovery_rows, "ret_3m_pct"),
            "avg_recovery_ret_5m_pct": _avg(recovery_rows, "ret_5m_pct"),
            "avg_recovery_ret_10m_pct": _avg(recovery_rows, "ret_10m_pct"),
            "avg_recovery_max_favorable_pct": _avg(recovery_rows, "max_favorable_pct"),
            "avg_recovery_max_adverse_pct": _avg(recovery_rows, "max_adverse_pct"),
            "avg_recovery_close_like_ret_pct": _avg(recovery_rows, "close_like_ret_pct"),
            "entry_allowed_rows": 0,
            "expectancy_buy_ready_rows": 0,
            "paper_order_route": False,
            "broker_order_route": False,
            "orders_modified": False,
            "fills_modified": False,
            "research_only": True,
        },
        "interpretation": (
            "Append-only virtual outcome history for recovery reentry candidates. "
            "This is not an entry approval, broker route, or threshold relaxation."
        ),
    }
    return payload


def build() -> dict[str, Any]:
    generated_at = datetime.now().isoformat(timespec="seconds")
    review_rows = _read_csv(REVIEW_CSV)
    condition_status = _history_condition_status(_read_json(HISTORY_SUMMARY_JSON))
    rows: list[dict[str, Any]] = []
    for row in review_rows:
        classification, reason, conditions = _classify(row, condition_status)
        rows.append(
            {
                "generated_at": generated_at,
                "observation_key": "|".join(
                    [
                        generated_at,
                        row.get("code", ""),
                        classification,
                        row.get("path_signal", ""),
                        row.get("blockers", ""),
                    ]
                ),
                "code": row.get("code", ""),
                "name": row.get("name", ""),
                "candidate_class": classification,
                "candidate_reason": reason,
                "matched_conditions": "|".join(conditions),
                "intent_class": row.get("intent_class", ""),
                "lob_status": row.get("lob_status", ""),
                "blockers": row.get("blockers", ""),
                "outcome_class": row.get("outcome_class", ""),
                "path_signal": row.get("path_signal", ""),
                "v_rebound_path_status": row.get("v_rebound_path_status", ""),
                "ret_3m_pct": row.get("ret_3m_pct", ""),
                "ret_5m_pct": row.get("ret_5m_pct", ""),
                "ret_10m_pct": row.get("ret_10m_pct", ""),
                "max_favorable_pct": row.get("max_favorable_pct", ""),
                "max_adverse_pct": row.get("max_adverse_pct", ""),
                "close_like_ret_pct": row.get("close_like_ret_pct", ""),
                "change_pct": _float(row.get("change_pct")),
                "remaining_to_30pct": _float(row.get("remaining_to_30pct")),
                "rvol20": _float(row.get("rvol20")),
                "trading_value": _float(row.get("trading_value")),
                "paper_virtual_candidate": classification == "RECOVERY_REENTRY_CANDIDATE",
                "entry_allowed": False,
                "expectancy_buy_ready": False,
                "paper_order_route": False,
                "broker_order_route": False,
                "orders_modified": False,
                "fills_modified": False,
                "research_only": True,
            }
        )
    counts = Counter(str(row["candidate_class"]) for row in rows)
    payload = {
        "generated_at": generated_at,
        "status": "OK",
        "scope": "surge_recovery_reentry_candidates",
        "source_files": {
            "review_csv": str(REVIEW_CSV),
            "history_summary_json": str(HISTORY_SUMMARY_JSON),
        },
        "summary": {
            "rows": len(rows),
            "candidate_class_counts": dict(sorted(counts.items())),
            "recovery_reentry_candidates": sum(1 for row in rows if row["candidate_class"] == "RECOVERY_REENTRY_CANDIDATE"),
            "paper_virtual_candidate_rows": sum(1 for row in rows if row["paper_virtual_candidate"] is True),
            "entry_allowed_rows": 0,
            "expectancy_buy_ready_rows": 0,
            "paper_order_route": False,
            "broker_order_route": False,
            "orders_modified": False,
            "fills_modified": False,
            "research_only": True,
        },
        "interpretation": (
            "Virtual/research candidate layer for blocked surge rows that recovered with positive V-rebound. "
            "This does not approve orders, route broker trades, or relax detector thresholds."
        ),
        "rows": rows,
    }
    fields = [
        "generated_at",
        "code",
        "name",
        "candidate_class",
        "candidate_reason",
        "matched_conditions",
        "intent_class",
        "lob_status",
        "blockers",
        "outcome_class",
        "path_signal",
        "v_rebound_path_status",
        "ret_3m_pct",
        "ret_5m_pct",
        "ret_10m_pct",
        "max_favorable_pct",
        "max_adverse_pct",
        "close_like_ret_pct",
        "change_pct",
        "remaining_to_30pct",
        "rvol20",
        "trading_value",
        "paper_virtual_candidate",
        "entry_allowed",
        "expectancy_buy_ready",
        "paper_order_route",
        "broker_order_route",
        "orders_modified",
        "fills_modified",
        "research_only",
    ]
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows, fields)
    history_fields = ["generated_at", "observation_key", *[field for field in fields if field != "generated_at"]]
    _append_csv(HISTORY_CSV, rows, history_fields)
    _append_jsonl(HISTORY_JSONL, rows, history_fields)
    history_payload = _history_summary(_read_history_csv(HISTORY_CSV))
    HISTORY_SUMMARY_JSON.write_text(json.dumps(history_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    payload["summary"]["history_rows"] = history_payload["summary"]["history_rows"]
    payload["source_files"]["history_csv"] = str(HISTORY_CSV)
    payload["source_files"]["history_jsonl"] = str(HISTORY_JSONL)
    payload["source_files"]["history_summary_json"] = str(HISTORY_SUMMARY_JSON)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> int:
    payload = build()
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
