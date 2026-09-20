"""Audit consistency between NO_LOB recheck queue and latest LOB snapshot.

The NO_LOB recheck queue is produced by an on-demand hoga probe. The latest LOB
snapshot can be produced by a later fetch with its own max-fetch cap. This
report makes any source/timing mismatch explicit without changing policy.
"""
from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

RECHECK_CSV = LOG_DIR / "surge_no_lob_recheck_queue_latest.csv"
OBSERVE_CSV = LOG_DIR / "surge_wait_lob_hoga_observe_latest.csv"
LATEST_LOB_CSV = LOG_DIR / "surge_lob_latest.csv"

OUT_JSON = LOG_DIR / "lob_recheck_source_consistency_audit_latest.json"
OUT_CSV = LOG_DIR / "lob_recheck_source_consistency_audit_latest.csv"

KST = timezone(timedelta(hours=9))


def _now() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: List[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _code(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text.isdigit() else text


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_dt(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=KST)
    return dt.astimezone(KST)


def _minutes_between(start: Any, end: Any) -> Optional[float]:
    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end)
    if not start_dt or not end_dt:
        return None
    return round((end_dt - start_dt).total_seconds() / 60.0, 4)


def _by_code(rows: Iterable[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code:
            out[code] = row
    return out


def _status(row: Dict[str, str]) -> str:
    available = _truthy(row.get("lob_available"))
    status = str(row.get("lob_status") or "").strip().upper()
    fetch = str(row.get("hoga_fetch_status") or "").strip().upper()
    if available and status in {"OK", "BID_ONLY_LIMIT", "LOB_OK", "AVAILABLE"}:
        return "LOB_OK"
    if fetch == "SKIP_MAX_FETCH":
        return "SKIP_MAX_FETCH"
    if status == "NO_LOB":
        return "NO_LOB"
    if fetch:
        return f"FETCH_{fetch}"
    return "UNKNOWN"


def _classify(queue_status: str, observe_status: str, latest_status: str) -> str:
    if queue_status == "LOB_OK" and observe_status == "LOB_OK" and latest_status == "LOB_OK":
        return "CONSISTENT_LOB_OK"
    if queue_status == "LOB_OK" and latest_status == "SKIP_MAX_FETCH":
        return "LATEST_FETCH_CAP_AFTER_RECHECK_OK"
    if queue_status == "LOB_OK" and latest_status == "NO_LOB":
        return "LATEST_NO_LOB_AFTER_RECHECK_OK"
    if queue_status == "LOB_OK" and latest_status not in {"LOB_OK", "UNKNOWN"}:
        return "LATEST_LOB_STATUS_CHANGED_AFTER_RECHECK"
    if queue_status != "LOB_OK":
        return "QUEUE_NOT_LOB_OK"
    return "UNKNOWN_CONSISTENCY"


def _row(queue: Dict[str, str], observe: Dict[str, str], latest: Dict[str, str]) -> Dict[str, Any]:
    code = _code(queue.get("code"))
    queue_status = _status(queue)
    observe_status = _status(observe)
    latest_status = _status(latest)
    queue_ts = queue.get("ts", "")
    observe_ts = observe.get("ts", "")
    latest_ts = latest.get("ts", "")
    return {
        "code": code,
        "consistency_class": _classify(queue_status, observe_status, latest_status),
        "queue_ts": queue_ts,
        "observe_ts": observe_ts,
        "latest_lob_ts": latest_ts,
        "queue_to_latest_lob_minutes": _minutes_between(queue_ts, latest_ts),
        "queue_probe_class": queue.get("probe_class", ""),
        "queue_lob_status_eval": queue_status,
        "observe_lob_status_eval": observe_status,
        "latest_lob_status_eval": latest_status,
        "queue_lob_available": _truthy(queue.get("lob_available")),
        "queue_lob_status": queue.get("lob_status", ""),
        "queue_hoga_fetch_status": queue.get("hoga_fetch_status", ""),
        "queue_spread_bps": round(_f(queue.get("spread_bps")), 6),
        "observe_lob_available": _truthy(observe.get("lob_available")),
        "observe_lob_status": observe.get("lob_status", ""),
        "observe_hoga_fetch_status": observe.get("hoga_fetch_status", ""),
        "observe_spread_bps": round(_f(observe.get("spread_bps")), 6),
        "latest_lob_available": _truthy(latest.get("lob_available")),
        "latest_lob_status": latest.get("lob_status", ""),
        "latest_hoga_fetch_status": latest.get("hoga_fetch_status", ""),
        "latest_spread_bps": round(_f(latest.get("spread_bps")), 6),
        "entry_approval_changed": False,
        "trading_allowed": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }


def main() -> int:
    generated_at = _now()
    queue_rows = _read_csv(RECHECK_CSV)
    observe_by_code = _by_code(_read_csv(OBSERVE_CSV))
    latest_by_code = _by_code(_read_csv(LATEST_LOB_CSV))
    rows = [
        _row(queue, observe_by_code.get(_code(queue.get("code")), {}), latest_by_code.get(_code(queue.get("code")), {}))
        for queue in queue_rows
    ]
    classes = sorted({str(row.get("consistency_class")) for row in rows})
    payload = {
        "generated_at": generated_at,
        "scope": "read_only_lob_recheck_source_consistency_audit",
        "policy_note": "Audit only. No live entry, order route, threshold, or gate behavior is changed.",
        "source_files": {
            "recheck_csv": str(RECHECK_CSV),
            "observe_csv": str(OBSERVE_CSV),
            "latest_lob_csv": str(LATEST_LOB_CSV),
        },
        "summary": {
            "rows": len(rows),
            "consistency_class_counts": {key: sum(1 for row in rows if row.get("consistency_class") == key) for key in classes},
            "latest_not_lob_ok_rows": sum(1 for row in rows if row.get("latest_lob_status_eval") != "LOB_OK"),
            "entry_approval_changed": False,
            "trading_effect": False,
            "policy_effect": False,
            "policy_change_applied": False,
        },
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"out_json": str(OUT_JSON), "out_csv": str(OUT_CSV), "summary": payload["summary"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
