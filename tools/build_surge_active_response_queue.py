from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SOURCE_JSON = LOG_DIR / "surge_active_response_layer_latest.json"
SOURCE_CSV = LOG_DIR / "surge_active_response_layer_latest.csv"
OUT_JSON = LOG_DIR / "surge_active_response_queue_latest.json"
OUT_CSV = LOG_DIR / "surge_active_response_queue_latest.csv"
HISTORY_CSV = LOG_DIR / "surge_active_response_queue_history.csv"
HISTORY_JSONL = LOG_DIR / "surge_active_response_queue_history.jsonl"

QUEUE_LABELS = {"PROBE_READY", "WAIT_LOB", "WAIT_RECLAIM"}

QUEUE_FIELDS = [
    "queue_generated_at",
    "source_ts",
    "ts",
    "date",
    "code",
    "detected_surge_type",
    "active_response_label",
    "active_response_reason",
    "active_response_next_check",
    "entry_decision",
    "entry_reason",
    "entry_allowed",
    "entry_blocked",
    "change_pct",
    "rvol20",
    "trading_value",
    "lob_status",
    "lob_available",
    "spread_bps",
    "markout_1step_bps",
    "exclude_reasons",
    "research_only",
    "policy_change",
    "entry_approval_changed",
    "paper_order_route",
    "broker_order_route",
    "trading_route",
]


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists() or path.stat().st_size <= 5:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        return list(csv.DictReader(fp))


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size <= 0:
        return {}
    for enc in ("utf-8-sig", "utf-8"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except UnicodeDecodeError:
            continue
        except json.JSONDecodeError:
            return {}
    return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _append_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists() and path.stat().st_size > 5
    with path.open("a", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")


def _bool_false_flags(row: dict[str, Any]) -> None:
    row["research_only"] = True
    row["policy_change"] = False
    row["entry_approval_changed"] = False
    row["paper_order_route"] = False
    row["broker_order_route"] = False
    row["trading_route"] = False


def build() -> dict[str, Any]:
    generated_at = _now_ts()
    source_meta = _read_json(SOURCE_JSON)
    source_rows = _read_csv(SOURCE_CSV)

    if not source_rows:
        payload = {
            "status": "FAIL",
            "reason": "SOURCE_MISSING_OR_EMPTY",
            "generated_at": generated_at,
            "scope": "read_only_surge_active_response_queue",
            "research_only": True,
            "policy_change": False,
            "entry_approval_changed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_route": False,
            "source_files": {"active_response_json": str(SOURCE_JSON), "active_response_csv": str(SOURCE_CSV)},
            "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV)},
            "access_issues": ["active response layer source is missing or empty"],
        }
        _write_json(OUT_JSON, payload)
        _write_csv(OUT_CSV, [], QUEUE_FIELDS)
        return payload

    source_ts = str(source_meta.get("source_ts") or source_meta.get("generated_at") or "")
    queue_rows: list[dict[str, Any]] = []
    for source_row in source_rows:
        label = str(source_row.get("active_response_label") or "").strip()
        if label not in QUEUE_LABELS:
            continue
        row = dict(source_row)
        row["queue_generated_at"] = generated_at
        row["source_ts"] = source_ts
        _bool_false_flags(row)
        queue_rows.append(row)

    _write_csv(OUT_CSV, queue_rows, QUEUE_FIELDS)
    if queue_rows:
        _append_csv(HISTORY_CSV, queue_rows, QUEUE_FIELDS)

    payload = {
        "status": "PASS",
        "reason": "ok",
        "generated_at": generated_at,
        "source_ts": source_ts,
        "scope": "read_only_surge_active_response_queue",
        "research_only": True,
        "policy_change": False,
        "entry_approval_changed": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "trading_route": False,
        "source_files": {
            "active_response_json": str(SOURCE_JSON),
            "active_response_csv": str(SOURCE_CSV),
        },
        "outputs": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
            "history_csv": str(HISTORY_CSV),
            "history_jsonl": str(HISTORY_JSONL),
        },
        "source_counts": {
            "active_response_rows": len(source_rows),
            "queue_rows": len(queue_rows),
            "source_detected_rows": source_meta.get("source_counts", {}).get("detected_rows", ""),
        },
        "queue_label_counts": dict(Counter(row.get("active_response_label") for row in queue_rows)),
        "queue_next_check_counts": dict(Counter(row.get("active_response_next_check") for row in queue_rows)),
        "production_entry_decision_counts": dict(Counter(row.get("entry_decision") for row in queue_rows)),
        "production_entry_reason_counts": dict(Counter(row.get("entry_reason") for row in queue_rows)),
        "codes_by_label": {
            label: [row.get("code") for row in queue_rows if row.get("active_response_label") == label]
            for label in sorted(QUEUE_LABELS)
        },
        "rows": queue_rows[:100],
        "access_issues": [
            "read-only queue only; does not approve live or paper orders",
            "PROBE_READY is queued for review only; existing production entry decisions are not bypassed",
            "WAIT_LOB requires later LOB availability before any entry review",
            "WAIT_RECLAIM requires VWAP or high reclaim evidence before any entry review",
            "existing production entry decisions are preserved and not bypassed",
        ],
    }
    _write_json(OUT_JSON, payload)
    _append_jsonl(HISTORY_JSONL, payload)
    return payload


if __name__ == "__main__":
    print(json.dumps(build(), ensure_ascii=False, indent=2))
