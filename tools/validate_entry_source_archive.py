from __future__ import annotations

import csv
import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER_DIR = ROOT / "paper"

FILLS = PAPER_DIR / "fills.csv"
ARCHIVE = LOG_DIR / "entry_source_archive_history.csv"
OUT_JSON = LOG_DIR / "entry_source_archive_validation_latest.json"
OUT_CSV = LOG_DIR / "entry_source_archive_validation_latest.csv"

REQUIRED_NOTE_KEYS = ["entry_source_kind", "candidate_snapshot_id", "entry_order_id", "entry_trace_id"]
SURGE_SOURCE_KEYS = ["surge_type", "surge_score_final", "surge_rvol20", "surge_spread_bps"]
SECTOR_LINEAGE_KEYS = [
    "_sector_lineage_complete",
    "_sector_union_candidate",
    "_sector_union_ok",
    "_sector_union_strength_min",
    "_sector_lineage_fields_present",
]


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    return []


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _note_value(note: Any, key: str) -> str:
    text = str(note or "").replace(" | ", ";")
    m = re.search(rf"(?:^|[;|])\s*{re.escape(key)}=([^;|]*)", text)
    return m.group(1).strip() if m else ""


def _json_obj(value: Any) -> dict[str, Any]:
    try:
        obj = json.loads(str(value or "{}"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _archive_payload_status(archive: dict[str, str]) -> tuple[bool, str, dict[str, Any]]:
    if not archive:
        return False, "NO_ARCHIVE", {}
    source_kind = str(archive.get("entry_source_kind") or "").strip().upper()
    payload = _json_obj(archive.get("source_row_json"))
    if source_kind != "SURGE_RUNTIME":
        return True, "NOT_SURGE_RUNTIME", payload
    if not payload:
        return False, "SURGE_RUNTIME_SOURCE_ROW_JSON_EMPTY", payload
    missing = [
        key for key in SURGE_SOURCE_KEYS
        if str(payload.get(key) if payload.get(key) is not None else archive.get(key, "")).strip() == ""
    ]
    if missing:
        return False, "SURGE_RUNTIME_SOURCE_KEYS_MISSING:" + ",".join(missing), payload
    if str(payload.get("_surge_immediate") or archive.get("is_surge_immediate") or "").strip() in {"", "0", "0.0"}:
        return False, "SURGE_RUNTIME_IMMEDIATE_FLAG_MISSING", payload
    return True, "PASS_SURGE_RUNTIME_SOURCE_PAYLOAD", payload


def _sector_lineage_status(payload: dict[str, Any]) -> tuple[bool, str]:
    if not payload:
        return False, "NO_PAYLOAD"
    if str(payload.get("candidate_origin") or "").strip().upper() != "SECTOR_PREFILTER_UNION":
        return True, "NOT_SECTOR_PREFILTER_UNION"
    missing = [key for key in SECTOR_LINEAGE_KEYS if key not in payload]
    if missing:
        return False, "SECTOR_LINEAGE_KEYS_MISSING:" + ",".join(missing)
    if not bool(payload.get("_sector_lineage_complete")):
        return False, "SECTOR_LINEAGE_FIELDS_INCOMPLETE"
    return True, "PASS_SECTOR_LINEAGE_PAYLOAD"


def _ts_text(row: dict[str, str]) -> str:
    return str(row.get("datetime") or row.get("ts") or row.get("date") or "")


def build() -> dict[str, Any]:
    fills = _read_csv(FILLS)
    archive_rows = _read_csv(ARCHIVE)
    archive_by_order = {str(row.get("entry_order_id") or ""): row for row in archive_rows if row.get("entry_order_id")}

    buy_rows: list[dict[str, Any]] = []
    for row in fills:
        if str(row.get("side") or "").upper() != "BUY":
            continue
        note = row.get("note", "")
        order_id = str(row.get("order_id") or _note_value(note, "entry_order_id") or "")
        has_required = all(bool(_note_value(note, key)) for key in REQUIRED_NOTE_KEYS)
        has_new_keys = bool(_note_value(note, "entry_source_kind")) and bool(_note_value(note, "candidate_snapshot_id"))
        archive = archive_by_order.get(order_id, {})
        payload_ok, payload_reason, payload = _archive_payload_status(archive)
        sector_payload_ok, sector_payload_reason = _sector_lineage_status(payload)
        if has_required and archive and payload_ok:
            status = "PASS_ARCHIVE_MATCHED"
        elif has_required and archive and not payload_ok:
            status = "FAIL_ARCHIVE_SOURCE_PAYLOAD"
        elif has_required and not archive:
            status = "FAIL_NOTE_WITHOUT_ARCHIVE"
        elif has_new_keys and not archive:
            status = "FAIL_NEW_KEYS_WITHOUT_ARCHIVE"
        else:
            status = "LEGACY_OR_PRE_CHANGE_BUY"
        buy_rows.append({
            "ts": _ts_text(row),
            "code": str(row.get("code") or "").zfill(6),
            "order_id": order_id,
            "status": status,
            "has_entry_source_kind": bool(_note_value(note, "entry_source_kind")),
            "has_candidate_snapshot_id": bool(_note_value(note, "candidate_snapshot_id")),
            "has_archive": bool(archive),
            "entry_source_kind": _note_value(note, "entry_source_kind"),
            "candidate_snapshot_id": _note_value(note, "candidate_snapshot_id"),
            "archive_source_payload_ok": payload_ok,
            "archive_source_payload_reason": payload_reason,
            "archive_source_payload_key_count": len(payload),
            "archive_surge_type": archive.get("surge_type", "") or str(payload.get("surge_type") or ""),
            "archive_surge_score_final": archive.get("surge_score_final", "") or str(payload.get("surge_score_final") or ""),
            "archive_candidate_origin": str(payload.get("candidate_origin") or ""),
            "archive_sector_lineage_ok": sector_payload_ok,
            "archive_sector_lineage_reason": sector_payload_reason,
            "archive_sector_union_candidate": bool(payload.get("_sector_union_candidate")),
            "archive_sector_union_ok": bool(payload.get("_sector_union_ok")),
            "archive_sector_strength_min": str(payload.get("_sector_union_strength_min") or ""),
        })

    status_counts = Counter(str(row.get("status") or "") for row in buy_rows)
    post_change_rows = [
        row for row in buy_rows
        if row["status"] in {"PASS_ARCHIVE_MATCHED", "FAIL_NOTE_WITHOUT_ARCHIVE", "FAIL_NEW_KEYS_WITHOUT_ARCHIVE"}
    ]
    failures = [
        row for row in post_change_rows
        if row["status"] in {"FAIL_NOTE_WITHOUT_ARCHIVE", "FAIL_NEW_KEYS_WITHOUT_ARCHIVE", "FAIL_ARCHIVE_SOURCE_PAYLOAD"}
    ]
    if failures:
        status = "FAIL"
        reason = "post_change_buy_archive_mismatch"
    elif post_change_rows:
        status = "PASS"
        reason = "post_change_buy_archive_matched"
    else:
        status = "PENDING_NO_POST_CHANGE_BUY"
        reason = "no_buy_with_entry_source_keys_yet"

    result = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "status": status,
        "reason": reason,
        "policy_change": False,
        "trading_effect": False,
        "source": {
            "fills": str(FILLS),
            "archive": str(ARCHIVE),
        },
        "scope": {
            "buy_rows": len(buy_rows),
            "archive_rows": len(archive_rows),
            "post_change_buy_rows": len(post_change_rows),
            "failures": len(failures),
        },
        "breakdown": {
            "status_counts": dict(sorted(status_counts.items())),
        },
        "artifacts": {
            "csv": str(OUT_CSV),
        },
    }
    fields = [
        "ts", "code", "order_id", "status", "has_entry_source_kind",
        "has_candidate_snapshot_id", "has_archive", "entry_source_kind", "candidate_snapshot_id",
        "archive_source_payload_ok", "archive_source_payload_reason", "archive_source_payload_key_count",
        "archive_surge_type", "archive_surge_score_final", "archive_candidate_origin",
        "archive_sector_lineage_ok", "archive_sector_lineage_reason",
        "archive_sector_union_candidate", "archive_sector_union_ok", "archive_sector_strength_min",
    ]
    OUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, buy_rows[-100:], fields)
    return result


def main() -> int:
    result = build()
    print(json.dumps({
        "status": result["status"],
        "reason": result["reason"],
        "scope": result["scope"],
        "breakdown": result["breakdown"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
