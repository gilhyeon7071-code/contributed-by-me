"""Trace archived source payloads for unresolved current-day entries."""
from __future__ import annotations

import csv
import datetime as dt
import json
import re
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CODES = {"003280", "058970", "195940", "087010", "328380"}

POST_ENTRY_CSV = LOG_DIR / "post_entry_learning_latest.csv"
ARCHIVE_CSV = LOG_DIR / "entry_source_archive_history.csv"
OUT_JSON = LOG_DIR / "entry_archive_rootcause_trace_latest.json"
OUT_CSV = LOG_DIR / "entry_archive_rootcause_trace_latest.csv"


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return [{str(k): str(v) for k, v in row.items()} for row in csv.DictReader(f)]
        except UnicodeDecodeError:
            continue
    return []


def _json_obj(value: Any) -> Dict[str, Any]:
    try:
        obj = json.loads(str(value or "{}"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _note_value(note: Any, key: str) -> str:
    text = str(note or "").replace(" | ", ";")
    match = re.search(rf"(?:^|[;|])\s*{re.escape(key)}=([^;|]*)", text)
    return match.group(1).strip() if match else ""


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value if value is not None else "").strip()
        if not text or text.lower() == "nan":
            return default
        return float(text)
    except Exception:
        return default


def _b(value: Any) -> bool:
    return str(value if value is not None else "").strip().lower() in {"1", "true", "t", "yes", "y"}


def _now() -> str:
    kst = dt.timezone(dt.timedelta(hours=9))
    return dt.datetime.now(tz=kst).isoformat(timespec="seconds")


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _root_cause(payload: Dict[str, Any]) -> str:
    positive_reason = str(payload.get("positive_entry_reason") or "")
    if "fresh_sector_allowed_fallback" in positive_reason:
        return "FRESH_SECTOR_ALLOWED_FALLBACK_SELECTED_ARCHIVED_ROW"
    if str(payload.get("candidate_origin") or "").upper() == "SECTOR_PREFILTER_UNION":
        return "SECTOR_PREFILTER_UNION_ARCHIVED_ROW"
    return "ARCHIVED_ROW_OTHER"


def main() -> int:
    post = [
        row
        for row in _read_csv(POST_ENTRY_CSV)
        if row.get("code") in CODES and row.get("entry_origin") == "FILL_OBSERVED_UNRESOLVED"
    ]
    archive_by_order = {
        str(row.get("entry_order_id") or ""): row
        for row in _read_csv(ARCHIVE_CSV)
        if str(row.get("entry_order_id") or "")
    }

    rows: List[Dict[str, Any]] = []
    for entry in sorted(post, key=lambda row: row.get("code", "")):
        order_id = str(entry.get("order_id") or "")
        archive = archive_by_order.get(order_id, {})
        payload = _json_obj(archive.get("source_row_json"))
        sector_lineage_fields = payload.get("_sector_lineage_fields_present")
        rows.append(
            {
                "code": entry.get("code", ""),
                "name": entry.get("name", ""),
                "order_id": order_id,
                "entry_ts": entry.get("entry_ts", ""),
                "unrealized_return_pct": entry.get("unrealized_return_pct", ""),
                "exit_reason": entry.get("exit_reason", ""),
                "entry_source_kind": archive.get("entry_source_kind", ""),
                "candidate_snapshot_id": archive.get("candidate_snapshot_id", ""),
                "archive_matched": bool(archive),
                "archive_entry_timing": archive.get("entry_timing", ""),
                "archive_fallback_stage": archive.get("fallback_stage", ""),
                "archive_horizon": archive.get("horizon", ""),
                "candidate_origin": payload.get("candidate_origin", ""),
                "execution_pool": str(payload.get("execution_pool", "")),
                "natural_pass": str(payload.get("natural_pass", "")),
                "relax_level": payload.get("relax_level", ""),
                "final_score": payload.get("final_score", ""),
                "entry_execution_score": payload.get("entry_execution_score", ""),
                "entry_execution_reason": payload.get("entry_execution_reason", ""),
                "positive_entry_ok": str(payload.get("positive_entry_ok", "")),
                "positive_entry_reason": payload.get("positive_entry_reason", ""),
                "sector_action": payload.get("sector_action", ""),
                "sector_entry_allowed": str(payload.get("sector_entry_allowed", "")),
                "sector_strength": payload.get("sector_strength", ""),
                "sector_union_ok": str(payload.get("_sector_union_ok", "")),
                "sector_lineage_complete": str(payload.get("_sector_lineage_complete", "")),
                "sector_lineage_fields_present": json.dumps(sector_lineage_fields, ensure_ascii=False, sort_keys=True),
                "execution_lob_status": payload.get("execution_lob_status", ""),
                "execution_orderflow_tag": payload.get("execution_orderflow_tag", ""),
                "krx_caution": str(payload.get("krx_caution", "")),
                "junk_risk_grade": payload.get("junk_risk_grade", ""),
                "junk_flags": payload.get("junk_flags", ""),
                "select_score": payload.get("_select_score", ""),
                "entry_gate_decision": payload.get("_entry_gate_decision", ""),
                "entry_gate_reason": payload.get("_entry_gate_reason", ""),
                "risk_orch_scale": payload.get("_risk_orch_scale", ""),
                "root_cause_class": _root_cause(payload),
            }
        )

    fallback_rows = [row for row in rows if row["root_cause_class"] == "FRESH_SECTOR_ALLOWED_FALLBACK_SELECTED_ARCHIVED_ROW"]
    sector_wait_rows = [row for row in rows if str(row.get("sector_action") or "").upper() == "WAIT"]
    no_lob_rows = [row for row in rows if str(row.get("execution_lob_status") or "").upper() == "NO_LOB"]
    no_history_rows = [row for row in rows if str(row.get("execution_orderflow_tag") or "").upper() == "NO_HISTORY"]
    loss_or_stop_rows = [row for row in rows if _f(row.get("unrealized_return_pct")) < 0 or row.get("exit_reason")]

    payload = {
        "generated_at": _now(),
        "schema_version": "entry_archive_rootcause_trace_v1",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "scope": "archived source payload trace for unresolved current-day entries",
        "summary": {
            "traced_codes": len(rows),
            "archive_matched": sum(1 for row in rows if row["archive_matched"]),
            "fresh_sector_allowed_fallback_rows": len(fallback_rows),
            "sector_wait_rows": len(sector_wait_rows),
            "no_lob_rows": len(no_lob_rows),
            "no_history_rows": len(no_history_rows),
            "loss_or_stop_rows": len(loss_or_stop_rows),
            "fallback_codes": [row["code"] for row in fallback_rows],
            "loss_or_stop_codes": [row["code"] for row in loss_or_stop_rows],
        },
        "artifacts": {
            "post_entry_learning_csv": str(POST_ENTRY_CSV),
            "entry_source_archive_history": str(ARCHIVE_CSV),
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
        },
        "rows": rows,
        "interpretation": {
            "finding": (
                "The unresolved entry group has archive matches. The archived source rows were selected through "
                "fresh_sector_allowed_fallback with sector WAIT/strength 0.4, not through a clean natural pass."
            ),
            "boundary": "Read-only trace; no trading, policy, order, fill, or ledger changes.",
        },
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"status": "OK", "rows": len(rows), "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
