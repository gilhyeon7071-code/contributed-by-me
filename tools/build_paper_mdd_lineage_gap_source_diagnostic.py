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

MISSING_CSV = LOG_DIR / "paper_mdd_candidate_missing_split_latest.csv"
TRADES_CALC = PAPER_DIR / "trades_calc.csv"
ENTRY_SOURCE_ARCHIVE = LOG_DIR / "entry_source_archive_history.csv"

OUT_JSON = LOG_DIR / "paper_mdd_lineage_gap_source_diagnostic_latest.json"
OUT_CSV = LOG_DIR / "paper_mdd_lineage_gap_source_diagnostic_latest.csv"


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


def _order_files(signal_date: str) -> list[Path]:
    return sorted(PAPER_DIR.glob(f"orders_{signal_date}_broker_submit_*.csv"))


def _archive_by_entry_order_id() -> dict[str, dict[str, str]]:
    return {
        str(row.get("entry_order_id") or ""): row
        for row in _read_csv(ENTRY_SOURCE_ARCHIVE)
        if str(row.get("entry_order_id") or "")
    }


def _entry_source_class(note: str, order: dict[str, str] | None, archive: dict[str, str] | None) -> str:
    if archive:
        kind = str(archive.get("entry_source_kind") or "").strip().upper() or "UNKNOWN"
        return f"ENTRY_SOURCE_ARCHIVE_MATCHED_{kind}"
    surge_type = _note_value(note, "surge_type")
    surge_immediate = _note_value(note, "surge_immediate")
    fallback_stage = _note_value(note, "fallback_stage")
    if surge_type or surge_immediate == "1":
        return "UNEXPECTED_SURGE_METADATA_PRESENT"
    if fallback_stage == "0(intraday_realtime)":
        return "GENERAL_INTRADAY_REALTIME_ENTRY_SOURCE_ARCHIVE_MISSING"
    if fallback_stage:
        return "NONZERO_FALLBACK_STAGE_SOURCE_ARCHIVE_MISSING"
    if order:
        return "ORDER_FOUND_BUT_NOTE_SOURCE_INCOMPLETE"
    return "ORDER_NOT_FOUND"


def build() -> dict[str, Any]:
    missing_rows = [
        row for row in _read_csv(MISSING_CSV)
        if row.get("missing_root_cause") == "INTRADAY_REALTIME_FALLBACK_OR_LINEAGE_GAP"
        or row.get("missing_root_cause") == "ENTRY_SOURCE_ARCHIVE_AVAILABLE_NO_CANDIDATE_MATCH"
    ]
    trades_by_id = {str(row.get("trade_id") or ""): row for row in _read_csv(TRADES_CALC)}
    archive_by_oid = _archive_by_entry_order_id()

    orders_by_date: dict[str, list[dict[str, str]]] = {}
    order_file_names: dict[str, list[str]] = {}
    for signal_date in sorted({row.get("signal_date", "") for row in missing_rows}):
        orders: list[dict[str, str]] = []
        names: list[str] = []
        for path in _order_files(signal_date):
            names.append(path.name)
            orders.extend(_read_csv(path))
        orders_by_date[signal_date] = orders
        order_file_names[signal_date] = names

    out_rows: list[dict[str, Any]] = []
    for row in missing_rows:
        trade = trades_by_id.get(str(row.get("trade_id") or ""), {})
        trade_note = trade.get("note", "")
        entry_oid = _note_value(trade_note, "entry_order_id") or _note_value(trade_note, "order_id")
        signal_date = str(row.get("signal_date") or "")
        code = str(row.get("code") or "").zfill(6)
        matches = [
            order for order in orders_by_date.get(signal_date, [])
            if str(order.get("side") or "").upper() == "BUY"
            and str(order.get("code") or "").zfill(6) == code
            and (
                str(order.get("entry_order_id") or "") == entry_oid
                or _note_value(order.get("note", ""), "entry_order_id") == entry_oid
            )
        ]
        order = matches[0] if matches else None
        order_note = order.get("note", "") if order else ""
        combined_note = ";".join(x for x in [trade_note, order_note] if x)
        archive = archive_by_oid.get(entry_oid, {})
        out_rows.append({
            "trade_id": row.get("trade_id", ""),
            "code": code,
            "signal_date": signal_date,
            "entry_ts": row.get("entry_ts", ""),
            "exit_reason": row.get("exit_reason", ""),
            "net_ret": row.get("net_ret", ""),
            "entry_order_id": entry_oid,
            "source_class": _entry_source_class(combined_note, order, archive),
            "entry_archive_matched": bool(archive),
            "archive_entry_source_kind": archive.get("entry_source_kind", ""),
            "archive_candidate_snapshot_id": archive.get("candidate_snapshot_id", ""),
            "archive_entry_trace_id": archive.get("entry_trace_id", ""),
            "archive_entry_timing": archive.get("entry_timing", ""),
            "archive_fallback_stage": archive.get("fallback_stage", ""),
            "order_match_count": len(matches),
            "order_files": ";".join(order_file_names.get(signal_date, [])),
            "order_type": order.get("order_type", "") if order else "",
            "dispatch_ts": order.get("dispatch_ts", "") if order else "",
            "dispatch_status": order.get("dispatch_status", "") if order else "",
            "horizon": _note_value(combined_note, "horizon"),
            "entry_timing": _note_value(combined_note, "entry_timing"),
            "fallback_stage": _note_value(combined_note, "fallback_stage"),
            "surge_type": _note_value(combined_note, "surge_type"),
            "surge_immediate": _note_value(combined_note, "surge_immediate"),
            "sizing": _note_value(combined_note, "sizing"),
            "split_entry": _note_value(combined_note, "split_entry"),
            "entry_trace_id": _note_value(combined_note, "entry_trace_id"),
            "entry_intent_id": _note_value(combined_note, "entry_intent_id"),
            "replay_chain_id": _note_value(combined_note, "replay_chain_id"),
            "source_row": order.get("source_row", "") if order else "",
            "score_policy": order.get("score_policy", "") if order else "",
            "score_action": order.get("score_action", "") if order else "",
            "orderflow_guard": order.get("orderflow_guard", "") if order else "",
            "production_risk_guard": order.get("production_risk_guard", "") if order else "",
        })

    source_counts = Counter(str(row.get("source_class") or "") for row in out_rows)
    horizon_counts = Counter(str(row.get("horizon") or "") for row in out_rows)
    order_match_counts = Counter(str(row.get("order_match_count") or "") for row in out_rows)
    result = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "status": "FAIL",
        "policy_change": False,
        "trading_effect": False,
        "source": {
            "missing_csv": str(MISSING_CSV),
            "trades_calc": str(TRADES_CALC),
            "entry_source_archive": str(ENTRY_SOURCE_ARCHIVE),
        },
        "scope": {
            "lineage_gap_rows": len(out_rows),
            "signal_dates": sorted({str(row.get("signal_date") or "") for row in out_rows}),
            "codes": sorted({str(row.get("code") or "") for row in out_rows}),
        },
        "breakdown": {
            "source_class_counts": dict(sorted(source_counts.items())),
            "horizon_counts": dict(sorted(horizon_counts.items())),
            "order_match_count_counts": dict(sorted(order_match_counts.items())),
        },
        "artifacts": {
            "csv": str(OUT_CSV),
        },
        "interpretation": [
            "This is a read-only source-lineage diagnostic for MDD candidate-missing rows.",
            "If entry_source_archive_history.csv contains the entry_order_id, the row is classified from that captured runtime source.",
            "GENERAL_INTRADAY_REALTIME_ENTRY_SOURCE_ARCHIVE_MISSING means the order note has entry_timing/fallback_stage but no archived candidate/source row link.",
            "No STOP/DDM, Gate, order, fill, ledger, broker, or policy value was changed.",
        ],
    }
    fields = [
        "trade_id", "code", "signal_date", "entry_ts", "exit_reason", "net_ret",
        "entry_order_id", "source_class", "entry_archive_matched", "archive_entry_source_kind",
        "archive_candidate_snapshot_id", "archive_entry_trace_id", "archive_entry_timing",
        "archive_fallback_stage", "order_match_count", "order_files", "order_type",
        "dispatch_ts", "dispatch_status", "horizon", "entry_timing", "fallback_stage",
        "surge_type", "surge_immediate", "sizing", "split_entry", "entry_trace_id",
        "entry_intent_id", "replay_chain_id", "source_row", "score_policy",
        "score_action", "orderflow_guard", "production_risk_guard",
    ]
    OUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, out_rows, fields)
    return result


def main() -> int:
    result = build()
    print(json.dumps({
        "status": result["status"],
        "scope": result["scope"],
        "breakdown": result["breakdown"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
