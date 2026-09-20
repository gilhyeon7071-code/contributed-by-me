from __future__ import annotations

import csv
import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
ROOTB_LEDGER = Path(r"E:\vibe\buffett\data\ledger\paper_fills_ledger.csv")
LOG_DIR = ROOT / "2_Logs"
CAUSE_CSV = LOG_DIR / "entry_quality_cause_bucket_report_latest.csv"
PAPER_FILLS = ROOT / "paper" / "fills.csv"
PAPER_TRADES = ROOT / "paper" / "trades.csv"
PAPER_TRADES_CALC = ROOT / "paper" / "trades_calc.csv"
OUT_JSON = LOG_DIR / "legacy_oper_missing_context_diagnostic_latest.json"
OUT_CSV = LOG_DIR / "legacy_oper_missing_context_diagnostic_latest.csv"


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return [dict(r) for r in csv.DictReader(f)]
        except UnicodeDecodeError:
            continue
    with path.open("r", newline="") as f:
        return [dict(r) for r in csv.DictReader(f)]


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "entry_order_id",
        "code",
        "entry_ymd",
        "bucket",
        "paper_buy_note_trace",
        "rootb_buy_structured_trace",
        "rootb_buy_lineage_origin",
        "rootb_buy_source",
        "sell_note_trace_count",
        "sell_note_trace_examples",
        "rootb_sell_trace_count",
        "rootb_sell_trace_examples",
        "classification",
        "ledger_pnl_sum",
        "ledger_rows",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _note_fields(note: Any) -> Dict[str, str]:
    text = str(note or "").replace(" | ", ";")
    out: Dict[str, str] = {}
    for part in text.split(";"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip()
        if key:
            out[key] = value.strip()
    return out


def _ymd(value: Any) -> str:
    digits = re.sub(r"[^0-9]", "", str(value or ""))
    return digits[:8] if len(digits) >= 8 else ""


def _flt(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _uniq(values: Iterable[str]) -> List[str]:
    return sorted({str(v).strip() for v in values if str(v).strip()})


def _cause_targets(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    out = []
    for row in rows:
        if row.get("cause_bucket") == "LEGACY_OPER_MISSING_CONTEXT":
            out.append(row)
    return out


def _rootb_order_id(row: Dict[str, str]) -> str:
    return (
        row.get("entry_order_id")
        or row.get("source_order_id")
        or row.get("order_id")
        or ""
    ).strip()


def run() -> Dict[str, Any]:
    cause_rows = _cause_targets(_read_csv(CAUSE_CSV))
    paper_fills = _read_csv(PAPER_FILLS)
    paper_trades = _read_csv(PAPER_TRADES)
    paper_trades_calc = _read_csv(PAPER_TRADES_CALC)
    rootb_rows = _read_csv(ROOTB_LEDGER)

    fills_by_order: Dict[str, List[Dict[str, str]]] = {}
    for row in paper_fills:
        fills_by_order.setdefault(str(row.get("order_id") or "").strip(), []).append(row)

    rootb_by_order: Dict[str, List[Dict[str, str]]] = {}
    for row in rootb_rows:
        oid = _rootb_order_id(row)
        if oid:
            rootb_by_order.setdefault(oid, []).append(row)

    sell_notes_by_trace: Dict[str, List[str]] = {}
    for row in paper_fills:
        if str(row.get("side") or "").upper() != "SELL":
            continue
        note = _note_fields(row.get("note", ""))
        trace = note.get("entry_trace_id") or note.get("source_trace_id") or ""
        if trace:
            sell_notes_by_trace.setdefault(trace, []).append(str(row.get("order_id") or ""))
    for row in paper_trades + paper_trades_calc:
        note = _note_fields(row.get("note", ""))
        trace = note.get("entry_trace_id") or note.get("source_trace_id") or ""
        if trace:
            sell_notes_by_trace.setdefault(trace, []).append(str(row.get("trade_id") or row.get("order_id") or ""))

    detail_rows: List[Dict[str, Any]] = []
    class_counts: Counter[str] = Counter()
    for target in cause_rows:
        entry_order_id = str(target.get("entry_order_id") or "").strip()
        code = str(target.get("code") or "").zfill(6)
        entry_ymd = str(target.get("entry_ymd") or "").strip()

        buy_rows = [
            r for r in fills_by_order.get(entry_order_id, [])
            if str(r.get("side") or "").upper() == "BUY"
        ]
        paper_buy_traces = _uniq(
            (_note_fields(r.get("note", "")).get("entry_trace_id") or "") for r in buy_rows
        )

        rootb_matches = rootb_by_order.get(entry_order_id, [])
        rootb_buy_rows = [
            r for r in rootb_matches
            if str(r.get("side") or "").upper() == "BUY"
        ]
        rootb_sell_rows = [
            r for r in rootb_matches
            if str(r.get("side") or "").upper() == "SELL"
        ]
        rootb_buy_traces = _uniq(r.get("entry_trace_id") or "" for r in rootb_buy_rows)
        rootb_lineage = _uniq(r.get("lineage_origin") or "" for r in rootb_buy_rows)
        rootb_source = _uniq(r.get("source") or "" for r in rootb_buy_rows)
        rootb_sell_traces = _uniq(
            (r.get("entry_trace_id") or _note_fields(r.get("note", "")).get("entry_trace_id") or "")
            for r in rootb_sell_rows
        )
        recovered_sell_traces = _uniq(
            trace for trace in sell_notes_by_trace
            if code in trace and (entry_ymd in trace or _ymd(entry_ymd) in trace)
        )

        if paper_buy_traces:
            classification = "paper_buy_trace_present"
        elif rootb_buy_traces:
            classification = "paper_buy_note_missing_but_rootb_structured_trace_available"
        elif recovered_sell_traces or rootb_sell_traces:
            classification = "buy_trace_missing_sell_trace_recoverable"
        else:
            classification = "true_trace_and_context_missing"
        class_counts[classification] += 1

        detail_rows.append(
            {
                "entry_order_id": entry_order_id,
                "code": code,
                "entry_ymd": entry_ymd,
                "bucket": target.get("cause_bucket", ""),
                "paper_buy_note_trace": ",".join(paper_buy_traces),
                "rootb_buy_structured_trace": ",".join(rootb_buy_traces),
                "rootb_buy_lineage_origin": ",".join(rootb_lineage),
                "rootb_buy_source": ",".join(rootb_source),
                "sell_note_trace_count": len(recovered_sell_traces),
                "sell_note_trace_examples": ",".join(recovered_sell_traces[:5]),
                "rootb_sell_trace_count": len(rootb_sell_traces),
                "rootb_sell_trace_examples": ",".join(rootb_sell_traces[:5]),
                "classification": classification,
                "ledger_pnl_sum": target.get("ledger_pnl_sum", ""),
                "ledger_rows": target.get("ledger_rows", ""),
            }
        )

    _write_csv(OUT_CSV, detail_rows)
    payload = {
        "generated_at": _now_ts(),
        "inputs": {
            "cause_csv": str(CAUSE_CSV),
            "paper_fills": str(PAPER_FILLS),
            "paper_trades": str(PAPER_TRADES),
            "paper_trades_calc": str(PAPER_TRADES_CALC),
            "rootb_ledger": str(ROOTB_LEDGER),
        },
        "target_bucket": "LEGACY_OPER_MISSING_CONTEXT",
        "target_orders": len(cause_rows),
        "classification_counts": dict(sorted(class_counts.items())),
        "rows_written": len(detail_rows),
        "output_csv": str(OUT_CSV),
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
