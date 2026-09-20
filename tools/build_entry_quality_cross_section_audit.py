from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
LEDGER_CSV = Path(r"E:\vibe\buffett\data\ledger\paper_fills_ledger.csv")
TRACE_CSV = LOG_DIR / "entry_trace_performance_latest.csv"
CONTEXT_CSV = LOG_DIR / "normal_intraday_entry_context_latest.csv"
OUT_JSON = LOG_DIR / "entry_quality_cross_section_audit_latest.json"
OUT_CSV = LOG_DIR / "entry_quality_cross_section_audit_latest.csv"


STOP_REASONS = ("exit_reason=STOP;", "exit_reason=STOP_GAP;")


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    return []


def ymd(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def norm_code(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[-6:].zfill(6) if digits else ""


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def add_flag(flags: dict[str, dict[str, Any]], order_id: str, code: str, category: str, evidence: dict[str, Any]) -> None:
    if not order_id:
        return
    item = flags.setdefault(
        order_id,
        {
            "entry_order_id": order_id,
            "code": code,
            "categories": [],
            "evidence": {},
        },
    )
    if category not in item["categories"]:
        item["categories"].append(category)
    item["evidence"][category] = evidence


def main() -> int:
    ledger = read_csv(LEDGER_CSV)
    traces = read_csv(TRACE_CSV)
    contexts = read_csv(CONTEXT_CSV)
    context_ids = {r.get("entry_trace_id", "") for r in contexts}
    flags: dict[str, dict[str, Any]] = {}

    rows_by_entry_order: dict[str, list[dict[str, str]]] = {}
    for row in ledger:
        entry_order_id = str(row.get("entry_order_id") or row.get("source_order_id") or row.get("order_id") or "").strip()
        if entry_order_id:
            rows_by_entry_order.setdefault(entry_order_id, []).append(row)

    for order_id, rows in rows_by_entry_order.items():
        buy_rows = [r for r in rows if str(r.get("side", "")).upper() == "BUY"]
        sell_rows = [r for r in rows if str(r.get("side", "")).upper() == "SELL"]
        if len(buy_rows) > 1:
            add_flag(
                flags,
                order_id,
                norm_code(buy_rows[0].get("code")),
                "DUPLICATE_BUY_ROWS",
                {
                    "buy_rows": len(buy_rows),
                    "sell_rows": len(sell_rows),
                    "buy_datetimes": "|".join(r.get("datetime", "") for r in buy_rows),
                    "sell_datetimes": "|".join(r.get("datetime", "") for r in sell_rows),
                },
            )

    ordered = sorted(
        ledger,
        key=lambda r: (norm_code(r.get("code")), str(r.get("datetime") or r.get("date") or ""), str(r.get("input_seq") or "")),
    )
    last_stop_by_code: dict[str, dict[str, str]] = {}
    for row in ordered:
        code = norm_code(row.get("code"))
        side = str(row.get("side", "")).upper()
        note = str(row.get("note", "") or "")
        if side == "SELL" and any(token in note for token in STOP_REASONS):
            last_stop_by_code[code] = row
            continue
        if side == "BUY" and code in last_stop_by_code:
            prev = last_stop_by_code[code]
            prev_day = ymd(prev.get("date") or prev.get("datetime"))
            buy_day = ymd(row.get("date") or row.get("datetime"))
            if prev_day and buy_day and 0 <= int(buy_day) - int(prev_day) <= 3:
                order_id = str(row.get("entry_order_id") or row.get("order_id") or "").strip()
                add_flag(
                    flags,
                    order_id,
                    code,
                    "NEAR_DAY_REENTRY_AFTER_STOP",
                    {
                        "previous_stop_datetime": prev.get("datetime", ""),
                        "buy_datetime": row.get("datetime", ""),
                        "previous_stop_order_id": prev.get("order_id", ""),
                        "days_raw_diff": int(buy_day) - int(prev_day),
                    },
                )

    for row in traces:
        entry_trace_id = str(row.get("entry_trace_id") or "")
        if not entry_trace_id.startswith("MISSING_TRACE_PAPER_BUY_"):
            continue
        if entry_trace_id in context_ids:
            continue
        code = norm_code(row.get("code"))
        entry_ymd = ymd(row.get("entry_ymd") or row.get("entry_ts"))
        order_id = f"PAPER_BUY_{code}_{ymd(entry_trace_id)}" if False else ""
        # MISSING_TRACE_PAPER_BUY_<code>_<signal_date>
        parts = entry_trace_id.split("_")
        signal_date = parts[-1] if parts else ""
        if code and signal_date:
            order_id = f"PAPER_BUY_{code}_{signal_date}"
        add_flag(
            flags,
            order_id,
            code,
            "LEGACY_MISSING_CONTEXT_ENTRY",
            {
                "entry_trace_id": entry_trace_id,
                "entry_ymd": entry_ymd,
                "entry_class": row.get("entry_class", ""),
                "entry_ts": row.get("entry_ts", ""),
            },
        )

    out_rows: list[dict[str, Any]] = []
    for item in flags.values():
        order_id = item["entry_order_id"]
        linked = rows_by_entry_order.get(order_id, [])
        out_rows.append(
            {
                "entry_order_id": order_id,
                "code": item["code"],
                "categories": "|".join(sorted(item["categories"])),
                "category_count": len(item["categories"]),
                "ledger_rows": len(linked),
                "ledger_buy_rows": sum(1 for r in linked if str(r.get("side", "")).upper() == "BUY"),
                "ledger_sell_rows": sum(1 for r in linked if str(r.get("side", "")).upper() == "SELL"),
                "ledger_realized_pnl_sum": sum(to_float(r.get("realized_pnl_krw")) for r in linked),
                "first_datetime": min((r.get("datetime", "") for r in linked), default=""),
                "last_datetime": max((r.get("datetime", "") for r in linked), default=""),
                "evidence_json": json.dumps(item["evidence"], ensure_ascii=False, sort_keys=True),
            }
        )
    out_rows.sort(key=lambda r: (str(r["first_datetime"]), str(r["entry_order_id"])))

    summary = {
        "flagged_entry_orders": len(out_rows),
        "duplicate_buy_orders": sum("DUPLICATE_BUY_ROWS" in r["categories"] for r in out_rows),
        "near_day_reentry_after_stop_orders": sum("NEAR_DAY_REENTRY_AFTER_STOP" in r["categories"] for r in out_rows),
        "legacy_missing_context_orders": sum("LEGACY_MISSING_CONTEXT_ENTRY" in r["categories"] for r in out_rows),
        "ledger_realized_pnl_sum": sum(to_float(r.get("ledger_realized_pnl_sum")) for r in out_rows),
    }
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS" if out_rows else "WARN",
        "scope": "read_only_entry_quality_cross_section_audit",
        "inputs": {
            "ledger": str(LEDGER_CSV),
            "trace": str(TRACE_CSV),
            "context": str(CONTEXT_CSV),
        },
        "summary": summary,
        "rows": out_rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if out_rows:
        with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
            writer.writeheader()
            writer.writerows(out_rows)
    print(json.dumps({"status": payload["status"], "json": str(OUT_JSON), "csv": str(OUT_CSV)}, ensure_ascii=False))
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
