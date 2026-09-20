from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
IN_JSON = LOG_DIR / "entry_quality_cross_section_audit_latest.json"
OUT_JSON = LOG_DIR / "entry_quality_cause_bucket_report_latest.json"
OUT_CSV = LOG_DIR / "entry_quality_cause_bucket_report_latest.csv"
OUT_SUMMARY_CSV = LOG_DIR / "entry_quality_cause_bucket_summary_latest.csv"
OPER_START_YMD = "20260301"


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def ymd(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def cause_bucket(row: dict[str, Any]) -> tuple[str, str, str]:
    categories = set(str(row.get("categories", "")).split("|"))
    ledger_rows = to_int(row.get("ledger_rows"))
    first_ymd = ymd(row.get("first_datetime"))

    if ledger_rows <= 0:
        return (
            "TRACE_ONLY_NO_LEDGER",
            "trace exists but no linked ledger rows",
            "data_lineage_only",
        )
    if len(categories) >= 2:
        return (
            "MULTI_CAUSE_ENTRY_QUALITY",
            "multiple entry-quality flags overlap",
            "logic_and_lineage_review",
        )
    if "DUPLICATE_BUY_ROWS" in categories:
        return (
            "DUPLICATE_BUY_EXECUTION_INTEGRITY",
            "same entry order has multiple BUY rows",
            "execution_integrity_review",
        )
    if "NEAR_DAY_REENTRY_AFTER_STOP" in categories:
        return (
            "REENTRY_AFTER_STOP_RISK",
            "BUY occurred within 0-3 raw ymd days after STOP or STOP_GAP",
            "entry_timing_policy_review",
        )
    if "LEGACY_MISSING_CONTEXT_ENTRY" in categories:
        if first_ymd and first_ymd < OPER_START_YMD:
            return (
                "LEGACY_PRE_OPER_MISSING_CONTEXT",
                "pre-operating entry has no current intraday context row",
                "historical_data_quality_review",
            )
        return (
            "LEGACY_OPER_MISSING_CONTEXT",
            "operating-window entry has no current intraday context row",
            "runtime_lineage_review",
        )
    return ("UNCLASSIFIED_ENTRY_QUALITY", "flagged row did not match known bucket", "manual_review")


def main() -> int:
    data = json.loads(IN_JSON.read_text(encoding="utf-8"))
    rows = data.get("rows") if isinstance(data.get("rows"), list) else []
    out_rows: list[dict[str, Any]] = []
    summary: dict[str, dict[str, Any]] = {}

    for row in rows:
        bucket, reason, lane = cause_bucket(row)
        pnl = to_float(row.get("ledger_realized_pnl_sum"))
        item = {
            "cause_bucket": bucket,
            "review_lane": lane,
            "bucket_reason": reason,
            "entry_order_id": str(row.get("entry_order_id", "")),
            "code": str(row.get("code", "")),
            "categories": str(row.get("categories", "")),
            "category_count": to_int(row.get("category_count")),
            "ledger_rows": to_int(row.get("ledger_rows")),
            "ledger_buy_rows": to_int(row.get("ledger_buy_rows")),
            "ledger_sell_rows": to_int(row.get("ledger_sell_rows")),
            "ledger_realized_pnl_sum": pnl,
            "first_datetime": str(row.get("first_datetime", "")),
            "last_datetime": str(row.get("last_datetime", "")),
            "evidence_json": str(row.get("evidence_json", "")),
        }
        out_rows.append(item)
        s = summary.setdefault(
            bucket,
            {
                "cause_bucket": bucket,
                "review_lane": lane,
                "orders": 0,
                "ledger_linked_orders": 0,
                "ledger_rows": 0,
                "ledger_buy_rows": 0,
                "ledger_sell_rows": 0,
                "pnl_sum": 0.0,
                "loss_orders": 0,
                "profit_orders": 0,
                "zero_pnl_orders": 0,
            },
        )
        s["orders"] += 1
        s["ledger_linked_orders"] += 1 if item["ledger_rows"] > 0 else 0
        s["ledger_rows"] += item["ledger_rows"]
        s["ledger_buy_rows"] += item["ledger_buy_rows"]
        s["ledger_sell_rows"] += item["ledger_sell_rows"]
        s["pnl_sum"] += pnl
        s["loss_orders"] += 1 if pnl < 0 else 0
        s["profit_orders"] += 1 if pnl > 0 else 0
        s["zero_pnl_orders"] += 1 if pnl == 0 else 0

    summary_rows = sorted(summary.values(), key=lambda r: (float(r["pnl_sum"]), str(r["cause_bucket"])))
    out_rows.sort(key=lambda r: (str(r["cause_bucket"]), float(r["ledger_realized_pnl_sum"]), str(r["entry_order_id"])))
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS" if out_rows else "WARN",
        "scope": "read_only_entry_quality_cause_bucket_report",
        "inputs": {"audit_json": str(IN_JSON), "oper_start_ymd": OPER_START_YMD},
        "summary": {
            "flagged_entry_orders": len(out_rows),
            "bucket_count": len(summary_rows),
            "ledger_linked_orders": sum(1 for r in out_rows if r["ledger_rows"] > 0),
            "trace_only_orders": sum(1 for r in out_rows if r["ledger_rows"] <= 0),
            "pnl_sum": sum(float(r["ledger_realized_pnl_sum"]) for r in out_rows),
        },
        "bucket_summary": summary_rows,
        "rows": out_rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if out_rows:
        with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
            writer.writeheader()
            writer.writerows(out_rows)
    if summary_rows:
        with OUT_SUMMARY_CSV.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
            writer.writeheader()
            writer.writerows(summary_rows)
    print(
        json.dumps(
            {
                "status": payload["status"],
                "json": str(OUT_JSON),
                "csv": str(OUT_CSV),
                "summary_csv": str(OUT_SUMMARY_CSV),
            },
            ensure_ascii=False,
        )
    )
    return 0 if out_rows else 1


if __name__ == "__main__":
    raise SystemExit(main())
