from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
TRADES_CSV = ROOT / "paper" / "trades_calc.csv"
LEDGER_CSV = Path(r"E:\vibe\buffett\data\ledger\paper_fills_ledger.csv")
CONTEXT_CSV = LOG_DIR / "normal_intraday_entry_context_latest.csv"
TRACE_CSV = LOG_DIR / "entry_trace_performance_latest.csv"
OUT_JSON = LOG_DIR / "reentry_exit_timing_094170_latest.json"
OUT_CSV = LOG_DIR / "reentry_exit_timing_094170_latest.csv"
CODE = "094170"


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


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def norm_code(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[-6:].zfill(6) if digits else ""


def note_value(note: str, key: str) -> str:
    for part in str(note or "").replace("|", ";").split(";"):
        part = part.strip()
        if part.startswith(key + "="):
            return part.split("=", 1)[1].strip()
    return ""


def main() -> int:
    trades = [r for r in read_csv(TRADES_CSV) if norm_code(r.get("code")) == CODE]
    ledger = [r for r in read_csv(LEDGER_CSV) if norm_code(r.get("code")) == CODE]
    traces = {r.get("entry_trace_id", ""): r for r in read_csv(TRACE_CSV) if norm_code(r.get("code")) == CODE}
    contexts = {r.get("entry_trace_id", ""): r for r in read_csv(CONTEXT_CSV) if norm_code(r.get("code")) == CODE}

    rows: list[dict[str, Any]] = []
    for trade in trades:
        trace_id = note_value(trade.get("note", ""), "entry_trace_id")
        entry_order_id = note_value(trade.get("note", ""), "entry_order_id") or note_value(trade.get("note", ""), "order_id")
        linked_ledger = [
            r for r in ledger
            if r.get("entry_order_id") == entry_order_id
            or r.get("source_order_id") == entry_order_id
            or r.get("order_id") == entry_order_id
        ]
        linked_buys = [r for r in linked_ledger if str(r.get("side", "")).upper() == "BUY"]
        linked_sells = [r for r in linked_ledger if str(r.get("side", "")).upper() == "SELL"]
        ctx = contexts.get(trace_id, {})
        trace = traces.get(trace_id, {})
        rows.append(
            {
                "trade_id": trade.get("trade_id", ""),
                "entry_order_id": entry_order_id,
                "entry_trace_id": trace_id,
                "entry_ts": trade.get("entry_ts", ""),
                "exit_ts": trade.get("exit_ts", ""),
                "code": CODE,
                "qty": to_float(trade.get("qty")),
                "entry_price": to_float(trade.get("entry_price")),
                "exit_price": to_float(trade.get("exit_price")),
                "paper_net_ret": to_float(trade.get("net_ret")),
                "trace_net_ret_sum": to_float(trace.get("net_ret_sum")),
                "trace_exit_reasons": trace.get("exit_reasons", ""),
                "ledger_rows": len(linked_ledger),
                "ledger_buy_rows": len(linked_buys),
                "ledger_sell_rows": len(linked_sells),
                "ledger_buy_datetimes": "|".join(r.get("datetime", "") for r in linked_buys),
                "ledger_sell_datetimes": "|".join(r.get("datetime", "") for r in linked_sells),
                "ledger_realized_pnl_sum": sum(to_float(r.get("realized_pnl_krw")) for r in linked_ledger),
                "p1_entry_gate_decision_before_p1": ctx.get("p1_entry_gate_decision_before_p1", ""),
                "risk_flags": ctx.get("risk_flags", ""),
                "weak_context": ctx.get("weak_context", ""),
                "p0_daily_loss_active": ctx.get("p0_daily_loss_active", ""),
                "p0_last_day_ret": ctx.get("p0_last_day_ret", ""),
                "p0_max_drawdown_pct": ctx.get("p0_max_drawdown_pct", ""),
                "diagnosis_axis": "",
            }
        )

    rows.sort(key=lambda r: str(r.get("entry_ts", "")))
    previous_stop_exit_ts = ""
    for row in rows:
        if previous_stop_exit_ts and str(row.get("entry_ts", ""))[:10] == previous_stop_exit_ts[:10]:
            row["diagnosis_axis"] = "SAME_DAY_REENTRY_AFTER_STOP"
        if int(row.get("ledger_buy_rows", 0)) > 1:
            row["diagnosis_axis"] = (str(row.get("diagnosis_axis") or "") + "|DUPLICATE_BUY_ROWS").strip("|")
        if "STOP" in str(row.get("trace_exit_reasons", "")):
            previous_stop_exit_ts = str(row.get("exit_ts", ""))

    summary = {
        "target_trades": len(rows),
        "same_day_reentry_after_stop_rows": sum("SAME_DAY_REENTRY_AFTER_STOP" in str(r.get("diagnosis_axis", "")) for r in rows),
        "duplicate_buy_trade_rows": sum("DUPLICATE_BUY_ROWS" in str(r.get("diagnosis_axis", "")) for r in rows),
        "total_paper_net_ret": sum(to_float(r.get("paper_net_ret")) for r in rows),
        "total_ledger_realized_pnl": sum(to_float(r.get("ledger_realized_pnl_sum")) for r in rows),
    }
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS" if len(rows) >= 2 else "WARN",
        "scope": "read_only_094170_reentry_exit_timing_diagnostic",
        "inputs": {
            "trades": str(TRADES_CSV),
            "ledger": str(LEDGER_CSV),
            "trace": str(TRACE_CSV),
            "context": str(CONTEXT_CSV),
        },
        "summary": summary,
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if rows:
        with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    print(json.dumps({"status": payload["status"], "json": str(OUT_JSON), "csv": str(OUT_CSV)}, ensure_ascii=False))
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
