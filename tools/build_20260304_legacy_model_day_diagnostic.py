from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
BT_TRADES_CSV = ROOT / "12_Risk_Controlled" / "report_backtest_trades_v41_1.csv"
PAPER_TRADES_CSV = ROOT / "paper" / "trades_calc.csv"
LEDGER_CSV = Path(r"E:\vibe\buffett\data\ledger\paper_fills_ledger.csv")
TRACE_CSV = LOG_DIR / "entry_trace_performance_latest.csv"
CONTEXT_CSV = LOG_DIR / "normal_intraday_entry_context_latest.csv"
VALIDATION_JSON = LOG_DIR / "backtest_validation_latest.json"
OUT_JSON = LOG_DIR / "legacy_model_day_20260304_latest.json"
OUT_CSV = LOG_DIR / "legacy_model_day_20260304_latest.csv"
TARGET_YMD = "20260304"


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


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except UnicodeDecodeError:
            continue
    return {}


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


def validation_return() -> float:
    doc = read_json(VALIDATION_JSON)
    rows = (((doc.get("artifacts") or {}).get("base_series") or {}).get("returns") or [])
    for row in rows if isinstance(rows, list) else []:
        if isinstance(row, dict) and ymd(row.get("date")) == TARGET_YMD:
            return to_float(row.get("return"))
    return 0.0


def main() -> int:
    bt_rows = [
        r for r in read_csv(BT_TRADES_CSV)
        if ymd(r.get("entry_date")) == TARGET_YMD or ymd(r.get("exit_date")) == TARGET_YMD
    ]
    paper_rows = [
        r for r in read_csv(PAPER_TRADES_CSV)
        if ymd(r.get("entry_ts")) == TARGET_YMD or ymd(r.get("exit_ts")) == TARGET_YMD
    ]
    ledger_rows = [r for r in read_csv(LEDGER_CSV) if ymd(r.get("date")) == TARGET_YMD or norm_code(r.get("code")) in {norm_code(x.get("code")) for x in paper_rows}]
    trace_rows = [r for r in read_csv(TRACE_CSV) if ymd(r.get("entry_ymd") or r.get("entry_ts")) == TARGET_YMD]
    context_rows = [r for r in read_csv(CONTEXT_CSV) if ymd(r.get("entry_ymd") or r.get("entry_ts")) == TARGET_YMD]

    paper_entry_rows = [r for r in paper_rows if ymd(r.get("entry_ts")) == TARGET_YMD]
    output_rows: list[dict[str, Any]] = []
    for row in sorted(paper_entry_rows, key=lambda r: to_float(r.get("net_ret"))):
        code = norm_code(row.get("code"))
        order_token = f"PAPER_BUY_{code}_20260303"
        linked_ledger = [r for r in ledger_rows if r.get("entry_order_id") == order_token or r.get("order_id") == order_token or r.get("source_order_id") == order_token]
        output_rows.append(
            {
                "code": code,
                "trade_id": row.get("trade_id", ""),
                "entry_ts": row.get("entry_ts", ""),
                "exit_ts": row.get("exit_ts", ""),
                "qty": to_float(row.get("qty")),
                "entry_price": to_float(row.get("entry_price")),
                "exit_price": to_float(row.get("exit_price")),
                "paper_net_ret": to_float(row.get("net_ret")),
                "entry_order_id": order_token,
                "ledger_rows": len(linked_ledger),
                "ledger_buy_rows": sum(1 for r in linked_ledger if str(r.get("side", "")).upper() == "BUY"),
                "ledger_sell_rows": sum(1 for r in linked_ledger if str(r.get("side", "")).upper() == "SELL"),
                "ledger_realized_pnl_sum": sum(to_float(r.get("realized_pnl_krw")) for r in linked_ledger),
                "entry_trace_id": next((r.get("entry_trace_id", "") for r in trace_rows if norm_code(r.get("code")) == code), ""),
                "trace_entry_class": next((r.get("entry_class", "") for r in trace_rows if norm_code(r.get("code")) == code), ""),
                "has_current_intraday_context": any(norm_code(r.get("code")) == code for r in context_rows),
                "diagnosis_axis": "LEGACY_NEXT_OPEN_MISSING_CURRENT_CONTEXT",
            }
        )

    bt_028670 = [r for r in bt_rows if norm_code(r.get("code")) == "028670"]
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS" if output_rows else "WARN",
        "scope": "read_only_20260304_legacy_model_day_diagnostic",
        "inputs": {
            "validation": str(VALIDATION_JSON),
            "backtest_trades": str(BT_TRADES_CSV),
            "paper_trades": str(PAPER_TRADES_CSV),
            "ledger": str(LEDGER_CSV),
            "trace": str(TRACE_CSV),
            "context": str(CONTEXT_CSV),
        },
        "summary": {
            "target_ymd": TARGET_YMD,
            "validation_return": validation_return(),
            "paper_entry_rows": len(paper_entry_rows),
            "entry_trace_rows": len(trace_rows),
            "entry_context_rows": len(context_rows),
            "paper_net_ret_sum": sum(to_float(r.get("paper_net_ret")) for r in output_rows),
            "ledger_realized_pnl_sum": sum(to_float(r.get("ledger_realized_pnl_sum")) for r in output_rows),
            "backtest_028670_rows": len(bt_028670),
            "backtest_028670_ret_sum": sum(to_float(r.get("ret")) for r in bt_028670),
            "all_missing_current_intraday_context": all(not bool(r.get("has_current_intraday_context")) for r in output_rows),
        },
        "rows": output_rows,
        "backtest_028670_rows": bt_028670,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if output_rows:
        with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(output_rows[0].keys()))
            writer.writeheader()
            writer.writerows(output_rows)
    print(json.dumps({"status": payload["status"], "json": str(OUT_JSON), "csv": str(OUT_CSV)}, ensure_ascii=False))
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
