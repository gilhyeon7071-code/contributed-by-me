from __future__ import annotations

import csv
import datetime as dt
import json
import math
import re
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
TARGET_DATES = ["20260304", "20260428", "20260608"]

VALIDATION_JSON = LOG_DIR / "backtest_validation_latest.json"
BT_TRADES_CSV = ROOT / "12_Risk_Controlled" / "report_backtest_trades_v41_1.csv"
PAPER_TRADES_CSV = ROOT / "paper" / "trades_calc.csv"
LEDGER_CSV = Path(r"E:\vibe\buffett\data\ledger\paper_fills_ledger.csv")
ENTRY_TRACE_CSV = LOG_DIR / "entry_trace_performance_latest.csv"
ENTRY_CONTEXT_CSV = LOG_DIR / "normal_intraday_entry_context_latest.csv"

OUT_JSON = LOG_DIR / "backtest_worst_day_driver_diagnostic_latest.json"
OUT_CSV = LOG_DIR / "backtest_worst_day_driver_diagnostic_latest.csv"


def _now() -> str:
    return dt.datetime.now().replace(microsecond=0).isoformat()


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except UnicodeDecodeError:
            continue
        except Exception:
            return {}
    return {}


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as fh:
                return list(csv.DictReader(fh))
        except UnicodeDecodeError:
            continue
        except Exception:
            return []
    return []


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "target_date",
        "validation_return",
        "bt_trade_rows",
        "bt_ret_sum",
        "bt_top_codes",
        "paper_entry_rows",
        "paper_exit_rows",
        "paper_net_sum",
        "ledger_buy_rows",
        "ledger_sell_rows",
        "ledger_realized_pnl_sum",
        "entry_trace_rows",
        "entry_context_rows",
        "top_entry_paths",
        "top_risk_flags",
        "driver_class",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})
    tmp.replace(path)


def _ymd(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _num(value: Any, default: float = math.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def _sum(rows: List[Dict[str, str]], key: str) -> float:
    vals = [_num(row.get(key)) for row in rows]
    vals = [v for v in vals if math.isfinite(v)]
    return float(sum(vals)) if vals else 0.0


def _counts(values: List[str]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for value in values:
        value = str(value or "").strip()
        if not value:
            continue
        out[value] = out.get(value, 0) + 1
    return dict(sorted(out.items(), key=lambda kv: (-kv[1], kv[0])))


def _top(counts: Dict[str, int], limit: int = 5) -> str:
    return ";".join(f"{key}:{value}" for key, value in list(counts.items())[:limit])


def _note_value(note: Any, key: str) -> str:
    match = re.search(rf"(?:^|[;|]){re.escape(key)}=([^;|]+)", str(note or ""))
    return match.group(1).strip() if match else ""


def _validation_return_by_date() -> Dict[str, float]:
    doc = _read_json(VALIDATION_JSON)
    rows = (((doc.get("artifacts") or {}).get("base_series") or {}).get("returns") or [])
    out: Dict[str, float] = {}
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        ymd = _ymd(row.get("date") or row.get("timestamp"))
        ret = _num(row.get("return"))
        if ymd and math.isfinite(ret):
            out[ymd] = ret
    return out


def _entry_path(row: Dict[str, str]) -> str:
    parts = [
        f"timing={row.get('entry_timing', '')}",
        f"split={row.get('split_entry', '')}",
        f"class={row.get('entry_class', '')}",
        f"horizon={row.get('horizon', '')}",
        f"surge={row.get('surge_immediate', '')}",
    ]
    return "|".join(parts)


def _driver_class(entry_context_rows: List[Dict[str, str]], bt_rows: List[Dict[str, str]], paper_rows: List[Dict[str, str]]) -> str:
    if entry_context_rows:
        flags = "|".join(str(r.get("risk_flags", "")) for r in entry_context_rows)
        if "p0_rolling_dd_ge_10pct" in flags:
            return "ENTRY_CONTEXT_P0_DD_WEAKNESS"
        return "ENTRY_CONTEXT_WEAKNESS"
    if bt_rows:
        return "BACKTEST_MODEL_DAY_LOSS"
    if paper_rows:
        return "PAPER_TRADE_DAY_LOSS"
    return "NO_LOCAL_TRADE_LINK"


def build() -> Dict[str, Any]:
    validation_returns = _validation_return_by_date()
    bt = _read_csv(BT_TRADES_CSV)
    paper = _read_csv(PAPER_TRADES_CSV)
    ledger = _read_csv(LEDGER_CSV)
    trace = _read_csv(ENTRY_TRACE_CSV)
    context = _read_csv(ENTRY_CONTEXT_CSV)

    rows: List[Dict[str, Any]] = []
    detail: Dict[str, Any] = {}
    for ymd in TARGET_DATES:
        bt_rows = [r for r in bt if _ymd(r.get("entry_date") or r.get("signal_date") or r.get("exit_date")) == ymd or _ymd(r.get("exit_date")) == ymd]
        paper_entry = [r for r in paper if _ymd(r.get("entry_ts")) == ymd]
        paper_exit = [r for r in paper if _ymd(r.get("exit_ts")) == ymd]
        paper_rows = paper_entry + [r for r in paper_exit if r not in paper_entry]
        ledger_buy = [r for r in ledger if _ymd(r.get("date")) == ymd and str(r.get("side", "")).upper() == "BUY"]
        ledger_sell = [r for r in ledger if _ymd(r.get("date")) == ymd and str(r.get("side", "")).upper() == "SELL"]
        trace_rows = [r for r in trace if _ymd(r.get("entry_ymd") or r.get("entry_ts") or r.get("signal_date")) == ymd]
        context_rows = [r for r in context if _ymd(r.get("entry_ymd") or r.get("entry_ts")) == ymd]
        ledger_rows = ledger_buy + ledger_sell
        row = {
            "target_date": ymd,
            "validation_return": validation_returns.get(ymd),
            "bt_trade_rows": len(bt_rows),
            "bt_ret_sum": _sum(bt_rows, "ret"),
            "bt_top_codes": _top(_counts([str(r.get("code", "")).zfill(6) for r in bt_rows])),
            "paper_entry_rows": len(paper_entry),
            "paper_exit_rows": len(paper_exit),
            "paper_net_sum": _sum(paper_rows, "net_ret"),
            "ledger_buy_rows": len(ledger_buy),
            "ledger_sell_rows": len(ledger_sell),
            "ledger_realized_pnl_sum": _sum(ledger_rows, "realized_pnl_krw"),
            "entry_trace_rows": len(trace_rows),
            "entry_context_rows": len(context_rows),
            "top_entry_paths": _top(_counts([_entry_path(r) for r in trace_rows])),
            "top_risk_flags": _top(_counts([str(r.get("risk_flags", "")) for r in context_rows])),
            "driver_class": _driver_class(context_rows, bt_rows, paper_rows),
        }
        rows.append(row)
        detail[ymd] = {
            "summary": row,
            "backtest_worst_rows": sorted(bt_rows, key=lambda r: _num(r.get("ret"), 999.0))[:10],
            "paper_worst_rows": sorted(paper_rows, key=lambda r: _num(r.get("net_ret"), 999.0))[:10],
            "ledger_rows": ledger_rows[:30],
            "entry_trace_rows": trace_rows[:30],
            "entry_context_rows": context_rows[:30],
        }

    payload = {
        "generated_at": _now(),
        "status": "PASS",
        "scope": "read_only_backtest_worst_day_driver_diagnostic",
        "target_dates": TARGET_DATES,
        "source_files": {
            "validation": str(VALIDATION_JSON),
            "backtest_trades": str(BT_TRADES_CSV),
            "paper_trades": str(PAPER_TRADES_CSV),
            "ledger": str(LEDGER_CSV),
            "entry_trace": str(ENTRY_TRACE_CSV),
            "entry_context": str(ENTRY_CONTEXT_CSV),
        },
        "policy_change_applied": False,
        "trading_effect": False,
        "order_fill_ledger_changed": False,
        "rows": rows,
        "detail": detail,
        "interpretation": {
            "boundary": "Read-only attribution only; no gate, score, threshold, order, fill, ledger, or config mutation.",
            "next_step": "Only dates with concrete local trade or entry-context linkage should be promoted to a policy candidate.",
        },
    }
    return payload


def main() -> int:
    payload = build()
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, payload["rows"])
    print(json.dumps({"status": payload["status"], "json": str(OUT_JSON), "csv": str(OUT_CSV)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
