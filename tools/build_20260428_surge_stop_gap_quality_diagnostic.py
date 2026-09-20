from __future__ import annotations

import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SPLIT_JSON = LOG_DIR / "entry_split_driver_20260428_latest.json"
TRADES_CSV = ROOT / "paper" / "trades_calc.csv"
LEDGER_CSV = Path(r"E:\vibe\buffett\data\ledger\paper_fills_ledger.csv")
OUT_JSON = LOG_DIR / "surge_stop_gap_quality_20260428_latest.json"
OUT_CSV = LOG_DIR / "surge_stop_gap_quality_20260428_latest.csv"
TARGET_YMD = "20260428"
TARGET_CODES = {"006360", "322000"}


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
        out = float(value)
    except Exception:
        return default
    return out


def parse_note(note: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for part in re.split(r"[;|]", note or ""):
        part = part.strip()
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def parse_qty_arrow(value: str) -> tuple[float, float]:
    m = re.match(r"\s*([0-9.]+)\s*->\s*([0-9.]+)\s*", value or "")
    if not m:
        return 0.0, 0.0
    return to_float(m.group(1)), to_float(m.group(2))


def linked_trades(trades: list[dict[str, str]], trace_id: str, code: str) -> list[dict[str, str]]:
    out = []
    for row in trades:
        if row.get("code") != code:
            continue
        if not str(row.get("entry_ts", "")).startswith("2026-04-28"):
            continue
        if trace_id in row.get("note", ""):
            out.append(row)
    return out


def linked_ledger(ledger: list[dict[str, str]], trace_id: str, code: str) -> list[dict[str, str]]:
    keys = ("entry_trace_id", "source_trace_id", "trace_id")
    return [r for r in ledger if r.get("code") == code and any(r.get(k) == trace_id for k in keys)]


def scan_historical_surge_rows() -> dict[str, list[dict[str, str]]]:
    hits: dict[str, list[dict[str, str]]] = {code: [] for code in TARGET_CODES}
    scan_paths = [
        LOG_DIR / "surge_realtime_latest.csv",
        LOG_DIR / "surge_sanity_labeled_latest.csv",
        LOG_DIR / "surge_lob_latest.csv",
        LOG_DIR / "surge_shadow_probe_candidate_report_latest.csv",
        LOG_DIR / "surge_recovery_reentry_candidates_latest.csv",
    ]
    for path in scan_paths:
        if not path.exists():
            continue
        rows = read_csv(path)
        for row in rows:
            code = row.get("code")
            if code not in TARGET_CODES:
                continue
            date = str(row.get("date") or row.get("entry_ymd") or row.get("ymd") or "")[:8]
            ts = str(row.get("ts") or row.get("entry_ts") or "")
            if date == TARGET_YMD or TARGET_YMD in ts:
                trimmed = {
                    "source": str(path),
                    "ts": ts,
                    "date": date,
                    "entry_decision": row.get("entry_decision", ""),
                    "entry_allowed": row.get("entry_allowed", ""),
                    "entry_blocked": row.get("entry_blocked", ""),
                    "exclude_reasons": row.get("exclude_reasons", ""),
                    "lob_status": row.get("lob_status", ""),
                    "orderflow_tag": row.get("orderflow_tag", ""),
                    "surge_score": row.get("surge_score", ""),
                    "change_pct": row.get("change_pct", ""),
                    "rvol20": row.get("rvol20", ""),
                    "intraday_high_drawdown_pct": row.get("intraday_high_drawdown_pct", ""),
                }
                hits[code].append(trimmed)
    return hits


def sum_key(rows: list[dict[str, str]], key: str) -> float:
    return sum(to_float(r.get(key)) for r in rows)


def build_row(split_row: dict[str, Any], trades: list[dict[str, str]], ledger: list[dict[str, str]], surge_hits: list[dict[str, str]]) -> dict[str, Any]:
    code = str(split_row.get("code"))
    trace_id = str(split_row.get("entry_trace_id"))
    linked = linked_trades(trades, trace_id, code)
    led = linked_ledger(ledger, trace_id, code)
    note_meta = parse_note(linked[0].get("note", "")) if linked else {}
    raw_qty, reduced_qty = parse_qty_arrow(note_meta.get("sector_hrp_qty", ""))
    reduction_pct = 1.0 - (reduced_qty / raw_qty) if raw_qty > 0 else 0.0
    worst_trade = min(linked, key=lambda r: to_float(r.get("net_ret")), default={})

    entry_notional = to_float(split_row.get("entry_notional"))
    ledger_pnl = sum_key(led, "realized_pnl_krw")
    loss_per_1m = ledger_pnl / (entry_notional / 1_000_000.0) if entry_notional else 0.0
    has_pre_entry_row = len(surge_hits) > 0
    has_lob_evidence = any(str(r.get("lob_status", "")).upper() not in {"", "NO_LOB"} for r in surge_hits)

    return {
        "code": code,
        "entry_trace_id": trace_id,
        "entry_ts": split_row.get("entry_ts", ""),
        "entry_qty": split_row.get("entry_qty", 0),
        "entry_price": split_row.get("entry_price", 0),
        "entry_notional": entry_notional,
        "trace_net_ret_sum": split_row.get("net_ret_sum", 0),
        "ledger_realized_pnl_sum": ledger_pnl,
        "ledger_loss_per_1m_notional": loss_per_1m,
        "exit_reasons": split_row.get("exit_reasons", ""),
        "last_exit_ymd": split_row.get("last_exit_ymd", ""),
        "paper_trade_rows": len(linked),
        "worst_paper_trade_id": worst_trade.get("trade_id", ""),
        "worst_paper_exit_ts": worst_trade.get("exit_ts", ""),
        "worst_paper_net_ret": to_float(worst_trade.get("net_ret"), 0.0),
        "fallback_stage": note_meta.get("fallback_stage", ""),
        "surge_immediate": note_meta.get("surge_immediate", split_row.get("surge_immediate", "")),
        "sector_hrp_reduce": note_meta.get("sector_hrp_reduce", ""),
        "sector_hrp_mult": to_float(note_meta.get("sector_hrp_mult"), 0.0),
        "sector_hrp_qty_raw": raw_qty,
        "sector_hrp_qty_reduced": reduced_qty,
        "sector_hrp_reduction_pct": reduction_pct,
        "sector_exposure_share": to_float(note_meta.get("sector_exposure_share"), 0.0),
        "historical_surge_candidate_rows": len(surge_hits),
        "historical_pre_entry_quality_available": has_pre_entry_row,
        "historical_lob_evidence_available": has_lob_evidence,
        "root_axis": "STOP_GAP_RESIDUAL_AFTER_SIZE_REDUCTION",
        "evidence_gap": "" if has_pre_entry_row else "NO_20260428_SURGE_CANDIDATE_ROW_FOUND_IN_CURRENT_LOGS",
        "candidate_next_test": "surge_stop_gap_pre_entry_quality_or_cap_whatif",
    }


def main() -> int:
    split = read_json(SPLIT_JSON)
    rows = split.get("rows") if isinstance(split.get("rows"), list) else []
    target_rows = [
        r for r in rows
        if r.get("driver_group") == "SURGE_STOP_GAP" and str(r.get("code")) in TARGET_CODES
    ]
    trades = read_csv(TRADES_CSV)
    ledger = read_csv(LEDGER_CSV)
    historical = scan_historical_surge_rows()
    out_rows = [build_row(r, trades, ledger, historical.get(str(r.get("code")), [])) for r in target_rows]
    out_rows.sort(key=lambda r: str(r.get("entry_ts", "")))

    summary = {
        "target_entries": len(out_rows),
        "trace_net_ret_sum": sum(to_float(r.get("trace_net_ret_sum")) for r in out_rows),
        "ledger_realized_pnl_sum": sum(to_float(r.get("ledger_realized_pnl_sum")) for r in out_rows),
        "entry_notional_sum": sum(to_float(r.get("entry_notional")) for r in out_rows),
        "historical_pre_entry_rows_found": sum(int(r.get("historical_surge_candidate_rows", 0)) for r in out_rows),
        "all_had_sector_hrp_reduce": all(str(r.get("sector_hrp_reduce")) == "1" for r in out_rows) if out_rows else False,
        "all_missing_20260428_pre_entry_quality": all(not bool(r.get("historical_pre_entry_quality_available")) for r in out_rows) if out_rows else False,
    }

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS" if len(out_rows) == 2 else "WARN",
        "scope": "read_only_20260428_surge_stop_gap_quality_diagnostic",
        "inputs": {
            "split": str(SPLIT_JSON),
            "trades": str(TRADES_CSV),
            "ledger": str(LEDGER_CSV),
            "surge_log_scan": [
                str(LOG_DIR / "surge_realtime_latest.csv"),
                str(LOG_DIR / "surge_sanity_labeled_latest.csv"),
                str(LOG_DIR / "surge_lob_latest.csv"),
                str(LOG_DIR / "surge_shadow_probe_candidate_report_latest.csv"),
                str(LOG_DIR / "surge_recovery_reentry_candidates_latest.csv"),
            ],
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
