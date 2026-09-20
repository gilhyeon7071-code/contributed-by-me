"""Record surge probe sizing review rows into a read-only virtual ledger.

This consumer is intentionally separate from order staging. It does not write
orders_exec, fills, broker routes, or RootB ledgers.
"""
from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

INPUT_JSON = LOG_DIR / "surge_probe_sizing_review_latest.json"
OUT_JSON = LOG_DIR / "surge_probe_sizing_virtual_ledger_latest.json"
OUT_CSV = LOG_DIR / "surge_probe_sizing_virtual_ledger_latest.csv"
HISTORY_CSV = LOG_DIR / "surge_probe_sizing_virtual_ledger_history.csv"


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: List[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    if not fields:
        fields = ["ledger_id"]
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def _append_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    fields: List[str] = []
    if path.exists() and path.stat().st_size > 5:
        with path.open("r", encoding="utf-8-sig", newline="") as fp:
            reader = csv.reader(fp)
            try:
                fields = [str(x) for x in next(reader)]
            except StopIteration:
                fields = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    exists = path.exists() and path.stat().st_size > 5
    with path.open("a", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_row(row: Dict[str, Any]) -> tuple[bool, str]:
    bad: List[str] = []
    for key in ("paper_order_route", "broker_order_route", "dispatch_enabled", "trading_allowed"):
        if _truthy(row.get(key)):
            bad.append(f"{key}_unexpected_true")
    if _truthy(row.get("entry_approval_changed")):
        bad.append("entry_approval_changed_unexpected_true")
    if _truthy(row.get("policy_change")):
        bad.append("policy_change_unexpected_true")
    if not _truthy(row.get("must_not_dispatch")):
        bad.append("must_not_dispatch_missing")
    status = str(row.get("review_status") or "")
    if status != "SIZING_REVIEW_ONLY_NOT_ROUTED":
        bad.append(f"review_status_not_loggable:{status or 'EMPTY'}")
    qty = int(float(row.get("review_qty") or 0))
    notional = float(row.get("review_notional_krw") or 0.0)
    if qty <= 0 or notional <= 0:
        bad.append("non_positive_qty_or_notional")
    return not bad, "|".join(bad)


def main() -> int:
    ts = datetime.now().isoformat(timespec="seconds")
    source = _read_json(INPUT_JSON)
    source_rows = list(source.get("rows") or [])
    rows: List[Dict[str, Any]] = []
    for row in source_rows:
        safe, block_reason = _safe_row(row)
        code = str(row.get("code") or "").zfill(6)
        rows.append({
            "ledger_id": f"SURGE_SIZING_REVIEW_{code}_{ts.replace(':', '').replace('-', '')}",
            "ledger_ts": ts,
            "ledger_status": "VIRTUAL_REVIEW_LOGGED" if safe else "BLOCKED",
            "blocked_reasons": block_reason,
            "source_status": source.get("status", ""),
            "code": code,
            "review_qty": row.get("review_qty", ""),
            "review_notional_krw": row.get("review_notional_krw", ""),
            "estimated_loss_at_stop_krw": row.get("estimated_loss_at_stop_krw", ""),
            "probe_stop_loss_pct": row.get("probe_stop_loss_pct", ""),
            "probe_timebox_min": row.get("probe_timebox_min", ""),
            "lob_quality_class": row.get("lob_quality_class", ""),
            "spread_bps": row.get("spread_bps", ""),
            "ask_depth_levels_live": row.get("ask_depth_levels_live", ""),
            "ret_10m_pct": row.get("ret_10m_pct", ""),
            "outcome_status_10m": row.get("outcome_status_10m", ""),
            "orders_exec_write": False,
            "fills_write": False,
            "rootb_ledger_write": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "dispatch_enabled": False,
            "entry_approval_changed": False,
            "policy_change": False,
            "trading_allowed": False,
            "research_only": True,
            "must_not_dispatch": True,
        })
    logged_rows = [row for row in rows if row.get("ledger_status") == "VIRTUAL_REVIEW_LOGGED"]
    payload = {
        "ts": ts,
        "status": "OK",
        "scope": "surge_probe_sizing_virtual_ledger_read_only",
        "source_sizing_json": str(INPUT_JSON),
        "risk_contract": {
            "orders_exec_write": False,
            "fills_write": False,
            "rootb_ledger_write": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "dispatch_enabled": False,
            "entry_approval_changed": False,
            "policy_change": False,
            "trading_allowed": False,
            "research_only": True,
            "must_not_dispatch": True,
        },
        "summary": {
            "source_rows": len(source_rows),
            "ledger_rows": len(rows),
            "virtual_logged_rows": len(logged_rows),
            "blocked_rows": len(rows) - len(logged_rows),
            "orders_exec_write_rows": 0,
            "fills_write_rows": 0,
            "rootb_ledger_write_rows": 0,
            "broker_order_route_rows": 0,
            "dispatch_enabled_rows": 0,
            "trading_allowed_rows": 0,
        },
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    _append_csv(HISTORY_CSV, logged_rows)
    print(json.dumps({
        "status": "OK",
        "source_rows": len(source_rows),
        "virtual_logged_rows": len(logged_rows),
        "blocked_rows": len(rows) - len(logged_rows),
        "out_json": str(OUT_JSON),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
