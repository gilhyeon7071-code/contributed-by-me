"""Consume surge EV staged probes into an isolated paper-only probe log.

Default behavior is fail-closed. Unless SURGE_EV_PAPER_PROBE_CONSUMER_ENABLED=1,
the script only records that staged rows were blocked by the consumer kill
switch. It never writes orders_exec, fills, RootB ledger, or broker routes.
"""
from __future__ import annotations

import csv
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

STAGED_JSON = LOG_DIR / "surge_ev_paper_probe_staged_latest.json"
OUT_JSON = LOG_DIR / "surge_ev_paper_probe_consumer_latest.json"
OUT_CSV = LOG_DIR / "surge_ev_paper_probe_consumer_latest.csv"
LEDGER_LATEST_CSV = LOG_DIR / "surge_ev_paper_probe_virtual_ledger_latest.csv"
LEDGER_HISTORY_CSV = LOG_DIR / "surge_ev_paper_probe_virtual_ledger_history.csv"

ENABLE_ENV = "SURGE_EV_PAPER_PROBE_CONSUMER_ENABLED"


def _enabled() -> bool:
    return str(os.getenv(ENABLE_ENV, "0") or "0").strip().lower() in {"1", "true", "yes", "y", "on"}


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
        fields = ["consumer_id"]
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def _append_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
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


def _consume_row(row: Dict[str, Any], enabled: bool, ts: str) -> Dict[str, Any]:
    stage_id = str(row.get("stage_id") or "")
    blocked_reasons: List[str] = []
    if not enabled:
        blocked_reasons.append("CONSUMER_KILL_SWITCH_OFF")
    if _truthy(row.get("dispatch_enabled")):
        blocked_reasons.append("STAGED_DISPATCH_ENABLED_UNEXPECTED")
    if _truthy(row.get("paper_order_route")):
        blocked_reasons.append("STAGED_PAPER_ROUTE_UNEXPECTED")
    if _truthy(row.get("broker_order_route")):
        blocked_reasons.append("STAGED_BROKER_ROUTE_UNEXPECTED")
    if _truthy(row.get("trading_allowed")):
        blocked_reasons.append("STAGED_TRADING_ALLOWED_UNEXPECTED")
    if not _truthy(row.get("must_not_dispatch")):
        blocked_reasons.append("MUST_NOT_DISPATCH_MISSING")

    safe_to_log_virtual = bool(enabled and not blocked_reasons)
    consumer_status = "VIRTUAL_PAPER_PROBE_LOGGED" if safe_to_log_virtual else "BLOCKED"
    return {
        "consumer_id": f"CONSUME_{stage_id}_{ts.replace(':', '').replace('-', '')}",
        "consumer_ts": ts,
        "consumer_status": consumer_status,
        "blocked_reasons": "|".join(blocked_reasons),
        "stage_id": stage_id,
        "stage_status": row.get("stage_status", ""),
        "rule": row.get("rule", ""),
        "code": row.get("code", ""),
        "side": "BUY_PROBE",
        "qty": row.get("suggested_qty", ""),
        "reference_price": row.get("entry_reference_price", ""),
        "notional_krw": row.get("suggested_notional_krw", ""),
        "estimated_loss_at_stop_krw": row.get("estimated_loss_at_stop_krw", ""),
        "probe_stop_loss_pct": row.get("probe_stop_loss_pct", ""),
        "probe_timebox_min": row.get("probe_timebox_min", ""),
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
    }


def main() -> int:
    ts = datetime.now().isoformat(timespec="seconds")
    enabled = _enabled()
    staged = _read_json(STAGED_JSON)
    staged_rows = list(staged.get("rows") or [])
    rows = [_consume_row(row, enabled=enabled, ts=ts) for row in staged_rows]
    virtual_rows = [row for row in rows if row.get("consumer_status") == "VIRTUAL_PAPER_PROBE_LOGGED"]
    payload = {
        "ts": ts,
        "status": "OK",
        "scope": "surge_ev_paper_probe_consumer",
        "enabled_env": ENABLE_ENV,
        "enabled": enabled,
        "source_staged_json": str(STAGED_JSON),
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
            "default_fail_closed": True,
        },
        "summary": {
            "staged_rows": len(staged_rows),
            "consumer_rows": len(rows),
            "virtual_logged_rows": len(virtual_rows),
            "blocked_rows": len(rows) - len(virtual_rows),
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
    _write_csv(LEDGER_LATEST_CSV, virtual_rows)
    _append_csv(LEDGER_HISTORY_CSV, virtual_rows)
    print(json.dumps({
        "status": "OK",
        "enabled": enabled,
        "staged_rows": len(staged_rows),
        "virtual_logged_rows": len(virtual_rows),
        "blocked_rows": len(rows) - len(virtual_rows),
        "out_json": str(OUT_JSON),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
