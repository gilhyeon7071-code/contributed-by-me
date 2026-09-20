from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
STAGED_JSON = LOG_DIR / "surge_ev_paper_probe_staged_latest.json"
CONSUMER_JSON = LOG_DIR / "surge_ev_paper_probe_consumer_latest.json"
OUT_JSON = LOG_DIR / "surge_virtual_probe_safety_audit_latest.json"


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def main() -> int:
    now = datetime.now().isoformat(timespec="seconds")
    staged = _read_json(STAGED_JSON)
    consumer = _read_json(CONSUMER_JSON)
    violations: List[Dict[str, Any]] = []
    for row in staged.get("rows") or []:
        bad = []
        for key in ("paper_order_route", "broker_order_route", "dispatch_enabled", "trading_allowed"):
            if _truthy(row.get(key)):
                bad.append(key)
        if not _truthy(row.get("must_not_dispatch")):
            bad.append("must_not_dispatch_missing")
        if bad:
            violations.append({"code": row.get("code", ""), "stage_id": row.get("stage_id", ""), "violations": bad})
    consumer_summary = consumer.get("summary") or {}
    consumer_route_zero = all(
        int(consumer_summary.get(key) or 0) == 0
        for key in (
            "orders_exec_write_rows",
            "fills_write_rows",
            "rootb_ledger_write_rows",
            "broker_order_route_rows",
            "dispatch_enabled_rows",
            "trading_allowed_rows",
        )
    )
    enable_precheck = "PASS" if not violations else "FAIL"
    payload = {
        "ts": now,
        "status": "OK" if enable_precheck == "PASS" and consumer_route_zero else "WARN",
        "scope": "surge_virtual_probe_safety_audit",
        "source_staged_json": str(STAGED_JSON),
        "source_consumer_json": str(CONSUMER_JSON),
        "summary": {
            "staged_rows": len(staged.get("rows") or []),
            "stage_route_violations": len(violations),
            "consumer_enabled_now": consumer.get("enabled"),
            "consumer_route_zero": consumer_route_zero,
            "virtual_enable_precheck": enable_precheck,
            "would_log_virtual_rows_if_enabled": len(staged.get("rows") or []) if enable_precheck == "PASS" else 0,
            "orders_exec_write": False,
            "fills_write": False,
            "rootb_ledger_write": False,
            "broker_order_route": False,
            "dispatch_enabled": False,
            "trading_allowed": False,
        },
        "risk_contract": {
            "audit_only": True,
            "env_changed": False,
            "orders_exec_write": False,
            "fills_write": False,
            "rootb_ledger_write": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "dispatch_enabled": False,
            "trading_allowed": False,
            "policy_change": False,
            "entry_approval_changed": False,
            "research_only": True,
            "must_not_dispatch": True,
        },
        "violations": violations,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": payload["status"], **payload["summary"], "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
