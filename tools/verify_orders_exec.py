from __future__ import annotations

"""Control Center V2 Forensic 'Evidence 검증 액션' wrapper — evidence_id=orders_exec_integrity.

Read-only. Reuses build_order_fill_e2e_chain_report.build_report() (the existing
validated E2E chain report) as the single source of truth and filters to the
orders_exec-scoped checks only. Does not write any files, does not touch
orders/fills/ledger/Gate/LOCK. Always exits 0 — PASS/FAIL is carried in the
JSON body's "status" field so the calling Node route can always JSON.parse(stdout).
"""

import importlib.util
import json
from pathlib import Path

TOOLS = Path(__file__).resolve().parent


def _load_module(name: str):
    spec = importlib.util.spec_from_file_location(name, TOOLS / f"{name}.py")
    if spec is None or spec.loader is None:
        raise RuntimeError(f"{name} load failed")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


e2e = _load_module("build_order_fill_e2e_chain_report")

ORDERS_EXEC_CHECKS = {
    "D_rule",
    "orders_exec_exists",
    "orders_exec_date",
    "orders_exec_contract",
    "orders_vs_fills_qty",
    "paper_order_validation",
    "execution_safety_contract",
    "kis_api_sync",
    "broker_fill_match_report",
}


def main() -> int:
    try:
        payload = e2e.build_report()
    except Exception as exc:
        print(json.dumps({
            "status": "FAIL",
            "evidence_id": "orders_exec_integrity",
            "reason": f"build_report_error: {exc}",
        }, ensure_ascii=False))
        return 0

    all_rows = payload.get("rows", []) if isinstance(payload, dict) else []
    rows = [r for r in all_rows if r.get("check") in ORDERS_EXEC_CHECKS]

    # Same aggregation semantics as build_order_fill_e2e_chain_report.build_report():
    # only FAIL/ERROR is a hard failure; UNKNOWN/"" is PARTIAL; WARN/APPLIED/etc. still PASS.
    hard_fail_rows = [r for r in rows if str(r.get("status", "")).upper() in {"FAIL", "ERROR"}]
    unknown_rows = [r for r in rows if str(r.get("status", "")).upper() in {"UNKNOWN", ""}]
    if not rows:
        status = "NA"
    elif hard_fail_rows:
        status = "FAIL"
    elif unknown_rows:
        status = "PARTIAL"
    else:
        status = "PASS"

    result = {
        "status": status,
        "evidence_id": "orders_exec_integrity",
        "D": payload.get("D"),
        "checked_rows": len(rows),
        "hard_fail_rows": len(hard_fail_rows),
        "unknown_rows": len(unknown_rows),
        "rows": rows,
        "source_report": payload.get("sources", {}).get("orders_exec", ""),
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
