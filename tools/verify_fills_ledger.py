from __future__ import annotations

"""Control Center V2 Forensic 'Evidence 검증 액션' wrapper — evidence_id=fills_ledger_reconcile.

Read-only. Reuses build_order_fill_e2e_chain_report.build_report() (the existing
validated E2E chain report) as the single source of truth and filters to the
fills-to-ledger reconciliation checks (RootA fills/trades and RootB live/ledger).
Does not write any files, does not touch orders/fills/ledger/Gate/LOCK. Always
exits 0 — PASS/FAIL is carried in the JSON body's "status" field so the calling
Node route can always JSON.parse(stdout).
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

FILLS_LEDGER_CHECKS = {
    "fills_to_trades",
    "paper_state_reconcile",
    "rootb_live_to_ledger",
    "rootb_sync_from_roota",
    "stats_pnl_asof",
    "canonical_fills_shadow",
    "ssot_health_card",
}


def main() -> int:
    try:
        payload = e2e.build_report()
    except Exception as exc:
        print(json.dumps({
            "status": "FAIL",
            "evidence_id": "fills_ledger_reconcile",
            "reason": f"build_report_error: {exc}",
        }, ensure_ascii=False))
        return 0

    all_rows = payload.get("rows", []) if isinstance(payload, dict) else []
    rows = [r for r in all_rows if r.get("check") in FILLS_LEDGER_CHECKS]

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
        "evidence_id": "fills_ledger_reconcile",
        "D": payload.get("D"),
        "checked_rows": len(rows),
        "hard_fail_rows": len(hard_fail_rows),
        "unknown_rows": len(unknown_rows),
        "rows": rows,
        "source_reports": {
            "rootb_live": payload.get("sources", {}).get("rootb_live", ""),
            "rootb_ledger": payload.get("sources", {}).get("rootb_ledger", ""),
        },
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
