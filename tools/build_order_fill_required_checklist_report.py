from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER = ROOT / "paper"


def _read_json(path: Path) -> Dict[str, Any]:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except Exception:
            continue
    return {}


def _append(rows: List[Dict[str, Any]], item: str, status: str, evidence: Any, note: str) -> None:
    rows.append({"item": item, "status": status, "evidence": evidence, "note": note})


def _order_validation_regression_status(report: Dict[str, Any]) -> str:
    status = str(report.get("status") or "")
    if status == "PASS":
        return "PASS"
    issues = set(str(x) for x in (report.get("issues") or []))
    components = report.get("component_status") if isinstance(report.get("component_status"), dict) else {}
    non_prevalidation_bad = [
        key for key, value in components.items()
        if key != "pre_validation" and str(value) != "PASS"
    ]
    if status == "WARN" and issues == {"pre_validation:entry_ready_zero_after_caps"} and not non_prevalidation_bad:
        return "PASS_WITH_ENTRY_WARN"
    return "FAIL"


def build_report() -> Dict[str, Any]:
    e2e = _read_json(LOG_DIR / "order_fill_e2e_chain_report_latest.json")
    broker = _read_json(LOG_DIR / "broker_live_e2e_readiness_latest.json")
    p0_contract = _read_json(LOG_DIR / f"p0_orders_exec_contract_{e2e.get('D') or '20260528'}.json")
    order_validation = _read_json(LOG_DIR / "paper_order_validation_report_latest.json")
    pnl = _read_json(LOG_DIR / "paper_pnl_summary_last.json")
    rootb_sync = _read_json(LOG_DIR / "rootb_fills_from_roota_latest.json")
    ledger_dry = _read_json(LOG_DIR / "ledger_live_fills_dry_run_latest.json")
    reconcile = _read_json(LOG_DIR / "reconcile_paper_state_from_fills_latest.json")
    ssot = _read_json(LOG_DIR / "ssot_health_card_latest.json")

    d = str(e2e.get("D") or "")
    rows: List[Dict[str, Any]] = []
    summary = e2e.get("summary") if isinstance(e2e.get("summary"), dict) else {}

    qty_status = str(summary.get("qty_compare_status") or "")
    _append(
        rows,
        "actual_fills_vs_orders_match",
        "PASS" if qty_status == "PASS" else "FAIL",
        {
            "orders_rows": summary.get("orders_rows"),
            "fills_rows_D": summary.get("fills_rows_D"),
            "orders_qty_by_side_code": summary.get("orders_qty_by_side_code"),
            "fills_qty_by_side_code": summary.get("fills_qty_by_side_code"),
        },
        "rows can differ when one order is split; quantity by side+code must match",
    )
    _append(
        rows,
        "fill_qty_price_side_consistency",
        "PASS" if p0_contract.get("status") == "PASS" and qty_status == "PASS" else "FAIL",
        {
            "p0_orders_exec_contract": p0_contract.get("summary"),
            "e2e_qty_compare_status": qty_status,
        },
        "orders contract checks positive qty/date and E2E checks side+code quantity match",
    )
    _append(
        rows,
        "ledger_reflection",
        "PASS" if not summary.get("rootb_missing_ledger_fill_ids") and ledger_dry.get("status") == "PASS" else "FAIL",
        {
            "rootb_live_rows_D": summary.get("rootb_live_rows_D"),
            "rootb_ledger_rows_D": summary.get("rootb_ledger_rows_D"),
            "missing_ledger_fill_ids": summary.get("rootb_missing_ledger_fill_ids"),
            "ledger_dry_status": ledger_dry.get("status"),
        },
        "RootB live fill ids for D are covered by ledger",
    )
    _append(
        rows,
        "stats_reflection",
        "PASS" if str(pnl.get("as_of_ymd") or "") == d and e2e.get("status") == "PASS" else "FAIL",
        {
            "pnl_as_of_ymd": pnl.get("as_of_ymd"),
            "trades_used": pnl.get("trades_used"),
            "rows_as_of": pnl.get("rows_as_of"),
            "trades_exit_rows_D": summary.get("trades_exit_rows_D"),
            "trades_calc_exit_rows_D": summary.get("trades_calc_exit_rows_D"),
        },
        "stats artifact is as-of D and trade exit rows match SELL fills",
    )

    broker_summary = broker.get("summary") if isinstance(broker.get("summary"), dict) else {}
    broker_status = str(broker.get("status") or "")
    _append(
        rows,
        "paper_broker_date_mixing",
        "PASS_WITH_BROKER_NA" if broker_status == "NOT_EVALUABLE" else ("PASS" if broker_status == "PASS" else "FAIL"),
        {
            "paper_D": d,
            "broker_status": broker_status,
            "prod_effective_ord_no_rows": broker_summary.get("prod_effective_ord_no_rows"),
            "kis_fills_api_rows": broker_summary.get("kis_fills_api_rows"),
            "kis_sync_mock_resolved": broker_summary.get("kis_sync_mock_resolved"),
        },
        "paper/mock chain is D-aligned; broker-live is separated as NOT_EVALUABLE because broker evidence is absent",
    )

    asof_ok = str(pnl.get("as_of_ymd") or "") == d and str(rootb_sync.get("as_of_ymd") or "") == d
    run_ids = {
        "pnl_run_id": pnl.get("run_id"),
        "rootb_sync_run_id": rootb_sync.get("run_id"),
        "reconcile_run_id": reconcile.get("run_id"),
    }
    _append(
        rows,
        "as_of_run_id_consistency",
        "PASS" if asof_ok and all(v for v in run_ids.values()) else "FAIL",
        {
            "D": d,
            "pnl_as_of_ymd": pnl.get("as_of_ymd"),
            "rootb_sync_as_of_ymd": rootb_sync.get("as_of_ymd"),
            "run_ids": run_ids,
        },
        "as_of fields align to D and key downstream run_id values exist",
    )

    order_validation_regression_status = _order_validation_regression_status(order_validation)
    _append(
        rows,
        "regression_validation",
        "PASS" if e2e.get("status") == "PASS" and order_validation_regression_status == "PASS" and (ssot.get("overall") or {}).get("status") == "PASS"
        else ("PASS_WITH_ENTRY_WARN" if e2e.get("status") == "PASS" and order_validation_regression_status == "PASS_WITH_ENTRY_WARN" and (ssot.get("overall") or {}).get("status") == "PASS" else "FAIL"),
        {
            "order_fill_e2e_status": e2e.get("status"),
            "paper_order_validation_status": order_validation.get("status"),
            "paper_order_validation_issues": order_validation.get("issues"),
            "ssot_health_card_status": (ssot.get("overall") or {}).get("status"),
            "component_status": order_validation.get("component_status"),
        },
        "order/fill regression remains PASS; entry_ready_zero_after_caps is reported as entry-selection warning",
    )

    hard_fail = any(str(r["status"]).upper() == "FAIL" for r in rows)
    status = "PASS_WITH_BROKER_NA" if not hard_fail and any(str(r["status"]).upper().endswith("BROKER_NA") for r in rows) else ("PASS" if not hard_fail else "FAIL")
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z"),
        "schema_version": "order_fill_required_checklist_v1",
        "scope": "read_only_required_order_fill_checklist",
        "status": status,
        "D": d,
        "policy_change": False,
        "trading_effect": False,
        "sources": {
            "order_fill_e2e": str(LOG_DIR / "order_fill_e2e_chain_report_latest.json"),
            "broker_live_e2e": str(LOG_DIR / "broker_live_e2e_readiness_latest.json"),
            "p0_orders_exec_contract": str(LOG_DIR / f"p0_orders_exec_contract_{d}.json"),
            "paper_order_validation": str(LOG_DIR / "paper_order_validation_report_latest.json"),
            "paper_pnl_summary": str(LOG_DIR / "paper_pnl_summary_last.json"),
            "rootb_sync": str(LOG_DIR / "rootb_fills_from_roota_latest.json"),
            "ledger_dry_run": str(LOG_DIR / "ledger_live_fills_dry_run_latest.json"),
        },
        "rows": rows,
    }


def main() -> int:
    payload = build_report()
    out_json = LOG_DIR / "order_fill_required_checklist_latest.json"
    out_csv = LOG_DIR / "order_fill_required_checklist_latest.csv"
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["item", "status", "evidence", "note"])
        writer.writeheader()
        writer.writerows(payload["rows"])
    print(json.dumps({"status": payload["status"], "D": payload["D"], "out_json": str(out_json), "out_csv": str(out_csv)}, ensure_ascii=False))
    return 0 if not str(payload["status"]).startswith("FAIL") else 1


if __name__ == "__main__":
    raise SystemExit(main())
