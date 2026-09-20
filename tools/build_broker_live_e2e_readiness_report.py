from __future__ import annotations

import csv
import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PAPER = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"


def _read_json(path: Path) -> Dict[str, Any]:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except Exception:
            continue
    return {}


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc).fillna("")
        except Exception:
            continue
    return pd.DataFrame()


def _read_xlsx(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_excel(path, dtype=str).fillna("")
    except Exception:
        return pd.DataFrame()


def _norm_ymd(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text[:8]


def _derive_d() -> str:
    fills = _read_csv(PAPER / "fills.csv")
    if fills.empty or "datetime" not in fills.columns:
        return ""
    fills["_ymd"] = fills["datetime"].map(_norm_ymd)
    buys = fills.loc[fills.get("side", "").astype(str).str.upper().eq("BUY")]
    if not buys.empty:
        return str(buys["_ymd"].max() or "")
    return str(fills["_ymd"].max() or "")


def _append(rows: List[Dict[str, Any]], check: str, status: str, value: Any, expected: Any, evidence: str) -> None:
    rows.append({"check": check, "status": status, "value": value, "expected": expected, "evidence": evidence})


def build_report(evaluation_date: str = "") -> Dict[str, Any]:
    fill_d = _derive_d()
    requested_date = _norm_ymd(evaluation_date)
    d = requested_date or fill_d
    d_source = "requested_date" if requested_date else "fills_latest_buy_d"
    orders_exec_path = PAPER / f"orders_{d}_exec.xlsx"
    prod_submit_path = PAPER / f"orders_{d}_broker_submit_prod.csv"
    mock_submit_path = PAPER / f"orders_{d}_broker_submit_mock.csv"
    kis_fills_path = PAPER / f"kis_fills_api_{d}.csv"
    kis_sync_path = LOG_DIR / f"kis_fills_sync_{d}.json"
    fill_match_path = LOG_DIR / f"fills_match_report_{d}.json"
    prod_verify_path = LOG_DIR / f"kis_ssot_verify_{d}_prod_latest.json"
    mock_verify_path = LOG_DIR / f"kis_ssot_verify_{d}_mock_latest.json"

    orders_exec = _read_xlsx(orders_exec_path)
    prod_submit = _read_csv(prod_submit_path)
    mock_submit = _read_csv(mock_submit_path)
    kis_fills = _read_csv(kis_fills_path)
    kis_sync = _read_json(kis_sync_path)
    fill_match = _read_json(fill_match_path)
    prod_verify = _read_json(prod_verify_path)
    mock_verify = _read_json(mock_verify_path)

    prod_effective_ord = 0
    if not prod_submit.empty and "ord_no" in prod_submit.columns:
        prod_effective_ord = int((prod_submit["ord_no"].astype(str).str.strip() != "").sum())
    mock_effective_ord = 0
    if not mock_submit.empty and "ord_no" in mock_submit.columns:
        mock_effective_ord = int((mock_submit["ord_no"].astype(str).str.strip() != "").sum())

    kis_fill_rows = int(len(kis_fills))
    rows: List[Dict[str, Any]] = []
    _append(rows, "D_rule_reference", "PASS" if fill_d else "FAIL", fill_d, "latest BUY ymd from fills.csv", str(PAPER / "fills.csv"))
    _append(rows, "evaluation_date", "PASS" if d else "FAIL", d, "requested date or D_rule_reference", str(orders_exec_path))
    _append(rows, "D_rule_alignment", "PASS" if (not requested_date or requested_date == fill_d) else "NOT_EVALUABLE", {"fill_D": fill_d, "evaluation_date": d}, "same after real BUY fills exist", str(PAPER / "fills.csv"))
    has_orders_exec = len(orders_exec) > 0
    _append(rows, "orders_exec_rows", "PASS" if has_orders_exec else "NOT_EVALUABLE", int(len(orders_exec)), ">0 for order/fill E2E proof", str(orders_exec_path))
    prod_submit_exists_status = "PASS" if prod_submit_path.exists() else ("NOT_EVALUABLE" if not has_orders_exec else "FAIL")
    _append(rows, "prod_submit_exists", prod_submit_exists_status, str(prod_submit_path), "exists when orders_exec has rows", str(prod_submit_path))
    _append(rows, "prod_submit_effective_ord_no", "NOT_EVALUABLE" if prod_effective_ord == 0 else "PASS", prod_effective_ord, ">0 for broker-live proof", str(prod_submit_path))
    _append(rows, "kis_fills_api_rows", "NOT_EVALUABLE" if kis_fill_rows == 0 else "PASS", kis_fill_rows, ">0 for broker-live proof", str(kis_fills_path))
    _append(rows, "kis_sync_soft_fail", "PASS" if not bool(kis_sync.get("api_soft_fail", False)) else "FAIL", kis_sync.get("api_soft_fail"), False, str(kis_sync_path))
    _append(rows, "fill_match_report", "NOT_EVALUABLE" if kis_fill_rows == 0 else str(fill_match.get("status") or "PASS"), fill_match.get("counts", {}), "nonzero broker fills matched", str(fill_match_path))
    prod_ssot_status = str(prod_verify.get("overall_status") or "")
    mock_ssot_status = str(mock_verify.get("overall_status") or "")
    _append(rows, "kis_ssot_prod", prod_ssot_status or "NOT_EVALUABLE", prod_verify.get("warnings", []), "PASS when broker order_no and fills exist", str(prod_verify_path))
    _append(rows, "kis_ssot_mock", mock_ssot_status or "NOT_EVALUABLE", mock_verify.get("warnings", []), "PASS when broker order_no and fills exist", str(mock_verify_path))

    broker_live_ready = bool(prod_effective_ord > 0 and kis_fill_rows > 0 and str(prod_verify.get("overall_status") or "").upper() == "PASS")
    if broker_live_ready:
        status = "PASS"
        verdict = "BROKER_LIVE_E2E_READY"
        next_action = "VERIFY_REAL_BROKER_FILLS_TO_LEDGER"
    else:
        status = "NOT_EVALUABLE"
        verdict = "BROKER_LIVE_E2E_NOT_EVALUABLE"
        next_action = "WAIT_FOR_REAL_BROKER_ORDER_NO_AND_KIS_FILLS"

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z"),
        "schema_version": "broker_live_e2e_readiness_v1",
        "scope": "read_only_broker_live_e2e_readiness",
        "status": status,
        "D": d,
        "D_rule_reference": fill_d,
        "D_source": d_source,
        "policy_change": False,
        "trading_effect": False,
        "verdict": verdict,
        "next_action": next_action,
        "summary": {
            "orders_exec_rows": int(len(orders_exec)),
            "prod_submit_rows": int(len(prod_submit)),
            "prod_effective_ord_no_rows": prod_effective_ord,
            "mock_submit_rows": int(len(mock_submit)),
            "mock_effective_ord_no_rows": mock_effective_ord,
            "kis_fills_api_rows": kis_fill_rows,
            "kis_sync_mock_resolved": kis_sync.get("mock_resolved"),
            "kis_sync_rows_normalized": kis_sync.get("rows_normalized"),
            "prod_ssot_status": prod_verify.get("overall_status"),
            "mock_ssot_status": mock_verify.get("overall_status"),
        },
        "sources": {
            "orders_exec": str(orders_exec_path),
            "prod_submit": str(prod_submit_path),
            "mock_submit": str(mock_submit_path),
            "kis_fills_api": str(kis_fills_path),
            "kis_sync": str(kis_sync_path),
            "fills_match_report": str(fill_match_path),
            "kis_ssot_prod": str(prod_verify_path),
            "kis_ssot_mock": str(mock_verify_path),
        },
        "rows": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build read-only broker-live E2E readiness report.")
    parser.add_argument("--date", default="", help="Optional YYYYMMDD evaluation date. Keeps fills D as reference.")
    args = parser.parse_args()
    payload = build_report(args.date)
    out_json = LOG_DIR / "broker_live_e2e_readiness_latest.json"
    out_csv = LOG_DIR / "broker_live_e2e_readiness_latest.csv"
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["check", "status", "value", "expected", "evidence"])
        writer.writeheader()
        writer.writerows(payload["rows"])
    print(json.dumps({"status": payload["status"], "verdict": payload["verdict"], "next_action": payload["next_action"], "summary": payload["summary"], "out_json": str(out_json), "out_csv": str(out_csv)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
