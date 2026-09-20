from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PAPER = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"
ROOTB = Path("E:/vibe/buffett")


def _read_json(path: Path) -> Dict[str, Any]:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except Exception:
            continue
    return {}


def _norm_ymd(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text[:8]


def _norm_code(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text.zfill(6)[-6:] if text else ""


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value).replace(",", "").strip()
        if not text:
            return default
        out = float(text)
        if out != out:
            return default
        return out
    except Exception:
        return default


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc).fillna("")
        except Exception:
            continue
    return pd.DataFrame()


def _derive_d(fills: pd.DataFrame) -> str:
    if fills.empty or "datetime" not in fills.columns:
        return ""
    work = fills.copy()
    work["_ymd"] = work["datetime"].map(_norm_ymd)
    if "side" in work.columns:
        buys = work.loc[work["side"].astype(str).str.upper().eq("BUY")]
        if not buys.empty:
            return str(buys["_ymd"].max() or "")
    return str(work["_ymd"].max() or "")


def _side_code_qty(df: pd.DataFrame, qty_col: str) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if df.empty or "code" not in df.columns or "side" not in df.columns or qty_col not in df.columns:
        return out
    for _, row in df.iterrows():
        code = _norm_code(row.get("code"))
        side = str(row.get("side") or "").upper()
        if not code or side not in {"BUY", "SELL"}:
            continue
        key = f"{side}:{code}"
        out[key] = out.get(key, 0.0) + _to_float(row.get(qty_col), 0.0)
    return {k: float(v) for k, v in sorted(out.items())}


def _compare_qty(left: Dict[str, float], right: Dict[str, float]) -> Dict[str, Any]:
    keys = sorted(set(left) | set(right))
    diffs = []
    for key in keys:
        lv = float(left.get(key, 0.0))
        rv = float(right.get(key, 0.0))
        if abs(lv - rv) > 1e-9:
            diffs.append({"key": key, "orders_qty": lv, "fills_qty": rv, "diff": rv - lv})
    return {"status": "PASS" if not diffs else "FAIL", "diffs": diffs}


def _filter_ymd(df: pd.DataFrame, col: str, d: str) -> pd.DataFrame:
    if df.empty or col not in df.columns:
        return pd.DataFrame(columns=list(df.columns))
    return df.loc[df[col].map(_norm_ymd).eq(d)].copy()


def _append(rows: List[Dict[str, Any]], check: str, status: str, value: Any, expected: Any, evidence: str) -> None:
    rows.append(
        {
            "check": check,
            "status": status,
            "value": value,
            "expected": expected,
            "evidence": evidence,
        }
    )


def build_report() -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    fills = _read_csv(PAPER / "fills.csv")
    d = _derive_d(fills)
    orders_path = PAPER / f"orders_{d}_exec.xlsx"
    fills_d = _filter_ymd(fills, "datetime", d)

    if orders_path.exists():
        orders = pd.read_excel(orders_path, dtype=str).fillna("")
    else:
        orders = pd.DataFrame()

    orders_dates = sorted({_norm_ymd(x) for x in orders.get("exec_date", pd.Series(dtype=str)).tolist() if _norm_ymd(x)})
    orders_qty = _side_code_qty(orders, "fill_qty")
    fills_qty = _side_code_qty(fills_d, "qty")
    qty_cmp = _compare_qty(orders_qty, fills_qty)

    trades = _read_csv(PAPER / "trades.csv")
    trades_calc = _read_csv(PAPER / "trades_calc.csv")
    trades_d = _filter_ymd(trades, "exit_date", d)
    trades_calc_d = _filter_ymd(trades_calc, "exit_ts", d)
    sell_fills_d = fills_d.loc[fills_d.get("side", pd.Series("", index=fills_d.index)).astype(str).str.upper().eq("SELL")].copy() if not fills_d.empty else pd.DataFrame()

    p0_contract = _read_json(LOG_DIR / f"p0_orders_exec_contract_{d}.json")
    order_validation = _read_json(LOG_DIR / "paper_order_validation_report_latest.json")
    execution_safety = _read_json(LOG_DIR / "execution_safety_contract_latest.json")
    fills_match = _read_json(LOG_DIR / f"fills_match_report_{d}.json")
    kis_sync = _read_json(LOG_DIR / f"kis_fills_sync_{d}.json")
    canonical = _read_json(LOG_DIR / "canonical_fills_shadow_latest.json")
    reconcile = _read_json(LOG_DIR / "reconcile_paper_state_from_fills_latest.json")
    ssot = _read_json(LOG_DIR / "ssot_health_card_latest.json")
    pnl = _read_json(LOG_DIR / "paper_pnl_summary_last.json")
    ledger_dry = _read_json(LOG_DIR / "ledger_live_fills_dry_run_latest.json")
    rootb_sync = _read_json(LOG_DIR / "rootb_fills_from_roota_latest.json")

    live = _read_csv(ROOTB / "data" / "live" / "live_fills.csv")
    ledger = _read_csv(ROOTB / "data" / "ledger" / "paper_fills_ledger.csv")
    live_d = _filter_ymd(live, "date", d)
    ledger_d = _filter_ymd(ledger, "date", d)
    live_fill_ids = {str(x) for x in live_d.get("fill_id", pd.Series(dtype=str)).tolist() if str(x).strip()}
    ledger_fill_ids = {str(x) for x in ledger_d.get("fill_id", pd.Series(dtype=str)).tolist() if str(x).strip()}
    missing_ledger_ids = sorted(live_fill_ids - ledger_fill_ids)

    _append(rows, "D_rule", "PASS" if d else "FAIL", d, "latest BUY ymd from fills.csv", str(PAPER / "fills.csv"))
    _append(rows, "orders_exec_exists", "PASS" if orders_path.exists() else "FAIL", str(orders_path), "exists", str(orders_path))
    _append(rows, "orders_exec_date", "PASS" if orders_dates == [d] else "FAIL", ",".join(orders_dates), d, str(orders_path))
    _append(rows, "orders_exec_contract", str(p0_contract.get("status") or "UNKNOWN"), p0_contract.get("summary", {}), "PASS", str(LOG_DIR / f"p0_orders_exec_contract_{d}.json"))
    _append(rows, "orders_vs_fills_qty", qty_cmp["status"], {"orders": orders_qty, "fills": fills_qty, "diffs": qty_cmp["diffs"]}, "same qty by side+code", "orders_exec.xlsx vs fills.csv")
    _append(rows, "paper_order_validation", str(order_validation.get("status") or "UNKNOWN"), order_validation.get("component_status", {}), "PASS", str(LOG_DIR / "paper_order_validation_report_latest.json"))
    _append(rows, "execution_safety_contract", str(execution_safety.get("status") or "UNKNOWN"), execution_safety.get("summary", {}), "PASS", str(LOG_DIR / "execution_safety_contract_latest.json"))
    _append(rows, "kis_api_sync", "PASS" if str(kis_sync.get("api_soft_fail", "")).lower() == "false" else "FAIL", {"rows_normalized": kis_sync.get("rows_normalized"), "mock_resolved": kis_sync.get("mock_resolved")}, "api_soft_fail=false", str(LOG_DIR / f"kis_fills_sync_{d}.json"))
    _append(rows, "broker_fill_match_report", "PASS" if (fills_match.get("checks") or {}).get("exec_date_eq_D") and (fills_match.get("checks") or {}).get("side_mismatch_zero") and (fills_match.get("checks") or {}).get("overfill_zero") else "FAIL", fills_match.get("counts", {}), "date/side/overfill checks pass", str(LOG_DIR / f"fills_match_report_{d}.json"))
    _append(rows, "canonical_fills_shadow", str(canonical.get("status") or "UNKNOWN"), {"input_rows_for_D": canonical.get("input_rows_for_D"), "conflicts": canonical.get("conflicts")}, "PASS", str(LOG_DIR / "canonical_fills_shadow_latest.json"))
    _append(rows, "fills_to_trades", "PASS" if len(sell_fills_d) == len(trades_d) == len(trades_calc_d) else "FAIL", {"sell_fills_D": len(sell_fills_d), "trades_exit_D": len(trades_d), "trades_calc_exit_D": len(trades_calc_d)}, "SELL fills D == trades exit D == trades_calc exit D", "fills.csv / trades.csv / trades_calc.csv")
    _append(rows, "paper_state_reconcile", str(reconcile.get("status") or "UNKNOWN"), {"expected": reconcile.get("expected_open_positions"), "actual": reconcile.get("actual_open_positions"), "issues": reconcile.get("issues")}, "PASS", str(LOG_DIR / "reconcile_paper_state_from_fills_latest.json"))
    _append(rows, "rootb_live_to_ledger", "PASS" if not missing_ledger_ids and str(ledger_dry.get("status") or "") == "PASS" else "FAIL", {"live_rows_D": len(live_d), "ledger_rows_D": len(ledger_d), "missing_fill_ids": missing_ledger_ids, "dry_run_status": ledger_dry.get("status")}, "all D live fill_ids covered by ledger", str(LOG_DIR / "ledger_live_fills_dry_run_latest.json"))
    _append(rows, "rootb_sync_from_roota", str(rootb_sync.get("status") or "UNKNOWN"), {"roota_rows_D": rootb_sync.get("roota_rows_D"), "live_rows_D_after": rootb_sync.get("live_rows_D_after"), "ledger_rows_D_after": rootb_sync.get("ledger_rows_D_after")}, "APPLIED or PASS", str(LOG_DIR / "rootb_fills_from_roota_latest.json"))
    _append(rows, "stats_pnl_asof", "PASS" if str(pnl.get("as_of_ymd") or "") == d else "FAIL", {"as_of_ymd": pnl.get("as_of_ymd"), "trades_used": pnl.get("trades_used"), "rows_as_of": pnl.get("rows_as_of")}, d, str(LOG_DIR / "paper_pnl_summary_last.json"))
    _append(rows, "ssot_health_card", str((ssot.get("overall") or {}).get("status") or "UNKNOWN"), ssot.get("overall", {}), "PASS", str(LOG_DIR / "ssot_health_card_latest.json"))

    statuses = {str(r["status"]).upper() for r in rows}
    hard_fail = any(str(r["status"]).upper() in {"FAIL", "ERROR"} for r in rows)
    unknown = any(str(r["status"]).upper() in {"UNKNOWN", ""} for r in rows)
    status = "PASS" if not hard_fail and not unknown else ("FAIL" if hard_fail else "PARTIAL")

    broker_mode = "MOCK_OR_PAPER_ONLY"
    if _to_float(kis_sync.get("rows_normalized"), 0.0) > 0:
        broker_mode = "BROKER_FILLS_PRESENT"
    elif str(kis_sync.get("mock_resolved") or "").strip():
        broker_mode = str(kis_sync.get("mock_resolved"))

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z"),
        "schema_version": "order_fill_e2e_chain_report_v1",
        "scope": "read_only_orders_fills_ledger_stats_chain",
        "status": status,
        "D": d,
        "policy_change": False,
        "trading_effect": False,
        "broker_mode": broker_mode,
        "summary": {
            "orders_rows": int(len(orders)),
            "fills_rows_D": int(len(fills_d)),
            "fills_side_counts_D": fills_d.get("side", pd.Series(dtype=str)).astype(str).str.upper().value_counts().to_dict() if not fills_d.empty else {},
            "orders_qty_by_side_code": orders_qty,
            "fills_qty_by_side_code": fills_qty,
            "qty_compare_status": qty_cmp["status"],
            "trades_exit_rows_D": int(len(trades_d)),
            "trades_calc_exit_rows_D": int(len(trades_calc_d)),
            "rootb_live_rows_D": int(len(live_d)),
            "rootb_ledger_rows_D": int(len(ledger_d)),
            "rootb_missing_ledger_fill_ids": missing_ledger_ids,
        },
        "sources": {
            "orders_exec": str(orders_path),
            "fills": str(PAPER / "fills.csv"),
            "trades": str(PAPER / "trades.csv"),
            "trades_calc": str(PAPER / "trades_calc.csv"),
            "rootb_live": str(ROOTB / "data" / "live" / "live_fills.csv"),
            "rootb_ledger": str(ROOTB / "data" / "ledger" / "paper_fills_ledger.csv"),
            "p0_orders_exec_contract": str(LOG_DIR / f"p0_orders_exec_contract_{d}.json"),
            "paper_order_validation": str(LOG_DIR / "paper_order_validation_report_latest.json"),
            "execution_safety_contract": str(LOG_DIR / "execution_safety_contract_latest.json"),
            "fills_match_report": str(LOG_DIR / f"fills_match_report_{d}.json"),
            "kis_fills_sync": str(LOG_DIR / f"kis_fills_sync_{d}.json"),
            "canonical_fills_shadow": str(LOG_DIR / "canonical_fills_shadow_latest.json"),
            "reconcile_paper_state": str(LOG_DIR / "reconcile_paper_state_from_fills_latest.json"),
            "ssot_health_card": str(LOG_DIR / "ssot_health_card_latest.json"),
        },
        "rows": rows,
    }


def main() -> int:
    payload = build_report()
    out_json = LOG_DIR / "order_fill_e2e_chain_report_latest.json"
    out_csv = LOG_DIR / "order_fill_e2e_chain_report_latest.csv"
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["check", "status", "value", "expected", "evidence"])
        writer.writeheader()
        writer.writerows(payload["rows"])
    print(
        json.dumps(
            {
                "status": payload["status"],
                "D": payload["D"],
                "broker_mode": payload["broker_mode"],
                "summary": payload["summary"],
                "out_json": str(out_json),
                "out_csv": str(out_csv),
            },
            ensure_ascii=False,
        )
    )
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
