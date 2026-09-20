from __future__ import annotations

import argparse
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

INPUT_CSV = LOG_DIR / "surge_probe_review_lob_quality_latest.csv"
ACTIVE_RESPONSE_CSV = LOG_DIR / "surge_active_response_layer_latest.csv"
LIVE_READINESS_CSV = LOG_DIR / "surge_live_readiness_audit_latest.csv"
CAPITAL_JSON = LOG_DIR / "capital_operation_decision_report_latest.json"
OUT_JSON = LOG_DIR / "surge_probe_sizing_review_latest.json"
OUT_CSV = LOG_DIR / "surge_probe_sizing_review_latest.csv"
OUT_COLUMNS = [
    "code",
    "name",
    "review_status",
    "ret_10m_pct",
    "outcome_status_10m",
    "lob_quality_class",
    "current_price",
    "spread_bps",
    "review_qty",
    "review_notional_krw",
    "paper_order_route",
    "broker_order_route",
    "dispatch_enabled",
    "entry_approval_changed",
    "policy_change",
    "trading_allowed",
    "research_only",
    "must_not_dispatch",
]

BASE_ACCOUNT_RISK_PCT = 0.0005
MAX_DAILY_PROBE_RISK_PCT = 0.0010
PROBE_STOP_LOSS_PCT = 0.012
PROBE_TIMEBOX_MIN = 15
MAX_DAILY_PROBE_SLOTS = 2
MIN_LOT_QTY = 1


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(str(value).replace(",", ""))
    except Exception:
        return default


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _code(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text.zfill(6)[-6:] if text else ""


def _paper_ready_review_rows(existing_codes: set[str]) -> pd.DataFrame:
    readiness = _read_csv(LIVE_READINESS_CSV)
    active = _read_csv(ACTIVE_RESPONSE_CSV)
    if readiness.empty or active.empty:
        return pd.DataFrame()
    readiness = readiness[readiness["paper_order_readiness"].astype(str).eq("PAPER_READY")].copy()
    if readiness.empty:
        return pd.DataFrame()
    readiness["_code"] = readiness["code"].map(_code)
    readiness = readiness[~readiness["_code"].isin(existing_codes)].copy()
    if readiness.empty:
        return pd.DataFrame()
    active["_code"] = active["code"].map(_code)
    active_by_code = active.drop_duplicates("_code", keep="first").set_index("_code")

    rows: list[dict[str, Any]] = []
    for _, ready_row in readiness.iterrows():
        code = str(ready_row.get("_code") or "")
        if not code or code not in active_by_code.index:
            continue
        active_row = active_by_code.loc[code]
        rows.append(
            {
                "code": code,
                "name": active_row.get("name", ""),
                "review_status_source": "live_readiness_paper_ready",
                "ret_10m_pct": "",
                "outcome_status_10m": "",
                "lob_quality_class": "LIVE_READINESS_PAPER_READY",
                "current_price": active_row.get("current_price", ""),
                "spread_bps": active_row.get("spread_bps", ""),
                "ask_depth_levels_live": active_row.get("ask_depth_levels", ""),
                "order_imbalance_l1": active_row.get("order_imbalance_l1", ""),
                "paper_ready_reason": ready_row.get("active_response_reason", ""),
                "entry_decision": ready_row.get("entry_decision", ""),
                "entry_reason": ready_row.get("entry_reason", ""),
                "exclude_reasons": ready_row.get("exclude_reasons", ""),
            }
        )
    return pd.DataFrame(rows)


def _capital_basis() -> dict[str, Any]:
    payload = _read_json(CAPITAL_JSON)
    account = payload.get("account_basis") or {}
    budget = payload.get("capital_budget") or {}
    runtime = payload.get("runtime_basis") or {}
    order_ctx = ((payload.get("order_budget_contract") or {}).get("context") or {})
    mtime = datetime.fromtimestamp(CAPITAL_JSON.stat().st_mtime).isoformat(timespec="seconds") if CAPITAL_JSON.exists() else ""
    today = datetime.now().strftime("%Y%m%d")
    return {
        "source_capital_json": str(CAPITAL_JSON),
        "capital_basis_mtime": mtime,
        "capital_basis_stale": today not in mtime.replace("-", ""),
        "equity_est_krw": _to_float(account.get("equity_est_krw"), _to_float(budget.get("capital_total_krw"), 0.0)),
        "runtime_scale": max(0.0, _to_float(runtime.get("position_size_multiplier"), _to_float((payload.get("decision") or {}).get("runtime_scale"), 0.0))),
        "stage_notional_krw": _to_float(order_ctx.get("stage_notional_krw"), _to_float(budget.get("basic_per_symbol_krw"), 0.0)),
        "surge_total_krw": _to_float(budget.get("surge_total_krw"), 0.0),
        "account_daily_loss_pct": _to_float(account.get("account_daily_loss_pct"), 0.0),
        "account_daily_loss_limit_pct": _to_float(account.get("account_daily_loss_limit_pct"), 0.0),
    }


def _qty_for(notional: float, price: float) -> int:
    if notional <= 0 or price <= 0:
        return 0
    return max(MIN_LOT_QTY, int(math.floor(notional / price)))


def _slippage_bps(spread_bps: float) -> float:
    return max(5.0, min(30.0, spread_bps / 2.0))


def _build_rows(input_csv: Path, capital: dict[str, Any]) -> pd.DataFrame:
    df = _read_csv(input_csv)
    work = pd.DataFrame()
    if not df.empty:
        work = df[df["lob_quality_class"].astype(str).eq("LOB_OK_REVIEWABLE")].copy()
    existing_codes = set(work["code"].map(_code)) if not work.empty and "code" in work.columns else set()
    paper_ready = _paper_ready_review_rows(existing_codes)
    if not paper_ready.empty:
        work = pd.concat([work, paper_ready], ignore_index=True, sort=False)
    if work.empty:
        return pd.DataFrame()

    equity = float(capital["equity_est_krw"])
    runtime_scale = float(capital["runtime_scale"])
    base_risk_krw = equity * BASE_ACCOUNT_RISK_PCT * runtime_scale
    daily_risk_cap_krw = equity * MAX_DAILY_PROBE_RISK_PCT * runtime_scale
    stage_cap_krw = float(capital["stage_notional_krw"]) * runtime_scale
    surge_slot_cap_krw = float(capital["surge_total_krw"]) * 0.01 if float(capital["surge_total_krw"]) > 0 else stage_cap_krw
    risk_notional_krw = base_risk_krw / PROBE_STOP_LOSS_PCT if PROBE_STOP_LOSS_PCT > 0 else 0.0
    proposed_cap_krw = max(0.0, min(risk_notional_krw, stage_cap_krw, surge_slot_cap_krw))

    rows = []
    for _, row in work.iterrows():
        price = _to_float(row.get("current_price"))
        qty = _qty_for(proposed_cap_krw, price)
        notional = qty * price
        est_loss = notional * PROBE_STOP_LOSS_PCT
        spread_bps = _to_float(row.get("spread_bps"))
        slip_bps = _slippage_bps(spread_bps)
        review_status = "SIZING_REVIEW_ONLY_NOT_ROUTED"
        if bool(capital.get("capital_basis_stale")):
            review_status = "SIZING_REVIEW_STALE_CAPITAL_BASIS"
        if qty <= 0 or est_loss > daily_risk_cap_krw:
            review_status = "SIZING_REVIEW_NOT_READY"
        rows.append(
            {
                "code": str(row.get("code") or "").zfill(6),
                "name": row.get("name", ""),
                "review_status": review_status,
                "ret_10m_pct": _to_float(row.get("ret_10m_pct")),
                "outcome_status_10m": row.get("outcome_status_10m", ""),
                "lob_quality_class": row.get("lob_quality_class", ""),
                "current_price": round(price, 2),
                "spread_bps": round(spread_bps, 6),
                "ask_depth_levels_live": _to_float(row.get("ask_depth_levels_live")),
                "order_imbalance_l1": round(_to_float(row.get("order_imbalance_l1")), 6),
                "slippage_bps_assumption": round(slip_bps, 6),
                "base_account_risk_pct": BASE_ACCOUNT_RISK_PCT,
                "runtime_scale": round(runtime_scale, 6),
                "risk_per_probe_krw": round(base_risk_krw, 2),
                "daily_risk_cap_krw": round(daily_risk_cap_krw, 2),
                "stage_cap_krw": round(stage_cap_krw, 2),
                "surge_slot_cap_krw": round(surge_slot_cap_krw, 2),
                "risk_notional_cap_krw": round(risk_notional_krw, 2),
                "proposed_probe_notional_cap_krw": round(proposed_cap_krw, 2),
                "review_qty": int(qty),
                "review_notional_krw": round(notional, 2),
                "probe_stop_loss_pct": PROBE_STOP_LOSS_PCT,
                "estimated_loss_at_stop_krw": round(est_loss, 2),
                "probe_timebox_min": PROBE_TIMEBOX_MIN,
                "max_daily_probe_slots": MAX_DAILY_PROBE_SLOTS,
                "capital_basis_stale": bool(capital.get("capital_basis_stale")),
                "capital_basis_mtime": capital.get("capital_basis_mtime", ""),
                "paper_order_route": False,
                "broker_order_route": False,
                "dispatch_enabled": False,
                "entry_approval_changed": False,
                "policy_change": False,
                "trading_allowed": False,
                "research_only": True,
                "must_not_dispatch": True,
            }
        )
    return pd.DataFrame(rows)


def _to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    return json.loads(df.where(pd.notna(df), None).to_json(orient="records", force_ascii=False))


def build_report(input_csv: Path) -> tuple[dict[str, Any], pd.DataFrame]:
    capital = _capital_basis()
    rows = _build_rows(input_csv, capital)
    status = "PASS" if not rows.empty else "NO_LOB_OK_REVIEWABLE_ROWS"
    payload = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "scope": "surge_probe_sizing_review_read_only",
        "input_csv": str(input_csv),
        "capital_basis": capital,
        "sizing_contract": {
            "base_account_risk_pct": BASE_ACCOUNT_RISK_PCT,
            "max_daily_probe_risk_pct": MAX_DAILY_PROBE_RISK_PCT,
            "probe_stop_loss_pct": PROBE_STOP_LOSS_PCT,
            "probe_timebox_min": PROBE_TIMEBOX_MIN,
            "max_daily_probe_slots": MAX_DAILY_PROBE_SLOTS,
            "min_lot_qty": MIN_LOT_QTY,
        },
        "summary": {
            "rows": int(len(rows)),
            "codes": int(rows["code"].nunique()) if not rows.empty else 0,
            "capital_basis_stale": bool(capital.get("capital_basis_stale")),
            "total_review_notional_krw": round(float(rows["review_notional_krw"].sum()), 2) if not rows.empty else 0.0,
            "total_estimated_loss_at_stop_krw": round(float(rows["estimated_loss_at_stop_krw"].sum()), 2) if not rows.empty else 0.0,
            "paper_order_route": False,
            "broker_order_route": False,
            "dispatch_enabled": False,
            "trading_allowed": False,
            "policy_change": False,
        },
        "rows": _to_records(rows),
        "risk_contract": {
            "paper_order_route": False,
            "broker_order_route": False,
            "dispatch_enabled": False,
            "entry_approval_changed": False,
            "policy_change": False,
            "trading_allowed": False,
            "research_only": True,
            "must_not_dispatch": True,
            "purpose": "read-only probe sizing review; no order route",
        },
    }
    return payload, rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", default=str(INPUT_CSV))
    parser.add_argument("--output-json", default=str(OUT_JSON))
    parser.add_argument("--output-csv", default=str(OUT_CSV))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    payload, rows = build_report(Path(args.input_csv))
    if args.dry_run:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        if not rows.empty:
            print(rows.to_string(index=False))
        return 0 if payload.get("status") == "PASS" else 2

    out_json = Path(args.output_json)
    out_csv = Path(args.output_csv)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if rows.empty:
        rows = pd.DataFrame(columns=OUT_COLUMNS)
    rows.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(
        "[SURGE_PROBE_SIZING_REVIEW] "
        f"status={payload.get('status')} rows={payload.get('summary', {}).get('rows', 0)} "
        f"json={out_json} csv={out_csv}"
    )
    return 0 if str(payload.get("status") or "").startswith("NO_") or payload.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
