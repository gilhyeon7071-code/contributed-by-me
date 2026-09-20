"""Build read-only readiness contracts for surge EV micro-probe candidates.

This prepares sizing and risk envelopes for later paper-probe review. It does
not approve entries, route orders, or change production policy.
"""
from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

VERDICT_JSON = LOG_DIR / "surge_ev_conditional_verdict_latest.json"
BUCKET_CSV = LOG_DIR / "surge_ev_bucket_decomposition_latest.csv"
CAPITAL_JSON = LOG_DIR / "capital_operation_decision_report_latest.json"
OUT_JSON = LOG_DIR / "surge_ev_probe_readiness_latest.json"
OUT_CSV = LOG_DIR / "surge_ev_probe_readiness_latest.csv"

BASE_ACCOUNT_RISK_PCT = 0.0005
MAX_DAILY_PROBE_RISK_PCT = 0.0010
PROBE_STOP_LOSS_PCT = 0.012
PROBE_TIMEBOX_MIN = 15
MAX_DAILY_PROBE_SLOTS = 2
MIN_RULE_N = 5
MAX_RULE_STOP_RATE = 0.0


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except Exception:
        return default


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists() or path.stat().st_size <= 5:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        return list(csv.DictReader(fp))


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: List[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    if not fields:
        fields = ["rule"]
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def _capital_basis() -> Dict[str, float]:
    payload = _read_json(CAPITAL_JSON)
    account = payload.get("account_basis") or {}
    budget = payload.get("capital_budget") or {}
    runtime = payload.get("runtime_basis") or {}
    order_ctx = ((payload.get("order_budget_contract") or {}).get("context") or {})
    equity = _to_float(account.get("equity_est_krw"), _to_float(budget.get("capital_total_krw"), 0.0))
    runtime_scale = _to_float(runtime.get("position_size_multiplier"), _to_float((payload.get("decision") or {}).get("runtime_scale"), 0.0))
    stage_notional = _to_float(order_ctx.get("stage_notional_krw"), _to_float(budget.get("basic_per_symbol_krw"), 0.0))
    surge_total = _to_float(budget.get("surge_total_krw"), 0.0)
    return {
        "equity_est_krw": equity,
        "runtime_scale": runtime_scale,
        "stage_notional_krw": stage_notional,
        "surge_total_krw": surge_total,
        "account_daily_loss_pct": _to_float(account.get("account_daily_loss_pct"), 0.0),
        "account_daily_loss_limit_pct": _to_float(account.get("account_daily_loss_limit_pct"), 0.0),
    }


def _row_matches_rule(row: Dict[str, Any], rule: str) -> bool:
    if str(row.get("session_bucket") or "") != "REGULAR_PRIMARY_EVALUATED":
        return False
    if rule == "SCORE_RVOL_12_16_ALL":
        return (
            row.get("source") == "SCORE_RVOL_LOB_CONFIRMED_CLEAN"
            and row.get("change_bucket") == "12-16%"
        )
    if rule == "MICRO_PROBE_WATCH_ALL":
        return row.get("source") == "SURGE_BLOCKER_LOB_CONFIRMED_WATCH"
    if rule == "MICRO_PROBE_WATCH_12_16_ALL":
        return (
            row.get("source") == "SURGE_BLOCKER_LOB_CONFIRMED_WATCH"
            and row.get("change_bucket") == "12-16%"
        )
    if rule == "MICRO_PROBE_WATCH_SPREAD_LE40":
        return (
            row.get("source") == "SURGE_BLOCKER_LOB_CONFIRMED_WATCH"
            and _to_float(row.get("spread_bps")) <= 40.0
        )
    if rule == "WATCH_SCORE_RVOL_12_16_SPREAD_LE15_RVOL_3_4_2":
        return (
            row.get("source") == "SCORE_RVOL_LOB_CONFIRMED_CLEAN"
            and row.get("change_bucket") == "12-16%"
            and _to_float(row.get("spread_bps")) <= 15.0
            and 3.0 <= _to_float(row.get("rvol20")) <= 4.2
        )
    return False


def _readiness_row(rule_verdict: Dict[str, Any], bucket_rows: List[Dict[str, Any]], capital: Dict[str, float]) -> Dict[str, Any]:
    rule = str(rule_verdict.get("rule") or "")
    rows = [row for row in bucket_rows if _row_matches_rule(row, rule)]
    equity = capital["equity_est_krw"]
    runtime_scale = max(0.0, capital["runtime_scale"])
    base_risk_krw = equity * BASE_ACCOUNT_RISK_PCT * runtime_scale
    daily_risk_cap_krw = equity * MAX_DAILY_PROBE_RISK_PCT * runtime_scale
    stage_cap_krw = capital["stage_notional_krw"] * runtime_scale
    surge_slot_cap_krw = capital["surge_total_krw"] * 0.01 if capital["surge_total_krw"] > 0 else stage_cap_krw
    risk_notional_krw = base_risk_krw / PROBE_STOP_LOSS_PCT if PROBE_STOP_LOSS_PCT > 0 else 0.0
    proposed_notional_krw = max(0.0, min(risk_notional_krw, stage_cap_krw, surge_slot_cap_krw))
    estimated_loss_krw = proposed_notional_krw * PROBE_STOP_LOSS_PCT
    rule_n = int(_to_float(rule_verdict.get("n"), 0.0))
    stop_rate = _to_float(rule_verdict.get("stop_rate"), 1.0)
    avg_ret = _to_float(rule_verdict.get("avg_primary_ret_pct"))
    med_ret = _to_float(rule_verdict.get("median_primary_ret_pct"))
    sample_ready = bool(rule_n >= MIN_RULE_N and stop_rate <= MAX_RULE_STOP_RATE and avg_ret > 0.0 and med_ret > 0.0)
    capital_ready = bool(proposed_notional_krw > 0.0 and estimated_loss_krw <= daily_risk_cap_krw)
    readiness_status = "SHADOW_PAPER_PROBE_READY_NOT_ROUTED" if sample_ready and capital_ready else "NOT_READY"
    return {
        "rule": rule,
        "readiness_status": readiness_status,
        "sample_ready": sample_ready,
        "capital_ready": capital_ready,
        "n": rule_n,
        "avg_primary_ret_pct": round(avg_ret, 6),
        "median_primary_ret_pct": round(med_ret, 6),
        "stop_rate": round(stop_rate, 6),
        "positive_rows": int(_to_float(rule_verdict.get("positive_rows"), 0.0)),
        "candidate_codes": ",".join(sorted({str(row.get("code") or "") for row in rows})),
        "base_account_risk_pct": BASE_ACCOUNT_RISK_PCT,
        "runtime_scale": round(runtime_scale, 6),
        "max_daily_probe_slots": MAX_DAILY_PROBE_SLOTS,
        "max_daily_probe_risk_pct": MAX_DAILY_PROBE_RISK_PCT,
        "daily_risk_cap_krw": round(daily_risk_cap_krw, 2),
        "risk_per_probe_krw": round(base_risk_krw, 2),
        "stage_cap_krw": round(stage_cap_krw, 2),
        "surge_slot_cap_krw": round(surge_slot_cap_krw, 2),
        "proposed_probe_notional_krw": round(proposed_notional_krw, 2),
        "probe_stop_loss_pct": PROBE_STOP_LOSS_PCT,
        "estimated_loss_at_stop_krw": round(estimated_loss_krw, 2),
        "probe_timebox_min": PROBE_TIMEBOX_MIN,
        "requires_lob_ok": True,
        "requires_spread_bps_lte": 15.0 if "SPREAD_LE15" in rule else "",
        "requires_rvol20_min": 3.0 if "RVOL_3_4_2" in rule else "",
        "requires_rvol20_max": 4.2 if "RVOL_3_4_2" in rule else "",
        "same_day_reentry_allowed": False,
        "scale_up_allowed": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "entry_approval_changed": False,
        "policy_change": False,
        "trading_allowed": False,
        "research_only": True,
        "must_not_dispatch": True,
    }


def main() -> int:
    now = datetime.now().isoformat(timespec="seconds")
    verdict = _read_json(VERDICT_JSON)
    final_verdict = verdict.get("final_verdict") or {}
    watch_rules = set(final_verdict.get("watch_probe_expand_rules") or [])
    verdict_rows = [
        row for row in verdict.get("verdicts", [])
        if row.get("rule") in watch_rules and row.get("verdict") == "WATCH_PROBE_EXPAND"
    ]
    bucket_rows = _read_csv(BUCKET_CSV)
    capital = _capital_basis()
    rows = [_readiness_row(row, bucket_rows, capital) for row in verdict_rows]
    payload = {
        "ts": now,
        "status": "OK",
        "scope": "surge_ev_probe_readiness",
        "source_verdict_json": str(VERDICT_JSON),
        "source_bucket_csv": str(BUCKET_CSV),
        "source_capital_json": str(CAPITAL_JSON),
        "risk_contract": {
            "paper_order_route": False,
            "broker_order_route": False,
            "entry_approval_changed": False,
            "policy_change": False,
            "trading_allowed": False,
            "research_only": True,
            "must_not_dispatch": True,
            "purpose": "readiness sizing contract only; no order route",
        },
        "capital_basis": capital,
        "sizing_contract": {
            "base_account_risk_pct": BASE_ACCOUNT_RISK_PCT,
            "max_daily_probe_risk_pct": MAX_DAILY_PROBE_RISK_PCT,
            "probe_stop_loss_pct": PROBE_STOP_LOSS_PCT,
            "probe_timebox_min": PROBE_TIMEBOX_MIN,
            "max_daily_probe_slots": MAX_DAILY_PROBE_SLOTS,
        },
        "summary": {
            "watch_rules": sorted(watch_rules),
            "rows": len(rows),
            "ready_rows": sum(1 for row in rows if row.get("readiness_status") == "SHADOW_PAPER_PROBE_READY_NOT_ROUTED"),
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_allowed": False,
        },
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"status": "OK", "rows": len(rows), "ready_rows": payload["summary"]["ready_rows"], "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
