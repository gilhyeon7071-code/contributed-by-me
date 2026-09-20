from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import paper_engine as pe  # noqa: E402
# [2026-09-13] The 2026-08-07 package split stopped re-exporting these from
#   paper_engine/__init__; every reference below raised AttributeError at import
#   or first call. Import from the module that defines them instead.
from paper_engine.guards import _detect_explicit_market_events
from paper_engine.io import MARKET_EVENT_GUARD_STATUS_PATH, T2_SETTLEMENT_CASH_STATUS_PATH, _write_market_event_guard_status
from paper_engine.settlement import _initialize_t2_cash_state, _t2_apply_buy_budget, _write_t2_settlement_status


LOG_DIR = ROOT / "2_Logs"
BACKTEST_VALIDATION = LOG_DIR / "backtest_validation_latest.json"
OUT_JSON = LOG_DIR / "t2_market_event_deferred_validation_latest.json"
DEFERRED_JSON = LOG_DIR / "backtest_deferred_validation_status_latest.json"


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _deferred_gates(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for gate in doc.get("gate_results") or []:
        if not isinstance(gate, dict):
            continue
        details = gate.get("details") if isinstance(gate.get("details"), dict) else {}
        summary = str(gate.get("summary") or "")
        deferred = bool(details.get("deferred")) or bool(details.get("skipped")) or "deferred" in summary.lower() or "insufficient" in summary.lower()
        if not deferred:
            continue
        out.append({
            "name": str(gate.get("name") or ""),
            "passed": bool(gate.get("passed", False)),
            "summary": summary,
            "details": details,
            "closure_rule": "collect required operating observations and rerun official validation; do not treat code presence as PASS",
        })
    return out


def build_deferred_status() -> Dict[str, Any]:
    doc = _read_json(BACKTEST_VALIDATION)
    gates = _deferred_gates(doc)
    status = "PASS" if BACKTEST_VALIDATION.exists() else "FAIL"
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "source_path": str(BACKTEST_VALIDATION),
        "source_exists": BACKTEST_VALIDATION.exists(),
        "source_passed": doc.get("passed") if doc else None,
        "deferred_count": len(gates),
        "deferred_gates": gates,
        "policy_effect": False,
        "trading_effect": False,
    }
    DEFERRED_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def validate_t2_cash() -> Dict[str, Any]:
    cfg = {
        "capital_total": 1_000_000,
        "t2_settlement_cash": {
            "enabled": True,
            "settlement_lag_business_days": 2,
            "auto_initialize_from_capital": True,
            "ledger_max_rows": 50,
        },
    }
    state = {
        "open_positions": [{"code": "000001", "qty": 2, "entry_price": 100_000}],
        "processed_signals": [],
        "next_trade_seq": 1,
    }
    init = _initialize_t2_cash_state(state, cfg, "20260602")
    block_qty, block_check = _t2_apply_buy_budget(
        state,
        cfg,
        code="000002",
        entry_day="20260602",
        entry_price=900_000,
        qty=1,
        cost_buffer=1.01,
        min_qty=1,
    )
    reduce_qty, reduce_check = _t2_apply_buy_budget(
        state,
        cfg,
        code="000003",
        entry_day="20260602",
        entry_price=200_000,
        qty=5,
        cost_buffer=1.0,
        min_qty=1,
    )
    _write_t2_settlement_status(state, cfg, "20260602", [block_check, reduce_check])
    return {
        "status": "PASS" if block_qty == 0 and reduce_qty == 4 else "FAIL",
        "init": init,
        "block_qty": block_qty,
        "block_check": block_check,
        "reduce_qty": reduce_qty,
        "reduce_check": reduce_check,
    }


def validate_market_event_guard() -> Dict[str, Any]:
    event_doc = {
        "as_of_ymd": "20260602",
        "events": [
            {"date": "20260602", "code": "005930", "event_type": "VI", "status": "ACTIVE"},
            {"date": "20260602", "event_type": "CIRCUIT_BREAKER", "scope": "MARKET"},
        ],
    }
    cfg = {
        "explicit_market_event_guard": {
            "enabled": True,
            "block_event_types": ["VI", "TRADING_HALT", "CIRCUIT_BREAKER", "CB"],
            "market_wide_event_types": ["CIRCUIT_BREAKER", "CB"],
        }
    }
    import pandas as pd

    cdf = pd.DataFrame([{"code": "005930"}, {"code": "000660"}])
    result = _detect_explicit_market_events(event_doc, cdf, cfg, "20260602")
    _write_market_event_guard_status({
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "runtime_ymd": "20260602",
        "status": "BLOCK" if result.get("blocked") else "PASS",
        "policy_effect": True,
        "trading_effect": bool(result.get("blocked")),
        "source_path": "synthetic_validation",
        "market_wide": bool(result.get("market_wide")),
        "blocked_codes": result.get("blocked_codes", []),
        "events": result.get("events", []),
    })
    return {
        "status": "PASS" if result.get("market_wide") and result.get("blocked") else "FAIL",
        "result": result,
    }


def main() -> int:
    deferred = build_deferred_status()
    t2 = validate_t2_cash()
    market = validate_market_event_guard()
    checks = {
        "t2_cash": t2,
        "market_event_guard": market,
        "deferred_validation_status": {
            "status": "PASS" if deferred.get("source_exists") else "FAIL",
            "deferred_count": deferred.get("deferred_count"),
            "source_passed": deferred.get("source_passed"),
        },
    }
    status = "PASS" if all(str(v.get("status")) == "PASS" for v in checks.values()) else "FAIL"
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "checks": checks,
        "artifacts": {
            "deferred_validation_status": str(DEFERRED_JSON),
            "validation_latest": str(OUT_JSON),
            "t2_settlement_cash_status": str(T2_SETTLEMENT_CASH_STATUS_PATH),
            "market_event_guard_status": str(MARKET_EVENT_GUARD_STATUS_PATH),
        },
        "policy_effect": False,
        "trading_effect": False,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": status, "out": str(OUT_JSON), "deferred": str(DEFERRED_JSON)}, ensure_ascii=False))
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
