"""T2 settlement cash helpers for paper_engine.

Split out from the legacy ``paper_engine.py`` as part of the settlement module
refactor (Phase 3).
"""

from __future__ import annotations


__all__ = [
    '_merge_last_t2_state_fields',
    '_set_last_t2_initialized_state',
    '_add_business_days_weekend_only',
    '_normalize_pending_settlement_rows',
    '_initialize_t2_cash_state',
    '_t2_apply_buy_budget',
    '_t2_record_buy_cash',
    '_t2_record_sell_pending',
    '_write_t2_settlement_status',
]

import json
import math
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from paper_engine.common import _norm_ymd_text, _to_float, now_ts, _position_cost_basis, _get_dict, _get_list
from paper_engine.io import T2_SETTLEMENT_CASH_STATUS_PATH, _json_safe
from utils.common import norm_code, now_ymd


_LAST_T2_INITIALIZED_STATE: Dict[str, Any] = {}


def _merge_last_t2_state_fields(target_state: Dict[str, Any]) -> Dict[str, Any]:
    """Carry T2 cash fields computed during candidate loading into later runtime state."""
    if isinstance(_LAST_T2_INITIALIZED_STATE, dict) and _LAST_T2_INITIALIZED_STATE:
        for key in ("settled_cash", "pending_settlement", "cash_ledger", "settlement_cash_meta"):
            if key in _LAST_T2_INITIALIZED_STATE:
                target_state[key] = _LAST_T2_INITIALIZED_STATE.get(key)
    return target_state


def _set_last_t2_initialized_state(state: Optional[Dict[str, Any]]) -> None:
    global _LAST_T2_INITIALIZED_STATE
    _LAST_T2_INITIALIZED_STATE = dict(state) if state else {}



def _add_business_days_weekend_only(ymd: Any, days: int) -> str:
    start_text = _norm_ymd_text(ymd)
    if not start_text:
        return ""
    try:
        cur = datetime.strptime(start_text, "%Y%m%d")
    except Exception:
        return ""
    remaining = max(0, int(days or 0))
    while remaining > 0:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            remaining -= 1
    return cur.strftime("%Y%m%d")


def _normalize_pending_settlement_rows(rows: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            continue
        amount = float(_to_float(row.get("amount"), 0.0) or 0.0)
        if amount <= 0:
            continue
        trade_date = _norm_ymd_text(row.get("trade_date"))
        settle_date = _norm_ymd_text(row.get("settle_date"))
        if not settle_date:
            settle_date = _add_business_days_weekend_only(trade_date, 2)
        out.append({
            "trade_date": trade_date,
            "settle_date": settle_date,
            "amount": float(amount),
            "source": str(row.get("source") or ""),
            "order_id": str(row.get("order_id") or ""),
            "code": norm_code(row.get("code", "")),
        })
    return out


def _initialize_t2_cash_state(state: Dict[str, Any], cfg: Dict[str, Any], runtime_ymd: str) -> Dict[str, Any]:
    policy = cfg.get("t2_settlement_cash", {}) if isinstance(cfg.get("t2_settlement_cash"), dict) else {}
    if not bool(policy.get("enabled", True)):
        return {"enabled": False, "state": state, "released_amount": 0.0, "released_rows": 0}
    pending = _normalize_pending_settlement_rows(state.get("pending_settlement"))
    due_rows: List[Dict[str, Any]] = []
    keep_rows: List[Dict[str, Any]] = []
    runtime_text = _norm_ymd_text(runtime_ymd) or now_ymd()
    for row in pending:
        settle_date = str(row.get("settle_date") or "")
        if settle_date and settle_date <= runtime_text:
            due_rows.append(row)
        else:
            keep_rows.append(row)
    released_amount = float(sum(float(row.get("amount") or 0.0) for row in due_rows))
    settled_raw = state.get("settled_cash")
    initialized = False
    if settled_raw is None and bool(policy.get("auto_initialize_from_capital", True)):
        capital_total = float(_to_float(cfg.get("capital_total"), 0.0) or 0.0)
        open_cost = sum(_position_cost_basis(pos) for pos in state.get("open_positions", []) if isinstance(pos, dict))
        pending_amount = float(sum(float(row.get("amount") or 0.0) for row in keep_rows))
        settled_raw = max(0.0, capital_total - open_cost - pending_amount)
        initialized = True
    settled_cash = float(_to_float(settled_raw, 0.0) or 0.0) + released_amount
    state["settled_cash"] = float(max(0.0, settled_cash))
    state["pending_settlement"] = keep_rows
    meta = _get_dict(state, "settlement_cash_meta")
    meta.update({
        "enabled": True,
        "last_runtime_ymd": runtime_text,
        "settlement_lag_business_days": int(policy.get("settlement_lag_business_days", 2) or 2),
        "weekend_only_calendar": bool(policy.get("weekend_only_calendar", True)),
        "auto_initialized_from_capital": bool(initialized),
        "released_amount": float(released_amount),
        "released_rows": int(len(due_rows)),
    })
    state["settlement_cash_meta"] = meta
    if due_rows:
        ledger = _get_list(state, "cash_ledger")
        for row in due_rows:
            ledger.append({
                "ts": now_ts(),
                "event": "SETTLEMENT_RELEASE",
                "runtime_ymd": runtime_text,
                "amount": float(row.get("amount") or 0.0),
                "settle_date": str(row.get("settle_date") or ""),
                "source": str(row.get("source") or ""),
                "order_id": str(row.get("order_id") or ""),
                "code": str(row.get("code") or ""),
            })
        max_rows = max(10, int(policy.get("ledger_max_rows", 500) or 500))
        state["cash_ledger"] = ledger[-max_rows:]
    return {
        "enabled": True,
        "state": state,
        "settled_cash": float(state.get("settled_cash") or 0.0),
        "pending_rows": int(len(keep_rows)),
        "pending_amount": float(sum(float(row.get("amount") or 0.0) for row in keep_rows)),
        "released_amount": float(released_amount),
        "released_rows": int(len(due_rows)),
        "auto_initialized": bool(initialized),
    }


def _t2_apply_buy_budget(
    state: Dict[str, Any],
    cfg: Dict[str, Any],
    *,
    code: str,
    entry_day: str,
    entry_price: float,
    qty: int,
    cost_buffer: float,
    min_qty: int,
) -> Tuple[int, Dict[str, Any]]:
    policy = cfg.get("t2_settlement_cash", {}) if isinstance(cfg.get("t2_settlement_cash"), dict) else {}
    if not bool(policy.get("enabled", True)):
        return int(qty), {"enabled": False, "action": "SKIP_DISABLED"}
    settled_cash = float(_to_float(state.get("settled_cash"), 0.0) or 0.0)
    unit_cost = max(0.0, float(entry_price) * max(1.0, float(cost_buffer or 1.0)))
    requested_qty = int(qty or 0)
    requested_cost = float(requested_qty) * unit_cost
    min_qty_i = max(1, int(min_qty or 1))
    if requested_qty <= 0 or unit_cost <= 0:
        return int(qty), {"enabled": True, "action": "NOOP", "reason": "non_positive_qty_or_price"}
    if requested_cost <= settled_cash + 1e-9:
        return requested_qty, {
            "enabled": True,
            "action": "ALLOW",
            "code": str(code),
            "entry_day": str(entry_day),
            "settled_cash_before": float(settled_cash),
            "requested_qty": int(requested_qty),
            "allowed_qty": int(requested_qty),
            "requested_cost": float(requested_cost),
        }
    qty_fit = int(math.floor(max(0.0, settled_cash) / unit_cost))
    if qty_fit >= min_qty_i:
        return int(qty_fit), {
            "enabled": True,
            "action": "REDUCE",
            "code": str(code),
            "entry_day": str(entry_day),
            "settled_cash_before": float(settled_cash),
            "requested_qty": int(requested_qty),
            "allowed_qty": int(qty_fit),
            "requested_cost": float(requested_cost),
            "allowed_cost": float(qty_fit * unit_cost),
        }
    return 0, {
        "enabled": True,
        "action": "BLOCK",
        "reason": "INSUFFICIENT_SETTLED_CASH",
        "code": str(code),
        "entry_day": str(entry_day),
        "settled_cash_before": float(settled_cash),
        "requested_qty": int(requested_qty),
        "requested_cost": float(requested_cost),
        "min_qty": int(min_qty_i),
        "qty_fit": int(qty_fit),
    }


def _t2_record_buy_cash(state: Dict[str, Any], cfg: Dict[str, Any], *, code: str, entry_day: str, order_id: str, amount: float) -> None:
    policy = _get_dict(cfg, "t2_settlement_cash")
    if not bool(policy.get("enabled", True)):
        return
    amount_f = max(0.0, float(amount or 0.0))
    state["settled_cash"] = max(0.0, float(_to_float(state.get("settled_cash"), 0.0) or 0.0) - amount_f)
    ledger = _get_list(state, "cash_ledger")
    ledger.append({
        "ts": now_ts(),
        "event": "BUY_CASH_DEBIT",
        "trade_date": _norm_ymd_text(entry_day),
        "code": norm_code(code),
        "order_id": str(order_id or ""),
        "amount": float(amount_f),
        "settled_cash_after": float(state.get("settled_cash") or 0.0),
    })
    state["cash_ledger"] = ledger[-max(10, int(policy.get("ledger_max_rows", 500) or 500)):]

    # [TELEGRAM NOTIFICATION HOOK]
    try:
        import subprocess
        import sys
        from pathlib import Path
        tools_dir = Path(__file__).parent / "tools"
        if tools_dir.joinpath("telegram_notifier.py").exists():
            subprocess.Popen([
                sys.executable, str(tools_dir / "telegram_notifier.py"),
                "--msg", f"[매수 진입] 종목코드: {norm_code(code)} / 금액: {amount_f:,.0f}원 / 주문번호: {order_id}",
                "--event", "buy"
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass


def _t2_record_sell_pending(state: Dict[str, Any], cfg: Dict[str, Any], *, code: str, trade_date: str, order_id: str, amount: float) -> None:
    policy = _get_dict(cfg, "t2_settlement_cash")
    if not bool(policy.get("enabled", True)):
        return
    amount_f = max(0.0, float(amount or 0.0))
    if amount_f <= 0:
        return
    lag = max(0, int(policy.get("settlement_lag_business_days", 2) or 2))
    row = {
        "trade_date": _norm_ymd_text(trade_date),
        "settle_date": _add_business_days_weekend_only(trade_date, lag),
        "amount": float(amount_f),
        "source": "SELL",
        "order_id": str(order_id or ""),
        "code": norm_code(code),
    }
    pending = _normalize_pending_settlement_rows(state.get("pending_settlement"))
    pending.append(row)
    state["pending_settlement"] = pending
    ledger = _get_list(state, "cash_ledger")
    ledger.append({
        "ts": now_ts(),
        "event": "SELL_PENDING_SETTLEMENT",
        **row,
    })
    state["cash_ledger"] = ledger[-max(10, int(policy.get("ledger_max_rows", 500) or 500)):]

    # [TELEGRAM NOTIFICATION HOOK]
    try:
        import subprocess
        import sys
        from pathlib import Path
        tools_dir = Path(__file__).parent / "tools"
        if tools_dir.joinpath("telegram_notifier.py").exists():
            subprocess.Popen([
                sys.executable, str(tools_dir / "telegram_notifier.py"),
                "--msg", f"[매도 청산] 종목코드: {norm_code(code)} / 금액: {amount_f:,.0f}원 / 주문번호: {order_id}",
                "--event", "sell"
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass


def _write_t2_settlement_status(state: Dict[str, Any], cfg: Dict[str, Any], runtime_ymd: str, checks: List[Dict[str, Any]]) -> Dict[str, Any]:
    pending = _normalize_pending_settlement_rows(state.get("pending_settlement"))
    payload: Dict[str, Any] = {
        "generated_at": now_ts(),
        "runtime_ymd": _norm_ymd_text(runtime_ymd) or now_ymd(),
        "status": "PASS",
        "policy_effect": bool((cfg.get("t2_settlement_cash") or {}).get("enabled", True)) if isinstance(cfg.get("t2_settlement_cash"), dict) else True,
        "trading_effect": any(str(row.get("action") or "").upper() in {"BLOCK", "REDUCE"} for row in checks),
        "settled_cash": float(_to_float(state.get("settled_cash"), 0.0) or 0.0),
        "pending_rows": int(len(pending)),
        "pending_amount": float(sum(float(row.get("amount") or 0.0) for row in pending)),
        "checks": checks[-50:],
        "meta": state.get("settlement_cash_meta") if isinstance(state.get("settlement_cash_meta"), dict) else {},
        "calendar_scope": "weekend_only_business_days",
    }
    T2_SETTLEMENT_CASH_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    T2_SETTLEMENT_CASH_STATUS_PATH.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
