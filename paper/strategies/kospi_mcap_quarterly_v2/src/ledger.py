"""E4 원장 · NAV · 영구 중단 래치 — 체결 로그에서 매번 처음부터 다시 계산한다(멱등). 순수 계산 + 래치 파일.

- 현금은 둘로 나눈다: 경제적 현금(체결 즉시 반영) / 결제 완료 현금(체결일 + 2 거래일). 2026-09-17
  topn 매도 뒤 예수금 총액만 본 계산이 낙폭 −96.9% 오탐을 냈다.
- 평가 가격이 하나라도 없으면 NAV 는 INCOMPLETE 이고 래치를 판정하지 않는다(C10: 완전한 NAV 에서만).
- 래치: NAV <= 전략 자본 × 0.75 (시스템 정의 −25%). 한 번 켜지면 파일로 남고 자동으로 풀리지 않는다.
spec/E4_ledger_nav.md 참조.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional


def settle_date(trade_date: str, next_trading_day: Callable[[str], str], days: int) -> str:
    d = trade_date
    for _ in range(days):
        d = next_trading_day(d)
    return d


def build_ledger(capital: float, fills: List[Dict[str, Any]], *, as_of: str, closes: Dict[str, float],
                 cfg: Dict[str, Any], next_trading_day: Callable[[str], str]) -> Dict[str, Any]:
    fee_pct, tax_pct = float(cfg["fee_pct"]), float(cfg["sell_tax_pct"])
    days = int(cfg["settlement_trading_days"])
    cash = float(capital)
    settled = float(capital)
    receivable = payable = fees = taxes = realized = 0.0
    pos: Dict[str, Dict[str, float]] = {}
    incidents: List[Dict[str, Any]] = []
    for f in sorted((x for x in fills if x["trade_date"] <= as_of), key=lambda x: (x["observed_at"], x["fill_id"])):
        amount, qty, code = float(f["amount"]), int(f["qty"]), f["code"]
        fee = amount * fee_pct
        p = pos.setdefault(code, {"qty": 0, "cost": 0.0})
        sd = settle_date(f["trade_date"], next_trading_day, days)
        if f["side"] == "BUY":
            flow = -(amount + fee)
            p["qty"] += qty
            p["cost"] += amount + fee
        else:
            tax = amount * tax_pct
            taxes += tax
            flow = amount - fee - tax
            if qty > p["qty"]:
                incidents.append({"type": "NEGATIVE_POSITION", "code": code, "detail": [p["qty"], qty]})
            avg = p["cost"] / p["qty"] if p["qty"] > 0 else 0.0
            realized += flow - avg * qty
            p["cost"] -= avg * qty
            p["qty"] -= qty
        fees += fee
        cash += flow
        if sd <= as_of:
            settled += flow
        elif flow > 0:
            receivable += flow
        else:
            payable += -flow

    positions = []
    missing = []
    market_value = 0.0
    for code, p in sorted(pos.items()):
        if p["qty"] == 0:
            continue
        px = closes.get(code)
        if px is None or float(px) <= 0:
            missing.append(code)
            value = None
        else:
            value = float(px) * p["qty"]
            market_value += value
        positions.append({"code": code, "qty": int(p["qty"]), "cost": p["cost"], "close": px, "value": value})
    status = "COMPLETE" if not missing and not incidents else "INCOMPLETE"
    nav = cash + market_value if status == "COMPLETE" else None
    return {"as_of": as_of, "status": status, "missing_prices": missing, "incidents": incidents,
            "capital": float(capital), "cash_economic": cash, "cash_settled": settled,
            "receivable": receivable, "payable": payable, "fees": fees, "taxes": taxes,
            "realized_pnl": realized, "market_value": market_value, "nav": nav,
            "nav_return": (nav / capital - 1) if nav is not None else None, "positions": positions}


def evaluate_latch(ledger: Dict[str, Any], cfg: Dict[str, Any], latch_path: Path) -> Dict[str, Any]:
    """이미 켜져 있으면 그대로(자동 해제 없음). 완전한 NAV 가 기준 이하이면 켜고 파일로 남긴다."""
    lp = Path(latch_path)
    if lp.exists():
        return {**json.loads(lp.read_text(encoding="utf-8")), "active": True, "new": False}
    if ledger["status"] != "COMPLETE":
        return {"active": False, "new": False, "reason": "NAV_INCOMPLETE_NOT_EVALUATED"}
    threshold = float(ledger["capital"]) * float(cfg["latch_nav_ratio"])
    if ledger["nav"] <= threshold:
        rec = {"triggered_as_of": ledger["as_of"], "nav": ledger["nav"], "threshold": threshold}
        lp.parent.mkdir(parents=True, exist_ok=True)
        lp.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
        return {**rec, "active": True, "new": True}
    return {"active": False, "new": False, "nav": ledger["nav"], "threshold": threshold}
