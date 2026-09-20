"""E1 주문 생성 — 목표 포트폴리오(비중은 선정일 고정)를 집행 시점 호가로 주문 수량으로 바꾼다. 순수 계산.

블록 출처: C9 A(시장성 지정가: 매수 매도호가1 올림 / 매도 매수호가1 내림, 호가 5초, 커버리지 10%),
          C9 B(매도 먼저, 확인된 현금 안에서만 매수, 같은 종목 매수·매도 동시 금지),
          C9 C(결정적 intent_id / order_id), C7(정수화, targets.add_shares_c7 공유),
          utils/krx_tick.py(유가증권 7단계 호가단위, 2026-09-08 거래소 거절 뒤 단일 출처).
보내기(발주)·체결은 하지 않는다. spec/E1_order_generation.md 참조.
"""
from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from paper.strategies.kospi_mcap_quarterly_v2.src.targets import add_shares_c7, floor_qty
from utils.krx_tick import round_tick


def target_version(target: pd.DataFrame) -> str:
    """목표 포트폴리오의 종목·바스켓 비중으로 만든 짧은 해시. 목표가 바뀌면 주문 ID 도 바뀐다."""
    rows = target.sort_values("code")[["code", "basket_weight"]]
    payload = "\n".join(f"{c}:{w:.12f}" for c, w in zip(rows["code"], rows["basket_weight"]))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def check_quotes(codes: List[str], quotes: Dict[str, Dict[str, Any]], now: datetime, max_age: float
                 ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str]]:
    """쓸 수 있는 호가와, 못 쓰는 종목별 사유."""
    ok: Dict[str, Dict[str, Any]] = {}
    bad: Dict[str, str] = {}
    for code in codes:
        q = quotes.get(code)
        if not q:
            bad[code] = "QUOTE_MISSING"
            continue
        try:
            age = (now - q["quote_ts"]).total_seconds()
        except Exception:
            bad[code] = "QUOTE_TS_INVALID"
            continue
        if age > max_age or age < -1:
            bad[code] = f"QUOTE_STALE:{age:.1f}s"
            continue
        if not all(float(q.get(k) or 0) > 0 for k in ("ask1", "bid1", "ask1_qty", "bid1_qty")):
            bad[code] = "QUOTE_NON_POSITIVE"
            continue
        ok[code] = q
    return ok, bad


def _order(cfg, rebalance_id, selection_date, execution_date, tv, code, side, qty, limit_price, quote, reason):
    intent_id = f"{rebalance_id}|{code}|{side}|{tv}"  # rebalance_id 가 이미 strategy_id 를 담고 있다
    return {"strategy_id": cfg["strategy_id"], "rebalance_id": rebalance_id, "selection_date": selection_date,
            "execution_date": execution_date, "target_version": tv, "intent_id": intent_id, "attempt_no": 1,
            "order_id": f"{intent_id}|1", "code": code, "side": side, "order_type": "LIMIT", "time_in_force": "DAY",
            "limit_price": int(limit_price), "requested_qty": int(qty), "notional": int(limit_price) * int(qty),
            "quote_ts": quote["quote_ts"].isoformat(timespec="seconds"), "ask1": quote["ask1"], "bid1": quote["bid1"],
            "reason": reason}


def plan_orders(
    target: pd.DataFrame,
    *,
    selection_date: str,
    execution_date: str,
    exposure: float,
    strategy_equity: float,
    holdings: Dict[str, int],
    cash_available: float,
    quotes: Dict[str, Dict[str, Any]],
    now: datetime,
    cfg: Dict[str, Any],
    phase: str,
    event_tag: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """phase='SELL' 이면 매도 주문만, 'BUY' 면 (매도 체결 확인 뒤의 보유·현금으로) 매수 주문만 만든다.
    event_tag: 같은 선정 바구니의 노출 변경(scale_target.scale_event_tag). 없으면 분기 재구성 ID 그대로."""
    reasons: List[str] = []
    rebalance_id = f"{cfg['strategy_id']}|{selection_date}" + (f"|{event_tag}" if event_tag else "")
    # [2026-09-19] 노출은 목표 파일(account_target_weight 합)과 summary(exposure) 두 곳에서 온다.
    #   짝이 틀리면 수량은 한 노출로, 예산·상한은 다른 노출로 계산된다 -> 멈춘다(독립 검토 8번).
    weight_sum = float(target["account_target_weight"].sum())
    if abs(weight_sum - float(exposure)) > 1e-6:
        return [], {"status": "STOP", "phase": phase,
                    "reasons": [f"TARGET_EXPOSURE_MISMATCH:weights={weight_sum:.6f}:exposure={float(exposure):.6f}"]}
    tv = target_version(target)
    t = target.sort_values(["market_cap", "code"], ascending=[False, True]).reset_index(drop=True)
    target_codes = list(t["code"])
    all_codes = sorted(set(target_codes) | {c for c, q in holdings.items() if q > 0})

    usable, bad = check_quotes(all_codes, quotes, now, float(cfg["quote_max_age_seconds"]))
    bad_target_weight = float(t.loc[t["code"].isin(bad.keys()), "account_target_weight"].sum())
    if bad_target_weight >= float(cfg["execution_coverage_fail_pct"]):
        return [], {"status": "STOP", "reasons": [f"FAIL_EXECUTION_COVERAGE:{bad_target_weight:.4f}"],
                    "unorderable": bad, "phase": phase}

    cap = float(cfg["weight_cap"])
    allocable = strategy_equity * (1 - float(cfg["cost_reserve_pct"]))
    t["target_value"] = allocable * t["account_target_weight"]
    t["orderable"] = t["code"].isin(usable.keys())
    t["buy_price"] = [round_tick(float(usable[c]["ask1"]), up=True) if c in usable else None for c in t["code"]]

    # 목표 수량: 호가 가능한 종목만, 매수 지정가(매도호가1 올림) 기준 C7 (예산은 전체 목표 예산)
    ok_t = t[t["orderable"]].reset_index(drop=True)
    prices = [float(p) for p in ok_t["buy_price"]]
    floor = floor_qty(list(ok_t["target_value"]), prices)
    budget = allocable * exposure
    spent_floor = sum(q * p for q, p in zip(floor, prices))
    tq, _, _ = add_shares_c7(list(ok_t["target_value"]), prices, floor, [float(m) for m in ok_t["market_cap"]],
                             budget - spent_floor, allocable * cap * exposure)
    desired = dict(zip(ok_t["code"], tq))

    orders: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    if phase == "SELL":
        for code in all_codes:
            h = int(holdings.get(code, 0))
            if h <= 0:
                continue
            if code not in usable:
                skipped.append({"code": code, "side": "SELL", "reason": bad.get(code)})
                continue
            want = desired.get(code, 0) if code in target_codes else 0
            if code in target_codes and code not in desired:  # 목표 종목인데 호가 불가 -> 건드리지 않음
                continue
            sell = h - want
            if sell > 0:
                q = usable[code]
                orders.append(_order(cfg, rebalance_id, selection_date, execution_date, tv, code, "SELL", sell,
                                     round_tick(float(q["bid1"]), up=False), q,
                                     "NOT_IN_TARGET" if code not in target_codes else "ABOVE_TARGET"))
    elif phase == "BUY":
        buys = []
        for code in ok_t["code"]:
            add = desired[code] - int(holdings.get(code, 0))
            if add > 0:
                buys.append((code, add))
        est = sum(add * round_tick(float(usable[c]["ask1"]), up=True) for c, add in buys) * (1 + float(cfg["buy_cost_estimate_pct"]))
        if est > cash_available:
            # C9: 체결 뒤 현금에 맞춰 수량을 조용히 줄이지 않는다
            return [], {"status": "STOP", "reasons": [f"BUY_CASH_SHORT:need={est:.0f}>available={cash_available:.0f}"],
                        "phase": phase}
        for code, add in buys:
            q = usable[code]
            orders.append(_order(cfg, rebalance_id, selection_date, execution_date, tv, code, "BUY", add,
                                 round_tick(float(q["ask1"]), up=True), q, "BELOW_TARGET"))
    else:
        raise ValueError(f"phase must be SELL or BUY: {phase}")

    # C9 B1 사전 차단
    sides: Dict[str, set] = {}
    for o in orders:
        sides.setdefault(o["code"], set()).add(o["side"])
        if o["requested_qty"] <= 0:
            reasons.append(f"ORDER_QTY_NON_POSITIVE:{o['code']}")
        if o["side"] == "SELL" and o["requested_qty"] > int(holdings.get(o["code"], 0)):
            reasons.append(f"SELL_EXCEEDS_HOLDING:{o['code']}")
    if any(len(v) > 1 for v in sides.values()):
        reasons.append("BUY_SELL_SAME_CODE")
    if len({o["order_id"] for o in orders}) != len(orders):
        reasons.append("DUPLICATE_ORDER_ID")

    summary = {
        "status": "STOP" if reasons else "OK", "reasons": reasons, "phase": phase,
        "rebalance_id": rebalance_id, "target_version": tv, "orders": len(orders),
        "notional": int(sum(o["notional"] for o in orders)),
        "unorderable": bad, "execution_shortfall_weight": bad_target_weight, "skipped": skipped,
        "desired_invested_at_buy_price": float(sum(desired[c] * p for c, p in zip(ok_t["code"], prices))),
        "budget": budget, "cash_available": cash_available,
    }
    return (orders if not reasons else []), summary
