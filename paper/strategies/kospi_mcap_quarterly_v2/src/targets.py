"""D3 국면 · D6 선정 · D7 목표 포트폴리오 — 순수 계산. 파일을 읽거나 쓰지 않는다.

블록 출처: C5(국면·t+1 효력), C6(20% 상한 반복 재배분), C7(정수화·10% 실패 기준).
spec/D3_regime.md, spec/D6_selection.md, spec/D7_target_portfolio.md 참조.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Tuple

import pandas as pd


# ---------------- D3 국면 ----------------

def compute_regime(kospi200: pd.DataFrame, selection_date: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """선정일 종가 vs 최근 ma_window 개 종가 평균. close == ma 는 above. 효력은 다음 거래일."""
    window = int(cfg["ma_window"])
    reasons: List[str] = []
    x = kospi200.sort_values("date")
    if x.empty or str(x["date"].iloc[-1]) != selection_date:
        reasons.append(f"REGIME_INDEX_DATE:last={x['date'].iloc[-1] if len(x) else None} selection={selection_date}")
    if len(x) < window:
        reasons.append(f"REGIME_WINDOW_SHORT:{len(x)}<{window}")
    if reasons:
        return {"status": "STOP", "reasons": reasons}
    tail = x.tail(window)
    close = float(tail["close"].iloc[-1])
    ma = float(tail["close"].astype(float).mean())
    regime = "below" if close < ma else "above"
    exposure = float(cfg["exposure_below"] if regime == "below" else cfg["exposure_above"])
    return {"status": "OK", "reasons": [], "observation_date": selection_date, "close": close,
            "ma": ma, "ma_window": window, "regime": regime, "exposure": exposure,
            "effective_from": "next_trading_day"}


# ---------------- D6 선정 ----------------

def select_top(universe: pd.DataFrame, cfg: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """시가총액 내림차순, 코드 오름차순으로 상위 N. N 에 못 미치면 멈춘다(N 을 줄이지 않는다)."""
    n = int(cfg["n_names"])
    u = universe.copy()
    u["market_cap"] = pd.to_numeric(u["market_cap"], errors="coerce")
    valid = u[u["market_cap"].fillna(0) > 0]
    ranked = valid.sort_values(["market_cap", "code"], ascending=[False, True]).reset_index(drop=True)
    sel = ranked.head(n).copy()
    sel["rank"] = range(1, len(sel) + 1)
    reasons = [] if len(sel) == n else [f"SELECTION_SHORT:{len(sel)}<{n}"]
    return sel, {"status": "STOP" if reasons else "OK", "reasons": reasons, "n_names": n,
                 "selected": int(len(sel)), "invalid_mcap_excluded": int(len(u) - len(valid))}


# ---------------- D7 목표 포트폴리오 ----------------

def cap_weights(market_caps: List[float], cap: float, max_rounds: int = 100) -> Tuple[List[float], int]:
    """C6: 상한 넘는 종목을 cap 으로 고정하고 나머지에 시총 비율로 재배분, 반복. (가중치, 반복 횟수)."""
    total = sum(market_caps)
    w = [m / total for m in market_caps]
    fixed = [False] * len(w)
    rounds = 0
    while rounds < max_rounds:
        over = [i for i, wi in enumerate(w) if not fixed[i] and wi > cap]
        if not over:
            break
        rounds += 1
        for i in over:
            fixed[i] = True
        remaining = 1.0 - cap * sum(fixed)
        free_mcap = sum(m for m, f in zip(market_caps, fixed) if not f)
        w = [cap if f else remaining * m / free_mcap for m, f in zip(market_caps, fixed)]
    return w, rounds


def floor_qty(target_values: List[float], prices: List[float]) -> List[int]:
    """목표금액 / 가격 내림. 부동소수 오차로 100000/10 이 9999.999… 가 되어 한 주 덜 사는 것을 막는다(2026-09-17 발견)."""
    return [int(math.floor(v / p + 1e-9)) for v, p in zip(target_values, prices)]


def add_shares_c7(target_values: List[float], prices: List[float], start_qty: List[int], mcaps: List[float],
                  budget_remaining: float, per_name_cap_value: float) -> Tuple[List[int], List[int], float]:
    """C7: 1주 추가가 목표금액 오차를 줄이고 남은 예산·종목 상한을 지키는 종목에, 개선액 큰 순으로 1주씩.
    동률이면 시총 큰 순, 그다음 입력 순서(시총 내림·코드 오름으로 정렬돼 있어야 함). D7 과 E1 이 같이 쓴다."""
    qty = list(start_qty)
    added = [0] * len(qty)
    cost = 0.0
    while True:
        best = None
        for i, (v, p) in enumerate(zip(target_values, prices)):
            improvement = abs(v - qty[i] * p) - abs(v - (qty[i] + 1) * p)
            if improvement <= 0 or cost + p > budget_remaining or (qty[i] + 1) * p > per_name_cap_value:
                continue
            key = (improvement, mcaps[i], -i)
            if best is None or key > best[0]:
                best = (key, i)
        if best is None:
            break
        i = best[1]
        qty[i] += 1
        added[i] += 1
        cost += prices[i]
    return qty, added, cost


def build_target_portfolio(selection: pd.DataFrame, exposure: float, cfg: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """C6 상한 → 노출 곱 → C7 정수화. 가격은 선정일 종가(계획용). 집행 가격은 E 층에서 다시 정한다."""
    reasons: List[str] = []
    cap = float(cfg["weight_cap"])
    s = selection.sort_values(["market_cap", "code"], ascending=[False, True]).reset_index(drop=True).copy()
    if len(s) < int(cfg["min_names_for_cap"]):
        return s, {"status": "STOP", "reasons": [f"CAP_TOO_FEW_NAMES:{len(s)}"]}
    mcaps = [float(x) for x in s["market_cap"]]
    total = sum(mcaps)
    s["raw_weight"] = [m / total for m in mcaps]
    weights, rounds = cap_weights(mcaps, cap)
    s["basket_weight"] = weights
    wsum = sum(weights)
    if abs(wsum - 1.0) > float(cfg["cap_sum_tolerance"]):
        reasons.append(f"CAP_SUM_ERROR:{wsum!r}")
    if max(weights) > cap + float(cfg["cap_sum_tolerance"]):
        reasons.append(f"CAP_EXCEEDED:{max(weights)!r}")

    capital = float(cfg["strategy_capital_krw"])
    reserve = capital * float(cfg["cost_reserve_pct"])
    allocable = capital - reserve
    target_budget = allocable * exposure
    s["account_target_weight"] = s["basket_weight"] * exposure
    s["target_value"] = allocable * s["account_target_weight"]
    s["price"] = pd.to_numeric(s["close"], errors="coerce").astype(float)
    if (s["price"].fillna(0) <= 0).any():
        reasons.append("PRICE_INVALID")
        return s, {"status": "STOP", "reasons": reasons}

    floor = floor_qty(list(s["target_value"]), list(s["price"]))
    s["floor_qty"] = floor
    spent_floor = sum(q * p for q, p in zip(floor, s["price"]))
    per_name_cap_value = allocable * cap * exposure
    qty, added, add_cost = add_shares_c7(list(s["target_value"]), list(s["price"]), floor, mcaps,
                                         target_budget - spent_floor, per_name_cap_value)
    spent = spent_floor + add_cost

    s["added_qty"] = added
    s["target_qty"] = qty
    s["actual_value"] = s["target_qty"] * s["price"]
    s["actual_weight"] = s["actual_value"] / allocable
    s["value_error"] = s["actual_value"] - s["target_value"]

    zero_w = float(s.loc[s["target_qty"] == 0, "account_target_weight"].sum())
    residual = target_budget - spent
    drift = residual / target_budget if target_budget > 0 else 0.0
    fail = float(cfg["integerization_fail_pct"])
    if zero_w >= fail:
        reasons.append(f"FAIL_INTEGERIZATION:zero_qty_target_weight={zero_w:.4f}>={fail}")
    if drift >= fail:
        reasons.append(f"FAIL_INTEGERIZATION:cash_drift={drift:.4f}>={fail}")
    if spent > target_budget + 1e-6:
        reasons.append(f"BUDGET_EXCEEDED:{spent}>{target_budget}")
    if (s["actual_weight"] > cap * exposure + 1e-12).any():
        reasons.append("ACCOUNT_WEIGHT_CAP_EXCEEDED")

    summary = {
        "status": "STOP" if reasons else "OK", "reasons": reasons,
        "names": int(len(s)), "cap_rounds": rounds, "capped_names": int(sum(w >= cap - 1e-12 for w in weights)),
        "basket_weight_sum": wsum, "exposure": exposure,
        "strategy_capital": capital, "cost_reserve": reserve, "allocable": allocable,
        "target_budget": target_budget, "intended_cash_by_exposure": allocable - target_budget,
        "invested": float(spent), "integerization_cash": float(residual), "integerization_cash_drift": drift,
        "zero_qty_names": int((s["target_qty"] == 0).sum()), "zero_qty_target_weight": zero_w,
        "floor_qty_total": int(sum(s["floor_qty"])), "added_qty_total": int(sum(added)),
        "abs_value_error_sum": float(s["value_error"].abs().sum()),
    }
    return s, summary
