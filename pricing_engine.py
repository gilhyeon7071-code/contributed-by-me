from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional


@dataclass(frozen=True)
class CostProfile:
    fee_pct: float
    slippage_pct: float
    sell_tax_pct: float = 0.0


@dataclass(frozen=True)
class CostComputation:
    gross_return: float
    net_return: float
    roundtrip_fee: float
    roundtrip_slippage: float
    sell_tax_pct: float


def _cost_num(src: Dict[str, Any], key: str, default: float) -> float:
    """`or` 를 쓰지 않는다 - 0.0 은 유효한 설정값이다.

    [2026-09-10] `float(cfg.get("fee_pct", 0.005) or 0.005)` 였다.
      설정에 `fee_pct: 0.0` 이 **정확히** 들어 있는데(브로커 실측: 수수료 0)
      `0.0 or 0.005` 가 0.005 를 내놓아, 엔진이 왕복 0.400% 대신
      **1.400% 를 청구**하고 있었다. 3.5배다.
      그리고 optimize_params_v41_1._warn_if_fee_diverges_from_live() 는
      **설정 파일을 직접 읽으므로** 0.00400 을 보고 경고를 내지 않았다 -
      드리프트 감시가 실효값이 아니라 설정값을 본다.
    같은 함정: [[feedback_or_falsy_trap_pattern]] (2026-07-24 generate_candidates 10곳)
    """
    v = src.get(key, default)
    return float(default if v is None else v)


def build_cost_profile(cfg: Dict[str, Any], profile_name: Optional[str] = None) -> CostProfile:
    base = CostProfile(
        # [2026-09-10] 최후 기본값도 라이브 값으로. 0.005 / 0.0 은 중립적인 기본값이 아니라
        #   **옛 오류값**이었다(수수료 0.5% / 거래세 없음). 어떤 경로로 들어와도 0.400% 가 되게 한다.
        fee_pct=_cost_num(cfg, "fee_pct", 0.0),
        slippage_pct=_cost_num(cfg, "slippage_pct", 0.001),
        sell_tax_pct=_cost_num(cfg, "sell_tax_pct", 0.002),
    )
    if not profile_name:
        return base
    profiles = cfg.get("cost_profiles", {}) if isinstance(cfg, dict) else {}
    profile_obj = profiles.get(profile_name, {}) if isinstance(profiles, dict) else {}
    if not isinstance(profile_obj, dict):
        return base
    return CostProfile(
        fee_pct=_cost_num(profile_obj, "fee_pct", base.fee_pct),
        slippage_pct=_cost_num(profile_obj, "slippage_pct", base.slippage_pct),
        sell_tax_pct=_cost_num(profile_obj, "sell_tax_pct", base.sell_tax_pct),
    )


def compute_costs(
    entry_price: float,
    exit_price: float,
    qty: int,
    *,
    fee_pct: float,
    slippage_pct: float,
    sell_tax_pct: float = 0.0,
) -> CostComputation:
    if float(entry_price or 0.0) <= 0:
        return CostComputation(
            gross_return=0.0,
            net_return=0.0,
            roundtrip_fee=0.0,
            roundtrip_slippage=0.0,
            sell_tax_pct=float(sell_tax_pct or 0.0),
        )
    gross = (float(exit_price) - float(entry_price)) / float(entry_price)
    fee_cost = (float(entry_price) + float(exit_price)) * float(qty) * float(fee_pct)
    slippage_cost = (float(entry_price) + float(exit_price)) * float(qty) * float(slippage_pct)
    cost_rate = ((float(entry_price) + float(exit_price)) / float(entry_price)) * (
        float(fee_pct) + float(slippage_pct)
    )
    net = gross - cost_rate - float(sell_tax_pct or 0.0)
    if not math.isfinite(net):
        net = 0.0
    return CostComputation(
        gross_return=float(gross),
        net_return=float(net),
        roundtrip_fee=float(fee_cost),
        roundtrip_slippage=float(slippage_cost),
        sell_tax_pct=float(sell_tax_pct or 0.0),
    )


def _tier_num(ts: Dict[str, Any], key: str, default: float) -> float:
    """티어 값. `or` 를 쓰지 않는다 - 0.0 은 유효한 슬리피지다."""
    v = ts.get(key, default)
    return float(default if v is None else v)


def resolve_slippage_pct_tiered(market_cap: float, cfg: Dict[str, Any], default_slippage_pct: float) -> float:
    ts = cfg.get("tiered_slippage", {}) if isinstance(cfg, dict) else {}
    if not ts.get("enabled"):
        return float(default_slippage_pct)
    large = float(ts.get("large_cap_krw", 1_000_000_000_000))
    mid = float(ts.get("mid_cap_krw", 300_000_000_000))
    # [2026-09-10] 티어 값이 없으면 **평탄 슬리피지로 떨어진다.**
    #   종전 기본값 0.003/0.005/0.010 은 아무도 재지 않은 코드 상수였고,
    #   실제로 그 값이 켜진 채 쓰였다(BROKEN_WINDOW_REGISTER C8).
    #   나중에 실측치 0.00139/0.00178/0.00238 로 교체됐으니 mid 기준 2.8배 과대였다.
    #   안 잰 티어가 조용히 평탄값의 5~10배가 되면 안 된다.
    #   설정에 값이 있으면 그 값을 쓰고, 없으면 아는 값(평탄)으로 돌아간다.
    d = float(default_slippage_pct)
    if float(market_cap or 0.0) >= large:
        return float(_tier_num(ts, "large_slip_pct", d))
    if float(market_cap or 0.0) >= mid:
        return float(_tier_num(ts, "mid_slip_pct", d))
    return float(_tier_num(ts, "small_slip_pct", d))


def calc_dynamic_lob_slippage(
    qty: int,
    current_price: float,
    ask_book: Iterable[Dict[str, Any]],
    *,
    default_slippage_pct: float = 0.005,
    unfilled_penalty_pct: float = 0.01,
) -> float:
    """Estimate buy-side slippage by sweeping available ask depth."""
    try:
        qty_i = int(qty or 0)
        ref_price = float(current_price or 0.0)
    except Exception:
        return float(default_slippage_pct)
    if qty_i <= 0 or ref_price <= 0:
        return float(default_slippage_pct)

    levels = []
    for level in ask_book or []:
        if not isinstance(level, dict):
            continue
        try:
            price = float(level.get("price", 0.0) or 0.0)
            vol = float(level.get("vol", level.get("qty", 0.0)) or 0.0)
        except Exception:
            continue
        if price > 0 and vol > 0:
            levels.append({"price": price, "vol": vol})
    if not levels:
        return float(default_slippage_pct)

    remaining = float(qty_i)
    total_cost = 0.0
    for level in levels:
        if remaining <= 0:
            break
        fill_qty = min(remaining, float(level["vol"]))
        total_cost += fill_qty * float(level["price"])
        remaining -= fill_qty

    if remaining > 0:
        highest_ask = float(levels[-1]["price"])
        total_cost += remaining * highest_ask * (1.0 + max(0.0, float(unfilled_penalty_pct)))

    vwap_price = total_cost / float(qty_i)
    slippage_pct = max(0.0, (vwap_price - ref_price) / ref_price)
    if not math.isfinite(slippage_pct):
        return float(default_slippage_pct)
    return float(max(slippage_pct, 0.0))


def calc_qty_by_sizing(entry_price: float, cfg: Dict[str, Any], min_qty: int = 1) -> int:
    if float(entry_price or 0.0) <= 0:
        return 0
    mode = str(cfg.get("sizing_mode", "fixed_qty") or "fixed_qty").strip().lower()
    min_qty = max(1, int(min_qty or 1))

    if mode == "fixed_qty":
        qty = int(cfg.get("fixed_qty", 1) or 1)
        return qty if qty >= min_qty else 0

    if mode == "fixed_cash":
        cash = float(cfg.get("cash_per_trade", 0) or 0)
    elif mode == "capital_slots":
        cap = float(cfg.get("capital_total", 0) or 0)
        slots = float(cfg.get("max_positions", 0) or 0)
        cash = (cap / slots) if (cap > 0 and slots > 0) else 0.0
    else:
        qty = int(cfg.get("fixed_qty", 1) or 1)
        return qty if qty >= min_qty else 0

    if cash <= 0:
        return 0
    qty = int(float(cash) // float(entry_price))
    return qty if qty >= min_qty else 0


def calc_entry_fee(entry_price: float, qty: int, fee_pct: float) -> float:
    return float(entry_price) * float(qty) * float(fee_pct)


def calc_exit_fee(exit_price: float, qty: int, fee_pct: float) -> float:
    return float(exit_price) * float(qty) * float(fee_pct)


def calc_roundtrip_fee(entry_price: float, exit_price: float, qty: int, fee_pct: float) -> float:
    return compute_costs(
        entry_price=entry_price,
        exit_price=exit_price,
        qty=qty,
        fee_pct=fee_pct,
        slippage_pct=0.0,
        sell_tax_pct=0.0,
    ).roundtrip_fee


def calc_roundtrip_slippage(entry_price: float, exit_price: float, qty: int, slippage_pct: float) -> float:
    return compute_costs(
        entry_price=entry_price,
        exit_price=exit_price,
        qty=qty,
        fee_pct=0.0,
        slippage_pct=slippage_pct,
        sell_tax_pct=0.0,
    ).roundtrip_slippage


def calc_net_return(
    entry_price: float,
    exit_price: float,
    fee_pct: float,
    slippage_pct: float,
    sell_tax_pct: float = 0.0,
) -> float:
    return compute_costs(
        entry_price=entry_price,
        exit_price=exit_price,
        qty=1,
        fee_pct=fee_pct,
        slippage_pct=slippage_pct,
        sell_tax_pct=sell_tax_pct,
    ).net_return
