from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple


@dataclass
class OrderScoreConfig:
    fee_bps: float = 2.0
    slippage_bps: float = 3.0
    replace_penalty_bps: float = 1.0
    age_decay_per_min: float = 0.06
    imbalance_weight: float = 0.00012
    spread_weight: float = 0.00001
    markout_base: float = 0.0008
    pfill_cut_retail_pro_rata: float = 0.10
    pfill_cut_price_time: float = 0.03


@dataclass
class OrderScoreFeature:
    side: str
    age_min: float
    remain_ratio: float
    imbalance: float
    spread_bps: float
    venue_type: str


def _clamp(v: float, lo: float, hi: float) -> float:
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


def estimate_p_fill(feat: OrderScoreFeature, cfg: OrderScoreConfig) -> float:
    # Recent unfilled order age lowers fill probability conservatively.
    age_term = 1.0 / (1.0 + max(0.0, feat.age_min) * max(0.001, cfg.age_decay_per_min))
    # Lower remaining quantity implies a higher fill probability.
    remain_term = 1.0 - _clamp(feat.remain_ratio, 0.0, 1.0)
    # Combine imbalance with order side.
    side_sign = 1.0 if str(feat.side).upper() == "BUY" else -1.0
    imbalance_term = 0.5 + 0.5 * _clamp(side_sign * feat.imbalance, -1.0, 1.0)
    raw = 0.45 * age_term + 0.35 * remain_term + 0.20 * imbalance_term
    return _clamp(raw, 0.0, 1.0)


def estimate_signed_markout(feat: OrderScoreFeature, cfg: OrderScoreConfig) -> float:
    side_sign = 1.0 if str(feat.side).upper() == "BUY" else -1.0
    # Favor imbalance in the order direction and penalize wider spreads.
    edge = cfg.imbalance_weight * side_sign * _clamp(feat.imbalance, -1.0, 1.0)
    spread_penalty = cfg.spread_weight * max(0.0, feat.spread_bps)
    return float(cfg.markout_base + edge - spread_penalty)


def estimate_tc(cfg: OrderScoreConfig) -> float:
    total_bps = max(0.0, cfg.fee_bps + cfg.slippage_bps + cfg.replace_penalty_bps)
    return total_bps / 10000.0


def score_order(feat: OrderScoreFeature, cfg: OrderScoreConfig) -> Tuple[float, float, float, float]:
    p_fill = estimate_p_fill(feat, cfg)
    signed_markout = estimate_signed_markout(feat, cfg)
    tc = estimate_tc(cfg)
    score = p_fill * signed_markout - tc
    return float(score), float(p_fill), float(signed_markout), float(tc)


def decide_action(score: float, p_fill: float, venue_type: str, cfg: OrderScoreConfig) -> str:
    vt = str(venue_type or "").strip().lower()
    if vt == "retail_pro_rata":
        cut = cfg.pfill_cut_retail_pro_rata
    else:
        cut = cfg.pfill_cut_price_time
    if float(score) < 0.0 or float(p_fill) < float(cut):
        return "CANCEL_REPOST"
    return "HOLD"

