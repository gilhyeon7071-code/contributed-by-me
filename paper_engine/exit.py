# -*- coding: utf-8 -*-
"""Exit/sell decision layer for paper_engine.

Split out from the legacy ``paper_engine.py`` to isolate position exit
decisions: stop-loss, trailing, take-profit, fundamental/technical/market
risk exits, and surge reversal exit logic.
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, cast

import pandas as pd
from state_containers import json_safe as _state_json_safe

from pricing_engine import (
    calc_exit_fee,
    calc_roundtrip_fee,
    calc_roundtrip_slippage,
)
from utils.common import norm_code, read_csv_safe

from paper_engine.common import (
    _calc_ma,
    _calc_rsi,
    _calc_sell_qty,
    _clamp_sell_ratio_pct,
    _extract_note_field,
    _extract_ymd_from_ts_text,
    _fund_float,
    _get_asset_type_rule,
    _get_dict,
    _norm_ymd_text,
    _parse_note_fields,
    _pct01_from_config,
    _pct100_from_config,
    _pos_list,
    _sig_float,
    _to_float,
    _to_int,
    _truthy,
    _adjust_take_profit_ratio,
    _classify_asset_type,
    _estimate_atr14_pct_from_ohlc,
    _normal_realtime_gap_policy,
    _norm_ts14,
    _resolve_dynamic_stop_loss_pct,
    _severity_from_rank,
    _severity_rank,
    _shift_severity,
    _v411_trade_sig,
    calc_net_ret,
    get_ohlc,
    now_ts,
    resolve_slip_pct,
)
from paper_engine.drawdown import _ddm_forced_sell_ratio_pct, _ddm_pos_key
from paper_engine.io import (
    AFTER_CLOSE_SUMMARY_LAST_PATH,
    FILLS,
    INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_CSV_PATH,
    INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_JSON_PATH,
    TRADES,
    _derive_d_from_fills_path,
    _intraday_price_window_since_entry,
    _latest_lob_row_for_code,
    _load_intraday_history_for_ymd,
)
from paper_engine.surge import (
    _calc_surge_reversal_signals,
    _normalize_surge_position_policy_fields,
)

def _paper_exit_ts(exit_day: Any) -> str:
    day = str(exit_day or "").strip()
    current = datetime.now()
    if day == current.strftime("%Y%m%d") and current.strftime("%H:%M:%S") < "15:20:00":
        return f"{day}T{current.strftime('%H:%M:%S')}"
    return f"{day}T15:20:00"

def _entry_order_remaining_qty_for_sell(
    *,
    code: str,
    entry_order_id: str,
    pending_fills: List[Any],
    schema: str,
) -> Dict[str, Any]:
    src = str(entry_order_id or "").strip()
    code_norm = norm_code(code)
    out: Dict[str, Any] = {
        "known": False,
        "entry_order_id": src,
        "buy_qty": 0,
        "sell_qty": 0,
        "remaining_qty": None,
    }
    if not code_norm or not src:
        return out

    def _accumulate(side: Any, qty: Any, order_id: Any, note: Any) -> None:
        side_u = str(side or "").strip().upper()
        qty_i = _to_int(qty, None)
        if qty_i is None or qty_i <= 0:
            return
        oid = str(order_id or "").strip()
        note_s = str(note or "")
        note_entry_oid = (
            _extract_note_field(note_s, "entry_order_id")
            or _extract_note_field(note_s, "source_order_id")
            or ""
        ).strip()
        if side_u == "BUY" and oid == src:
            out["buy_qty"] = int(out["buy_qty"] or 0) + int(qty_i or 0)
            out["known"] = True
        elif side_u == "SELL" and note_entry_oid == src:
            out["sell_qty"] = int(out["sell_qty"] or 0) + int(qty_i or 0)

    try:
        df = read_csv_safe(FILLS)
    except Exception:
        df = None
    if isinstance(df, pd.DataFrame) and not df.empty:
        for _, row in df.iterrows():
            if norm_code(row.get("code", "")) != code_norm:
                continue
            _accumulate(row.get("side"), row.get("qty"), row.get("order_id"), row.get("note"))

    for row in pending_fills or []:
        try:
            if str(schema) == "legacy":
                if len(row) < 7 or norm_code(row[1]) != code_norm:
                    continue
                _accumulate(row[2], row[3], row[5], row[6])
            else:
                if len(row) < 11 or norm_code(row[2]) != code_norm:
                    continue
                _accumulate(row[4], row[5], row[9], row[10])
        except Exception:
            continue

    if bool(out["known"]):
        out["remaining_qty"] = max(0, int(out["buy_qty"] or 0) - int(out["sell_qty"] or 0))
    return out

def _resolve_hold_close_drop_guard_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    raw = cfg.get("hold_close_drop_guard", {}) if isinstance(cfg, dict) else {}
    if not isinstance(raw, dict):
        raw = {}
    drop_pct = abs(_pct01_from_config(raw.get("drop_pct"), 0.05))
    drop_pct = max(0.0, min(0.5, float(drop_pct)))
    return {
        "enabled": bool(raw.get("enabled", False)),
        "drop_pct": drop_pct,
        "sell_ratio_pct": _clamp_sell_ratio_pct(_pct100_from_config(raw.get("sell_ratio_pct"), 100.0)),
        "min_hold_days": max(0, int(_to_int(raw.get("min_hold_days"), 1))),
    }

def _evaluate_hold_close_drop_guard(
    *,
    prev_close: Any,
    close_price: Any,
    hold_days_trading: int,
    is_entry_date: bool,
    guard_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    if not bool((guard_cfg or {}).get("enabled", False)):
        return {"triggered": False, "reason": "disabled"}
    if bool(is_entry_date):
        return {"triggered": False, "reason": "entry_date"}
    min_hold_days = max(0, int(_to_int((guard_cfg or {}).get("min_hold_days"), 1)))
    if int(hold_days_trading) < min_hold_days:
        return {"triggered": False, "reason": "min_hold_days"}
    prev = float(_to_float(prev_close, 0.0) or 0.0)
    cur = float(_to_float(close_price, 0.0) or 0.0)
    if prev <= 0 or cur <= 0:
        return {"triggered": False, "reason": "invalid_price"}
    threshold = abs(_pct01_from_config((guard_cfg or {}).get("drop_pct"), 0.05))
    threshold = max(0.0, min(0.5, float(threshold)))
    close_drop_pct = (prev - cur) / prev
    triggered = bool(threshold > 0 and close_drop_pct >= threshold)
    return {
        "triggered": triggered,
        "reason": "close_drop" if triggered else "below_threshold",
        "close_drop_pct": float(close_drop_pct),
        "threshold_pct": float(threshold),
        "sell_ratio_pct": _clamp_sell_ratio_pct(_pct100_from_config((guard_cfg or {}).get("sell_ratio_pct"), 100.0)),
        "prev_close": float(prev),
        "close": float(cur),
    }

def _get_sell_rules(cfg: Dict[str, Any]) -> Dict[str, Any]:
    out = cfg.get("sell_rules", {}) if isinstance(cfg, dict) else {}
    return out if isinstance(out, dict) else {}

def _resolve_stop_loss_pct(pos: Dict[str, Any], sell_rules: Dict[str, Any], fallback_stop: float) -> float:
    stop_cfg = sell_rules.get("stop_loss", {}) if isinstance(sell_rules.get("stop_loss"), dict) else {}
    asset_type = str(pos.get("asset_type") or "").strip()
    overrides = stop_cfg.get("asset_type_overrides", {}) if isinstance(stop_cfg.get("asset_type_overrides"), dict) else {}
    if asset_type and asset_type in overrides:
        return _pct01_from_config(overrides.get(asset_type), fallback_stop)
    if "default_pct" in stop_cfg:
        return _pct01_from_config(stop_cfg.get("default_pct"), fallback_stop)
    return float(fallback_stop)

def _resolve_trailing_pct(pos: Dict[str, Any], sell_rules: Dict[str, Any], fallback_trail: Optional[float]) -> Optional[float]:
    stop_cfg = sell_rules.get("stop_loss", {}) if isinstance(sell_rules.get("stop_loss"), dict) else {}
    if bool(stop_cfg.get("trailing_stop_enabled", False)):
        asset_type = str(pos.get("asset_type") or "").strip()
        type_rule = _get_asset_type_rule(asset_type, sell_rules) if asset_type else {}
        if "trailing_stop_pct" in type_rule:
            return _pct01_from_config(type_rule.get("trailing_stop_pct"), -0.15)
        return _pct01_from_config(stop_cfg.get("trailing_stop_pct"), -0.15)
    if fallback_trail in ("", "None", None):
        return None
    try:
        return float(fallback_trail)
    except Exception:
        return None

def _resolve_trailing_activation_pct(sell_rules: Dict[str, Any]) -> float:
    stop_cfg = sell_rules.get("stop_loss", {}) if isinstance(sell_rules.get("stop_loss"), dict) else {}
    return _pct01_from_config(stop_cfg.get("trailing_stop_activation_profit_pct"), 0.20)

def _calc_surge_intraday_reversal_exit(
    *,
    code: str,
    entry_ts: Any,
    entry_date: str,
    entry_price: float,
    max_close: float,
    intraday_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    if not bool((intraday_cfg or {}).get("enabled", False)):
        return {"exit": False, "reason": "disabled"}
    ymd8 = _norm_ymd_text(entry_date)
    if len(ymd8) != 8 or float(entry_price or 0.0) <= 0:
        return {"exit": False, "reason": "invalid_entry"}

    hist = _load_intraday_history_for_ymd(ymd8)
    if hist.empty:
        return {"exit": False, "reason": "missing_intraday_history"}
    code6 = str(code or "").zfill(6)
    work = hist[hist["code"] == code6].copy()
    if work.empty:
        return {"exit": False, "reason": "missing_code_history"}

    entry_ts14 = _norm_ts14(entry_ts)
    if len(entry_ts14) >= 12 and "_ts14" in work.columns:
        work = work[(work["_ts14"] == "") | (work["_ts14"] >= entry_ts14)].copy()
    min_points = max(2, int(_to_int(intraday_cfg.get("min_points"), 3)))
    if len(work) < min_points:
        return {"exit": False, "reason": "insufficient_points", "points": int(len(work))}

    lookback = max(min_points, int(_to_int(intraday_cfg.get("lookback_points"), 5)))
    work = work.tail(lookback).copy()
    prices = pd.to_numeric(work["current_price"], errors="coerce").dropna()
    if len(prices) < min_points:
        return {"exit": False, "reason": "insufficient_valid_prices", "points": int(len(prices))}

    current_price = float(prices.iloc[-1])
    # Intraday snapshot high is session-to-date and can include prices before entry_ts.
    # Reversal after an intraday entry must use only entry-or-later executable prices.
    high_cols = [float(entry_price or 0.0)]
    high_cols.append(float(prices.max() or 0.0))
    max_close_after_entry = _to_float(max_close, None)
    if max_close_after_entry is not None and float(max_close_after_entry) <= max(float(entry_price or 0.0), float(prices.max() or 0.0)):
        high_cols.append(float(max_close_after_entry))
    high_since_entry = max(high_cols)
    drawdown = (current_price / high_since_entry - 1.0) if high_since_entry > 0 else 0.0

    signals: List[str] = []
    high_rej_min = _pct01_from_config(intraday_cfg.get("high_rejection_min_drawdown_pct"), 0.025)
    if high_since_entry > 0 and drawdown <= -abs(high_rej_min):
        signals.append("INTRADAY_HIGH_REJECTION")

    down_points = max(2, int(_to_int(intraday_cfg.get("consecutive_down_points"), 3)))
    recent = prices.tail(down_points)
    if len(recent) >= down_points and bool((recent.diff().dropna() < 0).all()):
        signals.append("INTRADAY_LOWER_CLOSES")

    if "volume" in work.columns:
        vols = pd.to_numeric(work["volume"], errors="coerce").fillna(0.0)
        delta_vol = vols.diff().clip(lower=0.0).fillna(vols)
        if len(delta_vol) >= min_points:
            peak_delta = float(delta_vol.max() or 0.0)
            recent_avg = float(delta_vol.tail(2).mean() or 0.0)
            fade_ratio_max = max(0.0, float(_to_float(intraday_cfg.get("volume_fade_ratio_max"), 0.35)))
            if peak_delta > 0 and (recent_avg / peak_delta) <= fade_ratio_max:
                signals.append("INTRADAY_VOLUME_FADE")

    lob = _latest_lob_row_for_code(code6)
    if lob:
        spread_bps = float(_to_float(lob.get("spread_bps"), 0.0) or 0.0)
        risk_score = float(_to_float(lob.get("orderflow_risk_score"), 0.0) or 0.0)
        tag = str(lob.get("orderflow_tag", "") or "").strip().upper()
        spread_max = float(_to_float(intraday_cfg.get("spread_bps_hard"), 80.0))
        risk_max = float(_to_float(intraday_cfg.get("orderflow_risk_score_max"), 0.6))
        if spread_bps >= spread_max or risk_score >= risk_max or tag in {"CAUTION", "PAUSE", "NO_LOB"}:
            signals.append("INTRADAY_LOB_RISK")

    trigger_count = max(1, int(_to_int(intraday_cfg.get("trigger_count"), 2)))
    require_high_rejection = bool(intraday_cfg.get("require_high_rejection_for_exit", False))
    missing_required_high_rejection = require_high_rejection and "INTRADAY_HIGH_REJECTION" not in signals
    triggered = bool(len(signals) >= trigger_count) and not missing_required_high_rejection
    return {
        "exit": bool(triggered),
        "reason": (
            "missing_required_high_rejection"
            if missing_required_high_rejection and len(signals) >= trigger_count
            else ("triggered" if triggered else "not_triggered")
        ),
        "signals": signals,
        "trigger_count": int(trigger_count),
        "require_high_rejection_for_exit": bool(require_high_rejection),
        "current_price": float(current_price),
        "high_since_entry": float(high_since_entry),
        "drawdown_pct": float(drawdown),
        "points": int(len(work)),
        "latest_ts": str(work["_ts14"].iloc[-1]) if "_ts14" in work.columns and len(work) else "",
    }

def _resolve_take_profit_plan(asset_type: str, sell_rules: Dict[str, Any], tp_cfg: Dict[str, Any]) -> Tuple[List[Any], List[Any]]:
    type_rule = _get_asset_type_rule(asset_type, sell_rules)
    levels = type_rule.get("take_profit_levels") if isinstance(type_rule.get("take_profit_levels"), list) else tp_cfg.get("levels", [])
    ratios = type_rule.get("take_profit_ratios") if isinstance(type_rule.get("take_profit_ratios"), list) else tp_cfg.get("ratios", [])
    return list(levels or []), list(ratios or [])

def _resolve_position_max_hold_days(asset_type: str, sell_rules: Dict[str, Any], default_max_hold_days: int) -> int:
    type_rule = _get_asset_type_rule(asset_type, sell_rules)
    try:
        v = int(type_rule.get("max_hold_days", default_max_hold_days) or default_max_hold_days)
    except Exception:
        v = int(default_max_hold_days)
    return max(1, v)

def _check_fundamental_risk(position: Dict[str, Any], sell_rules: Dict[str, Any]) -> List[Dict[str, Any]]:
    cfg = sell_rules.get("fundamental_risk", {}) if isinstance(sell_rules.get("fundamental_risk"), dict) else {}
    if not bool(cfg.get("enabled", False)):
        return []
    fund = position.get("fundamentals", {}) if isinstance(position.get("fundamentals"), dict) else {}
    if not fund:
        return []

    signals: List[Dict[str, Any]] = []
    critical_reasons: List[str] = []

    debt_ratio = _fund_float(fund.get("debt_ratio"), 0.0)
    roe = _fund_float(fund.get("roe"), 0.0)
    revenue_growth_yoy = _fund_float(fund.get("revenue_growth_yoy"), 0.0)
    operating_margin = _fund_float(fund.get("operating_margin"), 0.0)
    dividend_yield = _fund_float(fund.get("dividend_yield"), 0.0)

    if debt_ratio > _fund_float(cfg.get("critical_debt_ratio"), 200.0):
        critical_reasons.append(f"debt_ratio={debt_ratio:.1f}")
    if roe < _fund_float(cfg.get("critical_roe"), 0.0):
        critical_reasons.append(f"roe={roe:.1f}")
    if revenue_growth_yoy < _fund_float(cfg.get("critical_revenue_growth_yoy"), -20.0):
        critical_reasons.append(f"revenue_growth_yoy={revenue_growth_yoy:.1f}")

    if critical_reasons:
        signals.append({
            "type": "FUNDAMENTAL_CRITICAL",
            "ratio": 100.0,
            "priority": 1,
            "reason": ";".join(critical_reasons),
            "confidence": 95.0,
        })

    warning_reasons: List[str] = []
    if operating_margin < _fund_float(cfg.get("warning_operating_margin"), 5.0):
        warning_reasons.append(f"operating_margin={operating_margin:.1f}")
    if str(position.get("asset_type") or "").strip() == "배당주" and dividend_yield < _fund_float(cfg.get("dividend_yield_floor"), 3.0):
        warning_reasons.append(f"dividend_yield={dividend_yield:.2f}")

    if warning_reasons:
        signals.append({
            "type": "FUNDAMENTAL_WARNING",
            "ratio": _pct100_from_config(cfg.get("warning_sell_ratio", 50.0), 50.0),
            "priority": 3,
            "reason": ";".join(warning_reasons),
            "confidence": 80.0,
        })

    signals.sort(key=lambda x: (int(x.get("priority", 99)), -float(x.get("confidence", 0.0))))
    return signals

def _hydrate_position_exit_tags_from_fills(pos: Dict[str, Any], fills_df: pd.DataFrame) -> int:
    if not isinstance(pos, dict) or not isinstance(fills_df, pd.DataFrame) or fills_df.empty:
        return 0
    required = {"code", "side", "note"}
    if not required.issubset(set(fills_df.columns)):
        return 0
    code = norm_code(pos.get("code", ""))
    if not code:
        return 0
    entry_order_id = str(pos.get("entry_order_id") or pos.get("source_order_id") or "").strip()
    if not entry_order_id:
        return 0

    work = fills_df.copy()
    work["code"] = work["code"].astype(str).str.zfill(6)
    work = work[
        (work["code"] == code)
        & work["side"].astype(str).str.upper().eq("SELL")
        & work["note"].astype(str).str.contains(f"entry_order_id={entry_order_id}", regex=False, na=False)
    ].copy()
    if work.empty:
        return 0

    changed = 0
    executed_tags = set(_pos_list(pos, "executed_sell_tags"))
    tp_levels = set(_pos_list(pos, "tp_taken_levels"))
    for _, row in work.iterrows():
        note = str(row.get("note") or "")
        sell_tag = _extract_note_field(note, "sell_tag")
        exit_reason = _extract_note_field(note, "exit_reason")
        tag = str(sell_tag or exit_reason or "").strip()
        if tag and tag not in executed_tags:
            executed_tags.add(tag)
            changed += 1
        tp_match = re.match(r"^TP_L(\d+)$", tag)
        if tp_match:
            level = tp_match.group(1)
            if level not in tp_levels:
                tp_levels.add(level)
                changed += 1
    if changed:
        pos["executed_sell_tags"] = sorted(executed_tags)
        pos["tp_taken_levels"] = sorted(tp_levels, key=lambda x: int(x) if str(x).isdigit() else str(x))
    return changed

def _calc_surge_exit_severity(
    *,
    exit_reason: str,
    hold_days_trading: int,
    entry_price: float,
    exit_price: float,
    atr14_pct: float,
    is_surge_pos: bool,
    market_regime: str,
    kill_switch_active: bool,
    reversal_count: int,
    prior_stop_count: int,
    dynamic_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    reason = str(exit_reason or "").strip().upper()
    severity = "NORMAL"
    reasons: List[str] = []
    if reason == "STOP_PREEMPTIVE_CLOSE":
        severity = "LIGHT"
        reasons.append("base:preemptive")
    elif reason == "STOP_GAP":
        severity = "HARD"
        reasons.append("base:gap_stop")
    elif reason in {"REVERSAL_NEXT_OPEN", "SURGE_INTRADAY_REVERSAL"}:
        severity = "HARD"
        reasons.append("base:reversal")
    elif reason == "TRAIL":
        severity = "LIGHT"
        reasons.append("base:trail")
    elif reason == "TRAIL_GAP":
        severity = "NORMAL"
        reasons.append("base:trail_gap")
    else:
        severity = "NORMAL"
        reasons.append("base:stop")

    gap_loss_pct = 0.0
    if entry_price > 0 and exit_price > 0:
        gap_loss_pct = max(0.0, (float(entry_price) - float(exit_price)) / float(entry_price))

    force_gap_loss_pct = max(0.0, float(_to_float(dynamic_cfg.get("force_gap_loss_pct"), 0.08)))
    hard_gap_loss_pct = max(0.0, float(_to_float(dynamic_cfg.get("hard_gap_loss_pct"), 0.05)))
    hard_atr14_pct = max(0.0, float(_to_float(dynamic_cfg.get("hard_atr14_pct"), 0.06)))

    if reason == "STOP_GAP" and gap_loss_pct >= force_gap_loss_pct:
        severity = "FORCE"
        reasons.append(f"force_gap>={force_gap_loss_pct:.4f}")
    elif reason == "STOP_GAP" and gap_loss_pct >= hard_gap_loss_pct and _severity_rank(severity) < _severity_rank("HARD"):
        severity = "HARD"
        reasons.append(f"hard_gap>={hard_gap_loss_pct:.4f}")

    if atr14_pct >= hard_atr14_pct and _severity_rank(severity) < _severity_rank("HARD"):
        severity = "HARD"
        reasons.append(f"atr14>={hard_atr14_pct:.4f}")

    if (
        bool(dynamic_cfg.get("t1_surge_relax_one_level", True))
        and is_surge_pos
        and hold_days_trading <= 1
        and _severity_rank(severity) < _severity_rank("FORCE")
        and not (reason == "STOP" and _severity_rank(severity) <= _severity_rank("NORMAL"))
        and reason != "STOP_GAP"
    ):
        severity = _shift_severity(severity, -1)
        reasons.append("relax:t1_surge")

    early_hold_days = max(0, int(_to_int(dynamic_cfg.get("early_loss_hold_days"), 1)))
    early_hard_loss_pct = max(0.0, float(_to_float(dynamic_cfg.get("early_loss_hard_pct"), 0.08)))
    early_force_loss_pct = max(0.0, float(_to_float(dynamic_cfg.get("early_loss_force_pct"), 0.12)))
    if (
        is_surge_pos
        and reason in {"STOP", "STOP_GAP", "STOP_PREEMPTIVE_CLOSE", "REVERSAL_NEXT_OPEN", "SURGE_INTRADAY_REVERSAL"}
        and int(hold_days_trading) <= early_hold_days
    ):
        if early_force_loss_pct > 0 and gap_loss_pct >= early_force_loss_pct:
            severity = "FORCE"
            reasons.append(f"early_force_loss>={early_force_loss_pct:.4f}")
        elif early_hard_loss_pct > 0 and gap_loss_pct >= early_hard_loss_pct and _severity_rank(severity) < _severity_rank("HARD"):
            severity = "HARD"
            reasons.append(f"early_hard_loss>={early_hard_loss_pct:.4f}")

    if bool(dynamic_cfg.get("risk_off_raise_one_level", True)) and (
        kill_switch_active or str(market_regime or "").strip().upper() in {"BEAR", "CRASH", "STAGFLATION", "RATE_HIKE_FEAR"}
    ):
        severity = _shift_severity(severity, 1)
        reasons.append("raise:risk_off")

    if bool(dynamic_cfg.get("reversal_raise_one_level", True)) and int(reversal_count) >= 2:
        severity = _shift_severity(severity, 1)
        reasons.append("raise:reversal")

    if bool(dynamic_cfg.get("repeat_stop_force_exit", False)) and int(prior_stop_count) >= 1:
        severity = "FORCE"
        reasons.append("force:repeat_stop")
    elif bool(dynamic_cfg.get("repeat_stop_raise_one_level", True)) and int(prior_stop_count) >= 1:
        severity = _shift_severity(severity, 1)
        reasons.append("raise:repeat_stop")

    return {
        "severity": severity,
        "severity_reasons": reasons,
        "gap_loss_pct": round(float(gap_loss_pct), 6),
    }

def _resolve_surge_exit_ratio_pct(
    *,
    severity: str,
    dynamic_cfg: Dict[str, Any],
    fallback_pct: float,
) -> float:
    base_ratios = dynamic_cfg.get("base_ratios", {}) if isinstance(dynamic_cfg.get("base_ratios"), dict) else {}
    rank_label = _severity_from_rank(_severity_rank(severity))
    default_map = {
        "LIGHT": 25.0,
        "NORMAL": 50.0,
        "HARD": 75.0,
        "FORCE": 100.0,
    }
    raw_value = base_ratios.get(rank_label, default_map.get(rank_label, fallback_pct))
    return _pct100_from_config(raw_value, fallback_pct)

def _evaluate_technical_exit(
    *,
    date_ymd: str,
    close_price: float,
    entry_price: float,
    current_profit_pct: float,
    hist_closes: List[float],
    executed_sell_tags: Set[str],
    tech_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    ma_periods = tech_cfg.get("ma_periods", [20, 60, 120]) if isinstance(tech_cfg.get("ma_periods"), list) else [20, 60, 120]
    ma20 = _calc_ma(hist_closes, int(ma_periods[0])) if len(ma_periods) >= 1 else None
    ma60 = _calc_ma(hist_closes, int(ma_periods[1])) if len(ma_periods) >= 2 else None
    ma120 = _calc_ma(hist_closes, int(ma_periods[2])) if len(ma_periods) >= 3 else None
    prev_closes = hist_closes[:-1]
    prev_ma20 = _calc_ma(prev_closes, int(ma_periods[0])) if len(ma_periods) >= 1 else None
    prev_ma60 = _calc_ma(prev_closes, int(ma_periods[1])) if len(ma_periods) >= 2 else None
    rsi = _calc_rsi(hist_closes, 14)
    technical_score = 0
    if ma20 is not None and close_price < ma20:
        technical_score += 10
    if ma60 is not None and close_price < ma60:
        technical_score += 20
    if ma120 is not None and close_price < ma120:
        technical_score += 30
    if (
        ma20 is not None and ma60 is not None
        and prev_ma20 is not None and prev_ma60 is not None
        and ma20 < ma60 and prev_ma20 >= prev_ma60
    ):
        technical_score += 25
    if rsi >= float(tech_cfg.get("rsi_overbought", 75) or 75) and current_profit_pct > 0:
        technical_score += 20

    high_thr = int(tech_cfg.get("high_score_threshold", 60) or 60)
    mid_thr = int(tech_cfg.get("mid_score_threshold", 40) or 40)
    tech_tag = f"TECH_{date_ymd}"
    if technical_score >= high_thr and tech_tag not in executed_sell_tags:
        return {
            "triggered": True,
            "exit_reason": "TECHNICAL",
            "sell_ratio_pct": _pct100_from_config(tech_cfg.get("high_score_sell_ratio", 50), 50),
            "sell_tag": tech_tag,
            "exit_price": close_price,
        }
    if technical_score >= mid_thr and tech_tag not in executed_sell_tags:
        return {
            "triggered": True,
            "exit_reason": "TECHNICAL",
            "sell_ratio_pct": _pct100_from_config(tech_cfg.get("mid_score_sell_ratio", 30), 30),
            "sell_tag": tech_tag,
            "exit_price": close_price,
        }
    return {"triggered": False}

def _evaluate_market_risk_exit(
    *,
    date_ymd: str,
    close_price: float,
    current_profit_pct: float,
    vix_proxy: Any,
    fx_ctx: Dict[str, Any],
    macro_snapshot: Dict[str, Any],
    p0_snapshot: Dict[str, Any],
    market_regime: str,
    market_cfg: Dict[str, Any],
    executed_sell_tags: Set[str],
) -> Dict[str, Any]:
    market_risk_score = 0
    if vix_proxy is not None:
        if float(vix_proxy) > float(market_cfg.get("vix_extreme", 40) or 40):
            market_risk_score += 40
        elif float(vix_proxy) > float(market_cfg.get("vix_high", 30) or 30):
            market_risk_score += 25
    fx_daily_abs = abs(_to_float((fx_ctx or {}).get("daily_abs_change"), 0.0)) if isinstance(fx_ctx, dict) else 0.0
    if fx_daily_abs > float(market_cfg.get("usd_krw_volatility", 15) or 15):
        market_risk_score += 15
    macro_metrics = macro_snapshot.get("market_metrics") if isinstance(macro_snapshot.get("market_metrics"), dict) else {}
    oil_shock_score = _to_float((macro_metrics or {}).get("oil_shock_score"), 0.0)
    if oil_shock_score >= float(market_cfg.get("oil_shock_extreme", 0.75) or 0.75):
        market_risk_score += 20
    elif oil_shock_score >= float(market_cfg.get("oil_shock_high", 0.45) or 0.45):
        market_risk_score += 10
    if str(p0_snapshot.get("market_regime") or "").upper() == "BEAR":
        market_risk_score += 20
    if str(market_regime or "").upper() in {"CRASH", "STAGFLATION"}:
        market_risk_score += 20
    elif str(market_regime or "").upper() == "RATE_HIKE_FEAR":
        market_risk_score += 10
    market_tag = f"MARKET_{date_ymd}"
    if market_risk_score >= 60 and market_tag not in executed_sell_tags:
        if current_profit_pct > 0 and market_risk_score >= 80:
            sell_ratio_pct = _pct100_from_config(market_cfg.get("extreme_sell_ratio_profit", 70), 70)
        elif current_profit_pct > 0:
            sell_ratio_pct = _pct100_from_config(market_cfg.get("high_sell_ratio_profit", 30), 30)
        else:
            sell_ratio_pct = _pct100_from_config(market_cfg.get("extreme_sell_ratio_loss", 30), 30)
        return {
            "triggered": True,
            "exit_reason": "MARKET_RISK",
            "sell_ratio_pct": sell_ratio_pct,
            "sell_tag": market_tag,
            "exit_price": close_price,
        }
    return {"triggered": False}

def _process_single_position_exit(
    *,
    config: Dict[str, Any],
    schema: str,
    prices_df: pd.DataFrame,
    pos: Dict[str, Any],
    idx_pos: int,
    fundamentals_db: Dict[str, Any],
    sector_db: Dict[str, Any],
    max_hold_days: int,
    sell_rules: Dict[str, Any],
    sell_rules_enabled: bool,
    ddm_liquidation_targets: Set[str],
    ddm_liquidation_price_mode: str,
    ddm_action: Any,
    fee_pct: float,
    slip_pct: float,
    sell_tax_pct: float,
    fills_new: List[Any],
    trades_new: List[Any],
    existing_fill_order_ids: Set[str],
    existing_trade_sigs: Set[str],
    committed_source_order_ids: Set[str],
    next_seq_start: int,
    stop_loss: float,
    take_profit: Any,
    trail_pct: Any,
    vix_proxy: Any,
    fx_ctx: Dict[str, Any],
    macro_snapshot: Dict[str, Any],
    p0_snapshot: Dict[str, Any],
    market_regime: str,
    active_candidate_codes: Set[str],
    candidate_dropout_cfg: Dict[str, Any],
    candidate_dropout_sold_codes: Set[str],
) -> Dict[str, Any]:
    cfg = config
    px = prices_df
    next_seq = int(next_seq_start)
    positions_out: List[Dict[str, Any]] = []

    code = str(pos["code"]).zfill(6)
    name = str(pos.get("name", "") or "")
    qty = int(pos.get("qty", 1))
    entry_date = str(pos.get("entry_date"))
    entry_ts = str(pos.get("entry_ts"))
    entry_price = float(_to_float(pos.get("entry_price"), 0.0) or 0.0)
    max_close = float(pos.get("max_close", entry_price))

    pos_market_cap = float(pos.get("market_cap") or 0)
    pos_slip_pct = max(
        resolve_slip_pct(pos_market_cap, cfg, slip_pct),
        float(_to_float(pos.get("entry_slippage_pct"), 0.0) or 0.0),
    )

    if not str(pos.get("sector") or "").strip() and isinstance(sector_db, dict):
        pos["sector"] = str(sector_db.get(code, "") or "").strip()
    if (not isinstance(pos.get("fundamentals"), dict) or not pos.get("fundamentals")) and isinstance(fundamentals_db, dict):
        pos["fundamentals"] = fundamentals_db.get(code, {})
    asset_type = str(pos.get("asset_type") or "").strip()
    if not asset_type:
        asset_type = _classify_asset_type(pos)
        pos["asset_type"] = asset_type

    stop = _resolve_stop_loss_pct(pos, sell_rules if sell_rules_enabled else {}, stop_loss)
    # ATR-based stop override: -(atr14_pct * multiplier), capped at -20%
    # Floor = config-resolved stop (asset_type_override or default_pct), so ATR never loosens the stop
    _atr_mult = float(cfg.get("atr_stop_multiplier", 0.0) or 0.0)
    _effective_atr14_pct = float(_to_float(pos.get("atr14_pct"), 0.0) or 0.0)
    if _effective_atr14_pct <= 0:
        _effective_atr14_pct = _estimate_atr14_pct_from_ohlc(px, code, entry_date)
        if _effective_atr14_pct > 0:
            pos["atr14_pct"] = float(_effective_atr14_pct)
            pos["atr14_pct_source"] = "ohlc_fallback"
    if _atr_mult > 0 and _effective_atr14_pct > 0:
        _atr_stop = max(-(_effective_atr14_pct * _atr_mult), -0.20)
        stop = max(_atr_stop, stop)  # keep tighter stop (closer to zero), never loosen configured stop
    # min_hold_days: number of trading days to protect from stop/fundamental exit
    _min_hold_days = int(cfg.get("min_hold_days", 0) or 0)
    _min_hold_protect_stop_loss = bool(cfg.get("min_hold_protect_stop_loss", True))
    tp = pos.get("take_profit", take_profit)
    tp = None if tp in ("", "None") else tp
    tp = float(tp) if tp is not None else None
    tr = _resolve_trailing_pct(pos, sell_rules if sell_rules_enabled else {}, trail_pct)
    trailing_activation_pct = _resolve_trailing_activation_pct(sell_rules if sell_rules_enabled else {})
    stop_cfg = sell_rules.get("stop_loss", {}) if isinstance(sell_rules.get("stop_loss"), dict) else {}
    preemptive_close_enabled = bool(stop_cfg.get("preemptive_close_enabled", True))
    general_stop_sell_ratio_pct = _pct100_from_config(stop_cfg.get("stop_sell_ratio_pct"), 100.0)
    general_stop_gap_sell_ratio_pct = _pct100_from_config(stop_cfg.get("stop_gap_sell_ratio_pct", stop_cfg.get("stop_sell_ratio_pct")), 100.0)
    general_preemptive_sell_ratio_pct = _pct100_from_config(
        stop_cfg.get("preemptive_sell_ratio_pct", stop_cfg.get("stop_sell_ratio_pct")),
        100.0,
    )
    hold_close_drop_guard_cfg = _resolve_hold_close_drop_guard_cfg(cfg)
    _preemptive_raw = _to_float(stop_cfg.get("preemptive_close_pct"), -3.5)
    if _preemptive_raw is None:
        _preemptive_raw = -3.5
    preemptive_close_pct = abs(float(_preemptive_raw))
    if preemptive_close_pct > 1.0:
        preemptive_close_pct /= 100.0
    preemptive_close_pct = max(0.0, min(0.5, preemptive_close_pct))
    # SURGE EXIT POLICY: override stop/hold/tp for surge-entered positions
    _is_surge_pos = _truthy(pos.get("_surge_immediate")) or bool(str(pos.get("surge_type", "") or "").strip())
    _surge_exit_cfg: Dict[str, Any] = {}
    _surge_tp_pct: Optional[float] = None
    _surge_stop_sell_ratio_pct: float = 100.0
    _surge_preemptive_sell_ratio_pct: float = 100.0
    _surge_reversal_cfg: Dict[str, Any] = {}
    _surge_intraday_reversal_cfg: Dict[str, Any] = {}
    _surge_dynamic_exit_ratio_cfg: Dict[str, Any] = {}
    _surge_dynamic_exit_ratio_enabled = False
    if _is_surge_pos:
        _normalize_surge_position_policy_fields(pos, cfg)
        _surge_exit_cfg = cfg.get("surge_exit_policy", {}) if isinstance(cfg.get("surge_exit_policy"), dict) else {}
        if bool(_surge_exit_cfg.get("enabled", False)):
            _se_stop = _to_float(_surge_exit_cfg.get("stop_loss_pct"), None)
            _pos_stop_override = _to_float(pos.get("surge_type_stop_pct"), None)
            if _pos_stop_override is not None:
                _se_stop = float(_pos_stop_override)
            if _se_stop is not None:
                # stop is negative ratio; tighter stop is closer to zero (e.g. -5% tighter than -8%)
                stop = max(float(_se_stop), stop)
            _surge_tp_pct = _to_float(_surge_exit_cfg.get("take_profit_pct"), None)
            _pos_tp_override = _to_float(pos.get("surge_type_tp_pct"), None)
            if _pos_tp_override is not None:
                _surge_tp_pct = float(_pos_tp_override)
            if _surge_tp_pct is not None:
                pos["take_profit"] = float(_surge_tp_pct)
            _surge_stop_sell_ratio_pct = _pct100_from_config(_surge_exit_cfg.get("stop_sell_ratio_pct"), 100.0)
            _surge_preemptive_sell_ratio_pct = _pct100_from_config(
                _surge_exit_cfg.get("preemptive_sell_ratio_pct", _surge_exit_cfg.get("stop_sell_ratio_pct")),
                100.0,
            )
            _surge_preemptive_close_raw = _to_float(_surge_exit_cfg.get("preemptive_close_pct"), None)
            if _surge_preemptive_close_raw is not None:
                preemptive_close_pct = abs(float(_surge_preemptive_close_raw))
                if preemptive_close_pct > 1.0:
                    preemptive_close_pct /= 100.0
                preemptive_close_pct = max(0.0, min(0.5, preemptive_close_pct))
            _surge_dynamic_exit_ratio_cfg = (
                _surge_exit_cfg.get("dynamic_exit_ratio", {})
                if isinstance(_surge_exit_cfg.get("dynamic_exit_ratio"), dict)
                else {}
            )
            _surge_dynamic_exit_ratio_enabled = bool(_surge_dynamic_exit_ratio_cfg.get("enabled", False))
            _surge_reversal_cfg = (
                _surge_exit_cfg.get("reversal_exit", {})
                if isinstance(_surge_exit_cfg.get("reversal_exit"), dict)
                else {}
            )
            _surge_intraday_reversal_cfg = (
                _surge_exit_cfg.get("intraday_reversal_exit", {})
                if isinstance(_surge_exit_cfg.get("intraday_reversal_exit"), dict)
                else {}
            )
            print(
                f"[SURGE_EXIT] code={code} stop={stop:.4f} tp={_surge_tp_pct} "
                f"surge_type={pos.get('surge_type','')}"
            )
    horizon_hold_days = _to_int(pos.get("horizon_max_hold_days"), 0)
    if _is_surge_pos and bool(_surge_exit_cfg.get("enabled", False)):
        _se_hold = _to_int(_surge_exit_cfg.get("max_hold_days"), 0)
        _pos_hold_override = _to_int(pos.get("surge_type_max_hold_days"), 0)
        if _pos_hold_override > 0:
            _se_hold = int(_pos_hold_override)
        if _se_hold > 0:
            # surge hold overrides horizon — surge momentum resolves within days
            position_max_hold_days = int(_se_hold)
        elif horizon_hold_days > 0:
            position_max_hold_days = int(horizon_hold_days)
        else:
            position_max_hold_days = _resolve_position_max_hold_days(asset_type, sell_rules if sell_rules_enabled else {}, max_hold_days)
    elif horizon_hold_days > 0:
        position_max_hold_days = int(horizon_hold_days)
    else:
        position_max_hold_days = _resolve_position_max_hold_days(asset_type, sell_rules if sell_rules_enabled else {}, max_hold_days)
    executed_sell_tags = set(_pos_list(pos, "executed_sell_tags"))
    tp_taken_levels = set(_pos_list(pos, "tp_taken_levels"))

    code_px = px[px["code"] == code]
    if code_px.empty:
        positions_out.append(pos)
        return {"positions_out": positions_out, "next_seq": int(next_seq)}

    dates = code_px.loc[code_px["date"] >= entry_date, "date"].tolist()
    if not dates:
        positions_out.append(pos)
        return {"positions_out": positions_out, "next_seq": int(next_seq)}
    latest_px_date = str(px["date"].max()) if "date" in px.columns and not px.empty else str(dates[-1])

    exit_day = None
    exit_price = None
    exit_reason = None
    stop_hit = tp_hit = trail_hit = False
    sell_ratio_pct = 100.0
    sell_tag = ""

    pos_key = _ddm_pos_key(pos, idx_pos)
    ddm_forced_day = None
    ddm_forced_price = None
    ddm_forced_reason = None
    ddm_forced_sell_ratio_pct = None
    if pos_key in ddm_liquidation_targets:
        forced_day = dates[-1]
        forced_ohlc = get_ohlc(px, code, forced_day)
        if forced_ohlc and float(forced_ohlc.get("close", 0) or 0) > 0:
            forced_px = float(forced_ohlc.get("close", 0) or 0)
            if ddm_liquidation_price_mode == "open":
                op = float(forced_ohlc.get("open", 0) or 0)
                if op > 0:
                    forced_px = op
            candidate_ddm_reason = f"DDM_LIQUIDATE_L{ddm_action.stage_idx if ddm_action.stage_idx >= 0 else 0}"
            if candidate_ddm_reason not in executed_sell_tags:
                ddm_forced_day = forced_day
                ddm_forced_price = forced_px
                ddm_forced_reason = candidate_ddm_reason
                ddm_forced_sell_ratio_pct = _ddm_forced_sell_ratio_pct(ddm_action)

    entry_dt_obj: Optional[datetime] = None
    try:
        entry_dt_obj = datetime.strptime(entry_date, "%Y%m%d")
    except Exception:
        entry_dt_obj = None
    hold_days_trading = 0
    hold_days_calendar = 0
    _surge_reversal_pending = False
    _surge_reversal_count = 0
    _exit_severity = ""
    _exit_severity_reasons: List[str] = []
    _gap_loss_pct = 0.0
    _exit_extra_note_fields = ""
    _prior_stop_count = max(0, int(_to_int(pos.get("stop_exit_count"), 0)))
    _normal_rt_policy = _normal_realtime_gap_policy(cfg)
    _normal_gap_exit_cfg = (
        _normal_rt_policy.get("overnight_gap_exit", {})
        if isinstance(_normal_rt_policy.get("overnight_gap_exit"), dict)
        else {}
    )
    _normal_gap_exit_enabled = bool(_normal_gap_exit_cfg.get("enabled", True)) if _normal_rt_policy else False
    _normal_gap_down_exit_pct = _pct01_from_config(
        _normal_gap_exit_cfg.get("gap_down_exit_pct"),
        _to_float(cfg.get("entry_gap_down_stop_pct"), 0.03),
    )
    _normal_gap_up_tp_pct = _pct01_from_config(_normal_gap_exit_cfg.get("gap_up_take_profit_pct"), 0.12)
    _normal_gap_down_sell_ratio_pct = _pct100_from_config(_normal_gap_exit_cfg.get("gap_down_sell_ratio_pct"), 100.0)
    _normal_gap_up_sell_ratio_pct = _pct100_from_config(_normal_gap_exit_cfg.get("gap_up_sell_ratio_pct"), 100.0)
    _surge_entry_rows = code_px.loc[code_px["date"] == entry_date]
    _surge_entry_volume = float(pd.to_numeric(_surge_entry_rows["volume"], errors="coerce").iloc[-1]) if (not _surge_entry_rows.empty and "volume" in _surge_entry_rows.columns) else 0.0
    if exit_day is None:
        for i, d in enumerate(dates):
            ohlc = get_ohlc(px, code, d)
            if not ohlc:
                continue

            # close 0 entries are invalid, skip this day
            if float(ohlc["close"]) <= 0:
                continue

            is_entry_date = (d == entry_date)
            if is_entry_date:
                max_close = max(max_close, float(ohlc["close"]))
                if _is_surge_pos and bool(_surge_intraday_reversal_cfg.get("enabled", False)):
                    _intraday_rev = _calc_surge_intraday_reversal_exit(
                        code=code,
                        entry_ts=entry_ts,
                        entry_date=entry_date,
                        entry_price=float(entry_price),
                        max_close=float(max_close),
                        intraday_cfg=_surge_intraday_reversal_cfg,
                    )
                    if bool(_intraday_rev.get("exit", False)):
                        exit_day = d
                        exit_price = float(_intraday_rev.get("current_price") or ohlc["close"])
                        exit_reason = "SURGE_INTRADAY_REVERSAL"
                        _surge_reversal_count = max(
                            int(_surge_reversal_count),
                            int(len(_intraday_rev.get("signals", []) or [])),
                        )
                        _exit_severity_reasons.extend(
                            [str(v) for v in (_intraday_rev.get("signals", []) or []) if str(v).strip()]
                        )
                        pos["last_surge_intraday_reversal"] = {
                            "ts": str(_intraday_rev.get("latest_ts", "") or ""),
                            "signals": list(_intraday_rev.get("signals", []) or []),
                            "drawdown_pct": float(_intraday_rev.get("drawdown_pct", 0.0) or 0.0),
                            "points": int(_intraday_rev.get("points", 0) or 0),
                        }
                        print(
                            f"[SURGE_INTRADAY_REVERSAL_EXIT] code={code} date={d} "
                            f"price={float(exit_price):.4f} signals={','.join(_intraday_rev.get('signals', []) or [])}"
                        )
                        break
                if _min_hold_protect_stop_loss:
                    continue

            hold_days_trading = i
            hold_days_calendar = i
            if entry_dt_obj is not None:
                try:
                    hold_days_calendar = (datetime.strptime(str(d), "%Y%m%d") - entry_dt_obj).days
                except Exception:
                    hold_days_calendar = i

            if _is_surge_pos and bool(_surge_reversal_cfg.get("enabled", False)) and _surge_reversal_pending and float(ohlc["open"]) > 0:
                exit_day, exit_price, exit_reason = d, float(ohlc["open"]), "REVERSAL_NEXT_OPEN"
                print(
                    f"[SURGE_REVERSAL_EXIT] code={code} date={d} open={float(ohlc['open']):.4f} "
                    f"reversal_count={_surge_reversal_count}"
                )
                break

            hist_closes = code_px.loc[code_px["date"] <= d, "close"].astype(float).tolist()
            hist_volumes: List[float] = []
            if "volume" in code_px.columns:
                hist_volumes = (
                    pd.to_numeric(code_px.loc[code_px["date"] <= d, "volume"], errors="coerce")
                    .fillna(0.0)
                    .astype(float)
                    .tolist()
                )
            current_profit_pct = ((float(ohlc["close"]) - entry_price) / entry_price * 100.0) if entry_price > 0 else 0.0
            peak_profit_pct = ((max_close - entry_price) / entry_price * 100.0) if entry_price > 0 else 0.0
            dynamic_stop = _resolve_dynamic_stop_loss_pct(
                base_stop_pct=float(stop),
                current_profit_pct=float(current_profit_pct),
                hold_days_trading=int(i),
                atr14_pct=float(_effective_atr14_pct),
                cfg=cfg,
            )
            stop_price = entry_price * (1.0 + dynamic_stop)
            trail_price = None
            if tr is not None and peak_profit_pct >= float(trailing_activation_pct * 100.0):
                trail_price = (max_close * (1.0 + tr))

            o, h, low, c = ohlc["open"], ohlc["high"], ohlc["low"], ohlc["close"]
            _entry_timing_text = str(pos.get("entry_timing", "") or "").strip().lower()
            _entry_ts14 = _norm_ts14(entry_ts)
            _intraday_realtime_pos = bool(
                _entry_timing_text == "intraday_realtime"
                or (
                    is_entry_date
                    and len(_entry_ts14) >= 12
                    and not _entry_ts14.endswith("090000")
                    and not _entry_ts14.endswith("152000")
                )
            )
            _same_close_pos = bool(
                (not _is_surge_pos)
                and (
                    _entry_timing_text == "same_close"
                    or (len(_entry_ts14) >= 12 and _entry_ts14.endswith("152000"))
                )
            )
            if (
                exit_day is None
                and _normal_gap_exit_enabled
                and _same_close_pos
                and (not is_entry_date)
                and int(i) == 1
                and entry_price > 0
                and float(o or 0.0) > 0
            ):
                _overnight_gap_pct = (float(o) - float(entry_price)) / float(entry_price)
                if _normal_gap_down_exit_pct > 0 and _overnight_gap_pct <= -abs(_normal_gap_down_exit_pct):
                    exit_day, exit_price, exit_reason = d, float(o), "NORMAL_EARLY_GAP_DOWN_EXIT"
                    stop_hit = True
                    _exit_extra_note_fields += f"normal_overnight_gap_pct={_overnight_gap_pct:.6f};"
                    print(
                        f"[NORMAL_EARLY_GAP_DOWN_EXIT] code={code} date={d} open={float(o):.4f} "
                        f"gap={_overnight_gap_pct:.4f} threshold=-{abs(_normal_gap_down_exit_pct):.4f}"
                    )
                    break
                if _normal_gap_up_tp_pct > 0 and _overnight_gap_pct >= abs(_normal_gap_up_tp_pct):
                    exit_day, exit_price, exit_reason = d, float(o), "NORMAL_EARLY_GAP_UP_TP"
                    tp_hit = True
                    _exit_extra_note_fields += f"normal_overnight_gap_pct={_overnight_gap_pct:.6f};"
                    print(
                        f"[NORMAL_EARLY_GAP_UP_TP] code={code} date={d} open={float(o):.4f} "
                        f"gap={_overnight_gap_pct:.4f} threshold={abs(_normal_gap_up_tp_pct):.4f}"
                    )
                    break
            if is_entry_date and _intraday_realtime_pos:
                _rt_window = _intraday_price_window_since_entry(code, entry_ts, entry_date)
                o = float(entry_price)
                if int(_rt_window.get("points", 0) or 0) > 0:
                    h = max(float(entry_price), float(_rt_window.get("high_price", entry_price) or entry_price))
                    low = min(float(entry_price), float(_rt_window.get("low_price", entry_price) or entry_price))
                    c = float(_rt_window.get("current_price", c) or c)
                    current_profit_pct = ((float(c) - entry_price) / entry_price * 100.0) if entry_price > 0 else 0.0
                    max_close = max(max_close, float(h))
                    peak_profit_pct = ((max_close - entry_price) / entry_price * 100.0) if entry_price > 0 else 0.0
                else:
                    h = max(float(entry_price), float(c))
                    low = min(float(entry_price), float(c))
            if _is_surge_pos and int(i) <= max(0, _to_int(_surge_dynamic_exit_ratio_cfg.get("early_loss_hold_days"), 1)):
                _early_rev_pct = _pct01_from_config(_surge_reversal_cfg.get("early_close_loss_pct"), 0.08)
                _surge_trigger_count = max(1, int(_to_int(_surge_reversal_cfg.get("trigger_count"), 2)))
                if _early_rev_pct > 0 and c <= entry_price * (1.0 - _early_rev_pct):
                    _surge_reversal_count = max(int(_surge_reversal_count), int(_surge_trigger_count))
                    _exit_severity_reasons.append(f"early_reversal_close_loss>={_early_rev_pct:.4f}")

            # min_hold_days guard: skip STOP/FUNDAMENTAL during protected period
            # When stop protection is disabled, entry-day STOP must use intraday OHLC.
            _in_protected_period = (_min_hold_days > 0 and i <= _min_hold_days)
            _protect_stop_loss = (_in_protected_period and _min_hold_protect_stop_loss)
            if _in_protected_period:
                max_close = max(max_close, float(c))
                # still allow TP and TIME exits below; skip to TP section
                # but first check TP via normal path — just skip STOP block
                pass

            # STOP / TRAIL priority:
            # if trail is active and trail_price > stop_price, trail is the binding constraint
            # → check TRAIL first so we exit at the higher price (less loss)
            trail_is_binding: bool = trail_price is not None and trail_price > stop_price

            if not _protect_stop_loss and is_entry_date:
                if d == latest_px_date:
                    if o <= stop_price:
                        exit_day, exit_price, exit_reason = d, o, "STOP_GAP"
                        stop_hit = True
                        break
                    elif low <= stop_price:
                        exit_day, exit_price, exit_reason = d, stop_price, "STOP"
                        stop_hit = True
                        break
                elif c <= stop_price:
                    exit_day, exit_price, exit_reason = d, c, "STOP"
                    stop_hit = True
                    break
            elif not _protect_stop_loss and trail_is_binding and trail_price is not None:
                # TRAIL first
                if o <= trail_price:
                    exit_day, exit_price, exit_reason = d, o, "TRAIL_GAP"
                    trail_hit = True
                    break
                elif low <= trail_price:
                    exit_day, exit_price, exit_reason = d, trail_price, "TRAIL"
                    trail_hit = True
                    break
                # STOP after (catches gap below stop_price when trail not yet breached)
                if o <= stop_price:
                    exit_day, exit_price, exit_reason = d, o, "STOP_GAP"
                    stop_hit = True
                    break
                elif low <= stop_price:
                    exit_day, exit_price, exit_reason = d, stop_price, "STOP"
                    stop_hit = True
                    break
            elif not _protect_stop_loss:
                # STOP first (trail not active or trail_price <= stop_price)
                if o <= stop_price:
                    exit_day, exit_price, exit_reason = d, o, "STOP_GAP"
                    stop_hit = True
                    break
                elif low <= stop_price:
                    exit_day, exit_price, exit_reason = d, stop_price, "STOP"
                    stop_hit = True
                    break
                # TRAIL after
                if trail_price is not None:
                    if o <= trail_price:
                        exit_day, exit_price, exit_reason = d, o, "TRAIL_GAP"
                        trail_hit = True
                        break
                    elif low <= trail_price:
                        exit_day, exit_price, exit_reason = d, trail_price, "TRAIL"
                        trail_hit = True
                        break

            if (
                not _protect_stop_loss
                and preemptive_close_enabled
                and exit_day is None
            ):
                preemptive_stop_price = entry_price * (1.0 - preemptive_close_pct)
                if c <= preemptive_stop_price:
                    exit_day, exit_price, exit_reason = d, c, "STOP_PREEMPTIVE_CLOSE"
                    stop_hit = True
                    break

            if exit_day is None and not is_entry_date and len(hist_closes) >= 2:
                _hold_close_drop_eval = _evaluate_hold_close_drop_guard(
                    prev_close=hist_closes[-2],
                    close_price=c,
                    hold_days_trading=int(i),
                    is_entry_date=bool(is_entry_date),
                    guard_cfg=hold_close_drop_guard_cfg,
                )
                if bool(_hold_close_drop_eval.get("triggered", False)):
                    exit_day, exit_price, exit_reason = d, c, "HOLD_CLOSE_DROP"
                    sell_ratio_pct = float(_hold_close_drop_eval.get("sell_ratio_pct", 100.0) or 100.0)
                    sell_tag = "HOLD_CLOSE_DROP"
                    _exit_extra_note_fields = (
                        f"hold_close_drop_pct={round(float(_hold_close_drop_eval.get('close_drop_pct', 0.0) or 0.0), 6)};"
                        f"hold_close_drop_threshold_pct={round(float(_hold_close_drop_eval.get('threshold_pct', 0.0) or 0.0), 6)};"
                        f"hold_close_drop_prev_close={round(float(_hold_close_drop_eval.get('prev_close', 0.0) or 0.0), 6)};"
                        f"hold_close_drop_close={round(float(_hold_close_drop_eval.get('close', 0.0) or 0.0), 6)};"
                    )
                    break

            if exit_day is None and ddm_forced_day is not None and str(d) == str(ddm_forced_day):
                exit_day = ddm_forced_day
                exit_price = ddm_forced_price
                exit_reason = ddm_forced_reason
                break

            if sell_rules_enabled and not _in_protected_period:
                fundamental_signals = _check_fundamental_risk(pos, sell_rules)
                if fundamental_signals:
                    fund_sig = next((sig for sig in fundamental_signals if str(sig.get("type", "")) not in executed_sell_tags), fundamental_signals[0])
                    fund_tag = str(fund_sig.get("type", "") or "").strip()
                    if fund_tag and fund_tag not in executed_sell_tags:
                        exit_day = d
                        exit_price = c
                        exit_reason = fund_tag
                        sell_ratio_pct = _pct100_from_config(fund_sig.get("ratio", 100.0), 100.0)
                        sell_tag = fund_tag
                        break

            # SURGE TP: flat take-profit for surge-entered positions (checked before staged TP)
            if _is_surge_pos and _surge_tp_pct is not None and exit_day is None and not _in_protected_period:
                _surge_tp_price = entry_price * (1.0 + float(_surge_tp_pct))
                if o >= _surge_tp_price:
                    exit_day, exit_price, exit_reason = d, o, "SURGE_TP_GAP"
                    tp_hit = True
                    break
                elif h >= _surge_tp_price:
                    exit_day, exit_price, exit_reason = d, _surge_tp_price, "SURGE_TP"
                    tp_hit = True
                    break

            # TP
            # STAGED TAKE PROFIT
            if sell_rules_enabled:
                tp_cfg = sell_rules.get("take_profit", {}) if isinstance(sell_rules.get("take_profit"), dict) else {}
                if bool(tp_cfg.get("enabled", False)):
                    levels, ratios = _resolve_take_profit_plan(asset_type, sell_rules, tp_cfg)
                    for level_raw, ratio_raw in zip(levels, ratios):
                        level_pct = _pct100_from_config(level_raw, 0.0)
                        level_key = str(int(round(level_pct)))
                        if level_key in tp_taken_levels:
                            continue
                        if level_pct <= 0:
                            continue
                        target_price = entry_price * (1.0 + level_pct / 100.0)
                        if o >= target_price or h >= target_price:
                            adjusted_ratio = _adjust_take_profit_ratio(
                                base_ratio_pct=_pct100_from_config(ratio_raw, 0.0),
                                asset_type=asset_type,
                                profit_level_pct=level_pct,
                                tp_cfg=tp_cfg,
                            )
                            if adjusted_ratio <= 0:
                                continue
                            exit_day = d
                            exit_price = o if o >= target_price else target_price
                            exit_reason = f"TP_L{level_key}" + ("_GAP" if o >= target_price else "")
                            tp_hit = True
                            sell_ratio_pct = adjusted_ratio
                            sell_tag = f"TP_L{level_key}"
                            break
                    if exit_day is not None:
                        break

                tech_cfg = sell_rules.get("technical", {}) if isinstance(sell_rules.get("technical"), dict) else {}
                if bool(tech_cfg.get("enabled", False)):
                    tech_eval = _evaluate_technical_exit(
                        date_ymd=str(d),
                        close_price=float(c),
                        entry_price=float(entry_price),
                        current_profit_pct=float(current_profit_pct),
                        hist_closes=hist_closes,
                        executed_sell_tags=executed_sell_tags,
                        tech_cfg=tech_cfg,
                    )
                    if tech_eval.get("triggered"):
                        exit_day = d
                        exit_price = float(tech_eval.get("exit_price", c))
                        exit_reason = str(tech_eval.get("exit_reason", "TECHNICAL"))
                        sell_ratio_pct = float(tech_eval.get("sell_ratio_pct", sell_ratio_pct))
                        sell_tag = str(tech_eval.get("sell_tag", ""))
                        break

                market_cfg = sell_rules.get("market_risk", {}) if isinstance(sell_rules.get("market_risk"), dict) else {}
                if bool(market_cfg.get("enabled", False)):
                    market_eval = _evaluate_market_risk_exit(
                        date_ymd=str(d),
                        close_price=float(c),
                        current_profit_pct=float(current_profit_pct),
                        vix_proxy=vix_proxy,
                        fx_ctx=fx_ctx,
                        macro_snapshot=macro_snapshot,
                        p0_snapshot=p0_snapshot,
                        market_regime=market_regime,
                        market_cfg=market_cfg,
                        executed_sell_tags=executed_sell_tags,
                    )
                    if market_eval.get("triggered"):
                        exit_day = d
                        exit_price = float(market_eval.get("exit_price", c))
                        exit_reason = str(market_eval.get("exit_reason", "MARKET_RISK"))
                        sell_ratio_pct = float(market_eval.get("sell_ratio_pct", sell_ratio_pct))
                        sell_tag = str(market_eval.get("sell_tag", ""))
                        break

                dropout_enabled = bool(candidate_dropout_cfg.get("enabled", False))
                dropout_min_hold_days = max(0, _to_int(candidate_dropout_cfg.get("min_hold_days"), 2))
                dropout_runtime_blocked = bool(candidate_dropout_cfg.get("_runtime_blocked", False))
                if (
                    dropout_enabled
                    and (not dropout_runtime_blocked)
                    and len(active_candidate_codes) > 0
                    and i >= dropout_min_hold_days
                    and code not in active_candidate_codes
                    and code not in candidate_dropout_sold_codes
                    and "CANDIDATE_DROPOUT" not in executed_sell_tags
                ):
                    exit_day = d
                    exit_price = c
                    exit_reason = "CANDIDATE_DROPOUT"
                    sell_ratio_pct = _pct100_from_config(candidate_dropout_cfg.get("sell_ratio_pct", 30.0), 30.0)
                    sell_tag = "CANDIDATE_DROPOUT"
                    candidate_dropout_sold_codes.add(code)
                    break

            # TIME (trading-day basis)
            if hold_days_trading >= position_max_hold_days:
                print(
                    f"[PAPER_ENGINE] TIME trigger: entry_date={entry_date} exit_day={d} "
                    f"hold_days_trading={hold_days_trading} hold_days_calendar={hold_days_calendar} "
                    f"max_hold_days={position_max_hold_days}"
                )
                exit_day, exit_price, exit_reason = d, c, "TIME"
                break

            if (
                _is_surge_pos
                and bool(_surge_reversal_cfg.get("enabled", False))
                and exit_day is None
                and len(hist_closes) >= 4
                and len(hist_volumes) >= 4
            ):
                _surge_high_since_entry = float(
                    pd.to_numeric(code_px.loc[code_px["date"] <= d, "high"], errors="coerce").fillna(0.0).max()
                )
                _surge_reversal_signals = _calc_surge_reversal_signals(
                    close_hist=hist_closes,
                    volume_hist=hist_volumes,
                    surge_volume=float(_surge_entry_volume),
                    high_since_entry=max(float(entry_price), _surge_high_since_entry),
                    reversal_cfg=_surge_reversal_cfg,
                )
                _surge_trigger_count = max(1, int(_to_int(_surge_reversal_cfg.get("trigger_count"), 2)))
                if len(_surge_reversal_signals) >= _surge_trigger_count:
                    _surge_reversal_pending = True
                    _surge_reversal_count = len(_surge_reversal_signals)
                    print(
                        f"[SURGE_REVERSAL_ARM] code={code} date={d} signals={','.join(_surge_reversal_signals)} "
                        f"trigger_count={_surge_trigger_count}"
                    )
            # update trailing reference only after intraday exits are checked (avoid look-ahead)
            max_close = max(max_close, float(c))
    if exit_day and exit_price is not None and float(exit_price) > 0:
        if str(exit_reason or "").startswith("DDM_LIQUIDATE_") and ddm_forced_sell_ratio_pct is not None:
            sell_ratio_pct = float(ddm_forced_sell_ratio_pct)
        elif _is_surge_pos:
            if str(exit_reason) in ("STOP", "STOP_GAP", "STOP_PREEMPTIVE_CLOSE", "REVERSAL_NEXT_OPEN", "SURGE_INTRADAY_REVERSAL", "TRAIL", "TRAIL_GAP"):
                fallback_ratio_pct = float(_surge_stop_sell_ratio_pct)
                if str(exit_reason) == "STOP_PREEMPTIVE_CLOSE":
                    fallback_ratio_pct = float(_surge_preemptive_sell_ratio_pct)
                if _surge_dynamic_exit_ratio_enabled:
                    _kill_switch_ctx = p0_snapshot.get("kill_switch", {}) if isinstance(p0_snapshot.get("kill_switch"), dict) else {}
                    _severity_eval = _calc_surge_exit_severity(
                        exit_reason=str(exit_reason),
                        hold_days_trading=int(hold_days_trading),
                        entry_price=float(entry_price),
                        exit_price=float(exit_price),
                        atr14_pct=float(_effective_atr14_pct),
                        is_surge_pos=bool(_is_surge_pos),
                        market_regime=str(market_regime or ""),
                        kill_switch_active=bool(_kill_switch_ctx.get("triggered", False)),
                        reversal_count=int(_surge_reversal_count),
                        prior_stop_count=int(_prior_stop_count),
                        dynamic_cfg=_surge_dynamic_exit_ratio_cfg,
                    )
                    _exit_severity = str(_severity_eval.get("severity", "") or "").strip().upper()
                    _exit_severity_reasons = [str(v).strip() for v in (_severity_eval.get("severity_reasons", []) or []) if str(v).strip()]
                    _gap_loss_pct = float(_severity_eval.get("gap_loss_pct", 0.0) or 0.0)
                    sell_ratio_pct = _resolve_surge_exit_ratio_pct(
                        severity=_exit_severity,
                        dynamic_cfg=_surge_dynamic_exit_ratio_cfg,
                        fallback_pct=fallback_ratio_pct,
                    )
                else:
                    sell_ratio_pct = fallback_ratio_pct
        else:
            if str(exit_reason) == "STOP":
                sell_ratio_pct = float(general_stop_sell_ratio_pct)
            elif str(exit_reason) == "STOP_GAP":
                sell_ratio_pct = float(general_stop_gap_sell_ratio_pct)
            elif str(exit_reason) == "STOP_PREEMPTIVE_CLOSE":
                sell_ratio_pct = float(general_preemptive_sell_ratio_pct)
            elif str(exit_reason) == "NORMAL_EARLY_GAP_DOWN_EXIT":
                sell_ratio_pct = float(_normal_gap_down_sell_ratio_pct)
            elif str(exit_reason) == "NORMAL_EARLY_GAP_UP_TP":
                sell_ratio_pct = float(_normal_gap_up_sell_ratio_pct)
        if entry_dt_obj is not None:
            try:
                hold_days_calendar = (datetime.strptime(str(exit_day), "%Y%m%d") - entry_dt_obj).days
            except Exception:
                hold_days_calendar = hold_days_trading
        else:
            hold_days_calendar = hold_days_trading
        sell_qty = _calc_sell_qty(qty, sell_ratio_pct)
        _src_oid_for_guard = str(pos.get("source_order_id") or pos.get("entry_order_id") or "").strip()
        _lineage_guard = _entry_order_remaining_qty_for_sell(
            code=code,
            entry_order_id=_src_oid_for_guard,
            pending_fills=fills_new,
            schema=schema,
        )
        if bool(_lineage_guard.get("known")):
            _remaining_qty = int(_lineage_guard.get("remaining_qty") or 0)
            if sell_qty > _remaining_qty:
                _exit_extra_note_fields += (
                    f"lineage_guard_requested_sell_qty={sell_qty};"
                    f"lineage_guard_remaining_qty={_remaining_qty};"
                    f"lineage_guard_entry_order_id={_src_oid_for_guard};"
                )
                print(
                    f"[LINEAGE_SELL_QTY_GUARD] code={code} entry_order_id={_src_oid_for_guard} "
                    f"sell_qty={sell_qty}->{_remaining_qty}"
                )
                sell_qty = _remaining_qty
        if sell_qty <= 0:
            pos["max_close"] = max_close
            positions_out.append(pos)
            return {"positions_out": positions_out, "next_seq": int(next_seq)}
        partial_exit = sell_qty < qty
        base_sell_order_id = f"PAPER_SELL_{code}_{exit_day}_{exit_reason}"
        if str(exit_reason) in {"STOP", "STOP_GAP", "STOP_PREEMPTIVE_CLOSE", "REVERSAL_NEXT_OPEN", "SURGE_INTRADAY_REVERSAL"} and int(_prior_stop_count) > 0:
            base_sell_order_id = f"{base_sell_order_id}_R{int(_prior_stop_count) + 1}"
        if base_sell_order_id in existing_fill_order_ids:
            _entry_oid_suffix = re.sub(r"[^A-Za-z0-9]+", "_", str(pos.get("entry_order_id", "") or "").strip()).strip("_")
            if _entry_oid_suffix:
                base_sell_order_id = f"{base_sell_order_id}_{_entry_oid_suffix[-24:]}"
        sell_order_id = base_sell_order_id
        if partial_exit:
            sell_order_id = f"{sell_order_id}_Q{sell_qty}"
        _intraday_rev_note = pos.get("last_surge_intraday_reversal", {}) if isinstance(pos.get("last_surge_intraday_reversal"), dict) else {}
        exit_note = (
            f"exit_reason={exit_reason};"
            f"signal_date={pos.get('signal_date')};"
            f"sell_ratio_pct={round(float(sell_ratio_pct), 4)};"
            f"sell_qty={sell_qty};"
            f"partial_exit={1 if partial_exit else 0};"
            f"hold_days_trading={hold_days_trading};"
            f"hold_days_calendar={hold_days_calendar};"
            f"exit_severity={_exit_severity};"
            f"severity_reasons={','.join(_exit_severity_reasons)};"
            f"gap_loss_pct={round(float(_gap_loss_pct), 6)};"
            f"{_exit_extra_note_fields}"
            f"prior_stop_count={_prior_stop_count};"
            f"sell_tag={sell_tag};"
            f"surge_intraday_reversal_ts={_intraday_rev_note.get('ts', '')};"
            f"surge_intraday_reversal_signals={','.join(_intraday_rev_note.get('signals', []) or [])};"
            f"surge_intraday_reversal_drawdown_pct={round(float(_intraday_rev_note.get('drawdown_pct', 0.0) or 0.0), 6)};"
            f"surge_intraday_reversal_points={_intraday_rev_note.get('points', '')};"
            f"entry_order_id={pos.get('entry_order_id', '')};"
            f"entry_intent_id={pos.get('entry_intent_id', '')};"
            f"entry_trace_id={pos.get('entry_trace_id', '')};"
            f"lineage_origin={pos.get('lineage_origin', '')};"
            f"source_order_id={pos.get('source_order_id', '')};"
            f"source_intent_id={pos.get('source_intent_id', '')};"
            f"source_trace_id={pos.get('source_trace_id', '')};"
            f"replay_chain_id={pos.get('replay_chain_id', '')};"
            f"replay_depth={pos.get('replay_depth', 0)};"
            f"surge_type={pos.get('surge_type', '')};"
            f"surge_score={pos.get('surge_score', '')};"
            f"surge_score_final={pos.get('surge_score_final', '')};"
            f"surge_rvol20={pos.get('surge_rvol20', '')};"
            f"surge_spread_bps={pos.get('surge_spread_bps', '')};"
            f"surge_orderflow_tag={pos.get('surge_orderflow_tag', '')};"
            f"surge_orderflow_risk_score={pos.get('surge_orderflow_risk_score', '')}"
        )

        if schema == "legacy":
            # SELL fill idempotent
            sell_fill_appended = False
            existing_sell_fill_reused = False
            if sell_order_id not in existing_fill_order_ids and base_sell_order_id not in existing_fill_order_ids:
                exit_ts_value = _paper_exit_ts(exit_day)
                fills_new.append([exit_ts_value, code, "SELL", sell_qty, float(exit_price), sell_order_id, exit_note])
                existing_fill_order_ids.add(sell_order_id)
                existing_fill_order_ids.add(base_sell_order_id)
                sell_fill_appended = True
            elif sell_order_id in existing_fill_order_ids:
                existing_sell_fill_reused = True

            # Keep fill/trade lifecycle parity on reruns:
            # when an existing sell fill is reused and its matching trade is missing,
            # recover only the trade row from the deterministic fill/position values.
            if sell_fill_appended or existing_sell_fill_reused:
                pnl_pct = calc_net_ret(entry_price, float(exit_price), fee_pct, pos_slip_pct, sell_tax_pct)
                # trade signature idempotent
                sig = "|".join([
                    code, entry_date, _sig_float(entry_price), exit_day, _sig_float(float(exit_price)),
                    _sig_float(round(pnl_pct, 8)), str(exit_reason), _sig_float(sell_qty),
                    f"signal_date={pos.get('signal_date')}",
                ])
                # source_order_id 기반 추가 중복 방지 (RECOVERED 다중실행 버그 방어)
                _src_oid = str(pos.get("source_order_id") or pos.get("entry_order_id") or "").strip()
                # partial exits from the same source order are valid distinct exits;
                # keep source_order_id dedupe only for full exits.
                _src_oid_blocked = bool((not partial_exit) and _src_oid and _src_oid in committed_source_order_ids)
                if sig not in existing_trade_sigs and not _src_oid_blocked:
                    trade_id = f"T{next_seq:06d}"
                    pnl_krw = round(float(pnl_pct) * float(entry_price) * float(sell_qty), 2)
                    trades_new.append([trade_id, code, entry_date, entry_price, exit_day, float(exit_price), round(pnl_pct, 8), pnl_krw, exit_reason, exit_note, int(_truthy(pos.get("_surge_immediate", 0)))])
                    existing_trade_sigs.add(sig)
                    if _src_oid:
                        committed_source_order_ids.add(_src_oid)
                    next_seq += 1

        else:
            # v41.1
            fee = calc_roundtrip_fee(entry_price, float(exit_price), sell_qty, fee_pct)
            slp = calc_roundtrip_slippage(entry_price, float(exit_price), sell_qty, pos_slip_pct)
            sell_fill_appended = False
            existing_sell_fill_reused = False
            exit_ts_value = _paper_exit_ts(exit_day)
            if sell_order_id not in existing_fill_order_ids and base_sell_order_id not in existing_fill_order_ids:
                fills_new.append([exit_ts_value, exit_day, code, name, "SELL", sell_qty, float(exit_price),
                                  round(calc_exit_fee(float(exit_price), sell_qty, fee_pct), 6),
                                  round(float(exit_price) * sell_qty * pos_slip_pct, 6),
                                  sell_order_id, exit_note])
                existing_fill_order_ids.add(sell_order_id)
                existing_fill_order_ids.add(base_sell_order_id)
                sell_fill_appended = True
            elif sell_order_id in existing_fill_order_ids:
                existing_sell_fill_reused = True
            gross = (float(exit_price) - entry_price) / entry_price
            net = calc_net_ret(entry_price, float(exit_price), fee_pct, pos_slip_pct, sell_tax_pct)
            trade_probe = pd.Series({
                "code": code,
                "entry_ts": entry_ts,
                "exit_ts": exit_ts_value,
                "qty": sell_qty,
                "entry_price": round(entry_price, 6),
                "exit_price": round(float(exit_price), 6),
                "net_ret": round(net, 8),
                "note": exit_note,
            })
            sig = _v411_trade_sig(trade_probe)
            if (sell_fill_appended or existing_sell_fill_reused) and sig not in existing_trade_sigs:
                trade_id = f"T{next_seq:06d}"
                trades_new.append([trade_id, entry_ts, exit_ts_value, code, name, "LONG", sell_qty,
                                   round(entry_price, 6), round(float(exit_price), 6), round(gross, 8), round(net, 8),
                                   round(fee, 6), round(slp, 6),
                                   "1" if stop_hit else "0", "1" if tp_hit else "0", "1" if trail_hit else "0",
                                   exit_note])
                existing_trade_sigs.add(sig)
                next_seq += 1
        if not sell_fill_appended:
            pos["max_close"] = max_close
            positions_out.append(pos)
            return {"positions_out": positions_out, "next_seq": int(next_seq)}
        if partial_exit:
            pos["qty"] = max(0, qty - sell_qty)
            # reflect exit_price into max_close: TP triggered means price reached exit_price,
            # so trailing stop for remaining position should be based on at least that level
            pos["max_close"] = max(max_close, float(exit_price))
            if str(exit_reason) in {"STOP", "STOP_GAP", "STOP_PREEMPTIVE_CLOSE", "REVERSAL_NEXT_OPEN", "SURGE_INTRADAY_REVERSAL"}:
                pos["stop_exit_count"] = int(_prior_stop_count) + 1
                pos["last_exit_severity"] = str(_exit_severity or "")
            pos["executed_sell_tags"] = sorted(set(_pos_list(pos, "executed_sell_tags") + ([sell_tag] if sell_tag else [])))
            if sell_tag.startswith("TP_L"):
                level_key = sell_tag.replace("TP_L", "")
                pos["tp_taken_levels"] = sorted(set(_pos_list(pos, "tp_taken_levels") + [level_key]))
            positions_out.append(pos)
    else:
        pos["max_close"] = max_close
        positions_out.append(pos)


    return {
        "positions_out": positions_out,
        "next_seq": int(next_seq),
    }




def _collect_persisted_today_sell_artifacts(schema: str, runtime_ymd: str) -> Dict[str, Any]:
    target_ymd = _norm_ymd_text(runtime_ymd)
    fills_df = read_csv_safe(FILLS)
    trades_df = read_csv_safe(TRADES)

    sell_fills: List[Dict[str, Any]] = []
    sell_trades: List[Dict[str, Any]] = []

    if isinstance(fills_df, pd.DataFrame) and not fills_df.empty:
        fills_ymd = pd.Series("", index=fills_df.index, dtype="object")
        if "date" in fills_df.columns:
            fills_ymd = fills_df["date"].map(_norm_ymd_text)
        elif "ts" in fills_df.columns:
            fills_ymd = fills_df["ts"].map(_extract_ymd_from_ts_text)
        elif "datetime" in fills_df.columns:
            fills_ymd = fills_df["datetime"].map(_extract_ymd_from_ts_text)
        fills_today = fills_df.loc[fills_ymd == target_ymd].copy()
        if not fills_today.empty and "side" in fills_today.columns:
            side = fills_today["side"].astype(str).str.strip().str.upper()
            fills_today = fills_today.loc[side == "SELL"].copy()
            for _, row in fills_today.iterrows():
                note_fields = _parse_note_fields(row.get("note", ""))
                order_id = str(row.get("order_id") or "").strip()
                base_order_id = order_id
                if "_Q" in base_order_id:
                    base_order_id = base_order_id.rsplit("_Q", 1)[0]
                sell_fills.append(
                    {
                        "order_id": order_id,
                        "base_order_id": base_order_id,
                        "code": str(row.get("code") or "").strip().zfill(6),
                        "runtime_ymd": target_ymd,
                        "qty": int(_to_int(row.get("qty"), 0)),
                        "price": float(_to_float(row.get("price"), 0.0) or 0.0),
                        "exit_reason": str(note_fields.get("exit_reason") or "").strip().upper(),
                        "sell_qty": int(_to_int(note_fields.get("sell_qty"), row.get("qty"))),
                        "partial_exit": bool(_truthy(note_fields.get("partial_exit", 0))),
                        "source_order_id": str(note_fields.get("source_order_id") or "").strip(),
                        "lineage_origin": str(note_fields.get("lineage_origin") or "").strip(),
                        "replay_chain_id": str(note_fields.get("replay_chain_id") or "").strip(),
                        "note_fields": note_fields,
                    }
                )

    if isinstance(trades_df, pd.DataFrame) and not trades_df.empty:
        trades_ymd = pd.Series("", index=trades_df.index, dtype="object")
        if "exit_date" in trades_df.columns:
            trades_ymd = trades_df["exit_date"].map(_norm_ymd_text)
        elif "exit_ts" in trades_df.columns:
            trades_ymd = trades_df["exit_ts"].map(_extract_ymd_from_ts_text)
        trades_today = trades_df.loc[trades_ymd == target_ymd].copy()
        if not trades_today.empty:
            for _, row in trades_today.iterrows():
                note = str(row.get("note") or row.iloc[-1] if len(row.index) > 0 else "")
                note_fields = _parse_note_fields(note)
                exit_reason = str(row.get("exit_reason") or "").strip().upper()
                if not exit_reason:
                    exit_reason = str(note_fields.get("exit_reason") or "").strip().upper()
                code = str(row.get("code") or "").strip().zfill(6)
                if not code and len(row.index) >= 2:
                    code = str(row.iloc[1] or "").strip().zfill(6)
                sell_qty = _to_int(note_fields.get("sell_qty"), 0)
                if sell_qty <= 0 and "qty" in trades_today.columns:
                    sell_qty = _to_int(row.get("qty"), 0)
                sell_trades.append(
                    {
                        "trade_id": str(row.get("trade_id") or row.iloc[0] if len(row.index) > 0 else "").strip(),
                        "code": code,
                        "runtime_ymd": target_ymd,
                        "exit_reason": exit_reason,
                        "sell_qty": int(max(0, sell_qty)),
                        "partial_exit": bool(_truthy(note_fields.get("partial_exit", 0))),
                        "source_order_id": str(note_fields.get("source_order_id") or "").strip(),
                        "lineage_origin": str(note_fields.get("lineage_origin") or "").strip(),
                        "replay_chain_id": str(note_fields.get("replay_chain_id") or "").strip(),
                        "note_fields": note_fields,
                    }
                )

    return {
        "sell_fills": sell_fills,
        "sell_trades": sell_trades,
    }


def _build_sell_order_lifecycle_summary(schema: str, runtime_ymd: str) -> Dict[str, Any]:
    artifacts = _collect_persisted_today_sell_artifacts(schema=schema, runtime_ymd=runtime_ymd)
    sell_fills = list(artifacts.get("sell_fills") or [])
    sell_trades = list(artifacts.get("sell_trades") or [])

    trade_key_counts: Dict[Tuple[str, str, int, str], int] = {}
    for trade in sell_trades:
        key = (
            str(trade.get("code") or "").strip(),
            str(trade.get("exit_reason") or "").strip().upper(),
            int(_to_int(trade.get("sell_qty"), 0)),
            str(trade.get("source_order_id") or "").strip(),
        )
        trade_key_counts[key] = int(trade_key_counts.get(key, 0)) + 1

    rows: List[Dict[str, Any]] = []
    partial_rows = 0
    full_rows = 0
    reconciled_rows = 0
    unreconciled_rows = 0
    reasons: Dict[str, int] = {}
    issues: List[str] = []

    for fill in sell_fills:
        code = str(fill.get("code") or "").strip()
        exit_reason = str(fill.get("exit_reason") or "").strip().upper()
        sell_qty = int(_to_int(fill.get("sell_qty"), 0))
        source_order_id = str(fill.get("source_order_id") or "").strip()
        partial_exit = bool(fill.get("partial_exit"))
        state = "FILLED_PARTIAL" if partial_exit else "FILLED_FULL"
        if partial_exit:
            partial_rows += 1
        else:
            full_rows += 1
        reasons[exit_reason or "UNKNOWN"] = int(reasons.get(exit_reason or "UNKNOWN", 0)) + 1
        match_key = (code, exit_reason, sell_qty, source_order_id)
        matched = int(trade_key_counts.get(match_key, 0)) > 0
        if matched:
            trade_key_counts[match_key] = int(trade_key_counts.get(match_key, 0)) - 1
            reconciled_rows += 1
        else:
            unreconciled_rows += 1
            issues.append(f"sell_fill_missing_trade:{code}:{exit_reason or 'UNKNOWN'}:{sell_qty}")
        rows.append(
            {
                "order_id": str(fill.get("order_id") or ""),
                "base_order_id": str(fill.get("base_order_id") or ""),
                "code": code,
                "sell_qty": sell_qty,
                "exit_reason": exit_reason,
                "state": state,
                "partial_exit": partial_exit,
                "reconciled_trade": matched,
                "source_order_id": source_order_id,
                "lineage_origin": str(fill.get("lineage_origin") or ""),
                "replay_chain_id": str(fill.get("replay_chain_id") or ""),
            }
        )

    orphan_trade_rows = int(sum(max(0, int(v)) for v in trade_key_counts.values()))
    if orphan_trade_rows > 0:
        issues.append(f"orphan_sell_trades:{orphan_trade_rows}")

    status = "PASS" if not issues else "FAIL"
    return {
        "generated_at": now_ts(),
        "runtime_ymd": str(_norm_ymd_text(runtime_ymd) or ""),
        "status": status,
        "counts": {
            "sell_fill_rows": int(len(sell_fills)),
            "sell_trade_rows": int(len(sell_trades)),
            "filled_partial_rows": int(partial_rows),
            "filled_full_rows": int(full_rows),
            "reconciled_rows": int(reconciled_rows),
            "unreconciled_rows": int(unreconciled_rows),
            "orphan_trade_rows": int(orphan_trade_rows),
        },
        "states_present": sorted(set(str(row.get("state") or "") for row in rows if str(row.get("state") or ""))),
        "reason_counts": reasons,
        "rows": rows[:50],
        "issues": sorted(set(issues)),
    }


def _build_partial_exit_policy_summary(schema: str, runtime_ymd: str) -> Dict[str, Any]:
    artifacts = _collect_persisted_today_sell_artifacts(schema=schema, runtime_ymd=runtime_ymd)
    sell_fills = list(artifacts.get("sell_fills") or [])
    sell_trades = list(artifacts.get("sell_trades") or [])

    partial_rows: List[Dict[str, Any]] = []
    ratio_counts: Dict[str, int] = {}
    reason_counts: Dict[str, int] = {}
    fill_order_counts: Dict[str, int] = {}
    trade_signature_counts: Dict[Tuple[str, str, int, str, str, str, int, str], int] = {}
    issues: List[str] = []

    partial_fill_rows = 0
    full_fill_rows = 0
    repeat_stop_rows = 0

    for fill in sell_fills:
        order_id = str(fill.get("order_id") or "").strip()
        if order_id:
            fill_order_counts[order_id] = int(fill_order_counts.get(order_id, 0)) + 1

        note_fields = dict(fill.get("note_fields") or {})
        exit_reason = str(fill.get("exit_reason") or "").strip().upper()
        partial_exit = bool(fill.get("partial_exit"))
        sell_ratio_pct = float(_to_float(note_fields.get("sell_ratio_pct"), 0.0) or 0.0)
        prior_stop_count = int(_to_int(note_fields.get("prior_stop_count"), 0))
        if partial_exit:
            partial_fill_rows += 1
        else:
            full_fill_rows += 1
        if prior_stop_count > 0:
            repeat_stop_rows += 1
        reason_counts[exit_reason or "UNKNOWN"] = int(reason_counts.get(exit_reason or "UNKNOWN", 0)) + 1
        ratio_key = f"{sell_ratio_pct:.4f}".rstrip("0").rstrip(".") if sell_ratio_pct else "0"
        ratio_counts[ratio_key] = int(ratio_counts.get(ratio_key, 0)) + 1
        partial_rows.append(
            {
                "order_id": order_id,
                "base_order_id": str(fill.get("base_order_id") or "").strip(),
                "code": str(fill.get("code") or "").strip(),
                "exit_reason": exit_reason,
                "sell_qty": int(_to_int(fill.get("sell_qty"), 0)),
                "sell_ratio_pct": sell_ratio_pct,
                "partial_exit": partial_exit,
                "prior_stop_count": prior_stop_count,
                "gap_loss_pct": float(_to_float(note_fields.get("gap_loss_pct"), 0.0) or 0.0),
                "severity": str(note_fields.get("exit_severity") or "").strip().upper(),
                "severity_reasons": str(note_fields.get("severity_reasons") or "").strip(),
                "sell_tag": str(note_fields.get("sell_tag") or "").strip(),
                "source_order_id": str(fill.get("source_order_id") or "").strip(),
                "lineage_origin": str(fill.get("lineage_origin") or "").strip(),
                "replay_chain_id": str(fill.get("replay_chain_id") or "").strip(),
            }
        )

    for trade in sell_trades:
        note_fields = dict(trade.get("note_fields") or {})
        sell_ratio_pct = float(_to_float(note_fields.get("sell_ratio_pct"), 0.0) or 0.0)
        ratio_key = f"{sell_ratio_pct:.4f}".rstrip("0").rstrip(".") if sell_ratio_pct else "0"
        partial_exit_key = str(_to_int(note_fields.get("partial_exit"), 0))
        prior_stop_count = int(_to_int(note_fields.get("prior_stop_count"), 0))
        signature = (
            str(trade.get("code") or "").strip(),
            str(trade.get("exit_reason") or "").strip().upper(),
            int(_to_int(trade.get("sell_qty"), 0)),
            str(note_fields.get("signal_date") or "").strip(),
            ratio_key,
            partial_exit_key,
            prior_stop_count,
            str(trade.get("replay_chain_id") or note_fields.get("replay_chain_id") or "").strip(),
        )
        trade_signature_counts[signature] = int(trade_signature_counts.get(signature, 0)) + 1

    duplicate_fill_order_ids = sorted([k for k, v in fill_order_counts.items() if k and int(v) > 1])
    duplicate_trade_signatures = [
        {
            "code": key[0],
            "exit_reason": key[1],
            "sell_qty": key[2],
            "signal_date": key[3],
            "sell_ratio_pct": key[4],
            "partial_exit": key[5],
            "prior_stop_count": key[6],
            "replay_chain_id": key[7],
            "count": int(count),
        }
        for key, count in trade_signature_counts.items()
        if int(count) > 1
    ]
    duplicate_trade_signatures = cast(List[Dict[str, Any]], sorted(
        cast(List[Dict[str, Any]], duplicate_trade_signatures),
        key=lambda x: (
            str(x.get("code") or ""),
            str(x.get("exit_reason") or ""),
            int(x.get("sell_qty") or 0),
            str(x.get("signal_date") or ""),
            str(x.get("sell_ratio_pct") or ""),
            str(x.get("partial_exit") or ""),
            int(x.get("prior_stop_count") or 0),
            str(x.get("replay_chain_id") or ""),
        ),
    ))

    if duplicate_fill_order_ids:
        issues.append(f"duplicate_fill_order_ids:{len(duplicate_fill_order_ids)}")
    if duplicate_trade_signatures:
        issues.append(f"duplicate_trade_signatures:{len(duplicate_trade_signatures)}")

    status = "PASS" if not issues else "FAIL"
    return {
        "generated_at": now_ts(),
        "runtime_ymd": str(_norm_ymd_text(runtime_ymd) or ""),
        "status": status,
        "counts": {
            "sell_fill_rows": int(len(sell_fills)),
            "sell_trade_rows": int(len(sell_trades)),
            "partial_fill_rows": int(partial_fill_rows),
            "full_fill_rows": int(full_fill_rows),
            "repeat_stop_rows": int(repeat_stop_rows),
            "unique_source_order_ids": int(len({str(row.get("source_order_id") or "").strip() for row in partial_rows if str(row.get("source_order_id") or "").strip()})),
        },
        "reason_counts": reason_counts,
        "sell_ratio_pct_counts": ratio_counts,
        "rows": partial_rows[:50],
        "duplicate_precheck": {
            "status": status,
            "duplicate_fill_order_ids": duplicate_fill_order_ids,
            "duplicate_trade_signatures": duplicate_trade_signatures[:50],
            "issues": list(issues),
        },
        "issues": list(issues),
    }


def _build_sell_recovery_chain_summary(schema: str, runtime_ymd: str) -> Dict[str, Any]:
    artifacts = _collect_persisted_today_sell_artifacts(schema=schema, runtime_ymd=runtime_ymd)
    sell_fills = list(artifacts.get("sell_fills") or [])
    sell_trades = list(artifacts.get("sell_trades") or [])

    trade_key_counts: Dict[Tuple[str, str, int, str, str, str], int] = {}
    for trade in sell_trades:
        key = (
            str(trade.get("code") or "").strip(),
            str(trade.get("exit_reason") or "").strip().upper(),
            int(_to_int(trade.get("sell_qty"), 0)),
            str(trade.get("source_order_id") or "").strip(),
            str(trade.get("lineage_origin") or "").strip().upper(),
            str(trade.get("replay_chain_id") or "").strip(),
        )
        trade_key_counts[key] = int(trade_key_counts.get(key, 0)) + 1

    lineage_counts: Dict[str, int] = {}
    missing_source_order_id = 0
    missing_lineage_origin = 0
    missing_replay_chain_id = 0
    trade_chain_matched_rows = 0
    trade_chain_unmatched_rows = 0
    issues: List[str] = []
    rows: List[Dict[str, Any]] = []

    replay_required_origins = {"RECOVERED_FROM_FILLS", "OPEN_ORDER_REPLAY"}

    for fill in sell_fills:
        code = str(fill.get("code") or "").strip()
        exit_reason = str(fill.get("exit_reason") or "").strip().upper()
        sell_qty = int(_to_int(fill.get("sell_qty"), 0))
        source_order_id = str(fill.get("source_order_id") or "").strip()
        lineage_origin = str(fill.get("lineage_origin") or "").strip().upper()
        replay_chain_id = str(fill.get("replay_chain_id") or "").strip()
        chain_issues: List[str] = []

        lineage_counts[lineage_origin or "UNKNOWN"] = int(lineage_counts.get(lineage_origin or "UNKNOWN", 0)) + 1
        if lineage_origin in replay_required_origins and not source_order_id:
            missing_source_order_id += 1
            chain_issues.append("source_order_id_missing")
        if not lineage_origin:
            missing_lineage_origin += 1
            chain_issues.append("lineage_origin_missing")
        if lineage_origin in replay_required_origins and not replay_chain_id:
            missing_replay_chain_id += 1
            chain_issues.append("replay_chain_id_missing")

        trade_key = (code, exit_reason, sell_qty, source_order_id, lineage_origin, replay_chain_id)
        trade_matched = int(trade_key_counts.get(trade_key, 0)) > 0
        if trade_matched:
            trade_key_counts[trade_key] = int(trade_key_counts.get(trade_key, 0)) - 1
            trade_chain_matched_rows += 1
        else:
            trade_chain_unmatched_rows += 1
            chain_issues.append("trade_chain_unmatched")

        rows.append(
            {
                "order_id": str(fill.get("order_id") or "").strip(),
                "base_order_id": str(fill.get("base_order_id") or "").strip(),
                "code": code,
                "exit_reason": exit_reason,
                "sell_qty": sell_qty,
                "source_order_id": source_order_id,
                "lineage_origin": lineage_origin,
                "replay_chain_id": replay_chain_id,
                "trade_chain_matched": bool(trade_matched),
                "issues": chain_issues,
            }
        )
        issues.extend(chain_issues)

    orphan_trade_chain_rows = int(sum(max(0, int(v)) for v in trade_key_counts.values()))
    if orphan_trade_chain_rows > 0:
        issues.append(f"orphan_trade_chain_rows:{orphan_trade_chain_rows}")

    unique_issues = sorted(set(str(x) for x in issues if str(x).strip()))
    status = "PASS" if not unique_issues else "FAIL"
    return {
        "generated_at": now_ts(),
        "runtime_ymd": str(_norm_ymd_text(runtime_ymd) or ""),
        "status": status,
        "counts": {
            "sell_fill_rows": int(len(sell_fills)),
            "sell_trade_rows": int(len(sell_trades)),
            "missing_source_order_id_rows": int(missing_source_order_id),
            "missing_lineage_origin_rows": int(missing_lineage_origin),
            "missing_replay_chain_id_rows": int(missing_replay_chain_id),
            "trade_chain_matched_rows": int(trade_chain_matched_rows),
            "trade_chain_unmatched_rows": int(trade_chain_unmatched_rows),
            "orphan_trade_chain_rows": int(orphan_trade_chain_rows),
        },
        "lineage_origin_counts": lineage_counts,
        "rows": rows[:50],
        "issues": unique_issues,
    }


def _build_sell_validation_report(
    runtime_ymd: str,
    persisted_today_metrics: Dict[str, Any],
    sell_order_lifecycle_summary: Dict[str, Any],
    entry_exit_lifecycle: Dict[str, Any],
    partial_exit_policy_summary: Dict[str, Any],
    sell_recovery_chain_summary: Dict[str, Any],
    symbol_stop_summary: Dict[str, Any],
) -> Dict[str, Any]:
    lifecycle_counts = _get_dict(sell_order_lifecycle_summary, "counts")
    entry_exit_counts = entry_exit_lifecycle if isinstance(entry_exit_lifecycle, dict) else {}
    partial_exit_counts = _get_dict(partial_exit_policy_summary, "counts")
    recovery_chain_counts = _get_dict(sell_recovery_chain_summary, "counts")
    symbol_stop_counts = _get_dict(symbol_stop_summary, "counts")
    sell_fill_rows = int(lifecycle_counts.get("sell_fill_rows", persisted_today_metrics.get("exit_fill_rows", 0)) or 0)
    sell_trade_rows = int(lifecycle_counts.get("sell_trade_rows", persisted_today_metrics.get("closed_trade_rows", 0)) or 0)
    partial_fill_rows = int(lifecycle_counts.get("filled_partial_rows", 0) or 0)
    partial_full_rows = int(lifecycle_counts.get("filled_full_rows", 0) or 0)
    stop_sell_rows = int(persisted_today_metrics.get("stop_sell_rows", 0) or 0)
    stop_trade_rows = int(persisted_today_metrics.get("stop_trade_rows", 0) or 0)
    issues: List[str] = []
    if sell_fill_rows != sell_trade_rows:
        issues.append(f"sell_fill_trade_mismatch:{sell_fill_rows}!={sell_trade_rows}")
    if stop_sell_rows != stop_trade_rows:
        issues.append(f"stop_sell_trade_mismatch:{stop_sell_rows}!={stop_trade_rows}")
    if int(lifecycle_counts.get("orphan_trade_rows", 0) or 0) > 0:
        issues.append(f"orphan_trade_rows:{int(lifecycle_counts.get('orphan_trade_rows', 0) or 0)}")
    if int(lifecycle_counts.get("unreconciled_rows", 0) or 0) > 0:
        issues.append(f"unreconciled_sell_rows:{int(lifecycle_counts.get('unreconciled_rows', 0) or 0)}")

    after_close_alignment: Dict[str, Any] = {
        "status": "NA",
        "path": str(AFTER_CLOSE_SUMMARY_LAST_PATH),
        "comparison_target_ymd": str(_norm_ymd_text(runtime_ymd) or ""),
        "fills_d_ymd": "",
        "after_close_as_of_ymd": "",
        "after_close_sell_count": None,
        "after_close_execution_rows": None,
        "skipped_reason": "",
        "issues": [],
    }
    try:
        if AFTER_CLOSE_SUMMARY_LAST_PATH.exists():
            after_close_obj = json.loads(AFTER_CLOSE_SUMMARY_LAST_PATH.read_text(encoding="utf-8"))
            after_close_exec = after_close_obj.get("executions") if isinstance(after_close_obj.get("executions"), dict) else {}
            after_close_sells = list(after_close_exec.get("sells") or [])
            after_close_as_of_ymd = _norm_ymd_text(after_close_obj.get("as_of_ymd"))
            after_close_generated_at = str(after_close_obj.get("generated_at") or "")
            after_close_sell_count = int(_to_int(after_close_exec.get("sell_count"), len(after_close_sells)))
            after_close_execution_rows = int(len(after_close_sells))
            runtime_ymd_norm = str(_norm_ymd_text(runtime_ymd) or "")
            fills_d_ymd = str(_derive_d_from_fills_path(FILLS) or "")
            latest_sell_fill_ts = ""
            stale_last_before_runtime_sells = False
            try:
                fills_df = read_csv_safe(FILLS)
                if isinstance(fills_df, pd.DataFrame) and not fills_df.empty and "side" in fills_df.columns:
                    dt_col = next((c for c in ["datetime", "ts", "date"] if c in fills_df.columns), None)
                    if dt_col:
                        sells_df = fills_df[
                            fills_df["side"].astype(str).str.strip().str.upper().eq("SELL")
                            & fills_df[dt_col].map(_extract_ymd_from_ts_text).eq(runtime_ymd_norm)
                        ].copy()
                        if not sells_df.empty:
                            latest_sell_fill_ts = str(sells_df[dt_col].astype(str).max())
                            summary_digits = re.sub(r"[^0-9]", "", after_close_generated_at)
                            latest_digits = re.sub(r"[^0-9]", "", latest_sell_fill_ts)
                            if len(summary_digits) >= 14 and len(latest_digits) >= 14:
                                summary_dt = datetime.strptime(summary_digits[:14], "%Y%m%d%H%M%S")
                                latest_dt = datetime.strptime(latest_digits[:14], "%Y%m%d%H%M%S")
                                stale_last_before_runtime_sells = bool(
                                    after_close_as_of_ymd == runtime_ymd_norm
                                    and sell_fill_rows > after_close_sell_count
                                    and latest_dt > summary_dt
                                )
            except Exception:
                latest_sell_fill_ts = latest_sell_fill_ts or ""
            after_close_issues: List[str] = []
            stale_last_for_non_runtime_day = (
                bool(after_close_as_of_ymd)
                and bool(runtime_ymd_norm)
                and after_close_as_of_ymd != runtime_ymd_norm
                and after_close_as_of_ymd == fills_d_ymd
                and sell_fill_rows == 0
                and sell_trade_rows == 0
            )
            skip_after_close_compare = bool(stale_last_for_non_runtime_day or stale_last_before_runtime_sells)
            if not skip_after_close_compare:
                if after_close_as_of_ymd and after_close_as_of_ymd != runtime_ymd_norm:
                    after_close_issues.append(f"after_close_asof_mismatch:{after_close_as_of_ymd}!={runtime_ymd_norm}")
                if after_close_sell_count != sell_fill_rows:
                    after_close_issues.append(f"after_close_sell_count_mismatch:{after_close_sell_count}!={sell_fill_rows}")
                if after_close_execution_rows != sell_fill_rows:
                    after_close_issues.append(f"after_close_execution_rows_mismatch:{after_close_execution_rows}!={sell_fill_rows}")
            after_close_alignment = {
                "status": "NA" if skip_after_close_compare else ("PASS" if not after_close_issues else "FAIL"),
                "path": str(AFTER_CLOSE_SUMMARY_LAST_PATH),
                "comparison_target_ymd": runtime_ymd_norm,
                "fills_d_ymd": fills_d_ymd,
                "after_close_as_of_ymd": str(after_close_as_of_ymd or ""),
                "after_close_generated_at": after_close_generated_at,
                "after_close_sell_count": int(after_close_sell_count),
                "after_close_execution_rows": int(after_close_execution_rows),
                "latest_sell_fill_ts": latest_sell_fill_ts,
                "skipped_reason": (
                    "after_close_last_matches_fills_d_but_not_runtime_day"
                    if stale_last_for_non_runtime_day
                    else "after_close_last_stale_before_latest_sell_fill"
                    if stale_last_before_runtime_sells
                    else ""
                ),
                "issues": after_close_issues,
            }
            issues.extend(after_close_issues)
        else:
            after_close_alignment["issues"] = ["after_close_summary_missing"]
    except Exception as e:
        after_close_alignment = {
            "status": "FAIL",
            "path": str(AFTER_CLOSE_SUMMARY_LAST_PATH),
            "after_close_as_of_ymd": "",
            "after_close_sell_count": None,
            "after_close_execution_rows": None,
            "issues": [f"after_close_summary_read_failed:{type(e).__name__}"],
        }
        issues.append(f"after_close_summary_read_failed:{type(e).__name__}")

    pending_alignment_issues: List[str] = []
    pending_exit_fill_rows = int(entry_exit_counts.get("exit_fill_rows", 0) or 0)
    pending_closed_trade_rows = int(entry_exit_counts.get("closed_trade_rows", 0) or 0)
    pending_stop_sell_rows = int(symbol_stop_counts.get("stop_sell_rows", 0) or 0)
    pending_stop_trade_rows = int(symbol_stop_counts.get("stop_trade_rows", 0) or 0)
    pending_partial_fill_rows = int(partial_exit_counts.get("partial_fill_rows", 0) or 0)
    pending_recovery_trade_rows = int(recovery_chain_counts.get("trade_chain_matched_rows", 0) or 0)

    if pending_exit_fill_rows != sell_fill_rows:
        pending_alignment_issues.append(f"pending_exit_fill_rows_mismatch:{pending_exit_fill_rows}!={sell_fill_rows}")
    if pending_closed_trade_rows != sell_trade_rows:
        pending_alignment_issues.append(f"pending_closed_trade_rows_mismatch:{pending_closed_trade_rows}!={sell_trade_rows}")
    if pending_stop_sell_rows != stop_sell_rows:
        pending_alignment_issues.append(f"pending_stop_sell_rows_mismatch:{pending_stop_sell_rows}!={stop_sell_rows}")
    if pending_stop_trade_rows != stop_trade_rows:
        pending_alignment_issues.append(f"pending_stop_trade_rows_mismatch:{pending_stop_trade_rows}!={stop_trade_rows}")
    if pending_partial_fill_rows != partial_fill_rows:
        pending_alignment_issues.append(f"pending_partial_fill_rows_mismatch:{pending_partial_fill_rows}!={partial_fill_rows}")
    if pending_recovery_trade_rows != sell_trade_rows:
        pending_alignment_issues.append(f"pending_recovery_trade_rows_mismatch:{pending_recovery_trade_rows}!={sell_trade_rows}")

    pending_alignment = {
        "status": "PASS" if not pending_alignment_issues else "FAIL",
        "pending_exit_fill_rows": int(pending_exit_fill_rows),
        "pending_closed_trade_rows": int(pending_closed_trade_rows),
        "pending_stop_sell_rows": int(pending_stop_sell_rows),
        "pending_stop_trade_rows": int(pending_stop_trade_rows),
        "pending_partial_fill_rows": int(pending_partial_fill_rows),
        "pending_recovery_trade_rows": int(pending_recovery_trade_rows),
        "issues": pending_alignment_issues,
    }
    issues.extend(pending_alignment_issues)

    state_mismatch_counts = {
        "sell_fill_vs_trade": int(abs(sell_fill_rows - sell_trade_rows)),
        "stop_sell_vs_trade": int(abs(stop_sell_rows - stop_trade_rows)),
        "pending_exit_vs_fill": int(abs(pending_exit_fill_rows - sell_fill_rows)),
        "pending_closed_trade_vs_trade": int(abs(pending_closed_trade_rows - sell_trade_rows)),
        "pending_partial_vs_partial_fill": int(abs(pending_partial_fill_rows - partial_fill_rows)),
        "recovery_trade_vs_trade": int(abs(pending_recovery_trade_rows - sell_trade_rows)),
    }

    status = "PASS" if not issues else "FAIL"
    return {
        "generated_at": now_ts(),
        "runtime_ymd": str(_norm_ymd_text(runtime_ymd) or ""),
        "status": status,
        "counts": {
            "sell_fill_rows": int(sell_fill_rows),
            "sell_trade_rows": int(sell_trade_rows),
            "stop_sell_rows": int(stop_sell_rows),
            "stop_trade_rows": int(stop_trade_rows),
            "partial_sell_rows": int(partial_fill_rows),
            "full_sell_rows": int(partial_full_rows),
        },
        "issues": issues,
        "sell_order_lifecycle_status": str(sell_order_lifecycle_summary.get("status") or ""),
        "stop_reasons": list(persisted_today_metrics.get("stop_reasons", []) or []),
        "after_close_alignment": after_close_alignment,
        "pending_alignment": pending_alignment,
        "state_mismatch_counts": state_mismatch_counts,
    }


def _build_legacy_derisk_summary(
    schema: str,
    runtime_ymd: str,
    ddm_enabled: bool,
    ddm_action: Any,
    ddm_force_liquidate_pct: float,
    ddm_liquidation_targets: set[str],
    open_positions: List[Dict[str, Any]],
    still_open_positions: List[Dict[str, Any]],
    fills_new: List[List[Any]],
) -> Dict[str, Any]:
    selected_target_keys: set[str] = set()
    selected_target_codes: List[str] = []
    target_key_by_order_id: Dict[str, str] = {}
    code_by_target_key: Dict[str, str] = {}
    for idx_pos, pos in enumerate(open_positions or []):
        target_key = _ddm_pos_key(pos, idx_pos)
        if target_key in (ddm_liquidation_targets or set()):
            code = str(pos.get("code") or "").strip()
            if code:
                norm = code.zfill(6)
                selected_target_keys.add(target_key)
                selected_target_codes.append(norm)
                code_by_target_key[target_key] = norm
                for order_field in ("entry_order_id", "source_order_id"):
                    order_id = str(pos.get(order_field) or "").strip()
                    if order_id:
                        target_key_by_order_id[order_id] = target_key
    selected_target_codes = sorted(set(selected_target_codes))

    executed_target_keys: set[str] = set()

    def _record_executed_target(code: str, note: str) -> None:
        if "exit_reason=DDM_LIQUIDATE_" not in note:
            return
        for note_field in ("entry_order_id", "source_order_id"):
            order_id = _extract_note_field(note, note_field)
            target_key = target_key_by_order_id.get(str(order_id or "").strip())
            if target_key:
                executed_target_keys.add(target_key)
                return
        norm_code = str(code or "").strip().zfill(6)
        matching_keys = [key for key, selected_code in code_by_target_key.items() if selected_code == norm_code]
        if len(matching_keys) == 1:
            executed_target_keys.add(matching_keys[0])

    for row in fills_new or []:
        try:
            side = str(row[2] if schema == "legacy" else row[4]).strip().upper()
            note = str(row[6] if schema == "legacy" else row[10]).strip()
            code = str(row[1] if schema == "legacy" else row[2]).strip()
        except Exception:
            continue
        if side != "SELL":
            continue
        _record_executed_target(code, note)

    runtime_ymd_norm = str(_norm_ymd_text(runtime_ymd) or "")
    persisted_fills = read_csv_safe(FILLS)
    if isinstance(persisted_fills, pd.DataFrame) and not persisted_fills.empty:
        for _, fill_row in persisted_fills.iterrows():
            row_dt = str(fill_row.get("datetime", fill_row.get("date", "")) or "").strip()
            row_ymd = str(_norm_ymd_text(row_dt) or "")
            if runtime_ymd_norm and row_ymd and row_ymd != runtime_ymd_norm:
                continue
            side = str(fill_row.get("side", "") or "").strip().upper()
            note = str(fill_row.get("note", "") or "").strip()
            code = str(fill_row.get("code", "") or "").strip()
            if side != "SELL":
                continue
            _record_executed_target(code, note)

    executed_target_codes = sorted({code_by_target_key.get(key, "") for key in executed_target_keys if code_by_target_key.get(key, "")})
    executed_target_keys_sorted = sorted(executed_target_keys)

    still_open_target_keys = {
        _ddm_pos_key(pos, idx_pos)
        for idx_pos, pos in enumerate(still_open_positions or [])
        if isinstance(pos, dict)
    }
    remaining_target_keys = sorted(selected_target_keys & still_open_target_keys)
    remaining_target_codes = sorted({code_by_target_key.get(key, "") for key in remaining_target_keys if code_by_target_key.get(key, "")})

    issues: List[str] = []
    status = "PASS"
    requires_full_liquidation = float(ddm_force_liquidate_pct or 0.0) >= 1.0
    if selected_target_keys and not executed_target_keys:
        status = "FAIL"
        issues.append("selected_targets_missing_execution")
    elif selected_target_keys and remaining_target_keys and requires_full_liquidation:
        status = "FAIL"
        issues.append("selected_targets_not_fully_liquidated")

    return {
        "generated_at": now_ts(),
        "status": status,
        "ddm_enabled": bool(ddm_enabled),
        "ddm_stage_idx": int(getattr(ddm_action, "stage_idx", -1)),
        "current_mdd_abs": float(getattr(ddm_action, "current_mdd_abs", 0.0) or 0.0),
        "threshold": float(getattr(ddm_action, "threshold", 0.0) or 0.0),
        "liquidate_weakest_pct": float(ddm_force_liquidate_pct or 0.0),
        "requires_full_liquidation": bool(requires_full_liquidation),
        "selected_target_codes": selected_target_codes,
        "executed_target_codes": executed_target_codes,
        "remaining_target_codes": remaining_target_codes,
        "selected_target_keys": sorted(selected_target_keys),
        "executed_target_keys": executed_target_keys_sorted,
        "remaining_target_keys": remaining_target_keys,
        "counts": {
            "open_positions_before": int(len(open_positions or [])),
            "open_positions_after": int(len(still_open_positions or [])),
            "selected_targets": int(len(selected_target_keys)),
            "executed_targets": int(len(executed_target_keys_sorted)),
            "remaining_targets": int(len(remaining_target_keys)),
        },
        "issues": issues,
    }


def _legacy_trade_sig(row: pd.Series) -> str:
    # Exclude trade_id from legacy matching.
    return "|".join([
        str(row.get("code", "")),
        str(row.get("entry_date", "")),
        _sig_float(row.get("entry_price", "")),
        str(row.get("exit_date", "")),
        _sig_float(row.get("exit_price", "")),
        _sig_float(row.get("pnl_pct", "")),
        str(row.get("exit_reason", "")),
        str(row.get("note", "")),
    ])


_SIGNAL_RE = re.compile(r"signal_date=(\d{8})")


def _extract_signal_date(note: Any) -> Optional[str]:
    if note is None:
        return None
    s = str(note)
    m = _SIGNAL_RE.search(s)
    return m.group(1) if m else None


def _normalize_partial_sell_order_id(order_id: Any) -> str:
    s = str(order_id or "").strip()
    if not s:
        return ""
    return re.sub(r"_Q\d+$", "", s)


def _sell_lifecycle_signature(*, code: Any, exit_reason: Any, sell_qty: Any, note: Any) -> tuple[str, str, int, str, str, str, int, str]:
    note_fields = _parse_note_fields(note)
    sell_ratio_pct = float(_to_float(note_fields.get("sell_ratio_pct"), 0.0) or 0.0)
    ratio_key = f"{sell_ratio_pct:.4f}".rstrip("0").rstrip(".") if sell_ratio_pct else "0"
    partial_exit_key = str(_to_int(note_fields.get("partial_exit"), 0))
    prior_stop_count = int(_to_int(note_fields.get("prior_stop_count"), 0))
    signal_date = str(note_fields.get("signal_date") or _extract_signal_date(note) or "").strip()
    replay_chain_id = str(note_fields.get("replay_chain_id") or "").strip()
    return (
        str(code or "").strip().zfill(6),
        str(exit_reason or note_fields.get("exit_reason") or "").strip().upper(),
        int(_to_int(note_fields.get("sell_qty") or sell_qty, 0)),
        signal_date,
        ratio_key,
        partial_exit_key,
        prior_stop_count,
        replay_chain_id,
    )


def _dedupe_partial_exit_fills(df_fills: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if not isinstance(df_fills, pd.DataFrame) or df_fills.empty:
        return df_fills, 0
    required_cols = {"order_id", "side", "note"}
    if not required_cols.issubset(set(df_fills.columns)):
        return df_fills, 0

    work = df_fills.copy()
    note_s = work["note"].astype(str)
    side_s = work["side"].astype(str).str.upper()
    work["_base_order_id"] = work["order_id"].astype(str).map(_normalize_partial_sell_order_id)
    partial_mask = side_s.eq("SELL") & note_s.str.contains("partial_exit=1", na=False) & work["_base_order_id"].astype(str).str.strip().ne("")
    if not bool(partial_mask.any()):
        return df_fills, 0

    dup_mask = pd.Series(False, index=work.index)
    dup_mask.loc[partial_mask] = work.loc[partial_mask, ["_base_order_id"]].duplicated(keep="first")
    removed = int(dup_mask.sum())
    if removed <= 0:
        return df_fills, 0

    out = work.loc[~dup_mask].copy()
    out = out.drop(columns=["_base_order_id"], errors="ignore")
    return out, removed


def _dedupe_sell_fills_by_lifecycle_signature(df_fills: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if not isinstance(df_fills, pd.DataFrame) or df_fills.empty:
        return df_fills, 0
    required_cols = {"code", "side", "qty", "note"}
    if not required_cols.issubset(set(df_fills.columns)):
        return df_fills, 0
    work = df_fills.copy()
    sell_mask = work["side"].astype(str).str.strip().str.upper().eq("SELL")
    if not bool(sell_mask.any()):
        return df_fills, 0
    work["_sell_lifecycle_sig"] = [
        _sell_lifecycle_signature(
            code=row.get("code", ""),
            exit_reason=_extract_note_field(row.get("note", ""), "exit_reason"),
            sell_qty=row.get("qty", 0),
            note=row.get("note", ""),
        )
        for _, row in work.iterrows()
    ]
    valid_mask = sell_mask & work["_sell_lifecycle_sig"].map(lambda x: bool(x[0] and x[1] and x[2] > 0 and x[3] and x[7]))
    if not bool(valid_mask.any()):
        return df_fills, 0
    dup_mask = pd.Series(False, index=work.index)
    dup_mask.loc[valid_mask] = work.loc[valid_mask, "_sell_lifecycle_sig"].duplicated(keep="first")
    removed = int(dup_mask.sum())
    if removed <= 0:
        return df_fills, 0
    out = work.loc[~dup_mask].copy()
    out = out.drop(columns=["_sell_lifecycle_sig"], errors="ignore")
    return out, removed


def _dedupe_recovered_trades_legacy(df_trades: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if not isinstance(df_trades, pd.DataFrame) or df_trades.empty:
        return df_trades, 0
    if "note" not in df_trades.columns:
        return df_trades, 0

    work = df_trades.copy()
    note_s = work["note"].astype(str)
    recovered_mask = note_s.str.contains("lineage_origin=RECOVERED_FROM_FILLS", na=False)
    if not bool(recovered_mask.any()):
        return df_trades, 0

    # source_order_id 기반 + 체결 핵심값 결합키로 중복 정규화
    work["_src_oid"] = note_s.map(lambda x: _extract_note_field(x, "source_order_id"))
    work["_sell_qty"] = note_s.map(lambda x: _extract_note_field(x, "sell_qty"))
    work["_partial_exit"] = note_s.map(lambda x: _extract_note_field(x, "partial_exit"))
    valid_mask = recovered_mask & work["_src_oid"].astype(str).str.strip().ne("")
    if not bool(valid_mask.any()):
        return df_trades, 0

    key_cols = ["code", "entry_date", "exit_date", "entry_price", "exit_price", "exit_reason", "_src_oid", "_sell_qty"]
    for c in key_cols:
        if c not in work.columns:
            work[c] = ""

    # stable keep-first: existing file order 유지
    dup_mask = pd.Series(False, index=work.index)
    dup_mask.loc[valid_mask] = work.loc[valid_mask, key_cols].duplicated(keep="first")
    removed = int(dup_mask.sum())
    if removed <= 0:
        return df_trades, 0

    out = work.loc[~dup_mask].copy()
    out = out.drop(columns=["_src_oid", "_sell_qty"], errors="ignore")
    return out, removed


def _dedupe_partial_exit_trades_legacy(df_trades: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if not isinstance(df_trades, pd.DataFrame) or df_trades.empty:
        return df_trades, 0
    if "note" not in df_trades.columns:
        return df_trades, 0

    work = df_trades.copy()
    note_s = work["note"].astype(str)
    work["_src_oid"] = note_s.map(lambda x: _extract_note_field(x, "source_order_id"))
    work["_entry_oid"] = note_s.map(lambda x: _extract_note_field(x, "entry_order_id"))
    valid_key = work["_src_oid"].astype(str).str.strip()
    alt_key = work["_entry_oid"].astype(str).str.strip()
    work["_dedupe_key"] = valid_key.where(valid_key.ne(""), alt_key)
    partial_mask = note_s.str.contains("partial_exit=1", na=False) & work["_dedupe_key"].astype(str).str.strip().ne("")
    if not bool(partial_mask.any()):
        return df_trades, 0

    key_cols = ["code", "entry_date", "exit_date", "exit_reason", "_dedupe_key"]
    for c in key_cols:
        if c not in work.columns:
            work[c] = ""

    dup_mask = pd.Series(False, index=work.index)
    dup_mask.loc[partial_mask] = work.loc[partial_mask, key_cols].duplicated(keep="first")
    removed = int(dup_mask.sum())
    if removed <= 0:
        return df_trades, 0

    out = work.loc[~dup_mask].copy()
    out = out.drop(columns=["_src_oid", "_entry_oid", "_dedupe_key"], errors="ignore")
    return out, removed


def _dedupe_trades_by_note_identity_legacy(df_trades: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if not isinstance(df_trades, pd.DataFrame) or df_trades.empty:
        return df_trades, 0
    if "note" not in df_trades.columns:
        return df_trades, 0
    work = df_trades.copy()
    note_s = work["note"].astype(str)
    work["_src_oid"] = note_s.map(lambda x: _extract_note_field(x, "source_order_id"))
    work["_entry_oid"] = note_s.map(lambda x: _extract_note_field(x, "entry_order_id"))
    work["_sell_qty"] = note_s.map(lambda x: _extract_note_field(x, "sell_qty"))
    work["_partial_exit"] = note_s.map(lambda x: _extract_note_field(x, "partial_exit"))
    valid_key = work["_src_oid"].astype(str).str.strip()
    alt_key = work["_entry_oid"].astype(str).str.strip()
    dedupe_key = valid_key.where(valid_key.ne(""), alt_key)
    valid_mask = dedupe_key.str.strip().ne("")
    if not bool(valid_mask.any()):
        return df_trades, 0
    work["_dedupe_key"] = dedupe_key
    key_cols = ["code", "entry_date", "exit_date", "exit_reason", "_dedupe_key", "_sell_qty", "_partial_exit"]
    for c in key_cols:
        if c not in work.columns:
            work[c] = ""
    dup_mask = pd.Series(False, index=work.index)
    dup_mask.loc[valid_mask] = work.loc[valid_mask, key_cols].duplicated(keep="first")
    removed = int(dup_mask.sum())
    if removed <= 0:
        return df_trades, 0
    out = work.loc[~dup_mask].copy()
    out = out.drop(columns=["_src_oid", "_entry_oid", "_sell_qty", "_partial_exit", "_dedupe_key"], errors="ignore")
    return out, removed


def _dedupe_sell_trades_by_lifecycle_signature_legacy(df_trades: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if not isinstance(df_trades, pd.DataFrame) or df_trades.empty:
        return df_trades, 0
    required_cols = {"code", "exit_reason", "note"}
    if not required_cols.issubset(set(df_trades.columns)):
        return df_trades, 0
    work = df_trades.copy()
    work["_sell_lifecycle_sig"] = [
        _sell_lifecycle_signature(
            code=row.get("code", ""),
            exit_reason=row.get("exit_reason", ""),
            sell_qty=_extract_note_field(row.get("note", ""), "sell_qty"),
            note=row.get("note", ""),
        )
        for _, row in work.iterrows()
    ]
    valid_mask = work["_sell_lifecycle_sig"].map(lambda x: bool(x[0] and x[1] and x[2] > 0 and x[3] and x[7]))
    if not bool(valid_mask.any()):
        return df_trades, 0
    dup_mask = pd.Series(False, index=work.index)
    dup_mask.loc[valid_mask] = work.loc[valid_mask, "_sell_lifecycle_sig"].duplicated(keep="first")
    removed = int(dup_mask.sum())
    if removed <= 0:
        return df_trades, 0
    out = work.loc[~dup_mask].copy()
    out = out.drop(columns=["_sell_lifecycle_sig"], errors="ignore")
    return out, removed



def _write_intraday_residual_overnight_guard_shadow(
    *,
    config: Dict[str, Any],
    schema: str,
    trades_new: List[Any],
    still_open: List[Dict[str, Any]],
    runtime_ymd: str,
) -> Dict[str, Any]:
    from paper_engine.common import _trade_row_for_intraday_residual_guard

    guard_cfg = config.get("intraday_residual_overnight_guard", {}) if isinstance(config, dict) else {}
    enabled = bool(guard_cfg.get("enabled", False)) if isinstance(guard_cfg, dict) else False
    shadow_only = bool(guard_cfg.get("shadow_only", False)) if isinstance(guard_cfg, dict) else False
    active = bool(enabled or shadow_only)
    scope = str(guard_cfg.get("scope", "intraday_realtime") if isinstance(guard_cfg, dict) else "intraday_realtime").strip().lower()

    def _position_entry_timing(pos: Dict[str, Any]) -> str:
        entry_timing = str(pos.get("entry_timing", "") or "").strip().lower()
        if entry_timing:
            return entry_timing
        entry_ts = str(pos.get("entry_ts", "") or "").strip()
        m = re.search(r"(?:T|\s)(\d{2}):(\d{2})(?::\d{2})?", entry_ts)
        if not m:
            return ""
        hhmm = int(m.group(1)) * 100 + int(m.group(2))
        if 900 <= hhmm <= 1530:
            return "intraday_realtime"
        return ""

    remaining_by_key: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for pos in still_open or []:
        if not isinstance(pos, dict):
            continue
        code = norm_code(pos.get("code", ""))
        entry_date = _norm_ymd_text(pos.get("entry_date", ""))
        qty = _to_int(pos.get("qty", 0), 0)
        if not code or not entry_date or qty <= 0:
            continue
        entry_timing = _position_entry_timing(pos)
        if scope == "intraday_realtime" and entry_timing != "intraday_realtime":
            continue
        remaining_by_key.setdefault((code, entry_date), []).append(pos)

    rows: List[Dict[str, Any]] = []
    if active:
        for raw_trade in trades_new or []:
            trade = _trade_row_for_intraday_residual_guard(raw_trade, schema)
            code = norm_code(trade.get("code", ""))
            entry_date = _norm_ymd_text(trade.get("entry_date", ""))
            exit_date = _norm_ymd_text(trade.get("exit_date", ""))
            net_ret = float(_to_float(trade.get("net_ret"), 0.0) or 0.0)
            if not code or not entry_date or entry_date != exit_date or net_ret >= 0:
                continue
            open_matches = remaining_by_key.get((code, entry_date), [])
            if not open_matches:
                continue
            for open_pos in open_matches:
                rows.append(
                    {
                        "runtime_ymd": str(runtime_ymd or ""),
                        "code": code,
                        "entry_date": entry_date,
                        "entry_ts": str(trade.get("entry_ts", "") or ""),
                        "exit_ts": str(trade.get("exit_ts", "") or ""),
                        "trade_id": str(trade.get("trade_id", "") or ""),
                        "exit_reason": str(trade.get("exit_reason", "") or ""),
                        "sell_qty": int(_to_int(trade.get("sell_qty"), 0)),
                        "remaining_qty": int(_to_int(open_pos.get("qty", 0), 0)),
                        "net_ret": float(net_ret),
                        "partial_exit": bool(trade.get("partial_exit", False)),
                        "is_surge": bool(_truthy(open_pos.get("_surge_immediate")) or str(open_pos.get("surge_type", "") or "").strip()),
                        "entry_timing": str(open_pos.get("entry_timing", "") or ""),
                        "surge_type": str(open_pos.get("surge_type", "") or ""),
                        "source_order_id": str(open_pos.get("source_order_id", "") or ""),
                        "entry_order_id": str(open_pos.get("entry_order_id", "") or ""),
                        "shadow_action": "WOULD_BLOCK_OVERNIGHT_RESIDUAL",
                    }
                )

    payload = {
        "generated_at": now_ts(),
        "status": "PASS" if active else "DISABLED",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "enabled": enabled,
        "shadow_only": shadow_only,
        "scope": scope,
        "runtime_ymd": str(runtime_ymd or ""),
        "candidates": int(len(rows)),
        "artifact_csv": str(INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_CSV_PATH),
        "records": rows,
    }
    INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(
        INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_CSV_PATH,
        index=False,
        encoding="utf-8-sig",
    )
    INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_JSON_PATH.write_text(
        json.dumps(_state_json_safe(payload), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return payload


def _apply_intraday_residual_overnight_guard_exits(
    *,
    config: Dict[str, Any],
    schema: str,
    prices_df: pd.DataFrame,
    still_open: List[Dict[str, Any]],
    shadow_payload: Dict[str, Any],
    runtime_ymd: str,
    fee_pct: float,
    slip_pct: float,
    sell_tax_pct: float,
    fills_new: List[Any],
    trades_new: List[Any],
    existing_fill_order_ids: Set[str],
    existing_trade_sigs: Set[str],
    next_seq_start: int,
) -> Dict[str, Any]:
    guard_cfg = config.get("intraday_residual_overnight_guard", {}) if isinstance(config, dict) else {}
    enabled = bool(guard_cfg.get("enabled", False)) if isinstance(guard_cfg, dict) else False
    shadow_only = bool(guard_cfg.get("shadow_only", False)) if isinstance(guard_cfg, dict) else True
    exit_before_overnight = bool(guard_cfg.get("exit_before_overnight", False)) if isinstance(guard_cfg, dict) else False
    active = bool(enabled and (not shadow_only) and exit_before_overnight)
    records = list((shadow_payload or {}).get("records") or [])
    candidate_keys = {
        (norm_code(row.get("code", "")), _norm_ymd_text(row.get("entry_date", "")))
        for row in records
        if isinstance(row, dict)
    }
    candidate_keys.discard(("", ""))

    next_seq = int(next_seq_start)
    positions_out: List[Dict[str, Any]] = []
    applied_rows: List[Dict[str, Any]] = []
    skipped_rows: List[Dict[str, Any]] = []
    reason = "INTRADAY_RESIDUAL_OVERNIGHT_GUARD"
    runtime_ymd_norm = _norm_ymd_text(runtime_ymd)

    if not active or not candidate_keys:
        return {
            "status": "DISABLED" if not active else "PASS",
            "active": bool(active),
            "candidate_count": int(len(candidate_keys)),
            "applied_count": 0,
            "skipped_count": 0,
            "still_open": list(still_open or []),
            "next_seq": int(next_seq),
            "trading_effect": False,
            "policy_effect": bool(active),
            "records": [],
            "skipped": [],
        }

    price_lookup: Dict[str, float] = {}
    if isinstance(prices_df, pd.DataFrame) and not prices_df.empty and {"code", "date", "close"}.issubset(prices_df.columns):
        px = prices_df.copy()
        px["_code_norm"] = px["code"].astype(str).map(norm_code)
        px["_date_norm"] = px["date"].astype(str).map(_norm_ymd_text)
        for _, row in px.loc[px["_date_norm"].eq(runtime_ymd_norm)].iterrows():
            close_px = _to_float(row.get("close"), 0.0)
            if close_px > 0:
                price_lookup[str(row.get("_code_norm") or "")] = float(close_px)

    for pos in still_open or []:
        if not isinstance(pos, dict):
            continue
        code = norm_code(pos.get("code", ""))
        entry_date = _norm_ymd_text(pos.get("entry_date", ""))
        key = (code, entry_date)
        if key not in candidate_keys:
            positions_out.append(pos)
            continue

        qty = _to_int(pos.get("qty", 0), 0)
        entry_price = _to_float(pos.get("entry_price"), 0.0)
        exit_price = float(price_lookup.get(code, 0.0) or 0.0)
        if qty <= 0 or entry_price <= 0 or exit_price <= 0 or not runtime_ymd_norm:
            skipped_rows.append(
                {
                    "code": code,
                    "entry_date": entry_date,
                    "qty": int(qty),
                    "entry_price": float(entry_price),
                    "exit_price": float(exit_price),
                    "reason": "missing_qty_or_price",
                }
            )
            positions_out.append(pos)
            continue

        source_order_id = str(pos.get("source_order_id") or pos.get("entry_order_id") or "").strip()
        source_suffix = re.sub(r"[^A-Za-z0-9]+", "_", source_order_id).strip("_")[-24:]
        base_sell_order_id = f"PAPER_SELL_{code}_{runtime_ymd_norm}_{reason}"
        if source_suffix:
            base_sell_order_id = f"{base_sell_order_id}_{source_suffix}"
        sell_order_id = base_sell_order_id
        sell_qty = int(qty)
        exit_ts_value = _paper_exit_ts(runtime_ymd_norm)
        exit_note = (
            f"exit_reason={reason};"
            f"signal_date={pos.get('signal_date')};"
            f"sell_ratio_pct=100.0;"
            f"sell_qty={sell_qty};"
            f"partial_exit=0;"
            f"hold_days_trading=0;"
            f"hold_days_calendar=0;"
            f"exit_severity=GUARD;"
            f"severity_reasons=same_day_loss_residual_overnight;"
            f"gap_loss_pct=0.0;"
            f"prior_stop_count={pos.get('stop_exit_count', 0)};"
            f"sell_tag={reason};"
            f"entry_order_id={pos.get('entry_order_id', '')};"
            f"entry_intent_id={pos.get('entry_intent_id', '')};"
            f"entry_trace_id={pos.get('entry_trace_id', '')};"
            f"lineage_origin={pos.get('lineage_origin', '')};"
            f"source_order_id={pos.get('source_order_id', '')};"
            f"source_intent_id={pos.get('source_intent_id', '')};"
            f"source_trace_id={pos.get('source_trace_id', '')};"
            f"replay_chain_id={pos.get('replay_chain_id', '')};"
            f"replay_depth={pos.get('replay_depth', 0)};"
            f"entry_timing={pos.get('entry_timing', '')};"
            f"surge_type={pos.get('surge_type', '')};"
            f"surge_score={pos.get('surge_score', '')};"
            f"surge_score_final={pos.get('surge_score_final', '')};"
            f"surge_rvol20={pos.get('surge_rvol20', '')};"
            f"surge_spread_bps={pos.get('surge_spread_bps', '')};"
            f"surge_orderflow_tag={pos.get('surge_orderflow_tag', '')};"
            f"surge_orderflow_risk_score={pos.get('surge_orderflow_risk_score', '')}"
        )

        sell_fill_appended = False
        existing_sell_fill_reused = False
        if schema == "legacy":
            if sell_order_id not in existing_fill_order_ids and base_sell_order_id not in existing_fill_order_ids:
                fills_new.append([exit_ts_value, code, "SELL", sell_qty, exit_price, sell_order_id, exit_note])
                existing_fill_order_ids.add(sell_order_id)
                existing_fill_order_ids.add(base_sell_order_id)
                sell_fill_appended = True
            elif sell_order_id in existing_fill_order_ids:
                existing_sell_fill_reused = True

            pnl_pct = calc_net_ret(entry_price, exit_price, fee_pct, slip_pct, sell_tax_pct)
            sig = "|".join([
                code,
                entry_date,
                _sig_float(entry_price),
                runtime_ymd_norm,
                _sig_float(exit_price),
                _sig_float(round(pnl_pct, 8)),
                reason,
                _sig_float(sell_qty),
                f"signal_date={pos.get('signal_date')}",
            ])
            if (sell_fill_appended or existing_sell_fill_reused) and sig not in existing_trade_sigs:
                trade_id = f"T{next_seq:06d}"
                pnl_krw = round(float(pnl_pct) * float(entry_price) * float(sell_qty), 2)
                trades_new.append([trade_id, code, entry_date, entry_price, runtime_ymd_norm, exit_price, round(pnl_pct, 8), pnl_krw, reason, exit_note, int(_truthy(pos.get("_surge_immediate", 0)))])
                existing_trade_sigs.add(sig)
                next_seq += 1
        else:
            name = str(pos.get("name", "") or "")
            entry_ts = str(pos.get("entry_ts", "") or entry_date)
            fee = calc_roundtrip_fee(entry_price, exit_price, sell_qty, fee_pct)
            slp = calc_roundtrip_slippage(entry_price, exit_price, sell_qty, slip_pct)
            if sell_order_id not in existing_fill_order_ids and base_sell_order_id not in existing_fill_order_ids:
                fills_new.append([
                    exit_ts_value,
                    runtime_ymd_norm,
                    code,
                    name,
                    "SELL",
                    sell_qty,
                    exit_price,
                    round(calc_exit_fee(exit_price, sell_qty, fee_pct), 6),
                    round(exit_price * sell_qty * slip_pct, 6),
                    sell_order_id,
                    exit_note,
                ])
                existing_fill_order_ids.add(sell_order_id)
                existing_fill_order_ids.add(base_sell_order_id)
                sell_fill_appended = True
            elif sell_order_id in existing_fill_order_ids:
                existing_sell_fill_reused = True
            gross = (exit_price - entry_price) / entry_price
            net = calc_net_ret(entry_price, exit_price, fee_pct, slip_pct, sell_tax_pct)
            trade_probe = pd.Series({
                "code": code,
                "entry_ts": entry_ts,
                "exit_ts": exit_ts_value,
                "qty": sell_qty,
                "entry_price": round(entry_price, 6),
                "exit_price": round(exit_price, 6),
                "net_ret": round(net, 8),
                "note": exit_note,
            })
            sig = _v411_trade_sig(trade_probe)
            if (sell_fill_appended or existing_sell_fill_reused) and sig not in existing_trade_sigs:
                trade_id = f"T{next_seq:06d}"
                trades_new.append([
                    trade_id,
                    entry_ts,
                    exit_ts_value,
                    code,
                    name,
                    "LONG",
                    sell_qty,
                    round(entry_price, 6),
                    round(exit_price, 6),
                    round(gross, 8),
                    round(net, 8),
                    round(fee, 6),
                    round(slp, 6),
                    "0",
                    "0",
                    "0",
                    exit_note,
                ])
                existing_trade_sigs.add(sig)
                next_seq += 1

        if sell_fill_appended or existing_sell_fill_reused:
            applied_rows.append(
                {
                    "code": code,
                    "entry_date": entry_date,
                    "exit_date": runtime_ymd_norm,
                    "sell_qty": int(sell_qty),
                    "exit_price": float(exit_price),
                    "order_id": sell_order_id,
                }
            )
        else:
            skipped_rows.append({"code": code, "entry_date": entry_date, "reason": "sell_fill_not_appended"})
            positions_out.append(pos)

    return {
        "status": "PASS",
        "active": True,
        "candidate_count": int(len(candidate_keys)),
        "applied_count": int(len(applied_rows)),
        "skipped_count": int(len(skipped_rows)),
        "still_open": positions_out,
        "next_seq": int(next_seq),
        "trading_effect": bool(applied_rows),
        "policy_effect": True,
        "records": applied_rows,
        "skipped": skipped_rows,
    }
