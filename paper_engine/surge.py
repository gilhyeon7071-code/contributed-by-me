# -*- coding: utf-8 -*-
"""Surge (급등) entry policy and runtime shadow helpers for paper_engine.

Split out from the legacy ``paper_engine.py`` to isolate surge-specific
entry decisions, LOB slippage, type-policy overrides, and sizing logic.
"""
from __future__ import annotations


__all__ = [
    '_surge_realtime_shadow_fields',
    '_write_surge_realtime_shadow_runtime_snapshot',
    '_surge_lob_slippage_pct',
    '_surge_type_policy_decision',
    '_surge_type_overrides_for_position',
    '_normalize_surge_position_policy_fields',
    '_surge_market_event_block_reason',
    '_compute_dynamic_max_new_surge',
    '_overheat_qty_decision',
    '_surge_execution_quality_qty_decision',
    '_calc_surge_reversal_signals',
    '_surge_after_state_from_active_label',
    '_load_surge_active_response_alert_rows',
    '_load_surge_live_readiness_audit_rows',
    '_inject_surge_immediate_candidates',
]

import json
import math
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from pricing_engine import calc_dynamic_lob_slippage
from state_containers import json_safe as _state_json_safe
from utils.common import norm_code

from paper_engine.common import (
    _ask_book_from_row,
    _bollinger_upper_from_closes,
    _calc_rsi,
    _clean_active_response_cell,
    _concat_drop_all_na_columns,
    _has_source_entry_cell,
    _load_disclosure_negative_codes_for_entry,
    _norm_ymd_text,
    _parse_policy_dt_text,
    _pct01_from_config,
    _source_entry_cell_text,
    _to_float,
    _to_int,
    _truthy,
    now_ts,
    _append_reduction_multiplier,
)
from paper_engine.io import (
    SURGE_ACTIVE_RESPONSE_LAYER_CSV_PATH,
    SURGE_LIVE_READINESS_AUDIT_CSV_PATH,
    SURGE_REALTIME_SHADOW_RUNTIME_CSV_PATH,
    SURGE_REALTIME_SHADOW_RUNTIME_JSON_PATH,
    SURGE_REALTIME_STATUS_PATH,
)


def _surge_realtime_shadow_fields(
    row: Any,
    *,
    is_surge_immediate: Optional[bool] = None,
    row_entry_timing: str = "",
    surge_type_text: str = "",
) -> Dict[str, Any]:
    if is_surge_immediate is None:
        is_surge_immediate = _truthy(row.get("_surge_immediate", False)) if hasattr(row, "get") else False
    entry_timing = str(
        row_entry_timing
        or (row.get("entry_timing") if hasattr(row, "get") else "")
        or (row.get("surge_type_entry_timing") if hasattr(row, "get") else "")
        or ""
    ).strip().lower()
    surge_type = str(
        surge_type_text
        or (row.get("surge_type") if hasattr(row, "get") else "")
        or ""
    ).strip()
    if surge_type.lower() in {"nan", "none", "null", "<na>"}:
        surge_type = ""

    if bool(is_surge_immediate):
        return {
            "surge_shadow_status": "SHADOW_ONLY",
            "surge_shadow_condition": "surge_immediate",
            "surge_shadow_mode": "shadow_block",
            "surge_shadow_action": "WOULD_HOLD",
            "surge_shadow_reason": "surge_immediate_shadow_block_candidate",
            "surge_shadow_entry_timing": entry_timing,
            "surge_shadow_type": surge_type,
        }
    if entry_timing == "intraday_realtime":
        return {
            "surge_shadow_status": "SHADOW_ONLY",
            "surge_shadow_condition": "intraday_realtime",
            "surge_shadow_mode": "shadow_watch",
            "surge_shadow_action": "WATCH_ONLY",
            "surge_shadow_reason": "intraday_realtime_secondary_watch",
            "surge_shadow_entry_timing": entry_timing,
            "surge_shadow_type": surge_type,
        }
    return {
        "surge_shadow_status": "SHADOW_ONLY",
        "surge_shadow_condition": "",
        "surge_shadow_mode": "observe",
        "surge_shadow_action": "ALLOW",
        "surge_shadow_reason": "",
        "surge_shadow_entry_timing": entry_timing,
        "surge_shadow_type": surge_type,
    }


def _write_surge_realtime_shadow_runtime_snapshot(
    *,
    entry_decision_rows: List[Dict[str, Any]],
    d_ref_ymd: str,
) -> None:
    if not isinstance(entry_decision_rows, list):
        entry_decision_rows = []
    rows: List[Dict[str, Any]] = []
    for src in entry_decision_rows:
        if not isinstance(src, dict):
            continue
        rows.append(
            {
                "generated_at": now_ts(),
                "d_ref": str(d_ref_ymd or ""),
                "code": norm_code(src.get("code", "")),
                "signal_date": str(src.get("signal_date", "") or ""),
                "rank_score": src.get("rank_score", ""),
                "signal": str(src.get("signal", "") or ""),
                "execution_reason": str(src.get("reason", "") or ""),
                "qty": src.get("qty", ""),
                "order_id": str(src.get("order_id", "") or ""),
                "is_surge": src.get("is_surge", ""),
                "surge_type_normalized": src.get("surge_type_normalized", ""),
                "surge_shadow_status": src.get("surge_shadow_status", "SHADOW_ONLY"),
                "surge_shadow_condition": src.get("surge_shadow_condition", ""),
                "surge_shadow_mode": src.get("surge_shadow_mode", ""),
                "surge_shadow_action": src.get("surge_shadow_action", ""),
                "surge_shadow_reason": src.get("surge_shadow_reason", ""),
                "surge_shadow_entry_timing": src.get("surge_shadow_entry_timing", ""),
                "surge_shadow_type": src.get("surge_shadow_type", ""),
            }
        )

    columns = [
        "generated_at",
        "d_ref",
        "code",
        "signal_date",
        "rank_score",
        "signal",
        "execution_reason",
        "qty",
        "order_id",
        "is_surge",
        "surge_type_normalized",
        "surge_shadow_status",
        "surge_shadow_condition",
        "surge_shadow_mode",
        "surge_shadow_action",
        "surge_shadow_reason",
        "surge_shadow_entry_timing",
        "surge_shadow_type",
    ]
    payload = pd.DataFrame(rows)
    if payload.empty:
        payload = pd.DataFrame(columns=columns)
    for col in columns:
        if col not in payload.columns:
            payload[col] = ""
    payload = payload[columns].copy()
    if not payload.empty:
        payload["_rank_n"] = pd.to_numeric(payload["rank_score"], errors="coerce")
        payload = payload.sort_values(
            ["d_ref", "_rank_n", "code"],
            ascending=[False, False, True],
            kind="mergesort",
        ).drop(columns=["_rank_n"], errors="ignore")

    SURGE_REALTIME_SHADOW_RUNTIME_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload.to_csv(SURGE_REALTIME_SHADOW_RUNTIME_CSV_PATH, index=False, encoding="utf-8-sig")
    action_counts = payload["surge_shadow_action"].astype(str).value_counts(dropna=False).to_dict()
    condition_counts = payload["surge_shadow_condition"].astype(str).value_counts(dropna=False).to_dict()
    would_hold = payload[payload["surge_shadow_action"].astype(str).str.upper() == "WOULD_HOLD"]
    summary = {
        "generated_at": now_ts(),
        "status": "PASS",
        "mode": "SHADOW_ONLY",
        "policy_change": False,
        "d_ref": str(d_ref_ymd or ""),
        "decision_rows": int(len(entry_decision_rows)),
        "snapshot_rows": int(len(payload)),
        "would_hold_rows": int(len(would_hold)),
        "artifact_csv": str(SURGE_REALTIME_SHADOW_RUNTIME_CSV_PATH),
        "action_counts": action_counts,
        "condition_counts": condition_counts,
        "top_would_hold_candidates": would_hold[
            ["code", "rank_score", "signal", "execution_reason", "surge_shadow_condition", "surge_shadow_reason"]
        ].head(10).to_dict("records"),
        "note": "Surge realtime guard runtime shadow only; does not change entry filtering, risk gates, sizing, orders, or fills.",
    }
    SURGE_REALTIME_SHADOW_RUNTIME_JSON_PATH.write_text(
        json.dumps(_state_json_safe(summary), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _surge_lob_slippage_pct(
    row: Any,
    *,
    qty: int,
    entry_price: float,
    base_slip_pct: float,
    cfg: Dict[str, Any],
) -> Tuple[float, str]:
    if qty <= 0 or entry_price <= 0:
        return float(base_slip_pct), "SKIP_INVALID"
    ask_book = _ask_book_from_row(row)
    default_slip = max(
        float(base_slip_pct),
        float(_to_float((cfg or {}).get("surge_lob_missing_slippage_pct"), 0.005)),
    )
    penalty = float(_to_float((cfg or {}).get("surge_lob_unfilled_penalty_pct"), 0.01))
    dynamic_slip = calc_dynamic_lob_slippage(
        int(qty),
        float(entry_price),
        ask_book,
        default_slippage_pct=default_slip,
        unfilled_penalty_pct=penalty,
    )
    if ask_book:
        return max(float(base_slip_pct), float(dynamic_slip)), f"LOB_SWEEP_L{len(ask_book)}"
    return max(float(base_slip_pct), float(dynamic_slip)), "NO_LOB_FALLBACK"


def _surge_type_policy_decision(row: Dict[str, Any], cfg: Dict[str, Any]) -> Tuple[bool, float, str, Dict[str, Any]]:
    policy = cfg.get("surge_entry_policy", {}) if isinstance(cfg.get("surge_entry_policy"), dict) else {}
    type_policy = policy.get("type_policy", {}) if isinstance(policy.get("type_policy"), dict) else {}
    if not bool(type_policy.get("enabled", False)):
        return True, 1.0, "", {}
    surge_type = str(row.get("surge_type", "") or "").strip().upper()
    blocked = {str(x).strip().upper() for x in (type_policy.get("blocked_types") or []) if str(x).strip()}
    allowed = {str(x).strip().upper() for x in (type_policy.get("allowed_types") or []) if str(x).strip()}
    if surge_type in blocked:
        return False, 0.0, f"SURGE_TYPE_BLOCK({surge_type})", {}
    if allowed and surge_type and surge_type not in allowed:
        return False, 0.0, f"SURGE_TYPE_NOT_ALLOWED({surge_type})", {}

    # 5. Volume-Based Divergence Block
    divergence_blocked = bool(type_policy.get("block_price_vol_divergence", False))
    if divergence_blocked:
        div_val = str(row.get("price_vol_divergence", "")).strip().lower()
        if div_val in {"true", "1", "yes", "t", "1.0"}:
            return False, 0.0, f"PRICE_VOL_DIVERGENCE_BLOCK({surge_type})", {}

    # 3. Trend Smoothness Check
    if surge_type in {"REG_SHORT_5D60", "REG_MID_15D100"}:
        min_smooth = float(type_policy.get("trend_smoothness_min", 0.0) or 0.0)
        if min_smooth > 0.0:
            try:
                smooth = float(str(row.get("trend_smoothness", "999.0")))
                if smooth < min_smooth:
                    return False, 0.0, f"CHOPPY_TREND({smooth:.2f}<{min_smooth})", {}
            except ValueError:
                pass

    session_filter = policy.get("afternoon_session_filter", {})
    if bool(session_filter.get("enabled", False)):
        block_time = int(session_filter.get("block_after_hhmm", 1300))
        from datetime import datetime
        now_hhmm = int(datetime.now().strftime("%H%M"))
        if now_hhmm >= block_time:
            afternoon_blocked = {str(x).strip().upper() for x in (session_filter.get("blocked_types") or [])}
            if surge_type in afternoon_blocked:
                return False, 0.0, f"AFTERNOON_SESSION_BLOCK({surge_type})", {}
    mult_map = type_policy.get("type_qty_multiplier", {}) if isinstance(type_policy.get("type_qty_multiplier"), dict) else {}
    mult = float(_to_float(mult_map.get(surge_type), 1.0))
    mult = max(0.0, min(1.0, mult))
    overrides_map = type_policy.get("type_overrides", {}) if isinstance(type_policy.get("type_overrides"), dict) else {}
    overrides: Dict[str, Any] = dict((overrides_map.get(surge_type) or {}) if surge_type else {})
    return True, mult, f"surge_type_mult={surge_type}:{mult:.2f}" if mult < 1.0 else "", overrides


def _surge_type_overrides_for_position(pos: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    policy = cfg.get("surge_entry_policy", {}) if isinstance(cfg.get("surge_entry_policy"), dict) else {}
    type_policy = policy.get("type_policy", {}) if isinstance(policy.get("type_policy"), dict) else {}
    overrides_map = type_policy.get("type_overrides", {}) if isinstance(type_policy.get("type_overrides"), dict) else {}
    surge_type = str(pos.get("surge_type", "") or "").strip().upper()
    return dict((overrides_map.get(surge_type) or {}) if surge_type else {})


def _normalize_surge_position_policy_fields(pos: Dict[str, Any], cfg: Dict[str, Any]) -> int:
    if not isinstance(pos, dict):
        return 0
    if not (_truthy(pos.get("_surge_immediate")) or bool(str(pos.get("surge_type", "") or "").strip())):
        return 0
    overrides = _surge_type_overrides_for_position(pos, cfg)
    changed = 0
    stop_pct = _to_float(pos.get("surge_type_stop_pct"), None)
    if stop_pct is None and "stop_loss_pct" in overrides:
        resolved = _to_float(overrides.get("stop_loss_pct"), None)
        if resolved is not None:
            pos["surge_type_stop_pct"] = float(resolved)
            changed += 1
            stop_pct = float(resolved)
    if stop_pct is not None:
        current_stop = _to_float(pos.get("stop_loss"), None)
        if current_stop is None or abs(float(current_stop) - float(stop_pct)) > 1e-9:
            pos["stop_loss"] = float(stop_pct)
            changed += 1
    tp_pct = _to_float(pos.get("surge_type_tp_pct"), None)
    if tp_pct is None and "take_profit_pct" in overrides:
        resolved = _to_float(overrides.get("take_profit_pct"), None)
        if resolved is not None:
            pos["surge_type_tp_pct"] = float(resolved)
            pos["take_profit"] = float(resolved)
            changed += 1
            tp_pct = float(resolved)
    if tp_pct is not None:
        current_take_profit = _to_float(pos.get("take_profit"), None)
        if current_take_profit is None or abs(float(current_take_profit) - float(tp_pct)) > 1e-9:
            pos["take_profit"] = float(tp_pct)
            changed += 1
    hold_days = _to_int(pos.get("surge_type_max_hold_days"), 0)
    if hold_days <= 0 and "max_hold_days" in overrides:
        resolved_hold = _to_int(overrides.get("max_hold_days"), 0)
        if resolved_hold > 0:
            pos["surge_type_max_hold_days"] = int(resolved_hold)
            changed += 1
            hold_days = int(resolved_hold)
    if hold_days > 0:
        current_horizon_hold = _to_int(pos.get("horizon_max_hold_days"), 0)
        if current_horizon_hold != int(hold_days):
            pos["horizon_max_hold_days"] = int(hold_days)
            changed += 1
    orderflow_tag = str(pos.get("surge_orderflow_tag", "") or "").strip().upper()
    if not orderflow_tag or orderflow_tag in {"NO_HISTORY", "NO_LOB", "UNKNOWN", "NAN", "NONE", "NULL"}:
        if pos.get("surge_orderflow_status") != "MISSING":
            pos["surge_orderflow_status"] = "MISSING"
            changed += 1
        if not bool(pos.get("surge_orderflow_missing", False)):
            pos["surge_orderflow_missing"] = True
            changed += 1
    else:
        if pos.get("surge_orderflow_status") != "OK":
            pos["surge_orderflow_status"] = "OK"
            changed += 1
        if bool(pos.get("surge_orderflow_missing", False)):
            pos["surge_orderflow_missing"] = False
            changed += 1
    return changed


def _surge_market_event_block_reason(row: Dict[str, Any], cfg: Dict[str, Any]) -> str:
    policy = cfg.get("surge_entry_policy", {}) if isinstance(cfg.get("surge_entry_policy"), dict) else {}
    guard = policy.get("market_event_guard", {}) if isinstance(policy.get("market_event_guard"), dict) else {}
    if not bool(guard.get("enabled", False)):
        return ""
    if bool(guard.get("block_on_exclude_reasons", True)):
        exclude_text = str(row.get("exclude_reasons", "") or "").upper()
        for term in guard.get("blocked_terms", []) or []:
            needle = str(term or "").strip().upper()
            if needle and needle in exclude_text:
                return f"SURGE_MARKET_EVENT_BLOCK({needle})"
    return ""


def _compute_dynamic_max_new_surge(
    candidate_df: pd.DataFrame,
    cfg: Dict[str, Any],
    *,
    capital_total: float,
    fallback_max_new_surge: int,
    existing_surge_count: int = 0,
    existing_surge_notional_krw: float = 0.0,
) -> Tuple[int, Dict[str, Any]]:
    policy = cfg.get("surge_entry_policy", {}) if isinstance(cfg.get("surge_entry_policy"), dict) else {}
    dyn = policy.get("dynamic_max_new", {}) if isinstance(policy.get("dynamic_max_new"), dict) else {}
    if not bool(dyn.get("enabled", False)):
        return int(max(0, fallback_max_new_surge)), {"enabled": False, "reason": "disabled"}
    if candidate_df is None or len(candidate_df) == 0:
        return 0, {"enabled": True, "reason": "no_candidates", "eligible_count": 0}

    hard_cap = max(0, _to_int(dyn.get("hard_cap", fallback_max_new_surge), fallback_max_new_surge))
    min_score = float(_to_float(dyn.get("min_score_final", policy.get("min_score_final", 75.0)), 75.0))
    medium_score = float(_to_float(dyn.get("medium_score_final", 82.0), 82.0))
    strong_score = float(_to_float(dyn.get("strong_score_final", 90.0), 90.0))
    medium_rvol = float(_to_float(dyn.get("medium_rvol20_min", 2.0), 2.0))
    strong_rvol = float(_to_float(dyn.get("strong_rvol20_min", 3.0), 3.0))
    medium_spread = float(_to_float(dyn.get("medium_spread_bps_max", 80.0), 80.0))
    strong_spread = float(_to_float(dyn.get("strong_spread_bps_max", 50.0), 50.0))
    risk_max = float(_to_float(dyn.get("orderflow_risk_score_max", 0.60), 0.60))
    weak_cap = max(0, _to_int(dyn.get("weak_cap", 1), 1))
    medium_cap = max(0, _to_int(dyn.get("medium_cap", 3), 3))
    strong_cap = max(0, _to_int(dyn.get("strong_cap", hard_cap), hard_cap))

    cdf = candidate_df.copy()
    if "_surge_immediate" in cdf.columns:
        cdf = cdf[pd.to_numeric(cdf["_surge_immediate"], errors="coerce").fillna(0).astype(int) == 1].copy()
    elif "surge_type" in cdf.columns:
        cdf = cdf[cdf["surge_type"].astype(str).str.strip() != ""].copy()
    else:
        cdf = cdf.iloc[0:0].copy()
    if len(cdf) == 0:
        return 0, {"enabled": True, "reason": "no_surge_candidates", "eligible_count": 0}

    score = pd.to_numeric(cdf.get("surge_score_final", cdf.get("final_score", 0.0)), errors="coerce").fillna(0.0)
    rvol = pd.to_numeric(cdf.get("surge_rvol20", cdf.get("rvol20", 0.0)), errors="coerce").fillna(0.0)
    spread = pd.to_numeric(cdf.get("surge_spread_bps", cdf.get("spread_bps", medium_spread)), errors="coerce").fillna(medium_spread)
    risk = pd.to_numeric(cdf.get("surge_orderflow_risk_score", cdf.get("orderflow_risk_score", 0.0)), errors="coerce").fillna(0.0)
    tag_source_obj = cdf.get("surge_orderflow_tag", cdf.get("orderflow_tag", ""))
    if isinstance(tag_source_obj, pd.Series):
        tag_source = tag_source_obj
    else:
        tag_source = pd.Series([""] * len(cdf), index=cdf.index)
    no_lob_mask = tag_source.astype(str).str.strip().str.upper().isin({"NO_LOB", "NO_HISTORY"})
    no_lob_cap = _to_int(dyn.get("no_lob_cap"), -1)

    eligible_mask = (score >= min_score) & (spread <= medium_spread) & (risk <= risk_max)
    medium_mask = eligible_mask & (score >= medium_score) & (rvol >= medium_rvol)
    strong_mask = eligible_mask & (score >= strong_score) & (rvol >= strong_rvol) & (spread <= strong_spread)
    eligible_count = int(eligible_mask.sum())
    medium_count = int(medium_mask.sum())
    strong_count = int(strong_mask.sum())
    no_lob_eligible_count = int((eligible_mask & no_lob_mask).sum())
    if eligible_count <= 0:
        return 0, {
            "enabled": True,
            "reason": "no_eligible_surge_candidates",
            "raw_surge_candidates": int(len(cdf)),
            "eligible_count": 0,
            "min_score_final": float(min_score),
            "medium_spread_bps_max": float(medium_spread),
            "orderflow_risk_score_max": float(risk_max),
        }

    quality_cap = weak_cap
    if medium_count > 0:
        quality_cap = max(quality_cap, medium_cap)
    if strong_count > 0:
        quality_cap = max(quality_cap, strong_cap)
    if no_lob_cap >= 0 and no_lob_eligible_count > 0 and no_lob_eligible_count == eligible_count:
        quality_cap = min(quality_cap, max(0, int(no_lob_cap)))

    total_alloc_pct = _pct01_from_config(policy.get("total_alloc_pct", 0.0), 0.0)
    per_symbol_pct = _pct01_from_config(policy.get("per_symbol_alloc_pct", 0.0), 0.0)
    split_cfg = cfg.get("split_entry", {}) if isinstance(cfg.get("split_entry"), dict) else {}
    first_ratio = max(0.01, min(1.0, float(_to_float(split_cfg.get("surge_first_ratio", 1.0), 1.0))))
    existing_count = max(0, int(existing_surge_count or 0))
    existing_notional = max(0.0, float(existing_surge_notional_krw or 0.0))
    budget_cap = 0
    budget_slots_left = 0
    if float(capital_total or 0.0) > 0 and total_alloc_pct > 0 and per_symbol_pct > 0:
        total_budget = float(capital_total) * float(total_alloc_pct)
        per_slot_budget = float(capital_total) * max(float(per_symbol_pct) * float(first_ratio), 1e-9)
        budget_cap = int(math.floor(total_budget / max(per_slot_budget, 1e-9)))
        budget_left = max(0.0, float(total_budget) - float(existing_notional))
        budget_slots_left = int(math.floor(budget_left / max(per_slot_budget, 1e-9)))
    if budget_cap <= 0:
        budget_cap = int(max(0, fallback_max_new_surge))
    if budget_slots_left <= 0:
        budget_slots_left = max(0, int(budget_cap) - int(existing_count))

    new_candidate_cap = max(
        0,
        min(
            max(0, int(hard_cap) - int(existing_count)),
            int(eligible_count),
            int(quality_cap),
            int(budget_slots_left),
        ),
    )
    dynamic_cap = max(0, min(int(hard_cap), int(existing_count) + int(new_candidate_cap), int(budget_cap)))
    return int(dynamic_cap), {
        "enabled": True,
        "reason": "dynamic_quality_budget_cap",
        "raw_surge_candidates": int(len(cdf)),
        "eligible_count": int(eligible_count),
        "medium_count": int(medium_count),
        "strong_count": int(strong_count),
        "no_lob_eligible_count": int(no_lob_eligible_count),
        "no_lob_cap": int(no_lob_cap) if no_lob_cap >= 0 else None,
        "quality_cap": int(quality_cap),
        "budget_cap": int(budget_cap),
        "budget_slots_left": int(budget_slots_left),
        "existing_surge_count": int(existing_count),
        "existing_surge_notional_krw": float(existing_notional),
        "new_candidate_cap": int(new_candidate_cap),
        "hard_cap": int(hard_cap),
        "fallback_max_new_surge": int(max(0, fallback_max_new_surge)),
        "dynamic_max_new_surge": int(dynamic_cap),
        "min_score_final": float(min_score),
        "medium_score_final": float(medium_score),
        "strong_score_final": float(strong_score),
        "medium_rvol20_min": float(medium_rvol),
        "strong_rvol20_min": float(strong_rvol),
        "medium_spread_bps_max": float(medium_spread),
        "strong_spread_bps_max": float(strong_spread),
        "orderflow_risk_score_max": float(risk_max),
    }


def _overheat_qty_decision(row: Dict[str, Any], cfg: Dict[str, Any], *, is_surge: bool) -> Tuple[float, str]:
    policy = cfg.get("entry_overheat_policy", {}) if isinstance(cfg.get("entry_overheat_policy"), dict) else {}
    if not bool(policy.get("enabled", False)):
        return 1.0, ""
    if is_surge and not bool(policy.get("apply_to_surge", True)):
        return 1.0, ""
    if (not is_surge) and not bool(policy.get("apply_to_normal", True)):
        return 1.0, ""
    v_accel = float(_to_float(row.get("v_accel"), 0.0) or 0.0)
    ret1_pct = float(_to_float(row.get("ret1_pct"), 0.0) or 0.0)
    atr14_pct = float(_to_float(row.get("atr14_pct"), 0.0) or 0.0)
    v_thr = float(_to_float(policy.get("v_accel_threshold"), 0.0) or 0.0)
    r_thr = float(_to_float(policy.get("ret1_pct_threshold"), 0.0) or 0.0)
    a_thr = float(_to_float(policy.get("atr14_pct_threshold"), 0.0) or 0.0)
    trigger = str(policy.get("trigger", "v_accel") or "v_accel").strip().lower()
    flags = {
        "v_accel": bool(v_thr > 0 and v_accel >= v_thr),
        "ret1_pct": bool(r_thr > 0 and ret1_pct >= r_thr),
        "atr14_pct": bool(a_thr > 0 and atr14_pct >= a_thr),
    }
    if trigger == "two_or_more":
        hit = sum(1 for x in flags.values() if x) >= 2
    elif trigger == "any":
        hit = any(flags.values())
    else:
        hit = bool(flags.get(trigger, False))
    if not hit:
        return 1.0, ""
    mult = max(0.0, min(1.0, float(_to_float(policy.get("reduce_multiplier"), 0.5))))
    hit_keys = ",".join(k for k, v in flags.items() if v)
    return mult, f"overheat_reduce={hit_keys}:mult={mult:.2f}"


def _surge_execution_quality_qty_decision(row: Dict[str, Any], cfg: Dict[str, Any], *, is_surge: bool) -> Tuple[float, str]:
    if not is_surge:
        return 1.0, ""
    surge_policy = cfg.get("surge_entry_policy", {}) if isinstance(cfg.get("surge_entry_policy"), dict) else {}
    paper_probe = surge_policy.get("paper_probe", {}) if isinstance(surge_policy.get("paper_probe"), dict) else {}
    policy = paper_probe.get("execution_quality_sizing", {}) if isinstance(paper_probe.get("execution_quality_sizing"), dict) else {}
    if not bool(policy.get("enabled", False)):
        return 1.0, ""
    scope = str(policy.get("scope", "paper_mock_surge_only") or "").strip().lower()
    if scope not in {"paper_mock_surge_only", "paper_surge_only", "all_surge_order_routes"}:
        return 1.0, ""

    reasons: List[str] = []
    multipliers: List[float] = []
    categories: List[str] = []

    tag = str(row.get("surge_orderflow_tag", row.get("orderflow_tag", "")) or "").strip().upper()
    slip_source = str(row.get("surge_lob_slippage_source", "") or "").strip().upper()
    rvol = float(_to_float(row.get("surge_rvol20", row.get("rvol20")), 0.0) or 0.0)
    spread = float(_to_float(row.get("surge_spread_bps", row.get("spread_bps")), 0.0) or 0.0)
    score = float(_to_float(row.get("surge_score_final", row.get("final_score")), 0.0) or 0.0)

    if tag == "NO_HISTORY":
        _append_reduction_multiplier(reasons, multipliers, categories, "no_history", policy.get("no_history_multiplier", 0.5), "orderflow")
    if slip_source == "LOB_SWEEP_L10":
        _append_reduction_multiplier(reasons, multipliers, categories, "lob_sweep_l10", policy.get("lob_sweep_multiplier", 0.5), "lob")
    if slip_source == "NO_LOB_FALLBACK":
        _append_reduction_multiplier(reasons, multipliers, categories, "no_lob_fallback", policy.get("no_lob_fallback_multiplier", 0.5), "lob")

    rvol_bands = policy.get("rvol_bands", []) if isinstance(policy.get("rvol_bands"), list) else []
    rvol_hit: Optional[Dict[str, Any]] = None
    for band in rvol_bands:
        if not isinstance(band, dict):
            continue
        threshold = float(_to_float(band.get("min"), 0.0) or 0.0)
        if threshold > 0.0 and rvol >= threshold:
            if rvol_hit is None or threshold >= float(_to_float(rvol_hit.get("min"), 0.0) or 0.0):
                rvol_hit = band
    if isinstance(rvol_hit, dict):
        threshold = float(_to_float(rvol_hit.get("min"), 0.0) or 0.0)
        _append_reduction_multiplier(reasons, multipliers, categories, f"rvol>={threshold:g}", rvol_hit.get("multiplier", 1.0), "rvol")

    spread_bands = policy.get("spread_bands", []) if isinstance(policy.get("spread_bands"), list) else []
    spread_hit: Optional[Dict[str, Any]] = None
    for band in spread_bands:
        if not isinstance(band, dict):
            continue
        threshold = float(_to_float(band.get("min_bps"), 0.0) or 0.0)
        if threshold > 0.0 and spread >= threshold:
            if spread_hit is None or threshold >= float(_to_float(spread_hit.get("min_bps"), 0.0) or 0.0):
                spread_hit = band
    if isinstance(spread_hit, dict):
        threshold = float(_to_float(spread_hit.get("min_bps"), 0.0) or 0.0)
        _append_reduction_multiplier(reasons, multipliers, categories, f"spread_bps>={threshold:g}", spread_hit.get("multiplier", 1.0), "spread")

    score_floor = float(_to_float(policy.get("score_reduce_below"), 0.0) or 0.0)
    if score_floor > 0.0 and score > 0.0 and score < score_floor:
        _append_reduction_multiplier(reasons, multipliers, categories, f"score_final<{score_floor:g}", policy.get("score_low_multiplier", 0.5), "score")

    if not multipliers:
        return 1.0, ""

    mult = min(multipliers)
    risk_count = len(set(categories))
    multi_cfg = policy.get("multi_condition", {}) if isinstance(policy.get("multi_condition"), dict) else {}
    if risk_count >= 3:
        mult = min(mult, float(_to_float(multi_cfg.get("three_or_more_multiplier"), mult) or mult))
    elif risk_count >= 2:
        mult = min(mult, float(_to_float(multi_cfg.get("two_or_more_multiplier"), mult) or mult))
    mult = max(0.0, min(1.0, mult))
    if not (0.0 < mult < 1.0):
        return 1.0, ""
    return mult, f"execution_quality_sizing={','.join(reasons)}:risk_count={risk_count}:mult={mult:.2f}"


def _calc_surge_reversal_signals(
    *,
    close_hist: List[float],
    volume_hist: List[float],
    surge_volume: float,
    high_since_entry: float,
    reversal_cfg: Dict[str, Any],
) -> List[str]:
    signals: List[str] = []
    volume_ratio_max = float(_to_float(reversal_cfg.get("volume_exhaustion_ratio_max"), 0.5))
    high_rej_min_dd = _pct01_from_config(reversal_cfg.get("high_rejection_min_drawdown_pct"), 0.0)

    if len(volume_hist) >= 4 and surge_volume > 0:
        post_avg = float(pd.Series(volume_hist[1:][-3:], dtype="float64").mean())
        if (post_avg / float(surge_volume)) < volume_ratio_max:
            signals.append("VOLUME_EXHAUSTION")

    if len(close_hist) >= 4 and high_since_entry > 0:
        recent = pd.Series(close_hist[-3:], dtype="float64")
        drawdown = (float(recent.iloc[-1]) / float(high_since_entry)) - 1.0
        if (
            bool((recent.diff().dropna() < 0).all())
            and float(recent.iloc[-1]) < float(high_since_entry)
            and drawdown <= -abs(high_rej_min_dd)
        ):
            signals.append("HIGH_REJECTION")

    if len(close_hist) >= 16:
        rsi_prev = _calc_rsi(close_hist[:-1], 14)
        rsi_curr = _calc_rsi(close_hist, 14)
        if rsi_prev >= 70.0 and rsi_curr < 70.0:
            signals.append("RSI_BREAKDOWN")

    if len(close_hist) >= 21:
        upper_prev = _bollinger_upper_from_closes(close_hist[:-1], 20, 2.0)
        upper_curr = _bollinger_upper_from_closes(close_hist, 20, 2.0)
        if upper_prev is not None and upper_curr is not None:
            if float(close_hist[-2]) > float(upper_prev) and float(close_hist[-1]) <= float(upper_curr):
                signals.append("BOLLINGER_REENTRY")

    return signals


def _surge_after_state_from_active_label(label: Any) -> str:
    label_text = _clean_active_response_cell(label).upper()
    if label_text == "ACTIVE_ENTRY_READY":
        return "SURGE_AFTER_ACTIVE_READY"
    if label_text == "PROBE_READY":
        return "SURGE_AFTER_PROBE_READY"
    if label_text == "WAIT_RECLAIM":
        return "SURGE_AFTER_WAIT_RECLAIM"
    if label_text == "WAIT_LOB":
        return "SURGE_AFTER_WAIT_LOB"
    if label_text == "HARD_EXCLUDE":
        return "SURGE_AFTER_HARD_EXCLUDE"
    return ""


def _load_surge_active_response_alert_rows(today_ymd: str) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    status: Dict[str, Any] = {
        "source": str(SURGE_ACTIVE_RESPONSE_LAYER_CSV_PATH),
        "exists": bool(SURGE_ACTIVE_RESPONSE_LAYER_CSV_PATH.exists()),
        "rows": 0,
        "matched_rows": 0,
        "label_counts": {},
        "reason": "",
    }
    if not SURGE_ACTIVE_RESPONSE_LAYER_CSV_PATH.exists():
        status["reason"] = "active_response_csv_missing"
        return [], status
    try:
        df = pd.read_csv(SURGE_ACTIVE_RESPONSE_LAYER_CSV_PATH, dtype={"code": str})
    except Exception as exc:
        status["reason"] = f"active_response_csv_read_fail:{type(exc).__name__}"
        return [], status
    if not isinstance(df, pd.DataFrame) or df.empty:
        status["reason"] = "active_response_csv_empty"
        return [], status
    status["rows"] = int(len(df))
    if "code" not in df.columns:
        status["reason"] = "active_response_csv_no_code"
        return [], status
    out = df.copy()
    out["code"] = out["code"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(6)
    out = out[out["code"].str.len().eq(6)].copy()
    if "date" in out.columns:
        date_norm = out["date"].map(_norm_ymd_text)
        out = out[(date_norm == str(today_ymd)) | (date_norm == "")].copy()
    if out.empty:
        status["reason"] = f"active_response_no_today_rows:{today_ymd}"
        return [], status
    if "active_response_label" in out.columns:
        label_series = out["active_response_label"].map(lambda v: _clean_active_response_cell(v).upper())
        status["label_counts"] = label_series.value_counts(dropna=False).to_dict()
    rows = out.to_dict("records")
    status["matched_rows"] = int(len(rows))
    status["reason"] = "ok"
    return rows, status


def _load_surge_live_readiness_audit_rows(today_ymd: str) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    status: Dict[str, Any] = {
        "source": str(SURGE_LIVE_READINESS_AUDIT_CSV_PATH),
        "exists": bool(SURGE_LIVE_READINESS_AUDIT_CSV_PATH.exists()),
        "rows": 0,
        "matched_rows": 0,
        "paper_ready_count": 0,
        "paper_readiness_counts": {},
        "reason": "",
    }
    if not SURGE_LIVE_READINESS_AUDIT_CSV_PATH.exists():
        status["reason"] = "live_readiness_audit_csv_missing"
        return {}, status
    try:
        df = pd.read_csv(SURGE_LIVE_READINESS_AUDIT_CSV_PATH, dtype={"code": str})
    except Exception as exc:
        status["reason"] = f"live_readiness_audit_csv_read_fail:{type(exc).__name__}"
        return {}, status
    if not isinstance(df, pd.DataFrame) or df.empty:
        status["reason"] = "live_readiness_audit_csv_empty"
        return {}, status
    status["rows"] = int(len(df))
    if "code" not in df.columns:
        status["reason"] = "live_readiness_audit_csv_no_code"
        return {}, status
    out = df.copy()
    out["code"] = out["code"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(6)
    out = out[out["code"].str.len().eq(6)].copy()
    if "source_ts" in out.columns:
        source_dates = out["source_ts"].map(_norm_ymd_text)
        out = out[(source_dates == str(today_ymd)) | (source_dates == "")].copy()
    if out.empty:
        status["reason"] = f"live_readiness_audit_no_today_rows:{today_ymd}"
        return {}, status
    if "paper_order_readiness" in out.columns:
        readiness_series = out["paper_order_readiness"].map(lambda v: _clean_active_response_cell(v).upper())
        status["paper_readiness_counts"] = readiness_series.value_counts(dropna=False).to_dict()
        status["paper_ready_count"] = int((readiness_series == "PAPER_READY").sum())
    rows_by_code: Dict[str, Dict[str, Any]] = {}
    for row in out.to_dict("records"):
        code = str(row.get("code", "") or "").zfill(6)
        if len(code) == 6 and code.isdigit():
            rows_by_code[code] = row
    status["matched_rows"] = int(len(rows_by_code))
    status["reason"] = "ok"
    return rows_by_code, status


def _inject_surge_immediate_candidates(
    candidate_df: pd.DataFrame,
    *,
    cfg: Dict[str, Any],
    intraday_realtime_mode: bool,
    open_codes: set[str],
    today_ymd: str,
    today_buy_code_counts: Optional[Dict[str, int]] = None,
) -> tuple[pd.DataFrame, Dict[str, Any]]:
    status: Dict[str, Any] = {
        "enabled": False,
        "applied": False,
        "added": 0,
        "updated_existing": 0,
        "selected_codes": [],
        "no_lob_selected_count": 0,
        "disclosure_blocked_count": 0,
        "disclosure_blocked_codes": [],
        "disclosure_status": "",
        "reason": "",
    }
    policy = cfg.get("surge_entry_policy", {}) if isinstance(cfg.get("surge_entry_policy"), dict) else {}
    if not bool(policy.get("enabled", False)):
        status["reason"] = "disabled"
        return candidate_df, status
    status["enabled"] = True
    if bool(policy.get("realtime_only", True)) and not intraday_realtime_mode:
        status["reason"] = "realtime_only"
        return candidate_df, status
    status["afternoon_session_block_count"] = 0
    status["price_vol_divergence_block_count"] = 0
    status["choppy_trend_block_count"] = 0

    if not SURGE_REALTIME_STATUS_PATH.exists():
        status["reason"] = "surge_file_missing"
        return candidate_df, status

    try:
        rt_obj = json.loads(SURGE_REALTIME_STATUS_PATH.read_text(encoding="utf-8"))
    except Exception:
        try:
            rt_obj = json.loads(SURGE_REALTIME_STATUS_PATH.read_text(encoding="utf-8-sig"))
        except Exception as e:
            status["reason"] = f"surge_file_read_fail:{type(e).__name__}"
            return candidate_df, status

    max_age_min = float(_to_float(policy.get("realtime_alert_max_age_minutes"), 10.0) or 0.0)
    if max_age_min > 0:
        raw_ts = str(
            rt_obj.get("ts")
            or rt_obj.get("generated_at")
            or rt_obj.get("as_of")
            or rt_obj.get("as_of_ymd")
            or ""
        ).strip()
        if not raw_ts:
            status["reason"] = "surge_file_missing_ts"
            status["max_age_minutes"] = float(max_age_min)
            return candidate_df, status
        try:
            ts_text = raw_ts
            if re.fullmatch(r"\d{8}", ts_text):
                ts_text = f"{ts_text[:4]}-{ts_text[4:6]}-{ts_text[6:8]}T00:00:00"
            rt_ts = datetime.fromisoformat(ts_text.replace("Z", "+00:00"))
            if rt_ts.tzinfo is not None:
                rt_ts = rt_ts.astimezone().replace(tzinfo=None)
            age_min = (datetime.now() - rt_ts).total_seconds() / 60.0
        except Exception as exc:
            status["reason"] = f"surge_file_ts_parse_fail:{type(exc).__name__}"
            status["surge_ts"] = raw_ts
            status["max_age_minutes"] = float(max_age_min)
            return candidate_df, status
        status["surge_ts"] = raw_ts
        status["age_minutes"] = round(float(age_min), 3)
        status["max_age_minutes"] = float(max_age_min)
        if age_min < -1.0:
            status["reason"] = f"surge_file_future_ts:{age_min:.1f}min"
            return candidate_df, status
        if age_min > max_age_min:
            status["reason"] = f"surge_file_stale:{age_min:.1f}min>{max_age_min:.1f}min"
            return candidate_df, status

    alerts = rt_obj.get("alerts") if isinstance(rt_obj.get("alerts"), list) else []
    active_response_rows, active_response_status = _load_surge_active_response_alert_rows(str(today_ymd))
    live_readiness_by_code, live_readiness_status = _load_surge_live_readiness_audit_rows(str(today_ymd))
    active_response_by_code: Dict[str, Dict[str, Any]] = {}
    for active_row in active_response_rows:
        a_code = str(active_row.get("code", "") or "").zfill(6)
        if len(a_code) == 6 and a_code.isdigit():
            active_response_by_code[a_code] = active_row
    status["active_response_layer"] = active_response_status
    status["surge_live_readiness_audit"] = live_readiness_status
    status["alert_source"] = "surge_realtime_json"
    if not alerts and active_response_rows:
        alerts = active_response_rows
        status["alert_source"] = "active_response_csv_fallback"
        status["alerts_fallback_rows"] = int(len(active_response_rows))
    elif alerts and active_response_rows:
        alert_codes = {
            str(row.get("code", "") or "").zfill(6)
            for row in alerts
            if isinstance(row, dict)
        }
        merged_active_rows: List[Dict[str, Any]] = []
        for active_row in active_response_rows:
            if not isinstance(active_row, dict):
                continue
            active_code = str(active_row.get("code", "") or "").zfill(6)
            if len(active_code) != 6 or not active_code.isdigit() or active_code in alert_codes:
                continue
            alerts.append(active_row)
            alert_codes.add(active_code)
            merged_active_rows.append(active_row)
        if merged_active_rows:
            status["alert_source"] = "surge_realtime_json_plus_active_response_csv"
            status["alerts_active_response_merged_rows"] = int(len(merged_active_rows))
    if not alerts:
        status["reason"] = "no_alerts"
        return candidate_df, status
    intraday_date = re.sub(r"[^0-9]", "", str(rt_obj.get("intraday_date") or ""))[:8]
    if intraday_date and intraday_date != str(today_ymd):
        status["reason"] = f"surge_intraday_date_mismatch:{intraday_date}!={today_ymd}"
        status["intraday_date"] = intraday_date
        status["today_ymd"] = str(today_ymd)
        return candidate_df, status

    min_score_final = float(_to_float(policy.get("min_score_final"), 75.0))
    top_n = max(1, int(_to_int(policy.get("top_n"), 3)))
    per_symbol_alloc_pct = _pct01_from_config(policy.get("per_symbol_alloc_pct", 0.02), 0.02)
    total_alloc_pct = _pct01_from_config(policy.get("total_alloc_pct", 0.06), 0.06)
    no_lob_policy = policy.get("no_lob_probe", {}) if isinstance(policy.get("no_lob_probe"), dict) else {}
    no_lob_max_selected = max(0, _to_int(no_lob_policy.get("max_selected"), 0))
    dyn_policy = policy.get("dynamic_max_new", {}) if isinstance(policy.get("dynamic_max_new"), dict) else {}
    dyn_quality_enabled = bool(dyn_policy.get("enabled", False))
    dyn_spread_max = float(_to_float(dyn_policy.get("medium_spread_bps_max", 40.0), 40.0))
    dyn_risk_max = float(_to_float(dyn_policy.get("orderflow_risk_score_max", 0.6), 0.6))
    paper_probe_policy = policy.get("paper_probe", {}) if isinstance(policy.get("paper_probe"), dict) else {}
    paper_probe_enabled = bool(paper_probe_policy.get("enabled", False))
    paper_probe_min_score = float(_to_float(paper_probe_policy.get("min_score_final"), 0.0) or 0.0)
    paper_probe_max_score_gap = float(_to_float(paper_probe_policy.get("max_score_gap"), 0.0) or 0.0)
    max_same_code_per_day = max(0, int(_to_int(policy.get("max_same_code_per_day"), 0) or 0))
    if per_symbol_alloc_pct <= 0 or total_alloc_pct <= 0:
        status["reason"] = "invalid_alloc_pct"
        return candidate_df, status

    disclosure_blocked_codes, disclosure_status, disclosure_generated_at, disclosure_fundamental_codes = _load_disclosure_negative_codes_for_entry()
    status["disclosure_status"] = disclosure_status

    budget_n = max(1, int(math.floor(total_alloc_pct / max(per_symbol_alloc_pct, 1e-9))))
    select_limit = max(1, min(top_n, budget_n))
    selected_alerts: List[Dict[str, Any]] = []
    for row in alerts:
        if not isinstance(row, dict):
            continue
        code = str(row.get("code", "")).zfill(6)
        if len(code) != 6 or not code.isdigit():
            continue
        alert_date = re.sub(r"[^0-9]", "", str(row.get("date") or intraday_date or ""))[:8]
        if alert_date and alert_date != str(today_ymd):
            continue
        if code in open_codes:
            continue
        active_src = active_response_by_code.get(code, {}) if active_response_by_code else {}
        active_label = _clean_active_response_cell(row.get("active_response_label") or active_src.get("active_response_label")).upper()
        active_reason = _clean_active_response_cell(row.get("active_response_reason") or active_src.get("active_response_reason"))
        active_next_check = _clean_active_response_cell(row.get("active_response_next_check") or active_src.get("active_response_next_check"))
        active_after_state = _surge_after_state_from_active_label(active_label)
        live_readiness_src = live_readiness_by_code.get(code, {}) if live_readiness_by_code else {}
        paper_order_readiness = _clean_active_response_cell(live_readiness_src.get("paper_order_readiness")).upper()
        paper_order_blockers = _clean_active_response_cell(live_readiness_src.get("paper_order_blockers"))
        live_trade_readiness = _clean_active_response_cell(live_readiness_src.get("live_trade_readiness")).upper()
        live_trade_blockers = _clean_active_response_cell(live_readiness_src.get("live_trade_blockers"))
        raw_entry_allowed = (
            row.get("entry_allowed")
            if _has_source_entry_cell(row.get("entry_allowed"))
            else active_src.get("entry_allowed", "")
        )
        raw_entry_blocked = (
            row.get("entry_blocked")
            if _has_source_entry_cell(row.get("entry_blocked"))
            else active_src.get("entry_blocked", "")
        )
        source_entry_decision = _clean_active_response_cell(row.get("entry_decision") or active_src.get("entry_decision")).upper()
        source_entry_reason = _clean_active_response_cell(row.get("entry_reason") or active_src.get("entry_reason"))
        reclaim_probe_transition = _clean_active_response_cell(row.get("reclaim_probe_transition") or active_src.get("reclaim_probe_transition")).upper()
        has_entry_allowed = _clean_active_response_cell(raw_entry_allowed) != ""
        paper_probe_readiness_override = bool(
            active_label == "PROBE_READY"
            and paper_order_readiness == "PAPER_READY"
            and _truthy(live_readiness_src.get("paper_order_route", ""))
        )
        if active_label:
            arc = status.setdefault("active_response_label_counts", {})
            if isinstance(arc, dict):
                arc[active_label] = int(arc.get(active_label, 0) or 0) + 1
            if active_label in {"WAIT_RECLAIM", "WAIT_LOB", "HARD_EXCLUDE"}:
                wait_key = f"active_response_{active_label.lower()}_count"
                status[wait_key] = int(status.get(wait_key, 0) or 0) + 1
                blocked_codes = status.setdefault(f"active_response_{active_label.lower()}_codes", [])
                if isinstance(blocked_codes, list) and code not in blocked_codes:
                    blocked_codes.append(code)
                continue
            if active_label not in {"ACTIVE_ENTRY_READY", "PROBE_READY"}:
                status["active_response_unknown_label_count"] = int(status.get("active_response_unknown_label_count", 0) or 0) + 1
                blocked_codes = status.setdefault("active_response_unknown_label_codes", [])
                if isinstance(blocked_codes, list) and code not in blocked_codes:
                    blocked_codes.append(code)
                continue
            if paper_order_readiness != "PAPER_READY" or not _truthy(live_readiness_src.get("paper_order_route", "")):
                status["surge_live_readiness_blocked_count"] = int(status.get("surge_live_readiness_blocked_count", 0) or 0) + 1
                blocked_codes = status.setdefault("surge_live_readiness_blocked_codes", [])
                if isinstance(blocked_codes, list) and code not in blocked_codes:
                    blocked_codes.append(code)
                blockers_by_code = status.setdefault("surge_live_readiness_blockers_by_code", {})
                if isinstance(blockers_by_code, dict):
                    blockers_by_code[code] = paper_order_blockers or paper_order_readiness or "PAPER_ORDER_READINESS_NOT_READY"
                continue
            if (
                (
                    has_entry_allowed
                    and not _truthy(raw_entry_allowed)
                    and source_entry_decision not in {"WAIT_EXECUTION"}
                )
                or _truthy(raw_entry_blocked)
                or source_entry_decision == "ENTRY_BLOCKED"
            ) and not paper_probe_readiness_override:
                status["active_response_source_entry_blocked_count"] = int(status.get("active_response_source_entry_blocked_count", 0) or 0) + 1
                blocked_codes = status.setdefault("active_response_source_entry_blocked_codes", [])
                if isinstance(blocked_codes, list) and code not in blocked_codes:
                    blocked_codes.append(code)
                reasons_by_code = status.setdefault("active_response_source_entry_blocked_reasons_by_code", {})
                if isinstance(reasons_by_code, dict):
                    reasons_by_code[code] = source_entry_reason or source_entry_decision or "SOURCE_ENTRY_BLOCKED"
                continue
        if max_same_code_per_day > 0 and today_buy_code_counts is not None:
            today_count = int(today_buy_code_counts.get(code, 0) or 0)
            if today_count >= max_same_code_per_day:
                status.setdefault("same_code_day_capped", [])
                if isinstance(status.get("same_code_day_capped"), list) and code not in status["same_code_day_capped"]:
                    status["same_code_day_capped"].append(code)
                continue
        if code in disclosure_blocked_codes:
            signal_ts = _parse_policy_dt_text(row.get("ts") or row.get("generated_at") or rt_obj.get("ts"))
            is_fundamental = code in disclosure_fundamental_codes
            do_block = (
                is_fundamental
                or disclosure_generated_at is None
                or signal_ts is None
                or disclosure_generated_at <= signal_ts
            )
            if do_block:
                blocked_codes = status.setdefault("disclosure_blocked_codes", [])
                if isinstance(blocked_codes, list) and code not in blocked_codes:
                    blocked_codes.append(code)
                status["disclosure_blocked_count"] = int(status.get("disclosure_blocked_count", 0) or 0) + 1
                continue
        score_final = float(_to_float(row.get("surge_score_final"), 0.0) or 0.0)
        market_block_reason = _surge_market_event_block_reason(row, cfg)
        if market_block_reason:
            status["market_event_blocked_count"] = int(status.get("market_event_blocked_count", 0) or 0) + 1
            blocked_codes = status.setdefault("market_event_blocked_codes", [])
            if isinstance(blocked_codes, list) and code not in blocked_codes:
                blocked_codes.append(code)
            reasons_by_code = status.setdefault("market_event_block_reasons_by_code", {})
            if isinstance(reasons_by_code, dict):
                reasons_by_code[code] = market_block_reason
            continue
        type_allowed, type_mult, type_reason, type_overrides = _surge_type_policy_decision(row, cfg)
        if not type_allowed:
            wait_reclaim_probe_cfg = policy.get("wait_reclaim_paper_probe", {}) if isinstance(policy.get("wait_reclaim_paper_probe"), dict) else {}
            wait_reclaim_allowed_types = {
                str(x).strip().upper()
                for x in (wait_reclaim_probe_cfg.get("allowed_types") or [])
                if str(x).strip()
            }
            surge_type_for_probe = str(row.get("surge_type", "") or "").strip().upper()
            type_reason_key = str(type_reason or "")
            type_policy_hard_blocked = type_reason_key.startswith("SURGE_TYPE_BLOCK")
            wait_reclaim_type_override_candidate = bool(
                active_label == "PROBE_READY"
                and paper_order_readiness == "PAPER_READY"
                and _truthy(live_readiness_src.get("paper_order_route", ""))
                and reclaim_probe_transition == "WAIT_RECLAIM_TO_PROBE_READY"
                and bool(wait_reclaim_probe_cfg.get("enabled", False))
                and bool(wait_reclaim_probe_cfg.get("bypass_type_block", False))
                and (not wait_reclaim_allowed_types or surge_type_for_probe in wait_reclaim_allowed_types)
            )
            wait_reclaim_type_override = bool(
                wait_reclaim_type_override_candidate
                and not type_policy_hard_blocked
            )
            if wait_reclaim_type_override:
                type_policy_cfg = policy.get("type_policy", {}) if isinstance(policy.get("type_policy"), dict) else {}
                mult_map = type_policy_cfg.get("type_qty_multiplier", {}) if isinstance(type_policy_cfg.get("type_qty_multiplier"), dict) else {}
                overrides_map = type_policy_cfg.get("type_overrides", {}) if isinstance(type_policy_cfg.get("type_overrides"), dict) else {}
                type_mult = float(_to_float(mult_map.get(surge_type_for_probe), 1.0))
                type_mult = max(0.0, min(1.0, type_mult))
                type_overrides = dict((overrides_map.get(surge_type_for_probe) or {}) if surge_type_for_probe else {})
                status["wait_reclaim_type_override_count"] = int(status.get("wait_reclaim_type_override_count", 0) or 0) + 1
                override_codes = status.setdefault("wait_reclaim_type_override_codes", [])
                if isinstance(override_codes, list) and code not in override_codes:
                    override_codes.append(code)
                type_allowed = True
                type_reason = f"WAIT_RECLAIM_PROBE_TYPE_OVERRIDE({surge_type_for_probe};{type_reason})"
            else:
                if wait_reclaim_type_override_candidate and type_policy_hard_blocked:
                    status["wait_reclaim_type_override_hard_block_denied_count"] = int(status.get("wait_reclaim_type_override_hard_block_denied_count", 0) or 0) + 1
                    hard_block_denied_codes = status.setdefault("wait_reclaim_type_override_hard_block_denied_codes", [])
                    if isinstance(hard_block_denied_codes, list) and code not in hard_block_denied_codes:
                        hard_block_denied_codes.append(code)
                status["surge_type_blocked_count"] = int(status.get("surge_type_blocked_count", 0) or 0) + 1
                reason_key = type_reason_key
                if reason_key.startswith("AFTERNOON_SESSION_BLOCK"):
                    status["afternoon_session_block_count"] = int(status.get("afternoon_session_block_count", 0) or 0) + 1
                elif reason_key.startswith("PRICE_VOL_DIVERGENCE_BLOCK"):
                    status["price_vol_divergence_block_count"] = int(status.get("price_vol_divergence_block_count", 0) or 0) + 1
                elif reason_key.startswith("CHOPPY_TREND"):
                    status["choppy_trend_block_count"] = int(status.get("choppy_trend_block_count", 0) or 0) + 1
                blocked_codes = status.setdefault("surge_type_blocked_codes", [])
                if isinstance(blocked_codes, list) and code not in blocked_codes:
                    blocked_codes.append(code)
                reasons_by_code = status.setdefault("surge_type_block_reasons_by_code", {})
                if isinstance(reasons_by_code, dict):
                    reasons_by_code[code] = str(type_reason or "SURGE_TYPE_BLOCK")
                continue
        _type_min_score = float(_to_float(type_overrides.get("min_score_final"), min_score_final) or min_score_final)
        if score_final < _type_min_score:
            paper_probe_allowed = bool(row.get("paper_probe_allowed", False))
            paper_probe_score_ok = bool(
                paper_probe_enabled
                and paper_probe_allowed
                and score_final >= paper_probe_min_score
                and (_type_min_score - score_final) <= max(0.0, paper_probe_max_score_gap)
            )
            if not paper_probe_score_ok:
                if paper_probe_allowed:
                    status["paper_probe_score_blocked_count"] = int(status.get("paper_probe_score_blocked_count", 0) or 0) + 1
                    blocked_codes = status.setdefault("paper_probe_score_blocked_codes", [])
                    if isinstance(blocked_codes, list) and code not in blocked_codes:
                        blocked_codes.append(code)
                continue
            status["paper_probe_selected_count"] = int(status.get("paper_probe_selected_count", 0) or 0) + 1
        _tag = str(row.get("orderflow_tag", "") or "").strip().upper()
        _lob_status = str(row.get("lob_status", "") or "").strip().upper()
        _spread_raw = _to_float(row.get("spread_bps"), None)
        _risk_score_raw = _to_float(row.get("orderflow_risk_score"), None)
        _quality_missing_fields: List[str] = []
        if _spread_raw is None:
            _quality_missing_fields.append("spread_bps")
        if _risk_score_raw is None:
            _quality_missing_fields.append("orderflow_risk_score")
        if not _tag or _tag in {"UNKNOWN", "NAN", "NONE", "NULL"}:
            _quality_missing_fields.append("orderflow_tag")
        if not _lob_status or _lob_status in {"UNKNOWN", "NAN", "NONE", "NULL"}:
            _quality_missing_fields.append("lob_status")
        if _quality_missing_fields:
            status["missing_quality_blocked_count"] = int(status.get("missing_quality_blocked_count", 0) or 0) + 1
            blocked_codes = status.setdefault("missing_quality_blocked_codes", [])
            if isinstance(blocked_codes, list) and code not in blocked_codes:
                blocked_codes.append(code)
            missing_by_code = status.setdefault("missing_quality_fields_by_code", {})
            if isinstance(missing_by_code, dict):
                missing_by_code[code] = sorted(set(_quality_missing_fields))
            continue
        _orderflow_no_history_with_lob = bool(_tag == "NO_HISTORY" and _lob_status == "OK")
        _is_no_lob = bool(
            row.get("no_lob_probe_allowed", False)
            or _tag == "NO_LOB"
            or _lob_status == "NO_LOB"
            or (_tag == "NO_HISTORY" and not _orderflow_no_history_with_lob)
        )
        if _orderflow_no_history_with_lob:
            status["orderflow_no_history_with_lob_ok_count"] = int(status.get("orderflow_no_history_with_lob_ok_count", 0) or 0) + 1
        _no_lob_probe_allowed = bool(row.get("no_lob_probe_allowed", False))
        if _is_no_lob and not _no_lob_probe_allowed:
            status["no_lob_blocked_count"] = int(status.get("no_lob_blocked_count", 0) or 0) + 1
            blocked_codes = status.setdefault("no_lob_blocked_codes", [])
            if isinstance(blocked_codes, list) and code not in blocked_codes:
                blocked_codes.append(code)
            continue
        if _is_no_lob and _no_lob_probe_allowed:
            status["no_lob_probe_selected_pre_cap_count"] = int(status.get("no_lob_probe_selected_pre_cap_count", 0) or 0) + 1
        if dyn_quality_enabled:
            _spread = float(_spread_raw if _spread_raw is not None else dyn_spread_max)
            _risk_score = float(_risk_score_raw if _risk_score_raw is not None else 0.0)
            if _spread > dyn_spread_max or _risk_score > dyn_risk_max:
                status["dynamic_quality_blocked_count"] = int(status.get("dynamic_quality_blocked_count", 0) or 0) + 1
                blocked_codes = status.setdefault("dynamic_quality_blocked_codes", [])
                if isinstance(blocked_codes, list) and code not in blocked_codes:
                    blocked_codes.append(code)
                continue
        selected_alerts.append(
            {
                "code": code,
                "surge_score": float(_to_float(row.get("surge_score"), score_final) or score_final),
                "surge_score_final": score_final,
                "surge_type": str(row.get("surge_type", "") or ""),
                "surge_type_qty_multiplier": float(type_mult),
                "surge_type_policy_note": str(type_reason or ""),
                "surge_type_overrides": type_overrides,
                "v_accel": float(_to_float(row.get("v_accel", row.get("rvol20")), 0.0) or 0.0),
                "rvol20": float(_to_float(row.get("rvol20"), 0.0) or 0.0),
                "ret1_pct": float(_to_float(row.get("ret1_pct"), _to_float(row.get("change_pct"), 0.0) * 100.0) or 0.0),
                "day_range_pct": float(_to_float(row.get("day_range_pct"), 0.0) or 0.0),
                "current_price": float(_to_float(row.get("current_price"), 0.0) or 0.0),
                "prev_close": float(_to_float(row.get("prev_close"), 0.0) or 0.0),
                "spread_bps": float(_to_float(row.get("spread_bps") or active_src.get("spread_bps"), 0.0) or 0.0),
                "markout_1step_bps": float(_to_float(row.get("markout_1step_bps") or active_src.get("markout_1step_bps"), 0.0) or 0.0),
                "orderflow_risk_score": float(_to_float(row.get("orderflow_risk_score") or active_src.get("orderflow_risk_score"), 0.0) or 0.0),
                "orderflow_tag": str(row.get("orderflow_tag") or active_src.get("orderflow_tag") or ""),
                "lob_status": str(row.get("lob_status") or active_src.get("lob_status") or ""),
                "active_response_label": active_label,
                "active_response_reason": active_reason,
                "active_response_next_check": active_next_check,
                "reclaim_probe_transition": reclaim_probe_transition,
                "reclaim_check_status": _clean_active_response_cell(row.get("reclaim_check_status") or active_src.get("reclaim_check_status")),
                "reclaim_evidence": _clean_active_response_cell(row.get("reclaim_evidence") or active_src.get("reclaim_evidence")),
                "surge_after_state": active_after_state,
                "surge_paper_order_readiness": paper_order_readiness,
                "surge_paper_order_blockers": paper_order_blockers,
                "surge_paper_order_route": bool(_truthy(live_readiness_src.get("paper_order_route", ""))),
                "surge_live_trade_readiness": live_trade_readiness,
                "surge_live_trade_blockers": live_trade_blockers,
                "source_entry_allowed": raw_entry_allowed,
                "source_entry_blocked": raw_entry_blocked,
                "source_entry_decision": source_entry_decision,
                "source_entry_reason": source_entry_reason,
                "no_lob_probe_allowed": bool(_truthy(row.get("no_lob_probe_allowed", "")) or _truthy(active_src.get("no_lob_probe_allowed", ""))),
                "paper_probe_allowed": bool(_truthy(row.get("paper_probe_allowed", "")) or _truthy(active_src.get("paper_probe_allowed", ""))),
                "paper_probe_block_reasons": str(row.get("paper_probe_block_reasons") or active_src.get("paper_probe_block_reasons") or ""),
                "paper_policy_relaxations": str(row.get("paper_policy_relaxations") or active_src.get("paper_policy_relaxations") or ""),
                "ask_depth_levels": float(_to_float(row.get("ask_depth_levels"), 0.0) or 0.0),
                **{
                    f"ask{i}": float(_to_float(row.get(f"ask{i}"), 0.0) or 0.0)
                    for i in range(1, 11)
                },
                **{
                    f"askq{i}": float(_to_float(row.get(f"askq{i}"), 0.0) or 0.0)
                    for i in range(1, 11)
                },
                "exclude_reasons": str(row.get("exclude_reasons", "") or ""),
            }
        )

    if not selected_alerts:
        if int(status.get("disclosure_blocked_count", 0) or 0) > 0:
            status["reason"] = "disclosure_negative_block"
        elif isinstance(status.get("same_code_day_capped"), list) and status["same_code_day_capped"]:
            status["reason"] = "same_code_day_capped"
        elif int(status.get("no_lob_blocked_count", 0) or 0) > 0:
            status["reason"] = "no_lob_blocked"
        elif int(status.get("missing_quality_blocked_count", 0) or 0) > 0:
            status["reason"] = "missing_quality_blocked"
        elif int(status.get("dynamic_quality_blocked_count", 0) or 0) > 0:
            status["reason"] = "dynamic_quality_blocked"
        elif int(status.get("market_event_blocked_count", 0) or 0) > 0:
            status["reason"] = "surge_market_event_block"
        elif int(status.get("surge_type_blocked_count", 0) or 0) > 0:
            status["reason"] = "surge_type_blocked"
        elif int(status.get("paper_probe_score_blocked_count", 0) or 0) > 0:
            status["reason"] = "paper_probe_score_blocked"
        elif int(status.get("paper_probe_shadow_only_blocked_count", 0) or 0) > 0:
            status["reason"] = "paper_probe_shadow_only"
        else:
            status["reason"] = "below_threshold"
        return candidate_df, status

    def _surge_entry_selection_priority(alert: Dict[str, Any]) -> tuple[int, float]:
        active_label = str(alert.get("active_response_label", "") or "").strip().upper()
        paper_ready = str(alert.get("surge_paper_order_readiness", "") or "").strip().upper()
        entry_decision = str(alert.get("entry_decision", "") or "").strip().upper()
        source_blocked = bool(str(alert.get("exclude_reasons", "") or "").strip())
        immediate_ready = (
            active_label == "ACTIVE_ENTRY_READY"
            and paper_ready == "PAPER_READY"
            and not source_blocked
        )
        if immediate_ready:
            return (0, -float(alert.get("surge_score_final", 0.0) or 0.0))
        if active_label == "PROBE_READY" and paper_ready == "PAPER_READY" and not source_blocked:
            return (1, -float(alert.get("surge_score_final", 0.0) or 0.0))
        if active_label == "PROBE_READY":
            return (2, -float(alert.get("surge_score_final", 0.0) or 0.0))
        if entry_decision == "ENTRY_ALLOWED" and not source_blocked:
            return (3, -float(alert.get("surge_score_final", 0.0) or 0.0))
        return (4, -float(alert.get("surge_score_final", 0.0) or 0.0))

    selected_alerts = sorted(selected_alerts, key=_surge_entry_selection_priority)
    if no_lob_max_selected > 0:
        capped_alerts: List[Dict[str, Any]] = []
        no_lob_selected = 0
        for al in selected_alerts:
            _tag = str(al.get("orderflow_tag", "") or "").strip().upper()
            _lob_status = str(al.get("lob_status", "") or "").strip().upper()
            _is_no_lob = bool(
                al.get("no_lob_probe_allowed", False)
                or _tag == "NO_LOB"
                or _lob_status == "NO_LOB"
                or (_tag == "NO_HISTORY" and _lob_status != "OK")
            )
            if _is_no_lob:
                if no_lob_selected >= no_lob_max_selected:
                    continue
                no_lob_selected += 1
            capped_alerts.append(al)
        selected_alerts = capped_alerts
        status["no_lob_selected_count"] = int(no_lob_selected)
        status["no_lob_max_selected"] = int(no_lob_max_selected)
    selected_alerts = selected_alerts[:select_limit]
    cdf = candidate_df.copy()
    existing_codes = set(cdf["code"].astype(str).str.zfill(6).tolist()) if "code" in cdf.columns else set()
    add_rows: List[Dict[str, Any]] = []

    for al in selected_alerts:
        code = str(al["code"]).zfill(6)
        status["selected_codes"].append(code)
        _al_overrides: Dict[str, Any] = al.get("surge_type_overrides") or {}
        _type_alloc_pct = _pct01_from_config(_al_overrides["alloc_pct"], per_symbol_alloc_pct) if "alloc_pct" in _al_overrides else per_symbol_alloc_pct
        _type_first_ratio = float(_to_float(_al_overrides.get("first_ratio"), -1.0))
        _type_stop_pct = _to_float(_al_overrides.get("stop_loss_pct"), None)
        _type_tp_pct = _to_float(_al_overrides.get("take_profit_pct"), None)
        _type_max_hold = _to_int(_al_overrides.get("max_hold_days"), 0)
        _type_entry_timing = str(_al_overrides.get("entry_timing", "realtime") or "realtime").strip().lower()
        _type_is_realtime = _type_entry_timing not in {"next_open", "nextopen"}
        mark_values = {
            "_surge_immediate": 1 if _type_is_realtime else 0,
            "surge_per_symbol_alloc_pct": float(_type_alloc_pct),
            "surge_total_alloc_pct": float(total_alloc_pct),
            "signal_date": today_ymd,
            "final_score": float(_to_float(al.get("surge_score_final"), 0.0) or 0.0),
            "score": float(_to_float(al.get("surge_score_final"), 0.0) or 0.0),
            "surge_score": float(_to_float(al.get("surge_score"), al.get("surge_score_final")) or 0.0),
            "surge_score_final": float(_to_float(al.get("surge_score_final"), 0.0) or 0.0),
            "carry_reason": "SURGE_IMMEDIATE" if _type_is_realtime else "SURGE_NEXT_OPEN",
            "entry_timing": _type_entry_timing,
            "surge_type_entry_timing": _type_entry_timing,
            "surge_type": str(al.get("surge_type", "") or ""),
            "surge_type_qty_multiplier": float(_to_float(al.get("surge_type_qty_multiplier"), 1.0)),
            "surge_type_policy_note": str(al.get("surge_type_policy_note", "") or ""),
            "surge_type_first_ratio": float(_type_first_ratio),
            "surge_type_stop_pct": _type_stop_pct,
            "surge_type_tp_pct": _type_tp_pct,
            "surge_type_max_hold_days": int(_type_max_hold),
            "surge_rvol20": float(_to_float(al.get("rvol20"), 0.0) or 0.0),
            "surge_spread_bps": float(_to_float(al.get("spread_bps"), 0.0) or 0.0),
            "surge_orderflow_tag": str(al.get("orderflow_tag", "") or ""),
            "surge_lob_status": str(al.get("lob_status", "") or ""),
            "surge_lob_available": bool(al.get("lob_available", False)),
            "active_response_label": _clean_active_response_cell(al.get("active_response_label")).upper(),
            "active_response_reason": _clean_active_response_cell(al.get("active_response_reason")),
            "active_response_next_check": _clean_active_response_cell(al.get("active_response_next_check")),
            "reclaim_probe_transition": _clean_active_response_cell(al.get("reclaim_probe_transition")).upper(),
            "reclaim_check_status": _clean_active_response_cell(al.get("reclaim_check_status")),
            "reclaim_evidence": _clean_active_response_cell(al.get("reclaim_evidence")),
            "surge_after_state": _surge_after_state_from_active_label(al.get("active_response_label")),
            "surge_paper_order_readiness": _clean_active_response_cell(al.get("surge_paper_order_readiness")).upper(),
            "surge_paper_order_blockers": _clean_active_response_cell(al.get("surge_paper_order_blockers")),
            "surge_paper_order_route": bool(al.get("surge_paper_order_route", False)),
            "paper_order_readiness": _clean_active_response_cell(al.get("surge_paper_order_readiness")).upper(),
            "paper_order_route": bool(al.get("surge_paper_order_route", False)),
            "surge_live_trade_readiness": _clean_active_response_cell(al.get("surge_live_trade_readiness")).upper(),
            "surge_live_trade_blockers": _clean_active_response_cell(al.get("surge_live_trade_blockers")),
            "source_entry_allowed": _source_entry_cell_text(al.get("source_entry_allowed", "")),
            "source_entry_blocked": _source_entry_cell_text(al.get("source_entry_blocked", "")),
            "source_entry_decision": _clean_active_response_cell(al.get("source_entry_decision")).upper(),
            "source_entry_reason": _clean_active_response_cell(al.get("source_entry_reason")),
            "surge_no_lob_probe_allowed": bool(al.get("no_lob_probe_allowed", False)),
            "surge_paper_probe_allowed": bool(al.get("paper_probe_allowed", False)),
            "paper_probe_allowed": bool(al.get("paper_probe_allowed", False)),
            "surge_paper_probe_block_reasons": str(al.get("paper_probe_block_reasons", "") or ""),
            "paper_probe_block_reasons": str(al.get("paper_probe_block_reasons", "") or ""),
            "paper_policy_relaxations": str(al.get("paper_policy_relaxations", "") or ""),
            "surge_orderflow_risk_score": float(_to_float(al.get("orderflow_risk_score"), 0.0) or 0.0),
            "markout_1step_bps": float(_to_float(al.get("markout_1step_bps"), 0.0) or 0.0),
            "ask_depth_levels": float(_to_float(al.get("ask_depth_levels"), 0.0) or 0.0),
            **{
                f"ask{i}": float(_to_float(al.get(f"ask{i}"), 0.0) or 0.0)
                for i in range(1, 11)
            },
            **{
                f"askq{i}": float(_to_float(al.get(f"askq{i}"), 0.0) or 0.0)
                for i in range(1, 11)
            },
            "v_accel": float(_to_float(al.get("v_accel"), 0.0) or 0.0),
            "ret1_pct": float(_to_float(al.get("ret1_pct"), 0.0) or 0.0),
            "day_range_pct": float(_to_float(al.get("day_range_pct"), 0.0) or 0.0),
            "current_price": float(_to_float(al.get("current_price"), 0.0) or 0.0),
            "prev_close": float(_to_float(al.get("prev_close"), 0.0) or 0.0),
            "orderflow_risk_score": float(_to_float(al.get("orderflow_risk_score"), 0.0) or 0.0),
            "orderflow_tag": str(al.get("orderflow_tag", "") or ""),
            "exclude_reasons": str(al.get("exclude_reasons", "") or ""),
            "captured_at": str(rt_obj.get("ts", "") or ""),
        }
        if code in existing_codes:
            mask = cdf["code"].astype(str).str.zfill(6) == code
            for k, v in mark_values.items():
                cdf.loc[mask, k] = v
            status["updated_existing"] = int(status["updated_existing"]) + 1
            continue
        row_new: Dict[str, Any] = {"code": code, "name": "", "signal_date": today_ymd}
        row_new.update(mark_values)
        add_rows.append(row_new)

    if add_rows:
        add_df = pd.DataFrame(add_rows)
        for col in cdf.columns:
            if col not in add_df.columns:
                add_df[col] = pd.NA
        for col in add_df.columns:
            if col not in cdf.columns:
                cdf[col] = pd.NA
        cdf = _concat_drop_all_na_columns([cdf, add_df[cdf.columns]], ignore_index=True)
        status["added"] = int(len(add_rows))
    status["applied"] = bool(int(status["added"]) > 0 or int(status["updated_existing"]) > 0)
    status["reason"] = "ok" if status["applied"] else "no_change"
    return cdf, status
