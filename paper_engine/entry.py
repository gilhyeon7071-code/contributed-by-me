# -*- coding: utf-8 -*-
"""Entry decision layer for paper_engine.

Split out from the legacy ``paper_engine.py`` to keep entry-related layer
decisions (alpha, risk, execution, opportunity, final) and runtime snapshot
writers in one module.
"""
from __future__ import annotations


__all__ = [
    '_entry_source_kind',
    '_entry_source_payload_with_lineage',
    '_entry_layer_rank_score',
    '_entry_alpha_layer_decision',
    '_entry_risk_layer_decision',
    '_entry_execution_layer_decision',
    '_entry_opportunity_decision',
    'final_entry_decision',
    '_write_entry_signal_snapshot',
    '_candidate_snapshot_id',
    '_normal_lob_observe_row',
    '_write_normal_entry_fill_quality_report',
    '_build_entry_risk_basis',
    '_is_split2_wait_reason',
    '_write_entry_decision_layers_snapshot',
    'calc_qty',
    '_apply_p1_calendar_gate',
    '_apply_p1_intraday_gate',
    '_apply_p1_event_gate',
    '_apply_p1_technical_gate',
    'apply_p1_entry_controls',
    '_normal_entry_execution_quality_decision',
    '_normal_exec_quality_qty_reduction',
    '_normal_close_auction_decision',
    '_latest_intraday_row_for_code_date',
    '_normal_intraday_momentum_decision',
    '_normal_reduced_min_qty_block_reason',
    '_normal_dynamic_slippage_pct',
    '_apply_entry_execution_score',
    '_apply_entry_selection_policy',
    '_apply_horizon_entry_policy',
    '_infer_horizon_label_from_row',
    '_horizon_max_hold_days_for_label',
    '_build_surge_entry_note_parts',
    '_resolve_entry_horizon_label',
    '_backfill_horizon_for_existing_order',
    '_write_p1_gate_status',
    '_build_recheck_pending_row',
    '_adaptive_good_stock_cfg',
    '_adaptive_good_stock_route_map',
    '_adaptive_good_stock_entry',
    '_adaptive_good_stock_allowed',
    '_adaptive_good_stock_wait_gapup_route',
    '_adaptive_good_stock_qty_mult',
    '_adaptive_good_stock_gapup_override_max',
    '_mark_adaptive_good_stock',
    'ColMap',
    '_evaluate_split_open_chase_block',
    '_evaluate_open_chase_entry_guard',
    '_evaluate_limit_price_tolerance_guard',
    '_evaluate_entry_gap_limit_guard',
    '_evaluate_entry_gap_risk_guard',
    '_evaluate_entry_liquidity_guard',
    '_resolve_entry_row_slippage',
    '_resolve_initial_entry_qty',
    '_apply_entry_gap_up_reduce_qty',
    '_apply_normal_entry_qty_reductions',
    '_apply_entry_weight_qty_adjustments',
    '_apply_surge_lob_exec_quality',
    '_apply_entry_risk_cap_atr_sizing',
    '_apply_entry_sector_corr_hrp',
    '_apply_entry_post_sector_qty_limits',
    '_apply_entry_ai_cap_min_qty',
    '_apply_entry_cap_limits',
    '_apply_entry_normal_exec_quality',
    '_apply_entry_normal_lob_fill_price_gate',
    '_apply_entry_t2_buy_budget_gate',
    '_prepare_entry_identity_context',
    '_build_entry_note_context',
    '_handle_existing_fill_idempotent_buy',
    '_resolve_split2_target_position_gate',
    '_commit_entry_fill_and_position',
    '_resolve_sector_label',
    '_apply_atr_position_sizing_qty',
    '_ewma_covariance_matrix',
    '_hrp_cluster_order',
    '_hrp_weights_from_cov',
    '_build_sector_correlation_guard',
    '_normal_intraday_realtime_block_reason',
    '_pick_signal_date_candidates_path',
    '_load_signal_date_top_codes_by_score',
    '_ymd_days_ago',
    '_split_second_confirmation_reason',
    'discover_recent_parquets',
    'infer_colmap',
    'load_prices_for_codes',
    '_recent_down_gap_stats',
    '_minimum_quantity_verification_cfg',
    'maybe_run_pnl_report',
    '_merge_guard_decision',
    '_evaluate_entry_guards',
    '_is_sector_fallback_observe_only_row',
    '_sector_fallback_observe_only_mask',
    '_check_positive_entry_criteria',
    '_apply_positive_entry_criteria',
    '_load_defense_signal_entry_map',
    '_apply_defense_signal_entry_policy',
    '_prepare_entry_candidate_pool',
    '_apply_signal_and_sector_caps',
    '_build_signal_tracking_context',
    '_prepare_pretrade_runtime',
    '_init_entry_loop_state',
    '_apply_runtime_caps_and_filters',
    '_find_split2_target_position',
    '_build_entry_fill_row',
    '_archive_entry_source_payload',
    '_normalize_entry_price_for_entry_guard',
    '_evaluate_entry_execution_quality_guard',
    '_resolve_missing_entry_ohlc_action',
    '_resolve_entry_price_with_fallback',
    '_apply_sector_corr_hrp_qty_adjustment',
    '_fit_surge_budget_allocation',
    '_apply_surge_news_overheat_qty_reductions',
    '_fit_split_second_budget',
    '_fit_ai_single_cap',
    '_apply_min_qty_verification_limit',
    '_fit_entry_qty_to_caps',
    '_post_entry_cap_block_reason',
    '_apply_normal_exec_quality',
    '_build_entry_note_seed',
    '_apply_normal_lob_fill_price',
    '_apply_entry_t2_buy_budget',
    '_backfill_idempotent_buy_horizon',
    '_register_entry_idempotency_key',
    '_merge_split_second_entry_position',
    '_is_split_second_carry_expired',
    '_build_split_second_carry_row',
    '_build_entry_base_position',
    '_build_entry_order_id',
    '_build_replay_entry_note_parts',
    '_build_entry_timing_note_parts',
    '_build_split_entry_note_parts',
    '_build_lineage_session_note_parts',
    '_record_buy_executed_decision',
    '_record_entry_t2_cash_if_needed',
    '_normalize_entry_loop_result',
    '_print_entry_fill_summary_v2',
    '_write_entry_runtime_snapshots_and_reports',
    '_run_post_exit_entry_recheck',
    '_process_entry_rows',
    'derive_fx_entry_status',
    'apply_fx_filter',
    'load_candidates_chosen_level',
    'load_candidates_execution_gate',
    'parse_relax_level_num',
    'pick_candidates',
]

import csv
import json
import os
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import pandas as pd

from paper_engine.common import (
    BASE_DIR,
    FX_SCORE_CAUTION_THRESHOLD,
    _entry_truthy,
    _to_float,
    _to_int,
    _truthy,
    _ask_book_from_row,
    _pct01_from_config,
    _normal_realtime_gap_policy,
    _stable_digest,
    now_ts,
    RUN_LABEL,
    _cap_max_new,
    _parse_hhmm,
    _finite_float_or_none,
    _clean_active_response_cell,
    _clean_sector_label,
    _source_entry_cell_text,
    _get_dict,
    _get_list,
    _append_reduction_multiplier,
    _paper_engine_phase_trace,
)
from paper_engine.guards import _detect_explicit_market_events
from paper_engine.io import (
    ENTRY_SIGNAL_SNAPSHOT_PATH,
    ENTRY_DECISION_LAYERS_RUNTIME_CSV_PATH,
    ENTRY_DECISION_LAYERS_RUNTIME_JSON_PATH,
    NORMAL_ENTRY_FILL_QUALITY_REPORT_CSV_PATH,
    NORMAL_ENTRY_FILL_QUALITY_REPORT_JSON_PATH,
    P1_GATE_STATUS_PATH,
    P1_GATE_STATUS_HISTORY_CSV_PATH,
    PENDING_SIGNALS_PATH,
    _latest_lob_row_for_code,
    _load_intraday_history_for_ymd,
    _json_safe as _state_json_safe,
    _write_market_event_guard_status,
)
from utils.common import norm_code, now_ymd
from paper_engine.config import _path_from_env

import math
import re
import subprocess
import sys
from datetime import timedelta
from state_containers import SignalTrackingState
from pricing_engine import calc_entry_fee, calc_qty_by_sizing
from utils.common import (
    read_csv_safe,
)
from paper_engine.drawdown import (
    _ddm_pct01,
    _ddm_sector_concentration,
    _ddm_select_liquidation_targets,
    _ddm_to_float,
)
from paper_engine.positions import (
    _archive_entry_source_row,
    _atomic_write_csv_with_backup,
    _backfill_horizon_note_in_fills,
    _compute_current_open_notional,
    _count_open_position_slots,
    _lineage_from_row,
    _load_pending_signals,
    _load_replay_orders,
    _load_replay_orders_raw,
    _partition_replay_orders_for_recovery,
    _reconcile_open_positions_with_fills,
    _recover_missing_sell_trades_legacy,
    _recover_open_positions,
    _revalidate_pending_signals,
)
from paper_engine.guards import (
    evaluate_backtest_validation_guard,
    evaluate_cross_source_integrity,
    evaluate_execution_health_guard,
    evaluate_global_outlier_watcher,
    evaluate_macro_news_guard,
    evaluate_production_risk_playbook_guard,
    evaluate_sigma_outlier_guard,
)
from paper_engine.exit import (
    _dedupe_partial_exit_fills,
    _dedupe_partial_exit_trades_legacy,
    _dedupe_recovered_trades_legacy,
    _dedupe_sell_fills_by_lifecycle_signature,
    _dedupe_sell_trades_by_lifecycle_signature_legacy,
    _dedupe_trades_by_note_identity_legacy,
    _extract_signal_date,
    _hydrate_position_exit_tags_from_fills,
    _legacy_trade_sig,
    _normalize_partial_sell_order_id,
)
from paper_engine.surge import (
    _overheat_qty_decision,
    _surge_execution_quality_qty_decision,
    _surge_lob_slippage_pct,
    _surge_realtime_shadow_fields,
)
from paper_engine.settlement import (
    _t2_apply_buy_budget,
    _t2_record_buy_cash,
    _set_last_t2_initialized_state,
    _initialize_t2_cash_state,
)
from paper_engine.state import (
    _max_date8_from_candidates,
    _prune_replay_queue_after_quarantine,
    _replay_queue_regeneration_reason,
    _run_replay_queue_regeneration,
    _summarize_replay_queue_scan,
    _sync_rootb_replay_summary,
    _write_recovery_status,
    _write_replay_quarantine,
    _write_replay_queue_status,
)
from paper_engine.io import (
    FILLS,
    LEGACY_FILLS_HEADER,
    LEGACY_TRADES_HEADER,
    LOG_DIR,
    PENDING_STATUS_PATH,
    RECOVERY_STATUS_PATH,
    REPLAY_CONSISTENCY_PATH,
    REPLAY_ORDERS_PATH,
    ROOTB_REPLAY_REGEN_SCRIPT,
    TRADES,
    V411_FILLS_HEADER,
    _derive_d_from_fills_path,
    _json_safe,
    load_state,
    save_state,
)
from paper_engine.common import (
    PAPER_SESSION_ID,
    _classify_asset_type,
    _concat_drop_all_na_columns,
    _estimate_atr14_pct_from_ohlc,
    _extract_note_field,
    _next_krx_session_ymd,
    _norm_ymd_text,
    _ops_policy,
    _v411_trade_sig,
    compute_dynamic_probe_floor,
    get_ohlc,
    next_trading_date,
    prev_trading_date,
    resolve_slip_pct,
)


_LAST_CARRYOVER_REVALIDATE_SUMMARY: Dict[str, Any] = {}


ENTRY_SIGNAL_SNAPSHOT_SCHEMA: List[str] = [
    "generated_at",
    "d_ref",
    "code",
    "signal_date",
    "signal",
    "reason",
    "rank_score",
    "rank_col",
    "is_surge",
    "is_replay",
    "is_carryover",
    "entry_day",
    "entry_price",
    "qty",
    "order_id",
    "news_implication_reduce_size_rows",
    "news_implication_reduce_size_multiplier",
    "news_implication_reduce_size_observe_only",
    "news_implication_watch_rows",
    "news_implication_watch_observe_only",
    "news_topic_candidate_effect",
    "news_topic_execution_effect",
    "news_topic_reduce_size_multiplier",
    "news_topic_watch_observe_only",
    "positive_entry_ok",
    "positive_entry_reason",
    "entry_gate_decision",
    "entry_gate_reason",
    "guard_severity_map",
    "risk_orch_scale",
    "scale_zero_causes",
    "validation_reduce_applied",
    "position_size_multiplier",
    "cap_top_n",
    "cap_fallback_reason",
    "qty_before_psm",
    "qty_after_psm",
    "final_qty",
    "final_decision_reason",
    "adaptive_good_stock_route",
    "adaptive_good_stock_action",
    "adaptive_good_stock_original_block",
    "adaptive_good_stock_entry_style",
    "adaptive_good_stock_size_hint",
    "adaptive_good_stock_confirmation_required",
    "adaptive_good_stock_quality_score",
    "adaptive_good_stock_post_split_qty_multiplier",
    "adaptive_good_stock_gapup_override_max_pct",
    "adaptive_good_stock_gapup_gap",
    "adaptive_good_stock_qty_before",
    "adaptive_good_stock_qty_after",
    "adaptive_good_stock_policy_source",
    "is_split_entry_2nd",
    "surge_type_normalized",
]

def _entry_source_kind(
    *,
    is_surge_immediate: bool,
    is_open_order_replay: bool,
    use_intraday_realtime_entry: bool,
    use_same_close_today: bool,
    force_next_open_entry: bool,
) -> str:
    if is_surge_immediate:
        return "SURGE_RUNTIME"
    if is_open_order_replay:
        return "OPEN_ORDER_REPLAY"
    if use_same_close_today and use_intraday_realtime_entry:
        return "INTRADAY_REALTIME"
    if use_same_close_today:
        return "SAME_CLOSE"
    if force_next_open_entry:
        return "NEXT_OPEN"
    return "UNKNOWN_ENTRY_SOURCE"


def _entry_source_payload_with_lineage(row: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(row or {})
    origin = str(payload.get("candidate_origin") or "").strip().upper()
    has_sector_action = "sector_action" in payload
    has_sector_allowed = "sector_entry_allowed" in payload
    has_sector_strength = "sector_strength" in payload
    sector_strength_min = float(_to_float(cfg.get("union_entry_strength_min", 0.65), 0.65))
    sector_action = str(payload.get("sector_action") or "").strip().upper()
    sector_allowed = _entry_truthy(payload.get("sector_entry_allowed")) if has_sector_allowed else False
    sector_strength = float(_to_float(payload.get("sector_strength"), 0.0) or 0.0)
    sector_lineage_complete = bool(has_sector_action and has_sector_allowed and has_sector_strength)
    sector_union_candidate = origin == "SECTOR_PREFILTER_UNION"
    sector_union_ok = bool(
        sector_union_candidate
        and sector_action == "BUY"
        and sector_allowed
        and sector_strength >= sector_strength_min
    )
    payload.setdefault("sector_action", "" if not has_sector_action else payload.get("sector_action"))
    payload.setdefault("sector_entry_allowed", "" if not has_sector_allowed else payload.get("sector_entry_allowed"))
    payload.setdefault("sector_strength", "" if not has_sector_strength else payload.get("sector_strength"))
    payload["_sector_union_strength_min"] = sector_strength_min
    payload["_sector_lineage_fields_present"] = {
        "sector_action": has_sector_action,
        "sector_entry_allowed": has_sector_allowed,
        "sector_strength": has_sector_strength,
    }
    payload["_sector_lineage_complete"] = sector_lineage_complete
    payload["_sector_union_candidate"] = sector_union_candidate
    payload["_sector_union_ok"] = sector_union_ok
    return payload


def _entry_layer_rank_score(row: Any, rank_col: str) -> Optional[float]:
    keys = [str(rank_col or "").strip(), "final_score", "score", "rank_score"]
    for key in keys:
        if not key:
            continue
        try:
            raw = row.get(key, None)
        except Exception:
            raw = None
        val = pd.to_numeric(pd.Series([raw]), errors="coerce").iloc[0]
        if pd.notna(val):
            return float(val)
    return None


def _entry_alpha_layer_decision(row: Any, rank_col: str) -> Tuple[str, str, Optional[float]]:
    score = _entry_layer_rank_score(row, rank_col)
    positive_ok = _truthy(row.get("positive_entry_ok", False)) if hasattr(row, "get") else False
    execution_pool = _truthy(row.get("execution_pool", False)) if hasattr(row, "get") else False
    if positive_ok or execution_pool:
        return "ALPHA_ELIGIBLE", "positive_entry_or_execution_pool", score
    if score is not None and score > 0:
        return "ALPHA_WATCH", "positive_rank_score_only", score
    return "ALPHA_WEAK", "no_positive_runtime_signal", score


def _entry_risk_layer_decision(risk_gate_runtime: Dict[str, Any]) -> Tuple[str, str]:
    if not isinstance(risk_gate_runtime, dict):
        return "UNKNOWN", "risk_runtime_missing"
    entry_gate = risk_gate_runtime.get("entry_gate", {})
    risk_orch = risk_gate_runtime.get("risk_orchestration", {})
    prod = risk_gate_runtime.get("production_risk", {})
    entry_decision = str(entry_gate.get("decision", "") if isinstance(entry_gate, dict) else "").strip().upper()
    prod_decision = str(prod.get("decision", "") if isinstance(prod, dict) else "").strip().upper()
    stop_new_orders = _truthy(entry_gate.get("stop_new_orders", False)) if isinstance(entry_gate, dict) else False
    risk_off = _truthy(risk_gate_runtime.get("risk_off", False))
    scale = pd.to_numeric(
        pd.Series([risk_orch.get("scale", None) if isinstance(risk_orch, dict) else None]),
        errors="coerce",
    ).iloc[0]
    if stop_new_orders or risk_off or entry_decision in {"BLOCK", "HARD_BLOCK"} or prod_decision in {"BLOCK", "HARD_BLOCK"}:
        return "BLOCKED", "hard_risk_or_stop_new_orders"
    if (
        entry_decision in {"REDUCE", "CAUTION", "SOFT_BLOCK"}
        or prod_decision in {"REDUCE", "CAUTION", "SOFT_BLOCK"}
        or (pd.notna(scale) and float(scale) < 1.0)
    ):
        return "REDUCED", "risk_size_or_caution_layer"
    return "ALLOW", "risk_runtime_allows"


def _entry_execution_layer_decision(row: Dict[str, Any]) -> Tuple[str, str]:
    signal = str(row.get("signal", "") or "").strip().upper()
    reason = str(row.get("reason", "") or "").strip().upper()
    qty = pd.to_numeric(pd.Series([row.get("qty", None)]), errors="coerce").iloc[0]
    if signal in {"BUY", "ENTRY"} or (pd.notna(qty) and float(qty) > 0):
        return "FILLED_OR_ORDERED", reason or "entry_signal"
    if signal in {"HOLD", "BLOCK", "SKIP"} or reason:
        return "BLOCKED_OR_HELD", reason or signal
    return "NOT_EVALUATED", "no_execution_decision"


def _entry_opportunity_decision(
    *,
    alpha_layer: str,
    risk_layer: str,
    execution_layer: str,
    execution_reason: str,
) -> Tuple[str, str]:
    alpha = str(alpha_layer or "").strip().upper()
    risk = str(risk_layer or "").strip().upper()
    execution = str(execution_layer or "").strip().upper()
    reason = str(execution_reason or "").strip().upper()

    if execution == "FILLED_OR_ORDERED":
        return "EXECUTED", "track_position"
    if alpha not in {"ALPHA_ELIGIBLE", "ALPHA_WATCH"}:
        return "NOT_ACTIONABLE", "alpha_not_strong_enough"
    if risk == "BLOCKED":
        return "RISK_WAIT", "wait_until_hard_risk_clears"
    if "CLOSE_CUTOFF" in reason:
        return "RECHECK_NEXT_SESSION", "recheck_when_market_reopens"
    if "MAX_NEW" in reason or "CAPACITY" in reason or "MAX_POSITIONS" in reason:
        return "RECHECK_CAPACITY", "recheck_when_entry_capacity_opens"
    if "SAME_CODE_DAY" in reason or "CODE_ALREADY_OPEN" in reason or "PROCESSED" in reason or "IDEMPOTENT" in reason:
        return "WAIT_EXISTING_EXPOSURE", "wait_for_position_or_same_day_reset"
    if risk == "REDUCED":
        return "RECHECK_RISK_REDUCED", "recheck_with_reduced_size_when_execution_window_opens"
    if execution == "BLOCKED_OR_HELD":
        return "RECHECK_REQUIRED", "recheck_execution_reason"
    return "WATCH", "keep_alpha_candidate_visible"


def final_entry_decision(
    p0: str,
    gate_daily: str,
    engine_status: str,
    fx_status: str,
    risk_off: bool = False,
    daily_return: Any = None,
    rally_day_ret_min: float = 0.025,
    rate_hike_fear_reduce_day_ret_floor: float = -0.015,
    allow_bear_rally_override: bool = True,
    allow_bear_live_recovery_override: bool = True,
    bear_live_recovery_day_ret_min: float = 0.0,
) -> Dict[str, str]:
    def _resolve_final_regime(
        p0_regime: str,
        macro_regime: str,
        gate_daily_status: str,
        risk_off_flag: bool,
        day_ret: Any,
    ) -> str:
        p0_u = str(p0_regime or "").upper()
        macro_u = str(macro_regime or "").upper()
        gate_u = str(gate_daily_status or "").upper()

        if gate_u == "BLOCK":
            return "BLOCK"

        if macro_u == "RALLY":
            return "ALLOW"

        if risk_off_flag:
            return "CAUTION"

        day_ret_v = None
        try:
            day_ret_v = float(day_ret)
        except Exception:
            day_ret_v = None

        if p0_u == "BEAR":
            if bool(allow_bear_rally_override) and (day_ret_v is not None) and (day_ret_v >= float(rally_day_ret_min)):
                return "ALLOW"
            if (
                bool(allow_bear_live_recovery_override)
                and macro_u in {"NORMAL", "RALLY"}
                and gate_u == "PASS"
                and not bool(risk_off_flag)
                and (day_ret_v is not None)
                and (day_ret_v >= float(bear_live_recovery_day_ret_min))
            ):
                return "ALLOW"
            return "CAUTION"

        if p0_u == "BULL":
            if macro_u in {"RATE_HIKE_FEAR", "STAGFLATION"}:
                return "CAUTION"
            return "ALLOW"

        if p0_u == "SIDEWAYS":
            if macro_u in {"RATE_HIKE_FEAR", "STAGFLATION"}:
                return "CAUTION"
            return "ALLOW"

        return "CAUTION"

    fx_hard_block = (str(fx_status or "").upper() == "EXTREME_HARD")
    fx_soft_block = (str(fx_status or "").upper() == "EXTREME_SOFT")

    if fx_hard_block:
        return {"decision": "BLOCK", "reason": "FX EXTREME_HARD"}

    final_regime = _resolve_final_regime(
        p0_regime=p0,
        macro_regime=engine_status,
        gate_daily_status=gate_daily,
        risk_off_flag=bool(risk_off),
        day_ret=daily_return,
    )

    if final_regime == "BLOCK":
        return {"decision": "BLOCK", "reason": "gate BLOCK"}

    if fx_soft_block:
        return {"decision": "REDUCE", "reason": "FX EXTREME_SOFT - 포지션 제한"}

    if final_regime == "CAUTION":
        dr = None
        try:
            dr = float(daily_return)
        except Exception:
            dr = None
        if (
            str(engine_status or "").upper() == "RATE_HIKE_FEAR"
            and str(gate_daily or "").upper() == "PASS"
            and (not bool(risk_off))
            and (dr is not None)
            and (dr >= float(rate_hike_fear_reduce_day_ret_floor))
        ):
            return {"decision": "REDUCE", "reason": f"RATE_HIKE_FEAR soft reduce (day_ret={dr:.4f})"}
        return {"decision": "CAUTION", "reason": f"{str(engine_status or '').upper()} macro caution"}

    return {"decision": "ALLOW", "reason": "전 층 통과"}

def _write_entry_signal_snapshot(rows: List[Dict[str, Any]], d_ref_ymd: str, rank_col: str) -> None:
    payload = pd.DataFrame(rows)
    if payload.empty:
        payload = pd.DataFrame(columns=ENTRY_SIGNAL_SNAPSHOT_SCHEMA)
    for col in ENTRY_SIGNAL_SNAPSHOT_SCHEMA:
        if col not in payload.columns:
            payload[col] = ""
    payload["generated_at"] = now_ts()
    payload["d_ref"] = str(d_ref_ymd or "")
    payload["rank_col"] = str(rank_col or "")
    payload = payload[ENTRY_SIGNAL_SNAPSHOT_SCHEMA].copy()
    try:
        payload["_rank_n"] = pd.to_numeric(payload["rank_score"], errors="coerce")
        payload = payload.sort_values(
            ["signal_date", "_rank_n", "code"],
            ascending=[False, False, True],
            kind="mergesort",
        ).drop(columns=["_rank_n"], errors="ignore")
    except Exception:
        pass
    payload.to_csv(ENTRY_SIGNAL_SNAPSHOT_PATH, index=False, encoding="utf-8-sig")

def _candidate_snapshot_id(row: Dict[str, Any], code: str, signal_date: str, entry_day: str, order_id: str, qty: int) -> str:
    return _stable_digest(
        "ENTRY_SOURCE",
        code,
        signal_date,
        entry_day,
        order_id,
        qty,
        row.get("signal_id", ""),
        row.get("captured_at", ""),
        row.get("final_score", row.get("score", "")),
        row.get("surge_type", ""),
    )

def _normal_lob_observe_row(
    src: Dict[str, Any],
    exec_row: Dict[str, Any],
    *,
    d_ref_ymd: str,
    rank_col: str,
) -> Dict[str, Any]:
    code = norm_code(src.get("code", exec_row.get("code", "")))
    close_px = _to_float(src.get("close"), None)
    entry_px = _to_float(exec_row.get("entry_price"), None)
    fallback_px = entry_px if entry_px and entry_px > 0 else close_px
    qty = _to_float(exec_row.get("qty"), None)
    lob = _latest_lob_row_for_code(code) if code else {}
    ask_book = _ask_book_from_row(lob) if lob else []
    sweep_qty = 0.0
    sweep_notional = 0.0
    observe_qty = float(qty) if qty and qty > 0 else 1.0
    remaining = observe_qty
    for level in ask_book:
        if remaining <= 0:
            break
        level_qty = min(float(level.get("vol", 0.0) or 0.0), remaining)
        if level_qty <= 0:
            continue
        sweep_qty += level_qty
        sweep_notional += level_qty * float(level.get("price", 0.0) or 0.0)
        remaining -= level_qty
    observed_exec_price = _to_float(exec_row.get("normal_executable_price"), None)
    sweep_price = (sweep_notional / sweep_qty) if sweep_qty > 0 and sweep_notional > 0 else None
    applied_fill_price = _to_float(exec_row.get("normal_lob_applied_fill_price"), None)
    simulated_fill_price = (
        applied_fill_price
        if applied_fill_price and applied_fill_price > 0
        else (
            observed_exec_price
            if observed_exec_price and observed_exec_price > 0
            else (sweep_price if sweep_price and sweep_price > 0 else fallback_px)
        )
    )
    lob_fill_method = "LOB_SWEEP" if simulated_fill_price and (observed_exec_price or sweep_price) else "CLOSE_FALLBACK"
    if not ask_book:
        lob_fill_method = "CLOSE_FALLBACK"
    return {
        "generated_at": now_ts(),
        "d_ref": str(d_ref_ymd or ""),
        "code": code,
        "name": str(src.get("name", exec_row.get("name", "")) or ""),
        "rank_col": str(rank_col or ""),
        "rank_score": src.get(rank_col, src.get("final_score", src.get("score", exec_row.get("rank_score", "")))),
        "signal": str(exec_row.get("signal", "") or ""),
        "execution_reason": str(exec_row.get("reason", "") or ""),
        "close": close_px if close_px is not None else "",
        "entry_price_close_basis": fallback_px if fallback_px is not None else "",
        "simulated_fill_price": simulated_fill_price if simulated_fill_price is not None else "",
        "lob_fill_method": lob_fill_method,
        "normal_lob_applied_fill_price": applied_fill_price if applied_fill_price is not None else "",
        "normal_close_basis_entry_price": exec_row.get("normal_close_basis_entry_price", ""),
        "normal_lob_fill_price_source": exec_row.get("normal_lob_fill_price_source", ""),
        "qty": qty if qty is not None else "",
        "ask1": _to_float(lob.get("ask1"), None) if lob else "",
        "bid1": _to_float(lob.get("bid1"), None) if lob else "",
        "spread_bps": _to_float(lob.get("spread_bps"), None) if lob else "",
        "normal_executable_qty": exec_row.get("normal_executable_qty", sweep_qty if sweep_qty > 0 else ""),
        "normal_executable_price": observed_exec_price if observed_exec_price is not None else (sweep_price if sweep_price is not None else ""),
        "markout_1step_bps": exec_row.get("normal_markout_1step_bps", _to_float(lob.get("markout_1step_bps"), None) if lob else ""),
        "orderflow_tag": exec_row.get("normal_orderflow_tag", str(lob.get("orderflow_tag", "") or "") if lob else ""),
        "lob_status": exec_row.get("normal_lob_status", str(lob.get("lob_status", "") or "") if lob else "MISSING"),
        "lob_available": str(lob.get("lob_available", "") or "") if lob else "False",
        "lob_source_ts": exec_row.get("normal_lob_source_ts", str(lob.get("ts", "") or "") if lob else ""),
        "exec_quality_decision": exec_row.get("normal_lob_check", ""),
        "exec_quality_reason": exec_row.get("normal_lob_reason", ""),
        "observe_only": not _truthy(exec_row.get("normal_lob_fill_price_changed")),
        "fills_price_changed": bool(_truthy(exec_row.get("normal_lob_fill_price_changed"))),
    }

def _write_normal_entry_fill_quality_report(
    candidate_df: pd.DataFrame,
    entry_decision_rows: List[Dict[str, Any]],
    d_ref_ymd: str,
    rank_col: str,
) -> None:
    if not isinstance(candidate_df, pd.DataFrame):
        candidate_df = pd.DataFrame()
    if not isinstance(entry_decision_rows, list):
        entry_decision_rows = []
    execution_by_code: Dict[str, Dict[str, Any]] = {}
    for item in entry_decision_rows:
        if not isinstance(item, dict):
            continue
        code = norm_code(item.get("code", ""))
        if code and not bool(item.get("is_surge", False)) and not bool(item.get("is_replay", False)):
            execution_by_code[code] = item

    rows: List[Dict[str, Any]] = []
    if not candidate_df.empty:
        for src in candidate_df.to_dict("records"):
            code = norm_code(src.get("code", ""))
            if not code:
                continue
            rows.append(_normal_lob_observe_row(src, execution_by_code.get(code, {}), d_ref_ymd=d_ref_ymd, rank_col=rank_col))
    else:
        for exec_row in entry_decision_rows:
            if not isinstance(exec_row, dict):
                continue
            if bool(exec_row.get("is_surge", False)) or bool(exec_row.get("is_replay", False)):
                continue
            rows.append(_normal_lob_observe_row(exec_row, exec_row, d_ref_ymd=d_ref_ymd, rank_col=rank_col))

    columns = [
        "generated_at", "d_ref", "code", "name", "rank_col", "rank_score",
        "signal", "execution_reason", "close", "entry_price_close_basis",
        "simulated_fill_price", "lob_fill_method", "normal_lob_applied_fill_price",
        "normal_close_basis_entry_price", "normal_lob_fill_price_source", "qty", "ask1", "bid1",
        "spread_bps", "normal_executable_qty", "normal_executable_price",
        "markout_1step_bps", "orderflow_tag", "lob_status", "lob_available",
        "lob_source_ts", "exec_quality_decision", "exec_quality_reason",
        "observe_only", "fills_price_changed",
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
        payload = payload.sort_values(["_rank_n", "code"], ascending=[False, True], kind="mergesort").drop(columns=["_rank_n"], errors="ignore")
    NORMAL_ENTRY_FILL_QUALITY_REPORT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload.to_csv(NORMAL_ENTRY_FILL_QUALITY_REPORT_CSV_PATH, index=False, encoding="utf-8-sig")
    changed_count = int(payload["fills_price_changed"].map(_truthy).sum()) if len(payload) else 0
    summary = {
        "generated_at": now_ts(),
        "status": "OK",
        "observe_only": bool(changed_count == 0),
        "fills_price_changed": bool(changed_count > 0),
        "fills_price_changed_rows": int(changed_count),
        "rows": int(len(payload)),
        "lob_fill_method_counts": payload["lob_fill_method"].astype(str).value_counts().to_dict() if len(payload) else {},
        "lob_status_counts": payload["lob_status"].astype(str).value_counts().to_dict() if len(payload) else {},
        "exec_quality_decision_counts": payload["exec_quality_decision"].astype(str).value_counts().to_dict() if len(payload) else {},
        "out_csv": str(NORMAL_ENTRY_FILL_QUALITY_REPORT_CSV_PATH),
    }
    NORMAL_ENTRY_FILL_QUALITY_REPORT_JSON_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

def _build_entry_risk_basis(risk_gate_runtime: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(risk_gate_runtime, dict):
        return {
            "basis_status": "MISSING",
            "basis_summary": "위험 런타임 정보가 없어 진입 위험 판단근거를 만들 수 없음",
            "basis_components": [],
        }

    entry_gate = risk_gate_runtime.get("entry_gate", {}) if isinstance(risk_gate_runtime.get("entry_gate"), dict) else {}
    risk_orch = risk_gate_runtime.get("risk_orchestration", {}) if isinstance(risk_gate_runtime.get("risk_orchestration"), dict) else {}
    kill_switch = risk_gate_runtime.get("kill_switch", {}) if isinstance(risk_gate_runtime.get("kill_switch"), dict) else {}
    crash_risk = risk_gate_runtime.get("crash_risk_off", {}) if isinstance(risk_gate_runtime.get("crash_risk_off"), dict) else {}
    prod = risk_gate_runtime.get("production_risk_playbook", {}) if isinstance(risk_gate_runtime.get("production_risk_playbook"), dict) else {}

    entry_decision = str(entry_gate.get("decision", "") or "").strip().upper()
    entry_reason = str(entry_gate.get("reason", "") or "").strip()
    stop_new_orders = bool(entry_gate.get("stop_new_orders", False))
    block_lock = bool(entry_gate.get("block_lock", False))
    fail_closed = bool(risk_gate_runtime.get("fail_closed", False))
    position_size_multiplier = _to_float(risk_gate_runtime.get("position_size_multiplier", 1.0), 1.0)
    scale = _to_float(risk_orch.get("scale", position_size_multiplier), position_size_multiplier)
    scale_zero_causes = [str(x) for x in (risk_orch.get("scale_zero_causes") or [])]
    advisory_flags = [str(x) for x in (risk_orch.get("risk_advisory_flags") or [])]
    dd_stop_triggered = bool(risk_orch.get("dd_stop_triggered", False))
    dd_advisory_only = bool(risk_orch.get("dd_advisory_only", False))
    es_triggered = bool(risk_orch.get("es_triggered", False))
    es_hard_block = bool(risk_orch.get("es_hard_block", False))
    account_risk_clear = bool(risk_orch.get("account_risk_clear", True))
    risk_off = bool(risk_gate_runtime.get("risk_off_enabled", False))
    risk_off_reasons = [str(x) for x in (risk_gate_runtime.get("risk_off_reasons") or [])]
    prod_decision = str(prod.get("decision", "") or "").strip().upper()
    prod_reason = str(prod.get("reason", "") or "").strip()

    components = [
        {
            "layer": "entry_gate",
            "decision": entry_decision,
            "reason": entry_reason,
            "basis_text": f"진입 게이트={entry_decision or 'UNKNOWN'}; 사유={entry_reason or '없음'}",
        },
        {
            "layer": "risk_orchestration",
            "scale": float(scale),
            "position_size_multiplier": float(position_size_multiplier),
            "scale_zero_causes": scale_zero_causes,
            "advisory_flags": advisory_flags,
            "basis_text": (
                f"위험 오케스트레이션 scale={scale:.3f}; "
                f"ES hard block={es_hard_block}; DD stop={dd_stop_triggered}; "
                f"DD advisory only={dd_advisory_only}; account risk clear={account_risk_clear}"
            ),
        },
        {
            "layer": "kill_switch",
            "triggered": bool(kill_switch.get("triggered", False)),
            "mode": str(kill_switch.get("mode", "") or ""),
            "basis_text": (
                f"킬스위치 발동={bool(kill_switch.get('triggered', False))}; "
                f"모드={str(kill_switch.get('mode', '') or '')}"
            ),
        },
        {
            "layer": "crash_risk_off",
            "triggered": bool(crash_risk.get("triggered", False)),
            "mode": str(crash_risk.get("mode", "") or ""),
            "basis_text": (
                f"급락 리스크 발동={bool(crash_risk.get('triggered', False))}; "
                f"모드={str(crash_risk.get('mode', '') or '')}"
            ),
        },
        {
            "layer": "production_risk_playbook",
            "decision": prod_decision,
            "reason": prod_reason,
            "basis_text": f"운영 리스크={prod_decision or 'UNKNOWN'}; 사유={prod_reason or '없음'}",
        },
    ]

    blockers = []
    reducers = []
    if stop_new_orders:
        blockers.append("stop_new_orders")
    if block_lock:
        blockers.append("entry_gate_block_lock")
    if fail_closed:
        blockers.append("fail_closed")
    if risk_off:
        blockers.extend(risk_off_reasons or ["risk_off_enabled"])
    if prod_decision in {"BLOCK", "HARD_BLOCK"}:
        blockers.append(f"production_risk={prod_decision}")
    if es_hard_block:
        blockers.append("es_hard_block")
    blockers.extend(scale_zero_causes)
    if entry_decision in {"REDUCE", "CAUTION", "SOFT_BLOCK"}:
        reducers.append(f"entry_gate={entry_decision}")
    if prod_decision in {"REDUCE", "CAUTION", "SOFT_BLOCK"}:
        reducers.append(f"production_risk={prod_decision}")
    if position_size_multiplier < 1.0 or scale < 1.0:
        reducers.append(f"size_multiplier={position_size_multiplier:.3f}")
    if es_triggered and not es_hard_block:
        reducers.append("es_reduction")
    reducers.extend(advisory_flags)

    if blockers:
        basis_status = "BLOCK"
        basis_summary = "신규 진입 차단 근거: " + "; ".join(dict.fromkeys(blockers))
    elif reducers:
        basis_status = "REDUCE"
        basis_summary = "신규 진입 축소/주의 근거: " + "; ".join(dict.fromkeys(reducers))
    else:
        basis_status = "ALLOW"
        basis_summary = "신규 진입 위험 차단 근거 없음"

    return {
        "basis_status": basis_status,
        "basis_summary": basis_summary,
        "basis_components": components,
        "block_reasons": list(dict.fromkeys(blockers)),
        "reduce_reasons": list(dict.fromkeys(reducers)),
    }

def _is_split2_wait_reason(row: Any) -> bool:
    try:
        if not _truthy(row.get("split_entry_2nd", False)):
            return False
        text = "|".join(
            str(row.get(k, "") or "")
            for k in ("split_confirmation_reason", "reason", "carry_origin_reason", "carry_reason")
        ).upper()
        return "SPLIT2ND_DIP_TOO_SHALLOW" in text
    except Exception:
        return False

def _write_entry_decision_layers_snapshot(
    candidate_df: pd.DataFrame,
    entry_decision_rows: List[Dict[str, Any]],
    d_ref_ymd: str,
    rank_col: str,
    risk_gate_runtime: Dict[str, Any],
) -> None:
    if not isinstance(candidate_df, pd.DataFrame):
        candidate_df = pd.DataFrame()
    if not isinstance(entry_decision_rows, list):
        entry_decision_rows = []

    risk_layer, risk_reason = _entry_risk_layer_decision(risk_gate_runtime)
    execution_by_code: Dict[str, Dict[str, Any]] = {}
    for item in entry_decision_rows:
        if not isinstance(item, dict):
            continue
        code = norm_code(item.get("code", ""))
        if code:
            execution_by_code[code] = item

    pending_by_code: Dict[str, Dict[str, Any]] = {}
    try:
        if PENDING_SIGNALS_PATH.exists():
            pending_df = pd.read_csv(PENDING_SIGNALS_PATH, dtype={"code": str})
            if isinstance(pending_df, pd.DataFrame) and not pending_df.empty:
                for pending_row in pending_df.to_dict("records"):
                    p_code = norm_code(pending_row.get("code", ""))
                    if p_code:
                        pending_by_code[p_code] = pending_row
    except Exception:
        pending_by_code = {}

    rows: List[Dict[str, Any]] = []
    source_rows = candidate_df.to_dict("records") if not candidate_df.empty else []
    seen_codes: set[str] = set()

    def _first_nonblank(*values: Any) -> Any:
        for value in values:
            text = str(value or "").strip()
            if text and text.lower() not in {"nan", "none", "null"}:
                return value
        return ""

    normal_detail_columns = [
        "normal_exec_quality_enabled",
        "normal_lob_check",
        "normal_lob_status",
        "normal_lob_reason",
        "normal_lob_source_ts",
        "normal_spread_bps",
        "normal_markout_1step_bps",
        "normal_executable_qty",
        "normal_executable_price",
        "normal_lob_ask_depth_levels",
        "normal_orderflow_tag",
        "normal_close_auction_action",
        "normal_close_auction_reason",
        "normal_close_auction_close_pos",
        "normal_close_auction_day_range_pct",
        "normal_close_auction_v_accel",
        "normal_close_auction_reduce_multiplier",
        "normal_close_auction_qty_before",
        "normal_close_auction_qty_after",
        "normal_intraday_momentum_action",
        "normal_intraday_momentum_reason",
        "normal_intraday_momentum_status",
        "normal_intraday_momentum_ts",
        "normal_intraday_value_ratio",
        "normal_intraday_rechecked_v_accel",
        "normal_intraday_reduce_multiplier",
        "normal_intraday_momentum_qty_before",
        "normal_intraday_momentum_qty_after",
        "normal_dynamic_slippage_pct",
        "normal_dynamic_slippage_reason",
        "normal_dynamic_slippage_multiplier",
        "normal_dynamic_slippage_tier",
        "normal_dynamic_slippage_trading_value",
        "normal_close_basis_entry_price",
        "normal_lob_applied_fill_price",
        "normal_lob_fill_price_source",
        "normal_lob_fill_price_changed",
    ]

    def _normal_detail_payload(exec_row: Dict[str, Any], src_row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            key: _first_nonblank(exec_row.get(key, ""), src_row.get(key, ""))
            for key in normal_detail_columns
        }

    for src in source_rows:
        code = norm_code(src.get("code", ""))
        if not code:
            continue
        pending_src = pending_by_code.get(code, {})
        src_for_wait = dict(src)
        if isinstance(pending_src, dict):
            for key in ("reason", "split_entry_2nd", "split_confirmation_reason", "carry_origin_reason", "carry_reason"):
                if not str(src_for_wait.get(key, "") or "").strip() and str(pending_src.get(key, "") or "").strip():
                    src_for_wait[key] = pending_src.get(key)
        seen_codes.add(code)
        alpha_layer, alpha_reason, score = _entry_alpha_layer_decision(src, rank_col)
        exec_row = execution_by_code.get(code, {})
        exec_layer, exec_reason = _entry_execution_layer_decision(exec_row)
        opportunity_state, next_action = _entry_opportunity_decision(
            alpha_layer=alpha_layer,
            risk_layer=risk_layer,
            execution_layer=exec_layer,
            execution_reason=exec_reason,
        )
        if not exec_row and _is_split2_wait_reason(src_for_wait):
            opportunity_state = "SPLIT2ND_WAIT_DIP"
            next_action = "wait_for_split_second_dip_band"
            exec_reason = str(src_for_wait.get("split_confirmation_reason") or src_for_wait.get("reason") or "SPLIT2ND_DIP_TOO_SHALLOW")
        rows.append(
            {
                "generated_at": now_ts(),
                "d_ref": str(d_ref_ymd or ""),
                "code": code,
                "name": str(src.get("name", "") or ""),
                "rank_col": str(rank_col or ""),
                "rank_score": "" if score is None else score,
                "alpha_layer": alpha_layer,
                "alpha_reason": alpha_reason,
                "risk_layer": risk_layer,
                "risk_reason": risk_reason,
                "execution_layer": exec_layer,
                "execution_reason": exec_reason,
                "opportunity_state": opportunity_state,
                "next_action": next_action,
                "signal": str(exec_row.get("signal", "") or ""),
                "entry_day": str(exec_row.get("entry_day", "") or ""),
                "qty": exec_row.get("qty", ""),
                "order_id": str(exec_row.get("order_id", "") or ""),
                "positive_entry_ok": src.get("positive_entry_ok", ""),
                "positive_entry_reason": src.get("positive_entry_reason", ""),
                "execution_pool": src.get("execution_pool", ""),
                "entry_gate_decision": exec_row.get("entry_gate_decision", ""),
                "entry_gate_reason": exec_row.get("entry_gate_reason", ""),
                "pending_reason": str(src_for_wait.get("reason", "") or ""),
                "split_entry_2nd": src_for_wait.get("split_entry_2nd", ""),
                "split_confirmation_reason": str(src_for_wait.get("split_confirmation_reason", "") or ""),
                "surge_shadow_status": exec_row.get("surge_shadow_status", ""),
                "surge_shadow_condition": exec_row.get("surge_shadow_condition", ""),
                "surge_shadow_mode": exec_row.get("surge_shadow_mode", ""),
                "surge_shadow_action": exec_row.get("surge_shadow_action", ""),
                "surge_shadow_reason": exec_row.get("surge_shadow_reason", ""),
                "active_response_label": src.get("active_response_label", exec_row.get("active_response_label", "")),
                "active_response_reason": src.get("active_response_reason", exec_row.get("active_response_reason", "")),
                "active_response_next_check": src.get("active_response_next_check", exec_row.get("active_response_next_check", "")),
                "surge_after_state": src.get("surge_after_state", exec_row.get("surge_after_state", "")),
                "surge_gap_up_override_gap": _first_nonblank(src.get("_surge_gap_up_override_gap"), exec_row.get("surge_gap_up_override_gap")),
                "surge_gap_up_override_blockers": _first_nonblank(src.get("_surge_gap_up_override_blockers"), exec_row.get("surge_gap_up_override_blockers")),
                "surge_gap_up_override_active_label": _first_nonblank(src.get("_surge_gap_up_override_active_label"), exec_row.get("surge_gap_up_override_active_label")),
                "surge_gap_up_override_paper_readiness": _first_nonblank(src.get("_surge_gap_up_override_paper_readiness"), exec_row.get("surge_gap_up_override_paper_readiness")),
                "surge_gap_up_override_paper_route_ok": _first_nonblank(src.get("_surge_gap_up_override_paper_route_ok"), exec_row.get("surge_gap_up_override_paper_route_ok")),
                "surge_gap_up_override_lob_status": _first_nonblank(src.get("_surge_gap_up_override_lob_status"), exec_row.get("surge_gap_up_override_lob_status")),
                "surge_gap_up_override_orderflow_tag": _first_nonblank(src.get("_surge_gap_up_override_orderflow_tag"), exec_row.get("surge_gap_up_override_orderflow_tag")),
                "surge_gap_up_override_spread_bps": _first_nonblank(src.get("_surge_gap_up_override_spread_bps"), exec_row.get("surge_gap_up_override_spread_bps")),
                "surge_gap_up_override_markout_bps": _first_nonblank(src.get("_surge_gap_up_override_markout_bps"), exec_row.get("surge_gap_up_override_markout_bps")),
                "surge_gap_up_override_score_final": _first_nonblank(src.get("_surge_gap_up_override_score_final"), exec_row.get("surge_gap_up_override_score_final")),
                "surge_gap_up_block_gap": _first_nonblank(src.get("_surge_gap_up_block_gap"), exec_row.get("surge_gap_up_block_gap")),
                "surge_gap_up_block_limit": _first_nonblank(src.get("_surge_gap_up_block_limit"), exec_row.get("surge_gap_up_block_limit")),
                "surge_gap_up_block_entry_price": _first_nonblank(src.get("_surge_gap_up_block_entry_price"), exec_row.get("surge_gap_up_block_entry_price")),
                "surge_gap_up_block_ref_close": _first_nonblank(src.get("_surge_gap_up_block_ref_close"), exec_row.get("surge_gap_up_block_ref_close")),
                "surge_gap_up_block_ref_date": _first_nonblank(src.get("_surge_gap_up_block_ref_date"), exec_row.get("surge_gap_up_block_ref_date")),
                **_normal_detail_payload(exec_row, src),
            }
        )

    for exec_row in entry_decision_rows:
        if not isinstance(exec_row, dict):
            continue
        code = norm_code(exec_row.get("code", ""))
        if not code or code in seen_codes:
            continue
        alpha_layer, alpha_reason, score = _entry_alpha_layer_decision(exec_row, rank_col)
        exec_layer, exec_reason = _entry_execution_layer_decision(exec_row)
        opportunity_state, next_action = _entry_opportunity_decision(
            alpha_layer=alpha_layer,
            risk_layer=risk_layer,
            execution_layer=exec_layer,
            execution_reason=exec_reason,
        )
        rows.append(
            {
                "generated_at": now_ts(),
                "d_ref": str(d_ref_ymd or ""),
                "code": code,
                "name": "",
                "rank_col": str(rank_col or ""),
                "rank_score": "" if score is None else score,
                "alpha_layer": alpha_layer,
                "alpha_reason": alpha_reason,
                "risk_layer": risk_layer,
                "risk_reason": risk_reason,
                "execution_layer": exec_layer,
                "execution_reason": exec_reason,
                "opportunity_state": opportunity_state,
                "next_action": next_action,
                "signal": str(exec_row.get("signal", "") or ""),
                "entry_day": str(exec_row.get("entry_day", "") or ""),
                "qty": exec_row.get("qty", ""),
                "order_id": str(exec_row.get("order_id", "") or ""),
                "positive_entry_ok": exec_row.get("positive_entry_ok", ""),
                "positive_entry_reason": exec_row.get("positive_entry_reason", ""),
                "execution_pool": "",
                "entry_gate_decision": exec_row.get("entry_gate_decision", ""),
                "entry_gate_reason": exec_row.get("entry_gate_reason", ""),
                "pending_reason": str(exec_row.get("reason", "") or ""),
                "split_entry_2nd": exec_row.get("split_entry_2nd", ""),
                "split_confirmation_reason": str(exec_row.get("split_confirmation_reason", "") or ""),
                "surge_shadow_status": exec_row.get("surge_shadow_status", ""),
                "surge_shadow_condition": exec_row.get("surge_shadow_condition", ""),
                "surge_shadow_mode": exec_row.get("surge_shadow_mode", ""),
                "surge_shadow_action": exec_row.get("surge_shadow_action", ""),
                "surge_shadow_reason": exec_row.get("surge_shadow_reason", ""),
                "active_response_label": exec_row.get("active_response_label", ""),
                "active_response_reason": exec_row.get("active_response_reason", ""),
                "active_response_next_check": exec_row.get("active_response_next_check", ""),
                "surge_after_state": exec_row.get("surge_after_state", ""),
                "surge_gap_up_override_gap": exec_row.get("surge_gap_up_override_gap", ""),
                "surge_gap_up_override_blockers": exec_row.get("surge_gap_up_override_blockers", ""),
                "surge_gap_up_override_active_label": exec_row.get("surge_gap_up_override_active_label", ""),
                "surge_gap_up_override_paper_readiness": exec_row.get("surge_gap_up_override_paper_readiness", ""),
                "surge_gap_up_override_paper_route_ok": exec_row.get("surge_gap_up_override_paper_route_ok", ""),
                "surge_gap_up_override_lob_status": exec_row.get("surge_gap_up_override_lob_status", ""),
                "surge_gap_up_override_orderflow_tag": exec_row.get("surge_gap_up_override_orderflow_tag", ""),
                "surge_gap_up_override_spread_bps": exec_row.get("surge_gap_up_override_spread_bps", ""),
                "surge_gap_up_override_markout_bps": exec_row.get("surge_gap_up_override_markout_bps", ""),
                "surge_gap_up_override_score_final": exec_row.get("surge_gap_up_override_score_final", ""),
                "surge_gap_up_block_gap": exec_row.get("surge_gap_up_block_gap", ""),
                "surge_gap_up_block_limit": exec_row.get("surge_gap_up_block_limit", ""),
                "surge_gap_up_block_entry_price": exec_row.get("surge_gap_up_block_entry_price", ""),
                "surge_gap_up_block_ref_close": exec_row.get("surge_gap_up_block_ref_close", ""),
                "surge_gap_up_block_ref_date": exec_row.get("surge_gap_up_block_ref_date", ""),
                **_normal_detail_payload(exec_row, {}),
            }
        )

    payload = pd.DataFrame(rows)
    columns = [
        "generated_at",
        "d_ref",
        "code",
        "name",
        "rank_col",
        "rank_score",
        "alpha_layer",
        "alpha_reason",
        "risk_layer",
        "risk_reason",
        "execution_layer",
        "execution_reason",
        "opportunity_state",
        "next_action",
        "signal",
        "entry_day",
        "qty",
        "order_id",
        "positive_entry_ok",
        "positive_entry_reason",
        "execution_pool",
        "entry_gate_decision",
        "entry_gate_reason",
        "pending_reason",
        "split_entry_2nd",
        "split_confirmation_reason",
        "surge_shadow_status",
        "surge_shadow_condition",
        "surge_shadow_mode",
        "surge_shadow_action",
        "surge_shadow_reason",
        "active_response_label",
        "active_response_reason",
        "active_response_next_check",
        "surge_after_state",
        "surge_gap_up_override_gap",
        "surge_gap_up_override_blockers",
        "surge_gap_up_override_active_label",
        "surge_gap_up_override_paper_readiness",
        "surge_gap_up_override_paper_route_ok",
        "surge_gap_up_override_lob_status",
        "surge_gap_up_override_orderflow_tag",
        "surge_gap_up_override_spread_bps",
        "surge_gap_up_override_markout_bps",
        "surge_gap_up_override_score_final",
        "surge_gap_up_block_gap",
        "surge_gap_up_block_limit",
        "surge_gap_up_block_entry_price",
        "surge_gap_up_block_ref_close",
        "surge_gap_up_block_ref_date",
        *normal_detail_columns,
    ]
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

    ENTRY_DECISION_LAYERS_RUNTIME_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload.to_csv(ENTRY_DECISION_LAYERS_RUNTIME_CSV_PATH, index=False, encoding="utf-8-sig")

    def _execution_review_bucket(row: Any) -> str:
        if not isinstance(row, pd.Series):
            return "UNKNOWN"
        execution = str(row.get("execution_layer", "") or "").strip().upper()
        opportunity = str(row.get("opportunity_state", "") or "").strip().upper()
        reason = str(row.get("execution_reason", "") or "").strip().upper()
        if execution == "FILLED_OR_ORDERED":
            return "EXECUTED"
        if opportunity == "SPLIT2ND_WAIT_DIP":
            return "WAITING_CONDITION"
        if opportunity in {"RECHECK_NEXT_SESSION", "RECHECK_CAPACITY", "RECHECK_RISK_REDUCED", "RECHECK_REQUIRED", "WATCH"}:
            return "HELD_FOR_RECHECK"
        if reason:
            return "HARD_BLOCKED"
        return "NOT_EVALUATED"

    if payload.empty:
        execution_review_counts: Dict[str, int] = {}
    else:
        execution_review_counts = payload.apply(_execution_review_bucket, axis=1).astype(str).value_counts(dropna=False).to_dict()

    selection_policy_meta = {}
    if isinstance(candidate_df, pd.DataFrame) and isinstance(getattr(candidate_df, "attrs", None), dict):
        selection_policy_meta = {
            k: v
            for k, v in candidate_df.attrs.items()
            if str(k).startswith("selection_policy_")
        }

    actionable_states = {"RECHECK_NEXT_SESSION", "RECHECK_CAPACITY", "RECHECK_RISK_REDUCED", "RECHECK_REQUIRED", "WATCH"}

    summary = {
        "generated_at": now_ts(),
        "status": "PASS",
        "d_ref": str(d_ref_ymd or ""),
        "rank_col": str(rank_col or ""),
        "candidate_rows": int(len(candidate_df)),
        "decision_rows": int(len(entry_decision_rows)),
        "snapshot_rows": int(len(payload)),
        "artifact_csv": str(ENTRY_DECISION_LAYERS_RUNTIME_CSV_PATH),
        "alpha_layer_counts": payload["alpha_layer"].astype(str).value_counts(dropna=False).to_dict(),
        "risk_layer_counts": payload["risk_layer"].astype(str).value_counts(dropna=False).to_dict(),
        "execution_layer_counts": payload["execution_layer"].astype(str).value_counts(dropna=False).to_dict(),
        "execution_review_counts": execution_review_counts,
        "selection_policy_meta": selection_policy_meta,
        "opportunity_state_counts": payload["opportunity_state"].astype(str).value_counts(dropna=False).to_dict(),
        "surge_shadow_action_counts": payload["surge_shadow_action"].astype(str).value_counts(dropna=False).to_dict(),
        "surge_shadow_condition_counts": payload["surge_shadow_condition"].astype(str).value_counts(dropna=False).to_dict(),
        "active_response_label_counts": payload["active_response_label"].astype(str).value_counts(dropna=False).to_dict(),
        "surge_after_state_counts": payload["surge_after_state"].astype(str).value_counts(dropna=False).to_dict(),
        "actionable_recheck_rows": int(
            payload["opportunity_state"]
            .astype(str)
            .str.upper()
            .isin(actionable_states)
            .sum()
        ),
        "top_actionable_candidates": payload[
            payload["opportunity_state"]
            .astype(str)
            .str.upper()
            .isin(actionable_states)
        ][["code", "rank_score", "opportunity_state", "next_action", "execution_reason"]]
        .head(10)
        .to_dict("records"),
        "policy_change": False,
        "note": "Opportunity layer snapshot; does not change entry filtering, risk gates, sizing, orders, or fills.",
    }
    ENTRY_DECISION_LAYERS_RUNTIME_JSON_PATH.write_text(
        json.dumps(_state_json_safe(summary), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

def calc_qty(entry_price: float, cfg: Dict[str, Any], fee_pct: float, slip_pct: float) -> int:
    """Return order quantity based on a single sizing policy.

    Supported modes (cfg['sizing_mode']):
      - 'fixed_qty'      : use cfg['fixed_qty']
      - 'fixed_cash'     : use cfg['cash_per_trade'] (KRW)
      - 'capital_slots'  : use cfg['capital_total'] / cfg['max_positions'] (KRW)

    If qty < cfg['min_qty'] or entry_price<=0, returns 0 (skip).
    """
    return calc_qty_by_sizing(
        entry_price=entry_price,
        cfg=cfg if isinstance(cfg, dict) else {},
        min_qty=int(cfg.get("min_qty", 1) or 1) if isinstance(cfg, dict) else 1,
    )


def _apply_p1_calendar_gate(max_new: int, calendar_policy: Dict[str, Any], status: Dict[str, Any]) -> int:
    if not bool(calendar_policy.get("enabled", False)):
        return int(max_new)

    status["states"]["calendar_enabled"] = True
    today_ymd = now_ymd()
    block_dates = {str(x).strip() for x in (calendar_policy.get("block_dates") or []) if str(x).strip()}
    reduce_dates = {str(x).strip() for x in (calendar_policy.get("reduce_dates") or []) if str(x).strip()}
    if today_ymd in block_dates:
        if int(max_new) != 0:
            status["actions"].append(f"calendar:block_date={today_ymd}")
        max_new = 0
    elif today_ymd in reduce_dates:
        prev_max_new = int(max_new)
        max_new = _cap_max_new(max_new, calendar_policy.get("reduce_max_new_cap", 1))
        if int(max_new) != prev_max_new:
            status["actions"].append(f"calendar:reduce_date={today_ymd} {prev_max_new}->{int(max_new)}")

    if bool(calendar_policy.get("option_expiry_reduce_enabled", False)):
        try:
            current_day = datetime.strptime(today_ymd, "%Y%m%d")
            week_of_month = ((int(current_day.day) - 1) // 7) + 1
            target_week = int(calendar_policy.get("option_expiry_week_of_month", 2) or 2)
            target_weekday = int(calendar_policy.get("option_expiry_weekday", 3) or 3)  # Mon=0
            if week_of_month == target_week and int(current_day.weekday()) == target_weekday:
                prev_max_new = int(max_new)
                max_new = _cap_max_new(max_new, calendar_policy.get("option_expiry_max_new_cap", 1))
                if int(max_new) != prev_max_new:
                    status["actions"].append(f"calendar:option_expiry_reduce {prev_max_new}->{int(max_new)}")
        except Exception:
            pass
    return int(max_new)

def _apply_p1_intraday_gate(
    candidate_df: pd.DataFrame,
    max_new: int,
    intraday_policy: Dict[str, Any],
    status: Dict[str, Any],
) -> tuple[pd.DataFrame, int]:
    if not bool(intraday_policy.get("enabled", False)):
        return candidate_df, int(max_new)

    status["states"]["intraday_enabled"] = True
    now_hhmm = int(datetime.now().strftime("%H%M"))
    morning_start = _parse_hhmm(intraday_policy.get("morning_start_hhmm", 900), 900)
    morning_end = _parse_hhmm(intraday_policy.get("morning_end_hhmm", 1000), 1000)
    lunch_start = _parse_hhmm(intraday_policy.get("lunch_start_hhmm", 1130), 1130)
    lunch_end = _parse_hhmm(intraday_policy.get("lunch_end_hhmm", 1330), 1330)
    status["states"]["intraday_now_hhmm"] = now_hhmm
    status["states"]["intraday_morning_window"] = f"{morning_start:04d}-{morning_end:04d}"
    status["states"]["intraday_lunch_window"] = f"{lunch_start:04d}-{lunch_end:04d}"

    if morning_start <= now_hhmm <= morning_end and "sector_strength" in candidate_df.columns:
        before_count = len(candidate_df)
        try:
            min_strength = float(intraday_policy.get("morning_sector_strength_min", 0.80) or 0.80)
        except Exception:
            min_strength = 0.80
        sector_strength = pd.to_numeric(candidate_df.get("sector_strength"), errors="coerce").fillna(0.0)
        below_mask = sector_strength < min_strength
        below_count = int(below_mask.sum())
        if below_count > 0:
            candidate_df = candidate_df.copy()
            candidate_df["morning_strength_soft_below_min"] = below_mask
            candidate_df["morning_strength_min"] = min_strength
            candidate_df["morning_strength_policy"] = "observe_only_no_filter"
            status["actions"].append(
                f"intraday:morning_strength_observe_only kept={before_count} "
                f"below_min={below_count} min={min_strength:.4f}"
            )

    if lunch_start <= now_hhmm <= lunch_end:
        prev_max_new = int(max_new)
        max_new = _cap_max_new(max_new, intraday_policy.get("lunch_max_new_cap", 1))
        if int(max_new) != prev_max_new:
            status["actions"].append(f"intraday:lunch_cap {prev_max_new}->{int(max_new)}")
    return candidate_df, int(max_new)

def _apply_p1_event_gate(
    candidate_df: pd.DataFrame,
    max_new: int,
    event_policy: Dict[str, Any],
    status: Dict[str, Any],
) -> tuple[pd.DataFrame, int]:
    if not bool(event_policy.get("enabled", False)):
        return candidate_df, int(max_new)

    status["states"]["event_enabled"] = True
    if "earnings_event_block" in candidate_df.columns:
        before_count = len(candidate_df)
        earnings_block = candidate_df["earnings_event_block"].astype(str).str.strip().str.upper().isin(["TRUE", "1", "Y", "YES"])
        candidate_df = candidate_df[~earnings_block].copy()
        after_count = len(candidate_df)
        if before_count != after_count:
            status["actions"].append(f"event:earnings_block_filter {before_count}->{after_count}")

    events_file_raw = str(event_policy.get("events_file", "") or "").strip()
    if not events_file_raw:
        return candidate_df, int(max_new)

    events_file = Path(events_file_raw)
    status["states"]["event_file_exists"] = bool(events_file.exists())
    if events_file.exists():
        try:
            event_doc = json.loads(events_file.read_text(encoding="utf-8"))
            explicit_event_guard = _detect_explicit_market_events(event_doc, candidate_df, event_policy, now_ymd())
            market_guard_payload = {
                "generated_at": now_ts(),
                "runtime_ymd": now_ymd(),
                "status": "BLOCK" if bool(explicit_event_guard.get("blocked")) else "PASS",
                "policy_effect": bool((event_policy.get("explicit_market_event_guard") or {}).get("enabled", True)) if isinstance(event_policy.get("explicit_market_event_guard"), dict) else True,
                "trading_effect": bool(explicit_event_guard.get("blocked")),
                "source_path": str(events_file),
                "market_wide": bool(explicit_event_guard.get("market_wide")),
                "blocked_codes": explicit_event_guard.get("blocked_codes", []),
                "events": explicit_event_guard.get("events", []),
            }
            _write_market_event_guard_status(market_guard_payload)
            if bool(explicit_event_guard.get("blocked")):
                blocked_codes = set(str(x).zfill(6) for x in (explicit_event_guard.get("blocked_codes") or []))
                if bool(explicit_event_guard.get("market_wide")):
                    if int(max_new) != 0:
                        status["actions"].append("event:explicit_market_event BLOCK market_wide")
                    max_new = 0
                elif blocked_codes and "code" in candidate_df.columns:
                    before_count = len(candidate_df)
                    candidate_df = candidate_df[~candidate_df["code"].astype(str).str.zfill(6).isin(blocked_codes)].copy()
                    after_count = len(candidate_df)
                    status["actions"].append(f"event:explicit_market_event_filter {before_count}->{after_count}")
            event_ymd = str(event_doc.get("as_of_ymd") or event_doc.get("date") or "").strip()
            event_level = str(event_doc.get("market_event_level") or event_doc.get("risk_level") or "").strip().upper()
            high_levels = {
                str(x).strip().upper()
                for x in (event_policy.get("high_risk_levels") or ["HIGH", "CRITICAL"])
                if str(x).strip()
            }
            if event_ymd == now_ymd() and event_level in high_levels:
                action = str(event_policy.get("high_risk_action", "REDUCE") or "REDUCE").strip().upper()
                if action == "BLOCK":
                    if int(max_new) != 0:
                        status["actions"].append(f"event:market_risk_level={event_level} BLOCK")
                    max_new = 0
                else:
                    prev_max_new = int(max_new)
                    max_new = _cap_max_new(max_new, event_policy.get("high_risk_max_new_cap", 1))
                    if int(max_new) != prev_max_new:
                        status["actions"].append(f"event:market_risk_level={event_level} REDUCE {prev_max_new}->{int(max_new)}")
            elif event_ymd == now_ymd() and event_level == "BOOST":
                if len(candidate_df) > 0:
                    candidate_df["boost_applied"] = True
                    status["actions"].append("event:market_risk_level=BOOST APPLIED")
        except Exception as exc:
            guard = _get_dict(event_policy, "explicit_market_event_guard")
            payload: Dict[str, Any] = {
                "generated_at": now_ts(),
                "runtime_ymd": now_ymd(),
                "status": "BLOCK" if bool(guard.get("fail_closed_on_parse_error", True)) else "WARN",
                "policy_effect": True,
                "trading_effect": bool(guard.get("fail_closed_on_parse_error", True)),
                "source_path": str(events_file),
                "error": f"{type(exc).__name__}: {exc}",
                "market_wide": bool(guard.get("fail_closed_on_parse_error", True)),
                "blocked_codes": [],
                "events": [],
            }
            _write_market_event_guard_status(payload)
            if bool(guard.get("fail_closed_on_parse_error", True)):
                status["actions"].append("event:explicit_market_event_parse_error BLOCK")
                max_new = 0
    else:
        if bool(event_policy.get("auto_stub_when_missing", True)):
            try:
                events_file.parent.mkdir(parents=True, exist_ok=True)
                stub = {
                    "generated_at": now_ts(),
                    "as_of_ymd": now_ymd(),
                    "market_event_level": "NORMAL",
                    "source": "paper_engine_event_gate_autostub",
                }
                events_file.write_text(json.dumps(stub, ensure_ascii=False, indent=2), encoding="utf-8")
                status["states"]["event_file_exists"] = True
                status["actions"].append("event:autostub_created NORMAL")
                _write_market_event_guard_status({
                    "generated_at": now_ts(),
                    "runtime_ymd": now_ymd(),
                    "status": "PASS",
                    "policy_effect": bool((event_policy.get("explicit_market_event_guard") or {}).get("enabled", True)) if isinstance(event_policy.get("explicit_market_event_guard"), dict) else True,
                    "trading_effect": False,
                    "source_path": str(events_file),
                    "market_wide": False,
                    "blocked_codes": [],
                    "events": [],
                    "note": "autostub_created",
                })
            except Exception:
                status["missing_inputs"].append(f"event_file_missing:{events_file}")
        else:
            status["missing_inputs"].append(f"event_file_missing:{events_file}")
    return candidate_df, int(max_new)

def _apply_p1_technical_gate(
    candidate_df: pd.DataFrame,
    max_new: int,
    technical_policy: Dict[str, Any],
    status: Dict[str, Any],
) -> int:
    if not bool(technical_policy.get("enabled", False)):
        return int(max_new)

    status["states"]["technical_enabled"] = True

    if "boost_applied" in candidate_df.columns and bool(candidate_df["boost_applied"].fillna(False).any()):
        status["actions"].append("technical:boost_applied_bypass (market_event_level=BOOST)")
        return int(max_new)

    try:
        min_pool = int(technical_policy.get("min_pool_size", 8) or 8)
    except Exception:
        min_pool = 8
    if len(candidate_df) < max(1, min_pool):
        return int(max_new)

    rsi = pd.to_numeric(candidate_df.get("rsi14"), errors="coerce")
    macd = candidate_df.get("macd_golden")
    volcorr = pd.to_numeric(candidate_df.get("vol_close_corr20"), errors="coerce")
    stoch = pd.to_numeric(candidate_df.get("stoch_k"), errors="coerce")
    boll_mid = pd.to_numeric(candidate_df.get("boll_mid_pos"), errors="coerce")
    obv_slope = pd.to_numeric(candidate_df.get("obv_slope"), errors="coerce")
    sma_ema_ok = candidate_df.get("sma_ema_trend_ok")

    valid = pd.Series([True] * len(candidate_df), index=candidate_df.index)
    if isinstance(rsi, pd.Series):
        valid = valid & rsi.notna()
    if isinstance(macd, pd.Series):
        valid = valid & macd.notna()
    if isinstance(volcorr, pd.Series):
        valid = valid & volcorr.notna()
    if not bool(valid.any()):
        return int(max_new)

    rsi_v = rsi[valid] if isinstance(rsi, pd.Series) else pd.Series(dtype=float)
    macd_v = macd[valid] if isinstance(macd, pd.Series) else pd.Series(dtype=object)
    vol_v = volcorr[valid] if isinstance(volcorr, pd.Series) else pd.Series(dtype=float)
    try:
        rsi_min = float(technical_policy.get("rsi_min", 45.0) or 45.0)
        rsi_max = float(technical_policy.get("rsi_max", 75.0) or 75.0)
        macd_min_ratio = _to_float(technical_policy.get("macd_golden_min_ratio", 0.05), 0.05)
        volcorr_min = float(technical_policy.get("volcorr_min", 0.03) or 0.03)
        rsi_ok_min_ratio = _to_float(technical_policy.get("rsi_ok_min_ratio", 0.50), 0.50)
        volcorr_ok_min_ratio = _to_float(technical_policy.get("volcorr_ok_min_ratio", 0.50), 0.50)
        stoch_min = _to_float(technical_policy.get("stoch_k_min", 20.0), 20.0)
        stoch_max = _to_float(technical_policy.get("stoch_k_max", 85.0), 85.0)
        stoch_ok_min_ratio = _to_float(technical_policy.get("stoch_ok_min_ratio", 0.50), 0.50)
        boll_mid_min = _to_float(technical_policy.get("boll_mid_min", 0.0), 0.0)
        boll_mid_ok_min_ratio = _to_float(technical_policy.get("boll_mid_ok_min_ratio", 0.50), 0.50)
        obv_slope_min = _to_float(technical_policy.get("obv_slope_min", 0.0), 0.0)
        obv_ok_min_ratio = _to_float(technical_policy.get("obv_ok_min_ratio", 0.50), 0.50)
        sma_ema_ok_min_ratio = _to_float(technical_policy.get("sma_ema_ok_min_ratio", 0.50), 0.50)
    except Exception:
        rsi_min, rsi_max, macd_min_ratio, volcorr_min = 45.0, 75.0, 0.05, 0.03
        rsi_ok_min_ratio, volcorr_ok_min_ratio = 0.50, 0.50
        stoch_min, stoch_max, stoch_ok_min_ratio = 20.0, 85.0, 0.50
        boll_mid_min, boll_mid_ok_min_ratio = 0.0, 0.50
        obv_slope_min, obv_ok_min_ratio = 0.0, 0.50
        sma_ema_ok_min_ratio = 0.50

    rsi_ok_ratio = float(((rsi_v >= rsi_min) & (rsi_v <= rsi_max)).mean()) if len(rsi_v) else 0.0
    macd_true = macd_v.astype(str).str.strip().str.upper().isin(["TRUE", "1", "Y", "YES"])
    macd_ok_ratio = float(macd_true.mean()) if len(macd_true) else 0.0
    volcorr_ok_ratio = float((vol_v >= volcorr_min).mean()) if len(vol_v) else 0.0

    optional_checks: List[bool] = []
    optional_labels: List[str] = []
    missing_optional_inputs: List[str] = []
    if "stoch_k" in candidate_df.columns:
        stoch_ok_ratio = float(((stoch >= stoch_min) & (stoch <= stoch_max)).mean()) if isinstance(stoch, pd.Series) and len(stoch) else 0.0
        optional_checks.append(stoch_ok_ratio >= stoch_ok_min_ratio)
        optional_labels.append(f"stoch_ok={stoch_ok_ratio:.2f}")
    else:
        missing_optional_inputs.append("technical:missing_col=stoch_k")
    if "boll_mid_pos" in candidate_df.columns:
        boll_ok_ratio = float((boll_mid >= boll_mid_min).mean()) if isinstance(boll_mid, pd.Series) and len(boll_mid) else 0.0
        optional_checks.append(boll_ok_ratio >= boll_mid_ok_min_ratio)
        optional_labels.append(f"boll_ok={boll_ok_ratio:.2f}")
    else:
        missing_optional_inputs.append("technical:missing_col=boll_mid_pos")
    if "obv_slope" in candidate_df.columns:
        obv_ok_ratio = float((obv_slope >= obv_slope_min).mean()) if isinstance(obv_slope, pd.Series) and len(obv_slope) else 0.0
        optional_checks.append(obv_ok_ratio >= obv_ok_min_ratio)
        optional_labels.append(f"obv_ok={obv_ok_ratio:.2f}")
    else:
        missing_optional_inputs.append("technical:missing_col=obv_slope")
    if "sma_ema_trend_ok" in candidate_df.columns:
        sma_ema_true = sma_ema_ok.astype(str).str.strip().str.upper().isin(["TRUE", "1", "Y", "YES"])
        sma_ema_ratio = float(sma_ema_true.mean()) if len(sma_ema_true) else 0.0
        optional_checks.append(sma_ema_ratio >= sma_ema_ok_min_ratio)
        optional_labels.append(f"sma_ema_ok={sma_ema_ratio:.2f}")
    else:
        missing_optional_inputs.append("technical:missing_col=sma_ema_trend_ok")
    for missing_input in missing_optional_inputs:
        if missing_input not in status["missing_inputs"]:
            status["missing_inputs"].append(missing_input)

    low_quality = (
        (macd_ok_ratio < macd_min_ratio)
        or (volcorr_ok_ratio < volcorr_ok_min_ratio)
        or (rsi_ok_ratio < rsi_ok_min_ratio)
        or (bool(optional_checks) and (not all(optional_checks)))
    )
    if not low_quality:
        return int(max_new)

    action = str(technical_policy.get("low_quality_action", "REDUCE") or "REDUCE").strip().upper()
    if action == "BLOCK":
        if int(max_new) != 0:
            status["actions"].append("technical:low_quality BLOCK")
        return 0

    prev_max_new = int(max_new)
    max_new = _cap_max_new(max_new, technical_policy.get("low_quality_max_new_cap", 1))
    if int(max_new) != prev_max_new:
        status["actions"].append(
            "technical:low_quality REDUCE "
            f"{prev_max_new}->{int(max_new)} "
            f"(rsi_ok={rsi_ok_ratio:.2f},macd_ok={macd_ok_ratio:.2f},volcorr_ok={volcorr_ok_ratio:.2f}"
            f"{',' if optional_labels else ''}{','.join(optional_labels)})"
        )
    return int(max_new)

def apply_p1_entry_controls(candidates: pd.DataFrame, max_new: int, cfg: Dict[str, Any]) -> tuple[pd.DataFrame, int, Dict[str, Any]]:
    candidate_df = candidates.copy()
    p1_policy = cfg.get("p1_entry_policy", {}) if isinstance(cfg, dict) else {}
    if not isinstance(p1_policy, dict) or (not bool(p1_policy.get("enabled", False))):
        return candidate_df, int(max_new), {"enabled": False, "actions": []}

    status: Dict[str, Any] = {
        "enabled": True,
        "actions": [],
        "missing_inputs": [],
        "states": {
            "calendar_enabled": False,
            "intraday_enabled": False,
            "event_enabled": False,
            "technical_enabled": False,
        },
    }

    calendar_policy = p1_policy.get("calendar", {}) if isinstance(p1_policy.get("calendar"), dict) else {}
    max_new = _apply_p1_calendar_gate(max_new, calendar_policy, status)

    intraday_policy = p1_policy.get("intraday", {}) if isinstance(p1_policy.get("intraday"), dict) else {}
    candidate_df, max_new = _apply_p1_intraday_gate(candidate_df, max_new, intraday_policy, status)

    event_policy = p1_policy.get("event_gate", {}) if isinstance(p1_policy.get("event_gate"), dict) else {}
    candidate_df, max_new = _apply_p1_event_gate(candidate_df, max_new, event_policy, status)

    technical_policy = p1_policy.get("technical_gate", {}) if isinstance(p1_policy.get("technical_gate"), dict) else {}
    max_new = _apply_p1_technical_gate(candidate_df, max_new, technical_policy, status)
    return candidate_df, int(max_new), status

def _normal_entry_execution_quality_decision(
    row: Any,
    cfg: Dict[str, Any],
    *,
    code: str,
    qty: int,
    entry_price: float,
) -> Tuple[bool, str, Dict[str, Any]]:
    policy = cfg.get("normal_entry_execution_quality", {}) if isinstance(cfg, dict) else {}
    policy = policy if isinstance(policy, dict) else {}
    meta: Dict[str, Any] = {
        "normal_exec_quality_enabled": bool(policy.get("enabled", True)),
        "normal_lob_check": "",
        "normal_lob_status": "",
        "normal_lob_reason": "",
        "normal_lob_source_ts": "",
        "normal_spread_bps": "",
        "normal_markout_1step_bps": "",
        "normal_executable_qty": "",
        "normal_executable_price": "",
        "normal_lob_ask_depth_levels": "",
        "normal_orderflow_tag": "",
    }
    if not bool(policy.get("enabled", True)):
        meta["normal_lob_check"] = "SKIP"
        meta["normal_lob_reason"] = "DISABLED"
        return True, "DISABLED", meta
    if int(qty or 0) <= 0 or float(entry_price or 0.0) <= 0.0:
        meta["normal_lob_check"] = "SKIP"
        meta["normal_lob_reason"] = "INVALID_QTY_OR_PRICE"
        return True, "INVALID_QTY_OR_PRICE", meta

    max_levels = max(1, min(10, int(_to_int(policy.get("max_depth_levels", 10), 10))))
    require_lob = bool(policy.get("require_lob", True))
    require_qty = bool(policy.get("require_executable_qty", True))
    min_qty_ratio = max(0.0, float(_to_float(policy.get("min_executable_qty_ratio", 1.0), 1.0)))
    max_spread_bps = float(_to_float(policy.get("max_spread_bps", 30.0), 30.0))
    min_markout_bps = float(_to_float(policy.get("min_markout_1step_bps", 0.0), 0.0) or 0.0)
    block_on_missing_markout = bool(policy.get("block_on_missing_markout", True))

    lob = _latest_lob_row_for_code(code)
    if not lob:
        meta.update({"normal_lob_check": "FAIL", "normal_lob_status": "MISSING", "normal_lob_reason": "NORMAL_LOB_MISSING"})
        return (not require_lob), ("NORMAL_LOB_MISSING" if require_lob else "LOB_MISSING_ALLOWED"), meta

    lob_status = str(lob.get("lob_status", "") or "").strip()
    meta["normal_lob_status"] = lob_status
    meta["normal_lob_source_ts"] = str(lob.get("ts", "") or "").strip()
    ask_book = _ask_book_from_row(lob, levels=max_levels)
    meta["normal_lob_ask_depth_levels"] = len(ask_book)

    lob_available = _truthy(lob.get("lob_available")) or bool(ask_book)
    if require_lob and ((not lob_available) or lob_status.upper() in {"NO_LOB", "MISSING", "UNKNOWN"}):
        meta.update({"normal_lob_check": "FAIL", "normal_lob_reason": "NORMAL_LOB_UNAVAILABLE"})
        return False, "NORMAL_LOB_UNAVAILABLE", meta

    orderflow_tag_block_tags: set[str] = {
        str(t or "").strip().upper()
        for t in (policy.get("orderflow_tag_block_tags") or ["CAUTION", "PAUSE", "NO_LOB"])
        if str(t or "").strip()
    }
    orderflow_tag = str(lob.get("orderflow_tag", "") or "").strip().upper()
    meta["normal_orderflow_tag"] = orderflow_tag
    if orderflow_tag and orderflow_tag in orderflow_tag_block_tags:
        meta.update({"normal_lob_check": "FAIL", "normal_lob_reason": f"NORMAL_ORDERFLOW_TAG_BLOCK:{orderflow_tag}"})
        return False, f"NORMAL_ORDERFLOW_TAG_BLOCK:{orderflow_tag}", meta

    spread_bps = _finite_float_or_none(lob.get("spread_bps"))
    if spread_bps is not None:
        meta["normal_spread_bps"] = float(spread_bps)
        if spread_bps > max_spread_bps:
            meta.update({"normal_lob_check": "FAIL", "normal_lob_reason": "NORMAL_SPREAD_BLOCK"})
            return False, "NORMAL_SPREAD_BLOCK", meta

    executable_qty = 0.0
    executable_notional = 0.0
    remaining = float(qty)
    for level in ask_book:
        if remaining <= 0:
            break
        level_qty = min(float(level.get("vol", 0.0) or 0.0), remaining)
        if level_qty <= 0:
            continue
        executable_qty += level_qty
        executable_notional += level_qty * float(level.get("price", 0.0) or 0.0)
        remaining -= level_qty
    executable_price = executable_notional / executable_qty if executable_qty > 0 else 0.0
    meta["normal_executable_qty"] = float(executable_qty)
    meta["normal_executable_price"] = float(executable_price) if executable_price > 0 else ""

    required_qty = float(qty) * min_qty_ratio
    if require_qty and executable_qty + 1e-9 < required_qty:
        meta.update({"normal_lob_check": "FAIL", "normal_lob_reason": "NORMAL_LOB_QTY_BLOCK"})
        return False, "NORMAL_LOB_QTY_BLOCK", meta

    markout_bps = _finite_float_or_none(lob.get("markout_1step_bps"))
    if markout_bps is None:
        if block_on_missing_markout:
            meta.update({"normal_lob_check": "FAIL", "normal_lob_reason": "NORMAL_MARKOUT_MISSING"})
            return False, "NORMAL_MARKOUT_MISSING", meta
    else:
        meta["normal_markout_1step_bps"] = float(markout_bps)
        if markout_bps < min_markout_bps:
            meta.update({"normal_lob_check": "FAIL", "normal_lob_reason": "NORMAL_MARKOUT_BLOCK"})
            return False, "NORMAL_MARKOUT_BLOCK", meta

    meta.update({"normal_lob_check": "PASS", "normal_lob_reason": "PASS"})
    return True, "PASS", meta

def _normal_exec_quality_qty_reduction(
    meta: Dict[str, Any],
    cfg: Dict[str, Any],
) -> Tuple[float, str]:
    policy = cfg.get("normal_entry_execution_quality", {}) if isinstance(cfg, dict) else {}
    policy = policy if isinstance(policy, dict) else {}
    red_cfg = policy.get("qty_reduction", {}) if isinstance(policy.get("qty_reduction"), dict) else {}
    if not bool(red_cfg.get("enabled", False)):
        return 1.0, ""

    reasons: List[str] = []
    multipliers: List[float] = []
    categories: List[str] = []

    spread_bps = _finite_float_or_none(meta.get("normal_spread_bps"))
    markout_bps = _finite_float_or_none(meta.get("normal_markout_1step_bps"))
    orderflow_tag = str(meta.get("normal_orderflow_tag", "") or "").strip().upper()

    spread_bands = red_cfg.get("spread_bands", []) if isinstance(red_cfg.get("spread_bands"), list) else []
    spread_hit: Optional[Dict[str, Any]] = None
    if spread_bps is not None:
        for band in spread_bands:
            if not isinstance(band, dict):
                continue
            threshold = float(_to_float(band.get("min_bps"), 0.0) or 0.0)
            if threshold > 0.0 and spread_bps >= threshold:
                if spread_hit is None or threshold >= float(_to_float(spread_hit.get("min_bps"), 0.0) or 0.0):
                    spread_hit = band
    if isinstance(spread_hit, dict):
        threshold = float(_to_float(spread_hit.get("min_bps"), 0.0) or 0.0)
        _append_reduction_multiplier(reasons, multipliers, categories, f"spread_bps>={threshold:g}", spread_hit.get("multiplier", 1.0), "spread")

    markout_bands = red_cfg.get("markout_bands", []) if isinstance(red_cfg.get("markout_bands"), list) else []
    markout_hit: Optional[Dict[str, Any]] = None
    if markout_bps is not None:
        for band in markout_bands:
            if not isinstance(band, dict):
                continue
            threshold = float(_to_float(band.get("max_bps"), 0.0) or 0.0)
            if threshold > 0.0 and markout_bps <= threshold:
                if markout_hit is None or threshold <= float(_to_float(markout_hit.get("max_bps"), float("inf")) or float("inf")):
                    markout_hit = band
    if isinstance(markout_hit, dict):
        threshold = float(_to_float(markout_hit.get("max_bps"), 0.0) or 0.0)
        _append_reduction_multiplier(reasons, multipliers, categories, f"markout_bps<={threshold:g}", markout_hit.get("multiplier", 1.0), "markout")

    caution_tags: set[str] = {
        str(t or "").strip().upper()
        for t in (red_cfg.get("orderflow_caution_reduce_tags") or ["CAUTION"])
        if str(t or "").strip()
    }
    if orderflow_tag and orderflow_tag in caution_tags:
        caution_mult = float(_to_float(red_cfg.get("orderflow_caution_multiplier"), 1.0))
        _append_reduction_multiplier(reasons, multipliers, categories, f"orderflow_{orderflow_tag}", caution_mult, "orderflow")

    if not multipliers:
        return 1.0, ""

    mult = min(multipliers)
    risk_count = len(set(categories))
    multi_cfg = red_cfg.get("multi_condition", {}) if isinstance(red_cfg.get("multi_condition"), dict) else {}
    if risk_count >= 2:
        mult = min(mult, float(_to_float(multi_cfg.get("two_or_more_multiplier"), mult) or mult))
    mult = max(0.0, min(1.0, mult))
    if not (0.0 < mult < 1.0):
        return 1.0, ""
    return mult, f"normal_exec_qty_reduction={','.join(reasons)}:risk_count={risk_count}:mult={mult:.2f}"

def _normal_close_auction_decision(
    cfg: Dict[str, Any],
    *,
    close_pos: float,
    day_range_pct: float,
    v_accel: float,
) -> Tuple[str, str, Dict[str, Any]]:
    policy = _normal_realtime_gap_policy(cfg)
    sub = policy.get("close_auction", {}) if isinstance(policy.get("close_auction"), dict) else {}
    if not policy or not bool(sub.get("enabled", True)):
        return "PASS", "DISABLED", {}
    block_close_pos_min = float(_to_float(sub.get("block_close_pos_min"), 0.50))
    reduce_close_pos_min = float(_to_float(sub.get("reduce_close_pos_min"), 0.70))
    max_day_range_pct = _pct01_from_config(sub.get("max_day_range_pct"), 0.07)
    block_v_accel_min = float(_to_float(sub.get("block_v_accel_min"), 1.0))
    reduce_v_accel_min = float(_to_float(sub.get("reduce_v_accel_min"), 1.5))
    reduce_mult = max(0.0, min(1.0, float(_to_float(sub.get("reduce_qty_multiplier"), 0.5))))
    reasons: List[str] = []
    if float(day_range_pct) > max_day_range_pct:
        reasons.append(f"day_range_pct>{max_day_range_pct:.4f}")
    if float(close_pos) < block_close_pos_min:
        reasons.append(f"close_pos<{block_close_pos_min:.4f}")
    if float(v_accel) < block_v_accel_min:
        reasons.append(f"v_accel<{block_v_accel_min:.4f}")
    meta = {
        "normal_close_auction_close_pos": float(close_pos),
        "normal_close_auction_day_range_pct": float(day_range_pct),
        "normal_close_auction_v_accel": float(v_accel),
        "normal_close_auction_reduce_multiplier": float(reduce_mult),
    }
    if reasons:
        return "BLOCK", "NORMAL_CLOSE_AUCTION_BLOCK(" + ",".join(reasons) + ")", meta
    reduce_reasons: List[str] = []
    if float(close_pos) < reduce_close_pos_min:
        reduce_reasons.append(f"close_pos<{reduce_close_pos_min:.4f}")
    if float(v_accel) < reduce_v_accel_min:
        reduce_reasons.append(f"v_accel<{reduce_v_accel_min:.4f}")
    if reduce_reasons and 0.0 < reduce_mult < 1.0:
        return "REDUCE", "NORMAL_CLOSE_AUCTION_REDUCE(" + ",".join(reduce_reasons) + ")", meta
    return "PASS", "PASS", meta

def _latest_intraday_row_for_code_date(code: str, ymd: str) -> Dict[str, Any]:
    hist = _load_intraday_history_for_ymd(ymd)
    if hist.empty or "code" not in hist.columns:
        return {}
    code6 = str(code or "").zfill(6)
    work = hist[hist["code"].astype(str).str.zfill(6) == code6].copy()
    if work.empty:
        return {}
    if "_ts14" in work.columns:
        work = work.sort_values("_ts14")
    return work.iloc[-1].to_dict()

def _normal_intraday_momentum_decision(
    row: Any,
    cfg: Dict[str, Any],
    *,
    code: str,
    entry_day: str,
) -> Tuple[str, str, Dict[str, Any]]:
    policy = _normal_realtime_gap_policy(cfg)
    sub = policy.get("intraday_momentum_recheck", {}) if isinstance(policy.get("intraday_momentum_recheck"), dict) else {}
    if not policy or not bool(sub.get("enabled", True)):
        return "PASS", "DISABLED", {}
    intraday = _latest_intraday_row_for_code_date(code, entry_day)
    require_intraday = bool(sub.get("require_intraday", True))
    if not intraday:
        return (
            "BLOCK" if require_intraday else "PASS",
            "NORMAL_INTRADAY_MOMENTUM_MISSING" if require_intraday else "INTRADAY_MISSING_ALLOWED",
            {"normal_intraday_momentum_status": "MISSING"},
        )
    row_v_accel = float(_to_float(row.get("v_accel") if hasattr(row, "get") else 0.0, 0.0) or 0.0)
    row_value = float(
        _to_float(
            (row.get("value") if hasattr(row, "get") else None)
            or (row.get("trading_value") if hasattr(row, "get") else None),
            0.0,
        )
        or 0.0
    )
    intraday_value = float(_to_float(intraday.get("trading_value"), 0.0) or 0.0)
    value_ratio = intraday_value / row_value if row_value > 0 and intraday_value > 0 else 1.0
    rechecked_v_accel = row_v_accel * min(1.0, max(0.0, value_ratio))
    # `or <default>` silently discards a configured 0, so the block could not be
    # turned off for observation. This is the path that emits
    # NORMAL_INTRADAY_MOMENTUM_BLOCK below.
    block_v_accel_min = float(_to_float(sub.get("block_v_accel_min"), 1.0))
    reduce_v_accel_min = float(_to_float(sub.get("reduce_v_accel_min"), 1.5))
    min_value_ratio = max(0.0, float(_to_float(sub.get("min_value_ratio"), 0.70)))
    reduce_mult = max(0.0, min(1.0, float(_to_float(sub.get("reduce_qty_multiplier"), 0.5))))
    meta = {
        "normal_intraday_momentum_status": "CHECKED",
        "normal_intraday_momentum_ts": str(intraday.get("ts", "") or ""),
        "normal_intraday_value_ratio": float(value_ratio),
        "normal_intraday_rechecked_v_accel": float(rechecked_v_accel),
        "normal_intraday_reduce_multiplier": float(reduce_mult),
    }
    if rechecked_v_accel < block_v_accel_min:
        return "BLOCK", f"NORMAL_INTRADAY_MOMENTUM_BLOCK(v_accel<{block_v_accel_min:.4f})", meta
    if (rechecked_v_accel < reduce_v_accel_min or value_ratio < min_value_ratio) and 0.0 < reduce_mult < 1.0:
        reasons = []
        if rechecked_v_accel < reduce_v_accel_min:
            reasons.append(f"v_accel<{reduce_v_accel_min:.4f}")
        if value_ratio < min_value_ratio:
            reasons.append(f"value_ratio<{min_value_ratio:.4f}")
        return "REDUCE", "NORMAL_INTRADAY_MOMENTUM_REDUCE(" + ",".join(reasons) + ")", meta
    return "PASS", "PASS", meta

def _normal_reduced_min_qty_block_reason(qty_after: int, entry_price: float, cfg: Dict[str, Any]) -> Tuple[str, float]:
    policy = _normal_realtime_gap_policy(cfg)
    sub = policy.get("intraday_momentum_recheck", {}) if isinstance(policy.get("intraday_momentum_recheck"), dict) else {}
    min_qty = max(1, int(_to_int(sub.get("min_reduced_qty"), 2)))
    min_notional = max(0.0, float(_to_float(sub.get("min_reduced_notional_krw"), 0.0) or 0.0))
    notional = float(max(0, int(qty_after))) * max(0.0, float(entry_price or 0.0))
    reasons: List[str] = []
    if int(qty_after) < int(min_qty):
        reasons.append(f"qty_after<{int(min_qty)}")
    if min_notional > 0.0 and notional < min_notional:
        reasons.append(f"notional<{min_notional:.0f}")
    if not reasons:
        return "", float(notional)
    return "NORMAL_INTRADAY_MOMENTUM_REDUCED_MIN_QTY_BLOCK(" + ",".join(reasons) + ")", float(notional)

def _normal_dynamic_slippage_pct(
    row: Any,
    cfg: Dict[str, Any],
    *,
    base_slip_pct: float,
    market_cap: float,
) -> Tuple[float, str, Dict[str, Any]]:
    policy = _normal_realtime_gap_policy(cfg)
    sub = policy.get("dynamic_slippage", {}) if isinstance(policy.get("dynamic_slippage"), dict) else {}
    if not policy or not bool(sub.get("enabled", True)):
        return float(base_slip_pct), "DISABLED", {}
    value = float(
        _to_float(
            (row.get("value") if hasattr(row, "get") else None)
            or (row.get("trading_value") if hasattr(row, "get") else None),
            0.0,
        )
        or 0.0
    )
    low_value = float(_to_float(sub.get("low_trading_value_krw"), 3_000_000_000))
    very_low_value = float(_to_float(sub.get("very_low_trading_value_krw"), 1_000_000_000))
    low_mult = max(1.0, float(_to_float(sub.get("low_trading_value_multiplier"), 1.5)))
    very_low_mult = max(1.0, float(_to_float(sub.get("very_low_trading_value_multiplier"), 2.0)))
    low_turnover_ratio = max(0.0, float(_to_float(sub.get("low_turnover_ratio"), 0.003)))
    low_turnover_mult = max(1.0, float(_to_float(sub.get("low_turnover_multiplier"), 1.5)))
    max_slip = max(float(base_slip_pct), float(_to_float(sub.get("max_slippage_pct"), 0.03)))
    multiplier = 1.0
    reasons: List[str] = []
    if value > 0 and very_low_value > 0 and value < very_low_value:
        multiplier = max(multiplier, very_low_mult)
        reasons.append(f"value<{very_low_value:.0f}")
    elif value > 0 and low_value > 0 and value < low_value:
        multiplier = max(multiplier, low_mult)
        reasons.append(f"value<{low_value:.0f}")
    turnover = value / float(market_cap) if value > 0 and float(market_cap or 0.0) > 0 else 0.0
    if turnover > 0 and turnover < low_turnover_ratio:
        multiplier = max(multiplier, low_turnover_mult)
        reasons.append(f"turnover<{low_turnover_ratio:.4f}")
    slip = min(max_slip, max(float(base_slip_pct), float(base_slip_pct) * multiplier))
    meta = {
        "normal_dynamic_slippage_base_pct": float(base_slip_pct),
        "normal_dynamic_slippage_pct": float(slip),
        "normal_dynamic_slippage_multiplier": float(multiplier),
        "normal_dynamic_slippage_trading_value": float(value),
        "normal_dynamic_slippage_turnover": float(turnover),
    }
    return slip, ("PASS" if not reasons else "NORMAL_DYNAMIC_SLIPPAGE(" + ",".join(reasons) + ")"), meta

def _apply_entry_execution_score(
    candidate_df: pd.DataFrame,
    *,
    cfg: Dict[str, Any],
    score_src: str,
) -> pd.DataFrame:
    if not isinstance(candidate_df, pd.DataFrame) or candidate_df.empty:
        return candidate_df

    cdf = candidate_df.copy()
    base = pd.to_numeric(cdf.get(score_src), errors="coerce").fillna(-1e9)
    score = base.astype(float).copy()
    reasons = pd.Series("", index=cdf.index, dtype="object")

    def _add_reason(mask: pd.Series, text: str) -> None:
        nonlocal reasons
        mask = mask.fillna(False).astype(bool)
        if not bool(mask.any()):
            return
        prev = reasons.loc[mask].astype(str)
        reasons.loc[mask] = prev.where(prev == "", prev + ";") + text

    policy = cfg.get("entry_execution_score_policy", {}) if isinstance(cfg.get("entry_execution_score_policy"), dict) else {}
    enabled = bool(policy.get("enabled", True))
    if not enabled:
        cdf["entry_execution_score"] = score
        cdf["entry_execution_reason"] = "disabled:base_score"
        print("[EXEC_SCORE] disabled -> base score only")
        return cdf

    positive_bonus = float(_to_float(policy.get("positive_entry_bonus"), 0.03))
    execution_pool_bonus = float(_to_float(policy.get("execution_pool_bonus"), 0.02))
    macd_pass_bonus = float(_to_float(policy.get("macd_pass_bonus"), 0.015))
    sector_allowed_bonus = float(_to_float(policy.get("sector_allowed_bonus"), 0.01))
    split_second_bonus = float(_to_float(policy.get("split_second_bonus"), 0.03))
    krx_watch_penalty = float(_to_float(policy.get("krx_watch_penalty"), 0.12))
    overheat_penalty = float(_to_float(policy.get("overheat_penalty"), 0.02))

    if "positive_entry_ok" in cdf.columns:
        mask = cdf["positive_entry_ok"].apply(_truthy)
        score = score + mask.astype(float) * positive_bonus
        _add_reason(mask, f"positive_entry:+{positive_bonus:g}")

    if "execution_pool" in cdf.columns:
        mask = cdf["execution_pool"].apply(_truthy)
        score = score + mask.astype(float) * execution_pool_bonus
        _add_reason(mask, f"execution_pool:+{execution_pool_bonus:g}")

    if "macd_final_guard_pass" in cdf.columns:
        mask = cdf["macd_final_guard_pass"].apply(_truthy)
        score = score + mask.astype(float) * macd_pass_bonus
        _add_reason(mask, f"macd_pass:+{macd_pass_bonus:g}")

    if "macd_final_guard_penalty" in cdf.columns:
        penalty = pd.to_numeric(cdf["macd_final_guard_penalty"], errors="coerce").fillna(0.0).clip(lower=0.0)
        if float(penalty.max() if len(penalty) else 0.0) > 0.0:
            score = score - penalty
            _add_reason(penalty > 0, "macd_penalty")

    if "sector_entry_allowed" in cdf.columns:
        mask = cdf["sector_entry_allowed"].apply(_truthy)
        score = score + mask.astype(float) * sector_allowed_bonus
        _add_reason(mask, f"sector_allowed:+{sector_allowed_bonus:g}")

    if "sector_strength" in cdf.columns:
        strength = pd.to_numeric(cdf["sector_strength"], errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0)
        strength_weight = float(_to_float(policy.get("sector_strength_weight"), 0.02))
        if float(strength.max() if len(strength) else 0.0) > 0.0 and strength_weight > 0:
            score = score + strength * strength_weight
            _add_reason(strength > 0, "sector_strength")

    if "split_entry_2nd" in cdf.columns:
        mask = cdf["split_entry_2nd"].astype(str).str.strip().str.lower().isin({"1", "true", "t", "y", "yes"})
        score = score + mask.astype(float) * split_second_bonus
        _add_reason(mask, f"split2:+{split_second_bonus:g}")

    for col in ["krx_admin", "krx_warning", "krx_risk", "krx_caution"]:
        if col in cdf.columns:
            mask = cdf[col].apply(_truthy)
            score = score - mask.astype(float) * krx_watch_penalty
            _add_reason(mask, f"{col}:-{krx_watch_penalty:g}")

    overheat_cfg = cfg.get("entry_overheat_policy", {}) if isinstance(cfg.get("entry_overheat_policy"), dict) else {}
    v_accel_th = float(_to_float(overheat_cfg.get("v_accel_threshold"), policy.get("v_accel_threshold", 3.0)))
    ret1_th = float(_to_float(overheat_cfg.get("ret1_pct_threshold"), policy.get("ret1_pct_threshold", 8.0)))
    atr_th = float(_to_float(overheat_cfg.get("atr14_pct_threshold"), policy.get("atr14_pct_threshold", 0.10)))
    if "v_accel" in cdf.columns:
        mask = pd.to_numeric(cdf["v_accel"], errors="coerce").fillna(0.0) >= v_accel_th
        score = score - mask.astype(float) * overheat_penalty
        _add_reason(mask, "overheat_v_accel")
    if "ret1_pct" in cdf.columns:
        mask = pd.to_numeric(cdf["ret1_pct"], errors="coerce").fillna(0.0) >= ret1_th
        score = score - mask.astype(float) * overheat_penalty
        _add_reason(mask, "overheat_ret1")
    if "atr14_pct" in cdf.columns:
        mask = pd.to_numeric(cdf["atr14_pct"], errors="coerce").fillna(0.0) >= atr_th
        score = score - mask.astype(float) * overheat_penalty
        _add_reason(mask, "overheat_atr")

    cdf["entry_execution_score"] = score
    cdf["entry_execution_reason"] = reasons.where(reasons != "", "base_score")
    if len(cdf) > 0:
        top = (
            cdf[["code", score_src, "entry_execution_score", "entry_execution_reason"]]
            .sort_values(["entry_execution_score", str(score_src), "code"], ascending=[False, False, True], kind="mergesort")
            .head(5)
        )
        print("[EXEC_SCORE] top")
        print(top.to_string(index=False))
    return cdf

def _apply_entry_selection_policy(
    candidate_df: pd.DataFrame,
    *,
    cfg: Dict[str, Any],
    rank_col: Optional[str],
    max_new: int,
    exclude_codes: Optional[set[str]] = None,
    same_code_day_buy_counts: Optional[Dict[str, int]] = None,
) -> pd.DataFrame:
    if not isinstance(candidate_df, pd.DataFrame) or candidate_df.empty:
        return candidate_df

    cdf = candidate_df.copy()
    score_src = rank_col if rank_col in cdf.columns else ("final_score" if "final_score" in cdf.columns else ("score" if "score" in cdf.columns else None))
    if score_src is None:
        print("[SELECT_POLICY] score source missing -> skip")
        return cdf

    cdf["_select_score"] = pd.to_numeric(cdf[score_src], errors="coerce")
    total_rows = int(len(cdf))
    invalid_rows = int(cdf["_select_score"].isna().sum())
    cdf = cdf[cdf["_select_score"].notna()].copy()
    print(
        f"[SCORE_VALIDATE] source={score_src} total={total_rows} "
        f"valid={len(cdf)} invalid={invalid_rows}"
    )
    if cdf.empty:
        return cdf

    _surge_src = cdf["_surge_immediate"] if "_surge_immediate" in cdf.columns else pd.Series(0, index=cdf.index)
    cdf["_is_surge"] = _surge_src.apply(_truthy).astype(int)
    cdf["code"] = cdf["code"].astype(str).str.zfill(6)
    cdf = _apply_entry_execution_score(cdf, cfg=cfg, score_src=score_src)
    if "entry_execution_score" in cdf.columns:
        cdf["_select_score"] = pd.to_numeric(cdf["entry_execution_score"], errors="coerce").fillna(cdf["_select_score"])
        score_src = "entry_execution_score"

    policy = cfg.get("entry_selection_policy", {}) if isinstance(cfg.get("entry_selection_policy"), dict) else {}
    policy_by_run = cfg.get("entry_selection_policy_by_run_label", {}) if isinstance(cfg.get("entry_selection_policy_by_run_label"), dict) else {}
    run_label_key = str(RUN_LABEL or "main").strip().lower()
    policy_override = policy_by_run.get(run_label_key) if isinstance(policy_by_run.get(run_label_key), dict) else {}
    if not policy_override:
        run_label_upper = run_label_key.upper()
        if isinstance(policy_by_run.get(run_label_upper), dict):
            policy_override = policy_by_run.get(run_label_upper)
    if isinstance(policy_override, dict) and policy_override:
        merged_policy = dict(policy)
        merged_policy.update(policy_override)
        policy = merged_policy
    split2_priority_mode = str(policy.get("split_entry_2nd_priority", "SCORE_BONUS") or "SCORE_BONUS").strip().upper()
    split2_select_mask = pd.Series(False, index=cdf.index)
    if "split_entry_2nd" in cdf.columns:
        split2_select_mask = cdf["split_entry_2nd"].astype(str).str.strip().str.lower().isin({"1", "true", "t", "y", "yes"})
    if exclude_codes:
        ex = {str(x).zfill(6) for x in exclude_codes if str(x).strip()}
        if ex:
            before = len(cdf)
            cdf = cdf[(~cdf["code"].isin(ex)) | split2_select_mask].copy()
            after = len(cdf)
            print(f"[SELECT_POLICY] exclude_open_codes applied: {before}->{after}")
            if cdf.empty:
                cdf.attrs["selection_policy_drop_reason"] = "exclude_open_codes"
                cdf.attrs["selection_policy_rows_before"] = int(before)
                cdf.attrs["selection_policy_rows_after"] = int(after)
                cdf.attrs["selection_policy_drop_codes"] = sorted(ex)
                return cdf
            split2_select_mask = pd.Series(False, index=cdf.index)
            if "split_entry_2nd" in cdf.columns:
                split2_select_mask = cdf["split_entry_2nd"].astype(str).str.strip().str.lower().isin({"1", "true", "t", "y", "yes"})

    split2_priority_rows = cdf.iloc[0:0].copy()
    if bool(split2_select_mask.any()):
        if split2_priority_mode in {"ABSOLUTE", "PRIORITY", "LEGACY_PRIORITY"}:
            split2_priority_rows = cdf[split2_select_mask].copy()
            cdf = cdf[~split2_select_mask].copy()
            print(
                f"[SELECT_POLICY] split_entry_2nd priority retained={len(split2_priority_rows)} "
                f"normal_candidates={len(cdf)}"
            )
            if cdf.empty:
                return split2_priority_rows
        else:
            print(
                f"[SELECT_POLICY] split_entry_2nd priority mode={split2_priority_mode} "
                f"score_bonus_rows={int(split2_select_mask.sum())}"
            )

    def _with_split2_priority(selected: pd.DataFrame) -> pd.DataFrame:
        if split2_priority_rows.empty:
            return selected
        if selected is None or selected.empty:
            return split2_priority_rows
        return pd.concat([split2_priority_rows, selected], axis=0, ignore_index=False)

    enabled = bool(policy.get("enabled", False))
    mode = str(policy.get("mode", "AUTO") or "AUTO").strip().upper()
    prefer = str(policy.get("prefer", "HIGHER_SCORE") or "HIGHER_SCORE").strip().upper()
    surge_first = bool(policy.get("surge_priority_first", True))
    one_pick_max_new_le = max(0, _to_int(policy.get("one_pick_when_max_new_le", 1), 1))
    if bool(policy.get("skip_same_code_day_already_buy", True)) and same_code_day_buy_counts:
        same_day_codes = {
            str(code).strip().zfill(6)
            for code, count in same_code_day_buy_counts.items()
            if int(count or 0) > 0 and str(code).strip()
        }
        if same_day_codes:
            before = len(cdf)
            cdf = cdf[~cdf["code"].isin(same_day_codes)].copy()
            after = len(cdf)
            print(
                f"[SELECT_POLICY] skip_same_code_day_already_buy applied: {before}->{after} "
                f"codes={sorted(same_day_codes)}"
            )
            if cdf.empty:
                cdf.attrs["selection_policy_drop_reason"] = "skip_same_code_day_already_buy"
                cdf.attrs["selection_policy_rows_before"] = int(before)
                cdf.attrs["selection_policy_rows_after"] = int(after)
                cdf.attrs["selection_policy_drop_codes"] = sorted(same_day_codes)
                return _with_split2_priority(cdf)

    # Deterministic ordering (order-independent baseline).
    cdf = cdf.sort_values(
        ["_is_surge", "_select_score", "code"],
        ascending=[(not surge_first), False, True],
        kind="mergesort",
    )

    if not enabled:
        return _with_split2_priority(cdf)

    if mode == "NORMAL_ONLY":
        before = len(cdf)
        cdf = cdf[cdf["_is_surge"] == 0].copy()
        print(f"[SELECT_POLICY] mode=NORMAL_ONLY {before}->{len(cdf)}")
        return _with_split2_priority(cdf)

    if mode == "SURGE_ONLY":
        before = len(cdf)
        cdf = cdf[cdf["_is_surge"] == 1].copy()
        print(f"[SELECT_POLICY] mode=SURGE_ONLY {before}->{len(cdf)}")
        return _with_split2_priority(cdf)

    one_of_two = (mode == "ONE_OF_TWO") or (mode == "AUTO" and int(max_new) <= int(one_pick_max_new_le))
    if one_of_two:
        surge_df = cdf[cdf["_is_surge"] == 1].sort_values(["_select_score", "code"], ascending=[False, True], kind="mergesort")
        normal_df = cdf[cdf["_is_surge"] == 0].sort_values(["_select_score", "code"], ascending=[False, True], kind="mergesort")
        top_surge = surge_df.iloc[0] if len(surge_df) else None
        top_normal = normal_df.iloc[0] if len(normal_df) else None

        picked = None
        if top_surge is not None and top_normal is not None:
            if prefer == "SURGE":
                picked = top_surge
            elif prefer == "NORMAL":
                picked = top_normal
            else:
                picked = top_surge if float(top_surge["_select_score"]) >= float(top_normal["_select_score"]) else top_normal
        elif top_surge is not None:
            picked = top_surge
        elif top_normal is not None:
            picked = top_normal

        if picked is not None:
            out = cdf[cdf["code"] == str(picked["code"]).zfill(6)].head(1).copy()
            fallback_policy = policy.get("fallback_after_block", {}) if isinstance(policy.get("fallback_after_block"), dict) else {}
            fallback_enabled = bool(fallback_policy.get("enabled", False))
            fallback_max = max(1, int(_to_int(fallback_policy.get("max_candidates", 1), 1)))
            fallback_when_max_new_le = max(0, int(_to_int(fallback_policy.get("when_max_new_le", one_pick_max_new_le), one_pick_max_new_le) or one_pick_max_new_le))
            if fallback_enabled and int(max_new) <= int(fallback_when_max_new_le) and fallback_max > 1:
                fallback = cdf[~cdf["code"].isin(out["code"].astype(str).str.zfill(6))].head(fallback_max - 1).copy()
                if not fallback.empty:
                    out = pd.concat([out, fallback], axis=0, ignore_index=False)
            print(
                f"[SELECT_POLICY] mode=ONE_OF_TWO picked={str(picked['code']).zfill(6)} "
                f"class={'SURGE' if int(picked['_is_surge']) == 1 else 'NORMAL'} "
                f"score={float(picked['_select_score']):.4f} selected_rows={len(out)}"
            )
            return _with_split2_priority(out)
        return _with_split2_priority(cdf.iloc[0:0].copy())

    print(f"[SELECT_POLICY] mode={mode} ordered_rows={len(cdf)}")
    return _with_split2_priority(cdf)

def _apply_horizon_entry_policy(candidate_df: pd.DataFrame, cfg: Dict[str, Any]) -> tuple[pd.DataFrame, Dict[str, Any]]:
    hp = cfg.get("horizon_entry_policy", {}) if isinstance(cfg.get("horizon_entry_policy"), dict) else {}
    if not bool(hp.get("enabled", False)):
        return candidate_df, {"enabled": False}

    rules = hp.get("label_rules", {}) if isinstance(hp.get("label_rules"), dict) else {}
    default_label = str(hp.get("default_label", "MID") or "MID").strip().upper()
    valid_labels = ["SHORT", "SWING", "MID", "LONG"]
    if default_label not in valid_labels:
        default_label = "MID"

    df = candidate_df.copy()

    def _hz_numeric_col(name: str, default: float = 0.0) -> pd.Series:
        if name in df.columns:
            src = df[name]
        else:
            src = pd.Series(default, index=df.index)
        return pd.to_numeric(src, errors="coerce").fillna(default)

    if "final_score" in df.columns:
        df["_hz_final_score"] = pd.to_numeric(df["final_score"], errors="coerce").fillna(0.0)
    else:
        df["_hz_final_score"] = 0.0
    df["_hz_v_accel"] = _hz_numeric_col("v_accel")
    df["_hz_rs_slope"] = _hz_numeric_col("rs_slope")
    df["_hz_atr14_pct"] = _hz_numeric_col("atr14_pct")
    df["_hz_stoch_k"] = _hz_numeric_col("stoch_k")

    df["horizon_label"] = default_label
    df["horizon_reason"] = "default"

    short_rule = rules.get("SHORT", {}) if isinstance(rules.get("SHORT"), dict) else {}
    swing_rule = rules.get("SWING", {}) if isinstance(rules.get("SWING"), dict) else {}
    long_rule = rules.get("LONG", {}) if isinstance(rules.get("LONG"), dict) else {}

    short_mask = (
        (df["_hz_v_accel"] >= _to_float(short_rule.get("min_v_accel"), 1.0))
        & (df["_hz_rs_slope"] >= _to_float(short_rule.get("min_rs_slope"), 6.0))
    )
    swing_mask = (
        (df["_hz_atr14_pct"] >= _to_float(swing_rule.get("min_atr14_pct"), 0.06))
        & (df["_hz_stoch_k"] >= _to_float(swing_rule.get("min_stoch_k"), 65.0))
    )
    long_mask = (df["_hz_atr14_pct"] <= _to_float(long_rule.get("max_atr14_pct"), 0.08))

    df.loc[long_mask, "horizon_label"] = "LONG"
    df.loc[long_mask, "horizon_reason"] = "low_vol"
    df.loc[swing_mask, "horizon_label"] = "SWING"
    df.loc[swing_mask, "horizon_reason"] = "swing_vol"
    df.loc[short_mask, "horizon_label"] = "SHORT"
    df.loc[short_mask, "horizon_reason"] = "short_momo"
    df.loc[(~short_mask) & (~swing_mask) & (~long_mask), "horizon_label"] = "MID"
    df.loc[(~short_mask) & (~swing_mask) & (~long_mask), "horizon_reason"] = "mid_default"

    def _label_rule(label: str) -> Dict[str, Any]:
        v = rules.get(str(label), {})
        return v if isinstance(v, dict) else {}

    def _h_allowed(row: pd.Series) -> bool:
        label = str(row.get("horizon_label", default_label) or default_label).upper()
        rr = _label_rule(label)
        min_fs = _to_float(rr.get("min_final_score"), 0.0)
        return float(row.get("_hz_final_score") or 0.0) >= min_fs

    def _h_weight(row: pd.Series) -> float:
        label = str(row.get("horizon_label", default_label) or default_label).upper()
        rr = _label_rule(label)
        w = _to_float(rr.get("entry_weight"), 1.0)
        return max(0.10, min(1.00, w))

    def _h_max_hold(row: pd.Series) -> int:
        label = str(row.get("horizon_label", default_label) or default_label).upper()
        rr = _label_rule(label)
        try:
            return max(1, int(rr.get("max_hold_days", int(cfg.get("max_hold_days", 10) or 10)) or int(cfg.get("max_hold_days", 10) or 10)))
        except Exception:
            return int(cfg.get("max_hold_days", 10) or 10)

    df["horizon_entry_allowed"] = df.apply(_h_allowed, axis=1)
    df["horizon_entry_weight"] = df.apply(_h_weight, axis=1)
    df["horizon_max_hold_days"] = df.apply(_h_max_hold, axis=1)

    before = len(df)
    kept = df[df["horizon_entry_allowed"]].copy()
    after = len(kept)
    counts = (
        kept["horizon_label"].astype(str).value_counts().to_dict()
        if "horizon_label" in kept.columns else {}
    )
    print(f"[HORIZON] enabled=True candidates={before}->{after} counts={counts}")
    return kept, {
        "enabled": True,
        "before": int(before),
        "after": int(after),
        "allowed": int(after),
        "counts": counts,
    }

def _infer_horizon_label_from_row(row: Any, cfg: Dict[str, Any]) -> str:
    hp = cfg.get("horizon_entry_policy", {}) if isinstance(cfg.get("horizon_entry_policy"), dict) else {}
    rules = hp.get("label_rules", {}) if isinstance(hp.get("label_rules"), dict) else {}
    default_label = str(hp.get("default_label", "MID") or "MID").strip().upper()
    if default_label not in {"SHORT", "SWING", "MID", "LONG"}:
        default_label = "MID"

    short_rule = rules.get("SHORT", {}) if isinstance(rules.get("SHORT"), dict) else {}
    swing_rule = rules.get("SWING", {}) if isinstance(rules.get("SWING"), dict) else {}
    long_rule = rules.get("LONG", {}) if isinstance(rules.get("LONG"), dict) else {}

    v_accel = _to_float(row.get("v_accel"), 0.0)
    rs_slope = _to_float(row.get("rs_slope"), 0.0)
    atr14_pct = _to_float(row.get("atr14_pct"), 0.0)
    stoch_k = _to_float(row.get("stoch_k"), 0.0)

    if (v_accel >= _to_float(short_rule.get("min_v_accel"), 1.0)) and (rs_slope >= _to_float(short_rule.get("min_rs_slope"), 6.0)):
        return "SHORT"
    if (atr14_pct >= _to_float(swing_rule.get("min_atr14_pct"), 0.06)) and (stoch_k >= _to_float(swing_rule.get("min_stoch_k"), 65.0)):
        return "SWING"
    if atr14_pct <= _to_float(long_rule.get("max_atr14_pct"), 0.08):
        return "LONG"
    return default_label

def _horizon_max_hold_days_for_label(label: str, cfg: Dict[str, Any]) -> int:
    hp = cfg.get("horizon_entry_policy", {}) if isinstance(cfg.get("horizon_entry_policy"), dict) else {}
    rules = hp.get("label_rules", {}) if isinstance(hp.get("label_rules"), dict) else {}
    base_hold = _to_int(cfg.get("max_hold_days"), 10)
    rr = rules.get(str(label).upper(), {}) if isinstance(rules.get(str(label).upper()), dict) else {}
    return max(1, _to_int(rr.get("max_hold_days"), base_hold))



def _build_surge_entry_note_parts(row: Any) -> List[str]:
    surge_note_fields = {
        "surge_type": row.get("surge_type", ""),
        "surge_score": row.get("surge_score", ""),
        "surge_score_final": row.get("surge_score_final", row.get("final_score", "")),
        "surge_rvol20": row.get("surge_rvol20", row.get("rvol20", "")),
        "surge_spread_bps": row.get("surge_spread_bps", row.get("spread_bps", "")),
        "surge_orderflow_tag": row.get("surge_orderflow_tag", row.get("orderflow_tag", "")),
        "surge_orderflow_risk_score": row.get("surge_orderflow_risk_score", row.get("orderflow_risk_score", "")),
        "surge_lob_slippage_pct": row.get("surge_lob_slippage_pct", ""),
        "surge_lob_slippage_source": row.get("surge_lob_slippage_source", ""),
    }
    surge_source_decision_fields = {
        "source_entry_decision": row.get("source_entry_decision", ""),
        "source_entry_reason": row.get("source_entry_reason", ""),
        "source_entry_allowed": row.get("source_entry_allowed", ""),
        "source_entry_blocked": row.get("source_entry_blocked", ""),
        "source_exclude_reasons": row.get("exclude_reasons", ""),
        "surge_paper_probe_allowed": row.get("surge_paper_probe_allowed", ""),
        "surge_paper_probe_block_reasons": row.get("surge_paper_probe_block_reasons", ""),
    }
    out: List[str] = []
    for key, value in surge_note_fields.items():
        value_text = str(value if value is not None else "").strip()
        if value_text:
            out.append(f"{key}={value_text}")
    for key, value in surge_source_decision_fields.items():
        value_text = str(value if value is not None else "").strip()
        if value_text:
            out.append(f"{key}={value_text}")
    return out


def _resolve_entry_horizon_label(row: Any, cfg: Dict[str, Any], *, is_surge_immediate: bool) -> str:
    horizon_label = str(row.get("horizon_label") or "").strip().upper()
    if horizon_label:
        return horizon_label
    if is_surge_immediate:
        return str(
            cfg.get("surge_entry_policy", {}).get("horizon_label", "SWING") or "SWING"
        ).strip().upper()
    return _infer_horizon_label_from_row(row, cfg)


def _backfill_horizon_for_existing_order(
    open_positions: List[Dict[str, Any]],
    order_id: str,
    row: Any,
    cfg: Dict[str, Any],
) -> bool:
    if not order_id:
        return False
    hz_label = str(row.get("horizon_label") or "").strip().upper()
    if not hz_label:
        hz_label = _infer_horizon_label_from_row(row, cfg)
    hz_hold = _horizon_max_hold_days_for_label(hz_label, cfg) if hz_label else 0
    changed = False
    for pos in list(open_positions or []):
        if str(pos.get("entry_order_id", "") or "") != str(order_id):
            continue
        if not str(pos.get("horizon_label", "") or "").strip() and hz_label:
            pos["horizon_label"] = hz_label
            changed = True
        if _to_int(pos.get("horizon_max_hold_days"), 0) <= 0 and hz_hold > 0:
            pos["horizon_max_hold_days"] = int(hz_hold)
            changed = True
    return changed

def _write_p1_gate_status(
    p1_controls: Dict[str, Any],
    *,
    max_new_before_p1: int,
    max_new_after_p1: int,
    entry_candidates_before_p1: int,
    entry_candidates_after_p1: int,
    market_regime: str,
    entry_decision_code: str,
    entry_decision_reason: str,
    risk_gate_runtime: Optional[Dict[str, Any]] = None,
) -> None:
    if not bool(p1_controls.get("enabled")):
        return

    actions = _get_list(p1_controls, "actions")
    if actions:
        print(f"[P1_GATE] actions={'; '.join(str(x) for x in actions)}")
    else:
        print("[P1_GATE] actions=none")
    try:
        run_label = str(os.environ.get("PAPER_RUN_LABEL", "main") or "main").strip().lower()
        if not run_label:
            run_label = "main"
        p1_payload: Dict[str, Any] = {
            "as_of_ymd": now_ymd(),
            "run_label": run_label,
            "enabled": True,
            "actions": [str(x) for x in actions],
            "missing_inputs": [str(x) for x in (p1_controls.get("missing_inputs") or [])],
            "states": dict(p1_controls.get("states") or {}),
            "entry_candidates_before": int(entry_candidates_before_p1),
            "entry_candidates_after": int(entry_candidates_after_p1),
            "max_new_before": int(max_new_before_p1),
            "max_new_after": int(max_new_after_p1),
            "market_regime": str(market_regime or ""),
            "entry_gate_decision_before_p1": str(entry_decision_code or ""),
            "entry_gate_reason_before_p1": str(entry_decision_reason or ""),
            "entry_risk_basis": _build_entry_risk_basis(dict(risk_gate_runtime or {})),
            "risk_gate_runtime": dict(risk_gate_runtime or {}),
        }
        P1_GATE_STATUS_PATH.write_text(json.dumps(p1_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        try:
            expected_header = [
                "ts",
                "as_of_ymd",
                "run_label",
                "actions_count",
                "missing_inputs_count",
                "entry_candidates_before",
                "entry_candidates_after",
                "max_new_before",
                "max_new_after",
                "market_regime",
                "entry_gate_decision_before_p1",
            ]
            if P1_GATE_STATUS_HISTORY_CSV_PATH.exists():
                try:
                    with P1_GATE_STATUS_HISTORY_CSV_PATH.open("r", newline="", encoding="utf-8-sig") as rf:
                        rows = list(csv.reader(rf))
                    if rows:
                        migrated = [expected_header]
                        run_labels = {"main", "shadow", "unknown"}
                        needs_rewrite = False
                        header = rows[0]
                        if "run_label" not in header:
                            needs_rewrite = True
                        for row in rows[1:]:
                            normalized = None
                            if len(row) == 10:
                                needs_rewrite = True
                                normalized = [
                                    row[0],
                                    row[1],
                                    "unknown",
                                    row[2],
                                    row[3],
                                    row[4],
                                    row[5],
                                    row[6],
                                    row[7],
                                    row[8],
                                    row[9],
                                ]
                            elif len(row) >= 11:
                                v2_ok = row[2].strip().lower() in run_labels and row[3].strip().isdigit()
                                if v2_ok:
                                    normalized = row[:11]
                                else:
                                    transitional = row[2].strip().lower() == "unknown" and row[3].strip().lower() in run_labels
                                    if transitional:
                                        needs_rewrite = True
                                        normalized = [
                                            row[0],
                                            row[1],
                                            row[3].strip().lower(),
                                            row[4] if len(row) > 4 else "0",
                                            row[5] if len(row) > 5 else "0",
                                            row[6] if len(row) > 6 else "0",
                                            row[7] if len(row) > 7 else "0",
                                            row[8] if len(row) > 8 else "0",
                                            row[9] if len(row) > 9 else "0",
                                            row[10] if len(row) > 10 else "",
                                            row[11] if len(row) > 11 else "",
                                        ]
                            if normalized is not None:
                                migrated.append(normalized)
                        if needs_rewrite:
                            with P1_GATE_STATUS_HISTORY_CSV_PATH.open("w", newline="", encoding="utf-8-sig") as wf:
                                csv.writer(wf).writerows(migrated)
                except Exception:
                    pass
            if not P1_GATE_STATUS_HISTORY_CSV_PATH.exists():
                P1_GATE_STATUS_HISTORY_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
                with P1_GATE_STATUS_HISTORY_CSV_PATH.open("w", newline="", encoding="utf-8-sig") as hf:
                    csv.writer(hf).writerow(expected_header)
            with P1_GATE_STATUS_HISTORY_CSV_PATH.open("a", newline="", encoding="utf-8-sig") as hf:
                csv.writer(hf).writerow([
                    now_ts(),
                    p1_payload.get("as_of_ymd", ""),
                    p1_payload.get("run_label", "main"),
                    len(p1_payload.get("actions", []) or []),
                    len(p1_payload.get("missing_inputs", []) or []),
                    p1_payload.get("entry_candidates_before", 0),
                    p1_payload.get("entry_candidates_after", 0),
                    p1_payload.get("max_new_before", 0),
                    p1_payload.get("max_new_after", 0),
                    p1_payload.get("market_regime", ""),
                    p1_payload.get("entry_gate_decision_before_p1", ""),
                ])
        except Exception as e:
            print(f"[P1_GATE] history write failed: {type(e).__name__}: {e}")
    except Exception as e:
        print(f"[P1_GATE] status write failed: {type(e).__name__}: {e}")


# =============================================================================
# Phase 4a: functions moved from paper_engine.py
# =============================================================================


def _build_recheck_pending_row(
    row: Any,
    *,
    code: str,
    signal_date: str,
    name: str,
    reason: str,
    today_ymd: str,
) -> Optional[Dict[str, Any]]:
    next_session = _next_krx_session_ymd(today_ymd)
    if not next_session:
        return None
    return {
        "signal_date": str(signal_date or today_ymd),
        "code": str(code or "").zfill(6),
        "name": str(name or ""),
        "carry_reason": "ENTRY_RECHECK_READY",
        "carry_origin_reason": "CLOSE_CUTOFF_RECHECK",
        "captured_at": now_ts(),
        "carryover_count": max(1, _to_int(row.get("carryover_count", 0), 0) + 1),
        "fallback_stage": 0,
        "score": row.get("score", None),
        "final_score": row.get("final_score", None),
        "carryover_max_age_days": 1,
        "entry_day_override": next_session,
        "entry_timing": row.get("entry_timing", ""),
        "surge_type_entry_timing": row.get("surge_type_entry_timing", ""),
        "_surge_immediate": 1 if _truthy(row.get("_surge_immediate")) else 0,
        "surge_type": row.get("surge_type", ""),
        "signal": "HOLD",
        "reason": str(reason or "CLOSE_CUTOFF_RECHECK"),
        "rank_score": row.get("final_score", row.get("score", "")),
    }
DEFENSE_SIGNAL_SHADOW_PATH = _path_from_env("PAPER_DEFENSE_SIGNAL_SHADOW_PATH", LOG_DIR / "defense_signal_shadow_latest.csv")
SECTOR_CORRELATION_STATUS_PATH = _path_from_env("PAPER_SECTOR_CORR_STATUS_PATH", LOG_DIR / "sector_correlation_latest.json")
def _adaptive_good_stock_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    policy = cfg.get("adaptive_good_stock_entry", {}) if isinstance(cfg, dict) else {}
    return policy if isinstance(policy, dict) and bool(policy.get("enabled", False)) else {}
def _adaptive_good_stock_route_map(cfg: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    policy = _adaptive_good_stock_cfg(cfg)
    if not policy:
        return {}
    path = Path(str(policy.get("policy_design_path") or (LOG_DIR / "adaptive_entry_condition_policy_design_latest.json")))
    if not path.exists():
        print(f"[ADAPTIVE_GOOD_STOCK_ENTRY] policy_missing path={path}")
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        print(f"[ADAPTIVE_GOOD_STOCK_ENTRY] policy_read_fail path={path} reason={type(exc).__name__}:{exc}")
        return {}
    rows = obj.get("current_candidate_mapping") if isinstance(obj, dict) else None
    if not isinstance(rows, list):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        code = norm_code(item.get("code", ""))
        route = str(item.get("policy_route") or "").strip().upper()
        if code and route:
            row = dict(item)
            row["policy_route"] = route
            row["_policy_source_path"] = str(path)
            out[code] = row
    return out
def _adaptive_good_stock_entry(route_map: Dict[str, Dict[str, Any]], code: Any) -> Dict[str, Any]:
    return route_map.get(norm_code(code), {}) if isinstance(route_map, dict) else {}
def _adaptive_good_stock_allowed(cfg: Dict[str, Any], key: str, route: str, default_routes: set[str]) -> bool:
    policy = _adaptive_good_stock_cfg(cfg)
    if not policy:
        return False
    raw = policy.get(key)
    routes = {str(x or "").strip().upper() for x in raw} if isinstance(raw, list) else set(default_routes)
    return str(route or "").strip().upper() in routes
def _adaptive_good_stock_wait_gapup_route(cfg: Dict[str, Any]) -> str:
    return str(_adaptive_good_stock_cfg(cfg).get("gapup_wait_route", "WAIT_PULLBACK_RECLAIM") or "WAIT_PULLBACK_RECLAIM").strip().upper()
def _adaptive_good_stock_qty_mult(cfg: Dict[str, Any], route: str) -> float:
    policy = _adaptive_good_stock_cfg(cfg)
    keys = {
        "QUALITY_PROBE_TIER1": "tier1_post_split_qty_multiplier",
        "QUALITY_PROBE_TIER2": "tier2_post_split_qty_multiplier",
        "PULLBACK_RECOVERY_PROBE": "pullback_post_split_qty_multiplier",
    }
    key = keys.get(str(route or "").strip().upper())
    if not policy or not key:
        return 1.0
    try:
        value = float(policy.get(key, 1.0))
    except Exception:
        return 1.0
    return max(0.0, min(1.0, value)) if math.isfinite(value) else 1.0
def _adaptive_good_stock_gapup_override_max(cfg: Dict[str, Any], route: str) -> float:
    policy = _adaptive_good_stock_cfg(cfg)
    route = str(route or "").strip().upper()
    key_by_route = {
        "QUALITY_PROBE_TIER1": "tier1_gapup_override_max_pct",
        "QUALITY_PROBE_TIER2": "tier2_gapup_override_max_pct",
    }
    key = key_by_route.get(route)
    if not policy or not key:
        return 0.0
    try:
        value = float(policy.get(key, 0.0))
    except Exception:
        return 0.0
    return max(0.0, value) if math.isfinite(value) else 0.0
def _mark_adaptive_good_stock(row: Any, entry: Dict[str, Any], original_block: str, action: str) -> None:
    if not isinstance(entry, dict) or not entry:
        return
    row["adaptive_good_stock_route"] = str(entry.get("policy_route") or "").strip().upper()
    row["adaptive_good_stock_action"] = str(action or "")
    row["adaptive_good_stock_original_block"] = str(original_block or "")
    row["adaptive_good_stock_entry_style"] = str(entry.get("entry_style") or "")
    row["adaptive_good_stock_size_hint"] = str(entry.get("size_hint") or "")
    row["adaptive_good_stock_confirmation_required"] = str(entry.get("confirmation_required") or "")
    row["adaptive_good_stock_quality_score"] = entry.get("quality_score_experimental", "")
    row["adaptive_good_stock_policy_source"] = str(entry.get("_policy_source_path") or "")
@dataclass
class ColMap:
    date: str
    code: str
    open: str
    high: str
    low: str
    close: str
    name: Optional[str] = None
def _evaluate_split_open_chase_block(cfg: Dict[str, Any], entry_price: Any, open_price: Any) -> Dict[str, Any]:
    split_cfg = cfg.get("split_entry", {}) if isinstance(cfg, dict) and isinstance(cfg.get("split_entry"), dict) else {}
    threshold = _pct01_from_config(split_cfg.get("max_open_to_entry_chase_pct", 0.0), 0.0)
    out = {
        "enabled": bool(split_cfg.get("enabled", False)) and threshold > 0.0,
        "blocked": False,
        "reason": "",
        "open_to_entry_chase_pct": None,
        "max_open_to_entry_chase_pct": float(threshold),
        "open_price": None,
        "entry_price": None,
    }
    if not out["enabled"]:
        return out
    entry_px = float(_to_float(entry_price, 0.0) or 0.0)
    open_px = float(_to_float(open_price, 0.0) or 0.0)
    out["entry_price"] = entry_px
    out["open_price"] = open_px
    if entry_px <= 0.0 or open_px <= 0.0:
        out["blocked"] = True
        out["reason"] = "OPEN_CHASE_BLOCK_PRICE_MISSING"
        return out
    chase_pct = (entry_px - open_px) / open_px
    out["open_to_entry_chase_pct"] = float(chase_pct)
    if chase_pct > threshold:
        out["blocked"] = True
        out["reason"] = "OPEN_CHASE_BLOCK"
    return out


def _evaluate_open_chase_entry_guard(
    row: Any,
    cfg: Dict[str, Any],
    adaptive_good_stock_route_map: Dict[str, Dict[str, Any]],
    *,
    code: str,
    entry_price: float,
    open_price: Any,
    is_split_2nd: bool,
    is_open_order_replay: bool,
) -> Dict[str, Any]:
    if is_split_2nd or is_open_order_replay:
        return {"enabled": False, "blocked": False, "adaptive_allowed": False, "reason": ""}
    guard = _evaluate_split_open_chase_block(cfg, entry_price, open_price)
    row_updates: Dict[str, Any] = {}
    if guard.get("enabled"):
        row_updates = {
            "open_to_entry_chase_pct": guard.get("open_to_entry_chase_pct"),
            "max_open_to_entry_chase_pct": guard.get("max_open_to_entry_chase_pct"),
            "open_chase_open_price": guard.get("open_price"),
            "open_chase_entry_price": guard.get("entry_price"),
        }
    if not bool(guard.get("blocked", False)):
        return {
            "enabled": bool(guard.get("enabled", False)),
            "blocked": False,
            "adaptive_allowed": False,
            "reason": "",
            "row_updates": row_updates,
            "guard": guard,
        }
    reason = str(guard.get("reason") or "OPEN_CHASE_BLOCK")
    adaptive_row = _adaptive_good_stock_entry(adaptive_good_stock_route_map, code)
    adaptive_route = str(adaptive_row.get("policy_route") or "").strip().upper()
    adaptive_allowed = _adaptive_good_stock_allowed(
        cfg,
        "open_chase_probe_routes",
        adaptive_route,
        {"QUALITY_PROBE_TIER1", "QUALITY_PROBE_TIER2"},
    )
    return {
        "enabled": bool(guard.get("enabled", False)),
        "blocked": True,
        "adaptive_allowed": bool(adaptive_allowed),
        "adaptive_row": adaptive_row,
        "adaptive_route": adaptive_route,
        "reason": reason,
        "row_updates": row_updates,
        "guard": guard,
    }



def _evaluate_limit_price_tolerance_guard(
    cfg: Dict[str, Any],
    px: pd.DataFrame,
    *,
    code: str,
    effective_signal_date: str,
    entry_price: float,
    is_open_order_replay: bool,
    fb_enabled: bool,
    fallback_stage: int,
) -> Dict[str, Any]:
    lpt = cfg.get("limit_price_tolerance", {}) if isinstance(cfg, dict) else {}
    enabled = bool(lpt.get("enabled")) and not is_open_order_replay and not (fb_enabled and fallback_stage > 0)
    if not enabled:
        return {"enabled": False, "blocked": False, "reason": ""}
    sig_ohlc = get_ohlc(px, code, effective_signal_date)
    ref_close = float(sig_ohlc["close"]) if sig_ohlc and float(sig_ohlc.get("close", 0)) > 0 else 0.0
    if ref_close <= 0:
        return {"enabled": True, "blocked": False, "reason": "", "ref_close": ref_close}
    max_dev = float(lpt.get("max_deviation_pct", 0.005))
    deviation = abs(float(entry_price) - ref_close) / ref_close
    if deviation <= max_dev:
        return {
            "enabled": True,
            "blocked": False,
            "reason": "",
            "ref_close": ref_close,
            "deviation": deviation,
            "max_deviation": max_dev,
        }
    print(
        f"[SKIP_LIMIT_PRICE] code={code} entry={entry_price} ref_close={ref_close} "
        f"deviation={deviation:.4f} > max={max_dev:.4f}"
    )
    return {
        "enabled": True,
        "blocked": True,
        "reason": "LIMIT_PRICE_TOLERANCE",
        "ref_close": ref_close,
        "deviation": deviation,
        "max_deviation": max_dev,
    }


def _evaluate_entry_gap_limit_guard(
    ops_policy: Dict[str, Any],
    *,
    code: str,
    gap: float,
    gap_entry_price: float,
    prev_close: float,
    gap_ref_date: str,
    entry_gap_down_stop_pct_runtime: float,
    use_same_close_today: bool,
    fallback_stage: int,
    is_open_order_replay: bool,
) -> Dict[str, Any]:
    if use_same_close_today:
        return {"blocked": False, "reason": ""}

    entry_gap_down_stop_pct = float(entry_gap_down_stop_pct_runtime or 0.0)
    if entry_gap_down_stop_pct > 0 and gap <= -abs(entry_gap_down_stop_pct):
        print(
            f"[SKIP_GAPDOWN] code={code} gap={gap:.3f} <= -{abs(entry_gap_down_stop_pct):.3f} "
            f"entry={gap_entry_price} ref_close={prev_close} ref_date={gap_ref_date}"
        )
        return {"blocked": True, "reason": "GAPDOWN_BLOCK"}

    fbp = ops_policy.get("entry_fallback_policy", {}) if isinstance(ops_policy, dict) else {}
    if bool(fbp.get("enabled")) and fallback_stage > 0 and not is_open_order_replay:
        stage_limits = fbp.get("stage_gap_up_limits", [0.03, 0.02, 0.01])
        fb_idx = min(fallback_stage - 1, len(stage_limits) - 1)
        fb_gap_limit = float(stage_limits[fb_idx])
        if gap > fb_gap_limit:
            print(f"[SKIP_FALLBACK_GAP] code={code} stage={fallback_stage} gap={gap:.3f} > limit={fb_gap_limit:.3f}")
            return {"blocked": True, "reason": "FALLBACK_GAP_BLOCK", "limit": fb_gap_limit}

    return {"blocked": False, "reason": ""}


def _evaluate_entry_gap_risk_guard(
    row: Dict[str, Any],
    cfg: Dict[str, Any],
    px: pd.DataFrame,
    *,
    code: str,
    entry_day: str,
    effective_signal_date: str,
    use_same_close_today: bool,
    is_open_order_replay: bool,
    is_surge_immediate: bool,
    use_intraday_realtime_entry: bool,
    entry_gap_up_reduce_gap: Any,
    surge_active_gap_override_allowed: bool,
) -> Dict[str, Any]:
    gap_guard = cfg.get("entry_gap_risk_guard", {}) if isinstance(cfg, dict) else {}
    enabled = bool(isinstance(gap_guard, dict) and gap_guard.get("enabled", False)) and not is_open_order_replay
    if not enabled:
        return {"enabled": False, "blocked": False, "reason": ""}

    gap_lookback = max(2, int(_to_int(gap_guard.get("lookback_sessions"), 20)))
    gap_threshold = _pct01_from_config(gap_guard.get("down_gap_threshold_pct"), 0.08)
    gap_max_count = max(0, int(_to_int(gap_guard.get("max_down_gap_count"), 0) or 0))
    gap_as_of = entry_day if use_same_close_today else effective_signal_date
    gap_stats = _recent_down_gap_stats(
        px=px,
        code=code,
        as_of_ymd=str(gap_as_of),
        lookback_sessions=int(gap_lookback),
        threshold_pct=float(gap_threshold),
    )
    gap_count = int(gap_stats.get("count", 0) or 0)
    gap_risk_override_allowed = False
    max_override_gap_count = gap_max_count

    if gap_count > gap_max_count:
        surge_policy_cfg = cfg.get("surge_entry_policy", {}) if isinstance(cfg, dict) else {}
        active_gap_risk_override_cfg = (
            surge_policy_cfg.get("active_entry_gap_risk_override", {})
            if isinstance(surge_policy_cfg, dict)
            else {}
        )
        if not isinstance(active_gap_risk_override_cfg, dict):
            active_gap_risk_override_cfg = {}
        if bool(active_gap_risk_override_cfg.get("enabled", False)):
            active_label_for_gap_risk = str(row.get("active_response_label", "") or "").strip().upper()
            max_override_gap_count = max(
                0,
                int(_to_int(active_gap_risk_override_cfg.get("max_down_gap_count"), gap_max_count) or gap_max_count),
            )
            min_current_gap = _pct01_from_config(
                active_gap_risk_override_cfg.get("min_current_gap_pct"),
                0.0,
            )
            require_active_gap_up_pass = bool(
                active_gap_risk_override_cfg.get("require_active_gap_up_override_pass", True)
            )
            current_gap = float(entry_gap_up_reduce_gap or 0.0)
            gap_risk_override_allowed = bool(
                is_surge_immediate
                and use_intraday_realtime_entry
                and active_label_for_gap_risk == "ACTIVE_ENTRY_READY"
                and gap_count <= max_override_gap_count
                and current_gap >= min_current_gap
                and ((not require_active_gap_up_pass) or bool(surge_active_gap_override_allowed))
            )
        if gap_risk_override_allowed:
            row["_surge_active_gap_risk_override"] = 1
            row["_surge_active_gap_risk_down_gap_count"] = gap_count
            row["_surge_active_gap_risk_worst_gap_pct"] = float(gap_stats.get("worst_gap_pct", 0.0) or 0.0)
            print(
                f"[SURGE_ACTIVE_GAP_RISK_OVERRIDE] code={code} "
                f"down_gap_count={gap_count} max={max_override_gap_count} "
                f"current_gap={float(entry_gap_up_reduce_gap or 0.0):.3f}"
            )

    if gap_count > gap_max_count and not gap_risk_override_allowed:
        print(
            f"[SKIP_GAP_RISK] code={code} down_gap_count={gap_count} "
            f"> max={gap_max_count} threshold={gap_threshold:.3f} "
            f"worst_gap={float(gap_stats.get('worst_gap_pct', 0.0) or 0.0):.3f}"
        )
        return {
            "enabled": True,
            "blocked": True,
            "reason": "GAP_RISK_HISTORY_BLOCK",
            "count": gap_count,
            "max_count": gap_max_count,
            "stats": gap_stats,
        }

    return {
        "enabled": True,
        "blocked": False,
        "reason": "",
        "count": gap_count,
        "max_count": gap_max_count,
        "override_allowed": bool(gap_risk_override_allowed),
        "stats": gap_stats,
    }


def _evaluate_entry_liquidity_guard(
    row: Dict[str, Any],
    cfg: Dict[str, Any],
    *,
    code: str,
    is_open_order_replay: bool,
    is_surge_immediate: bool,
) -> Dict[str, Any]:
    elc = cfg.get("entry_liquidity_check", {}) if isinstance(cfg, dict) else {}
    enabled = bool(elc.get("enabled")) and not is_open_order_replay
    if not enabled:
        return {"enabled": False, "blocked": False, "reason": ""}

    min_val = float(elc.get("min_trading_value_krw", 1_000_000_000))
    row_val = float(row.get("value") or 0)
    # Surge-immediate injected rows can be added without a precomputed "value" column.
    # In that case keep the liquidity check fail-open for surge path only.
    if is_surge_immediate and row_val <= 0:
        row_val = min_val
    if row_val < min_val:
        print(f"[SKIP_LIQUIDITY] code={code} trading_value={row_val:.0f} < min={min_val:.0f}")
        return {
            "enabled": True,
            "blocked": True,
            "reason": "LIQUIDITY_BLOCK",
            "trading_value": row_val,
            "min_trading_value": min_val,
        }

    return {
        "enabled": True,
        "blocked": False,
        "reason": "",
        "trading_value": row_val,
        "min_trading_value": min_val,
    }


def _resolve_entry_row_slippage(
    row: Dict[str, Any],
    cfg: Dict[str, Any],
    *,
    code: str,
    base_slip_pct: float,
    is_surge_immediate: bool,
    is_open_order_replay: bool,
) -> Dict[str, Any]:
    row_market_cap = float(row.get("market_cap") or 0)
    row_slip_pct = resolve_slip_pct(row_market_cap, cfg, base_slip_pct)
    sector_risk_note_parts: List[str] = []

    if (not is_surge_immediate) and (not is_open_order_replay):
        normal_slip_pct, normal_slip_reason, normal_slip_meta = _normal_dynamic_slippage_pct(
            row,
            cfg,
            base_slip_pct=float(row_slip_pct),
            market_cap=float(row_market_cap),
        )
        for normal_slip_key, normal_slip_val in normal_slip_meta.items():
            row[normal_slip_key] = normal_slip_val
        row["normal_dynamic_slippage_reason"] = normal_slip_reason
        if float(normal_slip_pct) > float(row_slip_pct) + 1e-12:
            print(
                f"[NORMAL_DYNAMIC_SLIPPAGE] code={code} slip={float(row_slip_pct):.4f}->{float(normal_slip_pct):.4f} "
                f"reason={normal_slip_reason}"
            )
            row_slip_pct = float(normal_slip_pct)
            sector_risk_note_parts.append(f"normal_dynamic_slippage={float(row_slip_pct):.4f}")
            sector_risk_note_parts.append(str(normal_slip_reason or ""))

    return {
        "row_market_cap": row_market_cap,
        "row_slip_pct": row_slip_pct,
        "sector_risk_note_parts": sector_risk_note_parts,
    }


def _resolve_initial_entry_qty(
    row: Dict[str, Any],
    cfg: Dict[str, Any],
    *,
    code: str,
    entry_price: float,
    fee_pct: float,
    row_slip_pct: float,
    is_split_2nd: bool,
    is_open_order_replay: bool,
    is_surge_immediate: bool,
    cap_basic_qty: Callable[[int, float, str], int],
) -> Dict[str, Any]:
    qty_override = _to_int(row.get("qty_override"))
    if is_split_2nd:
        qty = max(0, _to_int(row.get("split_remaining_qty", 0), 0))
        return {"qty": int(qty), "qty_override": qty_override, "reason": "SPLIT_SECOND_REMAINING"}
    if is_open_order_replay and qty_override is not None and qty_override > 0:
        return {"qty": int(qty_override), "qty_override": qty_override, "reason": "REPLAY_QTY_OVERRIDE"}

    spl_cfg = cfg.get("split_entry", {}) if isinstance(cfg, dict) else {}
    qty = calc_qty(entry_price, cfg, fee_pct, row_slip_pct)
    if not is_surge_immediate:
        qty = cap_basic_qty(int(qty), float(entry_price), code)
    if spl_cfg.get("enabled") and qty > 0:
        first_ratio_key = "surge_first_ratio" if is_surge_immediate else "first_ratio"
        first_ratio = float(spl_cfg.get(first_ratio_key, spl_cfg.get("first_ratio", 0.5)))
        qty = max(1, int(math.floor(qty * first_ratio)))

    adaptive_good_stock_mult = _adaptive_good_stock_qty_mult(cfg, str(row.get("adaptive_good_stock_route") or ""))
    if qty > 0 and 0.0 < adaptive_good_stock_mult < 1.0:
        old_qty_adaptive_good_stock = int(qty)
        qty = max(1, int(math.floor(float(qty) * adaptive_good_stock_mult)))
        qty = min(int(old_qty_adaptive_good_stock), int(qty))
        row["adaptive_good_stock_post_split_qty_multiplier"] = round(float(adaptive_good_stock_mult), 6)
        row["adaptive_good_stock_qty_before"] = int(old_qty_adaptive_good_stock)
        row["adaptive_good_stock_qty_after"] = int(qty)
        print(
            f"[ADAPTIVE_GOOD_STOCK_QTY] code={code} qty={old_qty_adaptive_good_stock}->{qty} "
            f"route={row.get('adaptive_good_stock_route', '')} mult={adaptive_good_stock_mult:.3f}"
        )

    return {"qty": int(qty), "qty_override": qty_override, "reason": "CALCULATED"}


def _apply_entry_gap_up_reduce_qty(
    row: Dict[str, Any],
    cfg: Dict[str, Any],
    *,
    code: str,
    qty: int,
    entry_gap_up_reduce_gap: Any,
    is_surge_immediate: bool,
    is_open_order_replay: bool,
) -> Dict[str, Any]:
    entry_gap_reduce_cfg = cfg.get("entry_gap_up_reduce", {}) if isinstance(cfg, dict) else {}
    if not (
        qty > 0
        and ((not is_surge_immediate) or bool(row.get("_surge_no_lob_gap_up_reduce")))
        and not is_open_order_replay
        and isinstance(entry_gap_reduce_cfg, dict)
        and bool(entry_gap_reduce_cfg.get("enabled", False))
        and entry_gap_up_reduce_gap is not None
    ):
        return {"qty": int(qty), "applied": False, "reason": ""}

    gap_reduce_threshold = _pct01_from_config(entry_gap_reduce_cfg.get("threshold_pct"), 0.0)
    gap_reduce_mult = float(_to_float(entry_gap_reduce_cfg.get("qty_multiplier"), 1.0))
    gap_reduce_min_qty = max(0, int(_to_int(entry_gap_reduce_cfg.get("min_qty"), 1)))
    if not (
        gap_reduce_threshold > 0
        and 0.0 < gap_reduce_mult < 1.0
        and float(entry_gap_up_reduce_gap) >= gap_reduce_threshold
    ):
        return {"qty": int(qty), "applied": False, "reason": ""}

    old_qty_gap_reduce = int(qty)
    new_qty = int(math.floor(float(qty) * gap_reduce_mult))
    if old_qty_gap_reduce > 0 and gap_reduce_min_qty > 0:
        new_qty = max(gap_reduce_min_qty, new_qty)
    new_qty = min(int(old_qty_gap_reduce), int(new_qty))
    row["_entry_gap_up_reduce_before_qty"] = int(old_qty_gap_reduce)
    row["_entry_gap_up_reduce_after_qty"] = int(new_qty)
    row["_entry_gap_up_reduce_gap"] = round(float(entry_gap_up_reduce_gap), 6)
    print(
        f"[ENTRY_GAP_UP_REDUCE] code={code} qty {old_qty_gap_reduce}->{new_qty} "
        f"gap={float(entry_gap_up_reduce_gap):.3f} threshold={gap_reduce_threshold:.3f} "
        f"mult={gap_reduce_mult:.3f}"
    )
    return {
        "qty": int(new_qty),
        "applied": True,
        "reason": "ENTRY_GAP_UP_REDUCE",
        "before_qty": int(old_qty_gap_reduce),
        "after_qty": int(new_qty),
        "gap": float(entry_gap_up_reduce_gap),
        "threshold": float(gap_reduce_threshold),
        "multiplier": float(gap_reduce_mult),
    }


def _apply_normal_entry_qty_reductions(
    row: Dict[str, Any],
    cfg: Dict[str, Any],
    adaptive_good_stock_route_map: Dict[str, Dict[str, Any]],
    sector_risk_note_parts: List[str],
    *,
    code: str,
    entry_day: str,
    entry_price: float,
    qty: int,
    use_same_close_today: bool,
    is_surge_immediate: bool,
    is_open_order_replay: bool,
    is_split_2nd: bool,
) -> Dict[str, Any]:
    if qty <= 0 or is_surge_immediate or is_open_order_replay or is_split_2nd:
        return {"qty": int(qty), "blocked": False, "reason": ""}

    auction_reduce_mult = float(_to_float(row.get("normal_close_auction_reduce_multiplier"), 1.0))
    if str(row.get("normal_close_auction_action", "") or "").upper() == "REDUCE" and 0.0 < auction_reduce_mult < 1.0:
        old_qty_auction = int(qty)
        qty = max(1, int(math.floor(float(qty) * auction_reduce_mult)))
        row["normal_close_auction_qty_before"] = int(old_qty_auction)
        row["normal_close_auction_qty_after"] = int(qty)
        sector_risk_note_parts.append(f"normal_close_auction_qty={old_qty_auction}->{qty}")
        sector_risk_note_parts.append(str(row.get("normal_close_auction_reason", "") or ""))
        print(
            f"[NORMAL_CLOSE_AUCTION_QTY_REDUCE] code={code} qty={old_qty_auction}->{qty} "
            f"reason={row.get('normal_close_auction_reason', '')}"
        )

    if not use_same_close_today:
        return {"qty": int(qty), "blocked": False, "reason": ""}

    mom_action, mom_reason, mom_meta = _normal_intraday_momentum_decision(
        row,
        cfg,
        code=code,
        entry_day=entry_day,
    )
    for mom_key, mom_val in mom_meta.items():
        row[mom_key] = mom_val
    row["normal_intraday_momentum_action"] = mom_action
    row["normal_intraday_momentum_reason"] = mom_reason
    if mom_action == "BLOCK":
        adaptive_good_stock_row = _adaptive_good_stock_entry(adaptive_good_stock_route_map, code)
        adaptive_good_stock_route = str(adaptive_good_stock_row.get("policy_route") or "").strip().upper()
        if _adaptive_good_stock_allowed(
            cfg,
            "intraday_momentum_probe_routes",
            adaptive_good_stock_route,
            {"PULLBACK_RECOVERY_PROBE"},
        ):
            _mark_adaptive_good_stock(
                row,
                adaptive_good_stock_row,
                mom_reason,
                "INTRADAY_MOMENTUM_SOFTENED_TO_REDUCE",
            )
            mom_action = "REDUCE"
            row["normal_intraday_momentum_action"] = mom_action
            row["normal_intraday_momentum_reason"] = f"ADAPTIVE_GOOD_STOCK_ENTRY({mom_reason})"
            row["normal_intraday_reduce_multiplier"] = min(
                float(_to_float(row.get("normal_intraday_reduce_multiplier"), 1.0)),
                _adaptive_good_stock_qty_mult(cfg, adaptive_good_stock_route),
            )
            print(
                f"[ADAPTIVE_GOOD_STOCK_ENTRY] code={code} route={adaptive_good_stock_route} "
                f"original={mom_reason} action=REDUCE"
            )
        else:
            print(f"[NORMAL_INTRADAY_MOMENTUM_BLOCK] code={code} reason={mom_reason}")
            return {"qty": int(qty), "blocked": True, "reason": str(mom_reason or "NORMAL_INTRADAY_MOMENTUM_BLOCK")}

    if mom_action == "REDUCE":
        mom_mult = float(_to_float(row.get("normal_intraday_reduce_multiplier"), 1.0))
        if 0.0 < mom_mult < 1.0:
            old_qty_mom = int(qty)
            qty = max(1, int(math.floor(float(qty) * mom_mult)))
            row["normal_intraday_momentum_qty_before"] = int(old_qty_mom)
            row["normal_intraday_momentum_qty_after"] = int(qty)
            sector_risk_note_parts.append(f"normal_intraday_momentum_qty={old_qty_mom}->{qty}")
            sector_risk_note_parts.append(str(mom_reason or ""))
            mom_min_reason, mom_notional = _normal_reduced_min_qty_block_reason(qty, entry_price, cfg)
            if mom_min_reason:
                row["normal_intraday_momentum_min_block_reason"] = mom_min_reason
                print(
                    f"[NORMAL_INTRADAY_MOMENTUM_MIN_QTY_BLOCK] code={code} "
                    f"qty={old_qty_mom}->{qty} notional={mom_notional:.0f} reason={mom_min_reason}"
                )
                return {"qty": int(qty), "blocked": True, "reason": str(mom_min_reason)}
            print(
                f"[NORMAL_INTRADAY_MOMENTUM_REDUCE] code={code} qty={old_qty_mom}->{qty} "
                f"reason={mom_reason}"
            )

    return {"qty": int(qty), "blocked": False, "reason": ""}


def _apply_entry_weight_qty_adjustments(
    row: Dict[str, Any],
    cfg: Dict[str, Any],
    *,
    code: str,
    qty: int,
    position_size_multiplier: float,
    entry_decision_code: str,
) -> Dict[str, Any]:
    if position_size_multiplier < 1.0 and qty > 0:
        old_qty_psm = int(qty)
        row["_qty_before_psm"] = int(old_qty_psm)
        qty = int(math.floor(qty * position_size_multiplier))
        row["_qty_after_psm"] = int(qty)
        ro_val_cfg_qty = ((cfg.get("risk_orchestration", {}) if isinstance(cfg, dict) else {}).get("dd_stop_validation", {}) or {})
        if not isinstance(ro_val_cfg_qty, dict):
            ro_val_cfg_qty = {}
        ro_val_labels_qty = {
            str(x).strip().lower()
            for x in (ro_val_cfg_qty.get("allowed_run_labels") or [])
            if str(x).strip()
        }
        ro_val_label_ok_qty = (not ro_val_labels_qty) or str(RUN_LABEL or "").strip().lower() in ro_val_labels_qty
        ro_val_reduce_qty_floor = (
            bool(ro_val_cfg_qty.get("enabled", False))
            and str(ro_val_cfg_qty.get("mode", "") or "").strip().lower() in {"reduce", "validation_reduce"}
            and ro_val_label_ok_qty
            and old_qty_psm > 0
            and position_size_multiplier > 0.0
        )
        if qty <= 0 and ro_val_reduce_qty_floor:
            qty = 1
            row["_qty_after_psm"] = int(qty)
            print(f"[VALIDATION_REDUCE_MIN_QTY] code={code} psm_qty_floor {old_qty_psm}->1 mult={position_size_multiplier:.4f}")
        elif qty <= 0 and bool(_minimum_quantity_verification_cfg(cfg).get("enabled", False)):
            qty = 1
            row["_qty_after_psm"] = int(qty)
            print(f"[MIN_QTY_VERIFY] code={code} psm_qty_floor {old_qty_psm}->1 session={PAPER_SESSION_ID}")
        elif qty <= 0 and old_qty_psm >= max(1, _to_int(cfg.get("min_qty", 1), 1)) and position_size_multiplier > 0.0:
            psm_zero_causes = str(row.get("_scale_zero_causes", "") or "").strip()
            if not psm_zero_causes and str(entry_decision_code or "").strip().upper() != "BLOCK":
                qty = max(1, min(int(old_qty_psm), max(1, _to_int(cfg.get("min_qty", 1), 1))))
                row["_qty_after_psm"] = int(qty)
                row["_positive_reduce_min_qty_floor"] = True
                print(f"[POSITIVE_REDUCE_MIN_QTY] code={code} psm_qty_floor {old_qty_psm}->1 mult={position_size_multiplier:.4f}")

    sector_entry_weight = _to_float(row.get("sector_entry_weight"), 1.0)
    if qty > 0 and 0.0 < sector_entry_weight < 1.0:
        qty = max(1, int(math.floor(qty * sector_entry_weight)))
    horizon_entry_weight = _to_float(row.get("horizon_entry_weight"), 1.0)
    if qty > 0 and 0.0 < horizon_entry_weight < 1.0:
        qty = max(1, int(math.floor(qty * horizon_entry_weight)))
    trend_entry_weight = _to_float(row.get("trend_entry_weight"), 1.0)
    if qty > 0 and trend_entry_weight > 0.0 and abs(trend_entry_weight - 1.0) > 1e-9:
        qty = max(1, int(math.floor(qty * trend_entry_weight)))

    return {
        "qty": int(qty),
        "sector_entry_weight": sector_entry_weight,
        "horizon_entry_weight": horizon_entry_weight,
        "trend_entry_weight": trend_entry_weight,
    }


def _apply_surge_lob_exec_quality(
    row: Dict[str, Any],
    cfg: Dict[str, Any],
    *,
    code: str,
    qty: int,
    entry_price: float,
    row_slip_pct: float,
    is_surge_immediate: bool,
    is_open_order_replay: bool,
    exec_q_max: float,
    fc_propagate: bool,
    fail_closed_triggered: bool,
) -> Dict[str, Any]:
    if not (qty > 0 and is_surge_immediate):
        return {
            "row_slip_pct": float(row_slip_pct),
            "blocked": False,
            "reason": "",
            "exec_quality_blocked_delta": 0,
            "fail_closed_triggered": bool(fail_closed_triggered),
            "fail_closed_reason": "",
        }

    lob_slip_pct, lob_slip_source = _surge_lob_slippage_pct(
        row,
        qty=int(qty),
        entry_price=float(entry_price),
        base_slip_pct=float(row_slip_pct),
        cfg=cfg,
    )
    row_slip_pct = max(float(row_slip_pct), float(lob_slip_pct))
    row["surge_lob_slippage_pct"] = float(row_slip_pct)
    row["surge_lob_slippage_source"] = str(lob_slip_source)

    if exec_q_max > 0.0 and not is_open_order_replay and float(row_slip_pct) > float(exec_q_max):
        print(
            f"[SURGE_LOB_SLIPPAGE_BLOCK] code={code} "
            f"lob_slippage={float(row_slip_pct):.4f} > max={float(exec_q_max):.4f} "
            f"source={lob_slip_source}"
        )
        next_fail_closed_triggered = bool(fail_closed_triggered)
        next_fail_closed_reason = ""
        if fc_propagate and not fail_closed_triggered:
            next_fail_closed_triggered = True
            next_fail_closed_reason = f"SURGE_LOB_SLIPPAGE_BLOCK(code={code},slip={float(row_slip_pct):.4f})"
            print(
                f"[FAIL_CLOSED] triggered by SURGE_LOB_SLIPPAGE_BLOCK "
                f"code={code} -> max_new=0 for remaining"
            )
        return {
            "row_slip_pct": float(row_slip_pct),
            "blocked": True,
            "reason": "SURGE_LOB_SLIPPAGE_BLOCK",
            "exec_quality_blocked_delta": 1,
            "fail_closed_triggered": bool(next_fail_closed_triggered),
            "fail_closed_reason": next_fail_closed_reason,
            "lob_slip_source": str(lob_slip_source),
        }

    return {
        "row_slip_pct": float(row_slip_pct),
        "blocked": False,
        "reason": "",
        "exec_quality_blocked_delta": 0,
        "fail_closed_triggered": bool(fail_closed_triggered),
        "fail_closed_reason": "",
        "lob_slip_source": str(lob_slip_source),
    }


def _apply_entry_risk_cap_atr_sizing(
    row: Dict[str, Any],
    cfg: Dict[str, Any],
    *,
    code: str,
    qty: int,
    entry_price: float,
    capital_total: float,
    t_enabled: bool,
    t_seasonal_mult: float,
) -> Dict[str, Any]:
    risk_cap_pct = float(cfg.get("risk_cap_per_trade_pct", 0.0) or 0.0)
    if t_enabled and risk_cap_pct > 0.0:
        risk_cap_pct = float(risk_cap_pct) * float(t_seasonal_mult)
    if risk_cap_pct > 0.0 and qty > 0 and entry_price > 0:
        atr14 = float(row.get("atr14_pct") or 0.0)
        if atr14 > 0.0:
            gap_mult = float(cfg.get("gap_risk_multiplier", 2.5) or 2.5)
            gap_worst = min(max(atr14 * gap_mult, 0.06), 0.40)
            cap_total = float(cfg.get("capital_total", 0) or 0)
            risk_budget = cap_total * risk_cap_pct
            if risk_budget > 0:
                risk_qty = int(math.floor(risk_budget / (entry_price * gap_worst)))
                if risk_qty > 0 and risk_qty < qty:
                    print(
                        f"[RISK_CAP_SIZING] code={code} qty {qty}->{risk_qty} "
                        f"atr={atr14:.3f} gap_worst={gap_worst:.3f} "
                        f"risk_budget={risk_budget:.0f}"
                    )
                    qty = risk_qty

    atr14_entry = float(_to_float(row.get("atr14_pct"), 0.0) or 0.0)
    qty, atr_meta = _apply_atr_position_sizing_qty(
        qty=int(qty),
        entry_price=float(entry_price),
        atr14_pct=float(atr14_entry),
        capital_total=float(capital_total),
        cfg=cfg,
        code=code,
    )
    return {"qty": int(qty), "atr_meta": atr_meta}


def _apply_entry_sector_corr_hrp(
    row: Dict[str, Any],
    sector_corr_ctx: Dict[str, Any],
    sector_risk_note_parts: List[str],
    *,
    code: str,
    qty: int,
    row_sector_for_corr: str = "",
    sector_db: Optional[Dict[str, Any]] = None,
    resolve_sector: bool = False,
    allow_block: bool = True,
    allow_surge: bool = True,
    is_surge_immediate: bool = False,
) -> Dict[str, Any]:
    if resolve_sector:
        row_sector_for_corr = str(
            _resolve_sector_label(
                code,
                row.get("sector", ""),
                row.get("sector_name", ""),
                row.get("krx_sector", ""),
                sector_db=sector_db,
            )
        ).strip()

    should_apply = bool(
        qty > 0
        and bool(sector_corr_ctx.get("enabled", False))
        and row_sector_for_corr
        and (allow_surge or (not is_surge_immediate))
    )
    if not should_apply:
        return {
            "qty": int(qty),
            "row_sector_for_corr": str(row_sector_for_corr or ""),
            "blocked": False,
            "reduce_count": 0,
            "reason": "",
        }

    sector_corr_fit = _apply_sector_corr_hrp_qty_adjustment(
        sector_corr_ctx,
        sector_risk_note_parts,
        code=code,
        qty=qty,
        row_sector_for_corr=row_sector_for_corr,
        allow_block=allow_block,
    )
    return {
        "qty": int(sector_corr_fit.get("qty", qty)),
        "row_sector_for_corr": str(row_sector_for_corr or ""),
        "blocked": bool(sector_corr_fit.get("blocked", False)),
        "reduce_count": int(sector_corr_fit.get("reduce_count", 0)),
        "reason": str(sector_corr_fit.get("reason", "") or ""),
        "fit": sector_corr_fit,
    }


def _apply_entry_post_sector_qty_limits(
    row: Dict[str, Any],
    cfg: Dict[str, Any],
    sector_risk_note_parts: List[str],
    *,
    code: str,
    qty: int,
    entry_price: float,
    capital_total: float,
    is_surge_immediate: bool,
    is_split_2nd: bool,
    budget_policy_enabled: bool,
    split_alloc_pct: float,
    split_notional_krw: float,
) -> Dict[str, Any]:
    qty = _apply_surge_news_overheat_qty_reductions(
        row,
        cfg,
        sector_risk_note_parts,
        code=code,
        qty=qty,
        is_surge_immediate=is_surge_immediate,
    )

    if is_split_2nd and budget_policy_enabled and split_alloc_pct > 0.0:
        split_budget_fit = _fit_split_second_budget(
            qty=qty,
            entry_price=entry_price,
            capital_total=capital_total,
            split_alloc_pct=split_alloc_pct,
            split_notional_krw=split_notional_krw,
        )
        if split_budget_fit.get("blocked"):
            if str(split_budget_fit.get("reason", "")) == "budget_left":
                print(
                    f"[SKIP_SPLIT_BUDGET] code={code} split_budget_left={float(split_budget_fit.get('split_budget_left', 0.0)):.0f} "
                    f"split_notional={split_notional_krw:.0f} split_budget={float(split_budget_fit.get('split_total_budget', 0.0)):.0f}"
                )
            else:
                print(
                    f"[SKIP_SPLIT_BUDGET] code={code} qty_from_budget={int(split_budget_fit.get('split_qty_from_budget', 0))} "
                    f"budget_left={float(split_budget_fit.get('split_budget_left', 0.0)):.0f} entry_price={entry_price:.2f}"
                )
            return {
                "qty": int(qty),
                "blocked": True,
                "reason": "SPLIT_BUDGET_BLOCK",
                "split_budget_fit": split_budget_fit,
            }
        if split_budget_fit.get("adjusted"):
            old_qty = int(split_budget_fit.get("old_qty", qty))
            qty = int(split_budget_fit["qty"])
            print(
                f"[BUDGET_SPLIT_CAP] code={code} qty={old_qty}->{qty} "
                f"budget_left={float(split_budget_fit.get('split_budget_left', 0.0)):.0f}"
            )
        else:
            qty = int(split_budget_fit.get("qty", qty))
        return {
            "qty": int(qty),
            "blocked": False,
            "reason": "",
            "split_budget_fit": split_budget_fit,
        }

    return {"qty": int(qty), "blocked": False, "reason": "", "split_budget_fit": {}}


def _apply_entry_ai_cap_min_qty(
    cfg: Dict[str, Any],
    *,
    code: str,
    qty: int,
    entry_price: float,
    fee_pct: float,
    row_slip_pct: float,
    capital_total: float,
    t_enabled: bool,
    t_ai_codes: Set[str],
    t_ai_single_cap_pct: float,
    is_open_order_replay: bool,
    is_surge_immediate: bool,
) -> Dict[str, Any]:
    if (
        t_enabled
        and (not is_open_order_replay)
        and (not is_surge_immediate)
        and t_ai_codes
        and code in t_ai_codes
        and t_ai_single_cap_pct > 0
        and float(capital_total) > 0
        and float(entry_price) > 0
        and qty > 0
    ):
        ai_cap_fit = _fit_ai_single_cap(
            qty=qty,
            entry_price=entry_price,
            capital_total=capital_total,
            t_ai_single_cap_pct=t_ai_single_cap_pct,
        )
        if ai_cap_fit.get("blocked"):
            print(
                f"[SKIP_AI_SINGLE_CAP] code={code} cap_notional={float(ai_cap_fit.get('cap_notional', 0.0)):.0f} "
                f"entry_price={entry_price:.2f}"
            )
            return {
                "qty": int(qty),
                "blocked": True,
                "reason": "AI_FOCUS_SINGLE_CAP_BLOCK",
                "ai_cap_fit": ai_cap_fit,
            }
        if ai_cap_fit.get("adjusted"):
            old_qty = int(ai_cap_fit.get("old_qty", qty))
            qty = int(ai_cap_fit["qty"])
            print(
                f"[AI_SINGLE_CAP] code={code} qty={old_qty}->{qty} "
                f"cap_notional={float(ai_cap_fit.get('cap_notional', 0.0)):.0f}"
            )

    entry_notional = float(entry_price) * float(qty)
    entry_cost_buffer = 1.0 + max(0.0, float(fee_pct)) + max(0.0, float(row_slip_pct))
    entry_notional_for_cap = entry_notional * entry_cost_buffer
    min_qty_fit = _apply_min_qty_verification_limit(
        cfg,
        qty=qty,
        entry_price=entry_price,
        entry_cost_buffer=entry_cost_buffer,
        is_open_order_replay=is_open_order_replay,
    )
    min_qty_runtime = int(min_qty_fit["min_qty_runtime"])
    if min_qty_fit.get("adjusted"):
        old_qty = int(min_qty_fit.get("old_qty", qty))
        qty = int(min_qty_fit["qty"])
        entry_notional = float(min_qty_fit["entry_notional"])
        entry_notional_for_cap = float(min_qty_fit["entry_notional_for_cap"])
        print(f"[MIN_QTY_VERIFY] code={code} qty={old_qty}->{qty} session={PAPER_SESSION_ID}")

    return {
        "qty": int(qty),
        "blocked": False,
        "reason": "",
        "entry_notional": float(entry_notional),
        "entry_cost_buffer": float(entry_cost_buffer),
        "entry_notional_for_cap": float(entry_notional_for_cap),
        "min_qty_runtime": int(min_qty_runtime),
        "min_qty_fit": min_qty_fit,
    }


def _apply_entry_cap_limits(
    cfg: Dict[str, Any],
    *,
    code: str,
    qty: int,
    entry_price: float,
    entry_cost_buffer: float,
    entry_notional: float,
    entry_notional_for_cap: float,
    min_qty_runtime: int,
    is_open_order_replay: bool,
    is_surge_immediate: bool,
    gross_cap_krw: Optional[float],
    daily_new_cap_krw: Optional[float],
    current_open_notional: float,
    new_notional_krw: float,
) -> Dict[str, Any]:
    if is_open_order_replay:
        return {
            "qty": int(qty),
            "entry_notional": float(entry_notional),
            "entry_notional_for_cap": float(entry_notional_for_cap),
            "blocked": False,
            "cap_block_delta": 0,
            "reason": "",
        }

    cap_fit = _fit_entry_qty_to_caps(
        qty=qty,
        entry_price=entry_price,
        entry_cost_buffer=entry_cost_buffer,
        entry_notional_for_cap=entry_notional_for_cap,
        min_qty_runtime=min_qty_runtime,
        cfg=cfg,
        is_surge_immediate=is_surge_immediate,
        gross_cap_krw=gross_cap_krw,
        daily_new_cap_krw=daily_new_cap_krw,
        current_open_notional=current_open_notional,
        new_notional_krw=new_notional_krw,
    )
    if cap_fit.get("adjusted"):
        old_qty = int(cap_fit.get("old_qty", qty))
        qty = int(cap_fit["qty"])
        entry_notional = float(cap_fit["entry_notional"])
        entry_notional_for_cap = float(cap_fit["entry_notional_for_cap"])
        print(
            f"[ADJUST_QTY_CAP] code={code} reason={cap_fit.get('allowed_reason', '')} "
            f"qty={old_qty}->{qty} entry_notional={entry_notional:.0f} "
            f"cap_notional={entry_notional_for_cap:.0f} cap_left={float(cap_fit.get('allowed_notional', 0.0)):.0f}"
        )
    elif cap_fit.get("blocked"):
        allowed_reason = str(cap_fit.get("allowed_reason", "") or "")
        if allowed_reason == "gross_cap":
            post_gross = current_open_notional + new_notional_krw + entry_notional_for_cap
            print(f"[SKIP_GROSS_CAP] code={code} post_gross={post_gross:.0f} > gross_cap={gross_cap_krw:.0f}")
        elif allowed_reason == "daily_cap":
            post_new = new_notional_krw + entry_notional_for_cap
            print(f"[SKIP_DAILY_CAP] code={code} post_new={post_new:.0f} > daily_new_cap={daily_new_cap_krw:.0f}")
        else:
            print(
                f"[SKIP_CAP] code={code} cap_notional={entry_notional_for_cap:.0f} "
                f"cap_left={float(cap_fit.get('allowed_notional', 0.0)):.0f}"
            )
        return {
            "qty": int(qty),
            "entry_notional": float(entry_notional),
            "entry_notional_for_cap": float(entry_notional_for_cap),
            "blocked": True,
            "cap_block_delta": 1,
            "reason": str(allowed_reason or "cap_fit"),
            "cap_fit": cap_fit,
        }

    cap_block_reason, cap_post_value = _post_entry_cap_block_reason(
        entry_notional_for_cap=entry_notional_for_cap,
        gross_cap_krw=gross_cap_krw,
        daily_new_cap_krw=daily_new_cap_krw,
        current_open_notional=current_open_notional,
        new_notional_krw=new_notional_krw,
    )
    if cap_block_reason == "gross_cap":
        print(f"[SKIP_GROSS_CAP] code={code} post_gross={cap_post_value:.0f} > gross_cap={gross_cap_krw:.0f}")
        return {
            "qty": int(qty),
            "entry_notional": float(entry_notional),
            "entry_notional_for_cap": float(entry_notional_for_cap),
            "blocked": True,
            "cap_block_delta": 1,
            "reason": "gross_cap",
            "cap_fit": cap_fit,
        }
    if cap_block_reason == "daily_cap":
        print(f"[SKIP_DAILY_CAP] code={code} post_new={cap_post_value:.0f} > daily_new_cap={daily_new_cap_krw:.0f}")
        return {
            "qty": int(qty),
            "entry_notional": float(entry_notional),
            "entry_notional_for_cap": float(entry_notional_for_cap),
            "blocked": True,
            "cap_block_delta": 1,
            "reason": "daily_cap",
            "cap_fit": cap_fit,
        }

    return {
        "qty": int(qty),
        "entry_notional": float(entry_notional),
        "entry_notional_for_cap": float(entry_notional_for_cap),
        "blocked": False,
        "cap_block_delta": 0,
        "reason": "",
        "cap_fit": cap_fit,
    }


def _apply_entry_normal_exec_quality(
    row: Dict[str, Any],
    cfg: Dict[str, Any],
    sector_risk_note_parts: List[str],
    *,
    code: str,
    qty: int,
    entry_price: float,
    min_qty_runtime: int,
    is_open_order_replay: bool,
    is_surge_immediate: bool,
    is_split_2nd: bool,
) -> Dict[str, Any]:
    if not (qty > 0 and (not is_open_order_replay) and (not is_surge_immediate) and (not is_split_2nd)):
        return {"qty": int(qty), "blocked": False, "reason": ""}

    normal_exec_ok, normal_exec_reason, qty = _apply_normal_exec_quality(
        row,
        cfg,
        sector_risk_note_parts,
        code=code,
        qty=int(qty),
        entry_price=float(entry_price),
        min_qty_runtime=int(min_qty_runtime),
    )
    if normal_exec_ok:
        return {"qty": int(qty), "blocked": False, "reason": str(normal_exec_reason or "")}

    reason = str(normal_exec_reason)
    if reason != "NORMAL_EXEC_QUALITY_REDUCE_ZERO_QTY":
        print(
            f"[NORMAL_EXEC_QUALITY_BLOCK] code={code} reason={normal_exec_reason} "
            f"spread_bps={row.get('normal_spread_bps', '')} "
            f"exec_qty={row.get('normal_executable_qty', '')}/{int(qty)} "
            f"markout_1step_bps={row.get('normal_markout_1step_bps', '')}"
        )
    return {"qty": int(qty), "blocked": True, "reason": reason}


def _apply_entry_normal_lob_fill_price_gate(
    row: Dict[str, Any],
    sector_risk_note_parts: List[str],
    *,
    code: str,
    qty: int,
    entry_price: float,
    entry_cost_buffer: float,
    entry_notional: float,
    entry_notional_for_cap: float,
    is_open_order_replay: bool,
    is_surge_immediate: bool,
    is_split_2nd: bool,
    gross_cap_krw: Optional[float],
    daily_new_cap_krw: Optional[float],
    current_open_notional: float,
    new_notional_krw: float,
) -> Dict[str, Any]:
    if not (qty > 0 and (not is_open_order_replay) and (not is_surge_immediate) and (not is_split_2nd)):
        return {
            "entry_price": float(entry_price),
            "entry_notional": float(entry_notional),
            "entry_notional_for_cap": float(entry_notional_for_cap),
            "blocked": False,
            "cap_block_delta": 0,
            "reason": "",
            "decision_reason": "",
        }

    normal_lob_ok, entry_price, entry_notional, entry_notional_for_cap, normal_close_basis_entry_price = _apply_normal_lob_fill_price(
        row,
        sector_risk_note_parts,
        code=code,
        entry_price=float(entry_price),
        qty=int(qty),
        entry_cost_buffer=float(entry_cost_buffer),
    )
    if not normal_lob_ok:
        print(f"[NORMAL_LOB_FILL_PRICE_MISSING] code={code} normal_executable_price={row.get('normal_executable_price', '')}")
        return {
            "entry_price": float(entry_price),
            "entry_notional": float(entry_notional),
            "entry_notional_for_cap": float(entry_notional_for_cap),
            "blocked": True,
            "cap_block_delta": 0,
            "reason": "NORMAL_LOB_FILL_PRICE_MISSING",
            "decision_reason": "NORMAL_LOB_FILL_PRICE_MISSING",
        }
    normal_close_basis_entry_price = (
        float(normal_close_basis_entry_price)
        if normal_close_basis_entry_price is not None
        else float(entry_price)
    )

    print(
        f"[NORMAL_LOB_FILL_PRICE_APPLIED] code={code} "
        f"entry_price={float(normal_close_basis_entry_price):.4f}->{float(entry_price):.4f} "
        f"qty={int(qty)}"
    )
    if gross_cap_krw is not None:
        post_gross = float(current_open_notional) + float(new_notional_krw) + float(entry_notional_for_cap)
        if post_gross > float(gross_cap_krw) + 1e-9:
            print(f"[SKIP_GROSS_CAP] code={code} post_gross={post_gross:.0f} > gross_cap={float(gross_cap_krw):.0f} source=NORMAL_LOB_FILL_PRICE")
            return {
                "entry_price": float(entry_price),
                "entry_notional": float(entry_notional),
                "entry_notional_for_cap": float(entry_notional_for_cap),
                "blocked": True,
                "cap_block_delta": 1,
                "reason": "GROSS_CAP_AFTER_NORMAL_LOB_FILL_PRICE",
                "decision_reason": "",
            }
    if daily_new_cap_krw is not None:
        post_new = float(new_notional_krw) + float(entry_notional_for_cap)
        if post_new > float(daily_new_cap_krw) + 1e-9:
            print(f"[SKIP_DAILY_CAP] code={code} post_new={post_new:.0f} > daily_new_cap={float(daily_new_cap_krw):.0f} source=NORMAL_LOB_FILL_PRICE")
            return {
                "entry_price": float(entry_price),
                "entry_notional": float(entry_notional),
                "entry_notional_for_cap": float(entry_notional_for_cap),
                "blocked": True,
                "cap_block_delta": 1,
                "reason": "DAILY_CAP_AFTER_NORMAL_LOB_FILL_PRICE",
                "decision_reason": "",
            }

    return {
        "entry_price": float(entry_price),
        "entry_notional": float(entry_notional),
        "entry_notional_for_cap": float(entry_notional_for_cap),
        "blocked": False,
        "cap_block_delta": 0,
        "reason": "",
        "decision_reason": "",
    }


def _apply_entry_t2_buy_budget_gate(
    portfolio_state: Dict[str, Any],
    cfg: Dict[str, Any],
    t2_cash_checks: List[Any],
    *,
    code: str,
    entry_day: str,
    entry_price: float,
    qty: int,
    entry_cost_buffer: float,
    min_qty_runtime: int,
    entry_notional: float,
    entry_notional_for_cap: float,
    is_open_order_replay: bool,
) -> Dict[str, Any]:
    if is_open_order_replay:
        return {
            "qty": int(qty),
            "entry_notional": float(entry_notional),
            "entry_notional_for_cap": float(entry_notional_for_cap),
            "blocked": False,
            "cap_block_delta": 0,
            "reason": "",
            "decision_reason": "",
        }

    qty_after_t2, t2_check, entry_notional, entry_notional_for_cap, t2_reduced, old_qty_t2 = _apply_entry_t2_buy_budget(
        portfolio_state,
        cfg,
        code=code,
        entry_day=entry_day,
        entry_price=float(entry_price),
        qty=int(qty),
        entry_cost_buffer=float(entry_cost_buffer),
        min_qty_runtime=int(min_qty_runtime),
    )
    t2_cash_checks.append(t2_check)
    if int(qty_after_t2) <= 0:
        print(
            f"[SKIP_T2_SETTLED_CASH] code={code} requested_qty={int(qty)} "
            f"settled_cash={float(t2_check.get('settled_cash_before', 0.0)):.0f}"
        )
        return {
            "qty": int(qty),
            "entry_notional": float(entry_notional),
            "entry_notional_for_cap": float(entry_notional_for_cap),
            "blocked": True,
            "cap_block_delta": 1,
            "reason": "T2_SETTLED_CASH_BLOCK",
            "decision_reason": "T2_SETTLED_CASH_BLOCK",
        }
    if t2_reduced:
        qty = int(qty_after_t2)
        print(
            f"[T2_SETTLED_CASH_REDUCE] code={code} qty={old_qty_t2}->{qty} "
            f"settled_cash={float(t2_check.get('settled_cash_before', 0.0)):.0f}"
        )

    return {
        "qty": int(qty),
        "entry_notional": float(entry_notional),
        "entry_notional_for_cap": float(entry_notional_for_cap),
        "blocked": False,
        "cap_block_delta": 0,
        "reason": "",
        "decision_reason": "",
    }


def _prepare_entry_identity_context(
    row: Dict[str, Any],
    *,
    code: str,
    effective_signal_date: str,
    today_ymd: str,
    entry_day: str,
    qty: int,
    is_split_2nd: bool,
    is_open_order_replay: bool,
    replay_row_ok: bool,
    replay_order_id_include_entry_day: bool,
    allow_same_signal_reentry: bool,
    daily_idem_keys: Set[str],
    existing_fill_order_ids: Set[str],
    is_surge_immediate: bool,
    use_intraday_realtime_entry: bool,
    use_same_close_today: bool,
    force_next_open_entry: bool,
) -> Dict[str, Any]:
    if not is_open_order_replay:
        idem_duplicate, idem_key = _register_entry_idempotency_key(
            row,
            code=code,
            effective_signal_date=effective_signal_date,
            today_ymd=today_ymd,
            is_split_2nd=is_split_2nd,
            daily_idem_keys=daily_idem_keys,
        )
        if idem_duplicate:
            print(f"[IDEM_DUPLICATE] code={code} idem_key={idem_key}")
            return {
                "blocked": True,
                "reason": "IDEM_DUPLICATE",
                "decision_reason": "IDEM_DUPLICATE",
                "idempotent_skip_delta": 1,
                "idem_key": str(idem_key),
                "order_id": "",
                "lineage": {},
                "entry_source_kind": "",
                "candidate_snapshot_id": "",
            }

    order_id = _build_entry_order_id(
        code=code,
        effective_signal_date=effective_signal_date,
        entry_day=entry_day,
        row=row,
        allow_same_signal_reentry=allow_same_signal_reentry,
        is_open_order_replay=is_open_order_replay,
        replay_row_ok=replay_row_ok,
        replay_order_id_include_entry_day=replay_order_id_include_entry_day,
        existing_fill_order_ids=existing_fill_order_ids,
    )
    lineage = _lineage_from_row(row, code, effective_signal_date, entry_day, order_id, int(qty))
    entry_source_kind = _entry_source_kind(
        is_surge_immediate=is_surge_immediate,
        is_open_order_replay=is_open_order_replay,
        use_intraday_realtime_entry=use_intraday_realtime_entry,
        use_same_close_today=use_same_close_today,
        force_next_open_entry=force_next_open_entry,
    )
    candidate_snapshot_id = _candidate_snapshot_id(row, code, effective_signal_date, entry_day, order_id, int(qty))
    return {
        "blocked": False,
        "reason": "",
        "decision_reason": "",
        "idempotent_skip_delta": 0,
        "idem_key": "",
        "order_id": str(order_id),
        "lineage": lineage,
        "entry_source_kind": str(entry_source_kind),
        "candidate_snapshot_id": str(candidate_snapshot_id),
    }


def _build_entry_note_context(
    row: Dict[str, Any],
    cfg: Dict[str, Any],
    sector_risk_note_parts: List[str],
    lineage: Dict[str, Any],
    *,
    effective_signal_date: str,
    signal_date: str,
    qty: int,
    entry_day: str,
    entry_source_kind: str,
    candidate_snapshot_id: str,
    is_surge_immediate: bool,
    has_surge_type_policy: bool,
    is_open_order_replay: bool,
    replay_row_ok: bool,
    use_intraday_realtime_entry: bool,
    use_same_close_today: bool,
    force_next_open_entry: bool,
    fallback_note: str,
    is_split_2nd: bool,
) -> Dict[str, Any]:
    note_parts, horizon_label = _build_entry_note_seed(
        row,
        cfg,
        effective_signal_date=effective_signal_date,
        qty=int(qty),
        entry_source_kind=entry_source_kind,
        candidate_snapshot_id=candidate_snapshot_id,
        is_surge_immediate=is_surge_immediate,
        has_surge_type_policy=has_surge_type_policy,
    )
    note_parts.extend(_build_replay_entry_note_parts(
        row,
        is_open_order_replay=is_open_order_replay,
        replay_row_ok=replay_row_ok,
        entry_day=entry_day,
        signal_date=signal_date,
    ))
    note_parts.extend(_build_entry_timing_note_parts(
        use_same_close_today=use_same_close_today,
        use_intraday_realtime_entry=use_intraday_realtime_entry,
        force_next_open_entry=force_next_open_entry,
    ))
    if fallback_note:
        note_parts.append(fallback_note)
    note_parts.extend(_build_split_entry_note_parts(
        row,
        cfg,
        is_split_2nd=is_split_2nd,
        is_open_order_replay=is_open_order_replay,
    ))
    note_parts.extend(sector_risk_note_parts)
    note_parts.extend(_build_lineage_session_note_parts(lineage))
    return {
        "note_parts": note_parts,
        "note_text": ";".join(note_parts),
        "horizon_label": horizon_label,
    }


def _handle_existing_fill_idempotent_buy(
    existing_fill_order_ids: Set[str],
    open_pos: List[Dict[str, Any]],
    processed_signals: Set[str],
    record_decision: Callable[..., None],
    row: Dict[str, Any],
    cfg: Dict[str, Any],
    *,
    code: str,
    signal_date: str,
    effective_signal_date: str,
    entry_day: str,
    allow_same_signal_reentry: bool,
    is_open_order_replay: bool,
    is_surge_immediate: bool,
    is_carryover_row: bool,
    horizon_label: str,
    sig_key: str,
    entry_price: float,
    qty: int,
    order_id: str,
) -> Dict[str, Any]:
    if order_id not in existing_fill_order_ids:
        return {"blocked": False, "idempotent_skip_delta": 0}

    print(
        f"[SKIP_IDEMPOTENT_BUY] code={code} order_id={order_id} "
        f"signal_date={signal_date} effective_signal_date={effective_signal_date} "
        f"entry_day={entry_day} allow_same_reentry={allow_same_signal_reentry} "
        f"is_open_order_replay={is_open_order_replay}"
    )
    _backfill_idempotent_buy_horizon(
        open_pos,
        row,
        cfg,
        order_id=order_id,
        horizon_label=horizon_label,
    )
    processed_signals.add(sig_key)
    record_decision(
        code=code, signal_date=effective_signal_date, signal="HOLD", reason="IDEMPOTENT_BUY",
        row=row, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row,
        entry_day=entry_day, entry_price=entry_price, qty=qty, order_id=order_id
    )
    return {"blocked": True, "idempotent_skip_delta": 1}


def _resolve_split2_target_position_gate(
    open_pos: List[Dict[str, Any]],
    record_decision: Callable[..., None],
    row: Dict[str, Any],
    *,
    is_split_2nd: bool,
    code: str,
    effective_signal_date: str,
    is_surge_immediate: bool,
    is_open_order_replay: bool,
    is_carryover_row: bool,
) -> Dict[str, Any]:
    if not is_split_2nd:
        return {"blocked": False, "target_position": None, "reason": ""}

    split_first_order_id = str(row.get("split_first_order_id", "") or "").strip()
    target_position = _find_split2_target_position(
        open_pos,
        code=code,
        split_first_order_id=split_first_order_id,
    )
    if target_position is not None:
        return {"blocked": False, "target_position": target_position, "reason": ""}

    print(f"[SKIP_SPLIT2ND_NO_POSITION] code={code} signal_date={effective_signal_date} first_order_id={split_first_order_id}")
    record_decision(
        code=code, signal_date=effective_signal_date, signal="HOLD", reason="SPLIT2ND_NO_OPEN_POSITION",
        row=row, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
    )
    return {"blocked": True, "target_position": None, "reason": "SPLIT2ND_NO_OPEN_POSITION"}


def _resolve_entry_take_profit_pct(
    row: Dict[str, Any],
    cfg: Dict[str, Any],
    take_profit: Any,
    *,
    is_surge_immediate: bool,
    has_surge_type_policy: bool,
) -> float:
    resolved = _to_float(take_profit, None)
    if resolved is not None:
        return float(resolved)

    surge_like = bool(is_surge_immediate or has_surge_type_policy)
    if surge_like:
        for key in ("surge_type_tp_pct", "exit_take_profit_pct"):
            resolved = _to_float(row.get(key), None)
            if resolved is not None:
                return float(resolved)
        surge_exit_policy = cfg.get("surge_exit_policy") if isinstance(cfg, dict) else {}
        if isinstance(surge_exit_policy, dict):
            resolved = _to_float(surge_exit_policy.get("take_profit_pct"), None)
            if resolved is not None:
                return float(resolved)

    resolved = _to_float(cfg.get("take_profit_pct"), None) if isinstance(cfg, dict) else None
    if resolved is not None:
        return float(resolved)

    raise ValueError("take_profit_pct_missing")


def _commit_entry_fill_and_position(
    *,
    row: Dict[str, Any],
    cfg: Dict[str, Any],
    px: pd.DataFrame,
    schema: str,
    fills_new: List[Any],
    existing_fill_order_ids: Set[str],
    portfolio_state: Dict[str, Any],
    open_pos: List[Dict[str, Any]],
    open_codes: Set[str],
    processed_signals: Set[str],
    pending_carry_rows: List[Dict[str, Any]],
    surge_type_day_counts: Dict[str, int],
    same_code_day_buy_counts: Dict[str, int],
    record_decision: Callable[..., None],
    cap_basic_qty_fn: Callable[[int, float, str], int],
    split2_target_position: Optional[Dict[str, Any]],
    code: str,
    name: str,
    effective_signal_date: str,
    entry_day: str,
    entry_ts_value: str,
    row_entry_timing: str,
    entry_price: float,
    entry_notional: float,
    entry_notional_for_cap: float,
    qty: int,
    order_id: str,
    note_text: str,
    fee_pct: float,
    row_slip_pct: float,
    take_profit: float,
    stop_loss: float,
    trail_pct: float,
    row_market_cap: Any,
    horizon_label: str,
    lineage: Dict[str, Any],
    sector_db: Dict[str, Any],
    fundamentals_db: Dict[str, Any],
    entry_source_kind: str,
    candidate_snapshot_id: str,
    fallback_note: str,
    is_surge_immediate: bool,
    has_surge_type_policy: bool,
    is_open_order_replay: bool,
    is_carryover_row: bool,
    is_split_2nd: bool,
    sig_key: str,
    replay_key: str,
    ops_policy: Dict[str, Any],
    ops_enabled: bool,
    strict_same_day: bool,
    surge_type_text: str,
    current_surge_notional_krw: float,
) -> Dict[str, Any]:
    fills_new.append(_build_entry_fill_row(
        schema=schema,
        entry_ts_value=entry_ts_value,
        entry_day=entry_day,
        code=code,
        name=name,
        qty=int(qty),
        entry_price=float(entry_price),
        order_id=order_id,
        note_text=note_text,
        fee_pct=float(fee_pct),
        row_slip_pct=float(row_slip_pct),
    ))
    existing_fill_order_ids.add(order_id)
    _archive_entry_source_payload(
        row=row,
        cfg=cfg,
        entry_source_kind=entry_source_kind,
        candidate_snapshot_id=candidate_snapshot_id,
        code=code,
        name=name,
        effective_signal_date=effective_signal_date,
        entry_day=entry_day,
        entry_ts_value=entry_ts_value,
        lineage=lineage,
        qty=int(qty),
        entry_price=float(entry_price),
        horizon_label=horizon_label,
        row_entry_timing=row_entry_timing,
        fallback_note=fallback_note,
        is_surge_immediate=is_surge_immediate,
        has_surge_type_policy=has_surge_type_policy,
    )
    _record_entry_t2_cash_if_needed(
        portfolio_state,
        cfg,
        is_open_order_replay=is_open_order_replay,
        code=code,
        entry_day=entry_day,
        order_id=order_id,
        entry_notional_for_cap=float(entry_notional_for_cap),
    )
    _record_buy_executed_decision(
        record_decision,
        code=code,
        effective_signal_date=effective_signal_date,
        row=row,
        is_surge_immediate=is_surge_immediate,
        is_open_order_replay=is_open_order_replay,
        is_carryover_row=is_carryover_row,
        entry_day=entry_day,
        entry_price=float(entry_price),
        qty=int(qty),
        order_id=order_id,
    )

    base_position = _build_entry_base_position(
        row=row,
        cfg=cfg,
        px=px,
        code=code,
        name=name,
        qty=int(qty),
        effective_signal_date=effective_signal_date,
        entry_day=entry_day,
        entry_ts_value=entry_ts_value,
        row_entry_timing=row_entry_timing,
        entry_price=float(entry_price),
        take_profit=_resolve_entry_take_profit_pct(
            row,
            cfg,
            take_profit,
            is_surge_immediate=is_surge_immediate,
            has_surge_type_policy=has_surge_type_policy,
        ),
        stop_loss=float(stop_loss),
        trail_pct=float(trail_pct),
        row_slip_pct=float(row_slip_pct),
        row_market_cap=row_market_cap,
        horizon_label=horizon_label,
        lineage=lineage,
        sector_db=sector_db,
        fundamentals_db=fundamentals_db,
        is_surge_immediate=is_surge_immediate,
        has_surge_type_policy=has_surge_type_policy,
    )

    result = {
        "entry_ready_delta": 1,
        "new_count_delta": 0,
        "new_notional_delta": 0.0,
        "surge_new_count_delta": 0,
        "surge_notional_delta": 0.0,
        "split_notional_delta": 0.0,
        "open_order_replay_used_delta": 0,
        "partial_fill_expired_delta": 0,
    }

    if is_split_2nd:
        assert split2_target_position is not None
        _merge_split_second_entry_position(
            split2_target_position,
            row,
            qty=int(qty),
            entry_price=float(entry_price),
            entry_day=entry_day,
            order_id=order_id,
            lineage=lineage,
        )
        processed_signals.add(sig_key)
        result["new_notional_delta"] = float(entry_notional)
        result["split_notional_delta"] = float(entry_notional)
        if is_surge_immediate:
            result["surge_notional_delta"] = float(entry_notional)
        return result

    if not base_position["asset_type"]:
        base_position["asset_type"] = _classify_asset_type(base_position)
    open_pos.append(base_position)
    open_codes.add(code)
    processed_signals.add(replay_key if is_open_order_replay and replay_key else sig_key)
    if is_open_order_replay:
        result["open_order_replay_used_delta"] = 1
    else:
        result["new_notional_delta"] = float(entry_notional)
        if is_surge_immediate:
            result["surge_notional_delta"] = float(entry_notional)
            result["surge_new_count_delta"] = 1
            if surge_type_text:
                surge_type_day_counts[surge_type_text] = int(surge_type_day_counts.get(surge_type_text, 0)) + 1
            same_code_day_buy_counts[code] = int(same_code_day_buy_counts.get(code, 0) or 0) + 1
            print(
                f"[SURGE_BUY] code={code} qty={int(qty)} entry={float(entry_price):.2f} "
                f"entry_notional={float(entry_notional):.0f} surge_notional={float(current_surge_notional_krw) + float(result['surge_notional_delta']):.0f} "
                f"surge_type={surge_type_text} type_count={surge_type_day_counts.get(surge_type_text, 0)}"
            )
        else:
            result["new_count_delta"] = 1
            same_code_day_buy_counts[code] = int(same_code_day_buy_counts.get(code, 0) or 0) + 1

    split_cfg = cfg.get("split_entry", {}) if isinstance(cfg, dict) else {}
    partial_same_day_only = bool(ops_policy.get("partial_fill_same_day_only", True)) if isinstance(ops_policy, dict) else True
    if split_cfg.get("enabled") and ops_enabled and not is_open_order_replay:
        full_qty = calc_qty(float(entry_price), cfg, float(fee_pct), float(row_slip_pct))
        if not is_surge_immediate:
            full_qty = cap_basic_qty_fn(int(full_qty), float(entry_price), code)
        remaining = max(0, int(full_qty) - int(qty))
        if _is_split_second_carry_expired(
            remaining_qty=remaining,
            strict_same_day=strict_same_day,
            partial_same_day_only=partial_same_day_only,
            split_cfg=split_cfg,
        ):
            result["partial_fill_expired_delta"] = 1
            print(f"[PARTIAL_FILL_EXPIRED] code={code} remaining_qty={remaining} reason=STRICT_SAME_DAY_ONLY")
        elif remaining > 0:
            pending_carry_rows.append(_build_split_second_carry_row(
                row,
                split_cfg,
                effective_signal_date=effective_signal_date,
                code=code,
                name=name,
                remaining_qty=remaining,
                entry_price=float(entry_price),
                order_id=order_id,
                qty=int(qty),
                sig_key=sig_key,
            ))
            print(f"[SPLIT_ENTRY_1ST] code={code} qty={int(qty)} remaining={remaining} first_price={float(entry_price)}")

    return result


def _resolve_sector_label(code: str, *values: Any, sector_db: Optional[Dict[str, Any]] = None) -> str:
    for value in values:
        sector = _clean_sector_label(value)
        if sector:
            return sector
    if isinstance(sector_db, dict):
        return _clean_sector_label(sector_db.get(norm_code(code), ""))
    return ""
def _apply_atr_position_sizing_qty(
    *,
    qty: int,
    entry_price: float,
    atr14_pct: float,
    capital_total: float,
    cfg: Dict[str, Any],
    code: str,
) -> Tuple[int, Dict[str, Any]]:
    out_meta: Dict[str, Any] = {"enabled": False, "applied": False}
    if qty <= 0 or entry_price <= 0:
        return int(qty), out_meta
    atr_cfg = cfg.get("atr_position_sizing", {}) if isinstance(cfg.get("atr_position_sizing"), dict) else {}
    enabled = bool(atr_cfg.get("enabled", True))
    out_meta["enabled"] = bool(enabled)
    if not enabled:
        return int(qty), out_meta
    atr_val = float(atr14_pct or 0.0)
    if (not math.isfinite(atr_val)) or atr_val <= 0:
        out_meta["reason"] = "atr_missing_or_nonpositive"
        return int(qty), out_meta

    risk_per_trade_pct = _pct01_from_config(atr_cfg.get("risk_per_trade_pct"), 0.004)
    atr_mult = float(_to_float(atr_cfg.get("atr_multiplier"), 1.5))
    min_dist = _pct01_from_config(atr_cfg.get("min_stop_distance_pct"), 0.02)
    max_dist = _pct01_from_config(atr_cfg.get("max_stop_distance_pct"), 0.25)
    risk_budget = float(capital_total or 0.0) * float(risk_per_trade_pct)
    if (not math.isfinite(risk_budget)) or risk_budget <= 0:
        out_meta["reason"] = "risk_budget_nonpositive"
        return int(qty), out_meta

    stop_dist = min(max(atr_val * atr_mult, min_dist), max_dist)
    if not math.isfinite(stop_dist):
        out_meta["reason"] = "stop_distance_nonfinite"
        return int(qty), out_meta
    per_share_risk = float(entry_price) * float(stop_dist)
    if (not math.isfinite(per_share_risk)) or per_share_risk <= 0:
        out_meta["reason"] = "per_share_risk_nonpositive"
        return int(qty), out_meta

    atr_qty = int(math.floor(risk_budget / per_share_risk))
    out_meta.update(
        {
            "risk_per_trade_pct": float(risk_per_trade_pct),
            "atr_multiplier": float(atr_mult),
            "atr14_pct": float(atr_val),
            "stop_distance_pct": float(stop_dist),
            "risk_budget": float(risk_budget),
            "atr_qty": int(atr_qty),
        }
    )
    if atr_qty > 0 and atr_qty < qty:
        print(
            f"[ATR_POSITION_SIZING] code={code} qty={qty}->{atr_qty} "
            f"atr={atr_val:.4f} stop_dist={stop_dist:.4f} risk_budget={risk_budget:.0f}"
        )
        out_meta["applied"] = True
        return int(atr_qty), out_meta
    out_meta["applied"] = False
    return int(qty), out_meta
def _ewma_covariance_matrix(ret_df: pd.DataFrame, halflife_days: int) -> pd.DataFrame:
    if not isinstance(ret_df, pd.DataFrame) or ret_df.empty:
        return pd.DataFrame()
    x = ret_df.apply(pd.to_numeric, errors="coerce").dropna(axis=1, how="all").fillna(0.0)
    if x.shape[0] < 5 or x.shape[1] < 2:
        return pd.DataFrame()
    half = max(2.0, float(halflife_days or 60))
    decay = math.exp(math.log(0.5) / half)
    weights = [decay ** i for i in range(len(x) - 1, -1, -1)]
    w = pd.Series(weights, index=x.index, dtype="float64")
    w = w / float(w.sum())
    mean = x.mul(w, axis=0).sum(axis=0)
    centered = x - mean
    cov = centered.mul(w, axis=0).T.dot(centered)
    return cov.fillna(0.0)
def _hrp_cluster_order(corr: pd.DataFrame) -> List[str]:
    labels = [str(c) for c in corr.columns]
    if len(labels) <= 2:
        return labels
    c = corr.reindex(index=labels, columns=labels).fillna(0.0).clip(-1.0, 1.0)
    dist: Dict[tuple[str, str], float] = {}
    for i, a in enumerate(labels):
        for b in labels[i + 1:]:
            dist[(a, b)] = math.sqrt(max(0.0, 0.5 * (1.0 - float(c.loc[a, b]))))

    clusters: List[Any] = list(labels)

    def _leaves(node: Any) -> List[str]:
        if isinstance(node, str):
            return [node]
        out: List[str] = []
        for child in node:
            out.extend(_leaves(child))
        return out

    def _cluster_dist(left: Any, right: Any) -> float:
        vals: List[float] = []
        for a in _leaves(left):
            for b in _leaves(right):
                if a == b:
                    continue
                key = (a, b) if (a, b) in dist else (b, a)
                vals.append(float(dist.get(key, 1.0)))
        return min(vals) if vals else 1.0

    while len(clusters) > 1:
        best_i, best_j, best_d = 0, 1, float("inf")
        for i in range(len(clusters)):
            for j in range(i + 1, len(clusters)):
                d = _cluster_dist(clusters[i], clusters[j])
                if d < best_d:
                    best_i, best_j, best_d = i, j, d
        merged = (clusters[best_i], clusters[best_j])
        clusters = [c0 for k, c0 in enumerate(clusters) if k not in {best_i, best_j}]
        clusters.append(merged)
    return _leaves(clusters[0])
def _hrp_weights_from_cov(cov: pd.DataFrame) -> Dict[str, float]:
    if not isinstance(cov, pd.DataFrame) or cov.empty or cov.shape[1] < 2:
        return {}
    labels = [str(c) for c in cov.columns]
    cov = cov.reindex(index=labels, columns=labels).fillna(0.0)
    diag = pd.Series({s: max(float(cov.loc[s, s]), 1e-12) for s in labels})
    denom = diag.pow(0.5)
    corr = cov.div(denom, axis=0).div(denom, axis=1).fillna(0.0).clip(-1.0, 1.0)
    order = _hrp_cluster_order(corr)
    weights = pd.Series(1.0, index=order, dtype="float64")

    def _cluster_var(items: List[str]) -> float:
        sub = cov.reindex(index=items, columns=items).fillna(0.0)
        inv_diag = pd.Series({s: 1.0 / max(float(sub.loc[s, s]), 1e-12) for s in items})
        ivp = inv_diag / float(inv_diag.sum())
        return float(ivp.T.dot(sub).dot(ivp))

    clusters = [order]
    while clusters:
        next_clusters: List[List[str]] = []
        for cluster in clusters:
            if len(cluster) <= 1:
                continue
            split = len(cluster) // 2
            left = cluster[:split]
            right = cluster[split:]
            left_var = _cluster_var(left)
            right_var = _cluster_var(right)
            alpha = 0.5
            if (left_var + right_var) > 0:
                alpha = 1.0 - left_var / (left_var + right_var)
            weights.loc[left] *= alpha
            weights.loc[right] *= (1.0 - alpha)
            next_clusters.extend([left, right])
        clusters = next_clusters
    total = float(weights.sum())
    if total <= 0:
        return {}
    return {str(k): float(v / total) for k, v in weights.items()}
def _build_sector_correlation_guard(
    *,
    open_pos: List[Dict[str, Any]],
    candidate_df: pd.DataFrame,
    prices_df: pd.DataFrame,
    sector_db: Dict[str, Any],
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    gcfg = cfg.get("sector_correlation_guard", {}) if isinstance(cfg.get("sector_correlation_guard"), dict) else {}
    enabled = bool(gcfg.get("enabled", True))
    lookback = max(20, int(_to_int(gcfg.get("lookback_days"), 60)))
    reduce_thr = float(_to_float(gcfg.get("reduce_threshold_abs_corr"), 0.85))
    block_thr = float(_to_float(gcfg.get("block_threshold_abs_corr"), 0.97))
    reduce_mult = max(0.1, min(float(_to_float(gcfg.get("reduce_qty_multiplier"), 0.75)), 1.0))
    allow_block = bool(gcfg.get("allow_block", False))
    risk_budget_engine = str(gcfg.get("risk_budget_engine", "EWMA_HRP") or "EWMA_HRP").strip().upper()
    hrp_enabled = bool(gcfg.get("hrp_enabled", True)) and risk_budget_engine in {"EWMA_HRP", "HRP"}
    ewma_halflife = max(5, int(_to_int(gcfg.get("ewma_halflife_days"), 60)))
    hrp_overweight_tolerance = max(1.0, float(_to_float(gcfg.get("hrp_overweight_tolerance"), 1.10)))
    hrp_min_qty_multiplier = max(0.05, min(float(_to_float(gcfg.get("hrp_min_qty_multiplier"), 0.35)), 1.0))

    ctx: Dict[str, Any] = {
        "enabled": bool(enabled),
        "lookback_days": int(lookback),
        "reduce_threshold_abs_corr": float(reduce_thr),
        "block_threshold_abs_corr": float(block_thr),
        "reduce_qty_multiplier": float(reduce_mult),
        "allow_block": bool(allow_block),
        "risk_budget_engine": risk_budget_engine,
        "ewma_halflife_days": int(ewma_halflife),
        "hrp_enabled": bool(hrp_enabled),
        "hrp_overweight_tolerance": float(hrp_overweight_tolerance),
        "hrp_min_qty_multiplier": float(hrp_min_qty_multiplier),
        "sector_corr_score": {},
        "sector_hrp_weight": {},
        "sector_exposure_share": {},
        "sector_hrp_qty_multiplier": {},
        "exposure_by_sector": {},
    }
    if not enabled or not isinstance(prices_df, pd.DataFrame) or prices_df.empty:
        return ctx

    last_close_map: Dict[str, float] = {}
    try:
        _lc = prices_df.sort_values(["code", "date"]).groupby("code")["close"].last()
        for _c, _v in _lc.items():
            _cc = norm_code(_c)
            _vv = float(_to_float(_v, 0.0) or 0.0)
            if _cc and _vv > 0:
                last_close_map[_cc] = _vv
    except Exception:
        pass

    exposure_by_sector: Dict[str, float] = {}
    code_to_sector: Dict[str, str] = {}
    candidate_sector_by_code: Dict[str, str] = {}
    if isinstance(candidate_df, pd.DataFrame) and not candidate_df.empty:
        for _, r in candidate_df.iterrows():
            c = norm_code(r.get("code", ""))
            if not c:
                continue
            sec = _resolve_sector_label(
                c,
                r.get("sector", ""),
                r.get("sector_name", ""),
                r.get("krx_sector", ""),
                sector_db=sector_db,
            )
            if sec:
                candidate_sector_by_code[c] = sec

    for p in (open_pos or []):
        c = norm_code(p.get("code", ""))
        if not c:
            continue
        sec = _resolve_sector_label(c, p.get("sector", ""), candidate_sector_by_code.get(c, ""), sector_db=sector_db)
        if sec:
            code_to_sector[c] = sec
        q = int(_to_int(p.get("qty"), 0) or 0)
        px = float(_to_float(last_close_map.get(c, p.get("entry_price", 0)), 0.0) or 0.0)
        if sec and q > 0 and px > 0:
            exposure_by_sector[sec] = exposure_by_sector.get(sec, 0.0) + (q * px)

    if isinstance(candidate_df, pd.DataFrame) and not candidate_df.empty:
        for _, r in candidate_df.iterrows():
            c = norm_code(r.get("code", ""))
            if not c:
                continue
            sec = _resolve_sector_label(
                c,
                r.get("sector", ""),
                r.get("sector_name", ""),
                r.get("krx_sector", ""),
                sector_db=sector_db,
            )
            if sec and c not in code_to_sector:
                code_to_sector[c] = sec

    used_codes = [c for c in code_to_sector.keys() if c]
    if not used_codes:
        ctx["exposure_by_sector"] = exposure_by_sector
        return ctx

    pxw = prices_df.copy()
    pxw["code"] = pxw["code"].map(norm_code)
    pxw = pxw[pxw["code"].isin(used_codes)][["date", "code", "close"]].copy()
    if pxw.empty:
        ctx["exposure_by_sector"] = exposure_by_sector
        return ctx

    pxw["close"] = pd.to_numeric(pxw["close"], errors="coerce")
    pxw = pxw.dropna(subset=["date", "code", "close"])
    pxw = pxw.sort_values(["code", "date"], kind="mergesort")
    pxw["ret"] = pxw.groupby("code")["close"].pct_change()
    pxw = pxw.dropna(subset=["ret"])
    if pxw.empty:
        ctx["exposure_by_sector"] = exposure_by_sector
        return ctx

    pxw["sector"] = pxw["code"].map(code_to_sector).fillna("").astype(str).str.strip()
    pxw = pxw[pxw["sector"] != ""]
    if pxw.empty:
        ctx["exposure_by_sector"] = exposure_by_sector
        return ctx

    sec_ret = (
        pxw.groupby(["date", "sector"], as_index=False)["ret"].mean()
        .pivot(index="date", columns="sector", values="ret")
        .sort_index()
    )
    if len(sec_ret) > lookback:
        sec_ret = sec_ret.tail(lookback)
    if sec_ret.shape[1] < 2:
        ctx["exposure_by_sector"] = exposure_by_sector
        return ctx

    corr = sec_ret.corr(min_periods=max(10, lookback // 3)).fillna(0.0)
    dom = {k: v for k, v in exposure_by_sector.items() if v > 0}
    total_dom = float(sum(dom.values()))
    score_map: Dict[str, float] = {}
    for sec in corr.columns:
        if not dom or total_dom <= 0:
            score_map[str(sec)] = 0.0
            continue
        s = 0.0
        w = 0.0
        for dsec, dnot in dom.items():
            if dsec not in corr.columns:
                continue
            ww = float(dnot) / total_dom
            cv = abs(float(_to_float(corr.loc[sec, dsec], 0.0) or 0.0))
            s += ww * cv
            w += ww
        score_map[str(sec)] = float(s / w) if w > 0 else 0.0

    ctx["sector_corr_score"] = score_map
    ctx["exposure_by_sector"] = exposure_by_sector
    hrp_weight_map: Dict[str, float] = {}
    exposure_share_map: Dict[str, float] = {}
    hrp_qty_multiplier_map: Dict[str, float] = {}
    if hrp_enabled:
        cov = _ewma_covariance_matrix(sec_ret, ewma_halflife)
        hrp_weight_map = _hrp_weights_from_cov(cov)
        total_exposure = float(sum(float(v) for v in exposure_by_sector.values() if float(v) > 0))
        if total_exposure > 0:
            exposure_share_map = {
                str(k): float(v) / total_exposure
                for k, v in exposure_by_sector.items()
                if float(v) > 0
            }
        for sec in sec_ret.columns:
            sec_key = str(sec)
            target_w = float(hrp_weight_map.get(sec_key, 0.0) or 0.0)
            exposure_w = float(exposure_share_map.get(sec_key, 0.0) or 0.0)
            mult = 1.0
            if target_w > 0 and exposure_w > target_w * hrp_overweight_tolerance:
                mult = max(hrp_min_qty_multiplier, min(1.0, target_w / exposure_w))
            hrp_qty_multiplier_map[sec_key] = float(mult)
    ctx["sector_hrp_weight"] = hrp_weight_map
    ctx["sector_exposure_share"] = exposure_share_map
    ctx["sector_hrp_qty_multiplier"] = hrp_qty_multiplier_map
    try:
        status = {
            "generated_at": now_ts(),
            "enabled": bool(enabled),
            "lookback_days": int(lookback),
            "risk_budget_engine": risk_budget_engine,
            "ewma_halflife_days": int(ewma_halflife),
            "hrp_enabled": bool(hrp_enabled),
            "sector_count": int(sec_ret.shape[1]),
            "rows_used": int(sec_ret.shape[0]),
            "exposure_by_sector": exposure_by_sector,
            "sector_corr_score": score_map,
            "sector_hrp_weight": hrp_weight_map,
            "sector_exposure_share": exposure_share_map,
            "sector_hrp_qty_multiplier": hrp_qty_multiplier_map,
            "thresholds": {
                "reduce_threshold_abs_corr": float(reduce_thr),
                "block_threshold_abs_corr": float(block_thr),
                "reduce_qty_multiplier": float(reduce_mult),
                "allow_block": bool(allow_block),
                "hrp_overweight_tolerance": float(hrp_overweight_tolerance),
                "hrp_min_qty_multiplier": float(hrp_min_qty_multiplier),
            },
        }
        SECTOR_CORRELATION_STATUS_PATH.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass
    return ctx
def _normal_intraday_realtime_block_reason(
    *,
    cfg: Dict[str, Any],
    is_intraday_realtime_entry: bool,
    is_surge_immediate: bool,
    is_split_2nd: bool,
    is_open_order_replay: bool,
    entry_gate_decision: str,
    entry_gate_reason: str = "",
    position_size_multiplier: float = 0.0,
    run_label: str = "",
    p0_rolling_dd_abs: Optional[float] = None,
    p0_rolling_dd_source: str = "",
) -> str:
    policy = cfg.get("normal_intraday_realtime_policy", {}) if isinstance(cfg, dict) else {}
    if not isinstance(policy, dict):
        policy = {}
    if not bool(policy.get("enabled", True)):
        return ""
    if not bool(is_intraday_realtime_entry):
        return ""
    if bool(is_surge_immediate) or bool(is_split_2nd) or bool(is_open_order_replay):
        return ""
    if not bool(policy.get("block_when_entry_gate_not_allow", True)):
        return ""
    blocked = {
        str(x).strip().upper()
        for x in (policy.get("blocked_entry_gate_decisions") or ["CAUTION", "REDUCE", "BLOCK"])
        if str(x).strip()
    }
    decision = str(entry_gate_decision or "").strip().upper()
    if decision in blocked:
        if decision == "REDUCE" and bool(policy.get("allow_dd_stop_validation_reduce", True)):
            ro_val_cfg = (cfg.get("risk_orchestration", {}) if isinstance(cfg, dict) else {}).get("dd_stop_validation", {})
            if not isinstance(ro_val_cfg, dict):
                ro_val_cfg = {}
            allowed_labels = ro_val_cfg.get("allowed_run_labels", ["main", "validation", "tuning"])
            if not isinstance(allowed_labels, list):
                allowed_labels = ["main", "validation", "tuning"]
            label_allowed = str(run_label or "main").strip().lower() in {
                str(x or "").strip().lower()
                for x in allowed_labels
                if str(x or "").strip()
            }
            mode = str(ro_val_cfg.get("mode", "") or "").strip().lower()
            if (
                bool(ro_val_cfg.get("enabled", False))
                and mode in {"reduce", "validation_reduce"}
                and label_allowed
                and "validation_reduce" in str(entry_gate_reason or "")
                and float(_to_float(position_size_multiplier, 0.0)) > 0.0
            ):
                return ""
        return f"NORMAL_INTRADAY_ENTRY_GATE_NOT_ALLOW(decision={decision})"
    if bool(policy.get("block_when_p0_rolling_dd_ge_threshold", False)):
        threshold = _ddm_pct01(abs(_to_float(policy.get("p0_rolling_dd_block_pct"), 0.10)), 0.10)
        dd_abs = _ddm_pct01(abs(_to_float(p0_rolling_dd_abs, 0.0)), 0.0)
        if threshold > 0.0 and dd_abs >= threshold:
            source = str(p0_rolling_dd_source or "p0_kill_switch.max_drawdown_pct").strip()
            return (
                "NORMAL_INTRADAY_P0_ROLLING_DD_BLOCK"
                f"(dd={dd_abs:.4f},threshold={threshold:.4f},source={source})"
            )
    return ""
def _pick_signal_date_candidates_path(signal_date: str) -> Optional[Path]:
    sd = re.sub(r"[^0-9]", "", str(signal_date or ""))[:8]
    if len(sd) != 8:
        return None
    exact = LOG_DIR / f"candidates_v41_1_{sd}.csv"
    if exact.exists():
        return exact
    candidates: List[Tuple[str, Path]] = []
    for fp in LOG_DIR.glob("candidates_v41_1_*.csv"):
        m = re.fullmatch(r"candidates_v41_1_(\d{8})\.csv", fp.name)
        if not m:
            continue
        ymd = m.group(1)
        if ymd <= sd:
            candidates.append((ymd, fp))
    if not candidates:
        return None
    return max(candidates, key=lambda x: x[0])[1]
def _load_signal_date_top_codes_by_score(signal_date: str, top_n: int = 3) -> Tuple[Optional[set[str]], str]:
    path = _pick_signal_date_candidates_path(signal_date)
    if path is None:
        return None, f"candidates_missing_for_signal_date={signal_date}"
    try:
        cdf = pd.read_csv(path, dtype=str)
    except Exception as e:
        return None, f"candidates_read_error={path}:{type(e).__name__}"
    if "code" not in cdf.columns or "score" not in cdf.columns:
        return None, f"candidates_missing_cols(code/score) path={path}"

    cdf["code6"] = cdf["code"].astype(str).str.zfill(6)
    cdf["score_num"] = pd.to_numeric(cdf["score"], errors="coerce")
    sort_cols = ["score_num"]
    ascending = [False]
    if "value" in cdf.columns:
        cdf["value_num"] = pd.to_numeric(cdf["value"], errors="coerce")
        sort_cols.append("value_num")
        ascending.append(False)
    cdf = cdf.sort_values(sort_cols, ascending=ascending, kind="mergesort")
    top_codes = cdf["code6"].dropna().astype(str).head(max(1, int(top_n))).tolist()
    return set(top_codes), f"candidates_ok path={path} top_n={top_n} top_codes={top_codes}"
def _ymd_days_ago(ref_ymd: str, target_ymd: str) -> Optional[int]:
    try:
        ref_dt = datetime.strptime(str(ref_ymd), "%Y%m%d")
        tgt_dt = datetime.strptime(str(target_ymd), "%Y%m%d")
        return int((ref_dt - tgt_dt).days)
    except Exception:
        return None
def _split_second_confirmation_reason(row: Dict[str, Any], cfg: Dict[str, Any]) -> str:
    split = cfg.get("split_entry", {}) if isinstance(cfg.get("split_entry"), dict) else {}
    conf = split.get("second_confirmation", {}) if isinstance(split.get("second_confirmation"), dict) else {}
    if not bool(conf.get("enabled", False)):
        return ""
    missing_action = str(conf.get("missing_feature_action", "ALLOW_WITH_NOTE") or "ALLOW_WITH_NOTE").strip().upper()

    if bool(conf.get("require_ma60_support_bounce", False)):
        if "ma60_support_bounce" not in row:
            if missing_action == "BLOCK":
                return "SPLIT2ND_CONFIRM_MISSING(ma60_support_bounce)"
        else:
            bounce_val = str(row.get("ma60_support_bounce", "")).strip().lower()
            if bounce_val not in {"true", "1", "yes", "t", "1.0"}:
                return "SPLIT2ND_CONFIRM_FAIL(NO_MA60_SUPPORT_BOUNCE)"

    checks = [
        ("v_accel", "max_v_accel"),
        ("ret1_pct", "max_ret1_pct"),
        ("atr14_pct", "max_atr14_pct"),
    ]
    missing: List[str] = []
    for col, key in checks:
        limit = float(_to_float(conf.get(key), 0.0) or 0.0)
        if limit <= 0:
            continue
        val = _to_float(row.get(col), None)
        if val is None:
            missing.append(col)
            continue
        if float(val) > limit:
            return f"SPLIT2ND_CONFIRM_FAIL({col}={float(val):.4f}>{limit:.4f})"
    if missing and missing_action == "BLOCK":
        return f"SPLIT2ND_CONFIRM_MISSING({','.join(missing)})"
    return ""
def discover_recent_parquets(cfg: Dict[str, Any], root_override: Optional[Path] = None, top_n_override: Optional[int] = None) -> List[Path]:
    root = Path(root_override or cfg["parquet_root"]).resolve()
    top_n = int(top_n_override or cfg.get("parquet_top_n_recent", 120))

    cand: List[Tuple[float, Path]] = []
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if not fn.lower().endswith(".parquet"):
                continue
            p = Path(dirpath) / fn
            try:
                mt = p.stat().st_mtime
            except Exception:
                continue
            cand.append((mt, p))
            if len(cand) > top_n * 4:
                cand.sort(key=lambda x: x[0], reverse=True)
                cand = cand[:top_n]
    cand.sort(key=lambda x: x[0], reverse=True)
    return [p for _, p in cand[:top_n]]
def infer_colmap(cols: List[str]) -> Optional[ColMap]:
    lc = {c.lower(): c for c in cols}

    def pick(keys: List[str]) -> Optional[str]:
        for k in keys:
            if k in lc:
                return lc[k]
        return None

    date = pick(["date", "dt", "trade_date", "yyyymmdd", "ymd"])
    code = pick(["code", "ticker", "symbol"])
    o = pick(["open", "시가"])
    h = pick(["high", "고가"])
    low = pick(["low", "저가"])
    c = pick(["close", "종가"])
    name = pick(["name", "name_kor"])
    if not (date and code and o and h and low and c):
        return None
    return ColMap(date=date, code=code, open=o, high=h, low=low, close=c, name=name)
def load_prices_for_codes(cfg: Dict[str, Any], codes: List[str]) -> pd.DataFrame:
    """Load recent OHLCV parquet rows for the requested codes.

    Reads only required columns with PyArrow where possible and concatenates
    recent parquet files until the configured file limit is reached.
    """
    import pyarrow.parquet as pq

    recent = discover_recent_parquets(cfg)
    if not recent:
        raise SystemExit("[FATAL] no parquet found under parquet_root")

    max_open = int(cfg.get("parquet_max_open_files", 30))
    frames: List[pd.DataFrame] = []
    opened = 0

    for p in recent:
        if opened >= max_open:
            break
        try:
            # Inspect schema before reading parquet columns.
            pf = pq.ParquetFile(str(p))
            schema_cols = list(pf.schema_arrow.names)

            cm = infer_colmap(schema_cols)
            if not cm:
                continue

            # Read only columns required for normalized OHLC rows.
            cols = [cm.date, cm.code, cm.open, cm.high, cm.low, cm.close] + ([cm.name] if cm.name else [])
            df = pd.read_parquet(p, columns=cols, engine="pyarrow")

            ren = {cm.date: "date", cm.code: "code", cm.open: "open", cm.high: "high", cm.low: "low", cm.close: "close"}
            if cm.name:
                ren[cm.name] = "name"
            df = df.rename(columns=ren)
            frames.append(df)
            opened += 1
        except Exception as e:
            # Skip unreadable parquet files and continue with older files.
            print(f"[WARN] parquet load skip: {p.name} - {type(e).__name__}")
            continue

    if not frames:
        raise SystemExit("[FATAL] parquet found but none matched OHLC schema (need date/code/open/high/low/close)")

    px = pd.concat(frames, ignore_index=True)
    px["code"] = px["code"].astype(str).str.zfill(6)

    if pd.api.types.is_datetime64_any_dtype(px["date"]):
        px["date"] = px["date"].dt.strftime("%Y%m%d")
    else:
        px["date"] = px["date"].astype(str).str.replace("-", "").str[:8]

    for col in ["open", "high", "low", "close"]:
        px[col] = pd.to_numeric(px[col], errors="coerce")

    codes_set = set(norm_code(c) for c in codes if norm_code(c))
    if codes_set:
        px = px[px["code"].isin(codes_set)]
    else:
        # candidates may be empty on some days; keep all codes to avoid fatal
        print("[WARN] empty codes list; using all codes in price table")
    px = px.dropna(subset=["date", "open", "high", "low", "close"])

    # Keep rows with positive OHLC values only.
    px = px[(px["open"] > 0) & (px["high"] > 0) & (px["low"] > 0) & (px["close"] > 0)]

    if px.empty:
        raise SystemExit("[FATAL] price table empty after code filtering")

    if codes_set:
        found_codes = set(px["code"].astype(str).str.zfill(6).unique().tolist())
        missing_codes = sorted(c for c in codes_set if c not in found_codes)
        if missing_codes:
            search_cfg = cfg.get("parquet_search", {}) if isinstance(cfg.get("parquet_search"), dict) else {}
            search_root_raw = str(search_cfg.get("root", "") or "").strip()
            search_root = Path(search_root_raw) if search_root_raw else BASE_DIR
            if not search_root.is_absolute():
                search_root = (BASE_DIR / search_root).resolve()
            search_top_n = int(search_cfg.get("top_n_recent", 80) or 80)
            search_max_open = int(search_cfg.get("max_open_files", 20) or 20)
            extra_recent = discover_recent_parquets(cfg, root_override=search_root, top_n_override=search_top_n)
            seen_paths = {str(p.resolve()) for p in recent}
            extra_frames: List[pd.DataFrame] = []
            extra_opened = 0
            target_missing = set(missing_codes)

            for p in extra_recent:
                if extra_opened >= search_max_open or not target_missing:
                    break
                if str(p.resolve()) in seen_paths:
                    continue
                try:
                    pf = pq.ParquetFile(str(p))
                    schema_cols = list(pf.schema_arrow.names)
                    cm = infer_colmap(schema_cols)
                    if not cm:
                        continue
                    cols = [cm.date, cm.code, cm.open, cm.high, cm.low, cm.close] + ([cm.name] if cm.name else [])
                    edf = pd.read_parquet(p, columns=cols, engine="pyarrow")
                    ren = {cm.date: "date", cm.code: "code", cm.open: "open", cm.high: "high", cm.low: "low", cm.close: "close"}
                    if cm.name:
                        ren[cm.name] = "name"
                    edf = edf.rename(columns=ren)
                    edf["code"] = edf["code"].astype(str).str.zfill(6)
                    edf = edf[edf["code"].isin(target_missing)].copy()
                    if edf.empty:
                        continue
                    if pd.api.types.is_datetime64_any_dtype(edf["date"]):
                        edf["date"] = edf["date"].dt.strftime("%Y%m%d")
                    else:
                        edf["date"] = edf["date"].astype(str).str.replace("-", "").str[:8]
                    for col in ["open", "high", "low", "close"]:
                        edf[col] = pd.to_numeric(edf[col], errors="coerce")
                    edf = edf.dropna(subset=["date", "open", "high", "low", "close"])
                    edf = edf[(edf["open"] > 0) & (edf["high"] > 0) & (edf["low"] > 0) & (edf["close"] > 0)]
                    if edf.empty:
                        continue
                    extra_frames.append(edf)
                    extra_opened += 1
                    target_missing -= set(edf["code"].unique().tolist())
                except Exception as e:
                    print(f"[WARN] parquet search fallback skip: {p.name} - {type(e).__name__}")
                    continue

            if extra_frames:
                px = pd.concat([px] + extra_frames, ignore_index=True)
                px = px.sort_values(["code", "date", "close"]).drop_duplicates(["code", "date"], keep="last")
                px = px.sort_values(["code", "date"]).reset_index(drop=True)
                found_codes = set(px["code"].astype(str).str.zfill(6).unique().tolist())
                still_missing = sorted(c for c in codes_set if c not in found_codes)
                print(
                    f"[PX_FALLBACK] search_root={search_root} recovered={len(codes_set)-len(still_missing)-len(found_codes-set(missing_codes))} "
                    f"missing_before={len(missing_codes)} missing_after={len(still_missing)}"
                )

    # (code,date) 중복 제거 — close 기준 last 유지
    px = px.sort_values(["code", "date", "close"]).drop_duplicates(["code", "date"], keep="last")
    px = px.sort_values(["code", "date"]).reset_index(drop=True)

    # ── 실시간 가격 override (PAPER_INTRADAY_PRICE_PATH 환경변수) ──────────
    intraday_price_path_raw = str(os.getenv("PAPER_INTRADAY_PRICE_PATH", "") or "").strip()
    if not intraday_price_path_raw:
        default_intraday_price_path = LOG_DIR / "intraday_prices_latest.csv"
        if default_intraday_price_path.exists():
            intraday_price_path_raw = str(default_intraday_price_path)
            print(f"[INTRADAY_PRICE] default path selected: {default_intraday_price_path}")
    if intraday_price_path_raw:
        intraday_price_path = Path(intraday_price_path_raw)
        if intraday_price_path.exists():
            try:
                rt = pd.read_csv(intraday_price_path, dtype=str)
                rt.columns = [c.strip() for c in rt.columns]
                need_cols = {"date", "code", "current_price"}
                if need_cols.issubset(set(rt.columns)):
                    rt["code"] = rt["code"].astype(str).str.zfill(6)
                    rt["date"] = rt["date"].astype(str).str.replace("-", "").str[:8]
                    for col in ["current_price", "open", "high", "low"]:
                        if col in rt.columns:
                            rt[col] = pd.to_numeric(rt[col], errors="coerce")
                    # current_price > 0 인 행만 override
                    rt_valid = rt[rt["current_price"].fillna(0) > 0].copy()
                    if not rt_valid.empty:
                        # close = current_price; open/high/low 있으면 덮어씌움
                        rt_valid["close"] = rt_valid["current_price"]
                        for col in ["open", "high", "low"]:
                            if col not in rt_valid.columns or rt_valid[col].isna().all():
                                rt_valid[col] = rt_valid["current_price"]
                        merge_cols = ["code", "date"]
                        rt_valid = rt_valid[merge_cols + ["open", "high", "low", "close"]].copy()
                        # px에 없는 오늘 행 추가 + 있는 행 override
                        rt_valid["_rt"] = True
                        px["_rt"] = False
                        combined = pd.concat([px, rt_valid], ignore_index=True)
                        combined = combined.sort_values(["code", "date", "_rt"]).drop_duplicates(
                            ["code", "date"], keep="last"
                        )
                        combined = combined.drop(columns=["_rt"]).sort_values(["code", "date"]).reset_index(drop=True)
                        overridden = len(rt_valid)
                        print(
                            f"[INTRADAY_PRICE] override={overridden} codes from {intraday_price_path.name}"
                        )
                        px = combined
                else:
                    print(
                        f"[INTRADAY_PRICE] skip: missing cols {need_cols - set(rt.columns)} in {intraday_price_path.name}"
                    )
            except Exception as _e:
                print(f"[INTRADAY_PRICE] load failed ({intraday_price_path.name}): {_e}")
        else:
            print(f"[INTRADAY_PRICE] path not found: {intraday_price_path_raw}")
    # ─────────────────────────────────────────────────────────────────────────

    return px
def _recent_down_gap_stats(
    px: pd.DataFrame,
    code: str,
    as_of_ymd: str,
    lookback_sessions: int,
    threshold_pct: float,
) -> Dict[str, Any]:
    if not isinstance(px, pd.DataFrame) or px.empty:
        return {"count": 0, "worst_gap_pct": 0.0, "hits": []}
    as_of = _norm_ymd_text(as_of_ymd)
    if len(as_of) != 8:
        return {"count": 0, "worst_gap_pct": 0.0, "hits": []}
    work = px.loc[(px["code"] == str(code).zfill(6)) & (px["date"] <= as_of)].copy()
    if work.empty or not {"date", "open", "close"}.issubset(set(work.columns)):
        return {"count": 0, "worst_gap_pct": 0.0, "hits": []}
    work = work.sort_values("date").tail(max(2, int(lookback_sessions) + 1))
    dates = work["date"].astype(str).tolist()
    opens = pd.to_numeric(work["open"], errors="coerce").fillna(0.0).astype(float).tolist()
    closes = pd.to_numeric(work["close"], errors="coerce").fillna(0.0).astype(float).tolist()
    hits: List[Dict[str, Any]] = []
    worst_gap = 0.0
    threshold = abs(float(threshold_pct or 0.0))
    for i in range(1, len(dates)):
        prev_close = float(closes[i - 1])
        cur_open = float(opens[i])
        if prev_close <= 0 or cur_open <= 0:
            continue
        gap = (cur_open - prev_close) / prev_close
        worst_gap = min(worst_gap, float(gap))
        if threshold > 0 and gap <= -threshold:
            hits.append(
                {
                    "date": str(dates[i]),
                    "gap_pct": round(float(gap), 6),
                    "open": round(float(cur_open), 6),
                    "prev_close": round(float(prev_close), 6),
                }
            )
    return {"count": int(len(hits)), "worst_gap_pct": round(float(worst_gap), 6), "hits": hits[-5:]}
def _minimum_quantity_verification_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    raw = cfg.get("minimum_quantity_verification", {}) if isinstance(cfg, dict) else {}
    if not isinstance(raw, dict):
        raw = {}
    allowed_labels = raw.get("allowed_run_labels", ["validation"])
    if not isinstance(allowed_labels, list):
        allowed_labels = ["validation"]
    label_allowed = str(RUN_LABEL or "").strip().lower() in {
        str(x or "").strip().lower() for x in allowed_labels
    }
    enabled = bool(raw.get("enabled", False)) and label_allowed and bool(PAPER_SESSION_ID)
    return {
        "enabled": bool(enabled),
        "max_new": max(1, _to_int(raw.get("max_new", 1), 1)),
        "max_qty": max(1, _to_int(raw.get("max_qty", 1), 1)),
        "position_size_multiplier": max(0.0, min(1.0, _to_float(raw.get("position_size_multiplier", 0.01), 0.01))),
        "allow_reasons": [str(x).strip() for x in (raw.get("allow_reasons", ["dd_stop", "kelly_zero"]) or [])],
    }
def maybe_run_pnl_report() -> None:
    if _truthy(os.getenv("PAPER_SKIP_PNL_REPORT", "")):
        print("[PNL] skip paper_pnl_report by PAPER_SKIP_PNL_REPORT")
        return
    pnl = BASE_DIR / "paper_pnl_report.py"
    if pnl.exists():
        try:
            subprocess.run([sys.executable, str(pnl)], cwd=str(BASE_DIR))
        except Exception:
            pass
def _merge_guard_decision(
    current_decision: str,
    current_reason: str,
    *,
    guard_tag: str,
    guard_decision: str,
    guard_reason: str,
    guard_severity: str = "hard",
) -> tuple[str, str]:
    decision = str(current_decision or "ALLOW").upper()
    reason = str(current_reason or "")
    g_decision = str(guard_decision or "ALLOW").upper()
    g_reason = str(guard_reason or "")
    severity = str(guard_severity or "hard").strip().lower()

    if severity == "advisory":
        return decision, reason

    if severity == "soft" and g_decision == "BLOCK":
        g_decision = "REDUCE"

    if g_decision == "BLOCK" and decision != "BLOCK":
        decision = "BLOCK"
        reason = f"{reason}; {guard_tag}={g_reason}" if reason else f"{guard_tag}={g_reason}"
    elif g_decision in {"CAUTION", "REDUCE"} and decision not in {"BLOCK", "REDUCE"}:
        decision = g_decision
        reason = f"{reason}; {guard_tag}={g_reason}" if reason else f"{guard_tag}={g_reason}"
    return decision, reason
def _evaluate_entry_guards(
    cfg: Dict[str, Any],
    log_dir: Path,
    entry_decision_code: str,
    entry_decision_reason: str,
    *,
    apply_execution_guard: bool = True,
    current_risk_orch: Optional[Dict[str, Any]] = None,
) -> tuple[str, str, Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    outlier_gate = evaluate_global_outlier_watcher(cfg, log_dir)
    integrity_gate = evaluate_cross_source_integrity(cfg, log_dir)
    sigma_guard = evaluate_sigma_outlier_guard(cfg, log_dir)
    execution_guard = evaluate_execution_health_guard(cfg)
    production_risk_guard = evaluate_production_risk_playbook_guard(cfg, current_risk_orch=current_risk_orch)
    if not apply_execution_guard:
        execution_guard["decision"] = "ALLOW"
        execution_guard["reason"] = "shadow_bypass"
    macro_news_guard = evaluate_macro_news_guard(cfg, RUN_LABEL)
    backtest_validation_guard = evaluate_backtest_validation_guard(cfg)
    btval_pol = cfg.get("backtest_validation_guard") if isinstance(cfg, dict) else {}
    if not isinstance(btval_pol, dict):
        btval_pol = {}
    backtest_validation_merge_decision = str(backtest_validation_guard.get("decision") or "ALLOW").upper()
    backtest_validation_merge_reason = str(backtest_validation_guard.get("reason") or "")
    if backtest_validation_merge_decision == "CAUTION" and not bool(btval_pol.get("caution_affects_entry", False)):
        backtest_validation_guard["entry_effective_decision"] = "ALLOW"
        backtest_validation_guard["entry_effective_reason"] = f"advisory:{backtest_validation_merge_reason}" if backtest_validation_merge_reason else "advisory"
        backtest_validation_merge_decision = "ALLOW"
        backtest_validation_merge_reason = ""

    entry_decision_code, entry_decision_reason = _merge_guard_decision(
        entry_decision_code,
        entry_decision_reason,
        guard_tag="production_risk",
        guard_decision=str(production_risk_guard.get("decision") or "ALLOW"),
        guard_reason=str(production_risk_guard.get("reason") or ""),
        guard_severity="hard",
    )
    entry_decision_code, entry_decision_reason = _merge_guard_decision(
        entry_decision_code,
        entry_decision_reason,
        guard_tag="execution",
        guard_decision=str(execution_guard.get("decision") or "ALLOW"),
        guard_reason=str(execution_guard.get("reason") or ""),
        guard_severity="hard",
    )
    entry_decision_code, entry_decision_reason = _merge_guard_decision(
        entry_decision_code,
        entry_decision_reason,
        guard_tag="outlier",
        guard_decision=str(outlier_gate.get("decision") or "ALLOW"),
        guard_reason=str(outlier_gate.get("reason") or ""),
        guard_severity="soft",
    )
    entry_decision_code, entry_decision_reason = _merge_guard_decision(
        entry_decision_code,
        entry_decision_reason,
        guard_tag="sigma",
        guard_decision=str(sigma_guard.get("decision") or "ALLOW"),
        guard_reason=str(sigma_guard.get("reason") or ""),
        guard_severity="soft",
    )
    entry_decision_code, entry_decision_reason = _merge_guard_decision(
        entry_decision_code,
        entry_decision_reason,
        guard_tag="macro_news",
        guard_decision=str(macro_news_guard.get("decision") or "ALLOW"),
        guard_reason=str(macro_news_guard.get("reason") or ""),
        guard_severity="soft",
    )
    entry_decision_code, entry_decision_reason = _merge_guard_decision(
        entry_decision_code,
        entry_decision_reason,
        guard_tag="integrity",
        guard_decision=str(integrity_gate.get("decision") or "ALLOW"),
        guard_reason=str(integrity_gate.get("reason") or ""),
        guard_severity="advisory",
    )
    entry_decision_code, entry_decision_reason = _merge_guard_decision(
        entry_decision_code,
        entry_decision_reason,
        guard_tag="backtest_validation",
        guard_decision=backtest_validation_merge_decision,
        guard_reason=backtest_validation_merge_reason,
        guard_severity="advisory",
    )
    return (
        entry_decision_code,
        entry_decision_reason,
        outlier_gate,
        integrity_gate,
        sigma_guard,
        execution_guard,
        macro_news_guard,
        backtest_validation_guard,
        production_risk_guard,
    )
def _is_sector_fallback_observe_only_row(row: Any) -> bool:
    if not hasattr(row, "get"):
        return False
    origin = str(row.get("candidate_origin") or "").strip().upper()
    natural_pass = _entry_truthy(row.get("natural_pass", False))
    # Only the explicit sector-prefilter union fallback is observe-only.
    # General candidates with relax_level=NONE continue through the normal
    # positive-entry and sector-eligibility checks below.
    return bool(origin == "SECTOR_PREFILTER_UNION" and (not natural_pass))
def _sector_fallback_observe_only_mask(df: pd.DataFrame) -> pd.Series:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.Series(False, index=(df.index if isinstance(df, pd.DataFrame) else None))
    if "candidate_origin" in df.columns:
        origin = df["candidate_origin"].astype(str).str.strip().str.upper().eq("SECTOR_PREFILTER_UNION")
    else:
        origin = pd.Series(False, index=df.index)
    if "natural_pass" in df.columns:
        natural = df["natural_pass"].map(_entry_truthy)
    else:
        natural = pd.Series(False, index=df.index)
    return origin & (~natural)
def _check_positive_entry_criteria(row: pd.Series, cfg: Dict[str, Any]) -> Dict[str, Any]:
    pol = cfg.get("positive_entry_criteria", {}) if isinstance(cfg.get("positive_entry_criteria"), dict) else {}
    if not bool(pol.get("enabled", True)):
        return {"ok": True, "reason": "positive_entry_criteria_disabled"}

    reasons: List[str] = []
    missing: List[str] = []

    score_cols = pol.get("score_columns", ["final_score", "score"])
    if not isinstance(score_cols, list):
        score_cols = ["final_score", "score"]
    score_col = next((str(c) for c in score_cols if str(c) in row.index), "")
    min_score = float(_to_float(pol.get("min_score", 0.0), 0.0) or 0.0)
    score_ok = False
    if score_col:
        score_val = _to_float(row.get(score_col), None)
        score_ok = bool(score_val is not None and float(score_val) > min_score)
        if score_ok:
            reasons.append(f"{score_col}>{min_score:g}")
    else:
        missing.append("score")

    sector_strength_min = float(_to_float(cfg.get("union_entry_strength_min", 0.65), 0.65))
    sector_strength = float(_to_float(row.get("sector_strength"), 0.0) or 0.0)
    sector_allowed = _entry_truthy(row.get("sector_entry_allowed")) if "sector_entry_allowed" in row.index else True
    union_ok = (
        bool(pol.get("allow_sector_union", True))
        and str(row.get("candidate_origin") or "").strip().upper() == "SECTOR_PREFILTER_UNION"
        and str(row.get("sector_action") or "").strip().upper() == "BUY"
        and sector_allowed
        and sector_strength >= sector_strength_min
    )

    execution_ok = True
    if bool(pol.get("require_execution_pool_when_present", True)) and "execution_pool" in row.index:
        execution_ok = _entry_truthy(row.get("execution_pool")) or bool(union_ok)
        if execution_ok:
            reasons.append("execution_pool" if _entry_truthy(row.get("execution_pool")) else "sector_union")

    sector_ok = True
    if bool(pol.get("require_sector_entry_when_present", True)) and "sector_entry_allowed" in row.index:
        sector_ok = bool(sector_allowed)
        if sector_ok:
            reasons.append("sector_entry_allowed")

    ok = bool(score_ok and execution_ok and sector_ok)
    failed: List[str] = []
    if not score_ok:
        failed.append("score")
    if not execution_ok:
        failed.append("execution_pool")
    if not sector_ok:
        failed.append("sector_entry_allowed")
    if missing:
        reasons.append("missing:" + ",".join(missing))
    reason = ",".join(reasons if ok else failed)
    return {"ok": ok, "reason": reason or ("positive_entry_ok" if ok else "positive_entry_fail")}
def _apply_positive_entry_criteria(candidate_df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    if not isinstance(candidate_df, pd.DataFrame) or candidate_df.empty:
        return candidate_df
    pol = cfg.get("positive_entry_criteria", {}) if isinstance(cfg.get("positive_entry_criteria"), dict) else {}
    if not bool(pol.get("enabled", True)):
        return candidate_df
    before = len(candidate_df)
    out = candidate_df.copy()
    checks = out.apply(lambda row: _check_positive_entry_criteria(row, cfg), axis=1)
    out["positive_entry_ok"] = checks.map(lambda x: bool(x.get("ok")) if isinstance(x, dict) else False)
    out["positive_entry_reason"] = checks.map(lambda x: str(x.get("reason") or "") if isinstance(x, dict) else "")
    ok_count = int(out["positive_entry_ok"].sum())
    print(f"[ENTRY_POOL] positive_entry_criteria ok={ok_count}/{before}")
    if bool(pol.get("hard_filter", True)):
        pre_filter_df = out.copy()
        out = out[out["positive_entry_ok"]].copy()
        fallback_cfg = pol.get("fresh_sector_allowed_fallback", {}) if isinstance(pol.get("fresh_sector_allowed_fallback"), dict) else {}
        if bool(fallback_cfg.get("enabled", False)):
            split2_ok_mask = pd.Series(False, index=out.index)
            if "split_entry_2nd" in out.columns:
                split2_ok_mask = out["split_entry_2nd"].astype(str).str.strip().str.lower().isin({"1", "true", "t", "y", "yes"})
            fresh_ok_count = int((~split2_ok_mask).sum()) if len(out) else 0
            trigger_lte = max(0, int(_to_int(fallback_cfg.get("trigger_when_fresh_ok_lte", 0), 0) or 0))
            if fresh_ok_count <= trigger_lte and not pre_filter_df.empty:
                fb = pre_filter_df.copy()
                split2_src_mask = pd.Series(False, index=fb.index)
                if "split_entry_2nd" in fb.columns:
                    split2_src_mask = fb["split_entry_2nd"].astype(str).str.strip().str.lower().isin({"1", "true", "t", "y", "yes"})
                score_col = "final_score" if "final_score" in fb.columns else ("score" if "score" in fb.columns else "")
                if score_col:
                    fb["_fresh_fallback_score"] = pd.to_numeric(fb[score_col], errors="coerce")
                else:
                    fb["_fresh_fallback_score"] = pd.NA
                strength = pd.to_numeric(fb.get("sector_strength"), errors="coerce").fillna(0.0)
                sector_allowed = fb.get("sector_entry_allowed", pd.Series(True, index=fb.index)).astype(str).str.strip().str.upper().isin(["TRUE", "1", "Y", "YES", "T"])
                actions_raw = fallback_cfg.get("allowed_sector_actions", ["BUY", "WAIT"])
                if not isinstance(actions_raw, list):
                    actions_raw = ["BUY", "WAIT"]
                allowed_actions = {str(x).strip().upper() for x in actions_raw if str(x).strip()}
                action_ok = fb.get("sector_action", pd.Series("BUY", index=fb.index)).astype(str).str.strip().str.upper().isin(allowed_actions)
                origin_required = str(fallback_cfg.get("candidate_origin", "SECTOR_PREFILTER_UNION") or "").strip().upper()
                origin_ok = pd.Series(True, index=fb.index)
                if origin_required and "candidate_origin" in fb.columns:
                    origin_ok = fb["candidate_origin"].astype(str).str.strip().str.upper().eq(origin_required)
                min_score_fb = float(_to_float(fallback_cfg.get("min_final_score", 0.10), 0.10))
                min_strength_fb = float(_to_float(fallback_cfg.get("min_sector_strength", 0.40), 0.40))
                max_candidates_fb = max(1, int(_to_int(fallback_cfg.get("max_candidates", 1), 1)))
                fb_mask = (
                    (~split2_src_mask)
                    & sector_allowed
                    & action_ok
                    & origin_ok
                    & (strength >= min_strength_fb)
                    & (fb["_fresh_fallback_score"] >= min_score_fb)
                )
                fb = fb[fb_mask].copy()
                if not fb.empty:
                    fb = fb.sort_values(["_fresh_fallback_score", "code"], ascending=[False, True], kind="mergesort").head(max_candidates_fb).copy()
                    fb["positive_entry_ok"] = True
                    fb["positive_entry_reason"] = (
                        "fresh_sector_allowed_fallback:"
                        f"{score_col}>={min_score_fb:g},"
                        f"sector_strength>={min_strength_fb:g},"
                        f"sector_action={'+'.join(sorted(allowed_actions)) or 'ANY'}"
                    )
                    if "execution_pool" in fb.columns:
                        fb["execution_pool"] = True
                    out_codes = set(out["code"].astype(str).str.zfill(6).tolist()) if "code" in out.columns else set()
                    fb = fb[~fb["code"].astype(str).str.zfill(6).isin(out_codes)].copy() if "code" in fb.columns else fb
                    if not fb.empty:
                        out = _concat_drop_all_na_columns([out, fb], ignore_index=True)
                        print(
                            f"[ENTRY_POOL] fresh_sector_allowed_fallback added={len(fb)} "
                            f"fresh_ok={fresh_ok_count} min_score={min_score_fb:.3f} "
                            f"min_strength={min_strength_fb:.3f}"
                        )
        after = len(out)
        print(f"[ENTRY_POOL] positive_entry_criteria hard_filter applied: {before}->{after}")
    return out
def _load_defense_signal_entry_map(cfg: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    pol = cfg.get("defense_signal_entry_policy", {}) if isinstance(cfg.get("defense_signal_entry_policy"), dict) else {}
    if not bool(pol.get("enabled", False)):
        return {}
    path_raw = str(pol.get("artifact_path") or DEFENSE_SIGNAL_SHADOW_PATH)
    path = Path(path_raw)
    if not path.is_absolute():
        path = BASE_DIR / path
    if not path.exists():
        missing_action = str(pol.get("missing_action") or "ALLOW").strip().upper()
        if missing_action != "ALLOW":
            print(f"[ENTRY_POOL] defense_signal artifact missing action={missing_action} path={path}")
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            df = pd.read_csv(path, encoding=enc, dtype={"code": str})
            break
        except UnicodeDecodeError:
            continue
        except Exception as exc:
            print(f"[ENTRY_POOL] defense_signal read failed path={path} err={exc}")
            return {}
    else:
        return {}
    if df.empty or "code" not in df.columns:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for _, row in df.iterrows():
        code = norm_code(row.get("code", ""))
        if not code:
            continue
        out[code] = {
            "defense_action_shadow": str(row.get("defense_action_shadow", "") or ""),
            "general_action_shadow": str(row.get("general_action_shadow", "") or ""),
            "surge_action_shadow": str(row.get("surge_action_shadow", "") or ""),
            "defense_signal_score": _to_float(row.get("defense_signal_score"), 0.0),
            "would_block_general": _truthy(row.get("would_block_general", False)),
            "would_block_surge": _truthy(row.get("would_block_surge", False)),
            "defense_reasons": str(row.get("defense_reasons", "") or ""),
            "route_scope": str(row.get("route_scope", "") or ""),
        }
    return out
def _apply_defense_signal_entry_policy(candidate_df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    if not isinstance(candidate_df, pd.DataFrame) or candidate_df.empty or "code" not in candidate_df.columns:
        return candidate_df
    pol = cfg.get("defense_signal_entry_policy", {}) if isinstance(cfg.get("defense_signal_entry_policy"), dict) else {}
    if not bool(pol.get("enabled", False)):
        return candidate_df
    defense_by_code = _load_defense_signal_entry_map(cfg)
    if not defense_by_code:
        return candidate_df

    out = candidate_df.copy()
    codes = out["code"].astype(str).map(norm_code)
    matched_count = int(codes.map(lambda c: c in defense_by_code).sum())
    out["defense_action_shadow"] = codes.map(lambda c: defense_by_code.get(c, {}).get("defense_action_shadow", ""))
    out["defense_signal_score"] = codes.map(lambda c: defense_by_code.get(c, {}).get("defense_signal_score", 0.0))
    out["defense_signal_reasons"] = codes.map(lambda c: defense_by_code.get(c, {}).get("defense_reasons", ""))

    general_actions = {
        str(x).strip().upper()
        for x in (pol.get("block_actions_general") or ["GENERAL_SHADOW_WAIT"])
        if str(x).strip()
    }
    surge_actions = {
        str(x).strip().upper()
        for x in (pol.get("block_actions_surge") or ["SURGE_SHADOW_BLOCK"])
        if str(x).strip()
    }

    block_general_enabled = bool(pol.get("block_general", True))
    block_surge_enabled = bool(pol.get("block_surge", True))
    general_block = codes.map(
        lambda c: bool(
            block_general_enabled
            and defense_by_code.get(c, {}).get("would_block_general", False)
            and str(defense_by_code.get(c, {}).get("general_action_shadow", "")).strip().upper() in general_actions
        )
    )

    surge_type = out.get("surge_type", pd.Series("", index=out.index)).astype(str).str.strip().str.upper()
    row_is_surge = surge_type.ne("") & surge_type.ne("NONE")
    for col in ("is_realtime_surge", "surge_flag", "_surge_immediate"):
        if col in out.columns:
            row_is_surge = row_is_surge | out[col].map(_truthy)
    surge_block = codes.map(
        lambda c: bool(
            block_surge_enabled
            and defense_by_code.get(c, {}).get("would_block_surge", False)
            and str(defense_by_code.get(c, {}).get("surge_action_shadow", "")).strip().upper() in surge_actions
        )
    ) & row_is_surge

    block_mask = general_block | surge_block
    out["defense_signal_entry_block"] = block_mask
    out["defense_signal_entry_block_reason"] = [
        "defense_signal_general_block" if bool(g) else ("defense_signal_surge_block" if bool(s) else "")
        for g, s in zip(general_block.tolist(), surge_block.tolist())
    ]
    before = len(out)
    out = out[~block_mask].copy()
    after = len(out)
    blocked = before - after
    print(
        "[ENTRY_POOL] defense_signal_entry_policy applied: "
        f"{before}->{after} matched={matched_count} blocked={blocked}"
    )
    return out
def _prepare_entry_candidate_pool(
    candidate_df: pd.DataFrame,
    cfg: Dict[str, Any],
    *,
    exclude_codes: Optional[set[str]] = None,
) -> tuple[pd.DataFrame, int]:
    validation_candidate_count = len(candidate_df)
    news_implication_policy = cfg.get("news_implication_entry_policy", {}) if isinstance(cfg.get("news_implication_entry_policy"), dict) else {}
    news_implication_enabled = bool(news_implication_policy.get("enabled", True))
    if "news_implication_block_rows" in candidate_df.columns:
        before = len(candidate_df)
        candidate_df = candidate_df.copy()
        block_rows = pd.to_numeric(candidate_df["news_implication_block_rows"], errors="coerce").fillna(0.0)
        block_observe_only = bool(news_implication_policy.get("block_observe_only", True))
        candidate_df["news_implication_block_observe_only"] = block_rows.map(
            lambda v: bool(news_implication_enabled and block_observe_only and float(v or 0.0) > 0.0)
        )
        block_count = int((block_rows > 0).sum())
        if block_count > 0:
            if news_implication_enabled and not block_observe_only:
                candidate_df = candidate_df[block_rows <= 0].copy()
                after = len(candidate_df)
                print(f"[ENTRY_POOL] news_implication_block active applied: {before}->{after}")
            else:
                print(f"[ENTRY_POOL] news_implication_block observe_only count={block_count}")
    if "news_implication_reduce_size_rows" in candidate_df.columns:
        candidate_df = candidate_df.copy()
        reduce_rows = pd.to_numeric(candidate_df["news_implication_reduce_size_rows"], errors="coerce").fillna(0.0)
        reduce_multiplier = max(0.0, min(1.0, float(_to_float(news_implication_policy.get("reduce_size_multiplier"), 0.5))))
        observe_only = bool(news_implication_policy.get("reduce_size_observe_only", True))
        candidate_df["news_implication_reduce_size_multiplier"] = reduce_rows.map(
            lambda v: float(reduce_multiplier) if news_implication_enabled and not observe_only and float(v or 0.0) > 0.0 else 1.0
        )
        candidate_df["news_implication_reduce_size_observe_only"] = reduce_rows.map(
            lambda v: bool(news_implication_enabled and observe_only and float(v or 0.0) > 0.0)
        )
        reduce_count = int((reduce_rows > 0).sum())
        if reduce_count > 0:
            reduce_mode = "observe_only" if observe_only else "active"
            print(
                f"[ENTRY_POOL] news_implication_reduce_size {reduce_mode} count={reduce_count} "
                f"multiplier={reduce_multiplier:.3f}"
            )
    if "news_implication_watch_rows" in candidate_df.columns:
        candidate_df = candidate_df.copy()
        watch_rows = pd.to_numeric(candidate_df["news_implication_watch_rows"], errors="coerce").fillna(0.0)
        observe_only = bool(news_implication_policy.get("watch_observe_only", True))
        candidate_df["news_implication_watch_observe_only"] = watch_rows.map(
            lambda v: bool(news_implication_enabled and observe_only and float(v or 0.0) > 0.0)
        )
        watch_count = int((watch_rows > 0).sum())
        if watch_count > 0:
            print(f"[ENTRY_POOL] news_implication_watch observe_only count={watch_count}")
    if "news_topic_execution_effect" in candidate_df.columns:
        candidate_df = candidate_df.copy()
        news_topic_policy = cfg.get("news_topic_execution_policy", {}) if isinstance(cfg.get("news_topic_execution_policy"), dict) else {}
        news_topic_enabled = bool(news_topic_policy.get("enabled", True))
        reduce_multiplier = max(0.0, min(1.0, float(_to_float(news_topic_policy.get("reduce_size_multiplier"), 0.5))))
        effects = candidate_df["news_topic_execution_effect"].astype(str).str.strip().str.lower()
        if "news_topic_l3_execution_allowed" in candidate_df.columns:
            l3_allowed = candidate_df["news_topic_l3_execution_allowed"].map(_truthy)
        else:
            l3_allowed = pd.Series(False, index=candidate_df.index)
        if news_topic_enabled:
            block_mask = effects.isin({"block_order", "avoid_chase", "block_review"}) & l3_allowed
            block_count = int(block_mask.sum())
            if block_count > 0:
                before = len(candidate_df)
                candidate_df = candidate_df[~block_mask].copy()
                after = len(candidate_df)
                print(f"[ENTRY_POOL] news_topic_block_order applied: {before}->{after} blocked={block_count}")
                effects = candidate_df["news_topic_execution_effect"].astype(str).str.strip().str.lower()
                l3_allowed = candidate_df["news_topic_l3_execution_allowed"].map(_truthy) if "news_topic_l3_execution_allowed" in candidate_df.columns else pd.Series(False, index=candidate_df.index)
        reduce_mask = (effects.eq("reduce_size") & l3_allowed) if news_topic_enabled else pd.Series(False, index=candidate_df.index)
        candidate_df["news_topic_reduce_size_multiplier"] = reduce_mask.map(
            lambda v: float(reduce_multiplier) if bool(v) else 1.0
        )
        candidate_df["news_topic_watch_observe_only"] = effects.eq("watch_only") if news_topic_enabled else False
        reduce_count = int(reduce_mask.sum())
        watch_count = int(candidate_df["news_topic_watch_observe_only"].sum()) if "news_topic_watch_observe_only" in candidate_df.columns else 0
        if reduce_count > 0:
            print(f"[ENTRY_POOL] news_topic_reduce_size active count={reduce_count} multiplier={reduce_multiplier:.3f}")
        if watch_count > 0:
            print(f"[ENTRY_POOL] news_topic_watch observe_only count={watch_count}")
    news_signal_policy = cfg.get("news_signal_shadow_entry_policy", {}) if isinstance(cfg.get("news_signal_shadow_entry_policy"), dict) else {}
    if bool(news_signal_policy.get("enabled", False)) and "code" in candidate_df.columns:
        stage_path = Path(str(news_signal_policy.get("path") or (LOG_DIR / "news_signal_shadow_stage_latest.csv")))
        if not stage_path.is_absolute():
            stage_path = BASE_DIR / stage_path
        allowed_stages = {
            str(x).strip().upper()
            for x in (news_signal_policy.get("allow_stages") or ["CONFIRMED_SHADOW"])
            if str(x).strip()
        }
        block_stages = {
            str(x).strip().upper()
            for x in (news_signal_policy.get("block_stages") or ["BLOCK_SHADOW", "PUBLISHER_UNVERIFIED_SHADOW"])
            if str(x).strip()
        }
        observe_only = bool(news_signal_policy.get("observe_only", True))
        try:
            stage_df = pd.read_csv(stage_path, encoding="utf-8-sig")
        except Exception as e:
            print(f"[ENTRY_POOL] news_signal_shadow_stage unavailable path={stage_path} err={type(e).__name__}")
            stage_df = pd.DataFrame()
        if not stage_df.empty and "code" in stage_df.columns and "news_signal_stage" in stage_df.columns:
            stage_df = stage_df.copy()
            stage_df["code"] = stage_df["code"].astype(str).map(norm_code)
            stage_map = stage_df.drop_duplicates("code", keep="last").set_index("code").to_dict("index")
            candidate_df = candidate_df.copy()
            codes = candidate_df["code"].astype(str).map(norm_code)
            candidate_df["news_signal_order_stage"] = codes.map(lambda c: str((stage_map.get(c) or {}).get("news_signal_stage", "")))
            candidate_df["news_signal_order_effect"] = candidate_df["news_signal_order_stage"].astype(str).str.upper().map(
                lambda s: "allow" if s in allowed_stages else ("block" if s in block_stages else "none")
            )
            block_mask = candidate_df["news_signal_order_effect"].eq("block")
            block_count = int(block_mask.sum())
            if block_count > 0:
                mode = "observe_only" if observe_only else "active"
                print(f"[ENTRY_POOL] news_signal_shadow_stage {mode} block_count={block_count}")
                if not observe_only:
                    candidate_df = candidate_df[~block_mask].copy()
    candidate_df = _apply_defense_signal_entry_policy(candidate_df, cfg)
    if exclude_codes:
        ex = {str(x).zfill(6) for x in exclude_codes if str(x).strip()}
        if ex and "code" in candidate_df.columns:
            before = len(candidate_df)
            candidate_df = candidate_df.copy()
            candidate_df["code"] = candidate_df["code"].astype(str).str.zfill(6)
            split2_mask = pd.Series(False, index=candidate_df.index)
            if "split_entry_2nd" in candidate_df.columns:
                split2_mask = candidate_df["split_entry_2nd"].astype(str).str.strip().str.lower().isin({"1", "true", "t", "y", "yes"})
            candidate_df = candidate_df[(~candidate_df["code"].isin(ex)) | split2_mask].copy()
            after = len(candidate_df)
            print(f"[ENTRY_POOL] exclude_open_codes prefilter applied: {before}->{after}")
    sector_fallback_observe_only = _sector_fallback_observe_only_mask(candidate_df)
    sector_fallback_observe_count = int(sector_fallback_observe_only.sum()) if len(sector_fallback_observe_only) else 0
    if sector_fallback_observe_count > 0:
        before = len(candidate_df)
        codes_sample = []
        if "code" in candidate_df.columns:
            codes_sample = candidate_df.loc[sector_fallback_observe_only, "code"].astype(str).str.zfill(6).head(10).tolist()
        candidate_df = candidate_df[~sector_fallback_observe_only].copy()
        after = len(candidate_df)
        print(
            f"[ENTRY_POOL] sector_fallback_observe_only applied: {before}->{after} "
            f"blocked={sector_fallback_observe_count} reason=SECTOR_FALLBACK_OBSERVE_ONLY "
            f"codes={','.join(codes_sample)}"
        )
    candidate_df = _apply_positive_entry_criteria(candidate_df, cfg)
    _req_ep = bool(cfg.get("positive_entry_criteria", {}).get("require_execution_pool_when_present", True))
    if "execution_pool" in candidate_df.columns and _req_ep:
        before = len(candidate_df)
        execution_pool = candidate_df["execution_pool"].astype(str).str.strip().str.upper()
        entry_mask = execution_pool.isin(["TRUE", "1", "Y", "YES"])
        union_added = 0
        if "candidate_origin" in candidate_df.columns and "sector_action" in candidate_df.columns and "sector_entry_allowed" in candidate_df.columns:
            try:
                union_strength_min = float(cfg.get("union_entry_strength_min", 0.65) or 0.65)
            except Exception:
                union_strength_min = 0.65
            sector_strength_src = candidate_df["sector_strength"] if "sector_strength" in candidate_df.columns else pd.Series(0.0, index=candidate_df.index)
            sector_strength = pd.to_numeric(sector_strength_src, errors="coerce").fillna(0.0)
            origin_union = candidate_df["candidate_origin"].astype(str).str.strip().str.upper().eq("SECTOR_PREFILTER_UNION")
            action_buy = candidate_df["sector_action"].astype(str).str.strip().str.upper().eq("BUY")
            allowed = candidate_df["sector_entry_allowed"].astype(str).str.strip().str.upper().isin(["TRUE", "1"])
            union_mask = origin_union & action_buy & allowed & (sector_strength >= union_strength_min)
            union_added = int((union_mask & ~entry_mask).sum())
            if union_added > 0:
                entry_mask = entry_mask | union_mask
                print(f"[ENTRY_POOL] union_conditional_added={union_added} strength_min={union_strength_min:.2f}")
        candidate_df = candidate_df[entry_mask].copy()
        after = len(candidate_df)
        print(f"[ENTRY_POOL] execution_pool=True applied: {before}->{after}")
    if "final_score" in candidate_df.columns:
        before = len(candidate_df)
        candidate_df["_entry_final_score"] = pd.to_numeric(candidate_df["final_score"], errors="coerce")
        candidate_df = candidate_df[candidate_df["_entry_final_score"] > 0].copy()
        after = len(candidate_df)
        print(f"[ENTRY_POOL] final_score>0 applied: {before}->{after}")
    # MIN_ENTRY_SCORE: config-driven quality floor applied to final_score (if available) else score.
    _min_entry_score = float(cfg.get("min_entry_score", 0.0) or 0.0)
    if _min_entry_score > 0:
        if "_entry_final_score" in candidate_df.columns:
            _score_col = "_entry_final_score"
        elif "score" in candidate_df.columns:
            candidate_df["_entry_score_basis"] = pd.to_numeric(candidate_df["score"], errors="coerce").fillna(0.0)
            _score_col = "_entry_score_basis"
        else:
            _score_col = None
        if _score_col:
            _effective_min_entry_score = float(_min_entry_score)
            _min_entry_cfg = cfg.get("min_entry_score_policy", {}) if isinstance(cfg.get("min_entry_score_policy"), dict) else {}
            if bool(_min_entry_cfg.get("enabled", False)):
                _score_series = pd.to_numeric(candidate_df[_score_col], errors="coerce").dropna()
                _min_candidates = max(1, int(_to_float(_min_entry_cfg.get("min_candidates"), 3)))
                if int(len(_score_series)) >= _min_candidates:
                    _q = float(_to_float(_min_entry_cfg.get("quantile"), 0.60))
                    _q = max(0.0, min(1.0, _q))
                    _q_value = float(_score_series.quantile(_q))
                    _min_floor = float(_to_float(_min_entry_cfg.get("min_floor"), 0.20))
                    _max_cap = float(_to_float(_min_entry_cfg.get("max_cap"), _min_entry_score) or _min_entry_score)
                    _dynamic_min = max(_min_floor, min(_q_value, _max_cap))
                    # Never make threshold stricter than fixed floor.
                    _effective_min_entry_score = min(float(_min_entry_score), float(_dynamic_min))
                    print(
                        f"[ENTRY_POOL] min_entry_score_policy enabled: "
                        f"base={_min_entry_score:.3f} q={_q:.2f} q_value={_q_value:.3f} "
                        f"floor={_min_floor:.3f} cap={_max_cap:.3f} "
                        f"effective={_effective_min_entry_score:.3f}"
                    )
                else:
                    print(
                        f"[ENTRY_POOL] min_entry_score_policy skip: "
                        f"rows={len(_score_series)} < min_candidates={_min_candidates}"
                    )
            before = len(candidate_df)
            pre_min_df = candidate_df.copy()
            candidate_df = candidate_df[candidate_df[_score_col] >= _effective_min_entry_score].copy()
            after = len(candidate_df)
            allow_min_score_fallback_top1 = bool(_min_entry_cfg.get("allow_fallback_top1", True))
            if allow_min_score_fallback_top1 and after == 0 and before > 0:
                fallback_src = pre_min_df[pre_min_df[_score_col].notna()].copy()
                if not fallback_src.empty:
                    fallback_src = fallback_src.sort_values([_score_col, "code"], ascending=[False, True], kind="mergesort")
                    candidate_df = fallback_src.head(1).copy()
                    after = len(candidate_df)
                    print(
                        f"[ENTRY_POOL] min_entry_score fallback_top1 applied ({_score_col}): "
                        f"{before}->0->{after} threshold={_effective_min_entry_score:.3f}"
                    )
            print(f"[ENTRY_POOL] min_entry_score={_effective_min_entry_score:.3f} applied ({_score_col}): {before}->{after}")
    if "sector_entry_allowed" in candidate_df.columns:
        before = len(candidate_df)
        pre_sector_df = candidate_df.copy()
        entry_allowed = candidate_df["sector_entry_allowed"].astype(str).str.strip().str.upper()
        candidate_df = candidate_df[entry_allowed.isin(["TRUE", "1"])].copy()
        after = len(candidate_df)
        allow_sector_fallback_top1 = bool(cfg.get("sector_entry_fallback_top1", True))
        if allow_sector_fallback_top1 and after == 0 and before > 0:
            score_col = "_entry_final_score" if "_entry_final_score" in pre_sector_df.columns else None
            if score_col is None and "final_score" in pre_sector_df.columns:
                pre_sector_df["_sector_fallback_score"] = pd.to_numeric(pre_sector_df["final_score"], errors="coerce")
                score_col = "_sector_fallback_score"
            if score_col is None and "score" in pre_sector_df.columns:
                pre_sector_df["_sector_fallback_score"] = pd.to_numeric(pre_sector_df["score"], errors="coerce")
                score_col = "_sector_fallback_score"
            if score_col is not None:
                keep_df = pre_sector_df[pre_sector_df[score_col].notna()].copy()
                if not keep_df.empty:
                    keep_df = keep_df.sort_values([score_col, "code"], ascending=[False, True], kind="mergesort")
                    candidate_df = keep_df.head(1).copy()
                    after = len(candidate_df)
                    print(f"[ENTRY_POOL] sector_entry_allowed fallback_top1 applied: {before}->0->{after}")
        print(f"[ENTRY_POOL] sector_entry_allowed=True applied: {before}->{after}")
    elif "sector_score" in candidate_df.columns:
        before = len(candidate_df)
        candidate_df["_entry_sector_score"] = pd.to_numeric(candidate_df["sector_score"], errors="coerce").fillna(0.0)
        candidate_df = candidate_df[candidate_df["_entry_sector_score"] > 0].copy()
        after = len(candidate_df)
        print(f"[ENTRY_POOL] sector_score>0 applied: {before}->{after}")
    elif "sector_action" in candidate_df.columns:
        before = len(candidate_df)
        sector_action = candidate_df["sector_action"].astype(str).str.strip().str.upper()
        candidate_df = candidate_df[sector_action == "BUY"].copy()
        after = len(candidate_df)
        print(f"[ENTRY_POOL] sector_action=BUY applied: {before}->{after}")
    if validation_candidate_count != len(candidate_df):
        print(f"[ENTRY_POOL] validation_candidates={validation_candidate_count} entry_candidates={len(candidate_df)}")
    return candidate_df, validation_candidate_count
def _apply_signal_and_sector_caps(
    candidate_df: pd.DataFrame,
    rank_col: Optional[str],
    cap_signal_top_n: int,
    max_per_sector: int,
) -> pd.DataFrame:
    if cap_signal_top_n > 0 and ("signal_date" in candidate_df.columns):
        if rank_col:
            candidate_df["_score_n"] = pd.to_numeric(candidate_df[rank_col], errors="coerce")
            tie_col = "trading_value" if "trading_value" in candidate_df.columns else ("value" if "value" in candidate_df.columns else None)
            if tie_col:
                candidate_df["_tie_n"] = pd.to_numeric(candidate_df[tie_col], errors="coerce")
                candidate_df = candidate_df.sort_values(["signal_date", "_score_n", "_tie_n"], ascending=[True, False, False])
            else:
                candidate_df = candidate_df.sort_values(["signal_date", "_score_n"], ascending=[True, False])

            candidate_df["_rk_sig"] = candidate_df.groupby("signal_date").cumcount() + 1
            before = len(candidate_df)
            candidate_df = candidate_df[candidate_df["_rk_sig"] <= cap_signal_top_n].copy()
            after = len(candidate_df)
            print(f"[CAP] cap_signal_top_n={cap_signal_top_n} applied: {before}->{after}")
        else:
            print("[CAP] ranking column missing (need final_score or score); cap_signal_top_n ignored")

    if max_per_sector > 0:
        sector_col = next(
            (col for col in ("sector_code", "krx_sector", "sector") if col in candidate_df.columns),
            None,
        )
        if not sector_col:
            print("[CAP] max_per_sector set but sector column missing; ignored")
        else:
            candidate_df["_sector_key"] = candidate_df[sector_col].astype(str).str.strip()
            miss = (candidate_df["_sector_key"] == "") | (candidate_df["_sector_key"].str.lower() == "nan")
            candidate_df.loc[miss, "_sector_key"] = "__NA__" + candidate_df.loc[miss, "code"].astype(str).str.zfill(6)
            if rank_col:
                candidate_df["_score_sec"] = pd.to_numeric(candidate_df[rank_col], errors="coerce")
                candidate_df = candidate_df.sort_values(["signal_date", "_sector_key", "_score_sec"], ascending=[True, True, False])

            candidate_df["_rk_sector"] = candidate_df.groupby(["signal_date", "_sector_key"]).cumcount() + 1
            before = len(candidate_df)
            candidate_df = candidate_df[candidate_df["_rk_sector"] <= max_per_sector].copy()
            after = len(candidate_df)
            print(f"[CAP] max_per_sector={max_per_sector} applied: {before}->{after}")
    return candidate_df
def _build_signal_tracking_context(
    config: Dict[str, Any],
    *,
    schema: str,
    stop_loss: float,
    take_profit: Any,
    trail_pct: Any,
    fee_pct: float,
    slip_pct: float,
    sell_tax_pct: float,
    replay_enabled: bool,
    replay_min_age_days: int,
    replay_require_no_open_positions: bool,
) -> SignalTrackingState:
    state = load_state()
    processed_signals = set(str(x) for x in (state.get("processed_signals") or []))
    ops_policy = _ops_policy(config)
    if bool(ops_policy.get("enabled", False)) and bool(ops_policy.get("carryover_no_next_day_enabled", True)):
        max_age = max(1, _to_int(ops_policy.get("carryover_max_age_days", 2), 2))
        pending_for_replay = _load_pending_signals(max_age_days=max_age)
        if len(pending_for_replay) > 0 and "carry_reason" in pending_for_replay.columns:
            carry_mask = pending_for_replay["carry_reason"].astype(str).str.strip().str.upper().isin(["REVALIDATE_PENDING", "EXTEND_CARRYOVER"])
            if bool(carry_mask.any()):
                processed_signals.update(
                    (
                        pending_for_replay.loc[carry_mask, "code"].astype(str).str.zfill(6)
                        + ":"
                        + pending_for_replay.loc[carry_mask, "signal_date"].astype(str)
                    ).tolist()
                )

    existing_fill_order_ids: set[str] = set()
    existing_trade_sigs: set[str] = set()
    committed_signal_keys: set[str] = set()
    df_fills = read_csv_safe(FILLS)
    same_close_filled_today = 0
    if df_fills is not None and "order_id" in df_fills.columns:
        deduped_fills_df, deduped_fill_rows = _dedupe_partial_exit_fills(df_fills)
        deduped_fills_df, deduped_fill_sig_rows = _dedupe_sell_fills_by_lifecycle_signature(deduped_fills_df)
        deduped_fill_rows = int(deduped_fill_rows) + int(deduped_fill_sig_rows)
        if deduped_fill_rows > 0:
            fills_header = LEGACY_FILLS_HEADER if schema == "legacy" else V411_FILLS_HEADER
            backup_path = _atomic_write_csv_with_backup(FILLS, deduped_fills_df, fills_header)
            print(
                f"[PARTIAL_EXIT_DEDUP] fills dedup applied removed={deduped_fill_rows} "
                f"(partial_order={deduped_fill_rows - int(deduped_fill_sig_rows)}, lifecycle={int(deduped_fill_sig_rows)}) "
                f"backup={backup_path or 'none'}"
            )
            df_fills = deduped_fills_df
        existing_fill_order_ids = set(df_fills["order_id"].astype(str).tolist())
        existing_fill_order_ids.update(
            {
                _normalize_partial_sell_order_id(v)
                for v in df_fills["order_id"].astype(str).tolist()
                if _normalize_partial_sell_order_id(v)
            }
        )
        if "code" in df_fills.columns and "side" in df_fills.columns and "note" in df_fills.columns:
            buy_mask = df_fills["side"].astype(str).str.upper().eq("BUY")
            for _, rr in df_fills.loc[buy_mask].iterrows():
                code = str(rr.get("code", "")).zfill(6)
                signal_day = _extract_signal_date(rr.get("note"))
                note_text = str(rr.get("note", "") or "")
                fill_dt = str(rr.get("datetime", "") or rr.get("ts", "") or "").strip()
                if code and signal_day:
                    committed_signal_keys.add(f"{code}:{signal_day}")
                if (
                    (
                        "entry_timing=same_close" in note_text
                        or "entry_timing=intraday_realtime" in note_text
                    )
                    and "surge_immediate=1" not in note_text
                    and fill_dt.startswith(now_ymd())
                ):
                    same_close_filled_today += 1

    df_trades = read_csv_safe(TRADES)
    if schema == "legacy" and isinstance(df_trades, pd.DataFrame) and len(df_trades) > 0:
        deduped_trades_df, deduped_rows_recovered = _dedupe_recovered_trades_legacy(df_trades)
        deduped_trades_df, deduped_rows_partial = _dedupe_partial_exit_trades_legacy(deduped_trades_df)
        deduped_trades_df, deduped_rows_note = _dedupe_trades_by_note_identity_legacy(deduped_trades_df)
        deduped_trades_df, deduped_rows_lifecycle = _dedupe_sell_trades_by_lifecycle_signature_legacy(deduped_trades_df)
        deduped_rows = int(deduped_rows_recovered) + int(deduped_rows_partial) + int(deduped_rows_note) + int(deduped_rows_lifecycle)
        if deduped_rows > 0:
            backup_path = _atomic_write_csv_with_backup(TRADES, deduped_trades_df, LEGACY_TRADES_HEADER)
            print(
                f"[RECOVERED_DEDUP] trades dedup applied removed={deduped_rows} "
                f"(recovered={deduped_rows_recovered}, partial_exit={deduped_rows_partial}, "
                f"note_identity={deduped_rows_note}, lifecycle={deduped_rows_lifecycle}) "
                f"backup={backup_path or 'none'}"
            )
            df_trades = deduped_trades_df
        sell_trade_recovery = _recover_missing_sell_trades_legacy(
            runtime_ymd=now_ymd(),
            fee_pct=float(fee_pct),
            pos_slip_pct=float(slip_pct),
            sell_tax_pct=float(sell_tax_pct),
        )
        if int(sell_trade_recovery.get("recovered_rows", 0) or 0) > 0:
            print(
                f"[SELL_TRADE_RECOVERY] recovered={sell_trade_recovery.get('recovered_rows')} "
                f"orders={','.join(sell_trade_recovery.get('recovered_order_ids') or [])} "
                f"backup={sell_trade_recovery.get('backup_path') or 'none'}"
            )
            df_trades = read_csv_safe(TRADES)
    committed_source_order_ids: set[str] = set()  # source_order_id 기반 중복방지
    if df_trades is not None and len(df_trades) > 0:
        if "code" in df_trades.columns and "note" in df_trades.columns:
            for _, rr in df_trades.iterrows():
                code = str(rr.get("code", "")).zfill(6)
                signal_day = _extract_signal_date(rr.get("note"))
                if signal_day:
                    processed_signals.add(f"{code}:{signal_day}")
                # source_order_id 추출 → RECOVERED 중복방지용
                _note_str = str(rr.get("note", "") or "")
                _partial_exit = _extract_note_field(_note_str, "partial_exit") in {"1", "true", "TRUE", "yes", "YES"}
                # Only full exits reserve the source order id. Partial exits from the same
                # entry must not block the later full-exit trade for the remaining shares.
                if not _partial_exit:
                    _src_oid_m = re.search(r"source_order_id=([^;]+)", _note_str)
                    if _src_oid_m:
                        committed_source_order_ids.add(_src_oid_m.group(1).strip())
                    # entry_order_id도 등록 (source가 없는 일반 RECOVERED 케이스)
                    _eid_m = re.search(r"entry_order_id=([^;]+)", _note_str)
                    if _eid_m:
                        committed_source_order_ids.add(_eid_m.group(1).strip())

        try:
            for _, rr in df_trades.iterrows():
                if schema == "legacy":
                    existing_trade_sigs.add(_legacy_trade_sig(rr))
                else:
                    existing_trade_sigs.add(_v411_trade_sig(rr))
        except Exception:
            pass

    open_pos: List[Dict[str, Any]] = state.get("open_positions", []) or []
    reconcile_open_summary: Dict[str, Any] = {}
    if schema == "legacy":
        open_pos, reconcile_open_summary = _reconcile_open_positions_with_fills(
            open_positions=open_pos,
            fills_df=df_fills if isinstance(df_fills, pd.DataFrame) else pd.DataFrame(),
            stop_loss=stop_loss,
            take_profit=take_profit,
            trail_pct=trail_pct,
        )
        state["open_positions"] = open_pos
        if reconcile_open_summary.get("added_codes") or reconcile_open_summary.get("removed_codes") or reconcile_open_summary.get("qty_adjusted_codes"):
            print(
                "[RECOVER] open_positions reconciled from fills "
                f"before={reconcile_open_summary.get('state_open_before', 0)} "
                f"after={reconcile_open_summary.get('state_open_after', 0)} "
                f"fills_open_codes={reconcile_open_summary.get('fills_open_codes', 0)} "
                f"added={len(reconcile_open_summary.get('added_codes', []))} "
                f"removed={len(reconcile_open_summary.get('removed_codes', []))} "
                f"qty_adjusted={len(reconcile_open_summary.get('qty_adjusted_codes', []))}"
            )
        restored_sell_progress = sum(_hydrate_position_exit_tags_from_fills(pos, df_fills) for pos in open_pos)
        if restored_sell_progress > 0:
            print(f"[RECOVER] open_positions sell progress restored from fills count={int(restored_sell_progress)}")

    df_fills_backfilled, fills_horizon_note_backfilled = _backfill_horizon_note_in_fills(
        df_fills if isinstance(df_fills, pd.DataFrame) else pd.DataFrame(),
        open_pos,
    )
    if fills_horizon_note_backfilled > 0:
        fills_header = LEGACY_FILLS_HEADER if schema == "legacy" else V411_FILLS_HEADER
        backup_path = _atomic_write_csv_with_backup(FILLS, df_fills_backfilled, fills_header)
        df_fills = df_fills_backfilled
        print(
            f"[HORIZON] fills note backfilled rows={fills_horizon_note_backfilled} "
            f"backup={backup_path or 'none'}"
        )

    for position in open_pos:
        code = norm_code(position.get("code", ""))
        signal_day = re.sub(r"[^0-9]", "", str(position.get("signal_date") or ""))[:8]
        if code and len(signal_day) == 8:
            committed_signal_keys.add(f"{code}:{signal_day}")
    open_codes = set(norm_code(p.get("code", "")) for p in open_pos if norm_code(p.get("code", "")))
    initial_open_count = len(open_pos)
    replay_global_ok = (not replay_require_no_open_positions) or (initial_open_count == 0)
    if replay_enabled:
        print(
            f"[REPLAY] stale_signal_replay on age_days>={replay_min_age_days} "
            + f"require_no_open_positions={replay_require_no_open_positions} "
            + f"initial_open={initial_open_count} active={replay_global_ok}"
        )

    return SignalTrackingState(
        state=state,
        processed_signals=processed_signals,
        existing_fill_order_ids=existing_fill_order_ids,
        existing_trade_sigs=existing_trade_sigs,
        committed_source_order_ids=committed_source_order_ids,
        committed_signal_keys=committed_signal_keys,
        df_fills=df_fills,
        df_trades=df_trades,
        same_close_filled_today=int(same_close_filled_today),
        open_pos=open_pos,
        open_codes=open_codes,
        replay_global_ok=bool(replay_global_ok),
    )
def _prepare_pretrade_runtime(
    candidate_df: pd.DataFrame,
    config: Dict[str, Any],
    state: Dict[str, Any],
    open_pos: List[Dict[str, Any]],
    open_codes: set[str],
    *,
    schema: str,
    stop_loss: float,
    take_profit: Any,
    trail_pct: Any,
    capital_total: float,
    max_gross_exposure_pct: float,
    max_daily_new_exposure_pct: float,
    ddm_enabled: bool,
    ddm_cfg: Dict[str, Any],
    ddm_force_liquidate_pct: float,
) -> Dict[str, Any]:
    if "code" not in candidate_df.columns:
        candidate_df = candidate_df.copy()
        candidate_df["code"] = pd.Series(dtype="object")
    entry_sector_col = "sector_code" if "sector_code" in candidate_df.columns else ("sector" if "sector" in candidate_df.columns else None)
    code_to_sector: Dict[str, str] = {}
    if entry_sector_col:
        for _, rr in candidate_df[["code", entry_sector_col]].dropna(subset=["code"]).iterrows():
            cc = norm_code(rr.get("code", ""))
            if cc:
                code_to_sector[cc] = str(rr.get(entry_sector_col, "") or "").strip()

    gross_cap_krw: Optional[float] = None
    daily_new_cap_krw: Optional[float] = None
    if capital_total > 0:
        gross_cap_krw = capital_total * max_gross_exposure_pct
        daily_new_cap_krw = capital_total * max_daily_new_exposure_pct

    codes = sorted(set(norm_code(c) for c in candidate_df["code"].tolist() if norm_code(c)) | set(open_codes))
    if not codes:
        state["open_positions"] = []
        save_state(state)
        maybe_run_pnl_report()

        # [FIX] Touch reporting files to prevent RUNTIME_CHAIN_GUARD_WARN stale errors
        import json
        for path_obj in [PENDING_STATUS_PATH, RECOVERY_STATUS_PATH, REPLAY_CONSISTENCY_PATH]:
            if path_obj and path_obj.exists():
                try:
                    data = json.loads(path_obj.read_text(encoding="utf-8"))
                    data["generated_at"] = now_ts()
                    path_obj.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                except Exception as e:
                    print(f"[WARN] Failed to touch {path_obj.name}: {e}")

        print("============================================================")
        print(f"[PAPER_ENGINE] ts={now_ts()} schema={schema}")
        print("[PAPER_ENGINE] new_fills=0 new_trades=0 open_positions=0")
        print(f"[PAPER_ENGINE] stop_loss={stop_loss} take_profit={take_profit} trail_pct={trail_pct}")
        print("============================================================")
        return {"early_return": 0}

    px = load_prices_for_codes(config, codes)
    current_open_notional = _compute_current_open_notional(open_pos, px)

    block_same_sector_entry = False
    blocked_sector_value: Optional[str] = None
    sector_concentration = 0.0
    if ddm_enabled and bool(ddm_cfg.get("sector_concentration_block", True)) and entry_sector_col:
        blocked_sector_value, sector_concentration = _ddm_sector_concentration(open_pos, code_to_sector, px=px)
        sector_limit = _ddm_pct01(ddm_cfg.get("sector_concentration_limit", 0.40), 0.40)
        if blocked_sector_value and sector_concentration > sector_limit:
            block_same_sector_entry = True
            print(
                f"[DDM] sector concentration block enabled: sector={blocked_sector_value} concentration={sector_concentration:.2f} > limit={sector_limit:.2f}"
            )

    if capital_total > 0:
        print(f"[RISK_CAP] capital_total={capital_total:.0f} gross_cap={gross_cap_krw:.0f} daily_new_cap={daily_new_cap_krw:.0f} open_notional={current_open_notional:.0f}")

    ddm_liquidation_targets: set[str] = set()
    ddm_liquidation_price_mode = str(ddm_cfg.get("liquidation_price", "close") or "close").strip().lower()
    if ddm_enabled and ddm_force_liquidate_pct > 0 and open_pos and (px is not None) and (not px.empty):
        ddm_liquidation_targets = _ddm_select_liquidation_targets(
            open_pos,
            px,
            ddm_force_liquidate_pct,
            min_positions_for_partial_liquidation=int(
                _ddm_to_float(ddm_cfg.get("min_positions_for_partial_liquidation"), 1)
            ),
        )
        if ddm_liquidation_targets:
            print(
                f"[DDM] forced liquidation targets={len(ddm_liquidation_targets)} / {len(open_pos)} (pct={ddm_force_liquidate_pct:.2f})"
            )

    return {
        "early_return": None,
        "entry_sector_col": entry_sector_col,
        "block_same_sector_entry": block_same_sector_entry,
        "blocked_sector_value": blocked_sector_value,
        "sector_concentration": float(sector_concentration),
        "gross_cap_krw": gross_cap_krw,
        "daily_new_cap_krw": daily_new_cap_krw,
        "px": px,
        "current_open_notional": float(current_open_notional),
        "ddm_liquidation_targets": ddm_liquidation_targets,
        "ddm_liquidation_price_mode": ddm_liquidation_price_mode,
    }
def _init_entry_loop_state(
    *,
    same_close_entry_mode: bool,
    same_close_filled_today: int,
    ops_enabled: bool,
    ops_policy: Dict[str, Any],
    replay_recovery_summary: Dict[str, Any],
    risk_off_enabled: bool,
    market_regime: str,
    d_ref_ymd: str,
    portfolio_state: Optional[Dict[str, Any]] = None,
    p0_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    p0_metrics = (
        ((p0_snapshot.get("kill_switch") or {}).get("metrics") or {})
        if isinstance(p0_snapshot, dict)
        else {}
    )
    p0_rolling_dd_abs = _ddm_pct01(abs(_to_float(p0_metrics.get("max_drawdown_pct"), 0.0)), 0.0)
    return {
        "fills_new": [],
        "trades_new": [],
        "new_count": int(same_close_filled_today) if same_close_entry_mode else 0,
        "new_notional_krw": 0.0,
        "surge_new_count": 0,
        "surge_type_day_counts": {},
        "surge_notional_krw": 0.0,
        "split_notional_krw": 0.0,
        "evaluated_count": 0,
        "no_next_day_count": 0,
        "entry_ready_count": 0,
        "cap_block_count": 0,
        "processed_skip_count": 0,
        "max_new_skip_count": 0,
        "idempotent_skip_count": 0,
        "stale_replay_used_count": 0,
        "open_order_replay_used_count": 0,
        # Entry-loop 기준일은 fills-D가 아니라 실행일(now) 축으로 유지한다.
        # (fills-D와 혼용되면 strict_same_day_only에서 미래/과거 신호 오판정이 발생할 수 있음)
        "today_ymd": str(d_ref_ymd or now_ymd()),
        "pending_carry_rows": [],
        "entry_decisions": [],
        "carry_max_age": max(1, _to_int(ops_policy.get("carryover_max_age_days", 2), 2)) if ops_enabled else 2,
        "replay_due_today_count": int(replay_recovery_summary.get("eligible_rows", 0)) if ops_enabled and bool(ops_policy.get("carryover_no_next_day_enabled", True)) else 0,
        "carryover_market_gate_block": bool(risk_off_enabled) or str(market_regime or "").upper() in {"CRASH", "STAGFLATION"},
        "max_positions_blocked": False,
        "_daily_retry_counts": {},   # code:D → attempt count (retry limit guard)
        "_daily_idem_keys": set(),   # code:signal_id:D idempotency keys
        "_same_code_day_buy_counts": {},
        "fail_closed_triggered": False,
        "fail_closed_reason": "",
        "p0_rolling_dd_abs": float(p0_rolling_dd_abs),
        "p0_rolling_dd_source": "p0.kill_switch.metrics.max_drawdown_pct",
        "ttl_expired_count": 0,
        "retry_blocked_count": 0,
        "exec_quality_blocked_count": 0,
        "quote_stale_blocked_count": 0,
        "close_cutoff_blocked_count": 0,
        "partial_fill_expired_count": 0,
        "portfolio_state": portfolio_state if isinstance(portfolio_state, dict) else {},
        "t2_cash_checks": [],
    }
def _apply_runtime_caps_and_filters(
    candidate_df: pd.DataFrame,
    *,
    max_new: int,
    base_max_new: int,
    market_regime: str,
    regime_info: Dict[str, Any],
    config: Dict[str, Any],
    ops_policy: Dict[str, Any],
    ops_enabled: bool,
    risk_off_hard: bool,
    rank_col: Optional[str],
    max_per_sector_runtime: int,
    trend_overlay_ctx: Optional[Dict[str, Any]] = None,
) -> tuple[pd.DataFrame, int, bool]:
    cap_signal_top_n = int(config.get("cap_signal_top_n", 0) or 0)
    max_per_sector = int(max_per_sector_runtime or 0)

    raw_candidate_count = len(candidate_df)
    shrink_min_candidates = max(1, _to_int(ops_policy.get("universe_shrink_min_candidates", 8), 8))
    universe_shrink_candidates = raw_candidate_count <= shrink_min_candidates if raw_candidate_count > 0 else False
    if ops_enabled and universe_shrink_candidates:
        print(f"[OPS] universe_shrink detected: candidates={raw_candidate_count} <= {shrink_min_candidates}")
        if cap_signal_top_n > 0 and bool(ops_policy.get("universe_shrink_disable_signal_cap", True)):
            print(f"[OPS] universe_shrink -> disable cap_signal_top_n (was {cap_signal_top_n})")
            cap_signal_top_n = 0
        if max_per_sector > 0 and bool(ops_policy.get("universe_shrink_disable_sector_cap", True)):
            print(f"[OPS] universe_shrink -> disable max_per_sector cap (was {max_per_sector})")
            max_per_sector = 0

    probe_floor = compute_dynamic_probe_floor(
        base_max_new=base_max_new,
        market_regime=market_regime,
        regime_info=regime_info,
        cfg=config,
        risk_off_hard=risk_off_hard,
        universe_shrink=universe_shrink_candidates,
    )
    if probe_floor > 0 and max_new < probe_floor:
        old_max_new = max_new
        max_new = min(base_max_new, probe_floor)
        print(f"[OPS] dynamic_probe_floor applied: {old_max_new}->{max_new} (regime={market_regime})")

    tctx = trend_overlay_ctx if isinstance(trend_overlay_ctx, dict) else {}
    if isinstance(candidate_df, pd.DataFrame) and not candidate_df.empty:
        candidate_df = candidate_df.copy()
        candidate_df["code"] = candidate_df["code"].astype(str).str.zfill(6)
        candidate_df["trend_entry_weight"] = 1.0
        if bool(tctx.get("enabled", False)):
            if bool(tctx.get("cutting_active", False)):
                growth_codes = tctx.get("growth_codes", set()) if isinstance(tctx.get("growth_codes", set()), set) else set()
                defensive_codes = tctx.get("defensive_codes", set()) if isinstance(tctx.get("defensive_codes", set()), set) else set()
                growth_w = max(0.10, min(_to_float(tctx.get("growth_entry_weight"), 1.15), 2.0))
                defensive_w = max(0.10, min(_to_float(tctx.get("defensive_entry_weight"), 0.90), 2.0))
                if growth_codes:
                    gmask = candidate_df["code"].isin(growth_codes)
                    candidate_df.loc[gmask, "trend_entry_weight"] = growth_w
                if defensive_codes:
                    dmask = candidate_df["code"].isin(defensive_codes)
                    candidate_df.loc[dmask, "trend_entry_weight"] = defensive_w

            ai_codes = tctx.get("ai_focus_codes", set()) if isinstance(tctx.get("ai_focus_codes", set()), set) else set()
            ai_topn = max(0, int(_to_int(tctx.get("ai_focus_daily_top_n", 0), 0) or 0))
            if ai_codes and ai_topn > 0 and rank_col and rank_col in candidate_df.columns:
                before = len(candidate_df)
                candidate_df["_rank_ai"] = pd.to_numeric(candidate_df[rank_col], errors="coerce").fillna(-1e9)
                ai_df = candidate_df[candidate_df["code"].isin(ai_codes)].copy()
                non_ai_df = candidate_df[~candidate_df["code"].isin(ai_codes)].copy()
                if not ai_df.empty:
                    ai_df = (
                        ai_df.sort_values(["signal_date", "_rank_ai", "code"], ascending=[True, False, True], kind="mergesort")
                        .groupby("signal_date", as_index=False, group_keys=False)
                        .head(ai_topn)
                    )
                    candidate_df = pd.concat([non_ai_df, ai_df], ignore_index=True)
                    candidate_df = candidate_df.sort_values(["signal_date", "_rank_ai", "code"], ascending=[True, False, True], kind="mergesort")
                    after = len(candidate_df)
                    print(f"[TREND_2026] ai_focus_daily_top_n={ai_topn} applied: {before}->{after}")

    candidate_df = _apply_signal_and_sector_caps(
        candidate_df,
        rank_col=rank_col,
        cap_signal_top_n=int(cap_signal_top_n),
        max_per_sector=int(max_per_sector),
    )
    return candidate_df, int(max_new), bool(universe_shrink_candidates)

def _find_split2_target_position(
    open_pos: List[Dict[str, Any]],
    *,
    code: str,
    split_first_order_id: str,
) -> Optional[Dict[str, Any]]:
    for pos in open_pos:
        if norm_code(pos.get("code", "")) != code:
            continue
        if split_first_order_id and str(pos.get("entry_order_id", "") or "").strip() != split_first_order_id:
            continue
        return pos
    return None

def _build_entry_fill_row(
    *,
    schema: str,
    entry_ts_value: str,
    entry_day: str,
    code: str,
    name: str,
    qty: int,
    entry_price: float,
    order_id: str,
    note_text: str,
    fee_pct: float,
    row_slip_pct: float,
) -> List[Any]:
    if schema == "legacy":
        return [entry_ts_value, code, "BUY", qty, entry_price, order_id, note_text]
    fee = calc_entry_fee(entry_price, qty, fee_pct)
    slp = float(entry_price) * float(qty) * float(row_slip_pct)
    return [
        entry_ts_value,
        entry_day,
        code,
        name,
        "BUY",
        qty,
        entry_price,
        round(fee, 6),
        round(slp, 6),
        order_id,
        note_text,
    ]


def _archive_entry_source_payload(
    *,
    row: Any,
    cfg: Dict[str, Any],
    entry_source_kind: str,
    candidate_snapshot_id: str,
    code: str,
    name: str,
    effective_signal_date: str,
    entry_day: str,
    entry_ts_value: str,
    lineage: Dict[str, Any],
    qty: int,
    entry_price: float,
    horizon_label: str,
    row_entry_timing: str,
    fallback_note: str,
    is_surge_immediate: bool,
    has_surge_type_policy: bool,
) -> None:
    source_row_payload_raw = dict(row) if hasattr(row, "items") else {}
    source_row_payload = _entry_source_payload_with_lineage(source_row_payload_raw, cfg)
    _archive_entry_source_row(
        source_row_payload,
        {
            "archived_at": now_ts(),
            "run_label": str(RUN_LABEL or ""),
            "paper_session_id": str(PAPER_SESSION_ID or ""),
            "entry_source_kind": entry_source_kind,
            "candidate_snapshot_id": candidate_snapshot_id,
            "code": code,
            "name": name,
            "signal_date": effective_signal_date,
            "entry_day": entry_day,
            "entry_ts": entry_ts_value,
            "entry_order_id": lineage["entry_order_id"],
            "entry_intent_id": lineage["entry_intent_id"],
            "entry_trace_id": lineage["entry_trace_id"],
            "replay_chain_id": lineage["replay_chain_id"],
            "qty": qty,
            "entry_price": entry_price,
            "horizon": horizon_label,
            "entry_timing": row_entry_timing,
            "fallback_stage": str(fallback_note or "").replace("fallback_stage=", ""),
            "is_surge_immediate": 1 if is_surge_immediate else 0,
            "surge_type": str(row.get("surge_type", "") or "") if (is_surge_immediate or has_surge_type_policy) else "",
            "surge_score_final": str(row.get("surge_score_final", row.get("final_score", "")) or ""),
            "surge_rvol20": str(row.get("surge_rvol20", row.get("rvol20", "")) or ""),
            "surge_spread_bps": str(row.get("surge_spread_bps", row.get("spread_bps", "")) or ""),
            "source_entry_decision": str(row.get("source_entry_decision", "") or ""),
            "source_entry_reason": str(row.get("source_entry_reason", "") or ""),
            "source_entry_allowed": _source_entry_cell_text(row.get("source_entry_allowed", "")),
            "source_entry_blocked": _source_entry_cell_text(row.get("source_entry_blocked", "")),
            "source_exclude_reasons": str(row.get("exclude_reasons", "") or ""),
            "surge_paper_probe_allowed": str(row.get("surge_paper_probe_allowed", "") or ""),
            "surge_paper_probe_block_reasons": str(row.get("surge_paper_probe_block_reasons", "") or ""),
            "signal_id": str(row.get("signal_id", "") or ""),
            "captured_at": str(row.get("captured_at", "") or ""),
            "source_row_json": json.dumps(_json_safe(source_row_payload), ensure_ascii=False, separators=(",", ":")),
        },
    )








def _normalize_entry_price_for_entry_guard(entry_price: Any) -> Dict[str, Any]:
    try:
        normalized = float(entry_price)
    except Exception:
        normalized = 0.0
    valid = bool(math.isfinite(float(normalized)) and float(normalized) > 0.0)
    return {"entry_price": float(normalized), "valid": valid}

def _evaluate_entry_execution_quality_guard(
    ops_policy: Dict[str, Any],
    px: pd.DataFrame,
    *,
    code: str,
    effective_signal_date: str,
    entry_price: float,
    is_open_order_replay: bool,
) -> Dict[str, Any]:
    exec_q_max = float(ops_policy.get("exec_quality_max_slippage_pct", 0.0)) if isinstance(ops_policy, dict) else 0.0
    if exec_q_max <= 0.0 or float(entry_price or 0.0) <= 0.0 or is_open_order_replay:
        return {"blocked": False, "reason": ""}
    eq_ref_ohlc = get_ohlc(px, code, effective_signal_date)
    eq_ref_close = float(eq_ref_ohlc.get("close", 0.0) or 0.0) if eq_ref_ohlc else 0.0
    if eq_ref_close <= 0:
        return {"blocked": False, "reason": "", "ref_close": eq_ref_close}
    eq_slip = abs(float(entry_price) - eq_ref_close) / eq_ref_close
    if eq_slip <= exec_q_max:
        return {
            "blocked": False,
            "reason": "",
            "ref_close": eq_ref_close,
            "slippage": eq_slip,
            "max_slippage": exec_q_max,
        }
    print(
        f"[EXEC_QUALITY_FAIL] code={code} entry={float(entry_price):.2f} "
        f"ref_close={eq_ref_close:.2f} slippage={eq_slip:.4f} > max={exec_q_max:.4f}"
    )
    return {
        "blocked": True,
        "reason": "EXEC_QUALITY_FAIL",
        "ref_close": eq_ref_close,
        "slippage": eq_slip,
        "max_slippage": exec_q_max,
    }

def _resolve_missing_entry_ohlc_action(
    row: Any,
    ops_policy: Dict[str, Any],
    *,
    ops_enabled: bool,
    strict_same_day: bool,
    use_same_close_today: bool,
    fallback_stage: int,
    effective_signal_date: str,
    entry_day: str,
    today_ymd: str,
    code: str,
    name: str,
    row_entry_timing: str,
    is_surge_immediate: bool,
) -> Dict[str, Any]:
    fbp = ops_policy.get("entry_fallback_policy", {}) if isinstance(ops_policy, dict) else {}
    fb_enabled = bool(fbp.get("enabled") and ops_enabled)
    next_stage = int(fallback_stage) + 1
    max_stage = int(fbp.get("max_stage", 3))
    if fb_enabled and next_stage > max_stage:
        print(f"[SKIP_FALLBACK_EXHAUSTED] code={code} stage={fallback_stage} max_stage={max_stage}")
        return {"action": "RECORD_BLOCK", "reason": "FALLBACK_EXHAUSTED"}

    if ops_enabled and use_same_close_today:
        if strict_same_day:
            print(f"[EXPIRED_SAME_DAY_UNFILLED] code={code} signal_date={effective_signal_date} reason=PRICE_MISSING")
            return {"action": "SKIP", "reason": "EXPIRED_SAME_DAY_UNFILLED"}
        print(
            f"[CARRY_SAME_DAY_PRICE_WAIT] code={code} signal_date={effective_signal_date} "
            f"entry_day={entry_day} fallback_stage={next_stage if fb_enabled else fallback_stage}"
        )
        return {
            "action": "APPEND_CARRY",
            "carry_row": {
                "signal_date": effective_signal_date,
                "code": code,
                "name": name,
                "carry_reason": "ENTRY_RETRY_READY",
                "carry_origin_reason": "SAME_DAY_PRICE_MISSING",
                "captured_at": now_ts(),
                "carryover_count": max(1, _to_int(row.get("carryover_count", 0), 0) + 1),
                "fallback_stage": next_stage if fb_enabled else fallback_stage,
                "score": row.get("score", None),
                "final_score": row.get("final_score", None),
                "entry_timing": row_entry_timing,
                "surge_type_entry_timing": row.get("surge_type_entry_timing", ""),
                "_surge_immediate": 1 if is_surge_immediate else 0,
                "surge_type": row.get("surge_type", ""),
                "surge_per_symbol_alloc_pct": row.get("surge_per_symbol_alloc_pct", None),
                "surge_total_alloc_pct": row.get("surge_total_alloc_pct", None),
                "surge_type_qty_multiplier": row.get("surge_type_qty_multiplier", None),
                "surge_type_policy_note": row.get("surge_type_policy_note", ""),
                "surge_type_first_ratio": row.get("surge_type_first_ratio", None),
                "surge_type_stop_pct": row.get("surge_type_stop_pct", None),
                "surge_type_tp_pct": row.get("surge_type_tp_pct", None),
                "surge_type_max_hold_days": row.get("surge_type_max_hold_days", None),
                "v_accel": row.get("v_accel", None),
                "ret1_pct": row.get("ret1_pct", None),
                "atr14_pct": row.get("atr14_pct", None),
            },
        }

    if (
        ops_enabled
        and bool(ops_policy.get("carryover_no_next_day_enabled", True))
        and len(entry_day) == 8
        and entry_day > today_ymd
    ):
        if strict_same_day:
            print(
                f"[EXPIRED_ENTRY_DAY_WAIT] code={code} signal_date={effective_signal_date} "
                f"entry_day={entry_day} reason=FAIL_CLOSED"
            )
            return {"action": "SKIP", "reason": "EXPIRED_ENTRY_DAY_WAIT"}
        print(
            f"[CARRY_ENTRY_DAY_WAIT] code={code} signal_date={effective_signal_date} "
            f"entry_day={entry_day} fallback_stage={next_stage if fb_enabled else fallback_stage}"
        )
        return {
            "action": "APPEND_CARRY",
            "carry_row": {
                "signal_date": effective_signal_date,
                "code": code,
                "name": name,
                "carry_reason": "ENTRY_RETRY_READY",
                "carry_origin_reason": "ENTRY_DAY_WAIT",
                "entry_day_override": entry_day,
                "captured_at": now_ts(),
                "carryover_count": max(1, _to_int(row.get("carryover_count", 0), 0) + 1),
                "fallback_stage": next_stage if fb_enabled else fallback_stage,
                "score": row.get("score", None),
                "final_score": row.get("final_score", None),
                "entry_timing": row_entry_timing,
                "surge_type_entry_timing": row.get("surge_type_entry_timing", ""),
                "_surge_immediate": 1 if is_surge_immediate else 0,
                "surge_type": row.get("surge_type", ""),
                "surge_per_symbol_alloc_pct": row.get("surge_per_symbol_alloc_pct", None),
                "surge_total_alloc_pct": row.get("surge_total_alloc_pct", None),
                "surge_type_qty_multiplier": row.get("surge_type_qty_multiplier", None),
                "surge_type_policy_note": row.get("surge_type_policy_note", ""),
                "surge_type_first_ratio": row.get("surge_type_first_ratio", None),
                "surge_type_stop_pct": row.get("surge_type_stop_pct", None),
                "surge_type_tp_pct": row.get("surge_type_tp_pct", None),
                "surge_type_max_hold_days": row.get("surge_type_max_hold_days", None),
            },
        }
    return {"action": "SKIP", "reason": "NO_OHLC"}

def _resolve_entry_price_with_fallback(
    row: Any,
    cfg: Dict[str, Any],
    fb_policy: Dict[str, Any],
    px: pd.DataFrame,
    *,
    code: str,
    entry_day: str,
    effective_signal_date: str,
    ohlc: Dict[str, Any],
    fallback_stage: int,
    fb_enabled: bool,
    fb_max_stage: int,
    use_same_close_today: bool,
    use_intraday_realtime_entry: bool,
    strict_same_day: bool,
    is_surge_immediate: bool,
) -> Dict[str, Any]:
    entry_price = 0.0
    entry_ts_value = f"{entry_day}T09:00:00"
    fallback_note = ""
    if fb_enabled and fallback_stage > fb_max_stage:
        print(f"[SKIP_FALLBACK_EXHAUSTED] code={code} stage={fallback_stage} max_stage={fb_max_stage}")
        return {"action": "SKIP", "reason": "FALLBACK_EXHAUSTED"}
    if strict_same_day and fallback_stage > 0:
        print(f"[BLOCK_FALLBACK_STAGE] code={code} stage={fallback_stage} reason=STRICT_STAGE0_ONLY")
        return {"action": "RECORD_BLOCK", "reason": "BLOCK_FALLBACK_STAGE"}

    if fb_enabled and use_same_close_today and fallback_stage == 0:
        if use_intraday_realtime_entry:
            realtime_px = float(ohlc.get("close", 0.0) or 0.0)
            if realtime_px <= 0:
                realtime_px = float(ohlc.get("open", 0.0) or 0.0)
            entry_price = realtime_px
            entry_ts_value = f"{entry_day}T{datetime.now().strftime('%H:%M:%S')}"
            fallback_note = "fallback_stage=0(intraday_realtime)"
        else:
            high_px = float(ohlc.get("high", 0.0) or 0.0)
            low_px = float(ohlc.get("low", 0.0) or 0.0)
            close_px = float(ohlc.get("close", 0.0) or 0.0)
            day_range = high_px - low_px
            close_pos = ((close_px - low_px) / day_range) if day_range > 0 else 0.0
            day_range_pct = (day_range / close_px) if close_px > 0 else 1.0
            v_accel = float(_to_float(row.get("v_accel"), 0.0) or 0.0)
            auction_action, auction_reason, auction_meta = _normal_close_auction_decision(
                cfg,
                close_pos=float(close_pos),
                day_range_pct=float(day_range_pct),
                v_accel=float(v_accel),
            )
            for auction_key, auction_val in auction_meta.items():
                row[auction_key] = auction_val
            row["normal_close_auction_action"] = auction_action
            row["normal_close_auction_reason"] = auction_reason
            if auction_action == "BLOCK":
                print(
                    f"[NORMAL_CLOSE_AUCTION_BLOCK] code={code} reason={auction_reason} "
                    f"close_pos={close_pos:.3f} day_range_pct={day_range_pct:.3f} v_accel={v_accel:.3f}"
                )
                return {"action": "RECORD_BLOCK", "reason": auction_reason}
            if auction_action == "REDUCE":
                print(
                    f"[NORMAL_CLOSE_AUCTION_REDUCE] code={code} reason={auction_reason} "
                    f"close_pos={close_pos:.3f} day_range_pct={day_range_pct:.3f} v_accel={v_accel:.3f}"
                )
            entry_price = close_px
            entry_ts_value = f"{entry_day}T15:20:00"
            fallback_note = "fallback_stage=0(close_auction)"
    elif fb_enabled and fallback_stage == 1:
        sig_ohlc = get_ohlc(px, code, effective_signal_date)
        sig_close = float(sig_ohlc.get("close", 0.0) or 0.0) if sig_ohlc else 0.0
        open_px = float(ohlc.get("open", 0.0) or 0.0)
        gap_max = float(_to_float(fb_policy.get("next_open_gap_up_max_pct"), 0.03))
        limit_pct = float(_to_float(fb_policy.get("next_open_limit_pct"), 0.01))
        if sig_close <= 0 or open_px <= 0:
            if is_surge_immediate:
                print(f"[SKIP_SURGE_PRICE_INVALID] code={code} stage=1 sig_close={sig_close:.4f} open={open_px:.4f}")
            return {"action": "SKIP", "reason": "PRICE_INVALID_STAGE1"}
        gap = (open_px - sig_close) / sig_close
        limit_px = sig_close * (1.0 + limit_pct)
        if gap >= gap_max or open_px > limit_px:
            next_day2 = next_trading_date(px, code, entry_day)
            print(
                f"[CARRY_NEXT_OPEN_UNFILLED] code={code} open={open_px:.2f} "
                f"sig_close={sig_close:.2f} gap={gap:.3f} limit_px={limit_px:.2f}"
            )
            return {
                "action": "CARRY_NEXT_OPEN_UNFILLED",
                "next_day2": next_day2,
                "append_carry": bool(next_day2 and next_day2 != entry_day and fallback_stage + 1 <= fb_max_stage),
            }
        entry_price = open_px
        entry_ts_value = f"{entry_day}T09:00:00"
        fallback_note = "fallback_stage=1(next_open_limit)"
    elif fb_enabled and fallback_stage == 2:
        sig_ohlc = get_ohlc(px, code, effective_signal_date)
        sig_close = float(sig_ohlc.get("close", 0.0) or 0.0) if sig_ohlc else 0.0
        low_px = float(ohlc.get("low", 0.0) or 0.0)
        close_px = float(ohlc.get("close", 0.0) or 0.0)
        limit_pct2 = float(_to_float(fb_policy.get("intraday_limit_pct"), 0.01))
        if sig_close <= 0 or low_px <= 0:
            if is_surge_immediate:
                print(f"[SKIP_SURGE_PRICE_INVALID] code={code} stage=2 sig_close={sig_close:.4f} low={low_px:.4f}")
            return {"action": "SKIP", "reason": "PRICE_INVALID_STAGE2"}
        limit_px2 = sig_close * (1.0 + limit_pct2)
        if low_px > limit_px2:
            print(
                f"[DROP_SIGNAL_UNFILLED] code={code} stage=2 low={low_px:.2f} "
                f"limit_px={limit_px2:.2f} reason=INTRADAY_LIMIT_UNFILLED"
            )
            return {"action": "SKIP", "reason": "INTRADAY_LIMIT_UNFILLED"}
        entry_price = min(limit_px2, close_px if close_px > 0 else limit_px2)
        entry_ts_value = f"{entry_day}T09:05:00"
        fallback_note = "fallback_stage=2(intraday_limit)"
    else:
        if use_same_close_today:
            entry_price = float(ohlc["close"])
            if use_intraday_realtime_entry:
                entry_ts_value = f"{entry_day}T{datetime.now().strftime('%H:%M:%S')}"
            else:
                entry_ts_value = f"{entry_day}T15:20:00"
        else:
            entry_price = float(ohlc["open"])
            entry_ts_value = f"{entry_day}T09:00:00"
    return {
        "action": "OK",
        "entry_price": float(entry_price),
        "entry_ts_value": entry_ts_value,
        "fallback_note": fallback_note,
    }

def _apply_sector_corr_hrp_qty_adjustment(
    sector_corr_ctx: Dict[str, Any],
    sector_risk_note_parts: List[str],
    *,
    code: str,
    qty: int,
    row_sector_for_corr: str,
    allow_block: bool,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "blocked": False,
        "reason": "",
        "qty": int(qty),
        "reduce_count": 0,
    }
    if int(qty) <= 0 or (not bool(sector_corr_ctx.get("enabled", False))) or (not row_sector_for_corr):
        return result

    corr_score = float(
        _to_float(
            (sector_corr_ctx.get("sector_corr_score", {}) or {}).get(row_sector_for_corr, 0.0),
            0.0,
        )
        or 0.0
    )
    corr_reduce_thr = float(_to_float(sector_corr_ctx.get("reduce_threshold_abs_corr"), 0.85))
    corr_block_thr = float(_to_float(sector_corr_ctx.get("block_threshold_abs_corr"), 0.97))
    corr_reduce_mult = float(_to_float(sector_corr_ctx.get("reduce_qty_multiplier"), 0.75))
    corr_allow_block = bool(allow_block) and bool(sector_corr_ctx.get("allow_block", False))
    if corr_allow_block and corr_score >= corr_block_thr:
        print(
            f"[SECTOR_CORR_BLOCK] code={code} sector={row_sector_for_corr} "
            f"corr_score={corr_score:.3f} >= {corr_block_thr:.3f}"
        )
        result.update({
            "blocked": True,
            "reason": "SECTOR_CORR_BLOCK",
            "corr_score": corr_score,
            "corr_block_thr": corr_block_thr,
        })
        return result

    new_qty = int(qty)
    if corr_score >= corr_reduce_thr and 0.0 < corr_reduce_mult < 1.0:
        old_qty = int(new_qty)
        new_qty = max(1, int(math.floor(float(new_qty) * corr_reduce_mult)))
        if new_qty < old_qty:
            result["reduce_count"] = int(result.get("reduce_count", 0) or 0) + 1
            print(
                f"[SECTOR_CORR_REDUCE] code={code} sector={row_sector_for_corr} "
                f"corr_score={corr_score:.3f} qty={old_qty}->{new_qty} mult={corr_reduce_mult:.2f}"
            )
            sector_risk_note_parts.extend([
                "sector_corr_reduce=1",
                f"sector_corr_score={corr_score:.3f}",
                f"sector_corr_mult={corr_reduce_mult:.2f}",
                f"sector_corr_qty={old_qty}->{new_qty}",
            ])

    hrp_mult = float(
        _to_float(
            (sector_corr_ctx.get("sector_hrp_qty_multiplier", {}) or {}).get(row_sector_for_corr, 1.0),
            1.0,
        )
        or 1.0
    )
    if 0.0 < hrp_mult < 1.0:
        old_qty = int(new_qty)
        new_qty = max(1, int(math.floor(float(new_qty) * hrp_mult)))
        if new_qty < old_qty:
            result["reduce_count"] = int(result.get("reduce_count", 0) or 0) + 1
            hrp_w = float(
                _to_float(
                    (sector_corr_ctx.get("sector_hrp_weight", {}) or {}).get(row_sector_for_corr, 0.0),
                    0.0,
                )
                or 0.0
            )
            exp_w = float(
                _to_float(
                    (sector_corr_ctx.get("sector_exposure_share", {}) or {}).get(row_sector_for_corr, 0.0),
                    0.0,
                )
                or 0.0
            )
            print(
                f"[SECTOR_HRP_REDUCE] code={code} sector={row_sector_for_corr} "
                f"qty={old_qty}->{new_qty} mult={hrp_mult:.2f} "
                f"hrp_weight={hrp_w:.3f} exposure_share={exp_w:.3f}"
            )
            sector_risk_note_parts.extend([
                "sector_hrp_reduce=1",
                f"sector_hrp_mult={hrp_mult:.2f}",
                f"sector_hrp_qty={old_qty}->{new_qty}",
                f"sector_hrp_weight={hrp_w:.3f}",
                f"sector_exposure_share={exp_w:.3f}",
            ])
    result["qty"] = int(new_qty)
    return result

def _fit_surge_budget_allocation(
    row: Any,
    cfg: Dict[str, Any],
    *,
    qty: int,
    entry_price: float,
    capital_total: float,
    surge_notional_krw: float,
    is_surge_immediate: bool,
) -> Dict[str, Any]:
    per_symbol_alloc_pct = _pct01_from_config(row.get("surge_per_symbol_alloc_pct", 0.02), 0.02)
    total_alloc_pct = _pct01_from_config(row.get("surge_total_alloc_pct", 0.06), 0.06)
    surge_total_budget = float(capital_total) * float(total_alloc_pct) if float(capital_total) > 0 else 0.0
    surge_budget_left = max(0.0, surge_total_budget - surge_notional_krw) if is_surge_immediate else float("inf")
    if surge_budget_left <= 0:
        return {
            "blocked": True,
            "reason": "SURGE_BUDGET_EXHAUSTED",
            "surge_budget_left": surge_budget_left,
            "surge_total_budget": surge_total_budget,
            "target_notional": 0.0,
            "qty_from_budget": 0,
        }

    target_notional = float(capital_total) * float(per_symbol_alloc_pct) if float(capital_total) > 0 else 0.0
    if target_notional <= 0:
        return {
            "blocked": True,
            "reason": "SURGE_INVALID_TARGET_NOTIONAL",
            "surge_budget_left": surge_budget_left,
            "surge_total_budget": surge_total_budget,
            "target_notional": target_notional,
            "qty_from_budget": 0,
        }

    split_entry_cfg = cfg.get("split_entry", {}) if isinstance(cfg.get("split_entry", {}), dict) else {}
    if bool(split_entry_cfg.get("enabled", False)):
        type_first_ratio = float(_to_float(row.get("surge_type_first_ratio"), -1.0))
        surge_first_ratio = float(
            type_first_ratio if 0.0 < type_first_ratio <= 1.0
            else split_entry_cfg.get("surge_first_ratio", 1.0)
            or 1.0
        )
        target_notional *= max(0.0, min(1.0, surge_first_ratio))

    target_notional = min(target_notional, surge_budget_left)
    surge_policy_cfg = cfg.get("surge_entry_policy", {}) if isinstance(cfg.get("surge_entry_policy", {}), dict) else {}
    min_qty_surge = max(1, _to_int(surge_policy_cfg.get("min_qty", 1), 1))
    if (
        (not math.isfinite(float(entry_price)))
        or float(entry_price) <= 0
        or (not math.isfinite(float(target_notional)))
        or float(target_notional) <= 0
    ):
        return {
            "blocked": True,
            "reason": "SURGE_INVALID_PRICE_OR_BUDGET",
            "surge_budget_left": surge_budget_left,
            "surge_total_budget": surge_total_budget,
            "target_notional": target_notional,
            "qty_from_budget": 0,
        }

    qty_from_budget = int(math.floor(target_notional / float(entry_price)))
    if qty_from_budget < min_qty_surge:
        return {
            "blocked": True,
            "reason": "SURGE_BUDGET_MIN_QTY",
            "surge_budget_left": surge_budget_left,
            "surge_total_budget": surge_total_budget,
            "target_notional": target_notional,
            "qty_from_budget": qty_from_budget,
        }

    old_qty = int(qty)
    new_qty = min(old_qty, int(qty_from_budget))
    return {
        "blocked": False,
        "adjusted": int(new_qty) < int(old_qty),
        "risk_qty_cap": int(old_qty) < int(qty_from_budget),
        "old_qty": int(old_qty),
        "qty": int(new_qty),
        "surge_budget_left": surge_budget_left,
        "surge_total_budget": surge_total_budget,
        "target_notional": target_notional,
        "qty_from_budget": int(qty_from_budget),
    }

def _apply_surge_news_overheat_qty_reductions(
    row: Any,
    cfg: Dict[str, Any],
    sector_risk_note_parts: List[str],
    *,
    code: str,
    qty: int,
    is_surge_immediate: bool,
) -> int:
    new_qty = int(qty)
    if new_qty > 0 and is_surge_immediate:
        type_mult = float(_to_float(row.get("surge_type_qty_multiplier"), 1.0))
        if 0.0 < type_mult < 1.0:
            old_qty = int(new_qty)
            new_qty = max(1, int(math.floor(float(new_qty) * type_mult)))
            if new_qty < old_qty:
                type_note = str(row.get("surge_type_policy_note", "") or "").strip()
                print(f"[SURGE_TYPE_REDUCE] code={code} qty={old_qty}->{new_qty} mult={type_mult:.2f} {type_note}")
                sector_risk_note_parts.append(f"surge_type_qty={old_qty}->{new_qty}")
                if type_note:
                    sector_risk_note_parts.append(type_note)
    if new_qty > 0 and is_surge_immediate:
        exec_quality_mult, exec_quality_note = _surge_execution_quality_qty_decision(row, cfg, is_surge=is_surge_immediate)
        if 0.0 < exec_quality_mult < 1.0:
            old_qty = int(new_qty)
            new_qty = max(1, int(math.floor(float(new_qty) * exec_quality_mult)))
            if new_qty < old_qty:
                print(f"[SURGE_EXEC_QUALITY_SIZE] code={code} qty={old_qty}->{new_qty} {exec_quality_note}")
                sector_risk_note_parts.append(f"surge_exec_quality_qty={old_qty}->{new_qty}")
                if exec_quality_note:
                    sector_risk_note_parts.append(exec_quality_note)
    if new_qty > 0:
        overheat_mult, overheat_note = _overheat_qty_decision(row, cfg, is_surge=is_surge_immediate)
        if 0.0 < overheat_mult < 1.0:
            old_qty = int(new_qty)
            new_qty = max(1, int(math.floor(float(new_qty) * overheat_mult)))
            if new_qty < old_qty:
                print(f"[OVERHEAT_REDUCE] code={code} qty={old_qty}->{new_qty} {overheat_note}")
                sector_risk_note_parts.append(f"overheat_qty={old_qty}->{new_qty}")
                if overheat_note:
                    sector_risk_note_parts.append(overheat_note)
    if new_qty > 0 and not is_surge_immediate:
        news_implication_policy = cfg.get("news_implication_entry_policy", {}) if isinstance(cfg.get("news_implication_entry_policy"), dict) else {}
        if bool(news_implication_policy.get("enabled", True)):
            reduce_rows = float(_to_float(row.get("news_implication_reduce_size_rows"), 0.0) or 0.0)
            reduce_observe_only = _truthy(row.get("news_implication_reduce_size_observe_only"))
            reduce_mult = float(_to_float(row.get("news_implication_reduce_size_multiplier"), 1.0))
            reduce_mult = max(0.0, min(1.0, reduce_mult))
            if reduce_rows > 0.0 and (not reduce_observe_only) and 0.0 < reduce_mult < 1.0:
                old_qty = int(new_qty)
                new_qty = max(1, int(math.floor(float(new_qty) * reduce_mult)))
                if new_qty < old_qty:
                    print(f"[NEWS_IMPLICATION_REDUCE] code={code} qty={old_qty}->{new_qty} mult={reduce_mult:.2f}")
                    sector_risk_note_parts.extend([
                        "news_implication_reduce=1",
                        f"news_implication_mult={reduce_mult:.2f}",
                        f"news_implication_qty={old_qty}->{new_qty}",
                    ])
        topic_effect = str(row.get("news_topic_execution_effect", "") or "").strip().lower()
        if topic_effect == "reduce_size":
            topic_mult = float(_to_float(row.get("news_topic_reduce_size_multiplier"), 1.0))
            topic_mult = max(0.0, min(1.0, topic_mult))
            if 0.0 < topic_mult < 1.0:
                old_qty = int(new_qty)
                new_qty = max(1, int(math.floor(float(new_qty) * topic_mult)))
                if new_qty < old_qty:
                    print(f"[NEWS_TOPIC_REDUCE] code={code} qty={old_qty}->{new_qty} mult={topic_mult:.2f}")
                    sector_risk_note_parts.extend([
                        "news_topic_reduce=1",
                        f"news_topic_effect={topic_effect}",
                        f"news_topic_mult={topic_mult:.2f}",
                        f"news_topic_qty={old_qty}->{new_qty}",
                    ])
    return int(new_qty)

def _fit_split_second_budget(
    *,
    qty: int,
    entry_price: float,
    capital_total: float,
    split_alloc_pct: float,
    split_notional_krw: float,
) -> Dict[str, Any]:
    split_total_budget = float(capital_total) * float(split_alloc_pct) if float(capital_total) > 0 else 0.0
    split_budget_left = max(0.0, split_total_budget - split_notional_krw)
    if split_budget_left <= 0:
        return {
            "blocked": True,
            "reason": "budget_left",
            "split_budget_left": split_budget_left,
            "split_total_budget": split_total_budget,
        }
    split_qty_from_budget = int(math.floor(split_budget_left / float(entry_price)))
    if split_qty_from_budget <= 0:
        return {
            "blocked": True,
            "reason": "qty_from_budget",
            "split_budget_left": split_budget_left,
            "split_total_budget": split_total_budget,
            "split_qty_from_budget": split_qty_from_budget,
        }
    if int(qty) > split_qty_from_budget:
        return {
            "blocked": False,
            "adjusted": True,
            "old_qty": int(qty),
            "qty": int(split_qty_from_budget),
            "split_budget_left": split_budget_left,
            "split_total_budget": split_total_budget,
            "split_qty_from_budget": split_qty_from_budget,
        }
    return {
        "blocked": False,
        "adjusted": False,
        "qty": int(qty),
        "split_budget_left": split_budget_left,
        "split_total_budget": split_total_budget,
        "split_qty_from_budget": split_qty_from_budget,
    }



def _fit_ai_single_cap(
    *,
    qty: int,
    entry_price: float,
    capital_total: float,
    t_ai_single_cap_pct: float,
) -> Dict[str, Any]:
    cap_notional = float(capital_total) * float(t_ai_single_cap_pct)
    cap_qty = int(math.floor(cap_notional / float(entry_price)))
    if cap_qty <= 0:
        return {
            "blocked": True,
            "cap_notional": cap_notional,
            "cap_qty": cap_qty,
        }
    if int(qty) > cap_qty:
        return {
            "blocked": False,
            "adjusted": True,
            "old_qty": int(qty),
            "qty": int(cap_qty),
            "cap_notional": cap_notional,
            "cap_qty": cap_qty,
        }
    return {
        "blocked": False,
        "adjusted": False,
        "qty": int(qty),
        "cap_notional": cap_notional,
        "cap_qty": cap_qty,
    }

def _apply_min_qty_verification_limit(
    cfg: Dict[str, Any],
    *,
    qty: int,
    entry_price: float,
    entry_cost_buffer: float,
    is_open_order_replay: bool,
) -> Dict[str, Any]:
    min_qty_runtime = max(1, _to_int(cfg.get("min_qty", 1), 1))
    min_qty_verification_loop = _minimum_quantity_verification_cfg(cfg)
    old_qty = int(qty)
    new_qty = int(qty)
    adjusted = False
    if bool(min_qty_verification_loop.get("enabled", False)) and not is_open_order_replay:
        mqv_max_qty = max(1, _to_int(min_qty_verification_loop.get("max_qty", 1), 1))
        if new_qty > mqv_max_qty:
            new_qty = int(mqv_max_qty)
            adjusted = True
    entry_notional = float(entry_price) * float(new_qty)
    entry_notional_for_cap = entry_notional * float(entry_cost_buffer)
    return {
        "min_qty_runtime": int(min_qty_runtime),
        "qty": int(new_qty),
        "old_qty": int(old_qty),
        "adjusted": bool(adjusted),
        "entry_notional": float(entry_notional),
        "entry_notional_for_cap": float(entry_notional_for_cap),
    }

def _fit_entry_qty_to_caps(
    *,
    qty: int,
    entry_price: float,
    entry_cost_buffer: float,
    entry_notional_for_cap: float,
    min_qty_runtime: int,
    cfg: Dict[str, Any],
    is_surge_immediate: bool,
    gross_cap_krw: Optional[float],
    daily_new_cap_krw: Optional[float],
    current_open_notional: float,
    new_notional_krw: float,
) -> Dict[str, Any]:
    allowed_notional = float("inf")
    allowed_reason = ""
    if gross_cap_krw is not None:
        gross_left = float(gross_cap_krw) - float(current_open_notional + new_notional_krw)
        if gross_left < allowed_notional:
            allowed_notional = gross_left
            allowed_reason = "gross_cap"
    if daily_new_cap_krw is not None:
        daily_left = float(daily_new_cap_krw) - float(new_notional_krw)
        if daily_left < allowed_notional:
            allowed_notional = daily_left
            allowed_reason = "daily_cap"
    if allowed_notional == float("inf") or entry_notional_for_cap <= allowed_notional + 1e-9:
        return {"blocked": False, "adjusted": False, "qty": int(qty), "allowed_reason": allowed_reason, "allowed_notional": allowed_notional}
    qty_fit = int(math.floor(max(0.0, allowed_notional) / (float(entry_price) * float(entry_cost_buffer))))
    min_qty_req = int(min_qty_runtime)
    if is_surge_immediate:
        min_qty_req = max(min_qty_req, max(1, _to_int(cfg.get("surge_entry_policy", {}).get("min_qty", 1), 1)))
    if qty_fit >= min_qty_req:
        new_qty = int(qty_fit)
        entry_notional = float(entry_price) * float(new_qty)
        return {
            "blocked": False,
            "adjusted": True,
            "qty": new_qty,
            "old_qty": int(qty),
            "entry_notional": entry_notional,
            "entry_notional_for_cap": entry_notional * float(entry_cost_buffer),
            "allowed_reason": allowed_reason,
            "allowed_notional": allowed_notional,
        }
    return {
        "blocked": True,
        "adjusted": False,
        "qty": int(qty),
        "allowed_reason": allowed_reason,
        "allowed_notional": allowed_notional,
    }


def _post_entry_cap_block_reason(
    *,
    entry_notional_for_cap: float,
    gross_cap_krw: Optional[float],
    daily_new_cap_krw: Optional[float],
    current_open_notional: float,
    new_notional_krw: float,
) -> Tuple[str, float]:
    if gross_cap_krw is not None:
        post_gross = current_open_notional + new_notional_krw + entry_notional_for_cap
        if post_gross > gross_cap_krw + 1e-9:
            return "gross_cap", float(post_gross)
    if daily_new_cap_krw is not None:
        post_new = new_notional_krw + entry_notional_for_cap
        if post_new > daily_new_cap_krw + 1e-9:
            return "daily_cap", float(post_new)
    return "", 0.0


def _apply_normal_exec_quality(
    row: Any,
    cfg: Dict[str, Any],
    sector_risk_note_parts: List[str],
    *,
    code: str,
    qty: int,
    entry_price: float,
    min_qty_runtime: int,
) -> Tuple[bool, str, int]:
    normal_exec_ok, normal_exec_reason, normal_exec_meta = _normal_entry_execution_quality_decision(
        row,
        cfg,
        code=code,
        qty=int(qty),
        entry_price=float(entry_price),
    )
    for normal_exec_key, normal_exec_val in normal_exec_meta.items():
        row[normal_exec_key] = normal_exec_val
    if not normal_exec_ok:
        return False, str(normal_exec_reason), int(qty)
    sector_risk_note_parts.extend([
        "normal_exec_quality=PASS",
        f"normal_spread_bps={row.get('normal_spread_bps', '')}",
        f"normal_exec_qty={row.get('normal_executable_qty', '')}",
        f"normal_markout_1step_bps={row.get('normal_markout_1step_bps', '')}",
        f"normal_orderflow_tag={row.get('normal_orderflow_tag', '')}",
    ])
    neq_mult, neq_mult_reason = _normal_exec_quality_qty_reduction(normal_exec_meta, cfg)
    new_qty = int(qty)
    if 0.0 < neq_mult < 1.0:
        old_qty = int(qty)
        new_qty = max(int(min_qty_runtime), int(math.floor(float(qty) * neq_mult)))
        row["normal_exec_qty_reduction"] = neq_mult_reason
        print(
            f"[NORMAL_EXEC_QUALITY_REDUCE] code={code} qty={old_qty}->{new_qty} "
            f"mult={neq_mult:.2f} reason={neq_mult_reason}"
        )
        if int(new_qty) <= 0:
            return False, "NORMAL_EXEC_QUALITY_REDUCE_ZERO_QTY", int(new_qty)
        normal_exec_ok, normal_exec_reason, normal_exec_meta = _normal_entry_execution_quality_decision(
            row,
            cfg,
            code=code,
            qty=int(new_qty),
            entry_price=float(entry_price),
        )
        for normal_exec_key, normal_exec_val in normal_exec_meta.items():
            row[normal_exec_key] = normal_exec_val
        if not normal_exec_ok:
            return False, str(normal_exec_reason), int(new_qty)
    return True, "", int(new_qty)


def _build_entry_note_seed(
    row: Any,
    cfg: Dict[str, Any],
    *,
    effective_signal_date: str,
    qty: int,
    entry_source_kind: str,
    candidate_snapshot_id: str,
    is_surge_immediate: bool,
    has_surge_type_policy: bool,
) -> Tuple[List[str], str]:
    note_parts = [f"signal_date={effective_signal_date}", f"sizing={str(cfg.get('sizing_mode','fixed_qty'))}", f"qty={qty}"]
    note_parts.append(f"entry_source_kind={entry_source_kind}")
    note_parts.append(f"candidate_snapshot_id={candidate_snapshot_id}")
    signal_ts = str(row.get("captured_at") or "").strip()
    if signal_ts:
        note_parts.append(f"signal_ts={signal_ts}")
    if is_surge_immediate:
        note_parts.append("surge_immediate=1")
    if is_surge_immediate or has_surge_type_policy:
        note_parts.extend(_build_surge_entry_note_parts(row))
    horizon_label = _resolve_entry_horizon_label(row, cfg, is_surge_immediate=is_surge_immediate)
    if horizon_label:
        note_parts.append(f"horizon={horizon_label}")
    return note_parts, horizon_label

def _apply_normal_lob_fill_price(
    row: Any,
    sector_risk_note_parts: List[str],
    *,
    code: str,
    entry_price: float,
    qty: int,
    entry_cost_buffer: float,
) -> Tuple[bool, float, float, float, Optional[float]]:
    normal_lob_fill_px = _to_float(row.get("normal_executable_price"), None)
    if normal_lob_fill_px is None or float(normal_lob_fill_px) <= 0.0:
        return False, float(entry_price), float(entry_price) * float(qty), float(entry_price) * float(qty) * float(entry_cost_buffer), None
    normal_close_basis_entry_price = float(entry_price)
    applied_entry_price = float(normal_lob_fill_px)
    row["normal_close_basis_entry_price"] = float(normal_close_basis_entry_price)
    row["normal_lob_applied_fill_price"] = float(applied_entry_price)
    row["normal_lob_fill_price_source"] = "LOB_SWEEP"
    row["normal_lob_fill_price_changed"] = 1
    sector_risk_note_parts.extend([
        "normal_fill_price_source=LOB_SWEEP",
        f"normal_close_basis_entry_price={normal_close_basis_entry_price:.6f}",
        f"normal_lob_applied_fill_price={float(applied_entry_price):.6f}",
    ])
    entry_notional = float(applied_entry_price) * float(qty)
    entry_notional_for_cap = entry_notional * float(entry_cost_buffer)
    return True, applied_entry_price, entry_notional, entry_notional_for_cap, normal_close_basis_entry_price


def _apply_entry_t2_buy_budget(
    portfolio_state: Dict[str, Any],
    cfg: Dict[str, Any],
    *,
    code: str,
    entry_day: str,
    entry_price: float,
    qty: int,
    entry_cost_buffer: float,
    min_qty_runtime: int,
) -> Tuple[int, Dict[str, Any], float, float, bool, int]:
    qty_after_t2, t2_check = _t2_apply_buy_budget(
        portfolio_state,
        cfg,
        code=code,
        entry_day=entry_day,
        entry_price=float(entry_price),
        qty=int(qty),
        cost_buffer=float(entry_cost_buffer),
        min_qty=int(min_qty_runtime),
    )
    old_qty = int(qty)
    new_qty = int(qty_after_t2)
    entry_notional = float(entry_price) * float(new_qty)
    entry_notional_for_cap = entry_notional * float(entry_cost_buffer)
    return new_qty, t2_check, entry_notional, entry_notional_for_cap, bool(new_qty != old_qty), old_qty


def _backfill_idempotent_buy_horizon(
    open_pos: List[Dict[str, Any]],
    row: Any,
    cfg: Dict[str, Any],
    *,
    order_id: str,
    horizon_label: str,
) -> Tuple[str, int]:
    hz_label = horizon_label or _infer_horizon_label_from_row(row, cfg)
    hz_hold = _horizon_max_hold_days_for_label(hz_label, cfg) if hz_label else 0
    for pos in open_pos:
        if str(pos.get("entry_order_id", "") or "") != order_id:
            continue
        if not str(pos.get("horizon_label", "") or "").strip() and hz_label:
            pos["horizon_label"] = hz_label
        if _to_int(pos.get("horizon_max_hold_days"), 0) <= 0 and hz_hold > 0:
            pos["horizon_max_hold_days"] = int(hz_hold)
    return str(hz_label or ""), int(hz_hold or 0)

def _register_entry_idempotency_key(
    row: Any,
    *,
    code: str,
    effective_signal_date: str,
    today_ymd: str,
    is_split_2nd: bool,
    daily_idem_keys: Set[str],
) -> Tuple[bool, str]:
    sig_id_val = str(row.get("signal_id") or "").strip()
    idem_leg = "split2nd" if is_split_2nd else "entry"
    idem_key = f"{code}:{sig_id_val or effective_signal_date}:{today_ymd}:{idem_leg}"
    if is_split_2nd:
        split_first_order_id = str(row.get("split_first_order_id", "") or "").strip()
        if split_first_order_id:
            idem_key = f"{idem_key}:{split_first_order_id}"
    if idem_key in daily_idem_keys:
        return True, idem_key
    daily_idem_keys.add(idem_key)
    return False, idem_key


def _merge_split_second_entry_position(
    position: Dict[str, Any],
    row: Any,
    *,
    qty: int,
    entry_price: float,
    entry_day: str,
    order_id: str,
    lineage: Dict[str, Any],
) -> None:
    position["qty"] = int(position.get("qty", 0)) + int(qty)
    split_entries = position.get("split_second_entries")
    if not isinstance(split_entries, list):
        split_entries = []
    split_entries.append({
        "second_order_id": order_id,
        "first_order_id": str(row.get("split_first_order_id", "") or ""),
        "qty": int(qty),
        "entry_price": float(entry_price),
        "entry_day": str(entry_day),
        "trace_id": str(lineage.get("entry_trace_id", "") or ""),
    })
    position["split_second_entries"] = split_entries


def _is_split_second_carry_expired(
    *,
    remaining_qty: int,
    strict_same_day: bool,
    partial_same_day_only: bool,
    split_cfg: Dict[str, Any],
) -> bool:
    return bool(
        int(remaining_qty) > 0
        and strict_same_day
        and partial_same_day_only
        and not bool(split_cfg.get("allow_split_second_carryover", True))
    )


def _build_split_second_carry_row(
    row: Any,
    split_cfg: Dict[str, Any],
    *,
    effective_signal_date: str,
    code: str,
    name: str,
    remaining_qty: int,
    entry_price: float,
    order_id: str,
    qty: int,
    sig_key: str,
) -> Dict[str, Any]:
    max_days2 = max(1, int(split_cfg.get("second_entry_max_days", 1)))
    return {
        "signal_date": effective_signal_date,
        "code": code,
        "name": name,
        "carry_reason": "ENTRY_RETRY_READY",
        "carry_origin_reason": "SPLIT_ENTRY_2ND",
        "split_entry_2nd": True,
        "split_remaining_qty": int(remaining_qty),
        "split_first_entry_price": entry_price,
        "split_first_order_id": order_id,
        "split_first_qty": int(qty),
        "split_first_fill_id": order_id,
        "split_signal_id": sig_key,
        "captured_at": now_ts(),
        "carryover_count": 1,
        "carryover_max_age_days": max_days2,
        "score": row.get("score", None),
        "final_score": row.get("final_score", None),
    }


def _build_entry_base_position(
    *,
    row: Any,
    cfg: Dict[str, Any],
    px: Dict[str, Any],
    code: str,
    name: str,
    qty: int,
    effective_signal_date: str,
    entry_day: str,
    entry_ts_value: str,
    row_entry_timing: str,
    entry_price: float,
    take_profit: float,
    stop_loss: float,
    trail_pct: float,
    row_slip_pct: float,
    row_market_cap: Any,
    horizon_label: str,
    lineage: Dict[str, Any],
    sector_db: Dict[str, Any],
    fundamentals_db: Dict[str, Any],
    is_surge_immediate: bool,
    has_surge_type_policy: bool,
) -> Dict[str, Any]:
    position_sector = str(row.get("sector", "") or row.get("sector_name", "") or row.get("krx_sector", "") or sector_db.get(code, "") or "").strip()
    position_fund = fundamentals_db.get(code, {}) if isinstance(fundamentals_db, dict) else {}
    position_asset_type = str(row.get("asset_type", "") or "").strip()
    position_atr14_pct = float(_to_float(row.get("atr14_pct"), 0.0) or 0.0)
    position_atr14_source = str(row.get("atr14_pct_source", "") or "").strip()
    position_surge_type = str(row.get("surge_type", "") or "") if (is_surge_immediate or has_surge_type_policy) else ""
    position_surge_stop_pct = _to_float(row.get("surge_type_stop_pct"), None) if (is_surge_immediate or has_surge_type_policy) else None
    position_surge_tp_pct = _to_float(row.get("surge_type_tp_pct"), None) if (is_surge_immediate or has_surge_type_policy) else None
    position_surge_max_hold_days = _to_int(row.get("surge_type_max_hold_days"), 0) if (is_surge_immediate or has_surge_type_policy) else 0
    position_take_profit = take_profit
    position_stop_loss = stop_loss
    if (is_surge_immediate or has_surge_type_policy) and position_surge_tp_pct is not None:
        position_take_profit = float(position_surge_tp_pct)
    if (is_surge_immediate or has_surge_type_policy) and position_surge_stop_pct is not None:
        position_stop_loss = float(position_surge_stop_pct)
    position_horizon_max_hold_days = (
        _to_int(row.get("horizon_max_hold_days"), 0)
        if _to_int(row.get("horizon_max_hold_days"), 0) > 0
        else _horizon_max_hold_days_for_label(horizon_label, cfg)
    )
    if (is_surge_immediate or has_surge_type_policy) and position_surge_max_hold_days > 0:
        position_horizon_max_hold_days = int(position_surge_max_hold_days)
    position_orderflow_tag = str(row.get("surge_orderflow_tag", row.get("orderflow_tag", "")) or "") if (is_surge_immediate or has_surge_type_policy) else ""
    position_orderflow_tag_norm = position_orderflow_tag.strip().upper()
    position_orderflow_missing = bool(
        (is_surge_immediate or has_surge_type_policy)
        and (not position_orderflow_tag_norm or position_orderflow_tag_norm in {"NO_HISTORY", "NO_LOB", "UNKNOWN", "NAN", "NONE", "NULL"})
    )
    if position_atr14_pct <= 0:
        position_atr14_pct = _estimate_atr14_pct_from_ohlc(px, code, entry_day)
        if position_atr14_pct > 0:
            position_atr14_source = "entry_ohlc_fallback"
    return {
        "code": code, "name": name, "qty": qty,
        "signal_date": effective_signal_date,
        "entry_date": entry_day,
        "entry_ts": entry_ts_value,
        "entry_timing": row_entry_timing,
        "entry_price": entry_price,
        "max_close": entry_price,
        "asset_type": position_asset_type,
        "sector": position_sector,
        "fundamentals": position_fund,
        "tp_taken_levels": [],
        "executed_sell_tags": [],
        "stop_loss": position_stop_loss,
        "take_profit": position_take_profit,
        "trail_pct": trail_pct,
        "atr14_pct": float(position_atr14_pct),
        "atr14_pct_source": position_atr14_source,
        "entry_order_id": lineage["entry_order_id"],
        "entry_intent_id": lineage["entry_intent_id"],
        "entry_trace_id": lineage["entry_trace_id"],
        "lineage_origin": lineage["lineage_origin"],
        "source_order_id": lineage["source_order_id"],
        "source_intent_id": lineage["source_intent_id"],
        "source_trace_id": lineage["source_trace_id"],
        "replay_chain_id": lineage["replay_chain_id"],
        "replay_depth": lineage["replay_depth"],
        "market_cap": row_market_cap,
        "entry_slippage_pct": float(row_slip_pct),
        "horizon_label": horizon_label,
        "horizon_max_hold_days": position_horizon_max_hold_days,
        "_surge_immediate": 1 if is_surge_immediate else 0,
        "surge_type": position_surge_type,
        "surge_type_stop_pct": position_surge_stop_pct,
        "surge_type_tp_pct": position_surge_tp_pct,
        "surge_type_max_hold_days": position_surge_max_hold_days,
        "surge_score": _to_float(row.get("surge_score"), None) if (is_surge_immediate or has_surge_type_policy) else None,
        "surge_score_final": _to_float(row.get("surge_score_final", row.get("final_score")), None) if (is_surge_immediate or has_surge_type_policy) else None,
        "surge_rvol20": _to_float(row.get("surge_rvol20", row.get("rvol20")), None) if (is_surge_immediate or has_surge_type_policy) else None,
        "surge_spread_bps": _to_float(row.get("surge_spread_bps", row.get("spread_bps")), None) if (is_surge_immediate or has_surge_type_policy) else None,
        "surge_orderflow_tag": position_orderflow_tag,
        "surge_orderflow_status": "MISSING" if position_orderflow_missing else ("OK" if (is_surge_immediate or has_surge_type_policy) else ""),
        "surge_orderflow_missing": bool(position_orderflow_missing),
        "surge_orderflow_risk_score": _to_float(row.get("surge_orderflow_risk_score", row.get("orderflow_risk_score")), None) if (is_surge_immediate or has_surge_type_policy) else None,
        "surge_lob_slippage_pct": _to_float(row.get("surge_lob_slippage_pct"), None) if (is_surge_immediate or has_surge_type_policy) else None,
        "surge_lob_slippage_source": str(row.get("surge_lob_slippage_source", "") or "") if (is_surge_immediate or has_surge_type_policy) else "",
        "normal_lob_check": str(row.get("normal_lob_check", "") or "") if not is_surge_immediate else "",
        "normal_lob_status": str(row.get("normal_lob_status", "") or "") if not is_surge_immediate else "",
        "normal_lob_reason": str(row.get("normal_lob_reason", "") or "") if not is_surge_immediate else "",
        "normal_spread_bps": _to_float(row.get("normal_spread_bps"), None) if not is_surge_immediate else None,
        "normal_markout_1step_bps": _to_float(row.get("normal_markout_1step_bps"), None) if not is_surge_immediate else None,
        "normal_executable_qty": _to_float(row.get("normal_executable_qty"), None) if not is_surge_immediate else None,
        "normal_executable_price": _to_float(row.get("normal_executable_price"), None) if not is_surge_immediate else None,
        "normal_close_auction_action": str(row.get("normal_close_auction_action", "") or "") if not is_surge_immediate else "",
        "normal_close_auction_reason": str(row.get("normal_close_auction_reason", "") or "") if not is_surge_immediate else "",
        "normal_intraday_momentum_action": str(row.get("normal_intraday_momentum_action", "") or "") if not is_surge_immediate else "",
        "normal_intraday_momentum_reason": str(row.get("normal_intraday_momentum_reason", "") or "") if not is_surge_immediate else "",
        "normal_dynamic_slippage_pct": _to_float(row.get("normal_dynamic_slippage_pct"), None) if not is_surge_immediate else None,
        "normal_dynamic_slippage_reason": str(row.get("normal_dynamic_slippage_reason", "") or "") if not is_surge_immediate else "",
    }


def _build_entry_order_id(
    *,
    code: str,
    effective_signal_date: str,
    entry_day: str,
    row: Any,
    allow_same_signal_reentry: bool,
    is_open_order_replay: bool,
    replay_row_ok: bool,
    replay_order_id_include_entry_day: bool,
    existing_fill_order_ids: Set[str],
) -> str:
    order_id = f"PAPER_BUY_{code}_{effective_signal_date}"
    if allow_same_signal_reentry and (not is_open_order_replay):
        order_id = f"{order_id}_R{entry_day}"
    if is_open_order_replay:
        order_id = f"PAPER_REPLAY_{code}_{entry_day}"
        replay_source_order_id = str(row.get("replay_source_order_id", "") or "").strip()
        if replay_source_order_id:
            order_id = f"{order_id}_{replay_source_order_id[-12:]}"
    if replay_row_ok and replay_order_id_include_entry_day:
        order_id = f"{order_id}_R{entry_day}"
    if allow_same_signal_reentry and (not is_open_order_replay) and (order_id in existing_fill_order_ids):
        base_order_id = order_id
        seq = 2
        while seq <= 9999 and order_id in existing_fill_order_ids:
            order_id = f"{base_order_id}_N{seq}"
            seq += 1
        print(
            f"[REENTRY_ORDER_ID] code={code} base={base_order_id} adjusted={order_id} "
            f"signal_date={effective_signal_date} entry_day={entry_day}"
        )
    return order_id


def _build_replay_entry_note_parts(row: Any, *, is_open_order_replay: bool, replay_row_ok: bool, entry_day: str, signal_date: str) -> List[str]:
    out: List[str] = []
    if is_open_order_replay:
        out.append("open_order_replay=1")
        out.append(f"replay_entry_day={entry_day}")
        out.append(f"remaining_action={str(row.get('remaining_action') or '')}")
        out.append(f"source_order_id={str(row.get('replay_source_order_id') or '')}")
        out.append(f"source_intent_id={str(row.get('replay_source_intent_id') or '')}")
        out.append(f"source_trace_id={str(row.get('replay_source_trace_id') or '')}")
    if replay_row_ok:
        out.append("replay_stale_signal=1")
        out.append(f"raw_signal_date={signal_date}")
    return out


def _build_entry_timing_note_parts(*, use_same_close_today: bool, use_intraday_realtime_entry: bool, force_next_open_entry: bool) -> List[str]:
    if use_same_close_today:
        return ["entry_timing=intraday_realtime" if use_intraday_realtime_entry else "entry_timing=same_close"]
    if force_next_open_entry:
        return ["entry_timing=next_open"]
    return []


def _build_split_entry_note_parts(row: Any, cfg: Dict[str, Any], *, is_split_2nd: bool, is_open_order_replay: bool) -> List[str]:
    if is_split_2nd:
        return [
            "split_entry=2nd",
            f"split_first_price={row.get('split_first_entry_price', '')}",
            f"split_first_order_id={row.get('split_first_order_id', '')}",
        ]
    if cfg.get("split_entry", {}).get("enabled") and not is_open_order_replay:
        return ["split_entry=1st"]
    return []


def _build_lineage_session_note_parts(lineage: Dict[str, Any]) -> List[str]:
    out = [
        f"entry_order_id={lineage['entry_order_id']}",
        f"entry_intent_id={lineage['entry_intent_id']}",
        f"entry_trace_id={lineage['entry_trace_id']}",
        f"replay_chain_id={lineage['replay_chain_id']}",
        f"replay_depth={lineage['replay_depth']}",
    ]
    if PAPER_SESSION_ID:
        out.append(f"paper_session_id={PAPER_SESSION_ID}")
    if RUN_LABEL:
        out.append(f"run_label={RUN_LABEL}")
    return out
def _record_buy_executed_decision(
    record_decision: Callable[..., None],
    *,
    code: str,
    effective_signal_date: str,
    row: Any,
    is_surge_immediate: bool,
    is_open_order_replay: bool,
    is_carryover_row: bool,
    entry_day: str,
    entry_price: float,
    qty: int,
    order_id: str,
) -> None:
    record_decision(
        code=code,
        signal_date=effective_signal_date,
        signal="BUY",
        reason="BUY_EXECUTED",
        row=row,
        is_surge=is_surge_immediate,
        is_replay=is_open_order_replay,
        is_carryover=is_carryover_row,
        entry_day=entry_day,
        entry_price=entry_price,
        qty=qty,
        order_id=order_id,
    )


def _record_entry_t2_cash_if_needed(
    portfolio_state: Dict[str, Any],
    cfg: Dict[str, Any],
    *,
    is_open_order_replay: bool,
    code: str,
    entry_day: str,
    order_id: str,
    entry_notional_for_cap: float,
) -> bool:
    if is_open_order_replay:
        return False
    _t2_record_buy_cash(
        portfolio_state,
        cfg,
        code=code,
        entry_day=entry_day,
        order_id=order_id,
        amount=float(entry_notional_for_cap),
    )
    return True


def _normalize_entry_loop_result(
    loop_result: Dict[str, Any],
    *,
    fallback_portfolio_state: Optional[Dict[str, Any]] = None,
    fallback_t2_cash_checks: Optional[List[Any]] = None,
) -> Dict[str, Any]:
    return {
        "fills_new": loop_result["fills_new"],
        "trades_new": loop_result["trades_new"],
        "new_count": int(loop_result["new_count"]),
        "new_notional_krw": float(loop_result["new_notional_krw"]),
        "surge_new_count": int(loop_result.get("surge_new_count", 0)),
        "surge_notional_krw": float(loop_result.get("surge_notional_krw", 0.0)),
        "split_notional_krw": float(loop_result.get("split_notional_krw", 0.0)),
        "evaluated_count": int(loop_result["evaluated_count"]),
        "no_next_day_count": int(loop_result["no_next_day_count"]),
        "entry_ready_count": int(loop_result["entry_ready_count"]),
        "cap_block_count": int(loop_result["cap_block_count"]),
        "processed_skip_count": int(loop_result["processed_skip_count"]),
        "max_new_skip_count": int(loop_result.get("max_new_skip_count", 0)),
        "idempotent_skip_count": int(loop_result.get("idempotent_skip_count", 0)),
        "stale_replay_used_count": int(loop_result["stale_replay_used_count"]),
        "open_order_replay_used_count": int(loop_result["open_order_replay_used_count"]),
        "max_positions_blocked": bool(loop_result.get("max_positions_blocked", False)),
        "today_ymd": str(loop_result["today_ymd"]),
        "pending_carry_rows": loop_result["pending_carry_rows"],
        "portfolio_state": _get_dict(loop_result, "portfolio_state", fallback_portfolio_state or {}),
        "t2_cash_checks": _get_list(loop_result, "t2_cash_checks", fallback_t2_cash_checks or []),
        "entry_decisions": loop_result.get("entry_decisions", []),
        "fail_closed_triggered": bool(loop_result.get("fail_closed_triggered", False)),
        "fail_closed_reason": str(loop_result.get("fail_closed_reason", "")),
    }


def _print_entry_fill_summary_v2(loop_result: Dict[str, Any]) -> None:
    fail_closed = bool(loop_result.get("fail_closed_triggered", False))
    fail_closed_reason = str(loop_result.get("fail_closed_reason", ""))
    summary_keys = (
        "ttl_expired_count",
        "retry_blocked_count",
        "exec_quality_blocked_count",
        "quote_stale_blocked_count",
        "close_cutoff_blocked_count",
        "partial_fill_expired_count",
    )
    if not fail_closed and not any(loop_result.get(key, 0) > 0 for key in summary_keys):
        return
    print(
        f"[FILL_SUMMARY_V2] ttl_expired={loop_result.get('ttl_expired_count',0)} "
        f"retry_blocked={loop_result.get('retry_blocked_count',0)} "
        f"exec_quality_blocked={loop_result.get('exec_quality_blocked_count',0)} "
        f"quote_stale_blocked={loop_result.get('quote_stale_blocked_count',0)} "
        f"close_cutoff_blocked={loop_result.get('close_cutoff_blocked_count',0)} "
        f"partial_fill_expired={loop_result.get('partial_fill_expired_count',0)} "
        f"fail_closed={fail_closed if fail_closed else 'no'}"
        f"{(' fail_closed_reason=' + fail_closed_reason) if fail_closed else ''}"
    )


def _write_entry_runtime_snapshots_and_reports(
    *,
    candidate_df: pd.DataFrame,
    full_candidate_df_for_report: pd.DataFrame,
    entry_decisions: Any,
    d_ref_ymd: str,
    rank_col: str,
    risk_gate_runtime: Any,
    write_surge_realtime_shadow_runtime_snapshot: Callable[..., None],
    trace_enabled: bool = True,
) -> None:
    entry_decision_rows = entry_decisions if isinstance(entry_decisions, list) else []
    risk_gate = risk_gate_runtime if isinstance(risk_gate_runtime, dict) else {}
    if trace_enabled:
        _paper_engine_phase_trace("entry_signal_snapshot_before", decisions=len(entry_decision_rows))
    _write_entry_signal_snapshot(
        rows=entry_decision_rows,
        d_ref_ymd=str(d_ref_ymd),
        rank_col=str(rank_col or ""),
    )
    if trace_enabled:
        _paper_engine_phase_trace("entry_signal_snapshot_after")
        _paper_engine_phase_trace("entry_decision_layers_before", candidates=len(candidate_df))
    _write_entry_decision_layers_snapshot(
        candidate_df=candidate_df,
        entry_decision_rows=entry_decision_rows,
        d_ref_ymd=str(d_ref_ymd),
        rank_col=str(rank_col or ""),
        risk_gate_runtime=risk_gate,
    )
    if trace_enabled:
        _paper_engine_phase_trace("entry_decision_layers_after")
        _paper_engine_phase_trace("normal_entry_fill_quality_before")
    _write_normal_entry_fill_quality_report(
        candidate_df=full_candidate_df_for_report,
        entry_decision_rows=entry_decision_rows,
        d_ref_ymd=str(d_ref_ymd),
        rank_col=str(rank_col or ""),
    )
    if trace_enabled:
        _paper_engine_phase_trace("normal_entry_fill_quality_after")
        _paper_engine_phase_trace("surge_realtime_shadow_runtime_before")
    write_surge_realtime_shadow_runtime_snapshot(
        entry_decision_rows=entry_decision_rows,
        d_ref_ymd=str(d_ref_ymd),
    )
    if trace_enabled:
        _paper_engine_phase_trace("surge_realtime_shadow_runtime_after")


def _run_post_exit_entry_recheck(
    *,
    candidate_df: pd.DataFrame,
    exit_only_mode: bool,
    max_positions_blocked: bool,
    max_positions: int,
    still_open: List[Dict[str, Any]],
    new_count: int,
    max_new: int,
    max_new_surge: int,
    capital_total: float,
    schema: str,
    config: Dict[str, Any],
    prices_df: pd.DataFrame,
    fee_pct: float,
    slip_pct: float,
    gap_up_max_pct_runtime: float,
    entry_gap_down_stop_pct_runtime: float,
    stop_loss: float,
    take_profit: Any,
    trail_pct: Any,
    same_close_entry_mode: bool,
    intraday_realtime_mode: bool,
    processed_signals: Set[str],
    committed_signal_keys: Set[str],
    replay_enabled: bool,
    replay_global_ok: bool,
    replay_min_age_days: int,
    replay_order_id_include_entry_day: bool,
    ops_enabled: bool,
    ops_policy: Dict[str, Any],
    carryover_market_gate_block: bool,
    carryover_revalidate_summary: Dict[str, Any],
    market_regime: str,
    risk_off_enabled: bool,
    block_same_sector_entry: bool,
    entry_sector_col: str,
    blocked_sector_value: str,
    sector_concentration: float,
    gross_cap_krw: Optional[float],
    daily_new_cap_krw: Optional[float],
    current_open_notional: float,
    position_size_multiplier: float,
    fundamentals_db: Dict[str, Any],
    sector_db: Dict[str, Any],
    trend_overlay_ctx: Optional[Dict[str, Any]],
    existing_fill_order_ids: Set[str],
    open_codes: Set[str],
    max_positions_override_allowed: bool,
    fills_new: List[List[Any]],
    trades_new: List[List[Any]],
    new_notional_krw: float,
    surge_new_count: int,
    surge_notional_krw: float,
    split_notional_krw: float,
    evaluated_count: int,
    no_next_day_count: int,
    entry_ready_count: int,
    cap_block_count: int,
    processed_skip_count: int,
    idempotent_skip_count: int,
    stale_replay_used_count: int,
    open_order_replay_used_count: int,
    today_ymd: str,
    pending_carry_rows: List[Any],
    entry_decision_code: str,
    entry_decision_reason: str,
    loop_state: Dict[str, Any],
    portfolio_state: Dict[str, Any],
    t2_cash_checks: List[Any],
) -> Dict[str, Any]:
    if not (
        (not exit_only_mode)
        and max_positions_blocked
        and max_positions > 0
        and len(still_open) < max_positions
        and len(candidate_df) > 0
        and new_count < max_new
    ):
        return {
            "applied": False,
            "current_open_notional": float(current_open_notional),
        }

    slots_after_exit = max(0, int(max_positions) - int(len(still_open)))
    print(
        f"[ENTRY_RECHECK_AFTER_EXIT] triggered=1 slots_after_exit={slots_after_exit} "
        f"open_after_exit={len(still_open)} max_positions={max_positions}"
    )
    current_open_notional = _compute_current_open_notional(still_open, prices_df)
    open_codes_after_exit = {str(pos.get("code", "")).zfill(6) for pos in still_open}
    loop_state_recheck = {
        "fills_new": fills_new,
        "trades_new": trades_new,
        "new_count": int(new_count),
        "new_notional_krw": float(new_notional_krw),
        "surge_new_count": int(surge_new_count),
        "surge_notional_krw": float(surge_notional_krw),
        "split_notional_krw": float(split_notional_krw),
        "evaluated_count": int(evaluated_count),
        "no_next_day_count": int(no_next_day_count),
        "entry_ready_count": int(entry_ready_count),
        "cap_block_count": int(cap_block_count),
        "processed_skip_count": int(processed_skip_count),
        "idempotent_skip_count": int(idempotent_skip_count),
        "stale_replay_used_count": int(stale_replay_used_count),
        "open_order_replay_used_count": int(open_order_replay_used_count),
        "max_positions_blocked": False,
        "today_ymd": str(today_ymd),
        "pending_carry_rows": pending_carry_rows,
        "entry_decision_code": str(entry_decision_code or ""),
        "entry_decision_reason": str(entry_decision_reason or ""),
        "p0_rolling_dd_abs": loop_state.get("p0_rolling_dd_abs", 0.0),
        "p0_rolling_dd_source": loop_state.get("p0_rolling_dd_source", "p0.kill_switch.metrics.max_drawdown_pct"),
        "_same_code_day_buy_counts": loop_state.get("_same_code_day_buy_counts", {}),
        "portfolio_state": portfolio_state,
        "t2_cash_checks": t2_cash_checks,
    }
    recheck_result = _process_entry_rows(
        candidate_df,
        max_new=int(max_new),
        max_new_surge=int(max_new_surge),
        capital_total=float(capital_total),
        max_positions=int(max_positions),
        schema=str(schema),
        config=config,
        prices_df=prices_df,
        fee_pct=float(fee_pct),
        slip_pct=float(slip_pct),
        gap_up_max_pct_runtime=float(gap_up_max_pct_runtime),
        entry_gap_down_stop_pct_runtime=float(entry_gap_down_stop_pct_runtime),
        stop_loss=float(stop_loss),
        take_profit=take_profit,
        trail_pct=trail_pct,
        same_close_entry_mode=bool(same_close_entry_mode),
        intraday_realtime_mode=bool(intraday_realtime_mode),
        processed_signals=processed_signals,
        committed_signal_keys=committed_signal_keys,
        replay_enabled=bool(replay_enabled),
        replay_global_ok=bool(replay_global_ok),
        replay_min_age_days=int(replay_min_age_days),
        replay_order_id_include_entry_day=bool(replay_order_id_include_entry_day),
        ops_enabled=bool(ops_enabled),
        ops_policy=ops_policy,
        carryover_market_gate_block=bool(carryover_market_gate_block),
        carryover_revalidate_summary=carryover_revalidate_summary,
        market_regime=str(market_regime or ''),
        risk_off_enabled=bool(risk_off_enabled),
        block_same_sector_entry=bool(block_same_sector_entry),
        entry_sector_col=str(entry_sector_col or ''),
        blocked_sector_value=str(blocked_sector_value or ''),
        sector_concentration=float(sector_concentration),
        gross_cap_krw=gross_cap_krw,
        daily_new_cap_krw=daily_new_cap_krw,
        current_open_notional=float(current_open_notional),
        position_size_multiplier=float(position_size_multiplier),
        fundamentals_db=fundamentals_db,
        sector_db=sector_db,
        trend_overlay_ctx=trend_overlay_ctx,
        existing_fill_order_ids=existing_fill_order_ids,
        open_pos=still_open,
        open_codes=open_codes_after_exit,
        max_positions_override_allowed=bool(max_positions_override_allowed),
        loop_state=loop_state_recheck,
    )
    recheck_loop = _normalize_entry_loop_result(
        recheck_result,
        fallback_portfolio_state=portfolio_state,
        fallback_t2_cash_checks=t2_cash_checks,
    )
    print(
        f"[ENTRY_RECHECK_AFTER_EXIT] result new_count={int(recheck_loop['new_count'])} "
        f"entry_ready={int(recheck_loop['entry_ready_count'])} open_positions={len(still_open)}"
    )
    return {
        "applied": True,
        "current_open_notional": float(current_open_notional),
        "recheck_loop": recheck_loop,
    }


def _process_entry_rows(
    candidate_df: pd.DataFrame,
    *,
    max_new: int,
    max_new_surge: int,
    capital_total: float,
    max_positions: int = 0,
    schema: str,
    config: Dict[str, Any],
    prices_df: pd.DataFrame,
    fee_pct: float,
    slip_pct: float,
    gap_up_max_pct_runtime: float,
    entry_gap_down_stop_pct_runtime: float,
    stop_loss: float,
    take_profit: Any,
    trail_pct: Any,
    same_close_entry_mode: bool,
    intraday_realtime_mode: bool,
    processed_signals: Set[str],
    committed_signal_keys: Set[str],
    replay_enabled: bool,
    replay_global_ok: bool,
    replay_min_age_days: int,
    replay_order_id_include_entry_day: bool,
    ops_enabled: bool,
    ops_policy: Dict[str, Any],
    carryover_market_gate_block: bool,
    carryover_revalidate_summary: Dict[str, Any],
    market_regime: str,
    risk_off_enabled: bool,
    block_same_sector_entry: bool,
    entry_sector_col: str,
    blocked_sector_value: str,
    sector_concentration: float,
    gross_cap_krw: Optional[float],
    daily_new_cap_krw: Optional[float],
    current_open_notional: float,
    position_size_multiplier: float,
    fundamentals_db: Dict[str, Any],
    sector_db: Dict[str, Any],
    trend_overlay_ctx: Optional[Dict[str, Any]],
    existing_fill_order_ids: Set[str],
    open_pos: List[Dict[str, Any]],
    open_codes: Set[str],
    max_positions_override_allowed: bool,
    loop_state: Dict[str, Any],
) -> Dict[str, Any]:
    cdf = candidate_df
    cfg = config
    px = prices_df

    fills_new = loop_state['fills_new']
    trades_new = loop_state['trades_new']
    new_count = int(loop_state['new_count'])
    new_notional_krw = float(loop_state['new_notional_krw'])
    surge_new_count = int(loop_state.get('surge_new_count', 0))
    surge_type_day_counts: Dict[str, int] = dict(loop_state.get('surge_type_day_counts') or {})
    surge_notional_krw = float(loop_state.get('surge_notional_krw', 0.0))
    split_notional_krw = float(loop_state.get('split_notional_krw', 0.0))
    evaluated_count = int(loop_state['evaluated_count'])
    no_next_day_count = int(loop_state['no_next_day_count'])
    entry_ready_count = int(loop_state['entry_ready_count'])
    cap_block_count = int(loop_state['cap_block_count'])
    processed_skip_count = int(loop_state['processed_skip_count'])
    max_new_skip_count = int(loop_state.get('max_new_skip_count', 0))
    idempotent_skip_count = int(loop_state.get('idempotent_skip_count', 0))
    stale_replay_used_count = int(loop_state['stale_replay_used_count'])
    open_order_replay_used_count = int(loop_state['open_order_replay_used_count'])
    max_positions_blocked = bool(loop_state.get('max_positions_blocked', False))
    today_ymd = str(loop_state['today_ymd'])
    pending_carry_rows = loop_state['pending_carry_rows']
    entry_decisions = loop_state.get('entry_decisions', [])
    entry_decision_code = str(loop_state.get("entry_decision_code", "") or "")
    entry_decision_reason = str(loop_state.get("entry_decision_reason", "") or "")
    allow_same_signal_reentry = bool(cfg.get("allow_same_code_reentry", False))
    _daily_retry_counts: Dict[str, int] = loop_state.get('_daily_retry_counts', {})
    _daily_idem_keys: set = loop_state.get('_daily_idem_keys', set())
    _same_code_day_buy_counts: Dict[str, int] = loop_state.setdefault('_same_code_day_buy_counts', {})
    fail_closed_triggered = bool(loop_state.get('fail_closed_triggered', False))
    fail_closed_reason = str(loop_state.get('fail_closed_reason', ''))
    p0_rolling_dd_abs = _ddm_pct01(abs(_to_float(loop_state.get("p0_rolling_dd_abs"), 0.0)), 0.0)
    p0_rolling_dd_source = str(loop_state.get("p0_rolling_dd_source") or "p0.kill_switch.metrics.max_drawdown_pct")
    ttl_expired_count = int(loop_state.get('ttl_expired_count', 0))
    retry_blocked_count = int(loop_state.get('retry_blocked_count', 0))
    exec_quality_blocked_count = int(loop_state.get('exec_quality_blocked_count', 0))
    quote_stale_blocked_count = int(loop_state.get('quote_stale_blocked_count', 0))
    close_cutoff_blocked_count = int(loop_state.get('close_cutoff_blocked_count', 0))
    partial_fill_expired_count = int(loop_state.get('partial_fill_expired_count', 0))
    sector_corr_block_count = int(loop_state.get('sector_corr_block_count', 0))
    sector_corr_reduce_count = int(loop_state.get('sector_corr_reduce_count', 0))
    portfolio_state: Dict[str, Any] = _get_dict(loop_state, "portfolio_state")
    t2_cash_checks: List[Any] = _get_list(loop_state, "t2_cash_checks")
    _fc_propagate = bool(ops_policy.get('fail_closed_propagate_block', True)) if isinstance(ops_policy, dict) else True
    _exec_q_max = float(ops_policy.get("exec_quality_max_slippage_pct", 0.0)) if isinstance(ops_policy, dict) else 0.0
    tctx = trend_overlay_ctx if isinstance(trend_overlay_ctx, dict) else {}
    t_enabled = bool(tctx.get("enabled", False))
    t_ai_codes = tctx.get("ai_focus_codes", set()) if isinstance(tctx.get("ai_focus_codes", set()), set) else set()
    t_ai_max_open_count = max(0, int(_to_int(tctx.get("ai_focus_max_open_count", 0), 0) or 0))
    t_ai_single_cap_pct = max(0.0, min(_to_float(tctx.get("ai_focus_single_name_cap_pct"), 0.0), 1.0))
    t_seasonal_mult = max(0.10, min(_to_float(tctx.get("seasonal_multiplier"), 1.0), 1.5))
    if t_enabled:
        print(
            f"[TREND_2026] cutting_active={bool(tctx.get('cutting_active', False))} "
            f"seasonal_multiplier={t_seasonal_mult:.3f} ai_max_open={t_ai_max_open_count} "
            f"ai_single_cap_pct={t_ai_single_cap_pct:.3f}"
        )
    sector_corr_ctx = _build_sector_correlation_guard(
        open_pos=open_pos,
        candidate_df=cdf,
        prices_df=px,
        sector_db=sector_db,
        cfg=cfg,
    )
    budget_policy = cfg.get("capital_budget_policy", {}) if isinstance(cfg.get("capital_budget_policy"), dict) else {}
    budget_policy_enabled = bool(budget_policy.get("enabled", False))
    basic_alloc_pct = max(0.0, min(1.0, _pct01_from_config(budget_policy.get("basic_alloc_pct", 0.40), 0.40)))
    basic_target_positions = max(1, _to_int(budget_policy.get("basic_target_positions", 18), 18))
    split_alloc_pct = max(
        0.0,
        min(
            1.0,
            _pct01_from_config(
                (cfg.get("split_entry", {}) if isinstance(cfg.get("split_entry"), dict) else {}).get(
                    "budget_alloc_pct",
                    budget_policy.get("split_alloc_pct", 0.18),
                ),
                0.18,
            ),
        ),
    )
    surge_policy_for_budget = cfg.get("surge_entry_policy", {}) if isinstance(cfg.get("surge_entry_policy"), dict) else {}
    configured_surge_alloc_pct = _pct01_from_config(budget_policy.get("surge_alloc_pct", surge_policy_for_budget.get("total_alloc_pct", 0.0)), 0.0)
    active_surge_alloc_pct = _pct01_from_config(surge_policy_for_budget.get("total_alloc_pct", configured_surge_alloc_pct), configured_surge_alloc_pct)
    surge_reserved_alloc_pct = max(0.0, min(1.0, max(float(configured_surge_alloc_pct), float(active_surge_alloc_pct))))
    recovery_alloc_pct = max(0.0, min(1.0, _pct01_from_config(budget_policy.get("recovery_alloc_pct", 0.0), 0.0)))
    reserve_alloc_pct = max(0.0, min(1.0, _pct01_from_config(budget_policy.get("reserve_alloc_pct", 0.0), 0.0)))
    common_reserved_alloc_pct = min(1.0, float(surge_reserved_alloc_pct) + float(split_alloc_pct) + float(recovery_alloc_pct) + float(reserve_alloc_pct))
    basic_alloc_remaining_pct = max(0.0, 1.0 - float(common_reserved_alloc_pct))
    effective_basic_alloc_pct = min(float(basic_alloc_pct), float(basic_alloc_remaining_pct))
    if budget_policy_enabled and abs(float(effective_basic_alloc_pct) - float(basic_alloc_pct)) > 1e-9:
        print(
            f"[BUDGET_COMMON_LINK] normal_basic_alloc {basic_alloc_pct:.3f}->{effective_basic_alloc_pct:.3f} "
            f"reserved surge={surge_reserved_alloc_pct:.3f} split={split_alloc_pct:.3f} "
            f"recovery={recovery_alloc_pct:.3f} reserve={reserve_alloc_pct:.3f}"
        )
    basic_per_symbol_pct = effective_basic_alloc_pct / float(basic_target_positions)

    def _cap_basic_qty(qty_in: int, entry_px: float, code_val: str) -> int:
        if not budget_policy_enabled or qty_in <= 0 or entry_px <= 0 or float(capital_total) <= 0:
            return int(qty_in)
        cap_notional = float(capital_total) * float(basic_per_symbol_pct)
        cap_qty = int(math.floor(cap_notional / float(entry_px)))
        if cap_qty <= 0:
            print(
                f"[BUDGET_BASIC_BLOCK] code={code_val} cap_notional={cap_notional:.0f} "
                f"entry_price={entry_px:.2f}"
            )
            return 0
        if qty_in > cap_qty:
            print(
                f"[BUDGET_BASIC_CAP] code={code_val} qty={qty_in}->{cap_qty} "
                f"basic_alloc={effective_basic_alloc_pct:.3f} configured_basic_alloc={basic_alloc_pct:.3f} "
                f"target_positions={basic_target_positions}"
            )
            return int(cap_qty)
        return int(qty_in)

    def _record_decision(
        *,
        code: str,
        signal_date: str,
        signal: str,
        reason: str,
        row: Any,
        is_surge: bool,
        is_replay: bool,
        is_carryover: bool,
        entry_day: str = "",
        entry_price: Any = "",
        qty: Any = "",
        order_id: str = "",
    ) -> None:
        rank_val = pd.to_numeric(pd.Series([row.get("final_score", row.get("score", None))]), errors="coerce").iloc[0]
        entry_decisions.append(
            {
                "code": str(code or "").zfill(6),
                "signal_date": str(signal_date or ""),
                "signal": str(signal or ""),
                "reason": str(reason or ""),
                "rank_score": (float(rank_val) if pd.notna(rank_val) else ""),
                "is_surge": bool(is_surge),
                "is_replay": bool(is_replay),
                "is_carryover": bool(is_carryover),
                "entry_day": str(entry_day or ""),
                "entry_price": entry_price,
                "qty": qty,
                "order_id": str(order_id or ""),
                "news_implication_reduce_size_rows": row.get("news_implication_reduce_size_rows", ""),
                "news_implication_reduce_size_multiplier": row.get("news_implication_reduce_size_multiplier", ""),
                "news_implication_reduce_size_observe_only": row.get("news_implication_reduce_size_observe_only", ""),
                "news_implication_watch_rows": row.get("news_implication_watch_rows", ""),
                "news_implication_watch_observe_only": row.get("news_implication_watch_observe_only", ""),
                "news_topic_candidate_effect": row.get("news_topic_candidate_effect", ""),
                "news_topic_execution_effect": row.get("news_topic_execution_effect", ""),
                "news_topic_reduce_size_multiplier": row.get("news_topic_reduce_size_multiplier", ""),
                "news_topic_watch_observe_only": row.get("news_topic_watch_observe_only", ""),
                "positive_entry_ok": row.get("positive_entry_ok", ""),
                "positive_entry_reason": row.get("positive_entry_reason", ""),
                "entry_gate_decision": row.get("_entry_gate_decision", ""),
                "entry_gate_reason": row.get("_entry_gate_reason", ""),
                "guard_severity_map": row.get("_guard_severity_map", ""),
                "risk_orch_scale": row.get("_risk_orch_scale", ""),
                "scale_zero_causes": row.get("_scale_zero_causes", ""),
                "validation_reduce_applied": row.get("_validation_reduce_applied", ""),
                "position_size_multiplier": row.get("_position_size_multiplier", ""),
                "cap_top_n": row.get("_cap_top_n", ""),
                "cap_fallback_reason": row.get("_cap_fallback_reason", ""),
                "qty_before_psm": row.get("_qty_before_psm", ""),
                "qty_after_psm": row.get("_qty_after_psm", ""),
                "final_qty": qty,
                "final_decision_reason": str(reason or ""),
                "normal_exec_quality_enabled": row.get("normal_exec_quality_enabled", ""),
                "normal_lob_check": row.get("normal_lob_check", ""),
                "normal_lob_status": row.get("normal_lob_status", ""),
                "normal_lob_reason": row.get("normal_lob_reason", ""),
                "normal_lob_source_ts": row.get("normal_lob_source_ts", ""),
                "normal_spread_bps": row.get("normal_spread_bps", ""),
                "normal_markout_1step_bps": row.get("normal_markout_1step_bps", ""),
                "normal_executable_qty": row.get("normal_executable_qty", ""),
                "normal_executable_price": row.get("normal_executable_price", ""),
                "normal_close_basis_entry_price": row.get("normal_close_basis_entry_price", ""),
                "normal_lob_applied_fill_price": row.get("normal_lob_applied_fill_price", ""),
                "normal_lob_fill_price_source": row.get("normal_lob_fill_price_source", ""),
                "normal_lob_fill_price_changed": row.get("normal_lob_fill_price_changed", ""),
                "normal_lob_ask_depth_levels": row.get("normal_lob_ask_depth_levels", ""),
                "normal_close_auction_action": row.get("normal_close_auction_action", ""),
                "normal_close_auction_reason": row.get("normal_close_auction_reason", ""),
                "normal_close_auction_close_pos": row.get("normal_close_auction_close_pos", ""),
                "normal_close_auction_day_range_pct": row.get("normal_close_auction_day_range_pct", ""),
                "normal_close_auction_v_accel": row.get("normal_close_auction_v_accel", ""),
                "normal_intraday_momentum_action": row.get("normal_intraday_momentum_action", ""),
                "normal_intraday_momentum_reason": row.get("normal_intraday_momentum_reason", ""),
                "normal_intraday_momentum_status": row.get("normal_intraday_momentum_status", ""),
                "normal_intraday_value_ratio": row.get("normal_intraday_value_ratio", ""),
                "normal_intraday_rechecked_v_accel": row.get("normal_intraday_rechecked_v_accel", ""),
                "adaptive_good_stock_route": row.get("adaptive_good_stock_route", ""),
                "adaptive_good_stock_action": row.get("adaptive_good_stock_action", ""),
                "adaptive_good_stock_original_block": row.get("adaptive_good_stock_original_block", ""),
                "adaptive_good_stock_entry_style": row.get("adaptive_good_stock_entry_style", ""),
                "adaptive_good_stock_size_hint": row.get("adaptive_good_stock_size_hint", ""),
                "adaptive_good_stock_confirmation_required": row.get("adaptive_good_stock_confirmation_required", ""),
                "adaptive_good_stock_quality_score": row.get("adaptive_good_stock_quality_score", ""),
                "adaptive_good_stock_post_split_qty_multiplier": row.get("adaptive_good_stock_post_split_qty_multiplier", ""),
                "adaptive_good_stock_gapup_override_max_pct": row.get("adaptive_good_stock_gapup_override_max_pct", ""),
                "adaptive_good_stock_gapup_gap": row.get("adaptive_good_stock_gapup_gap", ""),
                "adaptive_good_stock_qty_before": row.get("adaptive_good_stock_qty_before", ""),
                "adaptive_good_stock_qty_after": row.get("adaptive_good_stock_qty_after", ""),
                "adaptive_good_stock_policy_source": row.get("adaptive_good_stock_policy_source", ""),
                "normal_dynamic_slippage_pct": row.get("normal_dynamic_slippage_pct", ""),
                "normal_dynamic_slippage_reason": row.get("normal_dynamic_slippage_reason", ""),
                "normal_dynamic_slippage_multiplier": row.get("normal_dynamic_slippage_multiplier", ""),
                "is_split_entry_2nd": row.get("split_entry_2nd", ""),
                "surge_type_normalized": row.get("_surge_type_normalized", ""),
                "surge_shadow_status": row.get("_surge_shadow_status", ""),
                "surge_shadow_condition": row.get("_surge_shadow_condition", ""),
                "surge_shadow_mode": row.get("_surge_shadow_mode", ""),
                "surge_shadow_action": row.get("_surge_shadow_action", ""),
                "surge_shadow_reason": row.get("_surge_shadow_reason", ""),
                "surge_shadow_entry_timing": row.get("_surge_shadow_entry_timing", ""),
                "surge_shadow_type": row.get("_surge_shadow_type", ""),
                "active_response_label": row.get("active_response_label", ""),
                "active_response_reason": row.get("active_response_reason", ""),
                "active_response_next_check": row.get("active_response_next_check", ""),
                "surge_after_state": row.get("surge_after_state", ""),
                "surge_gap_up_override_gap": row.get("_surge_gap_up_override_gap", ""),
                "surge_gap_up_override_blockers": row.get("_surge_gap_up_override_blockers", ""),
                "surge_gap_up_override_active_label": row.get("_surge_gap_up_override_active_label", ""),
                "surge_gap_up_override_paper_readiness": row.get("_surge_gap_up_override_paper_readiness", ""),
                "surge_gap_up_override_paper_route_ok": row.get("_surge_gap_up_override_paper_route_ok", ""),
                "surge_gap_up_override_lob_status": row.get("_surge_gap_up_override_lob_status", ""),
                "surge_gap_up_override_orderflow_tag": row.get("_surge_gap_up_override_orderflow_tag", ""),
                "surge_gap_up_override_spread_bps": row.get("_surge_gap_up_override_spread_bps", ""),
                "surge_gap_up_override_markout_bps": row.get("_surge_gap_up_override_markout_bps", ""),
                "surge_gap_up_override_score_final": row.get("_surge_gap_up_override_score_final", ""),
                "surge_gap_up_block_gap": row.get("_surge_gap_up_block_gap", ""),
                "surge_gap_up_block_limit": row.get("_surge_gap_up_block_limit", ""),
                "surge_gap_up_block_entry_price": row.get("_surge_gap_up_block_entry_price", ""),
                "surge_gap_up_block_ref_close": row.get("_surge_gap_up_block_ref_close", ""),
                "surge_gap_up_block_ref_date": row.get("_surge_gap_up_block_ref_date", ""),
                "strategy_type": str(row.get("strategy_type")) if row.get("strategy_type") else ("SURGE" if is_surge else ("CARRYOVER" if is_carryover else "NORMAL")),
                "final_score": row.get("final_score", ""),
                "alloc_weight": row.get("alloc_weight", ""),
            }
        )

    top_score_cap_cfg = cfg.get("entry_signal_date_top_score_cap", {}) if isinstance(cfg.get("entry_signal_date_top_score_cap"), dict) else {}
    top_score_cap_enabled = bool(top_score_cap_cfg.get("enabled", True))
    top_score_cap_n = max(1, int(_to_int(top_score_cap_cfg.get("top_n", 5), 5)))
    top_score_cap_fallback_pool_lt_n = bool(top_score_cap_cfg.get("fallback_if_pool_lt_n", True))
    top_score_cap_fallback_no_overlap = bool(top_score_cap_cfg.get("fallback_when_selected_pool_has_no_top_overlap", True))
    top_score_cap_fallback_require_positive = bool(top_score_cap_cfg.get("fallback_require_positive_entry", True))
    top_score_cap_cache: Dict[str, Tuple[Optional[set[str]], str]] = {}
    top_score_cap_pool_codes_by_signal_date: Dict[str, set[str]] = {}
    top_score_cap_pool_has_overlap_cache: Dict[str, bool] = {}
    if top_score_cap_enabled and top_score_cap_fallback_no_overlap and isinstance(cdf, pd.DataFrame) and not cdf.empty and "code" in cdf.columns:
        try:
            _sig_series = cdf["signal_date"] if "signal_date" in cdf.columns else pd.Series(today_ymd, index=cdf.index)
            _tmp_pool = pd.DataFrame(
                {
                    "_signal_date": _sig_series.map(lambda x: _norm_ymd_text(x) if len(_norm_ymd_text(x)) == 8 else today_ymd),
                    "_code": cdf["code"].astype(str).map(lambda x: str(x).zfill(6)),
                }
            )
            for _sd, _grp in _tmp_pool.groupby("_signal_date"):
                top_score_cap_pool_codes_by_signal_date[str(_sd)] = {str(x).zfill(6) for x in _grp["_code"].tolist() if str(x).strip()}
        except Exception as _cap_pool_err:
            print(f"[ENTRY_CAP_FALLBACK] pool_prepare=FAIL reason={type(_cap_pool_err).__name__}:{_cap_pool_err}")

    # Pre-loop: close cutoff check (intraday_realtime only) — evaluated once, blocks all remaining entries
    _close_cutoff_min = int(ops_policy.get("close_cutoff_minutes", 10)) if isinstance(ops_policy, dict) else 10
    if _close_cutoff_min > 0 and intraday_realtime_mode and not fail_closed_triggered:
        try:
            _market_close_dt = datetime.strptime(f"{today_ymd}153000", "%Y%m%d%H%M%S")
            _cutoff_dt = _market_close_dt - timedelta(minutes=_close_cutoff_min)
            if datetime.now() >= _cutoff_dt:
                fail_closed_triggered = True
                fail_closed_reason = f"CLOSE_CUTOFF(cutoff={_cutoff_dt.strftime('%H:%M')})"
                print(f"[CLOSE_CUTOFF_BLOCK] cutoff={_cutoff_dt.strftime('%H:%M')} now={datetime.now().strftime('%H:%M:%S')} → max_new forced=0")
        except Exception:
            pass

    adaptive_good_stock_route_map = _adaptive_good_stock_route_map(cfg)
    if adaptive_good_stock_route_map:
        print(f"[ADAPTIVE_GOOD_STOCK_ENTRY] loaded candidates={len(adaptive_good_stock_route_map)}")

    for _, r in cdf.iterrows():
        evaluated_count += 1
        is_open_order_replay = _truthy(r.get("_replay_order")) or str(r.get("carry_reason", "")).strip().upper() == "OPEN_ORDER_REPLAY"
        is_carryover_row = _truthy(r.get("_carryover")) or str(r.get("carry_reason", "")).strip().upper() in {"ENTRY_RETRY_READY", "ENTRY_RECHECK_READY", "EXTEND_CARRYOVER", "REVALIDATE_PENDING"}
        is_surge_immediate = _truthy(r.get("_surge_immediate"))
        row_entry_timing = str(r.get("entry_timing") or r.get("surge_type_entry_timing") or "").strip().lower()
        force_next_open_entry = row_entry_timing in {"next_open", "nextopen"}
        surge_type_text = str(r.get("surge_type", "") or "").strip()
        if surge_type_text.lower() in {"nan", "none", "null", "<na>"}:
            surge_type_text = ""
        r["_surge_type_normalized"] = surge_type_text
        _surge_shadow = _surge_realtime_shadow_fields(
            r,
            is_surge_immediate=bool(is_surge_immediate),
            row_entry_timing=row_entry_timing,
            surge_type_text=surge_type_text,
        )
        for _shadow_key, _shadow_val in _surge_shadow.items():
            r[f"_{_shadow_key}"] = _shadow_val
        has_surge_type_policy = bool(surge_type_text)
        is_split_2nd = _truthy(r.get("split_entry_2nd"))
        r["_entry_gate_decision"] = str(r.get("_entry_gate_decision", "") or entry_decision_code or "")
        r["_entry_gate_reason"] = str(r.get("_entry_gate_reason", "") or entry_decision_reason or "")
        r["_guard_severity_map"] = "execution=hard,outlier=soft,sigma=soft,macro_news=soft,integrity=advisory,backtest_validation=advisory"
        r["_position_size_multiplier"] = float(position_size_multiplier)
        _row_entry_gate_reason = str(r.get("_entry_gate_reason", "") or "")
        r["_validation_reduce_applied"] = "validation_reduce" in _row_entry_gate_reason
        r["_cap_top_n"] = max(1, int(_to_int((cfg.get("entry_signal_date_top_score_cap", {}) if isinstance(cfg.get("entry_signal_date_top_score_cap"), dict) else {}).get("top_n", 5), 5)))
        code = str(r["code"]).zfill(6)
        name = str(r.get("name", "") or "")
        signal_date = str(r.get("signal_date") or today_ymd)
        signal_date = re.sub(r"[^0-9]", "", str(signal_date or ""))
        if len(signal_date) != 8:
            signal_date = today_ymd
        if (
            _is_sector_fallback_observe_only_row(r)
            and not is_open_order_replay
            and not is_split_2nd
            and not is_surge_immediate
        ):
            print(f"[SECTOR_FALLBACK_OBSERVE_ONLY] code={code} signal_date={signal_date}")
            _record_decision(
                code=code, signal_date=signal_date, signal="HOLD", reason="SECTOR_FALLBACK_OBSERVE_ONLY",
                row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
            )
            continue
        if top_score_cap_enabled and not is_open_order_replay and not is_surge_immediate:
            if signal_date not in top_score_cap_cache:
                top_score_cap_cache[signal_date] = _load_signal_date_top_codes_by_score(signal_date, top_n=top_score_cap_n)
            top_codes, top_msg = top_score_cap_cache[signal_date]
            if top_codes is not None and code not in top_codes:
                fallback_allowed = False
                _pool_size = len(top_codes)
                if top_score_cap_fallback_pool_lt_n and _pool_size < top_score_cap_n:
                    fallback_allowed = True
                    print(
                        f"[ENTRY_CAP_FALLBACK] pool_size={_pool_size} < top_n={top_score_cap_n} "
                        f"→ cap released for signal_date={signal_date}"
                    )
                elif top_score_cap_fallback_no_overlap:
                    if signal_date not in top_score_cap_pool_has_overlap_cache:
                        pool_codes = top_score_cap_pool_codes_by_signal_date.get(signal_date, set())
                        top_score_cap_pool_has_overlap_cache[signal_date] = bool(pool_codes & set(top_codes))
                    pool_has_overlap = bool(top_score_cap_pool_has_overlap_cache.get(signal_date, True))
                    positive_ok = True
                    if top_score_cap_fallback_require_positive:
                        positive_ok = bool(r.get("positive_entry_ok", False))
                    fallback_allowed = (not pool_has_overlap) and positive_ok
                if fallback_allowed:
                    loop_state["cap_top3_fallback_count"] = int(_to_int(loop_state.get("cap_top3_fallback_count"), 0) or 0) + 1
                    reason = f"CAP_SIGNALDATE_TOP{top_score_cap_n}_FALLBACK_NO_POOL_OVERLAP"
                    r["_cap_fallback_reason"] = reason
                    print(f"[ENTRY_CAP_FALLBACK] code={code} signal_date={signal_date} reason={reason} {top_msg}")
                else:
                    max_new_skip_count += 1
                    reason = f"CAP_SIGNALDATE_TOP{top_score_cap_n}_BY_SCORE"
                    print(f"[ENTRY_CAP_BLOCK] code={code} signal_date={signal_date} reason={reason} {top_msg}")
                    _record_decision(
                        code=code, signal_date=signal_date, signal="HOLD", reason=reason,
                        row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
                    )
                    continue
        # Fail-Closed propagation: hard block from close_cutoff or prior BLOCK event
        if (
            fail_closed_triggered
            and not is_open_order_replay
            and ((not is_surge_immediate) or str(fail_closed_reason or "").startswith("CLOSE_CUTOFF"))
        ):
            max_new_skip_count += 1
            _fail_closed_decision_reason = f"FAIL_CLOSED_PROPAGATE({fail_closed_reason})"
            _record_decision(
                code=code, signal_date=signal_date, signal="HOLD", reason=f"FAIL_CLOSED_PROPAGATE({fail_closed_reason})",
                row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
            )
            if str(fail_closed_reason or "").startswith("CLOSE_CUTOFF") and not is_carryover_row:
                recheck_row = _build_recheck_pending_row(
                    r,
                    code=code,
                    signal_date=signal_date,
                    name=name,
                    reason=_fail_closed_decision_reason,
                    today_ymd=today_ymd,
                )
                if recheck_row:
                    pending_carry_rows.append(recheck_row)
                    print(
                        f"[ENTRY_RECHECK_QUEUE] code={code} signal_date={signal_date} "
                        f"next_session={recheck_row.get('entry_day_override')} reason=CLOSE_CUTOFF"
                    )
            continue

        if (not is_open_order_replay) and (not is_surge_immediate) and (not is_split_2nd) and new_count >= max_new:
            max_new_skip_count += 1
            print(f"[SKIP_MAX_NEW] code={code} new_count={new_count} >= max_new={max_new}")
            _record_decision(
                code=code, signal_date=signal_date, signal="HOLD", reason="MAX_NEW_REACHED",
                row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
            )
            _existing_order_id = f"PAPER_BUY_{code}_{signal_date}"
            if _backfill_horizon_for_existing_order(open_pos, _existing_order_id, r, cfg):
                print(f"[HORIZON] backfilled existing position metadata order_id={_existing_order_id}")
            continue
        if (not is_open_order_replay) and is_surge_immediate and surge_new_count >= max_new_surge:
            max_new_skip_count += 1
            print(f"[SKIP_SURGE_MAX_NEW] code={code} surge_new_count={surge_new_count} >= max_new_surge={max_new_surge}")
            _record_decision(
                code=code, signal_date=signal_date, signal="HOLD", reason="SURGE_MAX_NEW_REACHED",
                row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
            )
            continue
        _max_new_per_type = int(_to_int(
            ((cfg.get("surge_entry_policy") or {}).get("type_policy") or {}).get("max_new_per_type", 0), 0
        ))
        if (
            not is_open_order_replay
            and is_surge_immediate
            and _max_new_per_type > 0
            and surge_type_text
            and surge_type_day_counts.get(surge_type_text, 0) >= _max_new_per_type
        ):
            max_new_skip_count += 1
            print(f"[SKIP_SURGE_TYPE_MAX] code={code} surge_type={surge_type_text} "
                  f"type_count={surge_type_day_counts.get(surge_type_text, 0)} >= max_new_per_type={_max_new_per_type}")
            _record_decision(
                code=code, signal_date=signal_date, signal="HOLD", reason="SURGE_TYPE_MAX_REACHED",
                row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
            )
            continue
        if (
            t_enabled
            and (not is_open_order_replay)
            and (not is_surge_immediate)
            and t_ai_codes
            and code in t_ai_codes
            and t_ai_max_open_count > 0
        ):
            ai_open_count = len(
                [
                    p for p in open_pos
                    if isinstance(p, dict) and norm_code(p.get("code", "")) in t_ai_codes
                ]
            )
            if ai_open_count >= t_ai_max_open_count:
                print(
                    f"[SKIP_AI_FOCUS_CAP] code={code} ai_open_count={ai_open_count} "
                    f">= max_open={t_ai_max_open_count}"
                )
                _record_decision(
                    code=code, signal_date=signal_date, signal="HOLD", reason="AI_FOCUS_MAX_OPEN_CAP",
                    row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
                )
                continue
        open_slots = _count_open_position_slots(open_pos)
        if (
            (not is_open_order_replay)
            and max_positions > 0
            and open_slots >= max_positions
            and not bool(max_positions_override_allowed)
        ):
            print(
                f"[SKIP_MAX_POSITIONS] code={code} open_slots={open_slots} "
                f"raw_open={len(open_pos)} >= max_positions={max_positions}"
            )
            _record_decision(
                code=code, signal_date=signal_date, signal="HOLD", reason="MAX_POSITIONS_BLOCK",
                row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
            )
            max_positions_blocked = True
            break
        if is_carryover_row and carryover_market_gate_block:
            carryover_revalidate_summary["loaded"] = int(carryover_revalidate_summary.get("loaded", 0))
            carryover_revalidate_summary["entry_retry_ready_rows"] = int(carryover_revalidate_summary.get("entry_retry_ready_rows", 0))
            carryover_revalidate_summary["revalidate_failed_rows"] = int(carryover_revalidate_summary.get("revalidate_failed_rows", 0)) + 1
            carryover_revalidate_summary.setdefault("failed_codes", []).append(code)
            failed_reason_counts = carryover_revalidate_summary.setdefault("failed_reason_counts", {})
            failed_reason_counts["MARKET_GATE_FAILED"] = int(failed_reason_counts.get("MARKET_GATE_FAILED", 0)) + 1
            print(f"[CARRYOVER_GATE] REVALIDATE_FAILED code={code} reason=MARKET_GATE_FAILED regime={market_regime} risk_off={risk_off_enabled}")
            _record_decision(
                code=code, signal_date=signal_date, signal="HOLD", reason="MARKET_GATE_FAILED",
                row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
            )
            continue
        if block_same_sector_entry and entry_sector_col and blocked_sector_value:
            row_sector = str(r.get(entry_sector_col, "") or "").strip()
            if row_sector and row_sector == blocked_sector_value:
                print(
                    f"[SKIP_SECTOR_CONCENTRATION] code={code} sector={row_sector} concentration={sector_concentration:.2f}"
                )
                _record_decision(
                    code=code, signal_date=signal_date, signal="HOLD", reason="SECTOR_CONCENTRATION_BLOCK",
                    row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
                )
                continue

        if intraday_realtime_mode and same_close_entry_mode and (not is_open_order_replay):
            signal_date = today_ymd

        raw_sig_key = f"{code}:{signal_date}"
        signal_age_days = _ymd_days_ago(today_ymd, signal_date)
        replay_row_ok = False
        effective_signal_date = signal_date
        replay_key = ""
        carry_origin_reason = str(r.get("carry_origin_reason", "") or "").strip().upper()
        fallback_stage = max(0, _to_int(r.get("fallback_stage", 0), 0))
        fb_policy = (ops_policy.get("entry_fallback_policy", {}) if isinstance(ops_policy, dict) else {})
        fb_enabled = bool(ops_enabled and isinstance(fb_policy, dict) and fb_policy.get("enabled"))
        fb_signal_valid_days = max(1, int(_to_int(fb_policy.get("signal_valid_days", 2), 2)))
        fb_max_stage = max(0, int(_to_int(fb_policy.get("max_stage", 3), 3)))
        if fb_enabled and signal_age_days is not None and signal_age_days > fb_signal_valid_days:
            print(
                f"[SKIP_FALLBACK_SIGNAL_EXPIRED] code={code} signal_date={signal_date} "
                f"age_days={signal_age_days} valid_days={fb_signal_valid_days}"
            )
            _record_decision(
                code=code, signal_date=signal_date, signal="HOLD", reason="FALLBACK_SIGNAL_EXPIRED",
                row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
            )
            continue

        # strict_same_day_only: signal_date != today → DROP_STALE_SIGNAL (no exceptions for carryover)
        _strict_same_day = bool(ops_enabled and ops_policy.get("strict_same_day_only", False))
        if _strict_same_day and not is_open_order_replay and not is_split_2nd:
            if effective_signal_date != today_ymd:
                print(
                    f"[DROP_STALE_SIGNAL] code={code} signal_date={effective_signal_date} "
                    f"today={today_ymd} age_days={signal_age_days}"
                )
                _record_decision(
                    code=code, signal_date=effective_signal_date, signal="HOLD", reason="DROP_STALE_SIGNAL",
                    row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
                )
                continue

        # Signal TTL: expire if captured_at is too old (intraday_realtime only)
        _ttl_min = int(ops_policy.get("signal_ttl_minutes", 0)) if isinstance(ops_policy, dict) else 0
        if _ttl_min > 0 and intraday_realtime_mode and not is_open_order_replay:
            _cap_at = str(r.get("captured_at") or "").strip()
            if _cap_at:
                try:
                    _age_min = (datetime.now() - datetime.fromisoformat(_cap_at)).total_seconds() / 60.0
                    if _age_min > _ttl_min:
                        ttl_expired_count += 1
                        print(f"[SIGNAL_TTL_EXPIRED] code={code} signal_date={effective_signal_date} age_min={_age_min:.1f} > ttl={_ttl_min}")
                        _record_decision(
                            code=code, signal_date=effective_signal_date, signal="HOLD", reason="SIGNAL_TTL_EXPIRED",
                            row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
                        )
                        continue
                except Exception:
                    pass

        if is_surge_immediate and not is_open_order_replay and not is_split_2nd:
            _surge_policy = cfg.get("surge_entry_policy", {}) if isinstance(cfg.get("surge_entry_policy"), dict) else {}
            _surge_max_same_code = max(0, int(_to_int(_surge_policy.get("max_same_code_per_day"), 0) or 0))
            if _surge_max_same_code > 0:
                _same_code_count = int(_same_code_day_buy_counts.get(code, 0) or 0)
                if _same_code_count >= _surge_max_same_code:
                    print(
                        f"[SKIP_SURGE_SAME_CODE_DAY_LIMIT] code={code} "
                        f"count={_same_code_count} >= max={_surge_max_same_code} today={today_ymd}"
                    )
                    _record_decision(
                        code=code, signal_date=effective_signal_date, signal="HOLD", reason="same_code_day_capped",
                        row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
                    )
                    continue

        if (not allow_same_signal_reentry) and (not is_open_order_replay) and (not is_split_2nd):
            _same_code_count = int(_same_code_day_buy_counts.get(code, 0) or 0)
            if _same_code_count > 0:
                print(
                    f"[SKIP_SAME_CODE_DAY_ALREADY_BUY] code={code} "
                    f"count={_same_code_count} today={today_ymd}"
                )
                processed_skip_count += 1
                _record_decision(
                    code=code, signal_date=effective_signal_date, signal="HOLD", reason="SAME_CODE_DAY_ALREADY_BUY",
                    row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
                )
                continue

        # Order retry limit: block same code if already attempted too many times today
        _max_retry = int(ops_policy.get("max_retry_per_code_per_day", 0)) if isinstance(ops_policy, dict) else 0
        if _max_retry > 0 and not is_open_order_replay and not is_split_2nd:
            _retry_key = f"{code}:{today_ymd}"
            _retry_so_far = _daily_retry_counts.get(_retry_key, 0)
            if _retry_so_far >= _max_retry:
                retry_blocked_count += 1
                print(f"[RETRY_LIMIT_EXCEEDED] code={code} attempts={_retry_so_far} >= max={_max_retry} today={today_ymd}")
                _record_decision(
                    code=code, signal_date=effective_signal_date, signal="HOLD", reason="RETRY_LIMIT_EXCEEDED",
                    row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
                )
                continue
            _daily_retry_counts[_retry_key] = _retry_so_far + 1

        entry_day_override = re.sub(r"[^0-9]", "", str(r.get("entry_day_override") or ""))[:8]
        if is_open_order_replay and len(entry_day_override) == 8:
            replay_source_order_id = str(r.get("replay_source_order_id", "") or "").strip()
            replay_key = f"{code}:OPEN_REPLAY:{entry_day_override}:{replay_source_order_id}"
            if replay_key in processed_signals:
                processed_skip_count += 1
                continue

        raw_sig_processed = raw_sig_key in processed_signals
        raw_sig_committed = raw_sig_key in committed_signal_keys
        if (not allow_same_signal_reentry) and (not is_open_order_replay) and replay_enabled and raw_sig_processed and raw_sig_committed:
            if replay_global_ok and (signal_age_days is not None) and (signal_age_days >= replay_min_age_days):
                prev_day = prev_trading_date(px, code, today_ymd)
                if prev_day and re.fullmatch(r"\d{8}", prev_day):
                    effective_signal_date = prev_day
                    replay_row_ok = True

        sig_key = f"{code}:{effective_signal_date}" if not is_split_2nd else f"{code}:{effective_signal_date}:split2nd"
        if (not allow_same_signal_reentry) and (not is_open_order_replay) and (not is_split_2nd) and raw_sig_processed and raw_sig_committed:
            if replay_row_ok and (sig_key not in processed_signals):
                stale_replay_used_count += 1
            else:
                processed_skip_count += 1
                _record_decision(
                    code=code, signal_date=effective_signal_date, signal="HOLD", reason="PROCESSED_DUPLICATE",
                    row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
                )
                continue
        elif (not allow_same_signal_reentry) and (not is_open_order_replay) and (not is_split_2nd) and (sig_key in processed_signals):
            processed_skip_count += 1
            _record_decision(
                code=code, signal_date=effective_signal_date, signal="HOLD", reason="SIGNAL_ALREADY_COMMITTED",
                row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
            )
            continue
        # Always block opening a second position for an already-open code.
        # Reentry policy applies after position close, not while a position is still open.
        # This prevents stale_signal_replay from duplicating open lots and causing SELL qty mismatch.
        if (not is_open_order_replay) and (not is_split_2nd) and code in open_codes:
            print(f"[SKIP_CODE_ALREADY_OPEN] code={code} signal_date={effective_signal_date} effective={effective_signal_date}")
            processed_skip_count += 1
            _record_decision(
                code=code, signal_date=effective_signal_date, signal="HOLD", reason="CODE_ALREADY_OPEN",
                row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
            )
            continue
        use_same_close_today = same_close_entry_mode and (not force_next_open_entry) and (not is_open_order_replay) and effective_signal_date == today_ymd
        use_intraday_realtime_entry = bool(intraday_realtime_mode and use_same_close_today and (not is_open_order_replay))
        _normal_intraday_block_reason = _normal_intraday_realtime_block_reason(
            cfg=cfg,
            is_intraday_realtime_entry=bool(use_intraday_realtime_entry),
            is_surge_immediate=bool(is_surge_immediate),
            is_split_2nd=bool(is_split_2nd),
            is_open_order_replay=bool(is_open_order_replay),
            entry_gate_decision=str(r.get("_entry_gate_decision", "") or entry_decision_code or ""),
            entry_gate_reason=str(r.get("_entry_gate_reason", "") or entry_decision_reason or ""),
            position_size_multiplier=float(position_size_multiplier),
            run_label=str(RUN_LABEL or "main"),
            p0_rolling_dd_abs=float(p0_rolling_dd_abs),
            p0_rolling_dd_source=str(p0_rolling_dd_source),
        )
        if _normal_intraday_block_reason:
            max_new_skip_count += 1
            print(f"[NORMAL_INTRADAY_POLICY_BLOCK] code={code} reason={_normal_intraday_block_reason}")
            _record_decision(
                code=code, signal_date=effective_signal_date, signal="HOLD", reason=_normal_intraday_block_reason,
                row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
            )
            continue
        _source_entry_decision_runtime = _clean_active_response_cell(r.get("source_entry_decision", "")).upper()
        _source_entry_blocked_runtime = (
            _truthy(_source_entry_cell_text(r.get("source_entry_blocked", "")))
            or _source_entry_decision_runtime == "ENTRY_BLOCKED"
        )
        _source_entry_probe_ready_override = bool(
            is_surge_immediate
            and use_intraday_realtime_entry
            and _clean_active_response_cell(r.get("active_response_label", "")).upper() == "PROBE_READY"
            and (
                _clean_active_response_cell(r.get("paper_order_readiness", "")).upper() == "PAPER_READY"
                or _clean_active_response_cell(r.get("surge_paper_order_readiness", "")).upper() == "PAPER_READY"
            )
            and (_truthy(r.get("paper_order_route", "")) or _truthy(r.get("surge_paper_order_route", "")))
            and (_truthy(r.get("paper_probe_allowed", "")) or _truthy(r.get("surge_paper_probe_allowed", "")))
        )
        if _source_entry_blocked_runtime and not is_open_order_replay and not is_split_2nd and not _source_entry_probe_ready_override:
            reason = _clean_active_response_cell(r.get("source_entry_reason", "")) or _source_entry_decision_runtime or "SOURCE_ENTRY_BLOCKED"
            max_new_skip_count += 1
            print(f"[SOURCE_ENTRY_POLICY_BLOCK] code={code} reason={reason}")
            _record_decision(
                code=code, signal_date=effective_signal_date, signal="HOLD", reason=f"SOURCE_ENTRY_POLICY_BLOCK:{reason}",
                row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
            )
            continue
        if _source_entry_blocked_runtime and _source_entry_probe_ready_override:
            r["_source_entry_policy_probe_ready_override"] = 1
            print(f"[SOURCE_ENTRY_POLICY_PROBE_READY_OVERRIDE] code={code} reason={_clean_active_response_cell(r.get('source_entry_reason', '')) or _source_entry_decision_runtime}")
        if use_same_close_today and carry_origin_reason in {"ENTRY_DAY_WAIT", "NO_NEXT_DAY"}:
            entry_day_override = ""
        entry_day = effective_signal_date if use_same_close_today else (entry_day_override if len(entry_day_override) == 8 else next_trading_date(px, code, effective_signal_date))
        if not entry_day:
            no_next_day_count += 1
            print(f"[SKIP_NO_NEXT_DAY] code={code} signal_date={effective_signal_date} raw_signal_date={signal_date} reason=no_next_trading_date_in_prices")
            _record_decision(
                code=code, signal_date=effective_signal_date, signal="HOLD", reason="NO_NEXT_TRADING_DAY",
                row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
            )
            # strict_same_day_only: no carryover — record as EXPIRED (FAIL-CLOSED)
            if _strict_same_day:
                print(f"[EXPIRED_NO_NEXT_DAY] code={code} signal_date={effective_signal_date} reason=FAIL_CLOSED")
            elif ops_enabled and bool(ops_policy.get("carryover_no_next_day_enabled", True)):
                next_carry_reason = "EXTEND_CARRYOVER" if _truthy(r.get("_carryover")) or str(r.get("carry_reason", "")).strip().upper() == "ENTRY_RETRY_READY" else "REVALIDATE_PENDING"
                pending_carry_rows.append({
                    "signal_date": effective_signal_date,
                    "code": code,
                    "name": name,
                    "carry_reason": next_carry_reason,
                    "carry_origin_reason": "NO_NEXT_DAY",
                    "captured_at": now_ts(),
                    "carryover_count": max(1, _to_int(r.get("carryover_count", 0), 0) + 1),
                    "score": r.get("score", None),
                    "final_score": r.get("final_score", None),
                    "entry_timing": row_entry_timing,
                    "surge_type_entry_timing": r.get("surge_type_entry_timing", ""),
                    "_surge_immediate": 1 if is_surge_immediate else 0,
                    "surge_type": r.get("surge_type", ""),
                    "surge_per_symbol_alloc_pct": r.get("surge_per_symbol_alloc_pct", None),
                    "surge_total_alloc_pct": r.get("surge_total_alloc_pct", None),
                    "surge_type_qty_multiplier": r.get("surge_type_qty_multiplier", None),
                    "surge_type_policy_note": r.get("surge_type_policy_note", ""),
                    "surge_type_first_ratio": r.get("surge_type_first_ratio", None),
                    "surge_type_stop_pct": r.get("surge_type_stop_pct", None),
                    "surge_type_tp_pct": r.get("surge_type_tp_pct", None),
                    "surge_type_max_hold_days": r.get("surge_type_max_hold_days", None),
                })
            continue

        # Quote staleness guard: block if entry_day data is too old vs today
        _quote_max_age = int(ops_policy.get("quote_max_age_days", 1)) if isinstance(ops_policy, dict) else 1
        if _quote_max_age > 0 and entry_day and not is_open_order_replay:
            _quote_age = _ymd_days_ago(today_ymd, entry_day)
            if _quote_age is not None and _quote_age > _quote_max_age:
                quote_stale_blocked_count += 1
                print(f"[QUOTE_STALE_BLOCK] code={code} entry_day={entry_day} age_days={_quote_age} > max={_quote_max_age}")
                _record_decision(
                    code=code, signal_date=effective_signal_date, signal="HOLD", reason="QUOTE_STALE_BLOCK",
                    row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
                )
                if _fc_propagate and not fail_closed_triggered:
                    fail_closed_triggered = True
                    fail_closed_reason = f"QUOTE_STALE_BLOCK(code={code},age={_quote_age}d)"
                    print(f"[FAIL_CLOSED] triggered by QUOTE_STALE_BLOCK code={code} → max_new=0 for remaining")
                continue

        ohlc = get_ohlc(px, code, entry_day)
        if not ohlc and is_surge_immediate and use_intraday_realtime_entry:
            surge_rt_price = float(_to_float(r.get("current_price"), 0.0) or 0.0)
            if surge_rt_price > 0:
                ohlc = {
                    "open": surge_rt_price,
                    "high": surge_rt_price,
                    "low": surge_rt_price,
                    "close": surge_rt_price,
                    "volume": float(_to_float(r.get("volume_now"), 0.0) or 0.0),
                    "trading_value": float(_to_float(r.get("trading_value"), 0.0) or 0.0),
                }
                print(
                    f"[SURGE_RT_PRICE_FALLBACK] code={code} entry_day={entry_day} "
                    f"price={surge_rt_price:.4f} source=surge_alert_current_price"
                )
        if not ohlc and use_intraday_realtime_entry and bool(_truthy(r.get("promoted_recheck", False))):
            promoted_rt_price = float(_to_float(r.get("current_price"), _to_float(r.get("close"), 0.0)) or 0.0)
            promoted_trading_value = float(_to_float(r.get("current_trading_value"), _to_float(r.get("trading_value"), 0.0)) or 0.0)
            if promoted_rt_price > 0:
                ohlc = {
                    "open": promoted_rt_price,
                    "high": promoted_rt_price,
                    "low": promoted_rt_price,
                    "close": promoted_rt_price,
                    "volume": 0.0,
                    "trading_value": promoted_trading_value,
                }
                print(
                    f"[PROMOTED_RECHECK_RT_PRICE_FALLBACK] code={code} entry_day={entry_day} "
                    f"price={promoted_rt_price:.4f} source=promoted_recheck_current_price"
                )
        if not ohlc:
            _missing_ohlc_action = _resolve_missing_entry_ohlc_action(
                r,
                ops_policy,
                ops_enabled=ops_enabled,
                strict_same_day=_strict_same_day,
                use_same_close_today=use_same_close_today,
                fallback_stage=fallback_stage,
                effective_signal_date=effective_signal_date,
                entry_day=entry_day,
                today_ymd=today_ymd,
                code=code,
                name=name,
                row_entry_timing=row_entry_timing,
                is_surge_immediate=is_surge_immediate,
            )
            if str(_missing_ohlc_action.get("action", "") or "") == "RECORD_BLOCK":
                _record_decision(
                    code=code, signal_date=effective_signal_date, signal="HOLD", reason=str(_missing_ohlc_action.get("reason", "") or ""),
                    row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
                )
            elif str(_missing_ohlc_action.get("action", "") or "") == "APPEND_CARRY":
                _carry_row = _missing_ohlc_action.get("carry_row")
                if isinstance(_carry_row, dict):
                    pending_carry_rows.append(_carry_row)
            continue
        _entry_price_fit = _resolve_entry_price_with_fallback(
            r,
            cfg,
            fb_policy,
            px,
            code=code,
            entry_day=entry_day,
            effective_signal_date=effective_signal_date,
            ohlc=ohlc,
            fallback_stage=fallback_stage,
            fb_enabled=fb_enabled,
            fb_max_stage=fb_max_stage,
            use_same_close_today=use_same_close_today,
            use_intraday_realtime_entry=use_intraday_realtime_entry,
            strict_same_day=_strict_same_day,
            is_surge_immediate=is_surge_immediate,
        )
        _entry_price_action = str(_entry_price_fit.get("action", "") or "")
        if _entry_price_action == "RECORD_BLOCK":
            _entry_price_reason = str(_entry_price_fit.get("reason", "") or "")
            _record_decision(
                code=code, signal_date=effective_signal_date, signal="HOLD", reason=_entry_price_reason,
                row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
            )
            continue
        if _entry_price_action == "CARRY_NEXT_OPEN_UNFILLED":
            if bool(_entry_price_fit.get("append_carry", False)):
                pending_carry_rows.append({
                    "signal_date": effective_signal_date,
                    "code": code,
                    "name": name,
                    "carry_reason": "ENTRY_RETRY_READY",
                    "carry_origin_reason": "NEXT_OPEN_UNFILLED",
                    "entry_day_override": str(_entry_price_fit.get("next_day2", "") or ""),
                    "captured_at": now_ts(),
                    "carryover_count": max(1, _to_int(r.get("carryover_count", 0), 0) + 1),
                    "fallback_stage": 2,
                    "score": r.get("score", None),
                    "final_score": r.get("final_score", None),
                })
            continue
        if _entry_price_action != "OK":
            continue
        entry_price = float(_entry_price_fit.get("entry_price", 0.0) or 0.0)
        entry_ts_value = str(_entry_price_fit.get("entry_ts_value", f"{entry_day}T09:00:00") or f"{entry_day}T09:00:00")
        fallback_note = str(_entry_price_fit.get("fallback_note", "") or "")
        # Execution quality guard: block if slippage vs signal_date close exceeds threshold
        _exec_quality_guard = _evaluate_entry_execution_quality_guard(
            ops_policy,
            px,
            code=code,
            effective_signal_date=effective_signal_date,
            entry_price=entry_price,
            is_open_order_replay=is_open_order_replay,
        )
        if bool(_exec_quality_guard.get("blocked", False)):
            exec_quality_blocked_count += 1
            _record_decision(
                code=code, signal_date=effective_signal_date, signal="HOLD", reason="EXEC_QUALITY_FAIL",
                row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
            )
            if _fc_propagate and not fail_closed_triggered:
                fail_closed_triggered = True
                fail_closed_reason = f"EXEC_QUALITY_FAIL(code={code},slip={float(_exec_quality_guard.get('slippage', 0.0)):.4f})"
                print(f"[FAIL_CLOSED] triggered by EXEC_QUALITY_FAIL code={code} → max_new=0 for remaining")
            continue
        _entry_price_validity = _normalize_entry_price_for_entry_guard(entry_price)
        entry_price = float(_entry_price_validity.get("entry_price", 0.0))
        if not bool(_entry_price_validity.get("valid", False)):
            if is_surge_immediate:
                print(f"[SKIP_SURGE_ENTRY_PRICE] code={code} entry_price={entry_price}")
            continue

        _open_chase_action = _evaluate_open_chase_entry_guard(
            r,
            cfg,
            adaptive_good_stock_route_map,
            code=code,
            entry_price=entry_price,
            open_price=ohlc.get("open", 0.0),
            is_split_2nd=is_split_2nd,
            is_open_order_replay=is_open_order_replay,
        )
        for _open_chase_key, _open_chase_val in (_open_chase_action.get("row_updates") or {}).items():
            r[_open_chase_key] = _open_chase_val
        if bool(_open_chase_action.get("blocked", False)):
            _open_chase_guard = _open_chase_action.get("guard", {}) if isinstance(_open_chase_action.get("guard"), dict) else {}
            reason = str(_open_chase_action.get("reason") or "OPEN_CHASE_BLOCK")
            chase_pct = _open_chase_guard.get("open_to_entry_chase_pct")
            max_chase_pct = float(_open_chase_guard.get("max_open_to_entry_chase_pct") or 0.0)
            if chase_pct is None:
                print(
                    f"[OPEN_CHASE_BLOCK] code={code} entry={entry_price:.2f} "
                    f"open={float(_open_chase_guard.get('open_price') or 0.0):.2f} reason={reason}"
                )
            else:
                print(
                    f"[OPEN_CHASE_BLOCK] code={code} chase={float(chase_pct):.4f} "
                    f"> max={max_chase_pct:.4f} entry={entry_price:.2f} "
                    f"open={float(_open_chase_guard.get('open_price') or 0.0):.2f}"
            )
            if bool(_open_chase_action.get("adaptive_allowed", False)):
                _open_chase_adaptive_row = _open_chase_action.get("adaptive_row")
                _mark_adaptive_good_stock(
                    r,
                    _open_chase_adaptive_row if isinstance(_open_chase_adaptive_row, dict) else {},
                    reason,
                    "OPEN_CHASE_SOFTENED_TO_PROBE",
                )
                print(f"[ADAPTIVE_GOOD_STOCK_ENTRY] code={code} route={str(_open_chase_action.get('adaptive_route') or '').strip().upper()} original={reason} action=PROBE")
            else:
                _record_decision(
                    code=code,
                    signal_date=effective_signal_date,
                    signal="HOLD",
                    reason=reason,
                    row=r,
                    is_surge=is_surge_immediate,
                    is_replay=is_open_order_replay,
                    is_carryover=is_carryover_row,
                    entry_price=entry_price,
                )
                continue

        # split entry 2nd: dip condition check
        if is_split_2nd:
            _spl = cfg.get("split_entry", {}) if isinstance(cfg, dict) else {}
            _first_price = float(r.get("split_first_entry_price") or 0)
            _dip_min = float(_spl.get("second_dip_min_pct", 0.01))
            _dip_max = float(_spl.get("second_dip_max_pct", 0.03))
            _dip = ((_first_price - float(entry_price)) / _first_price) if _first_price > 0 else None
            def _keep_split2_pending(reason: str) -> None:
                pending_carry_rows.append({
                    "signal_date": effective_signal_date,
                    "code": code,
                    "name": name,
                    "carry_reason": "ENTRY_RETRY_READY",
                    "carry_origin_reason": "SPLIT_ENTRY_2ND",
                    "split_entry_2nd": True,
                    "split_remaining_qty": r.get("split_remaining_qty", 0),
                    "split_first_entry_price": r.get("split_first_entry_price", ""),
                    "split_first_order_id": r.get("split_first_order_id", ""),
                    "split_first_qty": r.get("split_first_qty", ""),
                    "split_first_fill_id": r.get("split_first_fill_id", ""),
                    "split_signal_id": r.get("split_signal_id", sig_key),
                    "split_current_entry_price": float(entry_price),
                    "split_dip_pct": "" if _dip is None else float(_dip),
                    "split_dip_min_pct": float(_dip_min),
                    "split_dip_max_pct": float(_dip_max),
                    "split_confirmation_reason": reason,
                    "captured_at": r.get("captured_at", now_ts()),
                    "carryover_count": max(1, _to_int(r.get("carryover_count", 1), 1)),
                    "carryover_max_age_days": max(1, _to_int(r.get("carryover_max_age_days", _spl.get("second_entry_max_days", 1)), 1)),
                    "score": r.get("score", None),
                    "final_score": r.get("final_score", None),
                    "reason": reason,
                })
            if _first_price > 0 and _dip is not None:
                if _dip < _dip_min:
                    _keep_split2_pending("SPLIT2ND_DIP_TOO_SHALLOW")
                    print(f"[SKIP_SPLIT2ND_DIP] code={code} dip={_dip:.4f} < min={_dip_min:.4f} (not enough dip)")
                    continue
                if _dip > _dip_max:
                    _keep_split2_pending("SPLIT2ND_DIP_TOO_DEEP")
                    print(f"[SKIP_SPLIT2ND_DIP] code={code} dip={_dip:.4f} > max={_dip_max:.4f} (too deep)")
                    continue
            _confirm_reason = _split_second_confirmation_reason(r, cfg)
            if _confirm_reason:
                _keep_split2_pending(_confirm_reason)
                print(f"[SKIP_SPLIT2ND_CONFIRM] code={code} reason={_confirm_reason}")
                _record_decision(
                    code=code, signal_date=effective_signal_date, signal="HOLD", reason=_confirm_reason,
                    row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
                )
                continue

        # limit price tolerance: reject if entry_price deviates too far from signal_date close
        _limit_price_guard = _evaluate_limit_price_tolerance_guard(
            cfg,
            px,
            code=code,
            effective_signal_date=effective_signal_date,
            entry_price=entry_price,
            is_open_order_replay=is_open_order_replay,
            fb_enabled=fb_enabled,
            fallback_stage=fallback_stage,
        )
        if bool(_limit_price_guard.get("blocked", False)):
            continue
        # gap filters: next-open compares to signal-date close; same-close compares to prior session close.
        prev_ohlc = get_ohlc(px, code, effective_signal_date)
        gap_ref_ohlc = prev_ohlc
        gap_ref_date: Optional[str] = effective_signal_date
        if use_same_close_today:
            gap_ref_date = prev_trading_date(px, code, effective_signal_date)
            gap_ref_ohlc = get_ohlc(px, code, gap_ref_date) if gap_ref_date else None
        entry_gap_up_reduce_gap = None
        _surge_active_gap_override_allowed = False
        if gap_ref_ohlc and float(gap_ref_ohlc["close"]) > 0:
            prev_close = float(gap_ref_ohlc["close"])
            gap_entry_price = float(entry_price)
            if use_intraday_realtime_entry:
                row_current_price = _to_float(r.get("current_price"), None)
                if row_current_price is not None and float(row_current_price) > 0:
                    gap_entry_price = float(row_current_price)
                elif is_surge_immediate:
                    row_prev_close = _to_float(r.get("prev_close"), None)
                    row_change_pct = _to_float(r.get("ret1_pct"), None)
                    if row_change_pct is not None and abs(float(row_change_pct)) > 1.0:
                        row_change_pct = float(row_change_pct) / 100.0
                    if row_prev_close is not None and float(row_prev_close) > 0 and row_change_pct is not None:
                        gap_entry_price = float(row_prev_close) * (1.0 + float(row_change_pct))
            gap = (gap_entry_price - prev_close) / prev_close
            entry_gap_up_reduce_gap = float(gap)

            gap_up_max_pct = float(gap_up_max_pct_runtime or 0.0)
            if bool(_truthy(r.get("promoted_recheck", False))):
                promoted_gap_up_max_pct = float(_to_float(cfg.get("promoted_recheck_gap_up_max_pct"), 0.20))
                if promoted_gap_up_max_pct > 0:
                    gap_up_max_pct = max(gap_up_max_pct, promoted_gap_up_max_pct)
            if gap_up_max_pct > 0 and gap > gap_up_max_pct:
                _entry_gap_reduce_cfg = cfg.get("entry_gap_up_reduce", {}) if isinstance(cfg, dict) else {}
                _surge_probe_gap_override_cfg = {}
                _surge_active_gap_override_cfg = {}
                _surge_wait_reclaim_probe_cfg = {}
                if isinstance(cfg, dict):
                    _surge_policy_cfg = cfg.get("surge_entry_policy", {})
                    if isinstance(_surge_policy_cfg, dict):
                        _surge_active_gap_override_cfg = _surge_policy_cfg.get("active_entry_gap_up_override", {})
                        if not isinstance(_surge_active_gap_override_cfg, dict):
                            _surge_active_gap_override_cfg = {}
                        _paper_probe_cfg = _surge_policy_cfg.get("paper_probe", {})
                        if isinstance(_paper_probe_cfg, dict):
                            _surge_probe_gap_override_cfg = _paper_probe_cfg.get("gap_up_override", {})
                            if not isinstance(_surge_probe_gap_override_cfg, dict):
                                _surge_probe_gap_override_cfg = {}
                        _surge_wait_reclaim_probe_cfg = _surge_policy_cfg.get("wait_reclaim_paper_probe", {})
                        if not isinstance(_surge_wait_reclaim_probe_cfg, dict):
                            _surge_wait_reclaim_probe_cfg = {}
                _surge_probe_gap_override_allowed = False
                _surge_active_gap_override_allowed = False
                if (
                    is_surge_immediate
                    and use_intraday_realtime_entry
                    and (
                        bool(_surge_probe_gap_override_cfg.get("enabled", False))
                        or bool(_surge_active_gap_override_cfg.get("enabled", False))
                        or bool(_surge_wait_reclaim_probe_cfg.get("enabled", False))
                    )
                ):
                    _active_label = str(r.get("active_response_label", "") or "").strip().upper()
                    _paper_readiness = _clean_active_response_cell(r.get("paper_order_readiness")).upper()
                    if not _paper_readiness:
                        _paper_readiness = _clean_active_response_cell(r.get("surge_paper_order_readiness")).upper()
                    _paper_route_raw = r.get("paper_order_route", "")
                    _surge_paper_route_raw = r.get("surge_paper_order_route", "")
                    if _truthy(_surge_paper_route_raw) and not _truthy(_paper_route_raw):
                        _paper_route_raw = _surge_paper_route_raw
                    elif not _clean_active_response_cell(_paper_route_raw):
                        _paper_route_raw = _surge_paper_route_raw
                    _paper_route_ok = _truthy(_paper_route_raw)
                    _paper_probe_flag = _truthy(r.get("paper_probe_allowed", ""))
                    _lob_status = str(r.get("lob_status", "") or "").strip().upper()
                    if not _lob_status or _lob_status in {"NAN", "NONE", "NULL"}:
                        _lob_status = str(r.get("surge_lob_status", "") or "").strip().upper()
                    _lob_available = _truthy(r.get("lob_available", "")) or _truthy(r.get("surge_lob_available", ""))
                    _orderflow_tag = str(r.get("surge_orderflow_tag", r.get("orderflow_tag", "")) or "").strip().upper()
                    _spread_raw = _to_float(r.get("surge_spread_bps", r.get("spread_bps")), None)
                    _spread_bps = 1e9 if _spread_raw is None else float(_spread_raw)
                    _markout_raw = _to_float(r.get("markout_1step_bps"), None)
                    _markout_bps = -1e9 if _markout_raw is None else float(_markout_raw)
                    _score_final = float(_to_float(r.get("surge_score_final", r.get("final_score")), 0.0) or 0.0)
                    _override_max_gap = _pct01_from_config(_surge_probe_gap_override_cfg.get("max_gap_pct"), 0.0)
                    _override_max_spread = float(_to_float(_surge_probe_gap_override_cfg.get("max_spread_bps"), 0.0) or 0.0)
                    _override_min_markout = float(_to_float(_surge_probe_gap_override_cfg.get("min_markout_1step_bps"), 0.0) or 0.0)
                    _override_min_score = float(_to_float(_surge_probe_gap_override_cfg.get("min_score_final"), 0.0) or 0.0)
                    _reclaim_probe_ready = bool(
                        _active_label == "PROBE_READY"
                        and str(r.get("reclaim_probe_transition", "") or "").strip().upper() == "WAIT_RECLAIM_TO_PROBE_READY"
                    )
                    if _reclaim_probe_ready:
                        _reclaim_override_max_gap = _pct01_from_config(_surge_wait_reclaim_probe_cfg.get("max_gap_pct"), 0.0)
                        if _reclaim_override_max_gap > 0:
                            _override_max_gap = max(float(_override_max_gap), float(_reclaim_override_max_gap))
                    _require_lob_ok = bool(_surge_probe_gap_override_cfg.get("require_lob_ok", True))
                    _require_orderflow_ok = bool(_surge_probe_gap_override_cfg.get("require_orderflow_tag_ok", True))
                    _require_paper_ready = bool(_surge_probe_gap_override_cfg.get("require_paper_ready", True))
                    _require_paper_route = bool(_surge_probe_gap_override_cfg.get("require_paper_order_route", True))
                    _paper_ready_ok = (
                        (not _require_paper_ready)
                        or _paper_readiness == "PAPER_READY"
                        or (_active_label == "PROBE_READY" and _paper_probe_flag)
                    )
                    _paper_route_ok2 = (not _require_paper_route) or _paper_route_ok or (_active_label == "PROBE_READY" and _paper_probe_flag)
                    _lob_ok = (not _require_lob_ok) or _lob_available or _lob_status == "OK"
                    _reclaim_allow_no_history = bool(
                        _reclaim_probe_ready
                        and bool(_surge_wait_reclaim_probe_cfg.get("allow_orderflow_no_history", True))
                    )
                    _orderflow_ok = (
                        (not _require_orderflow_ok)
                        or _orderflow_tag == "OK"
                        or (_reclaim_allow_no_history and _orderflow_tag == "NO_HISTORY")
                    )
                    _surge_probe_gap_override_allowed = bool(
                        _active_label == "PROBE_READY"
                        and _override_max_gap > 0
                        and gap <= _override_max_gap
                        and _paper_ready_ok
                        and _paper_route_ok2
                        and _lob_ok
                        and _orderflow_ok
                        and (_override_max_spread <= 0 or _spread_bps <= _override_max_spread)
                        and _markout_bps >= _override_min_markout
                        and _score_final >= _override_min_score
                    )
                    _active_override_max_gap = _pct01_from_config(_surge_active_gap_override_cfg.get("max_gap_pct"), 0.0)
                    _active_override_max_spread = float(_to_float(_surge_active_gap_override_cfg.get("max_spread_bps"), 0.0) or 0.0)
                    _active_override_min_markout = float(_to_float(_surge_active_gap_override_cfg.get("min_markout_1step_bps"), 0.0) or 0.0)
                    _active_override_min_score = float(_to_float(_surge_active_gap_override_cfg.get("min_score_final"), 0.0) or 0.0)
                    _active_require_lob_ok = bool(_surge_active_gap_override_cfg.get("require_lob_ok", True))
                    _active_require_orderflow_ok = bool(_surge_active_gap_override_cfg.get("require_orderflow_tag_ok", True))
                    _active_require_paper_ready = bool(_surge_active_gap_override_cfg.get("require_paper_ready", True))
                    _active_require_paper_route = bool(_surge_active_gap_override_cfg.get("require_paper_order_route", True))
                    _active_paper_ready_ok = (not _active_require_paper_ready) or _paper_readiness == "PAPER_READY"
                    _active_paper_route_ok = (not _active_require_paper_route) or _paper_route_ok
                    _active_lob_ok = (not _active_require_lob_ok) or _lob_available or _lob_status == "OK"
                    _active_orderflow_ok = (not _active_require_orderflow_ok) or _orderflow_tag == "OK"
                    _surge_active_gap_override_allowed = bool(
                        _active_label == "ACTIVE_ENTRY_READY"
                        and bool(_surge_active_gap_override_cfg.get("enabled", False))
                        and _active_override_max_gap > 0
                        and gap <= _active_override_max_gap
                        and _active_paper_ready_ok
                        and _active_paper_route_ok
                        and _active_lob_ok
                        and _active_orderflow_ok
                        and (_active_override_max_spread <= 0 or _spread_bps <= _active_override_max_spread)
                        and _markout_bps >= _active_override_min_markout
                        and _score_final >= _active_override_min_score
                    )
                    if _surge_probe_gap_override_allowed:
                        r["_surge_probe_gap_up_override"] = 1
                        r["_surge_probe_gap_up_original_limit"] = round(float(gap_up_max_pct), 6)
                        r["_surge_probe_gap_up_override_limit"] = round(float(_override_max_gap), 6)
                        r["_surge_gap_up_override_gap"] = round(float(gap), 6)
                        r["_surge_gap_up_override_blockers"] = ""
                        print(
                            f"[SURGE_PROBE_GAPUP_OVERRIDE] code={code} gap={gap:.3f} "
                            f"limit={gap_up_max_pct:.3f}->{_override_max_gap:.3f} "
                            f"spread={_spread_bps:.2f} markout={_markout_bps:.2f} "
                            f"orderflow={_orderflow_tag or '-'}"
                        )
                    elif _surge_active_gap_override_allowed:
                        r["_surge_active_gap_up_override"] = 1
                        r["_surge_active_gap_up_original_limit"] = round(float(gap_up_max_pct), 6)
                        r["_surge_active_gap_up_override_limit"] = round(float(_active_override_max_gap), 6)
                        r["_surge_gap_up_override_gap"] = round(float(gap), 6)
                        r["_surge_gap_up_override_blockers"] = ""
                        print(
                            f"[SURGE_ACTIVE_GAPUP_OVERRIDE] code={code} gap={gap:.3f} "
                            f"limit={gap_up_max_pct:.3f}->{_active_override_max_gap:.3f} "
                            f"spread={_spread_bps:.2f} markout={_markout_bps:.2f} "
                            f"orderflow={_orderflow_tag or '-'}"
                        )
                    else:
                        _override_blockers = []
                        if _active_label == "ACTIVE_ENTRY_READY":
                            if not bool(_surge_active_gap_override_cfg.get("enabled", False)):
                                _override_blockers.append("active_override_disabled")
                            if not (_active_override_max_gap > 0):
                                _override_blockers.append(f"active_max_gap={_active_override_max_gap:.3f}")
                            if gap > _active_override_max_gap:
                                _override_blockers.append(f"gap={gap:.3f}>{_active_override_max_gap:.3f}")
                            if not _active_paper_ready_ok:
                                _override_blockers.append(f"paper_ready={_paper_readiness or '-'}")
                            if not _active_paper_route_ok:
                                _override_blockers.append(f"paper_route={_paper_route_ok}")
                            if not _active_lob_ok:
                                _override_blockers.append(f"lob={_lob_status or '-'}")
                            if not _active_orderflow_ok:
                                _override_blockers.append(f"orderflow={_orderflow_tag or '-'}")
                            if _active_override_max_spread > 0 and _spread_bps > _active_override_max_spread:
                                _override_blockers.append(f"spread={_spread_bps:.2f}>{_active_override_max_spread:.2f}")
                            if _markout_bps < _active_override_min_markout:
                                _override_blockers.append(f"markout={_markout_bps:.2f}<{_active_override_min_markout:.2f}")
                            if _score_final < _active_override_min_score:
                                _override_blockers.append(f"score={_score_final:.2f}<{_active_override_min_score:.2f}")
                        else:
                            if _active_label != "PROBE_READY":
                                _override_blockers.append(f"active_label={_active_label or '-'}")
                            if not (_override_max_gap > 0):
                                _override_blockers.append(f"max_gap={_override_max_gap:.3f}")
                            if gap > _override_max_gap:
                                _override_blockers.append(f"gap={gap:.3f}>{_override_max_gap:.3f}")
                            if not _paper_ready_ok:
                                _override_blockers.append(f"paper_ready={_paper_readiness or '-'}")
                            if not _paper_route_ok2:
                                _override_blockers.append(f"paper_route={_paper_route_ok}")
                            if not _lob_ok:
                                _override_blockers.append(f"lob={_lob_status or '-'}")
                            if not _orderflow_ok:
                                _override_blockers.append(f"orderflow={_orderflow_tag or '-'}")
                            if _override_max_spread > 0 and _spread_bps > _override_max_spread:
                                _override_blockers.append(f"spread={_spread_bps:.2f}>{_override_max_spread:.2f}")
                            if _markout_bps < _override_min_markout:
                                _override_blockers.append(f"markout={_markout_bps:.2f}<{_override_min_markout:.2f}")
                            if _score_final < _override_min_score:
                                _override_blockers.append(f"score={_score_final:.2f}<{_override_min_score:.2f}")
                        r["_surge_gap_up_override_gap"] = round(float(gap), 6)
                        r["_surge_gap_up_override_blockers"] = ";".join(_override_blockers) or "UNKNOWN"
                        r["_surge_gap_up_override_active_label"] = _active_label
                        r["_surge_gap_up_override_paper_readiness"] = _paper_readiness
                        r["_surge_gap_up_override_paper_route_ok"] = bool(_paper_route_ok)
                        r["_surge_gap_up_override_lob_status"] = _lob_status
                        r["_surge_gap_up_override_orderflow_tag"] = _orderflow_tag
                        r["_surge_gap_up_override_spread_bps"] = round(float(_spread_bps), 6)
                        r["_surge_gap_up_override_markout_bps"] = round(float(_markout_bps), 6)
                        r["_surge_gap_up_override_score_final"] = round(float(_score_final), 6)
                        print(
                            f"[SURGE_PROBE_GAPUP_OVERRIDE_BLOCKED] code={code} "
                            f"blockers={';'.join(_override_blockers) or 'UNKNOWN'}"
                        )
                _allow_surge_no_lob_gap_reduce = bool(
                    is_surge_immediate
                    and use_intraday_realtime_entry
                    and bool(_entry_gap_reduce_cfg.get("apply_to_surge_no_lob_probe", False))
                    and bool(r.get("surge_no_lob_probe_allowed") or r.get("no_lob_probe_allowed"))
                )
                if _surge_probe_gap_override_allowed or _surge_active_gap_override_allowed:
                    pass
                elif _allow_surge_no_lob_gap_reduce:
                    r["_surge_no_lob_gap_up_reduce"] = 1
                    r["_entry_gap_up_reduce_gap"] = round(float(gap), 6)
                    print(f"[SURGE_NO_LOB_GAP_REDUCE] code={code} gap={gap:.3f} > {gap_up_max_pct} "
                          f"entry={gap_entry_price} ref_close={prev_close} ref_date={gap_ref_date}")
                else:
                    reason = "SURGE_GAPUP_BLOCK" if is_surge_immediate and use_intraday_realtime_entry else "GAPUP_BLOCK"
                    r["_surge_gap_up_block_gap"] = round(float(gap), 6)
                    r["_surge_gap_up_block_limit"] = round(float(gap_up_max_pct), 6)
                    r["_surge_gap_up_block_entry_price"] = round(float(gap_entry_price), 6)
                    r["_surge_gap_up_block_ref_close"] = round(float(prev_close), 6)
                    r["_surge_gap_up_block_ref_date"] = str(gap_ref_date or "")
                    _adaptive_good_stock_row = _adaptive_good_stock_entry(adaptive_good_stock_route_map, code)
                    _adaptive_good_stock_route = str(_adaptive_good_stock_row.get("policy_route") or "").strip().upper()
                    _adaptive_good_stock_gap_max = _adaptive_good_stock_gapup_override_max(cfg, _adaptive_good_stock_route)
                    _adaptive_good_stock_gap_ok = bool(
                        _adaptive_good_stock_allowed(
                            cfg,
                            "open_chase_probe_routes",
                            _adaptive_good_stock_route,
                            {"QUALITY_PROBE_TIER1", "QUALITY_PROBE_TIER2"},
                        )
                        and _adaptive_good_stock_gap_max > 0.0
                        and float(gap) <= float(_adaptive_good_stock_gap_max)
                    )
                    if _adaptive_good_stock_gap_ok:
                        _mark_adaptive_good_stock(r, _adaptive_good_stock_row, reason, "GAPUP_SOFTENED_TO_PROBE_REDUCE")
                        r["adaptive_good_stock_gapup_override_max_pct"] = round(float(_adaptive_good_stock_gap_max), 6)
                        r["adaptive_good_stock_gapup_gap"] = round(float(gap), 6)
                        print(
                            f"[ADAPTIVE_GOOD_STOCK_GAPUP] code={code} route={_adaptive_good_stock_route} "
                            f"gap={gap:.3f} max={_adaptive_good_stock_gap_max:.3f} action=PROBE_REDUCE"
                        )
                    else:
                        if _adaptive_good_stock_route == _adaptive_good_stock_wait_gapup_route(cfg):
                            _mark_adaptive_good_stock(r, _adaptive_good_stock_row, reason, "GAPUP_WAIT_PULLBACK_RECLAIM")
                            reason = _adaptive_good_stock_wait_gapup_route(cfg)
                            print(f"[ADAPTIVE_GOOD_STOCK_WAIT] code={code} route={_adaptive_good_stock_route} original=GAPUP_BLOCK action=WAIT_PULLBACK_RECLAIM")
                        print(f"[SKIP_GAPUP] code={code} gap={gap:.3f} > {gap_up_max_pct} "
                              f"entry={gap_entry_price} ref_close={prev_close} ref_date={gap_ref_date}")
                        _record_decision(
                            code=code, signal_date=effective_signal_date, signal="HOLD", reason=reason,
                            row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
                        )
                        continue

            _entry_gap_limit_guard = _evaluate_entry_gap_limit_guard(
                ops_policy,
                code=code,
                gap=gap,
                gap_entry_price=gap_entry_price,
                prev_close=prev_close,
                gap_ref_date=str(gap_ref_date or ""),
                entry_gap_down_stop_pct_runtime=entry_gap_down_stop_pct_runtime,
                use_same_close_today=use_same_close_today,
                fallback_stage=fallback_stage,
                is_open_order_replay=is_open_order_replay,
            )
            if bool(_entry_gap_limit_guard.get("blocked", False)):
                continue

        _entry_gap_risk_guard = _evaluate_entry_gap_risk_guard(
            r,
            cfg,
            px,
            code=code,
            entry_day=entry_day,
            effective_signal_date=effective_signal_date,
            use_same_close_today=use_same_close_today,
            is_open_order_replay=is_open_order_replay,
            is_surge_immediate=is_surge_immediate,
            use_intraday_realtime_entry=use_intraday_realtime_entry,
            entry_gap_up_reduce_gap=entry_gap_up_reduce_gap,
            surge_active_gap_override_allowed=_surge_active_gap_override_allowed,
        )
        if bool(_entry_gap_risk_guard.get("blocked", False)):
            _record_decision(
                code=code,
                signal_date=effective_signal_date,
                signal="HOLD",
                reason=str(_entry_gap_risk_guard.get("reason") or "GAP_RISK_HISTORY_BLOCK"),
                row=r,
                is_surge=is_surge_immediate,
                is_replay=is_open_order_replay,
                is_carryover=is_carryover_row,
            )
            continue

        _entry_liquidity_guard = _evaluate_entry_liquidity_guard(
            r,
            cfg,
            code=code,
            is_open_order_replay=is_open_order_replay,
            is_surge_immediate=is_surge_immediate,
        )
        if bool(_entry_liquidity_guard.get("blocked", False)):
            continue

        _entry_slippage = _resolve_entry_row_slippage(
            r,
            cfg,
            code=code,
            base_slip_pct=slip_pct,
            is_surge_immediate=is_surge_immediate,
            is_open_order_replay=is_open_order_replay,
        )
        row_market_cap = float(_entry_slippage.get("row_market_cap", 0.0) or 0.0)
        row_slip_pct = float(_entry_slippage.get("row_slip_pct", slip_pct) or slip_pct)
        sector_risk_note_parts: List[str] = list(_entry_slippage.get("sector_risk_note_parts", []) or [])
        row_sector_for_corr = ""
        _initial_entry_qty = _resolve_initial_entry_qty(
            r,
            cfg,
            code=code,
            entry_price=entry_price,
            fee_pct=fee_pct,
            row_slip_pct=row_slip_pct,
            is_split_2nd=is_split_2nd,
            is_open_order_replay=is_open_order_replay,
            is_surge_immediate=is_surge_immediate,
            cap_basic_qty=_cap_basic_qty,
        )
        qty_override = _initial_entry_qty.get("qty_override")
        qty = int(_initial_entry_qty.get("qty", 0) or 0)
        if not (is_split_2nd or (is_open_order_replay and qty_override is not None and int(qty_override) > 0)):
            _entry_gap_up_reduce_qty = _apply_entry_gap_up_reduce_qty(
                r,
                cfg,
                code=code,
                qty=qty,
                entry_gap_up_reduce_gap=entry_gap_up_reduce_gap,
                is_surge_immediate=is_surge_immediate,
                is_open_order_replay=is_open_order_replay,
            )
            qty = int(_entry_gap_up_reduce_qty.get("qty", qty) or 0)
            _normal_entry_qty_reduce = _apply_normal_entry_qty_reductions(
                r,
                cfg,
                adaptive_good_stock_route_map,
                sector_risk_note_parts,
                code=code,
                entry_day=entry_day,
                entry_price=entry_price,
                qty=qty,
                use_same_close_today=use_same_close_today,
                is_surge_immediate=is_surge_immediate,
                is_open_order_replay=is_open_order_replay,
                is_split_2nd=is_split_2nd,
            )
            qty = int(_normal_entry_qty_reduce.get("qty", qty) or 0)
            if bool(_normal_entry_qty_reduce.get("blocked", False)):
                _record_decision(
                    code=code,
                    signal_date=effective_signal_date,
                    signal="HOLD",
                    reason=str(_normal_entry_qty_reduce.get("reason") or "NORMAL_ENTRY_QTY_BLOCK"),
                    row=r,
                    is_surge=is_surge_immediate,
                    is_replay=is_open_order_replay,
                    is_carryover=is_carryover_row,
                )
                continue
            _entry_weight_qty = _apply_entry_weight_qty_adjustments(
                r,
                cfg,
                code=code,
                qty=qty,
                position_size_multiplier=position_size_multiplier,
                entry_decision_code=entry_decision_code,
            )
            qty = int(_entry_weight_qty.get("qty", qty) or 0)
            _surge_lob_exec_quality = _apply_surge_lob_exec_quality(
                r,
                cfg,
                code=code,
                qty=qty,
                entry_price=entry_price,
                row_slip_pct=row_slip_pct,
                is_surge_immediate=is_surge_immediate,
                is_open_order_replay=is_open_order_replay,
                exec_q_max=_exec_q_max,
                fc_propagate=_fc_propagate,
                fail_closed_triggered=fail_closed_triggered,
            )
            row_slip_pct = float(_surge_lob_exec_quality.get("row_slip_pct", row_slip_pct) or row_slip_pct)
            exec_quality_blocked_count += int(_surge_lob_exec_quality.get("exec_quality_blocked_delta", 0) or 0)
            if bool(_surge_lob_exec_quality.get("fail_closed_triggered", fail_closed_triggered)):
                fail_closed_triggered = True
            if str(_surge_lob_exec_quality.get("fail_closed_reason", "") or ""):
                fail_closed_reason = str(_surge_lob_exec_quality.get("fail_closed_reason", ""))
            if bool(_surge_lob_exec_quality.get("blocked", False)):
                _record_decision(
                    code=code,
                    signal_date=effective_signal_date,
                    signal="HOLD",
                    reason=str(_surge_lob_exec_quality.get("reason") or "SURGE_LOB_SLIPPAGE_BLOCK"),
                    row=r,
                    is_surge=is_surge_immediate,
                    is_replay=is_open_order_replay,
                    is_carryover=is_carryover_row,
                )
                continue
            # Risk-cap sizing: cap position so worst-case gap loss <= risk_cap_per_trade_pct of capital.
            # gap_worst_pct = atr14_pct * gap_risk_multiplier (proxy for overnight gap risk).
            # max_notional = risk_budget / gap_worst_pct  →  risk_qty = max_notional / entry_price.
            _entry_risk_cap_atr = _apply_entry_risk_cap_atr_sizing(
                r,
                cfg,
                code=code,
                qty=qty,
                entry_price=entry_price,
                capital_total=capital_total,
                t_enabled=t_enabled,
                t_seasonal_mult=t_seasonal_mult,
            )
            qty = int(_entry_risk_cap_atr.get("qty", qty) or 0)
            _entry_sector_corr_hrp = _apply_entry_sector_corr_hrp(
                r,
                sector_corr_ctx,
                sector_risk_note_parts,
                code=code,
                qty=qty,
                sector_db=sector_db,
                resolve_sector=True,
                allow_block=True,
                allow_surge=False,
                is_surge_immediate=is_surge_immediate,
            )
            row_sector_for_corr = str(_entry_sector_corr_hrp.get("row_sector_for_corr", "") or "")
            if bool(_entry_sector_corr_hrp.get("blocked", False)):
                sector_corr_block_count += 1
                _record_decision(
                    code=code, signal_date=signal_date, signal="HOLD", reason="SECTOR_CORR_BLOCK",
                    row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
                )
                continue
            sector_corr_reduce_count += int(_entry_sector_corr_hrp.get("reduce_count", 0))
            qty = int(_entry_sector_corr_hrp.get("qty", qty))
        if qty <= 0:
            if is_surge_immediate:
                print(f"[SKIP_SURGE_QTY] code={code} entry_price={entry_price:.4f} mode={str(cfg.get('sizing_mode',''))}")
                _record_decision(
                    code=code, signal_date=signal_date, signal="HOLD", reason="SURGE_QTY_ZERO",
                    row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row,
                    entry_price=entry_price, qty=qty
                )
            else:
                print(f"[SKIP_QTY_ZERO] code={code} entry_price={entry_price:.4f} mode={str(cfg.get('sizing_mode',''))}")
                _record_decision(
                    code=code, signal_date=signal_date, signal="HOLD", reason="QTY_ZERO",
                    row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row,
                    entry_price=entry_price, qty=qty
                )
            continue

        if is_surge_immediate or has_surge_type_policy:
            surge_budget_fit = _fit_surge_budget_allocation(
                r, cfg,
                qty=qty,
                entry_price=entry_price,
                capital_total=capital_total,
                surge_notional_krw=surge_notional_krw,
                is_surge_immediate=is_surge_immediate,
            )
            if bool(surge_budget_fit.get("blocked", False)):
                surge_budget_reason = str(surge_budget_fit.get("reason", "") or "")
                if surge_budget_reason == "SURGE_BUDGET_EXHAUSTED":
                    print(
                        f"[SKIP_SURGE_BUDGET] code={code} surge_budget_left={float(surge_budget_fit.get('surge_budget_left', 0.0)):.0f} "
                        f"surge_notional={surge_notional_krw:.0f} total_budget={float(surge_budget_fit.get('surge_total_budget', 0.0)):.0f}"
                    )
                    _record_decision(
                        code=code, signal_date=signal_date, signal="HOLD", reason="SURGE_BUDGET_EXHAUSTED",
                        row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row,
                        entry_price=entry_price, qty=qty
                    )
                elif surge_budget_reason == "SURGE_INVALID_TARGET_NOTIONAL":
                    print(f"[SKIP_SURGE_BUDGET] code={code} invalid_target_notional={float(surge_budget_fit.get('target_notional', 0.0)):.0f}")
                    _record_decision(
                        code=code, signal_date=signal_date, signal="HOLD", reason="SURGE_INVALID_TARGET_NOTIONAL",
                        row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row,
                        entry_price=entry_price, qty=qty
                    )
                elif surge_budget_reason == "SURGE_INVALID_PRICE_OR_BUDGET":
                    print(
                        f"[SKIP_SURGE_BUDGET] code={code} invalid_price_or_budget "
                        f"target_notional={surge_budget_fit.get('target_notional')} entry_price={entry_price}"
                    )
                    _record_decision(
                        code=code, signal_date=signal_date, signal="HOLD", reason="SURGE_INVALID_PRICE_OR_BUDGET",
                        row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row,
                        entry_price=entry_price, qty=0
                    )
                else:
                    qty_from_budget = int(surge_budget_fit.get("qty_from_budget", 0))
                    print(
                        f"[SKIP_SURGE_BUDGET] code={code} qty_from_budget={qty_from_budget} "
                        f"target_notional={float(surge_budget_fit.get('target_notional', 0.0)):.0f} entry_price={entry_price:.2f}"
                    )
                    _record_decision(
                        code=code, signal_date=signal_date, signal="HOLD", reason="SURGE_BUDGET_MIN_QTY",
                        row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row,
                        entry_price=entry_price, qty=qty_from_budget
                    )
                continue
            _qty_before_surge_budget = int(surge_budget_fit.get("old_qty", qty))
            qty_from_budget = int(surge_budget_fit.get("qty_from_budget", qty))
            target_notional = float(surge_budget_fit.get("target_notional", 0.0))
            qty = int(surge_budget_fit.get("qty", qty))
            r["_surge_qty_before_budget_cap"] = int(_qty_before_surge_budget)
            r["_surge_qty_from_budget"] = int(qty_from_budget)
            r["_surge_qty_after_budget_cap"] = int(qty)
            if bool(surge_budget_fit.get("adjusted", False)):
                print(
                    f"[SURGE_BUDGET_CAP] code={code} qty {_qty_before_surge_budget}->{qty} "
                    f"target_notional={target_notional:.0f}"
                )
            elif bool(surge_budget_fit.get("risk_qty_cap", False)):
                print(
                    f"[SURGE_RISK_QTY_CAP] code={code} qty={qty} "
                    f"budget_qty={qty_from_budget}"
                )
            _entry_sector_corr_hrp = _apply_entry_sector_corr_hrp(
                r,
                sector_corr_ctx,
                sector_risk_note_parts,
                code=code,
                qty=qty,
                row_sector_for_corr=row_sector_for_corr,
                resolve_sector=False,
                allow_block=False,
                allow_surge=True,
                is_surge_immediate=is_surge_immediate,
            )
            sector_corr_reduce_count += int(_entry_sector_corr_hrp.get("reduce_count", 0))
            qty = int(_entry_sector_corr_hrp.get("qty", qty))
        _post_sector_qty_limits = _apply_entry_post_sector_qty_limits(
            r,
            cfg,
            sector_risk_note_parts,
            code=code,
            qty=qty,
            entry_price=entry_price,
            capital_total=capital_total,
            is_surge_immediate=is_surge_immediate,
            is_split_2nd=is_split_2nd,
            budget_policy_enabled=budget_policy_enabled,
            split_alloc_pct=split_alloc_pct,
            split_notional_krw=split_notional_krw,
        )
        qty = int(_post_sector_qty_limits.get("qty", qty) or 0)
        if bool(_post_sector_qty_limits.get("blocked", False)):
            continue

        _ai_min_qty_fit = _apply_entry_ai_cap_min_qty(
            cfg,
            code=code,
            qty=qty,
            entry_price=entry_price,
            fee_pct=fee_pct,
            row_slip_pct=row_slip_pct,
            capital_total=capital_total,
            t_enabled=t_enabled,
            t_ai_codes=t_ai_codes,
            t_ai_single_cap_pct=t_ai_single_cap_pct,
            is_open_order_replay=is_open_order_replay,
            is_surge_immediate=is_surge_immediate,
        )
        if bool(_ai_min_qty_fit.get("blocked", False)):
            _record_decision(
                code=code, signal_date=signal_date, signal="HOLD", reason=str(_ai_min_qty_fit.get("reason") or "AI_FOCUS_SINGLE_CAP_BLOCK"),
                row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
            )
            continue
        qty = int(_ai_min_qty_fit.get("qty", qty) or 0)
        entry_notional = float(_ai_min_qty_fit["entry_notional"])
        entry_cost_buffer = float(_ai_min_qty_fit["entry_cost_buffer"])
        entry_notional_for_cap = float(_ai_min_qty_fit["entry_notional_for_cap"])
        min_qty_runtime = int(_ai_min_qty_fit["min_qty_runtime"])

        _entry_cap_limits = _apply_entry_cap_limits(
            cfg,
            code=code,
            qty=qty,
            entry_price=entry_price,
            entry_cost_buffer=entry_cost_buffer,
            entry_notional=entry_notional,
            entry_notional_for_cap=entry_notional_for_cap,
            min_qty_runtime=min_qty_runtime,
            is_open_order_replay=is_open_order_replay,
            is_surge_immediate=is_surge_immediate,
            gross_cap_krw=gross_cap_krw,
            daily_new_cap_krw=daily_new_cap_krw,
            current_open_notional=current_open_notional,
            new_notional_krw=new_notional_krw,
        )
        qty = int(_entry_cap_limits.get("qty", qty) or 0)
        entry_notional = float(_entry_cap_limits.get("entry_notional", entry_notional))
        entry_notional_for_cap = float(_entry_cap_limits.get("entry_notional_for_cap", entry_notional_for_cap))
        cap_block_count += int(_entry_cap_limits.get("cap_block_delta", 0) or 0)
        if bool(_entry_cap_limits.get("blocked", False)):
            continue

        _normal_exec_quality = _apply_entry_normal_exec_quality(
            r,
            cfg,
            sector_risk_note_parts,
            code=code,
            qty=qty,
            entry_price=entry_price,
            min_qty_runtime=min_qty_runtime,
            is_open_order_replay=is_open_order_replay,
            is_surge_immediate=is_surge_immediate,
            is_split_2nd=is_split_2nd,
        )
        qty = int(_normal_exec_quality.get("qty", qty) or 0)
        if bool(_normal_exec_quality.get("blocked", False)):
            _record_decision(
                code=code,
                signal_date=effective_signal_date,
                signal="HOLD",
                reason=str(_normal_exec_quality.get("reason") or "NORMAL_EXEC_QUALITY_BLOCK"),
                row=r,
                is_surge=is_surge_immediate,
                is_replay=is_open_order_replay,
                is_carryover=is_carryover_row,
            )
            continue
        _normal_lob_gate = _apply_entry_normal_lob_fill_price_gate(
            r,
            sector_risk_note_parts,
            code=code,
            qty=qty,
            entry_price=entry_price,
            entry_cost_buffer=entry_cost_buffer,
            entry_notional=entry_notional,
            entry_notional_for_cap=entry_notional_for_cap,
            is_open_order_replay=is_open_order_replay,
            is_surge_immediate=is_surge_immediate,
            is_split_2nd=is_split_2nd,
            gross_cap_krw=gross_cap_krw,
            daily_new_cap_krw=daily_new_cap_krw,
            current_open_notional=current_open_notional,
            new_notional_krw=new_notional_krw,
        )
        entry_price = float(_normal_lob_gate.get("entry_price", entry_price))
        entry_notional = float(_normal_lob_gate.get("entry_notional", entry_notional))
        entry_notional_for_cap = float(_normal_lob_gate.get("entry_notional_for_cap", entry_notional_for_cap))
        cap_block_count += int(_normal_lob_gate.get("cap_block_delta", 0) or 0)
        if bool(_normal_lob_gate.get("blocked", False)):
            _normal_lob_decision_reason = str(_normal_lob_gate.get("decision_reason", "") or "")
            if _normal_lob_decision_reason:
                _record_decision(
                    code=code, signal_date=effective_signal_date, signal="HOLD", reason=_normal_lob_decision_reason,
                    row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
                )
            continue
        _t2_budget_gate = _apply_entry_t2_buy_budget_gate(
            portfolio_state,
            cfg,
            t2_cash_checks,
            code=code,
            entry_day=entry_day,
            entry_price=float(entry_price),
            qty=int(qty),
            entry_cost_buffer=float(entry_cost_buffer),
            min_qty_runtime=int(min_qty_runtime),
            entry_notional=entry_notional,
            entry_notional_for_cap=entry_notional_for_cap,
            is_open_order_replay=is_open_order_replay,
        )
        qty = int(_t2_budget_gate.get("qty", qty) or 0)
        entry_notional = float(_t2_budget_gate.get("entry_notional", entry_notional))
        entry_notional_for_cap = float(_t2_budget_gate.get("entry_notional_for_cap", entry_notional_for_cap))
        cap_block_count += int(_t2_budget_gate.get("cap_block_delta", 0) or 0)
        if bool(_t2_budget_gate.get("blocked", False)):
            _t2_decision_reason = str(_t2_budget_gate.get("decision_reason", "") or "")
            if _t2_decision_reason:
                _record_decision(
                    code=code, signal_date=effective_signal_date, signal="HOLD", reason=_t2_decision_reason,
                    row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
                )
            continue
        identity_context = _prepare_entry_identity_context(
            r,
            code=code,
            effective_signal_date=effective_signal_date,
            today_ymd=today_ymd,
            entry_day=entry_day,
            qty=qty,
            is_split_2nd=is_split_2nd,
            is_open_order_replay=is_open_order_replay,
            replay_row_ok=replay_row_ok,
            replay_order_id_include_entry_day=replay_order_id_include_entry_day,
            allow_same_signal_reentry=allow_same_signal_reentry,
            daily_idem_keys=_daily_idem_keys,
            existing_fill_order_ids=existing_fill_order_ids,
            is_surge_immediate=is_surge_immediate,
            use_intraday_realtime_entry=use_intraday_realtime_entry,
            use_same_close_today=use_same_close_today,
            force_next_open_entry=force_next_open_entry,
        )
        idempotent_skip_count += int(identity_context.get("idempotent_skip_delta", 0) or 0)
        if bool(identity_context.get("blocked", False)):
            _identity_decision_reason = str(identity_context.get("decision_reason", "") or "IDEM_DUPLICATE")
            _record_decision(
                code=code, signal_date=effective_signal_date, signal="HOLD", reason=_identity_decision_reason,
                row=r, is_surge=is_surge_immediate, is_replay=is_open_order_replay, is_carryover=is_carryover_row
            )
            continue
        order_id = str(identity_context.get("order_id", "") or "")
        lineage = identity_context.get("lineage", {}) if isinstance(identity_context.get("lineage", {}), dict) else {}
        entry_source_kind = str(identity_context.get("entry_source_kind", "") or "")
        candidate_snapshot_id = str(identity_context.get("candidate_snapshot_id", "") or "")

        note_context = _build_entry_note_context(
            r,
            cfg,
            sector_risk_note_parts,
            lineage,
            effective_signal_date=effective_signal_date,
            signal_date=signal_date,
            qty=qty,
            entry_day=entry_day,
            entry_source_kind=entry_source_kind,
            candidate_snapshot_id=candidate_snapshot_id,
            is_surge_immediate=is_surge_immediate,
            has_surge_type_policy=has_surge_type_policy,
            is_open_order_replay=is_open_order_replay,
            replay_row_ok=replay_row_ok,
            use_intraday_realtime_entry=use_intraday_realtime_entry,
            use_same_close_today=use_same_close_today,
            force_next_open_entry=force_next_open_entry,
            fallback_note=fallback_note,
            is_split_2nd=is_split_2nd,
        )
        note_text = str(note_context.get("note_text", "") or "")
        horizon_label = str(note_context.get("horizon_label", "") or "")

        existing_fill_gate = _handle_existing_fill_idempotent_buy(
            existing_fill_order_ids,
            open_pos,
            processed_signals,
            _record_decision,
            r,
            cfg,
            code=code,
            signal_date=signal_date,
            effective_signal_date=effective_signal_date,
            entry_day=entry_day,
            allow_same_signal_reentry=allow_same_signal_reentry,
            is_open_order_replay=is_open_order_replay,
            is_surge_immediate=is_surge_immediate,
            is_carryover_row=is_carryover_row,
            horizon_label=horizon_label,
            sig_key=sig_key,
            entry_price=entry_price,
            qty=qty,
            order_id=order_id,
        )
        idempotent_skip_count += int(existing_fill_gate.get("idempotent_skip_delta", 0) or 0)
        if bool(existing_fill_gate.get("blocked", False)):
            continue

        split2_target_gate = _resolve_split2_target_position_gate(
            open_pos,
            _record_decision,
            r,
            is_split_2nd=is_split_2nd,
            code=code,
            effective_signal_date=effective_signal_date,
            is_surge_immediate=is_surge_immediate,
            is_open_order_replay=is_open_order_replay,
            is_carryover_row=is_carryover_row,
        )
        if bool(split2_target_gate.get("blocked", False)):
            continue
        _split2_target_position = split2_target_gate.get("target_position")

        # Counted here, not earlier: 17 further exits (8 of them silent) sit between the
        # sizing/cap checks and this point, so incrementing before them counted candidates
        # that were subsequently blocked. Consumers read this as "attempted"
        # (state.py [FILL_SUMMARY] attempted=/_unfilled), so it must mean orders actually
        # entered, otherwise a blocked candidate reports as an unfilled attempt.
        entry_commit = _commit_entry_fill_and_position(
            row=r,
            cfg=cfg,
            px=px,
            schema=schema,
            fills_new=fills_new,
            existing_fill_order_ids=existing_fill_order_ids,
            portfolio_state=portfolio_state,
            open_pos=open_pos,
            open_codes=open_codes,
            processed_signals=processed_signals,
            pending_carry_rows=pending_carry_rows,
            surge_type_day_counts=surge_type_day_counts,
            same_code_day_buy_counts=_same_code_day_buy_counts,
            record_decision=_record_decision,
            cap_basic_qty_fn=_cap_basic_qty,
            split2_target_position=_split2_target_position,
            code=code,
            name=name,
            effective_signal_date=effective_signal_date,
            entry_day=entry_day,
            entry_ts_value=entry_ts_value,
            row_entry_timing=row_entry_timing,
            entry_price=entry_price,
            entry_notional_for_cap=entry_notional_for_cap,
            entry_notional=entry_notional,
            qty=qty,
            order_id=order_id,
            note_text=note_text,
            fee_pct=fee_pct,
            row_slip_pct=row_slip_pct,
            take_profit=take_profit,
            stop_loss=stop_loss,
            trail_pct=trail_pct,
            row_market_cap=row_market_cap,
            horizon_label=horizon_label,
            lineage=lineage,
            sector_db=sector_db,
            fundamentals_db=fundamentals_db,
            entry_source_kind=entry_source_kind,
            candidate_snapshot_id=candidate_snapshot_id,
            fallback_note=fallback_note,
            is_surge_immediate=is_surge_immediate,
            has_surge_type_policy=has_surge_type_policy,
            is_open_order_replay=is_open_order_replay,
            is_carryover_row=is_carryover_row,
            is_split_2nd=is_split_2nd,
            sig_key=sig_key,
            replay_key=replay_key,
            ops_policy=ops_policy,
            ops_enabled=ops_enabled,
            strict_same_day=_strict_same_day,
            surge_type_text=surge_type_text,
            current_surge_notional_krw=surge_notional_krw,
        )
        entry_ready_count += int(entry_commit.get("entry_ready_delta", 0) or 0)
        new_count += int(entry_commit.get("new_count_delta", 0) or 0)
        new_notional_krw += float(entry_commit.get("new_notional_delta", 0.0) or 0.0)
        surge_new_count += int(entry_commit.get("surge_new_count_delta", 0) or 0)
        surge_notional_krw += float(entry_commit.get("surge_notional_delta", 0.0) or 0.0)
        split_notional_krw += float(entry_commit.get("split_notional_delta", 0.0) or 0.0)
        open_order_replay_used_count += int(entry_commit.get("open_order_replay_used_delta", 0) or 0)
        partial_fill_expired_count += int(entry_commit.get("partial_fill_expired_delta", 0) or 0)
    return {
        'fills_new': fills_new,
        'trades_new': trades_new,
        'new_count': int(new_count),
        'new_notional_krw': float(new_notional_krw),
        'surge_new_count': int(surge_new_count),
        'surge_type_day_counts': dict(surge_type_day_counts),
        'surge_notional_krw': float(surge_notional_krw),
        'split_notional_krw': float(split_notional_krw),
        'evaluated_count': int(evaluated_count),
        'no_next_day_count': int(no_next_day_count),
        'entry_ready_count': int(entry_ready_count),
        'cap_block_count': int(cap_block_count),
        'processed_skip_count': int(processed_skip_count),
        'max_new_skip_count': int(max_new_skip_count),
        'idempotent_skip_count': int(idempotent_skip_count),
        'stale_replay_used_count': int(stale_replay_used_count),
        'open_order_replay_used_count': int(open_order_replay_used_count),
        'max_positions_blocked': bool(max_positions_blocked),
        'today_ymd': today_ymd,
        'pending_carry_rows': pending_carry_rows,
        'entry_decisions': entry_decisions,
        '_daily_retry_counts': _daily_retry_counts,
        '_daily_idem_keys': _daily_idem_keys,
        'fail_closed_triggered': bool(fail_closed_triggered),
        'fail_closed_reason': str(fail_closed_reason),
        'ttl_expired_count': int(ttl_expired_count),
        'retry_blocked_count': int(retry_blocked_count),
        'exec_quality_blocked_count': int(exec_quality_blocked_count),
        'quote_stale_blocked_count': int(quote_stale_blocked_count),
        'close_cutoff_blocked_count': int(close_cutoff_blocked_count),
        'partial_fill_expired_count': int(partial_fill_expired_count),
        'sector_corr_block_count': int(sector_corr_block_count),
        'sector_corr_reduce_count': int(sector_corr_reduce_count),
        'portfolio_state': portfolio_state,
        't2_cash_checks': t2_cash_checks,
    }


FX_HARD_BLOCK_THRESHOLD = -0.50

def derive_fx_entry_status(fx_ctx: Dict[str, Any], fx_policy: Optional[Dict[str, Any]] = None) -> str:
    if not isinstance(fx_ctx, dict) or not fx_ctx:
        return "NORMAL"
    daily_move = abs(_to_float(fx_ctx.get("daily_abs_change"), 0.0))
    daily_change = _to_float(fx_ctx.get("daily_change"), 0.0)
    rolling_20d_avg = abs(_to_float(fx_ctx.get("avg_abs_change_20"), 0.0))
    policy = fx_policy if isinstance(fx_policy, dict) else {}
    try:
        hard_block_level = float(policy.get("daily_abs_change_block_level", 15.0) or 15.0)
    except Exception:
        hard_block_level = 15.0
    try:
        soft_avg_threshold = float(policy.get("soft_avg_abs_change_threshold", 10.0) or 10.0)
    except Exception:
        soft_avg_threshold = 10.0
    try:
        soft_daily_threshold = float(policy.get("soft_daily_abs_change_threshold", 5.0) or 5.0)
    except Exception:
        soft_daily_threshold = 5.0
    try:
        caution_avg_threshold = float(policy.get("caution_avg_abs_change_threshold", soft_avg_threshold) or soft_avg_threshold)
    except Exception:
        caution_avg_threshold = soft_avg_threshold
    try:
        direction_flat_band = float(policy.get("direction_flat_band", 0.5) or 0.5)
    except Exception:
        direction_flat_band = 0.5
    level = _to_float(fx_ctx.get("level"), 0.0)
    level_band = str(fx_ctx.get("level_band") or "").upper()
    anchor_source = str(policy.get("stable_anchor_source", "ma20") or "ma20").strip().lower()
    if anchor_source == "ma60":
        anchor_level = _to_float(fx_ctx.get("ma60"), 0.0)
    elif anchor_source == "level":
        anchor_level = level
    else:
        anchor_level = _to_float(fx_ctx.get("ma20"), 0.0)
    if anchor_level <= 0:
        anchor_level = level
    try:
        anchor_deviation_band = float(policy.get("stable_anchor_deviation_band", 25.0) or 25.0)
    except Exception:
        anchor_deviation_band = 25.0
    anchor_deviation = (level - anchor_level) if anchor_level > 0 else 0.0
    is_upward = daily_change > direction_flat_band
    is_flat = abs(daily_change) <= direction_flat_band
    is_stable_anchor_zone = abs(anchor_deviation) <= anchor_deviation_band
    try:
        crisis_fx_level = float(policy.get("crisis_fx_level", 1520.0) or 1520.0)
    except Exception:
        crisis_fx_level = 1520.0
    require_crisis_level = bool(policy.get("hard_block_requires_crisis_level", False))
    is_crisis_level = bool(level >= crisis_fx_level or level_band == "CRISIS")

    # 방향성 반영: 상승 충격일 때만 강한 진입 제한, 하락/보합은 기준환율 안정영역을 우선 적용
    if is_upward:
        if daily_move > hard_block_level:
            if require_crisis_level and (not is_crisis_level):
                return "EXTREME_SOFT"
            return "EXTREME_HARD"
        if rolling_20d_avg > soft_avg_threshold and daily_move > soft_daily_threshold:
            return "EXTREME_SOFT"
        if rolling_20d_avg > caution_avg_threshold:
            return "CAUTION"
    else:
        if is_crisis_level and (not is_stable_anchor_zone):
            return "CAUTION"
        if (not is_flat) and (level_band == "CRISIS"):
            return "CAUTION"
    return "NORMAL"

def apply_fx_filter(candidates: pd.DataFrame, fx_status: str, max_new: int) -> tuple[pd.DataFrame, int]:
    cdf = candidates.copy()
    if "fx_score" not in cdf.columns:
        return cdf, int(max_new)

    cdf["_fx_score_n"] = pd.to_numeric(cdf["fx_score"], errors="coerce").fillna(0.0)
    status = str(fx_status or "").upper()

    if status == "EXTREME_HARD":
        before = len(cdf)
        cdf = cdf[cdf["_fx_score_n"] >= 0].copy()
        after = len(cdf)
        print(f"[FX] EXTREME_HARD nonnegative filter applied: {before}->{after}")
        return cdf, int(max_new)

    before = len(cdf)
    cdf = cdf[cdf["_fx_score_n"] > FX_HARD_BLOCK_THRESHOLD].copy()
    after = len(cdf)
    if before != after:
        print(
            f"[FX] soft regime hard-block filter applied: {before}->{after} "
            f"threshold={FX_HARD_BLOCK_THRESHOLD:.2f}"
        )

    soft_caution_count = int(
        (
            (cdf["_fx_score_n"] > FX_HARD_BLOCK_THRESHOLD)
            & (cdf["_fx_score_n"] <= FX_SCORE_CAUTION_THRESHOLD)
        ).sum()
    )
    if soft_caution_count > 0:
        old_max_new = int(max_new)
        max_new = max(1, int(max_new) - soft_caution_count)
        print(
            f"[FX] soft caution reduce max_new: {old_max_new}->{int(max_new)} "
            f"count={soft_caution_count} threshold={FX_SCORE_CAUTION_THRESHOLD:.2f}"
        )

    return cdf, int(max_new)

def load_candidates_chosen_level(log_dir: Path) -> Optional[str]:
    """Return chosen_level from candidates_latest_meta.json, if available."""
    p = log_dir / "candidates_latest_meta.json"
    if not p.exists():
        return None
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        try:
            obj = json.loads(p.read_text(encoding="utf-8-sig"))
        except Exception:
            return None
    if not isinstance(obj, dict):
        return None
    lv = obj.get("chosen_level")
    if lv is None:
        return None
    s = str(lv).strip().upper()
    return s if s else None

def load_candidates_execution_gate(log_dir: Path) -> Dict[str, Any]:
    """Return fail-closed execution eligibility from candidates_latest_meta.json."""
    p = log_dir / "candidates_latest_meta.json"
    if not p.exists():
        return {"ok": True, "reason": "meta_missing", "explicit": False}
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        try:
            obj = json.loads(p.read_text(encoding="utf-8-sig"))
        except Exception as exc:
            return {"ok": False, "reason": f"meta_read_failed:{type(exc).__name__}", "explicit": True}
    if not isinstance(obj, dict):
        return {"ok": False, "reason": "meta_not_object", "explicit": True}

    quality_gate_raw = obj.get("quality_gate")
    stable_gate_raw = obj.get("stable_param_gate")
    quality_gate: Dict[Any, Any] = quality_gate_raw if isinstance(quality_gate_raw, dict) else {}
    stable_gate: Dict[Any, Any] = stable_gate_raw if isinstance(stable_gate_raw, dict) else {}
    official = quality_gate.get("official_use_allowed")
    stable_ok = stable_gate.get("ok")
    reasons: List[str] = []
    if official is False:
        reasons.append("official_use_allowed_false")
    if stable_ok is False:
        reasons.append("stable_param_gate_false")
    if reasons:
        gate_reason = str(stable_gate.get("reason") or quality_gate.get("reason") or "")
        if gate_reason:
            reasons.append(gate_reason)
        return {"ok": False, "reason": ";".join(reasons), "explicit": True}
    return {"ok": True, "reason": "official_use_allowed_not_false", "explicit": True}

_RELAX_LEVEL_RE = re.compile(r"^L(\d+)$")
def parse_relax_level_num(chosen_level: Optional[str]) -> Optional[int]:
    if not chosen_level:
        return None
    m = _RELAX_LEVEL_RE.match(str(chosen_level).strip().upper())
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None

def pick_candidates(cfg: Dict[str, Any]) -> pd.DataFrame:
    global _LAST_CARRYOVER_REVALIDATE_SUMMARY
    _LAST_CARRYOVER_REVALIDATE_SUMMARY.clear()
    _set_last_t2_initialized_state({})
    cpath = Path(cfg["candidates_latest_data"])
    if not cpath.exists():
        raise SystemExit(f"[FATAL] missing candidates file: {cpath}")
    df = pd.read_csv(cpath)

    # Prefer enriched sidecars in priority order.
    # This keeps base candidates immutable while allowing fail-soft fallbacks.
    d_base = _max_date8_from_candidates(df)
    sidecar_suffixes = [
        ".with_final_score.csv",
        ".with_news_score.csv",
        ".with_sector_score.csv",
    ]
    sidecar_paths: List[Path] = []
    for suffix in sidecar_suffixes:
        sidecar_paths.append(cpath.with_name(cpath.stem + suffix))
        canonical = LOG_DIR / ("candidates_latest_data" + suffix)
        if canonical not in sidecar_paths:
            sidecar_paths.append(canonical)

    for sidecar in sidecar_paths:
        if not sidecar.exists():
            continue
        try:
            sdf = pd.read_csv(sidecar)
        except Exception as e:
            print(f"[CAND] sidecar read failed: {sidecar.name} {type(e).__name__}: {e}")
            continue
        if "code" not in sdf.columns:
            print(f"[CAND] sidecar ignored (no code column): {sidecar.name}")
            continue
        d_side = _max_date8_from_candidates(sdf)
        if d_base and d_side and d_base != d_side:
            print(f"[CAND] sidecar stale: {sidecar.name} base={d_base} sidecar={d_side}")
            continue
        df = sdf
        print(f"[CAND] using sidecar: {sidecar.name} (date={d_side or d_base or 'n/a'})")
        break

    promoted_path = LOG_DIR / "promoted_recheck_candidates_latest.csv"
    if promoted_path.exists():
        try:
            promoted_df = pd.read_csv(promoted_path)
            if "code" in promoted_df.columns and not promoted_df.empty:
                promoted_df["code"] = promoted_df["code"].astype(str).str.zfill(6)
                all_cols = list(dict.fromkeys(list(df.columns) + list(promoted_df.columns)))
                df = df.reindex(columns=all_cols)
                promoted_df = promoted_df.reindex(columns=all_cols)
                before_promoted_merge = int(len(df))
                df = _concat_drop_all_na_columns([df, promoted_df], ignore_index=True)
                print(f"[CAND] promoted_recheck merged: {before_promoted_merge}->{len(df)} from {promoted_path.name}")
        except Exception as e:
            print(f"[CAND] promoted_recheck ignored: {type(e).__name__}: {e}")

    if "code" not in df.columns:
        raise SystemExit(f"[FATAL] candidates has no 'code' column: cols={df.columns.tolist()}")

    execution_gate = load_candidates_execution_gate(LOG_DIR)
    if not bool(execution_gate.get("ok", True)):
        before_gate = int(len(df))
        df = df.iloc[0:0].copy()
        print(
            "[CAND_EXECUTION_GATE] candidates cleared "
            f"{before_gate}->0 reason={execution_gate.get('reason', '')}"
        )

    if "date_yyyymmdd" in df.columns:
        df["signal_date"] = df["date_yyyymmdd"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    elif "date" in df.columns:
        df["signal_date"] = df["date"].astype(str).str.replace("-", "").str[:8]
    elif "signal_date" in df.columns:
        df["signal_date"] = df["signal_date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    else:
        df["signal_date"] = now_ymd()

    if "name" not in df.columns:
        df["name"] = ""

    df["code"] = df["code"].astype(str).str.zfill(6)
    d_ref_ymd = _derive_d_from_fills_path(FILLS)
    before_d_filter = int(len(df))
    df_all = df.copy()
    sig_all = df_all["signal_date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    df = df_all[sig_all == str(d_ref_ymd)].copy()
    _staleness_fallback_sig = ""
    if df.empty:
        valid_sig = sig_all[sig_all.str.len() == 8]
        fallback_sig = str(valid_sig.max() or "") if len(valid_sig) else ""
        if fallback_sig:
            df = df_all[sig_all == fallback_sig].copy()
            _staleness_fallback_sig = fallback_sig
            print(f"[CAND_D_FILTER_FALLBACK] d_ref={d_ref_ymd} fallback_signal_date={fallback_sig}")
    print(f"[CAND_D_FILTER] d_ref={d_ref_ymd} rows={before_d_filter}->{len(df)}")
    if _staleness_fallback_sig:
        _stale_cfg = cfg.get("normal_candidate_staleness_check", {}) if isinstance(cfg.get("normal_candidate_staleness_check"), dict) else {}
        if bool(_stale_cfg.get("enabled", True)):
            _stale_max_days = max(1, int(_to_int(_stale_cfg.get("max_stale_days", 3), 3)))
            _stale_block = bool(_stale_cfg.get("block_if_stale", False))
            _stale_days = _ymd_days_ago(str(d_ref_ymd), _staleness_fallback_sig)
            if _stale_days is not None and _stale_days > _stale_max_days:
                print(
                    f"[CAND_STALENESS_STALE] signal_date={_staleness_fallback_sig} d_ref={d_ref_ymd} "
                    f"days_old={_stale_days} > max_stale_days={_stale_max_days} block_if_stale={_stale_block}"
                )
                if _stale_block:
                    print("[CAND_STALENESS_STALE] clearing candidates (block_if_stale=True)")
                    df = df.iloc[0:0].copy()
            else:
                print(f"[CAND_STALENESS_OK] signal_date={_staleness_fallback_sig} d_ref={d_ref_ymd} days_old={_stale_days}")
    state = load_state()
    recovered_open_pos, recovery_summary = _recover_open_positions(state, cfg)
    state["open_positions"] = recovered_open_pos
    t2_init = _initialize_t2_cash_state(state, cfg, now_ymd())
    _set_last_t2_initialized_state(state)
    if bool(t2_init.get("enabled", False)):
        print(
            f"[T2_CASH] settled_cash={float(t2_init.get('settled_cash', 0.0)):.0f} "
            f"pending_rows={int(t2_init.get('pending_rows', 0))} "
            f"released={float(t2_init.get('released_amount', 0.0)):.0f}"
        )
    replay_recovery_summary: Dict[str, Any] = {
        "eligible_rows": 0,
        "resume_rows": 0,
        "discard_rows": 0,
        "discard_codes": [],
        "discard_source_order_ids": [],
        "manual_review_rows": 0,
        "manual_review_codes": [],
    }
    replay_regen_result: Dict[str, Any] = {
        "attempted": False,
        "ok": False,
        "trigger_reason": "",
        "script_path": str(ROOTB_REPLAY_REGEN_SCRIPT),
        "started_at": "",
        "finished_at": "",
        "returncode": None,
        "stdout": "",
        "stderr": "",
    }
    replay_queue_scan: Dict[str, Any] = {}
    replay_queue_guard_reason = ""
    replay_quarantine_status: Dict[str, Any] = {}
    replay_prune_status: Dict[str, Any] = {}
    replay_consistency_status: Dict[str, Any] = {}
    replay_consistency_remediation: Dict[str, Any] = {}
    replay_summary_sync_status: Dict[str, Any] = {}
    carryover_revalidate_summary: Dict[str, Any] = {}
    if recovery_summary.get("normalized_fields", 0) or recovery_summary.get("deduped_positions", 0):
        print(
            "[RECOVERY] open_positions normalized=%s deduped=%s before=%s after=%s"
            % (
                recovery_summary.get("normalized_fields", 0),
                recovery_summary.get("deduped_positions", 0),
                recovery_summary.get("open_positions_before", 0),
                recovery_summary.get("open_positions_after", 0),
            )
        )

    op = _ops_policy(cfg)
    if bool(op.get("enabled", False)):
        max_age = max(1, _to_int(op.get("carryover_max_age_days", 2), 2))
        pending = _load_pending_signals(max_age_days=max_age)
        carryover_enabled = bool(op.get("carryover_no_next_day_enabled", True))
        if not carryover_enabled and len(pending) > 0:
            split2_pending_mask = pd.Series(False, index=pending.index)
            if "split_entry_2nd" in pending.columns:
                split2_pending_mask = split2_pending_mask | pending["split_entry_2nd"].astype(str).str.strip().str.lower().isin({"1", "true", "t", "y", "yes"})
            if "carry_origin_reason" in pending.columns:
                split2_pending_mask = split2_pending_mask | pending["carry_origin_reason"].astype(str).str.strip().str.upper().eq("SPLIT_ENTRY_2ND")
            pending = pending[split2_pending_mask].copy()
        if (not carryover_enabled) and pending.empty:
            _LAST_CARRYOVER_REVALIDATE_SUMMARY.clear()
        pending_revalidate_gap = max(0.0, float(op.get("carryover_revalidate_score_gap_max", 0.05)))
        pending_revalidate_min_final_score = float(op.get("carryover_revalidate_min_final_score", 0.0))
        pending_revalidated, pending_reval_stats = _revalidate_pending_signals(
            pending,
            df,
            score_gap_max=pending_revalidate_gap,
            min_final_score=pending_revalidate_min_final_score,
            market_gate_block=False,
            sector_gate_enabled=True,
        )
        if len(pending_revalidated) > 0:
            if "_carryover" not in df.columns:
                df["_carryover"] = 0
            for c in df.columns:
                if c not in pending_revalidated.columns:
                    pending_revalidated[c] = pd.NA
            for c in pending_revalidated.columns:
                if c not in df.columns:
                    df[c] = pd.NA
            df = _concat_drop_all_na_columns([df, pending_revalidated[df.columns]], ignore_index=True)
        if pending_reval_stats["loaded"] > 0:
            carryover_revalidate_summary = {
                "loaded": int(pending_reval_stats["loaded"]),
                "entry_retry_ready_rows": int(pending_reval_stats["revalidated"]),
                "revalidate_failed_rows": int(pending_reval_stats["dropped"]),
                "score_gap_failed": int(pending_reval_stats.get("score_gap_failed", 0)),
                "min_score_failed": int(pending_reval_stats.get("min_score_failed", 0)),
                "failed_codes": list(pending_reval_stats.get("failed_codes", []) or []),
                "failed_reason_counts": dict(pending_reval_stats.get("failed_reason_counts", {}) or {}),
            }
            print(
                "[CAND] carryover revalidated: loaded=%s kept=%s dropped=%s score_gap_failed=%s min_score_failed=%s from %s"
                % (
                    pending_reval_stats["loaded"],
                    pending_reval_stats["revalidated"],
                    pending_reval_stats["dropped"],
                    pending_reval_stats.get("score_gap_failed", 0),
                    pending_reval_stats.get("min_score_failed", 0),
                    PENDING_SIGNALS_PATH.name,
                )
            )
        _LAST_CARRYOVER_REVALIDATE_SUMMARY.clear()
        _LAST_CARRYOVER_REVALIDATE_SUMMARY.update(carryover_revalidate_summary or {})
        replay_raw = _load_replay_orders_raw(max_age_days=max_age)
        replay_queue_scan = _summarize_replay_queue_scan(replay_raw)
        replay_regen_reason = _replay_queue_regeneration_reason(replay_raw)
        if replay_regen_reason:
            replay_regen_result = _run_replay_queue_regeneration(replay_regen_reason)
            if replay_regen_result.get("ok"):
                print("[RECOVERY] replay queue regeneration succeeded")
            else:
                replay_queue_guard_reason = replay_regen_reason
                print(
                    "[RECOVERY] replay queue regeneration failed: %s"
                    % (str(replay_regen_result.get("stderr") or replay_regen_result.get("returncode") or "unknown"))
                )
        replay_pending = _load_replay_orders(max_age_days=max_age)
        replay_pending, replay_filter_summary = _partition_replay_orders_for_recovery(
            replay_pending,
            recovered_open_pos,
            queue_guard_reason=replay_queue_guard_reason,
        )
        replay_recovery_summary = {
            **replay_filter_summary,
        }
        if replay_recovery_summary.get("discard_rows", 0):
            print(
                "[RECOVERY] replay discarded rows=%s codes=%s"
                % (
                    replay_recovery_summary.get("discard_rows", 0),
                    ",".join(replay_recovery_summary.get("discard_codes", [])[:10]) or "-",
                )
            )
        if replay_recovery_summary.get("manual_review_rows", 0):
            print(
                "[RECOVERY] replay manual-review rows=%s codes=%s reason=%s"
                % (
                    replay_recovery_summary.get("manual_review_rows", 0),
                    ",".join(replay_recovery_summary.get("manual_review_codes", [])[:10]) or "-",
                    replay_recovery_summary.get("queue_guard_reason", "") or "lineage/manual",
                )
            )
        if len(replay_pending) > 0:
            if "_carryover" not in df.columns:
                df["_carryover"] = 0
            if "_replay_order" not in df.columns:
                df["_replay_order"] = 0
            for c in df.columns:
                if c not in replay_pending.columns:
                    replay_pending[c] = pd.NA
            for c in replay_pending.columns:
                if c not in df.columns:
                    df[c] = pd.NA
            df = _concat_drop_all_na_columns([replay_pending[df.columns], df], ignore_index=True)
            print(f"[CAND] replay queue merged: +{len(replay_pending)} rows from {REPLAY_ORDERS_PATH.name}")
    replay_quarantine_status = _write_replay_quarantine(replay_recovery_summary)
    replay_prune_status = _prune_replay_queue_after_quarantine(replay_raw if 'replay_raw' in locals() else pd.DataFrame(), replay_recovery_summary)
    replay_summary_sync_status = _sync_rootb_replay_summary(_write_replay_queue_status(max_age if 'max_age' in locals() else 2, 0))
    recovery_status_doc = _write_recovery_status(
        recovery_summary,
        replay_recovery_summary,
        replay_queue_scan,
        replay_quarantine_status,
        replay_prune_status,
        replay_consistency_status,
        replay_consistency_remediation,
        replay_summary_sync_status,
    )
    recovery_status_doc["replay_regeneration"] = replay_regen_result
    RECOVERY_STATUS_PATH.write_text(json.dumps(recovery_status_doc, ensure_ascii=False, indent=2), encoding="utf-8")

    if len(df) > 0:
        if "_carryover" not in df.columns:
            df["_carryover"] = 0
        if "_replay_order" not in df.columns:
            df["_replay_order"] = 0
        score_col = "final_score" if "final_score" in df.columns else ("score" if "score" in df.columns else None)
        if score_col:
            df["_score_dedup"] = pd.to_numeric(df[score_col], errors="coerce").fillna(-1e18)
            df = df.sort_values(["signal_date", "_replay_order", "code", "_carryover", "_score_dedup"], ascending=[False, False, True, False, False])
        else:
            df = df.sort_values(["signal_date", "_replay_order", "code", "_carryover"], ascending=[False, False, True, False])
        if "entry_day_override" in df.columns:
            df["_dedup_entry_day"] = df["entry_day_override"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
            df["_dedup_entry_day"] = df["_dedup_entry_day"].where(df["_dedup_entry_day"].str.len() == 8, df["signal_date"])
            df = df.drop_duplicates(["code", "_dedup_entry_day"], keep="first").copy()
        else:
            df = df.drop_duplicates(["code", "signal_date"], keep="first").copy()

    return df
