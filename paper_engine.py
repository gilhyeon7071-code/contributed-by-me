# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

import pandas as pd
from pricing_engine import (
    build_cost_profile,
)

# Logging setup.
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("paper_engine")

__version__ = "1.0.0"

# Ensure utils import works regardless of cwd.
ROOT = Path(os.environ.get("STOC_BASE_DIR", r"E:\1_Data"))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if __name__ == "paper_engine":
    __path__ = [str(ROOT / "paper_engine")]

# Shared helpers.
from utils.common import (
    is_daily_loss_reason,
    norm_code,
    now_ymd,
    read_csv_safe,
)
from utils.pipeline_audit import log_pipeline_event, count_rows
from paper_engine.config import load_config, _path_from_env, _deep_merge_dict
from paper_engine.io import (
    detect_schema,
    ensure_csv,
    _migrate_legacy_trades_header_if_needed,
    load_state,
    FILLS,
    TRADES,
    TRADES_CALC,
    STATE_PATH,
    LOG_DIR,
    _derive_d_from_fills_path,
    LEGACY_FILLS_HEADER,
    LEGACY_TRADES_HEADER,
    V411_FILLS_HEADER,
    V411_TRADES_HEADER,
    ROOTB_REPLAY_REGEN_SCRIPT,
    SURGE_REALTIME_STATUS_PATH,
    SURGE_ACTIVE_RESPONSE_LAYER_CSV_PATH,
    SURGE_LIVE_READINESS_AUDIT_CSV_PATH,
    _write_dashboard_compat_csv,
)
from paper_engine.drawdown import (
    _apply_drawdown_entry_capacity,
)
from paper_engine.common import (
    _to_float,
    _to_int,
    _truthy,
    _pct01_from_config,
    now_ts,
    _extract_ymd_from_ts_text,
    _load_fundamentals_db,
    _concat_drop_all_na_columns,
    RUN_LABEL,
    PAPER_SESSION_ID,
    _ops_policy,
    compute_dynamic_probe_floor,
    _safe_gate_float,
    _safe_gate_int,
    _load_sector_db,
    _paper_engine_phase_trace,
    _get_dict,
    _get_list,
)
from paper_engine.entry import (
    BASE_DIR,
    _apply_entry_selection_policy,
    _apply_horizon_entry_policy,
    _apply_runtime_caps_and_filters,
    _build_entry_risk_basis,
    _build_signal_tracking_context,
    _evaluate_entry_guards,
    _init_entry_loop_state,
    _minimum_quantity_verification_cfg,
    _normalize_entry_loop_result,
    _prepare_entry_candidate_pool,
    _prepare_pretrade_runtime,
    _print_entry_fill_summary_v2,
    _process_entry_rows,
    _write_entry_runtime_snapshots_and_reports,
    _write_p1_gate_status,
    apply_p1_entry_controls,
    final_entry_decision,
    load_prices_for_codes,
    maybe_run_pnl_report,
    derive_fx_entry_status,
    apply_fx_filter,
    load_candidates_chosen_level,
    parse_relax_level_num,
    pick_candidates,
    _LAST_CARRYOVER_REVALIDATE_SUMMARY,
)
from paper_engine.surge import (
    _write_surge_realtime_shadow_runtime_snapshot,
    _inject_surge_immediate_candidates as _surge_inject_surge_immediate_candidates,
    _compute_dynamic_max_new_surge,
)
from paper_engine import surge as _surge_module


def _inject_surge_immediate_candidates(*args: Any, **kwargs: Any):
    _surge_module.SURGE_REALTIME_STATUS_PATH = SURGE_REALTIME_STATUS_PATH
    _surge_module.SURGE_ACTIVE_RESPONSE_LAYER_CSV_PATH = SURGE_ACTIVE_RESPONSE_LAYER_CSV_PATH
    _surge_module.SURGE_LIVE_READINESS_AUDIT_CSV_PATH = SURGE_LIVE_READINESS_AUDIT_CSV_PATH
    return _surge_inject_surge_immediate_candidates(*args, **kwargs)

from paper_engine.exit import (
    _get_sell_rules,
    _run_intraday_residual_overnight_runtime,
)
from paper_engine.positions import (
    _append_fills_trades_with_rollback,
    _finalize_written_fills_runtime_state,
    _recover_open_positions,
    _compute_current_open_notional,
    _count_open_position_slots,
    _run_open_positions_rebalance_runtime,
)
from paper_engine.regime import (
    resolve_market_regime,
    load_latest_gate_snapshot,
    resolve_bear_sizing_confirmation,
)
from paper_engine.guards import (
    _apply_risk_off_entry_block,
    _apply_relax_ladder_entry_cap,
)
from paper_engine.risk_orchestration import (
    _apply_fx_and_rally_caps,
    _apply_max_positions_precheck,
    _apply_post_entry_gate_sizing_adjustments,
    _build_initial_sizing_context,
    _compute_risk_orch_scale,
)
from paper_engine.settlement import (
    _merge_last_t2_state_fields,
    _record_sell_pending_and_write_t2_status,
)
from paper_engine.state import (
    _build_risk_reason_details,
    load_latest_p0_snapshot,
    read_latest_stable_params,
    _stable_params_usable,
    _align_p0_snapshot_with_latest_pnl_for_ddm,
    load_latest_macro_snapshot,
    _build_trend_overlay_2026_context,
    _write_recovery_status,
    _run_replay_runtime_refresh,
    _build_entry_runtime_ops_summary,
    _finalize_paper_engine_runtime,
)

PAPER_DIR = BASE_DIR / "paper"

DDM_STATUS_PATH = _path_from_env(
    "PAPER_DDM_STATUS_PATH",
    LOG_DIR / "paper_ddm_status_latest.json",
)
BACKTEST_DEFERRED_VALIDATION_STATUS_PATH = _path_from_env(
    "PAPER_BACKTEST_DEFERRED_VALIDATION_STATUS_PATH",
    LOG_DIR / "backtest_deferred_validation_status_latest.json",
)

# Shared helper aliases are imported from utils.common.


# latest_file helper is imported from utils.common.

# read_csv_safe helper is imported from utils.common.


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="paper_engine",
        description="Paper trading engine for v41_1 strategy execution.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="Show version and exit.",
    )
    _args = parser.parse_args()

    cfg = load_config()
    _audit_ymd = now_ymd() or datetime.now().strftime("%Y%m%d")
    log_pipeline_event(
        stage="paper_engine_main",
        batch_label="[7/9]",
        event="START",
        date=_audit_ymd,
        input_files={
            "fills": str(FILLS),
            "trades": str(TRADES),
            "state": str(STATE_PATH),
        },
    )
    max_positions_config_value = _to_int(cfg.get("max_positions"), 0)
    max_positions_stable_value = None
    max_positions_intraday_value = None
    max_positions_source = "config"
    _migrate_legacy_trades_header_if_needed(TRADES)
    schema = detect_schema()
    if schema == "v41.1":
        print("[PAPER_ENGINE] WARNING: v41.1 schema active. dashboard_stock_v2.py expects legacy columns; "
              "legacy compat copies will be written to paper/fills_dashboard_compat.csv and "
              "paper/trades_dashboard_compat.csv")
    print("[PAPER_ENGINE] run_label=%s fills=%s trades=%s state=%s" % (RUN_LABEL, FILLS, TRADES, STATE_PATH))

    # ensure files exist with detected schema
    if schema == "legacy":
        ensure_csv(FILLS, LEGACY_FILLS_HEADER)
        ensure_csv(TRADES, LEGACY_TRADES_HEADER)
    else:
        ensure_csv(FILLS, V411_FILLS_HEADER)
        ensure_csv(TRADES, V411_TRADES_HEADER)

    stable = read_latest_stable_params()
    stop_loss = float(_to_float(cfg.get("stop_loss_pct"), -0.05))
    take_profit = cfg.get("take_profit_pct", None)
    trail_pct = cfg.get("trail_pct", None)
    budget_policy_cfg = _get_dict(cfg, "capital_budget_policy")
    budget_target_positions = (
        _to_int(budget_policy_cfg.get("basic_target_positions"), 0)
        if bool(budget_policy_cfg.get("enabled", False))
        else 0
    )
    stable_ok, stable_reason = _stable_params_usable(stable, cfg)
    if stable_ok:
        stop_loss = float(stable.get("stop_loss", stop_loss))
        take_profit = stable.get("take_profit", take_profit)
        trail_pct = stable.get("trail_pct", trail_pct)
        stable_max_pos = _to_int(stable.get("max_pos"), 0)
        stable_hold_days = _to_int(stable.get("hold"), 0)
        if stable_max_pos > 0:
            max_positions_stable_value = int(stable_max_pos)
            if budget_target_positions <= 0:
                cfg["max_positions"] = int(stable_max_pos)
                max_positions_source = "stable_params"
        if stable_hold_days > 0:
            cfg["max_hold_days"] = int(stable_hold_days)
        if stable_max_pos > 0 or stable_hold_days > 0:
            print(
                f"[CFG_STABLE_APPLIED] max_positions={cfg.get('max_positions')} "
                f"max_hold_days={cfg.get('max_hold_days')}"
            )
    else:
        print(f"[CFG_STABLE_SKIPPED] reason={stable_reason}")
    if budget_target_positions > 0:
        cfg["max_positions"] = int(budget_target_positions)
        max_positions_source = "capital_budget_policy"
        print(f"[CFG_BUDGET_APPLIED] max_positions={cfg.get('max_positions')}")
    intraday_max_pos = _to_int(os.getenv("PAPER_INTRADAY_MAX_POSITIONS", ""), 0)
    if intraday_max_pos > 0:
        cfg["max_positions"] = int(intraday_max_pos)
        max_positions_intraday_value = int(intraday_max_pos)
        max_positions_source = "intraday_env"
        print(f"[CFG_INTRADAY_OVERRIDE] max_positions={cfg.get('max_positions')}")
    sell_rules = _get_sell_rules(cfg)
    sell_rules_enabled = bool(sell_rules.get("enabled", False))
    fundamentals_db = _load_fundamentals_db(sell_rules if sell_rules_enabled else {})
    sector_db = _load_sector_db()

    cost_profile_name = str(os.getenv("PAPER_COST_PROFILE", "paper") or "paper").strip().lower()
    cost_profile = build_cost_profile(cfg, profile_name=cost_profile_name)
    fee_pct = float(cost_profile.fee_pct)
    slip_pct = float(cost_profile.slippage_pct)
    sell_tax_pct = float(cost_profile.sell_tax_pct)
    print(
        f"[COST_PROFILE] name={cost_profile_name} "
        f"fee_pct={fee_pct:.6f} slippage_pct={slip_pct:.6f} sell_tax_pct={sell_tax_pct:.6f}"
    )

    p0_snapshot = _align_p0_snapshot_with_latest_pnl_for_ddm(load_latest_p0_snapshot(LOG_DIR), LOG_DIR)
    macro_snapshot = load_latest_macro_snapshot(LOG_DIR)
    trend_overlay_ctx = _build_trend_overlay_2026_context(
        cfg=cfg,
        macro_snapshot=macro_snapshot,
        today_ymd=now_ymd(),
    )
    if bool(trend_overlay_ctx.get("enabled", False)):
        print(
            f"[TREND_2026] enabled=1 cutting_active={bool(trend_overlay_ctx.get('cutting_active', False))} "
            f"rate_hawkish={trend_overlay_ctx.get('rate_hawkish')} "
            f"seasonal_multiplier={float(_to_float(trend_overlay_ctx.get('seasonal_multiplier'), 1.0)):.3f}"
        )
    rpol = cfg.get("regime_entry_policy", {}) if isinstance(cfg, dict) else {}
    if not isinstance(rpol, dict):
        rpol = {}
    try:
        gate_max_age_days = int(float(rpol.get("gate_daily_max_age_days", 2) or 2))
    except Exception:
        gate_max_age_days = 2
    p0_snapshot_path = Path(str(p0_snapshot.get("path") or "")) if str(p0_snapshot.get("path") or "").strip() else None
    gate_snapshot = load_latest_gate_snapshot(LOG_DIR, max_age_days=max(0, gate_max_age_days), latest_p0_path=p0_snapshot_path)
    if gate_snapshot.get("p0_aligned") is False:
        print(
            "[GATE_DAILY_ALIGN] BLOCK stale gate_daily "
            f"gate_p0={gate_snapshot.get('p0_daily_check')} latest_p0={p0_snapshot.get('path')} "
            f"reason={gate_snapshot.get('p0_alignment_reason')}"
        )
    regime_info = resolve_market_regime(cfg, p0_snapshot, macro_snapshot, gate_snapshot=gate_snapshot)
    market_regime = str(regime_info.get("regime") or "NORMAL").upper()
    regime_overrides = cfg.get("regime_overrides", {}) if isinstance(cfg, dict) else {}
    bear_sizing_confirmation = resolve_bear_sizing_confirmation(LOG_DIR, market_regime)
    regime_override_key = market_regime
    if market_regime == "BEAR" and not bear_sizing_confirmation.get("confirmed"):
        regime_override_key = "NORMAL"
        print(
            f"[REGIME_OVERRIDE] BEAR sizing not confirmed reason={bear_sizing_confirmation.get('reason')} "
            f"entry_gate={bear_sizing_confirmation.get('entry_gate_decision')} "
            f"risk_orch_scale={bear_sizing_confirmation.get('risk_orch_scale')} -> using NORMAL override"
        )
    if isinstance(regime_overrides, dict):
        regime_override = regime_overrides.get(regime_override_key)
        if isinstance(regime_override, dict):
            _deep_merge_dict(cfg, regime_override)
            print(
                f"[REGIME_OVERRIDE] applied={regime_override_key} (market_regime={market_regime}) "
                f"keys={','.join(sorted(str(k) for k in regime_override.keys()))}"
            )
            stop_loss = float(_to_float(cfg.get("stop_loss_pct"), stop_loss) or stop_loss)
            take_profit = cfg.get("take_profit_pct", take_profit)
            trail_pct = cfg.get("trail_pct", trail_pct)
            budget_policy_cfg = _get_dict(cfg, "capital_budget_policy")
            budget_target_positions = (
                _to_int(budget_policy_cfg.get("basic_target_positions"), 0)
                if bool(budget_policy_cfg.get("enabled", False))
                else 0
            )
            if budget_target_positions > 0 and max_positions_intraday_value is None:
                cfg["max_positions"] = int(budget_target_positions)
                max_positions_source = "capital_budget_policy:regime_override"
                print(f"[REGIME_OVERRIDE] max_positions={cfg.get('max_positions')} source={max_positions_source}")
            sell_rules = _get_sell_rules(cfg)
            sell_rules_enabled = bool(sell_rules.get("enabled", False))
            fundamentals_db = _load_fundamentals_db(sell_rules if sell_rules_enabled else {})
            cost_profile = build_cost_profile(cfg, profile_name=cost_profile_name)
            fee_pct = float(cost_profile.fee_pct)
            slip_pct = float(cost_profile.slippage_pct)
            sell_tax_pct = float(cost_profile.sell_tax_pct)
            print(
                f"[REGIME_OVERRIDE] recalculated stop_loss={stop_loss} "
                f"take_profit={take_profit} trail_pct={trail_pct} "
                f"sell_rules_enabled={sell_rules_enabled} "
                f"fee_pct={fee_pct:.6f} slippage_pct={slip_pct:.6f} sell_tax_pct={sell_tax_pct:.6f}"
            )
    regime_policy = cfg.get("regime_entry_policy", {}) if isinstance(cfg, dict) else {}
    if not isinstance(regime_policy, dict):
        regime_policy = {}
    ops_policy = _ops_policy(cfg)
    ops_enabled = bool(ops_policy.get("enabled", False))
    state = load_state()
    recovered_open_pos, recovery_summary = _recover_open_positions(state, cfg)
    state["open_positions"] = recovered_open_pos
    replay_recovery_summary: Dict[str, Any] = {
        "eligible_rows": 0,
        "resume_rows": 0,
        "discard_rows": 0,
        "discard_codes": [],
        "discard_source_order_ids": [],
        "manual_review_rows": 0,
        "manual_review_codes": [],
        "queue_guard_reason": "",
        "queue_guard_rows": 0,
        "discard_details": [],
        "manual_review_details": [],
        "manual_review_snapshot_rows": [],
        "resume_details": [],
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
    replay_quarantine_status: Dict[str, Any] = {}
    replay_prune_status: Dict[str, Any] = {}
    replay_consistency_status: Dict[str, Any] = {}
    replay_consistency_remediation: Dict[str, Any] = {}
    recovery_status_doc = _write_recovery_status(
        recovery_summary,
        replay_recovery_summary,
        replay_queue_scan,
        replay_quarantine_status,
        replay_prune_status,
        replay_consistency_status,
        replay_consistency_remediation,
    )
    print(f"[REGIME] regime={market_regime} info={regime_info}")
    risk_off_enabled = bool(p0_snapshot.get("risk_off_enabled", False))
    risk_off_reasons = list(p0_snapshot.get("risk_off_reasons") or [])
    chosen_level = load_candidates_chosen_level(LOG_DIR)
    chosen_level_num = parse_relax_level_num(chosen_level)

    base_max_new = int(cfg.get("max_new_trades_per_day", 3))
    max_new = base_max_new
    surge_policy = cfg.get("surge_entry_policy", {}) if isinstance(cfg.get("surge_entry_policy"), dict) else {}
    max_new_surge = max(0, _to_int(surge_policy.get("max_new_surge", 2), 2))
    exit_only_mode = str(os.getenv("PAPER_EXIT_ONLY", "") or os.getenv("PAPER_NO_ENTRY", "") or "").strip().lower() in {"1", "true", "yes", "on"}
    dynamic_max_new_surge_meta: Dict[str, Any] = {"enabled": False, "reason": "not_evaluated"}
    max_new_zero_reason = ""
    def _capture_max_new_zero(stage: str) -> None:
        nonlocal max_new_zero_reason
        if int(max_new) <= 0 and not str(max_new_zero_reason or "").strip():
            max_new_zero_reason = str(stage or "unknown")

    if exit_only_mode:
        max_new = 0
        max_new_surge = 0
        dynamic_max_new_surge_meta = {"enabled": False, "reason": "exit_only_mode"}
        _capture_max_new_zero("exit_only_mode")
        print("[ENTRY_EXIT_ONLY] PAPER_EXIT_ONLY active -> max_new=0 max_new_surge=0")

    aec = cfg.get("adaptive_entry_control", {}) if isinstance(cfg, dict) else {}
    adaptive_enabled = bool(isinstance(aec, dict) and aec.get("enabled", False))

    ks_cfg = cfg.get("kill_switch", {}) if isinstance(cfg, dict) else {}
    ks_mode = str(ks_cfg.get("mode", "BLOCK")).upper()
    if ks_mode not in {"BLOCK", "REDUCE"}:
        print(f"[RISK_GATE_CFG_WARN] kill_switch.mode invalid={ks_mode} -> BLOCK")
        ks_mode = "BLOCK"
    ks_reduce_factor, ks_reduce_factor_fb = _safe_gate_float(
        ks_cfg.get("reduce_factor", 0.5),
        0.5,
        min_v=0.0,
        max_v=1.0,
    )
    ks_min_new, ks_min_new_fb = _safe_gate_int(
        ks_cfg.get("min_new_trades_per_day", 1),
        1,
        min_v=0,
    )

    crash_cfg = cfg.get("crash_risk_off", {}) if isinstance(cfg, dict) else {}
    crash_mode = str(crash_cfg.get("mode", "BLOCK")).upper()
    if crash_mode not in {"BLOCK", "REDUCE"}:
        print(f"[RISK_GATE_CFG_WARN] crash_risk_off.mode invalid={crash_mode} -> BLOCK")
        crash_mode = "BLOCK"
    crash_reduce_factor, crash_reduce_factor_fb = _safe_gate_float(
        crash_cfg.get("reduce_factor", 0.5),
        0.5,
        min_v=0.0,
        max_v=1.0,
    )
    crash_min_new, crash_min_new_fb = _safe_gate_int(
        crash_cfg.get("min_new_trades_per_day", 1),
        1,
        min_v=0,
    )

    ks_snap = _get_dict(p0_snapshot, "kill_switch")
    ks_limits = _get_dict(ks_snap, "limits")
    cr_snap = _get_dict(p0_snapshot, "crash_risk_off")
    cr_limits = _get_dict(cr_snap, "limits")
    print(
        "[RISK_GATE_CFG] "
        f"risk_off_enabled={bool(risk_off_enabled)} "
        f"kill_switch.enabled={bool(ks_snap.get('enabled', False))} "
        f"kill_switch.triggered={bool(ks_snap.get('triggered', False))} "
        f"kill_switch.mode={ks_mode} "
        f"kill_switch.reduce_factor={ks_reduce_factor:.4f} "
        f"kill_switch.min_new={int(ks_min_new)} "
        f"kill_switch.limit.max_dd={ks_limits.get('max_drawdown_pct')} "
        f"kill_switch.limit.max_daily_loss={ks_limits.get('max_daily_loss_pct')} "
        f"crash.enabled={bool(cr_snap.get('enabled', False))} "
        f"crash.triggered={bool(cr_snap.get('triggered', False))} "
        f"crash.mode={crash_mode} "
        f"crash.reduce_factor={crash_reduce_factor:.4f} "
        f"crash.min_new={int(crash_min_new)} "
        f"crash.limit.trigger_max_dd={cr_limits.get('trigger_max_dd_pct')} "
        f"crash.limit.trigger_day_ret={cr_limits.get('trigger_day_ret_pct')} "
        f"fallback_used={{'ks_reduce_factor':{bool(ks_reduce_factor_fb)},'ks_min_new':{bool(ks_min_new_fb)},"
        f"'crash_reduce_factor':{bool(crash_reduce_factor_fb)},'crash_min_new':{bool(crash_min_new_fb)}}}"
    )

    risk_reason_details: List[Dict[str, Any]] = _build_risk_reason_details(list(risk_off_reasons or []), p0_snapshot)
    risk_off_entry_block = _apply_risk_off_entry_block(
        cfg=cfg,
        p0_snapshot=p0_snapshot,
        log_dir=LOG_DIR,
        risk_off_enabled=bool(risk_off_enabled),
        risk_off_reasons=list(risk_off_reasons or []),
        risk_reason_details=list(risk_reason_details or []),
        run_label=str(RUN_LABEL or ""),
        kill_switch_cfg=ks_cfg,
        kill_switch_mode=ks_mode,
        kill_switch_reduce_factor=float(ks_reduce_factor),
        kill_switch_min_new=int(ks_min_new),
        crash_mode=crash_mode,
        crash_reduce_factor=float(crash_reduce_factor),
        crash_min_new=int(crash_min_new),
        adaptive_entry_control=aec,
        adaptive_enabled=bool(adaptive_enabled),
        base_max_new=int(base_max_new),
        max_new=int(max_new),
    )
    max_new = int(risk_off_entry_block.get("max_new", max_new) or 0)
    risk_off_hard = bool(risk_off_entry_block.get("risk_off_hard", False))
    risk_off_has_kill = bool(risk_off_entry_block.get("risk_off_has_kill", False))
    daily_loss_relief_active = bool(risk_off_entry_block.get("daily_loss_relief_active", False))
    risk_off_zero_stage = str(risk_off_entry_block.get("risk_off_zero_stage", "risk_off") or "risk_off")
    _capture_max_new_zero(risk_off_zero_stage)
    # Regime policy override (operations matrix)
    if bool(regime_policy.get("enabled", False)):
        if market_regime == "RALLY":
            if (
                bool(regime_policy.get("rally_probe_under_kill_switch_block", True))
                and risk_off_enabled
                and risk_off_has_kill
                and (not risk_off_hard)
                and max_new <= 0
            ):
                try:
                    rally_probe_max_new = int(regime_policy.get("rally_probe_max_new", 1) or 1)
                except Exception:
                    rally_probe_max_new = 1
                rally_probe_max_new = max(1, rally_probe_max_new)
                max_new = min(base_max_new, rally_probe_max_new)
                print(f"[REGIME] RALLY -> probe reopen max_new={max_new} under kill_switch block")

    max_new = _apply_relax_ladder_entry_cap(
        chosen_level=chosen_level,
        chosen_level_num=chosen_level_num,
        adaptive_entry_control=aec,
        base_max_new=base_max_new,
        max_new=max_new,
    )
    ddm_runtime = _apply_drawdown_entry_capacity(
        cfg=cfg,
        p0_snapshot=p0_snapshot,
        gate_snapshot=gate_snapshot,
        trades_path=TRADES,
        log_dir=LOG_DIR,
        ddm_status_path=DDM_STATUS_PATH,
        max_new=max_new,
    )
    ddm_cfg = cast(Dict[str, Any], ddm_runtime.get("ddm_cfg") or {})
    ddm_enabled = bool(ddm_runtime.get("ddm_enabled", False))
    consecutive_loss_days = int(ddm_runtime.get("consecutive_loss_days", 0) or 0)
    ddm_context = cast(Dict[str, Any], ddm_runtime.get("ddm_context") or {})
    ddm_action = ddm_runtime.get("ddm_action")
    ddm_exposure_cap = cast(Optional[float], ddm_runtime.get("ddm_exposure_cap"))
    ddm_force_liquidate_pct = float(ddm_runtime.get("ddm_force_liquidate_pct", 0.0) or 0.0)
    max_new = int(ddm_runtime.get("max_new", max_new) or 0)
    _capture_max_new_zero("ddm_stage_cap")

    max_hold_days = int(cfg.get("max_hold_days", 10))
    entry_timing_mode = str(cfg.get("entry_timing_mode", "next_open") or "next_open").strip().lower()
    same_close_entry_mode = entry_timing_mode in {"same_close", "close", "t_close", "close_entry"}
    intraday_realtime_mode_env = _truthy(os.getenv("PAPER_INTRADAY_REALTIME_MODE", ""))
    intraday_realtime_mode = bool(intraday_realtime_mode_env or bool(ops_enabled and ops_policy.get("strict_same_day_only", False)))
    print(
        f"[ENTRY_MODE] timing={entry_timing_mode} same_close={same_close_entry_mode} "
        f"intraday_realtime={intraday_realtime_mode} env={intraday_realtime_mode_env} "
        f"strict_same_day={bool(ops_enabled and ops_policy.get('strict_same_day_only', False))}"
    )
    max_positions = int(cfg.get("max_positions", 0) or 0)
    max_positions_meta = {
        "effective": int(max_positions),
        "source": str(max_positions_source),
        "config": int(max_positions_config_value),
        "stable_params": max_positions_stable_value,
        "intraday_env": max_positions_intraday_value,
    }

    replay_cfg = cfg.get("stale_signal_replay", {}) if isinstance(cfg.get("stale_signal_replay", {}), dict) else {}
    replay_enabled = bool(replay_cfg.get("enabled", False))
    replay_min_age_days = max(1, _to_int(replay_cfg.get("min_signal_age_days", 2), 2))
    replay_require_no_open_positions = bool(replay_cfg.get("require_no_open_positions", True))
    replay_order_id_include_entry_day = bool(replay_cfg.get("order_id_include_entry_day", True))

    initial_sizing = _build_initial_sizing_context(
        cfg=cfg,
        p0_snapshot=p0_snapshot,
        macro_snapshot=macro_snapshot,
        log_dir=LOG_DIR,
        ddm_cfg=ddm_cfg,
        consecutive_loss_days=int(consecutive_loss_days),
        ddm_exposure_cap=ddm_exposure_cap,
        max_new=int(max_new),
        max_positions_meta=max_positions_meta,
    )
    capital_total = float(initial_sizing.get("capital_total", 0.0) or 0.0)
    capital_total_configured = float(initial_sizing.get("capital_total_configured", capital_total) or 0.0)
    account_equity_for_allocation = initial_sizing.get("account_equity_for_allocation")
    max_positions_meta = cast(Dict[str, Any], initial_sizing.get("max_positions_meta") or max_positions_meta)
    max_gross_exposure_pct = float(initial_sizing.get("max_gross_exposure_pct", 1.0) or 0.0)
    max_daily_new_exposure_pct = float(initial_sizing.get("max_daily_new_exposure_pct", 1.0) or 0.0)
    capital_budget_policy = cast(Dict[str, Any], initial_sizing.get("capital_budget_policy") or {})
    capital_budget_enabled = bool(initial_sizing.get("capital_budget_enabled", False))
    max_new = int(initial_sizing.get("max_new", max_new) or 0)
    position_size_multiplier = float(initial_sizing.get("position_size_multiplier", 1.0) or 1.0)
    vix_proxy = initial_sizing.get("vix_proxy")
    max_per_sector_runtime = int(cfg.get("max_per_sector", 0) or 0)

    gap_up_max_pct_runtime = float(cfg.get("gap_up_max_pct", 0.0) or 0.0)
    entry_gap_down_stop_pct_runtime = float(cfg.get("entry_gap_down_stop_pct", 0.0) or 0.0)
    fx_policy = cfg.get("fx_entry_policy", {}) if isinstance(cfg.get("fx_entry_policy"), dict) else {}
    fx_ctx: Dict[str, Any] = _get_dict(macro_snapshot, "fx_context")
    _p0_mr = p0_snapshot.get("market_regime") or (p0_snapshot.get("meta") or {}).get("market_regime")
    p0_market_status = str(_p0_mr or "").upper() or "UNKNOWN"
    gate_market_status = str(gate_snapshot.get("gate_status") or "").upper() or "UNKNOWN"
    risk_orch_trades_path = TRADES_CALC if (TRADES_CALC.exists() and not str(os.getenv("PAPER_TRADES_PATH", "") or "").strip()) else TRADES
    risk_orch_ctx = _compute_risk_orch_scale(
        cfg=cfg,
        market_regime=str(market_regime or ""),
        p0_snapshot=(p0_snapshot if isinstance(p0_snapshot, dict) else {}),
        fee_pct=float(fee_pct),
        slip_pct=float(slip_pct),
        sell_tax_pct=float(sell_tax_pct),
        trades_path=risk_orch_trades_path,
    )
    if bool(risk_orch_ctx.get("enabled", False)):
        ro_scale = max(0.0, _to_float(risk_orch_ctx.get("scale", 1.0), 1.0))
        old_psm = position_size_multiplier
        position_size_multiplier = min(position_size_multiplier, ro_scale)
        print(
            f"[RISK_ORCH] regime={risk_orch_ctx.get('regime')} "
            f"edge={_to_float(risk_orch_ctx.get('edge', 0.0), 0.0):.6f} "
            f"var={_to_float(risk_orch_ctx.get('variance', 0.0), 0.0):.6f} "
            f"es={_to_float(risk_orch_ctx.get('es', 0.0), 0.0):.6f} "
            f"est_vol={_to_float(risk_orch_ctx.get('est_vol', 0.0), 0.0):.6f} "
            f"target_vol={_to_float(risk_orch_ctx.get('target_vol', 0.0), 0.0):.6f} "
            f"scale={ro_scale:.3f} position_size_multiplier={old_psm:.3f}->{position_size_multiplier:.3f}"
        )
    try:
        ro_payload = {
            "generated_at": now_ts(),
            "run_label": str(RUN_LABEL or ""),
            "market_regime": str(market_regime or ""),
            "position_size_multiplier": float(position_size_multiplier),
            "risk_orchestration": risk_orch_ctx,
        }
        (LOG_DIR / "risk_orchestration_latest.json").write_text(
            json.dumps(ro_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as _roe:
        print(f"[RISK_ORCH] write_status=FAIL reason={type(_roe).__name__}:{_roe}")

    paper_validation_sample_policy = (
        cfg.get("paper_validation_sample_policy", {})
        if isinstance(cfg.get("paper_validation_sample_policy"), dict)
        else {}
    )

    def _paper_validation_sample_cap(default_cap: int, decision_code: str = "") -> tuple[int, str]:
        if not bool(paper_validation_sample_policy.get("enabled", False)):
            return int(default_cap), "disabled"
        allowed_labels = {
            str(x or "").strip().lower()
            for x in (paper_validation_sample_policy.get("allowed_run_labels") or ["main", "validation", "tuning"])
        }
        run_label = str(RUN_LABEL or "main").strip().lower() or "main"
        if run_label not in allowed_labels:
            return int(default_cap), f"run_label_not_allowed:{run_label}"
        allowed_decisions = {
            str(x or "").strip().upper()
            for x in (paper_validation_sample_policy.get("allow_entry_gate_decisions") or ["CAUTION", "REDUCE"])
        }
        decision_text = str(decision_code or "").strip().upper()
        if decision_text and decision_text not in allowed_decisions:
            return int(default_cap), f"decision_not_allowed:{decision_text}"
        if bool(paper_validation_sample_policy.get("require_account_risk_clear", True)) and not bool(risk_orch_ctx.get("account_risk_clear", False)):
            return int(default_cap), "account_risk_not_clear"
        if bool(paper_validation_sample_policy.get("require_no_es_hard_block", True)) and bool(risk_orch_ctx.get("es_hard_block", False)):
            return int(default_cap), "es_hard_block"
        if bool(paper_validation_sample_policy.get("require_no_dd_stop_triggered", True)) and bool(risk_orch_ctx.get("dd_stop_triggered", False)):
            return int(default_cap), "dd_stop_triggered"
        scale = max(0.0, min(1.0, _to_float(risk_orch_ctx.get("scale", position_size_multiplier), position_size_multiplier)))
        scale_floor = max(0.0, min(1.0, _to_float(paper_validation_sample_policy.get("min_scale_floor", 0.0), 0.0)))
        effective_scale = max(scale, scale_floor)
        derived_cap = max(1, int(math.ceil(max(0, int(base_max_new)) * effective_scale)))
        return min(max(0, int(base_max_new)), derived_cap), f"risk_scale_cap:{effective_scale:.3f}"

    sample_cap_pre_p1, sample_cap_pre_p1_reason = _paper_validation_sample_cap(0)
    if sample_cap_pre_p1 > int(max_new):
        old_max_new = int(max_new)
        max_new = min(int(base_max_new), int(sample_cap_pre_p1))
        print(
            f"[PAPER_VALIDATION_SAMPLE] pre_p1 max_new floor {old_max_new}->{int(max_new)} "
            f"sample_cap={sample_cap_pre_p1} reason={sample_cap_pre_p1_reason}"
        )
    if str(RUN_LABEL or "").strip().lower() == "shadow":
        _rr = [str(r) for r in (risk_off_reasons or [])]
        _daily_only = bool(_rr) and all(
            (is_daily_loss_reason(r) or ("kill_switch" in str(r)))
            for r in _rr
        )
        if gate_market_status == "BLOCK" and _daily_only:
            gate_market_status = "PASS"
            print("[ENTRY_GATE] shadow bypass gate BLOCK from daily-loss kill_switch reasons")
    elif daily_loss_relief_active:
        _rr = [str(r) for r in (risk_off_reasons or [])]
        _daily_only = bool(_rr) and all(
            (is_daily_loss_reason(r) or ("kill_switch" in str(r)))
            for r in _rr
        )
        if gate_market_status == "BLOCK" and _daily_only:
            gate_market_status = "PASS"
            print("[ENTRY_GATE] main bypass gate BLOCK from daily-loss kill_switch reasons (adaptive relief)")
    fx_status = derive_fx_entry_status(fx_ctx, fx_policy)
    entry_decision = final_entry_decision(
        p0=p0_market_status,
        gate_daily=gate_market_status,
        engine_status=market_regime,
        fx_status=fx_status,
        risk_off=bool(risk_off_enabled),
        daily_return=regime_info.get("day_ret"),
        rally_day_ret_min=_to_float(regime_policy.get("rally_day_ret_min", 0.025), 0.025),
        rate_hike_fear_reduce_day_ret_floor=_to_float(regime_policy.get("rate_hike_fear_reduce_day_ret_floor", -0.015), -0.015),
        allow_bear_rally_override=bool(regime_policy.get("allow_bear_rally_override", True)),
    )
    entry_decision_code = str(entry_decision.get("decision") or "ALLOW").upper()
    entry_decision_reason = str(entry_decision.get("reason") or "")
    if (
        str(market_regime or "").upper() == "CRASH"
        and bool(regime_policy.get("crash_force_block", True))
    ):
        entry_decision_code = "BLOCK"
        entry_decision_reason = (
            f"{entry_decision_reason};regime_crash_force_block"
            if entry_decision_reason
            else "regime_crash_force_block"
        )
    (
        entry_decision_code,
        entry_decision_reason,
        outlier_gate,
        integrity_gate,
        sigma_guard,
        execution_guard,
        macro_news_guard,
        backtest_validation_guard,
        production_risk_guard,
    ) = _evaluate_entry_guards(
        cfg,
        LOG_DIR,
        entry_decision_code,
        entry_decision_reason,
        apply_execution_guard=(str(RUN_LABEL or "").strip().lower() == "main"),
        current_risk_orch=risk_orch_ctx,
    )

    outlier_decision = str(outlier_gate.get("decision") or "ALLOW").upper()
    outlier_reason = str(outlier_gate.get("reason") or "")
    integrity_decision = str(integrity_gate.get("decision") or "ALLOW").upper()
    integrity_reason = str(integrity_gate.get("reason") or "")
    sigma_decision = str(sigma_guard.get("decision") or "ALLOW").upper()
    sigma_reason = str(sigma_guard.get("reason") or "")
    execution_decision = str(execution_guard.get("decision") or "ALLOW").upper()
    execution_reason = str(execution_guard.get("reason") or "")
    macro_news_decision = str(macro_news_guard.get("decision") or "ALLOW").upper()
    macro_news_reason = str(macro_news_guard.get("reason") or "")
    backtest_validation_decision = str(backtest_validation_guard.get("decision") or "ALLOW").upper()
    backtest_validation_reason = str(backtest_validation_guard.get("reason") or "")
    production_risk_decision = str(production_risk_guard.get("decision") or "ALLOW").upper()
    production_risk_reason = str(production_risk_guard.get("reason") or "")
    print(
        f"[OUTLIER_GATE] decision={outlier_decision} reason={outlier_reason} "
        f"path={outlier_gate.get('path')} asof={outlier_gate.get('as_of_ymd')} age_days={outlier_gate.get('age_days')}"
    )
    print(
        f"[INTEGRITY_GATE] decision={integrity_decision} reason={integrity_reason} "
        f"ref={integrity_gate.get('reference_ymd')} max_skew_days={integrity_gate.get('max_skew_days')}"
    )
    print(
        f"[SIGMA_GUARD] decision={sigma_decision} reason={sigma_reason} "
        f"current={sigma_guard.get('current_rows')} mean={sigma_guard.get('mean_rows')} "
        f"std={sigma_guard.get('std_rows')} z={sigma_guard.get('zscore')}"
    )
    print(
        f"[EXECUTION_GUARD] decision={execution_decision} reason={execution_reason} "
        f"lifecycle={execution_guard.get('lifecycle_status')} "
        f"state_machine={execution_guard.get('state_machine_status')} "
        f"slippage_avg_bps={execution_guard.get('slippage_avg_bps')}"
    )
    print(
        f"[PROD_RISK_PLAYBOOK] action={production_risk_guard.get('action')} "
        f"decision={production_risk_decision} reason={production_risk_reason} "
        f"source_fresh={production_risk_guard.get('source_fresh')} "
        f"path={production_risk_guard.get('artifact_path')}"
    )
    if production_risk_decision in {"REDUCE", "CAUTION", "BLOCK"}:
        prp_cfg = cfg.get("production_risk_playbook", {}) if isinstance(cfg.get("production_risk_playbook"), dict) else {}
        prp_mult_default = 0.0 if production_risk_decision == "BLOCK" else 0.5
        prp_mult = max(0.0, min(1.0, _to_float(prp_cfg.get("hard_size_multiplier" if production_risk_decision == "BLOCK" else "soft_size_multiplier", prp_mult_default), prp_mult_default)))
        old_psm_prp = float(position_size_multiplier)
        position_size_multiplier = min(float(position_size_multiplier), float(prp_mult))
        print(
            f"[PROD_RISK_PLAYBOOK] mitigation size_multiplier "
            f"{old_psm_prp:.3f}->{float(position_size_multiplier):.3f}"
        )
    print(
        f"[MACRO_NEWS_GUARD] decision={macro_news_decision} reason={macro_news_reason} "
        f"macro_stale_ratio={macro_news_guard.get('macro_stale_ratio')} "
        f"macro_critical_bad={macro_news_guard.get('macro_critical_bad')} "
        f"news_quality={macro_news_guard.get('news_quality')} quota_guard={macro_news_guard.get('news_quota_guard_stop')}"
    )
    print(
        f"[BTVAL_GUARD] decision={backtest_validation_decision} reason={backtest_validation_reason} "
        f"failed={','.join(backtest_validation_guard.get('failed_gates', [])[:5]) or '-'} "
        f"asof={backtest_validation_guard.get('as_of')} age_days={backtest_validation_guard.get('age_days')}"
    )
    print(
        f"[ENTRY_GATE] decision={entry_decision_code} reason={entry_decision_reason} "
        f"p0={p0_market_status} gate={gate_market_status} engine={market_regime} fx={fx_status}"
    )
    if (
        intraday_realtime_mode
        and entry_decision_code == "BLOCK"
        and "outlier=stage:dashboard:FAIL" in str(entry_decision_reason or "")
    ):
        entry_decision_code = "REDUCE"
        entry_decision_reason = f"{entry_decision_reason};intraday_relax_dashboard_outlier"
        print(f"[ENTRY_GATE] intraday relax: BLOCK->REDUCE reason={entry_decision_reason}")

    _ro_validation_reduce_applied = False
    _ro_val_cfg_for_surge_floor: Dict[str, Any] = {}
    if bool(risk_orch_ctx.get("enabled", False)) and float(position_size_multiplier) <= 0.0:
        _ro_reasons: List[str] = []
        _ro_zero_causes = risk_orch_ctx.get("scale_zero_causes", [])
        if not isinstance(_ro_zero_causes, list):
            _ro_zero_causes = []
        _ro_zero_cause_set = {str(x or "").strip() for x in _ro_zero_causes if str(x or "").strip()}
        _ro_dd_stop_triggered = False
        if "dd_stop" in _ro_zero_cause_set:
            _ro_dd_stop_triggered = True
            _ro_reasons.append("dd_stop")
        if "es_hard_block" in _ro_zero_cause_set:
            _ro_reasons.append("es_hard_block")
        if "kelly_zero" in _ro_zero_cause_set:
            _ro_reasons.append("kelly_zero")
        _ro_reason = "risk_orch_size_zero" + (":" + ",".join(_ro_reasons) if _ro_reasons else "")
        _ro_val_cfg = (cfg.get("risk_orchestration", {}) if isinstance(cfg, dict) else {}).get("dd_stop_validation", {})
        if isinstance(_ro_val_cfg, dict):
            _ro_val_cfg_for_surge_floor = _ro_val_cfg
        _ro_val_enabled = bool(_ro_val_cfg.get("enabled", False)) if isinstance(_ro_val_cfg, dict) else False
        _ro_val_mode = str(_ro_val_cfg.get("mode", "block") if isinstance(_ro_val_cfg, dict) else "block").strip().lower()
        _ro_allowed_labels = _ro_val_cfg.get("allowed_run_labels", ["main", "validation", "tuning"]) if isinstance(_ro_val_cfg, dict) else []
        if not isinstance(_ro_allowed_labels, list):
            _ro_allowed_labels = ["main", "validation", "tuning"]
        _ro_label_allowed = str(RUN_LABEL or "main").strip().lower() in {
            str(x or "").strip().lower() for x in _ro_allowed_labels
        }
        _ro_val_allow_reasons = _ro_val_cfg.get("allow_reasons", ["dd_stop"]) if isinstance(_ro_val_cfg, dict) else ["dd_stop"]
        if not isinstance(_ro_val_allow_reasons, list):
            _ro_val_allow_reasons = ["dd_stop"]
        _ro_val_allowed_reason_set = {
            str(x or "").strip()
            for x in _ro_val_allow_reasons
            if str(x or "").strip()
        }
        _ro_val_reasons_allowed = bool(_ro_reasons) and set(_ro_reasons).issubset(_ro_val_allowed_reason_set)
        _ro_val_require_dd_stop = bool(_ro_val_cfg.get("require_dd_stop", True)) if isinstance(_ro_val_cfg, dict) else True
        _ro_val_dd_condition = (bool(_ro_dd_stop_triggered) if _ro_val_require_dd_stop else bool(_ro_reasons))
        _ro_validation_reduce_applied = False
        _min_qty_verification = _minimum_quantity_verification_cfg(cfg)
        _min_qty_allowed_reason_set = {str(x).strip() for x in (_min_qty_verification.get("allow_reasons") or []) if str(x).strip()}
        _min_qty_reason_allowed = bool(_ro_reasons) and set(_ro_reasons).issubset(_min_qty_allowed_reason_set)
        if (
            bool(_min_qty_verification.get("enabled", False))
            and _ro_dd_stop_triggered
            and _min_qty_reason_allowed
            and entry_decision_code != "BLOCK"
        ):
            entry_decision_code = "REDUCE"
            entry_decision_reason = (
                f"{entry_decision_reason};{_ro_reason};minimum_quantity_verification"
                if entry_decision_reason else
                f"{_ro_reason};minimum_quantity_verification"
            )
            _mqv_mult = float(_min_qty_verification.get("position_size_multiplier", 0.01) or 0.01)
            if _mqv_mult > 0.0:
                position_size_multiplier = max(float(position_size_multiplier), _mqv_mult)
            _ro_validation_reduce_applied = True
            print(
                f"[ENTRY_GATE] minimum quantity verification REDUCE propagated: "
                f"session={PAPER_SESSION_ID} reason={_ro_reason} position_size_multiplier={position_size_multiplier:.3f}"
            )
        if (
            _ro_val_enabled
            and _ro_val_mode in {"reduce", "validation_reduce"}
            and _ro_label_allowed
            and _ro_val_dd_condition
            and _ro_val_reasons_allowed
            and entry_decision_code != "BLOCK"
        ):
            entry_decision_code = "REDUCE"
            entry_decision_reason = f"{entry_decision_reason};{_ro_reason};validation_reduce" if entry_decision_reason else f"{_ro_reason};validation_reduce"
            _ro_probe_mult = max(0.0, min(1.0, _to_float(_ro_val_cfg.get("position_size_multiplier", 0.10), 0.10)))
            if _ro_probe_mult > 0.0:
                position_size_multiplier = max(float(position_size_multiplier), float(_ro_probe_mult))
            _ro_validation_reduce_applied = True
            print(
                f"[ENTRY_GATE] risk orchestration validation REDUCE propagated: "
                f"reason={_ro_reason} position_size_multiplier={position_size_multiplier:.3f}"
            )
        if not _ro_validation_reduce_applied and entry_decision_code != "BLOCK":
            entry_decision_code = "BLOCK"
            entry_decision_reason = f"{entry_decision_reason};{_ro_reason}" if entry_decision_reason else _ro_reason
            print(f"[ENTRY_GATE] risk orchestration BLOCK propagated: reason={_ro_reason}")

    entry_gate_block_lock = (entry_decision_code == "BLOCK")

    def _enforce_entry_gate_block_max_new(current_max_new: int, stage: str) -> int:
        if not entry_gate_block_lock:
            return int(current_max_new)
        if int(current_max_new) != 0:
            print(f"[ENTRY_GATE] BLOCK lock keeps max_new=0 at {stage}: {int(current_max_new)}->0")
        return 0

    _min_qty_verification2 = _minimum_quantity_verification_cfg(cfg)
    post_entry_sizing = _apply_post_entry_gate_sizing_adjustments(
        cfg=cfg,
        market_regime=str(market_regime or ""),
        entry_decision_code=str(entry_decision_code or ""),
        entry_decision_reason=str(entry_decision_reason or ""),
        risk_orch_ctx=risk_orch_ctx,
        position_size_multiplier=float(position_size_multiplier),
        capital_budget_enabled=bool(capital_budget_enabled),
        capital_budget_policy=capital_budget_policy,
        base_max_new=int(base_max_new),
        max_new=int(max_new),
        max_gross_exposure_pct=float(max_gross_exposure_pct),
        paper_validation_sample_cap_func=_paper_validation_sample_cap,
        minimum_quantity_verification_cfg=_min_qty_verification2,
    )
    max_new = int(post_entry_sizing.get("max_new", max_new) or 0)
    max_gross_exposure_pct = float(post_entry_sizing.get("max_gross_exposure_pct", max_gross_exposure_pct) or 0.0)
    _capture_max_new_zero("entry_gate_decision")
    fx_rally_caps = _apply_fx_and_rally_caps(
        fx_policy=fx_policy,
        fx_ctx=fx_ctx,
        fx_status=str(fx_status or ""),
        regime_policy=regime_policy,
        market_regime=str(market_regime or ""),
        max_new=int(max_new),
        max_gross_exposure_pct=float(max_gross_exposure_pct),
        max_daily_new_exposure_pct=float(max_daily_new_exposure_pct),
        max_per_sector_runtime=int(max_per_sector_runtime),
        gap_up_max_pct_runtime=float(gap_up_max_pct_runtime),
        entry_gap_down_stop_pct_runtime=float(entry_gap_down_stop_pct_runtime),
    )
    max_new = int(fx_rally_caps.get("max_new", max_new) or 0)
    max_gross_exposure_pct = float(fx_rally_caps.get("max_gross_exposure_pct", max_gross_exposure_pct) or 0.0)
    max_daily_new_exposure_pct = float(fx_rally_caps.get("max_daily_new_exposure_pct", max_daily_new_exposure_pct) or 0.0)
    max_per_sector_runtime = int(fx_rally_caps.get("max_per_sector_runtime", max_per_sector_runtime) or 0)
    gap_up_max_pct_runtime = float(fx_rally_caps.get("gap_up_max_pct_runtime", gap_up_max_pct_runtime) or 0.0)
    entry_gap_down_stop_pct_runtime = float(
        fx_rally_caps.get("entry_gap_down_stop_pct_runtime", entry_gap_down_stop_pct_runtime) or 0.0
    )

    cdf = pick_candidates(cfg)
    state = _merge_last_t2_state_fields(state)
    carryover_revalidate_summary: Dict[str, Any] = dict(_LAST_CARRYOVER_REVALIDATE_SUMMARY or {})
    rank_col = "final_score" if "final_score" in cdf.columns else ("score" if "score" in cdf.columns else None)
    if rank_col:
        print(f"[CAND] ranking key: {rank_col}")

    pool_state = load_state()
    pool_open_pos = pool_state.get("open_positions", []) if isinstance(pool_state, dict) else []
    pool_open_codes = {
        norm_code(p.get("code", ""))
        for p in pool_open_pos
        if isinstance(p, dict) and norm_code(p.get("code", ""))
    }
    _full_candidate_df_for_report = cdf.copy()
    cdf, validation_cand_count = _prepare_entry_candidate_pool(
        cdf,
        cfg,
        exclude_codes=pool_open_codes,
    )
    cdf, horizon_status = _apply_horizon_entry_policy(cdf, cfg)

    max_new_before_p1 = int(max_new)
    entry_candidates_before_p1 = int(len(cdf))
    split2_p1_mask = pd.Series(False, index=cdf.index)
    if "split_entry_2nd" in cdf.columns:
        split2_p1_mask = cdf["split_entry_2nd"].astype(str).str.strip().str.lower().isin({"1", "true", "t", "y", "yes"})
    cdf_split2_p1 = cdf[split2_p1_mask].copy()
    cdf_p1_base = cdf[~split2_p1_mask].copy()
    cdf_p1_base, max_new, p1_controls = apply_p1_entry_controls(cdf_p1_base, max_new, cfg)
    if len(cdf_split2_p1) > 0:
        cdf = _concat_drop_all_na_columns([cdf_split2_p1, cdf_p1_base], ignore_index=True)
        if isinstance(p1_controls, dict):
            p1_controls.setdefault("actions", []).append(f"split_entry_2nd:bypass_p1_new_entry_filters kept={len(cdf_split2_p1)}")
        print(f"[P1_GATE] split_entry_2nd bypass applied: kept={len(cdf_split2_p1)}")
    else:
        cdf = cdf_p1_base
    max_new = _enforce_entry_gate_block_max_new(max_new, "post_p1")
    sample_cap_post_p1, sample_cap_post_p1_reason = _paper_validation_sample_cap(max_new, entry_decision_code)
    if sample_cap_post_p1 > int(max_new):
        old_max_new = int(max_new)
        max_new = min(int(base_max_new), int(sample_cap_post_p1))
        if isinstance(p1_controls, dict):
            p1_controls.setdefault("actions", []).append(
                f"paper_validation_sample:post_p1_floor {old_max_new}->{int(max_new)} reason={sample_cap_post_p1_reason}"
            )
        print(
            f"[PAPER_VALIDATION_SAMPLE] post_p1 max_new floor {old_max_new}->{int(max_new)} "
            f"sample_cap={sample_cap_post_p1} reason={sample_cap_post_p1_reason}"
        )
    _capture_max_new_zero("p1_controls")
    entry_candidates_after_p1 = int(len(cdf))
    risk_gate_runtime = {
        "risk_off_enabled": bool(risk_off_enabled),
        "risk_off_reasons": list(risk_off_reasons or []),
        "risk_reason_details": list(risk_reason_details or []),
        "kill_switch": {
            "enabled": bool(ks_snap.get("enabled", False)),
            "triggered": bool(ks_snap.get("triggered", False)),
            "mode": str(ks_mode),
            "reduce_factor": float(ks_reduce_factor),
            "min_new_trades_per_day": int(ks_min_new),
            "limits": dict(ks_limits or {}),
            "metrics": dict((ks_snap.get("metrics") if isinstance(ks_snap.get("metrics"), dict) else {}) or {}),
        },
        "crash_risk_off": {
            "enabled": bool(cr_snap.get("enabled", False)),
            "triggered": bool(cr_snap.get("triggered", False)),
            "mode": str(crash_mode),
            "reduce_factor": float(crash_reduce_factor),
            "min_new_trades_per_day": int(crash_min_new),
            "limits": dict(cr_limits or {}),
            "metrics": dict((cr_snap.get("metrics") if isinstance(cr_snap.get("metrics"), dict) else {}) or {}),
        },
        "entry_gate": {
            "decision": str(entry_decision_code or ""),
            "reason": str(entry_decision_reason or ""),
            "block_lock": bool(entry_gate_block_lock),
            "max_new_before_p1": int(max_new_before_p1),
            "max_new_after_p1": int(max_new),
            "paper_validation_sample_cap": int(sample_cap_post_p1),
            "paper_validation_sample_reason": str(sample_cap_post_p1_reason or ""),
            "max_new_zero_reason": str(max_new_zero_reason or ""),
            "stop_new_orders": bool(int(max_new) <= 0),
        },
        "risk_orchestration": dict(risk_orch_ctx or {}),
        "production_risk_playbook": dict(production_risk_guard or {}),
        "position_size_multiplier": float(position_size_multiplier),
        "fail_closed": bool(entry_gate_block_lock and int(max_new) <= 0),
    }
    risk_gate_runtime["entry_risk_basis"] = _build_entry_risk_basis(risk_gate_runtime)
    if bool(entry_gate_block_lock and int(max_new) <= 0):
        print(
            "[FAIL_CLOSED] "
            f"post_p1_stop_new_orders=true max_new={int(max_new)} "
            f"reason={entry_decision_reason} zero_reason={max_new_zero_reason}"
        )
    _write_p1_gate_status(
        p1_controls,
        max_new_before_p1=max_new_before_p1,
        max_new_after_p1=int(max_new),
        entry_candidates_before_p1=entry_candidates_before_p1,
        entry_candidates_after_p1=entry_candidates_after_p1,
        market_regime=str(market_regime or ""),
        entry_decision_code=str(entry_decision_code or ""),
        entry_decision_reason=str(entry_decision_reason or ""),
        risk_gate_runtime=risk_gate_runtime,
    )
    if horizon_status:
        try:
            print(
                "[HORIZON] post-entry-pool "
                f"enabled={bool(horizon_status.get('enabled', False))} "
                f"before={int(horizon_status.get('before', 0) or 0)} "
                f"after={int(horizon_status.get('after', 0) or 0)} "
                f"allowed={int(horizon_status.get('allowed', 0) or 0)}"
            )
        except Exception:
            pass

    if bool(fx_policy.get("enabled", False)) and fx_ctx and bool(fx_policy.get("bias_keep_nonnegative_fx_score_only", True)):
        fx_level = _to_float(fx_ctx.get("level"), 0.0)
        try:
            weak_fx_level = float(fx_policy.get("weak_fx_export_bias_level", 1450.0) or 1450.0)
        except Exception:
            weak_fx_level = 1450.0
        try:
            strong_fx_level = float(fx_policy.get("strong_fx_domestic_bias_level", 1380.0) or 1380.0)
        except Exception:
            strong_fx_level = 1380.0
            if fx_level is not None and "fx_score" in cdf.columns:
                if fx_level >= weak_fx_level:
                    before = len(cdf)
                    cdf, max_new = apply_fx_filter(cdf, fx_status, max_new)
                    after = len(cdf)
                    print(f"[FX] weak-fx export bias filter applied: {before}->{after} level={fx_level:.2f} max_new={int(max_new)}")
                elif fx_level <= strong_fx_level:
                    before = len(cdf)
                    cdf, max_new = apply_fx_filter(cdf, fx_status, max_new)
                    after = len(cdf)
                    print(f"[FX] strong-fx domestic bias filter applied: {before}->{after} level={fx_level:.2f} max_new={int(max_new)}")
            max_new = _enforce_entry_gate_block_max_new(max_new, "post_fx_filter")
            _capture_max_new_zero("fx_filter")

    cdf, max_new, universe_shrink_candidates = _apply_runtime_caps_and_filters(
        cdf,
        max_new=int(max_new),
        base_max_new=int(base_max_new),
        market_regime=str(market_regime or ""),
        regime_info=regime_info,
        config=cfg,
        ops_policy=ops_policy,
        ops_enabled=bool(ops_enabled),
        risk_off_hard=bool(risk_off_hard),
        rank_col=rank_col,
        max_per_sector_runtime=int(max_per_sector_runtime),
        trend_overlay_ctx=trend_overlay_ctx,
    )
    max_new = _enforce_entry_gate_block_max_new(max_new, "post_runtime_caps")
    _capture_max_new_zero("runtime_caps")

    signal_tracking = _build_signal_tracking_context(
        cfg,
        schema=schema,
        stop_loss=stop_loss,
        take_profit=take_profit,
        trail_pct=trail_pct,
        fee_pct=float(fee_pct),
        slip_pct=float(slip_pct),
        sell_tax_pct=float(sell_tax_pct),
        replay_enabled=bool(replay_enabled),
        replay_min_age_days=int(replay_min_age_days),
        replay_require_no_open_positions=bool(replay_require_no_open_positions),
    )
    state = signal_tracking.state
    state = _merge_last_t2_state_fields(state)
    processed_signals = signal_tracking.processed_signals
    existing_fill_order_ids = signal_tracking.existing_fill_order_ids
    existing_trade_sigs = signal_tracking.existing_trade_sigs
    committed_source_order_ids = signal_tracking.committed_source_order_ids
    committed_signal_keys = signal_tracking.committed_signal_keys
    df_fills = signal_tracking.df_fills
    same_close_filled_today = int(signal_tracking.same_close_filled_today)
    open_pos = signal_tracking.open_pos
    open_codes = signal_tracking.open_codes
    open_slot_count = _count_open_position_slots(open_pos)
    replay_global_ok = bool(signal_tracking.replay_global_ok)
    d_ref_ymd = _derive_d_from_fills_path(FILLS)
    entry_today_ref_ymd = now_ymd()
    print(f"[ENTRY_DATE_REF] entry_today={entry_today_ref_ymd} fills_d_ref={d_ref_ymd}")
    _today_buy_code_counts: Dict[str, int] = {}
    _today_surge_buy_codes: set[str] = set()
    _today_surge_notional_krw = 0.0
    if df_fills is not None and not df_fills.empty:
        _f = df_fills
        _f_date_col = next((c for c in ["date", "exec_date", "datetime"] if c in _f.columns), None)
        _f_side_col = next((c for c in ["side", "buy_sell"] if c in _f.columns), None)
        _f_code_col = next((c for c in ["code", "ticker"] if c in _f.columns), None)
        if _f_date_col and _f_side_col and _f_code_col:
            _f_d = _f[_f_date_col].astype(str).str.replace(r"\D", "", regex=True).str[:8]
            _f_buy = _f[
                (_f_d == str(entry_today_ref_ymd))
                & (_f[_f_side_col].astype(str).str.upper().str.strip() == "BUY")
            ]
            _note_col = "note" if "note" in _f_buy.columns else None
            for _idx, _buy_row in _f_buy.iterrows():
                _c = str(_buy_row.get(_f_code_col, "") or "").strip().zfill(6)
                _today_buy_code_counts[_c] = _today_buy_code_counts.get(_c, 0) + 1
                _note_s = str(_buy_row.get(_note_col, "") if _note_col else "")
                if "surge_immediate=1" in _note_s:
                    _today_surge_buy_codes.add(_c)
                    _qty = _to_float(_buy_row.get("qty"), 0.0)
                    _price = _to_float(_buy_row.get("price"), 0.0)
                    if math.isfinite(float(_qty)) and math.isfinite(float(_price)) and _qty > 0 and _price > 0:
                        _today_surge_notional_krw += float(_qty) * float(_price)
    cdf, surge_inject_status = _inject_surge_immediate_candidates(
        cdf,
        cfg=cfg,
        intraday_realtime_mode=bool(intraday_realtime_mode),
        open_codes=set(open_codes),
        today_ymd=str(entry_today_ref_ymd),
        today_buy_code_counts=_today_buy_code_counts,
    )
    max_positions_precheck = _apply_max_positions_precheck(
        cfg=cfg,
        run_label=str(RUN_LABEL or ""),
        entry_decision_reason=str(entry_decision_reason or ""),
        open_slot_count=int(open_slot_count),
        max_positions=int(max_positions),
        max_new=int(max_new),
        max_positions_meta=max_positions_meta,
        capital_total=float(capital_total or 0.0),
        max_gross_exposure_pct=float(max_gross_exposure_pct),
        capital_budget_enabled=bool(capital_budget_enabled),
        open_pos=open_pos,
        open_codes=set(open_codes),
        load_prices_for_codes_func=load_prices_for_codes,
        compute_current_open_notional_func=_compute_current_open_notional,
    )
    max_new = int(max_positions_precheck.get("max_new", max_new) or 0)
    max_positions_meta = cast(Dict[str, Any], max_positions_precheck.get("max_positions_meta") or max_positions_meta)
    max_positions_override_allowed = bool(max_positions_precheck.get("max_positions_override_allowed", False))
    if max_positions_precheck.get("max_new_zero_stage"):
        _capture_max_new_zero(str(max_positions_precheck.get("max_new_zero_stage") or ""))
    if bool(surge_inject_status.get("enabled", False)):
        print(
            "[SURGE_IMMEDIATE] "
            f"applied={bool(surge_inject_status.get('applied', False))} "
            f"added={int(surge_inject_status.get('added', 0) or 0)} "
            f"updated={int(surge_inject_status.get('updated_existing', 0) or 0)} "
            f"codes={','.join(surge_inject_status.get('selected_codes', [])[:5]) or '-'} "
            f"reason={str(surge_inject_status.get('reason', ''))}"
        )
    _old_max_new_surge = int(max_new_surge)
    if exit_only_mode:
        max_new_surge = 0
        dynamic_max_new_surge_meta = {"enabled": False, "reason": "exit_only_mode"}
    else:
        max_new_surge, dynamic_max_new_surge_meta = _compute_dynamic_max_new_surge(
            cdf,
            cfg,
            capital_total=float(capital_total),
            fallback_max_new_surge=int(max_new_surge),
            existing_surge_count=int(len(_today_surge_buy_codes)),
            existing_surge_notional_krw=float(_today_surge_notional_krw),
        )
    if bool(dynamic_max_new_surge_meta.get("enabled", False)):
        print(
            "[SURGE_DYNAMIC_MAX_NEW] "
            f"max_new_surge={_old_max_new_surge}->{int(max_new_surge)} "
            f"eligible={dynamic_max_new_surge_meta.get('eligible_count', 0)} "
            f"medium={dynamic_max_new_surge_meta.get('medium_count', 0)} "
            f"strong={dynamic_max_new_surge_meta.get('strong_count', 0)} "
            f"budget_cap={dynamic_max_new_surge_meta.get('budget_cap', 0)} "
            f"budget_left_slots={dynamic_max_new_surge_meta.get('budget_slots_left', 0)} "
            f"quality_cap={dynamic_max_new_surge_meta.get('quality_cap', 0)} "
            f"existing={dynamic_max_new_surge_meta.get('existing_surge_count', 0)} "
            f"new_cap={dynamic_max_new_surge_meta.get('new_candidate_cap', 0)} "
            f"reason={dynamic_max_new_surge_meta.get('reason', '')}"
        )
    if (
        (not exit_only_mode)
        and bool(_ro_validation_reduce_applied)
        and int(max_new) > 0
        and int(max_new_surge) <= 0
    ):
        _surge_validation_floor = max(0, _to_int(_ro_val_cfg_for_surge_floor.get("max_new_surge", 0), 0))
        if "_surge_immediate" in cdf.columns:
            _surge_floor_candidates = int(pd.to_numeric(cdf["_surge_immediate"], errors="coerce").fillna(0).astype(int).sum())
        elif "surge_type" in cdf.columns:
            _surge_floor_candidates = int(cdf["surge_type"].astype(str).str.strip().ne("").sum())
        else:
            _surge_floor_candidates = 0
        if _surge_validation_floor > 0 and _surge_floor_candidates > 0:
            _old_floor_surge = int(max_new_surge)
            max_new_surge = max(int(max_new_surge), min(int(max_new), int(_surge_validation_floor)))
            dynamic_max_new_surge_meta["validation_reduce_floor_applied"] = True
            dynamic_max_new_surge_meta["validation_reduce_floor"] = int(_surge_validation_floor)
            dynamic_max_new_surge_meta["validation_reduce_floor_candidates"] = int(_surge_floor_candidates)
            print(
                "[SURGE_VALIDATION_REDUCE_FLOOR] "
                f"max_new_surge={_old_floor_surge}->{int(max_new_surge)} "
                f"floor={int(_surge_validation_floor)} candidates={int(_surge_floor_candidates)}"
            )

    cdf_for_ops_alert = cdf.copy()
    cdf = _apply_entry_selection_policy(
        cdf,
        cfg=cfg,
        rank_col=rank_col,
        max_new=int(max_new),
        exclude_codes=set(open_codes),
        same_code_day_buy_counts=_today_buy_code_counts,
    )
    if isinstance(cdf, pd.DataFrame) and not cdf.empty:
        cdf["_entry_gate_decision"] = str(entry_decision_code or "")
        cdf["_entry_gate_reason"] = str(entry_decision_reason or "")
        cdf["_risk_orch_scale"] = risk_orch_ctx.get("scale", "") if isinstance(risk_orch_ctx, dict) else ""
        _scale_zero_causes = risk_orch_ctx.get("scale_zero_causes", []) if isinstance(risk_orch_ctx, dict) else []
        cdf["_scale_zero_causes"] = ",".join(str(x) for x in _scale_zero_causes) if isinstance(_scale_zero_causes, list) else str(_scale_zero_causes or "")
    elif isinstance(cdf, pd.DataFrame) and cdf.empty:
        _write_entry_runtime_snapshots_and_reports(
            candidate_df=cdf,
            full_candidate_df_for_report=_full_candidate_df_for_report,
            entry_decisions=[],
            d_ref_ymd=str(d_ref_ymd),
            rank_col=str(rank_col or ""),
            risk_gate_runtime=(risk_gate_runtime if isinstance(risk_gate_runtime, dict) else {}),
            write_surge_realtime_shadow_runtime_snapshot=_write_surge_realtime_shadow_runtime_snapshot,
            trace_enabled=False,
        )

    pretrade_runtime = _prepare_pretrade_runtime(
        cdf,
        cfg,
        state,
        open_pos,
        open_codes,
        schema=schema,
        stop_loss=stop_loss,
        take_profit=take_profit,
        trail_pct=trail_pct,
        capital_total=float(capital_total),
        max_gross_exposure_pct=float(max_gross_exposure_pct),
        max_daily_new_exposure_pct=float(max_daily_new_exposure_pct),
        ddm_enabled=bool(ddm_enabled),
        ddm_cfg=ddm_cfg,
        ddm_force_liquidate_pct=float(ddm_force_liquidate_pct),
    )
    early_return = pretrade_runtime.get("early_return")
    if early_return is not None:
        return int(early_return)

    entry_sector_col = pretrade_runtime.get("entry_sector_col")
    block_same_sector_entry = bool(pretrade_runtime.get("block_same_sector_entry"))
    blocked_sector_value = pretrade_runtime.get("blocked_sector_value")
    sector_concentration = float(pretrade_runtime.get("sector_concentration", 0.0) or 0.0)
    gross_cap_krw = pretrade_runtime.get("gross_cap_krw")
    daily_new_cap_krw = pretrade_runtime.get("daily_new_cap_krw")
    px: pd.DataFrame = pretrade_runtime.get("px") if isinstance(pretrade_runtime.get("px"), pd.DataFrame) else pd.DataFrame()
    current_open_notional = float(pretrade_runtime.get("current_open_notional", 0.0) or 0.0)
    ddm_liquidation_targets = pretrade_runtime.get("ddm_liquidation_targets") or set()
    ddm_liquidation_price_mode = str(pretrade_runtime.get("ddm_liquidation_price_mode") or "close")

    price_universe_codes = int(px["code"].nunique()) if ("code" in px.columns and len(px) > 0) else 0
    shrink_min_price_codes = max(0, _to_int(ops_policy.get("universe_shrink_min_price_codes", 0), 0))
    if ops_enabled and shrink_min_price_codes > 0 and price_universe_codes > 0 and price_universe_codes <= shrink_min_price_codes:
        universe_shrink_candidates = True
        print(f"[OPS] universe_shrink detected: price_codes={price_universe_codes} <= {shrink_min_price_codes}")
        probe_floor2 = compute_dynamic_probe_floor(
            base_max_new=base_max_new,
            market_regime=market_regime,
            regime_info=regime_info,
            cfg=cfg,
            risk_off_hard=risk_off_hard,
            universe_shrink=True,
        )
        if probe_floor2 > 0 and max_new < probe_floor2:
            old_max_new = max_new
            max_new = min(base_max_new, probe_floor2)
            print(f"[OPS] dynamic_probe_floor(recheck) applied: {old_max_new}->{max_new}")
        max_new = _enforce_entry_gate_block_max_new(max_new, "post_ops_recheck")
        _capture_max_new_zero("ops_recheck")

    if exit_only_mode:
        max_new = 0
        max_new_surge = 0
        _capture_max_new_zero("exit_only_mode_final")
        print("[ENTRY_EXIT_ONLY] final entry lock -> max_new=0 max_new_surge=0")

    loop_state = _init_entry_loop_state(
        same_close_entry_mode=bool(same_close_entry_mode),
        same_close_filled_today=int(same_close_filled_today),
        ops_enabled=bool(ops_enabled),
        ops_policy=ops_policy,
        replay_recovery_summary=replay_recovery_summary,
        risk_off_enabled=bool(risk_off_enabled),
        market_regime=str(market_regime or ""),
        d_ref_ymd=str(entry_today_ref_ymd),
        portfolio_state=state,
        p0_snapshot=p0_snapshot,
    )
    loop_state["entry_decision_code"] = str(entry_decision_code or "")
    loop_state["entry_decision_reason"] = str(entry_decision_reason or "")
    loop_state["_same_code_day_buy_counts"] = dict(_today_buy_code_counts)
    loop_state["surge_new_count"] = int(len(_today_surge_buy_codes))
    loop_state["surge_notional_krw"] = float(_today_surge_notional_krw)
    if _today_surge_buy_codes:
        print(
            "[SURGE_DAILY_CAP_SEED] "
            f"existing_surge_codes={int(len(_today_surge_buy_codes))} "
            f"existing_surge_notional={float(_today_surge_notional_krw):.0f}"
        )
    if int(max_new) <= 0 and len(cdf) > 0:
        print(
            f"[ENTRY_CAP_ZERO] candidates={len(cdf)} reason={max_new_zero_reason or 'unknown'} "
            f"surge_cap={int(max_new_surge)}"
        )
    fills_new = loop_state["fills_new"]
    trades_new = loop_state["trades_new"]
    new_count = int(loop_state["new_count"])
    new_notional_krw = float(loop_state["new_notional_krw"])
    surge_new_count = int(loop_state.get("surge_new_count", 0))
    surge_notional_krw = float(loop_state.get("surge_notional_krw", 0.0))
    split_notional_krw = float(loop_state.get("split_notional_krw", 0.0))
    evaluated_count = int(loop_state["evaluated_count"])
    no_next_day_count = int(loop_state["no_next_day_count"])
    entry_ready_count = int(loop_state["entry_ready_count"])
    cap_block_count = int(loop_state["cap_block_count"])
    processed_skip_count = int(loop_state["processed_skip_count"])
    idempotent_skip_count = int(loop_state.get("idempotent_skip_count", 0))
    stale_replay_used_count = int(loop_state["stale_replay_used_count"])
    open_order_replay_used_count = int(loop_state["open_order_replay_used_count"])
    today_ymd = str(loop_state["today_ymd"])
    pending_carry_rows = loop_state["pending_carry_rows"]
    carry_max_age = int(loop_state["carry_max_age"])
    replay_due_today_count = int(loop_state["replay_due_today_count"])
    carryover_market_gate_block = bool(loop_state["carryover_market_gate_block"])
    loop_result = _process_entry_rows(
        cdf,
        max_new=int(max_new),
        max_new_surge=int(max_new_surge),
        capital_total=float(capital_total),
        max_positions=int(max_positions),
        schema=str(schema),
        config=cfg,
        prices_df=px,
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
        open_pos=open_pos,
        open_codes=open_codes,
        max_positions_override_allowed=bool(max_positions_override_allowed),
        loop_state=loop_state,
    )
    entry_loop = _normalize_entry_loop_result(
        loop_result,
        fallback_portfolio_state=state,
        fallback_t2_cash_checks=t2_cash_checks,
    )
    fills_new = entry_loop["fills_new"]
    trades_new = entry_loop["trades_new"]
    new_count = int(entry_loop["new_count"])
    new_notional_krw = float(entry_loop["new_notional_krw"])
    surge_new_count = int(entry_loop["surge_new_count"])
    surge_notional_krw = float(entry_loop["surge_notional_krw"])
    split_notional_krw = float(entry_loop["split_notional_krw"])
    evaluated_count = int(entry_loop["evaluated_count"])
    no_next_day_count = int(entry_loop["no_next_day_count"])
    entry_ready_count = int(entry_loop["entry_ready_count"])
    cap_block_count = int(entry_loop["cap_block_count"])
    processed_skip_count = int(entry_loop["processed_skip_count"])
    max_new_skip_count = int(entry_loop["max_new_skip_count"])
    idempotent_skip_count = int(entry_loop["idempotent_skip_count"])
    stale_replay_used_count = int(entry_loop["stale_replay_used_count"])
    open_order_replay_used_count = int(entry_loop["open_order_replay_used_count"])
    max_positions_blocked = bool(entry_loop["max_positions_blocked"])
    today_ymd = str(entry_loop["today_ymd"])
    pending_carry_rows = entry_loop["pending_carry_rows"]
    state = cast(Dict[str, Any], entry_loop["portfolio_state"])
    t2_cash_checks = cast(List[Dict[str, Any]], entry_loop["t2_cash_checks"])
    entry_decisions = entry_loop["entry_decisions"]
    _print_entry_fill_summary_v2(loop_result)
    _write_entry_runtime_snapshots_and_reports(
        candidate_df=cdf,
        full_candidate_df_for_report=_full_candidate_df_for_report,
        entry_decisions=entry_decisions,
        d_ref_ymd=str(d_ref_ymd),
        rank_col=str(rank_col or ""),
        risk_gate_runtime=(risk_gate_runtime if isinstance(risk_gate_runtime, dict) else {}),
        write_surge_realtime_shadow_runtime_snapshot=_write_surge_realtime_shadow_runtime_snapshot,
    )
    ops_runtime = _build_entry_runtime_ops_summary(
        fills_new=fills_new,
        schema=str(schema),
        ops_enabled=bool(ops_enabled),
        ops_policy=ops_policy,
        pending_carry_rows=pending_carry_rows,
        carry_max_age=int(carry_max_age),
        px=px,
        market_regime=str(market_regime or ""),
        regime_info=regime_info,
        config=cfg,
        max_new=int(max_new),
        max_new_zero_reason=str(max_new_zero_reason or ""),
        candidate_df=cdf,
        price_universe_codes=int(price_universe_codes),
        evaluated_count=int(evaluated_count),
        entry_ready_count=int(entry_ready_count),
        new_count=int(new_count),
        no_next_day_count=int(no_next_day_count),
        cap_block_count=int(cap_block_count),
        processed_skip_count=int(processed_skip_count),
        max_new_skip_count=int(max_new_skip_count),
        idempotent_skip_count=int(idempotent_skip_count),
        stale_replay_used_count=int(stale_replay_used_count),
        open_order_replay_used_count=int(open_order_replay_used_count),
        entry_decisions=(entry_decisions if isinstance(entry_decisions, list) else []),
        entry_candidate_df=cdf_for_ops_alert,
        universe_shrink_candidates=bool(universe_shrink_candidates),
        open_slot_count=int(open_slot_count),
        max_positions=int(max_positions),
        max_positions_meta=max_positions_meta,
        replay_enabled=bool(replay_enabled),
    )
    entry_fill_rows_runtime = int(ops_runtime["entry_fill_rows_runtime"])
    expected_min_fills = int(ops_runtime["expected_min_fills"])
    ops_alert = dict(ops_runtime.get("ops_alert") or {})
    replay_sync_result = _run_replay_runtime_refresh(
        ops_enabled=bool(ops_enabled),
        carry_max_age=int(carry_max_age),
        open_order_replay_used_count=int(open_order_replay_used_count),
        replay_recovery_summary=replay_recovery_summary,
        replay_queue_scan=replay_queue_scan,
        replay_quarantine_status=replay_quarantine_status,
        replay_prune_status=replay_prune_status,
        replay_consistency_status=replay_consistency_status,
        replay_consistency_remediation=replay_consistency_remediation,
        recovered_open_pos=recovered_open_pos,
        recovery_status_doc=recovery_status_doc,
    )
    replay_summary_sync_status = replay_sync_result["replay_summary_sync_status"]
    replay_consistency_status = replay_sync_result["replay_consistency_status"]
    replay_consistency_remediation = replay_sync_result["replay_consistency_remediation"]
    replay_recovery_summary = replay_sync_result["replay_recovery_summary"]
    replay_queue_scan = replay_sync_result["replay_queue_scan"]
    replay_quarantine_status = replay_sync_result["replay_quarantine_status"]
    replay_prune_status = replay_sync_result["replay_prune_status"]
    replay_queue_status = replay_sync_result["replay_queue_status"]
    recovery_status_doc = replay_sync_result["recovery_status_doc"]
    position_result = _run_open_positions_rebalance_runtime(
        config=cfg,
        schema=str(schema),
        prices_df=px,
        open_pos=open_pos,
        fundamentals_db=fundamentals_db,
        sector_db=sector_db,
        max_hold_days=int(max_hold_days),
        sell_rules=sell_rules,
        sell_rules_enabled=bool(sell_rules_enabled),
        ddm_liquidation_targets=ddm_liquidation_targets,
        ddm_liquidation_price_mode=str(ddm_liquidation_price_mode),
        ddm_action=ddm_action,
        fee_pct=float(fee_pct),
        slip_pct=float(slip_pct),
        sell_tax_pct=float(sell_tax_pct),
        fills_new=fills_new,
        trades_new=trades_new,
        existing_fill_order_ids=existing_fill_order_ids,
        existing_trade_sigs=existing_trade_sigs,
        committed_source_order_ids=committed_source_order_ids,
        next_seq_start=int(state.get("next_trade_seq", 1)),
        stop_loss=float(stop_loss),
        take_profit=take_profit,
        trail_pct=trail_pct,
        vix_proxy=vix_proxy,
        fx_ctx=fx_ctx,
        macro_snapshot=macro_snapshot,
        p0_snapshot=p0_snapshot,
        market_regime=str(market_regime or ""),
        candidate_df=cdf,
    )
    still_open = position_result["still_open"]
    next_seq = int(position_result["next_seq"])
    residual_runtime = _run_intraday_residual_overnight_runtime(
        config=cfg,
        schema=str(schema),
        prices_df=px,
        still_open=still_open,
        runtime_ymd=str(today_ymd),
        fee_pct=float(fee_pct),
        slip_pct=float(slip_pct),
        sell_tax_pct=float(sell_tax_pct),
        fills_new=fills_new,
        trades_new=trades_new,
        existing_fill_order_ids=existing_fill_order_ids,
        existing_trade_sigs=existing_trade_sigs,
        next_seq_start=int(next_seq),
    )
    residual_guard_shadow = residual_runtime["residual_guard_shadow"]
    residual_guard_exit = residual_runtime["residual_guard_exit"]
    still_open = residual_runtime["still_open"]
    next_seq = int(residual_runtime["next_seq"])

    if (not exit_only_mode) and max_positions_blocked and max_positions > 0 and len(still_open) < max_positions and len(cdf) > 0 and new_count < max_new:
        slots_after_exit = max(0, int(max_positions) - int(len(still_open)))
        print(
            f"[ENTRY_RECHECK_AFTER_EXIT] triggered=1 slots_after_exit={slots_after_exit} "
            f"open_after_exit={len(still_open)} max_positions={max_positions}"
        )
        current_open_notional = _compute_current_open_notional(still_open, px)
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
            "portfolio_state": state,
            "t2_cash_checks": t2_cash_checks,
        }
        recheck_result = _process_entry_rows(
            cdf,
            max_new=int(max_new),
            max_new_surge=int(max_new_surge),
            capital_total=float(capital_total),
            max_positions=int(max_positions),
            schema=str(schema),
            config=cfg,
            prices_df=px,
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
            fallback_portfolio_state=state,
            fallback_t2_cash_checks=t2_cash_checks,
        )
        fills_new = recheck_loop["fills_new"]
        trades_new = recheck_loop["trades_new"]
        new_count = int(recheck_loop["new_count"])
        new_notional_krw = float(recheck_loop["new_notional_krw"])
        surge_new_count = int(recheck_loop["surge_new_count"])
        surge_notional_krw = float(recheck_loop["surge_notional_krw"])
        split_notional_krw = float(recheck_loop["split_notional_krw"])
        evaluated_count = int(recheck_loop["evaluated_count"])
        no_next_day_count = int(recheck_loop["no_next_day_count"])
        entry_ready_count = int(recheck_loop["entry_ready_count"])
        cap_block_count = int(recheck_loop["cap_block_count"])
        processed_skip_count = int(recheck_loop["processed_skip_count"])
        idempotent_skip_count = int(recheck_loop["idempotent_skip_count"])
        stale_replay_used_count = int(recheck_loop["stale_replay_used_count"])
        open_order_replay_used_count = int(recheck_loop["open_order_replay_used_count"])
        pending_carry_rows = recheck_loop["pending_carry_rows"]
        state = cast(Dict[str, Any], recheck_loop["portfolio_state"])
        t2_cash_checks = cast(List[Dict[str, Any]], recheck_loop["t2_cash_checks"])
        print(
            f"[ENTRY_RECHECK_AFTER_EXIT] result new_count={new_count} "
            f"entry_ready={entry_ready_count} open_positions={len(still_open)}"
        )

    fills_header = LEGACY_FILLS_HEADER if schema == "legacy" else V411_FILLS_HEADER
    trades_header = LEGACY_TRADES_HEADER if schema == "legacy" else V411_TRADES_HEADER
    write_txn_result = _append_fills_trades_with_rollback(
        fills_path=FILLS,
        trades_path=TRADES,
        fills_new=fills_new,
        trades_new=trades_new,
        schema=str(schema),
        fills_header=fills_header,
        trades_header=trades_header,
        prices_df=px,
        write_dashboard_compat_csv=_write_dashboard_compat_csv,
    )
    fills_new = cast(List[List[Any]], write_txn_result.get("fills_new", fills_new))
    if schema == "v41.1" and bool(write_txn_result.get("dashboard_compat_updated", False)):
        print(f"[DASHBOARD_COMPAT] legacy copies updated: {FILLS.parent / 'fills_dashboard_compat.csv'}, "
              f"{TRADES.parent / 'trades_dashboard_compat.csv'}")
    if int(write_txn_result.get("return_code", 0) or 0) != 0:
        return 2
    written_finalize = _finalize_written_fills_runtime_state(
        schema=str(schema),
        fills_path=FILLS,
        fills_new=fills_new,
        still_open=still_open,
        stop_loss=float(stop_loss),
        take_profit=take_profit,
        trail_pct=trail_pct,
        ops_alert=ops_alert,
        prices_df=px,
        current_open_notional=float(current_open_notional),
        gross_cap_krw=gross_cap_krw,
        ops_enabled=bool(ops_enabled),
    )
    still_open = cast(List[Dict[str, Any]], written_finalize["still_open"])
    ops_alert = dict(written_finalize.get("ops_alert") or {})
    current_open_notional = float(written_finalize.get("current_open_notional", current_open_notional))
    log_pipeline_event(
        stage="paper_engine_main",
        batch_label="[7/9]",
        event="END",
        date=_audit_ymd,
        output_files={
            "fills": {"path": str(FILLS), "new_rows": int(len(fills_new)), "total_rows": count_rows(FILLS)},
            "trades": {"path": str(TRADES), "new_rows": int(len(trades_new)), "total_rows": count_rows(TRADES)},
        },
        metrics={
            "fills_new": int(len(fills_new)),
            "trades_new": int(len(trades_new)),
            "ops_alert_filled": int(ops_alert.get("filled", 0) or 0),
            "schema": schema,
        },
        status="PASS",
    )

    _record_sell_pending_and_write_t2_status(
        state=state,
        cfg=cfg,
        schema=str(schema),
        fills_new=fills_new,
        runtime_ymd=now_ymd(),
        t2_cash_checks=t2_cash_checks,
    )

    _finalize_paper_engine_runtime(
        maybe_run_pnl_report_func=maybe_run_pnl_report,
        summary_schema=str(schema),
        fills_new=fills_new,
        trades_new=trades_new,
        still_open=still_open,
        stop_loss=float(stop_loss),
        take_profit=take_profit,
        trail_pct=trail_pct,
        persist_kwargs={
            "state": state,
            "still_open": still_open,
            "next_seq": int(next_seq),
            "processed_signals": processed_signals,
            "carry_max_age": int(carry_max_age),
            "ops_enabled": bool(ops_enabled),
            "schema": str(schema),
            "fills_new": fills_new,
            "trades_new": trades_new,
            "initial_open_count": int(len(open_pos)),
            "replay_recovery_summary": replay_recovery_summary,
            "replay_due_today_count": int(replay_due_today_count),
            "recovery_summary": recovery_summary,
            "replay_queue_status": replay_queue_status,
            "replay_consistency_status": replay_consistency_status,
            "ddm_enabled": bool(ddm_enabled),
            "ddm_action": ddm_action,
            "ddm_force_liquidate_pct": float(ddm_force_liquidate_pct),
            "ddm_liquidation_targets": ddm_liquidation_targets,
            "open_pos": open_pos,
            "stop_loss": float(stop_loss),
            "take_profit": take_profit,
            "trail_pct": trail_pct,
            "recovery_status_doc": recovery_status_doc,
            "market_regime": str(market_regime or ""),
            "max_new": int(max_new),
            "max_new_surge": int(max_new_surge),
            "max_new_zero_reason": str(max_new_zero_reason or ""),
            "candidate_df": cdf,
            "entry_decisions": (entry_decisions if isinstance(entry_decisions, list) else []),
            "surge_inject_status": surge_inject_status,
            "entry_ready_count": int(entry_ready_count),
            "no_next_day_count": int(no_next_day_count),
            "open_order_replay_used_count": int(open_order_replay_used_count),
            "replay_regen_result": replay_regen_result,
            "replay_queue_scan": replay_queue_scan,
            "replay_quarantine_status": replay_quarantine_status,
            "replay_prune_status": replay_prune_status,
            "replay_consistency_remediation": replay_consistency_remediation,
            "replay_summary_sync_status": replay_summary_sync_status,
            "carryover_revalidate_summary": carryover_revalidate_summary,
            "runtime_ymd": str(today_ymd),
            "ops_alert": ops_alert,
            "capital_total": float(capital_total),
            "current_open_notional": float(current_open_notional),
            "gross_cap_krw": gross_cap_krw,
            "daily_new_cap_krw": daily_new_cap_krw,
            "max_positions_meta": max_positions_meta,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
