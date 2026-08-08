"""Risk orchestration helpers for paper_engine.

Split out from the legacy ``paper_engine.py`` as part of the risk module
refactor (Phase 3).
"""

from __future__ import annotations


__all__ = [
    '_risk_orch_pct01',
    '_risk_orch_recent_trade_stats',
    '_build_initial_sizing_context',
    '_apply_post_entry_gate_sizing_adjustments',
    '_apply_fx_and_rally_caps',
    '_apply_max_positions_precheck',
    '_compute_risk_orch_scale',
]

from pathlib import Path
from typing import Any, Callable, Dict, List

import pandas as pd

from paper_engine.common import _pct01_from_config, _to_float, _to_int, _get_dict
from paper_engine.drawdown import _ddm_extract_vix_proxy, _ddm_pct01, _ddm_to_float
from utils.common import read_csv_safe

def _risk_orch_pct01(v: Any, default: float) -> float:
    try:
        x = float(v)
    except Exception:
        x = float(default)
    if x > 1.0 and x <= 100.0:
        x = x / 100.0
    return max(0.0, min(1.0, x))


def _risk_orch_recent_trade_stats(trades_path: Path, lookback_trades: int) -> Dict[str, float]:
    try:
        df = read_csv_safe(trades_path)
    except Exception:
        return {"edge": 0.0, "variance": 0.0, "est_vol": 0.0}
    if df is None or len(df) == 0:
        return {"edge": 0.0, "variance": 0.0, "est_vol": 0.0}
    ret_col = "net_ret" if "net_ret" in df.columns else ("pnl_pct" if "pnl_pct" in df.columns else None)
    if not ret_col:
        return {"edge": 0.0, "variance": 0.0, "est_vol": 0.0}
    dd = pd.to_numeric(df[ret_col], errors="coerce").dropna()
    if len(dd) <= 1:
        return {"edge": 0.0, "variance": 0.0, "est_vol": 0.0}
    lookback = max(10, int(lookback_trades or 60))
    ss = dd.tail(lookback)
    edge = float(ss.mean()) if len(ss) > 0 else 0.0
    var = float(ss.var(ddof=0)) if len(ss) > 1 else 0.0
    est_vol = float(ss.std(ddof=0)) if len(ss) > 1 else 0.0
    alpha = 0.10
    try:
        q = float(ss.quantile(alpha))
    except Exception:
        q = 0.0
    tail = ss[ss <= q]
    es = float(tail.mean()) if len(tail) > 0 else 0.0
    return {"edge": edge, "variance": max(var, 0.0), "est_vol": max(est_vol, 0.0), "es": es, "es_alpha": alpha}


def _build_initial_sizing_context(
    *,
    cfg: Dict[str, Any],
    p0_snapshot: Dict[str, Any],
    macro_snapshot: Dict[str, Any],
    log_dir: Path,
    ddm_cfg: Dict[str, Any],
    consecutive_loss_days: int,
    ddm_exposure_cap: Any,
    max_new: int,
    max_positions_meta: Dict[str, Any],
) -> Dict[str, Any]:
    capital_total = float(cfg.get("capital_total", 0) or 0)
    capital_total_configured = float(capital_total)
    account_equity_for_allocation = None
    try:
        _p0_metrics = ((p0_snapshot.get("kill_switch") or {}).get("metrics") or {}) if isinstance(p0_snapshot, dict) else {}
        _p0_account = _p0_metrics.get("account_basis") if isinstance(_p0_metrics, dict) else None
        if isinstance(_p0_account, dict) and _p0_account.get("status") == "PASS":
            account_equity_for_allocation = _to_float(_p0_account.get("equity_est"), 0.0)
            if account_equity_for_allocation > 0.0 and capital_total_configured > 0.0:
                capital_total = min(capital_total_configured, float(account_equity_for_allocation))
                print(
                    f"[CAPITAL_BASIS] configured={capital_total_configured:.0f} "
                    f"account_equity={float(account_equity_for_allocation):.0f} "
                    f"effective={capital_total:.0f} basis=min(configured,account_equity)"
                )
    except Exception as e:
        print(f"[CAPITAL_BASIS][WARN] account_equity_unavailable={type(e).__name__}:{e}")

    max_positions_meta = dict(max_positions_meta or {})
    max_positions_meta["capital_basis"] = {
        "configured_capital_total": float(capital_total_configured),
        "account_equity": (None if account_equity_for_allocation is None else float(account_equity_for_allocation)),
        "effective_capital_total": float(capital_total),
        "basis": "min(configured_capital_total,account_equity)" if account_equity_for_allocation else "configured_capital_total",
    }
    max_gross_exposure_pct = _risk_orch_pct01(cfg.get("max_gross_exposure_pct", 1.0), 1.0)
    max_daily_new_exposure_pct = _risk_orch_pct01(cfg.get("max_daily_new_exposure_pct", 1.0), 1.0)
    capital_budget_policy = cfg.get("capital_budget_policy", {}) if isinstance(cfg.get("capital_budget_policy"), dict) else {}
    capital_budget_enabled = bool(capital_budget_policy.get("enabled", False))
    if capital_budget_enabled:
        budget_gross_cap = max(
            0.0,
            min(1.0, _pct01_from_config(capital_budget_policy.get("gross_exposure_pct", 0.55), 0.55)),
        )
        old_exp = max_gross_exposure_pct
        max_gross_exposure_pct = min(max_gross_exposure_pct, budget_gross_cap)
        print(
            f"[BUDGET_POLICY] enabled=true gross_exposure {old_exp:.3f}->{max_gross_exposure_pct:.3f} "
            f"basic={_pct01_from_config(capital_budget_policy.get('basic_alloc_pct', 0.40), 0.40):.3f} "
            f"surge={_pct01_from_config(capital_budget_policy.get('surge_alloc_pct', 0.06), 0.06):.3f} "
            f"split={_pct01_from_config(capital_budget_policy.get('split_alloc_pct', 0.18), 0.18):.3f} "
            f"recovery={_pct01_from_config(capital_budget_policy.get('recovery_alloc_pct', 0.10), 0.10):.3f} "
            f"reserve={_pct01_from_config(capital_budget_policy.get('reserve_alloc_pct', 0.20), 0.20):.3f}"
        )
    macro_exposure_mult = _risk_orch_pct01((macro_snapshot or {}).get("exposure_multiplier", 1.0), 1.0)
    if macro_exposure_mult < 1.0:
        old_exp = max_gross_exposure_pct
        max_gross_exposure_pct = min(max_gross_exposure_pct, max_gross_exposure_pct * macro_exposure_mult)
        print(f"[MACRO] exposure_multiplier={macro_exposure_mult:.3f} -> gross_exposure {old_exp:.3f}->{max_gross_exposure_pct:.3f}")

    def _apply_budget_defensive_floor(old_exp: float, current_exp: float, current_max_new: int) -> tuple[float, int]:
        if not capital_budget_enabled:
            return float(current_exp), int(current_max_new)
        defensive_floor = max(
            0.0,
            min(
                1.0,
                _pct01_from_config(
                    capital_budget_policy.get("defensive_floor_exposure_pct", 0.45),
                    0.45,
                ),
            ),
        )
        if 0.0 < defensive_floor < old_exp and current_exp < defensive_floor:
            current_exp = old_exp
            defensive_max_new = max(0, _to_int(capital_budget_policy.get("defensive_max_new", 1), 1))
            if defensive_max_new > 0:
                old_max_new = int(current_max_new)
                current_max_new = min(int(current_max_new), defensive_max_new)
                print(
                    f"[BUDGET_DDM_BAND] floor={defensive_floor:.3f} cap={current_exp:.3f} "
                    f"defensive_max_new={defensive_max_new} max_new {old_max_new}->{current_max_new}"
                )
        return float(current_exp), int(current_max_new)

    cons_loss_thr = int(_ddm_to_float(ddm_cfg.get("consecutive_loss_days_threshold", 3), 3))
    cons_loss_mult = _ddm_pct01(ddm_cfg.get("consecutive_loss_exposure_multiplier", 0.5), 0.5)
    if consecutive_loss_days >= max(1, cons_loss_thr):
        old_exp = max_gross_exposure_pct
        max_gross_exposure_pct = max(0.0, min(1.0, max_gross_exposure_pct * cons_loss_mult))
        max_gross_exposure_pct, max_new = _apply_budget_defensive_floor(old_exp, max_gross_exposure_pct, int(max_new))
        print(f"[DDM] consecutive_loss_days={consecutive_loss_days} >= {cons_loss_thr} -> gross_exposure {old_exp:.3f}->{max_gross_exposure_pct:.3f}")

    if ddm_exposure_cap is not None:
        old_exp = max_gross_exposure_pct
        max_gross_exposure_pct = min(max_gross_exposure_pct, _ddm_pct01(ddm_exposure_cap, max_gross_exposure_pct))
        max_gross_exposure_pct, max_new = _apply_budget_defensive_floor(old_exp, max_gross_exposure_pct, int(max_new))
        print(f"[DDM] stage exposure cap applied: {old_exp:.3f}->{max_gross_exposure_pct:.3f}")

    position_size_multiplier = 1.0
    vix_proxy = _ddm_extract_vix_proxy(macro_snapshot, log_dir)
    vix_thr = _ddm_to_float(ddm_cfg.get("vix_proxy_threshold", 30.0), 30.0)
    high_vol_mult = _ddm_pct01(ddm_cfg.get("high_vol_position_size_multiplier", 0.7), 0.7)
    if vix_proxy is not None and vix_proxy > vix_thr:
        position_size_multiplier = min(position_size_multiplier, high_vol_mult)
        print(f"[DDM] vix_proxy={vix_proxy:.2f} > {vix_thr:.2f} -> position_size_multiplier={position_size_multiplier:.2f}")

    return {
        "capital_total": float(capital_total),
        "capital_total_configured": float(capital_total_configured),
        "account_equity_for_allocation": account_equity_for_allocation,
        "max_positions_meta": max_positions_meta,
        "max_gross_exposure_pct": float(max_gross_exposure_pct),
        "max_daily_new_exposure_pct": float(max_daily_new_exposure_pct),
        "capital_budget_policy": capital_budget_policy,
        "capital_budget_enabled": bool(capital_budget_enabled),
        "max_new": int(max_new),
        "position_size_multiplier": float(position_size_multiplier),
        "vix_proxy": vix_proxy,
    }


def _apply_post_entry_gate_sizing_adjustments(
    *,
    cfg: Dict[str, Any],
    market_regime: str,
    entry_decision_code: str,
    entry_decision_reason: str,
    risk_orch_ctx: Dict[str, Any],
    position_size_multiplier: float,
    capital_budget_enabled: bool,
    capital_budget_policy: Dict[str, Any],
    base_max_new: int,
    max_new: int,
    max_gross_exposure_pct: float,
    paper_validation_sample_cap_func: Callable[[int, str], tuple[int, str]],
    minimum_quantity_verification_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    decision_code = str(entry_decision_code or "").upper()
    decision_reason = str(entry_decision_reason or "")
    if decision_code == "BLOCK":
        if max_new != 0:
            print(f"[ENTRY_GATE] BLOCK -> force max_new=0 ({decision_reason})")
        max_new = 0
        print(
            "[FAIL_CLOSED] "
            f"entry_gate_block_propagated=true stop_new_orders=true max_new={int(max_new)} "
            f"reason={decision_reason}"
        )
    elif decision_code == "CAUTION":
        old_max_new = max_new
        sample_cap, sample_cap_reason = paper_validation_sample_cap_func(1, decision_code)
        max_new = min(base_max_new, max(max_new, sample_cap))
        old_exp = max_gross_exposure_pct
        caution_gross_cap = 0.50
        if capital_budget_enabled:
            caution_gross_cap = max(
                0.0,
                min(
                    1.0,
                    _pct01_from_config(
                        capital_budget_policy.get("caution_gross_exposure_pct", 0.55),
                        0.55,
                    ),
                ),
            )
        max_gross_exposure_pct = min(max_gross_exposure_pct, caution_gross_cap)
        print(
            f"[ENTRY_GATE] CAUTION -> max_new {old_max_new}->{max_new}, "
            f"gross_exposure {old_exp:.3f}->{max_gross_exposure_pct:.3f} "
            f"sample_cap={sample_cap} sample_reason={sample_cap_reason}"
        )
    elif decision_code == "REDUCE":
        old_max_new = max_new
        sample_cap, sample_cap_reason = paper_validation_sample_cap_func(1, decision_code)
        max_new = min(base_max_new, max(max_new, sample_cap))
        print(f"[ENTRY_GATE] REDUCE -> max_new {old_max_new}->{max_new} sample_cap={sample_cap} sample_reason={sample_cap_reason}")

    bear_sizing_policy = cfg.get("bear_sizing_policy", {}) if isinstance(cfg.get("bear_sizing_policy"), dict) else {}
    if bool(bear_sizing_policy.get("enabled", False)):
        allowed_regimes = {
            str(x).strip().upper()
            for x in (bear_sizing_policy.get("market_regimes") or ["BEAR"])
            if str(x).strip()
        }
        allowed_decisions = {
            str(x).strip().upper()
            for x in (bear_sizing_policy.get("entry_gate_decisions") or ["CAUTION"])
            if str(x).strip()
        }
        ro_scale_for_bear = max(0.0, min(1.0, _to_float(risk_orch_ctx.get("scale", position_size_multiplier), position_size_multiplier)))
        max_ro_scale = max(0.0, min(1.0, _to_float(bear_sizing_policy.get("max_risk_orch_scale", 0.50), 0.50)))
        if (
            str(market_regime or "").strip().upper() in allowed_regimes
            and decision_code in allowed_decisions
            and ro_scale_for_bear <= max_ro_scale
        ):
            old_exp = max_gross_exposure_pct
            bear_gross_cap = max(0.0, min(1.0, _pct01_from_config(bear_sizing_policy.get("gross_exposure_cap_pct", 0.40), 0.40)))
            max_gross_exposure_pct = min(max_gross_exposure_pct, bear_gross_cap)
            print(
                f"[BEAR_SIZING] applied=true gross_exposure {old_exp:.3f}->{max_gross_exposure_pct:.3f} "
                f"cap={bear_gross_cap:.3f} regime={market_regime} entry_gate={decision_code} "
                f"risk_orch_scale={ro_scale_for_bear:.3f} max_scale={max_ro_scale:.3f}"
            )
        else:
            print(
                f"[BEAR_SIZING] applied=false regime={market_regime} entry_gate={decision_code} "
                f"risk_orch_scale={ro_scale_for_bear:.3f} max_scale={max_ro_scale:.3f}"
            )
    if (
        bool(((cfg.get("risk_orchestration", {}) if isinstance(cfg, dict) else {}).get("dd_stop_validation", {}) or {}).get("enabled", False))
        and "validation_reduce" in decision_reason
        and decision_code == "REDUCE"
    ):
        _ro_val_cfg2 = ((cfg.get("risk_orchestration", {}) if isinstance(cfg, dict) else {}).get("dd_stop_validation", {}) or {})
        _ro_val_max_new = max(1, _to_int(_ro_val_cfg2.get("max_new", 1), 1))
        if max_new < _ro_val_max_new:
            old_max_new = int(max_new)
            max_new = min(base_max_new, _ro_val_max_new)
            print(f"[ENTRY_GATE] validation REDUCE floor max_new {old_max_new}->{max_new}")
    if (
        bool(minimum_quantity_verification_cfg.get("enabled", False))
        and "minimum_quantity_verification" in decision_reason
        and decision_code == "REDUCE"
    ):
        _mqv_max_new = max(1, _to_int(minimum_quantity_verification_cfg.get("max_new", 1), 1))
        if max_new < _mqv_max_new:
            old_max_new = int(max_new)
            max_new = min(base_max_new, _mqv_max_new)
            print(f"[ENTRY_GATE] minimum quantity verification floor max_new {old_max_new}->{max_new}")
    return {
        "max_new": int(max_new),
        "max_gross_exposure_pct": float(max_gross_exposure_pct),
    }


def _apply_fx_and_rally_caps(
    *,
    fx_policy: Dict[str, Any],
    fx_ctx: Dict[str, Any],
    fx_status: str,
    regime_policy: Dict[str, Any],
    market_regime: str,
    max_new: int,
    max_gross_exposure_pct: float,
    max_daily_new_exposure_pct: float,
    max_per_sector_runtime: int,
    gap_up_max_pct_runtime: float,
    entry_gap_down_stop_pct_runtime: float,
) -> Dict[str, Any]:
    if bool(fx_policy.get("enabled", False)) and fx_ctx:
        fx_vol_band = str(fx_ctx.get("volatility_band") or "").upper()
        fx_three_day_extreme = bool(fx_ctx.get("three_day_extreme"))
        fx_daily_abs_change = _to_float(fx_ctx.get("daily_abs_change"), 0.0)
        try:
            weak_fx_level = float(fx_policy.get("weak_fx_export_bias_level", 1450.0) or 1450.0)
        except Exception:
            weak_fx_level = 1450.0
        try:
            strong_fx_level = float(fx_policy.get("strong_fx_domestic_bias_level", 1380.0) or 1380.0)
        except Exception:
            strong_fx_level = 1380.0
        _ = (weak_fx_level, strong_fx_level)
        if fx_status == "EXTREME_HARD":
            print(
                f"[FX] EXTREME_HARD: daily_abs_change={fx_daily_abs_change:.2f} "
                f"avg_abs_change_20={fx_ctx.get('avg_abs_change_20')}"
            )
        elif fx_status == "EXTREME_SOFT":
            print(
                f"[FX] EXTREME_SOFT: daily_abs_change={fx_daily_abs_change:.2f} "
                f"avg_abs_change_20={fx_ctx.get('avg_abs_change_20')}"
            )
        elif fx_status == "CAUTION":
            print(
                f"[FX] CAUTION: daily_abs_change={fx_daily_abs_change:.2f} "
                f"avg_abs_change_20={fx_ctx.get('avg_abs_change_20')}"
            )
        elif fx_vol_band == "HIGH":
            try:
                high_vol_cap = int(fx_policy.get("high_vol_reduce_max_new_to", 1) or 1)
            except Exception:
                high_vol_cap = 1
            old_max_new = max_new
            max_new = min(max_new, max(0, high_vol_cap))
            print(f"[FX] high volatility cap applied: {old_max_new}->{max_new}")
        if bool(fx_policy.get("three_day_extreme_force_defensive", True)) and fx_three_day_extreme:
            old_exp = max_gross_exposure_pct
            max_gross_exposure_pct = min(max_gross_exposure_pct, 0.35)
            print(f"[FX] three_day_extreme defensive gross cap: {old_exp:.3f}->{max_gross_exposure_pct:.3f}")

    if bool(regime_policy.get("enabled", False)) and market_regime == "RALLY":
        max_gross_exposure_pct = min(
            max_gross_exposure_pct,
            _risk_orch_pct01(regime_policy.get("rally_max_gross_exposure_pct", max_gross_exposure_pct), max_gross_exposure_pct),
        )
        max_daily_new_exposure_pct = min(
            max_daily_new_exposure_pct,
            _risk_orch_pct01(regime_policy.get("rally_max_daily_new_exposure_pct", max_daily_new_exposure_pct), max_daily_new_exposure_pct),
        )
        try:
            rally_mps = int(regime_policy.get("rally_max_per_sector", max_per_sector_runtime) or max_per_sector_runtime)
            if rally_mps > 0:
                if max_per_sector_runtime > 0:
                    max_per_sector_runtime = min(max_per_sector_runtime, rally_mps)
                else:
                    max_per_sector_runtime = rally_mps
        except Exception:
            pass

        try:
            rally_gap_up = float(regime_policy.get("rally_gap_up_max_pct", gap_up_max_pct_runtime) or gap_up_max_pct_runtime)
            if rally_gap_up > 0:
                if gap_up_max_pct_runtime > 0:
                    gap_up_max_pct_runtime = min(gap_up_max_pct_runtime, rally_gap_up)
                else:
                    gap_up_max_pct_runtime = rally_gap_up
        except Exception:
            pass

        try:
            rally_gap_down = float(regime_policy.get("rally_entry_gap_down_stop_pct", entry_gap_down_stop_pct_runtime) or entry_gap_down_stop_pct_runtime)
            if rally_gap_down > 0:
                entry_gap_down_stop_pct_runtime = max(entry_gap_down_stop_pct_runtime, abs(rally_gap_down))
        except Exception:
            pass

        print(
            f"[REGIME] RALLY caps -> gross={max_gross_exposure_pct:.3f} daily={max_daily_new_exposure_pct:.3f} "
            f"max_per_sector={max_per_sector_runtime} gap_up={gap_up_max_pct_runtime:.3f} "
            f"gap_down_stop={entry_gap_down_stop_pct_runtime:.3f}"
        )

    return {
        "max_new": int(max_new),
        "max_gross_exposure_pct": float(max_gross_exposure_pct),
        "max_daily_new_exposure_pct": float(max_daily_new_exposure_pct),
        "max_per_sector_runtime": int(max_per_sector_runtime),
        "gap_up_max_pct_runtime": float(gap_up_max_pct_runtime),
        "entry_gap_down_stop_pct_runtime": float(entry_gap_down_stop_pct_runtime),
    }


def _apply_max_positions_precheck(
    *,
    cfg: Dict[str, Any],
    run_label: str,
    entry_decision_reason: str,
    open_slot_count: int,
    max_positions: int,
    max_new: int,
    max_positions_meta: Dict[str, Any],
    capital_total: float,
    max_gross_exposure_pct: float,
    capital_budget_enabled: bool,
    open_pos: List[Dict[str, Any]],
    open_codes: set[str],
    load_prices_for_codes_func: Callable[[Dict[str, Any], List[str]], pd.DataFrame],
    compute_current_open_notional_func: Callable[[List[Dict[str, Any]], pd.DataFrame], float],
) -> Dict[str, Any]:
    max_positions_meta = dict(max_positions_meta or {})
    max_new_zero_stage = ""
    if max_positions > 0 and int(open_slot_count) >= int(max_positions):
        mp_override_meta: Dict[str, Any] = {
            "enabled": False,
            "applied": False,
            "mode": "",
            "reason": "disabled",
            "open_notional": None,
            "gross_cap_krw": None,
            "capital_total": float(capital_total or 0.0),
        }
        ro_val_cfg = ((cfg.get("risk_orchestration", {}) if isinstance(cfg, dict) else {}).get("dd_stop_validation", {}) or {})
        mp_override_cfg = ro_val_cfg.get("max_positions_full_override", {}) if isinstance(ro_val_cfg, dict) else {}
        if not isinstance(mp_override_cfg, dict):
            mp_override_cfg = {}
        mp_override_enabled = bool(mp_override_cfg.get("enabled", False))
        mp_override_mode = str(mp_override_cfg.get("mode", "gross_exposure_cap") or "gross_exposure_cap").strip().lower()
        allowed_labels = {
            str(x).strip().lower()
            for x in (ro_val_cfg.get("allowed_run_labels") or [])
            if str(x).strip()
        }
        label_allowed = (not allowed_labels) or str(run_label or "").strip().lower() in allowed_labels
        is_validation_reduce = (
            bool(ro_val_cfg.get("enabled", False))
            and str(ro_val_cfg.get("mode", "") or "").strip().lower() in {"reduce", "validation_reduce"}
            and label_allowed
            and "validation_reduce" in str(entry_decision_reason or "")
        )
        allow_max_positions_override = False
        if mp_override_enabled and is_validation_reduce and mp_override_mode == "gross_exposure_cap":
            try:
                mp_px = load_prices_for_codes_func(cfg, sorted(set(open_codes)))
                mp_price_ok = (mp_px is not None) and (not mp_px.empty)
                mp_open_notional = compute_current_open_notional_func(open_pos, mp_px)
            except Exception:
                mp_price_ok = False
                mp_open_notional = 0.0
            mp_gross_cap = (
                float(capital_total) * float(max_gross_exposure_pct)
                if float(capital_total or 0.0) > 0.0
                else 0.0
            )
            mp_reason = "gross_exposure_under_cap"
            if not mp_price_ok:
                mp_reason = "price_unavailable"
            elif mp_gross_cap <= 0:
                mp_reason = "gross_cap_missing"
            elif mp_open_notional >= mp_gross_cap:
                mp_reason = "gross_exposure_cap_reached"
            mp_override_meta.update(
                {
                    "enabled": True,
                    "mode": mp_override_mode,
                    "reason": mp_reason,
                    "price_ok": bool(mp_price_ok),
                    "open_notional": float(mp_open_notional),
                    "gross_cap_krw": float(mp_gross_cap),
                    "gross_exposure_pct": (
                        float(mp_open_notional) / float(capital_total)
                        if float(capital_total or 0.0) > 0.0
                        else None
                    ),
                    "max_gross_exposure_pct": float(max_gross_exposure_pct),
                }
            )
            if mp_price_ok and mp_gross_cap > 0 and mp_open_notional < mp_gross_cap:
                allow_max_positions_override = True
                mp_override_meta["applied"] = True
        elif capital_budget_enabled:
            try:
                mp_px = load_prices_for_codes_func(cfg, sorted(set(open_codes)))
                mp_price_ok = (mp_px is not None) and (not mp_px.empty)
                mp_open_notional = compute_current_open_notional_func(open_pos, mp_px)
            except Exception:
                mp_price_ok = False
                mp_open_notional = 0.0
            mp_gross_cap = (
                float(capital_total) * float(max_gross_exposure_pct)
                if float(capital_total or 0.0) > 0.0
                else 0.0
            )
            mp_reason = "gross_exposure_under_cap"
            if not mp_price_ok:
                mp_reason = "price_unavailable"
            elif mp_gross_cap <= 0:
                mp_reason = "gross_cap_missing"
            elif mp_open_notional >= mp_gross_cap:
                mp_reason = "gross_exposure_cap_reached"
            mp_override_meta.update(
                {
                    "enabled": True,
                    "mode": "budget_gross_exposure_cap",
                    "reason": mp_reason,
                    "price_ok": bool(mp_price_ok),
                    "open_notional": float(mp_open_notional),
                    "gross_cap_krw": float(mp_gross_cap),
                    "gross_exposure_pct": (
                        float(mp_open_notional) / float(capital_total)
                        if float(capital_total or 0.0) > 0.0
                        else None
                    ),
                    "max_gross_exposure_pct": float(max_gross_exposure_pct),
                }
            )
            if mp_price_ok and mp_gross_cap > 0 and mp_open_notional < mp_gross_cap:
                allow_max_positions_override = True
                mp_override_meta["applied"] = True
        elif mp_override_enabled:
            mp_override_meta.update(
                {
                    "enabled": True,
                    "mode": mp_override_mode,
                    "reason": "not_validation_reduce",
                }
            )
        max_positions_meta["validation_exposure_override"] = mp_override_meta
        if allow_max_positions_override:
            print(
                f"[ENTRY_MAX_POSITIONS_VALIDATION_OVERRIDE] open_slots={int(open_slot_count)} "
                f">= max_positions={int(max_positions)} but open_notional={float(mp_override_meta.get('open_notional') or 0.0):.0f} "
                f"< gross_cap={float(mp_override_meta.get('gross_cap_krw') or 0.0):.0f} -> keep max_new={int(max_new)}"
            )
        else:
            if int(max_new) > 0:
                print(
                    f"[ENTRY_MAX_POSITIONS_PRECHECK] open_slots={int(open_slot_count)} "
                    f">= max_positions={int(max_positions)} -> max_new {int(max_new)}->0"
                )
            max_new = 0
            max_new_zero_stage = "max_positions_full"
    max_positions_override_allowed = bool(
        _get_dict(max_positions_meta, "validation_exposure_override").get("applied", False)
    )
    return {
        "max_new": int(max_new),
        "max_positions_meta": max_positions_meta,
        "max_positions_override_allowed": bool(max_positions_override_allowed),
        "max_new_zero_stage": str(max_new_zero_stage or ""),
    }


def _compute_risk_orch_scale(
    cfg: Dict[str, Any],
    *,
    market_regime: str,
    p0_snapshot: Dict[str, Any],
    fee_pct: float,
    slip_pct: float,
    sell_tax_pct: float,
    trades_path: Path,
) -> Dict[str, Any]:
    ro_cfg = cfg.get("risk_orchestration", {}) if isinstance(cfg, dict) else {}
    if not isinstance(ro_cfg, dict) or (not bool(ro_cfg.get("enabled", False))):
        return {"enabled": False, "scale": 1.0, "reason": "disabled"}
    lb = max(10, _to_int(ro_cfg.get("lookback_trades", 60), 60))
    stat = _risk_orch_recent_trade_stats(trades_path, lb)
    edge = float(stat.get("edge", 0.0) or 0.0)
    variance = float(stat.get("variance", 0.0) or 0.0)
    est_vol = float(stat.get("est_vol", 0.0) or 0.0)
    es = float(stat.get("es", 0.0) or 0.0)
    es_alpha = max(0.01, min(0.50, _to_float(stat.get("es_alpha", 0.10), 0.10)))
    target_vol = max(1e-6, _to_float(ro_cfg.get("target_vol", 0.03), 0.03))
    if variance > 0:
        f_kelly_raw = edge / variance
    else:
        f_kelly_raw = 0.0
    f_kelly = max(0.0, min(float(_to_float(ro_cfg.get("f_kelly_cap", 1.5), 1.5)), float(f_kelly_raw)))
    kelly_role = str(ro_cfg.get("kelly_role", "sizing") or "sizing").strip().lower()
    if kelly_role not in {"sizing", "confirm_only"}:
        kelly_role = "sizing"
    conf_map = ro_cfg.get("regime_confidence", {}) if isinstance(ro_cfg.get("regime_confidence"), dict) else {}
    regime_key = str(market_regime or "NORMAL").upper()
    c_default = _to_float(ro_cfg.get("c_default", 0.35), 0.35)
    c_min = _to_float(ro_cfg.get("c_min", 0.25), 0.25)
    c_max = _to_float(ro_cfg.get("c_max", 0.50), 0.50)
    c = _to_float(conf_map.get(regime_key, c_default), c_default)
    c = max(c_min, min(c_max, c))
    vol_ratio = min(float(_to_float(ro_cfg.get("vol_ratio_cap", 3.0), 3.0)), target_vol / max(est_vol, 1e-6))
    base_scale = max(0.0, c * f_kelly * vol_ratio)
    tc_est = max(0.0, float(fee_pct) + float(slip_pct) + float(sell_tax_pct))
    if edge > 0:
        tc_adj = edge / max(edge + tc_est, 1e-9)
        base_scale *= max(0.0, min(1.0, tc_adj))
    else:
        _edge_zero_floor = max(0.0, _to_float(ro_cfg.get("edge_zero_floor_scale", 0.0), 0.0))
        base_scale = _edge_zero_floor
    scale_after_kelly_tc = float(base_scale)
    max_scale = max(0.0, _to_float(ro_cfg.get("max_scale", 1.0), 1.0))
    scale = min(base_scale, max_scale)
    dcfg = ro_cfg.get("dd_taper", {}) if isinstance(ro_cfg.get("dd_taper"), dict) else {}
    dd_cap = abs(_to_float(dcfg.get("dd_cap", 0.10), 0.10))
    dd_stop = abs(_to_float(dcfg.get("dd_stop", 0.15), 0.15))
    dd_taper_mode = str(dcfg.get("mode", "actuate") or "actuate").strip().lower()
    dd_advisory_only = dd_taper_mode in {"advisory", "confirm_only", "ddm_owner"}
    kill = (p0_snapshot.get("kill_switch") if isinstance(p0_snapshot.get("kill_switch"), dict) else {}) or {}
    kmet = (kill.get("metrics") if isinstance(kill.get("metrics"), dict) else {}) or {}
    strategy_basis = (kmet.get("strategy_basis") if isinstance(kmet.get("strategy_basis"), dict) else {}) or {}
    account_basis = (kmet.get("account_basis") if isinstance(kmet.get("account_basis"), dict) else {}) or {}
    strategy_dd = abs(_to_float(strategy_basis.get("max_drawdown_pct", kmet.get("max_drawdown_pct", 0.0)), 0.0))
    account_dd_raw = None
    if str(account_basis.get("status") or "").upper() == "PASS" and account_basis.get("max_drawdown_pct") is not None:
        account_dd_raw = abs(_to_float(account_basis.get("max_drawdown_pct", 0.0), 0.0))
    kill_cfg = cfg.get("kill_switch", {}) if isinstance(cfg.get("kill_switch"), dict) else {}
    account_dd_limit = abs(_to_float(kill_cfg.get("max_drawdown_pct", dd_stop), dd_stop))
    account_daily_limit = abs(_to_float(kill_cfg.get("max_daily_loss_pct", 0.0), 0.0))
    account_daily_loss = abs(min(0.0, _to_float(account_basis.get("daily_loss_pct", 0.0), 0.0)))
    account_risk_clear = bool(
        str(account_basis.get("status") or "").upper() == "PASS"
        and account_dd_raw is not None
        and (account_dd_limit <= 0.0 or float(account_dd_raw) < float(account_dd_limit))
        and (account_daily_limit <= 0.0 or float(account_daily_loss) < float(account_daily_limit))
        and (not bool(kill.get("triggered", False)))
    )
    account_clear_dd_override_cfg = _get_dict(dcfg, "account_clear_strategy_dd_override")
    account_clear_dd_override_enabled = bool(account_clear_dd_override_cfg.get("enabled", False))
    account_clear_dd_override_applied = bool(
        account_clear_dd_override_enabled
        and dd_advisory_only
        and account_risk_clear
        and account_dd_raw is not None
    )
    if account_clear_dd_override_applied and account_dd_raw is not None:
        cur_dd = float(account_dd_raw)
        dd_basis = "account_basis_strategy_dd_override"
    else:
        cur_dd = float(strategy_dd)
        dd_basis = "strategy_basis"
    dd_stop_triggered = False
    dd_taper_triggered = False
    if dd_stop > 0 and cur_dd >= dd_stop:
        dd_stop_triggered = True
        if not dd_advisory_only:
            scale = 0.0
    elif dd_stop > dd_cap and cur_dd >= dd_cap:
        dd_taper_triggered = True
        if not dd_advisory_only:
            taper = (dd_stop - cur_dd) / max(dd_stop - dd_cap, 1e-9)
            scale *= max(0.0, min(1.0, taper))
    scale_after_dd = float(scale)
    es_cfg = ro_cfg.get("es_gate", {}) if isinstance(ro_cfg.get("es_gate"), dict) else {}
    es_cfg_eff = dict(es_cfg)
    es_by_regime = es_cfg.get("by_regime", {}) if isinstance(es_cfg.get("by_regime"), dict) else {}
    if isinstance(es_by_regime.get(regime_key), dict):
        for _k, _v in es_by_regime.get(regime_key, {}).items():
            es_cfg_eff[_k] = _v
    es_limit = abs(_to_float(es_cfg_eff.get("limit", 0.05), 0.05))
    es_enabled = bool(es_cfg_eff.get("enabled", True))
    es_reduction = _risk_orch_pct01(es_cfg_eff.get("reduction_factor", 0.5), 0.5)
    es_hard_block = bool(es_cfg_eff.get("hard_block", False))
    es_triggered = False
    if es_enabled and es_limit > 0:
        if abs(es) >= es_limit:
            es_triggered = True
            if es_hard_block:
                scale = 0.0
            else:
                scale *= es_reduction
    scale_before_account_clear_floor = float(scale)
    account_clear_floor_cfg = ro_cfg.get("account_clear_min_scale", {}) if isinstance(ro_cfg.get("account_clear_min_scale"), dict) else {}
    account_clear_floor_enabled = bool(account_clear_floor_cfg.get("enabled", False))
    account_clear_floor = max(0.0, _to_float(account_clear_floor_cfg.get("min_scale", 0.0), 0.0))
    account_clear_floor_requires_dd_advisory = bool(account_clear_floor_cfg.get("require_dd_advisory_only", True))
    account_clear_floor_applied = False
    if (
        account_clear_floor_enabled
        and account_risk_clear
        and account_clear_floor > 0.0
        and scale > 0.0
        and scale < account_clear_floor
        and not (es_triggered and es_hard_block)
        and ((not account_clear_floor_requires_dd_advisory) or dd_advisory_only)
    ):
        scale = min(max_scale, account_clear_floor)
        account_clear_floor_applied = True
    scale = max(0.0, min(max_scale, scale))
    risk_advisory_flags: List[str] = []
    scale_zero_causes: List[str] = []
    if float(f_kelly) <= 0.0:
        if kelly_role == "confirm_only":
            risk_advisory_flags.append("kelly_zero")
        else:
            scale_zero_causes.append("kelly_zero")
    if dd_stop_triggered:
        if dd_advisory_only:
            risk_advisory_flags.append("dd_stop")
        else:
            scale_zero_causes.append("dd_stop")
    if es_triggered and es_hard_block:
        scale_zero_causes.append("es_hard_block")
    return {
        "enabled": True,
        "scale": float(scale),
        "edge": float(edge),
        "variance": float(variance),
        "est_vol": float(est_vol),
        "es": float(es),
        "es_alpha": float(es_alpha),
        "target_vol": float(target_vol),
        "f_kelly_raw": float(f_kelly_raw),
        "f_kelly": float(f_kelly),
        "kelly_role": kelly_role,
        "c": float(c),
        "tc_est": float(tc_est),
        "scale_after_kelly_tc": float(scale_after_kelly_tc),
        "dd_current": float(cur_dd),
        "dd_basis": dd_basis,
        "strategy_dd_current": float(strategy_dd),
        "account_dd_current": (None if account_dd_raw is None else float(account_dd_raw)),
        "account_risk_clear": bool(account_risk_clear),
        "account_dd_limit": float(account_dd_limit),
        "account_daily_loss": float(account_daily_loss),
        "account_daily_loss_limit": float(account_daily_limit),
        "dd_cap": float(dd_cap),
        "dd_stop": float(dd_stop),
        "dd_taper_mode": dd_taper_mode,
        "dd_advisory_only": bool(dd_advisory_only),
        "account_clear_strategy_dd_override_enabled": bool(account_clear_dd_override_enabled),
        "account_clear_strategy_dd_override_applied": bool(account_clear_dd_override_applied),
        "dd_stop_triggered": bool(dd_stop_triggered),
        "dd_taper_triggered": bool(dd_taper_triggered),
        "scale_after_dd": float(scale_after_dd),
        "scale_before_account_clear_floor": float(scale_before_account_clear_floor),
        "account_clear_min_scale_enabled": bool(account_clear_floor_enabled),
        "account_clear_min_scale": float(account_clear_floor),
        "account_clear_min_scale_applied": bool(account_clear_floor_applied),
        "es_limit": float(es_limit),
        "es_triggered": bool(es_triggered),
        "es_hard_block": bool(es_hard_block),
        "scale_zero_causes": scale_zero_causes,
        "risk_advisory_flags": risk_advisory_flags,
        "es_reduction_factor": float(es_reduction),
        "es_cfg_mode": ("regime_override" if isinstance(es_by_regime.get(regime_key), dict) else "default"),
        "regime": regime_key,
        "lookback_trades": int(lb),
    }
