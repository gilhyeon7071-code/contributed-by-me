"""Risk orchestration helpers for paper_engine.

Split out from the legacy ``paper_engine.py`` as part of the risk module
refactor (Phase 3).
"""

from __future__ import annotations


__all__ = [
    '_risk_orch_pct01',
    '_risk_orch_recent_trade_stats',
    '_build_initial_sizing_context',
    '_compute_risk_orch_scale',
]

from pathlib import Path
from typing import Any, Dict, List

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
