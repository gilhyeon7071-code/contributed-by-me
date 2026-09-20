"""Drawdown management helpers for paper_engine.

Split out from the legacy ``paper_engine.py`` as part of the risk/drawdown
module refactor (Phase 3).
"""

from __future__ import annotations


__all__ = [
    'DrawdownAction',
    '_ddm_to_float',
    '_ddm_pct01',
    'calc_max_new',
    '_ddm_extract_vix_proxy',
    '_ddm_is_hard_block',
    '_ddm_select_action',
    '_ddm_forced_sell_ratio_pct',
    '_apply_drawdown_entry_capacity',
    '_ddm_count_consecutive_loss_days',
    '_ddm_pos_key',
    '_ddm_select_liquidation_targets',
    '_ddm_sector_concentration',
]

import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from paper_engine.common import _get_dict
from utils.common import norm_code, read_csv_safe


@dataclass
class DrawdownAction:
    current_mdd_abs: float
    stage_idx: int
    threshold: float
    new_entry_allowed_pct: float
    liquidate_weakest_pct: float
    max_exposure: Optional[float]
    metric_basis: str = ""
    metric_details: Optional[Dict[str, Any]] = None


STAGE_MIN_NEW = {0: 1, 1: 1, 2: 1, 3: 0, 4: 0}


def _ddm_to_float(v: Any, d: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(d)


def _ddm_pct01(v: Any, d: float) -> float:
    x = _ddm_to_float(v, d)
    if x > 1.0 and x <= 100.0:
        x = x / 100.0
    return max(0.0, min(1.0, x))


def calc_max_new(base_max_new: int, entry_pct: float, stage: int) -> int:
    entry_pct_clamped = max(0.0, min(1.0, float(entry_pct)))
    raw = math.floor(max(0, int(base_max_new)) * entry_pct_clamped)
    min_allowed = int(STAGE_MIN_NEW.get(int(stage), 0))
    if entry_pct_clamped <= 0.0:
        min_allowed = 0
    result = max(min_allowed, raw)
    print(
        f"[DDM] max_new calc: base={int(base_max_new)} "
        f"* entry_pct={entry_pct_clamped:.2f} "
        f"= raw={int(raw)} "
        f"-> min_floor={int(min_allowed)} "
        f"-> final={int(result)} "
        f"(stage={int(stage)})"
    )
    return int(result)


def _ddm_extract_vix_proxy(macro_snapshot: Dict[str, Any], log_dir: Path) -> Optional[float]:
    mm = _get_dict(macro_snapshot, "market_metrics")
    if "vix" in mm:
        try:
            return _ddm_to_float(mm.get("vix"), 0.0)
        except Exception:
            pass
    try:
        p = log_dir / "macro_feature_external_latest.json"
        if not p.exists():
            return None
        j = json.loads(p.read_text(encoding="utf-8"))
        us = (((j.get("signals") or {}).get("us") or {}).get("raw") or {}) if isinstance(j, dict) else {}
        if isinstance(us, dict) and ("vix" in us):
            return _ddm_to_float(us.get("vix"), 0.0)
        s_vix = (((j.get("series") or {}).get("VIXCLS") or {}).get("latest_value")) if isinstance(j, dict) else None
        if s_vix is not None:
            return float(s_vix)
    except Exception:
        return None
    return None


def _ddm_is_hard_block(mdd_abs: float, context: Dict[str, Any], hard_cfg: Dict[str, Any]) -> bool:
    mdd_threshold = _ddm_pct01(hard_cfg.get("mdd_threshold"), 0.36)
    consecutive_loss_days_threshold = max(1, int(_ddm_to_float(hard_cfg.get("consecutive_loss_days"), 5)))
    require_system_stress = bool(hard_cfg.get("require_system_stress", True))

    consecutive_loss_days = int(_ddm_to_float(context.get("consecutive_loss_days"), 0))
    kill_switch_active = bool(context.get("kill_switch_active", False))
    risk_off = bool(context.get("risk_off", False))
    gate_daily = str(context.get("gate_daily") or "").upper()

    mdd_critical = float(mdd_abs) >= float(mdd_threshold)
    loss_streak = consecutive_loss_days >= consecutive_loss_days_threshold
    system_stress = kill_switch_active or risk_off
    market_block = gate_daily not in {"", "PASS"}

    if require_system_stress:
        return bool(mdd_critical and (loss_streak or system_stress or market_block))
    return bool(mdd_critical and (loss_streak or market_block))


def _ddm_select_action(cfg: Dict[str, Any], p0_snapshot: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> DrawdownAction:
    dm = cfg.get("drawdown_manager") if isinstance(cfg, dict) else {}
    if not isinstance(dm, dict):
        dm = {}
    ctx = context if isinstance(context, dict) else {}

    ks = _get_dict(p0_snapshot, "kill_switch")
    metrics = _get_dict(ks, "metrics")
    account_basis = _get_dict(metrics, "account_basis")
    strategy_basis = _get_dict(metrics, "strategy_basis")
    dd_rolling = _ddm_pct01(abs(_ddm_to_float(metrics.get("max_drawdown_pct"), 0.0)), 0.0)
    dd_lifetime = _ddm_pct01(abs(_ddm_to_float(metrics.get("debug_lifetime_max_drawdown_pct"), 0.0)), 0.0)
    use_lifetime = bool(dm.get("use_debug_lifetime_mdd", False))
    metric_mode = str(metrics.get("mode") or "")
    has_rolling_metric = metric_mode.startswith("rolling") and ("max_drawdown_pct" in metrics)
    # [2026-09-09 사용자 결정] MDD SSOT 를 두 갈래로 분리했다.
    #   Account Risk SSOT      account_equity drawdown  -> DDM stage / 신규진입 capacity
    #   Strategy Health Observer  rolling_weighted_mean_60 -> 관측·경고·전략 검증 전용.
    #                             **DDM 직접 차단에는 사용하지 않는다.**
    # 왜 폴백을 끊는가: 2026-05-27 감사(ddm_risk_policy_value_source_audit)가 잰 대로
    #   전략 계열 정의 5종은 전부 Stage4_HARD_BLOCK(진입 0%)로 가고 account_equity 는 비발동이다.
    #   중간이 없으므로 "account 가 없을 때 rolling 으로 대신한다" 는 대체가 아니라
    #   **다른 정책으로의 조용한 전환**이다. 그래서 대체하지 않고 막는다(fail-closed).
    #   rolling/lifetime 값은 metric_details 에 그대로 남겨 관측 용도로 쓴다.
    account_unavailable = False
    if str(account_basis.get("status") or "").upper() == "PASS" and account_basis.get("max_drawdown_pct") is not None:
        current_mdd_abs = _ddm_pct01(abs(_ddm_to_float(account_basis.get("max_drawdown_pct"), 0.0)), 0.0)
        metric_basis = "account_equity"
    else:
        # Account Risk SSOT 가 없다. 위험을 모르는 상태이므로 신규 진입을 열지 않는다.
        current_mdd_abs = 0.0
        metric_basis = "account_equity_unavailable"
        account_unavailable = True
    metric_details = {
        "basis": metric_basis,
        "account_basis": account_basis,
        "strategy_basis": strategy_basis,
        # 아래 두 값은 **관측 전용**이다 (Strategy Health Observer). 차단 판정에 쓰지 않는다.
        "strategy_rolling_mdd_abs": dd_rolling,
        "strategy_lifetime_mdd_abs": dd_lifetime,
        "strategy_metrics_role": "observer_only_not_used_for_blocking",
        "has_rolling_metric": bool(has_rolling_metric),
        "use_debug_lifetime_mdd": bool(use_lifetime),
        "account_unavailable": bool(account_unavailable),
    }

    default_action = DrawdownAction(
        current_mdd_abs=current_mdd_abs,
        stage_idx=-1,
        threshold=0.0,
        new_entry_allowed_pct=1.0,
        liquidate_weakest_pct=0.0,
        max_exposure=None,
        metric_basis=metric_basis,
        metric_details=metric_details,
    )

    if account_unavailable:
        # Account Risk SSOT 부재 = 위험 미상. 전략 계열 지표로 대체하지 않고 신규 진입만 닫는다.
        #   강제 청산은 하지 않는다 - 값을 모르는 상태에서 파는 것은 판단이 아니라 추측이다.
        return DrawdownAction(
            current_mdd_abs=0.0,
            stage_idx=-2,
            threshold=0.0,
            new_entry_allowed_pct=0.0,
            liquidate_weakest_pct=0.0,
            max_exposure=0.0,
            metric_basis=metric_basis,
            metric_details=metric_details,
        )

    raw_stages = dm.get("stages")
    if not isinstance(raw_stages, list) or (not raw_stages):
        return default_action

    stages: List[Dict[str, float]] = []
    for idx, row in enumerate(raw_stages):
        if not isinstance(row, dict):
            continue
        stages.append({
            "mdd": _ddm_pct01(row.get("mdd"), 0.0),
            "new_entry_allowed_pct": _ddm_pct01(row.get("new_entry_allowed_pct"), 1.0),
            "liquidate_weakest_pct": _ddm_pct01(row.get("liquidate_weakest_pct"), 0.0),
            "max_exposure": _ddm_pct01(row.get("max_exposure"), 1.0),
            "_source_idx": float(idx),
        })
    if not stages:
        return default_action
    stages.sort(
        key=lambda x: (
            x["mdd"],
            -x["new_entry_allowed_pct"],
            x["liquidate_weakest_pct"],
            -x["max_exposure"],
            x["_source_idx"],
        )
    )

    hard_cfg = dm.get("hard_block_conditions") if isinstance(dm.get("hard_block_conditions"), dict) else {}
    hard_threshold = _ddm_pct01(hard_cfg.get("mdd_threshold"), 0.36) if hard_cfg else None
    hard_block = _ddm_is_hard_block(current_mdd_abs, ctx, hard_cfg) if hard_cfg else False

    action = default_action
    for i, stg in enumerate(stages):
        if hard_threshold is not None and stg["mdd"] >= hard_threshold and not hard_block:
            break
        if current_mdd_abs >= stg["mdd"]:
            action = DrawdownAction(
                current_mdd_abs=current_mdd_abs,
                stage_idx=i + 1,
                threshold=stg["mdd"],
                new_entry_allowed_pct=stg["new_entry_allowed_pct"],
                liquidate_weakest_pct=stg["liquidate_weakest_pct"],
                max_exposure=stg["max_exposure"],
                metric_basis=metric_basis,
                metric_details=metric_details,
            )
        else:
            break
    return action


def _ddm_forced_sell_ratio_pct(ddm_action: Any) -> float:
    stage_idx = int(getattr(ddm_action, "stage_idx", -1) or -1)
    # DDM stage 4+ (MDD 25%+) intentionally fully exits selected positions.
    # liquidate_weakest_pct remains the weakest-position selection pct, not the
    # per-position sell ratio, for these emergency escalation stages.
    if stage_idx >= 4:
        return 100.0
    ratio_pct = _ddm_pct01(getattr(ddm_action, "liquidate_weakest_pct", 0.0), 0.0) * 100.0
    if ratio_pct <= 0.0:
        return 100.0
    return min(100.0, max(0.0, ratio_pct))


def _apply_drawdown_entry_capacity(
    *,
    cfg: Dict[str, Any],
    p0_snapshot: Dict[str, Any],
    gate_snapshot: Dict[str, Any],
    trades_path: Path,
    log_dir: Path,
    ddm_status_path: Path,
    max_new: int,
) -> Dict[str, Any]:
    ddm_cfg = cfg.get("drawdown_manager") if isinstance(cfg, dict) else {}
    if not isinstance(ddm_cfg, dict):
        ddm_cfg = {}
    ddm_enabled = bool(ddm_cfg.get("enabled", False))
    consecutive_loss_days = _ddm_count_consecutive_loss_days(trades_path)
    ddm_context = {
        "consecutive_loss_days": consecutive_loss_days,
        "kill_switch_active": bool(
            (
                (p0_snapshot.get("kill_switch") if isinstance(p0_snapshot.get("kill_switch"), dict) else {})
                or {}
            ).get("triggered", False)
        ),
        "risk_off": bool(p0_snapshot.get("risk_off_enabled", False)),
        "gate_daily": str(gate_snapshot.get("gate_status") or "").upper(),
    }
    print(f"[DDM] context={ddm_context}")
    ddm_action = _ddm_select_action(cfg, p0_snapshot, context=ddm_context)
    ddm_exposure_cap: Optional[float] = None
    ddm_force_liquidate_pct = 0.0

    if ddm_enabled:
        old_max_new = int(max_new)
        ddm_stage = max(0, int(getattr(ddm_action, "stage_idx", 0) or 0))
        ddm_cap_new = calc_max_new(old_max_new, ddm_action.new_entry_allowed_pct, ddm_stage)
        max_new = min(int(max_new), ddm_cap_new)
        ddm_exposure_cap = ddm_action.max_exposure
        ddm_force_liquidate_pct = float(ddm_action.liquidate_weakest_pct)
        ddm_forced_sell_ratio_pct = float(_ddm_forced_sell_ratio_pct(ddm_action))
        print(
            "[DDM] mdd_abs=%.4f stage=%s threshold=%.2f entry_pct=%.2f liquidation_selection_pct=%.2f forced_sell_ratio_pct=%.2f exposure_cap=%s max_new=%d->%d"
            % (
                ddm_action.current_mdd_abs,
                str(ddm_action.stage_idx),
                ddm_action.threshold,
                ddm_action.new_entry_allowed_pct,
                ddm_action.liquidate_weakest_pct,
                ddm_forced_sell_ratio_pct,
                ("None" if ddm_action.max_exposure is None else f"{ddm_action.max_exposure:.2f}"),
                old_max_new,
                max_new,
            )
        )
        try:
            ddm_status = {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "source": "paper_engine",
                "pnl_summary_path": str(log_dir / "paper_pnl_summary_last.json"),
                "ddm_enabled": bool(ddm_enabled),
                "current_mdd_abs": float(ddm_action.current_mdd_abs),
                "metric_basis": str(getattr(ddm_action, "metric_basis", "") or ""),
                "metric_details": (
                    getattr(ddm_action, "metric_details", None)
                    if isinstance(getattr(ddm_action, "metric_details", None), dict)
                    else {}
                ),
                "stage_idx": int(ddm_action.stage_idx),
                "threshold": float(ddm_action.threshold),
                "new_entry_allowed_pct": float(ddm_action.new_entry_allowed_pct),
                "liquidate_weakest_pct": float(ddm_action.liquidate_weakest_pct),
                "liquidation_selection_pct": float(ddm_action.liquidate_weakest_pct),
                "forced_sell_ratio_pct": float(ddm_forced_sell_ratio_pct),
                "ddm_pct_field_semantics": {
                    "liquidate_weakest_pct": "legacy_alias_for_liquidation_selection_pct",
                    "liquidation_selection_pct": "weakest_position_selection_pct",
                    "forced_sell_ratio_pct": "per_selected_position_sell_ratio_pct",
                    "stage_4_plus": "full_exit_selected_positions_intentional_emergency_escalation",
                    "dd_ratio_hard": "entry_gate_daily_loss_ratio_independent_from_drawdown_manager_mdd",
                },
                "max_exposure": ddm_action.max_exposure,
                "max_new_before": int(old_max_new),
                "max_new_after": int(max_new),
                "context": ddm_context,
                "p0_snapshot_path": p0_snapshot.get("path") if isinstance(p0_snapshot, dict) else None,
                "p0_as_of_ymd": p0_snapshot.get("as_of_ymd") if isinstance(p0_snapshot, dict) else None,
                "pnl_alignment": (
                    p0_snapshot.get("ddm_pnl_alignment")
                    if isinstance(p0_snapshot, dict) and isinstance(p0_snapshot.get("ddm_pnl_alignment"), dict)
                    else {}
                ),
            }
            ddm_status_path.write_text(json.dumps(ddm_status, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[DDM_STATUS] json={ddm_status_path}")
        except Exception as e:
            print(f"[DDM_STATUS][WARN] write_failed={type(e).__name__}:{e}")

    return {
        "ddm_cfg": ddm_cfg,
        "ddm_enabled": bool(ddm_enabled),
        "consecutive_loss_days": int(consecutive_loss_days),
        "ddm_context": ddm_context,
        "ddm_action": ddm_action,
        "ddm_exposure_cap": ddm_exposure_cap,
        "ddm_force_liquidate_pct": float(ddm_force_liquidate_pct),
        "max_new": int(max_new),
    }


def _ddm_count_consecutive_loss_days(trades_path: Path) -> int:
    if not trades_path.exists():
        return 0
    df = read_csv_safe(trades_path)
    if df is None or df.empty:
        return 0

    ret_col = None
    for c in ("pnl_pct", "net_ret", "ret", "return_pct"):
        if c in df.columns:
            ret_col = c
            break
    if not ret_col:
        return 0

    exit_col = None
    for c in ("exit_date", "exit_ts"):
        if c in df.columns:
            exit_col = c
            break
    if not exit_col:
        return 0

    try:
        cols = [exit_col, ret_col]
        pnl_krw_col = "pnl_krw" if "pnl_krw" in df.columns else None
        if pnl_krw_col:
            cols.append(pnl_krw_col)
        t = df[cols].copy()
        t[ret_col] = pd.to_numeric(t[ret_col], errors="coerce")
        t["_exit_ymd"] = t[exit_col].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
        if pnl_krw_col:
            t[pnl_krw_col] = pd.to_numeric(t[pnl_krw_col], errors="coerce")
            t = t[(t["_exit_ymd"].str.len() == 8) & (~t[pnl_krw_col].isna())]
        else:
            t = t[(t["_exit_ymd"].str.len() == 8) & (~t[ret_col].isna())]
        if t.empty:
            return 0
        if pnl_krw_col:
            dly = t.groupby("_exit_ymd")[pnl_krw_col].sum().sort_index(ascending=False)
        else:
            dly = t.groupby("_exit_ymd")[ret_col].mean().sort_index(ascending=False)
        streak = 0
        for v in dly.tolist():
            if float(v) < 0.0:
                streak += 1
            else:
                break
        return int(streak)
    except Exception:
        return 0


def _ddm_pos_key(pos: Dict[str, Any], idx: int) -> str:
    code = norm_code(pos.get("code", ""))
    anchor = str(pos.get("entry_ts") or pos.get("entry_date") or idx)
    return f"{code}|{anchor}"


def _ddm_select_liquidation_targets(
    open_pos: List[Dict[str, Any]],
    px: pd.DataFrame,
    liquidate_pct: float,
    min_positions_for_partial_liquidation: int = 1,
) -> set[str]:
    pct = _ddm_pct01(liquidate_pct, 0.0)
    if pct <= 0.0 or not open_pos:
        return set()
    min_positions = max(1, int(_ddm_to_float(min_positions_for_partial_liquidation, 1)))
    if 0.0 < pct < 1.0 and len(open_pos) < min_positions:
        return set()
    rows: List[Tuple[str, float]] = []
    for i, pos in enumerate(open_pos):
        code = norm_code(pos.get("code", ""))
        if not code:
            continue
        entry_price = _ddm_to_float(pos.get("entry_price"), 0.0)
        if entry_price <= 0:
            continue
        cpx = px[px["code"] == code]
        if cpx.empty:
            continue
        try:
            cpx = cpx.sort_values("date")
            last_close = _ddm_to_float(cpx.iloc[-1]["close"], 0.0)
            if last_close <= 0:
                continue
            pnl_pct = (last_close - entry_price) / entry_price
            rows.append((_ddm_pos_key(pos, i), float(pnl_pct)))
        except Exception:
            continue
    if not rows:
        return set()
    n = int(len(rows) * pct)
    if pct > 0 and n <= 0:
        n = 1
    n = max(0, min(len(rows), n))
    rows.sort(key=lambda x: x[1])
    return set(k for k, _ in rows[:n])


def _ddm_sector_concentration(
    open_pos: List[Dict[str, Any]],
    code_to_sector: Dict[str, str],
    px: Optional[pd.DataFrame] = None,
) -> Tuple[Optional[str], float]:
    if not open_pos:
        return None, 0.0
    total_notional = 0.0
    sec_notional: Dict[str, float] = {}
    for idx_pos, pos in enumerate(open_pos):
        code = norm_code(pos.get("code", ""))
        qty = int(_ddm_to_float(pos.get("qty"), 0.0))
        entry_price = _ddm_to_float(pos.get("entry_price"), 0.0)
        # use last close price if px available, otherwise fall back to entry_price
        price = entry_price
        if px is not None and not px.empty and code:
            _cpx = px[px["code"] == code]
            if not _cpx.empty:
                _lc = _ddm_to_float(_cpx.sort_values("date").iloc[-1]["close"], 0.0)
                if _lc > 0:
                    price = _lc
        notional = float(qty) * float(price) if qty > 0 and price > 0 else 0.0
        if notional <= 0:
            continue
        total_notional += notional
        sec = str(code_to_sector.get(code, "") or "").strip()
        if not sec:
            continue
        sec_notional[sec] = sec_notional.get(sec, 0.0) + notional
    if total_notional <= 0 or (not sec_notional):
        return None, 0.0
    top_sector, top_notional = max(sec_notional.items(), key=lambda kv: kv[1])
    ratio = float(top_notional) / float(total_notional)
    return top_sector, ratio
