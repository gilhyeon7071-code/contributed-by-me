"""Standalone guard evaluations split from paper_engine.py.

This module holds policy guards extracted from paper_engine.py during Phase 3c.
Complex guards (global_outlier_watcher, macro_news_guard, backtest_validation_guard)
were moved here in Phase 3c (3/3).
"""

from __future__ import annotations


__all__ = [
    'evaluate_cross_source_integrity',
    'evaluate_sigma_outlier_guard',
    'evaluate_execution_health_guard',
    '_production_risk_stage',
    '_production_risk_max_stage',
    'evaluate_production_risk_playbook_guard',
    'evaluate_global_outlier_watcher',
    'evaluate_macro_news_guard',
    'evaluate_backtest_validation_guard',
    '_detect_explicit_market_events',
    'count_kill_switch_streak_days',
    'compute_adaptive_kill_cap',
]

import json
import math
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# Ensure utils import works regardless of cwd.
ROOT = Path(os.environ.get("STOC_BASE_DIR", r"E:\1_Data"))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.common import latest_file, norm_code, now_ymd, is_daily_loss_reason  # noqa: E402
from paper_engine.config import _path_from_env  # noqa: E402
from paper_engine.common import (  # noqa: E402
    _is_hard_block_reason,
    _to_float,
    _to_int,
    _load_json_with_date,
    _business_day_gap,
    _parse_policy_dt_text,
    _event_doc_rows,
    _norm_ymd_text,
    _get_dict,
    _get_list,
)
from paper_engine.io import (  # noqa: E402
    PRODUCTION_RISK_PLAYBOOK_PATH,
    FINAL_SCORE_MERGE_STATUS_PATH,
    CANDIDATES_META_PATH,
    PENDING_ENTRY_STATUS_PATH,
    NEWS_COLLECT_STATUS_PATH,
    PAPER_FILLS_LEDGER_PATH,
)


# Path constants moved from paper_engine.py (guard-specific).
INTEGRATED_OPS_SNAPSHOT_PATH = _path_from_env(
    "PAPER_INTEGRATED_OPS_SNAPSHOT_PATH",
    ROOT / "2_Logs" / "integrated_ops_snapshot_latest.json",
)
NEWS_SCORE_STATUS_PATH = _path_from_env(
    "PAPER_NEWS_SCORE_STATUS_PATH",
    ROOT / "2_Logs" / "news_score_status_latest.json",
)
MACRO_SIGNAL_LATEST_PATH = _path_from_env(
    "PAPER_MACRO_SIGNAL_PATH",
    ROOT / "2_Logs" / "macro_signal_latest.json",
)
RATE_EXTERNAL_STATUS_PATH = _path_from_env(
    "PAPER_RATE_EXTERNAL_STATUS_PATH",
    ROOT / "2_Logs" / "rate_series_external_status_latest.json",
)
BACKTEST_VALIDATION_LATEST_PATH = _path_from_env(
    "PAPER_BACKTEST_VALIDATION_LATEST_PATH",
    ROOT / "2_Logs" / "backtest_validation_latest.json",
)

def evaluate_cross_source_integrity(cfg: Dict[str, Any], log_dir: Path) -> Dict[str, Any]:
    pol = _get_dict(cfg, "cross_source_integrity")
    if not isinstance(pol, dict):
        pol = {}
    enabled = bool(pol.get("enabled", False))
    out: Dict[str, Any] = {
        "enabled": enabled,
        "decision": "ALLOW",
        "reason": "",
        "max_skew_days": None,
        "reference_ymd": None,
        "sources": {},
        "issues": [],
    }
    if not enabled:
        out["reason"] = "policy_disabled"
        return out

    signal_status_path = latest_file(log_dir, "signal_integration_status_*.json")
    source_defs: Dict[str, Dict[str, Any]] = {
        "candidates_meta": {"path": CANDIDATES_META_PATH, "keys": ["latest_date", "as_of", "as_of_ymd"]},
        # generated_at is runtime timestamp, not trading/session as-of. Using it here causes false skew blocks.
        "final_score_merge": {"path": FINAL_SCORE_MERGE_STATUS_PATH, "keys": ["asof_ymd", "as_of"]},
        "signal_integration": {"path": signal_status_path, "keys": ["asof_ymd", "as_of"]},
        # pending_entry contains operational sub-docs like replay_consistency.as_of; do not treat those as candidate-chain as-of.
        "pending_entry": {"path": PENDING_ENTRY_STATUS_PATH, "keys": ["as_of_ymd", "as_of"], "allow_replay_consistency_fallback": False},
        "news_collect": {"path": NEWS_COLLECT_STATUS_PATH, "keys": ["as_of_ymd", "as_of"]},
    }

    required_sources = {str(x) for x in list(pol.get("required_sources") or [])}
    optional_sources = {str(x) for x in list(pol.get("optional_sources") or [])}
    if not required_sources:
        # Require only artifacts that carry an explicit trading/session as-of.
        required_sources = {"candidates_meta", "final_score_merge"}
    if not optional_sources:
        optional_sources = {"signal_integration", "pending_entry", "news_collect"}
    target_sources = sorted(required_sources | optional_sources)

    required_values: List[str] = []
    optional_values: List[str] = []
    issues: List[str] = []
    missing_required: List[str] = []
    for key in target_sources:
        spec = source_defs.get(key)
        if not isinstance(spec, dict):
            continue
        p = spec.get("path")
        if not isinstance(p, Path):
            info = {"path": "", "exists": False, "as_of_ymd": None, "error": "path_unresolved"}
        else:
            info = _load_json_with_date(
                p,
                list(spec.get("keys") or []),
                allow_replay_consistency_fallback=bool(spec.get("allow_replay_consistency_fallback", True)),
            )
        out["sources"][key] = info
        ymd = str(info.get("as_of_ymd") or "")
        if re.fullmatch(r"\d{8}", ymd):
            if key in required_sources:
                required_values.append(ymd)
            elif key in optional_sources:
                optional_values.append(ymd)
        elif key in required_sources:
            missing_required.append(key)

    if required_values:
        ref = max(required_values)
        out["reference_ymd"] = ref
        try:
            max_skew_days = int(float(pol.get("max_skew_days", 1) or 1))
        except Exception:
            max_skew_days = 1
        out["max_skew_days"] = max_skew_days
        try:
            ref_dt = datetime.strptime(ref, "%Y%m%d")
        except Exception:
            ref_dt = None
        if ref_dt is not None:
            for key, info in out["sources"].items():
                ymd = str(info.get("as_of_ymd") or "")
                if not re.fullmatch(r"\d{8}", ymd):
                    continue
                try:
                    skew = _business_day_gap(ymd, ref) if ref else None
                    if skew is None:
                        continue
                    info["skew_days"] = int(skew)
                    if skew > max_skew_days:
                        issues.append(f"skew:{key}:{ymd}->{ref}:{int(skew)}d")
                except Exception:
                    continue
    else:
        issues.append("no_valid_asof")

    if missing_required:
        issues.append(f"missing_required:{','.join(missing_required)}")
        if bool(pol.get("block_on_missing_required", False)):
            out["decision"] = "BLOCK"
            out["reason"] = f"missing_required:{','.join(missing_required)}"
            out["issues"] = issues
            return out

    out["issues"] = issues
    if any(s.startswith("skew:") for s in issues):
        out["decision"] = "BLOCK"
        out["reason"] = next((s for s in issues if s.startswith("skew:")), "skew_detected")
    elif issues:
        out["decision"] = "CAUTION"
        out["reason"] = issues[0]
    else:
        out["decision"] = "ALLOW"
        out["reason"] = "ok"
    return out


def evaluate_sigma_outlier_guard(cfg: Dict[str, Any], log_dir: Path) -> Dict[str, Any]:
    pol = _get_dict(cfg, "sigma_outlier_guard")
    if not isinstance(pol, dict):
        pol = {}
    enabled = bool(pol.get("enabled", False))
    out: Dict[str, Any] = {
        "enabled": enabled,
        "decision": "ALLOW",
        "reason": "",
        "current_rows": None,
        "mean_rows": None,
        "std_rows": None,
        "zscore": None,
        "history_len": 0,
        "issues": [],
        "param_issues": [],
    }
    if not enabled:
        out["reason"] = "policy_disabled"
        return out

    latest_path = FINAL_SCORE_MERGE_STATUS_PATH
    if not latest_path.exists():
        out["decision"] = "CAUTION"
        out["reason"] = "latest_missing"
        out["issues"] = ["latest_missing"]
        return out

    try:
        latest_obj = json.loads(latest_path.read_text(encoding="utf-8"))
        current_rows = float(latest_obj.get("rows", 0) or 0)
    except Exception as exc:
        out["decision"] = "CAUTION"
        out["reason"] = f"latest_read_fail:{type(exc).__name__}"
        out["issues"] = [out["reason"]]
        return out

    out["current_rows"] = current_rows

    param_issues: List[str] = []
    if bool(pol.get("check_param_ranges", True)):
        range_cfg = _get_dict(pol, "param_ranges")

        def _range_pair(name: str, default_lo: float, default_hi: float) -> tuple[float, float]:
            raw = range_cfg.get(name)
            if isinstance(raw, (list, tuple)) and len(raw) >= 2:
                try:
                    return float(raw[0]), float(raw[1])
                except Exception:
                    pass
            return float(default_lo), float(default_hi)

        def _check_range(name: str, value: Any, default_lo: float, default_hi: float) -> None:
            try:
                x = float(value)
            except Exception:
                return
            lo, hi = _range_pair(name, default_lo, default_hi)
            if x < lo or x > hi:
                param_issues.append(f"param_out_of_range:{name}:{x:.6g} not in [{lo:.6g},{hi:.6g}]")

        regime_policy = _get_dict(cfg, "regime_entry_policy")
        _check_range(
            "regime_entry_policy.rally_day_ret_min_proxy",
            regime_policy.get("rally_day_ret_min_proxy"),
            -0.05,
            0.10,
        )
        _check_range(
            "regime_entry_policy.rally_day_ret_min",
            regime_policy.get("rally_day_ret_min"),
            -0.05,
            0.10,
        )
        _check_range("max_gross_exposure_pct", cfg.get("max_gross_exposure_pct"), 0.0, 1.0)
        _check_range("max_daily_new_exposure_pct", cfg.get("max_daily_new_exposure_pct"), 0.0, 1.0)

        meta_obj: Dict[str, Any] = {}
        if CANDIDATES_META_PATH.exists():
            try:
                meta_obj = json.loads(CANDIDATES_META_PATH.read_text(encoding="utf-8"))
            except Exception:
                try:
                    meta_obj = json.loads(CANDIDATES_META_PATH.read_text(encoding="utf-8-sig"))
                except Exception:
                    meta_obj = {}
        chosen = _get_dict(meta_obj, "chosen_params")
        # rs was redefined from a ratio to a difference on 2026-07-28, moving rs_lim
        # from ~1.6 to ~0.05. This range still carried the old ratio scale, so every
        # valid value was reported as out_of_range. Bounds match the production clamp
        # in generate_candidates_v41_1.py (min(max(x, -0.10), 1.00)) and the optimizer
        # search space in optimize_params_v41_1.py.
        _check_range("candidate.rs_lim", chosen.get("rs_lim"), -0.10, 1.00)
        _check_range("candidate.disparity20_max", chosen.get("disparity20_max"), 1.0, 1.5)
        _check_range("candidate.disparity60_max", chosen.get("disparity60_max"), 1.0, 1.8)

    out["param_issues"] = list(param_issues)
    if param_issues:
        out["issues"] = list(param_issues)
        if bool(pol.get("block_on_param_out_of_range", True)):
            out["decision"] = "BLOCK"
            out["reason"] = param_issues[0]
            return out
        out["decision"] = "CAUTION"
        out["reason"] = param_issues[0]
    files = sorted(log_dir.glob("final_score_merge_status_*.json"), key=lambda p: p.stat().st_mtime)
    try:
        lookback = max(5, int(float(pol.get("lookback_files", 20) or 20)))
    except Exception:
        lookback = 20
    files = files[-lookback:]
    hist_vals: List[float] = []
    for p in files:
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
            val = float(obj.get("rows", 0) or 0)
            hist_vals.append(val)
        except Exception:
            continue

    # Exclude current latest row from baseline when possible.
    baseline = hist_vals[:-1] if len(hist_vals) > 1 else []
    out["history_len"] = len(baseline)
    try:
        min_history = max(3, int(float(pol.get("min_history", 5) or 5)))
    except Exception:
        min_history = 5
    if len(baseline) < min_history:
        out["reason"] = "insufficient_history"
        return out

    mu = sum(baseline) / float(len(baseline))
    var = sum((x - mu) ** 2 for x in baseline) / float(max(1, len(baseline)))
    sigma = math.sqrt(max(0.0, var))
    out["mean_rows"] = mu
    out["std_rows"] = sigma

    if sigma > 0:
        z = (current_rows - mu) / sigma
        out["zscore"] = z
    else:
        z = 0.0
        out["zscore"] = 0.0

    try:
        z_caution = float(pol.get("zscore_caution", 3.0) or 3.0)
        z_block = float(pol.get("zscore_block", 5.0) or 5.0)
    except Exception:
        z_caution, z_block = 3.0, 5.0
    try:
        ratio_caution = float(pol.get("fallback_ratio_caution", 0.4) or 0.4)
        ratio_block = float(pol.get("fallback_ratio_block", 0.2) or 0.2)
    except Exception:
        ratio_caution, ratio_block = 0.4, 0.2

    ratio = (current_rows / mu) if mu > 0 else 1.0
    if sigma > 0 and abs(z) >= z_block:
        out["decision"] = "BLOCK"
        out["reason"] = f"zscore_block:{z:.2f}"
        out["issues"] = list(param_issues) + [out["reason"]]
    elif sigma > 0 and abs(z) >= z_caution:
        out["decision"] = "CAUTION"
        out["reason"] = f"zscore_caution:{z:.2f}"
        out["issues"] = list(param_issues) + [out["reason"]]
    elif mu > 0 and ratio <= ratio_block:
        out["decision"] = "BLOCK"
        out["reason"] = f"ratio_block:{ratio:.3f}"
        out["issues"] = list(param_issues) + [out["reason"]]
    elif mu > 0 and ratio <= ratio_caution:
        out["decision"] = "CAUTION"
        out["reason"] = f"ratio_caution:{ratio:.3f}"
        out["issues"] = list(param_issues) + [out["reason"]]
    else:
        if param_issues:
            out["decision"] = "CAUTION"
            out["reason"] = param_issues[0]
            out["issues"] = list(param_issues)
        else:
            out["decision"] = "ALLOW"
            out["reason"] = "ok"
    return out


def evaluate_execution_health_guard(cfg: Dict[str, Any]) -> Dict[str, Any]:
    pol = _get_dict(cfg, "execution_health_guard")
    if not isinstance(pol, dict):
        pol = {}
    enabled = bool(pol.get("enabled", False))
    out: Dict[str, Any] = {
        "enabled": enabled,
        "decision": "ALLOW",
        "reason": "",
        "issues": [],
        "lifecycle_status": None,
        "state_machine_status": None,
        "symbol_stop_status": None,
        "slippage_avg_bps": None,
    }
    if not enabled:
        out["reason"] = "policy_disabled"
        return out

    issues: List[str] = []
    cautions: List[str] = []
    try:
        block_lifecycle = {str(x).upper() for x in list(pol.get("block_lifecycle_statuses") or [])}
    except Exception:
        block_lifecycle = {"FAIL", "BLOCK", "ERROR"}
    try:
        block_state_machine = {str(x).upper() for x in list(pol.get("block_state_machine_statuses") or [])}
    except Exception:
        block_state_machine = {"FAIL", "BLOCK", "ERROR"}
    try:
        caution_symbol_stop = {str(x).upper() for x in list(pol.get("caution_symbol_stop_statuses") or [])}
    except Exception:
        caution_symbol_stop = {"WARN", "PARTIAL", "FAIL"}

    if PENDING_ENTRY_STATUS_PATH.exists():
        try:
            obj = json.loads(PENDING_ENTRY_STATUS_PATH.read_text(encoding="utf-8"))
        except Exception as exc:
            obj = {}
            cautions.append(f"pending_read_fail:{type(exc).__name__}")
        lifecycle = _get_dict(obj, "entry_exit_lifecycle")
        sm = _get_dict(obj, "state_machine_summary")
        stop = _get_dict(obj, "symbol_stop_summary")
        status_reason = str(obj.get("status_reason") or "").upper()
        pending_queue_len = int(_to_int(obj.get("pending_queue_len", 0), 0))
        execution_guard_open_only = (status_reason == "OPEN_POSITION_ACTIVE" and pending_queue_len == 0)
        lifecycle_status = str(lifecycle.get("status") or "").upper()
        state_machine_status = str(sm.get("status") or "").upper()
        symbol_stop_status = str(stop.get("status") or "").upper()
        out["lifecycle_status"] = lifecycle_status or None
        out["state_machine_status"] = state_machine_status or None
        out["symbol_stop_status"] = symbol_stop_status or None
        if lifecycle_status in block_lifecycle and (not execution_guard_open_only):
            issues.append(f"lifecycle:{lifecycle_status}")
        if state_machine_status in block_state_machine and (not execution_guard_open_only):
            issues.append(f"state_machine:{state_machine_status}")
        if symbol_stop_status in caution_symbol_stop:
            cautions.append(f"symbol_stop:{symbol_stop_status}")
    else:
        cautions.append("pending_status_missing")

    if PAPER_FILLS_LEDGER_PATH.exists():
        try:
            lookback_rows = max(5, int(float(pol.get("slippage_lookback_rows", 30) or 30)))
        except Exception:
            lookback_rows = 30
        try:
            slippage_caution = float(pol.get("slippage_bps_caution", 10.0) or 10.0)
            slippage_block = float(pol.get("slippage_bps_block", 20.0) or 20.0)
        except Exception:
            slippage_caution, slippage_block = 10.0, 20.0
        try:
            ldf = pd.read_csv(PAPER_FILLS_LEDGER_PATH, usecols=["slippage_bps_model"]).tail(lookback_rows)
            vals = pd.to_numeric(ldf["slippage_bps_model"], errors="coerce").dropna().abs()
            if not vals.empty:
                avg_bps = float(vals.mean())
                out["slippage_avg_bps"] = avg_bps
                if avg_bps >= slippage_block:
                    issues.append(f"slippage_block:{avg_bps:.2f}bps")
                elif avg_bps >= slippage_caution:
                    cautions.append(f"slippage_caution:{avg_bps:.2f}bps")
        except Exception as exc:
            cautions.append(f"slippage_read_fail:{type(exc).__name__}")
    else:
        cautions.append("ledger_missing")

    out["issues"] = list(dict.fromkeys(issues + cautions))
    if issues:
        out["decision"] = "BLOCK"
        out["reason"] = issues[0]
    elif cautions:
        out["decision"] = "CAUTION"
        out["reason"] = cautions[0]
    else:
        out["decision"] = "ALLOW"
        out["reason"] = "ok"
    return out


def _production_risk_stage(value: float, watch: float, soft: float, hard: float) -> str:
    if value >= hard:
        return "HARD"
    if value >= soft:
        return "SOFT"
    if value >= watch:
        return "WATCH"
    return "NORMAL"


def _production_risk_max_stage(stages: List[str]) -> str:
    order = {"NORMAL": 0, "WATCH": 1, "SOFT": 2, "HARD": 3}
    return max(stages or ["NORMAL"], key=lambda x: order.get(str(x), 0))


def evaluate_production_risk_playbook_guard(cfg: Dict[str, Any], current_risk_orch: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    pol = _get_dict(cfg, "production_risk_playbook")
    if not isinstance(pol, dict):
        pol = {}
    enabled = bool(pol.get("enabled", False))
    out: Dict[str, Any] = {
        "enabled": enabled,
        "decision": "ALLOW",
        "reason": "",
        "action": "NORMAL",
        "issues": [],
        "artifact_path": str(PRODUCTION_RISK_PLAYBOOK_PATH),
        "source_fresh": False,
        "mitigations": {},
    }
    if not enabled:
        out["reason"] = "policy_disabled"
        return out

    path = Path(str(pol.get("artifact_path") or PRODUCTION_RISK_PLAYBOOK_PATH))
    out["artifact_path"] = str(path)
    if not path.exists():
        missing_action = str(pol.get("missing_action", "ALLOW") or "ALLOW").upper()
        out["decision"] = missing_action if missing_action in {"ALLOW", "CAUTION", "REDUCE", "BLOCK"} else "ALLOW"
        out["reason"] = "playbook_missing"
        out["issues"] = ["playbook_missing"]
        return out

    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        if not isinstance(payload, dict):
            raise ValueError("payload_not_object")
    except Exception as exc:
        out["decision"] = "BLOCK" if bool(pol.get("fail_closed_on_read_error", True)) else "CAUTION"
        out["reason"] = f"playbook_read_fail:{type(exc).__name__}"
        out["issues"] = [out["reason"]]
        return out

    source_fresh = bool(payload.get("source_fresh", False))
    out["source_fresh"] = source_fresh
    out["generated_at"] = payload.get("generated_at")
    max_age_minutes = max(0.0, _to_float(pol.get("max_age_minutes", 30), 30.0))
    out["max_age_minutes"] = max_age_minutes
    generated_at_raw = str(payload.get("generated_at") or "").strip()
    generated_at_dt = _parse_policy_dt_text(generated_at_raw)
    age_minutes = None
    if generated_at_dt is not None:
        age_minutes = max(0.0, (datetime.now() - generated_at_dt).total_seconds() / 60.0)
    out["age_minutes"] = age_minutes
    action = str(payload.get("action") or "NORMAL").upper()
    if action not in {"NORMAL", "WATCH", "SOFT", "HARD"}:
        action = "NORMAL"
    out["action"] = action
    out["reason"] = str(payload.get("reason") or "ok")
    out["signals"] = _get_list(payload, "signals")
    out["mitigations"] = _get_dict(payload, "mitigations")
    out["issues"] = [out["reason"]] if action != "NORMAL" else []
    current_ro_used = False
    if isinstance(current_risk_orch, dict) and bool(current_risk_orch.get("enabled", False)):
        dd_pct = abs(_to_float(current_risk_orch.get("dd_current"), 0.0)) * 100.0
        dd_cfg = _get_dict(pol, "dd_pct")
        dd_role = str(pol.get("drawdown_role") or "blocking").strip().lower()
        dd_advisory = dd_role in {"advisory", "confirm_only", "ddm_owned", "ddm_owner"}
        dd_stage = _production_risk_stage(
            dd_pct,
            _to_float(dd_cfg.get("watch"), 0.25),
            _to_float(dd_cfg.get("soft"), 0.5),
            _to_float(dd_cfg.get("hard"), 1.0),
        )
        signals = [s for s in out["signals"] if not (isinstance(s, dict) and str(s.get("name") or "") == "drawdown")]
        signals.insert(
            0,
            {
                "name": "drawdown",
                "stage": dd_stage,
                "value": dd_pct,
                "unit": "pct",
                "role": dd_role,
                "advisory": dd_advisory,
                "sample_ok": True,
                "reason": f"dd_pct={dd_pct:.4f}",
                "source": "current_risk_orchestration",
                "risk_orchestration_current": True,
            },
        )
        action_signals = [s for s in signals if isinstance(s, dict) and not bool(s.get("advisory", False))]
        advisory_signals = [s for s in signals if isinstance(s, dict) and bool(s.get("advisory", False))]
        hard_count = sum(1 for s in action_signals if s.get("stage") == "HARD")
        soft_count = sum(1 for s in action_signals if s.get("stage") == "SOFT")
        watch_count = sum(1 for s in action_signals if s.get("stage") == "WATCH")
        action = _production_risk_max_stage([str(s.get("stage") or "NORMAL") for s in action_signals])
        reasons = [f"{s.get('name')}:{s.get('stage')}:{s.get('reason')}" for s in action_signals if s.get("stage") != "NORMAL"]
        advisory_reasons = [f"{s.get('name')}:{s.get('stage')}:{s.get('reason')}" for s in advisory_signals if s.get("stage") != "NORMAL"]
        out["signals"] = signals
        out["advisory_signals"] = advisory_signals
        out["advisory_reason"] = ";".join(advisory_reasons)
        out["rules"] = {
            "hard_count": hard_count,
            "soft_count": soft_count,
            "watch_count": watch_count,
            "concurrent_hard": hard_count >= 2,
            "advisory_count": len(advisory_signals),
        }
        out["action"] = action
        out["reason"] = ";".join(reasons) if reasons else "ok"
        out["issues"] = [out["reason"]] if action != "NORMAL" else []
        out["source_fresh"] = True
        out["current_risk_orchestration_used"] = True
        current_ro_used = True

    if (not current_ro_used) and max_age_minutes > 0 and (generated_at_dt is None or age_minutes is None or age_minutes > max_age_minutes):
        source_fresh = False
        out["source_fresh"] = False
        stale_action = str(pol.get("stale_action", "CAUTION") or "CAUTION").upper()
        out["decision"] = stale_action if stale_action in {"ALLOW", "CAUTION", "REDUCE", "BLOCK"} else "CAUTION"
        out["reason"] = "playbook_stale_age"
        out["issues"] = ["playbook_stale_age"]
        return out

    if not source_fresh:
        stale_action = str(pol.get("stale_action", "CAUTION") or "CAUTION").upper()
        out["decision"] = stale_action if stale_action in {"ALLOW", "CAUTION", "REDUCE", "BLOCK"} else "CAUTION"
        out["reason"] = "playbook_stale"
        out["issues"] = ["playbook_stale"]
        return out

    if action == "HARD":
        out["decision"] = str(pol.get("hard_entry_decision", "BLOCK") or "BLOCK").upper()
    elif action == "SOFT":
        out["decision"] = str(pol.get("soft_entry_decision", "REDUCE") or "REDUCE").upper()
    elif action == "WATCH":
        out["decision"] = str(pol.get("watch_entry_decision", "CAUTION") or "CAUTION").upper()
    else:
        out["decision"] = "ALLOW"
        out["reason"] = "ok"
    if out["decision"] not in {"ALLOW", "CAUTION", "REDUCE", "BLOCK"}:
        out["decision"] = "ALLOW"
    return out


def evaluate_global_outlier_watcher(cfg: Dict[str, Any], log_dir: Path) -> Dict[str, Any]:
    pol = _get_dict(cfg, "global_outlier_watcher")
    if not isinstance(pol, dict):
        pol = {}
    enabled = bool(pol.get("enabled", False))
    out: Dict[str, Any] = {
        "enabled": enabled,
        "decision": "ALLOW",
        "reason": "",
        "path": str(INTEGRATED_OPS_SNAPSHOT_PATH),
        "as_of_ymd": None,
        "age_days": None,
        "issues": [],
    }
    if not enabled:
        out["reason"] = "policy_disabled"
        return out

    p = INTEGRATED_OPS_SNAPSHOT_PATH
    if not p.exists():
        if bool(pol.get("block_on_snapshot_missing", False)):
            out["decision"] = "BLOCK"
            out["reason"] = "snapshot_missing"
            out["issues"] = ["snapshot_missing"]
        else:
            out["reason"] = "snapshot_missing_allow"
        return out

    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
    except Exception as exc:
        if bool(pol.get("block_on_snapshot_missing", False)):
            out["decision"] = "BLOCK"
            out["reason"] = "snapshot_read_fail"
            out["issues"] = [f"snapshot_read_fail:{type(exc).__name__}"]
        else:
            out["reason"] = "snapshot_read_fail_allow"
            out["issues"] = [f"snapshot_read_fail:{type(exc).__name__}"]
        return out

    summary = _get_dict(obj, "summary")
    as_of = str(summary.get("as_of_ymd") or "")
    out["as_of_ymd"] = as_of or None
    if re.fullmatch(r"\d{8}", as_of or ""):
        try:
            bd_gap = _business_day_gap(as_of, now_ymd())
            out["age_days"] = float(bd_gap) if bd_gap is not None else None
        except Exception:
            out["age_days"] = None

    issues: List[str] = []
    cautions: List[str] = []
    try:
        stale_max_age_days = int(float(pol.get("stale_max_age_days", 2) or 2))
    except Exception:
        stale_max_age_days = 2
    if out["age_days"] is not None and float(out["age_days"]) > float(stale_max_age_days):
        issues.append(f"snapshot_stale:{out['age_days']}")

    news_chain_healthy = False
    try:
        news_score_obj = json.loads(NEWS_SCORE_STATUS_PATH.read_text(encoding="utf-8")) if NEWS_SCORE_STATUS_PATH.exists() else {}
    except Exception:
        try:
            news_score_obj = json.loads(NEWS_SCORE_STATUS_PATH.read_text(encoding="utf-8-sig")) if NEWS_SCORE_STATUS_PATH.exists() else {}
        except Exception:
            news_score_obj = {}
    try:
        final_score_obj = json.loads(FINAL_SCORE_MERGE_STATUS_PATH.read_text(encoding="utf-8")) if FINAL_SCORE_MERGE_STATUS_PATH.exists() else {}
    except Exception:
        try:
            final_score_obj = json.loads(FINAL_SCORE_MERGE_STATUS_PATH.read_text(encoding="utf-8-sig")) if FINAL_SCORE_MERGE_STATUS_PATH.exists() else {}
        except Exception:
            final_score_obj = {}
    try:
        news_quality = str(news_score_obj.get("quality") or "").strip().upper()
        news_reason = str(_get_dict(news_score_obj, "meta").get("reason") or news_score_obj.get("reason") or "").strip().lower()
        mapped_rows = int(news_score_obj.get("mapped_rows", 0) or 0)
        news_score_rows = int(news_score_obj.get("rows", 0) or 0)
        news_gate_doc = _get_dict(final_score_obj, "news_gate")
        news_gate_quality = str(news_gate_doc.get("quality") or "").strip().upper()
        _news_reason_ok = (
            news_reason in {"", "ok"}
            or news_reason.startswith("candidate_article_coverage_zero")
        )
        news_chain_healthy = (
            news_quality in {"PASS", "WARN"}
            and _news_reason_ok
            and mapped_rows > 0
            and news_score_rows > 0
            and news_gate_quality in {"PASS", "WARN"}
        )
    except Exception:
        news_chain_healthy = False

    try:
        block_stage_statuses = {str(x).upper() for x in list(pol.get("block_stage_statuses") or [])}
    except Exception:
        block_stage_statuses = {"FAIL", "BLOCK", "ERROR"}
    try:
        caution_stage_statuses = {str(x).upper() for x in list(pol.get("caution_stage_statuses") or [])}
    except Exception:
        caution_stage_statuses = {"WARN", "PARTIAL"}
    try:
        ignore_stage_keys = {str(x) for x in list(pol.get("ignore_stage_keys") or [])}
    except Exception:
        ignore_stage_keys = {"audit", "paper_engine"}
    if not ignore_stage_keys:
        ignore_stage_keys = {"audit", "paper_engine"}
    else:
        ignore_stage_keys |= {"paper_engine"}
    try:
        block_calc_issue_states = {str(x).upper() for x in list(pol.get("block_calc_issue_states") or [])}
    except Exception:
        block_calc_issue_states = {"ISSUE"}
    try:
        ignore_calc_issue_keys = {str(x) for x in list(pol.get("ignore_calc_issue_keys") or [])}
    except Exception:
        ignore_calc_issue_keys = {"paper_engine"}
    if not ignore_calc_issue_keys:
        ignore_calc_issue_keys = {"paper_engine"}
    else:
        ignore_calc_issue_keys |= {"paper_engine"}
    try:
        ignore_blocking_issue_keys = {str(x) for x in list(pol.get("ignore_blocking_issue_keys") or [])}
    except Exception:
        ignore_blocking_issue_keys = {"batch_execution", "entry_capacity_zero", "ddm_entry_cap"}

    effective_blocking_rows = (
        obj.get("blocking_issues_effective")
        if isinstance(obj.get("blocking_issues_effective"), list)
        else []
    )
    has_effective_blocking = bool(len(effective_blocking_rows) > 0)

    for row in _get_list(obj, "stage_status"):
        if not isinstance(row, dict):
            continue
        key = str(row.get("key") or "")
        if key == "news_score" and news_chain_healthy:
            continue
        if key == "news_collect" and news_chain_healthy:
            try:
                news_obj = json.loads(NEWS_COLLECT_STATUS_PATH.read_text(encoding="utf-8"))
            except Exception:
                news_obj = {}
            news_collect_reason = str(news_obj.get("reason") or "").strip().lower()
            news_collect_err = int(news_obj.get("error_count", 0) or 0)
            news_collect_fallback = str(news_obj.get("fallback_mode") or "").strip().lower()
            if (
                news_collect_err == 0
                and (
                    news_collect_reason in {"quota_budget_guard", "naver_quota_exceeded"}
                    or "budget_guard_soft_skip" in news_collect_fallback
                )
            ):
                continue
        if key in ignore_stage_keys:
            continue
        status = str(row.get("status") or "").upper()
        # Dashboard status is a display/consumer health signal.  Do not let
        # dashboard WARN/PARTIAL/FAIL become an entry gate caution by itself;
        # real trade blockers must be present in blocking_issues_effective.
        if key == "dashboard" and not has_effective_blocking:
            continue
        if status in block_stage_statuses:
            issues.append(f"stage:{key}:{status}")
        elif status in caution_stage_statuses:
            cautions.append(f"stage:{key}:{status}")

    for row in _get_list(obj, "calc_issue_rows"):
        if not isinstance(row, dict):
            continue
        key = str(row.get("key") or "")
        if key == "news_score" and news_chain_healthy:
            continue
        if key in ignore_calc_issue_keys:
            continue
        st = str(row.get("calc_issue_state") or "").upper()
        if key == "liquidity_filter" and st == "ISSUE":
            cautions.append(f"calc_issue:{key}:{st}")
            continue
        if st in block_calc_issue_states:
            issues.append(f"calc_issue:{key}:{st}")

    blocking_rows = (
        _get_list(obj, "blocking_issues_effective")
        if isinstance(obj.get("blocking_issues_effective"), list)
        else _get_list(obj, "blocking_issues")
    )
    for row in blocking_rows:
        if not isinstance(row, dict):
            continue
        key = str(row.get("key") or "")
        if key == "news_score_quality_gate" and news_chain_healthy:
            continue
        if key in ignore_blocking_issue_keys:
            continue
        if key == "entry_order_runtime":
            cs = str(row.get("current_state") or "")
            if (
                "status_reason=OPEN_POSITION_ACTIVE" in cs
                and "pending_queue=0" in cs
                and "entry_ready=0" in cs
            ):
                continue
        if key == "pending_entry_flow":
            cs = str(row.get("current_state") or "")
            if (
                "status_reason=OPEN_POSITION_ACTIVE" in cs
                and "pending_queue_len=0" in cs
            ):
                continue
        if key == "input_collection_runtime":
            try:
                news_obj = json.loads(NEWS_COLLECT_STATUS_PATH.read_text(encoding="utf-8"))
            except Exception:
                news_obj = {}
            news_status = str(news_obj.get("quality") or news_obj.get("status") or "").upper()
            news_reason = str(news_obj.get("reason") or "").strip().lower()
            news_err = int(news_obj.get("error_count", 0) or 0)
            news_fetched = int(news_obj.get("fetched", 0) or 0)
            news_saved = int(news_obj.get("saved", 0) or 0)
            if news_reason in {"weekend_guard_skip", "holiday_guard_skip"} and news_chain_healthy:
                continue
            if (
                news_status == "PASS"
                and news_reason in {"", "ok"}
                and news_err == 0
                and news_fetched >= news_saved
            ):
                continue
        st = str(row.get("status") or "").upper()
        if st in {"FAIL", "BLOCK", "ERROR", "PARTIAL", "WARN"}:
            issues.append(f"blocking:{key}:{st}")

    out["issues"] = list(dict.fromkeys(issues + cautions))
    if issues:
        out["decision"] = "BLOCK"
        out["reason"] = issues[0]
    elif cautions:
        out["decision"] = "CAUTION"
        out["reason"] = cautions[0]
    else:
        out["decision"] = "ALLOW"
        out["reason"] = "ok"
    return out



def evaluate_macro_news_guard(cfg: Dict[str, Any], run_label: str = "main") -> Dict[str, Any]:
    pol = _get_dict(cfg, "macro_news_guard")
    if not isinstance(pol, dict):
        pol = {}
    enabled = bool(pol.get("enabled", False))
    out: Dict[str, Any] = {
        "enabled": enabled,
        "decision": "ALLOW",
        "reason": "",
        "issues": [],
        "macro_stale_ratio": None,
        "macro_critical_bad": None,
        "news_quality": None,
        "news_quota_guard_stop": None,
    }
    if not enabled:
        out["reason"] = "policy_disabled"
        return out
    if str(run_label or "").strip().lower() == "shadow" and bool(pol.get("shadow_bypass", True)):
        out["decision"] = "ALLOW"
        out["reason"] = "shadow_bypass"
        return out

    issues: List[str] = []
    cautions: List[str] = []

    macro_guard_used = False
    if bool(pol.get("macro_use_critical_guard", True)) and MACRO_SIGNAL_LATEST_PATH.exists():
        try:
            mo = json.loads(MACRO_SIGNAL_LATEST_PATH.read_text(encoding="utf-8"))
            fg = _get_dict(mo, "freshness_guard")
            if fg:
                macro_guard_used = True
                critical_bad = int(float(fg.get("critical_bad", 0) or 0))
                out["macro_critical_bad"] = critical_bad
                try:
                    critical_bad_block = max(1, int(float(pol.get("macro_critical_bad_block", 1) or 1)))
                except Exception:
                    critical_bad_block = 1
                if critical_bad >= critical_bad_block:
                    critical_details = _get_list(fg, "critical_details")
                    bad_critical_details = [
                        r
                        for r in critical_details
                        if isinstance(r, dict)
                        and str(r.get("freshness") or "").upper() != "OK"
                    ]
                    unknown_no_data = (
                        bool(bad_critical_details)
                        and int(len(bad_critical_details)) == int(critical_bad)
                        and all(str((r or {}).get("freshness") or "").upper() == "UNKNOWN" for r in bad_critical_details)
                        and all(int(float((r or {}).get("rows", 0) or 0)) <= 0 for r in bad_critical_details)
                    )
                    unknown_action = str(pol.get("unknown_critical_no_data_action", "BLOCK") or "BLOCK").strip().upper()
                    allowed_unknown_labels = {
                        str(x).strip().lower()
                        for x in (pol.get("unknown_critical_no_data_allowed_run_labels") or [])
                        if str(x).strip()
                    }
                    unknown_label_allowed = (not allowed_unknown_labels) or str(run_label or "").strip().lower() in allowed_unknown_labels
                    if unknown_no_data and unknown_action in {"CAUTION", "WARN"} and unknown_label_allowed:
                        cautions.append(f"macro_critical_unknown_no_data:{critical_bad}")
                    else:
                        issues.append(f"macro_critical_bad:{critical_bad}")
        except Exception as exc:
            cautions.append(f"macro_signal_read_fail:{type(exc).__name__}")

    if not macro_guard_used:
        if RATE_EXTERNAL_STATUS_PATH.exists():
            try:
                ro = json.loads(RATE_EXTERNAL_STATUS_PATH.read_text(encoding="utf-8"))
                fs = _get_dict(ro, "indicator_freshness_summary")
                stale = float(fs.get("stale", 0) or 0)
                total = float(fs.get("total", 0) or 0)
                ratio = (stale / total) if total > 0 else 0.0
                out["macro_stale_ratio"] = ratio
                try:
                    stale_caution = float(pol.get("macro_stale_ratio_caution", 0.5) or 0.5)
                    stale_block = float(pol.get("macro_stale_ratio_block", 0.8) or 0.8)
                except Exception:
                    stale_caution, stale_block = 0.5, 0.8
                if ratio >= stale_block:
                    issues.append(f"macro_stale_ratio_block:{ratio:.2f}")
                elif ratio >= stale_caution:
                    cautions.append(f"macro_stale_ratio_caution:{ratio:.2f}")
            except Exception as exc:
                cautions.append(f"macro_status_read_fail:{type(exc).__name__}")
        else:
            cautions.append("macro_status_missing")

    if NEWS_COLLECT_STATUS_PATH.exists():
        try:
            no = json.loads(NEWS_COLLECT_STATUS_PATH.read_text(encoding="utf-8"))
            news_quality = str(no.get("quality") or "").upper()
            out["news_quality"] = news_quality or None
            quota = _get_dict(no, "quota_budget")
            quota_guard_stop = bool(quota.get("guard_stop") or quota.get("session_guard_stop") or no.get("quota_exhausted_external"))
            out["news_quota_guard_stop"] = quota_guard_stop
            news_reason = str(no.get("reason") or "").strip().lower()
            fallback_mode = str(no.get("fallback_mode") or "").strip().lower()
            quota_soft_skip = (
                news_reason in {"quota_budget_guard", "naver_quota_exceeded"}
                or ("budget_guard_soft_skip" in fallback_mode)
            )
            score_fallback_ok = False
            if quota_soft_skip:
                try:
                    score_obj = (
                        json.loads(NEWS_SCORE_STATUS_PATH.read_text(encoding="utf-8"))
                        if NEWS_SCORE_STATUS_PATH.exists()
                        else {}
                    )
                except Exception:
                    score_obj = {}
                try:
                    final_score_obj = (
                        json.loads(FINAL_SCORE_MERGE_STATUS_PATH.read_text(encoding="utf-8"))
                        if FINAL_SCORE_MERGE_STATUS_PATH.exists()
                        else {}
                    )
                except Exception:
                    final_score_obj = {}
                score_quality = str(score_obj.get("quality") or "").strip().upper()
                score_reason = str(
                    ((score_obj.get("meta") or {}).get("reason"))
                    if isinstance(score_obj.get("meta"), dict)
                    else score_obj.get("reason") or ""
                ).strip().lower()
                mapped_rows = int(score_obj.get("mapped_rows", 0) or 0)
                score_rows = int(score_obj.get("rows", 0) or 0)
                score_nonzero_rows = int(score_obj.get("nonzero_rows", 0) or 0)
                news_gate_doc = _get_dict(final_score_obj, "news_gate")
                news_gate = str(news_gate_doc.get("gate") or final_score_obj.get("news_gate") or "").strip().upper()
                news_gate_quality = str(news_gate_doc.get("quality") or "").strip().upper()
                score_fallback_ok = (
                    score_quality in {"PASS", "WARN"}
                    and score_reason in {"", "ok"}
                    and mapped_rows > 0
                    and score_rows > 0
                    and score_nonzero_rows > 0
                    and news_gate == "OPEN"
                    and news_gate_quality in {"PASS", "WARN"}
                )
                if score_fallback_ok:
                    out["news_quality"] = "PASS_SCORE_FALLBACK"
            if news_reason in {"weekend_guard_skip", "holiday_guard_skip"}:
                try:
                    score_obj = (
                        json.loads(NEWS_SCORE_STATUS_PATH.read_text(encoding="utf-8"))
                        if NEWS_SCORE_STATUS_PATH.exists()
                        else {}
                    )
                except Exception:
                    score_obj = {}
                score_quality = str(score_obj.get("quality") or "").strip().upper()
                score_reason = str(
                    ((score_obj.get("meta") or {}).get("reason"))
                    if isinstance(score_obj.get("meta"), dict)
                    else score_obj.get("reason") or ""
                ).strip().lower()
                mapped_rows = int(score_obj.get("mapped_rows", 0) or 0)
                score_fallback_ok = score_quality == "PASS" and score_reason in {"", "ok"} and mapped_rows > 0
                if score_fallback_ok:
                    out["news_quality"] = "PASS_SCORE_FALLBACK"
            try:
                block_news_quality = {str(x).upper() for x in list(pol.get("news_quality_block_statuses") or [])}
            except Exception:
                block_news_quality = {"FAIL", "BLOCK", "ERROR"}
            if news_quality in block_news_quality and not score_fallback_ok:
                issues.append(f"news_quality:{news_quality}")
            if quota_guard_stop:
                if bool(pol.get("news_quota_guard_block", True)) and not quota_soft_skip:
                    issues.append("news_quota_guard_stop")
                elif not score_fallback_ok:
                    cautions.append("news_quota_guard_stop")
        except Exception as exc:
            cautions.append(f"news_status_read_fail:{type(exc).__name__}")
    else:
        cautions.append("news_status_missing")

    out["issues"] = list(dict.fromkeys(issues + cautions))
    if issues:
        out["decision"] = "BLOCK"
        out["reason"] = issues[0]
    elif cautions:
        out["decision"] = "CAUTION"
        out["reason"] = cautions[0]
    else:
        out["decision"] = "ALLOW"
        out["reason"] = "ok"
    return out



def evaluate_backtest_validation_guard(cfg: Dict[str, Any]) -> Dict[str, Any]:
    pol = _get_dict(cfg, "backtest_validation_guard")
    if not isinstance(pol, dict):
        pol = {}
    enabled = bool(pol.get("enabled", False))
    out: Dict[str, Any] = {
        "enabled": enabled,
        "decision": "ALLOW",
        "reason": "",
        "issues": [],
        "path": str(BACKTEST_VALIDATION_LATEST_PATH),
        "as_of": None,
        "age_days": None,
        "failed_gates": [],
        "validation_mode": "builtin_or_unknown",
        "strategy_source": None,
        "backtest_source": None,
    }
    if not enabled:
        out["reason"] = "policy_disabled"
        return out
    if not BACKTEST_VALIDATION_LATEST_PATH.exists():
        if bool(pol.get("block_on_missing", False)):
            out["decision"] = "BLOCK"
            out["reason"] = "missing_validation_report"
            out["issues"] = ["missing_validation_report"]
        else:
            out["decision"] = "CAUTION"
            out["reason"] = "missing_validation_report"
            out["issues"] = ["missing_validation_report"]
        return out

    try:
        obj = json.loads(BACKTEST_VALIDATION_LATEST_PATH.read_text(encoding="utf-8"))
    except Exception:
        try:
            obj = json.loads(BACKTEST_VALIDATION_LATEST_PATH.read_text(encoding="utf-8-sig"))
        except Exception as exc:
            out["decision"] = "CAUTION"
            out["reason"] = f"read_fail:{type(exc).__name__}"
            out["issues"] = [out["reason"]]
            return out

    try:
        stale_max_age_days = float(pol.get("stale_max_age_days", 3) or 3)
    except Exception:
        stale_max_age_days = 3.0
    try:
        mtime = datetime.fromtimestamp(BACKTEST_VALIDATION_LATEST_PATH.stat().st_mtime)
        age_days = (datetime.now() - mtime).total_seconds() / 86400.0
        out["as_of"] = mtime.strftime("%Y%m%d")
        out["age_days"] = round(age_days, 4)
        if age_days > stale_max_age_days:
            out["decision"] = "CAUTION"
            out["reason"] = f"stale:{age_days:.2f}d"
            out["issues"] = [out["reason"]]
    except Exception:
        pass

    integration = _get_dict(obj, "integration")
    if not integration:
        artifacts = _get_dict(obj, "artifacts")
        alt_integration = artifacts.get("integration")
        if isinstance(alt_integration, dict):
            integration = alt_integration
    strategy_source = str(integration.get("strategy_source") or "")
    backtest_source = str(integration.get("backtest_source") or "")
    out["strategy_source"] = strategy_source or None
    out["backtest_source"] = backtest_source or None
    is_real_strategy_validation = (
        "backtest_real_strategy_adapter.py:real_strategy_signal" in strategy_source
        or "backtest_real_strategy_adapter.py:real_strategy_backtest" in backtest_source
    )
    out["validation_mode"] = "real_strategy" if is_real_strategy_validation else "builtin_or_unknown"

    gate_results = _get_list(obj, "gate_results")
    if not gate_results and isinstance(obj.get("gates"), list):
        gate_results = obj.get("gates")
    failed_names: List[str] = []
    for g in gate_results:
        if not isinstance(g, dict):
            continue
        if bool(g.get("passed", False)):
            continue
        name = str(g.get("name") or "").strip()
        if name:
            failed_names.append(name)
    out["failed_gates"] = sorted(set(failed_names))

    block_names = {str(x).strip() for x in list(pol.get("block_gate_names") or []) if str(x).strip()}
    caution_names = {str(x).strip() for x in list(pol.get("caution_gate_names") or []) if str(x).strip()}
    if is_real_strategy_validation:
        caution_names = caution_names.union(block_names).union({"signal_quality_ic_ir"})
        block_names = set()
    hit_block = sorted([x for x in failed_names if x in block_names])
    hit_caution = sorted([x for x in failed_names if x in caution_names])
    issues: List[str] = []
    if hit_block:
        issues.extend([f"gate_block:{x}" for x in hit_block])
    if hit_caution:
        issues.extend([f"gate_caution:{x}" for x in hit_caution])

    overall_passed = bool(obj.get("passed", True))
    if (not overall_passed) and bool(pol.get("caution_on_overall_fail", True)):
        issues.append("overall_fail")

    if hit_block:
        out["decision"] = "BLOCK"
        out["reason"] = issues[0] if issues else "gate_block"
    elif hit_caution or ((not overall_passed) and bool(pol.get("caution_on_overall_fail", True))):
        if str(out.get("decision") or "").upper() != "BLOCK":
            out["decision"] = "CAUTION"
        if not str(out.get("reason") or ""):
            out["reason"] = issues[0] if issues else "overall_fail"
    else:
        if str(out.get("decision") or "").upper() == "ALLOW":
            out["reason"] = "ok"
    out["issues"] = list(dict.fromkeys([str(x) for x in issues if str(x)]))
    return out


def _detect_explicit_market_events(
    event_doc: Any,
    candidate_df: pd.DataFrame,
    policy: Dict[str, Any],
    runtime_ymd: str,
) -> Dict[str, Any]:
    guard = _get_dict(policy, "explicit_market_event_guard")
    if not bool(guard.get("enabled", True)):
        return {"enabled": False, "blocked": False, "blocked_codes": [], "market_wide": False, "events": []}
    block_types = {
        str(x).strip().upper()
        for x in (guard.get("block_event_types") or ["VI", "TRADING_HALT", "CIRCUIT_BREAKER", "CB"])
        if str(x).strip()
    }
    market_wide_types = {
        str(x).strip().upper()
        for x in (guard.get("market_wide_event_types") or ["CIRCUIT_BREAKER", "CB"])
        if str(x).strip()
    }
    candidate_codes = set()
    if isinstance(candidate_df, pd.DataFrame) and "code" in candidate_df.columns:
        candidate_codes = {norm_code(v) for v in candidate_df["code"].astype(str).tolist()}
    events: List[Dict[str, Any]] = []
    blocked_codes: set[str] = set()
    market_wide = False
    runtime_text = _norm_ymd_text(runtime_ymd) or now_ymd()
    for row in _event_doc_rows(event_doc):
        raw_date = _norm_ymd_text(row.get("as_of_ymd") or row.get("date") or row.get("event_date") or row.get("ymd"))
        if raw_date and raw_date != runtime_text:
            continue
        raw_type = str(row.get("event_type") or row.get("type") or row.get("reason") or row.get("category") or row.get("event") or "").strip().upper()
        raw_status = str(row.get("status") or row.get("state") or row.get("level") or row.get("market_event_level") or "").strip().upper()
        text = " ".join(str(row.get(k) or "") for k in ("title", "message", "note", "reason", "event_type", "type", "status", "level")).upper()
        text_tokens = {tok for tok in re.split(r"[^A-Z0-9_]+", text) if tok}
        matched = {
            term
            for term in block_types
            if term
            and (
                term == raw_type
                or term == raw_status
                or (len(term) <= 2 and term in text_tokens)
                or (len(term) > 2 and term in text)
            )
        }
        if not matched:
            continue
        code = norm_code(row.get("code", ""))
        event_record = {
            "event_type": sorted(matched),
            "code": code,
            "market_wide": bool((not code) or bool(matched & market_wide_types) or str(row.get("scope") or "").strip().upper() in {"MARKET", "MARKET_WIDE", "ALL"}),
            "raw": {k: row.get(k) for k in ("as_of_ymd", "date", "code", "event_type", "type", "status", "level", "scope", "reason", "title") if k in row},
        }
        events.append(event_record)
        if event_record["market_wide"]:
            market_wide = True
        elif code and (not candidate_codes or code in candidate_codes):
            blocked_codes.add(code)
    return {
        "enabled": True,
        "blocked": bool(market_wide or blocked_codes),
        "blocked_codes": sorted(blocked_codes),
        "market_wide": bool(market_wide),
        "events": events,
    }


def count_kill_switch_streak_days(log_dir: Path, max_scan_days: int = 30) -> int:
    """Count consecutive days where risk_off was enabled by kill_switch (non-hard-data reasons)."""
    files = sorted(log_dir.glob("p0_daily_check_*.json"), key=lambda p: p.name, reverse=True)
    seen_days: set[str] = set()
    streak = 0
    for p in files:
        m = re.search(r"p0_daily_check_(\d{8})_", p.name)
        if not m:
            continue
        ymd = m.group(1)
        if ymd in seen_days:
            continue
        seen_days.add(ymd)
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        ro = _get_dict(obj, "risk_off")
        enabled = bool(ro.get("enabled"))
        reasons = _get_list(ro, "reasons")
        reasons = [str(x) for x in reasons]
        has_kill = "kill_switch" in reasons
        hard = any(_is_hard_block_reason(r) for r in reasons)
        if enabled and has_kill and not hard:
            streak += 1
            if streak >= int(max_scan_days):
                break
            continue
        break
    return int(streak)

def compute_adaptive_kill_cap(base_max_new: int, cfg: Dict[str, Any], p0_snapshot: Dict[str, Any], streak_days: int) -> Optional[Tuple[int, str]]:
    aec = cfg.get("adaptive_entry_control", {}) if isinstance(cfg, dict) else {}
    if not isinstance(aec, dict) or not bool(aec.get("enabled", False)):
        return None

    ks = _get_dict(p0_snapshot, "kill_switch")
    metrics = _get_dict(ks, "metrics")
    limits = _get_dict(ks, "limits")
    reasons = _get_list(ks, "reasons")
    reasons = [str(x) for x in reasons]
    hard_metrics = _get_dict(metrics, "hard_trigger_metrics")
    account_basis = _get_dict(metrics, "account_basis")
    account_ok = str(account_basis.get("status") or "").upper() == "PASS"

    day_loss_raw = hard_metrics.get("daily_loss_pct", hard_metrics.get("last_day_ret"))
    if day_loss_raw is None and account_ok:
        day_loss_raw = account_basis.get("daily_loss_pct", account_basis.get("last_day_ret"))
    if day_loss_raw is None:
        day_loss_raw = metrics.get("last_day_ret")

    dd_raw = hard_metrics.get("max_drawdown_pct")
    dd_basis = str(metrics.get("hard_trigger_basis") or "")
    if dd_raw is None and account_ok:
        dd_raw = account_basis.get("max_drawdown_pct")
        dd_basis = str(account_basis.get("basis") or "account_equity")
    if dd_raw is None:
        dd_raw = metrics.get("max_drawdown_pct")
        dd_basis = str(dd_basis or "strategy_return_curve_fallback")

    day_loss = abs(min(0.0, _to_float(day_loss_raw, 0.0)))
    day_lim = abs(_to_float(limits.get("max_daily_loss_pct"), 0.0))
    day_ratio = (day_loss / day_lim) if day_lim > 0 else 0.0
    hard_daily_loss_active = bool(metrics.get("hard_daily_loss_active", metrics.get("daily_loss_active", True)))
    daily_loss_hit = bool(any(is_daily_loss_reason(r) for r in reasons) or (hard_daily_loss_active and day_ratio >= 1.0))

    r_soft = _to_float(aec.get("dd_ratio_soft"), 1.00)
    r_mid = _to_float(aec.get("dd_ratio_mid"), 1.10)
    r_hard = _to_float(aec.get("dd_ratio_hard"), 1.25)
    f_soft = _to_float(aec.get("reduce_soft"), 0.50)
    f_mid = _to_float(aec.get("reduce_mid"), 0.30)
    f_hard = _to_float(aec.get("reduce_hard"), 0.15)
    probe_min = max(1, _to_int(aec.get("probe_min_new"), 1))
    relief_after = max(1, _to_int(aec.get("relief_after_streak_days"), 3))
    relief_min = max(1, _to_int(aec.get("relief_min_new"), 1))
    daily_recovery_ratio = max(1.0, _to_float(aec.get("daily_loss_recovery_ratio"), 1.10))
    daily_hard_block_ratio = max(daily_recovery_ratio, _to_float(aec.get("daily_loss_hard_block_ratio"), 1.25))

    if daily_loss_hit:
        if day_ratio >= daily_hard_block_ratio:
            detail = (
                f"daily_loss_hard_block ratio={day_ratio:.3f} "
                f"hard_block_ratio={daily_hard_block_ratio:.3f} "
                f"daily_loss={_to_float(day_loss_raw, 0.0):.4f} "
                f"active={bool(hard_daily_loss_active)} "
                f"basis={str(hard_metrics.get('daily_loss_basis') or dd_basis or 'fallback')}"
            )
            return 0, detail
        # auto-relief: streak 충족 또는 손실비율이 회복구간이면 소량 재개
        if int(streak_days) >= relief_after or day_ratio <= daily_recovery_ratio:
            base = max(0, int(base_max_new))
            if base <= 0:
                return 0, "base_max_new=0"
            probe = max(relief_min, probe_min)
            cap = min(base, max(1, probe))
            detail = (
                f"daily_loss_relief ratio={day_ratio:.3f} "
                f"recovery_ratio={daily_recovery_ratio:.3f} "
                f"streak={int(streak_days)} cap={int(cap)}"
            )
            return int(cap), detail
        detail = (
            f"daily_loss_block ratio={day_ratio:.3f} "
            f"recovery_ratio={daily_recovery_ratio:.3f} "
            f"streak={int(streak_days)}<relief_after={int(relief_after)}"
        )
        return 0, detail

    dd = abs(_to_float(dd_raw, 0.0))
    dd_lim = abs(_to_float(limits.get("max_drawdown_pct"), 0.25))
    if dd_lim <= 0:
        return None
    dd_ratio = dd / dd_lim

    if dd_ratio >= r_hard:
        factor = f_hard
        band = "HARD"
    elif dd_ratio >= r_mid:
        factor = f_mid
        band = "MID"
    elif dd_ratio >= r_soft:
        factor = f_soft
        band = "SOFT"
    else:
        factor = min(1.0, max(f_soft, 0.0))
        band = "BELOW_SOFT"

    base = max(0, int(base_max_new))
    if base <= 0:
        return 0, "base_max_new=0"
    cap = int(math.floor(base * max(0.0, min(1.0, factor))))
    cap = max(probe_min, cap)
    if int(streak_days) >= relief_after:
        cap = max(cap, relief_min)
    cap = min(base, cap)
    detail = f"dd_ratio={dd_ratio:.3f} band={band} factor={factor:.2f} streak={int(streak_days)} basis={dd_basis or 'fallback'}"
    return int(cap), detail
