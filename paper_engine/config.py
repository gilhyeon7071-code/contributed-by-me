"""Configuration loading and defaults for paper_engine.

This module holds the default configuration dictionary and the loader that
merges on-disk ``paper_engine_config.json`` with environment overrides.
It was split out from the legacy ``paper_engine.py`` as the first step of
the module refactor.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict

from paper_engine.common import _path_from_env

# Base directory is the project root (parent of the paper_engine package).
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "2_Logs"
PAPER_DIR = BASE_DIR / "paper"


CONFIG_PATH = _path_from_env("PAPER_CONFIG_PATH", PAPER_DIR / "paper_engine_config.json")


def _deep_merge_dict(base: Dict[str, Any], extra: Dict[str, Any]) -> Dict[str, Any]:
    for k, v in extra.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge_dict(base[k], v)
        else:
            base[k] = v
    return base


def _parse_override_value(raw: str) -> Any:
    s = str(raw).strip()
    low = s.lower()
    if low in ("true", "false"):
        return low == "true"
    if low in ("none", "null"):
        return None
    if re.fullmatch(r"[-+]?\d+", s):
        try:
            return int(s)
        except Exception:
            return s
    if re.fullmatch(r"[-+]?\d*\.\d+(e[-+]?\d+)?", s, flags=re.IGNORECASE) or re.fullmatch(r"[-+]?\d+e[-+]?\d+", s, flags=re.IGNORECASE):
        try:
            return float(s)
        except Exception:
            return s
    return s


def _set_nested_key(target: Dict[str, Any], dotted_key: str, value: Any) -> None:
    parts = [p for p in str(dotted_key).split(".") if p]
    if not parts:
        return
    cur: Dict[str, Any] = target
    for p in parts[:-1]:
        nxt = cur.get(p)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[p] = nxt
        cur = nxt
    cur[parts[-1]] = value


def _apply_env_kv_overrides(cfg: Dict[str, Any]) -> Dict[str, Any]:
    raw = str(os.getenv("PAPER_CFG_OVERRIDES", "") or "").strip()
    if not raw:
        return cfg
    applied: list[str] = []
    for chunk in raw.split(";"):
        part = chunk.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        key = str(k).strip()
        if not key:
            continue
        _set_nested_key(cfg, key, _parse_override_value(v))
        applied.append(key)
    if applied:
        print(f"[CONFIG] kv overrides applied: keys={sorted(applied)}")
    return cfg


def _apply_env_config_overrides(cfg: Dict[str, Any]) -> Dict[str, Any]:
    raw = str(os.getenv("PAPER_CFG_OVERRIDES_JSON", "") or "").strip()
    if raw:
        try:
            overrides = json.loads(raw)
            if isinstance(overrides, dict):
                _deep_merge_dict(cfg, overrides)
                print(f"[CONFIG] env overrides applied: keys={sorted(overrides.keys())}")
            else:
                print("[CONFIG] PAPER_CFG_OVERRIDES_JSON must be object; ignored")
        except Exception as e:
            print(f"[CONFIG] invalid PAPER_CFG_OVERRIDES_JSON ignored: {type(e).__name__}")
    return _apply_env_kv_overrides(cfg)


DEFAULT_CONFIG: Dict[str, Any] = {
    "max_new_trades_per_day": 3,
    "fixed_qty": 1,
    "max_hold_days": 10,                 # default holding period for time-based exit.
    "min_hold_days": 2,                  # min trading days before fundamental/time exits
    "min_hold_protect_stop_loss": True,  # when False, STOP/TRAIL/PREEMPTIVE are active even during min_hold
    "atr_stop_multiplier": 2.0,         # if >0, use -(atr14_pct * multiplier) as stop (0=disabled)
    "allow_same_code_reentry": False,
    "entry_timing_mode": "next_open",    # next_open | same_close
    "gap_up_max_pct": 0.0,               # 0 = disabled; >0 limits next-session gap-up entry.
    "entry_gap_up_reduce": {
        "enabled": False,
        "threshold_pct": 0.05,
        "qty_multiplier": 0.5,
        "min_qty": 1,
    },
    "entry_gap_down_stop_pct": 0.0,      # 0 = disabled; >0 = T?? ?? T+1?? ???(??) ??
    "entry_gap_risk_guard": {
        "enabled": False,
        "lookback_sessions": 20,
        "down_gap_threshold_pct": 0.08,
        "max_down_gap_count": 0,
    },

    # Transaction cost defaults.
    "fee_pct": 0.005,
    "slippage_pct": 0.001,
    "tiered_slippage": {
        "enabled": False,
        "large_cap_krw": 1_000_000_000_000,
        "mid_cap_krw":   300_000_000_000,
        "large_slip_pct": 0.003,
        "mid_slip_pct":   0.005,
        "small_slip_pct": 0.010,
    },

    # tax (optional, default 0)
    "sell_tax_pct": 0.0,
    "entry_liquidity_check": {
        "enabled": False,
        "min_trading_value_krw": 1_000_000_000,   # 10억
    },
    "normal_entry_execution_quality": {
        "enabled": True,
        "require_lob": True,
        "max_spread_bps": 30.0,
        "require_executable_qty": True,
        "min_executable_qty_ratio": 1.0,
        "max_depth_levels": 10,
        "min_markout_1step_bps": 0.0,
        "block_on_missing_markout": True,
        "orderflow_tag_block_tags": ["PAUSE", "NO_LOB"],
        "qty_reduction": {
            "enabled": False,
            "spread_bands": [
                {"min_bps": 20.0, "multiplier": 0.75},
                {"min_bps": 25.0, "multiplier": 0.5},
            ],
            "markout_bands": [
                {"max_bps": 5.0, "multiplier": 0.75},
            ],
            "orderflow_caution_multiplier": 0.75,
            "orderflow_caution_reduce_tags": ["CAUTION"],
            "multi_condition": {
                "two_or_more_multiplier": 0.5,
            },
        },
    },
    "normal_candidate_staleness_check": {
        "enabled": True,
        "max_stale_days": 3,
        "block_if_stale": False,
    },
    "normal_realtime_gap_policy": {
        "enabled": True,
        "close_auction": {
            "enabled": True,
            "block_close_pos_min": 0.50,
            "reduce_close_pos_min": 0.70,
            "max_day_range_pct": 0.07,
            "block_v_accel_min": 1.0,
            "reduce_v_accel_min": 1.5,
            "reduce_qty_multiplier": 0.5,
        },
        "intraday_momentum_recheck": {
            "enabled": True,
            "require_intraday": True,
            "block_v_accel_min": 1.0,
            "reduce_v_accel_min": 1.5,
            "min_value_ratio": 0.70,
            "reduce_qty_multiplier": 0.5,
            "min_reduced_qty": 2,
            "min_reduced_notional_krw": 0,
        },
        "overnight_gap_exit": {
            "enabled": True,
            "gap_down_exit_pct": 0.03,
            "gap_up_take_profit_pct": 0.12,
            "gap_down_sell_ratio_pct": 100.0,
            "gap_up_sell_ratio_pct": 100.0,
        },
        "dynamic_slippage": {
            "enabled": True,
            "low_trading_value_krw": 3_000_000_000,
            "very_low_trading_value_krw": 1_000_000_000,
            "low_trading_value_multiplier": 1.5,
            "very_low_trading_value_multiplier": 2.0,
            "low_turnover_ratio": 0.003,
            "low_turnover_multiplier": 1.5,
            "max_slippage_pct": 0.03,
        },
    },
    "limit_price_tolerance": {
        "enabled": False,
        "max_deviation_pct": 0.005,   # 신호 종가 대비 ±0.5%
    },
    "split_entry": {
        "enabled": False,
        "first_ratio": 0.5,
        "surge_first_ratio": 0.3,
        "second_dip_min_pct": 0.01,   # 1차 진입가 대비 -1% 이상 하락
        "second_dip_max_pct": 0.03,   # 1차 진입가 대비 -3% 이상은 진입 안 함
        "second_entry_max_days": 1,
        "budget_alloc_pct": 0.18,
        "allow_split_second_carryover": True,
        "second_confirmation": {
            "enabled": True,
            "max_v_accel": 3.1520902710607213,
            "max_ret1_pct": 8.849700179324415,
            "max_atr14_pct": 0.09907047892735515,
            "missing_feature_action": "ALLOW_WITH_NOTE",
        },
    },
    "sector_rebalance": {
        "enabled": False,
        "limit_pct": 0.40,       # 업종 집중도 상한 (현재가 기준)
        "sell_ratio_pct": 30.0,  # 초과 시 해당 업종 최약 포지션 부분 청산 비율
    },

    "news_implication_entry_policy": {
        "enabled": True,
        "block_observe_only": False,
        "reduce_size_observe_only": False,
        "reduce_size_multiplier": 0.5,
        "watch_observe_only": False,
    },
    "news_signal_shadow_entry_policy": {
        "enabled": False,
        "observe_only": True,
        "path": str(LOG_DIR / "news_signal_shadow_stage_latest.csv"),
        "allow_stages": ["CONFIRMED_SHADOW"],
        "block_stages": ["BLOCK_SHADOW", "PUBLISHER_UNVERIFIED_SHADOW"],
    },

    # Candidate input path.
    "candidates_latest_data": str(LOG_DIR / "candidates_latest_data.csv"),

    # Parquet price source.
    "parquet_root": str(BASE_DIR),
    "parquet_top_n_recent": 120,
    "parquet_max_open_files": 30,
    "max_gross_exposure_pct": 1.0,       # 0~1 (or 0~100); cap on open+new notional / capital_total
    "max_daily_new_exposure_pct": 1.0,   # 0~1 (or 0~100); cap on same-day newly deployed notional
    "capital_budget_policy": {
        "enabled": True,
        "gross_exposure_pct": 0.55,
        "basic_alloc_pct": 0.40,
        "basic_target_positions": 18,
        "surge_alloc_pct": 0.06,
        "split_alloc_pct": 0.18,
        "recovery_alloc_pct": 0.10,
        "reserve_alloc_pct": 0.20,
        "caution_gross_exposure_pct": 0.55,
        "defensive_floor_exposure_pct": 0.45,
        "defensive_max_new": 0,
        "recovery_enabled": False,
        "reserve_trade_enabled": False,
    },
    "t2_settlement_cash": {
        "enabled": True,
        "settlement_lag_business_days": 2,
        "weekend_only_calendar": True,
        "block_on_insufficient_settled_cash": True,
        "auto_initialize_from_capital": True,
        "ledger_max_rows": 500,
    },
    "max_per_sector": 0,
    "sector_correlation_guard": {
        "enabled": True,
        "lookback_days": 120,
        "reduce_threshold_abs_corr": 0.85,
        "block_threshold_abs_corr": 0.97,
        "reduce_qty_multiplier": 0.75,
        "allow_block": False,
        "risk_budget_engine": "EWMA_HRP",
        "ewma_halflife_days": 60,
        "hrp_enabled": True,
        "hrp_overweight_tolerance": 1.10,
        "hrp_min_qty_multiplier": 0.35,
    },
    "union_entry_strength_min": 0.65,    # min sector_strength for SECTOR_PREFILTER_UNION conditional inclusion
    "positive_entry_criteria": {
        "enabled": True,
        "hard_filter": True,
        "min_score": 0.0,
        "score_columns": ["final_score", "score"],
        "require_execution_pool_when_present": True,
        "require_sector_entry_when_present": True,
        "allow_sector_union": True,
    },
    "normal_intraday_realtime_policy": {
        "enabled": True,
        "block_when_entry_gate_not_allow": True,
        "blocked_entry_gate_decisions": ["BLOCK"],
        "allow_dd_stop_validation_reduce": False,
        "block_when_p0_rolling_dd_ge_threshold": False,
        "p0_rolling_dd_block_pct": 0.10,
    },
    # Adaptive operations policy:
    # avoid permanent all-stop by scaling entry allowance from live risk metrics.
    "adaptive_entry_control": {
        "enabled": True,
        "kill_switch_override_block": True,   # allow dynamic REDUCE even if kill_switch.mode=BLOCK
        "dd_ratio_soft": 1.00,                # |dd| / limit threshold
        "dd_ratio_mid": 1.10,
        "dd_ratio_hard": 1.25,
        "reduce_soft": 0.50,                  # applied to base max_new
        "reduce_mid": 0.30,
        "reduce_hard": 0.15,
        "probe_min_new": 1,                   # never zero for non-hard-data kill_switch day
        "relief_after_streak_days": 3,        # if blocked/reduced for N consecutive days
        "relief_min_new": 1,                  # keep small participation alive
        "dynamic_relax_l5_factor": 0.10,      # L5 cap scales with base max_new
    },
    # Risk orchestration for regime-adaptive sizing (vol-target + fractional Kelly + DD taper).
    "risk_orchestration": {
        "enabled": False,
        "lookback_trades": 60,
        "target_vol": 0.03,
        "f_kelly_cap": 1.5,
        "c_default": 0.35,
        "c_min": 0.25,
        "c_max": 0.50,
        "vol_ratio_cap": 3.0,
        "max_scale": 1.0,
        "edge_zero_floor_scale": 0.25,
        "regime_confidence": {
            "RALLY": 0.45,
            "NORMAL": 0.40,
            "BEAR": 0.30,
            "CRASH": 0.25,
            "STAGFLATION": 0.25,
            "RATE_HIKE_FEAR": 0.30
        },
        "dd_taper": {
            "dd_cap": 0.10,
            "dd_stop": 0.15,
            "account_clear_strategy_dd_override": {
                "enabled": False
            }
        },
        "account_clear_min_scale": {
            "enabled": True,
            "min_scale": 0.25,
            "require_dd_advisory_only": True
        },
        "dd_stop_validation": {
            "enabled": True,
            "mode": "validation_reduce",
            "max_new": 1,
            "position_size_multiplier": 0.25,
            "require_dd_stop": True,
            "allowed_run_labels": ["main", "validation", "tuning"],
            "max_positions_full_override": {
                "enabled": False,
                "mode": "gross_exposure_cap"
            }
        },
        "es_gate": {
            "enabled": True,
            "limit": 0.05,
            "hard_block": False,
            "reduction_factor": 0.5,
            "by_regime": {
                "CRASH": {"limit": 0.04, "hard_block": True, "reduction_factor": 0.0},
                "STAGFLATION": {"limit": 0.045, "hard_block": True, "reduction_factor": 0.0},
                "BEAR": {"limit": 0.05, "hard_block": False, "reduction_factor": 0.35},
                "RALLY": {"limit": 0.06, "hard_block": False, "reduction_factor": 0.6},
                "NORMAL": {"limit": 0.05, "hard_block": False, "reduction_factor": 0.5},
                "RATE_HIKE_FEAR": {"limit": 0.05, "hard_block": False, "reduction_factor": 0.4}
            }
        }
    },
    "production_risk_playbook": {
        "enabled": True,
        "artifact_path": str(LOG_DIR / "production_risk_playbook_latest.json"),
        "missing_action": "ALLOW",
        "watch_entry_decision": "CAUTION",
        "soft_entry_decision": "REDUCE",
        "hard_entry_decision": "BLOCK",
        "soft_size_multiplier": 0.5,
        "hard_size_multiplier": 0.0,
        "max_age_minutes": 30,
    },
    # Regime policy:
    # - NORMAL: default behavior
    # - RALLY: allow small probe even under kill_switch BLOCK (non-hard-data only)
    # - CRASH: force block new entries
    "regime_entry_policy": {
        "enabled": True,
        "rally_day_ret_min": 0.025,
        "rate_hike_fear_reduce_day_ret_floor": -0.015,
        "crash_day_ret_max": -0.025,
        "p0_bear_promote_enabled": True,
        "p0_bear_allowed_macro_regimes": ["NORMAL", "RECOVERY", "SIDEWAYS", "VOLATILE"],
        "rate_hike_fear_as_crash": False,
        "rate_hike_fear_crash_conditions": {
            "daily_return_threshold": -1.50,  # percent scale
            "require_gate_block": True,
            "require_risk_off": True,
        },
        "allow_rally_on_macro_volatile": True,
        "allow_rally_when_macro_risk_off": True,
        "gate_daily_prefer": True,
        "gate_daily_max_age_days": 2,
        "rally_probe_under_kill_switch_block": True,
        "rally_probe_max_new": 1,
        "rally_max_per_sector": 1,
        "rally_max_gross_exposure_pct": 0.60,
        "rally_max_daily_new_exposure_pct": 0.15,
        "rally_gap_up_max_pct": 0.015,
        "rally_entry_gap_down_stop_pct": 0.03,
        "crash_force_block": True,
    },
    "regime_overrides": {
        "RALLY": {
            "max_gross_exposure_pct": 0.60,
            "max_daily_new_exposure_pct": 0.15,
            "gap_up_max_pct": 0.08,
            "stop_loss_pct": -0.04,
            "capital_budget_policy": {
                "gross_exposure_pct": 0.60,
                "surge_alloc_pct": 0.45,
                "split_alloc_pct": 0.20,
            },
            "surge_entry_policy": {
                "total_alloc_pct": 0.45,
                "max_new_surge": 6,
            },
            "surge_exit_policy": {
                "stop_loss_pct": -0.04,
                "max_hold_days": 5,
            },
        },
        "NORMAL": {
            "max_hold_days": 15,
            "stop_loss_pct": -0.07,
            "capital_budget_policy": {
                "gross_exposure_pct": 0.55,
                "surge_alloc_pct": 0.15,
                "split_alloc_pct": 0.40,
            },
            "split_entry": {
                "enabled": True,
                "budget_alloc_pct": 0.40,
            },
            "surge_entry_policy": {
                "total_alloc_pct": 0.15,
                "max_new_surge": 2,
            },
            "sell_rules": {
                "stop_loss": {
                    "default_pct": -7.0,
                },
            },
        },
        "BEAR": {
            "max_hold_days": 15,
            "stop_loss_pct": -0.07,
            "capital_budget_policy": {
                "gross_exposure_pct": 0.55,
                "surge_alloc_pct": 0.15,
                "split_alloc_pct": 0.40,
            },
            "split_entry": {
                "enabled": True,
                "budget_alloc_pct": 0.40,
            },
            "surge_entry_policy": {
                "total_alloc_pct": 0.15,
                "max_new_surge": 2,
            },
            "sell_rules": {
                "stop_loss": {
                    "default_pct": -7.0,
                },
            },
        },
        "CRASH": {
            "max_new_trades_per_day": 0,
            "max_gross_exposure_pct": 0.0,
            "max_daily_new_exposure_pct": 0.0,
            "capital_budget_policy": {
                "gross_exposure_pct": 0.0,
                "basic_alloc_pct": 0.0,
                "surge_alloc_pct": 0.0,
                "split_alloc_pct": 0.0,
                "recovery_alloc_pct": 0.0,
            },
            "split_entry": {
                "budget_alloc_pct": 0.0,
            },
            "surge_entry_policy": {
                "total_alloc_pct": 0.0,
                "max_new_surge": 0,
            },
        },
    },
    "trend_overlay_2026": {
        "enabled": True,
        "cutting_hawkish_max": -0.20,
        "seasonal_risk_multiplier": {
            "Q1": 1.00,
            "Q2": 1.00,
            "Q3": 0.85,
            "Q4": 0.75
        },
        "cutting_overlay": {
            "growth_entry_weight": 1.15,
            "defensive_entry_weight": 0.90,
            "growth_codes": [],
            "defensive_codes": []
        },
        "ai_semiconductor_overlay": {
            "daily_top_n": 2,
            "max_open_positions": 2,
            "single_name_cap_pct": 0.15,
            "semiconductor_codes": [],
            "ai_software_codes": [],
            "it_hardware_codes": []
        }
    },
    "fx_entry_policy": {
        "enabled": True,
        "crisis_force_block": True,
        "extreme_vol_force_block": True,
        "hard_block_requires_crisis_level": False,
        "high_vol_reduce_max_new_to": 1,
        "three_day_extreme_force_defensive": True,
        # 현실화: level_band 기준(1300/1380/1450/1520)과 정렬
        "weak_fx_export_bias_level": 1450.0,    # 구 1480 → 1450 (FAIR/WEAK 경계)
        "strong_fx_domestic_bias_level": 1380.0, # 구 1420 → 1380 (STRONG/FAIR 경계)
        "crisis_fx_level": 1520.0,               # level_band CRISIS 기준(>1520)과 정렬
        "daily_abs_change_block_level": 10.0,
        "direction_flat_band": 0.5,              # 환율 상승/하락/보합 구분(원)
        "stable_anchor_source": "ma20",          # 기준환율 앵커: ma20|ma60|level
        "stable_anchor_deviation_band": 25.0,    # 앵커 근처 허용 편차(원)
        "bias_keep_nonnegative_fx_score_only": True,
    },
    "p1_entry_policy": {
        "enabled": True,
        "calendar": {
            "enabled": True,
            "block_dates": [],
            "reduce_dates": [],
            "reduce_max_new_cap": 1,
            "option_expiry_reduce_enabled": True,
            "option_expiry_week_of_month": 2,
            "option_expiry_weekday": 3,
            "option_expiry_max_new_cap": 1,
        },
        "intraday": {
            "enabled": False,
            "morning_start_hhmm": 900,
            "morning_end_hhmm": 1000,
            "morning_sector_strength_min": 0.80,
            "lunch_start_hhmm": 1130,
            "lunch_end_hhmm": 1330,
            "lunch_max_new_cap": 1,
            "power_hour_start_hhmm": 1430,
            "power_hour_end_hhmm": 1530,
        },
        "event_gate": {
            "enabled": True,
            "events_file": str(LOG_DIR / "market_event_gate_latest.json"),
            "auto_stub_when_missing": True,
            "high_risk_levels": ["HIGH", "CRITICAL"],
            "high_risk_action": "REDUCE",
            "high_risk_max_new_cap": 1,
            "explicit_market_event_guard": {
                "enabled": True,
                "block_event_types": ["VI", "TRADING_HALT", "CIRCUIT_BREAKER", "CB"],
                "market_wide_event_types": ["CIRCUIT_BREAKER", "CB"],
                "block_action": "BLOCK",
                "fail_closed_on_parse_error": True,
            },
        },
        "technical_gate": {
            "enabled": True,
            "min_pool_size": 8,
            "rsi_min": 45.0,
            "rsi_max": 75.0,
            "macd_golden_min_ratio": 0.05,
            "volcorr_min": 0.03,
            "low_quality_action": "REDUCE",
            "low_quality_max_new_cap": 1,
        },
    },
    "horizon_entry_policy": {
        "enabled": True,
        "default_label": "MID",
        "label_rules": {
            # Fast momentum breakout profile.
            "SHORT": {
                "min_final_score": 0.03,
                "min_v_accel": 1.0,
                "min_rs_slope": 6.0,
                "entry_weight": 0.80,
                "max_hold_days": 7,
            },
            # Volatility expansion profile.
            "SWING": {
                "min_final_score": 0.02,
                "min_atr14_pct": 0.06,
                "min_stoch_k": 65.0,
                "entry_weight": 0.70,
                "max_hold_days": 12,
            },
            # Balanced trend profile.
            "MID": {
                "min_final_score": 0.02,
                "entry_weight": 1.00,
                "max_hold_days": 20,
            },
            # Slow trend / low-vol profile.
            "LONG": {
                "min_final_score": 0.015,
                "max_atr14_pct": 0.08,
                "entry_weight": 1.00,
                "max_hold_days": 30,
            },
        },
    },
    "surge_entry_policy": {
        "enabled": True,
        "realtime_only": True,
        "top_n": 3,
        "min_score_final": 75.0,
        "max_new_surge": 2,
        "per_symbol_alloc_pct": 0.02,
        "total_alloc_pct": 0.06,
        "min_qty": 1,
        "realtime_alert_max_age_minutes": 10,
        "type_policy": {
            "enabled": True,
            "allowed_types": ["PRICE_VOL_BREAKOUT", "PRICE_RANGE_BREAKOUT", "REG_SHORT_5D60", "REG_MID_15D100"],
            "blocked_types": ["LIMIT_UP_NEAR"],
            "type_qty_multiplier": {
                "PRICE_VOL_BREAKOUT": 1.0,
                "PRICE_RANGE_BREAKOUT": 1.0,
                "REG_SHORT_5D60": 1.0,
                "REG_MID_15D100": 1.0,
            },
            "type_overrides": {
                "PRICE_VOL_BREAKOUT": {
                    "min_score_final": 75.0,
                    "alloc_pct": 0.02,
                    "first_ratio": 0.30,
                    "stop_loss_pct": -0.05,
                    "take_profit_pct": 0.10,
                    "max_hold_days": 5,
                    "entry_timing": "realtime",
                },
                "PRICE_RANGE_BREAKOUT": {
                    "min_score_final": 80.0,
                    "alloc_pct": 0.015,
                    "first_ratio": 0.25,
                    "stop_loss_pct": -0.04,
                    "take_profit_pct": 0.08,
                    "max_hold_days": 3,
                    "entry_timing": "realtime",
                },
                "REG_SHORT_5D60": {
                    "min_score_final": 70.0,
                    "alloc_pct": 0.02,
                    "first_ratio": 0.50,
                    "stop_loss_pct": -0.07,
                    "take_profit_pct": 0.15,
                    "max_hold_days": 5,
                    "entry_timing": "next_open",
                },
                "REG_MID_15D100": {
                    "min_score_final": 85.0,
                    "alloc_pct": 0.015,
                    "first_ratio": 0.40,
                    "stop_loss_pct": -0.05,
                    "take_profit_pct": 0.08,
                    "max_hold_days": 3,
                    "entry_timing": "next_open",
                },
            },
        },
        "market_event_guard": {
            "enabled": True,
            "block_on_exclude_reasons": True,
            "blocked_terms": ["VI", "DISCLOSURE", "KRX_WARNING", "KRX_CAUTION", "ADMIN", "TRADING_HALT"],
        },
    },
    "entry_overheat_policy": {
        "enabled": True,
        "apply_to_normal": True,
        "apply_to_surge": True,
        "v_accel_threshold": 3.1520902710607213,
        "ret1_pct_threshold": 8.849700179324415,
        "atr14_pct_threshold": 0.09907047892735515,
        "trigger": "v_accel",
        "reduce_multiplier": 0.5,
    },
    "entry_selection_policy": {
        "enabled": True,
        "mode": "AUTO",                # AUTO | ONE_OF_TWO | NORMAL_ONLY | SURGE_ONLY
        "prefer": "HIGHER_SCORE",      # HIGHER_SCORE | SURGE | NORMAL
        "surge_priority_first": True,  # deterministic order when mode != ONE_OF_TWO
        "one_pick_when_max_new_le": 1,
        "skip_same_code_day_already_buy": True,
        "fallback_after_block": {
            "enabled": True,
            "max_candidates": 3,
            "when_max_new_le": 1,
        },
    },
    "defense_signal_entry_policy": {
        "enabled": True,
        "artifact_path": str(LOG_DIR / "defense_signal_shadow_latest.csv"),
        "block_general": True,
        "block_surge": True,
        "block_actions_general": ["GENERAL_SHADOW_WAIT"],
        "block_actions_surge": ["SURGE_SHADOW_BLOCK"],
        "missing_action": "ALLOW",
    },
    "entry_signal_date_top_score_cap": {
        "enabled": True,
        "top_n": 3,
        "fallback_when_selected_pool_has_no_top_overlap": True,
        "fallback_require_positive_entry": True,
    },
    "surge_exit_policy": {
        "enabled": True,
        "stop_loss_pct": -0.05,
        "take_profit_pct": 0.10,
        "max_hold_days": 5,
        "stop_sell_ratio_pct": 40,
        "preemptive_sell_ratio_pct": 30,
        "preemptive_close_pct": -0.03,
        "dynamic_exit_ratio": {
            "enabled": True,
            "base_ratios": {
                "LIGHT": 25,
                "NORMAL": 50,
                "HARD": 75,
                "FORCE": 100,
            },
            "t1_surge_relax_one_level": True,
            "risk_off_raise_one_level": True,
            "reversal_raise_one_level": True,
            "repeat_stop_raise_one_level": True,
            "force_gap_loss_pct": 0.08,
            "hard_gap_loss_pct": 0.05,
            "early_loss_hold_days": 1,
            "early_loss_hard_pct": 0.08,
            "early_loss_force_pct": 0.12,
            "hard_atr14_pct": 0.06,
        },
        "reversal_exit": {
            "enabled": True,
            "trigger_count": 2,
            "volume_exhaustion_ratio_max": 0.6,
            "high_rejection_min_drawdown_pct": 0.0,
            "early_close_loss_pct": 0.08,
        },
        "intraday_reversal_exit": {
            "enabled": True,
            "min_points": 3,
            "lookback_points": 5,
            "trigger_count": 2,
            "require_high_rejection_for_exit": False,
            "high_rejection_min_drawdown_pct": 0.025,
            "consecutive_down_points": 3,
            "volume_fade_ratio_max": 0.35,
            "spread_bps_hard": 80.0,
            "orderflow_risk_score_max": 0.6,
        },
    },
    "intraday_residual_overnight_guard": {
        "enabled": False,
        "shadow_only": True,
        "scope": "intraday_realtime",
        "trigger_on_same_day_loss": True,
        "apply_to_surge": True,
        "apply_to_non_surge": True,
        "exit_before_overnight": False,
        "evidence_artifact": "2_Logs/intraday_residual_overnight_risk_audit_latest.json",
    },
    "global_outlier_watcher": {
        "enabled": True,
        "stale_max_age_days": 2,
        "block_on_snapshot_missing": False,
        "block_stage_statuses": ["FAIL", "BLOCK", "ERROR"],
        "caution_stage_statuses": ["WARN", "PARTIAL"],
        "ignore_stage_keys": ["audit"],
        "block_calc_issue_states": ["ISSUE"],
        "ignore_blocking_issue_keys": ["batch_execution", "entry_capacity_zero", "ddm_entry_cap"],
    },
    "cross_source_integrity": {
        "enabled": True,
        "max_skew_days": 1,
        "block_on_missing_required": False,
        "required_sources": ["candidates_meta", "final_score_merge", "signal_integration"],
        "optional_sources": ["pending_entry", "news_collect"],
    },
    "sigma_outlier_guard": {
        "enabled": True,
        "lookback_files": 20,
        "min_history": 5,
        "zscore_caution": 3.0,
        "zscore_block": 5.0,
        "fallback_ratio_caution": 0.4,
        "fallback_ratio_block": 0.2,
    },
    "execution_health_guard": {
        "enabled": True,
        "block_lifecycle_statuses": ["FAIL", "BLOCK", "ERROR"],
        "block_state_machine_statuses": ["FAIL", "BLOCK", "ERROR"],
        "caution_symbol_stop_statuses": ["WARN", "PARTIAL", "FAIL"],
        "slippage_lookback_rows": 30,
        "slippage_bps_caution": 10.0,
        "slippage_bps_block": 20.0,
    },
    "macro_news_guard": {
        "enabled": True,
        "macro_use_critical_guard": True,
        "macro_critical_bad_block": 1,
        "unknown_critical_no_data_action": "BLOCK",
        "unknown_critical_no_data_allowed_run_labels": ["validation"],
        "macro_stale_ratio_caution": 0.5,
        "macro_stale_ratio_block": 0.8,
        "news_quota_guard_block": True,
        "news_quality_block_statuses": ["FAIL", "BLOCK", "ERROR"],
    },
    "backtest_validation_guard": {
        "enabled": True,
        "stale_max_age_days": 3,
        "block_gate_names": ["acceptance_pnl_turnover", "cpcv_pbo"],
        "caution_gate_names": [],
        "caution_on_overall_fail": False,
        "block_on_missing": False,
        "caution_affects_entry": False,
    },
    "sell_rules": {
        "enabled": True,
        "fundamentals_csv_path": str(BASE_DIR / "_cache" / "dart_fundamental_latest.csv"),
        "stop_loss": {
            "default_pct": -10,
            "asset_type_overrides": {
                "성장주": -15,
                "가치주": -10,
                "테마주": -7,
                "배당주": -10,
                "경기민감주": -12,
                "경기방어주": -8,
            },
            "trailing_stop_enabled": True,
            "trailing_stop_activation_profit_pct": 20,
            "trailing_stop_pct": -15,
            "preemptive_close_enabled": True,
            "preemptive_close_pct": -3.5,
        },
        "take_profit": {
            "enabled": True,
            "levels": [20, 50, 100],
            "ratios": [30, 30, 40],
            "asset_type_adjustments": {
                "성장주": 0.7,
                "가치주": 1.2,
                "테마주": 1.5,
                "배당주": 0.5,
            },
        },
        "asset_type_rules": {
            "성장주": {
                "trailing_stop_pct": -20,
                "take_profit_levels": [30, 70, 150],
                "take_profit_ratios": [20, 30, 50],
            },
            "가치주": {
                "trailing_stop_pct": -15,
                "take_profit_levels": [20, 40, 60],
                "take_profit_ratios": [30, 40, 30],
            },
            "배당주": {
                "trailing_stop_pct": -12,
                "take_profit_levels": [30, 50],
                "take_profit_ratios": [40, 30],
                "dividend_yield_floor": 3.0,
            },
            "테마주": {
                "trailing_stop_pct": -10,
                "take_profit_levels": [15, 25, 40],
                "take_profit_ratios": [40, 40, 20],
                "max_hold_days": 60,
            },
            "경기민감주": {
                "trailing_stop_pct": -18,
                "take_profit_levels": [25, 50, 80],
                "take_profit_ratios": [30, 40, 30],
            },
            "경기방어주": {
                "trailing_stop_pct": -12,
                "take_profit_levels": [20, 35, 50],
                "take_profit_ratios": [30, 40, 30],
            },
        },
        "fundamental_risk": {
            "enabled": True,
            "critical_debt_ratio": 300.0,       # 200→300: 부채비율 기준 완화
            "critical_roe": -0.15,              # 0→-0.15: ROE -15% 이하만 위험 처리
            "critical_revenue_growth_yoy": -30.0,  # -20→-30: 매출 기준 완화
            "warning_operating_margin": 0.0,    # 5→0: warning 청산 사실상 비활성
            "dividend_yield_floor": 3.0,
            "warning_sell_ratio": 50.0,
        },
        "market_risk": {
            "enabled": True,
            "vix_high": 30,
            "vix_extreme": 40,
            "usd_krw_volatility": 15,
            "oil_shock_high": 0.45,
            "oil_shock_extreme": 0.75,
            "high_sell_ratio_profit": 30,
            "extreme_sell_ratio_profit": 70,
            "extreme_sell_ratio_loss": 30,
        },
        "candidate_dropout": {
            "enabled": False,
            "min_hold_days": 2,
            "sell_ratio_pct": 30.0,
        },
        "technical": {
            "enabled": True,
            "rsi_overbought": 75,
            "ma_periods": [20, 60, 120],
            "high_score_threshold": 60,
            "mid_score_threshold": 40,
            "high_score_sell_ratio": 50,
            "mid_score_sell_ratio": 30,
        },
    },
    "market_ops_policy": {
        "enabled": True,
        "slo_normal": 0.10,
        "slo_rally_base": 0.20,
        "slo_rally_per_ret": 2.0,
        "slo_rally_cap": 0.50,
        "slo_crash_base": 0.05,
        "slo_crash_per_ret": 0.8,
        "slo_crash_cap": 0.20,
        "probe_rally_min": 1,
        "probe_rally_ratio_base": 0.10,
        "probe_rally_ratio_per_ret": 2.0,
        "probe_rally_ratio_cap": 0.35,
        "probe_crash_min": 0,
        "probe_crash_ratio_base": 0.03,
        "probe_crash_ratio_per_ret": 0.8,
        "probe_crash_ratio_cap": 0.10,
        "universe_shrink_min_candidates": 8,
        "universe_shrink_min_price_codes": 0,
        "universe_shrink_disable_signal_cap": True,
        "universe_shrink_disable_sector_cap": True,
        "universe_shrink_probe_uplift": 0.05,
        "carryover_no_next_day_enabled": True,
        "carryover_max_age_days": 2,
        "carryover_revalidate_score_gap_max": 0.05,
        "carryover_revalidate_min_final_score": 0.0,
        "entry_fallback_policy": {
            "enabled": False,
            "max_stage": 3,
            "stage_gap_up_limits": [0.03, 0.02, 0.01],
            "signal_valid_days": 2,
            "auction_close_position_min": 0.70,
            "auction_day_range_max_pct": 0.07,
            "auction_v_accel_min": 1.5,
            "next_open_limit_pct": 0.01,
            "intraday_limit_pct": 0.01,
            "next_open_gap_up_max_pct": 0.03,
        },
        # --- Strict execution controls (v2) ---
        "signal_ttl_minutes": 0,               # 0=disabled; intraday_realtime only: expire if signal age > N min
        "max_retry_per_code_per_day": 0,        # 0=disabled; max same-code entry attempts per day
        "partial_fill_same_day_only": True,     # partial fill residuals do not carry to next day in strict mode
        "exec_quality_max_slippage_pct": 0.0,  # 0=disabled; block if |entry_price-ref_close|/ref_close > pct
        "close_cutoff_minutes": 10,             # block new entries N min before 15:30 KRX close (intraday_realtime only)
        "quote_max_age_days": 1,                # QUOTE_STALE_BLOCK if entry_day quote is older than N days vs today
        "fail_closed_propagate_block": True,    # BLOCK event → force max_new=0 for remaining candidates
    },
    "stale_signal_replay": {
        "enabled": False,
        "min_signal_age_days": 2,
        "require_no_open_positions": True,
        "order_id_include_entry_day": True,
    },

    "drawdown_manager": {
        "enabled": True,
        "use_debug_lifetime_mdd": False,
        "stages": [
            {"mdd": 0.10, "new_entry_allowed_pct": 0.50, "liquidate_weakest_pct": 0.00, "max_exposure": 0.60},
            {"mdd": 0.15, "new_entry_allowed_pct": 0.00, "liquidate_weakest_pct": 0.00, "max_exposure": 0.50},
            {"mdd": 0.20, "new_entry_allowed_pct": 0.00, "liquidate_weakest_pct": 0.30, "max_exposure": 0.40},
            {"mdd": 0.25, "new_entry_allowed_pct": 0.00, "liquidate_weakest_pct": 0.50, "max_exposure": 0.30},
            {"mdd": 0.30, "new_entry_allowed_pct": 0.00, "liquidate_weakest_pct": 1.00, "max_exposure": 0.00},
        ],
        "liquidation_price": "close",
        "consecutive_loss_days_threshold": 3,
        "consecutive_loss_exposure_multiplier": 0.5,
        "vix_proxy_threshold": 30.0,
        "high_vol_position_size_multiplier": 0.7,
        "sector_concentration_block": True,
        "sector_concentration_limit": 0.40,
    },
}


def load_config() -> Dict[str, Any]:
    """
    Load paper engine config with DEFAULT_CONFIG fallback and overrides.
    Missing keys are backfilled from DEFAULT_CONFIG to avoid KeyError on older configs.
    """
    if not CONFIG_PATH.exists():
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        CONFIG_PATH.write_text(json.dumps(DEFAULT_CONFIG, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[CONFIG] wrote default: {CONFIG_PATH}")
        return dict(DEFAULT_CONFIG)

    raw_cfg = ""
    try:
        raw_cfg = CONFIG_PATH.read_text(encoding="utf-8")
    except Exception:
        raw_cfg = CONFIG_PATH.read_text(encoding="utf-8-sig")
    if raw_cfg.startswith("\ufeff"):
        raw_cfg = raw_cfg.lstrip("\ufeff")
    loaded = json.loads(raw_cfg)
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    if isinstance(loaded, dict):
        _deep_merge_dict(cfg, loaded)
    cfg = _apply_env_config_overrides(cfg)

    # Keep defaults in-memory only; do not mutate config on runtime.
    # Config file changes must go through tools/paper_engine_config_lock.py.
    missing = [k for k in DEFAULT_CONFIG.keys() if k not in loaded]
    if missing:
        print(f"[CONFIG] missing keys defaulted in-memory only: {missing}")
    return cfg
