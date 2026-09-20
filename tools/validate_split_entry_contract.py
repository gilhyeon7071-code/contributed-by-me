from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(r"E:\1_Data")
ENGINE_PATH = ROOT / "paper_engine.py"
CONFIG_PATH = ROOT / "paper" / "paper_engine_config.json"
LOG_DIR = ROOT / "2_Logs"
LATEST_PATH = LOG_DIR / "split_entry_contract_validation_latest.json"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _dated_path() -> Path:
    return LOG_DIR / f"split_entry_contract_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _count(text: str, needle: str) -> int:
    return text.count(needle)


def _runtime_save_check() -> dict[str, Any]:
    sys.path.insert(0, str(ROOT))
    import paper_engine  # type: ignore

    tmp_path = LOG_DIR / "_tmp_split_entry_pending_runtime_check.csv"
    old_path = paper_engine.PENDING_SIGNALS_PATH
    paper_engine.PENDING_SIGNALS_PATH = tmp_path
    try:
        if tmp_path.exists():
            tmp_path.unlink()
        row = {
            "signal_date": "20260428",
            "code": "000001",
            "name": "SPLIT_TEST",
            "carry_reason": "ENTRY_RETRY_READY",
            "carry_origin_reason": "SPLIT_ENTRY_2ND",
            "split_entry_2nd": True,
            "split_remaining_qty": 1,
            "split_first_entry_price": 1000,
            "split_first_order_id": "PAPER_BUY_000001_TEST",
            "captured_at": _now(),
            "carryover_count": 1,
            "carryover_max_age_days": 1,
            "score": 1.0,
            "final_score": 1.0,
        }
        paper_engine._build_ops_alert_and_carryover(
            ops_enabled=True,
            ops_policy={
                "enabled": True,
                "carryover_no_next_day_enabled": False,
                "strict_same_day_only": True,
                "partial_fill_same_day_only": True,
            },
            pending_carry_rows=[row],
            carry_max_age=1,
            px=pd.DataFrame(),
            market_regime="NORMAL",
            regime_info={},
            config={},
            max_new=0,
            max_new_zero_reason="",
            candidate_count=0,
            price_universe_codes=0,
            evaluated_count=0,
            entry_ready_count=0,
            new_count=0,
            filled_count=0,
            no_next_day_count=0,
            cap_block_count=0,
            processed_skip_count=0,
            max_new_skip_count=0,
            idempotent_skip_count=0,
            stale_replay_used_count=0,
            open_order_replay_used_count=0,
            entry_decisions=[],
            universe_shrink_candidates=False,
            open_slot_count=0,
            max_positions=0,
        )
        exists = tmp_path.exists()
        rows = pd.read_csv(tmp_path, dtype=str).to_dict("records") if exists else []
        ok = bool(rows and str(rows[0].get("carry_origin_reason")) == "SPLIT_ENTRY_2ND")
        return {
            "status": "PASS" if ok else "FAIL",
            "temp_path": str(tmp_path),
            "rows": len(rows),
            "carry_origin_reason": rows[0].get("carry_origin_reason") if rows else "",
        }
    finally:
        paper_engine.PENDING_SIGNALS_PATH = old_path
        if tmp_path.exists():
            tmp_path.unlink()


def main() -> None:
    if not ENGINE_PATH.exists():
        raise FileNotFoundError(str(ENGINE_PATH))
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(str(CONFIG_PATH))

    engine = ENGINE_PATH.read_text(encoding="utf-8")
    cfg = _read_json(CONFIG_PATH)
    split = cfg.get("split_entry", {}) if isinstance(cfg.get("split_entry"), dict) else {}
    ops = cfg.get("market_ops_policy", {}) if isinstance(cfg.get("market_ops_policy"), dict) else {}
    overheat = cfg.get("entry_overheat_policy", {}) if isinstance(cfg.get("entry_overheat_policy"), dict) else {}
    surge_policy = cfg.get("surge_entry_policy", {}) if isinstance(cfg.get("surge_entry_policy"), dict) else {}
    surge_type_policy = surge_policy.get("type_policy", {}) if isinstance(surge_policy.get("type_policy"), dict) else {}
    surge_event_guard = surge_policy.get("market_event_guard", {}) if isinstance(surge_policy.get("market_event_guard"), dict) else {}
    surge_type_overrides = (
        surge_type_policy.get("type_overrides", {})
        if isinstance(surge_type_policy.get("type_overrides"), dict)
        else {}
    )
    global_surge_first_ratio = float(split.get("surge_first_ratio", 0.0) or 0.0)
    split_enabled = bool(split.get("enabled"))
    price_vol_first_ratio = float(
        (surge_type_overrides.get("PRICE_VOL_BREAKOUT", {}) or {}).get("first_ratio", 0.0) or 0.0
    )
    allowed_surge_types = {
        str(x).strip().upper()
        for x in (surge_type_policy.get("allowed_types") or [])
        if str(x).strip()
    }
    blocked_surge_types = {
        str(x).strip().upper()
        for x in (surge_type_policy.get("blocked_types") or [])
        if str(x).strip()
    }
    allowed_type_first_ratios = {
        key: float((surge_type_overrides.get(key, {}) or {}).get("first_ratio", 0.0) or 0.0)
        for key in sorted(allowed_surge_types)
    }
    allowed_type_first_ratio_policy = bool(allowed_type_first_ratios) and all(
        abs(float(v) - 0.3) < 1e-9 for v in allowed_type_first_ratios.values()
    )
    surge_first_ratio_effective_policy = (
        abs(global_surge_first_ratio - 0.3) < 1e-9
        and allowed_type_first_ratio_policy
        and "LIMIT_UP_NEAR" in blocked_surge_types
        and "surge_type_first_ratio" in engine
        and "_type_fr if 0.0 < _type_fr <= 1.0 else" in engine
    )
    split_open_chase_policy = (
        bool(split.get("enabled"))
        and abs(float(split.get("max_open_to_entry_chase_pct", 0.0) or 0.0) - 0.05) < 1e-9
        and "_evaluate_split_open_chase_block" in engine
        and "OPEN_CHASE_BLOCK" in engine
    )

    partial_branch_has_split_exception = (
        "_remaining > 0 and _strict_same_day and _partial_same_day_only"
        in engine
        and 'allow_split_second_carryover' in engine
        and "pending_carry_rows.append({" in engine
        and '"carry_origin_reason": "SPLIT_ENTRY_2ND"' in engine
    )
    split_second_pending_possible = bool(
        split.get("enabled")
        and ops.get("strict_same_day_only")
        and ops.get("partial_fill_same_day_only")
        and split.get("allow_split_second_carryover", False)
        and partial_branch_has_split_exception
    )

    split_alloc_used_for_second_only = (
        "if is_split_2nd and budget_policy_enabled and split_alloc_pct > 0.0:" in engine
    )
    split_second_save_exception = (
        "_has_split_second_carry" in engine
        and 'carry_origin_reason", "") or "").upper() == "SPLIT_ENTRY_2ND"' in engine
        and 'bool(ops_policy.get("carryover_no_next_day_enabled", True)) or _has_split_second_carry' in engine
    )
    first_order_id_usage_count = _count(engine, "split_first_order_id")
    first_order_id_linked = (
        "split_first_order_id={r.get('split_first_order_id'" in engine
        and '"first_order_id": str(r.get("split_first_order_id"' in engine
        and "split_second_entries" in engine
    )
    runtime_save_check = _runtime_save_check()

    checks = {
        "split_pullback_enabled_policy": {
            "status": "PASS" if split_enabled else "FAIL",
            "value": {
                "enabled": split_enabled,
                "expected": True,
                "reason": "pullback_probe_transition",
            },
        },
        "first_ratio_preserved_for_restore": {
            "status": "PASS" if 0.0 < float(split.get("first_ratio", 0.0) or 0.0) <= 1.0 else "FAIL",
            "value": {
                "first_ratio": split.get("first_ratio"),
                "used_now": split_enabled,
            },
        },
        "surge_first_ratio_effective_policy": {
            "status": "NA" if not split_enabled else ("PASS" if surge_first_ratio_effective_policy else "FAIL"),
            "value": {
                "global_fallback_surge_first_ratio": split.get("surge_first_ratio"),
                "price_vol_breakout_first_ratio": price_vol_first_ratio,
                "allowed_type_first_ratios": allowed_type_first_ratios,
                "blocked_types": sorted(blocked_surge_types),
                "engine_uses_type_override": "surge_type_first_ratio" in engine,
                "reason": "split_entry_disabled" if not split_enabled else "",
            },
        },
        "entry_overheat_v_accel_reduce": {
            "status": "PASS"
            if bool(overheat.get("enabled"))
            and str(overheat.get("trigger", "")).lower() == "v_accel"
            and abs(float(overheat.get("v_accel_threshold", 0.0) or 0.0) - 3.1520902710607213) < 1e-9
            and float(overheat.get("reduce_multiplier", 0.0) or 0.0) == 0.5
            and "_overheat_qty_decision" in engine
            and "[OVERHEAT_REDUCE]" in engine
            else "FAIL",
            "value": overheat,
        },
        "split_second_confirmation_policy": {
            "status": "PASS"
            if isinstance(split.get("second_confirmation"), dict)
            and bool(split.get("second_confirmation", {}).get("enabled"))
            and str(split.get("second_confirmation", {}).get("missing_feature_action", "")).upper() == "BLOCK"
            and "_split_second_confirmation_reason" in engine
            and "[SKIP_SPLIT2ND_CONFIRM]" in engine
            else "FAIL",
            "value": split.get("second_confirmation"),
        },
        "surge_type_policy_connected": {
            "status": "PASS"
            if bool(surge_type_policy.get("enabled"))
            and isinstance(surge_type_policy.get("allowed_types"), list)
            and isinstance(surge_type_policy.get("blocked_types"), list)
            and "_surge_type_policy_decision" in engine
            and "surge_type_qty_multiplier" in engine
            else "FAIL",
            "value": surge_type_policy,
        },
        "surge_market_event_guard_connected": {
            "status": "PASS"
            if bool(surge_event_guard.get("enabled"))
            and isinstance(surge_event_guard.get("blocked_terms"), list)
            and "_surge_market_event_block_reason" in engine
            and "exclude_reasons" in engine
            else "FAIL",
            "value": surge_event_guard,
        },
        "second_dip_range_1_5_to_4_pct": {
            "status": "PASS"
            if abs(float(split.get("second_dip_min_pct", 0.0) or 0.0) - 0.015) < 1e-9
            and abs(float(split.get("second_dip_max_pct", 0.0) or 0.0) - 0.04) < 1e-9
            else "FAIL",
            "value": {
                "second_dip_min_pct": split.get("second_dip_min_pct"),
                "second_dip_max_pct": split.get("second_dip_max_pct"),
            },
        },
        "split_first_open_chase_hard_block_policy": {
            "status": "PASS" if split_open_chase_policy else "FAIL",
            "value": {
                "max_open_to_entry_chase_pct": split.get("max_open_to_entry_chase_pct"),
                "engine_helper_connected": "_evaluate_split_open_chase_block" in engine,
                "engine_reason_connected": "OPEN_CHASE_BLOCK" in engine,
            },
        },
        "split_second_pending_not_blocked_by_partial_same_day": {
            "status": "NA" if not split_enabled else ("PASS" if split_second_pending_possible else "FAIL"),
            "value": {
                "strict_same_day_only": ops.get("strict_same_day_only"),
                "partial_fill_same_day_only": ops.get("partial_fill_same_day_only"),
                "allow_split_second_carryover": split.get("allow_split_second_carryover"),
                "source_branch_has_exception": partial_branch_has_split_exception,
                "reason": "split_entry_disabled" if not split_enabled else "",
            },
        },
        "split_budget_cap_scope": {
            "status": "PASS" if split_alloc_used_for_second_only else "FAIL",
            "value": {
                "budget_alloc_pct": split.get("budget_alloc_pct"),
                "capital_budget_split_alloc_pct": (cfg.get("capital_budget_policy") or {}).get("split_alloc_pct"),
                "applies_to": "second_entry_only" if split_alloc_used_for_second_only else "unknown",
            },
        },
        "split_second_pending_saved_even_when_general_carryover_disabled": {
            "status": "NA" if not split_enabled else ("PASS" if runtime_save_check.get("status") == "PASS" else "FAIL"),
            "value": {
                "market_ops_carryover_no_next_day_enabled": ops.get("carryover_no_next_day_enabled"),
                "source_save_branch_has_exception": split_second_save_exception,
                "runtime_save_check": runtime_save_check,
                "reason": "split_entry_disabled" if not split_enabled else "",
            },
        },
        "split_first_order_id_link_usage": {
            "status": "PASS" if first_order_id_linked else "WARN",
            "value": {
                "usage_count": first_order_id_usage_count,
                "meaning": "used_for_linking" if first_order_id_linked else "stored_only",
                "note_link": first_order_id_linked,
            },
        },
    }

    status_order = {"FAIL": 3, "WARN": 2, "PASS": 1, "NA": 1}
    overall = "PASS"
    for item in checks.values():
        st = str(item.get("status"))
        if status_order.get(st, 0) > status_order.get(overall, 0):
            overall = st

    result = {
        "generated_at": _now(),
        "status": overall,
        "inputs": {
            "engine": str(ENGINE_PATH),
            "config": str(CONFIG_PATH),
        },
        "checks": checks,
        "remaining_real_issues": [
            "split_first_order_id_link_not_verified"
        ]
        if not first_order_id_linked
        else [],
    }

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    latest_text = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    LATEST_PATH.write_text(latest_text, encoding="utf-8")
    dated = _dated_path()
    dated.write_text(latest_text, encoding="utf-8")
    print(json.dumps({"status": overall, "latest": str(LATEST_PATH), "dated": str(dated)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
