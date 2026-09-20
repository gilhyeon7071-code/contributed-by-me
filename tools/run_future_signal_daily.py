from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path("E:/1_Data")
LOGS = ROOT / "2_Logs"
MODEL_META = ROOT / "_cache" / "future_signal" / "future_signal_rule_baseline_model_meta_latest.json"
LATEST_JSON = LOGS / "future_signal_daily_status_latest.json"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    tmp.replace(path)


def _run_step(name: str, args: list[str], allow_nonzero: bool = False) -> dict[str, Any]:
    started = datetime.now().isoformat(timespec="seconds")
    proc = subprocess.run(
        [sys.executable, *args],
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        check=False,
    )
    return {
        "name": name,
        "command": [sys.executable, *args],
        "started_at": started,
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "rc": int(proc.returncode),
        "allow_nonzero": bool(allow_nonzero),
        "stdout_tail": (proc.stdout or "").strip()[-2000:],
        "stderr_tail": (proc.stderr or "").strip()[-2000:],
    }


def _status_ok(path: Path) -> bool:
    return str(_read_json(path).get("status") or "").upper() == "PASS"


def _forecast_validation_ok() -> bool:
    return _status_ok(LOGS / "forecast_score_validation_latest.json")


def _validation_ok() -> bool:
    state = str(_read_json(LOGS / "future_signal_validation_latest.json").get("validation_state") or "").upper()
    return state in {"WAITING", "EVALUATED", "EVALUATED_PARTIAL"}


def _calibration_ok() -> bool:
    state = str(_read_json(LOGS / "future_signal_calibration_latest.json").get("calibration_state") or "").upper()
    return state in {"WAITING", "UNCALIBRATED", "EVALUATED", "INTRADAY_EVALUATED"}


def main() -> int:
    steps: list[dict[str, Any]] = []
    failed: list[str] = []
    plan = [
        ("features", ["tools/build_future_signal_features.py"], False),
        ("baseline", ["tools/build_future_signal_baseline.py"], False),
        ("preview", ["tools/build_future_signal_preview.py", "--model-meta", str(MODEL_META)], False),
        ("validation", ["tools/validate_future_signal_preview.py"], True),
        ("market_event_log", ["tools/build_market_event_log.py"], False),
        ("cross_d_validation", ["tools/validate_future_signal_cross_d_history.py"], True),
        ("calibration", ["tools/validate_future_signal_calibration.py"], True),
        ("shadow_history", ["tools/update_future_signal_shadow_history.py"], False),
        ("history_integrity_audit", ["tools/build_future_signal_history_integrity_audit.py"], True),
        ("candidate_shadow_join", ["tools/build_future_signal_candidate_shadow_join.py"], False),
        ("intraday_overlay_watchlist", ["tools/build_future_signal_intraday_overlay_watchlist.py"], False),
        ("intraday_watchlist_markout", ["tools/build_future_signal_intraday_watchlist_markout.py"], False),
        ("e_detector_shadow", ["tools/build_e_detector_shadow.py"], False),
    ]

    for name, args, allow_nonzero in plan:
        step = _run_step(name, args, allow_nonzero=allow_nonzero)
        steps.append(step)
        if step["rc"] != 0 and not allow_nonzero:
            failed.append(f"{name}:rc={step['rc']}")
            break

    checks = {
        "features_pass": _status_ok(LOGS / "future_signal_features_latest.json"),
        "baseline_pass": _status_ok(LOGS / "future_signal_baseline_latest.json"),
        "preview_pass": _status_ok(LOGS / "future_signal_preview_latest.json"),
        "forecast_validation_pass": _forecast_validation_ok(),
        "validation_state_allowed": _validation_ok(),
        "calibration_state_allowed": _calibration_ok(),
        "shadow_history_pass": _status_ok(LOGS / "future_signal_shadow_history_latest.json"),
        "history_integrity_audit_pass": _status_ok(LOGS / "future_signal_history_integrity_audit_latest.json"),
        "candidate_shadow_join_pass": _status_ok(LOGS / "future_signal_candidate_shadow_join_latest.json"),
        "intraday_overlay_watchlist_pass": _status_ok(LOGS / "future_signal_intraday_overlay_watchlist_latest.json"),
        "intraday_watchlist_markout_pass": _status_ok(LOGS / "future_signal_intraday_watchlist_markout_latest.json"),
        "e_detector_shadow_pass": _status_ok(LOGS / "e_detector_shadow_latest.json"),
    }
    # forecast_validation_pass is diagnostic-only; forecast_score improvements are pending.
    # It does not gate generation_ok or daily status.
    _gating_keys = {
        "features_pass",
        "baseline_pass",
        "preview_pass",
        "validation_state_allowed",
        "calibration_state_allowed",
        "shadow_history_pass",
        "candidate_shadow_join_pass",
        "intraday_overlay_watchlist_pass",
        "intraday_watchlist_markout_pass",
        "e_detector_shadow_pass",
    }
    for key, ok in checks.items():
        if key in _gating_keys and not ok:
            failed.append(key)
    generation_ok = bool(
        checks["features_pass"]
        and checks["baseline_pass"]
        and checks["preview_pass"]
        and checks["validation_state_allowed"]
        and checks["calibration_state_allowed"]
        and checks["shadow_history_pass"]
        and checks["candidate_shadow_join_pass"]
        and checks["intraday_overlay_watchlist_pass"]
        and checks["intraday_watchlist_markout_pass"]
        and checks["e_detector_shadow_pass"]
    )

    preview = _read_json(LOGS / "future_signal_preview_latest.json")
    forecast_validation = _read_json(LOGS / "forecast_score_validation_latest.json")
    validation = _read_json(LOGS / "future_signal_validation_latest.json")
    market_event_log = _read_json(LOGS / "market_event_log_latest.json")
    cross_d_validation = _read_json(LOGS / "future_signal_cross_d_validation_latest.json")
    calibration = _read_json(LOGS / "future_signal_calibration_latest.json")
    shadow = _read_json(LOGS / "future_signal_shadow_history_latest.json")
    history_integrity_audit = _read_json(LOGS / "future_signal_history_integrity_audit_latest.json")
    candidate_shadow_join = _read_json(LOGS / "future_signal_candidate_shadow_join_latest.json")
    intraday_overlay_watchlist = _read_json(LOGS / "future_signal_intraday_overlay_watchlist_latest.json")
    intraday_watchlist_markout = _read_json(LOGS / "future_signal_intraday_watchlist_markout_latest.json")
    e_detector = _read_json(LOGS / "e_detector_shadow_latest.json")

    status = "PASS" if not failed else "FAIL"
    d = str(preview.get("D") or shadow.get("latest_D") or "unknown")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "reason": "ok" if not failed else "|".join(dict.fromkeys(failed)),
        "D": d,
        "official_signal_definition": {
            "official_signal_name": "future_signal_preview",
            "official_signal_output": str(LOGS / "future_signal_preview_latest.json"),
            "official_signal_csv": str((preview.get("outputs") or {}).get("csv") or LOGS / f"future_signal_preview_{d}.csv"),
            "definition": "read_only_multi_horizon_future_prediction_preview",
            "horizon_types": {
                "INTRADAY": 0,
                "SHORT": 2,
                "SWING": 5,
                "MID": 15,
                "LONG": 30,
            },
            "non_official_trade_concepts": {
                "forecast_score": "final_score_auxiliary_component",
                "horizon_label": "entry_and_holding_period_policy_label",
            },
            "trading_policy": {
                "read_only_for_trading": True,
                "trading_approved": bool(calibration.get("trading_approved", False)),
            },
        },
        "steps": steps,
        "checks": checks,
        "summary": {
            "preview_status": preview.get("status"),
            "preview_rows": preview.get("rows"),
            "preview_abstain_rows": preview.get("abstain_rows"),
            "preview_horizon_types": preview.get("horizon_types"),
            "forecast_validation_status": forecast_validation.get("status"),
            "forecast_validation_reason": forecast_validation.get("reason"),
            "validation_state": validation.get("validation_state"),
            "validation_reason": validation.get("reason"),
            "market_event_log_D": market_event_log.get("D"),
            "market_event_log_sidecar": bool(market_event_log.get("sidecar", False)),
            "market_event_log_circuit_breaker": bool(market_event_log.get("circuit_breaker", False)),
            "cross_d_validation_status": cross_d_validation.get("status"),
            "cross_d_validation_reason": cross_d_validation.get("reason"),
            "cross_d_realized_rows": cross_d_validation.get("realized_rows"),
            "cross_d_waiting_rows": cross_d_validation.get("waiting_rows"),
            "cross_d_exclude_market_event_day_rows": cross_d_validation.get("exclude_market_event_day_rows"),
            "sample_collection_state": (cross_d_validation.get("sample_collection_state") or {}).get("state"),
            "sample_collection_review_ready": bool((cross_d_validation.get("sample_collection_state") or {}).get("review_ready", False)),
            "sample_collection_unmet_requirements": (cross_d_validation.get("sample_collection_state") or {}).get("unmet_requirements"),
            "calibration_state": calibration.get("calibration_state"),
            "calibration_reason": calibration.get("reason"),
            "trading_approved": bool(calibration.get("trading_approved", False)),
            "shadow_history_rows": shadow.get("history_rows"),
            "shadow_unique_D": shadow.get("unique_D"),
            "history_integrity_audit_status": history_integrity_audit.get("status"),
            "history_integrity_audit_stale_or_unmatched_rows": history_integrity_audit.get("stale_or_unmatched_rows"),
            "history_integrity_audit_row_reason_breakdown": history_integrity_audit.get("row_reason_breakdown"),
            "candidate_shadow_join_status": candidate_shadow_join.get("status"),
            "candidate_shadow_join_rows": candidate_shadow_join.get("joined_rows"),
            "candidate_shadow_join_matched_rows": candidate_shadow_join.get("matched_candidate_rows"),
            "intraday_overlay_watchlist_status": intraday_overlay_watchlist.get("status"),
            "intraday_overlay_watchlist_condition": intraday_overlay_watchlist.get("condition"),
            "intraday_overlay_watchlist_rows": intraday_overlay_watchlist.get("watchlist_rows"),
            "intraday_overlay_watchlist_history_rows": intraday_overlay_watchlist.get("history_rows"),
            "intraday_watchlist_markout_status": intraday_watchlist_markout.get("status"),
            "intraday_watchlist_markout_evaluated_rows": intraday_watchlist_markout.get("evaluated_rows"),
            "intraday_watchlist_markout_metrics": intraday_watchlist_markout.get("metrics"),
            "intraday_watchlist_markout_current_condition": intraday_watchlist_markout.get("current_condition"),
            "intraday_watchlist_markout_current_condition_evaluated_rows": intraday_watchlist_markout.get("current_condition_evaluated_rows"),
            "intraday_watchlist_markout_current_condition_metrics": intraday_watchlist_markout.get("current_condition_metrics"),
            "e_detector_status": e_detector.get("status"),
            "e_detector_reason": e_detector.get("reason"),
            "e_detector_target_arl": e_detector.get("target_arl"),
            "e_detector_threshold_log": e_detector.get("threshold_log"),
            "e_detector_log_e_sr": e_detector.get("log_e_sr"),
            "e_detector_alert": bool(e_detector.get("alert", False)),
            "e_detector_ready_rows": e_detector.get("ready_rows"),
        },
        "policy": {
            "read_only_for_trading": True,
            "batch_exit_allowed": generation_ok,
            "batch_exit_basis": "read_only_signal_outputs_generated" if generation_ok else "signal_generation_failed",
            "orders_modified": False,
            "fills_modified": False,
            "ledger_modified": False,
            "stats_modified": False,
            "gate_modified": False,
            "risk_lock_modified": False,
            "score_modified": False,
            "threshold_applied": False,
            "trading_approved": bool(calibration.get("trading_approved", False)),
        },
        "sample_collection_policy": cross_d_validation.get("sample_collection_policy") or {},
        "sample_collection_state": cross_d_validation.get("sample_collection_state") or {},
        "outputs": {
            "latest_json": str(LATEST_JSON),
            "preview": str(LOGS / "future_signal_preview_latest.json"),
            "forecast_validation": str(LOGS / "forecast_score_validation_latest.json"),
            "validation": str(LOGS / "future_signal_validation_latest.json"),
            "market_event_log": str(LOGS / "market_event_log_latest.json"),
            "cross_d_validation": str(LOGS / "future_signal_cross_d_validation_latest.json"),
            "calibration": str(LOGS / "future_signal_calibration_latest.json"),
            "shadow_history": str(LOGS / "future_signal_shadow_history_latest.json"),
            "history_integrity_audit": str(LOGS / "future_signal_history_integrity_audit_latest.json"),
            "candidate_shadow_join": str(LOGS / "future_signal_candidate_shadow_join_latest.json"),
            "intraday_overlay_watchlist": str(LOGS / "future_signal_intraday_overlay_watchlist_latest.json"),
            "intraday_watchlist_markout": str(LOGS / "future_signal_intraday_watchlist_markout_latest.json"),
            "e_detector_shadow": str(LOGS / "e_detector_shadow_latest.json"),
        },
    }
    _write_json(LOGS / f"future_signal_daily_status_{d}.json", payload)
    _write_json(LATEST_JSON, payload)
    print(
        f"[FUTURE_DAILY] status={status} reason={payload['reason']} D={d} "
        f"preview={payload['summary']['preview_status']} "
        f"validation={payload['summary']['validation_state']} "
        f"calibration={payload['summary']['calibration_state']} "
        f"shadow_unique_D={payload['summary']['shadow_unique_D']} "
        f"e_detector_alert={payload['summary']['e_detector_alert']}"
    )
    return 0 if generation_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
