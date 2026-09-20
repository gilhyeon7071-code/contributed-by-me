from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CFG_PATH = ROOT / "paper" / "paper_engine_config.json"


def _read_json(path: Path) -> Dict[str, Any]:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except Exception:
            continue
    return {}


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        text = str(value).replace(",", "").strip()
        if not text:
            return default
        out = float(text)
        if out != out:
            return default
        return out
    except Exception:
        return default


def _pct01(value: Any, default: float) -> float:
    out = _to_float(value, default)
    if abs(out) > 1.0:
        out /= 100.0
    return out


def build_report() -> Dict[str, Any]:
    cfg = _read_json(CFG_PATH)
    macro = _read_json(LOG_DIR / "macro_signal_latest.json")
    risk = _read_json(LOG_DIR / "risk_orchestration_latest.json")
    p1 = _read_json(LOG_DIR / "p1_entry_gate_status_latest.json")
    capital = _read_json(LOG_DIR / "capital_operation_decision_report_latest.json")
    validation = _read_json(LOG_DIR / "crash_es_hardblock_validation_latest.json")

    regime_policy = cfg.get("regime_entry_policy") if isinstance(cfg.get("regime_entry_policy"), dict) else {}
    risk_detail = risk.get("risk_orchestration") if isinstance(risk.get("risk_orchestration"), dict) else {}
    macro_metrics = macro.get("market_metrics") if isinstance(macro.get("market_metrics"), dict) else {}

    macro_ret1 = _to_float(macro_metrics.get("ret1"), 0.0)
    crash_day_ret_max = _pct01(regime_policy.get("crash_day_ret_max"), -0.025)
    runtime_regime = str(risk.get("market_regime") or p1.get("market_regime") or "").upper()
    es = _to_float(risk_detail.get("es"), 0.0)
    es_limit = abs(_to_float(risk_detail.get("es_limit"), 0.0))
    position_size_multiplier = _to_float(risk.get("position_size_multiplier"), _to_float(risk_detail.get("scale"), 0.0))
    scale_zero_causes = [str(x) for x in (risk_detail.get("scale_zero_causes") or [])]
    account_risk_clear = bool(risk_detail.get("account_risk_clear", False))
    order_budget_status = str((capital.get("decision") or {}).get("order_budget_contract_status") or "").upper()

    crash_clear = bool(runtime_regime != "CRASH" or macro_ret1 > crash_day_ret_max)
    es_clear = bool(abs(es) < es_limit) if es_limit > 0 else True
    hardblock_clear = bool("es_hard_block" not in scale_zero_causes and position_size_multiplier > 0.0)
    capital_recheck_ready = bool(crash_clear and es_clear and hardblock_clear and account_risk_clear and order_budget_status == "PASS")

    blockers: List[str] = []
    if not crash_clear:
        blockers.append("CRASH_NOT_RELEASED")
    if not es_clear:
        blockers.append("ES_LIMIT_NOT_RELEASED")
    if not hardblock_clear:
        blockers.append("RUNTIME_SCALE_STILL_ZERO")
    if not account_risk_clear:
        blockers.append("ACCOUNT_RISK_NOT_CLEAR")
    if order_budget_status != "PASS":
        blockers.append("ORDER_BUDGET_NOT_PASS")

    if capital_recheck_ready:
        monitor_status = "RECHECK_READY"
        next_action = "REVIEW_BUY_CANDIDATES"
    else:
        monitor_status = "WAIT"
        next_action = "KEEP_NO_NEW_CAPITAL_DEPLOYMENT"

    rows = [
        {
            "condition": "crash_release",
            "status": "PASS" if crash_clear else "WAIT",
            "current": f"regime={runtime_regime}; macro_ret1={macro_ret1:.6f}",
            "release_rule": f"regime!=CRASH or macro_ret1>{crash_day_ret_max:.6f}",
            "blocker": "" if crash_clear else "CRASH_NOT_RELEASED",
        },
        {
            "condition": "es_release",
            "status": "PASS" if es_clear else "WAIT",
            "current": f"es={es:.6f}; limit={es_limit:.6f}",
            "release_rule": "abs(es)<es_limit",
            "blocker": "" if es_clear else "ES_LIMIT_NOT_RELEASED",
        },
        {
            "condition": "runtime_scale_release",
            "status": "PASS" if hardblock_clear else "WAIT",
            "current": f"position_size_multiplier={position_size_multiplier:.6f}; causes={','.join(scale_zero_causes)}",
            "release_rule": "position_size_multiplier>0 and es_hard_block removed",
            "blocker": "" if hardblock_clear else "RUNTIME_SCALE_STILL_ZERO",
        },
        {
            "condition": "account_risk",
            "status": "PASS" if account_risk_clear else "WAIT",
            "current": str(account_risk_clear),
            "release_rule": "account_risk_clear=true",
            "blocker": "" if account_risk_clear else "ACCOUNT_RISK_NOT_CLEAR",
        },
        {
            "condition": "order_budget",
            "status": "PASS" if order_budget_status == "PASS" else "WAIT",
            "current": order_budget_status,
            "release_rule": "execution_safety_contract=PASS",
            "blocker": "" if order_budget_status == "PASS" else "ORDER_BUDGET_NOT_PASS",
        },
    ]

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z"),
        "schema_version": "crash_es_release_monitor_v1",
        "scope": "read_only_crash_es_release_monitor",
        "status": monitor_status,
        "policy_change": False,
        "trading_effect": False,
        "next_action": next_action,
        "blockers": blockers,
        "release_conditions": {
            "crash_clear": crash_clear,
            "es_clear": es_clear,
            "hardblock_clear": hardblock_clear,
            "account_risk_clear": account_risk_clear,
            "order_budget_status": order_budget_status,
            "capital_recheck_ready": capital_recheck_ready,
        },
        "current_values": {
            "runtime_regime": runtime_regime,
            "macro_ret1": macro_ret1,
            "crash_day_ret_max": crash_day_ret_max,
            "es": es,
            "es_limit": es_limit,
            "position_size_multiplier": position_size_multiplier,
            "scale_zero_causes": scale_zero_causes,
        },
        "sources": {
            "config": str(CFG_PATH),
            "macro_signal": str(LOG_DIR / "macro_signal_latest.json"),
            "risk_orchestration": str(LOG_DIR / "risk_orchestration_latest.json"),
            "p1_entry_gate": str(LOG_DIR / "p1_entry_gate_status_latest.json"),
            "capital_operation_decision": str(LOG_DIR / "capital_operation_decision_report_latest.json"),
            "crash_es_validation": str(LOG_DIR / "crash_es_hardblock_validation_latest.json"),
        },
        "source_generated_at": {
            "macro_signal": macro.get("generated_at") or macro.get("as_of_ymd"),
            "risk_orchestration": risk.get("generated_at"),
            "capital_operation_decision": capital.get("generated_at"),
            "crash_es_validation": validation.get("generated_at"),
        },
        "rows": rows,
    }


def main() -> int:
    payload = build_report()
    out_json = LOG_DIR / "crash_es_release_monitor_latest.json"
    out_csv = LOG_DIR / "crash_es_release_monitor_latest.csv"
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["condition", "status", "current", "release_rule", "blocker"])
        writer.writeheader()
        writer.writerows(payload["rows"])
    print(
        json.dumps(
            {
                "status": payload["status"],
                "next_action": payload["next_action"],
                "blockers": payload["blockers"],
                "out_json": str(out_json),
                "out_csv": str(out_csv),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
