from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List


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
    if out > 1.0:
        out /= 100.0
    return max(0.0, min(1.0, out))


def _get(dct: Dict[str, Any], path: Iterable[str], default: Any = None) -> Any:
    cur: Any = dct
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def _money(value: Any) -> float:
    return round(_to_float(value, 0.0), 2)


def build_report() -> Dict[str, Any]:
    cfg = _read_json(CFG_PATH)
    ddm = _read_json(LOG_DIR / "paper_ddm_status_latest.json")
    risk = _read_json(LOG_DIR / "risk_orchestration_latest.json")
    safety = _read_json(LOG_DIR / "execution_safety_contract_latest.json")
    month = _read_json(LOG_DIR / "entry_policy_month_split_report_latest.json")
    playbook = _read_json(LOG_DIR / "production_risk_playbook_latest.json")

    cap_total = _money(cfg.get("capital_total"))
    budget = cfg.get("capital_budget_policy") if isinstance(cfg.get("capital_budget_policy"), dict) else {}
    risk_detail = risk.get("risk_orchestration") if isinstance(risk.get("risk_orchestration"), dict) else {}
    ddm_account = _get(ddm, ["metric_details", "account_basis"], {})
    safety_ctx = safety.get("context") if isinstance(safety.get("context"), dict) else {}

    gross_pct = _pct01(budget.get("gross_exposure_pct"), _pct01(cfg.get("max_gross_exposure_pct"), 1.0))
    basic_pct = _pct01(budget.get("basic_alloc_pct"), 0.0)
    surge_pct = _pct01(budget.get("surge_alloc_pct"), 0.0)
    split_pct = _pct01(budget.get("split_alloc_pct"), 0.0)
    reserve_pct = _pct01(budget.get("reserve_alloc_pct"), 0.0)
    daily_new_pct = _pct01(cfg.get("max_daily_new_exposure_pct"), 1.0)
    max_positions = int(_to_float(cfg.get("max_positions"), 0.0))
    basic_target_positions = int(_to_float(budget.get("basic_target_positions"), max_positions or 1))

    basic_per_symbol_pct = basic_pct / float(max(1, basic_target_positions))
    gross_cap_krw = cap_total * gross_pct
    daily_new_cap_krw = cap_total * daily_new_pct
    basic_total_krw = cap_total * basic_pct
    basic_per_symbol_krw = cap_total * basic_per_symbol_pct
    surge_total_krw = cap_total * surge_pct
    split_total_krw = cap_total * split_pct
    reserve_krw = cap_total * reserve_pct

    account_dd = _to_float(risk_detail.get("account_dd_current"), _to_float(ddm.get("current_mdd_abs")))
    account_dd_limit = _to_float(risk_detail.get("account_dd_limit"), _to_float(_get(ddm, ["metric_details", "account_basis", "max_drawdown_pct"], 0.0)))
    if account_dd_limit < 0:
        account_dd_limit = abs(account_dd_limit)
    account_daily_loss = _to_float(risk_detail.get("account_daily_loss"), abs(_to_float(_get(ddm, ["metric_details", "account_basis", "daily_loss_pct"], 0.0))))
    account_daily_loss_limit = _to_float(risk_detail.get("account_daily_loss_limit"), _to_float(_get(cfg, ["kill_switch", "max_daily_loss_pct"], 0.0)))

    may_overall: Dict[str, Any] = {}
    if isinstance(month.get("windows"), dict):
        for row in _get(month, ["windows", "202605", "sections", "overall"], []) or []:
            if isinstance(row, dict) and row.get("group") == "ALL":
                may_overall = row
                break

    runtime_scale = _to_float(risk.get("position_size_multiplier"), _to_float(risk_detail.get("scale"), 0.0))
    scale_zero_causes = list(risk_detail.get("scale_zero_causes") or [])
    market_regime = str(risk.get("market_regime") or "")
    account_risk_clear = bool(risk_detail.get("account_risk_clear", False))
    safety_status = str(safety.get("status") or "UNKNOWN")
    safety_allowed = int(_to_float(_get(safety, ["summary", "allowed"], 0), 0.0))
    safety_blocked = int(_to_float(_get(safety, ["summary", "safety_blocked"], 0), 0.0))

    if runtime_scale <= 0:
        decision_status = "CAPITAL_DEFINED_BUT_RUNTIME_BLOCKED"
        decision_reason = ",".join(scale_zero_causes) or "position_size_multiplier_zero"
        capital_action = "NO_NEW_CAPITAL_DEPLOYMENT"
    elif not account_risk_clear:
        decision_status = "ACCOUNT_RISK_NOT_CLEAR"
        decision_reason = "account_dd_or_daily_loss_limit"
        capital_action = "NO_EXPANSION"
    elif safety_status != "PASS" or safety_blocked > 0:
        decision_status = "ORDER_BUDGET_CONTRACT_NOT_CLEAR"
        decision_reason = f"execution_safety_contract:{safety_status}"
        capital_action = "NO_EXPANSION"
    elif _to_float(may_overall.get("avg_net_ret"), 0.0) < 0 and _to_float(may_overall.get("win_rate"), 1.0) < 0.3:
        decision_status = "CAPITAL_AVAILABLE_SELECTIVE_ONLY"
        decision_reason = "may_realized_entry_quality_weak"
        capital_action = "SELECTIVE_MIN_SIZE_ONLY"
    else:
        decision_status = "CAPITAL_AVAILABLE_WITH_POLICY_LIMITS"
        decision_reason = "account_and_order_budget_clear"
        capital_action = "USE_CONFIGURED_BUDGET_LIMITS"

    rows: List[Dict[str, Any]] = [
        {"section": "account", "metric": "capital_total_krw", "value": cap_total, "note": "configured capital"},
        {"section": "account", "metric": "equity_est_krw", "value": _money(ddm_account.get("equity_est")), "note": "mark-to-market account basis when available"},
        {"section": "account", "metric": "realized_equity_est_krw", "value": _money(ddm_account.get("realized_equity_est")), "note": "realized account basis"},
        {"section": "account", "metric": "mark_to_market_equity_est_krw", "value": _money(ddm_account.get("mark_to_market_equity_est")), "note": "includes current holdings valuation"},
        {"section": "account", "metric": "unrealized_pnl_krw", "value": _money(ddm_account.get("unrealized_pnl_krw")), "note": "current open holdings mark-to-market PnL"},
        {"section": "account", "metric": "position_value_krw", "value": _money(ddm_account.get("position_value")), "note": "current open exposure"},
        {"section": "account", "metric": "account_dd_pct", "value": round(account_dd * 100.0, 6), "note": "account risk basis"},
        {"section": "account", "metric": "account_daily_loss_pct", "value": round(account_daily_loss * 100.0, 6), "note": "daily realized loss over capital"},
        {"section": "budget", "metric": "gross_cap_krw", "value": round(gross_cap_krw, 2), "note": f"{gross_pct:.4f} of capital"},
        {"section": "budget", "metric": "daily_new_cap_krw", "value": round(daily_new_cap_krw, 2), "note": f"{daily_new_pct:.4f} of capital"},
        {"section": "budget", "metric": "basic_total_krw", "value": round(basic_total_krw, 2), "note": f"{basic_pct:.4f} of capital"},
        {"section": "budget", "metric": "basic_per_symbol_krw", "value": round(basic_per_symbol_krw, 2), "note": f"basic / {basic_target_positions} target positions"},
        {"section": "budget", "metric": "surge_total_krw", "value": round(surge_total_krw, 2), "note": f"{surge_pct:.4f} of capital"},
        {"section": "budget", "metric": "split_total_krw", "value": round(split_total_krw, 2), "note": f"{split_pct:.4f} of capital"},
        {"section": "budget", "metric": "reserve_krw", "value": round(reserve_krw, 2), "note": f"{reserve_pct:.4f} of capital"},
        {"section": "runtime", "metric": "market_regime", "value": market_regime, "note": "latest risk orchestration"},
        {"section": "runtime", "metric": "position_size_multiplier", "value": runtime_scale, "note": "effective current execution scale"},
        {"section": "runtime", "metric": "scale_zero_causes", "value": ",".join(scale_zero_causes), "note": "hard runtime blockers"},
        {"section": "runtime", "metric": "entry_safety_status", "value": safety_status, "note": f"allowed={safety_allowed}, safety_blocked={safety_blocked}"},
        {"section": "performance", "metric": "may_avg_net_ret_pct", "value": round(_to_float(may_overall.get("avg_net_ret")) * 100.0, 6), "note": "realized trades_calc basis"},
        {"section": "performance", "metric": "may_win_rate_pct", "value": round(_to_float(may_overall.get("win_rate")) * 100.0, 6), "note": "realized trades_calc basis"},
        {"section": "decision", "metric": "decision_status", "value": decision_status, "note": decision_reason},
        {"section": "decision", "metric": "capital_action", "value": capital_action, "note": "read-only judgment; no policy mutation"},
    ]

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z"),
        "schema_version": "capital_operation_decision_report_v1",
        "scope": "read_only_capital_operation_decision_status",
        "policy_change": False,
        "trading_effect": False,
        "sources": {
            "config": str(CFG_PATH),
            "paper_ddm_status": str(LOG_DIR / "paper_ddm_status_latest.json"),
            "risk_orchestration": str(LOG_DIR / "risk_orchestration_latest.json"),
            "execution_safety_contract": str(LOG_DIR / "execution_safety_contract_latest.json"),
            "entry_policy_month_split": str(LOG_DIR / "entry_policy_month_split_report_latest.json"),
            "production_risk_playbook": str(LOG_DIR / "production_risk_playbook_latest.json"),
        },
        "source_generated_at": {
            "paper_ddm_status": ddm.get("generated_at"),
            "risk_orchestration": risk.get("generated_at"),
            "execution_safety_contract": safety.get("generated_at"),
            "entry_policy_month_split": month.get("generated_at"),
            "production_risk_playbook": playbook.get("generated_at"),
        },
        "decision": {
            "status": decision_status,
            "reason": decision_reason,
            "capital_action": capital_action,
            "account_risk_clear": account_risk_clear,
            "runtime_scale": runtime_scale,
            "scale_zero_causes": scale_zero_causes,
            "order_budget_contract_status": safety_status,
        },
        "capital_budget": {
            "capital_total_krw": cap_total,
            "gross_cap_pct": gross_pct,
            "gross_cap_krw": round(gross_cap_krw, 2),
            "daily_new_cap_pct": daily_new_pct,
            "daily_new_cap_krw": round(daily_new_cap_krw, 2),
            "basic_alloc_pct": basic_pct,
            "basic_total_krw": round(basic_total_krw, 2),
            "basic_target_positions": basic_target_positions,
            "basic_per_symbol_pct": round(basic_per_symbol_pct, 8),
            "basic_per_symbol_krw": round(basic_per_symbol_krw, 2),
            "surge_alloc_pct": surge_pct,
            "surge_total_krw": round(surge_total_krw, 2),
            "split_alloc_pct": split_pct,
            "split_total_krw": round(split_total_krw, 2),
            "reserve_alloc_pct": reserve_pct,
            "reserve_krw": round(reserve_krw, 2),
        },
        "account_basis": {
            "capital_total_krw": cap_total,
            "equity_est_krw": _money(ddm_account.get("equity_est")),
            "realized_equity_est_krw": _money(ddm_account.get("realized_equity_est")),
            "mark_to_market_equity_est_krw": _money(ddm_account.get("mark_to_market_equity_est")),
            "unrealized_pnl_krw": _money(ddm_account.get("unrealized_pnl_krw")),
            "position_value_krw": _money(ddm_account.get("position_value")),
            "account_dd_pct": round(account_dd * 100.0, 6),
            "account_dd_limit_pct": round(account_dd_limit * 100.0, 6),
            "account_daily_loss_pct": round(account_daily_loss * 100.0, 6),
            "account_daily_loss_limit_pct": round(account_daily_loss_limit * 100.0, 6),
        },
        "runtime_basis": {
            "market_regime": market_regime,
            "position_size_multiplier": runtime_scale,
            "risk_orchestration_scale": _to_float(risk_detail.get("scale"), 0.0),
            "scale_zero_causes": scale_zero_causes,
            "es": _to_float(risk_detail.get("es"), 0.0),
            "es_limit": _to_float(risk_detail.get("es_limit"), 0.0),
            "es_hard_block": bool(risk_detail.get("es_hard_block", False)),
            "risk_advisory_flags": list(risk_detail.get("risk_advisory_flags") or []),
            "strategy_dd_current_pct": round(_to_float(risk_detail.get("strategy_dd_current"), 0.0) * 100.0, 6),
            "account_risk_clear": account_risk_clear,
        },
        "order_budget_contract": {
            "status": safety_status,
            "summary": safety.get("summary", {}),
            "context": safety_ctx,
        },
        "performance_context": {
            "may_overall": may_overall,
        },
        "rows": rows,
    }


def main() -> int:
    payload = build_report()
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    out_json = LOG_DIR / "capital_operation_decision_report_latest.json"
    out_csv = LOG_DIR / "capital_operation_decision_report_latest.csv"
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["section", "metric", "value", "note"])
        writer.writeheader()
        writer.writerows(payload["rows"])
    print(
        json.dumps(
            {
                "status": "OK",
                "decision_status": payload["decision"]["status"],
                "capital_action": payload["decision"]["capital_action"],
                "out_json": str(out_json),
                "out_csv": str(out_csv),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
