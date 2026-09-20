from __future__ import annotations

import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CFG_PATH = ROOT / "paper" / "paper_engine_config.json"
RUN_LOG_PATH = LOG_DIR / "run_paper_daily_last.txt"
PNL_PATH = LOG_DIR / "paper_pnl_summary_last.json"
RISK_PATH = LOG_DIR / "risk_orchestration_latest.json"
DDM_PATH = LOG_DIR / "paper_ddm_status_latest.json"


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


def _latest_regex_float(text: str, pattern: str, default: Optional[float] = None) -> Optional[float]:
    matches = list(re.finditer(pattern, text))
    if not matches:
        return default
    return _to_float(matches[-1].group(1), default if default is not None else 0.0)


def _all_regex_rows(text: str, pattern: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for match in re.finditer(pattern, text):
        rows.append(match.groupdict())
    return rows


def _runtime_from_log() -> Dict[str, Any]:
    text = ""
    if RUN_LOG_PATH.exists():
        try:
            text = RUN_LOG_PATH.read_text(encoding="utf-8", errors="replace")
        except Exception:
            text = ""

    cap_rows = _all_regex_rows(
        text,
        r"\[CAPITAL_BASIS\] configured=(?P<configured>[0-9.]+) "
        r"account_equity=(?P<account_equity>[0-9.]+) "
        r"effective=(?P<effective>[0-9.]+) basis=(?P<basis>[^\r\n]+)",
    )
    macro_rows = _all_regex_rows(
        text,
        r"\[MACRO\] exposure_multiplier=(?P<mult>[0-9.]+) -> gross_exposure "
        r"(?P<before>[0-9.]+)->(?P<after>[0-9.]+)",
    )
    ddm_rows = _all_regex_rows(
        text,
        r"\[DDM\] consecutive_loss_days=(?P<days>[0-9]+) >= (?P<threshold>[0-9]+) "
        r"-> gross_exposure (?P<before>[0-9.]+)->(?P<after>[0-9.]+)",
    )
    budget_rows = _all_regex_rows(
        text,
        r"\[BUDGET_COMMON_LINK\] normal_basic_alloc (?P<basic_before>[0-9.]+)->(?P<basic_after>[0-9.]+) "
        r"reserved surge=(?P<surge>[0-9.]+) split=(?P<split>[0-9.]+) "
        r"recovery=(?P<recovery>[0-9.]+) reserve=(?P<reserve>[0-9.]+)",
    )
    risk_cap_rows = _all_regex_rows(
        text,
        r"\[RISK_CAP\] capital_total=(?P<capital_total>[0-9.]+) gross_cap=(?P<gross_cap>[0-9.]+) "
        r"daily_new_cap=(?P<daily_new_cap>[0-9.]+) open_notional=(?P<open_notional>[0-9.]+)",
    )

    latest_cap = cap_rows[-1] if cap_rows else {}
    latest_macro = macro_rows[-1] if macro_rows else {}
    latest_budget = budget_rows[-1] if budget_rows else {}
    latest_ddm = ddm_rows[-1] if ddm_rows else {}

    ddm_mult = 1.0
    if latest_ddm:
        before = _to_float(latest_ddm.get("before"), 0.0)
        after = _to_float(latest_ddm.get("after"), before)
        ddm_mult = after / before if before > 0 else 1.0

    return {
        "path": str(RUN_LOG_PATH),
        "exists": RUN_LOG_PATH.exists(),
        "capital_basis_latest": latest_cap,
        "macro_latest": latest_macro,
        "budget_common_latest": latest_budget,
        "ddm_consecutive_latest": latest_ddm,
        "ddm_consecutive_multiplier": round(ddm_mult, 6),
        "risk_cap_rows": risk_cap_rows[-6:],
        "line_counts": {
            "capital_basis": len(cap_rows),
            "macro": len(macro_rows),
            "ddm_consecutive": len(ddm_rows),
            "budget_common": len(budget_rows),
            "risk_cap": len(risk_cap_rows),
        },
    }


def _scenario_row(
    *,
    scenario: str,
    context: str,
    capital: float,
    gross_pct: float,
    macro_mult: float,
    ddm_mult: float,
    max_positions: int,
    current_gross_pct: float,
) -> Dict[str, Any]:
    effective_gross_pct = max(0.0, min(1.0, gross_pct * macro_mult * ddm_mult))
    gross_cap = capital * effective_gross_pct
    per_position_at_cap = gross_cap / float(max(1, max_positions))
    return {
        "scenario": scenario,
        "context": context,
        "configured_gross_exposure_pct": round(gross_pct, 6),
        "macro_multiplier": round(macro_mult, 6),
        "ddm_multiplier": round(ddm_mult, 6),
        "effective_gross_exposure_pct": round(effective_gross_pct, 6),
        "effective_gross_cap_krw": round(gross_cap, 2),
        "idle_floor_krw_if_full_cap": round(max(0.0, capital - gross_cap), 2),
        "per_position_cap_if_18_full_krw": round(per_position_at_cap, 2),
        "gross_cap_delta_vs_current_krw": round(capital * (effective_gross_pct - current_gross_pct), 2),
    }


def build_report() -> Dict[str, Any]:
    cfg = _read_json(CFG_PATH)
    pnl = _read_json(PNL_PATH)
    risk = _read_json(RISK_PATH)
    ddm = _read_json(DDM_PATH)
    runtime_log = _runtime_from_log()

    budget = cfg.get("capital_budget_policy") if isinstance(cfg.get("capital_budget_policy"), dict) else {}
    sizing_mode = str(cfg.get("sizing_mode") or "")
    configured_capital = _to_float(cfg.get("capital_total"), 0.0)
    log_effective_capital = _to_float(_get(runtime_log, ["capital_basis_latest", "effective"]), 0.0)
    capital_basis = log_effective_capital if log_effective_capital > 0 else configured_capital
    max_positions = max(1, int(_to_float(cfg.get("max_positions"), 1.0)))
    cash_per_trade = _to_float(cfg.get("cash_per_trade"), 0.0)
    current_budget_gross = _pct01(budget.get("gross_exposure_pct"), _pct01(cfg.get("max_gross_exposure_pct"), 1.0))
    macro_mult = _to_float(_get(runtime_log, ["macro_latest", "mult"]), 1.0) or 1.0
    ddm_mult = _to_float(runtime_log.get("ddm_consecutive_multiplier"), 1.0) or 1.0
    current_main_effective_gross = max(0.0, min(1.0, current_budget_gross * macro_mult))
    current_defensive_effective_gross = max(0.0, min(1.0, current_budget_gross * macro_mult * ddm_mult))

    basic_config = _pct01(budget.get("basic_alloc_pct"), 0.0)
    basic_target_positions = max(1, int(_to_float(budget.get("basic_target_positions"), max_positions)))
    budget_common = runtime_log.get("budget_common_latest") if isinstance(runtime_log.get("budget_common_latest"), dict) else {}
    runtime_basic_effective = _to_float(budget_common.get("basic_after"), basic_config)
    runtime_split_reserved = _to_float(budget_common.get("split"), _pct01(budget.get("split_alloc_pct"), 0.0))

    gross_candidates = [current_budget_gross, 0.65, 0.75, 0.80]
    seen = set()
    scenario_rows: List[Dict[str, Any]] = []
    for gross in gross_candidates:
        gross = round(max(0.0, min(1.0, gross)), 6)
        if gross in seen:
            continue
        seen.add(gross)
        label = "current" if abs(gross - current_budget_gross) < 1e-9 else f"gross_{gross:.2f}"
        scenario_rows.append(
            _scenario_row(
                scenario=label,
                context="main_macro_only",
                capital=capital_basis,
                gross_pct=gross,
                macro_mult=macro_mult,
                ddm_mult=1.0,
                max_positions=max_positions,
                current_gross_pct=current_main_effective_gross,
            )
        )
        scenario_rows.append(
            _scenario_row(
                scenario=label,
                context="defensive_macro_plus_ddm",
                capital=capital_basis,
                gross_pct=gross,
                macro_mult=macro_mult,
                ddm_mult=ddm_mult,
                max_positions=max_positions,
                current_gross_pct=current_defensive_effective_gross,
            )
        )

    strategies = pnl.get("strategies") if isinstance(pnl.get("strategies"), list) else []
    weak_strategy_rows = [
        {
            "strategy": str(row.get("strategy")),
            "trades_used": int(_to_float(row.get("trades_used"), 0.0)),
            "win_rate": round(_to_float(row.get("win_rate"), 0.0), 6),
            "avg_ret": round(_to_float(row.get("avg_ret"), 0.0), 6),
            "gross_pf": round(_to_float(row.get("gross_pf"), 0.0), 6),
        }
        for row in strategies
        if isinstance(row, dict)
    ]
    pf_values = [_to_float(row.get("gross_pf"), 0.0) for row in weak_strategy_rows]
    evidence_supports_immediate_expansion = bool(pf_values) and min(pf_values) >= 1.0

    if sizing_mode == "capital_slots":
        cash_per_trade_effect = "NOT_PRIMARY_IN_CAPITAL_SLOTS"
    elif sizing_mode == "fixed_cash":
        cash_per_trade_effect = "PRIMARY_PER_TRADE_NOTIONAL"
    else:
        cash_per_trade_effect = "MODE_DEPENDENT"

    recommendation_status = (
        "SCENARIO_ONLY_NO_IMMEDIATE_EXPANSION_EVIDENCE"
        if not evidence_supports_immediate_expansion
        else "EXPANSION_CANDIDATE_REQUIRES_POLICY_APPROVAL"
    )

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z"),
        "schema_version": "capital_sizing_scenario_report_v1",
        "scope": "read_only_capital_sizing_scenario",
        "policy_change": False,
        "trading_effect": False,
        "sources": {
            "config": str(CFG_PATH),
            "run_log": str(RUN_LOG_PATH),
            "paper_pnl_summary": str(PNL_PATH),
            "risk_orchestration": str(RISK_PATH),
            "paper_ddm_status": str(DDM_PATH),
        },
        "source_generated_at": {
            "paper_pnl_summary": pnl.get("generated_at"),
            "risk_orchestration": risk.get("generated_at"),
            "paper_ddm_status": ddm.get("generated_at"),
        },
        # [2026-09-10] `generated_at` 은 신선도가 아니다.
        #   paper_pnl_summary 는 매일 새로 생성돼 generated_at 이 늘 오늘인데,
        #   내용은 trades_calc.csv 의 마지막 청산일에 묶여 있다.
        #   2026-09-10 07:36 생성분의 as_of 가 20260825(11거래일 뒤) 였다.
        #   **내용이 언제 것인지는 as_of_freshness 를 봐야 한다.**
        "source_freshness": {
            "paper_pnl_summary": pnl.get("as_of_freshness"),
            "paper_pnl_summary_as_of": pnl.get("as_of"),
        },
        "current_config": {
            "sizing_mode": sizing_mode,
            "capital_total_krw": configured_capital,
            "capital_basis_used_krw": capital_basis,
            "cash_per_trade_krw": cash_per_trade,
            "cash_per_trade_effect": cash_per_trade_effect,
            "max_positions": max_positions,
            "nominal_capital_slot_krw": round(capital_basis / float(max_positions), 2),
            "gross_exposure_pct": current_budget_gross,
            "max_daily_new_exposure_pct": _pct01(cfg.get("max_daily_new_exposure_pct"), 1.0),
        },
        "allocation_basis": {
            "basic_alloc_pct_config": basic_config,
            "basic_target_positions": basic_target_positions,
            "basic_per_symbol_config_krw": round(capital_basis * basic_config / float(basic_target_positions), 2),
            "basic_alloc_pct_runtime_effective": round(runtime_basic_effective, 6),
            "basic_per_symbol_runtime_effective_krw": round(capital_basis * runtime_basic_effective / float(basic_target_positions), 2),
            "surge_alloc_pct_config": _pct01(budget.get("surge_alloc_pct"), 0.0),
            "split_alloc_pct_config": _pct01(budget.get("split_alloc_pct"), 0.0),
            "split_alloc_pct_runtime_reserved": round(runtime_split_reserved, 6),
            "recovery_alloc_pct_config": _pct01(budget.get("recovery_alloc_pct"), 0.0),
            "reserve_alloc_pct_config": _pct01(budget.get("reserve_alloc_pct"), 0.0),
        },
        "runtime_modifiers": {
            "macro_multiplier": macro_mult,
            "ddm_consecutive_multiplier_detected": ddm_mult,
            "current_main_effective_gross_pct": round(current_main_effective_gross, 6),
            "current_defensive_effective_gross_pct": round(current_defensive_effective_gross, 6),
            "risk_orchestration_position_size_multiplier": _to_float(risk.get("position_size_multiplier"), 0.0),
            "account_risk_clear": bool(_get(risk, ["risk_orchestration", "account_risk_clear"], False)),
        },
        "scenario_rows": scenario_rows,
        "performance_context": {
            "pnl_generated_at": pnl.get("generated_at"),
            "trades_used": int(_to_float(pnl.get("trades_used"), 0.0)),
            "avg_ret": round(_to_float(pnl.get("avg_ret"), 0.0), 6),
            "win_rate": round(_to_float(pnl.get("win_rate"), 0.0), 6),
            "gross_pf": round(_to_float(pnl.get("gross_pf"), 0.0), 6),
            "strategies": weak_strategy_rows,
        },
        "decision": {
            "status": recommendation_status,
            "reason": (
                "latest_strategy_pf_below_1_or_missing"
                if not evidence_supports_immediate_expansion
                else "all_strategy_pf_at_or_above_1"
            ),
            "cash_per_trade_raise_judgment": cash_per_trade_effect,
            "primary_tuning_axis": "gross_exposure_pct_and_allocation_caps",
            "read_only_next_artifact": "scenario_rows",
        },
        "runtime_log_extract": runtime_log,
    }


def main() -> int:
    payload = build_report()
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = LOG_DIR / "capital_sizing_scenario_report_latest.json"
    out_csv = LOG_DIR / "capital_sizing_scenario_report_latest.csv"
    out_json_stamped = LOG_DIR / f"capital_sizing_scenario_report_{stamp}.json"
    out_csv_stamped = LOG_DIR / f"capital_sizing_scenario_report_{stamp}.csv"

    for path in (out_json, out_json_stamped):
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    rows = payload["scenario_rows"]
    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    with out_csv_stamped.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print(
        json.dumps(
            {
                "status": "OK",
                "decision_status": payload["decision"]["status"],
                "out_json": str(out_json),
                "out_csv": str(out_csv),
                "out_json_stamped": str(out_json_stamped),
                "out_csv_stamped": str(out_csv_stamped),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
