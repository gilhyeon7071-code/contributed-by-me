from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
INPUTS = [
    ("operating", LOG_DIR / "backtest_validation_latest.json"),
    ("full_market", LOG_DIR / "backtest_validation_full_market_latest.json"),
]
OUT_JSON = LOG_DIR / "backtest_acceptance_failure_diagnostic_latest.json"
OUT_CSV = LOG_DIR / "backtest_acceptance_failure_diagnostic_latest.csv"


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _find_gate(report: Dict[str, Any], name: str) -> Dict[str, Any]:
    for gate in report.get("gate_results", []) or []:
        if gate.get("name") == name:
            return gate
    return {}


def _num(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
        if pd.isna(out):
            return None
        return out
    except Exception:
        return None


def _diagnose_one(label: str, path: Path) -> Dict[str, Any]:
    report = _load_json(path)
    gate = _find_gate(report, "acceptance_pnl_turnover")
    details = gate.get("details", {}) or {}
    artifacts = report.get("artifacts", {}) or {}
    base_metrics = artifacts.get("base_metrics", {}) or {}
    operating_window = artifacts.get("operating_window", {}) or {}
    returns = pd.DataFrame((artifacts.get("base_series", {}) or {}).get("returns", []) or [])
    if len(returns) and {"date", "return"}.issubset(returns.columns):
        returns["return"] = pd.to_numeric(returns["return"], errors="coerce")
        returns = returns.dropna(subset=["return"]).sort_values("return")
        negative_days = int((returns["return"] < 0).sum())
        worst_days = [
            {"date": str(row["date"]), "return": float(row["return"])}
            for _, row in returns.head(5).iterrows()
        ]
    else:
        negative_days = None
        worst_days = []

    pnl_low = _num(details.get("pnl_ci95_low"))
    pnl_med = _num(details.get("pnl_ci95_med"))
    pnl_high = _num(details.get("pnl_ci95_high"))
    turnover_monthly = _num(details.get("turnover_monthly"))
    turnover_limit = _num(details.get("turnover_limit_monthly"))

    pnl_gate_pass = pnl_low is not None and pnl_low > 0.0
    turnover_gate_pass = (
        turnover_monthly is not None
        and turnover_limit is not None
        and turnover_monthly <= turnover_limit
    )

    fail_causes: List[str] = []
    if not pnl_gate_pass:
        fail_causes.append("PNL_CI95_LOW_NOT_POSITIVE")
    if not turnover_gate_pass:
        fail_causes.append("MONTHLY_TURNOVER_ABOVE_LIMIT_OR_MISSING")

    return {
        "scope": label,
        "input": str(path),
        "input_mtime": datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds"),
        "report_passed": bool(report.get("passed")),
        "acceptance_passed": bool(gate.get("passed")) if gate else False,
        "pnl_ci95_low": pnl_low,
        "pnl_ci95_med": pnl_med,
        "pnl_ci95_high": pnl_high,
        "pnl_low_margin_to_zero": None if pnl_low is None else pnl_low - 0.0,
        "turnover_monthly": turnover_monthly,
        "turnover_limit_monthly": turnover_limit,
        "turnover_margin_to_limit": (
            None
            if turnover_monthly is None or turnover_limit is None
            else turnover_limit - turnover_monthly
        ),
        "pnl_gate_pass": bool(pnl_gate_pass),
        "turnover_gate_pass": bool(turnover_gate_pass),
        "fail_causes": ";".join(fail_causes),
        "oper_start_ymd": operating_window.get("oper_start_ymd"),
        "operating_rows": operating_window.get("operating_rows"),
        "raw_rows": operating_window.get("raw_rows"),
        "base_sharpe": _num(base_metrics.get("sharpe")),
        "base_max_drawdown": _num(base_metrics.get("max_drawdown")),
        "base_total_return": _num(base_metrics.get("pnl_total_return")),
        "base_annual_turnover": _num(base_metrics.get("annual_turnover")),
        "negative_return_days": negative_days,
        "worst_return_days": worst_days,
        "diagnosis": (
            "acceptance criteria pass"
            if not fail_causes
            else "acceptance blocked by lower-bound pnl confidence interval; turnover is within limit"
            if fail_causes == ["PNL_CI95_LOW_NOT_POSITIVE"]
            else "acceptance blocked by multiple or missing acceptance criteria"
        ),
        "policy_change": False,
        "trading_effect": False,
        "order_path_effect": False,
    }


def main() -> int:
    rows = [_diagnose_one(label, path) for label, path in INPUTS if path.exists()]
    overall_status = "PASS" if rows else "FAIL"
    blocking = [r for r in rows if r["fail_causes"]]
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "version": "backtest_acceptance_failure_diagnostic_v1",
        "status": overall_status,
        "quality": "PASS" if rows else "FAIL",
        "reason": "ok" if rows else "no_input_reports",
        "rows": len(rows),
        "blocking_rows": len(blocking),
        "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV)},
        "policy_change": False,
        "trading_effect": False,
        "order_path_effect": False,
        "diagnostics": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(rows).to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(json.dumps({"status": overall_status, "rows": len(rows), "blocking_rows": len(blocking)}, ensure_ascii=False))
    return 0 if rows else 2


if __name__ == "__main__":
    raise SystemExit(main())
