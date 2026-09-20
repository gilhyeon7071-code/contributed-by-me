from __future__ import annotations

import datetime as dt
import json
import math
import sys
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

sys.path.insert(0, str(ROOT / "tools"))

from backtest_real_strategy_adapter import real_strategy_signal  # noqa: E402
from backtest_validation_framework import _safe_pct_change, ensure_signal_schema, load_market_csv  # noqa: E402


def _finite_float(v: Any) -> float | None:
    try:
        x = float(v)
    except Exception:
        return None
    return x if math.isfinite(x) else None


def _corr(left: pd.Series, right: pd.Series) -> Dict[str, Any]:
    frame = pd.DataFrame({"left": pd.to_numeric(left, errors="coerce"), "right": pd.to_numeric(right, errors="coerce")}).dropna()
    value = frame["left"].corr(frame["right"]) if len(frame) else float("nan")
    return {"corr": _finite_float(value), "n_obs": int(len(frame))}


def _gate_detail(report: Dict[str, Any]) -> Dict[str, Any]:
    for gate in report.get("gate_results", []):
        if gate.get("name") == "look_ahead_proxy":
            return dict(gate.get("details", {}) or {})
    return {}


def build() -> Dict[str, Any]:
    validation_path = LOG_DIR / "backtest_validation_latest.json"
    market_path = LOG_DIR / "backtest_market_ohlc_real_overlap_latest.csv"
    report = json.loads(validation_path.read_text(encoding="utf-8"))
    market, market_meta = load_market_csv(market_path, "date", column_map={})
    params = ((report.get("artifacts", {}) or {}).get("integration", {}) or {}).get("params", {}) or {
        "fast": 8,
        "slow": 100,
        "allow_short": True,
        "position_scale": 0.7,
        "daily_gross_turnover_cap_pct": 0.008,
    }
    signal = ensure_signal_schema(real_strategy_signal(market, params), market.index)
    close_ret = _safe_pct_change(market["close"])
    future_ret = close_ret.shift(-1)

    gate = _gate_detail(report)
    threshold = float(gate.get("threshold", 0.2) or 0.2)
    variants: List[Dict[str, Any]] = []
    checks = [
        ("gate_signal_vs_next_return", signal["signal"], future_ret),
        ("position_vs_next_return", signal["position"], future_ret),
        ("signal_lag1_vs_next_return", signal["signal"].shift(1), future_ret),
        ("signal_vs_same_day_return", signal["signal"], close_ret),
        ("signal_vs_prev_day_return", signal["signal"], close_ret.shift(1)),
    ]
    for name, left, right in checks:
        row = {"name": name, **_corr(left, right)}
        c = row.get("corr")
        row["abs_corr"] = abs(float(c)) if c is not None else None
        row["exceeds_threshold"] = bool(c is not None and abs(float(c)) > threshold)
        variants.append(row)

    mismatch = float((signal["position"].fillna(0.0) != signal["signal"].shift(1).fillna(0.0)).mean())
    gate_corr = variants[0].get("corr")
    gate_abs = abs(float(gate_corr)) if gate_corr is not None else None
    return {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "source_json": str(validation_path),
        "market_csv": str(market_path),
        "status": "FAIL_CONFIRMED" if gate_abs is not None and gate_abs > threshold else "PASS",
        "policy_effect": False,
        "trading_effect": False,
        "gate_detail": gate,
        "threshold": threshold,
        "market_meta": market_meta,
        "params": params,
        "position_lag_mismatch_ratio": mismatch,
        "correlation_variants": variants,
        "interpretation": {
            "direct_cause": "abs(signal_vs_next_return_corr) > threshold" if gate_abs is not None and gate_abs > threshold else "not_failed_by_current_threshold",
            "gate_or_threshold_changed": False,
            "entry_approval_changed": False,
        },
    }


def main() -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    payload = build()
    latest_json = LOG_DIR / "backtest_lookahead_proxy_diagnostic_latest.json"
    latest_csv = LOG_DIR / "backtest_lookahead_proxy_diagnostic_latest.csv"
    latest_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(payload["correlation_variants"]).to_csv(latest_csv, index=False, encoding="utf-8-sig")
    print(f"[LOOKAHEAD] status={payload['status']} json={latest_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
