from __future__ import annotations

import csv
import importlib.util
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
MARKET_CSV = LOG_DIR / "backtest_market_ohlc_real_overlap_latest.csv"
OUT_JSON = LOG_DIR / "lookahead_proxy_diagnostic_latest.json"
OUT_CSV = LOG_DIR / "lookahead_proxy_diagnostic_latest.csv"
ADAPTER_PATH = ROOT / "tools" / "backtest_real_strategy_adapter.py"
PARAMS_JSON = LOG_DIR / "backtest_validation_params_surge_scenario.json"


def _safe_float(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    if pd.isna(out):
        return None
    return out


def _load_adapter() -> Any:
    spec = importlib.util.spec_from_file_location("bt_adapter_for_lookahead_diag", ADAPTER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load adapter: {ADAPTER_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_market() -> pd.DataFrame:
    df = pd.read_csv(MARKET_CSV, dtype=str, encoding="utf-8-sig")
    if "date" not in df.columns or "close" not in df.columns:
        raise ValueError("market csv missing date or close column")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "close"]).copy()
    df = df.sort_values("date").set_index("date")
    return df


def _load_params() -> dict[str, Any]:
    if not PARAMS_JSON.exists():
        return {}
    obj = json.loads(PARAMS_JSON.read_text(encoding="utf-8-sig"))
    return obj if isinstance(obj, dict) else {}


def _corr(frame: pd.DataFrame, signal_col: str, ret_col: str) -> float | None:
    work = frame[[signal_col, ret_col]].dropna()
    if len(work) < 2:
        return None
    return _safe_float(work[signal_col].corr(work[ret_col]))


def main() -> int:
    os.environ["BT_ENTRY_QUALITY_AUDIT_SCENARIO"] = "block_flagged_orders"
    adapter = _load_adapter()
    market = _load_market()
    params = _load_params()
    signal = adapter.real_strategy_signal(market, params)
    daily = adapter._apply_validation_turnover_cap(adapter._build_daily_frame(market), params)

    frame = pd.DataFrame(index=market.index)
    frame["close"] = pd.to_numeric(market["close"], errors="coerce")
    frame["signal"] = pd.to_numeric(signal.get("signal"), errors="coerce")
    frame["position"] = pd.to_numeric(signal.get("position"), errors="coerce")
    frame["turnover"] = pd.to_numeric(signal.get("turnover"), errors="coerce")
    frame["ledger_net_return"] = pd.to_numeric(daily.get("net_return"), errors="coerce")
    frame["market_future_ret_1d"] = frame["close"].pct_change().shift(-1)
    frame["market_same_day_ret"] = frame["close"].pct_change()

    oper_start = pd.to_datetime(os.getenv("PAPER_OPER_START_YMD", "20260301"), format="%Y%m%d", errors="coerce")
    if pd.isna(oper_start):
        oper_start = pd.Timestamp("2026-03-01")
    active = frame.loc[frame.index >= oper_start].copy()

    rows = []
    for idx, row in active.iterrows():
        sig = _safe_float(row.get("signal"))
        fut = _safe_float(row.get("market_future_ret_1d"))
        if sig is None or fut is None:
            continue
        rows.append(
            {
                "date": idx.strftime("%Y%m%d"),
                "signal": sig,
                "position": _safe_float(row.get("position")),
                "turnover": _safe_float(row.get("turnover")),
                "market_future_ret_1d": fut,
                "market_same_day_ret": _safe_float(row.get("market_same_day_ret")),
                "ledger_net_return": _safe_float(row.get("ledger_net_return")),
                "abs_signal_x_future_ret": abs(sig * fut),
                "signal_x_future_ret": sig * fut,
            }
        )
    rows.sort(key=lambda r: r["abs_signal_x_future_ret"], reverse=True)

    corr_frame = active.copy()
    lag_summary = []
    for lag in range(-3, 4):
        col = f"market_ret_shift_{lag}"
        corr_frame[col] = corr_frame["close"].pct_change().shift(lag)
        lag_summary.append(
            {
                "ret_shift": lag,
                "corr_signal": _corr(corr_frame, "signal", col),
                "corr_position": _corr(corr_frame, "position", col),
                "n_obs": int(corr_frame[["signal", col]].dropna().shape[0]),
            }
        )

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS" if rows else "WARN",
        "scope": "read_only_lookahead_proxy_diagnostic",
        "scenario_env": {"BT_ENTRY_QUALITY_AUDIT_SCENARIO": "block_flagged_orders"},
        "inputs": {
            "market_csv": str(MARKET_CSV),
            "adapter": str(ADAPTER_PATH),
            "params_json": str(PARAMS_JSON),
            "audit_json": str(LOG_DIR / "entry_quality_cross_section_audit_latest.json"),
        },
        "params": params,
        "summary": {
            "oper_start_ymd": oper_start.strftime("%Y%m%d"),
            "n_obs": len(rows),
            "lookahead_corr_signal_future_1d": _corr(active, "signal", "market_future_ret_1d"),
            "lookahead_threshold": 0.2,
            "position_lag_mismatch_ratio": _safe_float((active["position"].fillna(0.0) != active["signal"].shift(1).fillna(0.0)).mean()),
            "active_signal_positive_days": int((active["signal"].fillna(0.0) > 0).sum()),
            "active_turnover_positive_days": int((active["turnover"].fillna(0.0) > 0).sum()),
        },
        "lag_summary": lag_summary,
        "top_contributors": rows[:30],
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        fieldnames = list(rows[0].keys()) if rows else ["date"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(json.dumps({"status": payload["status"], "json": str(OUT_JSON), "csv": str(OUT_CSV)}, ensure_ascii=False))
    return 0 if rows else 1


if __name__ == "__main__":
    raise SystemExit(main())
