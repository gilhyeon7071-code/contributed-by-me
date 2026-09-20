from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import logging


BASE = Path(os.getenv("ROOTA", str(Path(__file__).resolve().parents[1])))
PAPER_DIR = BASE / "paper"
LOGS = BASE / "2_Logs"

FILLS_NORM = PAPER_DIR / "fills_norm.csv"
TRADES = PAPER_DIR / "trades.csv"
ENGINE_CFG = PAPER_DIR / "paper_engine_config.json"

OUT_LATEST = LOGS / "execution_health_observed_latest.json"




logger = logging.getLogger(__name__)

def _log_print(*args, **kwargs):
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")
    sep = kwargs.get("sep", " ")
    try:
        msg = sep.join(str(a) for a in args)
    except Exception:
        msg = " ".join(str(a) for a in args)
    logger.info(msg)
def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _status_by_threshold(value: float | None, caution: float, block: float) -> str:
    if value is None or not np.isfinite(value):
        return "NOT_EVALUABLE"
    if value >= block:
        return "FAIL"
    if value >= caution:
        return "PARTIAL"
    return "PASS"


def _build() -> dict[str, Any]:
    cfg = _read_json(ENGINE_CFG) if ENGINE_CFG.exists() else {}
    exec_guard = cfg.get("execution_health_guard") if isinstance(cfg.get("execution_health_guard"), dict) else {}
    sigma_guard = cfg.get("sigma_outlier_guard") if isinstance(cfg.get("sigma_outlier_guard"), dict) else {}
    slippage_rows = int(exec_guard.get("slippage_lookback_rows", 30) or 30)
    slippage_caution = _to_float(exec_guard.get("slippage_bps_caution", 10.0), 10.0)
    slippage_block = _to_float(exec_guard.get("slippage_bps_block", 20.0), 20.0)

    fills_df = _read_csv(FILLS_NORM) if FILLS_NORM.exists() else pd.DataFrame()
    trades_df = _read_csv(TRADES) if TRADES.exists() else pd.DataFrame()

    if not fills_df.empty:
        fills_df["price"] = pd.to_numeric(fills_df.get("price"), errors="coerce")
        fills_df["fill_price"] = pd.to_numeric(fills_df.get("fill_price"), errors="coerce")
        valid = fills_df[(fills_df["price"] > 0) & fills_df["fill_price"].notna()].copy()
        valid["slippage_bps"] = (valid["fill_price"] - valid["price"]).abs() / valid["price"] * 10000.0
        valid = valid.sort_values("ts")
        look = valid.tail(max(1, slippage_rows))
        sl_mean = float(look["slippage_bps"].mean()) if len(look) else None
        sl_p95 = float(look["slippage_bps"].quantile(0.95)) if len(look) else None
        sl_max = float(look["slippage_bps"].max()) if len(look) else None
        asof_ymd = str(look["date"].astype(str).max()) if "date" in look.columns and len(look) else datetime.now().strftime("%Y%m%d")
    else:
        sl_mean = sl_p95 = sl_max = None
        asof_ymd = datetime.now().strftime("%Y%m%d")

    sigma_policy_top_max = 0.80
    sigma_policy_n_min = 20
    if not trades_df.empty:
        trades_df["exit_date"] = pd.to_numeric(trades_df.get("exit_date"), errors="coerce")
        trades_df["pnl_krw"] = pd.to_numeric(trades_df.get("pnl_krw"), errors="coerce")
        t = trades_df[trades_df["pnl_krw"].notna()].copy()
        t = t.sort_values("exit_date")
        t = t.tail(max(20, slippage_rows))
        v = t["pnl_krw"].abs()
        denom = float(v.sum()) if len(v) else 0.0
        top_ratio = float(v.max() / denom) if denom > 0 else None
        sample_n = int(len(v))
    else:
        top_ratio = None
        sample_n = 0

    sigma_status = "NOT_EVALUABLE"
    if top_ratio is not None and sample_n > 0:
        sigma_status = "PASS" if (sample_n >= sigma_policy_n_min and top_ratio <= sigma_policy_top_max) else "FAIL"

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "asof_ymd": asof_ymd,
        "source": {
            "fills_norm": str(FILLS_NORM),
            "trades": str(TRADES),
            "engine_config": str(ENGINE_CFG),
        },
        "execution_health": {
            "status": _status_by_threshold(sl_mean, slippage_caution, slippage_block),
            "metric": {
                "slippage_bps_mean": sl_mean,
                "slippage_bps_p95": sl_p95,
                "slippage_bps_max": sl_max,
                "rows_used": int(max(0, min(slippage_rows, len(fills_df)))) if not fills_df.empty else 0,
            },
            "threshold": {
                "slippage_lookback_rows": slippage_rows,
                "slippage_bps_caution": slippage_caution,
                "slippage_bps_block": slippage_block,
            },
        },
        "sigma_outlier": {
            "status": sigma_status,
            "metric": {
                "top_contrib_ratio": top_ratio,
                "sample_n": sample_n,
            },
            "threshold": {
                "zscore_caution": _to_float(sigma_guard.get("zscore_caution", 3.0), 3.0),
                "zscore_block": _to_float(sigma_guard.get("zscore_block", 5.0), 5.0),
                "policy_top_contrib_ratio_max": sigma_policy_top_max,
                "policy_sample_n_min": sigma_policy_n_min,
            },
        },
    }
    return payload


def main() -> None:
    data = _build()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dated = LOGS / f"execution_health_observed_{stamp}.json"
    text = json.dumps(data, ensure_ascii=False, indent=2)
    OUT_LATEST.write_text(text, encoding="utf-8")
    dated.write_text(text, encoding="utf-8")
    _log_print(f"[OK] wrote {OUT_LATEST}")
    _log_print(f"[OK] wrote {dated}")


if __name__ == "__main__":
    main()
