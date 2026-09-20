import json
import logging
import math
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import numpy as np
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
HISTORY_PATH = LOG_DIR / "candidate_decision_outcome_ledger_history.csv"
OUT_JSON = LOG_DIR / "strategy_optimization_latest.json"

logger = logging.getLogger(__name__)

def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()

def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        val = float(v)
        if math.isnan(val) or math.isinf(val):
            return default
        return val
    except:
        return default

def sweep_parameters(df: pd.DataFrame, strategy: str) -> Dict[str, Any]:
    if df.empty:
        return {}

    mask = df["decision_type"].isin(["BUY", "WATCH", "HELD"])
    sub = df[mask].copy()
    if sub.empty:
        return {}
    
    sub["v_accel"] = sub["v_accel_original"].apply(lambda x: _to_float(x))
    sub["ret"] = sub["actual_pnl_pct"].apply(lambda x: _to_float(x))
    
    no_actual = sub["ret"] == 0.0
    sub.loc[no_actual, "ret"] = sub.loc[no_actual, "ret_close_or_latest_pct"].apply(lambda x: _to_float(x))

    # 노이즈 필터링: 아직 정산되지 않았거나(PENDING), 마크아웃 추적이 안 되어
    # 수익률이 0.0인 허수 데이터를 시뮬레이션 표본에서 제외합니다.
    sub = sub[sub["ret"] != 0.0].copy()
    
    if sub.empty:
        return {}

    v_accel_thresholds = [0.0, 0.5, 0.8, 1.0, 1.2, 1.5, 2.0]
    best_v_accel = 0.0
    best_v_accel_pnl = -9999.0
    best_v_win_rate = 0.0

    for th in v_accel_thresholds:
        filtered = sub[sub["v_accel"] >= th]
        if len(filtered) < 3:
            continue
        pnl = float(filtered["ret"].sum())
        win_rate = float((filtered["ret"] > 0).mean())
        if pnl > best_v_accel_pnl:
            best_v_accel_pnl = pnl
            best_v_accel = th
            best_v_win_rate = win_rate

    sl_thresholds = [-2.0, -4.0, -6.0, -8.0, -10.0]
    tp_thresholds = [3.0, 5.0, 8.0, 10.0, 15.0]
    
    best_sl = -4.0
    best_tp = 5.0
    best_sl_tp_pnl = -9999.0
    
    for sl in sl_thresholds:
        for tp in tp_thresholds:
            sim_pnl = 0.0
            for r in sub["ret"]:
                if r <= sl:
                    sim_pnl += sl
                elif r >= tp:
                    sim_pnl += tp
                else:
                    sim_pnl += r
            
            if sim_pnl > best_sl_tp_pnl:
                best_sl_tp_pnl = sim_pnl
                best_sl = sl
                best_tp = tp

    baseline_pnl = float(sub["ret"].sum())
    baseline_win_rate = float((sub["ret"] > 0).mean())

    return {
        "strategy": strategy,
        "sample_size": len(sub),
        "baseline": {
            "pnl": baseline_pnl,
            "win_rate": baseline_win_rate
        },
        "optimal_v_accel": {
            "threshold": best_v_accel,
            "simulated_pnl": best_v_accel_pnl,
            "simulated_win_rate": best_v_win_rate,
            "insight": f"v_accel >= {best_v_accel} 적용 시 누적 수익 극대화 확인"
        },
        "optimal_sl_tp": {
            "stop_loss_pct": best_sl / 100.0,
            "take_profit_pct": best_tp / 100.0,
            "simulated_pnl": float(best_sl_tp_pnl),
            "insight": f"손절 {best_sl:.1f}%, 익절 {best_tp:.1f}% 설정 추천"
        }
    }

def run() -> Dict[str, Any]:
    if not HISTORY_PATH.exists():
        return {"status": "FAIL", "reason": "No history file"}

    try:
        df = pd.read_csv(HISTORY_PATH, dtype=str)
    except Exception as e:
        return {"status": "FAIL", "reason": str(e)}
    
    strategies = ["normal", "surge", "sector_union"]
    results = []

    for st in strategies:
        st_df = df[df["strategy_group"] == st]
        opt = sweep_parameters(st_df, st)
        if opt:
            results.append(opt)

    payload = {
        "generated_at": _now_ts(),
        "status": "PASS",
        "optimization_results": results
    }

    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

if __name__ == "__main__":
    run()
