import sys
from pathlib import Path
import pandas as pd
import json

ROOT = Path("E:/1_Data")
TRADES_PATH = ROOT / "paper" / "trades.csv"
OUT_PATH = ROOT / "2_Logs" / "defensive_exit_simulation.json"

def _to_float(x):
    try:
        return float(x)
    except:
        return 0.0

def _get_stats(df):
    if df.empty: 
        return {"pf": 0.0, "win_rate": 0.0, "trades": 0, "avg_ret": 0.0}
    wins = df[df["net_ret"] > 0]["net_ret"].sum()
    losses = abs(df[df["net_ret"] < 0]["net_ret"].sum())
    pf = wins / losses if losses > 0 else 999.0
    
    win_count = len(df[df["net_ret"] > 0])
    win_rate = win_count / len(df)
    avg_ret = df["net_ret"].mean()
    
    return {
        "pf": round(pf, 3),
        "win_rate": round(win_rate, 3),
        "trades": len(df),
        "avg_ret": round(avg_ret, 4)
    }

def run():
    print(f"Loading {TRADES_PATH}...")
    trades = pd.read_csv(TRADES_PATH)
    trades["net_ret"] = trades["pnl_pct"].apply(_to_float)
    
    baseline = _get_stats(trades)
    
    # Exclude DDM Liquidations
    no_ddm = trades[~trades["exit_reason"].isin(["DDM_LIQUIDATE_L3", "DDM_LIQUIDATE_L4"])]
    stats_no_ddm = _get_stats(no_ddm)
    
    # Exclude Fundamental Critical
    no_fund = trades[trades["exit_reason"] != "FUNDAMENTAL_CRITICAL"]
    stats_no_fund = _get_stats(no_fund)
    
    # Exclude Preemptive Close
    no_pre = trades[trades["exit_reason"] != "STOP_PREEMPTIVE_CLOSE"]
    stats_no_pre = _get_stats(no_pre)
    
    # Exclude All Hyper-Defensive
    hyper_defensive = ["DDM_LIQUIDATE_L3", "DDM_LIQUIDATE_L4", "FUNDAMENTAL_CRITICAL", "STOP_PREEMPTIVE_CLOSE"]
    no_all_def = trades[~trades["exit_reason"].isin(hyper_defensive)]
    stats_no_all_def = _get_stats(no_all_def)
    
    res = {
        "1_baseline": baseline,
        "2_exclude_ddm": stats_no_ddm,
        "3_exclude_fundamental_critical": stats_no_fund,
        "4_exclude_preemptive_close": stats_no_pre,
        "5_exclude_all_defensive": stats_no_all_def
    }
    
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(res, f, indent=2)
    print(f"Saved results to {OUT_PATH}")
    print(json.dumps(res, indent=2))

if __name__ == "__main__":
    run()
