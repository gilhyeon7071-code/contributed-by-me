import sys
from pathlib import Path
import pandas as pd
import json
import ast

ROOT = Path("E:/1_Data")
sys.path.append(str(ROOT))
import paper_pnl_report as ppr

TRADES_PATH = ROOT / "paper" / "trades_calc.csv"
FILLS_PATH = ROOT / "paper" / "fills_norm.csv"
OUT_PATH = ROOT / "2_Logs" / "split_and_intraday_hypothesis_validation.json"

def _to_float(x):
    try:
        return float(x)
    except:
        return 0.0

def _get_pf(df):
    if df.empty: return 0.0
    wins = df[df["net_ret"] > 0]["net_ret"].sum()
    losses = df[df["net_ret"] < 0]["net_ret"].sum()
    if losses == 0: return 999.0
    return abs(wins / losses)

def run():
    print(f"Loading {TRADES_PATH}...")
    trades = pd.read_csv(TRADES_PATH)
    
    # 1. Assign Strategy
    trades["strategy"] = trades["note"].astype(str).apply(ppr._strategy_key_from_note)
    trades["net_ret"] = trades["net_ret"].apply(_to_float)
    
    # Baseline Overall PF
    baseline_total_pf = _get_pf(trades)
    
    # Hypothesis A: Drop Split Entry completely
    # We couldn't find "split_entry=2nd", so we simulate turning off the strategy entirely.
    trades_no_split = trades[trades["strategy"] != "split_entry"]
    hyp_a_pf = _get_pf(trades_no_split)
    
    # 2. Extract order_id from note to join with fills for exact time
    def _extract_order_id(note):
        for part in str(note).split("|"):
            if "order_id=" in part:
                return part.split("order_id=")[1].strip().split(";")[0]
        return ""
    trades["order_id"] = trades["note"].apply(_extract_order_id)
    
    print(f"Loading {FILLS_PATH}...")
    try:
        fills = pd.read_csv(FILLS_PATH)
        fills = fills[["order_id", "ts", "code"]].drop_duplicates(subset=["order_id"])
        trades = trades.merge(fills, on="order_id", how="left")
        
        # Convert ts to hour and minute
        trades["ts"] = pd.to_datetime(trades["ts"], errors="coerce")
        trades["hour"] = trades["ts"].dt.hour
        trades["minute"] = trades["ts"].dt.minute
        trades["time_val"] = trades["hour"] * 100 + trades["minute"]
    except Exception as e:
        print(f"Warning: Could not process fills_norm.csv: {e}")
        trades["time_val"] = pd.NA

    # Hypothesis B: Intraday Time Filter (Drop 10:30 ~ 13:30)
    intraday_trades = trades[trades["strategy"] == "intraday_realtime"].copy()
    baseline_intraday_pf = _get_pf(intraday_trades)
    
    # Keep only trades before 10:30 or after 13:30
    filtered_intraday = intraday_trades[(intraday_trades["time_val"].isna()) | (intraday_trades["time_val"] < 1030) | (intraday_trades["time_val"] >= 1330)]
    hyp_b_pf = _get_pf(filtered_intraday)
    
    # Prepare result JSON
    res = {
        "baseline_total_pf": round(baseline_total_pf, 3),
        "total_trades": len(trades),
        "hypothesis_a_split_entry": {
            "action": "Drop Split Entry completely",
            "trades_dropped": len(trades) - len(trades_no_split),
            "new_total_pf": round(hyp_a_pf, 3),
            "pf_improvement": round(hyp_a_pf - baseline_total_pf, 3)
        },
        "hypothesis_b_intraday": {
            "action": "Block 10:30 ~ 13:30",
            "baseline_intraday_pf": round(baseline_intraday_pf, 3),
            "original_intraday_trades": len(intraday_trades),
            "trades_dropped": len(intraday_trades) - len(filtered_intraday),
            "new_intraday_pf": round(hyp_b_pf, 3),
            "pf_improvement": round(hyp_b_pf - baseline_intraday_pf, 3)
        }
    }
    
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(res, f, indent=2)
    print(f"Saved results to {OUT_PATH}")
    print(json.dumps(res, indent=2))

if __name__ == "__main__":
    run()
