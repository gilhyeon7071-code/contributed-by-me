import sys
from pathlib import Path
import pandas as pd
import json
import numpy as np

ROOT = Path("E:/1_Data")
TRADES_PATH = ROOT / "paper" / "trades.csv"
OHLCV_PATH = ROOT / "paper" / "prices" / "ohlcv_paper.parquet"
OUT_PATH = ROOT / "2_Logs" / "entry_failure_analysis.json"

def _to_float(x):
    try:
        return float(x)
    except:
        return np.nan

def run():
    print(f"Loading {TRADES_PATH}...")
    trades = pd.read_csv(TRADES_PATH, dtype=str)
    
    # Keep only losing trades to analyze entry failures
    trades["net_ret"] = trades["pnl_pct"].apply(_to_float)
    losing_trades = trades[trades["net_ret"] < 0].copy()
    
    print(f"Loading {OHLCV_PATH}...")
    ohlcv = pd.read_parquet(OHLCV_PATH)
    ohlcv["code"] = ohlcv["code"].astype(str).str.zfill(6)
    ohlcv["date"] = ohlcv["date"].astype(str)
    
    # Merge trades with entry day OHLCV
    losing_trades["entry_date"] = losing_trades["entry_date"].astype(str).str.replace("-", "")
    losing_trades["code"] = losing_trades["code"].astype(str).str.zfill(6)
    
    df = losing_trades.merge(ohlcv, left_on=["code", "entry_date"], right_on=["code", "date"], how="left")
    
    # Calculate Entry Positioning Metrics
    df["entry_price"] = df["entry_price"].apply(_to_float)
    
    # 1. Did we buy the top? (Distance from High)
    df["dist_from_high_pct"] = (df["high"] - df["entry_price"]) / df["entry_price"]
    
    # 2. Did we buy a gap up? (Entry vs Open)
    df["gap_to_entry_pct"] = (df["entry_price"] - df["open"]) / df["open"]
    
    # 3. Did it collapse intraday after entry? (Close vs Entry)
    df["intraday_drawdown_pct"] = (df["close"] - df["entry_price"]) / df["entry_price"]
    
    # 4. Range of the candle
    df["candle_range_pct"] = (df["high"] - df["low"]) / df["low"]

    # Analysis Buckets
    total_losing = len(df)
    
    bought_the_top = len(df[df["dist_from_high_pct"] <= 0.01]) # Bought within 1% of the daily high
    huge_intraday_collapse = len(df[df["intraday_drawdown_pct"] < -0.05]) # Collapsed >5% on the same day AFTER buying
    chased_gap_up = len(df[df["gap_to_entry_pct"] > 0.05]) # Bought 5% higher than Open
    
    # Stop-Loss timing: Were they stopped out ON THE SAME DAY?
    df["exit_date"] = df["exit_date"].astype(str).str.replace("-", "")
    same_day_stops = len(df[df["entry_date"] == df["exit_date"]])
    
    res = {
        "total_losing_trades_analyzed": total_losing,
        "patterns": {
            "bought_at_daily_peak": {
                "count": bought_the_top,
                "pct_of_losses": round(bought_the_top / total_losing * 100, 1),
                "description": "Bought within 1% of the absolute High of the day"
            },
            "immediate_intraday_collapse": {
                "count": huge_intraday_collapse,
                "pct_of_losses": round(huge_intraday_collapse / total_losing * 100, 1),
                "description": "Price collapsed >5% from entry price by market close on the same day"
            },
            "chased_intraday_surge": {
                "count": chased_gap_up,
                "pct_of_losses": round(chased_gap_up / total_losing * 100, 1),
                "description": "Entry price was >5% higher than the Open price"
            },
            "stopped_out_same_day": {
                "count": same_day_stops,
                "pct_of_losses": round(same_day_stops / total_losing * 100, 1),
                "description": "Trade hit stop-loss on the exact same day it was entered"
            }
        },
        "averages": {
            "avg_dist_from_high_pct": round(df["dist_from_high_pct"].mean() * 100, 2),
            "avg_intraday_drawdown_from_entry_pct": round(df["intraday_drawdown_pct"].mean() * 100, 2)
        }
    }
    
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(res, f, indent=2)
    print(json.dumps(res, indent=2))

if __name__ == "__main__":
    run()
