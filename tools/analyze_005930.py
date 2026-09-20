import json
import pandas as pd
from pathlib import Path

ROOTA = Path("E:/1_Data")
code = "005930"

print("--- CANDIDATES ---")
cand_path = ROOTA / "2_Logs" / "candidates_latest_data.csv"
if cand_path.exists():
    df = pd.read_csv(cand_path, dtype=str)
    res = df[df["code"] == code]
    if not res.empty:
        print(res.to_dict("records")[0])
    else:
        print("Not in candidates")

print("--- MARKET RISING ---")
mr_path = ROOTA / "2_Logs" / "market_rising_latest.csv"
if mr_path.exists():
    df = pd.read_csv(mr_path, dtype=str)
    res = df[df["code"] == code]
    if not res.empty:
        print(res.to_dict("records")[0])
    else:
        print("Not in market rising")

print("--- SECTOR SSOT ---")
ssot_path = ROOTA / "_cache" / "sector_ssot.csv"
if ssot_path.exists():
    df = pd.read_csv(ssot_path, dtype=str)
    res = df[df["code"] == code]
    if not res.empty:
        print(res.to_dict("records")[0])
    else:
        print("Not in sector ssot")
