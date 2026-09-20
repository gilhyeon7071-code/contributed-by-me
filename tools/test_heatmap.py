import pandas as pd
from pathlib import Path

MARKET_RISING_CSV = Path("E:/1_Data/2_Logs/market_rising_latest.csv")
SECTOR_SSOT_CSV = Path("E:/1_Data/_cache/sector_ssot.csv")

def test():
    if not MARKET_RISING_CSV.exists() or not SECTOR_SSOT_CSV.exists():
        print("Files missing")
        return
        
    df_mr = pd.read_csv(MARKET_RISING_CSV, dtype={"code": str})
    df_ssot = pd.read_csv(SECTOR_SSOT_CSV, dtype={"code": str})
    
    # ensure 6 digit code
    df_mr["code"] = df_mr["code"].str.zfill(6)
    df_ssot["code"] = df_ssot["code"].str.zfill(6)
    
    merged = pd.merge(df_mr, df_ssot, on="code", how="left")
    print(merged[["code", "name_x", "krx_sector", "trading_value", "change_pct"]].head())

if __name__ == "__main__":
    test()
