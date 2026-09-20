from pathlib import Path
import pandas as pd
import json
import numpy as np
from datetime import datetime

ROOT = Path("E:/1_Data")
TRADES_PATH = ROOT / "paper" / "trades.csv"
OHLCV_PATH = ROOT / "paper" / "prices" / "ohlcv_paper.parquet"
OUT_PATH = ROOT / "2_Logs" / "surge_hard_block_validation.json"
OUT_CSV = ROOT / "2_Logs" / "surge_hard_block_validation.csv"
OPEN_CHASE_THRESHOLD = 0.05

def _to_float(x):
    try:
        return float(x)
    except:
        return np.nan

def _get_stats(df):
    if df.empty:
        return {"pf": 0.0, "win_rate": 0.0, "trades": 0, "avg_ret": 0.0}
    wins = df[df["net_ret"] > 0]["net_ret"].sum()
    losses = abs(df[df["net_ret"] < 0]["net_ret"].sum())
    pf = wins / losses if losses > 0 else 999.0
    win_rate = len(df[df["net_ret"] > 0]) / len(df)
    return {"pf": round(pf, 3), "win_rate": round(win_rate, 3), "trades": len(df), "avg_ret": round(df["net_ret"].mean(), 4)}


def _segment(name, df):
    blocked = df[df["open_to_entry_chase_pct"] > OPEN_CHASE_THRESHOLD].copy()
    kept = df[(df["open_to_entry_chase_pct"] <= OPEN_CHASE_THRESHOLD) | df["open_to_entry_chase_pct"].isna()].copy()
    return {
        "segment": name,
        "baseline": _get_stats(df),
        "blocked_open_chase_gt_5pct": _get_stats(blocked),
        "kept_after_hard_block": _get_stats(kept),
        "blocked_count": int(len(blocked)),
        "kept_count": int(len(kept)),
    }


def run():
    trades = pd.read_csv(TRADES_PATH, dtype=str)
    trades["net_ret"] = trades["pnl_pct"].apply(_to_float)
    
    ohlcv = pd.read_parquet(OHLCV_PATH)
    ohlcv["code"] = ohlcv["code"].astype(str).str.zfill(6)
    ohlcv["date"] = ohlcv["date"].astype(str)
    
    trades["entry_date"] = trades["entry_date"].astype(str).str.replace("-", "")
    trades["code"] = trades["code"].astype(str).str.zfill(6)
    
    df = trades.merge(ohlcv, left_on=["code", "entry_date"], right_on=["code", "date"], how="left", indicator=True)
    df["entry_price"] = df["entry_price"].apply(_to_float)
    df["open"] = df["open"].apply(_to_float)
    df["open_to_entry_chase_pct"] = (df["entry_price"] - df["open"]) / df["open"]
    
    segments = [_segment("all_trades", df)]
    for value, part in df.groupby(df["is_surge"].fillna("unknown")):
        label = "surge_trades" if str(value) == "1" else "non_surge_trades" if str(value) == "0" else f"is_surge_{value}"
        segments.append(_segment(label, part))

    report_rows = []
    for item in segments:
        report_rows.append({"segment": item["segment"], "bucket": "baseline", **item["baseline"]})
        report_rows.append({"segment": item["segment"], "bucket": "blocked_open_chase_gt_5pct", **item["blocked_open_chase_gt_5pct"]})
        report_rows.append({"segment": item["segment"], "bucket": "kept_after_hard_block", **item["kept_after_hard_block"]})
    
    res = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "open_to_entry_chase_hard_block_validation",
        "policy_candidate": {
            "blocker": "OPEN_CHASE_BLOCK",
            "threshold": OPEN_CHASE_THRESHOLD,
            "definition": "entry_price/current_price more than 5pct above same-day intraday open",
            "not_gap_up": True,
        },
        "source_files": {
            "trades": str(TRADES_PATH),
            "ohlcv": str(OHLCV_PATH),
        },
        "source_quality": {
            "trade_rows": int(len(trades)),
            "merged_rows": int((df["_merge"] == "both").sum()),
            "missing_ohlcv_rows": int((df["_merge"] != "both").sum()),
            "missing_open_to_entry_chase_pct_rows": int(df["open_to_entry_chase_pct"].isna().sum()),
            "is_surge_counts": {str(k): int(v) for k, v in df["is_surge"].fillna("unknown").value_counts().to_dict().items()},
        },
        "summary": {
            "all_trades": segments[0],
            "segments": segments,
            "paper_order_route": False,
            "broker_order_route": False,
            "orders_modified": False,
            "fills_modified": False,
            "research_only": True,
        },
    }
    
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(res, f, ensure_ascii=False, indent=2)
    pd.DataFrame(report_rows).to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(json.dumps(res, indent=2))

if __name__ == "__main__":
    run()
