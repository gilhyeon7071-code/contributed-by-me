import os
import re
from datetime import datetime
import pandas as pd

def _base_dir() -> str:
    return os.environ.get("STOC_BASE_DIR", os.path.dirname(os.path.abspath(__file__)))

def _ledger_path(base_dir: str) -> str:
    return os.path.join(base_dir, "virtual_ledger.csv")

def _normalize_ticker(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    s = re.sub(r"\.0$", "", s)
    s = re.sub(r"\D", "", s)
    if not s:
        return ""
    return s.zfill(6)

def _normalize_date(x) -> str:
    # Return YYYY-MM-DD
    if pd.isna(x):
        return ""
    if isinstance(x, datetime):
        return x.strftime("%Y-%m-%d")
    s = str(x).strip()
    if not s:
        return ""
    s = re.sub(r"\.0$", "", s)
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    s2 = re.sub(r"\D", "", s)
    if re.match(r"^\d{8}$", s2):
        return f"{s2[:4]}-{s2[4:6]}-{s2[6:8]}"
    return ""

def _to_float(x, default=0.0) -> float:
    if pd.isna(x):
        return float(default)
    s = str(x).strip().replace(",", "")
    if s == "":
        return float(default)
    try:
        return float(s)
    except Exception:
        return float(default)

def run_master_sync() -> int:
    base_dir = _base_dir()
    ledger_path = _ledger_path(base_dir)

    print("=" * 80)
    print("[LEDGER] Sync start")
    print(f"[INFO] ledger_path={ledger_path}")

    if not os.path.exists(ledger_path):
        print("[ERROR] virtual_ledger.csv not found")
        return 2

    try:
        ledger = pd.read_csv(ledger_path, dtype={"ticker": str}, encoding="utf-8-sig")
    except Exception:
        ledger = pd.read_csv(ledger_path, dtype={"ticker": str})

    if ledger.empty:
        print("[WARN] ledger is empty")
        return 0

    required = ["purchase_date", "ticker", "name", "buy_price", "status", "yield", "group", "days_held"]
    missing = [c for c in required if c not in ledger.columns]
    if missing:
        print(f"[ERROR] missing columns: {missing}")
        return 3

    before_rows = len(ledger)

    ledger["ticker"] = ledger["ticker"].apply(_normalize_ticker)
    ledger["purchase_date"] = ledger["purchase_date"].apply(_normalize_date)
    ledger["buy_price"] = ledger["buy_price"].apply(lambda x: int(round(_to_float(x, 0))))
    ledger["yield"] = ledger["yield"].apply(lambda x: float(_to_float(x, 0.0)))
    ledger["days_held"] = ledger["days_held"].apply(lambda x: int(round(_to_float(x, 0))))

    ledger = ledger[ledger["ticker"].astype(str).str.len() == 6].copy()

    ledger["_pd_sort"] = ledger["purchase_date"].replace("", "0000-00-00")
    ledger = ledger.sort_values(["ticker", "group", "_pd_sort"]).drop_duplicates(subset=["ticker", "group"], keep="last")
    ledger = ledger.drop(columns=["_pd_sort"])

    after_rows = len(ledger)

    managed = ledger[ledger["group"] == "MANAGED"].sort_values(["purchase_date", "ticker"])
    watch = ledger[ledger["group"] == "WATCH"].sort_values(["purchase_date", "ticker"])

    print(f"[LEDGER] rows: {before_rows} -> {after_rows}")
    print(f"[LEDGER] managed={len(managed)} watch={len(watch)}")

    if len(managed) > 0:
        print("[GROUP] MANAGED")
        for _, row in managed.iterrows():
            print(f" - {row['name']}({row['ticker']}): date={row['purchase_date']} status={row['status']} buy={row['buy_price']} yield={row['yield']:.2f} days={row['days_held']}")

    if len(watch) > 0:
        print("[GROUP] WATCH")
        for _, row in watch.iterrows():
            print(f" - {row['name']}({row['ticker']}): date={row['purchase_date']} status={row['status']}")

    try:
        ledger.to_csv(ledger_path, index=False, encoding="utf-8-sig")
        print("[LEDGER] saved")
    except Exception as e:
        print(f"[ERROR] save failed: {e}")
        return 4

    print("=" * 80)
    return 0

if __name__ == "__main__":
    raise SystemExit(run_master_sync())
