import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

def stop(msg: str, code: int = 2):
    print(msg)
    raise SystemExit(code)

def norm_ymd(x) -> str:
    s = "" if pd.isna(x) else str(x)
    import re
    m = re.search(r"(\d{8})", s)
    return m.group(1) if m else ""

def norm_code(x) -> str:
    s = "" if pd.isna(x) else str(x)
    import re
    m = re.search(r"(\d+)", s)
    if not m:
        return ""
    try:
        return str(int(m.group(1)))  # ledger 스타일(선행 0 제거)로 정규화
    except Exception:
        return m.group(1)

def main():
    if len(sys.argv) < 2:
        stop("STOP usage: python ledger_append_from_orders_exec.py <YYYYMMDD> [--apply]")
    D = sys.argv[1].strip()
    apply = ("--apply" in sys.argv[2:])

    ledger_path = Path(r"E:\1_Data\virtual_ledger.csv")
    orders_path = Path(rf"E:\1_Data\paper\orders_{D}_exec.xlsx")

    if not ledger_path.exists():
        stop(f"STOP missing ledger: {ledger_path}")
    if not orders_path.exists():
        stop(f"STOP missing orders_exec: {orders_path}")

    ledger = pd.read_csv(ledger_path)
    need_ledger_cols = ["purchase_date","ticker","name","buy_price","status","yield","group","days_held"]
    missing_ledger = [c for c in need_ledger_cols if c not in ledger.columns]
    if missing_ledger:
        stop(f"STOP ledger missing_cols={missing_ledger}")

    orders = pd.read_excel(orders_path, engine="openpyxl")
    need_orders_cols = ["exec_date","side","code","fill_price","is_stop"]
    missing_orders = [c for c in need_orders_cols if c not in orders.columns]
    if missing_orders:
        stop(f"STOP orders_exec missing_cols={missing_orders}")

    # 계약: exec_date 유일값이 D여야 함 (아니면 STOP)
    uniq = sorted({norm_ymd(x) for x in orders["exec_date"].tolist() if norm_ymd(x)})
    if uniq != [D]:
        stop(f"STOP exec_date_unique={uniq} expected={[D]}")

    # B 선택 반영: STOP(is_stop=True)은 ledger append 대상에서 제외
    o = orders.copy()
    o["side_u"] = o["side"].astype(str).str.upper()
    o["is_stop_b"] = o["is_stop"].astype(bool)

    buys = o[(o["side_u"] == "BUY") & (o["is_stop_b"] == False)].copy()

    if len(buys) == 0:
        print(f"[DRY] D={D} buy_rows=0 -> nothing to append")
        return 0

    buys["ticker_n"] = buys["code"].apply(norm_code)
    buys["buy_price_n"] = pd.to_numeric(buys["fill_price"], errors="coerce")

    buys = buys.dropna(subset=["buy_price_n"])
    buys = buys[buys["ticker_n"].astype(str).str.len() > 0].copy()

    # 기존 ledger 키( purchase_date + ticker ) 구성
    ledger_k = ledger.copy()
    ledger_k["purchase_date_n"] = ledger_k["purchase_date"].apply(norm_ymd)
    ledger_k["ticker_n"] = ledger_k["ticker"].apply(norm_code)
    existing_keys = set((ledger_k["purchase_date_n"].astype(str) + "|" + ledger_k["ticker_n"].astype(str)).tolist())

    rows = []
    for _, r in buys.iterrows():
        key = f"{D}|{r['ticker_n']}"
        if key in existing_keys:
            continue
        rows.append({
            "purchase_date": D,
            "ticker": int(r["ticker_n"]),     # CSV 저장 시 기존과 같이 숫자로 표시
            "name": "",                       # orders_exec에 name 없음 -> 빈값 유지
            "buy_price": float(r["buy_price_n"]),
            "status": "PENDING",              # 현재 ledger tail과 동일하게 유지
            "yield": 0.0,
            "group": "WATCH",                 # 현재 ledger tail과 동일하게 유지
            "days_held": 0                    # 신규 진입일 기준 0으로 고정(추후 갱신 로직에서 업데이트)
        })

    add = pd.DataFrame(rows, columns=need_ledger_cols)
    print(f"[PLAN] D={D} buys_core={len(buys)} to_append={len(add)} ledger_rows_before={len(ledger)}")

    if len(add) == 0:
        print("[PLAN] nothing to append (all duplicates)")
        return 0

    if not apply:
        print("[DRY] preview_append_rows:")
        print(add.to_string(index=False))
        print("[DRY] run with --apply to write (will create backup)")
        return 0

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = ledger_path.with_name(ledger_path.name + f".bak_{ts}")
    ledger_path.replace(bak)  # move to backup
    out = pd.concat([ledger, add], ignore_index=True)
    out.to_csv(ledger_path, index=False, encoding="utf-8")
    print(f"[APPLY] BACKUP={bak}")
    print(f"[APPLY] WROTE ={ledger_path} rows_after={len(out)} appended={len(add)}")
    return 0

if __name__ == "__main__":
    main()
