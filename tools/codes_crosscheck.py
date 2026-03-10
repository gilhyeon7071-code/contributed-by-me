# tools/codes_crosscheck.py
import json
import os
import pandas as pd

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
def P(*parts: str) -> str:
    return os.path.join(BASE, *parts)

def norm_code(x) -> str:
    s = "" if x is None else str(x).strip()
    s = s.replace(".0", "")
    return s.zfill(6) if s else ""

def read_state():
    path = P("paper", "paper_state.json")
    with open(path, "r", encoding="utf-8") as f:
        st = json.load(f)
    op = st.get("open_positions", []) or []
    codes = sorted({
        norm_code(i.get("code"))
        for i in op
        if isinstance(i, dict) and norm_code(i.get("code"))
    })
    return path, codes, op

def read_candidates():
    path = P("2_Logs", "candidates_latest_data.csv")
    if not os.path.exists(path):
        return path, []
    df = pd.read_csv(path)
    if "code" not in df.columns:
        return path, []
    codes = sorted({norm_code(x) for x in df["code"].tolist() if norm_code(x)})
    return path, codes

def read_prices_codes():
    path = P("paper", "prices", "ohlcv_paper.parquet")
    if not os.path.exists(path):
        return path, []
    df = pd.read_parquet(path, columns=["code"])
    if df.empty or "code" not in df.columns:
        return path, []
    codes = sorted(set(
        df["code"].astype(str)
          .str.replace(r"\.0$", "", regex=True)
          .str.strip()
          .str.zfill(6)
          .tolist()
    ))
    return path, codes

def read_fills():
    path = P("paper", "fills.csv")
    if not os.path.exists(path):
        return path, pd.DataFrame()
    try:
        df = pd.read_csv(path)
        return path, df
    except Exception:
        return path, pd.DataFrame()

def main():
    st_path, op_codes, _op = read_state()
    cand_path, cand_codes = read_candidates()
    px_path, px_codes = read_prices_codes()
    fills_path, df_fills = read_fills()

    print("STATE_FILE=", os.path.relpath(st_path, BASE))
    print("open_pos_codes=", op_codes)
    print("open_pos_count=", len(op_codes))

    print("CANDS_FILE=", os.path.relpath(cand_path, BASE), "EXISTS=", os.path.exists(cand_path))
    print("cand_codes=", cand_codes)
    print("cand_count=", len(cand_codes))

    print("PRICES_FILE=", os.path.relpath(px_path, BASE), "EXISTS=", os.path.exists(px_path))
    print("prices_codes_count=", len(px_codes))

    print("open_minus_cand=", sorted(set(op_codes) - set(cand_codes)))
    print("cand_minus_open=", sorted(set(cand_codes) - set(op_codes)))
    print("open_minus_prices=", sorted(set(op_codes) - set(px_codes)))

    print("FILLS_FILE=", os.path.relpath(fills_path, BASE), "EXISTS=", os.path.exists(fills_path))
    if df_fills.empty:
        print("fills_empty_or_unreadable=True")
        return

    cols = df_fills.columns.tolist()
    print("fills_cols=", cols)

    # legacy/v411 모두 대응: code/side/date 컬럼 위치를 최대한 추정
    if "code" in df_fills.columns:
        df_fills["_code"] = df_fills["code"].map(norm_code)
    elif len(cols) >= 2:
        df_fills["_code"] = df_fills.iloc[:, 1].map(norm_code)
    else:
        df_fills["_code"] = ""

    if "side" in df_fills.columns:
        df_fills["_side"] = df_fills["side"].astype(str).str.upper().str.strip()
    elif len(cols) >= 3:
        df_fills["_side"] = df_fills.iloc[:, 2].astype(str).str.upper().str.strip()
    else:
        df_fills["_side"] = ""

    if "date" in df_fills.columns:
        df_fills["_date"] = df_fills["date"].astype(str).str.replace("-", "").str.slice(0, 8)
    elif "datetime" in df_fills.columns:
        df_fills["_date"] = df_fills["datetime"].astype(str).str.replace("-", "").str.slice(0, 8)
    elif "ts" in df_fills.columns:
        df_fills["_date"] = df_fills["ts"].astype(str).str.replace("-", "").str.slice(0, 8)
    elif len(cols) >= 1:
        df_fills["_date"] = df_fills.iloc[:, 0].astype(str).str.replace("-", "").str.slice(0, 8)
    else:
        df_fills["_date"] = ""

    df_sel = df_fills[df_fills["_code"].isin(op_codes)].copy()
    print("fills_rows_for_open_codes=", len(df_sel))
    if len(df_sel):
        show_cols = [c for c in ["datetime", "ts", "date", "code", "side", "qty", "price", "order_id", "note"] if c in df_sel.columns]
        if not show_cols:
            show_cols = df_sel.columns.tolist()[:10]
        print("fills_tail10_for_open_codes=")
        print(df_sel[show_cols].tail(10).to_string(index=False))

if __name__ == "__main__":
    main()
