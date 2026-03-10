import argparse
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PAPER_DIR = BASE_DIR / "paper"

FILLS = PAPER_DIR / "fills.csv"
TRADES = PAPER_DIR / "trades.csv"

# === v41.1 현재 paper CSV 스키마 (fills.csv / trades.csv 실제 헤더 기준) ===
FILLS_HEADER = ["datetime", "code", "side", "qty", "price", "order_id", "note"]
TRADES_HEADER = [
    "trade_id", "code",
    "entry_date", "entry_price",
    "exit_date", "exit_price",
    "pnl_pct", "exit_reason", "note",
]

def _normalize_cols(cols):
    # pandas가 BOM(ufeff)을 첫 컬럼명에 남기는 경우 대비
    return [str(c).lstrip("\ufeff").strip() for c in cols]

def validate_one(path: Path, header: list[str]) -> tuple[bool, str]:
    if not path.exists():
        return False, "missing"
    df = pd.read_csv(path, dtype=str)
    df.columns = _normalize_cols(df.columns)
    cols = df.columns.tolist()
    if cols != header:
        return False, f"header mismatch: {cols}"
    return True, "ok"

def fix_one(path: Path, header: list[str]) -> Path:
    df = pd.read_csv(path, dtype=str)
    df.columns = _normalize_cols(df.columns)

    for c in header:
        if c not in df.columns:
            df[c] = ""

    df = df[header]
    out = path.with_name(path.stem + "_fixed.csv")
    # Excel 친화적으로 BOM 포함 저장(validator가 BOM을 무시하므로 안전)
    df.to_csv(out, index=False, encoding="utf-8-sig")
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fix", action="store_true")
    args = ap.parse_args()

    ok_f, msg_f = validate_one(FILLS, FILLS_HEADER)
    ok_t, msg_t = validate_one(TRADES, TRADES_HEADER)
    print(f"[CHECK] fills: {ok_f} ({msg_f}) -> {FILLS}")
    print(f"[CHECK] trades: {ok_t} ({msg_t}) -> {TRADES}")

    if args.fix:
        if not ok_f and FILLS.exists():
            out = fix_one(FILLS, FILLS_HEADER)
            print(f"[FIX] wrote: {out}")
        if not ok_t and TRADES.exists():
            out = fix_one(TRADES, TRADES_HEADER)
            print(f"[FIX] wrote: {out}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
