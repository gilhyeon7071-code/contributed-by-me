import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PAPER_DIR = BASE_DIR / "paper"

FILLS = PAPER_DIR / "fills.csv"
TRADES = PAPER_DIR / "trades.csv"

FILLS_HEADER = ["ts","date","code","name","side","qty","price","fee","slippage","order_id","note"]
TRADES_HEADER = ["trade_id","entry_ts","exit_ts","code","name","side","qty","entry_price","exit_price",
                 "gross_ret","net_ret","fee","slippage","stop_hit","take_profit_hit","trail_hit","note"]

def ensure_csv(path: Path, header: list[str]) -> None:
    if path.exists():
        return
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(header)

def main() -> int:
    PAPER_DIR.mkdir(parents=True, exist_ok=True)
    ensure_csv(FILLS, FILLS_HEADER)
    ensure_csv(TRADES, TRADES_HEADER)
    print(f"[PAPER] ok: {PAPER_DIR}")
    print(f"[PAPER] fills: {FILLS}")
    print(f"[PAPER] trades: {TRADES}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
