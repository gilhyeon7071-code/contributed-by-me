from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

TRADE_TR_ID = "H0STCNT0"
QUOTE_TR_ID = "H0STASP0"
TRADE_FIELDS_PER_RECORD = 46


def _find_tick_files(log_dir: Path, ymd: str) -> tuple[list[Path], list[Path]]:
    """Split kis_ws_ticks_*_{ymd}.jsonl into trade-worker files and hoga-worker files."""
    all_files = sorted(Path(p) for p in glob.glob(str(log_dir / f"kis_ws_ticks*_{ymd}.jsonl")))
    trade_files = [f for f in all_files if "hoga" not in f.stem]
    quote_files = [f for f in all_files if "hoga" in f.stem]
    return trade_files, quote_files


def _iter_raw_messages(path: Path):
    with path.open(encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue
            if d.get("event") != "message":
                continue
            raw = d.get("raw")
            if isinstance(raw, str) and raw.startswith("0|"):
                yield raw


def parse_trade_ticks(trade_files: list[Path]) -> pd.DataFrame:
    """Parse H0STCNT0 pipe frames into one row per individual execution."""
    rows: list[dict] = []
    for path in trade_files:
        for raw in _iter_raw_messages(path):
            parts = raw.split("|", 3)
            if len(parts) < 4 or parts[1] != TRADE_TR_ID:
                continue
            payload = parts[3]
            fields = payload.split("^")
            n_records = len(fields) // TRADE_FIELDS_PER_RECORD
            for i in range(n_records):
                f = fields[i * TRADE_FIELDS_PER_RECORD:(i + 1) * TRADE_FIELDS_PER_RECORD]
                if len(f) < 14:
                    continue
                try:
                    rows.append({
                        "code": f[0],
                        "hhmmss": f[1],
                        "price": float(f[2]),
                        "ask1": float(f[10]),
                        "bid1": float(f[11]),
                        "trade_volume": float(f[12]),
                        "cum_volume": float(f[13]),
                    })
                except (ValueError, IndexError):
                    continue
    if not rows:
        return pd.DataFrame(columns=["code", "hhmmss", "price", "ask1", "bid1", "trade_volume", "cum_volume"])
    df = pd.DataFrame(rows)
    # KIS batches the same execution into consecutive frames on reconnect/replay; cum_volume
    # is monotonic per code within a session, so it is a safe de-dup key alongside hhmmss.
    df = df.drop_duplicates(subset=["code", "hhmmss", "cum_volume"]).reset_index(drop=True)
    return df.sort_values(["code", "hhmmss"]).reset_index(drop=True)


def parse_quote_snapshots(quote_files: list[Path]) -> pd.DataFrame:
    """Parse H0STASP0 pipe frames into one row per order-book snapshot (top 3 levels)."""
    rows: list[dict] = []
    for path in quote_files:
        for raw in _iter_raw_messages(path):
            parts = raw.split("|", 3)
            if len(parts) < 4 or parts[1] != QUOTE_TR_ID:
                continue
            f = parts[3].split("^")
            if len(f) < 45:
                continue
            try:
                rows.append({
                    "code": f[0],
                    "hhmmss": f[1],
                    "ask1": float(f[3]), "ask2": float(f[4]), "ask3": float(f[5]),
                    "bid1": float(f[13]), "bid2": float(f[14]), "bid3": float(f[15]),
                    "ask_vol1": float(f[23]), "ask_vol2": float(f[24]), "ask_vol3": float(f[25]),
                    "bid_vol1": float(f[33]), "bid_vol2": float(f[34]), "bid_vol3": float(f[35]),
                    "total_ask_vol": float(f[43]), "total_bid_vol": float(f[44]),
                })
            except (ValueError, IndexError):
                continue
    if not rows:
        cols = ["code", "hhmmss", "ask1", "ask2", "ask3", "bid1", "bid2", "bid3",
                "ask_vol1", "ask_vol2", "ask_vol3", "bid_vol1", "bid_vol2", "bid_vol3",
                "total_ask_vol", "total_bid_vol"]
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["code", "hhmmss", "ask1", "bid1"]).reset_index(drop=True)
    df["spread"] = df["ask1"] - df["bid1"]
    df["spread_bps"] = np.where(df["bid1"] > 0, df["spread"] / df["bid1"] * 10000.0, np.nan)
    return df.sort_values(["code", "hhmmss"]).reset_index(drop=True)


def build_minute_bars(trades: pd.DataFrame) -> pd.DataFrame:
    """Aggregate individual executions into 1-minute OHLCV bars per code."""
    if trades.empty:
        cols = ["code", "minute", "open", "high", "low", "close", "volume", "trade_count", "vwap", "last_ask1", "last_bid1"]
        return pd.DataFrame(columns=cols)
    work = trades.copy()
    work["minute"] = work["hhmmss"].str.slice(0, 4)
    rows = []
    for (code, minute), g in work.groupby(["code", "minute"], sort=True):
        vol = g["trade_volume"].sum()
        rows.append({
            "code": code,
            "minute": minute,
            "open": float(g["price"].iloc[0]),
            "high": float(g["price"].max()),
            "low": float(g["price"].min()),
            "close": float(g["price"].iloc[-1]),
            "volume": float(vol),
            "trade_count": int(len(g)),
            "vwap": float((g["price"] * g["trade_volume"]).sum() / vol) if vol else float(g["price"].mean()),
            "last_ask1": float(g["ask1"].iloc[-1]),
            "last_bid1": float(g["bid1"].iloc[-1]),
        })
    return pd.DataFrame(rows).sort_values(["code", "minute"]).reset_index(drop=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Parse raw KIS WS trade/hoga ticks into minute bars and quote snapshots")
    ap.add_argument("--date", default=dt.datetime.now().strftime("%Y%m%d"), help="YYYYMMDD")
    ap.add_argument("--log-dir", default=str(LOG_DIR))
    ap.add_argument("--out-dir", default=str(LOG_DIR))
    args = ap.parse_args()

    log_dir = Path(args.log_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ymd = args.date

    trade_files, quote_files = _find_tick_files(log_dir, ymd)
    print(f"[INTRADAY_BARS] date={ymd} trade_files={len(trade_files)} quote_files={len(quote_files)}")

    trades = parse_trade_ticks(trade_files)
    quotes = parse_quote_snapshots(quote_files)
    bars = build_minute_bars(trades)

    bars_path = out_dir / f"intraday_minute_bars_{ymd}.csv"
    bars.to_csv(bars_path, index=False, encoding="utf-8-sig")
    (out_dir / "intraday_minute_bars_latest.csv").write_bytes(bars_path.read_bytes())
    
    bars_parquet_path = out_dir / f"intraday_minute_bars_{ymd}.parquet"
    if not bars.empty:
        bars.to_parquet(bars_parquet_path, index=False)
        bars.to_parquet(out_dir / "intraday_minute_bars_latest.parquet", index=False)

    quotes_path = out_dir / f"intraday_quote_snapshots_{ymd}.parquet"
    if not quotes.empty:
        quotes.to_parquet(quotes_path, index=False)

    trades_path = out_dir / f"intraday_trade_ticks_{ymd}.parquet"
    if not trades.empty:
        trades.to_parquet(trades_path, index=False)

    status = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "date": ymd,
        "trade_files": [str(p) for p in trade_files],
        "quote_files": [str(p) for p in quote_files],
        "trade_rows": int(len(trades)),
        "quote_rows": int(len(quotes)),
        "minute_bars": int(len(bars)),
        "codes_traded": int(trades["code"].nunique()) if not trades.empty else 0,
        "codes_quoted": int(quotes["code"].nunique()) if not quotes.empty else 0,
        "bars_csv": str(bars_path),
        "bars_parquet": str(bars_parquet_path) if not bars.empty else None,
        "quotes_parquet": str(quotes_path) if not quotes.empty else None,
        "trades_parquet": str(trades_path) if not trades.empty else None,
    }
    status_path = out_dir / f"intraday_minute_bars_status_{ymd}.json"
    status_path.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (out_dir / "intraday_minute_bars_status_latest.json").write_text(
        json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    print(
        f"[INTRADAY_BARS] trade_rows={status['trade_rows']} quote_rows={status['quote_rows']} "
        f"minute_bars={status['minute_bars']} codes_traded={status['codes_traded']} codes_quoted={status['codes_quoted']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
