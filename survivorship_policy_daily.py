from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from utils.common import norm_code


# -----------------------------------------------------------------------------
# Purpose
#   - Enforce survivorship / delist / halt policy for paper engine operation
#   - Exclude untradeable candidates BEFORE paper_engine runs
#   - Force-exit open positions that became untradeable (missing prices / halt)
#   - Write an audit log every run
# -----------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "2_Logs"
PAPER_DIR = BASE_DIR / "paper"

PRICES_PATH = PAPER_DIR / "prices" / "ohlcv_paper.parquet"
STATE_PATH = PAPER_DIR / "paper_state.json"
FILLS_PATH = PAPER_DIR / "fills.csv"
TRADES_PATH = PAPER_DIR / "trades.csv"
CANDS_PATH = LOG_DIR / "candidates_latest_data.csv"


def _now() -> datetime:
    return datetime.now()


def _tag(ts: Optional[datetime] = None) -> str:
    ts = ts or _now()
    return ts.strftime("%Y%m%d_%H%M%S")


def _ymd(ts: Optional[datetime] = None) -> str:
    ts = ts or _now()
    return ts.strftime("%Y%m%d")


def _safe_read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _safe_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _read_csv_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.DataFrame()


def _write_csv_df(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")




def _norm_date_str(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if not s:
        return ""
    # Accept: YYYY-MM-DD, YYYYMMDD, pandas Timestamp string, etc.
    s = s.replace("-", "").replace("/", "").replace(".", "")
    s = s[:8]
    return s if s.isdigit() and len(s) == 8 else ""


@dataclass
class PxCols:
    code: str
    date: str
    close: str
    volume: Optional[str] = None


def _infer_price_cols(df: pd.DataFrame) -> PxCols:
    cols = list(df.columns)
    lcols = {c.lower(): c for c in cols}

    def pick(cands: List[str]) -> Optional[str]:
        for k in cands:
            if k.lower() in lcols:
                return lcols[k.lower()]
        # try partial match
        for c in cols:
            lc = c.lower()
            for k in cands:
                if k.lower() in lc:
                    return c
        return None

    code = pick(["code", "ticker", "종목코드", "종목", "symbol"]) or "code"
    date = pick(["date", "일자", "dt", "datetime", "거래일"]) or "date"
    close = pick(["close", "종가", "adj_close", "종가(원)"]) or "close"
    volume = pick(["volume", "vol", "거래량", "qty"])  # optional

    return PxCols(code=code, date=date, close=close, volume=volume)


def _normalize_prices(df: pd.DataFrame, px: PxCols) -> pd.DataFrame:
    df = df.copy()
    df[px.code] = df[px.code].map(norm_code)

    if pd.api.types.is_datetime64_any_dtype(df[px.date]):
        df[px.date] = df[px.date].dt.strftime("%Y%m%d")
    else:
        df[px.date] = df[px.date].map(_norm_date_str)

    # close numeric
    df[px.close] = pd.to_numeric(df[px.close], errors="coerce")
    if px.volume and px.volume in df.columns:
        df[px.volume] = pd.to_numeric(df[px.volume], errors="coerce")

    df = df[(df[px.code].str.len() == 6) & (df[px.date].str.len() == 8)]
    return df


def _load_prices() -> Tuple[pd.DataFrame, Optional[PxCols], Optional[str]]:
    if not PRICES_PATH.exists():
        return pd.DataFrame(), None, None
    try:
        df = pd.read_parquet(PRICES_PATH)
    except Exception:
        # fallback: allow parquet errors to bubble with a clear message
        raise
    if df.empty:
        return df, None, None
    px = _infer_price_cols(df)
    df = _normalize_prices(df, px)
    if df.empty:
        return df, px, None
    date_max = str(df[px.date].max())
    return df, px, date_max


def _next_trade_day(trading_days: List[str], d: str) -> Optional[str]:
    # trading_days is sorted ascending
    for td in trading_days:
        if td > d:
            return td
    return None


def _append_fill_row(fills_path: Path, row: Dict[str, Any]) -> None:
    """Append a fill row to fills.csv preserving existing schema."""
    fills_path.parent.mkdir(parents=True, exist_ok=True)
    exists = fills_path.exists()

    # default legacy columns
    default_cols = ["datetime", "code", "side", "qty", "price", "order_id", "note"]
    cols: List[str] = default_cols

    if exists:
        with open(fills_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header and len(header) >= 6:
                cols = header

    write_header = not exists
    with open(fills_path, "a", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(cols)
        out = []
        for c in cols:
            out.append(row.get(c, ""))
        w.writerow(out)


def _append_trade_row(trades_path: Path, row: Dict[str, Any]) -> None:
    """Append a trade row to trades.csv preserving existing schema."""
    trades_path.parent.mkdir(parents=True, exist_ok=True)
    exists = trades_path.exists()
    default_cols = [
        "trade_id",
        "code",
        "entry_date",
        "entry_price",
        "exit_date",
        "exit_price",
        "pnl_pct",
        "pnl_krw",
        "exit_reason",
        "note",
    ]
    cols: List[str] = default_cols
    max_trade_id = 0

    if exists:
        with open(trades_path, "r", encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            if r.fieldnames:
                cols = list(r.fieldnames)
            for rec in r:
                try:
                    max_trade_id = max(max_trade_id, int(str(rec.get("trade_id", "0")).strip() or 0))
                except Exception:
                    continue

    if "trade_id" in cols and not row.get("trade_id"):
        row["trade_id"] = str(max_trade_id + 1)

    write_header = not exists
    with open(trades_path, "a", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(cols)
        out = []
        for c in cols:
            out.append(row.get(c, ""))
        w.writerow(out)


def main() -> int:
    ts = _now()
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Parameters (kept in-script to avoid config churn)
    FORCE_EXIT_MISSING_DAYS = 3  # trading days missing at the end
    FORCE_EXIT_HALT_DAYS = 3  # consecutive vol==0 at the end (if vol exists)

    prices_df, px, prices_date_max = _load_prices()
    if prices_df.empty or not px or not prices_date_max:
        out = {
            "ts": ts.isoformat(timespec="seconds"),
            "status": "SKIP",
            "reason": "prices_missing_or_empty",
            "prices_path": str(PRICES_PATH),
        }
        out_path = LOG_DIR / f"survivorship_daily_{_tag(ts)}.json"
        _safe_write_json(out_path, out)
        print(f"[SURV] wrote: {out_path}")
        print("[SURV] status=SKIP prices_missing_or_empty")
        return 0

    trading_days = sorted(prices_df[px.date].unique().tolist())
    # Per-code last info
    g = prices_df.groupby(px.code, sort=False)
    last_info: Dict[str, Dict[str, Any]] = {}
    for code, sub in g:
        sub2 = sub.sort_values(px.date)
        last_row = sub2.iloc[-1]
        last_date = str(last_row[px.date])
        last_close = float(last_row[px.close]) if pd.notna(last_row[px.close]) else None
        last_vol = None
        if px.volume and px.volume in sub2.columns:
            v = last_row[px.volume]
            last_vol = float(v) if pd.notna(v) else None
        # missing tail days
        try:
            idx_last = trading_days.index(last_date)
            missing_tail = len(trading_days) - 1 - idx_last
        except ValueError:
            missing_tail = None
        # consecutive halt tail
        halt_tail = None
        if px.volume and px.volume in sub2.columns:
            # count consecutive zero-vol at the tail
            vv = sub2[px.volume].fillna(0).astype(float).tolist()
            halt_tail = 0
            for x in reversed(vv):
                if x == 0:
                    halt_tail += 1
                else:
                    break
        last_info[code] = {
            "last_date": last_date,
            "last_close": last_close,
            "last_vol": last_vol,
            "missing_tail_days": missing_tail,
            "halt_tail_days": halt_tail,
        }

    # --- Candidates exclusion
    cand_df = _read_csv_df(CANDS_PATH)
    untradeable_candidates: List[Dict[str, Any]] = []
    cand_before = int(len(cand_df))
    cand_after = cand_before
    if not cand_df.empty and "code" in cand_df.columns and "date" in cand_df.columns:
        cand_df = cand_df.copy()
        cand_df["code"] = cand_df["code"].map(norm_code)
        cand_df["date_norm"] = cand_df["date"].map(_norm_date_str)

        drop_mask = [False] * len(cand_df)
        for i, row in cand_df.iterrows():
            code = row.get("code", "")
            sdate = row.get("date_norm", "")
            if not code or not sdate:
                continue
            entry_day = _next_trade_day(trading_days, sdate)
            if not entry_day:
                # can't trade yet
                continue

            info = last_info.get(code)
            if not info:
                drop_mask[i] = True
                untradeable_candidates.append(
                    {"code": code, "signal_date": sdate, "entry_day": entry_day, "reason": "NO_PRICE_SERIES"}
                )
                continue
            # If the code series ends before entry day -> cannot enter
            if info.get("last_date") and str(info["last_date"]) < entry_day:
                drop_mask[i] = True
                untradeable_candidates.append(
                    {
                        "code": code,
                        "signal_date": sdate,
                        "entry_day": entry_day,
                        "reason": f"NO_PRICE_ON_ENTRY(last_date={info.get('last_date')})",
                    }
                )
                continue
            # If volume exists and entry_day volume is 0 -> treat as halt entry
            if px.volume and px.volume in prices_df.columns:
                sub = prices_df[(prices_df[px.code] == code) & (prices_df[px.date] == entry_day)]
                if not sub.empty:
                    v = float(sub.iloc[0][px.volume]) if pd.notna(sub.iloc[0][px.volume]) else 0.0
                    if v == 0.0:
                        drop_mask[i] = True
                        untradeable_candidates.append(
                            {
                                "code": code,
                                "signal_date": sdate,
                                "entry_day": entry_day,
                                "reason": "HALT_ON_ENTRY(vol=0)",
                            }
                        )

        if any(drop_mask):
            cand_df = cand_df.loc[[not x for x in drop_mask]].copy()
            cand_df.drop(columns=["date_norm"], errors="ignore", inplace=True)
            _write_csv_df(CANDS_PATH, cand_df)
        cand_after = int(len(cand_df))

    # --- Open positions forced exit
    state = _safe_read_json(STATE_PATH, {"open_positions": [], "processed_signals": {}})
    open_positions = state.get("open_positions") or []
    forced_exits: List[Dict[str, Any]] = []
    still_open: List[Dict[str, Any]] = []

    for pos in open_positions:
        code = norm_code(pos.get("code"))
        qty = int(pos.get("qty") or 0)
        entry_date = _norm_date_str(pos.get("entry_date")) or ""
        entry_price = float(pos.get("entry_price") or 0.0)
        order_id = str(pos.get("order_id") or "")

        info = last_info.get(code)
        if not info:
            # no series at all -> force exit at entry_price
            forced_exits.append(
                {
                    "code": code,
                    "qty": qty,
                    "entry_date": entry_date,
                    "entry_price": entry_price,
                    "exit_date": entry_date,
                    "exit_price": entry_price,
                    "reason": "NO_PRICE_SERIES",
                }
            )
            continue

        missing_tail = info.get("missing_tail_days")
        halt_tail = info.get("halt_tail_days")
        last_date = info.get("last_date")
        last_close = info.get("last_close")
        # Decide untradeable
        is_missing = isinstance(missing_tail, int) and missing_tail >= FORCE_EXIT_MISSING_DAYS
        is_halt = isinstance(halt_tail, int) and halt_tail >= FORCE_EXIT_HALT_DAYS

        if is_missing or is_halt:
            exit_date = str(last_date) if last_date else prices_date_max
            exit_price = float(last_close) if last_close is not None else entry_price
            exit_reason = "DELIST_OR_MISSING" if is_missing else "HALT_VOL0"
            # Append SELL fill + trade
            dt_str = f"{exit_date}T15:20:00"
            sell_order_id = f"PAPER_SELL_{code}_{exit_date}_{exit_reason}"
            _append_fill_row(
                FILLS_PATH,
                {
                    "datetime": dt_str,
                    "code": code,
                    "side": "SELL",
                    "qty": qty,
                    "price": exit_price,
                    "order_id": sell_order_id,
                    "note": f"forced_exit=1;reason={exit_reason};orig_order_id={order_id}",
                },
            )
            pnl = round((exit_price - entry_price) * qty, 6)
            _append_trade_row(
                TRADES_PATH,
                {
                    "code": code,
                    "entry_date": entry_date,
                    "entry_price": entry_price,
                    "exit_date": exit_date,
                    "exit_price": exit_price,
                    "qty": qty,
                    "pnl_krw": pnl,
                    "note": f"forced_exit=1;reason={exit_reason};orig_order_id={order_id}",
                    "exit_reason": exit_reason,
                },
            )
            forced_exits.append(
                {
                    "code": code,
                    "qty": qty,
                    "entry_date": entry_date,
                    "entry_price": entry_price,
                    "exit_date": exit_date,
                    "exit_price": exit_price,
                    "reason": exit_reason,
                    "missing_tail_days": missing_tail,
                    "halt_tail_days": halt_tail,
                    "last_date": last_date,
                }
            )
        else:
            still_open.append(pos)

    # persist state if changed
    if len(still_open) != len(open_positions):
        state["open_positions"] = still_open
        _safe_write_json(STATE_PATH, state)

    out = {
        "ts": ts.isoformat(timespec="seconds"),
        "status": "PASS",
        "prices_date_max": prices_date_max,
        "candidates_before": cand_before,
        "candidates_after": cand_after,
        "untradeable_candidates": untradeable_candidates,
        "open_positions_before": len(open_positions),
        "open_positions_after": len(still_open),
        "forced_exits": forced_exits,
        "params": {
            "force_exit_missing_days": FORCE_EXIT_MISSING_DAYS,
            "force_exit_halt_days": FORCE_EXIT_HALT_DAYS,
        },
    }

    out_path = LOG_DIR / f"survivorship_daily_{_tag(ts)}.json"
    _safe_write_json(out_path, out)
    _safe_write_json(LOG_DIR / "survivorship_daily_last.json", out)

    print(f"[SURV] wrote: {out_path}")
    print(
        f"[SURV] status=PASS prices_date_max={prices_date_max} "
        f"cands:{cand_before}->{cand_after} open:{len(open_positions)}->{len(still_open)} "
        f"forced_exits={len(forced_exits)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
