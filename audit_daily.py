from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "2_Logs"
PAPER_DIR = BASE_DIR / "paper"
PRICES_PARQUET = PAPER_DIR / "prices" / "ohlcv_paper.parquet"
CAND_DATA = LOG_DIR / "candidates_latest_data.csv"
CAND_META = LOG_DIR / "candidates_latest_meta.json"
FILLS = PAPER_DIR / "fills.csv"
TRADES = PAPER_DIR / "trades.csv"

_SIGNAL_RE = re.compile(r"signal_date=(\d{8})")


def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _ymd_today() -> str:
    return date.today().strftime("%Y%m%d")


def _safe_read_text(p: Path) -> Optional[str]:
    try:
        return p.read_text(encoding="utf-8")
    except Exception:
        try:
            return p.read_text(encoding="utf-8-sig")
        except Exception:
            return None


def _safe_read_json(p: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        try:
            return json.loads(p.read_text(encoding="utf-8-sig"))
        except Exception:
            return None


def _load_csv(p: Path) -> Optional[pd.DataFrame]:
    if not p.exists() or p.stat().st_size == 0:
        return None
    for enc in ("utf-8-sig", "utf-8"):
        try:
            return pd.read_csv(p, encoding=enc)
        except Exception:
            continue
    try:
        return pd.read_csv(p)
    except Exception:
        return None


def _to_yyyymmdd(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    s = s.replace("-", "").replace("/", "").replace(".", "")
    m = re.match(r"^(\d{8})", s)
    return m.group(1) if m else None


def _extract_signal_date(note: Any) -> Optional[str]:
    if note is None:
        return None
    s = str(note)
    m = _SIGNAL_RE.search(s)
    return m.group(1) if m else None


def _next_weekday_yyyymmdd(ymd: str) -> Optional[str]:
    """Fallback: next weekday (ignores holidays)."""
    try:
        d = datetime.strptime(ymd, "%Y%m%d").date()
    except Exception:
        return None
    d = d + timedelta(days=1)
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d = d + timedelta(days=1)
    return d.strftime("%Y%m%d")


def _load_prices() -> Tuple[Optional[pd.DataFrame], List[str]]:
    flags: List[str] = []
    if not PRICES_PARQUET.exists() or PRICES_PARQUET.stat().st_size == 0:
        flags.append("prices_parquet_missing")
        return None, flags
    try:
        df = pd.read_parquet(PRICES_PARQUET)  # requires pyarrow/fastparquet on user machine
    except Exception as e:
        flags.append("prices_parquet_unreadable:" + (str(e)[:120]))
        return None, flags

    # Normalize common schemas
    # expected columns: date, code, open/high/low/close
    if "date" not in df.columns:
        flags.append("prices_missing_date_col")
        return None, flags

    if "code" not in df.columns:
        flags.append("prices_missing_code_col")
        return None, flags

    df = df.copy()
    df["date"] = df["date"].astype(str).str.replace("-", "").str[:8]
    df["code"] = df["code"].astype(str).str.zfill(6)

    # normalize open/close names
    if "open" not in df.columns:
        # sometimes Korean headers exist
        for k in ("시가", "Open", "OPEN"):
            if k in df.columns:
                df["open"] = pd.to_numeric(df[k], errors="coerce")
                break
    else:
        df["open"] = pd.to_numeric(df["open"], errors="coerce")

    if "close" not in df.columns:
        for k in ("종가", "Close", "CLOSE"):
            if k in df.columns:
                df["close"] = pd.to_numeric(df[k], errors="coerce")
                break
    else:
        df["close"] = pd.to_numeric(df["close"], errors="coerce")

    return df, flags


def _next_trading_date_from_prices(px: pd.DataFrame, code: str, signal_date: str) -> Optional[str]:
    dd = px[(px["code"] == code) & (px["date"] > signal_date)][["date"]]
    if dd.empty:
        return None
    # dd["date"] is yyyymmdd string, lexicographic works
    return str(dd["date"].min())


def _open_price(px: pd.DataFrame, code: str, ymd: str) -> Optional[float]:
    r = px[(px["code"] == code) & (px["date"] == ymd)]
    if r.empty:
        return None
    if "open" not in r.columns:
        return None
    try:
        v = float(r.iloc[0]["open"])
        return v
    except Exception:
        return None


def _approx_equal(a: Optional[float], b: Optional[float], tol: float = 1e-6) -> bool:
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) <= tol


def main() -> int:
    out_path = LOG_DIR / f"audit_daily_{_ymd_today()}_{datetime.now().strftime('%H%M%S')}.json"
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    report: Dict[str, Any] = {
        "generated_at": _now_ts(),
        "base_dir": str(BASE_DIR),
        "paths": {
            "candidates_latest_data": str(CAND_DATA),
            "candidates_latest_meta": str(CAND_META),
            "fills": str(FILLS),
            "trades": str(TRADES),
            "prices_parquet": str(PRICES_PARQUET),
        },
        "flags": [],
        "summary": {},
        "suspects": [],
    }

    # Load inputs
    cand_df = _load_csv(CAND_DATA)
    meta = _safe_read_json(CAND_META) if CAND_META.exists() else None
    fills_df = _load_csv(FILLS)
    trades_df = _load_csv(TRADES)

    px, px_flags = _load_prices()
    report["flags"].extend(px_flags)

    # Normalize candidate signals
    signals: List[Tuple[str, str]] = []  # (code, signal_date)
    if isinstance(cand_df, pd.DataFrame) and not cand_df.empty:
        df = cand_df.copy()
        if "code" in df.columns:
            df["code"] = df["code"].astype(str).str.zfill(6)
        if "signal_date" not in df.columns:
            if "date" in df.columns:
                df["signal_date"] = df["date"].astype(str).str.replace("-", "").str[:8]
            else:
                df["signal_date"] = _ymd_today()

        # keep unique (code, signal_date)
        for _, r in df.iterrows():
            code = str(r.get("code", "")).zfill(6)
            sd = _to_yyyymmdd(r.get("signal_date"))
            if code and sd:
                signals.append((code, sd))
        signals = sorted(set(signals))

    # Audit records
    suspects: List[Dict[str, Any]] = []

    # 1) Trades timing checks (strongest signal)
    trade_checked = 0
    if isinstance(trades_df, pd.DataFrame) and not trades_df.empty:
        tdf = trades_df.copy()

        # detect legacy vs v41.1
        if "entry_date" in tdf.columns:
            # legacy
            tdf["code"] = tdf["code"].astype(str).str.zfill(6)
            for idx, r in tdf.iterrows():
                code = str(r.get("code", "")).zfill(6)
                entry_date = _to_yyyymmdd(r.get("entry_date"))
                entry_price = None
                try:
                    entry_price = float(r.get("entry_price")) if r.get("entry_price") is not None else None
                except Exception:
                    entry_price = None

                note = r.get("note")
                sd = _extract_signal_date(note)
                trade_checked += 1

                if sd is None:
                    suspects.append({"type": "missing_signal_date", "source": "trades", "row": int(idx), "code": code})
                    continue
                if entry_date is None:
                    suspects.append({"type": "missing_entry_date", "source": "trades", "row": int(idx), "code": code, "signal_date": sd})
                    continue

                if entry_date <= sd:
                    suspects.append({
                        "type": "entry_not_after_signal",
                        "source": "trades",
                        "row": int(idx),
                        "code": code,
                        "signal_date": sd,
                        "entry_date": entry_date,
                    })

                expected_entry = None
                if px is not None:
                    expected_entry = _next_trading_date_from_prices(px, code, sd)
                else:
                    expected_entry = _next_weekday_yyyymmdd(sd)

                if expected_entry and entry_date != expected_entry:
                    suspects.append({
                        "type": "entry_date_mismatch_expected",
                        "source": "trades",
                        "row": int(idx),
                        "code": code,
                        "signal_date": sd,
                        "entry_date": entry_date,
                        "expected_entry_date": expected_entry,
                    })

                # price check: entry_price == open(entry_date)
                if px is not None and entry_date:
                    op = _open_price(px, code, entry_date)
                    if op is not None and entry_price is not None and (not _approx_equal(op, entry_price, tol=1e-3)):
                        suspects.append({
                            "type": "entry_price_mismatch_open",
                            "source": "trades",
                            "row": int(idx),
                            "code": code,
                            "entry_date": entry_date,
                            "entry_price": entry_price,
                            "expected_open": op,
                        })
        else:
            # v41.1 style (entry_ts)
            if "entry_ts" in tdf.columns:
                tdf["code"] = tdf["code"].astype(str).str.zfill(6)
                for idx, r in tdf.iterrows():
                    code = str(r.get("code", "")).zfill(6)
                    entry_ts = str(r.get("entry_ts", "")).strip()
                    entry_date = _to_yyyymmdd(entry_ts)
                    note = r.get("note")
                    sd = _extract_signal_date(note)
                    trade_checked += 1

                    if sd is None:
                        suspects.append({"type": "missing_signal_date", "source": "trades", "row": int(idx), "code": code})
                        continue
                    if entry_date is None:
                        suspects.append({"type": "missing_entry_date", "source": "trades", "row": int(idx), "code": code, "signal_date": sd})
                        continue
                    if entry_date <= sd:
                        suspects.append({
                            "type": "entry_not_after_signal",
                            "source": "trades",
                            "row": int(idx),
                            "code": code,
                            "signal_date": sd,
                            "entry_date": entry_date,
                        })

    # 2) BUY fills timing/price checks (secondary)
    fills_buy_checked = 0
    if isinstance(fills_df, pd.DataFrame) and not fills_df.empty:
        fdf = fills_df.copy()
        # legacy uses 'datetime', v41.1 uses 'ts' and 'date'
        if "code" in fdf.columns:
            fdf["code"] = fdf["code"].astype(str).str.zfill(6)

        for idx, r in fdf.iterrows():
            side = str(r.get("side", "")).upper()
            if side != "BUY":
                continue
            code = str(r.get("code", "")).zfill(6)
            note = r.get("note")
            sd = _extract_signal_date(note)
            fills_buy_checked += 1

            # fill date
            fill_date = None
            if "date" in fdf.columns:
                fill_date = _to_yyyymmdd(r.get("date"))
            if fill_date is None:
                fill_date = _to_yyyymmdd(r.get("datetime") or r.get("ts"))

            # fill price
            fill_price = None
            try:
                fill_price = float(r.get("price")) if r.get("price") is not None else None
            except Exception:
                fill_price = None

            if sd is None:
                suspects.append({"type": "missing_signal_date", "source": "fills", "row": int(idx), "code": code})
                continue
            if fill_date is None:
                suspects.append({"type": "missing_fill_date", "source": "fills", "row": int(idx), "code": code, "signal_date": sd})
                continue

            if fill_date <= sd:
                suspects.append({
                    "type": "fill_not_after_signal",
                    "source": "fills",
                    "row": int(idx),
                    "code": code,
                    "signal_date": sd,
                    "fill_date": fill_date,
                })

            expected_entry = None
            if px is not None:
                expected_entry = _next_trading_date_from_prices(px, code, sd)
            else:
                expected_entry = _next_weekday_yyyymmdd(sd)

            if expected_entry and fill_date != expected_entry:
                suspects.append({
                    "type": "fill_date_mismatch_expected",
                    "source": "fills",
                    "row": int(idx),
                    "code": code,
                    "signal_date": sd,
                    "fill_date": fill_date,
                    "expected_entry_date": expected_entry,
                })

            # price check: fill_price == open(fill_date)
            if px is not None and fill_date and fill_price is not None:
                op = _open_price(px, code, fill_date)
                if op is not None and (not _approx_equal(op, fill_price, tol=1e-3)):
                    suspects.append({
                        "type": "fill_price_mismatch_open",
                        "source": "fills",
                        "row": int(idx),
                        "code": code,
                        "fill_date": fill_date,
                        "fill_price": fill_price,
                        "expected_open": op,
                    })

    # Compose summary
    report["summary"] = {
        "signals_rows": int(len(cand_df)) if isinstance(cand_df, pd.DataFrame) else 0,
        "signals_unique": int(len(signals)),
        "fills_rows": int(len(fills_df)) if isinstance(fills_df, pd.DataFrame) else 0,
        "trades_rows": int(len(trades_df)) if isinstance(trades_df, pd.DataFrame) else 0,
        "checked_trades": int(trade_checked),
        "checked_buy_fills": int(fills_buy_checked),
        "lookahead_suspects": int(len(suspects)),
        "suspect_types": dict(pd.Series([s.get("type") for s in suspects]).value_counts()) if suspects else {},
        "status": "PASS" if len(suspects) == 0 else "FAIL",
        "meta_latest_date": _to_yyyymmdd((meta or {}).get("latest_date")),
        "meta_market_regime": (meta or {}).get("market_regime"),
    }
    report["suspects"] = suspects[:500]  # hard cap to keep file small

    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[AUDIT] wrote: {out_path}")
    print(f"[AUDIT] status={report['summary']['status']} lookahead_suspects={report['summary']['lookahead_suspects']} checked_trades={trade_checked} checked_buy_fills={fills_buy_checked}")

    if suspects:
        # print first few for quick visibility
        for s in suspects[:5]:
            print("[AUDIT] suspect:", json.dumps(s, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
