from __future__ import annotations

import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import pandas as pd

# --- PATCH: default requests timeout to avoid hangs ---
try:
    import requests
    _orig_request = requests.sessions.Session.request
    def _request_with_timeout(self, method, url, **kwargs):
        if kwargs.get("timeout", None) is None:
            kwargs["timeout"] = (5, 30)  # (connect, read) seconds
        return _orig_request(self, method, url, **kwargs)
    requests.sessions.Session.request = _request_with_timeout
except Exception:
    pass
# --- END PATCH ---


# ---- deps (fail fast) --------------------------------------------------------
try:
    from pykrx import stock
except Exception as e:  # pragma: no cover
    raise SystemExit(f"[FATAL] pykrx import failed: {type(e).__name__}: {e}")

# ---- helpers -----------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent

KOREAN_COL_MAP = {
    "시가": "open",
    "고가": "high",
    "저가": "low",
    "종가": "close",
    "거래량": "volume",
    "등락률": "change_rate",
}

MARKETS = ["KOSPI", "KOSDAQ"]


def _yyyymmdd(s: str) -> str:
    return str(s).replace("-", "").replace("/", "")[:8]


def _default_end_yyyymmdd() -> str:
    """Default end date = prev weekday/session of today (KRX)."""
    # Prefer exchange_calendars if available
    try:
        import exchange_calendars as xc
        import pandas as _pd

        cal = xc.get_calendar("XKRX")
        today = _pd.Timestamp(datetime.now().date())
        prev = cal.date_to_session(today, direction="previous")
        return prev.strftime("%Y%m%d")
    except Exception:
        d = datetime.now().date()
        # prev weekday (Mon-Fri)
        d = d - timedelta(days=1)
        while d.weekday() >= 5:
            d = d - timedelta(days=1)
        return d.strftime("%Y%m%d")


def _find_clean_parquets(root: Path) -> List[Path]:
    return sorted(root.rglob("krx_daily_*_clean.parquet"))


def _parquet_schema_cols(p: Path) -> Tuple[List[str], str]:
    """Return (schema_cols, date_type_hint)."""
    try:
        import pyarrow.parquet as pq  # type: ignore

        pf = pq.ParquetFile(p)
        cols = list(pf.schema_arrow.names)
        date_type = ""
        try:
            if "date" in cols:
                date_type = str(pf.schema_arrow.field("date").type)
        except Exception:
            date_type = ""
        return cols, date_type
    except Exception:
        df = pd.read_parquet(p)
        cols = list(df.columns)
        date_type = str(df["date"].dtype) if "date" in df.columns else ""
        return cols, date_type


def _parquet_date_max(p: Path) -> Optional[str]:
    """Compute date_max(YYYYMMDD) from parquet's date column."""
    try:
        df = pd.read_parquet(p, columns=["date"])
    except Exception:
        return None
    if df is None or df.empty or "date" not in df.columns:
        return None
    s = df["date"].astype(str).str.replace("-", "").str[:8]
    if s.empty:
        return None
    return str(s.max())


def _normalize_by_ticker_df(ymd: str, mkt: str, df: pd.DataFrame) -> pd.DataFrame:
    """Normalize output of stock.get_market_ohlcv_by_ticker()."""
    if df is None or df.empty:
        return pd.DataFrame()

    x = df.copy()
    x["code"] = x.index.astype(str).str.zfill(6)
    x["market"] = mkt
    x["date"] = ymd

    # map Korean columns
    for k, v in KOREAN_COL_MAP.items():
        if k in x.columns and v not in x.columns:
            x[v] = x[k]

    if "change_rate" not in x.columns:
        if "change" in x.columns:
            x["change_rate"] = x["change"]
        else:
            x["change_rate"] = pd.NA

    if "volume" not in x.columns:
        x["volume"] = 0

    x["value"] = pd.to_numeric(x.get("close", pd.NA), errors="coerce") * pd.to_numeric(
        x.get("volume", 0), errors="coerce"
    )

    keep = ["date", "code", "market", "open", "high", "low", "close", "volume", "value", "change_rate"]
    for c in keep:
        if c not in x.columns:
            x[c] = pd.NA

    out = x[keep].copy()
    out["date"] = out["date"].astype(str).str.replace("-", "").str[:8]
    out["code"] = out["code"].astype(str).str.zfill(6)

    for c in ["open", "high", "low", "close", "volume", "value", "change_rate"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.dropna(subset=["date", "code", "open", "high", "low", "close"])
    return out


def _fetch_day_by_date_probe(ymd: str, mkt: str, probe_codes: List[str]) -> pd.DataFrame:
    """
    Fallback when by_ticker() is empty.
    Uses by_date() for a small probe list (fast + stable enough to advance date_max).
    """
    recs = []
    i = 0
    n = len(probe_codes)
    for code in probe_codes:
        i += 1
        if (i == 1) or ((i % 50) == 0):
            print("[PROBE] {} {} {}/{} ok={}".format(ymd, mkt, i, n, len(recs)), flush=True)
        code6 = str(code).zfill(6)
        try:
            dft = stock.get_market_ohlcv_by_date(ymd, ymd, code6)
        except Exception:
            continue
        if dft is None or len(dft) == 0:
            continue

        row = dft.iloc[-1]
        rec = {
            "date": ymd,
            "code": code6,
            "market": mkt,
            "open": row.get("시가", row.get("open", pd.NA)),
            "high": row.get("고가", row.get("high", pd.NA)),
            "low": row.get("저가", row.get("low", pd.NA)),
            "close": row.get("종가", row.get("close", pd.NA)),
            "volume": row.get("거래량", row.get("volume", 0)),
            "change_rate": row.get("등락률", row.get("change_rate", pd.NA)),
        }
        recs.append(rec)

    if not recs:
        return pd.DataFrame()

    out = pd.DataFrame.from_records(recs)
    out["date"] = out["date"].astype(str).str.replace("-", "").str[:8]
    out["code"] = out["code"].astype(str).str.zfill(6)
    for c in ["open", "high", "low", "close", "volume", "change_rate"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out["value"] = pd.to_numeric(out.get("close", pd.NA), errors="coerce") * pd.to_numeric(
        out.get("volume", 0), errors="coerce"
    )
    out = out.dropna(subset=["date", "code", "open", "high", "low", "close"])
    return out


def _build_to_schema(df: pd.DataFrame, schema_cols: List[str], date_type_hint: str) -> pd.DataFrame:
    """
    df(standard columns) -> fit schema_cols. Missing columns become NA.
    If schema wants 'change_rate' but df has 'change', map it.
    """
    out = pd.DataFrame()

    if "date" in schema_cols:
        if "timestamp" in (date_type_hint or "").lower() or "date" in (date_type_hint or "").lower():
            out["date"] = pd.to_datetime(df["date"].astype(str), format="%Y%m%d", errors="coerce")
        else:
            out["date"] = df["date"].astype(str)

    for c in schema_cols:
        if c == "date":
            continue
        if c in df.columns:
            out[c] = df[c]
        elif c == "change_rate" and "change" in df.columns:
            out[c] = df["change"]
        else:
            out[c] = pd.NA

    return out


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Incrementally add missing KRX daily data into a new *_clean.parquet."
    )
    ap.add_argument(
        "--base", "--base-dir",
        default=str(BASE_DIR),
        help="Base directory to search krx_daily_*_clean.parquet (default: script dir)",
    )
    ap.add_argument(
        "--end",
        default=None,
        help="Target end date yyyymmdd (default: prev weekday/session of today)",
    )
    ap.add_argument(
        "--out-dir",
        default=None,
        help="Output directory (default: latest clean parquet's parent)",
    )
    ap.add_argument(
        "--probe-cap",
        type=int,
        default=50,
        help="Max probe codes to try per market if by_ticker() is empty (default: 50)",
    )

    ap.add_argument(
        "--min-uni",
        type=int,
        default=2000,
        help="Fail-closed if fetched day's universe (unique codes) is below this threshold (default: 2000)",
    )
    ap.add_argument(
        "--coverage",
        type=float,
        default=0.90,
        help="Required coverage ratio for by_date_probe fallback (default: 0.90)",
    )
    args = ap.parse_args()

    base = Path(args.base)
    if not base.exists():
        raise SystemExit(f"[FATAL] base not found: {base}")

    end_ymd = _yyyymmdd(args.end) if args.end else _default_end_yyyymmdd()

    clean_files = _find_clean_parquets(base)
    if not clean_files:
        raise SystemExit(f"[FATAL] no krx_daily_*_clean.parquet under base={base}")

    # pick best clean parquet by (date_max, ncode) with universe guard
    MIN_UNI = 2000
    cand = []
    for _p in clean_files:
        _dm = _parquet_date_max(_p)
        if not _dm:
            continue
        _n = 0
        try:
            _sl = pd.read_parquet(_p, columns=["date","code"])
            _d = pd.to_datetime(_sl["date"], errors="coerce")
            if len(_sl) > 0 and (not _d.isna().all()):
                _mx = _d.max()
                _n = int(_sl.loc[_d==_mx,"code"].astype(str).nunique())
        except Exception:
            _n = 0
        cand.append((_p, _dm, _n, _p.stat().st_mtime))
    if not cand:
        raise SystemExit(f"[FATAL] cannot read date_max from any clean parquet under base={base}")
    good = [t for t in cand if t[2] >= MIN_UNI]
    pick = max(good, key=lambda t:(t[1],t[2],t[3])) if good else max(cand, key=lambda t:(t[1],t[2],t[3]))
    latest, prev_max, prev_ncode, _ = pick
    if prev_ncode < MIN_UNI:
        raise SystemExit(f"[FATAL] latest clean universe degraded (ncode={prev_ncode} < MIN_UNI={MIN_UNI}). abort to avoid partial update. latest={latest}")

    schema_cols, date_type_hint = _parquet_schema_cols(latest)

    # probe codes from latest parquet (deterministic + small)
    probe_by_market = {}
    try:
        slim = pd.read_parquet(latest, columns=["code", "market"])
        for mkt in MARKETS:
            codes = (
                slim[slim["market"].astype(str) == mkt]["code"]
                .astype(str)
                .str.zfill(6)
                .dropna()
                .unique()
                .tolist()
            )
            # user-verified: 005930 by_date works at least for 2026-01-07~09
            if mkt == "KOSPI" and "005930" not in codes:
                codes = ["005930"] + codes
            probe_by_market[mkt] = codes[: max(1, int(args.probe_cap))]
    except Exception:
        probe_by_market = {m: (["005930"] if m == "KOSPI" else []) for m in MARKETS}

    start_dt = datetime.strptime(prev_max, "%Y%m%d").date() + timedelta(days=1)
    end_dt = datetime.strptime(end_ymd, "%Y%m%d").date()
    if start_dt > end_dt:
        print(f"[OK] nothing to do (prev_max={prev_max} >= end={end_ymd})")
        return 0

    all_frames = []
    fetched_days: List[str] = []

    # SSOT: trading sessions (XKRX). If unavailable, treat as unknown.
    cal = None
    try:
        import exchange_calendars as xc  # type: ignore
        cal = xc.get_calendar("XKRX")
    except Exception:
        cal = None
    skipped_non_session: List[str] = []

    cur = start_dt
    while cur <= end_dt:
        ymd = cur.strftime("%Y%m%d")
        # holiday/non-session: empty is normal (SKIP at date-loop level)
        if cal is not None:
            try:
                if not cal.is_session(ymd):
                    skipped_non_session.append(ymd)
                    print(f"[SKIP] {ymd} (non-session)")
                    cur += timedelta(days=1)
                    continue
            except Exception:
                pass
        day_frames = []
        used_fallback = False

        for mkt in MARKETS:
            df = None
            try:
                df = stock.get_market_ohlcv_by_ticker(ymd, market=mkt)
            except Exception as e:
                print(f"[WARN] pykrx by_ticker failed {ymd} {mkt}: {type(e).__name__}: {e}")
                df = None

            norm = _normalize_by_ticker_df(ymd, mkt, df) if df is not None else pd.DataFrame()

            if norm.empty:
                # Non-session day: empty is normal (defensive; should have been skipped above)
                if cal is not None:
                    try:
                        if not cal.is_session(ymd):
                            continue
                    except Exception:
                        pass
                # Trading session: empty means upstream endpoint is unhealthy -> abort (avoid degraded universe)
                # Trading session인데 by_ticker empty → by_date로 전체(=probe_cap만큼) 수집 시도
                fb = _fetch_day_by_date_probe(ymd, mkt, probe_by_market.get(mkt, []))
                if fb is None or fb.empty:
                    raise SystemExit(f"[FATAL] pykrx by_ticker empty for {ymd} {mkt} and by_date_probe empty. abort.")
                try:
                    n_fb = int(fb['code'].astype(str).nunique()) if 'code' in fb.columns else 0
                except Exception:
                    n_fb = 0
                exp = len(probe_by_market.get(mkt, []))
                min_ok = max(1, int(exp * float(args.coverage)))
                if n_fb < min_ok:
                    raise SystemExit(f"[FATAL] by_date_probe coverage low (ok={n_fb} < min_ok={min_ok}, exp={exp}) for {ymd} {mkt}. abort.")
                used_fallback = True
                norm = fb

            if not norm.empty:
                day_frames.append(norm)

        if not day_frames:
            print(f"[SKIP] {ymd} (no data)")
            cur += timedelta(days=1)
            continue

        day_df = pd.concat(day_frames, ignore_index=True)
        try:
            n_day = int(day_df["code"].astype(str).nunique()) if "code" in day_df.columns else 0
        except Exception:
            n_day = 0
        if n_day < MIN_UNI:
            raise SystemExit(f"[FATAL] universe degraded on {ymd} (ncode={n_day} < MIN_UNI={MIN_UNI}). abort (fail-closed, no write).")
        all_frames.append(day_df)
        fetched_days.append(ymd)
        print(f"[OK] {ymd} fetched (fallback=by_date_probe)" if used_fallback else f"[OK] {ymd} fetched")
        cur += timedelta(days=1)

    if not all_frames:
        if skipped_non_session:
            print(
                f"[OK] no trading sessions between {start_dt.strftime('%Y%m%d')} and {end_ymd}. "
                f"skipped_non_session={len(skipped_non_session)}"
            )
            return 0
        raise SystemExit(
            f"[FATAL] fetched nothing between {start_dt.strftime('%Y%m%d')} and {end_ymd}. "
            f"If today is a holiday/early run, try --end with an earlier yyyymmdd."
        )

    out_df = pd.concat(all_frames, ignore_index=True)
    out_df = _build_to_schema(out_df, schema_cols, date_type_hint)

    out_dir = Path(args.out_dir) if args.out_dir else latest.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    start_ymd = fetched_days[0]
    end_ymd2 = fetched_days[-1]
    out_path = out_dir / f"krx_daily_{start_ymd}_{end_ymd2}_clean.parquet"

    out_df.to_parquet(out_path, index=False, engine="pyarrow")
    print(f"[OK] wrote: {out_path}")
    print(f"[OK] days={len(fetched_days)} range={start_ymd}~{end_ymd2} rows={len(out_df)} schema_cols={len(schema_cols)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
