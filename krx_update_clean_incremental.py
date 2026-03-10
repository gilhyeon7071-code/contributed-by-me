from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# Default network timeout guard for requests-based dependencies.
try:
    import requests

    _orig_request = requests.sessions.Session.request

    def _request_with_timeout(self, method, url, **kwargs):
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = (5, 30)
        return _orig_request(self, method, url, **kwargs)

    requests.sessions.Session.request = _request_with_timeout
except Exception:
    pass

try:
    from pykrx import stock
except Exception as e:
    raise SystemExit(f"[FATAL] pykrx import failed: {type(e).__name__}: {e}")


BASE_DIR = Path(__file__).resolve().parent
MARKETS = ["KOSPI", "KOSDAQ"]

K_OPEN = "\uc2dc\uac00"
K_HIGH = "\uace0\uac00"
K_LOW = "\uc800\uac00"
K_CLOSE = "\uc885\uac00"
K_VOLUME = "\uac70\ub798\ub7c9"
K_CHANGE_RATE = "\ub4f1\ub77d\ub960"


def _yyyymmdd(s: str) -> str:
    return str(s).replace("-", "").replace("/", "")[:8]


def _default_end_yyyymmdd() -> str:
    try:
        import exchange_calendars as xc
        import pandas as _pd

        cal = xc.get_calendar("XKRX")
        today = _pd.Timestamp(datetime.now().date())
        prev = cal.date_to_session(today, direction="previous")
        return prev.strftime("%Y%m%d")
    except Exception:
        d = datetime.now().date() - timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d.strftime("%Y%m%d")


def _find_clean_parquets(root: Path) -> List[Path]:
    return sorted(root.rglob("krx_daily_*_clean.parquet"))


def _parquet_schema_cols(p: Path) -> Tuple[List[str], str]:
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
    try:
        df = pd.read_parquet(p, columns=["date"])
    except Exception:
        return None
    if df.empty or "date" not in df.columns:
        return None
    s = df["date"].astype(str).str.replace("-", "", regex=False).str[:8]
    return str(s.max()) if not s.empty else None


def _series_from_candidates(df: pd.DataFrame, names: List[str]) -> pd.Series:
    for n in names:
        if n in df.columns:
            return df[n]
    norm = {str(c).strip(): c for c in df.columns}
    for n in names:
        c = norm.get(str(n).strip())
        if c is not None:
            return df[c]
    return pd.Series([pd.NA] * len(df), index=df.index)


def _row_from_candidates(row: pd.Series, names: List[str], default=pd.NA):
    idx = {str(k).strip(): k for k in row.index}
    for n in names:
        if n in row.index:
            return row[n]
        key = idx.get(str(n).strip())
        if key is not None:
            return row[key]
    return default


def _normalize_by_ticker_df(ymd: str, mkt: str, df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    x = df.copy()
    x["code"] = x.index.astype(str).str.zfill(6)
    x["market"] = mkt
    x["date"] = ymd

    x["open"] = _series_from_candidates(x, ["open", "Open", K_OPEN])
    x["high"] = _series_from_candidates(x, ["high", "High", K_HIGH])
    x["low"] = _series_from_candidates(x, ["low", "Low", K_LOW])
    x["close"] = _series_from_candidates(x, ["close", "Close", K_CLOSE])
    x["volume"] = _series_from_candidates(x, ["volume", "Volume", K_VOLUME])
    x["change_rate"] = _series_from_candidates(
        x, ["change_rate", "change", "Change", K_CHANGE_RATE]
    )

    x["value"] = pd.to_numeric(x["close"], errors="coerce") * pd.to_numeric(
        x["volume"], errors="coerce"
    )

    keep = [
        "date",
        "code",
        "market",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
        "change_rate",
    ]
    out = x[keep].copy()
    out["date"] = out["date"].astype(str).str.replace("-", "", regex=False).str[:8]
    out["code"] = out["code"].astype(str).str.zfill(6)

    for c in ["open", "high", "low", "close", "volume", "value", "change_rate"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.dropna(subset=["date", "code", "open", "high", "low", "close"])
    return out


def _fetch_day_by_date_codes(ymd: str, mkt: str, codes: List[str]) -> pd.DataFrame:
    if not codes:
        return pd.DataFrame()

    recs = []
    n = len(codes)
    for i, code in enumerate(codes, start=1):
        if i == 1 or i % 50 == 0 or i == n:
            print(f"[PROBE] {ymd} {mkt} {i}/{n} ok={len(recs)}", flush=True)

        code6 = str(code).zfill(6)
        try:
            dft = stock.get_market_ohlcv_by_date(ymd, ymd, code6)
        except Exception:
            continue
        if dft is None or dft.empty:
            continue

        row = dft.iloc[-1]
        recs.append(
            {
                "date": ymd,
                "code": code6,
                "market": mkt,
                "open": _row_from_candidates(row, ["open", "Open", K_OPEN]),
                "high": _row_from_candidates(row, ["high", "High", K_HIGH]),
                "low": _row_from_candidates(row, ["low", "Low", K_LOW]),
                "close": _row_from_candidates(row, ["close", "Close", K_CLOSE]),
                "volume": _row_from_candidates(row, ["volume", "Volume", K_VOLUME], 0),
                "change_rate": _row_from_candidates(
                    row, ["change_rate", "change", "Change", K_CHANGE_RATE]
                ),
            }
        )

    if not recs:
        return pd.DataFrame()

    out = pd.DataFrame.from_records(recs)
    out["date"] = out["date"].astype(str).str.replace("-", "", regex=False).str[:8]
    out["code"] = out["code"].astype(str).str.zfill(6)
    for c in ["open", "high", "low", "close", "volume", "change_rate"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out["value"] = pd.to_numeric(out["close"], errors="coerce") * pd.to_numeric(
        out["volume"], errors="coerce"
    )
    out = out.dropna(subset=["date", "code", "open", "high", "low", "close"])
    return out


def _build_to_schema(df: pd.DataFrame, schema_cols: List[str], date_type_hint: str) -> pd.DataFrame:
    out = pd.DataFrame()

    if "date" in schema_cols:
        if "timestamp" in date_type_hint.lower() or "date" in date_type_hint.lower():
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


def _fallback_caps(total: int, seed: int) -> List[int]:
    if total <= 0:
        return []
    seed = max(1, int(seed))
    ramp = [seed, 300, 700, 1200, 1800, total]
    caps: List[int] = []
    for x in ramp:
        c = min(total, int(x))
        if c not in caps:
            caps.append(c)
    caps.sort()
    return caps


def _market_code_pool(latest: Path, latest_ymd: str) -> Dict[str, List[str]]:
    slim = pd.read_parquet(latest, columns=["date", "code", "market"])
    d = pd.to_datetime(slim["date"], errors="coerce")
    mx = datetime.strptime(latest_ymd, "%Y%m%d").date()
    mask = d.dt.date == mx
    sl = slim.loc[mask, ["code", "market"]].copy()

    out: Dict[str, List[str]] = {}
    for mkt in MARKETS:
        codes = (
            sl.loc[sl["market"].astype(str) == mkt, "code"]
            .astype(str)
            .str.zfill(6)
            .dropna()
            .drop_duplicates()
            .tolist()
        )
        out[mkt] = codes
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Incrementally add missing KRX daily data into a new *_clean.parquet.")
    ap.add_argument("--base", "--base-dir", default=str(BASE_DIR))
    ap.add_argument("--end", default=None)
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--probe-cap", type=int, default=50)
    ap.add_argument("--min-uni", type=int, default=2000)
    ap.add_argument("--coverage", type=float, default=0.90)
    args = ap.parse_args()

    base = Path(args.base)
    if not base.exists():
        raise SystemExit(f"[FATAL] base not found: {base}")

    end_ymd = _yyyymmdd(args.end) if args.end else _default_end_yyyymmdd()
    min_uni = max(1, int(args.min_uni))

    clean_files = _find_clean_parquets(base)
    if not clean_files:
        raise SystemExit(f"[FATAL] no krx_daily_*_clean.parquet under base={base}")

    cand = []
    for p in clean_files:
        dm = _parquet_date_max(p)
        if not dm:
            continue
        n = 0
        try:
            sl = pd.read_parquet(p, columns=["date", "code"])
            d = pd.to_datetime(sl["date"], errors="coerce")
            if len(sl) > 0 and not d.isna().all():
                mx = d.max()
                n = int(sl.loc[d == mx, "code"].astype(str).nunique())
        except Exception:
            n = 0
        cand.append((p, dm, n, p.stat().st_mtime))

    if not cand:
        raise SystemExit(f"[FATAL] cannot read date_max from any clean parquet under base={base}")

    good = [t for t in cand if t[2] >= min_uni]
    pick = max(good, key=lambda t: (t[1], t[2], t[3])) if good else max(cand, key=lambda t: (t[1], t[2], t[3]))
    latest, prev_max, prev_ncode, _ = pick

    if prev_ncode < min_uni:
        raise SystemExit(
            f"[FATAL] latest clean universe degraded (ncode={prev_ncode} < MIN_UNI={min_uni}). "
            f"abort to avoid partial update. latest={latest}"
        )

    schema_cols, date_type_hint = _parquet_schema_cols(latest)
    codes_by_market = _market_code_pool(latest, prev_max)

    total_prev_codes = max(1, sum(len(v) for v in codes_by_market.values()))
    market_min: Dict[str, int] = {}
    for mkt in MARKETS:
        cnt = len(codes_by_market.get(mkt, []))
        ratio = cnt / total_prev_codes if total_prev_codes else 0
        need = max(1, int(min_uni * ratio * 0.85))
        market_min[mkt] = min(need, cnt) if cnt > 0 else 0

    start_dt = datetime.strptime(prev_max, "%Y%m%d").date() + timedelta(days=1)
    end_dt = datetime.strptime(end_ymd, "%Y%m%d").date()
    if start_dt > end_dt:
        print(f"[OK] nothing to do (prev_max={prev_max} >= end={end_ymd})")
        return 0

    cal = None
    try:
        import exchange_calendars as xc  # type: ignore

        cal = xc.get_calendar("XKRX")
    except Exception:
        cal = None

    all_frames = []
    fetched_days: List[str] = []
    skipped_non_session: List[str] = []

    cur = start_dt
    while cur <= end_dt:
        ymd = cur.strftime("%Y%m%d")

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

            norm = _normalize_by_ticker_df(ymd, mkt, df) if df is not None else pd.DataFrame()

            if norm.empty:
                if cal is not None:
                    try:
                        if not cal.is_session(ymd):
                            continue
                    except Exception:
                        pass

                pool = codes_by_market.get(mkt, [])
                if not pool:
                    raise SystemExit(f"[FATAL] empty code pool for {mkt}. cannot fallback on {ymd}.")

                best_df = pd.DataFrame()
                best_n = 0
                best_cap = 0
                target_market = max(1, market_min.get(mkt, 1))

                for cap in _fallback_caps(len(pool), int(args.probe_cap)):
                    subset = pool[:cap]
                    fb = _fetch_day_by_date_codes(ymd, mkt, subset)
                    n_fb = int(fb["code"].astype(str).nunique()) if (fb is not None and not fb.empty and "code" in fb.columns) else 0
                    min_cov = max(1, int(cap * float(args.coverage)))

                    print(
                        f"[FALLBACK] {ymd} {mkt} cap={cap} ok={n_fb} min_cov={min_cov} market_need={target_market}",
                        flush=True,
                    )

                    if n_fb > best_n:
                        best_df = fb
                        best_n = n_fb
                        best_cap = cap

                    if n_fb >= min_cov and n_fb >= target_market:
                        norm = fb
                        used_fallback = True
                        break

                if norm.empty:
                    raise SystemExit(
                        f"[FATAL] by_ticker unavailable and fallback insufficient for {ymd} {mkt}. "
                        f"best_ok={best_n} best_cap={best_cap} market_need={target_market}. abort."
                    )

            if not norm.empty:
                day_frames.append(norm)

        if not day_frames:
            print(f"[SKIP] {ymd} (no data)")
            cur += timedelta(days=1)
            continue

        day_df = pd.concat(day_frames, ignore_index=True)
        n_day = int(day_df["code"].astype(str).nunique()) if "code" in day_df.columns else 0
        if n_day < min_uni:
            raise SystemExit(
                f"[FATAL] universe degraded on {ymd} (ncode={n_day} < MIN_UNI={min_uni}). "
                "abort (fail-closed, no write)."
            )

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
            "If today is a holiday/early run, try --end with an earlier yyyymmdd."
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
