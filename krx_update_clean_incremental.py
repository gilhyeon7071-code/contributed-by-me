from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import json


def _add_local_python_site_packages() -> None:
    userprofile = os.environ.get("USERPROFILE", "")
    major_minor = f"Python{sys.version_info.major}{sys.version_info.minor}"
    candidates = []
    if userprofile:
        candidates.append(Path(userprofile) / "AppData" / "Local" / "Programs" / "Python" / major_minor / "Lib" / "site-packages")
        candidates.append(Path(userprofile) / "AppData" / "Roaming" / "Python" / major_minor / "site-packages")
    for p in reversed(candidates):
        s = str(p)
        if p.exists() and s not in sys.path:
            sys.path.insert(0, s)


_add_local_python_site_packages()

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

from holiday_manager import HolidayManager

from tools.price_integrity import (
    build_prev_close_map,
    decide_corporate_action_repair,
    detect_day_price_issues,
    norm_ymd8,
)


BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "2_Logs"
MARKETS = ["KOSPI", "KOSDAQ"]
LOG_DIR.mkdir(parents=True, exist_ok=True)
TRADABILITY_CONTRACT_COLUMNS = [
    "tradability_checked",
    "tradability_blocked",
    "tradability_reason",
    "tradability_source",
]

K_OPEN = "\uc2dc\uac00"
K_HIGH = "\uace0\uac00"
K_LOW = "\uc800\uac00"
K_CLOSE = "\uc885\uac00"
K_VOLUME = "\uac70\ub798\ub7c9"
K_CHANGE_RATE = "\ub4f1\ub77d\ub960"


def _safe_print(*args, **kwargs) -> None:
    try:
        print(*args, **kwargs)
    except (BrokenPipeError, OSError):
        try:
            sys.stdout = open("NUL", "w", encoding="utf-8")
        except Exception:
            pass


def _yyyymmdd(s: str) -> str:
    return str(s).replace("-", "").replace("/", "")[:8]


def _default_end_yyyymmdd() -> str:
    hm = HolidayManager()
    today_d = datetime.now().date()
    today_ymd = today_d.strftime("%Y%m%d")
    now_local = datetime.now()
    after_close = (now_local.hour > 15) or (now_local.hour == 15 and now_local.minute >= 40)
    status = hm.explain(today_ymd)
    if status.is_open:
        if after_close:
            return today_ymd
        prev = hm.previous_trading_day(today_ymd)
    else:
        prev = hm.previous_trading_day(today_ymd, include_target=True)
    if prev:
        return prev
    try:
        import exchange_calendars as xc
        import pandas as _pd

        cal = xc.get_calendar("XKRX")
        today = _pd.Timestamp(today_d)
        if cal.is_session(today):
            if after_close:
                prev_ts = today
            else:
                prev_ts = cal.previous_session(today)
        else:
            prev_ts = cal.date_to_session(today, direction="previous")
        return _pd.Timestamp(prev_ts).strftime("%Y%m%d")
    except Exception:
        d = today_d - timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d.strftime("%Y%m%d")


def _find_clean_parquets(root: Path) -> List[Path]:
    search_dirs = [root, root / "krx_daily_archive"]
    extra_dirs = os.environ.get("KRX_CLEAN_SEARCH_DIRS", "").strip()
    if extra_dirs:
        for raw in extra_dirs.split(os.pathsep):
            raw = raw.strip()
            if raw:
                p = Path(raw)
                search_dirs.append(p if p.is_absolute() else root / p)

    out: List[Path] = []
    seen: set[str] = set()
    for d in search_dirs:
        if not d.exists() or not d.is_dir():
            continue
        for p in d.glob("krx_daily_*_clean.parquet"):
            key = str(p.resolve()).lower()
            if key not in seen:
                seen.add(key)
                out.append(p)
    return sorted(out)


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

    x["value"] = pd.to_numeric(x["close"], errors="coerce").astype("float64") * pd.to_numeric(x["volume"], errors="coerce").astype("float64")

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
    seen_codes: set[str] = set()
    retry_codes: List[str] = []
    n = len(codes)
    for i, code in enumerate(codes, start=1):
        if i == 1 or i % 50 == 0 or i == n:
            _safe_print(f"[PROBE] {ymd} {mkt} {i}/{n} ok={len(recs)}", flush=True)

        code6 = str(code).zfill(6)
        try:
            dft = stock.get_market_ohlcv_by_date(ymd, ymd, code6)
        except Exception:
            retry_codes.append(code6)
            continue
        if dft is None or dft.empty:
            retry_codes.append(code6)
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
        seen_codes.add(code6)

    retry_codes = [c for c in retry_codes if c not in seen_codes]
    if retry_codes:
        _safe_print(f"[PROBE_RETRY] {ymd} {mkt} retry_missing={len(retry_codes)}", flush=True)
    for code6 in retry_codes:
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
        seen_codes.add(code6)

    if not recs:
        return pd.DataFrame()

    out = pd.DataFrame.from_records(recs)
    out["date"] = out["date"].astype(str).str.replace("-", "", regex=False).str[:8]
    out["code"] = out["code"].astype(str).str.zfill(6)
    for c in ["open", "high", "low", "close", "volume", "change_rate"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out["value"] = pd.to_numeric(out["close"], errors="coerce").astype("float64") * pd.to_numeric(out["volume"], errors="coerce").astype("float64")
    out = out.dropna(subset=["date", "code", "open", "high", "low", "close"])
    return out


def _paper_history_code_pool() -> List[str]:
    path = BASE_DIR / "paper" / "prices" / "ohlcv_paper.parquet"
    if not path.exists():
        return []
    try:
        hist = pd.read_parquet(path, columns=["code"])
    except Exception:
        return []
    if hist.empty or "code" not in hist.columns:
        return []
    return (
        hist["code"]
        .astype(str)
        .str.zfill(6)
        .dropna()
        .drop_duplicates()
        .tolist()
    )


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


def _add_tradability_contract_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["tradability_checked"] = True
    out["tradability_blocked"] = False
    out["tradability_reason"] = ""
    out["tradability_source"] = "krx_price_integrity_clean_filter"
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


def _all_markets_provider_empty(
    ymd: str, codes_by_market: Dict[str, List[str]], sample_size: int = 3
) -> bool:
    checked = 0
    for mkt in MARKETS:
        for code in codes_by_market.get(mkt, [])[: max(1, sample_size)]:
            checked += 1
            try:
                dft = stock.get_market_ohlcv_by_date(ymd, ymd, str(code).zfill(6))
            except Exception:
                continue
            if dft is not None and not dft.empty:
                return False
    return checked > 0


def _safe_write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_latest_effective_counts() -> Dict[str, int]:
    status_path = LOG_DIR / "krx_price_integrity_status_latest.json"
    if not status_path.exists():
        return {}
    try:
        status = json.loads(status_path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    effective_by_date: Dict[str, int] = {}
    for row in status.get("soft_skipped_days") or []:
        ymd = str(row.get("date") or "").strip()
        if len(ymd) != 8:
            continue
        try:
            effective_by_date[ymd] = int(row.get("effective_ncode") or 0)
        except Exception:
            continue
    return effective_by_date


def _is_tolerated_zero_ohlc_block(row: Dict[str, object]) -> bool:
    reasons = {str(x) for x in (row.get("reasons") or [])}
    if not reasons.issubset({"non_positive_price", "close_outside_range"}):
        return False
    try:
        open_px = float(row.get("open") or 0.0)
        high_px = float(row.get("high") or 0.0)
        low_px = float(row.get("low") or 0.0)
        volume = float(row.get("volume") or 0.0)
        close_px = float(row.get("close") or 0.0)
    except Exception:
        return False
    return open_px == 0.0 and high_px == 0.0 and low_px == 0.0 and volume == 0.0 and close_px > 0.0


def _dynamic_universe_floor(min_uni: int, reference_ncode: int, hard_floor: int, max_drop_pct: float) -> int:
    min_uni = max(1, int(min_uni))
    reference_ncode = max(1, int(reference_ncode or 0))
    hard_floor = max(1, int(hard_floor or 1))
    max_drop_pct = max(0.0, min(0.95, float(max_drop_pct)))
    ratio_floor = int(reference_ncode * (1.0 - max_drop_pct))
    return max(hard_floor, min(min_uni, ratio_floor))


def _universe_policy_status(
    effective_ncode: int,
    min_uni: int,
    reference_ncode: int,
    hard_floor: int,
    max_drop_pct: float,
) -> Dict[str, object]:
    dynamic_floor = _dynamic_universe_floor(min_uni, reference_ncode, hard_floor, max_drop_pct)
    effective_ncode = int(effective_ncode or 0)
    reference_ncode = int(reference_ncode or 0)
    drop_pct = None
    if reference_ncode > 0:
        drop_pct = max(0.0, 1.0 - (float(effective_ncode) / float(reference_ncode)))
    return {
        "min_uni": int(min_uni),
        "hard_floor": int(hard_floor),
        "max_drop_pct": float(max_drop_pct),
        "reference_ncode": reference_ncode,
        "dynamic_floor": int(dynamic_floor),
        "effective_ncode": effective_ncode,
        "drop_pct": drop_pct,
        "pass": bool(effective_ncode >= dynamic_floor),
    }


def _fetch_adjusted_window(code: str, start_ymd: str, end_ymd: str, market: str) -> pd.DataFrame:
    try:
        raw = stock.get_market_ohlcv_by_date(start_ymd, end_ymd, code, adjusted=True)
    except TypeError:
        raw = stock.get_market_ohlcv_by_date(start_ymd, end_ymd, code)
    except Exception:
        return pd.DataFrame()
    if raw is None or raw.empty:
        return pd.DataFrame()
    out = raw.reset_index().copy()
    first = out.columns[0]
    out = out.rename(columns={first: "date"})
    out["date"] = out["date"].astype(str).map(norm_ymd8)
    out["code"] = str(code).zfill(6)
    out["market"] = str(market or "")
    out["open"] = _series_from_candidates(out, ["open", "Open", K_OPEN])
    out["high"] = _series_from_candidates(out, ["high", "High", K_HIGH])
    out["low"] = _series_from_candidates(out, ["low", "Low", K_LOW])
    out["close"] = _series_from_candidates(out, ["close", "Close", K_CLOSE])
    out["volume"] = _series_from_candidates(out, ["volume", "Volume", K_VOLUME])
    out["change_rate"] = _series_from_candidates(out, ["change_rate", "change", "Change", K_CHANGE_RATE])
    out["value"] = pd.to_numeric(out["close"], errors="coerce").astype("float64") * pd.to_numeric(out["volume"], errors="coerce").astype("float64")
    keep = ["date", "code", "market", "open", "high", "low", "close", "volume", "value", "change_rate"]
    out = out[keep].copy()
    for c in ["open", "high", "low", "close", "volume", "value", "change_rate"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out = out.dropna(subset=["date", "code", "open", "high", "low", "close"])
    return out[out["date"].isin([start_ymd, end_ymd])].copy()


def _market_code_pool(latest: Path, latest_ymd: str) -> Dict[str, List[str]]:
    try:
        slim = pd.read_parquet(latest, columns=["date", "code", "market"])
    except Exception:
        slim = pd.read_parquet(latest, columns=["date", "code"])
        market_map: Dict[str, str] = {}
        try:
            status = json.loads((LOG_DIR / "krx_price_integrity_status_latest.json").read_text(encoding="utf-8"))
            source_rels = []
            for key in ("out_path", "latest_source"):
                rel = str(status.get(key) or "").strip()
                if rel and rel not in source_rels:
                    source_rels.append(rel)
            src_mx = datetime.strptime(latest_ymd, "%Y%m%d").date()
            for source_rel in source_rels:
                source_path = BASE_DIR / source_rel
                if not source_path.exists():
                    continue
                try:
                    src = pd.read_parquet(source_path, columns=["date", "code", "market"])
                except Exception:
                    continue
                src_d = pd.to_datetime(src["date"], errors="coerce")
                src = src.loc[src_d.dt.date == src_mx, ["code", "market"]].copy()
                if src.empty:
                    continue
                market_map = {
                    str(c).zfill(6): str(m)
                    for c, m in zip(src["code"], src["market"])
                }
                if market_map:
                    break
        except Exception:
            market_map = {}
        if not market_map:
            market_map = _historical_market_map(latest_ymd, latest)
        if not market_map:
            try:
                for mkt in MARKETS:
                    codes = stock.get_market_ticker_list(latest_ymd, market=mkt) or []
                    for code in codes:
                        market_map[str(code).zfill(6)] = mkt
            except Exception:
                market_map = market_map or {}
        slim["market"] = slim["code"].astype(str).str.zfill(6).map(market_map).fillna("")
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
        try:
            live_codes = stock.get_market_ticker_list(latest_ymd, market=mkt) or []
        except Exception:
            live_codes = []
        for code in live_codes:
            code6 = str(code).zfill(6)
            if code6 not in codes:
                codes.append(code6)
        out[mkt] = codes
    return out


def _market_master_map(latest_ymd: str = "", latest: Optional[Path] = None) -> Dict[str, str]:
    """code -> KOSPI/KOSDAQ 마스터.

    [2026-08-21] 아래 보충 수집이 시장을 `"UNKNOWN"` 리터럴로 박아 넣는 바람에
    clean parquet 의 market 컬럼이 2026-05 부터 무너졌다(2026-08-20 기준 51.8%가 UNKNOWN).
    데이터가 없어진 게 아니라 수집 경로가 값을 버린 것이다 - 과거 파일에는 온전히 남아 있고,
    그것으로 오늘의 UNKNOWN 1,337건이 100% 복구된다(실측).

    1순위: `2_Logs/market_master_latest.json` (tools/build_market_master.py 산출물)
    2순위: 기존 `_historical_market_map()` (과거 clean parquet 직접 스캔)
    둘 다 비면 빈 dict 를 준다 - 그 경우 예전과 똑같이 UNKNOWN 이 남는다.
    상세: .agent/PLANS.md 2026-08-21 (11)
    """
    try:
        master_path = LOG_DIR / "market_master_latest.json"
        if master_path.exists():
            doc = json.loads(master_path.read_text(encoding="utf-8-sig"))
            codes = doc.get("codes")
            if isinstance(codes, dict) and codes:
                return {
                    str(k).zfill(6): str(v).upper()
                    for k, v in codes.items()
                    if str(v).upper() in set(MARKETS)
                }
    except Exception as exc:
        _safe_print(f"[MARKET_MASTER] read failed: {type(exc).__name__}: {exc}")
    if latest_ymd and latest is not None:
        try:
            return _historical_market_map(latest_ymd, latest)
        except Exception:
            return {}
    return {}


def _fill_market_from_master(df: pd.DataFrame, master: Dict[str, str]) -> Tuple[pd.DataFrame, int]:
    """market 이 유효하지 않은 행만 마스터로 채운다. 이미 값이 있으면 건드리지 않는다."""
    if df is None or df.empty or "market" not in df.columns or not master:
        return df, 0
    codes = df["code"].astype(str).str.zfill(6)
    cur = df["market"].astype(str).str.upper().str.strip()
    need = ~cur.isin(set(MARKETS))
    if not bool(need.any()):
        return df, 0
    filled = codes.where(need).map(master)
    hit = need & filled.notna()
    if bool(hit.any()):
        df.loc[hit, "market"] = filled[hit]
    return df, int(hit.sum())


def _historical_market_map(latest_ymd: str, latest: Path) -> Dict[str, str]:
    """Recover code->market from older clean parquet files when the latest file lacks market."""
    out: Dict[str, str] = {}
    candidates: List[Tuple[str, Path]] = []
    for p in _find_clean_parquets(BASE_DIR):
        if p == latest:
            continue
        parts = {part.lower() for part in p.parts}
        if "backup" in parts or "_pytest_tmp" in parts or "tmp" in parts:
            continue
        try:
            cols, _date_type = _parquet_schema_cols(p)
        except Exception:
            continue
        if "date" not in cols or "code" not in cols or "market" not in cols:
            continue
        dm = _parquet_date_max(p)
        if not dm or dm > latest_ymd:
            continue
        candidates.append((dm, p))

    for _dm, p in sorted(candidates, reverse=True):
        try:
            slim = pd.read_parquet(p, columns=["date", "code", "market"])
        except Exception:
            continue
        d = pd.to_datetime(slim["date"], errors="coerce")
        if d.isna().all():
            continue
        mx = d.max()
        sl = slim.loc[d == mx, ["code", "market"]].copy()
        sl["market"] = sl["market"].astype(str).str.strip()
        sl = sl.loc[sl["market"].isin(MARKETS)]
        for code, market in zip(sl["code"], sl["market"]):
            code6 = str(code).zfill(6)
            if code6 not in out:
                out[code6] = str(market)
        if all(mkt in set(out.values()) for mkt in MARKETS):
            break
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Incrementally add missing KRX daily data into a new *_clean.parquet.")
    ap.add_argument("--base", "--base-dir", default=str(BASE_DIR))
    ap.add_argument("--end", default=None)
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--probe-cap", type=int, default=50)
    ap.add_argument("--min-uni", type=int, default=2000)
    ap.add_argument("--coverage", type=float, default=0.90)
    ap.add_argument("--min-uni-hard-floor", type=int, default=1600)
    ap.add_argument("--min-uni-max-drop-pct", type=float, default=0.10)
    args = ap.parse_args()

    base = Path(args.base)
    if not base.exists():
        raise SystemExit(f"[FATAL] base not found: {base}")

    end_ymd = _yyyymmdd(args.end) if args.end else _default_end_yyyymmdd()
    min_uni = max(1, int(args.min_uni))
    min_uni_hard_floor = max(1, int(args.min_uni_hard_floor))
    min_uni_max_drop_pct = max(0.0, min(0.95, float(args.min_uni_max_drop_pct)))

    clean_files = _find_clean_parquets(base)
    if not clean_files:
        raise SystemExit(f"[FATAL] no krx_daily_*_clean.parquet under base={base}")

    effective_by_date = _load_latest_effective_counts()
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
        effective_n = max(n, int(effective_by_date.get(dm, 0)))
        cand.append((p, dm, n, effective_n, p.stat().st_mtime))

    if not cand:
        raise SystemExit(f"[FATAL] cannot read date_max from any clean parquet under base={base}")

    good = [t for t in cand if t[3] >= min_uni]
    pick = max(good, key=lambda t: (t[1], t[3], t[2], t[4])) if good else max(cand, key=lambda t: (t[1], t[3], t[2], t[4]))
    latest, prev_max, prev_ncode, prev_effective_ncode, _ = pick

    prev_policy = _universe_policy_status(
        effective_ncode=prev_effective_ncode,
        min_uni=min_uni,
        reference_ncode=min_uni,
        hard_floor=min_uni_hard_floor,
        max_drop_pct=min_uni_max_drop_pct,
    )
    if not bool(prev_policy.get("pass")):
        raise SystemExit(
            f"[FATAL] latest clean universe degraded (ncode_effective={prev_effective_ncode} clean={prev_ncode} "
            f"< dynamic_floor={prev_policy.get('dynamic_floor')} MIN_UNI={min_uni}). "
            f"abort to avoid partial update. latest={latest}"
        )

    schema_cols, date_type_hint = _parquet_schema_cols(latest)
    if "market" not in schema_cols:
        schema_cols.append("market")
    for col in TRADABILITY_CONTRACT_COLUMNS:
        if col not in schema_cols:
            schema_cols.append(col)
    codes_by_market = _market_code_pool(latest, prev_max)
    prev_close_map = {}
    try:
        prev_hist = pd.read_parquet(latest, columns=["date", "code", "close"])
        prev_close_map = build_prev_close_map(prev_hist)
    except Exception:
        prev_close_map = {}

    total_prev_codes = max(1, sum(len(v) for v in codes_by_market.values()))
    reference_tolerated_gap = max(0, int(prev_effective_ncode) - int(total_prev_codes))
    market_min: Dict[str, int] = {}
    market_targets: List[Tuple[str, int, float]] = []
    target_total = min(int(min_uni), int(total_prev_codes))
    assigned_total = 0
    for mkt in MARKETS:
        cnt = len(codes_by_market.get(mkt, []))
        ratio = cnt / total_prev_codes if total_prev_codes else 0
        raw_need = float(target_total) * ratio
        need = min(int(raw_need), cnt) if cnt > 0 else 0
        market_min[mkt] = need
        assigned_total += need
        market_targets.append((mkt, cnt, raw_need - int(raw_need)))
    remaining = max(0, int(target_total) - int(assigned_total))
    for mkt, cnt, _frac in sorted(market_targets, key=lambda x: x[2], reverse=True):
        if remaining <= 0:
            break
        room = max(0, int(cnt) - int(market_min.get(mkt, 0)))
        add = min(room, remaining)
        if add > 0:
            market_min[mkt] = int(market_min.get(mkt, 0)) + int(add)
            remaining -= int(add)

    start_dt = datetime.strptime(prev_max, "%Y%m%d").date() + timedelta(days=1)
    end_dt = datetime.strptime(end_ymd, "%Y%m%d").date()
    if start_dt > end_dt:
        _safe_print(f"[OK] nothing to do (prev_max={prev_max} >= end={end_ymd})")
        return 0

    holiday_manager = HolidayManager()
    cal = None
    try:
        import exchange_calendars as xc  # type: ignore

        cal = xc.get_calendar("XKRX")
    except Exception:
        cal = None

    all_frames = []
    fetched_days: List[str] = []
    skipped_non_session: List[Dict[str, str]] = []
    soft_skipped_days: List[Dict[str, object]] = []
    blocked_rows: List[Dict[str, object]] = []
    spike_events: List[Dict[str, object]] = []
    repair_events: List[Dict[str, object]] = []
    repair_frames: List[pd.DataFrame] = []

    cur = start_dt
    while cur <= end_dt:
        ymd = cur.strftime("%Y%m%d")

        market_status = holiday_manager.explain(ymd)
        if not market_status.is_open:
            skipped_non_session.append({"date": ymd, "reason": market_status.reason})
            _safe_print(f"[SKIP] {ymd} ({market_status.reason})")
            cur += timedelta(days=1)
            continue

        if cal is not None:
            try:
                if not cal.is_session(ymd):
                    skipped_non_session.append({"date": ymd, "reason": "XKRX_NON_SESSION"})
                    _safe_print(f"[SKIP] {ymd} (non-session)")
                    cur += timedelta(days=1)
                    continue
            except Exception:
                pass

        day_frames = []
        used_fallback = False
        day_provider_empty_skip = False

        for mkt in MARKETS:
            df = None
            try:
                df = stock.get_market_ohlcv_by_ticker(ymd, market=mkt)
            except Exception as e:
                _safe_print(f"[WARN] pykrx by_ticker failed {ymd} {mkt}: {type(e).__name__}: {e}")

            norm = _normalize_by_ticker_df(ymd, mkt, df) if df is not None else pd.DataFrame()

            if norm.empty:
                if not holiday_manager.explain(ymd).is_open:
                    continue
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

                    _safe_print(
                        f"[FALLBACK] {ymd} {mkt} cap={cap} ok={n_fb} min_cov={min_cov} market_need={target_market}",
                        flush=True,
                    )

                    if n_fb > best_n:
                        best_df = fb
                        best_n = n_fb
                        best_cap = cap

                    enough_for_probe = n_fb >= min_cov
                    enough_for_market = n_fb >= target_market
                    probed_past_market_need = cap >= target_market
                    final_cap_reached = cap >= len(pool)

                    if enough_for_probe and (enough_for_market or probed_past_market_need or final_cap_reached):
                        norm = fb
                        used_fallback = True
                        break

                if norm.empty:
                    if best_n == 0 and _all_markets_provider_empty(ymd, codes_by_market):
                        soft_skipped_days.append(
                            {
                                "date": ymd,
                                "raw_ncode": 0,
                                "clean_ncode": 0,
                                "effective_ncode": 0,
                                "reason": "provider_returned_no_rows_all_markets",
                                "universe_policy": {
                                    "pass": True,
                                    "basis": "no_new_write_provider_empty",
                                },
                            }
                        )
                        _safe_print(
                            f"[SOFT_SKIP] {ymd} provider returned no rows for all market samples; "
                            f"keep latest clean prev_max={prev_max}"
                        )
                        day_provider_empty_skip = True
                        break
                    raise SystemExit(
                        f"[FATAL] by_ticker unavailable and fallback insufficient for {ymd} {mkt}. "
                        f"best_ok={best_n} best_cap={best_cap} market_need={target_market}. abort."
                    )

            if not norm.empty:
                day_frames.append(norm)

        if day_provider_empty_skip:
            cur += timedelta(days=1)
            continue

        if not day_frames:
            _safe_print(f"[SKIP] {ymd} (no data)")
            cur += timedelta(days=1)
            continue

        day_df = pd.concat(day_frames, ignore_index=True)
        raw_n_day = int(day_df["code"].astype(str).nunique()) if "code" in day_df.columns else 0
        history_codes = _paper_history_code_pool()
        if raw_n_day < min_uni and history_codes and "code" in day_df.columns:
            seen_day_codes = set(day_df["code"].astype(str).str.zfill(6))
            supplemental_codes = [code for code in history_codes if code not in seen_day_codes]
            if supplemental_codes:
                supplemental = _fetch_day_by_date_codes(ymd, "UNKNOWN", supplemental_codes)
                if supplemental is not None and not supplemental.empty:
                    # [2026-08-21] 보충 경로가 박아 넣은 "UNKNOWN" 을 마스터로 되돌린다.
                    #
                    # **기본 OFF 다.** 켜면 신규 행만 올바른 market 을 갖고 과거 파일은 UNKNOWN 인 채로
                    # 남아, generate_candidates 의 peer-group 집계에서 한 종목의 라벨이 패널 중간에
                    # 바뀐다. KOSPI 그룹 구성이 516종목 -> 1,853종목으로 튀면서 m_ret_20(20일)과
                    # market_is_bull(60일 MA)이 구성 변화만으로 왜곡된다.
                    # 소비자를 market_resolved 로 옮기는 결정이 먼저다. 그 전에 켜면 안 된다.
                    # 상세: .agent/PLANS.md 2026-08-21 (11)
                    if str(os.environ.get("KRX_FILL_MARKET_FROM_MASTER", "0")).strip().lower() in {"1", "true", "yes", "on"}:
                        supplemental, _mkt_filled = _fill_market_from_master(supplemental, _market_master_map())
                        if _mkt_filled:
                            _safe_print(f"[MARKET_MASTER] {ymd} supplemental market filled={_mkt_filled}")
                    day_df = pd.concat([day_df, supplemental], ignore_index=True)
                    _safe_print(
                        f"[SUPPLEMENT] {ymd} paper_history missing={len(supplemental_codes)} "
                        f"fetched={supplemental['code'].astype(str).nunique()}"
                    )
                raw_n_day = int(day_df["code"].astype(str).nunique()) if "code" in day_df.columns else 0
        day_df, blocked_df, spikes = detect_day_price_issues(day_df, prev_close_map)
        tolerated_blocked_codes: set[str] = set()
        if blocked_df is not None and not blocked_df.empty:
            blocked_rec = blocked_df.to_dict(orient="records")
            blocked_rows.extend(blocked_rec)
            tolerated_blocked_codes = {
                str(row.get("code") or "").zfill(6)
                for row in blocked_rec
                if _is_tolerated_zero_ohlc_block(row)
            }
        if spikes:
            for item in spikes:
                code = str(item.get("code") or "").zfill(6)
                prev_date = norm_ymd8(item.get("prev_date"))
                market = str(day_df.loc[day_df["code"] == code, "market"].iloc[0]) if not day_df.loc[day_df["code"] == code].empty else ""
                if len(prev_date) != 8 or not market:
                    spike_events.append({**item, "repair_applied": False, "repair_reason": "missing_prev_context"})
                    continue
                adj = _fetch_adjusted_window(code, prev_date, ymd, market)
                if adj.empty:
                    spike_events.append({**item, "repair_applied": False, "repair_reason": "adjusted_fetch_empty"})
                    continue
                prev_adj = adj.loc[adj["date"] == prev_date]
                cur_adj = adj.loc[adj["date"] == ymd]
                if prev_adj.empty or cur_adj.empty:
                    spike_events.append({**item, "repair_applied": False, "repair_reason": "adjusted_window_incomplete"})
                    continue
                decision = decide_corporate_action_repair(
                    prev_close_raw=item.get("prev_close"),
                    close_raw=item.get("raw_close"),
                    prev_close_adjusted=prev_adj["close"].iloc[-1],
                    close_adjusted=cur_adj["close"].iloc[-1],
                )
                spike_events.append(
                    {
                        **item,
                        "repair_applied": bool(decision.apply_adjusted),
                        "repair_reason": decision.reason,
                        "adjusted_move_abs": float(decision.adjusted_move_abs),
                    }
                )
                if not decision.apply_adjusted:
                    continue
                repair_frames.append(adj)
                repair_events.append(
                    {
                        "code": code,
                        "prev_date": prev_date,
                        "date": ymd,
                        "market": market,
                        "raw_move_abs": float(decision.raw_move_abs),
                        "adjusted_move_abs": float(decision.adjusted_move_abs),
                        "reason": decision.reason,
                    }
                )
                day_df = day_df.loc[day_df["code"] != code].copy()
                day_df = pd.concat([day_df, adj.loc[adj["date"] == ymd]], ignore_index=True)
        n_day = int(day_df["code"].astype(str).nunique()) if "code" in day_df.columns else 0
        effective_n_day = int(n_day + len(tolerated_blocked_codes))
        universe_policy = _universe_policy_status(
            effective_ncode=effective_n_day,
            min_uni=min_uni,
            reference_ncode=prev_effective_ncode,
            hard_floor=min_uni_hard_floor,
            max_drop_pct=min_uni_max_drop_pct,
        )
        full_pool_covered = n_day >= total_prev_codes and total_prev_codes > 0
        raw_pool_missing = max(0, int(total_prev_codes - raw_n_day))
        almost_full_pool_covered = total_prev_codes > 0 and raw_pool_missing <= 2
        adjusted_reference_gap = max(0, int(reference_tolerated_gap - raw_pool_missing))
        if used_fallback and (full_pool_covered or almost_full_pool_covered) and adjusted_reference_gap > 0:
            effective_n_day = max(
                effective_n_day,
                int(n_day + len(tolerated_blocked_codes) + adjusted_reference_gap),
            )
            universe_policy = _universe_policy_status(
                effective_ncode=effective_n_day,
                min_uni=min_uni,
                reference_ncode=prev_effective_ncode,
                hard_floor=min_uni_hard_floor,
                max_drop_pct=min_uni_max_drop_pct,
            )
        if (
            n_day < min_uni
            and effective_n_day >= min_uni
            and tolerated_blocked_codes
            and used_fallback
            and (full_pool_covered or almost_full_pool_covered)
            and adjusted_reference_gap > 0
        ):
            soft_skipped_days.append(
                {
                    "date": ymd,
                    "raw_ncode": raw_n_day,
                    "clean_ncode": n_day,
                    "effective_ncode": effective_n_day,
                    "tolerated_zero_ohlc_blocks": len(tolerated_blocked_codes),
                    "reference_tolerated_gap": int(adjusted_reference_gap),
                    "raw_pool_missing": int(raw_pool_missing),
                    "reason": "skip_write_due_tolerated_zero_ohlc_and_reference_gap",
                    "universe_policy": universe_policy,
                }
            )
            _safe_print(
                f"[SOFT_ALLOW] {ymd} raw={raw_n_day} clean={n_day} effective={effective_n_day} "
                f"tolerated_zero_ohlc={len(tolerated_blocked_codes)} "
                f"reference_tolerated_gap={adjusted_reference_gap} raw_pool_missing={raw_pool_missing} "
                "-> write clean snapshot"
            )
        elif n_day < min_uni and effective_n_day >= min_uni and tolerated_blocked_codes:
            soft_skipped_days.append(
                {
                    "date": ymd,
                    "raw_ncode": raw_n_day,
                    "clean_ncode": n_day,
                    "effective_ncode": effective_n_day,
                    "tolerated_zero_ohlc_blocks": len(tolerated_blocked_codes),
                    "reason": "skip_write_due_tolerated_zero_ohlc_deficit",
                    "universe_policy": universe_policy,
                }
            )
            _safe_print(
                f"[SOFT_ALLOW] {ymd} raw={raw_n_day} clean={n_day} effective={effective_n_day} "
                f"tolerated_zero_ohlc={len(tolerated_blocked_codes)} -> write clean snapshot"
            )
        elif n_day < min_uni and effective_n_day >= min_uni and used_fallback and (full_pool_covered or almost_full_pool_covered) and adjusted_reference_gap > 0:
            soft_skipped_days.append(
                {
                    "date": ymd,
                    "raw_ncode": raw_n_day,
                    "clean_ncode": n_day,
                    "effective_ncode": effective_n_day,
                    "tolerated_zero_ohlc_blocks": int(adjusted_reference_gap),
                    "reason": "skip_write_due_reference_tolerated_gap",
                    "raw_pool_missing": int(raw_pool_missing),
                    "universe_policy": universe_policy,
                }
            )
            _safe_print(
                f"[SOFT_ALLOW] {ymd} raw={raw_n_day} clean={n_day} effective={effective_n_day} "
                f"reference_tolerated_gap={adjusted_reference_gap} raw_pool_missing={raw_pool_missing} -> write clean snapshot"
            )
        if n_day < min_uni and bool(universe_policy.get("pass")) and not any(x.get("date") == ymd for x in soft_skipped_days):
            soft_skipped_days.append(
                {
                    "date": ymd,
                    "raw_ncode": raw_n_day,
                    "clean_ncode": n_day,
                    "effective_ncode": effective_n_day,
                    "tolerated_zero_ohlc_blocks": len(tolerated_blocked_codes),
                    "reason": "dynamic_universe_floor_allow",
                    "raw_pool_missing": int(raw_pool_missing),
                    "universe_policy": universe_policy,
                }
            )
            _safe_print(
                f"[SOFT_ALLOW] {ymd} raw={raw_n_day} clean={n_day} effective={effective_n_day} "
                f"dynamic_floor={universe_policy.get('dynamic_floor')} MIN_UNI={min_uni} "
                f"drop_pct={universe_policy.get('drop_pct')} -> write clean snapshot"
            )
        if not bool(universe_policy.get("pass")):
            raise SystemExit(
                f"[FATAL] universe degraded on {ymd} (ncode_effective={effective_n_day} raw={raw_n_day} clean={n_day} "
                f"< dynamic_floor={universe_policy.get('dynamic_floor')} MIN_UNI={min_uni}). "
                "abort (fail-closed, no write)."
            )

        all_frames.append(day_df)
        fetched_days.append(ymd)
        prev_close_map.update(build_prev_close_map(day_df[["date", "code", "close"]]))
        _safe_print(f"[OK] {ymd} fetched (fallback=by_date_probe)" if used_fallback else f"[OK] {ymd} fetched")
        cur += timedelta(days=1)

    if not all_frames:
        if skipped_non_session:
            status = {
                "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "status": "PASS",
                "latest_source": str(latest),
                "out_path": None,
                "range_from": start_dt.strftime("%Y%m%d"),
                "range_to": end_ymd,
                "fetched_days": [],
                "skipped_non_session": skipped_non_session,
                "soft_skipped_days": [],
                "blocked_rows": int(len(blocked_rows)),
                "spike_candidates": int(len(spike_events)),
                "repair_events": repair_events,
                "blocked_examples": blocked_rows[:20],
                "repair_backfill_rows": int(sum(len(x) for x in repair_frames)),
                "tolerated_zero_ohlc_blocks": int(
                    sum(1 for row in blocked_rows if _is_tolerated_zero_ohlc_block(row))
                ),
                "effective_reason": "no_trading_sessions",
                "source_calendar": "holiday_manager",
            }
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            _safe_write_json(LOG_DIR / "krx_price_integrity_status_latest.json", status)
            _safe_write_json(LOG_DIR / f"krx_price_integrity_status_{ts}.json", status)
            _safe_print(
                f"[OK] no trading sessions between {start_dt.strftime('%Y%m%d')} and {end_ymd}. "
                f"skipped_non_session={len(skipped_non_session)}"
            )
            return 0
        if soft_skipped_days:
            status = {
                "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "status": "PASS",
                "latest_source": str(latest),
                "out_path": None,
                "range_from": start_dt.strftime("%Y%m%d"),
                "range_to": end_ymd,
                "fetched_days": [],
                "soft_skipped_days": soft_skipped_days,
                "blocked_rows": int(len(blocked_rows)),
                "spike_candidates": int(len(spike_events)),
                "repair_events": repair_events,
                "blocked_examples": blocked_rows[:20],
                "repair_backfill_rows": int(sum(len(x) for x in repair_frames)),
                "tolerated_zero_ohlc_blocks": int(
                    sum(1 for row in blocked_rows if _is_tolerated_zero_ohlc_block(row))
                ),
            }
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            _safe_write_json(LOG_DIR / "krx_price_integrity_status_latest.json", status)
            _safe_write_json(LOG_DIR / f"krx_price_integrity_status_{ts}.json", status)
            _safe_print(
                f"[OK] no clean write needed (soft_skipped_days={len(soft_skipped_days)}) "
                "because all missing sessions were classified as soft-skips"
            )
            return 0
        raise SystemExit(
            f"[FATAL] fetched nothing between {start_dt.strftime('%Y%m%d')} and {end_ymd}. "
            "If today is a holiday/early run, try --end with an earlier yyyymmdd."
        )

    out_df = pd.concat(all_frames, ignore_index=True)
    if repair_frames:
        out_df = pd.concat([out_df, pd.concat(repair_frames, ignore_index=True)], ignore_index=True)
        out_df = out_df.drop_duplicates(subset=["date", "code"], keep="last")
    out_df = _add_tradability_contract_columns(out_df)
    out_df = _build_to_schema(out_df, schema_cols, date_type_hint)

    out_dir = Path(args.out_dir) if args.out_dir else latest.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    start_ymd = fetched_days[0]
    end_ymd2 = fetched_days[-1]
    out_path = out_dir / f"krx_daily_{start_ymd}_{end_ymd2}_clean.parquet"

    out_df.to_parquet(out_path, index=False, engine="pyarrow")
    _safe_print(f"[OK] wrote: {out_path}")
    _safe_print(f"[OK] days={len(fetched_days)} range={start_ymd}~{end_ymd2} rows={len(out_df)} schema_cols={len(schema_cols)}")
    status = {
        "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "status": "PASS",
        "latest_source": str(latest),
        "out_path": str(out_path),
        "range_from": start_ymd,
        "range_to": end_ymd2,
        "fetched_days": fetched_days,
        "soft_skipped_days": soft_skipped_days,
        "blocked_rows": int(len(blocked_rows)),
        "spike_candidates": int(len(spike_events)),
        "repair_events": repair_events,
        "blocked_examples": blocked_rows[:20],
        "repair_backfill_rows": int(sum(len(x) for x in repair_frames)),
        "tolerated_zero_ohlc_blocks": int(
            sum(1 for row in blocked_rows if _is_tolerated_zero_ohlc_block(row))
        ),
    }
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    _safe_write_json(LOG_DIR / "krx_price_integrity_status_latest.json", status)
    _safe_write_json(LOG_DIR / f"krx_price_integrity_status_{ts}.json", status)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
