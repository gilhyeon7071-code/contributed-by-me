from __future__ import annotations

from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Optional, List

import pandas as pd
import json
import os
import sys


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

try:
    from pykrx import stock
except Exception as e:
    raise SystemExit(f"[FATAL] pykrx import failed: {type(e).__name__}: {e}")

try:
    import requests

    _REQ_TIMEOUT_SEC = float(str(os.getenv("KRX_HTTP_TIMEOUT_SEC", "15")).strip() or "15")
    _REQ_ORIG = requests.sessions.Session.request

    def _request_with_default_timeout(self, method, url, **kwargs):
        kwargs.setdefault("timeout", _REQ_TIMEOUT_SEC)
        return _REQ_ORIG(self, method, url, **kwargs)

    requests.sessions.Session.request = _request_with_default_timeout
except Exception:
    pass

from tools.price_integrity import (
    build_prev_close_map,
    decide_corporate_action_repair,
    detect_day_price_issues,
    norm_ymd8,
)
from holiday_manager import HolidayManager

BASE = Path(__file__).resolve().parent
OUT = BASE / "paper" / "prices" / "ohlcv_paper.parquet"
LOG_DIR = BASE / "2_Logs"
CLEAN_DIR = BASE / "_krx_manual"
ARCHIVE_CLEAN_DIR = BASE / "krx_daily_archive"
OUT.parent.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

def expected_trading_date(today_d: date) -> date:
    # The local KRX holiday cache is the operational source of truth.  The
    # bundled exchange_calendars table can lag newly restored market holidays.
    holiday_calendar = HolidayManager()
    today8 = today_d.strftime("%Y%m%d")
    now_local = datetime.now()
    if holiday_calendar.is_market_open(today8):
        after_close = (now_local.hour > 15) or (now_local.hour == 15 and now_local.minute >= 40)
        if after_close:
            return today_d
    previous = holiday_calendar.previous_trading_day(today8)
    if previous:
        return datetime.strptime(previous, "%Y%m%d").date()
    try:
        import exchange_calendars as xc
        import pandas as _pd

        cal = xc.get_calendar("XKRX")
        today_ts = _pd.Timestamp(today_d)
        if cal.is_session(today_ts):
            after_close = (now_local.hour > 15) or (now_local.hour == 15 and now_local.minute >= 40)
            if after_close:
                return today_d
            return cal.previous_session(today_ts).date()
        return cal.date_to_session(today_ts, direction="previous").date()
    except Exception:
        return today_d

def prev_weekday_lag1(d: date) -> date:
    ymd = HolidayManager().previous_trading_day(d.strftime("%Y%m%d"))
    return datetime.strptime(ymd, "%Y%m%d").date()


def _next_trading_date(d: date) -> date:
    ymd = HolidayManager().next_trading_day(d.strftime("%Y%m%d"))
    return datetime.strptime(ymd, "%Y%m%d").date()


def _is_trading_ymd(ymd: str) -> bool:
    try:
        return bool(HolidayManager().is_market_open(str(ymd)))
    except Exception:
        return False

def norm8(d: date) -> str:
    return d.strftime("%Y%m%d")

def parquet_date_max(p: Path) -> Optional[str]:
    if not p.exists() or p.stat().st_size == 0:
        return None
    try:
        df = pd.read_parquet(p, columns=["date"])
        if df.empty:
            return None
        v = df["date"].dropna().max()
        if hasattr(v, "strftime"):
            return v.strftime("%Y%m%d")
        s = str(v)
        return s.replace("-", "")[:8]
    except Exception:
        return None


def _safe_write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _row_from_candidates(row: pd.Series, names: List[str], default=pd.NA):
    idx = {str(k).strip(): k for k in row.index}
    for n in names:
        if n in row.index:
            return row[n]
        key = idx.get(str(n).strip())
        if key is not None:
            return row[key]
    return default


def _fallback_code_pool(old: pd.DataFrame) -> List[str]:
    if old is None or old.empty or "code" not in old.columns:
        return []
    ref = old.copy()
    if "date" in ref.columns:
        try:
            latest = ref["date"].astype(str).max()
            ref = ref.loc[ref["date"].astype(str) == str(latest)].copy()
        except Exception:
            pass
    return (
        ref["code"]
        .astype(str)
        .str.zfill(6)
        .dropna()
        .drop_duplicates()
        .tolist()
    )


def _fetch_day_by_code_pool(ymd: str, codes: List[str]) -> pd.DataFrame:
    if not codes:
        return pd.DataFrame(columns=["date", "code", "open", "high", "low", "close", "volume", "value"])
    recs = []
    total = len(codes)
    for i, code in enumerate(codes, start=1):
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
                "open": _row_from_candidates(row, ["open", "Open", "\uc2dc\uac00"]),
                "high": _row_from_candidates(row, ["high", "High", "\uace0\uac00"]),
                "low": _row_from_candidates(row, ["low", "Low", "\uc800\uac00"]),
                "close": _row_from_candidates(row, ["close", "Close", "\uc885\uac00"]),
                "volume": _row_from_candidates(row, ["volume", "Volume", "\uac70\ub798\ub7c9"], 0),
            }
        )
        if i == 1 or i % 250 == 0 or i == total:
            print(f"[PRICES_FALLBACK] {ymd} {i}/{total} ok={len(recs)}", flush=True)
    if not recs:
        return pd.DataFrame(columns=["date", "code", "open", "high", "low", "close", "volume", "value"])
    out = pd.DataFrame.from_records(recs)
    for c in ["open", "high", "low", "close", "volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out["value"] = pd.to_numeric(out["close"], errors="coerce").astype("float64") * pd.to_numeric(out["volume"], errors="coerce").astype("float64")
    return out.dropna(subset=["date", "code", "open", "high", "low", "close"])


def _normalize_day_df(ymd: str, df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "code", "open", "high", "low", "close", "volume", "value"])
    out = df.reset_index().copy()
    first = out.columns[0]
    out = out.rename(columns={first: "code"})
    out["code"] = out["code"].astype(str).str.zfill(6)
    out["date"] = str(ymd)

    ren = {}
    for c in out.columns:
        if c in ("code", "date"):
            continue
        cl = str(c).lower()
        if cl in ("\uc2dc\uac00", "open"):
            ren[c] = "open"
        elif cl in ("\uace0\uac00", "high"):
            ren[c] = "high"
        elif cl in ("\uc800\uac00", "low"):
            ren[c] = "low"
        elif cl in ("\uc885\uac00", "close"):
            ren[c] = "close"
        elif cl in ("volume",):
            ren[c] = "volume"
        elif cl in ("value",):
            ren[c] = "value"
    out = out.rename(columns=ren)
    keep = [c for c in ["date", "code", "open", "high", "low", "close", "volume", "value"] if c in out.columns]
    out = out[keep].copy()
    required = {"date", "code", "open", "high", "low", "close"}
    if not required.issubset(set(out.columns)):
        return pd.DataFrame()
    for c in ["open", "high", "low", "close", "volume", "value"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    if "value" not in out.columns:
        out["value"] = pd.to_numeric(out.get("close"), errors="coerce").astype("float64") * pd.to_numeric(out.get("volume"), errors="coerce").astype("float64")
    out = out.dropna(subset=["date", "code", "open", "high", "low", "close"])
    return out


def _load_latest_clean_rows(target_dates: set[str]) -> pd.DataFrame:
    if not target_dates:
        return pd.DataFrame()
    files = []
    for clean_dir in (CLEAN_DIR, ARCHIVE_CLEAN_DIR):
        files.extend(sorted(clean_dir.glob("krx_daily_*_clean.parquet")))
    if not files:
        return pd.DataFrame()
    parts = []
    for p in files:
        if not any(d in p.name for d in target_dates):
            continue
        try:
            df = pd.read_parquet(p)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        cols = [c for c in ["date", "code", "open", "high", "low", "close", "volume", "value"] if c in df.columns]
        if len(cols) < 6:
            continue
        sl = df[cols].copy()
        sl["date"] = sl["date"].map(norm_ymd8)
        sl["code"] = sl["code"].astype(str).str.zfill(6)
        parts.append(sl)
    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True)
    return out.drop_duplicates(subset=["date", "code"], keep="last")


def _fetch_adjusted_window(code: str, start_ymd: str, end_ymd: str) -> pd.DataFrame:
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
    ren = {}
    for c in out.columns:
        cl = str(c).lower()
        if cl in ("\uc2dc\uac00", "open"):
            ren[c] = "open"
        elif cl in ("\uace0\uac00", "high"):
            ren[c] = "high"
        elif cl in ("\uc800\uac00", "low"):
            ren[c] = "low"
        elif cl in ("\uc885\uac00", "close"):
            ren[c] = "close"
        elif cl in ("volume",):
            ren[c] = "volume"
        elif cl in ("value",):
            ren[c] = "value"
    out = out.rename(columns=ren)
    keep = [c for c in ["date", "code", "open", "high", "low", "close", "volume", "value"] if c in out.columns]
    out = out[keep].copy()
    required = {"date", "code", "open", "high", "low", "close"}
    if not required.issubset(set(out.columns)):
        return pd.DataFrame()
    for c in ["open", "high", "low", "close", "volume", "value"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    if "value" not in out.columns:
        out["value"] = pd.to_numeric(out.get("close"), errors="coerce").astype("float64") * pd.to_numeric(out.get("volume"), errors="coerce").astype("float64")
    return out.dropna(subset=["date", "code", "open", "high", "low", "close"])

def main():
    today = date.today()
    end = expected_trading_date(today)
    end8 = norm8(end)

    last8 = parquet_date_max(OUT)
    if last8:
        start = _next_trading_date(datetime.strptime(last8, "%Y%m%d").date())
    else:
        start = end - timedelta(days=30)
        while start <= end and not _is_trading_ymd(norm8(start)):
            start += timedelta(days=1)

    if start > end:
        print(f"[PRICES] up-to-date: last={last8} end={end8}")
        return 0

    days: List[str] = []
    d = start
    while d <= end:
        ymd = norm8(d)
        if _is_trading_ymd(ymd):
            days.append(ymd)
        d += timedelta(days=1)

    if not days:
        print(f"[PRICES] up-to-date(no trading dates): last={last8} end={end8}")
        return 0

    print(f"[PRICES] target dates: {days[0]}..{days[-1]} n={len(days)} out={OUT}")

    old = pd.DataFrame()
    if OUT.exists() and OUT.stat().st_size > 0:
        try:
            old = pd.read_parquet(OUT)
        except Exception:
            old = pd.DataFrame()

    prev_close_map = build_prev_close_map(old)
    fallback_codes = _fallback_code_pool(old)
    parts = []
    blocked_rows = []
    no_data_days = []
    repair_rows = []
    repair_events = []
    spike_events = []
    for ymd in days:
        if not _is_trading_ymd(ymd):
            print(f"[PRICES] skip(non-trading day): {ymd}")
            continue
        day_parts = []
        markets = ["KOSPI", "KOSDAQ"]
        market_fail_count = 0
        for market in markets:
            df = None
            try:
                df = stock.get_market_ohlcv_by_ticker(ymd, market=market)
            except Exception as e:
                market_fail_count += 1
                print(f"[PRICES] by_ticker failed {ymd} {market}: {type(e).__name__}: {e}")
            norm = _normalize_day_df(ymd, df) if df is not None else pd.DataFrame()
            if not norm.empty:
                day_parts.append(norm)
        day_df = pd.concat(day_parts, ignore_index=True) if day_parts else pd.DataFrame()
        if not day_df.empty:
            day_df = day_df.drop_duplicates(subset=["date", "code"], keep="last")
        if day_df.empty:
            day_df = _load_latest_clean_rows({ymd})
            if not day_df.empty:
                print(f"[PRICES] fallback(clean parquet): {ymd} rows={len(day_df)}")
        if day_df.empty and fallback_codes and market_fail_count < len(markets):
            day_df = _fetch_day_by_code_pool(ymd, fallback_codes)
            if not day_df.empty:
                print(f"[PRICES] fallback(by_date_codes): {ymd} rows={len(day_df)}")
        if day_df.empty:
            no_data_days.append({"date": ymd, "market_fail_count": int(market_fail_count), "markets": markets})
            print(f"[PRICES] skip(no data): {ymd}")
            continue
        day_df, blocked_df, spikes = detect_day_price_issues(day_df, prev_close_map)
        if blocked_df is not None and not blocked_df.empty:
            blocked_rows.extend(blocked_df.to_dict(orient="records"))
        if spikes:
            for item in spikes:
                code = str(item.get("code") or "").zfill(6)
                prev_date = norm_ymd8(item.get("prev_date"))
                if len(prev_date) != 8:
                    spike_events.append({**item, "repair_applied": False, "repair_reason": "missing_prev_date"})
                    continue
                adj = _fetch_adjusted_window(code, prev_date, ymd)
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
                repl = adj.loc[adj["date"].isin([prev_date, ymd])].copy()
                if repl.empty:
                    continue
                repair_rows.append(repl)
                repair_events.append(
                    {
                        "code": code,
                        "prev_date": prev_date,
                        "date": ymd,
                        "raw_move_abs": float(decision.raw_move_abs),
                        "adjusted_move_abs": float(decision.adjusted_move_abs),
                        "reason": decision.reason,
                    }
                )
                day_df = day_df.loc[day_df["code"] != code].copy()
                day_df = pd.concat([day_df, repl.loc[repl["date"] == ymd]], ignore_index=True)
        if not day_df.empty:
            parts.append(day_df)
            prev_close_map.update(build_prev_close_map(day_df))

    if not parts:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        status = {
            "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "status": "FAIL",
            "reason": "no_price_parts_fetched",
            "date_max_before": last8,
            "target_dates": days,
            "no_data_days": no_data_days,
            "out": str(OUT),
        }
        _safe_write_json(LOG_DIR / "price_integrity_status_latest.json", status)
        _safe_write_json(LOG_DIR / f"price_integrity_status_{ts}.json", status)
        raise SystemExit("[FATAL] no price parts fetched (all holidays?)")

    new = pd.concat(parts, ignore_index=True)
    clean_overlay = _load_latest_clean_rows(set(new["date"].astype(str).unique().tolist()))
    if not clean_overlay.empty:
        new = pd.concat([new, clean_overlay], ignore_index=True).drop_duplicates(subset=["date", "code"], keep="last")

    merge_parts = [old, new]
    if repair_rows:
        merge_parts.append(pd.concat(repair_rows, ignore_index=True))
    merged = pd.concat(merge_parts, ignore_index=True).drop_duplicates(subset=["date", "code"], keep="last")

    merged.to_parquet(OUT, index=False)
    dm = merged["date"].astype(str).max()
    status = {
        "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "status": "PASS",
        "date_max": dm,
        "fetched_days": days,
        "rows_written": int(len(merged)),
        "blocked_rows": int(len(blocked_rows)),
        "no_data_days": no_data_days,
        "spike_candidates": int(len(spike_events)),
        "repair_applied_rows": int(sum(len(x) for x in repair_rows)),
        "repair_events": repair_events,
        "spike_events": spike_events,
        "blocked_examples": blocked_rows[:20],
        "clean_overlay_rows": int(len(clean_overlay)),
    }
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    _safe_write_json(LOG_DIR / "price_integrity_status_latest.json", status)
    _safe_write_json(LOG_DIR / f"price_integrity_status_{ts}.json", status)
    print(f"[PRICES] wrote: {OUT} rows={len(merged)} date_max={dm}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
