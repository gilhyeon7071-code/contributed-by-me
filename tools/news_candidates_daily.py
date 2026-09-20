from __future__ import annotations

import json
import math
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
DB = ROOT / "news_trading" / "data" / "trading.db"
DART_CORP_CODE_MAP = ROOT / "_cache" / "dart_corp_code_map.csv"

MARKET_RISING = LOGS / "market_rising_latest.csv"
SURGE_REALTIME = LOGS / "surge_realtime_latest.csv"
WITH_NEWS_SCORE = LOGS / "candidates_latest_data.with_news_score.csv"
OUT = LOGS / "news_candidates_latest.csv"
STATUS_LATEST = LOGS / "news_candidates_status_latest.json"

KST = timezone(timedelta(hours=9))


def _connect_news_db() -> sqlite3.Connection:
    timeout_sec = max(1.0, float(str(os.environ.get("NEWS_CANDIDATES_SQLITE_TIMEOUT_SEC", "30")).strip() or "30"))
    attempts = max(1, int(float(str(os.environ.get("NEWS_CANDIDATES_SQLITE_ATTEMPTS", "3")).strip() or "3")))
    last_exc: Exception | None = None
    uri = f"file:{DB.as_posix()}?mode=ro"
    for attempt in range(attempts):
        try:
            con = sqlite3.connect(uri, uri=True, timeout=timeout_sec)
            con.execute(f"PRAGMA busy_timeout={int(timeout_sec * 1000)}")
            con.execute("PRAGMA query_only=ON")
            return con
        except sqlite3.OperationalError as exc:
            last_exc = exc
            if "locked" not in str(exc).lower() or attempt >= attempts - 1:
                break
            time.sleep(min(2.0 * (attempt + 1), 5.0))
    if last_exc is not None:
        raise last_exc
    raise sqlite3.OperationalError("failed to connect news db")


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()
        except Exception:
            continue
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _norm_code6(v: Any) -> str:
    s = "".join(ch for ch in str(v or "") if ch.isdigit())
    return s if len(s) == 6 else ""


_INVALID_NAME_TEXT = {"", "nan", "none", "null", "<na>", "nat"}


def _clean_name(v: Any) -> str:
    s = str(v or "").strip()
    return "" if s.lower() in _INVALID_NAME_TEXT else s


def _clean_name_series(v: Any, index: Any | None = None) -> pd.Series:
    if isinstance(v, pd.Series):
        return v.map(_clean_name)
    return pd.Series(["" for _ in range(0 if index is None else len(index))], index=index)


def _load_reference_name_map() -> Dict[str, str]:
    if not DART_CORP_CODE_MAP.exists():
        return {}
    try:
        ref = _read_csv(DART_CORP_CODE_MAP)
    except Exception:
        return {}
    if ref.empty or "code" not in ref.columns or "corp_name" not in ref.columns:
        return {}
    out: Dict[str, str] = {}
    for _, row in ref.iterrows():
        code = _norm_code6(row.get("code"))
        name = _clean_name(row.get("corp_name"))
        if code and name and code not in out:
            out[code] = name
    return out


def _apply_reference_name_filter(df: pd.DataFrame, ref_names: Dict[str, str]) -> tuple[pd.DataFrame, Dict[str, int]]:
    meta = {
        "reference_codes": int(len(ref_names)),
        "rejected_unknown_code_rows": 0,
        "rejected_name_mismatch_rows": 0,
        "name_backfilled_rows": 0,
    }
    if df.empty or not ref_names or "code" not in df.columns:
        return df, meta
    out = df.copy()
    if "name" not in out.columns:
        out["name"] = ""
    out["code"] = out["code"].map(_norm_code6)
    ref_series = out["code"].map(ref_names).fillna("")
    name_series = out["name"].map(_clean_name)
    missing_name = name_series.eq("") & ref_series.ne("")
    if missing_name.any():
        out.loc[missing_name, "name"] = ref_series[missing_name]
        name_series = out["name"].map(_clean_name)
        meta["name_backfilled_rows"] = int(missing_name.sum())
    unknown = ref_series.eq("")
    mismatch = ref_series.ne("") & name_series.ne("") & name_series.ne(ref_series)
    meta["rejected_unknown_code_rows"] = int(unknown.sum())
    meta["rejected_name_mismatch_rows"] = int(mismatch.sum())
    return out[~(unknown | mismatch)].copy().reset_index(drop=True), meta


def _to_bool(v: Any) -> bool:
    return str(v or "").strip().upper() in {"TRUE", "1", "Y", "YES"}


def _merge_source_labels(*values: Any) -> str:
    labels: List[str] = []
    seen = set()
    for value in values:
        for part in str(value or "").replace(",", "|").split("|"):
            label = part.strip()
            if not label or label.lower() == "nan" or label in seen:
                continue
            seen.add(label)
            labels.append(label)
    return "|".join(labels)


def _is_direct_observed_source(code: Any, name: Any, title: Any) -> bool:
    code_s = _norm_code6(code)
    name_s = _clean_name(name)
    title_s = str(title or "").strip()
    if not title_s:
        return False
    if code_s and code_s in title_s:
        return True
    if name_s and name_s in title_s:
        return True
    return False


def _parse_dt(v: Any) -> datetime | pd.NaT:
    raw = str(v or "").strip()
    if not raw:
        return pd.NaT
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=KST)
            return dt.astimezone(KST)
        except Exception:
            continue
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        return dt.astimezone(KST)
    except Exception:
        return pd.NaT


def _load_rising(limit: int = 30) -> pd.DataFrame:
    if not MARKET_RISING.exists():
        return pd.DataFrame(columns=["code", "name", "market", "close", "ret1_pct", "watch_badge", "krx_caution", "krx_warning", "krx_risk", "krx_admin", "source_scope"])
    raw = _read_csv(MARKET_RISING)
    if "code" not in raw.columns:
        return pd.DataFrame(columns=["code", "name", "market", "close", "ret1_pct", "watch_badge", "krx_caution", "krx_warning", "krx_risk", "krx_admin", "source_scope"])
    work = raw.copy()
    if "rank" in work.columns:
        work["_rank"] = pd.to_numeric(work["rank"], errors="coerce")
        work = work.sort_values(["_rank", "code"], ascending=[True, True], kind="mergesort")
    out = pd.DataFrame()
    out["code"] = work["code"].map(_norm_code6)
    out["name"] = _clean_name_series(work["name"], work.index) if "name" in work.columns else ""
    out["market"] = ""
    out["close"] = pd.to_numeric(work.get("current_price", 0.0), errors="coerce").fillna(0.0)
    out["ret1_pct"] = pd.to_numeric(work.get("change_pct", 0.0), errors="coerce").fillna(0.0)
    out["watch_badge"] = work["watch_badge"].astype(str).str.strip() if "watch_badge" in work.columns else ""
    out["krx_caution"] = work.get("krx_caution", False)
    out["krx_warning"] = work.get("krx_warning", False)
    out["krx_risk"] = work.get("krx_risk", False)
    out["krx_admin"] = work.get("krx_admin", False)
    out["source_scope"] = "market_rising"
    out = out[out["code"] != ""].drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    if limit > 0 and len(out) > limit:
        out = out.head(limit).reset_index(drop=True)
    return out


def _load_surge() -> pd.DataFrame:
    if not SURGE_REALTIME.exists():
        return pd.DataFrame(columns=["code", "name", "market", "close", "ret1_pct", "watch_badge", "krx_caution", "krx_warning", "krx_risk", "krx_admin", "source_scope"])
    raw = _read_csv(SURGE_REALTIME)
    if "code" not in raw.columns:
        return pd.DataFrame(columns=["code", "name", "market", "close", "ret1_pct", "watch_badge", "krx_caution", "krx_warning", "krx_risk", "krx_admin", "source_scope"])
    work = raw.copy()
    if "surge_flag" in work.columns:
        work = work[work["surge_flag"].map(_to_bool)].copy()
    out = pd.DataFrame()
    out["code"] = work["code"].map(_norm_code6)
    out["name"] = _clean_name_series(work["name"], work.index) if "name" in work.columns else ""
    out["market"] = ""
    out["close"] = pd.to_numeric(work.get("current_price", 0.0), errors="coerce").fillna(0.0)
    out["ret1_pct"] = pd.to_numeric(work.get("change_pct", 0.0), errors="coerce").fillna(0.0)
    out["watch_badge"] = ""
    out["krx_caution"] = False
    out["krx_warning"] = False
    out["krx_risk"] = False
    out["krx_admin"] = False
    out["source_scope"] = "surge"
    out = out[out["code"] != ""].drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    return out


def _load_scored_candidates() -> pd.DataFrame:
    cols = [
        "date", "code", "name", "market", "close", "ret1_pct", "watch_badge",
        "krx_caution", "krx_warning", "krx_risk", "krx_admin",
        "news_score", "news_sentiment", "news_article_count", "news_freshest_age_hours",
        "news_source",
    ]
    if not WITH_NEWS_SCORE.exists():
        return pd.DataFrame(columns=cols)
    raw = _read_csv(WITH_NEWS_SCORE)
    if raw.empty or "code" not in raw.columns:
        return pd.DataFrame(columns=cols)
    out = pd.DataFrame()
    out["date"] = raw["date"] if "date" in raw.columns else ""
    out["code"] = raw["code"].map(_norm_code6)
    out["name"] = _clean_name_series(raw["name"], raw.index) if "name" in raw.columns else ""
    out["market"] = raw.get("market", "").astype(str) if "market" in raw.columns else ""
    out["close"] = pd.to_numeric(raw.get("close", 0.0), errors="coerce").fillna(0.0)
    out["ret1_pct"] = pd.to_numeric(raw.get("ret1_pct", 0.0), errors="coerce").fillna(0.0)
    out["watch_badge"] = raw["watch_badge"].astype(str).str.strip() if "watch_badge" in raw.columns else ""
    out["krx_caution"] = raw.get("krx_caution", False)
    out["krx_warning"] = raw.get("krx_warning", False)
    out["krx_risk"] = raw.get("krx_risk", False)
    out["krx_admin"] = raw.get("krx_admin", False)
    out["news_score"] = pd.to_numeric(raw.get("news_score", 0.0), errors="coerce").fillna(0.0)
    out["news_sentiment"] = pd.to_numeric(raw.get("news_sentiment", 0.0), errors="coerce").fillna(0.0)
    out["news_article_count"] = pd.to_numeric(raw.get("news_article_count", 0), errors="coerce").fillna(0).astype(int)
    out["news_freshest_age_hours"] = pd.to_numeric(raw.get("news_freshest_age_hours", 9999.0), errors="coerce").fillna(9999.0)
    out["news_source"] = raw.get("news_source", "").astype(str) if "news_source" in raw.columns else ""
    out = out[out["code"] != ""].drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    return out


def _max_date8_from_candidates(df: pd.DataFrame) -> str:
    if df.empty:
        return ""
    for col in ("date8", "as_of_ymd", "date_yyyymmdd", "date"):
        if col not in df.columns:
            continue
        raw = df[col]
        if col == "date":
            parsed = pd.to_datetime(raw, errors="coerce")
            vals = parsed.dt.strftime("%Y%m%d")
        else:
            vals = raw.astype(str).str.replace(r"\.0$", "", regex=True).str.replace(r"[^0-9]", "", regex=True).str[:8]
        vals = vals[vals.astype(str).str.len() == 8]
        if not vals.empty:
            return str(vals.max())
    return ""


def _date8_to_display(v: str, now: datetime) -> str:
    s = str(v or "").strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return now.strftime("%Y-%m-%d")


def _load_name_backfill(con: sqlite3.Connection, codes: List[str]) -> Dict[str, str]:
    norm_codes = sorted({_norm_code6(c) for c in codes if _norm_code6(c)})
    if not norm_codes:
        return {}
    tables = {str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    out: Dict[str, str] = {}
    placeholders = ",".join("?" for _ in norm_codes)

    sources = []
    if "source_signals_daily" in tables:
        sources.append((
            "source_signals_daily",
            f"""
            SELECT code, name, date8, fetched_at AS ts
            FROM source_signals_daily
            WHERE code IN ({placeholders})
            ORDER BY date8 DESC, fetched_at DESC
            """,
        ))
    if "signals_naver_daily" in tables:
        sources.append((
            "signals_naver_daily",
            f"""
            SELECT code, name, date8, updated_at AS ts
            FROM signals_naver_daily
            WHERE code IN ({placeholders})
            ORDER BY date8 DESC, updated_at DESC
            """,
        ))
    elif "signals" in tables:
        sources.append((
            "signals",
            f"""
            SELECT code, name, date8, updated_at AS ts
            FROM signals
            WHERE code IN ({placeholders})
            ORDER BY date8 DESC, updated_at DESC
            """,
        ))

    for _, sql in sources:
        try:
            rows = pd.read_sql_query(sql, con, params=norm_codes)
        except Exception:
            continue
        if rows.empty:
            continue
        rows["code"] = rows["code"].map(_norm_code6)
        rows["name"] = rows["name"].map(_clean_name)
        rows = rows[(rows["code"] != "") & (rows["name"] != "")]
        for _, row in rows.iterrows():
            code = str(row.get("code", ""))
            if code and code not in out:
                out[code] = str(row.get("name", ""))
    return out


def _load_db_signal_candidates(
    con: sqlite3.Connection,
    reference_ymd: str,
) -> pd.DataFrame:
    tables = {str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    target = "signals_naver_daily" if "signals_naver_daily" in tables else ("signals" if "signals" in tables else "")
    if not target:
        return pd.DataFrame(columns=[
            "code", "name", "market", "close", "ret1_pct", "watch_badge",
            "krx_caution", "krx_warning", "krx_risk", "krx_admin", "source_scope",
        ])

    ref = str(reference_ymd or "").strip()
    if len(ref) != 8 or not ref.isdigit():
        ref_dt = datetime.now(KST)
    else:
        ref_dt = datetime.strptime(ref, "%Y%m%d").replace(tzinfo=KST)
    max_lag_days = max(0, int(float(str(os.environ.get("NEWS_CANDIDATES_SIGNAL_MAX_LAG_DAYS", "5")).strip() or "5")))
    min_score = max(-1.0, min(1.0, float(str(os.environ.get("NEWS_CANDIDATES_SIGNAL_MIN_SCORE", "0.35")).strip() or "0.35")))
    max_rows = max(0, int(float(str(os.environ.get("NEWS_CANDIDATES_SIGNAL_MAX_ROWS", "20")).strip() or "20")))
    cutoff = (ref_dt - timedelta(days=max_lag_days)).strftime("%Y%m%d")

    sql = f"""
    SELECT code, name AS signal_name, news_score, headline_count, article_count, source, updated_at, date8
    FROM {target}
    WHERE date8 >= ? AND date8 <= ? AND CAST(COALESCE(news_score, 0) AS REAL) >= ?
    ORDER BY date8 DESC, CAST(COALESCE(news_score, 0) AS REAL) DESC, COALESCE(headline_count, article_count, 0) DESC
    """
    df = pd.read_sql_query(sql, con, params=[cutoff, ref_dt.strftime("%Y%m%d"), min_score])
    if df.empty:
        return pd.DataFrame(columns=[
            "code", "name", "market", "close", "ret1_pct", "watch_badge",
            "krx_caution", "krx_warning", "krx_risk", "krx_admin", "source_scope",
        ])
    df["code"] = df["code"].map(_norm_code6)
    df["_news_score"] = pd.to_numeric(df.get("news_score", 0.0), errors="coerce").fillna(0.0)
    df["_headline_count"] = pd.to_numeric(df.get("headline_count", df.get("article_count", 0)), errors="coerce").fillna(0.0)
    df["_candidate_priority"] = df["_news_score"] * df["_headline_count"].map(lambda x: math.log1p(max(0.0, float(x))))
    df = (
        df[df["code"] != ""]
        .sort_values(["_candidate_priority", "_headline_count", "_news_score", "date8"], ascending=[False, False, False, False], kind="mergesort")
        .drop_duplicates(subset=["code"], keep="first")
        .reset_index(drop=True)
    )
    if max_rows > 0 and len(df) > max_rows:
        df = df.head(max_rows).reset_index(drop=True)

    out = pd.DataFrame()
    out["code"] = df["code"]
    out["name"] = _clean_name_series(df["signal_name"], df.index) if "signal_name" in df.columns else ""
    name_backfill = _load_name_backfill(con, out["code"].astype(str).tolist())
    if name_backfill:
        missing_name = out["name"].map(_clean_name).eq("")
        out.loc[missing_name, "name"] = out.loc[missing_name, "code"].map(name_backfill).fillna("")
    out["market"] = ""
    out["close"] = 0.0
    out["ret1_pct"] = 0.0
    out["watch_badge"] = ""
    out["krx_caution"] = False
    out["krx_warning"] = False
    out["krx_risk"] = False
    out["krx_admin"] = False
    out["source_scope"] = "db_signal"
    return out


def _load_latest_signal_rows(con: sqlite3.Connection, codes: List[str], reference_ymd: str = "") -> pd.DataFrame:
    if not codes:
        return pd.DataFrame(columns=["code", "news_score", "headline_count", "article_count", "news_source", "updated_at", "news_signal_date8"])
    placeholders = ",".join("?" for _ in codes)
    tables = {str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    target = "signals_naver_daily" if "signals_naver_daily" in tables else ("signals" if "signals" in tables else "")
    if not target:
        return pd.DataFrame(columns=["code", "news_score", "headline_count", "article_count", "news_source", "updated_at", "news_signal_date8"])
    order_col = "updated_at" if target == "signals_naver_daily" else "updated_at"
    ref = str(reference_ymd or "").strip()
    if len(ref) != 8 or not ref.isdigit():
        ref_dt = datetime.now(KST)
    else:
        ref_dt = datetime.strptime(ref, "%Y%m%d").replace(tzinfo=KST)
    max_lag_days = max(0, int(float(str(os.environ.get("NEWS_CANDIDATES_SIGNAL_MAX_LAG_DAYS", "5")).strip() or "5")))
    cutoff = (ref_dt - timedelta(days=max_lag_days)).strftime("%Y%m%d")
    sql = f"""
    SELECT code, name AS signal_name, news_score, headline_count, article_count, source, updated_at, date8
    FROM {target}
    WHERE code IN ({placeholders}) AND date8 >= ? AND date8 <= ?
    """
    df = pd.read_sql_query(sql, con, params=list(codes) + [cutoff, ref_dt.strftime("%Y%m%d")])
    if df.empty:
        return pd.DataFrame(columns=["code", "news_score", "headline_count", "article_count", "news_source", "updated_at", "news_signal_date8"])
    df["code"] = df["code"].map(_norm_code6)
    df["_news_score"] = pd.to_numeric(df.get("news_score", 0.0), errors="coerce").fillna(0.0)
    df["_headline_count"] = pd.to_numeric(df.get("headline_count", df.get("article_count", 0)), errors="coerce").fillna(0.0)
    df["_candidate_priority"] = df["_news_score"] * df["_headline_count"].map(lambda x: math.log1p(max(0.0, float(x))))
    df = df.sort_values(
        ["code", "_candidate_priority", "_headline_count", "_news_score", "date8", order_col],
        ascending=[True, False, False, False, False, False],
        kind="mergesort",
    )
    df = df.drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    df = df.rename(columns={"source": "news_source", "date8": "news_signal_date8"})
    return df


def _load_latest_article_rows(con: sqlite3.Connection, codes: List[str], code_names: Dict[str, str] | None = None) -> pd.DataFrame:
    if not codes:
        return pd.DataFrame(columns=[
            "code", "published_at", "fetched_at", "source", "implication_score",
            "article_score", "llm_score", "observed_article_count", "observed_sources",
        ])
    tables = {str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    placeholders = ",".join("?" for _ in codes)
    code_names = code_names or {}
    frames: List[pd.DataFrame] = []
    if "news_articles_naver" in tables:
        sql = f"""
        SELECT code, name, published_at, fetched_at, title, source, implication_score, article_score, llm_score
        FROM news_articles_naver
        WHERE code IN ({placeholders})
        """
        frames.append(pd.read_sql_query(sql, con, params=codes))
    if "news_articles_google_rss" in tables:
        sql = f"""
        SELECT code, name, published_at, fetched_at, title, source,
               0.0 AS implication_score, 0.0 AS article_score, 0.0 AS llm_score
        FROM news_articles_google_rss
        WHERE code IN ({placeholders})
        """
        frames.append(pd.read_sql_query(sql, con, params=codes))
    if "news_articles_kis_title" in tables:
        sql = f"""
        SELECT code,
               name,
               CASE
                 WHEN LENGTH(COALESCE(data_dt, '')) = 8
                 THEN substr(data_dt, 1, 4) || '-' || substr(data_dt, 5, 2) || '-' || substr(data_dt, 7, 2) || ' ' ||
                      substr(substr('000000' || COALESCE(data_tm, '000000'), -6, 6), 1, 2) || ':' ||
                      substr(substr('000000' || COALESCE(data_tm, '000000'), -6, 6), 3, 2) || ':' ||
                      substr(substr('000000' || COALESCE(data_tm, '000000'), -6, 6), 5, 2)
                 ELSE NULL
               END AS published_at,
               collected_at AS fetched_at,
               title,
               'KIS_NEWS_TITLE' AS source,
               0.0 AS implication_score, 0.0 AS article_score, 0.0 AS llm_score
        FROM news_articles_kis_title
        WHERE code IN ({placeholders})
        """
        frames.append(pd.read_sql_query(sql, con, params=codes))
    required_cols = ["code", "name", "published_at", "fetched_at", "title", "source", "implication_score", "article_score", "llm_score"]
    clean_frames: List[pd.DataFrame] = []
    for frame in frames:
        if frame is None or frame.empty:
            continue
        frame = frame.copy()
        for col in required_cols:
            if col not in frame.columns:
                frame[col] = "" if col in {"code", "name", "published_at", "fetched_at", "title", "source"} else 0.0
        for col in ("implication_score", "article_score", "llm_score"):
            frame[col] = pd.to_numeric(frame[col], errors="coerce").fillna(0.0)
        clean_frames.append(frame[required_cols])
    frames = clean_frames
    if not frames:
        return pd.DataFrame(columns=[
            "code", "published_at", "fetched_at", "source", "implication_score",
            "article_score", "llm_score", "observed_article_count", "observed_sources",
        ])
    df = pd.concat(frames, ignore_index=True)
    df["code"] = df["code"].map(_norm_code6)
    df = df[df["code"] != ""].copy()
    external_mask = df["source"].astype(str).isin(["GOOGLE_NEWS_RSS", "KIS_NEWS_TITLE"])
    if external_mask.any():
        direct_mask = df.apply(
            lambda row: _is_direct_observed_source(
                row.get("code"),
                code_names.get(str(row.get("code") or ""), row.get("name")),
                row.get("title"),
            ),
            axis=1,
        )
        df = df[(~external_mask) | direct_mask].copy()
    if df.empty:
        return pd.DataFrame(columns=[
            "code", "published_at", "fetched_at", "source", "implication_score",
            "article_score", "llm_score", "observed_article_count", "observed_sources",
        ])
    freshest = df["published_at"].where(df["published_at"].notna(), df["fetched_at"]).map(_parse_dt)
    df["_fresh_dt"] = pd.to_datetime(freshest, errors="coerce", utc=True)
    observed_max_age_hours = max(
        1.0,
        float(str(os.environ.get("NEWS_CANDIDATES_OBSERVED_MAX_AGE_HOURS", "72")).strip() or "72"),
    )
    cutoff = pd.Timestamp(datetime.now(KST)).tz_convert("UTC") - pd.Timedelta(hours=observed_max_age_hours)
    recent_df = df[df["_fresh_dt"].notna() & (df["_fresh_dt"] >= cutoff)].copy()
    count_df = recent_df if not recent_df.empty else df.iloc[0:0].copy()
    counts = (
        count_df.groupby("code", dropna=False)
        .agg(
            observed_article_count=("code", "size"),
            observed_sources=("source", lambda s: "|".join(sorted({str(v or "").strip() for v in s if str(v or "").strip()}))),
        )
        .reset_index()
    )
    latest = (
        df.sort_values(["code", "_fresh_dt"], ascending=[True, False], kind="mergesort")
        .drop_duplicates(subset=["code"], keep="first")
        .drop(columns=["_fresh_dt"], errors="ignore")
        .reset_index(drop=True)
    )
    latest = latest.merge(counts, on="code", how="left")
    latest["observed_article_count"] = pd.to_numeric(
        latest.get("observed_article_count", 0), errors="coerce"
    ).fillna(0).astype(int)
    latest["observed_sources"] = latest.get("observed_sources", "").astype(str).replace({"nan": ""}).fillna("")
    return latest


def _load_article_implication_max(con: sqlite3.Connection, codes: List[str], lookback_days: int) -> pd.DataFrame:
    if not codes:
        return pd.DataFrame(columns=["code", "implication_score_max"])
    tables = {str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    if "news_articles_naver" not in tables:
        return pd.DataFrame(columns=["code", "implication_score_max"])
    cutoff = (datetime.now(KST) - timedelta(days=max(1, int(lookback_days)))).strftime("%Y%m%d")
    placeholders = ",".join("?" for _ in codes)
    sql = f"""
    SELECT code, MAX(CAST(COALESCE(implication_score, 0) AS REAL)) AS implication_score_max
    FROM news_articles_naver
    WHERE code IN ({placeholders}) AND date8 >= ?
    GROUP BY code
    """
    params = list(codes) + [cutoff]
    df = pd.read_sql_query(sql, con, params=params)
    if df.empty:
        return pd.DataFrame(columns=["code", "implication_score_max"])
    df["code"] = df["code"].map(_norm_code6)
    df["implication_score_max"] = pd.to_numeric(df.get("implication_score_max", 0.0), errors="coerce").fillna(0.0)
    return df.drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)


def main() -> int:
    now = datetime.now(KST)
    collect_mode = str(os.environ.get("NEWS_COLLECT_MODE", "production")).strip().lower()
    rising_df = _load_rising(limit=30)
    surge_df = _load_surge()
    scored_df = _load_scored_candidates()
    reference_ymd = str(os.environ.get("NEWS_CANDIDATES_REFERENCE_YMD", "")).strip()
    if not reference_ymd:
        reference_ymd = _max_date8_from_candidates(scored_df)
    if not reference_ymd:
        reference_ymd = now.strftime("%Y%m%d")
    output_date = _date8_to_display(reference_ymd, now)

    union_df = pd.concat([rising_df, surge_df], ignore_index=True)
    db_signal_df = pd.DataFrame()
    try:
        with _connect_news_db() as con:
            db_signal_df = _load_db_signal_candidates(con, reference_ymd)
    except Exception:
        db_signal_df = pd.DataFrame()
    if not db_signal_df.empty:
        union_df = pd.concat([union_df, db_signal_df], ignore_index=True)
    if union_df.empty:
        out = pd.DataFrame(columns=[
            "date", "code", "name", "market", "close", "ret1_pct", "news_score", "news_sentiment",
            "news_article_count", "news_freshest_age_hours", "news_source", "watch_badge",
            "krx_caution", "krx_warning", "krx_risk", "krx_admin", "candidate_origin", "news_source_scope"
        ])
        _write_csv(OUT, out)
        STATUS_LATEST.write_text(json.dumps({
            "generated_at": now.isoformat(timespec="seconds"),
            "reference_ymd": reference_ymd,
            "rows": 0,
            "reason": "input_empty",
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        return 0

    scope_rank = {"market_rising": 0, "surge": 1, "db_signal": 2}
    union_df["_scope_rank"] = union_df["source_scope"].map(scope_rank).fillna(9)
    union_df = union_df.sort_values(["code", "_scope_rank"]).drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    codes = union_df["code"].astype(str).tolist()

    lookback_days = max(1, int(float(str(os.environ.get("NEWS_LOOKBACK_DAYS", "3")).strip() or "3")))
    with _connect_news_db() as con:
        signal_df = _load_latest_signal_rows(con, codes, reference_ymd=reference_ymd)
        code_names = {
            str(row.get("code") or ""): _clean_name(row.get("name"))
            for _, row in union_df[["code", "name"]].iterrows()
            if str(row.get("code") or "")
        }
        article_df = _load_latest_article_rows(con, codes, code_names=code_names)
        implication_df = _load_article_implication_max(con, codes, lookback_days=lookback_days)

    merged = union_df.merge(signal_df, on="code", how="left")
    article_cols = [
        "code", "published_at", "fetched_at", "source", "implication_score",
        "observed_article_count", "observed_sources",
    ]
    for col in article_cols:
        if col not in article_df.columns:
            article_df[col] = pd.NA
    merged = merged.merge(article_df[article_cols], on="code", how="left", suffixes=("", "_article"))
    merged = merged.merge(implication_df[["code", "implication_score_max"]], on="code", how="left")

    freshest = merged["published_at"].where(merged["published_at"].notna(), merged["fetched_at"]).map(_parse_dt)
    freshest_ts = pd.to_datetime(freshest, errors="coerce", utc=True).dt.tz_convert(KST)
    now_kst = pd.Timestamp(now).tz_convert(KST)
    merged["news_freshest_age_hours"] = ((now_kst - freshest_ts).dt.total_seconds() / 3600.0).fillna(9999.0)
    ref_ts = pd.Timestamp(_date8_to_display(reference_ymd, now)).tz_localize(KST)
    signal_date = pd.to_datetime(
        merged.get("news_signal_date8", "").astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8],
        format="%Y%m%d",
        errors="coerce",
    )
    signal_ts = signal_date.dt.tz_localize(KST)
    merged["news_signal_lag_days"] = ((ref_ts - signal_ts).dt.total_seconds() / 86400.0).fillna(9999.0)
    merged["news_score"] = pd.to_numeric(merged.get("news_score", 0.0), errors="coerce").fillna(0.0)
    if collect_mode == "accumulate":
        implied_latest = pd.to_numeric(merged.get("implication_score", 0.0), errors="coerce").fillna(0.0)
        implied_max = pd.to_numeric(merged.get("implication_score_max", 0.0), errors="coerce").fillna(0.0)
        implied = implied_latest.where(implied_latest > 0.0, implied_max)
        merged["news_score"] = merged["news_score"].where(merged["news_score"] != 0.0, implied)
    merged["headline_count"] = pd.to_numeric(merged.get("headline_count", 0), errors="coerce").fillna(0).astype(int)
    merged["observed_article_count"] = pd.to_numeric(
        merged.get("observed_article_count", 0), errors="coerce"
    ).fillna(0).astype(int)
    if collect_mode == "accumulate":
        has_article = merged.get("published_at").notna() | merged.get("fetched_at").notna()
        merged.loc[has_article & (merged["headline_count"] <= 0), "headline_count"] = 1
    observed_more = merged["observed_article_count"] > merged["headline_count"]
    merged.loc[observed_more, "headline_count"] = merged.loc[observed_more, "observed_article_count"]
    merged["article_count"] = pd.to_numeric(merged.get("article_count", 0), errors="coerce").fillna(0).astype(int)
    merged["news_sentiment"] = merged["news_score"]
    merged["candidate_origin"] = "NEWS"
    merged["date"] = output_date
    merged["market"] = merged["market"].astype(str).replace({"nan": ""}).fillna("")
    for col in ["krx_caution", "krx_warning", "krx_risk", "krx_admin"]:
        merged[col] = merged[col].map(_to_bool)

    base_max_age_hours = max(1.0, float(str(os.environ.get("NEWS_CANDIDATES_MAX_AGE_HOURS", "24")).strip() or "24"))
    db_signal_max_age_hours = max(
        base_max_age_hours,
        float(str(os.environ.get("NEWS_CANDIDATES_SIGNAL_MAX_AGE_HOURS", "72")).strip() or "72"),
    )
    merged["news_candidate_max_age_hours"] = base_max_age_hours
    merged.loc[merged["source_scope"].astype(str).eq("db_signal"), "news_candidate_max_age_hours"] = db_signal_max_age_hours
    db_signal_max_lag_days = max(
        0.0,
        float(str(os.environ.get("NEWS_CANDIDATES_SIGNAL_MAX_LAG_DAYS", "5")).strip() or "5"),
    )
    db_signal_fresh = (
        merged["source_scope"].astype(str).eq("db_signal")
        & (merged["news_signal_lag_days"] >= 0.0)
        & (merged["news_signal_lag_days"] <= db_signal_max_lag_days)
    )

    filtered = merged[
        (merged["news_score"] > 0)
        & (merged["headline_count"] >= 1)
        & ((merged["news_freshest_age_hours"] <= merged["news_candidate_max_age_hours"]) | db_signal_fresh)
        & (~merged["krx_admin"])
    ].copy()

    if "observed_sources" in filtered.columns:
        observed_sources = filtered["observed_sources"].astype(str).replace({"nan": ""}).fillna("")
        existing_sources = filtered["news_source"].astype(str).replace({"nan": ""}).fillna("")
        filtered["news_source"] = [
            _merge_source_labels(existing, observed)
            for existing, observed in zip(existing_sources.tolist(), observed_sources.tolist())
        ]
    filtered["news_source"] = filtered["news_source"].astype(str).replace({"nan": ""}).fillna("")
    filtered["watch_badge"] = filtered["watch_badge"].astype(str).replace({"nan": ""}).fillna("")
    filtered["name"] = _clean_name_series(filtered["name"], filtered.index) if "name" in filtered.columns else ""
    filtered["news_source_scope"] = filtered["source_scope"]
    filtered["news_article_count"] = filtered["headline_count"]

    out = filtered[
        [
            "date",
            "code",
            "name",
            "market",
            "close",
            "ret1_pct",
            "news_score",
            "news_sentiment",
            "news_article_count",
            "news_freshest_age_hours",
            "news_source",
            "watch_badge",
            "krx_caution",
            "krx_warning",
            "krx_risk",
            "krx_admin",
            "candidate_origin",
            "news_source_scope",
        ]
    ].copy()
    if not scored_df.empty:
        scored_filtered = scored_df[
            (scored_df["news_score"] > 0)
            & (scored_df["news_article_count"] >= 1)
            & (scored_df["news_freshest_age_hours"] <= 24.0)
            & (~scored_df["krx_admin"].map(_to_bool))
        ].copy()
        if not scored_filtered.empty:
            scored_filtered["date"] = output_date
            scored_filtered["candidate_origin"] = "NEWS"
            scored_filtered["news_source_scope"] = "candidate_news_score"
            scored_filtered["name"] = _clean_name_series(scored_filtered["name"], scored_filtered.index) if "name" in scored_filtered.columns else ""
            scored_filtered = scored_filtered[
                [
                    "date",
                    "code",
                    "name",
                    "market",
                    "close",
                    "ret1_pct",
                    "news_score",
                    "news_sentiment",
                    "news_article_count",
                    "news_freshest_age_hours",
                    "news_source",
                    "watch_badge",
                    "krx_caution",
                    "krx_warning",
                    "krx_risk",
                    "krx_admin",
                    "candidate_origin",
                    "news_source_scope",
                ]
            ].copy()
            out = (
                pd.concat([out, scored_filtered], ignore_index=True)
                .drop_duplicates(subset=["code"], keep="first")
                .reset_index(drop=True)
            )
    ref_names = _load_reference_name_map()
    out, reference_name_filter = _apply_reference_name_filter(out, ref_names)
    _write_csv(OUT, out)

    score_positive = merged["news_score"] > 0
    headline_positive = merged["headline_count"] >= 1
    fresh_enough = (merged["news_freshest_age_hours"] <= merged["news_candidate_max_age_hours"]) | db_signal_fresh
    scored_score_positive = pd.Series(dtype=bool)
    scored_headline_positive = pd.Series(dtype=bool)
    scored_fresh_enough = pd.Series(dtype=bool)
    if not scored_df.empty:
        scored_score_positive = scored_df["news_score"] > 0
        scored_headline_positive = scored_df["news_article_count"] >= 1
        scored_fresh_enough = scored_df["news_freshest_age_hours"] <= 24.0

    zero_rows_reason = ""
    if len(out) == 0 and len(union_df) > 0:
        if int(fresh_enough.sum()) <= 0 and (scored_df.empty or int(scored_fresh_enough.sum()) <= 0):
            zero_rows_reason = "no_fresh_news_within_24h"
        elif int((score_positive & headline_positive & fresh_enough).sum()) <= 0:
            zero_rows_reason = "no_rows_after_score_headline_fresh_filters"
        else:
            zero_rows_reason = "no_rows_after_filters"

    status = {
        "generated_at": now.isoformat(timespec="seconds"),
        "reference_ymd": reference_ymd,
        "input_rows": int(len(union_df)),
        "rows": int(len(out)),
        "market_rising_rows": int(len(rising_df)),
        "surge_rows": int(len(surge_df)),
        "db_signal_rows": int(len(db_signal_df)),
        "nonzero_signal_rows": int(score_positive.sum()),
        "headline_rows": int(headline_positive.sum()),
        "observed_multisource_article_rows": int((merged["observed_article_count"] > 0).sum()),
        "observed_multisource_article_count_sum": int(merged["observed_article_count"].sum()),
        "observed_multisource_sources": {
            str(k): int(v)
            for k, v in merged.get("observed_sources", pd.Series(dtype=str))
            .astype(str)
            .replace({"nan": ""})
            .value_counts(dropna=False)
            .to_dict()
            .items()
            if str(k).strip()
        },
        "fresh_rows": int(fresh_enough.sum()),
        "score_headline_fresh_rows": int((score_positive & headline_positive & fresh_enough).sum()),
        "max_age_hours": {
            "default": float(base_max_age_hours),
            "db_signal": float(db_signal_max_age_hours),
        },
        "db_signal_max_lag_days": float(db_signal_max_lag_days),
        "scored_candidate_rows": int(len(scored_df)),
        "scored_candidate_nonzero_rows": int(scored_score_positive.sum()) if not scored_df.empty else 0,
        "scored_candidate_headline_rows": int(scored_headline_positive.sum()) if not scored_df.empty else 0,
        "scored_candidate_fresh_rows": int(scored_fresh_enough.sum()) if not scored_df.empty else 0,
        "min_news_freshest_age_hours": (
            None if merged.empty else round(float(merged["news_freshest_age_hours"].min()), 6)
        ),
        "output": str(OUT),
        "reference_name_filter": reference_name_filter,
        "reason": zero_rows_reason or "ok",
        "zero_rows_reason": zero_rows_reason,
    }
    STATUS_LATEST.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
