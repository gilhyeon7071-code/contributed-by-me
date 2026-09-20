from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
import sqlite3
import time
import urllib.parse
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
CACHE_DIR = ROOT / "_cache" / "google_news_rss_probe"
NEWS_DB = ROOT / "news_trading" / "data" / "trading.db"

IN_FILES = [
    LOGS / "candidates_latest_data.with_final_score.csv",
    LOGS / "candidates_latest_data.with_news_score.csv",
    LOGS / "candidates_latest_data.with_sector_score.csv",
    LOGS / "candidates_latest_data.filtered.csv",
    LOGS / "candidates_latest_data.csv",
]

KST = timezone(timedelta(hours=9))
RSS_BASE = "https://news.google.com/rss/search"

MACRO_QUERIES = [
    "foreigners dump asia stocks",
    "korea stocks foreign capital",
    "semiconductor sector rotation",
    "AI stocks crowded trades",
    "Fed rate cut emerging markets",
    "Asia tech stocks profit taking",
    "TSMC Samsung Electronics foreign selloff",
    "global asset allocation emerging markets"
]


def _now_kst() -> datetime:
    return datetime.now(KST)


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)


def _norm_code6(v: object) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s.zfill(6) if s else ""


def _pick_input() -> Optional[Path]:
    for path in IN_FILES:
        if path.exists():
            return path
    return None


def _strip_html(s: object) -> str:
    text = re.sub(r"<[^>]+>", " ", str(s or ""))
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _parse_pubdate(raw: object) -> Optional[datetime]:
    s = str(raw or "").strip()
    if not s:
        return None
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        return dt.astimezone(KST)
    except Exception:
        return None


def _cache_path(query: str, hl: str, gl: str, ceid: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(f"{query}|{hl}|{gl}|{ceid}".encode("utf-8")).hexdigest()
    return CACHE_DIR / f"{key}.xml"


def _read_cache(path: Path, ttl_sec: int) -> Optional[bytes]:
    if ttl_sec <= 0 or not path.exists():
        return None
    if time.time() - path.stat().st_mtime > ttl_sec:
        return None
    try:
        return path.read_bytes()
    except Exception:
        return None


def _write_cache(path: Path, data: bytes) -> None:
    try:
        path.write_bytes(data)
    except Exception:
        pass


def _build_url(query: str, hl: str, gl: str, ceid: str) -> str:
    qs = urllib.parse.urlencode(
        {
            "q": query,
            "hl": hl,
            "gl": gl,
            "ceid": ceid,
        }
    )
    return f"{RSS_BASE}?{qs}"


def _fetch_rss(
    query: str,
    hl: str,
    gl: str,
    ceid: str,
    timeout_sec: int,
    cache_ttl_sec: int,
) -> Tuple[Optional[bytes], bool, str, str]:
    cache = _cache_path(query, hl, gl, ceid)
    cached = _read_cache(cache, cache_ttl_sec)
    url = _build_url(query, hl, gl, ceid)
    if cached is not None:
        return cached, True, "cache_hit", url

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; google-news-rss-probe/1.0)",
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.7,en;q=0.6",
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=max(1, int(timeout_sec))) as resp:
            data = resp.read()
        _write_cache(cache, data)
        return data, False, "ok", url
    except urllib.error.HTTPError as e:
        return None, False, f"http_{int(e.code)}", url
    except urllib.error.URLError as e:
        return None, False, f"url_error:{e.reason}", url
    except Exception as e:
        return None, False, f"{type(e).__name__}", url


def _parse_rss(data: bytes) -> Tuple[str, List[Dict[str, object]], str]:
    try:
        root = ET.fromstring(data)
    except ET.ParseError as e:
        return "", [], f"xml_parse_error:{e}"

    channel = root.find("./channel")
    channel_title = ""
    if channel is not None:
        title_el = channel.find("title")
        channel_title = _strip_html(title_el.text if title_el is not None else "")

    items: List[Dict[str, object]] = []
    for item in root.findall(".//item"):
        title_el = item.find("title")
        desc_el = item.find("description")
        link_el = item.find("link")
        pub_el = item.find("pubDate")
        source_el = item.find("source")

        pub_dt = _parse_pubdate(pub_el.text if pub_el is not None else "")
        items.append(
            {
                "title": _strip_html(title_el.text if title_el is not None else ""),
                "description": _strip_html(desc_el.text if desc_el is not None else ""),
                "link": str(link_el.text or "").strip() if link_el is not None else "",
                "published_at": pub_dt.isoformat(timespec="seconds") if pub_dt else None,
                "source": _strip_html(source_el.text if source_el is not None else "GOOGLE_NEWS_RSS"),
            }
        )

    return channel_title, items, "ok" if items else "no_items"


def _article_key(code: str, link: object, title: object) -> str:
    basis = f"{_norm_code6(code)}|{str(link or '').strip()}|{str(title or '').strip()}"
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def _ensure_google_rss_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS news_articles_google_rss (
            article_key TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT,
            query TEXT,
            date8 TEXT,
            published_at TEXT,
            fetched_at TEXT,
            title TEXT,
            description TEXT,
            originallink TEXT,
            link TEXT,
            source TEXT NOT NULL DEFAULT 'GOOGLE_NEWS_RSS',
            run_id TEXT,
            channel_title TEXT,
            PRIMARY KEY(article_key, code)
        )
        """
    )


def _date8_from_iso(v: object, fallback: str) -> str:
    s = str(v or "").strip()
    if len(s) >= 10:
        d = re.sub(r"[^0-9]", "", s[:10])
        if len(d) == 8:
            return d
    return fallback


def _upsert_google_rss_rows(
    rows: List[Dict[str, object]],
    run_id: str,
    fetched_at: datetime,
) -> int:
    if not rows:
        return 0
    NEWS_DB.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(NEWS_DB), timeout=30.0)
    try:
        con.execute("PRAGMA busy_timeout=30000")
        _ensure_google_rss_table(con)
        inserted = 0
        for row in rows:
            exists = con.execute(
                """
                SELECT 1
                FROM news_articles_google_rss
                WHERE article_key = ? AND code = ?
                LIMIT 1
                """,
                (row["article_key"], row["code"]),
            ).fetchone()
            con.execute(
                """
                INSERT INTO news_articles_google_rss(
                    article_key, code, name, query, date8, published_at, fetched_at,
                    title, description, originallink, link, source, run_id, channel_title
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(article_key, code) DO UPDATE SET
                    name=excluded.name,
                    query=excluded.query,
                    date8=excluded.date8,
                    published_at=excluded.published_at,
                    fetched_at=excluded.fetched_at,
                    title=excluded.title,
                    description=excluded.description,
                    originallink=excluded.originallink,
                    link=excluded.link,
                    source=excluded.source,
                    run_id=excluded.run_id,
                    channel_title=excluded.channel_title
                """,
                (
                    row["article_key"],
                    row["code"],
                    row.get("name", ""),
                    row.get("query", ""),
                    row.get("date8", ""),
                    row.get("published_at", ""),
                    fetched_at.isoformat(timespec="seconds"),
                    row.get("title", ""),
                    row.get("description", ""),
                    row.get("originallink", ""),
                    row.get("link", ""),
                    "GOOGLE_NEWS_RSS",
                    run_id,
                    row.get("channel_title", ""),
                ),
            )
            if exists is None:
                inserted += 1
        con.commit()
        return int(inserted)
    finally:
        con.close()


def _load_symbols(max_symbols: int) -> List[Dict[str, str]]:
    input_path = _pick_input()
    if not input_path or not input_path.exists():
        return []
    df = _read_csv(input_path)
    if "code" not in df.columns:
        return []
    if "name" not in df.columns:
        df["name"] = ""
    out = (
        df.assign(code=lambda x: x["code"].map(_norm_code6), name=lambda x: x["name"].astype(str).str.strip())
        .loc[:, ["code", "name"]]
        .drop_duplicates(subset=["code"], keep="first")
    )
    out = out[(out["code"] != "") & (out["name"] != "")]
    if max_symbols > 0:
        out = out.head(max_symbols)
    return [{"code": str(r["code"]), "name": str(r["name"])} for _, r in out.iterrows()]


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Read-only Google News RSS connectivity probe.")
    ap.add_argument("--query", default=str(os.getenv("GOOGLE_NEWS_RSS_QUERY", "")).strip())
    ap.add_argument("--max-symbols", type=int, default=max(1, int(str(os.getenv("GOOGLE_NEWS_RSS_MAX_SYMBOLS", "10")).strip() or "10")))
    ap.add_argument("--max-items-per-symbol", type=int, default=max(1, int(str(os.getenv("GOOGLE_NEWS_RSS_MAX_ITEMS", "10")).strip() or "10")))
    ap.add_argument("--timeout-sec", type=int, default=max(1, int(str(os.getenv("GOOGLE_NEWS_RSS_TIMEOUT_SEC", "20")).strip() or "20")))
    ap.add_argument("--cache-ttl-sec", type=int, default=max(0, int(str(os.getenv("GOOGLE_NEWS_RSS_CACHE_TTL_SEC", "600")).strip() or "600")))
    ap.add_argument("--hl", default=str(os.getenv("GOOGLE_NEWS_RSS_HL", "ko")).strip() or "ko")
    ap.add_argument("--gl", default=str(os.getenv("GOOGLE_NEWS_RSS_GL", "KR")).strip() or "KR")
    ap.add_argument("--ceid", default=str(os.getenv("GOOGLE_NEWS_RSS_CEID", "KR:ko")).strip() or "KR:ko")
    ap.add_argument("--write-db", action="store_true", default=str(os.getenv("GOOGLE_NEWS_RSS_WRITE_DB", "")).strip().lower() in {"1", "true", "yes", "on"})
    ap.add_argument("--macro-mode", action="store_true", help="Use predefined global macro queries and English (US) settings")
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    now = _now_kst()
    run_id = now.strftime("googlerss_%Y%m%d_%H%M%S")

    if args.macro_mode:
        args.hl = "en"
        args.gl = "US"
        args.ceid = "US:en"
        symbols = [{"code": "MACRO", "name": q} for q in MACRO_QUERIES]
    else:
        symbols = [{"code": "", "name": str(args.query).strip()}] if str(args.query).strip() else _load_symbols(int(args.max_symbols))
    status: Dict[str, object] = {
        "generated_at": now.isoformat(timespec="seconds"),
        "run_id": run_id,
        "mode": "db_write_probe" if bool(args.write_db) else "read_only_probe",
        "db_write": bool(args.write_db),
        "db_path": str(NEWS_DB),
        "db_table": "news_articles_google_rss" if bool(args.write_db) else None,
        "db_rows_saved": 0,
        "input": str(_pick_input()) if _pick_input() else None,
        "symbol_count": int(len(symbols)),
        "item_count_total": 0,
        "quality": "FAIL",
        "reason": "",
        "rss": {
            "hl": str(args.hl),
            "gl": str(args.gl),
            "ceid": str(args.ceid),
            "cache_ttl_sec": int(args.cache_ttl_sec),
        },
        "results": [],
        "errors": [],
    }

    if not symbols:
        status["reason"] = "no_symbols_or_query"
    else:
        db_rows: List[Dict[str, object]] = []
        for sym in symbols:
            query = str(sym.get("name") or sym.get("code") or "").strip()
            if not query:
                continue
            data, from_cache, fetch_reason, url = _fetch_rss(
                query=query,
                hl=str(args.hl),
                gl=str(args.gl),
                ceid=str(args.ceid),
                timeout_sec=int(args.timeout_sec),
                cache_ttl_sec=int(args.cache_ttl_sec),
            )
            if data is None:
                status["errors"].append({"query": query, "reason": fetch_reason, "url": url})
                continue
            channel_title, items, parse_reason = _parse_rss(data)
            limited = items[: int(args.max_items_per_symbol)]
            status["item_count_total"] = int(status["item_count_total"]) + int(len(items))
            status["results"].append(
                {
                    "code": str(sym.get("code") or ""),
                    "name": str(sym.get("name") or query),
                    "query": query,
                    "url": url,
                    "from_cache": bool(from_cache),
                    "fetch_reason": fetch_reason,
                    "parse_reason": parse_reason,
                    "channel_title": channel_title,
                    "item_count": int(len(items)),
                    "items_sample": limited,
                }
            )
            for item in items:
                code = _norm_code6(sym.get("code") or "")
                link = str(item.get("link") or "").strip()
                title = str(item.get("title") or "").strip()
                db_rows.append(
                    {
                        "article_key": _article_key(code, link, title),
                        "code": code,
                        "name": str(sym.get("name") or query),
                        "query": query,
                        "date8": _date8_from_iso(item.get("published_at"), now.strftime("%Y%m%d")),
                        "published_at": str(item.get("published_at") or ""),
                        "title": title,
                        "description": str(item.get("description") or ""),
                        "originallink": link,
                        "link": link,
                        "channel_title": channel_title,
                    }
                )
        if bool(args.write_db) and db_rows:
            try:
                status["db_rows_saved"] = _upsert_google_rss_rows(db_rows, run_id, now)
            except Exception as e:
                status["errors"].append({"db_write": str(type(e).__name__), "message": str(e)[:200]})
        status["reason"] = "ok" if int(status["item_count_total"]) > 0 and not status["errors"] else "partial_or_empty"

    status["quality"] = "PASS" if status["reason"] == "ok" else ("WARN" if int(status["item_count_total"]) > 0 else "FAIL")
    LOGS.mkdir(parents=True, exist_ok=True)
    out = LOGS / f"google_news_rss_probe_{'macro_' if args.macro_mode else ''}{now.strftime('%Y%m%d')}.json"
    latest = LOGS / f"google_news_rss_probe_{'macro_' if args.macro_mode else ''}latest.json"
    text = json.dumps(status, ensure_ascii=False, indent=2)
    out.write_text(text, encoding="utf-8-sig")
    latest.write_text(text, encoding="utf-8-sig")
    print(
        f"[GOOGLE_NEWS_RSS_PROBE] quality={status['quality']} reason={status['reason']} "
        f"symbols={len(symbols)} items={status['item_count_total']} "
        f"db_write={status['db_write']} db_rows_saved={status['db_rows_saved']} status={latest}"
    )
    return 0 if status["quality"] in {"PASS", "WARN"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
