from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import os
import re
import sqlite3
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"
DB = ROOT / "news_trading" / "data" / "trading.db"
KST = timezone(timedelta(hours=9))

OUT_JSON = LOGS / "global_macro_rss_collect_latest.json"
OUT_CSV = LOGS / "global_macro_rss_collect_latest.csv"

SOURCE_URLS = {
    "yahoo_finance_top": "https://finance.yahoo.com/news/rssindex",
    "google_asia_markets": "https://news.google.com/rss/search?q=Asia%20stocks%20markets%20risk%20off&hl=en-US&gl=US&ceid=US:en",
    "google_ai_crowded_trades": "https://news.google.com/rss/search?q=AI%20bubble%20OR%20crowded%20trades%20OR%20tech%20profit%20taking&hl=en-US&gl=US&ceid=US:en",
    "google_south_korea_markets": "https://news.google.com/rss/search?q=South%20Korea%20stocks%20foreign%20investors%20Asia%20markets&hl=en-US&gl=US&ceid=US:en",
}

SAMPLE_ROWS = [
    {
        "source_id": "sample_global_macro",
        "source_name": "Sample Global Macro",
        "title": "Asia stocks slip as investors take profits in AI-linked technology shares",
        "description": "Foreign investors trimmed exposure to crowded semiconductor and AI trades as risk appetite cooled.",
        "link": "https://example.invalid/global-macro-sample-1",
        "published_at": "2026-07-02T08:00:00+09:00",
    },
    {
        "source_id": "sample_global_macro",
        "source_name": "Sample Global Macro",
        "title": "Defense shares outperform as funds rotate away from mega-cap technology",
        "description": "Portfolio managers cited sector rotation rather than broad liquidation across all Korean equities.",
        "link": "https://example.invalid/global-macro-sample-2",
        "published_at": "2026-07-02T08:10:00+09:00",
    },
]


def _now_kst() -> datetime:
    return datetime.now(KST)


def _clean_text(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _date8_from_published(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return _now_kst().strftime("%Y%m%d")
    try:
        dt = parsedate_to_datetime(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(KST).strftime("%Y%m%d")
    except Exception:
        pass
    digits = re.sub(r"[^0-9]", "", text)
    if len(digits) >= 8:
        return digits[:8]
    return _now_kst().strftime("%Y%m%d")


def _iso_from_published(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        dt = parsedate_to_datetime(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(KST).isoformat(timespec="seconds")
    except Exception:
        return text[:80]


def _article_key(source_id: str, title: str, link: str, published_at: str) -> str:
    raw = "|".join([source_id, title, link, published_at]).encode("utf-8", errors="ignore")
    return hashlib.sha1(raw).hexdigest()


def _fetch_text(url: str, timeout_sec: float) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 GlobalMacroRiskShadow/1.0",
            "Accept": "application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.1",
        },
    )
    with urllib.request.urlopen(req, timeout=max(1.0, float(timeout_sec))) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _parse_feed(xml_text: str, source_id: str, source_url: str, max_items: int) -> list[dict[str, Any]]:
    root = ET.fromstring(xml_text)
    items = root.findall(".//item")
    if not items:
        items = root.findall(".//{http://www.w3.org/2005/Atom}entry")
    rows: list[dict[str, Any]] = []
    for item in items[: max(1, int(max_items))]:
        title = _clean_text(item.findtext("title") or item.findtext("{http://www.w3.org/2005/Atom}title"))
        description = _clean_text(
            item.findtext("description")
            or item.findtext("summary")
            or item.findtext("{http://www.w3.org/2005/Atom}summary")
        )
        link = _clean_text(item.findtext("link"))
        if not link:
            link_node = item.find("{http://www.w3.org/2005/Atom}link")
            link = str(link_node.attrib.get("href", "") if link_node is not None else "").strip()
        published_raw = (
            item.findtext("pubDate")
            or item.findtext("published")
            or item.findtext("updated")
            or item.findtext("{http://www.w3.org/2005/Atom}published")
            or item.findtext("{http://www.w3.org/2005/Atom}updated")
            or ""
        )
        if not title:
            continue
        published_at = _iso_from_published(published_raw)
        rows.append(
            {
                "article_key": _article_key(source_id, title, link, published_at),
                "date8": _date8_from_published(published_raw),
                "source_id": source_id,
                "source_name": source_id.replace("_", " ").title(),
                "source_url": source_url,
                "title": title,
                "description": description,
                "link": link,
                "published_at": published_at,
                "language": "en",
            }
        )
    return rows


def _ensure_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS news_articles_global_macro (
            article_key TEXT PRIMARY KEY,
            date8 TEXT NOT NULL,
            source_id TEXT NOT NULL,
            source_name TEXT,
            source_url TEXT,
            title TEXT NOT NULL,
            description TEXT,
            link TEXT,
            published_at TEXT,
            language TEXT NOT NULL DEFAULT 'en',
            fetched_at TEXT NOT NULL,
            run_id TEXT NOT NULL
        )
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_news_articles_global_macro_date ON news_articles_global_macro(date8)")


def _write_db(rows: list[dict[str, Any]], run_id: str, fetched_at: str, db_path: Path) -> int:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path), timeout=max(1.0, float(os.getenv("NEWS_GLOBAL_MACRO_SQLITE_TIMEOUT_SEC", "30") or "30")))
    try:
        con.execute("PRAGMA busy_timeout=30000")
        _ensure_table(con)
        saved = 0
        for row in rows:
            con.execute(
                """
                INSERT OR REPLACE INTO news_articles_global_macro (
                    article_key, date8, source_id, source_name, source_url, title,
                    description, link, published_at, language, fetched_at, run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["article_key"],
                    row["date8"],
                    row["source_id"],
                    row.get("source_name", ""),
                    row.get("source_url", ""),
                    row["title"],
                    row.get("description", ""),
                    row.get("link", ""),
                    row.get("published_at", ""),
                    row.get("language", "en"),
                    fetched_at,
                    run_id,
                ),
            )
            saved += 1
        con.commit()
        return saved
    finally:
        con.close()


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "article_key",
        "date8",
        "source_id",
        "source_name",
        "title",
        "description",
        "link",
        "published_at",
        "language",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _sample_rows() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in SAMPLE_ROWS:
        item = dict(row)
        item["article_key"] = _article_key(item["source_id"], item["title"], item["link"], item["published_at"])
        item["date8"] = re.sub(r"[^0-9]", "", item["published_at"])[:8]
        item["source_url"] = item["link"]
        item["language"] = "en"
        out.append(item)
    return out


def collect(max_items_per_source: int, timeout_sec: float, sample_fixture: bool) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if sample_fixture:
        return _sample_rows(), []
    rows: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    for source_id, url in SOURCE_URLS.items():
        try:
            xml_text = _fetch_text(url, timeout_sec=timeout_sec)
            rows.extend(_parse_feed(xml_text, source_id, url, max_items=max_items_per_source))
        except Exception as exc:
            issues.append({"source_id": source_id, "url": url, "error": f"{type(exc).__name__}: {str(exc)[:180]}"})
    deduped = {str(row["article_key"]): row for row in rows}
    return list(deduped.values()), issues


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Collect global macro RSS headlines as a shadow-only news layer.")
    ap.add_argument("--dry-run", action="store_true", help="Do not write rows to trading.db.")
    ap.add_argument("--write-db", action="store_true", help="Create/update news_articles_global_macro in trading.db.")
    ap.add_argument("--sample-fixture", action="store_true", help="Use deterministic sample headlines for verification.")
    ap.add_argument("--max-items-per-source", type=int, default=int(os.getenv("NEWS_GLOBAL_MACRO_MAX_ITEMS", "8") or "8"))
    ap.add_argument("--timeout-sec", type=float, default=float(os.getenv("NEWS_GLOBAL_MACRO_FETCH_TIMEOUT_SEC", "8") or "8"))
    ap.add_argument("--db-path", default=str(DB), help="SQLite DB path. Defaults to the operational trading.db.")
    args = ap.parse_args(argv)

    LOGS.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    now = _now_kst()
    run_id = now.strftime("global_macro_rss_%Y%m%d_%H%M%S")
    fetched_at = now.isoformat(timespec="seconds")

    rows, issues = collect(
        max_items_per_source=max(1, int(args.max_items_per_source)),
        timeout_sec=max(1.0, float(args.timeout_sec)),
        sample_fixture=bool(args.sample_fixture),
    )
    rows.sort(key=lambda r: (str(r.get("date8", "")), str(r.get("published_at", "")), str(r.get("title", ""))), reverse=True)
    saved_rows = 0
    db_write_requested = bool(args.write_db and not args.dry_run)
    if db_write_requested and rows:
        saved_rows = _write_db(rows, run_id=run_id, fetched_at=fetched_at, db_path=Path(str(args.db_path)))

    _write_csv(OUT_CSV, rows)
    quality = "PASS" if rows else "FAIL_SOFT"
    status = {
        "generated_at": fetched_at,
        "run_id": run_id,
        "quality": quality,
        "reason": "ok" if rows else "no_rows_collected",
        "scope": "shadow_only_global_macro_rss",
        "row_count": len(rows),
        "saved_rows": int(saved_rows),
        "dry_run": bool(args.dry_run),
        "db_write_requested": db_write_requested,
        "db_table": "news_articles_global_macro" if db_write_requested else "",
        "db_path": str(Path(str(args.db_path))) if db_write_requested else "",
        "output_csv": str(OUT_CSV),
        "source_count": len(SOURCE_URLS),
        "source_issues": issues,
        "elapsed_sec": round(float(time.monotonic() - started), 3),
        "trading_effect": False,
        "gate_effect": False,
        "score_effect": False,
        "risk_lock_effect": False,
        "policy_change": False,
        "top_rows": rows[:10],
    }
    OUT_JSON.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"quality": quality, "rows": len(rows), "saved_rows": saved_rows, "output": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
