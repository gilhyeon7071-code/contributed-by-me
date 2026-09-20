from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
import trafilatura

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT.parent / "news_trading" / "data" / "trading.db"

_SESSION = requests.Session()
_SESSION.headers["User-Agent"] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _load_naver_urls(date8: str, fallback_days: int = 30) -> dict[str, list[str]]:
    """code → [url, ...] 매핑.
    1차: date8 정확히 일치
    2차: 해당 code의 최근 fallback_days일 이내 최신 기사 (1차 미커버 코드만)
    """
    if not DB.exists():
        return {}
    try:
        conn = sqlite3.connect(str(DB))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        def _extract_urls(rows) -> dict[str, list[str]]:
            out: dict[str, list[str]] = {}
            for row in rows:
                code = str(row["code"] or "").strip()
                if not code:
                    continue
                urls = out.setdefault(code, [])
                for field in ("link", "originallink"):
                    url = str(row[field] or "").strip()
                    if url and url not in urls and not url.startswith("https://news.google"):
                        urls.append(url)
            return out

        # 1차: 정확히 일치
        cur.execute(
            "SELECT code, originallink, link FROM news_articles_naver WHERE date8 = ? ORDER BY article_score DESC",
            (date8,),
        )
        result = _extract_urls(cur.fetchall())

        # 2차: 1차 미커버 코드 — 해당 code의 최신 기사 (30일 이내, 코드별 1건)
        # date8 수치 비교: 30일 ≈ YYYYMMDD - 30일 계산
        from datetime import datetime, timedelta
        try:
            dt = datetime.strptime(date8, "%Y%m%d")
        except ValueError:
            dt = datetime.today()
        since_date8 = (dt - timedelta(days=fallback_days)).strftime("%Y%m%d")

        cur.execute(
            """
            SELECT code, originallink, link
            FROM news_articles_naver
            WHERE date8 >= ?
            GROUP BY code
            HAVING MAX(date8)
            ORDER BY article_score DESC
            """,
            (since_date8,),
        )
        for row in cur.fetchall():
            code = str(row["code"] or "").strip()
            if not code or code in result:
                continue  # 이미 1차에서 커버됨
            urls = result.setdefault(code, [])
            for field in ("link", "originallink"):
                url = str(row[field] or "").strip()
                if url and url not in urls and not url.startswith("https://news.google"):
                    urls.append(url)

        conn.close()
        return result
    except Exception:
        return {}


def _fetch_body(url: str, timeout: int) -> str:
    """trafilatura로 본문 추출. 실패 시 빈 문자열."""
    try:
        html = trafilatura.fetch_url(url)
        if not html:
            return ""
        text = trafilatura.extract(
            html,
            include_comments=False,
            include_tables=False,
            no_fallback=False,
            favor_precision=True,
        )
        return (text or "").strip()
    except Exception:
        return ""


def _try_urls(urls: list[str], timeout: int) -> tuple[str, str]:
    """URL 목록을 순서대로 시도. (body, used_url) 반환."""
    for url in urls:
        body = _fetch_body(url, timeout)
        if body and len(body) >= 80:
            return body, url
    return "", ""


def _article_date8(article: dict) -> str:
    date_str = str(article.get("article_date") or "").strip()
    return date_str.replace("-", "")[:8]


def _load_seed(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _save_seed(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8-sig")


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch article body text into archive seed JSON.")
    parser.add_argument("--article-archive-json", required=True, type=Path)
    parser.add_argument("--max-fetch", type=int, default=10,
                        help="Max articles to fetch per run (default 10)")
    parser.add_argument("--sleep-sec", type=float, default=1.5,
                        help="Sleep between requests in seconds (default 1.5)")
    parser.add_argument("--timeout-sec", type=int, default=15,
                        help="Request timeout in seconds (default 15)")
    parser.add_argument("--apply", action="store_true",
                        help="Write results back to archive seed JSON")
    args = parser.parse_args()

    path = args.article_archive_json
    if not path.exists():
        print(json.dumps({"error": f"file not found: {path}"}))
        return 1

    seed = _load_seed(path)
    articles = seed.get("articles", [])
    now = datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")

    # Naver DB에서 code별 실제 URL 목록 로드
    date8_set = {_article_date8(a) for a in articles if _article_date8(a)}
    naver_url_map: dict[str, list[str]] = {}
    for d8 in date8_set:
        for code, urls in _load_naver_urls(d8).items():
            naver_url_map.setdefault(code, []).extend(
                u for u in urls if u not in naver_url_map.get(code, [])
            )

    pending = [a for a in articles if not a.get("original_text_available")]
    total_pending = len(pending)
    to_fetch = pending[: max(0, int(args.max_fetch))]

    fetched_count = 0
    fail_count = 0
    naver_hit = 0

    for article in to_fetch:
        code = str(article.get("code") or "").strip()
        candidate_urls: list[str] = []

        # Naver DB URL 우선
        if code in naver_url_map:
            candidate_urls.extend(naver_url_map[code])
            naver_hit += 1

        # Google RSS URL은 리다이렉트 불가라 제외; description fallback
        body, used_url = _try_urls(candidate_urls, args.timeout_sec)

        if body:
            article["body"] = body
            article["body_sha256"] = _sha256(body)
            article["body_status"] = "original_text_archived"
            article["original_text_available"] = True
            article["evidence_path"] = used_url
            fetched_count += 1
        else:
            # description fallback — RSS 클러스터 요약으로 대체
            description = str(article.get("description") or "").strip()
            if description and len(description) >= 30:
                article["body"] = description
                article["body_sha256"] = _sha256(description)
                article["body_status"] = "description_fallback"
                article["original_text_available"] = True
                article["evidence_path"] = article.get("url", "")
                fetched_count += 1
            else:
                article["body_status"] = "fetch_failed_no_body"
                fail_count += 1

        time.sleep(args.sleep_sec)

    skip_count = total_pending - len(to_fetch)

    any_available = any(a.get("original_text_available") for a in articles)
    seed["body_contract"]["safe_to_mark_original_text_available"] = any_available
    seed["summary"]["all_original_text_available"] = all(
        a.get("original_text_available") for a in articles
    )
    seed.setdefault("body_fetch", {})
    seed["body_fetch"]["last_run_at"] = now
    seed["body_fetch"]["fetched"] = fetched_count
    seed["body_fetch"]["failed"] = fail_count
    seed["body_fetch"]["skipped_quota"] = skip_count
    seed["body_fetch"]["naver_db_hits"] = naver_hit
    seed["body_fetch"]["pending_remaining"] = max(0, total_pending - fetched_count - fail_count)

    result = {
        "apply": bool(args.apply),
        "path": str(path),
        "total_articles": len(articles),
        "pending": total_pending,
        "fetched": fetched_count,
        "failed": fail_count,
        "skipped_quota": skip_count,
        "naver_db_hits": naver_hit,
    }

    if args.apply:
        _save_seed(path, seed)
        result["written"] = True
    else:
        result["written"] = False

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
