from __future__ import annotations

"""각 부처 보도자료 수집 파이프라인

소스 (실제 확인된 RSS):
  기획재정부    GET  https://mofe.go.kr/com/detailRssTagService.do?bbsId=MOSFBBS_000000000028
  산업통상자원부 POST https://www.motir.go.kr/kor/article/ATCL3f49a5a8c/rss
  과기정통부    GET  https://www.msit.go.kr/user/rss/rss.do?bbsSeqNo=94
  국토교통부    GET  https://www.molit.go.kr/dev/board/board_rss.jsp?rss_id=NEWS (쿠키)
  금융위원회    GET  https://www.fsc.go.kr/about/fsc_bbs_rss/?fid=0111
  보건복지부    GET  https://www.mohw.go.kr/rss/board.es?mid=a10503000000&bid=0027
  방위사업청    RSS없음 → Naver News DB 대체
  해양수산부    GET  https://www.mof.go.kr/doc/ko/rssFeed.do?bbsSeq=10

출력:
  ministry_press_signal_latest.json
  ministry_press_signal_YYYYMMDD.json
"""

import http.cookiejar
import json
import math
import os
import re
import sqlite3
import time
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import logging

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
CACHE_DIR = ROOT / "_cache" / "ministry_press"
POLICY_CONFIG = ROOT / "policy_agenda_config.json"
MINISTRY_CONFIG = ROOT / "ministry_rss_config.json"
NEWS_DB = ROOT / "news_trading" / "data" / "trading.db"

OUT_LATEST = LOGS / "ministry_press_signal_latest.json"
KST = timezone(timedelta(hours=9))

logger = logging.getLogger(__name__)


def _log_print(*args: Any, **kwargs: Any) -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="[%(levelname)s] %(asctime)s %(name)s - %(message)s",
        )
    sep = kwargs.get("sep", " ")
    try:
        msg = sep.join(str(a) for a in args)
    except Exception:
        msg = " ".join(str(a) for a in args)
    logger.info(msg)


def _now_kst() -> datetime:
    return datetime.now(KST)


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


# ── 캐시 ──────────────────────────────────────────────────

def _cache_path(ministry_id: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{ministry_id}_rss.xml"


def _read_cache(ministry_id: str, ttl_sec: int) -> Optional[bytes]:
    p = _cache_path(ministry_id)
    if not p.exists():
        return None
    if time.time() - p.stat().st_mtime > ttl_sec:
        return None
    try:
        return p.read_bytes()
    except Exception:
        return None


def _write_cache(ministry_id: str, data: bytes) -> None:
    try:
        _cache_path(ministry_id).write_bytes(data)
    except Exception:
        pass


# ── HTTP 수집 ──────────────────────────────────────────────

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
    "Accept-Language": "ko-KR,ko;q=0.9",
}


def _fetch_rss(
    ministry_id: str,
    url: str,
    method: str = "GET",
    cookie_seed_url: Optional[str] = None,
    cache_ttl_sec: int = 3600,
    timeout: int = 20,
    max_attempts: int = 3,
) -> Tuple[Optional[bytes], bool, str]:
    """RSS XML 바이트 반환. (data, from_cache, reason)
    CookieJar 기반 opener를 사용해 세션 쿠키 및 307 리다이렉트를 자동 처리.
    """
    cached = _read_cache(ministry_id, cache_ttl_sec)
    if cached:
        return cached, True, "cache_hit"

    headers = dict(_HEADERS)
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(cj),
    )

    if cookie_seed_url:
        try:
            seed_req = urllib.request.Request(cookie_seed_url, headers=headers)
            with opener.open(seed_req, timeout=timeout):
                pass
        except Exception:
            pass

    last_err = ""
    for attempt in range(1, max_attempts + 1):
        try:
            if method.upper() == "POST":
                req = urllib.request.Request(url, data=b"", headers=headers, method="POST")
            else:
                req = urllib.request.Request(url, headers=headers)

            with opener.open(req, timeout=timeout) as resp:
                raw = resp.read()
            _write_cache(ministry_id, raw)
            return raw, False, "ok"

        except urllib.error.HTTPError as e:
            last_err = f"http_{e.code}"
        except urllib.error.URLError as e:
            last_err = f"url_error:{e.reason}"
        except Exception as e:
            last_err = f"{type(e).__name__}"

        if attempt < max_attempts:
            time.sleep(2.0 * attempt)

    return None, False, f"fetch_failed:{last_err}"


# ── RSS 파싱 ───────────────────────────────────────────────

def _strip_html(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", str(s or ""))
    for ent, rep in [("&lt;", "<"), ("&gt;", ">"), ("&amp;", "&"),
                     ("&nbsp;", " "), ("&#13;", " "), ("&quot;", '"')]:
        s = s.replace(ent, rep)
    return re.sub(r"\s+", " ", s).strip()


def _parse_pubdate(raw: str) -> Optional[datetime]:
    raw = str(raw or "").strip()
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
        return dt.astimezone(KST)
    except Exception:
        pass
    # (fmt, expected_str_length) — len(fmt) is the literal format string length, NOT the date length
    for fmt, n in [
        ("%Y-%m-%d %H:%M:%S", 19),
        ("%Y-%m-%dT%H:%M:%S", 19),
        ("%Y-%m-%d", 10),
        ("%Y.%m.%d", 10),
    ]:
        try:
            return datetime.strptime(raw[:n], fmt).replace(tzinfo=KST)
        except Exception:
            continue
    return None


def _parse_rss(data: bytes) -> Tuple[List[Dict[str, Any]], str]:
    try:
        # BOM 제거
        if data.startswith(b"\xef\xbb\xbf"):
            data = data[3:]
        root = ET.fromstring(data)
    except ET.ParseError as e:
        # 인코딩 선언 제거 후 재시도
        try:
            cleaned = re.sub(rb"<\?xml[^?]*\?>", b"", data, count=1)
            root = ET.fromstring(cleaned)
        except Exception:
            return [], f"xml_parse_error:{e}"

    items: List[Dict[str, Any]] = []
    for item in root.findall(".//item"):
        title_el = item.find("title")
        desc_el = item.find("description")
        link_el = item.find("link")
        pub_el = item.find("pubDate")

        title = _strip_html(title_el.text if title_el is not None else "")
        desc = _strip_html(desc_el.text if desc_el is not None else "")
        link = (link_el.text or "").strip() if link_el is not None else ""
        pub_raw = (pub_el.text or "").strip() if pub_el is not None else ""
        pub_dt = _parse_pubdate(pub_raw)
        text = f"{title} {desc}".strip()

        if not text:
            continue
        items.append({
            "title": title,
            "description": desc,
            "text": text,
            "link": link,
            "pub_dt": pub_dt,
        })

    return items, ("ok" if items else "no_items")


# ── Naver News DB 대체 (방위사업청 등 RSS 없는 부처) ─────────

def _fetch_from_news_db(
    query_keywords: List[str],
    max_age_hours: float = 168,
) -> List[Dict[str, Any]]:
    """뉴스 DB에서 키워드 포함 기사를 부처 보도자료 대용으로 수집."""
    if not NEWS_DB.exists():
        _log_print(f"[MINISTRY] news DB not found: {NEWS_DB}")
        return []
    now_dt = _now_kst()
    cutoff = now_dt - timedelta(hours=max_age_hours)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    try:
        con = sqlite3.connect(str(NEWS_DB))
        col_info = con.execute('PRAGMA table_info("news_articles_naver")').fetchall()
        cols = {str(r[1]) for r in col_info}
        if not cols:
            _log_print("[MINISTRY] news_articles_naver table not found in DB")
            con.close()
            return []
        title_sql = '"title"' if "title" in cols else "NULL"
        desc_sql = '"description"' if "description" in cols else "NULL"
        pub_col = "published_at" if "published_at" in cols else "fetched_at"
        pub_sql = f'"{pub_col}"'
        rows = con.execute(
            f'SELECT {title_sql} AS title, {desc_sql} AS desc, {pub_sql} AS pub '
            f'FROM "news_articles_naver" WHERE {pub_sql} >= ?',
            (cutoff_str,)
        ).fetchall()
        con.close()
        _log_print(
            f"[MINISTRY] naver DB rows in window({max_age_hours:.0f}h): {len(rows)}"
        )
    except Exception as e:
        _log_print(f"[MINISTRY] news DB error: {e}")
        return []

    items: List[Dict[str, Any]] = []
    for title_raw, desc_raw, pub_raw in rows:
        title = _strip_html(title_raw or "")
        desc = _strip_html(desc_raw or "")
        text = f"{title} {desc}".strip()
        if not any(kw in text for kw in query_keywords):
            continue
        pub_dt = _parse_pubdate(str(pub_raw or ""))
        if pub_dt is None or pub_dt < cutoff:
            continue
        items.append({"title": title, "description": desc, "text": text,
                      "link": "", "pub_dt": pub_dt})
    return items


# ── 키워드 매칭 ────────────────────────────────────────────

def _keyword_hits(text: str, keywords: List[str]) -> int:
    return sum(1 for kw in keywords if kw in text)


def _score_items(
    items: List[Dict[str, Any]],
    agenda: Dict[str, Any],
    now_dt: datetime,
    max_age_days: int,
    half_life_hours: float = 48.0,
) -> Dict[str, Any]:
    """항목 목록을 단일 과제 기준으로 점수화."""
    cutoff = now_dt - timedelta(days=max_age_days)
    weighted_scores: List[float] = []
    total_weight = 0.0
    hit_count = 0

    for item in items:
        pub_dt = item.get("pub_dt")
        if pub_dt is None:
            pub_dt = now_dt  # pubDate 없는 항목은 최신으로 간주
        elif pub_dt < cutoff:
            continue
        text = item.get("text", "")
        pos = _keyword_hits(text, agenda.get("pos_keywords", []))
        neg = _keyword_hits(text, agenda.get("neg_keywords", []))
        if pos == 0 and neg == 0:
            continue
        denom = pos + neg
        raw_score = (pos - neg) / denom

        age_h = max(0.0, (now_dt - pub_dt).total_seconds() / 3600.0)
        w = 0.5 ** (age_h / half_life_hours) if half_life_hours > 0 else 1.0
        weighted_scores.append(raw_score * w)
        total_weight += w
        hit_count += 1

    if not weighted_scores or total_weight <= 0:
        return {"score": 0.0, "hit_count": 0, "has_signal": False}

    score = sum(weighted_scores) / total_weight
    score = max(-1.0, min(1.0, score))
    return {
        "score": round(float(score), 6),
        "hit_count": int(hit_count),
        "has_signal": abs(score) > 1e-6,
    }


# ── 메인 ──────────────────────────────────────────────────

def main() -> int:
    policy_cfg = _load_json(POLICY_CONFIG)
    ministry_cfg = _load_json(MINISTRY_CONFIG)
    if not policy_cfg or not ministry_cfg:
        _log_print("[MINISTRY] config missing")
        return 1

    agendas: List[Dict[str, Any]] = policy_cfg.get("agendas", [])
    agenda_map: Dict[str, Dict[str, Any]] = {a["id"]: a for a in agendas}
    ministries: List[Dict[str, Any]] = ministry_cfg.get("ministries", [])
    max_age_days = int(ministry_cfg.get("max_age_days", 7))
    cache_ttl = int(ministry_cfg.get("cache_ttl_sec", 3600))
    boost_mult = float(ministry_cfg.get("ministry_boost_multiplier", 1.5))

    now_dt = _now_kst()

    # 부처별 결과 수집
    ministry_results: List[Dict[str, Any]] = []
    # agenda_id → 부처별 점수 리스트 (최종 집계용)
    agenda_ministry_scores: Dict[str, List[Tuple[float, float]]] = {
        a["id"]: [] for a in agendas
    }

    for ministry in ministries:
        if not ministry.get("enabled", True):
            continue

        mid = str(ministry.get("id", ""))
        mname = str(ministry.get("name", mid))
        method = str(ministry.get("method", "GET")).upper()
        agenda_ids: List[str] = ministry.get("agenda_ids", [])
        m_weight = float(ministry.get("weight", 1.0))

        # 수집
        if method == "NAVER_NEWS":
            naver_kws: List[str] = ministry.get("naver_query", "").split()
            items = _fetch_from_news_db(naver_kws, max_age_hours=max_age_days * 24.0)
            fetch_reason = f"naver_db:{len(items)}items"
            from_cache = False
        else:
            url = str(ministry.get("url") or "")
            cookie_seed = ministry.get("cookie_seed_url")
            data, from_cache, fetch_reason = _fetch_rss(
                ministry_id=mid,
                url=url,
                method=method,
                cookie_seed_url=cookie_seed,
                cache_ttl_sec=cache_ttl,
            )
            if data is None:
                _log_print(f"[MINISTRY] {mname} fetch failed: {fetch_reason}")
                ministry_results.append({
                    "id": mid, "name": mname,
                    "fetch_reason": fetch_reason, "items": 0,
                    "agenda_scores": {}, "status": "FAIL",
                })
                continue
            items, parse_reason = _parse_rss(data)
            if not items:
                _log_print(f"[MINISTRY] {mname} parse: {parse_reason}")
                ministry_results.append({
                    "id": mid, "name": mname,
                    "fetch_reason": fetch_reason, "parse_reason": parse_reason,
                    "items": 0, "agenda_scores": {}, "status": "WARN",
                })
                continue

        # 과제별 점수
        agenda_scores: Dict[str, Any] = {}
        for aid in agenda_ids:
            agenda = agenda_map.get(aid)
            if not agenda:
                continue
            result = _score_items(items, agenda, now_dt, max_age_days)
            agenda_scores[aid] = {
                "name": agenda.get("name", ""),
                "score": result["score"],
                "hit_count": result["hit_count"],
                "has_signal": result["has_signal"],
                "sector_codes": agenda.get("sector_codes", []),
            }
            if result["has_signal"]:
                agenda_ministry_scores[aid].append(
                    (result["score"] * m_weight, m_weight)
                )

        status = "PASS" if any(v["has_signal"] for v in agenda_scores.values()) else "WARN"
        _log_print(
            f"[MINISTRY] {mname} items={len(items)} "
            f"from_cache={from_cache} status={status}"
        )
        ministry_results.append({
            "id": mid,
            "name": mname,
            "fetch_reason": fetch_reason,
            "from_cache": from_cache,
            "items": int(len(items)),
            "agenda_scores": agenda_scores,
            "status": status,
        })

    # 과제별 통합 점수 (여러 부처 평균)
    agenda_signal: Dict[str, Dict[str, Any]] = {}
    for agenda in agendas:
        aid = agenda["id"]
        pairs = agenda_ministry_scores[aid]
        if not pairs:
            agenda_signal[aid] = {
                "name": agenda.get("name", ""),
                "score": 0.0,
                "ministry_count": 0,
                "has_signal": False,
                "sector_codes": agenda.get("sector_codes", []),
            }
        else:
            total_w = sum(w for _, w in pairs)
            score = sum(s for s, _ in pairs) / total_w if total_w > 0 else 0.0
            score = max(-1.0, min(1.0, score))
            agenda_signal[aid] = {
                "name": agenda.get("name", ""),
                "score": round(float(score), 6),
                "ministry_count": int(len(pairs)),
                "has_signal": abs(score) > 1e-6,
                "sector_codes": agenda.get("sector_codes", []),
            }

    # 섹터별 부스트 맵
    sector_boost: Dict[str, float] = {}
    for info in agenda_signal.values():
        if not info["has_signal"]:
            continue
        score = float(info["score"])
        for sc in info.get("sector_codes", []):
            sc = str(sc).strip()
            if not sc:
                continue
            if sc not in sector_boost:
                sector_boost[sc] = []  # type: ignore[assignment]
            sector_boost[sc].append(score)  # type: ignore[union-attr]

    sector_boost_final: Dict[str, float] = {}
    for sc, scores in sector_boost.items():  # type: ignore[assignment]
        avg = sum(scores) / len(scores)  # type: ignore[arg-type]
        sector_boost_final[sc] = round(max(-1.0, min(1.0, avg)), 6)

    active = sum(1 for v in agenda_signal.values() if v["has_signal"])
    quality = "PASS" if active > 0 else "WARN"

    status_out: Dict[str, Any] = {
        "generated_at": now_dt.isoformat(timespec="seconds"),
        "max_age_days": max_age_days,
        "ministry_boost_multiplier": boost_mult,
        "ministry_count": len(ministry_results),
        "active_agendas": int(active),
        "quality": quality,
        "agenda_signal": agenda_signal,
        "sector_boost": sector_boost_final,
        "ministry_detail": ministry_results,
    }

    _write_json(OUT_LATEST, status_out)
    date8 = now_dt.strftime("%Y%m%d")
    _write_json(LOGS / f"ministry_press_signal_{date8}.json", status_out)

    _log_print(
        f"[MINISTRY] ministries={len(ministry_results)} "
        f"active_agendas={active} "
        f"sector_boost={list(sector_boost_final.keys())} "
        f"quality={quality}"
    )
    _log_print(f"[MINISTRY] wrote {OUT_LATEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
