from __future__ import annotations

"""국무회의 자료 수집 파이프라인
출처: https://www.korea.kr/rss/cabinet.xml (정책브리핑, 공개 RSS)
주기: 국무회의 개최 당일 게시 (주 1~2회, 주로 화요일)

흐름:
  RSS 파싱 → 최근 N일 항목 추출 → 과제별 키워드 매칭
  → cabinet_meeting_signal_latest.json 출력
  → policy_agenda_daily.py 에서 공식 결정 부스트에 활용
"""

import hashlib
import json
import math
import os
import re
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
CACHE_DIR = ROOT / "_cache" / "cabinet_meeting"
CONFIG = ROOT / "policy_agenda_config.json"

OUT_LATEST = LOGS / "cabinet_meeting_signal_latest.json"

RSS_URL = "https://www.korea.kr/rss/cabinet.xml"
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


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_config() -> Dict[str, Any]:
    if not CONFIG.exists():
        return {}
    try:
        return json.loads(CONFIG.read_text(encoding="utf-8"))
    except Exception:
        return {}


# ── 캐시 ──────────────────────────────────────────────────

def _cache_path() -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / "cabinet_rss_latest.xml"


def _read_cache(max_age_sec: int) -> Optional[bytes]:
    p = _cache_path()
    if not p.exists():
        return None
    age = time.time() - p.stat().st_mtime
    if age > max_age_sec:
        return None
    try:
        return p.read_bytes()
    except Exception:
        return None


def _write_cache(data: bytes) -> None:
    try:
        _cache_path().write_bytes(data)
    except Exception:
        pass


# ── RSS 수집 ───────────────────────────────────────────────

def _fetch_rss(
    url: str,
    timeout: int = 20,
    max_attempts: int = 3,
    retry_backoff: float = 2.0,
    cache_ttl_sec: int = 1800,
) -> Tuple[Optional[bytes], bool, str]:
    """RSS XML 바이트 반환. (data, from_cache, reason)"""
    cached = _read_cache(cache_ttl_sec)
    if cached:
        return cached, True, "cache_hit"

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; policy-tracker/1.0)",
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
    }
    last_err = ""
    for attempt in range(1, max_attempts + 1):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read()
            _write_cache(data)
            return data, False, "ok"
        except urllib.error.HTTPError as e:
            last_err = f"http_{e.code}"
        except urllib.error.URLError as e:
            last_err = f"url_error:{e.reason}"
        except Exception as e:
            last_err = f"{type(e).__name__}"
        if attempt < max_attempts:
            time.sleep(retry_backoff * attempt)

    return None, False, f"fetch_failed:{last_err}"


# ── RSS 파싱 ───────────────────────────────────────────────

def _strip_html(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", str(s or ""))
    s = s.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&").replace("&nbsp;", " ").replace("&#13;", " ")
    return re.sub(r"\s+", " ", s).strip()


def _parse_pubdate(raw: str) -> Optional[datetime]:
    raw = str(raw or "").strip()
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        return dt.astimezone(KST)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(raw[:19], fmt).replace(tzinfo=KST)
        except Exception:
            continue
    return None


def _parse_rss_items(data: bytes) -> Tuple[List[Dict[str, Any]], str]:
    """RSS XML → 항목 리스트 반환. (items, reason)"""
    try:
        root = ET.fromstring(data)
    except ET.ParseError as e:
        return [], f"xml_parse_error:{e}"

    ns = {"atom": "http://www.w3.org/2005/Atom"}
    items: List[Dict[str, Any]] = []

    # RSS 2.0: channel/item
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

        items.append({
            "title": title,
            "description": desc,
            "text": text,
            "link": link,
            "pub_raw": pub_raw,
            "pub_dt": pub_dt,
        })

    if not items:
        return [], "no_items"
    return items, "ok"


# ── 국무회의 항목 필터 ─────────────────────────────────────

_CABINET_PATTERNS = [
    "국무회의", "국무위원", "의결", "심의", "법률안", "대통령령", "총리령",
    "각령", "고시", "훈령", "행정", "안건",
]

_CABINET_TITLE_PATTERNS = [
    re.compile(r"제\s*\d+\s*회\s*국무회의"),
    re.compile(r"국무회의\s*(결과|보고|의결|심의|개최)"),
    re.compile(r"국무회의"),
]


def _is_cabinet_item(item: Dict[str, Any]) -> bool:
    title = item.get("title", "")
    return any(p.search(title) for p in _CABINET_TITLE_PATTERNS)


def _extract_session_number(title: str) -> Optional[int]:
    m = re.search(r"제\s*(\d+)\s*회", title)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            pass
    return None


# ── 키워드 매칭 ────────────────────────────────────────────

def _keyword_hits(text: str, keywords: List[str]) -> int:
    return sum(1 for kw in keywords if kw in text)


def _score_item_against_agendas(
    text: str,
    agendas: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """기사 1건 × 과제 목록 → 과제별 히트 결과."""
    result: Dict[str, Dict[str, Any]] = {}
    for agenda in agendas:
        pos = _keyword_hits(text, agenda.get("pos_keywords", []))
        neg = _keyword_hits(text, agenda.get("neg_keywords", []))
        if pos == 0 and neg == 0:
            continue
        denom = pos + neg
        score = (pos - neg) / denom
        result[agenda["id"]] = {
            "name": agenda.get("name", ""),
            "pos_hits": pos,
            "neg_hits": neg,
            "score": round(float(score), 6),
            "sector_codes": agenda.get("sector_codes", []),
        }
    return result


# ── 집계 ──────────────────────────────────────────────────

def _aggregate_agenda_signals(
    cabinet_items: List[Dict[str, Any]],
    agendas: List[Dict[str, Any]],
    now_dt: datetime,
    max_age_days: int,
) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    """
    최근 N일 국무회의 항목에서 과제별 공식 결정 신호 집계.
    반환: (agenda_id → signal, processed_items)
    """
    cutoff = now_dt - timedelta(days=max_age_days)
    agenda_hits: Dict[str, List[float]] = {a["id"]: [] for a in agendas}
    processed: List[Dict[str, Any]] = []

    for item in cabinet_items:
        pub_dt = item.get("pub_dt")
        if pub_dt is None or pub_dt < cutoff:
            continue

        text = item.get("text", "")
        hits = _score_item_against_agendas(text, agendas)
        age_days = max(0.0, (now_dt - pub_dt).total_seconds() / 86400.0)

        matched_agendas = []
        for aid, info in hits.items():
            agenda_hits[aid].append(float(info["score"]))
            matched_agendas.append({
                "id": aid,
                "name": info.get("name", ""),
                "score": info["score"],
                "pos_hits": info["pos_hits"],
                "neg_hits": info["neg_hits"],
                "sector_codes": info.get("sector_codes", []),
            })

        processed.append({
            "title": item.get("title", ""),
            "link": item.get("link", ""),
            "pub_dt": pub_dt.isoformat(timespec="seconds") if pub_dt else None,
            "age_days": round(float(age_days), 2),
            "session_number": _extract_session_number(item.get("title", "")),
            "matched_agendas": matched_agendas,
        })

    # 과제별 평균 점수 → 공식 결정 신호
    signals: Dict[str, Dict[str, Any]] = {}
    for agenda in agendas:
        aid = agenda["id"]
        scores = agenda_hits[aid]
        if not scores:
            signals[aid] = {
                "name": agenda.get("name", ""),
                "score": 0.0,
                "mention_count": 0,
                "official_decision": False,
                "sector_codes": agenda.get("sector_codes", []),
            }
        else:
            avg = sum(scores) / len(scores)
            signals[aid] = {
                "name": agenda.get("name", ""),
                "score": round(float(max(-1.0, min(1.0, avg))), 6),
                "mention_count": int(len(scores)),
                "official_decision": bool(avg > 0.0),  # 긍정 언급 = 공식 결정 진행 중
                "sector_codes": agenda.get("sector_codes", []),
            }

    return signals, processed


def _build_sector_boost(signals: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
    """sector_code → 국무회의 기반 부스트 스코어 (0.0 ~ 1.0)."""
    sector_scores: Dict[str, List[float]] = {}
    for info in signals.values():
        if not info.get("official_decision"):
            continue
        score = float(info.get("score", 0.0))
        for sc in info.get("sector_codes", []):
            sc = str(sc).strip()
            if not sc:
                continue
            if sc not in sector_scores:
                sector_scores[sc] = []
            sector_scores[sc].append(score)

    return {
        sc: round(float(sum(v) / len(v)), 6)
        for sc, v in sector_scores.items()
        if v
    }


# ── 메인 ──────────────────────────────────────────────────

def main() -> int:
    config = _load_config()
    if not config or "agendas" not in config:
        _log_print("[CABINET] config missing:", str(CONFIG))
        return 1

    agendas: List[Dict[str, Any]] = config.get("agendas", [])
    max_age_days = int(os.getenv("CABINET_MAX_AGE_DAYS", "14"))
    cache_ttl_sec = int(os.getenv("CABINET_CACHE_TTL_SEC", "1800"))

    now_dt = _now_kst()

    # RSS 수집
    rss_data, from_cache, fetch_reason = _fetch_rss(
        RSS_URL,
        cache_ttl_sec=cache_ttl_sec,
    )

    if rss_data is None:
        _log_print(f"[CABINET] RSS fetch failed: {fetch_reason}")
        status: Dict[str, Any] = {
            "generated_at": now_dt.isoformat(timespec="seconds"),
            "source": RSS_URL,
            "fetch_reason": fetch_reason,
            "from_cache": from_cache,
            "items_total": 0,
            "items_cabinet": 0,
            "items_processed": 0,
            "agenda_signals": {},
            "sector_boost": {},
            "processed_items": [],
            "quality": "FAIL",
        }
        _write_json(OUT_LATEST, status)
        return 1

    # RSS 파싱
    all_items, parse_reason = _parse_rss_items(rss_data)
    cabinet_items = [it for it in all_items if _is_cabinet_item(it)]

    _log_print(
        f"[CABINET] items_total={len(all_items)} "
        f"cabinet={len(cabinet_items)} "
        f"from_cache={from_cache} "
        f"fetch={fetch_reason}"
    )

    # 과제별 신호 집계
    agenda_signals, processed = _aggregate_agenda_signals(
        cabinet_items=cabinet_items,
        agendas=agendas,
        now_dt=now_dt,
        max_age_days=max_age_days,
    )

    sector_boost = _build_sector_boost(agenda_signals)

    decided_count = sum(
        1 for v in agenda_signals.values() if v.get("official_decision")
    )
    quality = "PASS" if decided_count > 0 else ("WARN" if cabinet_items else "FAIL")

    status = {
        "generated_at": now_dt.isoformat(timespec="seconds"),
        "source": RSS_URL,
        "fetch_reason": fetch_reason,
        "from_cache": from_cache,
        "max_age_days": max_age_days,
        "items_total": int(len(all_items)),
        "items_cabinet": int(len(cabinet_items)),
        "items_processed": int(len(processed)),
        "decided_agendas": int(decided_count),
        "quality": quality,
        "agenda_signals": {
            aid: {
                "name": v.get("name", ""),
                "score": v.get("score", 0.0),
                "mention_count": v.get("mention_count", 0),
                "official_decision": v.get("official_decision", False),
                "sector_codes": v.get("sector_codes", []),
            }
            for aid, v in agenda_signals.items()
        },
        "sector_boost": sector_boost,
        "processed_items": processed,
    }

    _write_json(OUT_LATEST, status)

    # 날짜별 스냅샷
    date8 = now_dt.strftime("%Y%m%d")
    _write_json(LOGS / f"cabinet_meeting_signal_{date8}.json", status)

    _log_print(
        f"[CABINET] decided={decided_count} "
        f"sector_boost_codes={list(sector_boost.keys())} "
        f"quality={quality}"
    )
    _log_print(f"[CABINET] wrote {OUT_LATEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
