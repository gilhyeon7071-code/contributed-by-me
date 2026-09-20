from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
DB = ROOT / "news_trading" / "data" / "trading.db"
KST = timezone(timedelta(hours=9))
VERSION = "IMPLICATION_RULE_V1"


POS_TERMS = (
    "상승", "급등", "강세", "신고가", "호재", "실적개선", "흑자", "수주", "계약",
    "공급", "제휴", "승인", "인가", "목표가 상향", "목표가상향", "매수", "성장",
    "돌파", "확대", "회복", "반등", "증가", "랠리", "저평가",
)
NEG_TERMS = (
    "하락", "급락", "약세", "악재", "실적악화", "적자", "제재", "소송", "수사",
    "유상증자", "감자", "부진", "우려", "리스크", "축소", "감소", "매도",
    "목표가 하향", "목표가하향", "경고", "난항", "손실",
)
MARKET_TERMS = (
    "코스피", "코스닥", "증시", "지수", "나스닥", "s&p", "다우", "시장", "외국인",
    "기관", "개미", "시총", "금리", "환율", "원달러", "유가", "국채", "fed", "fomc",
)
SECTOR_TERMS = (
    "반도체", "hbm", "메모리", "전력", "전선", "ai", "데이터센터", "조선", "방산",
    "배터리", "2차전지", "자동차", "바이오", "로봇", "화장품", "증권주",
)
OVERHEAT_TERMS = ("과열", "추격매수", "부담", "고평가", "반쪽 축제", "차익실현", "속도조절")
REVERSAL_TERMS = (
    "우려 해소", "악재 해소", "불확실성 해소", "리스크 해소", "실적악화 우려 해소",
    "적자 탈피", "적자폭 축소", "손실 축소", "부진 탈피", "감소세 둔화",
)
# 구조적·메가트렌드 키워드 — 1~6개월 시계 (company 단발 이슈와 구분)
LONG_TERMS = (
    "공급망", "공급망 재편", "탈탄소", "탈중국", "에너지전환", "에너지 전환",
    "ai전환", "디지털전환", "디지털 전환", "전기차 전환", "인구 구조",
    "장기 계획", "5개년", "로드맵", "국가 전략", "구조적", "패러다임 전환",
    "리쇼어링", "온쇼어링", "탈세계화", "친환경 전환", "넷제로", "탄소중립 정책",
)
RESEARCH_SOURCES = {"NAVER_FINANCE_RESEARCH"}
# 리서치 보고서 특화 방향성 키워드 (일반 POS/NEG_TERMS과 별도)
RESEARCH_POS_TERMS = (
    "목표가 상향", "목표가상향", "목표주가 상향", "목표주가상향",
    "투자의견 상향", "투자의견상향", "매수 의견", "적극매수",
    "실적 상향", "실적상향", "어닝 서프라이즈", "어닝서프라이즈",
    "outperform", "buy", "strong buy",
)
RESEARCH_NEG_TERMS = (
    "목표가 하향", "목표가하향", "목표주가 하향", "목표주가하향",
    "투자의견 하향", "투자의견하향", "매도 의견",
    "실적 하향", "실적하향", "어닝 쇼크", "어닝쇼크",
    "underperform", "sell", "중립 하향",
)


def _now_kst() -> datetime:
    return datetime.now(KST)


def _norm_date8(v: object) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s[:8] if len(s) >= 8 else ""


def _table_columns(con: sqlite3.Connection, table: str) -> List[str]:
    return [str(r[1]) for r in con.execute(f'PRAGMA table_info("{table}")').fetchall()]


def _ensure_columns(con: sqlite3.Connection) -> None:
    existing = set(_table_columns(con, "news_articles_naver"))
    additions = {
        "implication_scope": "TEXT",
        "implication_direction": "TEXT",
        "implication_strength": "REAL",
        "implication_confidence": "REAL",
        "implication_horizon": "TEXT",
        "implication_action": "TEXT",
        "implication_score": "REAL",
        "implication_reason": "TEXT",
        "implication_scored_at": "TEXT",
        "implication_version": "TEXT",
    }
    for col, typ in additions.items():
        if col not in existing:
            con.execute(f"ALTER TABLE news_articles_naver ADD COLUMN {col} {typ}")
    con.commit()


def _count_terms(text_l: str, terms: Tuple[str, ...]) -> int:
    return sum(1 for t in terms if t.lower() in text_l)


def _clean_name(name: object) -> str:
    s = str(name or "").strip()
    if s.lower() in {"nan", "none", "null"}:
        return ""
    return s


def _classify(row: Dict[str, Any]) -> Dict[str, Any]:
    title = str(row.get("title") or "").strip()
    desc = str(row.get("description") or "").strip()
    source = str(row.get("source") or "").strip().upper()
    name = _clean_name(row.get("name"))
    code = str(row.get("code") or "").strip()
    tags = str(row.get("entity_tags") or "").strip()
    text = " ".join([title, desc, tags, source])
    text_l = text.lower()

    pos_hits = int(row.get("pos_hits") or 0)
    neg_hits = int(row.get("neg_hits") or 0)
    pos_hits += _count_terms(text_l, POS_TERMS)
    neg_hits += _count_terms(text_l, NEG_TERMS)
    reversal_hits = _count_terms(text_l, REVERSAL_TERMS)
    if reversal_hits > 0:
        neg_hits = max(0, neg_hits - (reversal_hits * 2))
        pos_hits += reversal_hits
    market_hits = _count_terms(text_l, MARKET_TERMS)
    sector_hits = _count_terms(text_l, SECTOR_TERMS)
    overheat_hits = _count_terms(text_l, OVERHEAT_TERMS)
    long_hits = _count_terms(text_l, LONG_TERMS)
    company_title_hit = bool((name and name in title) or (code and code in title))
    company_hit = bool(company_title_hit or (name and name in desc) or (code and code in desc))

    article_score = row.get("article_score")
    llm_score = row.get("llm_score")
    try:
        base_score = float(llm_score if llm_score is not None else article_score)
    except Exception:
        base_score = 0.0
    base_score = max(-1.0, min(1.0, base_score))

    if source in RESEARCH_SOURCES:
        scope = "company" if company_hit else "sector"
        confidence = 0.75
        horizon = "medium"
    elif long_hits >= 2 and not company_title_hit:
        # 구조적·메가트렌드 뉴스: 복수 장기 키워드 + 특정 종목 제목 아님
        scope = "macro" if market_hits >= sector_hits else "sector"
        confidence = 0.58
        horizon = "long"
    elif market_hits >= max(1, sector_hits) and not company_title_hit:
        scope = "macro" if any(t in text_l for t in ("금리", "환율", "유가", "fed", "fomc", "국채")) else "market"
        confidence = 0.65
        horizon = "short"
    elif sector_hits > 0 and not company_title_hit:
        scope = "sector"
        confidence = 0.62
        horizon = "short"
    elif company_hit and (pos_hits or neg_hits or abs(base_score) >= 0.25):
        scope = "company"
        confidence = 0.70 if company_title_hit else 0.52
        horizon = "short"
    else:
        scope = "company" if company_hit else "market"
        confidence = 0.45
        horizon = "intraday"

    raw_direction = float(pos_hits - neg_hits)
    if abs(base_score) >= 0.15:
        raw_direction += base_score * 2.0
    if overheat_hits:
        raw_direction -= 1.0
    # 리서치 보고서 전용: 목표가/투자의견 키워드로 방향성 보정
    if source in RESEARCH_SOURCES:
        r_pos = _count_terms(text_l, RESEARCH_POS_TERMS)
        r_neg = _count_terms(text_l, RESEARCH_NEG_TERMS)
        raw_direction += (r_pos - r_neg) * 1.5
    if raw_direction > 0.25:
        direction = "positive"
    elif raw_direction < -0.25:
        direction = "negative"
    else:
        direction = "neutral"

    strength_seed = max(abs(base_score), min(1.0, abs(raw_direction) / 4.0))
    if source in RESEARCH_SOURCES:
        strength_seed = max(strength_seed, 0.45)
    if overheat_hits:
        strength_seed = max(strength_seed, 0.45)
    strength = max(0.0, min(1.0, strength_seed))
    strong_company_body_negative = bool(
        scope == "company"
        and company_hit
        and confidence >= 0.52
        and strength >= 0.65
        and (neg_hits >= 2 or base_score <= -0.65)
    )

    if direction == "neutral" or strength < 0.20:
        action = "ignore"
    elif scope in {"market", "macro"}:
        action = "reduce_size" if direction == "negative" else "watch"
    elif overheat_hits:
        action = "watch"
    elif direction == "positive":
        action = "boost"
    elif scope == "company" and strength >= 0.65 and (confidence >= 0.65 or strong_company_body_negative):
        action = "block"
    else:
        action = "penalize"

    if action == "boost":
        multiplier = 1.0 if scope == "company" else 0.55
        if source in RESEARCH_SOURCES:
            multiplier *= 0.75
        implication_score = strength * confidence * multiplier
    elif action == "penalize":
        multiplier = 1.0 if scope == "company" else 0.55
        implication_score = -strength * confidence * multiplier
    elif action == "block":
        implication_score = -0.75
    else:
        implication_score = 0.0

    reason_bits = [scope, direction, action]
    if source in RESEARCH_SOURCES:
        reason_bits.append("research")
    if overheat_hits:
        reason_bits.append("overheat")
    if company_hit:
        reason_bits.append("company_match")
    reason = ":".join(reason_bits)

    return {
        "implication_scope": scope,
        "implication_direction": direction,
        "implication_strength": round(float(strength), 6),
        "implication_confidence": round(float(confidence), 6),
        "implication_horizon": horizon,
        "implication_action": action,
        "implication_score": round(max(-1.0, min(1.0, float(implication_score))), 6),
        "implication_reason": reason[:200],
        "implication_scored_at": _now_kst().strftime("%Y-%m-%d %H:%M:%S"),
        "implication_version": VERSION,
    }


def _load_rows(con: sqlite3.Connection, as_of: str, lookback_days: int, limit: int, force: bool) -> List[Dict[str, Any]]:
    end_d = datetime.strptime(as_of, "%Y%m%d").date()
    start_d = end_d - timedelta(days=max(0, int(lookback_days)))
    where = "date8 >= ? AND date8 <= ?"
    params: List[Any] = [start_d.strftime("%Y%m%d"), as_of]
    if not force:
        where += " AND (implication_version IS NULL OR implication_version <> ?)"
        params.append(VERSION)
    q = (
        "SELECT article_key, code, name, date8, title, description, source, "
        "article_score, llm_score, entity_tags, pos_hits, neg_hits "
        "FROM news_articles_naver "
        f"WHERE {where} "
        "ORDER BY date8 DESC, fetched_at DESC "
        "LIMIT ?"
    )
    params.append(max(1, int(limit)))
    cur = con.execute(q, params)
    return [dict(zip([c[0] for c in cur.description], row)) for row in cur.fetchall()]


def _write_status(status: Dict[str, Any], as_of: str) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    text = json.dumps(status, ensure_ascii=False, indent=2)
    (LOGS / f"news_implication_status_{as_of}.json").write_text(text, encoding="utf-8")
    (LOGS / "news_implication_status_latest.json").write_text(text, encoding="utf-8")


def _sqlite_timeout_sec() -> float:
    raw = str(os.getenv("NEWS_IMPLICATION_SQLITE_TIMEOUT_SEC", "60")).strip() or "60"
    try:
        return max(1.0, float(raw))
    except ValueError:
        return 60.0


def _connect_db() -> sqlite3.Connection:
    timeout_sec = _sqlite_timeout_sec()
    con = sqlite3.connect(str(DB), timeout=timeout_sec)
    con.execute(f"PRAGMA busy_timeout={int(timeout_sec * 1000)}")
    return con


def main() -> int:
    ap = argparse.ArgumentParser(description="Classify article-level implications for collected news")
    ap.add_argument("--as-of", default=_now_kst().strftime("%Y%m%d"))
    ap.add_argument("--lookback-days", type=int, default=3)
    ap.add_argument("--max-rows", type=int, default=2000)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    as_of = _norm_date8(args.as_of) or _now_kst().strftime("%Y%m%d")
    status: Dict[str, Any] = {
        "generated_at": _now_kst().isoformat(timespec="seconds"),
        "asof_ymd": as_of,
        "db": str(DB),
        "table": "news_articles_naver",
        "version": VERSION,
        "rows_targeted": 0,
        "rows_scored": 0,
        "action_counts": {},
        "scope_counts": {},
        "quality": "FAIL",
        "reason": "",
    }
    if not DB.exists():
        status["reason"] = "db_missing"
        _write_status(status, as_of)
        return 0

    attempts = max(1, int(str(os.getenv("NEWS_IMPLICATION_SQLITE_RETRY", "3")).strip() or "3"))
    for attempt in range(1, attempts + 1):
        con = _connect_db()
        try:
            return _run_scoring(con, status, as_of, args)
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt >= attempts:
                raise
            time.sleep(min(5.0, 0.5 * attempt))
        finally:
            con.close()
    return 1


def _run_scoring(con: sqlite3.Connection, status: Dict[str, Any], as_of: str, args: argparse.Namespace) -> int:
        _ensure_columns(con)
        rows = _load_rows(con, as_of, int(args.lookback_days), int(args.max_rows), bool(args.force))
        status["rows_targeted"] = int(len(rows))
        action_counts: Dict[str, int] = {}
        scope_counts: Dict[str, int] = {}
        for row in rows:
            article_key = str(row.get("article_key") or "")
            if not article_key:
                continue
            result = _classify(row)
            con.execute(
                """
                UPDATE news_articles_naver
                SET implication_scope=?, implication_direction=?, implication_strength=?,
                    implication_confidence=?, implication_horizon=?, implication_action=?,
                    implication_score=?, implication_reason=?, implication_scored_at=?,
                    implication_version=?
                WHERE article_key=?
                """,
                (
                    result["implication_scope"],
                    result["implication_direction"],
                    result["implication_strength"],
                    result["implication_confidence"],
                    result["implication_horizon"],
                    result["implication_action"],
                    result["implication_score"],
                    result["implication_reason"],
                    result["implication_scored_at"],
                    result["implication_version"],
                    article_key,
                ),
            )
            action_counts[str(result["implication_action"])] = action_counts.get(str(result["implication_action"]), 0) + 1
            scope_counts[str(result["implication_scope"])] = scope_counts.get(str(result["implication_scope"]), 0) + 1
        con.commit()
        status["rows_scored"] = int(sum(action_counts.values()))
        status["action_counts"] = dict(sorted(action_counts.items()))
        status["scope_counts"] = dict(sorted(scope_counts.items()))
        end_d = datetime.strptime(as_of, "%Y%m%d").date()
        start_d = end_d - timedelta(days=max(0, int(args.lookback_days)))
        coverage_row = con.execute(
            """
            SELECT COUNT(*) AS total_rows,
                   SUM(CASE WHEN implication_version=? THEN 1 ELSE 0 END) AS version_rows,
                   SUM(CASE WHEN implication_score IS NOT NULL THEN 1 ELSE 0 END) AS scored_rows
            FROM news_articles_naver
            WHERE date8 >= ? AND date8 <= ?
            """,
            (VERSION, start_d.strftime("%Y%m%d"), as_of),
        ).fetchone()
        total_rows = int((coverage_row or [0, 0, 0])[0] or 0)
        version_rows = int((coverage_row or [0, 0, 0])[1] or 0)
        scored_rows = int((coverage_row or [0, 0, 0])[2] or 0)
        status["coverage"] = {
            "lookback_days": int(args.lookback_days),
            "rows_in_window": total_rows,
            "rows_with_version": version_rows,
            "rows_with_implication_score": scored_rows,
            "coverage_rate": round(float(version_rows) / float(total_rows), 6) if total_rows > 0 else 1.0,
        }
        status["quality"] = "PASS" if status["rows_scored"] == status["rows_targeted"] else ("WARN" if status["rows_scored"] > 0 else "SKIP")
        if status["rows_targeted"] == 0 and total_rows > 0 and version_rows == total_rows:
            status["quality"] = "PASS"
            status["reason"] = "already_scored"
        else:
            status["reason"] = "ok" if status["quality"] in {"PASS", "SKIP"} else "partial_scored"
        _write_status(status, as_of)
        print(f"[NEWS_IMPLICATION] quality={status['quality']} rows_scored={status['rows_scored']}/{status['rows_targeted']}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
