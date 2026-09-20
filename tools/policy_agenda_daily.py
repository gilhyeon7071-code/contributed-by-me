from __future__ import annotations

import json
import math
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import logging

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
NEWS_DB = ROOT / "news_trading" / "data" / "trading.db"
CONFIG = ROOT / "policy_agenda_config.json"
CABINET_SIGNAL = LOGS / "cabinet_meeting_signal_latest.json"
MINISTRY_SIGNAL = LOGS / "ministry_press_signal_latest.json"

# 신호 출처별 부스트 배율 (뉴스 기사 기준 = 1.0)
CABINET_BOOST_MULTIPLIER = 2.0   # 국무회의 의결 — 최고 신뢰
MINISTRY_BOOST_MULTIPLIER = 1.5  # 부처 보도자료 — 준공식

IN_FILES = [
    LOGS / "candidates_latest_data.with_news_score.csv",
    LOGS / "candidates_latest_data.with_sector_score.csv",
    LOGS / "candidates_latest_data.filtered.csv",
    LOGS / "candidates_latest_data.csv",
]
OUT = LOGS / "candidates_latest_data.with_policy_score.csv"
STATUS_OUT_LATEST = LOGS / "policy_agenda_signal_latest.json"

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


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
    except Exception:
        return default
    if math.isnan(x) or math.isinf(x):
        return default
    return x


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _norm_code6(v: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s.zfill(6) if s else ""


def _parse_dt(v: Any) -> Optional[datetime]:
    raw = str(v or "").strip()
    if not raw:
        return None
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
        return None


def _pick_input() -> Optional[Path]:
    for p in IN_FILES:
        if p.exists():
            return p
    return None


def _load_config() -> Dict[str, Any]:
    if not CONFIG.exists():
        return {}
    try:
        return json.loads(CONFIG.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_cabinet_signal() -> Tuple[Dict[str, float], Dict[str, Any]]:
    """cabinet_meeting_signal_latest.json 에서 sector_boost 맵 로드."""
    meta: Dict[str, Any] = {"available": False, "reason": "", "decided_agendas": 0}
    if not CABINET_SIGNAL.exists():
        meta["reason"] = "signal_missing"
        return {}, meta
    try:
        obj = json.loads(CABINET_SIGNAL.read_text(encoding="utf-8"))
        quality = str(obj.get("quality") or "FAIL").upper()
        if quality == "FAIL":
            meta["reason"] = f"quality_{quality}"
            return {}, meta
        sector_boost = {
            str(k): float(v)
            for k, v in (obj.get("sector_boost") or {}).items()
        }
        meta["available"] = True
        meta["reason"] = "ok"
        meta["quality"] = quality
        meta["decided_agendas"] = int(obj.get("decided_agendas") or 0)
        meta["generated_at"] = str(obj.get("generated_at") or "")
        meta["items_cabinet"] = int(obj.get("items_cabinet") or 0)
        return sector_boost, meta
    except Exception as e:
        meta["reason"] = f"load_fail:{type(e).__name__}"
        return {}, meta


def _load_ministry_signal() -> Tuple[Dict[str, float], Dict[str, Any]]:
    """ministry_press_signal_latest.json 에서 sector_boost 맵 로드."""
    meta: Dict[str, Any] = {"available": False, "reason": "", "active_agendas": 0}
    if not MINISTRY_SIGNAL.exists():
        meta["reason"] = "signal_missing"
        return {}, meta
    try:
        obj = json.loads(MINISTRY_SIGNAL.read_text(encoding="utf-8"))
        quality = str(obj.get("quality") or "FAIL").upper()
        if quality == "FAIL":
            meta["reason"] = f"quality_{quality}"
            return {}, meta
        sector_boost = {
            str(k): float(v)
            for k, v in (obj.get("sector_boost") or {}).items()
        }
        meta["available"] = True
        meta["reason"] = "ok"
        meta["quality"] = quality
        meta["active_agendas"] = int(obj.get("active_agendas") or 0)
        meta["ministry_count"] = int(obj.get("ministry_count") or 0)
        meta["generated_at"] = str(obj.get("generated_at") or "")
        return sector_boost, meta
    except Exception as e:
        meta["reason"] = f"load_fail:{type(e).__name__}"
        return {}, meta


def _keyword_hits(text: str, keywords: List[str]) -> int:
    count = 0
    for kw in keywords:
        if kw in text:
            count += 1
    return count


def _time_weight(event_dt: datetime, now_dt: datetime, half_life_hours: float) -> float:
    age_hours = max(0.0, (now_dt - event_dt).total_seconds() / 3600.0)
    if half_life_hours <= 0:
        return 1.0
    return 0.5 ** (age_hours / half_life_hours)


def _load_articles(
    max_age_hours: float,
    max_lag_days: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """뉴스 DB에서 최근 기사 로드. (code, title+description, published_at) 반환."""
    meta: Dict[str, Any] = {
        "db_path": str(NEWS_DB),
        "rows_raw": 0,
        "rows_used": 0,
        "reason": "",
    }
    if not NEWS_DB.exists():
        meta["reason"] = "db_missing"
        return [], meta

    now_dt = _now_kst()
    max_age_td = timedelta(hours=max(1.0, float(max_age_hours)))

    try:
        con = sqlite3.connect(str(NEWS_DB))
    except Exception as e:
        meta["reason"] = f"db_open_fail:{type(e).__name__}"
        return [], meta

    try:
        tables = [
            str(r[0])
            for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        ]
        if "news_articles_naver" not in tables:
            meta["reason"] = "table_missing"
            return [], meta

        # 사용 가능한 컬럼 확인
        col_info = con.execute('PRAGMA table_info("news_articles_naver")').fetchall()
        cols = {str(r[1]) for r in col_info}

        title_sql = '"title"' if "title" in cols else "NULL"
        desc_sql = '"description"' if "description" in cols else "NULL"
        pub_sql = '"published_at"' if "published_at" in cols else ('"fetched_at"' if "fetched_at" in cols else "NULL")

        rows = con.execute(
            f'SELECT "code", {title_sql} AS title, {desc_sql} AS description, '
            f'{pub_sql} AS published_at FROM "news_articles_naver"'
        ).fetchall()
        meta["rows_raw"] = len(rows)

        articles: List[Dict[str, Any]] = []
        for code_raw, title, desc, pub in rows:
            code = _norm_code6(code_raw)
            if not code:
                continue
            event_dt = _parse_dt(pub)
            if event_dt is None:
                continue
            age = (now_dt - event_dt).total_seconds()
            if age < 0 or (now_dt - event_dt) > max_age_td:
                continue
            text = f"{title or ''} {desc or ''}".strip()
            if not text:
                continue
            articles.append({"code": code, "text": text, "event_dt": event_dt})

        meta["rows_used"] = len(articles)
        meta["reason"] = "ok" if articles else "no_articles_in_window"
        return articles, meta
    except Exception as e:
        meta["reason"] = f"query_fail:{type(e).__name__}"
        return [], meta
    finally:
        try:
            con.close()
        except Exception:
            pass


def _compute_agenda_scores(
    articles: List[Dict[str, Any]],
    agendas: List[Dict[str, Any]],
    now_dt: datetime,
    half_life_hours: float,
) -> Dict[str, Dict[str, Any]]:
    """과제별(id → score) 가중 평균 점수 계산."""
    # agenda_id → list of (weighted_score, weight)
    agenda_hits: Dict[str, List[Tuple[float, float]]] = {a["id"]: [] for a in agendas}

    for art in articles:
        text = art["text"]
        event_dt = art["event_dt"]
        w = _time_weight(event_dt, now_dt, half_life_hours)
        if w < 1e-6:
            continue

        for agenda in agendas:
            pos = _keyword_hits(text, agenda.get("pos_keywords", []))
            neg = _keyword_hits(text, agenda.get("neg_keywords", []))
            if pos == 0 and neg == 0:
                continue
            denom = pos + neg
            raw_score = (pos - neg) / denom  # -1.0 ~ 1.0
            agenda_hits[agenda["id"]].append((raw_score * w, w))

    result: Dict[str, Dict[str, Any]] = {}
    for agenda in agendas:
        aid = agenda["id"]
        hits = agenda_hits[aid]
        if not hits:
            result[aid] = {
                "score": 0.0,
                "article_count": 0,
                "status": "no_hit",
                "name": agenda.get("name", ""),
                "weight": float(agenda.get("weight", 1.0)),
            }
            continue
        total_w = sum(w for _, w in hits)
        if total_w <= 0:
            score = 0.0
        else:
            score = sum(s for s, _ in hits) / total_w
        score = _clamp(score, -1.0, 1.0)
        result[aid] = {
            "score": round(float(score), 6),
            "article_count": int(len(hits)),
            "status": "ok",
            "name": agenda.get("name", ""),
            "weight": float(agenda.get("weight", 1.0)),
        }
    return result


def _blend(base: float, boost: float, multiplier: float) -> float:
    """기본 점수와 부스트 점수를 가중 합산."""
    return (base + boost * multiplier) / (1.0 + multiplier)


def _build_sector_signal(
    agenda_scores: Dict[str, Dict[str, Any]],
    agendas: List[Dict[str, Any]],
    cabinet_boost: Optional[Dict[str, float]] = None,
    ministry_boost: Optional[Dict[str, float]] = None,
) -> Dict[str, float]:
    """sector_code → policy_score 매핑.

    신호 우선순위 (높을수록 신뢰):
      국무회의 의결(cabinet) × 2.0  >  부처 보도자료(ministry) × 1.5  >  뉴스 기사 × 1.0
    """
    sector_weighted: Dict[str, List[Tuple[float, float]]] = {}

    for agenda in agendas:
        aid = agenda["id"]
        info = agenda_scores.get(aid, {})
        score = float(info.get("score", 0.0))
        agenda_weight = float(agenda.get("weight", 1.0))

        for sc in agenda.get("sector_codes", []):
            sc = str(sc).strip()
            if not sc:
                continue
            sector_weighted.setdefault(sc, [])
            sector_weighted[sc].append((score * agenda_weight, agenda_weight))

    sector_signal: Dict[str, float] = {}
    all_boosted_sectors: set = set()

    for sc, pairs in sector_weighted.items():
        total_w = sum(w for _, w in pairs)
        base = (sum(s for s, _ in pairs) / total_w) if total_w > 0 else 0.0

        # 1단계: 부처 보도자료 블렌드 (1.5배)
        if ministry_boost and sc in ministry_boost:
            mb = float(ministry_boost[sc])
            if abs(mb) > 1e-6:
                base = _blend(base, mb, MINISTRY_BOOST_MULTIPLIER)
                all_boosted_sectors.add(sc)

        # 2단계: 국무회의 의결 블렌드 (2.0배) — 최종 우선
        if cabinet_boost and sc in cabinet_boost:
            cb = float(cabinet_boost[sc])
            if abs(cb) > 1e-6:
                base = _blend(base, cb, CABINET_BOOST_MULTIPLIER)
                all_boosted_sectors.add(sc)

        sector_signal[sc] = round(_clamp(base, -1.0, 1.0), 6)

    # 뉴스 히트 없이 부처/국무회의 신호만 있는 섹터 추가
    for boost_map, mult in (
        (ministry_boost, MINISTRY_BOOST_MULTIPLIER),
        (cabinet_boost, CABINET_BOOST_MULTIPLIER),
    ):
        if not boost_map:
            continue
        for sc, bv in boost_map.items():
            if sc not in sector_signal and abs(float(bv)) > 1e-6:
                sector_signal[sc] = round(
                    _clamp(float(bv) * mult / (1.0 + mult), -1.0, 1.0), 6
                )
                all_boosted_sectors.add(sc)

    return sector_signal


def _apply_to_candidates(
    df: pd.DataFrame,
    sector_signal: Dict[str, float],
    agenda_scores: Dict[str, Dict[str, Any]],
    agendas: List[Dict[str, Any]],
) -> pd.DataFrame:
    """후보 종목별 policy_score / policy_agenda_tags 컬럼 추가."""
    # sector_code → agenda names (태그용)
    sector_to_names: Dict[str, List[str]] = {}
    for agenda in agendas:
        aid = agenda["id"]
        info = agenda_scores.get(aid, {})
        score = float(info.get("score", 0.0))
        if abs(score) < 1e-6:
            continue
        for sc in agenda.get("sector_codes", []):
            sc = str(sc).strip()
            if not sc:
                continue
            if sc not in sector_to_names:
                sector_to_names[sc] = []
            sector_to_names[sc].append(agenda.get("name", aid))

    sc_col = None
    for cand in ("sector_code", "krx_sector_code", "sec_code"):
        if cand in df.columns:
            sc_col = cand
            break

    policy_scores: List[float] = []
    policy_tags: List[str] = []
    policy_sources: List[str] = []

    for _, row in df.iterrows():
        sc_raw = str(row.get(sc_col, "") or "").strip() if sc_col else ""
        # sector_signal 키는 3자리 zero-padded ("009"), CSV는 "9"일 수 있어 정규화
        sc = sc_raw.zfill(3) if sc_raw.isdigit() else sc_raw
        p_score = sector_signal.get(sc, 0.0) if sc else 0.0
        tags = "|".join(sector_to_names.get(sc, [])) if sc else ""
        src = "SECTOR_MATCH" if abs(p_score) > 1e-6 else "FAIL_SOFT"
        policy_scores.append(round(float(p_score), 6))
        policy_tags.append(tags)
        policy_sources.append(src)

    df = df.copy()
    df["policy_score"] = policy_scores
    df["policy_agenda_tags"] = policy_tags
    df["policy_score_source"] = policy_sources
    return df


def main() -> int:
    config = _load_config()
    if not config or "agendas" not in config:
        _log_print("[POLICY_AGENDA] config missing or invalid:", str(CONFIG))
        return 1

    agendas: List[Dict[str, Any]] = config.get("agendas", [])
    half_life_hours = float(config.get("half_life_hours", 48))
    max_age_hours = float(config.get("max_age_hours", 168))
    max_lag_days = int(config.get("max_lag_days", 3))
    gov_name = str(config.get("government", ""))

    in_path = _pick_input()
    if in_path is None:
        _log_print("[POLICY_AGENDA] no candidate input file found")
        return 1

    df = _read_csv(in_path)
    if "code" not in df.columns:
        _log_print("[POLICY_AGENDA] missing code column in:", str(in_path))
        return 1

    now_dt = _now_kst()

    articles, article_meta = _load_articles(
        max_age_hours=max_age_hours,
        max_lag_days=max_lag_days,
    )

    agenda_scores = _compute_agenda_scores(
        articles=articles,
        agendas=agendas,
        now_dt=now_dt,
        half_life_hours=half_life_hours,
    )

    cabinet_boost, cabinet_meta = _load_cabinet_signal()
    ministry_boost, ministry_meta = _load_ministry_signal()
    sector_signal = _build_sector_signal(
        agenda_scores, agendas, cabinet_boost, ministry_boost
    )

    df = _apply_to_candidates(df, sector_signal, agenda_scores, agendas)
    _write_csv(OUT, df)

    nonzero = int((pd.to_numeric(df["policy_score"], errors="coerce").fillna(0.0) != 0).sum())
    rows_n = max(1, len(df))

    status: Dict[str, Any] = {
        "generated_at": now_dt.isoformat(timespec="seconds"),
        "government": gov_name,
        "config": str(CONFIG),
        "input": str(in_path),
        "output": str(OUT),
        "rows": int(rows_n),
        "nonzero_rows": int(nonzero),
        "nonzero_rate": round(float(nonzero) / float(rows_n), 6),
        "article_meta": article_meta,
        "cabinet_meta": cabinet_meta,
        "ministry_meta": ministry_meta,
        "cabinet_boost_multiplier": CABINET_BOOST_MULTIPLIER,
        "ministry_boost_multiplier": MINISTRY_BOOST_MULTIPLIER,
        "half_life_hours": half_life_hours,
        "max_age_hours": max_age_hours,
        "sector_signal": sector_signal,
        "agenda_scores": {
            aid: {
                "name": info.get("name", ""),
                "score": info.get("score", 0.0),
                "article_count": info.get("article_count", 0),
                "status": info.get("status", ""),
            }
            for aid, info in agenda_scores.items()
        },
    }
    _write_json(STATUS_OUT_LATEST, status)

    # 날짜별 스냅샷
    date8 = now_dt.strftime("%Y%m%d")
    snap_path = LOGS / f"policy_agenda_signal_{date8}.json"
    _write_json(snap_path, status)

    _log_print(
        f"[POLICY_AGENDA] input={in_path.name} rows={rows_n} "
        f"nonzero={nonzero} articles={article_meta.get('rows_used', 0)} "
        f"agendas={len(agendas)} "
        f"cabinet={'ON' if cabinet_meta.get('available') else 'OFF'}"
        f"(decided={cabinet_meta.get('decided_agendas', 0)}) "
        f"ministry={'ON' if ministry_meta.get('available') else 'OFF'}"
        f"(active={ministry_meta.get('active_agendas', 0)})"
    )
    _log_print(f"[POLICY_AGENDA] wrote {OUT}")
    _log_print(f"[POLICY_AGENDA] status={STATUS_OUT_LATEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
