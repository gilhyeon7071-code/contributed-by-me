from __future__ import annotations

import json
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
import sys
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from holiday_manager import HolidayManager

IN_SECTOR = LOGS / "candidates_latest_data.with_sector_score.csv"
IN_FILTERED = LOGS / "candidates_latest_data.filtered.csv"
IN_BASE = LOGS / "candidates_latest_data.csv"
OUT = LOGS / "candidates_latest_data.with_news_score.csv"
KST = timezone(timedelta(hours=9))




logger = logging.getLogger(__name__)

def _log_print(*args, **kwargs):
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")
    sep = kwargs.get("sep", " ")
    try:
        msg = sep.join(str(a) for a in args)
    except Exception:
        msg = " ".join(str(a) for a in args)
    logger.info(msg)
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


def _now_kst() -> datetime:
    return datetime.now(KST)


def _norm_code6(v: object) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s.zfill(6) if s else ""


def _norm_date8(v: object) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s[:8] if len(s) >= 8 else ""


def _parse_dt(v: object) -> Optional[datetime]:
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


def _ymd_lag_days(base_ymd: str, target_ymd: str) -> Optional[int]:
    b = _norm_date8(base_ymd)
    t = _norm_date8(target_ymd)
    if len(b) != 8 or len(t) != 8:
        return None
    try:
        bd = datetime.strptime(b, "%Y%m%d").date()
        td = datetime.strptime(t, "%Y%m%d").date()
        # lag is staleness-only metric; future-dated allowance should not surface as negative lag
        return max(int((bd - td).days), 0)
    except Exception:
        return None


def _max_allowed_date8(reference_ymd: str) -> str:
    ref = _norm_date8(reference_ymd)
    if len(ref) != 8:
        return _now_kst().strftime("%Y%m%d")
    try:
        ref_d = datetime.strptime(ref, "%Y%m%d").date()
    except Exception:
        return _now_kst().strftime("%Y%m%d")
    today_d = _now_kst().date()
    t1_mode = str(os.getenv("NEWS_SCORE_T1_MODE", "")).strip().lower() in {"1", "true", "yes"}
    if t1_mode:
        # T-1 mode: cap to yesterday so intraday/premarket gets stable pre-collected scores
        cal = HolidayManager()
        ref_prev = datetime.strptime(cal.previous_trading_day(ref_d.strftime("%Y%m%d")), "%Y%m%d").date()
        today_prev = datetime.strptime(cal.previous_trading_day(today_d.strftime("%Y%m%d")), "%Y%m%d").date()
        allowed_d = min(ref_prev, today_prev)
    else:
        forward_days = max(0, int(str(os.getenv("NEWS_SCORE_MAX_FORWARD_DAYS", "0")).strip() or "0"))
        allowed_d = min(ref_d + timedelta(days=forward_days), today_d)
    return allowed_d.strftime("%Y%m%d")


def _max_date8(df: pd.DataFrame) -> str:
    if "date_yyyymmdd" in df.columns:
        s = df["date_yyyymmdd"].astype(str)
    elif "date" in df.columns:
        s = df["date"].astype(str)
    elif "signal_date" in df.columns:
        s = df["signal_date"].astype(str)
    else:
        return _now_kst().strftime("%Y%m%d")
    d8 = s.map(_norm_date8)
    d8 = d8[d8.str.len() == 8]
    return str(d8.max()) if len(d8) else _now_kst().strftime("%Y%m%d")


def _resolve_reference_ymd(input_asof_ymd: str) -> Tuple[str, str]:
    override = str(os.getenv("NEWS_SCORE_REFERENCE_YMD", "")).strip()
    if override:
        low = override.lower()
        if low in {"today", "now", "kst_today"}:
            return _now_kst().strftime("%Y%m%d"), "env_today"
        ymd = _norm_date8(override)
        if len(ymd) == 8:
            return ymd, "env"
    return input_asof_ymd or _now_kst().strftime("%Y%m%d"), "input_asof"


def _dated_input_rank(path: Path) -> tuple[str, float]:
    if not path.exists():
        return ("", -1.0)
    try:
        df = _read_csv(path)
    except Exception:
        return ("", float(path.stat().st_mtime))
    if not isinstance(df, pd.DataFrame) or df.empty:
        return ("", float(path.stat().st_mtime))
    return (_max_date8(df), float(path.stat().st_mtime))


def _table_columns(con: sqlite3.Connection, table: str) -> List[str]:
    q = f'PRAGMA table_info("{table}")'
    rows = con.execute(q).fetchall()
    return [str(r[1]) for r in rows]


def _pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    low = {c.lower(): c for c in cols}
    for k in candidates:
        if k.lower() in low:
            return low[k.lower()]
    return None


def _split_entity_tags(v: object) -> List[str]:
    return _split_entity_tags_clean(v)
def _split_entity_tags_clean(v: object) -> List[str]:
    raw = str(v or "").strip()
    if not raw or raw.lower() in {"none", "nan", "null"}:
        return []
    rules: List[Tuple[str, Tuple[str, ...]]] = [
        ("\uae08\ub9ac", ("\uae08\ub9ac", "\uae30\uc900\uae08\ub9ac", "\uad6d\ucc44", "\ucc44\uad8c\uae08\ub9ac", "FOMC", "FED", "BOND")),
        ("\ud658\uc728", ("\ud658\uc728", "\uc6d0\ub2ec\ub7ec", "\uc678\ud658", "USD/KRW", "DOLLAR", "YEN")),
        ("\uc778\ud50c\ub808\uc774\uc158", ("\uc778\ud50c\ub808\uc774\uc158", "\ubb3c\uac00", "CPI", "PPI")),
        ("\ubc18\ub3c4\uccb4", ("\ubc18\ub3c4\uccb4", "SEMICON", "HBM", "\uba54\ubaa8\ub9ac")),
        ("\ubc30\ud130\ub9ac", ("\ubc30\ud130\ub9ac", "2\ucc28\uc804\uc9c0", "BATTERY", "\uc591\uadf9\uc7ac", "\uc74c\uadf9\uc7ac", "\uc804\ud574\uc9c8")),
        ("\uc870\uc120", ("\uc870\uc120", "SHIP", "LNG", "\ud574\uc6b4")),
        ("\uc790\ub3d9\ucc28", ("\uc790\ub3d9\ucc28", "AUTO", "EV", "\uc804\uae30\ucc28")),
        ("\ubc14\uc774\uc624", ("\ubc14\uc774\uc624", "BIO", "\uc2e0\uc57d", "\uc784\uc0c1")),
        ("AI", ("AI", "GPU", "\uc778\uacf5\uc9c0\ub2a5", "\ub370\uc774\ud130\uc13c\ud130")),
        ("\uc815\ucc45", ("\uc815\ucc45", "\uaddc\uc81c", "\ubc95\uc548", "\uc815\ubd80", "\uad6d\ud68c")),
    ]
    out: List[str] = []
    for tok in raw.split("|"):
        s = str(tok).strip()
        if not s or s.lower() in {"none", "nan", "null"}:
            continue
        u = s.upper()
        matched = ""
        for label, aliases in rules:
            if any((alias in s) or (alias.upper() in u) for alias in aliases):
                matched = label
                break
        out.append(matched or s)
    return out


CANONICAL_ENTITY_LABELS: Dict[str, str] = {
    "rate": "금리",
    "fx": "환율",
    "inflation": "인플레이션",
    "semicon": "반도체",
    "battery": "배터리",
    "ship": "조선",
    "auto": "자동차",
    "bio": "바이오",
    "ai": "AI",
    "policy": "정책",
}


CANONICAL_ENTITY_ALIASES: Dict[str, Tuple[str, ...]] = {
    "rate": ("\uae08\ub9ac", "\uae30\uc900\uae08\ub9ac", "\uad6d\ucc44", "\ucc44\uad8c\uae08\ub9ac", "FOMC", "FED", "BOND"),
    "fx": ("\ud658\uc728", "\uc6d0\ub2ec\ub7ec", "\uc678\ud658", "USD/KRW", "DOLLAR", "YEN"),
    "inflation": ("\uc778\ud50c\ub808\uc774\uc158", "\ubb3c\uac00", "CPI", "PPI"),
    "semicon": ("\ubc18\ub3c4\uccb4", "SEMICON", "HBM", "\uba54\ubaa8\ub9ac"),
    "battery": ("\ubc30\ud130\ub9ac", "2\ucc28\uc804\uc9c0", "BATTERY", "\uc591\uadf9\uc7ac", "\uc74c\uadf9\uc7ac", "\uc804\ud574\uc9c8"),
    "ship": ("\uc870\uc120", "SHIP", "LNG", "\ud574\uc6b4"),
    "auto": ("\uc790\ub3d9\ucc28", "AUTO", "EV", "\uc804\uae30\ucc28"),
    "bio": ("\ubc14\uc774\uc624", "BIO", "\uc2e0\uc57d", "\uc784\uc0c1"),
    "ai": ("AI", "GPU", "\uc778\uacf5\uc9c0\ub2a5", "\ub370\uc774\ud130\uc13c\ud130"),
    "policy": ("\uc815\ucc45", "\uaddc\uc81c", "\ubc95\uc548", "\uc815\ubd80", "\uad6d\ud68c"),
}


def _normalize_entity_tag_canonical(tag: object) -> str:
    t = str(tag or "").strip()
    if not t or t.lower() in {"none", "nan", "null"}:
        return ""
    u = t.upper()
    for key, aliases in CANONICAL_ENTITY_ALIASES.items():
        if any(alias and (alias in t or alias.upper() in u) for alias in aliases):
            return CANONICAL_ENTITY_LABELS[key]
    return t


def _split_entity_tags_canonical(v: object) -> List[str]:
    raw = str(v or "").strip()
    if not raw or raw.lower() in {"none", "nan", "null"}:
        return []
    out: List[str] = []
    for tok in raw.split("|"):
        n = _normalize_entity_tag_canonical(tok)
        if n:
            out.append(n)
    return out


def _pick_input() -> Path:
    candidates = [IN_SECTOR, IN_FILTERED, IN_BASE]
    ranked: list[tuple[str, int, float, Path]] = []
    stage_rank = {path: idx for idx, path in enumerate(reversed(candidates), start=1)}
    for path in candidates:
        date8, mtime = _dated_input_rank(path)
        if date8:
            ranked.append((date8, int(stage_rank.get(path, 0)), mtime, path))
    if ranked:
        ranked.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
        return ranked[0][3]
    for path in candidates:
        if path.exists():
            return path
    return IN_BASE


def _weighted_average(df: pd.DataFrame, now_dt: datetime, half_life_hours: float) -> Tuple[float, float]:
    if df.empty:
        return 0.0, -1.0
    weights: List[float] = []
    ages: List[float] = []
    for _, row in df.iterrows():
        ts = row.get("event_dt")
        if not isinstance(ts, datetime):
            weights.append(0.0)
            continue
        age_hours = max(0.0, (now_dt - ts).total_seconds() / 3600.0)
        ages.append(age_hours)
        if half_life_hours <= 0:
            weights.append(1.0)
        else:
            weights.append(0.5 ** (age_hours / half_life_hours))

    if not weights:
        return 0.0, -1.0

    work = df.copy()
    work["weight"] = weights
    work = work[work["weight"] > 0]
    if work.empty:
        return 0.0, -1.0
    denom = float(work["weight"].sum())
    if denom <= 0:
        return 0.0, -1.0
    score = float((work["score"] * work["weight"]).sum() / denom)
    freshest = min(ages) if ages else -1.0
    return float(max(-1.0, min(1.0, score))), float(freshest)


def _load_weighted_article_map(
    reference_ymd: str,
    max_lag_days: int,
    half_life_hours: float,
    max_age_hours: float,
) -> Tuple[Dict[str, Dict[str, float]], Dict[str, object]]:
    meta: Dict[str, object] = {
        "db_path": str(NEWS_DB),
        "table": None,
        "reason": "",
        "rows_raw": 0,
        "rows_used": 0,
        "used_date8": None,
        "used_lag_days": None,
        "half_life_hours": float(half_life_hours),
        "max_age_hours": float(max_age_hours),
        "max_lag_days": int(max_lag_days),
        "ner_top_tags": [],
        "ner_rows_with_tags": 0,
    }
    if not NEWS_DB.exists():
        meta["reason"] = "db_missing"
        return {}, meta

    now_dt = _now_kst()
    try:
        con = sqlite3.connect(str(NEWS_DB))
    except Exception as e:
        meta["reason"] = f"db_open_fail:{type(e).__name__}"
        return {}, meta

    try:
        tables = [str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if "news_articles_naver" not in tables:
            meta["reason"] = "article_table_missing"
            return {}, meta

        cols = _table_columns(con, "news_articles_naver")
        code_col = _pick_col(cols, ["code"])
        article_score_col = _pick_col(cols, ["article_score"])
        llm_score_col = _pick_col(cols, ["llm_score"])
        date_col = _pick_col(cols, ["date8"])
        published_col = _pick_col(cols, ["published_at"])
        fetched_col = _pick_col(cols, ["fetched_at"])
        entity_tags_col = _pick_col(cols, ["entity_tags"])
        entity_count_col = _pick_col(cols, ["entity_count"])
        implication_score_col = _pick_col(cols, ["implication_score"])
        implication_scope_col = _pick_col(cols, ["implication_scope"])
        implication_action_col = _pick_col(cols, ["implication_action"])
        implication_direction_col = _pick_col(cols, ["implication_direction"])
        implication_strength_col = _pick_col(cols, ["implication_strength"])
        implication_confidence_col = _pick_col(cols, ["implication_confidence"])
        implication_horizon_col = _pick_col(cols, ["implication_horizon"])
        if not (code_col and article_score_col and date_col and (published_col or fetched_col)):
            meta["reason"] = "article_table_unsupported"
            return {}, meta

        date_sql = f'"{published_col}"' if published_col else "NULL"
        fetch_sql = f'"{fetched_col}"' if fetched_col else "NULL"
        entity_tags_sql = f'"{entity_tags_col}"' if entity_tags_col else "NULL"
        entity_count_sql = f'"{entity_count_col}"' if entity_count_col else "0"
        llm_score_sql = f'"{llm_score_col}"' if llm_score_col else "NULL"
        implication_score_sql = f'"{implication_score_col}"' if implication_score_col else "NULL"
        implication_scope_sql = f'"{implication_scope_col}"' if implication_scope_col else "NULL"
        implication_action_sql = f'"{implication_action_col}"' if implication_action_col else "NULL"
        implication_direction_sql = f'"{implication_direction_col}"' if implication_direction_col else "NULL"
        implication_strength_sql = f'"{implication_strength_col}"' if implication_strength_col else "NULL"
        implication_confidence_sql = f'"{implication_confidence_col}"' if implication_confidence_col else "NULL"
        implication_horizon_sql = f'"{implication_horizon_col}"' if implication_horizon_col else "NULL"
        q = (
            f'SELECT "{code_col}" AS code, "{article_score_col}" AS article_score, {llm_score_sql} AS llm_score, '
            f'{implication_score_sql} AS implication_score, {implication_scope_sql} AS implication_scope, '
            f'{implication_action_sql} AS implication_action, {implication_direction_sql} AS implication_direction, '
            f'{implication_strength_sql} AS implication_strength, {implication_confidence_sql} AS implication_confidence, '
            f'{implication_horizon_sql} AS implication_horizon, "{date_col}" AS date8, '
            f'{date_sql} AS published_at, {fetch_sql} AS fetched_at, '
            f'{entity_tags_sql} AS entity_tags, {entity_count_sql} AS entity_count '
            'FROM "news_articles_naver"'
        )
        adf = pd.read_sql_query(q, con)
        meta["rows_raw"] = int(len(adf))
        if adf.empty:
            meta["reason"] = "article_rows_empty"
            return {}, meta

        adf["code"] = adf["code"].map(_norm_code6)
        adf["date8"] = adf["date8"].map(_norm_date8)
        adf["article_score"] = pd.to_numeric(adf["article_score"], errors="coerce")
        adf["llm_score"] = pd.to_numeric(adf["llm_score"], errors="coerce")
        adf["implication_score"] = pd.to_numeric(adf["implication_score"], errors="coerce")
        adf["implication_strength"] = pd.to_numeric(adf["implication_strength"], errors="coerce")
        adf["implication_confidence"] = pd.to_numeric(adf["implication_confidence"], errors="coerce")
        adf["score"] = adf["implication_score"].where(
            adf["implication_score"].notna(),
            adf["llm_score"].where(adf["llm_score"].notna(), adf["article_score"]),
        )
        meta["llm_score_rows_raw"] = int(adf["llm_score"].notna().sum())
        meta["implication_score_rows_raw"] = int(adf["implication_score"].notna().sum())
        meta["score_source"] = "implication_score_fallback_llm_score_fallback_article_score" if implication_score_col else ("llm_score_fallback_article_score" if llm_score_col else "article_score")
        adf["entity_count"] = pd.to_numeric(adf["entity_count"], errors="coerce").fillna(0).astype(int)
        adf["entity_tags"] = adf["entity_tags"].astype(str)
        adf["event_dt"] = adf["published_at"].map(_parse_dt)
        if fetched_col:
            adf["event_dt"] = adf["event_dt"].where(adf["event_dt"].notna(), adf["fetched_at"].map(_parse_dt))

        adf = adf[(adf["code"] != "") & (adf["date8"] != "")]
        adf = adf.dropna(subset=["score", "event_dt"])
        if adf.empty:
            meta["reason"] = "article_rows_invalid"
            return {}, meta

        ref_date8 = str(reference_ymd)
        max_date8 = _max_allowed_date8(ref_date8)
        meta["max_allowed_date8"] = str(max_date8)
        adf = adf[adf["date8"] <= max_date8]
        if adf.empty:
            meta["reason"] = "article_no_rows_upto_reference"
            return {}, meta

        used_date8 = str(adf["date8"].max() or "")
        meta["used_date8"] = used_date8
        lag_days = _ymd_lag_days(ref_date8, used_date8)
        meta["used_lag_days"] = lag_days
        if lag_days is not None and lag_days > int(max_lag_days):
            meta["reason"] = f"signals_stale_lag_{lag_days}d"
            return {}, meta

        max_age_td = timedelta(hours=max(0.0, float(max_age_hours)))
        adf = adf[(now_dt - adf["event_dt"]) <= max_age_td]
        adf = adf[(now_dt - adf["event_dt"]) >= timedelta(seconds=0)]
        if adf.empty:
            meta["reason"] = "article_no_recent_rows"
            return {}, meta

        tag_counts_all: Dict[str, int] = {}
        rows_with_tags = 0
        for _, row in adf.iterrows():
            tags = _split_entity_tags_canonical(row.get("entity_tags"))
            if tags:
                rows_with_tags += 1
            for t in tags:
                tag_counts_all[t] = int(tag_counts_all.get(t, 0)) + 1
        meta["ner_rows_with_tags"] = int(rows_with_tags)
        if tag_counts_all:
            top = sorted(tag_counts_all.items(), key=lambda x: (-x[1], x[0]))[:10]
            meta["ner_top_tags"] = [{"tag": str(k), "count": int(v)} for k, v in top]
        else:
            meta["ner_top_tags"] = []

        out: Dict[str, Dict[str, float]] = {}
        for code, grp in adf.groupby("code"):
            score, freshest_age = _weighted_average(grp.sort_values("event_dt", ascending=False), now_dt, half_life_hours)
            tag_counts: Dict[str, int] = {}
            action_counts: Dict[str, int] = {}
            scope_counts: Dict[str, int] = {}
            direction_counts: Dict[str, int] = {}
            horizon_counts: Dict[str, int] = {}
            for _, r in grp.iterrows():
                for t in _split_entity_tags_canonical(r.get("entity_tags")):
                    tag_counts[t] = int(tag_counts.get(t, 0)) + 1
                action = str(r.get("implication_action") or "").strip()
                scope = str(r.get("implication_scope") or "").strip()
                direction = str(r.get("implication_direction") or "").strip()
                horizon = str(r.get("implication_horizon") or "").strip()
                if action:
                    action_counts[action] = int(action_counts.get(action, 0)) + 1
                if scope:
                    scope_counts[scope] = int(scope_counts.get(scope, 0)) + 1
                if direction:
                    direction_counts[direction] = int(direction_counts.get(direction, 0)) + 1
                if horizon:
                    horizon_counts[horizon] = int(horizon_counts.get(horizon, 0)) + 1
            top_tags = sorted(tag_counts.items(), key=lambda x: (-x[1], x[0]))[:3]
            top_actions = sorted(action_counts.items(), key=lambda x: (-x[1], x[0]))[:3]
            top_scopes = sorted(scope_counts.items(), key=lambda x: (-x[1], x[0]))[:3]
            implication_rows = int(pd.to_numeric(grp.get("implication_score"), errors="coerce").notna().sum())
            strength_avg = float(pd.to_numeric(grp.get("implication_strength"), errors="coerce").dropna().mean()) if implication_rows else 0.0
            confidence_avg = float(pd.to_numeric(grp.get("implication_confidence"), errors="coerce").dropna().mean()) if implication_rows else 0.0
            risk_denominator = max(
                1.0,
                float(
                    action_counts.get("block", 0)
                    + action_counts.get("reduce_size", 0)
                    + action_counts.get("penalize", 0)
                    + action_counts.get("watch", 0)
                ),
            )
            out[str(code)] = {
                "score": round(float(score), 6),
                "article_count": int(len(grp)),
                "freshest_age_hours": round(float(freshest_age), 6) if freshest_age >= 0 else -1.0,
                "source": "DB_WEIGHTED_ARTICLE",
                "trace_table": "news_articles_naver",
                "trace_used_date8": str(used_date8 or ""),
                "trace_used_lag_days": int(lag_days) if lag_days is not None else -1,
                "trace_reason": "ok",
                "entity_count_total": int(pd.to_numeric(grp.get("entity_count"), errors="coerce").fillna(0).sum()),
                "entity_top_tags": "|".join([f"{k}:{v}" for k, v in top_tags]),
                "implication_rows": int(implication_rows),
                "implication_top_actions": "|".join([f"{k}:{v}" for k, v in top_actions]),
                "implication_top_scopes": "|".join([f"{k}:{v}" for k, v in top_scopes]),
                "implication_positive_rows": int(direction_counts.get("positive", 0)),
                "implication_negative_rows": int(direction_counts.get("negative", 0)),
                "implication_neutral_rows": int(direction_counts.get("neutral", 0)),
                "implication_boost_rows": int(action_counts.get("boost", 0)),
                "implication_penalize_rows": int(action_counts.get("penalize", 0)),
                "implication_block_rows": int(action_counts.get("block", 0)),
                "implication_reduce_size_rows": int(action_counts.get("reduce_size", 0)),
                "implication_watch_rows": int(action_counts.get("watch", 0)),
                "implication_ignore_rows": int(action_counts.get("ignore", 0)),
                "implication_company_rows": int(scope_counts.get("company", 0)),
                "implication_sector_rows": int(scope_counts.get("sector", 0)),
                "implication_market_rows": int(scope_counts.get("market", 0)),
                "implication_macro_rows": int(scope_counts.get("macro", 0)),
                "implication_intraday_rows": int(horizon_counts.get("intraday", 0)),
                "implication_short_rows": int(horizon_counts.get("short", 0)),
                "implication_medium_rows": int(horizon_counts.get("medium", 0)),
                "implication_strength_avg": round(float(strength_avg), 6) if pd.notna(strength_avg) else 0.0,
                "implication_confidence_avg": round(float(confidence_avg), 6) if pd.notna(confidence_avg) else 0.0,
                "implication_directness_score": round(float(scope_counts.get("company", 0)) / float(implication_rows), 6) if implication_rows else 0.0,
                "implication_risk_score": round(
                    min(
                        1.0,
                        (
                            float(action_counts.get("block", 0)) * 1.0
                            + float(action_counts.get("reduce_size", 0)) * 0.7
                            + float(action_counts.get("penalize", 0)) * 0.5
                            + float(direction_counts.get("negative", 0)) * 0.3
                        )
                        / float(risk_denominator),
                    ),
                    6,
                )
                if implication_rows
                else 0.0,
            }

        meta["table"] = "news_articles_naver"
        meta["rows_used"] = int(sum(int(v.get("article_count", 0)) for v in out.values()))
        meta["reason"] = "ok" if out else "article_zero_after_filter"
        return out, meta
    except Exception as e:
        meta["reason"] = f"article_query_fail:{type(e).__name__}"
        return {}, meta
    finally:
        try:
            con.close()
        except Exception:
            pass


def _load_signal_map(reference_ymd: str, max_lag_days: int) -> Tuple[Dict[str, Dict[str, float]], Dict[str, object]]:
    meta: Dict[str, object] = {
        "db_path": str(NEWS_DB),
        "table": None,
        "reason": "",
        "rows_raw": 0,
        "rows_used": 0,
        "used_date8": None,
        "used_lag_days": None,
        "max_lag_days": int(max_lag_days),
    }
    if not NEWS_DB.exists():
        meta["reason"] = "db_missing"
        return {}, meta

    try:
        con = sqlite3.connect(str(NEWS_DB))
    except Exception as e:
        meta["reason"] = f"db_open_fail:{type(e).__name__}"
        return {}, meta

    try:
        tbl_rows = con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        tables = [str(r[0]) for r in tbl_rows]

        table = None
        code_col = None
        date_col = None
        score_col = None
        checked: Dict[str, List[str]] = {}
        for cand_table in ["signals", "signals_naver_daily"]:
            if cand_table not in tables:
                continue
            cols = _table_columns(con, cand_table)
            checked[cand_table] = cols
            c_code = _pick_col(cols, ["code", "ticker", "symbol"])
            c_date = _pick_col(cols, ["date8", "date", "ymd", "trade_date", "signal_date", "created_at", "ts"])
            c_score = _pick_col(cols, ["news_score", "score", "sentiment", "signal_strength", "strength"])
            if c_code and c_date and c_score:
                table = cand_table
                code_col = c_code
                date_col = c_date
                score_col = c_score
                break

        if not table:
            meta["reason"] = "signals_table_missing_or_unsupported"
            meta["tables_checked"] = checked
            return {}, meta
        meta["table"] = table

        q = f'SELECT "{code_col}" AS code, "{date_col}" AS d, "{score_col}" AS score FROM "{table}"'
        sdf = pd.read_sql_query(q, con)
        meta["rows_raw"] = int(len(sdf))
        if sdf.empty:
            meta["reason"] = "signals_empty"
            return {}, meta

        sdf["code"] = sdf["code"].map(_norm_code6)
        sdf["date8"] = sdf["d"].map(_norm_date8)
        sdf["score"] = pd.to_numeric(sdf["score"], errors="coerce")

        sdf = sdf[(sdf["code"] != "") & (sdf["date8"] != "")]
        sdf = sdf.dropna(subset=["score"])
        ref_date8 = str(reference_ymd)
        max_date8 = _max_allowed_date8(ref_date8)
        meta["max_allowed_date8"] = str(max_date8)
        sdf = sdf[sdf["date8"] <= str(max_date8)]
        if sdf.empty:
            meta["reason"] = "signals_no_rows_upto_reference"
            return {}, meta

        used_date8 = str(sdf["date8"].max() or "")
        meta["used_date8"] = used_date8
        lag_days = _ymd_lag_days(ref_date8, used_date8)
        meta["used_lag_days"] = lag_days
        if lag_days is not None and lag_days > int(max_lag_days):
            meta["reason"] = f"signals_stale_lag_{lag_days}d"
            return {}, meta

        sdf = sdf.sort_values(["code", "date8"]).drop_duplicates(["code"], keep="last")

        mx = float(sdf["score"].abs().max()) if len(sdf) else 0.0
        meta["score_abs_max_before_scale"] = round(mx, 8)
        meta["score_scale_applied"] = bool(mx > 1.0)
        meta["score_scale_divisor"] = round(mx if mx > 1.0 else 1.0, 8)
        meta["score_scale_up_blocked"] = bool(0.0 < mx < 1.0)
        if mx > 1.0:
            sdf["score"] = sdf["score"] / mx
        sdf["score"] = sdf["score"].clip(lower=-1.0, upper=1.0)
        meta["score_abs_max_after_scale"] = round(float(sdf["score"].abs().max()) if len(sdf) else 0.0, 8)

        out = {
            str(r["code"]): {
                "score": float(r["score"]),
                "article_count": 0,
                "freshest_age_hours": -1.0,
                "source": "DB_SIGNAL",
                "trace_table": str(table or ""),
                "trace_used_date8": str(used_date8 or ""),
                "trace_used_lag_days": int(lag_days) if lag_days is not None else -1,
                "trace_reason": "ok",
                "entity_count_total": 0,
                "entity_top_tags": "",
            }
            for _, r in sdf.iterrows()
        }
        meta["rows_used"] = int(len(out))
        meta["reason"] = "ok" if out else "signals_zero_after_filter"
        return out, meta
    except Exception as e:
        meta["reason"] = f"signals_query_fail:{type(e).__name__}"
        return {}, meta
    finally:
        try:
            con.close()
        except Exception:
            pass


def _load_source_signal_map(reference_ymd: str, max_lag_days: int) -> Tuple[Dict[str, Dict[str, float]], Dict[str, object]]:
    meta: Dict[str, object] = {
        "db_path": str(NEWS_DB),
        "table": "source_signals_daily",
        "reason": "",
        "rows_raw": 0,
        "rows_used": 0,
        "used_date8": None,
        "used_lag_days": None,
        "max_lag_days": int(max_lag_days),
    }
    if not NEWS_DB.exists():
        meta["reason"] = "db_missing"
        return {}, meta

    try:
        con = sqlite3.connect(str(NEWS_DB))
    except Exception as e:
        meta["reason"] = f"db_open_fail:{type(e).__name__}"
        return {}, meta

    try:
        tables = [str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if "source_signals_daily" not in tables:
            meta["reason"] = "source_signal_table_missing"
            return {}, meta
        q = (
            "SELECT code, date8, source_score, source, event_title, impact, reason "
            "FROM source_signals_daily"
        )
        sdf = pd.read_sql_query(q, con)
        meta["rows_raw"] = int(len(sdf))
        if sdf.empty:
            meta["reason"] = "source_signal_empty"
            return {}, meta

        sdf["code"] = sdf["code"].map(_norm_code6)
        sdf["date8"] = sdf["date8"].map(_norm_date8)
        sdf["score"] = pd.to_numeric(sdf["source_score"], errors="coerce")
        sdf = sdf[(sdf["code"] != "") & (sdf["date8"] != "")]
        sdf = sdf.dropna(subset=["score"])
        max_date8 = _max_allowed_date8(str(reference_ymd))
        meta["max_allowed_date8"] = str(max_date8)
        sdf = sdf[sdf["date8"] <= str(max_date8)]
        if sdf.empty:
            meta["reason"] = "source_signal_no_rows_upto_reference"
            return {}, meta

        used_date8 = str(sdf["date8"].max() or "")
        meta["used_date8"] = used_date8
        lag_days = _ymd_lag_days(str(reference_ymd), used_date8)
        meta["used_lag_days"] = lag_days
        if lag_days is not None and lag_days > int(max_lag_days):
            meta["reason"] = f"source_signal_stale_lag_{lag_days}d"
            return {}, meta

        out: Dict[str, Dict[str, float]] = {}
        for code, grp in sdf.groupby("code"):
            scores = pd.to_numeric(grp["score"], errors="coerce").fillna(0.0)
            # Source events are sparse and stronger than article snippets. Use the strongest absolute event.
            idx = scores.abs().idxmax()
            row = grp.loc[idx]
            score = max(-1.0, min(1.0, float(row.get("score", 0.0) or 0.0)))
            out[str(code)] = {
                "score": round(score, 6),
                "article_count": int(len(grp)),
                "freshest_age_hours": -1.0,
                "source": "SOURCE_SIGNAL_DART",
                "trace_table": "source_signals_daily",
                "trace_used_date8": str(used_date8 or ""),
                "trace_used_lag_days": int(lag_days) if lag_days is not None else -1,
                "trace_reason": "ok",
                "entity_count_total": 0,
                "entity_top_tags": str(row.get("impact") or ""),
                "source_signal_score": round(score, 6),
                "source_signal_title": str(row.get("event_title") or ""),
                "source_signal_reason": str(row.get("reason") or ""),
            }

        meta["rows_used"] = int(sum(int(v.get("article_count", 0)) for v in out.values()))
        meta["reason"] = "ok" if out else "source_signal_zero_after_filter"
        return out, meta
    except Exception as e:
        meta["reason"] = f"source_signal_query_fail:{type(e).__name__}"
        return {}, meta
    finally:
        try:
            con.close()
        except Exception:
            pass


def _assess_news_score_quality(
    reason: str,
    mapped_rate: float,
    nonzero_rate: float,
    fallback_reason: str,
    fallback_coverage_rate: float,
    fallback_nonzero_rate: float,
    pass_mapped: float,
    pass_nonzero: float,
    warn_mapped: float,
) -> str:
    reason_norm = str(reason or "").strip().lower()
    fallback_reason_norm = str(fallback_reason or "").strip().lower()
    if reason_norm == "ok" and mapped_rate >= pass_mapped and nonzero_rate >= pass_nonzero:
        return "PASS"
    if (
        reason_norm == "candidate_article_coverage_zero"
        and fallback_reason_norm == "ok"
        and fallback_coverage_rate >= warn_mapped
        and fallback_nonzero_rate >= pass_nonzero
    ):
        return "WARN"
    if reason_norm in {"ok", "signals_zero_after_filter", "article_zero_after_filter"} and mapped_rate >= warn_mapped:
        return "WARN"
    return "FAIL"


def main() -> int:
    in_path = _pick_input()
    if not in_path.exists():
        raise SystemExit(f"[FATAL] missing input candidates file: {in_path}")

    df = _read_csv(in_path)
    if "code" not in df.columns:
        raise SystemExit(f"[FATAL] missing code column: {in_path}")

    df["code"] = df["code"].map(_norm_code6)
    input_asof_ymd = _max_date8(df)
    reference_ymd, reference_source = _resolve_reference_ymd(input_asof_ymd)

    max_lag_days = int(str(os.getenv("NEWS_MAX_LAG_DAYS", "2")).strip() or "2")
    max_lag_days = max(0, min(max_lag_days, 30))
    # 세션별 half_life 차등: 장중=4h(빠른감쇠), evening=18h, overnight/daily=36h
    _session_half_life = {
        "premarket": 6.0,
        "intraday": 4.0,
        "afterhours": 8.0,
        "evening": 18.0,
        "overnight": 36.0,
        "daily": 36.0,
    }
    _session = str(os.getenv("NEWS_SESSION_NAME", "daily")).strip().lower()
    _half_life_default = _session_half_life.get(_session, 18.0)
    half_life_hours = max(1.0, float(str(os.getenv("NEWS_SCORE_HALF_LIFE_HOURS", str(_half_life_default))).strip() or str(_half_life_default)))
    max_age_hours = max(1.0, float(str(os.getenv("NEWS_SCORE_MAX_AGE_HOURS", "72")).strip() or "72"))

    news_map, meta = _load_weighted_article_map(
        reference_ymd=reference_ymd,
        max_lag_days=max_lag_days,
        half_life_hours=half_life_hours,
        max_age_hours=max_age_hours,
    )
    signal_map, signal_meta = _load_signal_map(reference_ymd=reference_ymd, max_lag_days=max_lag_days)
    source_signal_map, source_signal_meta = _load_source_signal_map(reference_ymd=reference_ymd, max_lag_days=max_lag_days)
    candidate_codes = {str(c) for c in df["code"].astype(str).tolist() if str(c)}
    article_overlap_before_fallback = int(len(candidate_codes & set(news_map.keys())))
    signal_overlap = int(len(candidate_codes & set(signal_map.keys())))
    source_signal_overlap = int(len(candidate_codes & set(source_signal_map.keys())))
    source_signal_nonzero_overlap = int(
        sum(
            1
            for code in candidate_codes
            if abs(float((source_signal_map.get(str(code)) or {}).get("score", 0.0) or 0.0)) > 1e-12
        )
    )
    signal_nonzero_overlap = int(
        sum(
            1
            for code in candidate_codes
            if abs(float((signal_map.get(str(code)) or {}).get("score", 0.0) or 0.0)) > 1e-12
        )
    )
    if isinstance(meta, dict):
        meta["candidate_count"] = int(len(candidate_codes))
        meta["candidate_article_overlap"] = int(article_overlap_before_fallback)
        meta["candidate_signal_overlap"] = int(signal_overlap)
        meta["candidate_signal_nonzero_overlap"] = int(signal_nonzero_overlap)
        meta["candidate_source_signal_overlap"] = int(source_signal_overlap)
        meta["candidate_source_signal_nonzero_overlap"] = int(source_signal_nonzero_overlap)
        meta["signal_score_scale_observation"] = {
            "table": signal_meta.get("table"),
            "reason": signal_meta.get("reason"),
            "score_abs_max_before_scale": signal_meta.get("score_abs_max_before_scale"),
            "score_scale_applied": signal_meta.get("score_scale_applied"),
            "score_scale_divisor": signal_meta.get("score_scale_divisor"),
            "score_scale_up_blocked": signal_meta.get("score_scale_up_blocked"),
            "score_abs_max_after_scale": signal_meta.get("score_abs_max_after_scale"),
        }
        meta["source_signal_meta"] = source_signal_meta
    if news_map:
        fallback_added = 0
        for code, payload in signal_map.items():
            if str(code) not in news_map:
                payload_copy = dict(payload)
                if article_overlap_before_fallback == 0:
                    score_v = float(payload_copy.get("score", 0.0) or 0.0)
                    if abs(score_v) <= 1e-12:
                        payload_copy["source"] = "DB_SIGNAL_NO_ARTICLE_COVERAGE"
                        payload_copy["trace_reason"] = "candidate_article_coverage_zero_or_zero_score"
                news_map[str(code)] = payload_copy
                fallback_added += 1
        if isinstance(meta, dict):
            meta["fallback_signal_added_codes"] = int(fallback_added)
            meta["fallback_signal_total_codes"] = int(len(signal_map))
            meta["fallback_signal_reason"] = str(signal_meta.get("reason") or "")
    else:
        news_map, meta = signal_map, signal_meta

    source_signal_weight = max(
        0.0,
        min(1.0, float(str(os.getenv("NEWS_SOURCE_SIGNAL_WEIGHT", "0.25")).strip() or "0.25")),
    )
    source_signal_added = 0
    source_signal_blended = 0
    if source_signal_map and source_signal_weight > 0:
        for code, payload in source_signal_map.items():
            code_s = str(code)
            source_score = float((payload or {}).get("score", 0.0) or 0.0)
            if code_s in news_map:
                cur = dict(news_map.get(code_s) or {})
                cur_score = float(cur.get("score", 0.0) or 0.0)
                blended = (cur_score * (1.0 - source_signal_weight)) + (source_score * source_signal_weight)
                cur["score"] = round(max(-1.0, min(1.0, blended)), 6)
                cur["source"] = f"{cur.get('source', 'NEWS')}+SOURCE_SIGNAL"
                cur["trace_table"] = f"{cur.get('trace_table', '')}|source_signals_daily".strip("|")
                cur["trace_reason"] = "ok"
                cur["source_signal_score"] = round(source_score, 6)
                cur["source_signal_title"] = str((payload or {}).get("source_signal_title", ""))
                cur["source_signal_reason"] = str((payload or {}).get("source_signal_reason", ""))
                news_map[code_s] = cur
                source_signal_blended += 1
            else:
                news_map[code_s] = dict(payload)
                source_signal_added += 1
        if isinstance(meta, dict):
            meta["source_signal_weight"] = float(source_signal_weight)
            meta["source_signal_added_codes"] = int(source_signal_added)
            meta["source_signal_blended_codes"] = int(source_signal_blended)

    df["news_score"] = df["code"].map(lambda c: float((news_map.get(str(c)) or {}).get("score", 0.0)))
    df["news_sentiment"] = df["news_score"]
    df["news_source"] = df["code"].map(lambda c: str((news_map.get(str(c)) or {}).get("source", "FAIL_SOFT")))
    df["news_article_count"] = df["code"].map(lambda c: int((news_map.get(str(c)) or {}).get("article_count", 0)))
    df["news_freshest_age_hours"] = df["code"].map(
        lambda c: float((news_map.get(str(c)) or {}).get("freshest_age_hours", -1.0))
    )
    df["news_trace_table"] = df["code"].map(lambda c: str((news_map.get(str(c)) or {}).get("trace_table", "")))
    df["news_trace_used_date8"] = df["code"].map(lambda c: str((news_map.get(str(c)) or {}).get("trace_used_date8", "")))
    df["news_trace_used_lag_days"] = df["code"].map(lambda c: int((news_map.get(str(c)) or {}).get("trace_used_lag_days", -1)))
    df["news_trace_reason"] = df["code"].map(
        lambda c: str((news_map.get(str(c)) or {}).get("trace_reason", "FAIL_SOFT"))
    )
    df["news_entity_count"] = df["code"].map(lambda c: int((news_map.get(str(c)) or {}).get("entity_count_total", 0)))
    df["news_entity_top_tags"] = df["code"].map(
        lambda c: str((news_map.get(str(c)) or {}).get("entity_top_tags", ""))
    )
    df["news_source_signal_score"] = df["code"].map(
        lambda c: float((news_map.get(str(c)) or {}).get("source_signal_score", 0.0))
    )
    df["news_source_signal_reason"] = df["code"].map(
        lambda c: str((news_map.get(str(c)) or {}).get("source_signal_reason", ""))
    )
    df["news_implication_rows"] = df["code"].map(
        lambda c: int((news_map.get(str(c)) or {}).get("implication_rows", 0))
    )
    df["news_implication_top_actions"] = df["code"].map(
        lambda c: str((news_map.get(str(c)) or {}).get("implication_top_actions", ""))
    )
    df["news_implication_top_scopes"] = df["code"].map(
        lambda c: str((news_map.get(str(c)) or {}).get("implication_top_scopes", ""))
    )
    df["news_implication_positive_rows"] = df["code"].map(lambda c: int((news_map.get(str(c)) or {}).get("implication_positive_rows", 0)))
    df["news_implication_negative_rows"] = df["code"].map(lambda c: int((news_map.get(str(c)) or {}).get("implication_negative_rows", 0)))
    df["news_implication_neutral_rows"] = df["code"].map(lambda c: int((news_map.get(str(c)) or {}).get("implication_neutral_rows", 0)))
    df["news_implication_boost_rows"] = df["code"].map(lambda c: int((news_map.get(str(c)) or {}).get("implication_boost_rows", 0)))
    df["news_implication_penalize_rows"] = df["code"].map(lambda c: int((news_map.get(str(c)) or {}).get("implication_penalize_rows", 0)))
    df["news_implication_block_rows"] = df["code"].map(lambda c: int((news_map.get(str(c)) or {}).get("implication_block_rows", 0)))
    df["news_implication_reduce_size_rows"] = df["code"].map(lambda c: int((news_map.get(str(c)) or {}).get("implication_reduce_size_rows", 0)))
    df["news_implication_watch_rows"] = df["code"].map(lambda c: int((news_map.get(str(c)) or {}).get("implication_watch_rows", 0)))
    df["news_implication_ignore_rows"] = df["code"].map(lambda c: int((news_map.get(str(c)) or {}).get("implication_ignore_rows", 0)))
    df["news_implication_company_rows"] = df["code"].map(lambda c: int((news_map.get(str(c)) or {}).get("implication_company_rows", 0)))
    df["news_implication_sector_rows"] = df["code"].map(lambda c: int((news_map.get(str(c)) or {}).get("implication_sector_rows", 0)))
    df["news_implication_market_rows"] = df["code"].map(lambda c: int((news_map.get(str(c)) or {}).get("implication_market_rows", 0)))
    df["news_implication_macro_rows"] = df["code"].map(lambda c: int((news_map.get(str(c)) or {}).get("implication_macro_rows", 0)))
    df["news_implication_intraday_rows"] = df["code"].map(lambda c: int((news_map.get(str(c)) or {}).get("implication_intraday_rows", 0)))
    df["news_implication_short_rows"] = df["code"].map(lambda c: int((news_map.get(str(c)) or {}).get("implication_short_rows", 0)))
    df["news_implication_medium_rows"] = df["code"].map(lambda c: int((news_map.get(str(c)) or {}).get("implication_medium_rows", 0)))
    df["news_implication_strength_avg"] = df["code"].map(lambda c: float((news_map.get(str(c)) or {}).get("implication_strength_avg", 0.0)))
    df["news_implication_confidence_avg"] = df["code"].map(lambda c: float((news_map.get(str(c)) or {}).get("implication_confidence_avg", 0.0)))
    df["news_implication_directness_score"] = df["code"].map(lambda c: float((news_map.get(str(c)) or {}).get("implication_directness_score", 0.0)))
    df["news_implication_risk_score"] = df["code"].map(lambda c: float((news_map.get(str(c)) or {}).get("implication_risk_score", 0.0)))

    _write_csv(OUT, df)

    mapped = int((~df["news_source"].isin(["FAIL_SOFT", "DB_SIGNAL_NO_ARTICLE_COVERAGE"])).sum())
    rows_n = max(1, int(len(df)))
    nonzero = int((pd.to_numeric(df["news_score"], errors="coerce").fillna(0.0) != 0).sum())
    mapped_rate = float(mapped) / float(rows_n)
    nonzero_rate = float(nonzero) / float(rows_n)
    fallback_coverage_rate = float(signal_overlap) / float(rows_n)
    fallback_nonzero_rate = float(signal_nonzero_overlap) / float(rows_n)
    fallback_reason = str(meta.get("fallback_signal_reason") or "")
    reason = str(meta.get("reason") or "")
    rows_used = int(meta.get("rows_used") or 0) if isinstance(meta, dict) else 0
    if rows_used > 0 and article_overlap_before_fallback == 0:
        reason = "candidate_article_coverage_zero"
        if signal_nonzero_overlap <= 0:
            reason = "candidate_article_coverage_zero_with_zero_signal_fallback"
        if isinstance(meta, dict):
            meta["reason"] = reason

    pass_mapped = max(0.0, min(1.0, float(str(os.getenv("NEWS_SCORE_PASS_MAPPED_RATE", "0.60")).strip() or "0.60")))
    pass_nonzero = max(0.0, min(1.0, float(str(os.getenv("NEWS_SCORE_PASS_NONZERO_RATE", "0.10")).strip() or "0.10")))
    warn_mapped = max(0.0, min(1.0, float(str(os.getenv("NEWS_SCORE_WARN_MAPPED_RATE", "0.40")).strip() or "0.40")))

    quality = _assess_news_score_quality(
        reason=reason,
        mapped_rate=mapped_rate,
        nonzero_rate=nonzero_rate,
        fallback_reason=fallback_reason,
        fallback_coverage_rate=fallback_coverage_rate,
        fallback_nonzero_rate=fallback_nonzero_rate,
        pass_mapped=pass_mapped,
        pass_nonzero=pass_nonzero,
        warn_mapped=warn_mapped,
    )

    status = {
        "generated_at": _now_kst().isoformat(timespec="seconds"),
        "input": str(in_path),
        "output": str(OUT),
        "input_asof_ymd": input_asof_ymd,
        "asof_ymd": reference_ymd,
        "reference_source": reference_source,
        "rows": int(len(df)),
        "mapped_rows": mapped,
        "mapped_rate": round(mapped_rate, 6),
        "nonzero_rows": nonzero,
        "nonzero_rate": round(nonzero_rate, 6),
        "quality": quality,
        "max_lag_days": max_lag_days,
        "half_life_hours": half_life_hours,
        "max_age_hours": max_age_hours,
        "meta": meta,
        "fallback_signal_nonzero_rows": int(signal_nonzero_overlap),
        "fallback_signal_nonzero_rate": round(fallback_nonzero_rate, 6),
        "source_signal_rows": int(source_signal_overlap),
        "source_signal_nonzero_rows": int(source_signal_nonzero_overlap),
        "source_signal_meta": source_signal_meta,
        "trace": {
            "enabled": True,
            "rows_with_trace_table": int((df["news_trace_table"].astype(str).str.len() > 0).sum()),
            "rows_with_trace_date8": int((df["news_trace_used_date8"].astype(str).str.len() == 8).sum()),
            "rows_with_trace_reason_ok": int((df["news_trace_reason"].astype(str).str.upper() == "OK").sum()),
        },
        "ner_summary": {
            "enabled": True,
            "rows_with_entity": int((pd.to_numeric(df["news_entity_count"], errors="coerce").fillna(0) > 0).sum()),
            "coverage_rate": round(
                float((pd.to_numeric(df["news_entity_count"], errors="coerce").fillna(0) > 0).sum()) / float(rows_n),
                6,
            ),
            "top_tags": list(meta.get("ner_top_tags") or []),
            "rows_with_tags_source": int(meta.get("ner_rows_with_tags") or 0),
        },
    }

    st = LOGS / f"news_score_status_{reference_ymd}.json"
    st_latest = LOGS / "news_score_status_latest.json"
    st.write_text(json.dumps(status, ensure_ascii=True, indent=2), encoding="utf-8")
    st_latest.write_text(json.dumps(status, ensure_ascii=True, indent=2), encoding="utf-8")

    _log_print(
        f"[NEWS_SCORE] input={in_path.name} rows={len(df)} mapped={mapped} "
        f"mapped_rate={mapped_rate:.2%} nonzero_rate={nonzero_rate:.2%} "
        f"quality={quality} reason={meta.get('reason')}"
    )
    _log_print(f"[NEWS_SCORE] wrote {OUT}")
    _log_print(f"[NEWS_SCORE] status={st}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
