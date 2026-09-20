from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"
NEWS_DB = ROOT / "news_trading" / "data" / "trading.db"

IN_FILES = [
    LOGS / "candidates_latest_data.with_final_score.csv",
    LOGS / "candidates_latest_data.with_news_score.csv",
    LOGS / "candidates_latest_data.with_sector_score.csv",
    LOGS / "candidates_latest_data.filtered.csv",
    LOGS / "candidates_latest_data.csv",
]

OUT_CSV = LOGS / "news_source_match_quality_report_latest.csv"
OUT_JSON = LOGS / "news_source_match_quality_report_latest.json"
KST = timezone(timedelta(hours=9))

RANKING_TITLE_PATTERNS = [
    "상위",
    "하위",
    "상승률",
    "하락률",
    "순매수",
    "순매도",
    "매수체결",
    "매도체결",
    "거래량",
    "테마동향",
    "기술적 분석",
    "변동성완화장치",
    "VI 발동 종목",
]

MANUAL_ALIASES: Dict[str, List[str]] = {
    "삼성에스디에스": ["삼성SDS"],
    "HD현대에너지솔루션": ["HD현대에너지"],
    "글로벌텍스프리": ["GTF"],
}


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


def _table_exists(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return row is not None


def _aliases(name: str) -> List[str]:
    clean = str(name or "").strip()
    vals = [clean] if clean else []
    vals.extend(MANUAL_ALIASES.get(clean, []))
    out: List[str] = []
    for v in vals:
        s = str(v or "").strip()
        if s and s not in out:
            out.append(s)
    return out


def _has_alias(texts: Iterable[object], aliases: Iterable[str]) -> bool:
    joined = " ".join(str(x or "") for x in texts)
    return any(a and a in joined for a in aliases)


def _is_short_or_generic_alias(aliases: Iterable[str]) -> bool:
    for alias in aliases:
        a = str(alias or "").strip()
        if 0 < len(a) <= 2:
            return True
        if re.fullmatch(r"[A-Za-z]{1,3}", a):
            return True
    return False


def _is_ranking_title(title: object) -> bool:
    text = str(title or "")
    return any(p in text for p in RANKING_TITLE_PATTERNS)


def _date8_recent(v: object, reference: datetime, max_age_days: int) -> bool:
    s = re.sub(r"[^0-9]", "", str(v or ""))[:8]
    if len(s) != 8:
        return False
    try:
        d = datetime.strptime(s, "%Y%m%d").date()
    except ValueError:
        return False
    return 0 <= (reference.date() - d).days <= max_age_days


def _source_bucket(total: int, direct_rows: int, stale_direct_rows: int) -> str:
    if direct_rows > 0:
        return "direct_fresh"
    if stale_direct_rows > 0:
        return "direct_stale_or_review"
    if total > 0:
        return "covered_low_confidence"
    return "uncovered"


def _combined_bucket(direct_fresh: int, direct_review: int, total: int) -> str:
    if direct_fresh > 0:
        return "direct_fresh_observe"
    if direct_review > 0:
        return "direct_stale_or_review"
    if total > 0:
        return "covered_low_confidence"
    return "uncovered"


def _load_candidates(path: Path) -> pd.DataFrame:
    df = _read_csv(path)
    if "code" not in df.columns:
        raise ValueError("input_no_code")
    if "name" not in df.columns:
        df["name"] = ""
    df["code"] = df["code"].map(_norm_code6)
    df["name"] = df["name"].astype(str).str.strip()
    return df[df["code"] != ""].drop_duplicates("code", keep="first").reset_index(drop=True)


def _load_google(con: sqlite3.Connection, code: str) -> List[sqlite3.Row]:
    if not _table_exists(con, "news_articles_google_rss"):
        return []
    return list(
        con.execute(
            """
            SELECT title, description, link, date8
            FROM news_articles_google_rss
            WHERE code = ?
            """,
            (code,),
        )
    )


def _load_kis(con: sqlite3.Connection, code: str) -> List[sqlite3.Row]:
    if not _table_exists(con, "news_articles_kis_title"):
        return []
    return list(
        con.execute(
            """
            SELECT title, iscd1, iscd2, iscd3, iscd4, iscd5, data_dt
            FROM news_articles_kis_title
            WHERE code = ?
            """,
            (code,),
        )
    )


def _kis_code_match(row: sqlite3.Row, code: str) -> bool:
    vals = [_norm_code6(row[k]) for k in ("iscd1", "iscd2", "iscd3", "iscd4", "iscd5")]
    return code in vals


def main() -> int:
    now = _now_kst()
    fresh_window_days = 3
    in_path = _pick_input()
    status: Dict[str, object] = {
        "generated_at": now.isoformat(timespec="seconds"),
        "mode": "read_only_match_quality",
        "db_write": False,
        "score_effect": False,
        "trading_effect": False,
        "input": str(in_path) if in_path else None,
        "db": str(NEWS_DB),
        "output_csv": str(OUT_CSV),
        "quality": "FAIL",
        "reason": "",
        "rows": 0,
    }

    if not in_path:
        status["reason"] = "input_missing"
    elif not NEWS_DB.exists():
        status["reason"] = "db_missing"
    else:
        try:
            candidates = _load_candidates(in_path)
            con = sqlite3.connect(f"file:{NEWS_DB.as_posix()}?mode=ro", uri=True, timeout=30.0)
            con.row_factory = sqlite3.Row
            con.execute("PRAGMA busy_timeout=30000")
            rows: List[Dict[str, object]] = []
            try:
                for r in candidates.itertuples(index=False):
                    code = str(getattr(r, "code"))
                    name = str(getattr(r, "name", "") or "")
                    aliases = _aliases(name)
                    short_alias = _is_short_or_generic_alias(aliases)

                    google = _load_google(con, code)
                    kis = _load_kis(con, code)

                    google_alias_hits = sum(1 for x in google if _has_alias((x["title"], x["description"]), aliases))
                    google_ranking_hits = sum(1 for x in google if _is_ranking_title(x["title"]))
                    google_score_candidate = 0 if short_alias else max(0, google_alias_hits - google_ranking_hits)
                    google_fresh_rows = sum(1 for x in google if _date8_recent(x["date8"], now, fresh_window_days))
                    google_direct_fresh_rows = (
                        0
                        if short_alias
                        else sum(
                            1
                            for x in google
                            if _has_alias((x["title"], x["description"]), aliases)
                            and not _is_ranking_title(x["title"])
                            and _date8_recent(x["date8"], now, fresh_window_days)
                        )
                    )
                    google_quality_bucket = _source_bucket(
                        len(google),
                        google_direct_fresh_rows,
                        google_score_candidate,
                    )

                    kis_code_hits = sum(1 for x in kis if _kis_code_match(x, code))
                    kis_alias_hits = sum(1 for x in kis if _has_alias((x["title"],), aliases))
                    kis_code_or_alias_hits = sum(1 for x in kis if _kis_code_match(x, code) or _has_alias((x["title"],), aliases))
                    kis_ranking_hits = sum(1 for x in kis if _is_ranking_title(x["title"]))
                    kis_score_candidate = sum(
                        1
                        for x in kis
                        if (_kis_code_match(x, code) or _has_alias((x["title"],), aliases))
                        and not _is_ranking_title(x["title"])
                    )
                    kis_fresh_rows = sum(1 for x in kis if _date8_recent(x["data_dt"], now, fresh_window_days))
                    kis_code_fresh_rows = sum(
                        1
                        for x in kis
                        if _kis_code_match(x, code)
                        and not _is_ranking_title(x["title"])
                        and _date8_recent(x["data_dt"], now, fresh_window_days)
                    )
                    kis_direct_fresh_rows = sum(
                        1
                        for x in kis
                        if (_kis_code_match(x, code) or _has_alias((x["title"],), aliases))
                        and not _is_ranking_title(x["title"])
                        and _date8_recent(x["data_dt"], now, fresh_window_days)
                    )
                    kis_quality_bucket = _source_bucket(
                        len(kis),
                        kis_direct_fresh_rows,
                        kis_score_candidate,
                    )

                    google_total = len(google)
                    kis_total = len(kis)
                    combined_direct_fresh_rows = google_direct_fresh_rows + kis_direct_fresh_rows
                    combined_direct_review_rows = google_score_candidate + kis_score_candidate
                    combined_total = google_total + kis_total
                    combined_quality_bucket = _combined_bucket(
                        combined_direct_fresh_rows,
                        combined_direct_review_rows,
                        combined_total,
                    )
                    rows.append(
                        {
                            "code": code,
                            "name": name,
                            "aliases": "|".join(aliases),
                            "short_or_generic_alias": bool(short_alias),
                            "google_total": google_total,
                            "google_alias_hits": int(google_alias_hits),
                            "google_ranking_title_hits": int(google_ranking_hits),
                            "google_match_rate": round(float(google_alias_hits) / float(max(1, google_total)), 6),
                            "google_score_candidate_rows": int(google_score_candidate),
                            "google_fresh_rows": int(google_fresh_rows),
                            "google_direct_fresh_rows": int(google_direct_fresh_rows),
                            "google_quality_bucket": google_quality_bucket,
                            "kis_total": kis_total,
                            "kis_code_hits": int(kis_code_hits),
                            "kis_alias_hits": int(kis_alias_hits),
                            "kis_code_or_alias_hits": int(kis_code_or_alias_hits),
                            "kis_ranking_title_hits": int(kis_ranking_hits),
                            "kis_match_rate": round(float(kis_code_or_alias_hits) / float(max(1, kis_total)), 6),
                            "kis_score_candidate_rows": int(kis_score_candidate),
                            "kis_fresh_rows": int(kis_fresh_rows),
                            "kis_code_fresh_rows": int(kis_code_fresh_rows),
                            "kis_direct_fresh_rows": int(kis_direct_fresh_rows),
                            "kis_quality_bucket": kis_quality_bucket,
                            "combined_direct_fresh_rows": int(combined_direct_fresh_rows),
                            "combined_direct_review_rows": int(combined_direct_review_rows),
                            "combined_quality_bucket": combined_quality_bucket,
                            "quality_observe_candidate": bool(combined_direct_fresh_rows > 0),
                            "score_effect": False,
                            "trading_effect": False,
                        }
                    )
            finally:
                con.close()

            out = pd.DataFrame(rows)
            LOGS.mkdir(parents=True, exist_ok=True)
            out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
            row_count = max(1, int(len(out)))
            status.update(
                {
                    "quality": "PASS",
                    "reason": "ok",
                    "rows": int(len(out)),
                    "google_covered_rows": int((out["google_total"] > 0).sum()) if len(out) else 0,
                    "kis_covered_rows": int((out["kis_total"] > 0).sum()) if len(out) else 0,
                    "short_or_generic_alias_rows": int(out["short_or_generic_alias"].sum()) if len(out) else 0,
                    "google_score_candidate_rows": int((out["google_score_candidate_rows"] > 0).sum()) if len(out) else 0,
                    "kis_score_candidate_rows": int((out["kis_score_candidate_rows"] > 0).sum()) if len(out) else 0,
                    "fresh_window_days": fresh_window_days,
                    "google_direct_fresh_rows": int((out["google_direct_fresh_rows"] > 0).sum()) if len(out) else 0,
                    "kis_direct_fresh_rows": int((out["kis_direct_fresh_rows"] > 0).sum()) if len(out) else 0,
                    "combined_direct_fresh_rows": int((out["combined_direct_fresh_rows"] > 0).sum()) if len(out) else 0,
                    "quality_observe_candidate_rows": int(out["quality_observe_candidate"].sum()) if len(out) else 0,
                    "combined_quality_buckets": out["combined_quality_bucket"].value_counts().to_dict() if len(out) else {},
                    "google_score_candidate_rate": round(
                        float((out["google_score_candidate_rows"] > 0).sum()) / float(row_count), 6
                    )
                    if len(out)
                    else 0.0,
                    "kis_score_candidate_rate": round(
                        float((out["kis_score_candidate_rows"] > 0).sum()) / float(row_count), 6
                    )
                    if len(out)
                    else 0.0,
                    "rules": {
                        "quality_rules_version": "observe_quality_v1",
                        "fresh_window_days": fresh_window_days,
                        "kis_candidate": "code in iscd1..iscd5 OR title contains alias, excluding ranking-style titles",
                        "google_candidate": "title/description contains alias, excluding ranking-style titles; short/generic aliases are blocked",
                        "direct_fresh_observe": "candidate has direct non-ranking Google/KIS match inside the fresh window; observe-only, no score/trading effect",
                    },
                }
            )
        except Exception as e:
            status["reason"] = f"{type(e).__name__}:{str(e)[:200]}"

    OUT_JSON.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        f"[NEWS_SOURCE_MATCH_QUALITY] quality={status['quality']} reason={status['reason']} "
        f"rows={status.get('rows')} latest={OUT_JSON}"
    )
    return 0 if status["quality"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
