from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

import trafilatura


ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "news_trading" / "data" / "trading.db"
LOGS = ROOT / "2_Logs"
OUT_CSV = LOGS / "news_article_content_extract_latest.csv"
STATUS_LATEST = LOGS / "news_article_content_extract_status_latest.json"
KST = timezone(timedelta(hours=9))
VERSION = "TRAFILATURA_EXTRACT_V1"

FIELDS = [
    "date8",
    "code",
    "source",
    "title",
    "url",
    "implication_scope",
    "implication_direction",
    "implication_action",
    "implication_strength",
    "implication_confidence",
    "extract_status",
    "content_text_len",
    "content_hash",
    "evidence_snippet",
    "matched_terms",
    "extractor",
    "extracted_at",
]

KEY_TERMS = (
    "FOMO",
    "쏠림",
    "추격매수",
    "차익실현",
    "실적",
    "잠정실적",
    "어닝",
    "정책",
    "예산",
    "규제",
    "수주",
    "계약",
    "공급",
    "급락",
    "급등",
    "외국인",
    "기관",
    "순매수",
    "순매도",
    "환율",
    "금리",
    "전력",
    "반도체",
    "AI",
    "로봇",
    "데이터센터",
)

NOISE_PATTERNS = (
    r"AI\s*프리즘\*?\s*맞춤형\s*경제\s*브리핑.*?(?=\[주요\s*이슈\s*브리핑\]|■|\[)",
    r"편집자\s*주\s*:\s*.*?(?=\[주요\s*이슈\s*브리핑\]|■|\[)",
    r"AI\s*핵심\s*요약\s*beta-.*?(?=\[[^\]]+\]|\S+\s*=\s*)",
    r"!AI가\s*자동\s*생성한\s*요약으로\s*정확하지\s*않을\s*수\s*있어요\.",
    r"이\s*기사를\s*추천합니다\..*$",
    r"ⓒ\s*[^,]+,\s*무단\s*전재.*$",
)


def _now_kst() -> datetime:
    return datetime.now(KST)


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _norm_date8(value: Any) -> str:
    text = re.sub(r"[^0-9]", "", _as_text(value))
    return text[:8] if len(text) >= 8 else ""


def _norm_code(value: Any) -> str:
    text = re.sub(r"[^0-9]", "", _as_text(value))
    return text.zfill(6)[-6:] if text else ""


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in con.execute(f'PRAGMA table_info("{table}")').fetchall()}


def _require_columns(con: sqlite3.Connection) -> List[str]:
    required = {
        "date8",
        "code",
        "source",
        "title",
        "originallink",
        "link",
        "implication_scope",
        "implication_direction",
        "implication_action",
        "implication_strength",
        "implication_confidence",
        "fetched_at",
    }
    return sorted(required - _table_columns(con, "news_articles_naver"))


def _read_rows(
    con: sqlite3.Connection,
    as_of: str,
    lookback_days: int,
    max_rows: int,
    min_strength: float,
    min_confidence: float,
) -> List[Dict[str, Any]]:
    end_d = datetime.strptime(as_of, "%Y%m%d").date()
    start_d = end_d - timedelta(days=max(0, int(lookback_days)))
    query = """
        SELECT date8, code, source, title, originallink, link,
               implication_scope, implication_direction, implication_action,
               implication_strength, implication_confidence, fetched_at
        FROM news_articles_naver
        WHERE date8 >= ? AND date8 <= ?
          AND COALESCE(implication_action, '') IN ('boost', 'watch', 'reduce_size', 'penalize', 'block')
          AND COALESCE(implication_strength, 0) >= ?
          AND COALESCE(implication_confidence, 0) >= ?
        ORDER BY date8 DESC, fetched_at DESC
        LIMIT ?
    """
    cur = con.execute(
        query,
        (
            start_d.strftime("%Y%m%d"),
            as_of,
            float(min_strength),
            float(min_confidence),
            max(1, int(max_rows)),
        ),
    )
    columns = [str(col[0]) for col in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()[:16]


def _matched_terms(text: str) -> List[str]:
    low = text.lower()
    out: List[str] = []
    for term in KEY_TERMS:
        if term.lower() in low:
            out.append(term)
    return out


def _remove_noise(text: str) -> str:
    clean = re.sub(r"\s+", " ", text).strip()
    for pattern in NOISE_PATTERNS:
        clean = re.sub(pattern, " ", clean, flags=re.IGNORECASE)
        clean = re.sub(r"\s+", " ", clean).strip()
    return clean


def _snippet(text: str, terms: List[str], max_chars: int) -> str:
    clean = _remove_noise(text)
    if not clean:
        return ""
    positions = [clean.lower().find(term.lower()) for term in terms if clean.lower().find(term.lower()) >= 0]
    pos = min(positions) if positions else 0
    start = max(0, pos - max_chars // 3)
    end = min(len(clean), start + max_chars)
    return clean[start:end].strip()


def _extract_url(url: str) -> Dict[str, Any]:
    if not url:
        return {"status": "url_missing", "text": ""}
    try:
        downloaded = trafilatura.fetch_url(url)
    except Exception as exc:
        return {"status": f"fetch_error:{type(exc).__name__}", "text": ""}
    if not downloaded:
        return {"status": "fetch_empty", "text": ""}
    try:
        extracted = trafilatura.extract(
            downloaded,
            url=url,
            include_comments=False,
            include_tables=False,
            favor_precision=True,
            output_format="txt",
        )
    except Exception as exc:
        return {"status": f"extract_error:{type(exc).__name__}", "text": ""}
    if not extracted or not extracted.strip():
        return {"status": "extract_empty", "text": ""}
    return {"status": "ok", "text": extracted.strip()}


def _write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in FIELDS})


def _write_status(status: Dict[str, Any]) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    text = json.dumps(status, ensure_ascii=False, indent=2)
    STATUS_LATEST.write_text(text, encoding="utf-8")
    stamp = _now_kst().strftime("%Y%m%d_%H%M%S")
    (LOGS / f"news_article_content_extract_status_{stamp}.json").write_text(text, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract selected article body evidence with trafilatura.")
    parser.add_argument("--db", default=str(DB))
    parser.add_argument("--output", default=str(OUT_CSV))
    parser.add_argument("--as-of", default=_now_kst().strftime("%Y%m%d"))
    parser.add_argument("--lookback-days", type=int, default=3)
    parser.add_argument("--max-rows", type=int, default=10)
    parser.add_argument("--min-strength", type=float, default=0.45)
    parser.add_argument("--min-confidence", type=float, default=0.60)
    parser.add_argument("--snippet-chars", type=int, default=240)
    args = parser.parse_args()

    db_path = Path(args.db)
    out_path = Path(args.output)
    as_of = _norm_date8(args.as_of)
    started = _now_kst()
    status: Dict[str, Any] = {
        "generated_at": started.isoformat(timespec="seconds"),
        "db": str(db_path),
        "output": str(out_path),
        "asof_ymd": as_of,
        "lookback_days": int(args.lookback_days),
        "max_rows": int(args.max_rows),
        "min_strength": float(args.min_strength),
        "min_confidence": float(args.min_confidence),
        "snippet_chars": int(args.snippet_chars),
        "extractor": VERSION,
        "stores_full_body": False,
        "trading_effect": False,
        "quality": "FAIL",
    }
    if len(as_of) != 8:
        status["reason"] = "asof_ymd_invalid"
        _write_status(status)
        print(json.dumps(status, ensure_ascii=False, indent=2))
        return 1
    if not db_path.exists():
        status["reason"] = "db_missing"
        _write_status(status)
        print(json.dumps(status, ensure_ascii=False, indent=2))
        return 1

    with sqlite3.connect(str(db_path)) as con:
        missing = _require_columns(con)
        if missing:
            status["reason"] = "missing_columns"
            status["missing_columns"] = missing
            _write_status(status)
            print(json.dumps(status, ensure_ascii=False, indent=2))
            return 1
        rows = _read_rows(
            con,
            as_of,
            int(args.lookback_days),
            int(args.max_rows),
            float(args.min_strength),
            float(args.min_confidence),
        )

    out_rows: List[Dict[str, Any]] = []
    status_counts: Dict[str, int] = {}
    seen_urls: set[str] = set()
    duplicate_urls = 0
    for row in rows:
        url = _as_text(row.get("originallink")) or _as_text(row.get("link"))
        if not url:
            status_counts["url_missing"] = status_counts.get("url_missing", 0) + 1
            continue
        if url in seen_urls:
            duplicate_urls += 1
            continue
        seen_urls.add(url)
        result = _extract_url(url)
        text = _as_text(result.get("text"))
        terms = _matched_terms(text)
        extract_status = _as_text(result.get("status"))
        status_counts[extract_status] = status_counts.get(extract_status, 0) + 1
        out_rows.append(
            {
                "date8": _norm_date8(row.get("date8")),
                "code": _norm_code(row.get("code")),
                "source": _as_text(row.get("source")),
                "title": _as_text(row.get("title")),
                "url": url,
                "implication_scope": _as_text(row.get("implication_scope")),
                "implication_direction": _as_text(row.get("implication_direction")),
                "implication_action": _as_text(row.get("implication_action")),
                "implication_strength": f"{_as_float(row.get('implication_strength')):.6f}",
                "implication_confidence": f"{_as_float(row.get('implication_confidence')):.6f}",
                "extract_status": extract_status,
                "content_text_len": len(text),
                "content_hash": _content_hash(text) if text else "",
                "evidence_snippet": _snippet(text, terms, max(80, int(args.snippet_chars))),
                "matched_terms": "|".join(terms),
                "extractor": VERSION,
                "extracted_at": started.isoformat(timespec="seconds"),
            }
        )

    _write_csv(out_path, out_rows)
    ok_rows = sum(1 for row in out_rows if row.get("extract_status") == "ok")
    status.update(
        {
            "rows_selected": int(len(rows)),
            "rows_written": int(len(out_rows)),
            "rows_extract_ok": int(ok_rows),
            "duplicate_urls": int(duplicate_urls),
            "status_counts": dict(sorted(status_counts.items())),
            "quality": "PASS" if out_rows else "WARN",
            "reason": "ok" if out_rows else "no_rows_written",
        }
    )
    _write_status(status)
    print(json.dumps(status, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
