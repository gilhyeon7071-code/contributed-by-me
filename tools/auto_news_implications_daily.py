from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "news_trading" / "data" / "trading.db"
LOGS = ROOT / "2_Logs"
OUT_CSV = LOGS / "auto_news_implications_latest.csv"
STATUS_LATEST = LOGS / "auto_news_implications_status_latest.json"
CONTENT_SIDECAR = LOGS / "news_article_content_extract_latest.csv"
KST = timezone(timedelta(hours=9))
VERSION = "IMPLICATION_RULE_V1"

FIELDS = [
    "asof_ymd",
    "source_url",
    "source_title",
    "scope",
    "sector_tag",
    "related_codes",
    "direction",
    "action",
    "strength",
    "confidence",
    "horizon",
    "implication",
    "applies_to_existing_only",
    "direct_candidate_allowed",
    "reviewer",
    "reviewed_at",
]
PROMOTABLE_ACTIONS = {"boost", "watch", "reduce_size", "penalize", "block"}
VALID_SCOPES = {"market", "sector", "company", "macro"}
VALID_DIRECTIONS = {"positive", "negative", "neutral"}
VALID_HORIZONS = {"intraday", "short", "medium", "long"}


def _now_kst() -> datetime:
    return datetime.now(KST)


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _norm_date8(value: Any) -> str:
    text = re.sub(r"[^0-9]", "", _as_text(value))
    return text[:8] if len(text) >= 8 else ""


def _norm_code(value: Any) -> str:
    text = re.sub(r"[^0-9]", "", _as_text(value))
    return text.zfill(6)[-6:] if text else ""


def _table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in con.execute(f'PRAGMA table_info("{table}")').fetchall()}


def _require_columns(con: sqlite3.Connection) -> List[str]:
    required = {
        "date8",
        "code",
        "query",
        "title",
        "originallink",
        "link",
        "source",
        "entity_tags",
        "implication_scope",
        "implication_direction",
        "implication_strength",
        "implication_confidence",
        "implication_horizon",
        "implication_action",
        "implication_reason",
        "implication_version",
    }
    existing = _table_columns(con, "news_articles_naver")
    return sorted(required - existing)


def _read_rows(con: sqlite3.Connection, as_of: str, lookback_days: int, limit: int) -> List[Dict[str, Any]]:
    end_d = datetime.strptime(as_of, "%Y%m%d").date()
    start_d = end_d - timedelta(days=max(0, int(lookback_days)))
    query = """
        SELECT date8, code, query, title, originallink, link, source, entity_tags,
               implication_scope, implication_direction, implication_strength,
               implication_confidence, implication_horizon, implication_action,
               implication_reason, implication_version, fetched_at
        FROM news_articles_naver
        WHERE date8 >= ? AND date8 <= ?
        ORDER BY date8 DESC, fetched_at DESC
        LIMIT ?
    """
    cur = con.execute(query, (start_d.strftime("%Y%m%d"), as_of, max(1, int(limit))))
    columns = [str(col[0]) for col in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _sector_tag(row: Dict[str, Any]) -> str:
    tags = [x.strip() for x in _as_text(row.get("entity_tags")).replace(",", "|").split("|") if x.strip()]
    if tags:
        return tags[0][:80]
    query = _as_text(row.get("query"))
    return query[:80] if query else ""


def _read_content_sidecar(path: Path) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Any]]:
    meta: Dict[str, Any] = {
        "enabled": True,
        "input": str(path),
        "rows_raw": 0,
        "rows_usable": 0,
        "quality": "MISSING",
    }
    if not path.exists():
        return {}, meta
    out: Dict[str, Dict[str, str]] = {}
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                meta["rows_raw"] = int(meta["rows_raw"]) + 1
                url = _as_text(row.get("url"))
                status = _as_text(row.get("extract_status")).lower()
                snippet = _as_text(row.get("evidence_snippet"))
                if not url or status != "ok" or not snippet:
                    continue
                out[url] = {
                    "evidence_snippet": snippet[:240],
                    "matched_terms": _as_text(row.get("matched_terms")),
                    "content_hash": _as_text(row.get("content_hash")),
                }
    except Exception as exc:
        meta["quality"] = f"FAIL:{type(exc).__name__}"
        return {}, meta
    meta["rows_usable"] = int(len(out))
    meta["quality"] = "PASS"
    return out, meta


def _make_implication(row: Dict[str, Any], content_sidecar: Dict[str, Dict[str, str]]) -> str:
    title = _as_text(row.get("title"))
    reason = _as_text(row.get("implication_reason"))
    url = _as_text(row.get("originallink")) or _as_text(row.get("link"))
    content = content_sidecar.get(url, {})
    snippet = _as_text(content.get("evidence_snippet"))
    matched_terms = _as_text(content.get("matched_terms"))
    if title and reason:
        base = f"{title} / auto_reason={reason}"
    else:
        base = title or reason
    if snippet:
        suffix = f" / body_evidence={snippet}"
        if matched_terms:
            suffix += f" / body_terms={matched_terms}"
        return f"{base}{suffix}"
    return base


def _promote_row(
    row: Dict[str, Any],
    *,
    min_strength: float,
    min_confidence: float,
    now_text: str,
    content_sidecar: Dict[str, Dict[str, str]],
) -> Tuple[Dict[str, str] | None, str]:
    if _as_text(row.get("implication_version")) != VERSION:
        return None, "version_mismatch"
    asof = _norm_date8(row.get("date8"))
    if len(asof) != 8:
        return None, "asof_ymd_invalid"

    source_url = _as_text(row.get("originallink")) or _as_text(row.get("link"))
    if not source_url:
        return None, "source_url_missing"

    scope = _as_text(row.get("implication_scope")).lower()
    direction = _as_text(row.get("implication_direction")).lower()
    action = _as_text(row.get("implication_action")).lower()
    horizon = _as_text(row.get("implication_horizon")).lower() or "short"
    if scope not in VALID_SCOPES:
        return None, "scope_invalid"
    if direction not in VALID_DIRECTIONS:
        return None, "direction_invalid"
    if horizon not in VALID_HORIZONS:
        return None, "horizon_invalid"
    if action not in PROMOTABLE_ACTIONS:
        return None, "action_not_promotable"

    strength = max(0.0, min(1.0, _as_float(row.get("implication_strength"), 0.0)))
    confidence = max(0.0, min(1.0, _as_float(row.get("implication_confidence"), 0.0)))
    if strength < min_strength:
        return None, "strength_below_min"
    if confidence < min_confidence:
        return None, "confidence_below_min"

    code = _norm_code(row.get("code"))
    if scope == "company" and not code:
        return None, "company_related_codes_missing"

    sector_tag = _sector_tag(row) if scope == "sector" else ""
    if scope == "sector" and not sector_tag:
        return None, "sector_tag_missing"

    return {
        "asof_ymd": asof,
        "source_url": source_url,
        "source_title": _as_text(row.get("title")),
        "scope": scope,
        "sector_tag": sector_tag,
        "related_codes": code,
        "direction": direction,
        "action": action,
        "strength": f"{strength:.6f}",
        "confidence": f"{confidence:.6f}",
        "horizon": horizon,
        "implication": _make_implication(row, content_sidecar),
        "applies_to_existing_only": "True",
        "direct_candidate_allowed": "False",
        "reviewer": "auto_news_implication",
        "reviewed_at": now_text,
    }, ""


def _dedupe(rows: Iterable[Dict[str, str]]) -> Tuple[List[Dict[str, str]], int]:
    out: List[Dict[str, str]] = []
    seen: set[tuple[str, ...]] = set()
    duplicate_count = 0
    for row in rows:
        keys = [
            ("url", row.get("source_url", "")),
            (
                "title_code",
                row.get("asof_ymd", ""),
                row.get("source_title", ""),
                row.get("related_codes", ""),
            ),
            (
                "semantic",
                row.get("asof_ymd", ""),
                row.get("scope", ""),
                row.get("sector_tag", ""),
                row.get("action", ""),
                row.get("implication", ""),
            ),
        ]
        if any(tuple(key) in seen for key in keys):
            duplicate_count += 1
            continue
        for key in keys:
            seen.add(tuple(key))
        out.append(row)
    return out, duplicate_count


def _atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp_path.write_text(text, encoding=encoding)
    tmp_path.replace(path)


def _write_csv(path: Path, rows: Iterable[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with tmp_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in FIELDS})
    tmp_path.replace(path)


def _write_status(status: Dict[str, Any]) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    text = json.dumps(status, ensure_ascii=False, indent=2)
    _atomic_write_text(STATUS_LATEST, text, encoding="utf-8")
    stamp = _now_kst().strftime("%Y%m%d_%H%M%S")
    _atomic_write_text(LOGS / f"auto_news_implications_status_{stamp}.json", text, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Promote automatic news implications into analysis-only rows.")
    parser.add_argument("--db", default=str(DB))
    parser.add_argument("--output", default=str(OUT_CSV))
    parser.add_argument("--as-of", default=_now_kst().strftime("%Y%m%d"))
    parser.add_argument("--lookback-days", type=int, default=3)
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--min-strength", type=float, default=0.45)
    parser.add_argument("--min-confidence", type=float, default=0.60)
    parser.add_argument("--content-sidecar", default=str(CONTENT_SIDECAR))
    parser.add_argument("--content-sidecar-skip", action="store_true")
    args = parser.parse_args()

    db_path = Path(args.db)
    out_path = Path(args.output)
    content_sidecar_path = Path(args.content_sidecar)
    content_sidecar: Dict[str, Dict[str, str]] = {}
    content_meta: Dict[str, Any] = {
        "enabled": False,
        "input": str(content_sidecar_path),
        "rows_raw": 0,
        "rows_usable": 0,
        "quality": "DISABLED",
    }
    if not bool(args.content_sidecar_skip):
        content_sidecar, content_meta = _read_content_sidecar(content_sidecar_path)
    now_text = _now_kst().isoformat(timespec="seconds")
    status: Dict[str, Any] = {
        "generated_at": now_text,
        "db": str(db_path),
        "output": str(out_path),
        "asof_ymd": _norm_date8(args.as_of),
        "lookback_days": int(args.lookback_days),
        "limit": int(args.limit),
        "min_strength": float(args.min_strength),
        "min_confidence": float(args.min_confidence),
        "version": VERSION,
        "direct_candidate_policy": "forbidden",
        "candidate_append": False,
        "content_sidecar": content_meta,
        "quality": "FAIL",
    }
    if len(str(status["asof_ymd"])) != 8:
        status["reason"] = "asof_ymd_invalid"
        _write_status(status)
        print(json.dumps(status, ensure_ascii=False, indent=2))
        return 1
    if not db_path.exists():
        status["reason"] = "db_missing"
        _write_status(status)
        print(json.dumps(status, ensure_ascii=False, indent=2))
        return 1

    reject_counts: Dict[str, int] = {}
    promoted: List[Dict[str, str]] = []
    with sqlite3.connect(str(db_path)) as con:
        missing = _require_columns(con)
        if missing:
            status["reason"] = "missing_columns"
            status["missing_columns"] = missing
            _write_status(status)
            print(json.dumps(status, ensure_ascii=False, indent=2))
            return 1
        raw_rows = _read_rows(con, str(status["asof_ymd"]), int(args.lookback_days), int(args.limit))

    for raw in raw_rows:
        clean, reason = _promote_row(
            raw,
            min_strength=float(args.min_strength),
            min_confidence=float(args.min_confidence),
            now_text=now_text,
            content_sidecar=content_sidecar,
        )
        if clean is None:
            reject_counts[reason] = reject_counts.get(reason, 0) + 1
            continue
        promoted.append(clean)

    deduped, duplicate_count = _dedupe(promoted)
    rows_with_body_evidence = sum(1 for row in deduped if "body_evidence=" in str(row.get("implication", "")))
    _write_csv(out_path, deduped)
    status.update(
        {
            "rows_raw": int(len(raw_rows)),
            "rows_promoted_before_dedupe": int(len(promoted)),
            "rows_promoted": int(len(deduped)),
            "rows_with_body_evidence": int(rows_with_body_evidence),
            "rows_duplicate": int(duplicate_count),
            "reject_counts": dict(sorted(reject_counts.items())),
            "quality": "PASS",
        }
    )
    _write_status(status)
    print(json.dumps(status, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
