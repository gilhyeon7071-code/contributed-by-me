"""Build a read-only shadow promotion report for news/recheck candidates.

The report answers "what would have been reviewed if NEWS_ONLY or missed-move
rows were considered for promotion?" It does not set execution flags, create
orders, change gates, or write back to candidate input files.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

CANDIDATES_CSV = LOG_DIR / "candidates_latest_data.with_final_score.csv"
ACTION_REVIEW_JSON = LOG_DIR / "candidate_action_review_latest.json"
ACTION_QUEUE_JSON = LOG_DIR / "candidate_action_queue_latest.json"
ENTRY_DECISION_JSON = LOG_DIR / "entry_decision_layers_runtime_latest.json"

OUT_JSON = LOG_DIR / "news_recheck_shadow_promotion_latest.json"
OUT_CSV = LOG_DIR / "news_recheck_shadow_promotion_latest.csv"

MIN_SHADOW_SCORE = 0.05
MIN_NEWS_ARTICLES = 3
MIN_NEWS_SCORE = 0.10
MIN_MOVE_REVIEW_RETURN_PCT = 1.5


def _now_kst() -> str:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).isoformat(timespec="seconds")


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return [{str(k): str(v) for k, v in row.items()} for row in csv.DictReader(f)]
    except Exception:
        return []


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value or "").strip()
        if text == "":
            return float(default)
        return float(text)
    except Exception:
        return float(default)


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _code(row: Dict[str, Any]) -> str:
    raw = "".join(ch for ch in str(row.get("code") or "").strip() if ch.isdigit())
    return raw.zfill(6)[-6:] if raw else ""


def _name(row: Dict[str, Any]) -> str:
    return str(row.get("name") or "").strip()


def _candidate_lookup(rows: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        code = _code(row)
        if code:
            out.setdefault(code, row)
    return out


def _shadow_verdict(row: Dict[str, Any], candidate: Dict[str, Any]) -> tuple[str, str]:
    origin_hybrid = str(candidate.get("candidate_origin_hybrid") or "").strip().upper()
    review_bucket = str(row.get("review_bucket") or "").strip().upper()
    final_score = _f(candidate.get("final_score"), _f(row.get("final_score"), 0.0))
    news_articles = int(_f(candidate.get("news_article_count"), _f(row.get("news_article_count"), 0.0)))
    news_score = _f(candidate.get("news_score"), _f(row.get("news_score"), 0.0))
    ret_pct = _f(row.get("return_pct_since_first_seen"), 0.0)
    sector_allowed = _truthy(candidate.get("sector_entry_allowed") or row.get("sector_entry_allowed"))
    execution_pool = _truthy(candidate.get("execution_pool") or row.get("execution_pool"))

    reasons: List[str] = []
    if origin_hybrid == "NEWS_ONLY":
        reasons.append("NEWS_ONLY")
        if final_score < MIN_SHADOW_SCORE:
            reasons.append(f"final_score<{MIN_SHADOW_SCORE:.2f}")
        if news_articles < MIN_NEWS_ARTICLES:
            reasons.append(f"news_articles<{MIN_NEWS_ARTICLES}")
        if news_score < MIN_NEWS_SCORE:
            reasons.append(f"news_score<{MIN_NEWS_SCORE:.2f}")
        if sector_allowed:
            reasons.append("sector_entry_already_allowed")
        if execution_pool:
            reasons.append("execution_pool_already_true")
        if (
            final_score >= MIN_SHADOW_SCORE
            and news_articles >= MIN_NEWS_ARTICLES
            and news_score >= MIN_NEWS_SCORE
            and not sector_allowed
            and not execution_pool
        ):
            return "SHADOW_REVIEW_READY", ";".join(reasons)
        return "SHADOW_OBSERVE_ONLY", ";".join(reasons)

    if review_bucket in {"RECHECK_MISSED_MOVE_CANDIDATE", "WATCH_MISSED_MOVE_CANDIDATE", "BLOCKED_MOVED_REVIEW"}:
        reasons.append(review_bucket)
        if ret_pct < MIN_MOVE_REVIEW_RETURN_PCT:
            reasons.append(f"return_pct<{MIN_MOVE_REVIEW_RETURN_PCT:.2f}")
        if sector_allowed:
            reasons.append("sector_entry_already_allowed")
        if execution_pool:
            reasons.append("execution_pool_already_true")
        if ret_pct >= MIN_MOVE_REVIEW_RETURN_PCT and not sector_allowed and not execution_pool:
            return "SHADOW_REVIEW_READY", ";".join(reasons)
        return "SHADOW_OBSERVE_ONLY", ";".join(reasons)

    return "SHADOW_NOT_ELIGIBLE", "not_news_only_or_missed_move"


def _merge_rows() -> List[Dict[str, Any]]:
    candidate_rows = _read_csv(CANDIDATES_CSV)
    candidate_by_code = _candidate_lookup(candidate_rows)
    review_doc = _read_json(ACTION_REVIEW_JSON)
    review_rows = review_doc.get("rows") if isinstance(review_doc.get("rows"), list) else []

    rows_by_key: Dict[tuple[str, str, str], Dict[str, Any]] = {}
    for cand in candidate_rows:
        code = _code(cand)
        if not code:
            continue
        if str(cand.get("candidate_origin_hybrid") or "").strip().upper() == "NEWS_ONLY":
            key = ("candidate", code, "NEWS_ONLY")
            rows_by_key[key] = {
                "source": "candidate",
                "code": code,
                "name": _name(cand),
                "review_bucket": "NEWS_ONLY",
                "return_pct_since_first_seen": 0.0,
                "current_change_pct": 0.0,
                "current_trading_value": 0.0,
                "reason": "news_only_candidate",
            }

    for row in review_rows:
        if not isinstance(row, dict):
            continue
        code = _code(row)
        if not code:
            continue
        bucket = str(row.get("review_bucket") or "").strip()
        if bucket not in {"RECHECK_MISSED_MOVE_CANDIDATE", "WATCH_MISSED_MOVE_CANDIDATE", "BLOCKED_MOVED_REVIEW"}:
            continue
        key = (str(row.get("source") or "review"), code, bucket)
        rows_by_key[key] = {
            "source": str(row.get("source") or "review"),
            "code": code,
            "name": _name(row) or _name(candidate_by_code.get(code, {})),
            "review_bucket": bucket,
            "return_pct_since_first_seen": _f(row.get("return_pct_since_first_seen"), 0.0),
            "current_change_pct": _f(row.get("current_change_pct"), 0.0),
            "current_trading_value": _f(row.get("current_trading_value"), 0.0),
            "reason": str(row.get("reason") or row.get("review_note") or "").strip(),
        }

    out: List[Dict[str, Any]] = []
    for row in rows_by_key.values():
        cand = candidate_by_code.get(str(row.get("code")), {})
        verdict, reason = _shadow_verdict(row, cand)
        merged = {
            **row,
            "shadow_verdict": verdict,
            "shadow_reason": reason,
            "shadow_trading_allowed": False,
            "candidate_origin": str(cand.get("candidate_origin") or ""),
            "candidate_origin_hybrid": str(cand.get("candidate_origin_hybrid") or ""),
            "final_score": _f(cand.get("final_score"), 0.0),
            "news_score": _f(cand.get("news_score"), 0.0),
            "news_article_count": int(_f(cand.get("news_article_count"), 0.0)),
            "sector_entry_allowed": _truthy(cand.get("sector_entry_allowed")),
            "execution_pool": _truthy(cand.get("execution_pool")),
        }
        out.append(merged)
    out.sort(
        key=lambda r: (
            1 if r.get("shadow_verdict") == "SHADOW_REVIEW_READY" else 0,
            _f(r.get("return_pct_since_first_seen"), 0.0),
            _f(r.get("final_score"), 0.0),
        ),
        reverse=True,
    )
    return out


def _summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    verdict_counts: Dict[str, int] = {}
    bucket_counts: Dict[str, int] = {}
    for row in rows:
        verdict = str(row.get("shadow_verdict") or "")
        bucket = str(row.get("review_bucket") or "")
        verdict_counts[verdict] = verdict_counts.get(verdict, 0) + 1
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
    return {
        "rows": len(rows),
        "shadow_review_ready": int(verdict_counts.get("SHADOW_REVIEW_READY", 0)),
        "verdict_counts": verdict_counts,
        "bucket_counts": bucket_counts,
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "shadow_verdict",
        "shadow_reason",
        "shadow_trading_allowed",
        "source",
        "review_bucket",
        "code",
        "name",
        "candidate_origin",
        "candidate_origin_hybrid",
        "final_score",
        "news_score",
        "news_article_count",
        "sector_entry_allowed",
        "execution_pool",
        "return_pct_since_first_seen",
        "current_change_pct",
        "current_trading_value",
        "reason",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    rows = _merge_rows()
    payload = {
        "generated_at": _now_kst(),
        "schema_version": "news_recheck_shadow_promotion_v1",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "thresholds": {
            "min_shadow_score": MIN_SHADOW_SCORE,
            "min_news_articles": MIN_NEWS_ARTICLES,
            "min_news_score": MIN_NEWS_SCORE,
            "min_move_review_return_pct": MIN_MOVE_REVIEW_RETURN_PCT,
        },
        "summary": _summary(rows),
        "artifacts": {
            "candidates": str(CANDIDATES_CSV),
            "action_queue": str(ACTION_QUEUE_JSON),
            "action_review": str(ACTION_REVIEW_JSON),
            "entry_decision": str(ENTRY_DECISION_JSON),
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
        },
        "rows": rows,
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"status": "OK", **payload["summary"], "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
