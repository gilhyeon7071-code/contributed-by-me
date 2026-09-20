"""Evaluate draft promotion rules against the news/recheck shadow artifact.

This is a read-only policy-review helper. It does not approve trading, does not
write candidate inputs, and does not change paper-engine configuration.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

IN_JSON = LOG_DIR / "news_recheck_shadow_promotion_latest.json"
OUT_JSON = LOG_DIR / "news_recheck_shadow_policy_eval_latest.json"
OUT_CSV = LOG_DIR / "news_recheck_shadow_policy_eval_latest.csv"


POLICIES: Dict[str, Dict[str, Any]] = {
    "NEWS_ONLY_STRICT_REVIEW": {
        "scope": "NEWS_ONLY",
        "min_final_score": 0.05,
        "min_news_score": 0.10,
        "min_news_articles": 3,
        "min_return_pct": 0.0,
        "min_current_trading_value": 0.0,
        "max_rows_per_code": 1,
        "policy_stage": "shadow_review_only",
    },
    "MISSED_MOVE_STRICT_REVIEW": {
        "scope": "MISSED_MOVE",
        "min_final_score": 0.0,
        "min_news_score": 0.0,
        "min_news_articles": 0,
        "min_return_pct": 5.0,
        "min_current_trading_value": 10_000_000_000.0,
        "max_rows_per_code": 1,
        "policy_stage": "shadow_review_only",
    },
    "MISSED_MOVE_BROAD_REVIEW": {
        "scope": "MISSED_MOVE",
        "min_final_score": 0.0,
        "min_news_score": 0.0,
        "min_news_articles": 0,
        "min_return_pct": 3.0,
        "min_current_trading_value": 5_000_000_000.0,
        "max_rows_per_code": 1,
        "policy_stage": "shadow_review_only",
    },
}


def _now_kst() -> str:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).isoformat(timespec="seconds")


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


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


def _scope_match(row: Dict[str, Any], scope: str) -> bool:
    bucket = str(row.get("review_bucket") or "").strip().upper()
    hybrid = str(row.get("candidate_origin_hybrid") or "").strip().upper()
    reason = str(row.get("shadow_reason") or "").strip().upper()
    if scope == "NEWS_ONLY":
        return hybrid == "NEWS_ONLY" or bucket == "NEWS_ONLY" or reason == "NEWS_ONLY"
    if scope == "MISSED_MOVE":
        return bucket in {"RECHECK_MISSED_MOVE_CANDIDATE", "WATCH_MISSED_MOVE_CANDIDATE", "BLOCKED_MOVED_REVIEW"}
    return False


def _eligible(row: Dict[str, Any], policy: Dict[str, Any]) -> tuple[bool, str]:
    if str(row.get("shadow_verdict") or "") != "SHADOW_REVIEW_READY":
        return False, "shadow_not_ready"
    if str(row.get("shadow_trading_allowed") or "").lower() == "true":
        return False, "unexpected_trading_allowed"
    if not _scope_match(row, str(policy.get("scope") or "")):
        return False, "scope_mismatch"

    checks = [
        ("final_score", _f(row.get("final_score")), _f(policy.get("min_final_score"))),
        ("news_score", _f(row.get("news_score")), _f(policy.get("min_news_score"))),
        ("news_article_count", _f(row.get("news_article_count")), _f(policy.get("min_news_articles"))),
        ("return_pct_since_first_seen", _f(row.get("return_pct_since_first_seen")), _f(policy.get("min_return_pct"))),
        ("current_trading_value", _f(row.get("current_trading_value")), _f(policy.get("min_current_trading_value"))),
    ]
    failed = [f"{name}<{threshold:g}" for name, value, threshold in checks if value < threshold]
    if failed:
        return False, ";".join(failed)
    return True, "eligible_for_policy_review"


def _rows_for_policy(rows: Iterable[Dict[str, Any]], policy_name: str, policy: Dict[str, Any]) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    seen_by_code: Dict[str, int] = {}
    max_rows_per_code = max(1, int(_f(policy.get("max_rows_per_code"), 1)))
    for row in rows:
        code = str(row.get("code") or "").strip()
        ok, reason = _eligible(row, policy)
        if not ok:
            continue
        if seen_by_code.get(code, 0) >= max_rows_per_code:
            continue
        seen_by_code[code] = seen_by_code.get(code, 0) + 1
        selected.append(
            {
                "policy_name": policy_name,
                "policy_stage": str(policy.get("policy_stage") or "shadow_review_only"),
                "policy_review_allowed": True,
                "trading_allowed": False,
                "policy_effect": False,
                "code": code,
                "name": str(row.get("name") or ""),
                "source": str(row.get("source") or ""),
                "review_bucket": str(row.get("review_bucket") or ""),
                "candidate_origin_hybrid": str(row.get("candidate_origin_hybrid") or ""),
                "final_score": _f(row.get("final_score")),
                "news_score": _f(row.get("news_score")),
                "news_article_count": int(_f(row.get("news_article_count"))),
                "return_pct_since_first_seen": _f(row.get("return_pct_since_first_seen")),
                "current_change_pct": _f(row.get("current_change_pct")),
                "current_trading_value": _f(row.get("current_trading_value")),
                "eligibility_reason": reason,
            }
        )
    selected.sort(
        key=lambda r: (
            _f(r.get("return_pct_since_first_seen")),
            _f(r.get("current_trading_value")),
            _f(r.get("final_score")),
        ),
        reverse=True,
    )
    return selected


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "policy_name",
        "policy_stage",
        "policy_review_allowed",
        "trading_allowed",
        "policy_effect",
        "code",
        "name",
        "source",
        "review_bucket",
        "candidate_origin_hybrid",
        "final_score",
        "news_score",
        "news_article_count",
        "return_pct_since_first_seen",
        "current_change_pct",
        "current_trading_value",
        "eligibility_reason",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _count(rows: Iterable[Dict[str, Any]], key: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for row in rows:
        val = str(row.get(key) or "")
        out[val] = out.get(val, 0) + 1
    return out


def main() -> int:
    src = _read_json(IN_JSON)
    shadow_rows = src.get("rows") if isinstance(src.get("rows"), list) else []
    eval_rows: List[Dict[str, Any]] = []
    policy_summaries: Dict[str, Any] = {}

    for name, policy in POLICIES.items():
        selected = _rows_for_policy(shadow_rows, name, policy)
        eval_rows.extend(selected)
        policy_summaries[name] = {
            "selected_rows": len(selected),
            "selected_codes": sorted({str(r.get("code") or "") for r in selected if str(r.get("code") or "")}),
            "policy_stage": policy.get("policy_stage"),
            "trading_allowed": False,
            "policy_effect": False,
            "thresholds": policy,
        }

    payload = {
        "generated_at": _now_kst(),
        "schema_version": "news_recheck_shadow_policy_eval_v1",
        "generated_from": str(IN_JSON),
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "summary": {
            "source_rows": len(shadow_rows),
            "eval_rows": len(eval_rows),
            "policy_counts": _count(eval_rows, "policy_name"),
            "unique_codes": sorted({str(r.get("code") or "") for r in eval_rows if str(r.get("code") or "")}),
        },
        "policies": policy_summaries,
        "artifacts": {
            "input": str(IN_JSON),
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
        },
        "rows": eval_rows,
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, eval_rows)
    print(json.dumps({"status": "OK", **payload["summary"], "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
