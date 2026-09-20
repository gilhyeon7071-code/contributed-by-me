from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
INPUT_CANDIDATES = LOG_DIR / "candidates_latest_data.with_final_score.csv"
INPUT_NEWS_STATUS = LOG_DIR / "news_score_status_latest.json"
INPUT_MATCH_QUALITY = LOG_DIR / "news_source_match_quality_report_latest.json"
INPUT_SHADOW = LOG_DIR / "news_signal_shadow_stage_latest.csv"
OUT_JSON = LOG_DIR / "news_quality_audit_latest.json"
OUT_CSV = LOG_DIR / "news_quality_audit_latest.csv"


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _num_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(df[col], errors="coerce")


def _value_counts(df: pd.DataFrame, col: str) -> Dict[str, int]:
    if col not in df.columns:
        return {}
    return {str(k): int(v) for k, v in df[col].fillna("").astype(str).value_counts().to_dict().items()}


def _rows_for_review(df: pd.DataFrame, shadow: pd.DataFrame) -> List[Dict[str, Any]]:
    if df.empty:
        return []
    work = df.copy()
    if not shadow.empty and "code" in shadow.columns:
        cols = [c for c in ["code", "news_signal_stage", "news_signal_direction", "news_text_strength", "news_market_corroboration_score"] if c in shadow.columns]
        work = work.merge(shadow[cols], on="code", how="left", suffixes=("", "_shadow"))
    for col in ["news_score", "news_freshest_age_hours", "news_article_count", "news_implication_risk_score"]:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")
    review = work[
        (work.get("news_score", pd.Series(0, index=work.index)).fillna(0) != 0)
        | (work.get("news_signal_stage", pd.Series("", index=work.index)).fillna("").astype(str).isin(["CONFIRMED_SHADOW", "PRE_SIGNAL", "BLOCK_SHADOW"]))
    ].copy()
    if review.empty:
        return []
    keep = [
        "code",
        "name",
        "news_score",
        "news_sentiment",
        "news_article_count",
        "news_freshest_age_hours",
        "news_source_scope",
        "news_implication_top_actions",
        "news_implication_risk_score",
        "news_signal_stage",
        "news_signal_direction",
        "news_text_strength",
        "news_market_corroboration_score",
    ]
    keep = [c for c in keep if c in review.columns]
    review = review[keep].sort_values([c for c in ["news_signal_stage", "news_score", "news_freshest_age_hours"] if c in keep], ascending=[True, False, True][: len([c for c in ["news_signal_stage", "news_score", "news_freshest_age_hours"] if c in keep])])
    return review.fillna("").to_dict(orient="records")


def main() -> int:
    candidates = pd.read_csv(INPUT_CANDIDATES, dtype=str, encoding="utf-8-sig") if INPUT_CANDIDATES.exists() else pd.DataFrame()
    shadow = pd.read_csv(INPUT_SHADOW, dtype=str, encoding="utf-8-sig") if INPUT_SHADOW.exists() else pd.DataFrame()
    news_status = _read_json(INPUT_NEWS_STATUS)
    match_quality = _read_json(INPUT_MATCH_QUALITY)

    rows = int(len(candidates))
    news_score = _num_series(candidates, "news_score")
    article_count = _num_series(candidates, "news_article_count")
    freshest_hours = _num_series(candidates, "news_freshest_age_hours")
    risk_score = _num_series(candidates, "news_implication_risk_score")

    mapped_rate = news_status.get("mapped_rate")
    nonzero_rate = news_status.get("nonzero_rate")
    direct_fresh_rows = int(match_quality.get("combined_direct_fresh_rows") or 0)
    quality_observe_rows = int(match_quality.get("quality_observe_candidate_rows") or 0)
    stale_or_review_rows = int((match_quality.get("combined_quality_buckets") or {}).get("direct_stale_or_review") or 0)
    uncovered_rows = int((match_quality.get("combined_quality_buckets") or {}).get("uncovered") or 0)

    issues: List[str] = []
    if news_status.get("quality") != "PASS":
        issues.append("NEWS_SCORE_QUALITY_NOT_PASS")
    if mapped_rate is not None and float(mapped_rate) < 0.5:
        issues.append("MAPPED_RATE_LOW")
    if nonzero_rate is not None and float(nonzero_rate) < 0.1:
        issues.append("NONZERO_RATE_LOW")
    if rows and direct_fresh_rows == 0:
        issues.append("NO_DIRECT_FRESH_NEWS_MATCH")
    if stale_or_review_rows > direct_fresh_rows:
        issues.append("STALE_OR_REVIEW_GT_DIRECT_FRESH")
    if uncovered_rows > max(5, rows // 2):
        issues.append("UNCOVERED_ROWS_HIGH")
    if len(news_score):
        scored = news_score.fillna(0) != 0
        if len(article_count) and int((scored & (article_count.fillna(0) <= 0)).sum()) > 0:
            issues.append("SCORED_ROWS_WITHOUT_ARTICLE_COUNT_REVIEW")
        if len(freshest_hours) and int((scored & (freshest_hours.fillna(-1) < 0)).sum()) > 0:
            issues.append("SCORED_ROWS_WITHOUT_FRESHNESS_REVIEW")

    score_rows = []
    if rows:
        for _, row in candidates.iterrows():
            score_rows.append(
                {
                    "code": str(row.get("code", "")),
                    "name": str(row.get("name", "")),
                    "news_score": float(pd.to_numeric(row.get("news_score", 0), errors="coerce") or 0),
                    "news_sentiment": str(row.get("news_sentiment", "")),
                    "news_article_count": float(pd.to_numeric(row.get("news_article_count", 0), errors="coerce") or 0),
                    "news_freshest_age_hours": float(pd.to_numeric(row.get("news_freshest_age_hours", 0), errors="coerce") or 0),
                    "news_source_scope": str(row.get("news_source_scope", "")),
                    "news_implication_top_actions": str(row.get("news_implication_top_actions", "")),
                }
            )

    review_rows = _rows_for_review(candidates, shadow)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "version": "news_quality_audit_v1",
        "status": "PASS" if not issues else "WARN",
        "quality": "PASS" if not issues else "WARN",
        "reason": "ok" if not issues else ";".join(issues),
        "rows": rows,
        "input_candidates": str(INPUT_CANDIDATES),
        "input_news_status": str(INPUT_NEWS_STATUS),
        "input_match_quality": str(INPUT_MATCH_QUALITY),
        "input_shadow": str(INPUT_SHADOW),
        "mapped_rate": mapped_rate,
        "nonzero_rate": nonzero_rate,
        "news_score_nonzero_rows": int((news_score.fillna(0) != 0).sum()) if len(news_score) else 0,
        "news_score_negative_rows": int((news_score.fillna(0) < 0).sum()) if len(news_score) else 0,
        "news_score_positive_rows": int((news_score.fillna(0) > 0).sum()) if len(news_score) else 0,
        "scored_without_article_count_rows": int(((news_score.fillna(0) != 0) & (article_count.fillna(0) <= 0)).sum()) if len(news_score) and len(article_count) else 0,
        "scored_without_freshness_rows": int(((news_score.fillna(0) != 0) & (freshest_hours.fillna(-1) < 0)).sum()) if len(news_score) and len(freshest_hours) else 0,
        "news_article_count_sum": float(article_count.fillna(0).sum()) if len(article_count) else 0.0,
        "freshest_age_hours_min": None if freshest_hours.dropna().empty else float(freshest_hours.min()),
        "freshest_age_hours_max": None if freshest_hours.dropna().empty else float(freshest_hours.max()),
        "risk_score_max": None if risk_score.dropna().empty else float(risk_score.max()),
        "direct_fresh_rows": direct_fresh_rows,
        "quality_observe_candidate_rows": quality_observe_rows,
        "direct_stale_or_review_rows": stale_or_review_rows,
        "uncovered_rows": uncovered_rows,
        "shadow_stage_counts": _value_counts(shadow, "news_signal_stage"),
        "issues": issues,
        "review_rows": review_rows,
        "score_effect": False,
        "trading_effect": False,
        "policy_effect": False,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(score_rows).to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(json.dumps({"status": payload["status"], "rows": rows, "issues": issues}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
