from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"
NEWS_SCORE_CSV = LOGS / "candidates_latest_data.with_news_score.csv"
COVERAGE_CSV = LOGS / "google_news_rss_coverage_report_latest.csv"
OUT_CSV = LOGS / "news_source_score_impact_report_latest.csv"
OUT_JSON = LOGS / "news_source_score_impact_report_latest.json"
KST = timezone(timedelta(hours=9))


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


def _num(v: object, default: float = 0.0) -> float:
    try:
        x = pd.to_numeric(pd.Series([v]), errors="coerce").iloc[0]
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default


def _bool(v: object) -> bool:
    return str(v or "").strip().lower() in {"1", "true", "y", "yes"}


def _observed_sources(row: pd.Series) -> str:
    sources = []
    if _num(row.get("naver_article_count"), 0.0) > 0:
        sources.append("naver")
    if _num(row.get("google_rss_article_count"), 0.0) > 0:
        sources.append("google_rss")
    if _num(row.get("kis_title_count"), 0.0) > 0:
        sources.append("kis_title")
    return "|".join(sources)


def _load_inputs() -> tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], str]:
    if not NEWS_SCORE_CSV.exists():
        return None, None, "news_score_csv_missing"
    if not COVERAGE_CSV.exists():
        return None, None, "coverage_csv_missing"
    score_df = _read_csv(NEWS_SCORE_CSV)
    cov_df = _read_csv(COVERAGE_CSV)
    if "code" not in score_df.columns:
        return None, None, "news_score_no_code"
    if "code" not in cov_df.columns:
        return None, None, "coverage_no_code"
    return score_df, cov_df, "ok"


def main() -> int:
    now = _now_kst()
    status: Dict[str, object] = {
        "generated_at": now.isoformat(timespec="seconds"),
        "mode": "read_only_score_impact",
        "db_write": False,
        "news_score_csv": str(NEWS_SCORE_CSV),
        "coverage_csv": str(COVERAGE_CSV),
        "output_csv": str(OUT_CSV),
        "quality": "FAIL",
        "reason": "",
    }

    score_df, cov_df, reason = _load_inputs()
    if reason != "ok" or score_df is None or cov_df is None:
        status["reason"] = reason
    else:
        score = score_df.copy()
        cov = cov_df.copy()
        score["code"] = score["code"].map(_norm_code6)
        cov["code"] = cov["code"].map(_norm_code6)
        score = score[score["code"] != ""].drop_duplicates("code", keep="first")
        cov = cov[cov["code"] != ""].drop_duplicates("code", keep="first")

        keep_cov = [
            "code",
            "naver_article_count",
            "google_rss_article_count",
            "kis_title_count",
            "naver_covered",
            "google_rss_covered",
            "kis_title_covered",
            "any_covered",
        ]
        cov = cov[[c for c in keep_cov if c in cov.columns]]
        merged = score.merge(cov, on="code", how="left", suffixes=("", "_coverage"))

        if "name" not in merged.columns:
            merged["name"] = ""
        for col in ("news_score", "news_article_count", "news_entity_count", "news_implication_rows"):
            if col not in merged.columns:
                merged[col] = 0
            merged[col] = merged[col].map(_num)
        for col in ("naver_article_count", "google_rss_article_count", "kis_title_count"):
            if col not in merged.columns:
                merged[col] = 0
            merged[col] = merged[col].map(_num).astype(int)
        for col in ("naver_covered", "google_rss_covered", "kis_title_covered", "any_covered"):
            if col not in merged.columns:
                merged[col] = False
            merged[col] = merged[col].map(_bool)

        merged["current_news_nonzero"] = merged["news_score"].abs() > 1e-9
        merged["current_article_covered"] = merged["news_article_count"] > 0
        merged["google_fills_current_article_gap"] = merged["google_rss_covered"] & (~merged["current_article_covered"])
        merged["kis_fills_current_article_gap"] = merged["kis_title_covered"] & (~merged["current_article_covered"])
        merged["new_source_fills_current_article_gap"] = (
            merged["google_fills_current_article_gap"] | merged["kis_fills_current_article_gap"]
        )
        merged["new_source_count"] = merged["google_rss_article_count"] + merged["kis_title_count"]
        merged["observe_only"] = True
        merged["trading_effect"] = False
        merged["score_effect"] = False
        merged["observed_sources"] = merged.apply(_observed_sources, axis=1)
        merged["impact_bucket"] = "already_scored"
        merged.loc[~merged["current_news_nonzero"] & merged["new_source_fills_current_article_gap"], "impact_bucket"] = (
            "covered_by_new_source_but_score_zero"
        )
        merged.loc[merged["current_news_nonzero"] & merged["new_source_count"].gt(0), "impact_bucket"] = (
            "additional_context_for_existing_score"
        )
        merged.loc[~merged["any_covered"], "impact_bucket"] = "still_uncovered"

        out_cols = [
            "code",
            "name",
            "news_score",
            "news_source",
            "news_article_count",
            "news_entity_count",
            "news_implication_rows",
            "naver_article_count",
            "google_rss_article_count",
            "kis_title_count",
            "observed_sources",
            "observe_only",
            "trading_effect",
            "score_effect",
            "current_news_nonzero",
            "google_fills_current_article_gap",
            "kis_fills_current_article_gap",
            "new_source_fills_current_article_gap",
            "impact_bucket",
        ]
        out = merged[[c for c in out_cols if c in merged.columns]].copy()
        LOGS.mkdir(parents=True, exist_ok=True)
        out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

        rows = int(len(out))
        denom = max(1, rows)
        status.update(
            {
                "quality": "PASS",
                "reason": "ok",
                "observe_only": True,
                "trading_effect": False,
                "score_effect": False,
                "rows": rows,
                "current_news_nonzero_rows": int(merged["current_news_nonzero"].sum()),
                "current_article_covered_rows": int(merged["current_article_covered"].sum()),
                "google_gap_fill_rows": int(merged["google_fills_current_article_gap"].sum()),
                "kis_gap_fill_rows": int(merged["kis_fills_current_article_gap"].sum()),
                "new_source_gap_fill_rows": int(merged["new_source_fills_current_article_gap"].sum()),
                "new_source_gap_fill_rate": round(
                    float(merged["new_source_fills_current_article_gap"].sum()) / float(denom), 6
                ),
                "impact_buckets": {
                    str(k): int(v) for k, v in merged["impact_bucket"].value_counts(dropna=False).to_dict().items()
                },
            }
        )

    OUT_JSON.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        f"[NEWS_SOURCE_SCORE_IMPACT] quality={status['quality']} reason={status['reason']} "
        f"rows={status.get('rows')} latest={OUT_JSON}"
    )
    return 0 if status["quality"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
