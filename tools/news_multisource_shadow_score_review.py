from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"

INPUT_CSV = LOGS / "news_source_diversification_promotion_review_latest.csv"
OUT_CSV = LOGS / "news_multisource_shadow_score_review_latest.csv"
OUT_JSON = LOGS / "news_multisource_shadow_score_review_latest.json"

KST = timezone(timedelta(hours=9))


def _now_kst() -> datetime:
    return datetime.now(KST)


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc, dtype={"code": str})
        except Exception:
            continue
    return pd.read_csv(path, dtype={"code": str})


def _num(value: object, default: float = 0.0) -> float:
    try:
        parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.isna(parsed):
            return float(default)
        return float(parsed)
    except Exception:
        return float(default)


def _bool(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _shadow_score(row: pd.Series) -> tuple[float, str]:
    if str(row.get("promotion_bucket") or "") != "PROMOTION_CANDIDATE_OBSERVE_ONLY":
        return 0.0, "not_promotion_candidate"

    google_count = _num(row.get("google_rss_article_count"), 0.0)
    kis_count = _num(row.get("kis_title_count"), 0.0)
    naver_count = _num(row.get("naver_article_count"), 0.0)
    quality_bucket = str(row.get("combined_quality_bucket") or "")

    if quality_bucket != "direct_fresh_observe":
        return 0.0, f"quality_not_direct_fresh:{quality_bucket or 'blank'}"

    google_component = min(google_count, 3.0) * 0.04
    kis_component = min(kis_count, 3.0) * 0.04
    both_bonus = 0.04 if google_count > 0 and kis_count > 0 else 0.0
    naver_corroboration = 0.02 if naver_count > 0 else 0.0
    raw = google_component + kis_component + both_bonus + naver_corroboration
    score = round(min(raw, 0.20), 6)
    reason = (
        f"google={google_count:g}*0.04_cap3;"
        f"kis={kis_count:g}*0.04_cap3;"
        f"both_bonus={both_bonus:.2f};"
        f"naver_corroboration={naver_corroboration:.2f};"
        f"cap=0.20"
    )
    return score, reason


def main() -> int:
    status = {
        "generated_at": _now_kst().isoformat(timespec="seconds"),
        "mode": "read_only_multisource_shadow_score_review",
        "input": str(INPUT_CSV),
        "output_csv": str(OUT_CSV),
        "db_write": False,
        "score_effect": False,
        "trading_effect": False,
        "quality": "FAIL",
        "reason": "",
    }

    if not INPUT_CSV.exists():
        status["reason"] = "input_missing"
        OUT_JSON.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return 1

    df = _read_csv(INPUT_CSV)
    if df.empty or "code" not in df.columns:
        status["reason"] = "input_empty_or_missing_code"
        OUT_JSON.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return 1

    out = df.copy()
    out["code"] = out["code"].astype(str).str.replace(r"[^0-9]", "", regex=True).str.zfill(6)
    scores: list[float] = []
    reasons: list[str] = []
    for _, row in out.iterrows():
        score, reason = _shadow_score(row)
        scores.append(score)
        reasons.append(reason)
    out["shadow_news_score"] = scores
    out["shadow_score_reason"] = reasons
    out["shadow_action"] = out["shadow_news_score"].map(lambda x: "SHADOW_SCORE_CANDIDATE" if float(x) > 0 else "NO_SHADOW_SCORE")
    out["score_effect"] = False
    out["trading_effect"] = False

    promotion = out[out["shadow_action"] == "SHADOW_SCORE_CANDIDATE"].copy()
    review = out[out["promotion_bucket"].astype(str) == "OBSERVE_REVIEW"].copy() if "promotion_bucket" in out.columns else pd.DataFrame()
    out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    status.update(
        {
            "quality": "PASS",
            "reason": "ok",
            "rows": int(len(out)),
            "shadow_score_rows": int(len(promotion)),
            "shadow_score_max": float(promotion["shadow_news_score"].max()) if len(promotion) else 0.0,
            "shadow_score_sum": float(promotion["shadow_news_score"].sum()) if len(promotion) else 0.0,
            "observe_review_rows": int(len(review)),
            "shadow_candidates": promotion[
                [
                    "code",
                    "name",
                    "shadow_news_score",
                    "observed_sources",
                    "promotion_bucket",
                    "shadow_score_reason",
                ]
            ].to_dict(orient="records"),
            "score_effect": False,
            "trading_effect": False,
        }
    )
    OUT_JSON.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"[NEWS_MULTI_SHADOW] quality=PASS rows={len(out)} shadow_rows={len(promotion)} latest={OUT_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
