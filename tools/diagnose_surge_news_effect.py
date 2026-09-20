from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"
IN_CSV = LOGS / "surge_realtime_latest.csv"
IN_JSON = LOGS / "surge_realtime_latest.json"
OUT_JSON = LOGS / "surge_news_effect_latest.json"
OUT_CSV = LOGS / "surge_news_effect_rows_latest.csv"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _num(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if math.isfinite(out):
            return out
    except Exception:
        pass
    return default


def _bool_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.upper().isin({"1", "TRUE", "Y", "YES"})


def _without_news_reasons(reason: str) -> list[str]:
    parts = [p for p in str(reason or "").split("|") if p]
    return [p for p in parts if not p.startswith("NEWS_NEGATIVE:")]


def main() -> int:
    ts = datetime.now().isoformat(timespec="seconds")
    if not IN_CSV.exists():
        OUT_JSON.write_text(
            json.dumps({"ts": ts, "status": "MISSING_INPUT", "input": str(IN_CSV)}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return 1

    df = pd.read_csv(IN_CSV)
    meta = _read_json(IN_JSON)
    thresholds = meta.get("thresholds") if isinstance(meta.get("thresholds"), dict) else {}
    w_rule = _num(thresholds.get("weight_rule"), 0.65)
    w_news = _num(thresholds.get("weight_news"), 0.08)
    w_ml = _num(thresholds.get("weight_ml"), 0.27)
    total_w = w_rule + w_news + w_ml
    if total_w <= 0:
        w_rule_n, w_news_n, w_ml_n = 1.0, 0.0, 0.0
    else:
        w_rule_n, w_news_n, w_ml_n = w_rule / total_w, w_news / total_w, w_ml / total_w

    news_block_threshold = _num(thresholds.get("news_block_threshold"), -0.3)
    news_bonus_threshold = _num(thresholds.get("news_bonus_threshold"), 0.3)
    news_bonus_pts = max(0.0, _num(thresholds.get("news_bonus_pts"), 5.0))

    for col in ("surge_score", "news_score", "surge_ml_prob", "surge_score_final"):
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    if "exclude_reasons" not in df.columns:
        df["exclude_reasons"] = ""
    if "excluded_by_policy" not in df.columns:
        df["excluded_by_policy"] = False

    rule_s = df["surge_score"].clip(0.0, 100.0)
    ml_avail = df["surge_ml_prob"].between(0.0, 1.0) & (df["surge_ml_prob"].abs() > 1e-9)
    ml_norm = (df["surge_ml_prob"].clip(0.0, 1.0) * 100.0).fillna(0.0)

    news_raw = df["news_score"].clip(-1.0, 1.0)
    news_avail = news_raw.abs() > 1e-6
    news_norm = (news_raw + 1.0) * 50.0
    eff_w_rule = w_rule_n + w_news_n * (~news_avail).astype(float) + w_ml_n * (~ml_avail).astype(float)
    eff_w_news = w_news_n * news_avail.astype(float)
    eff_w_ml = w_ml_n * ml_avail.astype(float)
    news_bonus = (news_raw > news_bonus_threshold).astype(float) * news_bonus_pts
    final_with_news_calc = (rule_s * eff_w_rule + news_norm * eff_w_news + ml_norm * eff_w_ml + news_bonus).clip(0.0, 100.0)

    no_news_avail = pd.Series(False, index=df.index)
    no_news_norm = pd.Series(50.0, index=df.index)
    no_news_eff_w_rule = w_rule_n + w_news_n * (~no_news_avail).astype(float) + w_ml_n * (~ml_avail).astype(float)
    no_news_eff_w_news = w_news_n * no_news_avail.astype(float)
    no_news_eff_w_ml = w_ml_n * ml_avail.astype(float)
    final_without_news = (rule_s * no_news_eff_w_rule + no_news_norm * no_news_eff_w_news + ml_norm * no_news_eff_w_ml).clip(0.0, 100.0)

    out = df.copy()
    out["surge_score_final_with_news_calc"] = final_with_news_calc.round(6)
    out["surge_score_final_without_news"] = final_without_news.round(6)
    out["news_score_delta_pts"] = (final_with_news_calc - final_without_news).round(6)
    out["news_bonus_applied"] = news_bonus > 0
    out["news_negative_block"] = news_raw < news_block_threshold
    out["exclude_reasons_without_news"] = out["exclude_reasons"].map(lambda x: "|".join(_without_news_reasons(str(x))))
    out["excluded_without_news_negative"] = out["exclude_reasons_without_news"].astype(str).str.len() > 0
    out["blocked_only_by_news"] = out["news_negative_block"] & _bool_series(out["excluded_by_policy"]) & (~out["excluded_without_news_negative"])

    nonzero = out[out["news_score"].abs() > 1e-9].copy()
    top_positive = (
        out.sort_values("news_score_delta_pts", ascending=False)
        [["code", "news_score", "surge_score", "surge_score_final", "surge_score_final_without_news", "news_score_delta_pts", "excluded_by_policy", "exclude_reasons"]]
        .head(10)
        .to_dict(orient="records")
    )
    top_negative = (
        out.sort_values("news_score_delta_pts", ascending=True)
        [["code", "news_score", "surge_score", "surge_score_final", "surge_score_final_without_news", "news_score_delta_pts", "excluded_by_policy", "exclude_reasons"]]
        .head(10)
        .to_dict(orient="records")
    )
    eligible = out[~_bool_series(out["excluded_by_policy"])].copy()
    alerts = out[_bool_series(out.get("surge_flag", pd.Series(False, index=out.index)))].copy()

    summary = {
        "ts": ts,
        "status": "OK",
        "input_csv": str(IN_CSV),
        "input_json": str(IN_JSON),
        "output_csv": str(OUT_CSV),
        "rows": int(len(out)),
        "news_covered_rows": int(meta.get("news_score_covered_rows") or int((out["news_score"].notna()).sum())),
        "news_nonzero_rows": int(len(nonzero)),
        "positive_news_rows": int((out["news_score"] > 0).sum()),
        "negative_news_rows": int((out["news_score"] < 0).sum()),
        "news_bonus_rows": int(out["news_bonus_applied"].sum()),
        "news_negative_block_rows": int(out["news_negative_block"].sum()),
        "blocked_only_by_news_rows": int(out["blocked_only_by_news"].sum()),
        "eligible_rows": int(len(eligible)),
        "eligible_news_nonzero_rows": int((eligible["news_score"].abs() > 1e-9).sum()) if len(eligible) else 0,
        "alerts_rows": int(len(alerts)),
        "alerts_news_nonzero_rows": int((alerts["news_score"].abs() > 1e-9).sum()) if len(alerts) else 0,
        "avg_abs_delta_pts_all": round(float(out["news_score_delta_pts"].abs().mean()), 6) if len(out) else 0.0,
        "avg_abs_delta_pts_nonzero": round(float(nonzero["news_score_delta_pts"].abs().mean()), 6) if len(nonzero) else 0.0,
        "max_positive_delta_pts": round(float(out["news_score_delta_pts"].max()), 6) if len(out) else 0.0,
        "max_negative_delta_pts": round(float(out["news_score_delta_pts"].min()), 6) if len(out) else 0.0,
        "thresholds": {
            "weight_rule": w_rule,
            "weight_news": w_news,
            "weight_ml": w_ml,
            "news_block_threshold": news_block_threshold,
            "news_bonus_threshold": news_bonus_threshold,
            "news_bonus_pts": news_bonus_pts,
        },
        "top_positive_delta": top_positive,
        "top_negative_delta": top_negative,
        "interpretation": {
            "current_stage": "effect_measurement",
            "tuning_ready": bool(len(nonzero) >= 20 and int(out["news_negative_block"].sum()) >= 3),
            "reason": "Need more historical/nonzero news samples before threshold tuning" if not (len(nonzero) >= 20 and int(out["news_negative_block"].sum()) >= 3) else "Enough current samples for threshold grid review",
        },
    }

    keep_cols = [
        "code",
        "news_score",
        "surge_score",
        "surge_ml_prob",
        "surge_score_final",
        "surge_score_final_without_news",
        "news_score_delta_pts",
        "news_bonus_applied",
        "news_negative_block",
        "blocked_only_by_news",
        "excluded_by_policy",
        "exclude_reasons",
        "exclude_reasons_without_news",
    ]
    for col in keep_cols:
        if col not in out.columns:
            out[col] = ""
    out[keep_cols].to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        "[SURGE_NEWS_EFFECT] "
        f"rows={summary['rows']} nonzero={summary['news_nonzero_rows']} "
        f"bonus={summary['news_bonus_rows']} neg_block={summary['news_negative_block_rows']} "
        f"avg_abs_delta_nonzero={summary['avg_abs_delta_pts_nonzero']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
