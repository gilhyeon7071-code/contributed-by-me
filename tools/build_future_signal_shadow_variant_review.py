from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


ROOT = Path("E:/1_Data")
LOGS = ROOT / "2_Logs"
DEFAULT_CROSS_D = LOGS / "future_signal_cross_d_validation_latest.csv"
LATEST_JSON = LOGS / "future_signal_shadow_variant_review_latest.json"
LATEST_CSV = LOGS / "future_signal_shadow_variant_review_latest.csv"
DETAIL_CSV = LOGS / "future_signal_shadow_variant_review_details_latest.csv"
COMPONENT_CSV = LOGS / "future_signal_shadow_variant_review_components_latest.csv"
UNMATCHED_CSV = LOGS / "future_signal_shadow_variant_review_unmatched_latest.csv"

CORE_INVERT_COMPONENTS = {"forecast_score", "score", "rs", "v_accel"}
HORIZON_WEIGHTS: Dict[str, Dict[str, float]] = {
    "SHORT": {
        "final_score": 0.20,
        "forecast_score": 0.12,
        "score": 0.12,
        "sector_score": 0.08,
        "flow_score": 0.10,
        "fundamental_score_01": 0.05,
        "execution_lob_score": 0.12,
        "news_score": 0.09,
        "rs": 0.05,
        "v_accel": 0.07,
    },
    "SWING": {
        "final_score": 0.22,
        "forecast_score": 0.16,
        "score": 0.12,
        "sector_score": 0.10,
        "flow_score": 0.10,
        "fundamental_score_01": 0.08,
        "execution_lob_score": 0.07,
        "news_score": 0.05,
        "rs": 0.05,
        "v_accel": 0.05,
    },
}


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc, dtype={"code": str, "D": str})
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype={"code": str, "D": str})


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _norm_date8(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text[:8] if len(text) >= 8 else ""


def _norm_code(value: Any) -> str:
    text = str(value or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(6) if text else ""


def _clip_series(series: pd.Series, lo: float, hi: float) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").clip(lower=lo, upper=hi)


def _feature_value(df: pd.DataFrame, component: str) -> pd.Series:
    if component not in df.columns:
        return pd.Series(float("nan"), index=df.index)
    if component == "rs":
        return _clip_series(df[component], 0.0, 2.0) / 2.0
    if component == "v_accel":
        return _clip_series(df[component], 0.0, 4.0) / 4.0
    return _clip_series(df[component], 0.0, 1.0)


def _safe_corr(df: pd.DataFrame, left: str, right: str) -> float | None:
    if left not in df.columns or right not in df.columns:
        return None
    work = pd.DataFrame(
        {
            "left": pd.to_numeric(df[left], errors="coerce"),
            "right": pd.to_numeric(df[right], errors="coerce"),
        }
    ).dropna()
    if len(work) < 3:
        return None
    val = work["left"].corr(work["right"], method="spearman")
    if pd.isna(val):
        return None
    return round(float(val), 6)


def _metrics(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {
            "rows": 0,
            "hit_rate": None,
            "mean_return": None,
            "median_return": None,
            "q10_return": None,
            "q90_return": None,
        }
    ret = pd.to_numeric(df["realized_return"], errors="coerce").dropna()
    if ret.empty:
        return {
            "rows": 0,
            "hit_rate": None,
            "mean_return": None,
            "median_return": None,
            "q10_return": None,
            "q90_return": None,
        }
    return {
        "rows": int(len(ret)),
        "hit_rate": round(float((ret > 0).mean()), 6),
        "mean_return": round(float(ret.mean()), 6),
        "median_return": round(float(ret.median()), 6),
        "q10_return": round(float(ret.quantile(0.10)), 6),
        "q90_return": round(float(ret.quantile(0.90)), 6),
    }


def _load_features() -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for path in sorted(LOGS.glob("future_signal_features_*.csv")):
        match = re.search(r"(\d{8})", path.name)
        if not match:
            continue
        try:
            df = _read_csv(path)
        except Exception:
            continue
        if "code" not in df.columns:
            continue
        df = df.copy()
        df["D"] = match.group(1)
        df["code"] = df["code"].map(_norm_code)
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["D", "code"])
    return pd.concat(frames, ignore_index=True)


def _load_code_keys(pattern: str) -> set[tuple[str, str]]:
    keys: set[tuple[str, str]] = set()
    for path in sorted(LOGS.glob(pattern)):
        match = re.search(r"(\d{8})", path.name)
        if not match:
            continue
        try:
            df = _read_csv(path)
        except Exception:
            continue
        if "code" not in df.columns:
            continue
        d = match.group(1)
        for code in df["code"].map(_norm_code).dropna().astype(str):
            if code:
                keys.add((d, code))
    return keys


def _shadow_pred(df: pd.DataFrame, horizon: str, mode: str) -> pd.Series:
    weights = HORIZON_WEIGHTS[horizon]
    base = pd.Series(0.0, index=df.index)
    for component, weight in weights.items():
        value = _feature_value(df, component)
        if mode == "drop_core" and component in CORE_INVERT_COMPONENTS:
            value = pd.Series(0.0, index=df.index)
        elif mode == "invert_core" and component in CORE_INVERT_COMPONENTS:
            value = 1.0 - value
        base = base + value.fillna(0.0) * float(weight)

    atr = pd.to_numeric(df.get("atr14_pct", 0.0), errors="coerce").fillna(0.0).clip(lower=0.0)
    rsi = pd.to_numeric(df.get("rsi14", 50.0), errors="coerce").fillna(50.0)
    ret1 = pd.to_numeric(df.get("ret1_pct", 0.0), errors="coerce").fillna(0.0)
    penalty = pd.Series(0.0, index=df.index)
    penalty += ((atr - 0.07) * 2.0).clip(lower=0.0, upper=0.12)
    penalty += ((rsi - 78.0) / 100.0).clip(lower=0.0, upper=0.08)
    penalty += ((ret1 - 18.0) / 100.0).clip(lower=0.0, upper=0.08)
    signal = (base - penalty).clip(lower=0.0, upper=1.0)
    return (0.35 + signal * 0.30).clip(lower=0.35, upper=0.65)


def _variant_grade(horizon: str, variant: str, top20: Dict[str, Any], spearman: float | None) -> str:
    rows = int(top20.get("rows") or 0)
    mean_return = top20.get("mean_return")
    hit_rate = top20.get("hit_rate")
    if rows < 20:
        return "INSUFFICIENT_SAMPLE"
    if mean_return is None or hit_rate is None or spearman is None:
        return "INSUFFICIENT_METRIC"
    if horizon in {"SHORT", "SWING"} and variant == "invert_core" and mean_return > 0 and hit_rate >= 0.50 and spearman > 0:
        return f"SHADOW_PROMISING_{horizon}_ONLY"
    if spearman > 0 and mean_return <= 0:
        return "RANKING_IMPROVES_RETURN_STILL_NEGATIVE"
    if spearman > 0:
        return "SHADOW_WEAK_IMPROVEMENT"
    return "DISCARD_OR_WAIT"


def build_review(cross_d_csv: Path) -> Dict[str, Any]:
    if not cross_d_csv.exists():
        payload = {"status": "FAIL", "reason": "CROSS_D_CSV_MISSING", "path": str(cross_d_csv)}
        _write_json(LATEST_JSON, payload)
        return payload

    cross = _read_csv(cross_d_csv)
    required = {"D", "code", "horizon_type", "pred_up_prob", "realized_return"}
    missing = sorted(required - set(cross.columns))
    if missing:
        payload = {"status": "FAIL", "reason": "REQUIRED_COLUMN_MISSING", "missing": missing}
        _write_json(LATEST_JSON, payload)
        return payload

    cross = cross.copy()
    cross["D"] = cross["D"].map(_norm_date8)
    cross["code"] = cross["code"].map(_norm_code)
    cross["pred_up_prob"] = pd.to_numeric(cross["pred_up_prob"], errors="coerce")
    cross["realized_return"] = pd.to_numeric(cross["realized_return"], errors="coerce")
    cross = cross.dropna(subset=["D", "code", "horizon_type", "pred_up_prob", "realized_return"]).copy()

    features = _load_features()
    preview_keys = _load_code_keys("future_signal_preview_*.csv")
    candidate_keys = _load_code_keys("candidates_v41_1_*.csv")
    work_all = cross.merge(features, on=["D", "code"], how="left", suffixes=("", "_feature"))
    work_all = work_all[work_all["horizon_type"].isin(HORIZON_WEIGHTS)].copy()
    feature_match_col = next((c for c in ("final_score", "score", "forecast_score") if c in work_all.columns), "")
    work_all["feature_matched"] = bool(False)
    if feature_match_col:
        work_all["feature_matched"] = pd.to_numeric(work_all[feature_match_col], errors="coerce").notna()
    feature_dates = set(features["D"].dropna().astype(str).tolist()) if "D" in features.columns else set()
    feature_keys = (
        set(zip(features["D"].astype(str), features["code"].astype(str)))
        if {"D", "code"}.issubset(features.columns)
        else set()
    )
    work_all["feature_match_reason"] = "MATCHED"
    unmatched_mask = ~work_all["feature_matched"]
    if len(work_all):
        missing_date_mask = unmatched_mask & (~work_all["D"].astype(str).isin(feature_dates))
        work_all.loc[missing_date_mask, "feature_match_reason"] = "FEATURE_FILE_MISSING_FOR_D"
        code_absent_mask = unmatched_mask & (~missing_date_mask) & (
            ~work_all.apply(lambda row: (str(row.get("D")), str(row.get("code"))) in feature_keys, axis=1)
        )
        work_all.loc[code_absent_mask, "feature_match_reason"] = "FEATURE_CODE_ABSENT_FOR_D"
        stale_history_mask = code_absent_mask & (
            ~work_all.apply(lambda row: (str(row.get("D")), str(row.get("code"))) in preview_keys, axis=1)
        )
        work_all.loc[stale_history_mask, "feature_match_reason"] = "STALE_HISTORY_UNION_NOT_IN_CURRENT_PREVIEW"
        candidate_absent_mask = code_absent_mask & (~stale_history_mask) & (
            ~work_all.apply(lambda row: (str(row.get("D")), str(row.get("code"))) in candidate_keys, axis=1)
        )
        work_all.loc[candidate_absent_mask, "feature_match_reason"] = "PREVIEW_CODE_NOT_IN_CANDIDATES_FOR_D"
        value_missing_mask = unmatched_mask & (~missing_date_mask) & (~code_absent_mask)
        work_all.loc[value_missing_mask, "feature_match_reason"] = "FEATURE_VALUE_MISSING"
    work = work_all[work_all["feature_matched"]].copy()

    detail_rows: List[Dict[str, Any]] = []
    summary_rows: List[Dict[str, Any]] = []
    component_rows: List[Dict[str, Any]] = []

    for horizon, group in work.groupby("horizon_type"):
        group = group.copy()
        group["pred_current"] = group["pred_up_prob"]
        group["pred_contrarian_1_minus_pred"] = 1.0 - group["pred_up_prob"]
        group["pred_drop_core"] = _shadow_pred(group, str(horizon), "drop_core")
        group["pred_invert_core"] = _shadow_pred(group, str(horizon), "invert_core")

        for component in HORIZON_WEIGHTS[str(horizon)]:
            value = _feature_value(group, component)
            tmp = pd.DataFrame({"component_value": value, "realized_return": group["realized_return"]})
            component_rows.append(
                {
                    "horizon_type": str(horizon),
                    "component": component,
                    "weight": HORIZON_WEIGHTS[str(horizon)][component],
                    "rows": int(tmp.dropna().shape[0]),
                    "spearman_component_vs_return": _safe_corr(tmp, "component_value", "realized_return"),
                }
            )

        for variant in ("current", "contrarian_1_minus_pred", "drop_core", "invert_core"):
            pred_col = f"pred_{variant}"
            g = group.dropna(subset=[pred_col, "realized_return"]).copy()
            spearman = _safe_corr(g, pred_col, "realized_return")
            for top_pct in (0.10, 0.20, 0.30):
                if g.empty:
                    selected = g
                    cutoff = None
                else:
                    cutoff = float(g[pred_col].quantile(1.0 - top_pct))
                    selected = g[g[pred_col] >= cutoff].copy()
                met = _metrics(selected)
                row = {
                    "horizon_type": str(horizon),
                    "variant": variant,
                    "top_pct": top_pct,
                    "cutoff": None if cutoff is None else round(cutoff, 6),
                    "spearman_pred_vs_return": spearman,
                    **met,
                }
                row["grade"] = _variant_grade(str(horizon), variant, met, spearman)
                summary_rows.append(row)
            detail = group[
                [
                    "D",
                    "code",
                    "name",
                    "horizon_type",
                    "realized_return",
                    "pred_current",
                    "pred_contrarian_1_minus_pred",
                    "pred_drop_core",
                    "pred_invert_core",
                ]
            ].copy()
            detail_rows.extend(detail.to_dict(orient="records"))

    summary_df = pd.DataFrame(summary_rows)
    component_df = pd.DataFrame(component_rows)
    detail_df = pd.DataFrame(detail_rows).drop_duplicates()
    unmatched_df = work_all[~work_all["feature_matched"]].copy()
    unmatched_keep = [
        c
        for c in ["D", "code", "name", "horizon_type", "realized_return", "feature_match_reason"]
        if c in unmatched_df.columns
    ]
    unmatched_out = unmatched_df[unmatched_keep].copy() if unmatched_keep else pd.DataFrame()
    _write_csv(LATEST_CSV, summary_df)
    _write_csv(COMPONENT_CSV, component_df)
    _write_csv(DETAIL_CSV, detail_df)
    _write_csv(UNMATCHED_CSV, unmatched_out)

    promising = summary_df[summary_df["grade"].astype(str).str.startswith("SHADOW_PROMISING", na=False)]
    component_rank = component_df.copy()
    if not component_rank.empty and "spearman_component_vs_return" in component_rank.columns:
        component_rank["spearman_component_vs_return"] = pd.to_numeric(
            component_rank["spearman_component_vs_return"],
            errors="coerce",
        )
    strongest_negative = (
        component_rank.dropna(subset=["spearman_component_vs_return"])
        .sort_values("spearman_component_vs_return")
        .head(8)
        .to_dict(orient="records")
        if not component_rank.empty
        else []
    )
    strongest_positive = (
        component_rank.dropna(subset=["spearman_component_vs_return"])
        .sort_values("spearman_component_vs_return", ascending=False)
        .head(8)
        .to_dict(orient="records")
        if not component_rank.empty
        else []
    )
    unmatched_breakdown: Dict[str, Any] = {
        "by_reason": {},
        "by_date_horizon_reason": [],
    }
    if not unmatched_df.empty and "feature_match_reason" in unmatched_df.columns:
        unmatched_breakdown["by_reason"] = {
            str(k): int(v)
            for k, v in unmatched_df["feature_match_reason"].astype(str).value_counts().to_dict().items()
        }
        group_cols = [c for c in ["D", "horizon_type", "feature_match_reason"] if c in unmatched_df.columns]
        if group_cols:
            unmatched_breakdown["by_date_horizon_reason"] = (
                unmatched_df.groupby(group_cols)
                .size()
                .reset_index(name="rows")
                .sort_values(group_cols)
                .to_dict(orient="records")
            )
    payload = {
        "status": "PASS",
        "reason": "ok",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": str(cross_d_csv),
        "feature_files_used": int(len(list(LOGS.glob("future_signal_features_*.csv")))),
        "source_rows": int(len(work_all)),
        "feature_matched_rows": int(len(work)),
        "feature_unmatched_rows": int(len(work_all) - len(work)),
        "feature_unmatched_breakdown": unmatched_breakdown,
        "horizons": sorted(work["horizon_type"].dropna().astype(str).unique().tolist()),
        "variant_policy": {
            "scope": "diagnostic_shadow_only",
            "used_for_trading": False,
            "orders_modified": False,
            "fills_modified": False,
            "ledger_modified": False,
            "stats_modified": False,
            "gate_modified": False,
            "risk_lock_modified": False,
            "score_modified": False,
            "threshold_applied": False,
            "core_invert_components": sorted(CORE_INVERT_COMPONENTS),
        },
        "summary_rows": int(len(summary_df)),
        "component_rows": int(len(component_df)),
        "promising_rows": int(len(promising)),
        "promising_conditions": promising.to_dict(orient="records"),
        "component_spearman_summary": {
            "strongest_negative": strongest_negative,
            "strongest_positive": strongest_positive,
        },
        "outputs": {
            "json": str(LATEST_JSON),
            "csv": str(LATEST_CSV),
            "component_csv": str(COMPONENT_CSV),
            "detail_csv": str(DETAIL_CSV),
            "unmatched_csv": str(UNMATCHED_CSV),
        },
    }
    _write_json(LATEST_JSON, payload)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Build read-only future-signal shadow variant review.")
    ap.add_argument("--cross-d-csv", default=str(DEFAULT_CROSS_D))
    args = ap.parse_args()
    payload = build_review(Path(args.cross_d_csv))
    print(
        "[FUTURE_SHADOW_VARIANT_REVIEW] "
        f"status={payload.get('status')} reason={payload.get('reason')} "
        f"feature_matched_rows={payload.get('feature_matched_rows', 0)} "
        f"promising_rows={payload.get('promising_rows', 0)}"
    )
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
