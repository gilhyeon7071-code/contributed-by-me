import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"

DEFAULT_CROSS_D = LOGS / "future_signal_cross_d_validation_latest.csv"
DEFAULT_STALE_ROWS = LOGS / "future_signal_history_integrity_stale_rows_latest.csv"
LATEST_JSON = LOGS / "factor_alpha_shadow_review_latest.json"
LATEST_CSV = LOGS / "factor_alpha_shadow_review_latest.csv"
DETAIL_CSV = LOGS / "factor_alpha_shadow_review_details_latest.csv"

POSITIVE_FACTORS = ["news_score", "foreign_net_20d", "execution_lob_score"]
OVERHEAT_FACTORS = [
    "rs",
    "v_accel",
    "rsi14",
    "stoch_k",
    "disparity20",
    "disparity60",
    "adx14",
    "bb_width",
    "forecast_score",
    "score",
]
HORIZONS = ["INTRADAY", "SHORT", "SWING", "MID"]


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc, dtype=str)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype=str)


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _norm_code(v: Any) -> str:
    s = str(v or "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(6) if s.isdigit() else s


def _norm_ymd(v: Any) -> str:
    s = str(v or "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s[:8]


def _norm_horizon(v: Any) -> str:
    s = str(v or "").strip()
    return s[:-2] if s.endswith(".0") else s


def _row_key(row: pd.Series) -> Tuple[str, str, str]:
    return (_norm_ymd(row.get("D")), _norm_code(row.get("code")), _norm_horizon(row.get("horizon")))


def _stale_keys(stale_path: Path) -> Set[Tuple[str, str, str]]:
    if not stale_path.exists():
        return set()
    stale = _read_csv(stale_path)
    if stale.empty:
        return set()
    for col in ("D", "code", "horizon"):
        if col not in stale.columns:
            stale[col] = ""
    return {_row_key(r) for _, r in stale.iterrows()}


def _load_features() -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for path in sorted(LOGS.glob("future_signal_features_*.csv")):
        if path.name.endswith("_latest.csv"):
            continue
        d = path.stem.split("_")[-1]
        df = _read_csv(path)
        if "code" not in df.columns:
            continue
        df["D"] = d
        df["code"] = df["code"].map(_norm_code)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _rank_pct(s: pd.Series, *, ascending: bool = True) -> pd.Series:
    x = pd.to_numeric(s, errors="coerce")
    if x.notna().sum() == 0 or x.nunique(dropna=True) <= 1:
        return pd.Series([0.5] * len(s), index=s.index)
    return x.rank(method="average", pct=True, ascending=ascending).fillna(0.5)


def _safe_corr(df: pd.DataFrame, score_col: str) -> Any:
    if df.empty or score_col not in df.columns or "realized_return" not in df.columns:
        return None
    x = pd.to_numeric(df[score_col], errors="coerce")
    y = pd.to_numeric(df["realized_return"], errors="coerce")
    valid = x.notna() & y.notna()
    if int(valid.sum()) < 10 or x[valid].nunique() < 2 or y[valid].nunique() < 2:
        return None
    return round(float(x[valid].corr(y[valid], method="spearman")), 6)


def _metrics(df: pd.DataFrame, score_col: str, label: str, horizon: str, bucket: str) -> Dict[str, Any]:
    ret = pd.to_numeric(df.get("realized_return", pd.Series(dtype=float)), errors="coerce")
    return {
        "horizon_type": horizon,
        "variant": label,
        "bucket": bucket,
        "rows": int(len(df)),
        "hit_rate": round(float((ret > 0).mean()), 6) if len(df) else None,
        "mean_return": round(float(ret.mean()), 6) if len(df) else None,
        "median_return": round(float(ret.median()), 6) if len(df) else None,
        "q10_return": round(float(ret.quantile(0.10)), 6) if len(df) else None,
        "q90_return": round(float(ret.quantile(0.90)), 6) if len(df) else None,
        "spearman_score_vs_return": _safe_corr(df, score_col),
    }


def _grade(row: Dict[str, Any]) -> str:
    rows = int(row.get("rows") or 0)
    mean_return = row.get("mean_return")
    hit_rate = row.get("hit_rate")
    spearman = row.get("spearman_score_vs_return")
    if rows < 20:
        return "INSUFFICIENT_SAMPLE"
    if mean_return is None or hit_rate is None or spearman is None:
        return "INSUFFICIENT_METRIC"
    if mean_return > 0 and hit_rate >= 0.50 and spearman > 0:
        return "SHADOW_PROMISING"
    if spearman > 0 and mean_return <= 0:
        return "RANKING_IMPROVES_RETURN_STILL_NEGATIVE"
    if mean_return > 0 and spearman <= 0:
        return "RETURN_POSITIVE_RANKING_NOT_CONFIRMED"
    return "DISCARD_OR_WAIT"


def _build_variant_scores(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in set(POSITIVE_FACTORS + OVERHEAT_FACTORS + ["final_score", "pred_up_prob"]):
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce")

    # Ranking is computed within each signal day so the shadow layer compares candidates available at the same time.
    groups = out.groupby("D", dropna=False)
    out["score_current_final"] = groups["final_score"].transform(lambda s: _rank_pct(s, ascending=True))
    out["score_current_pred"] = groups["pred_up_prob"].transform(lambda s: _rank_pct(s, ascending=True))
    out["score_news_only"] = groups["news_score"].transform(lambda s: _rank_pct(s, ascending=True))
    out["score_foreign20_only"] = groups["foreign_net_20d"].transform(lambda s: _rank_pct(s, ascending=True))
    out["score_lob_only"] = groups["execution_lob_score"].transform(lambda s: _rank_pct(s, ascending=True))

    positive_parts = [
        out["score_news_only"] * 0.55,
        out["score_foreign20_only"] * 0.25,
        out["score_lob_only"] * 0.20,
    ]
    out["score_news_foreign_lob"] = sum(positive_parts)

    overheat_scores: List[pd.Series] = []
    for factor in OVERHEAT_FACTORS:
        score_col = f"_low_{factor}"
        out[score_col] = groups[factor].transform(lambda s: _rank_pct(s, ascending=False))
        overheat_scores.append(out[score_col])
    out["score_low_overheat_combo"] = pd.concat(overheat_scores, axis=1).mean(axis=1) if overheat_scores else 0.5
    out["score_news_low_overheat_combo"] = out["score_news_only"] * 0.55 + out["score_low_overheat_combo"] * 0.45
    out["score_alpha_combo"] = (
        out["score_news_only"] * 0.45
        + out["score_foreign20_only"] * 0.20
        + out["score_lob_only"] * 0.15
        + out["score_low_overheat_combo"] * 0.20
    )
    return out


def build_review(cross_d_csv: Path, stale_rows_csv: Path, output_json: Path, output_csv: Path, detail_csv: Path) -> Dict[str, Any]:
    generated_at = datetime.now().isoformat(timespec="seconds")
    if not cross_d_csv.exists():
        payload = {"generated_at": generated_at, "status": "FAIL", "reason": "CROSS_D_CSV_MISSING", "path": str(cross_d_csv)}
        _write_json(output_json, payload)
        return payload

    cross = _read_csv(cross_d_csv)
    features = _load_features()
    if cross.empty or features.empty:
        payload = {
            "generated_at": generated_at,
            "status": "FAIL",
            "reason": "INPUT_EMPTY",
            "cross_rows": int(len(cross)),
            "feature_rows": int(len(features)),
            "policy": _policy(),
        }
        _write_json(output_json, payload)
        return payload

    for col in ("D", "code", "horizon", "horizon_type", "realized_return"):
        if col not in cross.columns:
            cross[col] = ""
    cross["D"] = cross["D"].map(_norm_ymd)
    cross["code"] = cross["code"].map(_norm_code)
    cross["horizon"] = cross["horizon"].map(_norm_horizon)
    cross["realized_return"] = pd.to_numeric(cross["realized_return"], errors="coerce")
    stale = _stale_keys(stale_rows_csv)
    cross["_stale"] = cross.apply(lambda r: _row_key(r) in stale, axis=1)
    clean = cross[~cross["_stale"]].copy()

    features["D"] = features["D"].map(_norm_ymd)
    features["code"] = features["code"].map(_norm_code)
    merged = clean.merge(features, on=["D", "code"], how="left", suffixes=("", "_feature"))
    merged = merged[merged["feature_asof"].notna()].copy()
    merged = _build_variant_scores(merged)

    variants = {
        "current_final_score": "score_current_final",
        "current_pred_up_prob": "score_current_pred",
        "news_only": "score_news_only",
        "foreign_net_20d_only": "score_foreign20_only",
        "execution_lob_only": "score_lob_only",
        "news_foreign_lob": "score_news_foreign_lob",
        "low_overheat_combo": "score_low_overheat_combo",
        "news_low_overheat_combo": "score_news_low_overheat_combo",
        "alpha_combo": "score_alpha_combo",
    }

    summary_rows: List[Dict[str, Any]] = []
    for horizon in HORIZONS:
        hdf = merged[merged["horizon_type"].astype(str).str.upper() == horizon].copy()
        if hdf.empty:
            continue
        for label, score_col in variants.items():
            full = _metrics(hdf, score_col, label, horizon, "ALL_CLEAN")
            full["grade"] = _grade(full)
            summary_rows.append(full)
            for top_pct in (0.10, 0.20, 0.30):
                n = max(1, int(round(len(hdf) * top_pct)))
                top = hdf.sort_values(score_col, ascending=False).head(n).copy()
                row = _metrics(top, score_col, label, horizon, f"TOP_{int(top_pct * 100)}PCT")
                row["top_pct"] = top_pct
                row["grade"] = _grade(row)
                summary_rows.append(row)
            top3 = hdf.sort_values(["D", score_col], ascending=[True, False]).groupby("D", dropna=False).head(3)
            row = _metrics(top3, score_col, label, horizon, "TOP3_BY_D")
            row["grade"] = _grade(row)
            summary_rows.append(row)

    summary = pd.DataFrame(summary_rows)
    detail_cols = [
        "D",
        "code",
        "name",
        "horizon_type",
        "horizon",
        "realized_return",
        "pred_up_prob",
        "final_score",
        "news_score",
        "foreign_net_20d",
        "execution_lob_score",
        "rs",
        "v_accel",
        "rsi14",
        "stoch_k",
        "disparity20",
        "disparity60",
        "forecast_score",
        "score_current_final",
        "score_current_pred",
        "score_news_only",
        "score_news_foreign_lob",
        "score_low_overheat_combo",
        "score_news_low_overheat_combo",
        "score_alpha_combo",
    ]
    detail = merged[[c for c in detail_cols if c in merged.columns]].copy()
    _write_csv(output_csv, summary)
    _write_csv(detail_csv, detail)

    promising = summary[summary["grade"].astype(str).eq("SHADOW_PROMISING")].copy() if not summary.empty else pd.DataFrame()
    top20 = summary[summary["bucket"].astype(str).eq("TOP_20PCT")].copy() if not summary.empty else pd.DataFrame()
    payload = {
        "generated_at": generated_at,
        "status": "PASS",
        "reason": "ok",
        "cross_d_csv": str(cross_d_csv),
        "stale_rows_csv": str(stale_rows_csv),
        "raw_rows": int(len(cross)),
        "clean_rows": int(len(clean)),
        "feature_matched_rows": int(len(merged)),
        "horizons": sorted(merged["horizon_type"].dropna().astype(str).unique().tolist()),
        "variants": list(variants.keys()),
        "promising_rows": int(len(promising)),
        "promising_conditions": promising.to_dict(orient="records"),
        "top20_snapshot": top20.sort_values(["horizon_type", "mean_return"], ascending=[True, False]).to_dict(orient="records"),
        "outputs": {"json": str(output_json), "csv": str(output_csv), "details_csv": str(detail_csv)},
        "policy": _policy(),
    }
    _write_json(output_json, payload)
    return payload


def _policy() -> Dict[str, Any]:
    return {
        "diagnostic_only": True,
        "used_for_trading": False,
        "score_modified": False,
        "threshold_applied": False,
        "orders_modified": False,
        "fills_modified": False,
        "ledger_modified": False,
        "stats_modified": False,
        "gate_modified": False,
        "risk_lock_modified": False,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Read-only factor/alpha shadow review for future-signal clean realized rows.")
    ap.add_argument("--cross-d-csv", default=str(DEFAULT_CROSS_D))
    ap.add_argument("--stale-rows-csv", default=str(DEFAULT_STALE_ROWS))
    ap.add_argument("--output-json", default=str(LATEST_JSON))
    ap.add_argument("--output-csv", default=str(LATEST_CSV))
    ap.add_argument("--detail-csv", default=str(DETAIL_CSV))
    args = ap.parse_args()
    payload = build_review(
        cross_d_csv=Path(args.cross_d_csv),
        stale_rows_csv=Path(args.stale_rows_csv),
        output_json=Path(args.output_json),
        output_csv=Path(args.output_csv),
        detail_csv=Path(args.detail_csv),
    )
    print(
        f"[FACTOR_ALPHA_SHADOW] status={payload.get('status')} reason={payload.get('reason')} "
        f"clean_rows={payload.get('clean_rows', 0)} feature_matched={payload.get('feature_matched_rows', 0)} "
        f"promising_rows={payload.get('promising_rows', 0)}"
    )
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
