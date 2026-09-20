from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


ROOT = Path("E:/1_Data")
LOGS = ROOT / "2_Logs"
DEFAULT_CANDIDATES = LOGS / "candidates_latest_data.with_final_score.csv"
DEFAULT_PREVIEW_META = LOGS / "future_signal_preview_latest.json"
DEFAULT_CROSS_D_CSV = LOGS / "future_signal_cross_d_validation_latest.csv"
LATEST_JSON = LOGS / "future_signal_candidate_shadow_join_latest.json"
LATEST_CSV = LOGS / "future_signal_candidate_shadow_join_latest.csv"


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8-sig") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc, dtype={"code": str})
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype={"code": str})


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _norm_code(s: Any) -> str:
    text = str(s or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(6) if text else ""


def _safe_corr(df: pd.DataFrame, left: str, right: str) -> float | None:
    if len(df) < 2 or left not in df.columns or right not in df.columns:
        return None
    a = pd.to_numeric(df[left], errors="coerce")
    b = pd.to_numeric(df[right], errors="coerce")
    ok = a.notna() & b.notna()
    if int(ok.sum()) < 2:
        return None
    value = a.loc[ok].corr(b.loc[ok], method="spearman")
    if pd.isna(value):
        return None
    return round(float(value), 6)


def _horizon_metrics(cross_d_csv: Path) -> List[Dict[str, Any]]:
    if not cross_d_csv.exists():
        return []
    hist = _read_csv(cross_d_csv)
    required = {"horizon_type", "pred_up_prob", "realized_return", "actual_up"}
    if not required.issubset(hist.columns):
        return []
    hmap = {"D0": "INTRADAY", "D2": "SHORT", "D5": "SWING", "D15": "MID", "D30": "LONG"}
    hist["horizon_type_norm"] = hist["horizon_type"].astype(str).str.upper().replace(hmap)
    hist["pred_up_prob"] = pd.to_numeric(hist["pred_up_prob"], errors="coerce")
    hist["realized_return"] = pd.to_numeric(hist["realized_return"], errors="coerce")
    hist["actual_up"] = pd.to_numeric(hist["actual_up"], errors="coerce")
    out: List[Dict[str, Any]] = []
    for horizon, g in hist.dropna(subset=["pred_up_prob", "realized_return", "actual_up"]).groupby("horizon_type_norm"):
        top_n = max(1, int(len(g) * 0.30 + 0.999999)) if len(g) else 0
        top = g.sort_values("pred_up_prob", ascending=False).head(top_n) if top_n else g
        out.append(
            {
                "horizon_type": str(horizon),
                "rows": int(len(g)),
                "hit_rate": round(float(g["actual_up"].mean()), 6) if len(g) else None,
                "mean_realized_return": round(float(g["realized_return"].mean()), 6) if len(g) else None,
                "median_realized_return": round(float(g["realized_return"].median()), 6) if len(g) else None,
                "spearman_pred_up_prob_vs_return": _safe_corr(g, "pred_up_prob", "realized_return"),
                "top30_rows": int(len(top)),
                "top30_hit_rate": round(float(top["actual_up"].mean()), 6) if len(top) else None,
                "top30_mean_realized_return": round(float(top["realized_return"].mean()), 6) if len(top) else None,
            }
        )
    return sorted(out, key=lambda x: str(x.get("horizon_type") or ""))


def build_shadow_join(candidates_csv: Path, preview_meta_path: Path, cross_d_csv: Path) -> Dict[str, Any]:
    preview_meta = _read_json(preview_meta_path)
    preview_csv = Path(str((preview_meta.get("outputs") or {}).get("csv") or ""))

    if not candidates_csv.exists():
        payload = {"status": "FAIL", "reason": "CANDIDATES_CSV_MISSING", "path": str(candidates_csv)}
        _write_json(LATEST_JSON, payload)
        return payload
    if not preview_csv.exists():
        payload = {"status": "FAIL", "reason": "PREVIEW_CSV_MISSING", "path": str(preview_csv)}
        _write_json(LATEST_JSON, payload)
        return payload

    candidates = _read_csv(candidates_csv)
    preview = _read_csv(preview_csv)
    if "code" not in candidates.columns or "code" not in preview.columns:
        payload = {"status": "FAIL", "reason": "CODE_COLUMN_MISSING"}
        _write_json(LATEST_JSON, payload)
        return payload

    candidates = candidates.copy()
    preview = preview.copy()
    candidates["code"] = candidates["code"].map(_norm_code)
    preview["code"] = preview["code"].map(_norm_code)

    keep_candidate_cols = [
        c
        for c in [
            "date",
            "code",
            "name",
            "score",
            "final_score",
            "forecast_score",
            "forecast_label",
            "final_score_w_forecast",
            "news_score",
            "sector_score",
            "regime_score",
            "candidate_origin",
            "candidate_origin_hybrid",
        ]
        if c in candidates.columns
    ]
    keep_preview_cols = [
        c
        for c in [
            "D",
            "code",
            "horizon_type",
            "horizon",
            "horizon_days",
            "pred_up_prob",
            "expected_return",
            "downside_q10",
            "confidence",
            "abstain",
            "news_contribution",
            "model_version",
            "calibration_version",
        ]
        if c in preview.columns
    ]

    joined = preview[keep_preview_cols].merge(candidates[keep_candidate_cols], on="code", how="left", suffixes=("_future", ""))
    joined["used_for_trading"] = False
    joined["policy_note"] = "SHADOW_JOIN_ONLY_NOT_ROUTED"
    _write_csv(LATEST_CSV, joined)

    horizon_summary: List[Dict[str, Any]] = []
    for horizon, g in joined.groupby("horizon_type", dropna=False):
        horizon_summary.append(
            {
                "horizon_type": str(horizon),
                "rows": int(len(g)),
                "matched_candidate_rows": int(g["final_score"].notna().sum()) if "final_score" in g.columns else 0,
                "pred_up_prob_min": round(float(pd.to_numeric(g["pred_up_prob"], errors="coerce").min()), 6),
                "pred_up_prob_mean": round(float(pd.to_numeric(g["pred_up_prob"], errors="coerce").mean()), 6),
                "pred_up_prob_max": round(float(pd.to_numeric(g["pred_up_prob"], errors="coerce").max()), 6),
                "spearman_pred_vs_forecast_score": _safe_corr(g, "pred_up_prob", "forecast_score"),
                "spearman_pred_vs_final_score": _safe_corr(g, "pred_up_prob", "final_score"),
            }
        )

    payload = {
        "status": "PASS",
        "reason": "ok",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "D": str(preview_meta.get("D") or ""),
        "candidate_rows": int(len(candidates)),
        "preview_rows": int(len(preview)),
        "joined_rows": int(len(joined)),
        "matched_candidate_rows": int(joined["final_score"].notna().sum()) if "final_score" in joined.columns else 0,
        "horizon_summary": sorted(horizon_summary, key=lambda x: str(x.get("horizon_type") or "")),
        "historical_realized_metrics": _horizon_metrics(cross_d_csv),
        "policy": {
            "shadow_only": True,
            "used_for_trading": False,
            "orders_modified": False,
            "fills_modified": False,
            "ledger_modified": False,
            "stats_modified": False,
            "gate_modified": False,
            "risk_lock_modified": False,
            "score_modified": False,
            "threshold_applied": False,
        },
        "outputs": {
            "csv": str(LATEST_CSV),
            "json": str(LATEST_JSON),
        },
        "sources": {
            "candidates_csv": str(candidates_csv),
            "preview_csv": str(preview_csv),
            "preview_meta": str(preview_meta_path),
            "cross_d_csv": str(cross_d_csv),
        },
    }
    _write_json(LATEST_JSON, payload)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Build a read-only shadow join between final-score candidates and future-signal preview.")
    ap.add_argument("--candidates-csv", default=str(DEFAULT_CANDIDATES))
    ap.add_argument("--preview-meta", default=str(DEFAULT_PREVIEW_META))
    ap.add_argument("--cross-d-csv", default=str(DEFAULT_CROSS_D_CSV))
    args = ap.parse_args()

    payload = build_shadow_join(Path(args.candidates_csv), Path(args.preview_meta), Path(args.cross_d_csv))
    print(
        "[FUTURE_SHADOW_JOIN] "
        f"status={payload.get('status')} reason={payload.get('reason')} "
        f"joined_rows={payload.get('joined_rows', 0)}"
    )
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
