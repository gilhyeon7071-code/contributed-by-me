from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"

DEFAULT_INPUT = LOGS / "candidates_latest_data.with_final_score.csv"
LATEST_JSON = LOGS / "future_signal_preview_latest.json"

MODEL_META_REQUIRED_KEYS = [
    "model_version",
    "model_type",
    "model_path",
    "feature_version",
    "feature_schema_sha256",
    "train_window_start",
    "train_window_end",
    "calibration_version",
    "calibration_path",
    "target",
    "horizon",
    "created_at",
]

REQUIRED_COLUMNS = [
    "as_of",
    "run_id",
    "D",
    "code",
    "name",
    "horizon_type",
    "horizon",
    "horizon_days",
    "pred_up_prob",
    "expected_return",
    "downside_q10",
    "confidence",
    "abstain",
    "reason",
    "news_score",
    "news_weight",
    "news_contribution",
    "model_version",
    "feature_version",
    "feature_asof",
    "train_window_start",
    "train_window_end",
    "calibration_version",
]


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path)


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _norm_date8(v: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s[:8] if len(s) >= 8 else ""


def _norm_code(v: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s.zfill(6)[-6:] if s else ""


def _int_or_default(v: Any, default: int) -> int:
    if v is None:
        return int(default)
    if isinstance(v, str) and not v.strip():
        return int(default)
    try:
        return int(float(v))
    except Exception:
        return int(default)


def _latest_date8(df: pd.DataFrame) -> str:
    for col in ("date_yyyymmdd", "D", "as_of_ymd", "date"):
        if col not in df.columns:
            continue
        vals = df[col].map(_norm_date8)
        vals = vals[vals.str.len() == 8]
        if len(vals):
            return str(vals.max())
    return ""


def _file_sha16(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()[:16]
    except Exception:
        return ""


def _load_json(path: Optional[Path]) -> Dict[str, Any]:
    if not path:
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _validate_model_meta(model_meta_path: Optional[Path], model_meta: Dict[str, Any], d: str) -> List[str]:
    if not model_meta_path or not model_meta_path.exists():
        return ["MODEL_META_MISSING"]
    missing = [k for k in MODEL_META_REQUIRED_KEYS if not str(model_meta.get(k, "")).strip()]
    reasons: List[str] = []
    if missing:
        reasons.append("MODEL_META_REQUIRED_KEY_MISSING:" + ",".join(missing))
    model_path = Path(str(model_meta.get("model_path", ""))) if str(model_meta.get("model_path", "")).strip() else None
    calibration_path = (
        Path(str(model_meta.get("calibration_path", "")))
        if str(model_meta.get("calibration_path", "")).strip()
        else None
    )
    if model_path and not model_path.exists():
        reasons.append("MODEL_PATH_MISSING")
    if calibration_path and not calibration_path.exists():
        reasons.append("CALIBRATION_PATH_MISSING")
    train_end = _norm_date8(model_meta.get("train_window_end"))
    if d and train_end and train_end > d:
        reasons.append("MODEL_TRAIN_WINDOW_AFTER_D")
    return reasons


def _validate_feature_meta(feature_meta_path: Optional[Path], feature_meta: Dict[str, Any], model_meta: Dict[str, Any], d: str) -> List[str]:
    if not feature_meta_path or not feature_meta_path.exists():
        return ["FEATURE_ARTIFACT_MISSING"]
    reasons: List[str] = []
    feature_asof = _norm_date8(feature_meta.get("feature_asof"))
    if d and feature_asof and feature_asof > d:
        reasons.append("FEATURE_ASOF_AFTER_D")
    if str(feature_meta.get("status", "")).upper() != "PASS":
        reasons.append("FEATURE_ARTIFACT_NOT_PASS")
    if model_meta:
        if str(model_meta.get("feature_version", "")) and str(model_meta.get("feature_version")) != str(feature_meta.get("feature_version")):
            reasons.append("FEATURE_VERSION_MISMATCH")
        if str(model_meta.get("feature_schema_sha256", "")) and str(model_meta.get("feature_schema_sha256")) != str(feature_meta.get("feature_schema_sha256")):
            reasons.append("FEATURE_SCHEMA_MISMATCH")
    return reasons


def _build_run_id(d: str, input_path: Path, model_meta_path: Optional[Path]) -> str:
    parts = [
        d,
        _file_sha16(input_path),
        _file_sha16(model_meta_path) if model_meta_path else "no_model",
    ]
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]
    return f"FSP_{d}_{digest}"


def _duplicate_count(df: pd.DataFrame) -> int:
    cols = ["D", "code", "horizon_type", "horizon"]
    if any(c not in df.columns for c in cols):
        return 0
    return int(df.duplicated(subset=cols, keep=False).sum())


def _load_prediction_rows(model_meta: Dict[str, Any], d: str, horizon: int) -> List[Dict[str, Any]]:
    pred_path = Path(str(model_meta.get("prediction_path", ""))) if str(model_meta.get("prediction_path", "")).strip() else None
    if not pred_path or not pred_path.exists():
        return []
    try:
        pred = _read_csv(pred_path)
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    for _, r in pred.iterrows():
        code = _norm_code(r.get("code"))
        row_d = _norm_date8(r.get("D"))
        row_h = _int_or_default(r.get("horizon"), horizon)
        horizon_days = _int_or_default(r.get("horizon_days"), row_h)
        if not code or (d and row_d != d):
            continue
        out.append({
            "code": code,
            "name": str(r.get("name", "") or ""),
            "horizon_type": str(r.get("horizon_type", "") or f"D{row_h}"),
            "horizon": row_h,
            "horizon_days": horizon_days,
            "pred_up_prob": r.get("pred_up_prob", ""),
            "expected_return": r.get("expected_return", ""),
            "downside_q10": r.get("downside_q10", ""),
            "confidence": r.get("confidence", 0.0),
            "abstain": str(r.get("abstain", "")).strip().lower() in {"1", "true", "yes"},
            "reason": str(r.get("reason", "") or "OK"),
            "news_score": r.get("news_score", ""),
            "news_weight": r.get("news_weight", ""),
            "news_contribution": r.get("news_contribution", ""),
        })
    return out


def build_preview(
    *,
    input_path: Path,
    output_dir: Path,
    d_override: str = "",
    model_meta_path: Optional[Path] = None,
    feature_meta_path: Optional[Path] = None,
    horizon: int = 5,
) -> Dict[str, Any]:
    status = "FAIL"
    reasons: List[str] = []

    if not input_path.exists():
        payload = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "status": "FAIL",
            "reason": "INPUT_MISSING",
            "input": str(input_path),
            "outputs": {},
            "rows": 0,
        }
        return payload

    src = _read_csv(input_path)
    empty_input = len(src) == 0
    model_meta = _load_json(model_meta_path)
    feature_meta = _load_json(feature_meta_path)
    d = (
        _norm_date8(d_override)
        or _latest_date8(src)
        or _norm_date8(feature_meta.get("D"))
        or _norm_date8(model_meta.get("train_window_end"))
    )
    if not d:
        reasons.append("D_MISSING")

    reasons.extend(_validate_model_meta(model_meta_path, model_meta, d))
    reasons.extend(_validate_feature_meta(feature_meta_path, feature_meta, model_meta, d))

    feature_asof = _norm_date8(feature_meta.get("feature_asof")) or d
    if feature_asof and d and feature_asof > d:
        reasons.append("FEATURE_ASOF_AFTER_D")

    run_id = _build_run_id(d or "unknown", input_path, model_meta_path)
    out_csv = output_dir / f"future_signal_preview_{d or 'unknown'}.csv"
    out_json = output_dir / f"future_signal_preview_{d or 'unknown'}.json"
    feature_version = str(
        model_meta.get("feature_version")
        or feature_meta.get("feature_version")
        or "CANDIDATE_FINAL_SCORE_V1"
    )
    prediction_rows = _load_prediction_rows(model_meta, d, horizon)
    if model_meta and not prediction_rows and not empty_input:
        reasons.append("PREDICTION_ARTIFACT_MISSING")

    rows: List[Dict[str, Any]] = []
    source_names: Dict[str, str] = {}
    for _, r in src.iterrows():
        source_names[_norm_code(r.get("code"))] = str(r.get("name", "") or "")
    for pred in prediction_rows:
        code = _norm_code(pred.get("code"))
        if not code:
            continue
        row_horizon = _int_or_default(pred.get("horizon"), horizon)
        row_horizon_days = _int_or_default(pred.get("horizon_days"), row_horizon)
        row_abstain = bool(pred.get("abstain", False))
        row_reason = str(pred.get("reason") or "OK")
        if reasons:
            row_abstain = True
            row_reason = "|".join(reasons)
        rows.append(
            {
                "as_of": d,
                "run_id": run_id,
                "D": d,
                "code": code,
                "name": str(pred.get("name") or source_names.get(code, "")),
                "horizon_type": str(pred.get("horizon_type") or f"D{row_horizon}"),
                "horizon": row_horizon,
                "horizon_days": row_horizon_days,
                "pred_up_prob": pred.get("pred_up_prob", ""),
                "expected_return": pred.get("expected_return", ""),
                "downside_q10": pred.get("downside_q10", ""),
                "confidence": pred.get("confidence", 0.0),
                "abstain": row_abstain,
                "reason": row_reason,
                "news_score": pred.get("news_score", ""),
                "news_weight": pred.get("news_weight", ""),
                "news_contribution": pred.get("news_contribution", ""),
                "model_version": str(model_meta.get("model_version") or "NO_MODEL_PREVIEW_V0"),
                "feature_version": feature_version,
                "feature_asof": feature_asof,
                "train_window_start": str(model_meta.get("train_window_start") or ""),
                "train_window_end": str(model_meta.get("train_window_end") or ""),
                "calibration_version": str(model_meta.get("calibration_version") or ""),
            }
        )

    preview = pd.DataFrame(rows, columns=REQUIRED_COLUMNS)
    dup_count = _duplicate_count(preview)
    if len(preview) == 0 and not empty_input:
        reasons.append("PREDICTION_ROW_COUNT_ZERO")
    if dup_count > 0:
        reasons.append("DUPLICATE_D_CODE_HORIZON")

    if reasons:
        status = "FAIL"
    else:
        status = "PASS"

    if len(preview) and reasons:
        preview["abstain"] = True
        preview["reason"] = "|".join(dict.fromkeys(reasons))

    _write_csv(out_csv, preview)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "reason": "|".join(dict.fromkeys(reasons)) if reasons else ("empty_candidate_input" if empty_input else "ok"),
        "as_of": d,
        "D": d,
        "run_id": run_id,
        "input": str(input_path),
        "outputs": {
            "csv": str(out_csv),
            "json": str(out_json),
            "latest_json": str(LATEST_JSON),
        },
        "input_rows": int(len(src)),
        "rows": int(len(preview)),
        "abstain_rows": int(preview["abstain"].astype(bool).sum()) if len(preview) else 0,
        "duplicate_d_code_horizon_rows": dup_count,
        "horizon_types": sorted(preview["horizon_type"].dropna().astype(str).unique().tolist()) if len(preview) and "horizon_type" in preview.columns else [],
        "model_meta": str(model_meta_path) if model_meta_path else "",
        "feature_meta": str(feature_meta_path) if feature_meta_path else "",
        "model_version": str(model_meta.get("model_version") or "NO_MODEL_PREVIEW_V0"),
        "model_meta_required_keys": MODEL_META_REQUIRED_KEYS,
        "feature_version": feature_version,
        "feature_asof": feature_asof,
        "required_columns": REQUIRED_COLUMNS,
        "policy": {
            "read_only_for_trading": True,
            "orders_modified": False,
            "fills_modified": False,
            "ledger_modified": False,
            "stats_modified": False,
            "gate_modified": False,
            "risk_lock_modified": False,
        },
    }
    _write_json(out_json, payload)
    _write_json(LATEST_JSON, payload)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Build read-only future signal preview artifacts.")
    ap.add_argument("--input", default=str(DEFAULT_INPUT))
    ap.add_argument("--output-dir", default=str(LOGS))
    ap.add_argument("--D", default="")
    ap.add_argument("--model-meta", default="")
    ap.add_argument("--feature-meta", default=str(LOGS / "future_signal_features_latest.json"))
    ap.add_argument("--horizon", type=int, default=5)
    args = ap.parse_args()

    model_meta_path = Path(args.model_meta) if str(args.model_meta or "").strip() else None
    feature_meta_path = Path(args.feature_meta) if str(args.feature_meta or "").strip() else None
    payload = build_preview(
        input_path=Path(args.input),
        output_dir=Path(args.output_dir),
        d_override=args.D,
        model_meta_path=model_meta_path,
        feature_meta_path=feature_meta_path,
        horizon=int(args.horizon),
    )
    print(
        f"[FUTURE_PREVIEW] status={payload.get('status')} reason={payload.get('reason')} "
        f"rows={payload.get('rows')} abstain_rows={payload.get('abstain_rows')} "
        f"csv={(payload.get('outputs') or {}).get('csv', '')}"
    )
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
