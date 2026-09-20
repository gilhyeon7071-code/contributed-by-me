from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
MODEL_DIR = ROOT / "_cache" / "future_signal"

DEFAULT_FEATURE_META = LOGS / "future_signal_features_latest.json"
LATEST_JSON = LOGS / "future_signal_baseline_latest.json"
LATEST_MODEL_META = MODEL_DIR / "future_signal_rule_baseline_model_meta_latest.json"

MODEL_VERSION = "RULE_BASELINE_V0"
MODEL_TYPE = "RULE_BASELINE"
CALIBRATION_VERSION = "RULE_BASELINE_UNCALIBRATED_V0"
TARGET = "heuristic_5d_trade_worth_proxy"
HORIZON_SPECS = [
    {"horizon_type": "INTRADAY", "horizon_days": 0, "return_clip": 0.015, "downside_base": 0.008, "weights": {"final_score": 0.16, "forecast_score": 0.08, "score": 0.10, "sector_score": 0.05, "flow_score": 0.10, "fundamental_score_01": 0.03, "execution_lob_score": 0.20, "news_score": 0.14, "rs": 0.04, "v_accel": 0.10}},
    {"horizon_type": "SHORT", "horizon_days": 2, "return_clip": 0.025, "downside_base": 0.010, "weights": {"final_score": 0.20, "forecast_score": 0.12, "score": 0.12, "sector_score": 0.08, "flow_score": 0.10, "fundamental_score_01": 0.05, "execution_lob_score": 0.12, "news_score": 0.09, "rs": 0.05, "v_accel": 0.07}},
    {"horizon_type": "SWING", "horizon_days": 5, "return_clip": 0.040, "downside_base": 0.012, "weights": {"final_score": 0.22, "forecast_score": 0.16, "score": 0.12, "sector_score": 0.10, "flow_score": 0.10, "fundamental_score_01": 0.08, "execution_lob_score": 0.07, "news_score": 0.05, "rs": 0.05, "v_accel": 0.05}},
    {"horizon_type": "MID", "horizon_days": 15, "return_clip": 0.070, "downside_base": 0.018, "weights": {"final_score": 0.24, "forecast_score": 0.18, "score": 0.10, "sector_score": 0.12, "flow_score": 0.07, "fundamental_score_01": 0.13, "execution_lob_score": 0.03, "news_score": 0.03, "rs": 0.06, "v_accel": 0.04}},
    {"horizon_type": "LONG", "horizon_days": 30, "return_clip": 0.100, "downside_base": 0.025, "weights": {"final_score": 0.24, "forecast_score": 0.18, "score": 0.08, "sector_score": 0.12, "flow_score": 0.05, "fundamental_score_01": 0.20, "execution_lob_score": 0.01, "news_score": 0.02, "rs": 0.06, "v_accel": 0.04}},
]

PREDICTION_COLUMNS = [
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


def _load_json(path: Optional[Path]) -> Dict[str, Any]:
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _norm_date8(v: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s[:8] if len(s) >= 8 else ""


def _clip(v: float, lo: float, hi: float) -> float:
    if math.isnan(v) or math.isinf(v):
        return lo
    return max(lo, min(hi, v))


def _num(row: pd.Series, col: str, default: float = 0.0) -> float:
    try:
        return float(row.get(col, default))
    except Exception:
        return default


def _schema_sha(columns: List[str]) -> str:
    return hashlib.sha256("|".join(columns).encode("utf-8")).hexdigest()


def _file_sha16(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()[:16]
    except Exception:
        return ""


def _score_row(row: pd.Series, spec: Dict[str, Any]) -> Dict[str, Any]:
    weights = spec.get("weights") if isinstance(spec.get("weights"), dict) else {}
    news_score = _clip(_num(row, "news_score"), 0.0, 1.0)
    news_weight = float(weights.get("news_score", 0.0) or 0.0)
    base = (
        _clip(_num(row, "final_score"), 0.0, 1.0) * float(weights.get("final_score", 0.0) or 0.0)
        + _clip(_num(row, "forecast_score"), 0.0, 1.0) * float(weights.get("forecast_score", 0.0) or 0.0)
        + _clip(_num(row, "score"), 0.0, 1.0) * float(weights.get("score", 0.0) or 0.0)
        + _clip(_num(row, "sector_score"), 0.0, 1.0) * float(weights.get("sector_score", 0.0) or 0.0)
        + _clip(_num(row, "flow_score"), 0.0, 1.0) * float(weights.get("flow_score", 0.0) or 0.0)
        + _clip(_num(row, "fundamental_score_01"), 0.0, 1.0) * float(weights.get("fundamental_score_01", 0.0) or 0.0)
        + _clip(_num(row, "execution_lob_score"), 0.0, 1.0) * float(weights.get("execution_lob_score", 0.0) or 0.0)
        + news_score * news_weight
        + _clip(_num(row, "rs"), 0.0, 2.0) / 2.0 * float(weights.get("rs", 0.0) or 0.0)
        + _clip(_num(row, "v_accel"), 0.0, 4.0) / 4.0 * float(weights.get("v_accel", 0.0) or 0.0)
    )
    atr = max(0.0, _num(row, "atr14_pct"))
    rsi = _num(row, "rsi14", 50.0)
    ret1 = _num(row, "ret1_pct")
    risk_penalty = 0.0
    if atr > 0.07:
        risk_penalty += min(0.12, (atr - 0.07) * 2.0)
    if rsi > 78.0:
        risk_penalty += min(0.08, (rsi - 78.0) / 100.0)
    if ret1 > 18.0:
        risk_penalty += min(0.08, (ret1 - 18.0) / 100.0)

    signal = _clip(base - risk_penalty, 0.0, 1.0)
    pred_up_prob = round(_clip(0.35 + signal * 0.30, 0.35, 0.65), 6)
    ret_clip = float(spec.get("return_clip", 0.04) or 0.04)
    downside_base = float(spec.get("downside_base", 0.012) or 0.012)
    expected_return = round(_clip((signal - 0.50) * ret_clip, -ret_clip / 2.0, ret_clip / 2.0), 6)
    downside_q10 = round(-_clip(downside_base + atr * 0.25 + risk_penalty * 0.08, downside_base, max(0.05, ret_clip)), 6)
    confidence = round(_clip(abs(signal - 0.50) * 1.7, 0.0, 0.60), 6)
    abstain = bool(confidence < 0.08)
    reason = "LOW_CONFIDENCE" if abstain else "BASELINE_HEURISTIC"
    return {
        "pred_up_prob": pred_up_prob,
        "expected_return": expected_return,
        "downside_q10": downside_q10,
        "confidence": confidence,
        "abstain": abstain,
        "reason": reason,
        "news_score": round(news_score, 6),
        "news_weight": round(news_weight, 6),
        "news_contribution": round(news_score * news_weight, 6),
    }


def build_baseline(*, feature_meta_path: Path, output_dir: Path, horizon: int = 5) -> Dict[str, Any]:
    reasons: List[str] = []
    feature_meta = _load_json(feature_meta_path)
    if not feature_meta:
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "status": "FAIL",
            "reason": "FEATURE_META_MISSING",
            "rows": 0,
            "outputs": {},
        }
    if str(feature_meta.get("status", "")).upper() != "PASS":
        reasons.append("FEATURE_META_NOT_PASS")
    feature_csv = Path(str((feature_meta.get("outputs") or {}).get("csv", "")))
    if not feature_csv.exists():
        reasons.append("FEATURE_CSV_MISSING")

    d = _norm_date8(feature_meta.get("D"))
    if not d:
        reasons.append("D_MISSING")

    if reasons:
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "status": "FAIL",
            "reason": "|".join(reasons),
            "D": d,
            "rows": 0,
            "outputs": {},
        }

    features = _read_csv(feature_csv)
    empty_input = int(feature_meta.get("rows") or 0) == 0 and len(features) == 0
    rows: List[Dict[str, Any]] = []
    for _, row in features.iterrows():
        for spec in HORIZON_SPECS:
            scored = _score_row(row, spec)
            horizon_days = int(spec.get("horizon_days", horizon))
            rows.append(
                {
                    "D": d,
                    "code": str(row.get("code", "")).zfill(6),
                    "name": str(row.get("name", "") or ""),
                    "horizon_type": str(spec.get("horizon_type") or f"D{horizon_days}"),
                    "horizon": horizon_days,
                    "horizon_days": horizon_days,
                    **scored,
                    "model_version": MODEL_VERSION,
                    "feature_version": str(feature_meta.get("feature_version") or ""),
                    "feature_asof": str(feature_meta.get("feature_asof") or d),
                }
            )

    pred = pd.DataFrame(rows, columns=PREDICTION_COLUMNS)
    dup_count = int(pred.duplicated(subset=["D", "code", "horizon"], keep=False).sum()) if len(pred) else 0
    if len(pred) == 0 and not empty_input:
        reasons.append("PREDICTION_ROW_COUNT_ZERO")
    if dup_count > 0:
        reasons.append("DUPLICATE_D_CODE_HORIZON")

    out_csv = output_dir / f"future_signal_baseline_{d}.csv"
    out_json = output_dir / f"future_signal_baseline_{d}.json"
    model_artifact = MODEL_DIR / f"future_signal_rule_baseline_model_{d}.json"
    calibration_artifact = MODEL_DIR / f"future_signal_rule_baseline_calibration_{d}.json"
    model_meta_artifact = MODEL_DIR / f"future_signal_rule_baseline_model_meta_{d}.json"

    _write_csv(out_csv, pred)

    model_payload = {
        "model_version": MODEL_VERSION,
        "model_type": MODEL_TYPE,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "description": "Read-only multi-horizon heuristic baseline for preview comparison only.",
        "trading_approved": False,
        "horizon_specs": HORIZON_SPECS,
    }
    calibration_payload = {
        "calibration_version": CALIBRATION_VERSION,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "calibrated": False,
        "reason": "RULE_BASELINE_HEURISTIC_ONLY",
        "trading_approved": False,
    }
    _write_json(model_artifact, model_payload)
    _write_json(calibration_artifact, calibration_payload)

    model_meta_payload = {
        "model_version": MODEL_VERSION,
        "model_type": MODEL_TYPE,
        "model_path": str(model_artifact),
        "feature_version": str(feature_meta.get("feature_version") or ""),
        "feature_schema_sha256": str(feature_meta.get("feature_schema_sha256") or ""),
        "train_window_start": d,
        "train_window_end": d,
        "calibration_version": CALIBRATION_VERSION,
        "calibration_path": str(calibration_artifact),
        "target": TARGET,
        "horizon": int(horizon),
        "horizon_specs": HORIZON_SPECS,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "prediction_path": str(out_csv),
        "trading_approved": False,
    }
    _write_json(model_meta_artifact, model_meta_payload)
    _write_json(LATEST_MODEL_META, model_meta_payload)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "FAIL" if reasons else "PASS",
        "reason": "|".join(reasons) if reasons else ("empty_candidate_input" if empty_input else "ok"),
        "D": d,
        "horizon": int(horizon),
        "horizon_specs": HORIZON_SPECS,
        "input_rows": int(len(features)),
        "rows": int(len(pred)),
        "abstain_rows": int(pred["abstain"].astype(bool).sum()) if len(pred) else 0,
        "duplicate_d_code_horizon_rows": dup_count,
        "model_version": MODEL_VERSION,
        "model_type": MODEL_TYPE,
        "feature_version": str(feature_meta.get("feature_version") or ""),
        "feature_schema_sha256": str(feature_meta.get("feature_schema_sha256") or ""),
        "calibration_version": CALIBRATION_VERSION,
        "calibrated": False,
        "trading_approved": False,
        "outputs": {
            "csv": str(out_csv),
            "json": str(out_json),
            "latest_json": str(LATEST_JSON),
            "model_artifact": str(model_artifact),
            "calibration_artifact": str(calibration_artifact),
            "model_meta": str(model_meta_artifact),
            "model_meta_latest": str(LATEST_MODEL_META),
        },
        "prediction_schema_sha256": _schema_sha(PREDICTION_COLUMNS),
        "columns": PREDICTION_COLUMNS,
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
    ap = argparse.ArgumentParser(description="Build read-only future signal rule baseline predictions.")
    ap.add_argument("--feature-meta", default=str(DEFAULT_FEATURE_META))
    ap.add_argument("--output-dir", default=str(LOGS))
    ap.add_argument("--horizon", type=int, default=5)
    args = ap.parse_args()
    payload = build_baseline(feature_meta_path=Path(args.feature_meta), output_dir=Path(args.output_dir), horizon=int(args.horizon))
    print(
        f"[FUTURE_BASELINE] status={payload.get('status')} reason={payload.get('reason')} "
        f"rows={payload.get('rows')} abstain_rows={payload.get('abstain_rows')} "
        f"csv={(payload.get('outputs') or {}).get('csv', '')}"
    )
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
