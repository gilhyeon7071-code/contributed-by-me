from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
KRX_ARCHIVE = ROOT / "krx_daily_archive"

DEFAULT_FEATURE_META = LOGS / "future_signal_features_latest.json"
LATEST_JSON = LOGS / "future_signal_moirai2_preview_latest.json"

MODEL_KIND = "moirai2"
MODEL_ID = "Salesforce/moirai-2.0-R-small"
MODEL_VERSION = "MOIRAI2_ZERO_SHOT_V0"
PREDICTION_LENGTH = 5
CONTEXT_LENGTH = 30
MIN_HISTORY_ROWS = 35
DEFAULT_MAX_CODES = 6

PREVIEW_COLUMNS = [
    "as_of",
    "run_id",
    "D",
    "code",
    "name",
    "horizon_type",
    "horizon",
    "horizon_days",
    "current_close",
    "pred_median_price",
    "expected_return",
    "downside_q10",
    "upside_q90",
    "confidence",
    "abstain",
    "reason",
    "model_version",
    "model_id",
    "model_kind",
    "forecast_class",
    "forecast_keys",
    "feature_version",
    "feature_asof",
    "history_rows",
]


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_safe(payload), ensure_ascii=True, indent=2), encoding="utf-8")


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path)


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _norm_date8(value: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(value or ""))
    return s[:8] if len(s) >= 8 else ""


def _norm_code(value: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(value or ""))
    return s.zfill(6)[-6:] if s else ""


def _module_available(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def _dependency_state() -> Dict[str, Any]:
    modules = {
        "uni2ts": _module_available("uni2ts"),
        "gluonts": _module_available("gluonts"),
        "torch": _module_available("torch"),
        "huggingface_hub": _module_available("huggingface_hub"),
    }
    missing = [k for k, ok in modules.items() if not ok]
    return {"available": not missing, "modules": modules, "missing": missing}


def _schema_sha(columns: List[str]) -> str:
    return hashlib.sha256("|".join(columns).encode("utf-8")).hexdigest()


def _file_sha16(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()[:16]
    except Exception:
        return ""


def _build_run_id(d: str, feature_csv: Path) -> str:
    digest = hashlib.sha256(f"{d}|{_file_sha16(feature_csv)}|{MODEL_VERSION}".encode("utf-8")).hexdigest()[:16]
    return f"FSM2_{d}_{digest}"


def _archive_files_until(d: str) -> List[Path]:
    files: List[Path] = []
    for path in KRX_ARCHIVE.glob("krx_daily_*_clean.parquet"):
        dates = re.findall(r"(\d{8})", path.name)
        if dates and min(dates) <= d:
            files.append(path)
    return sorted(files)


def _load_price_history(codes: List[str], d: str) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    code_set = set(codes)
    for path in _archive_files_until(d):
        df = pd.read_parquet(path, columns=["date", "code", "close"])
        df["code"] = df["code"].map(_norm_code)
        df["date"] = df["date"].map(_norm_date8)
        df = df[(df["date"] <= d) & (df["code"].isin(code_set))].copy()
        if len(df):
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["date", "code", "close"])
    out = pd.concat(frames, ignore_index=True)
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out = out.dropna(subset=["date", "code", "close"])
    out = out[out["close"] > 0].drop_duplicates(subset=["date", "code"], keep="last")
    return out.sort_values(["code", "date"])


def _forecast_one(
    *,
    module: Any,
    series: pd.DataFrame,
    code: str,
    context_length: int,
    prediction_length: int,
    batch_size: int,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    from gluonts.dataset.pandas import PandasDataset
    from uni2ts.model.moirai2 import Moirai2Forecast

    work = series[series["code"] == code][["date", "close"]].copy()
    work["date"] = pd.to_datetime(work["date"], format="%Y%m%d", errors="coerce")
    work = work.dropna(subset=["date", "close"]).sort_values("date")
    history_rows = int(len(work))
    if history_rows < MIN_HISTORY_ROWS:
        return {}, {"status": "FAIL", "reason": "INSUFFICIENT_HISTORY", "history_rows": history_rows}
    idx = pd.date_range(work["date"].min(), work["date"].max(), freq="B")
    work = work.set_index("date").reindex(idx).ffill().dropna(subset=["close"]).reset_index(names="date")
    model_rows = int(len(work))

    ds = PandasDataset({code: work.set_index("date")[["close"]].rename(columns={"close": code})}, target=code, freq="B")
    model = Moirai2Forecast(
        module=module,
        prediction_length=prediction_length,
        context_length=context_length,
        target_dim=1,
        feat_dynamic_real_dim=ds.num_feat_dynamic_real,
        past_feat_dynamic_real_dim=ds.num_past_feat_dynamic_real,
    )
    predictor = model.create_predictor(batch_size=batch_size)
    forecast = next(iter(predictor.predict(ds)))
    arr = getattr(forecast, "forecast_array", None)
    keys = list(getattr(forecast, "forecast_keys", []) or [])
    if arr is None or not keys:
        return {}, {"status": "FAIL", "reason": "FORECAST_QUANTILES_MISSING", "history_rows": history_rows}
    q = pd.DataFrame(arr, index=keys)
    current_close = float(work["close"].iloc[-1])
    q10 = float(q.loc["0.1"].iloc[prediction_length - 1]) if "0.1" in q.index else math.nan
    q50 = float(q.loc["0.5"].iloc[prediction_length - 1]) if "0.5" in q.index else float(q.median(axis=0).iloc[prediction_length - 1])
    q90 = float(q.loc["0.9"].iloc[prediction_length - 1]) if "0.9" in q.index else math.nan
    expected_return = (q50 / current_close) - 1.0 if current_close > 0 else math.nan
    downside_q10 = (q10 / current_close) - 1.0 if current_close > 0 and math.isfinite(q10) else math.nan
    upside_q90 = (q90 / current_close) - 1.0 if current_close > 0 and math.isfinite(q90) else math.nan
    spread = abs(upside_q90 - downside_q10) if math.isfinite(upside_q90) and math.isfinite(downside_q10) else math.nan
    confidence = max(0.0, min(0.60, 0.60 - spread)) if math.isfinite(spread) else 0.0
    return {
        "current_close": round(current_close, 6),
        "pred_median_price": round(q50, 6),
        "expected_return": round(expected_return, 8),
        "downside_q10": round(downside_q10, 8) if math.isfinite(downside_q10) else None,
        "upside_q90": round(upside_q90, 8) if math.isfinite(upside_q90) else None,
        "confidence": round(confidence, 6),
        "forecast_class": type(forecast).__name__,
        "forecast_keys": ",".join(keys),
        "history_rows": history_rows,
        "model_rows": model_rows,
    }, {"status": "PASS", "reason": "ok", "history_rows": history_rows}


def build_moirai2_preview(
    *,
    feature_meta_path: Path,
    output_dir: Path,
    max_codes: int,
    context_length: int,
    prediction_length: int,
    batch_size: int,
) -> Dict[str, Any]:
    generated_at = datetime.now().isoformat(timespec="seconds")
    feature_meta = _load_json(feature_meta_path)
    d = _norm_date8(feature_meta.get("D"))
    out_csv = output_dir / f"future_signal_moirai2_preview_{d or 'unknown'}.csv"
    out_json = output_dir / f"future_signal_moirai2_preview_{d or 'unknown'}.json"
    base_policy = {
        "read_only_for_trading": True,
        "trading_approved": False,
        "orders_modified": False,
        "fills_modified": False,
        "ledger_modified": False,
        "stats_modified": False,
        "gate_modified": False,
        "risk_lock_modified": False,
    }
    base = {
        "generated_at": generated_at,
        "D": d,
        "model_version": MODEL_VERSION,
        "model_id": MODEL_ID,
        "model_kind": MODEL_KIND,
        "feature_meta": str(feature_meta_path),
        "outputs": {"csv": str(out_csv), "json": str(out_json), "latest_json": str(LATEST_JSON)},
        "policy": base_policy,
    }

    reasons: List[str] = []
    if not feature_meta:
        reasons.append("FEATURE_META_MISSING")
    if feature_meta and str(feature_meta.get("status", "")).upper() != "PASS":
        reasons.append("FEATURE_META_NOT_PASS")
    feature_csv = Path(str((feature_meta.get("outputs") or {}).get("csv", ""))) if feature_meta else Path("")
    if not d:
        reasons.append("D_MISSING")
    if not feature_csv.exists():
        reasons.append("FEATURE_CSV_MISSING")

    deps = _dependency_state()
    if not deps["available"]:
        reasons.append("MOIRAI2_DEPENDENCY_MISSING:" + ",".join(deps["missing"]))
    if reasons:
        payload = {**base, "status": "FAIL", "reason": "|".join(reasons), "rows": 0, "dependency_state": deps}
        _write_json(out_json, payload)
        _write_json(LATEST_JSON, payload)
        return payload

    features = _read_csv(feature_csv)
    features["code"] = features["code"].map(_norm_code)
    features = features[features["code"].astype(str).str.len() == 6].head(max_codes).copy()
    codes = features["code"].dropna().astype(str).tolist()
    price_history = _load_price_history(codes, d)

    from uni2ts.model.moirai2 import Moirai2Module

    module = Moirai2Module.from_pretrained(MODEL_ID)
    run_id = _build_run_id(d, feature_csv)
    rows: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    as_of = datetime.now().isoformat(timespec="seconds")
    for _, feature in features.iterrows():
        code = str(feature.get("code", ""))
        name = str(feature.get("name", "") or "")
        try:
            values, meta = _forecast_one(
                module=module,
                series=price_history,
                code=code,
                context_length=context_length,
                prediction_length=prediction_length,
                batch_size=batch_size,
            )
        except Exception as exc:
            values, meta = {}, {"status": "FAIL", "reason": f"FORECAST_FAILED:{type(exc).__name__}:{str(exc)[:160]}"}
        if meta.get("status") != "PASS":
            failures.append({"code": code, "name": name, **meta})
            rows.append(
                {
                    "as_of": as_of,
                    "run_id": run_id,
                    "D": d,
                    "code": code,
                    "name": name,
                    "horizon_type": "SWING",
                    "horizon": prediction_length,
                    "horizon_days": prediction_length,
                    "current_close": feature.get("close", ""),
                    "pred_median_price": "",
                    "expected_return": "",
                    "downside_q10": "",
                    "upside_q90": "",
                    "confidence": 0.0,
                    "abstain": True,
                    "reason": str(meta.get("reason") or "FORECAST_FAILED"),
                    "model_version": MODEL_VERSION,
                    "model_id": MODEL_ID,
                    "model_kind": MODEL_KIND,
                    "forecast_class": "",
                    "forecast_keys": "",
                    "feature_version": str(feature_meta.get("feature_version") or ""),
                    "feature_asof": str(feature_meta.get("feature_asof") or d),
                    "history_rows": int(meta.get("history_rows") or 0),
                }
            )
            continue
        rows.append(
            {
                "as_of": as_of,
                "run_id": run_id,
                "D": d,
                "code": code,
                "name": name,
                "horizon_type": "SWING",
                "horizon": prediction_length,
                "horizon_days": prediction_length,
                **values,
                "abstain": False,
                "reason": "MOIRAI2_ZERO_SHOT",
                "model_version": MODEL_VERSION,
                "model_id": MODEL_ID,
                "model_kind": MODEL_KIND,
                "feature_version": str(feature_meta.get("feature_version") or ""),
                "feature_asof": str(feature_meta.get("feature_asof") or d),
            }
        )

    pred = pd.DataFrame(rows, columns=PREVIEW_COLUMNS)
    dup_count = int(pred.duplicated(subset=["D", "code", "horizon_type", "horizon"], keep=False).sum()) if len(pred) else 0
    status_reasons: List[str] = []
    if len(pred) == 0:
        status_reasons.append("PREDICTION_ROW_COUNT_ZERO")
    if dup_count:
        status_reasons.append("DUPLICATE_D_CODE_HORIZON")
    if len(pred) and len(failures) == len(pred):
        status_reasons.append("ALL_ROWS_ABSTAIN")
    status = "FAIL" if status_reasons else "PASS"
    _write_csv(out_csv, pred)
    payload = {
        **base,
        "status": status,
        "reason": "|".join(status_reasons) if status_reasons else "ok",
        "rows": int(len(pred)),
        "abstain_rows": int(pred["abstain"].astype(bool).sum()) if len(pred) else 0,
        "failure_rows": int(len(failures)),
        "duplicate_d_code_horizon_rows": dup_count,
        "dependency_state": deps,
        "feature_version": str(feature_meta.get("feature_version") or ""),
        "feature_asof": str(feature_meta.get("feature_asof") or d),
        "prediction_length": int(prediction_length),
        "context_length": int(context_length),
        "max_codes": int(max_codes),
        "columns": PREVIEW_COLUMNS,
        "prediction_schema_sha256": _schema_sha(PREVIEW_COLUMNS),
        "failures": failures[:20],
    }
    _write_json(out_json, payload)
    _write_json(LATEST_JSON, payload)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Build read-only Moirai2 future-signal shadow preview.")
    ap.add_argument("--feature-meta", default=str(DEFAULT_FEATURE_META))
    ap.add_argument("--output-dir", default=str(LOGS))
    ap.add_argument("--max-codes", type=int, default=DEFAULT_MAX_CODES)
    ap.add_argument("--context-length", type=int, default=CONTEXT_LENGTH)
    ap.add_argument("--prediction-length", type=int, default=PREDICTION_LENGTH)
    ap.add_argument("--batch-size", type=int, default=4)
    args = ap.parse_args()
    payload = build_moirai2_preview(
        feature_meta_path=Path(args.feature_meta),
        output_dir=Path(args.output_dir),
        max_codes=max(1, int(args.max_codes)),
        context_length=max(5, int(args.context_length)),
        prediction_length=max(1, int(args.prediction_length)),
        batch_size=max(1, int(args.batch_size)),
    )
    print(
        f"[FUTURE_MOIRAI2_PREVIEW] status={payload.get('status')} reason={payload.get('reason')} "
        f"D={payload.get('D')} rows={payload.get('rows')} abstain={payload.get('abstain_rows')}"
    )
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
