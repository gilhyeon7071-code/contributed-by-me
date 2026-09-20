from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from tools.future_signal_prob_metrics import score_sample_frame


ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
LATEST_JSON = LOGS / "moirai_uni2ts_smoke_latest.json"

DEFAULT_MODEL_ID = "Salesforce/moirai-2.0-R-small"
DEFAULT_MODEL_KIND = "moirai2"
DEFAULT_CONTEXT_LENGTH = 200
DEFAULT_PATCH_SIZE = "auto"
DEFAULT_NUM_SAMPLES = 100
DEFAULT_PREDICTION_LENGTH = 5
DEFAULT_PIT_MIN_PVALUE = 0.001


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_safe(payload), ensure_ascii=True, indent=2), encoding="utf-8")


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path)


def _module_available(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def _dependency_state() -> Dict[str, Any]:
    modules = {
        "uni2ts": _module_available("uni2ts"),
        "gluonts": _module_available("gluonts"),
        "torch": _module_available("torch"),
        "huggingface_hub": _module_available("huggingface_hub"),
    }
    missing = [name for name, ok in modules.items() if not ok]
    return {
        "available": len(missing) == 0,
        "modules": modules,
        "missing": missing,
    }


def _fingerprint(samples: pd.DataFrame) -> str:
    cols = [c for c in ["row_id", "sample_index", "step", "value"] if c in samples.columns]
    if not cols:
        return ""
    stable = samples[cols].sort_values(cols).to_csv(index=False)
    return hashlib.sha256(stable.encode("utf-8")).hexdigest()[:16]


def _shape_checks(samples: pd.DataFrame, *, num_samples: int, prediction_length: int) -> Dict[str, Any]:
    required = {"row_id", "sample_index", "step", "value"}
    missing = sorted(required - set(samples.columns))
    if missing:
        return {"status": "FAIL", "reason": "SAMPLES_REQUIRED_COLUMN_MISSING", "missing": missing}
    work = samples.copy()
    work["row_id"] = work["row_id"].astype(str)
    work["sample_index"] = pd.to_numeric(work["sample_index"], errors="coerce")
    work["step"] = pd.to_numeric(work["step"], errors="coerce")
    bad_groups: List[Dict[str, Any]] = []
    for row_id, g in work.groupby("row_id", dropna=False):
        sample_count = int(g["sample_index"].nunique(dropna=True))
        step_count = int(g["step"].nunique(dropna=True))
        if sample_count != int(num_samples) or step_count != int(prediction_length):
            bad_groups.append(
                {
                    "row_id": str(row_id),
                    "sample_count": sample_count,
                    "step_count": step_count,
                }
            )
    return {
        "status": "PASS" if not bad_groups else "FAIL",
        "reason": "ok" if not bad_groups else "FORECAST_SAMPLE_SHAPE_MISMATCH",
        "row_ids": int(work["row_id"].nunique(dropna=True)),
        "expected": {
            "num_samples": int(num_samples),
            "prediction_length": int(prediction_length),
        },
        "bad_groups": bad_groups[:20],
    }


def _offline_sample_frames(*, num_samples: int, prediction_length: int, row_count: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    sample_rows: List[Dict[str, Any]] = []
    actual_rows: List[Dict[str, Any]] = []
    denom = max(1, row_count * prediction_length)
    for row_idx in range(row_count):
        row_id = f"offline_{row_idx:03d}"
        for step in range(prediction_length):
            q = ((row_idx * prediction_length + step) + 0.5) / denom
            center = -0.02 + 0.04 * q
            actual = center + (q - 0.5) * 0.08
            actual_rows.append({"row_id": row_id, "step": step, "actual": actual})
            for sample_idx in range(num_samples):
                centered = ((sample_idx + 0.5) / num_samples) - 0.5
                value = center + centered * 0.08
                sample_rows.append(
                    {
                        "row_id": row_id,
                        "sample_index": sample_idx,
                        "step": step,
                        "value": value,
                    }
                )
    return pd.DataFrame(sample_rows), pd.DataFrame(actual_rows)


def _load_wide_series(path: Path, date_column: str, value_column: str, series_id: str) -> pd.DataFrame:
    df = _read_csv(path)
    if date_column not in df.columns or value_column not in df.columns:
        raise ValueError("SERIES_REQUIRED_COLUMN_MISSING")
    out = df[[date_column, value_column]].copy()
    out[date_column] = pd.to_datetime(out[date_column], errors="coerce")
    out[value_column] = pd.to_numeric(out[value_column], errors="coerce")
    out = out.dropna(subset=[date_column, value_column]).sort_values(date_column)
    if len(out) == 0:
        raise ValueError("SERIES_VALID_ROW_ZERO")
    return out.set_index(date_column).rename(columns={value_column: series_id})


def _extract_forecast_samples(forecast: Any, *, num_samples: int, prediction_length: int) -> List[List[float]]:
    raw = getattr(forecast, "samples", None)
    if raw is None:
        raw = getattr(forecast, "forecast_array", None)
    if raw is None:
        raise ValueError("FORECAST_SAMPLES_MISSING")
    rows = raw.tolist() if hasattr(raw, "tolist") else raw
    if not isinstance(rows, list) or len(rows) == 0:
        raise ValueError("FORECAST_SAMPLES_EMPTY")
    if rows and isinstance(rows[0], (int, float)):
        rows = [rows]
    out: List[List[float]] = []
    for row in rows[:num_samples]:
        if not isinstance(row, list):
            continue
        vals = [float(x) for x in row[:prediction_length]]
        if len(vals) == prediction_length:
            out.append(vals)
    if len(out) == 0:
        raise ValueError("FORECAST_SAMPLES_EMPTY")
    return out


def _run_uni2ts_inference(
    *,
    series_csv: Path,
    date_column: str,
    value_column: str,
    series_id: str,
    model_kind: str,
    model_id: str,
    context_length: int,
    patch_size: str,
    batch_size: int,
    num_samples: int,
    prediction_length: int,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    from gluonts.dataset.pandas import PandasDataset
    from gluonts.dataset.split import split

    if model_kind == "moirai":
        from uni2ts.model.moirai import MoiraiForecast, MoiraiModule

        module = MoiraiModule.from_pretrained(model_id)
        ds = PandasDataset(dict(_load_wide_series(series_csv, date_column, value_column, series_id)), target=series_id)
        model = MoiraiForecast(
            module=module,
            prediction_length=prediction_length,
            context_length=context_length,
            patch_size=patch_size,
            num_samples=num_samples,
            target_dim=1,
            feat_dynamic_real_dim=ds.num_feat_dynamic_real,
            past_feat_dynamic_real_dim=ds.num_past_feat_dynamic_real,
        )
    elif model_kind == "moirai2":
        from uni2ts.model.moirai2 import Moirai2Forecast, Moirai2Module

        module = Moirai2Module.from_pretrained(model_id)
        ds = PandasDataset(dict(_load_wide_series(series_csv, date_column, value_column, series_id)), target=series_id)
        model = Moirai2Forecast(
            module=module,
            prediction_length=prediction_length,
            context_length=context_length,
            target_dim=1,
            feat_dynamic_real_dim=ds.num_feat_dynamic_real,
            past_feat_dynamic_real_dim=ds.num_past_feat_dynamic_real,
        )
    else:
        raise ValueError("UNSUPPORTED_MODEL_KIND")

    _, test_template = split(ds, offset=-prediction_length)
    test_data = test_template.generate_instances(
        prediction_length=prediction_length,
        windows=1,
        distance=prediction_length,
    )
    predictor = model.create_predictor(batch_size=batch_size)
    forecast = next(iter(predictor.predict(test_data.input)))
    label = next(iter(test_data.label))
    samples = _extract_forecast_samples(forecast, num_samples=num_samples, prediction_length=prediction_length)

    sample_rows: List[Dict[str, Any]] = []
    actual_rows: List[Dict[str, Any]] = []
    row_id = str(series_id)
    for sample_index, values in enumerate(samples):
        for step, value in enumerate(values):
            sample_rows.append(
                {
                    "row_id": row_id,
                    "sample_index": sample_index,
                    "step": step,
                    "value": float(value),
                }
            )

    label_target = label.get("target") if isinstance(label, dict) else None
    label_values = label_target.tolist() if hasattr(label_target, "tolist") else label_target
    if isinstance(label_values, list):
        for step, actual in enumerate(label_values[:prediction_length]):
            actual_rows.append({"row_id": row_id, "step": step, "actual": float(actual)})

    meta = {
        "series_csv": str(series_csv),
        "series_id": row_id,
        "forecast_class": type(forecast).__name__,
        "forecast_keys": list(getattr(forecast, "forecast_keys", []) or []),
        "effective_num_samples": len(samples),
        "actual_rows": len(actual_rows),
        "sample_rows": len(sample_rows),
    }
    return pd.DataFrame(sample_rows), pd.DataFrame(actual_rows), meta


def build_smoke_payload(
    *,
    output_dir: Path,
    samples_csv: Path | None,
    actuals_csv: Path | None,
    series_csv: Path | None,
    date_column: str,
    value_column: str,
    series_id: str,
    offline_self_test: bool,
    model_kind: str,
    model_id: str,
    context_length: int,
    patch_size: str,
    batch_size: int,
    num_samples: int,
    prediction_length: int,
    pit_min_pvalue: float,
) -> Dict[str, Any]:
    generated_at = datetime.now().isoformat(timespec="seconds")
    out_json = output_dir / f"moirai_uni2ts_smoke_{generated_at.replace(':', '').replace('-', '')}.json"
    dependency = _dependency_state()

    base: Dict[str, Any] = {
        "generated_at": generated_at,
        "model_family": "MOIRAI_UNI2TS",
        "model_kind": model_kind,
        "model_id": model_id,
        "params": {
            "context_length": int(context_length),
            "patch_size": patch_size,
            "batch_size": int(batch_size),
            "num_samples": int(num_samples),
            "prediction_length": int(prediction_length),
            "pit_min_pvalue": float(pit_min_pvalue),
        },
        "dependency_state": dependency,
        "policy": {
            "read_only_for_trading": True,
            "orders_modified": False,
            "fills_modified": False,
            "ledger_modified": False,
            "stats_modified": False,
            "gate_modified": False,
            "risk_lock_modified": False,
            "trading_approved": False,
        },
        "outputs": {
            "json": str(out_json),
            "latest_json": str(LATEST_JSON),
        },
    }

    if samples_csv and actuals_csv:
        samples = _read_csv(samples_csv)
        actuals = _read_csv(actuals_csv)
        inference_meta: Dict[str, Any] = {}
        input_mode = "PRECOMPUTED_SAMPLES"
    elif series_csv:
        if not dependency["available"]:
            return {
                **base,
                "status": "FAIL",
                "smoke_state": "BLOCKED",
                "reason": "UNI2TS_RUNTIME_DEPENDENCY_MISSING",
                "missing_dependencies": dependency["missing"],
                "input_mode": "SERIES_INFERENCE",
            }
        try:
            samples, actuals, inference_meta = _run_uni2ts_inference(
                series_csv=series_csv,
                date_column=date_column,
                value_column=value_column,
                series_id=series_id,
                model_kind=model_kind,
                model_id=model_id,
                context_length=int(context_length),
                patch_size=patch_size,
                batch_size=int(batch_size),
                num_samples=int(num_samples),
                prediction_length=int(prediction_length),
            )
        except Exception as exc:
            return {
                **base,
                "status": "FAIL",
                "smoke_state": "BLOCKED",
                "reason": f"UNI2TS_INFERENCE_FAILED:{type(exc).__name__}:{str(exc)[:200]}",
                "input_mode": "SERIES_INFERENCE",
            }
        input_mode = "SERIES_INFERENCE"
    elif offline_self_test:
        samples, actuals = _offline_sample_frames(
            num_samples=int(num_samples),
            prediction_length=int(prediction_length),
            row_count=20,
        )
        inference_meta = {}
        input_mode = "OFFLINE_SELF_TEST"
    else:
        if not dependency["available"]:
            return {
                **base,
                "status": "FAIL",
                "smoke_state": "BLOCKED",
                "reason": "UNI2TS_RUNTIME_DEPENDENCY_MISSING",
                "missing_dependencies": dependency["missing"],
                "input_mode": "DEPENDENCY_CHECK_ONLY",
            }
        return {
            **base,
            "status": "FAIL",
            "smoke_state": "BLOCKED",
            "reason": "MOIRAI_INFERENCE_ADAPTER_NOT_ENABLED",
            "input_mode": "DEPENDENCY_CHECK_ONLY",
        }

    shape_num_samples = int((inference_meta or {}).get("effective_num_samples") or num_samples)
    shape = _shape_checks(samples, num_samples=shape_num_samples, prediction_length=int(prediction_length))
    metrics = score_sample_frame(samples, actuals)
    pit_p = ((metrics.get("pit_ks") or {}).get("p_value")) if isinstance(metrics.get("pit_ks"), dict) else None
    crps_ok = metrics.get("mean_crps") is not None
    pit_ok = pit_p is not None and float(pit_p) > float(pit_min_pvalue)
    shape_ok = shape.get("status") == "PASS"
    seed_fingerprint = _fingerprint(samples)
    status = "PASS" if shape_ok and crps_ok and pit_ok and seed_fingerprint else "FAIL"
    failed = []
    if not shape_ok:
        failed.append("shape")
    if not crps_ok:
        failed.append("crps")
    if not pit_ok:
        failed.append("pit_ks")
    if not seed_fingerprint:
        failed.append("seed_fingerprint")

    return {
        **base,
        "status": status,
        "smoke_state": "EVALUATED" if status == "PASS" else "FAILED",
        "reason": "ok" if status == "PASS" else "|".join(failed),
        "input_mode": input_mode,
        "shape_check": shape,
        "metrics": metrics,
        "seed_fingerprint": seed_fingerprint,
        "inference_meta": inference_meta,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Run read-only Moirai/Uni2TS probabilistic forecast smoke checks.")
    ap.add_argument("--output-dir", default=str(LOGS))
    ap.add_argument("--samples-csv", default="")
    ap.add_argument("--actuals-csv", default="")
    ap.add_argument("--series-csv", default="")
    ap.add_argument("--date-column", default="date")
    ap.add_argument("--value-column", default="value")
    ap.add_argument("--series-id", default="series_0")
    ap.add_argument("--offline-self-test", action="store_true")
    ap.add_argument("--model-kind", choices=["moirai", "moirai2"], default=DEFAULT_MODEL_KIND)
    ap.add_argument("--model-id", default=DEFAULT_MODEL_ID)
    ap.add_argument("--context-length", type=int, default=DEFAULT_CONTEXT_LENGTH)
    ap.add_argument("--patch-size", default=DEFAULT_PATCH_SIZE)
    ap.add_argument("--batch-size", type=int, default=32)
    ap.add_argument("--num-samples", type=int, default=DEFAULT_NUM_SAMPLES)
    ap.add_argument("--prediction-length", type=int, default=DEFAULT_PREDICTION_LENGTH)
    ap.add_argument("--pit-min-pvalue", type=float, default=DEFAULT_PIT_MIN_PVALUE)
    args = ap.parse_args()

    payload = build_smoke_payload(
        output_dir=Path(args.output_dir),
        samples_csv=Path(args.samples_csv) if args.samples_csv else None,
        actuals_csv=Path(args.actuals_csv) if args.actuals_csv else None,
        series_csv=Path(args.series_csv) if args.series_csv else None,
        date_column=str(args.date_column),
        value_column=str(args.value_column),
        series_id=str(args.series_id),
        offline_self_test=bool(args.offline_self_test),
        model_kind=str(args.model_kind),
        model_id=str(args.model_id),
        context_length=max(1, int(args.context_length)),
        patch_size=str(args.patch_size),
        batch_size=max(1, int(args.batch_size)),
        num_samples=max(1, int(args.num_samples)),
        prediction_length=max(1, int(args.prediction_length)),
        pit_min_pvalue=float(args.pit_min_pvalue),
    )
    out_json = Path(str((payload.get("outputs") or {}).get("json") or LOGS / "moirai_uni2ts_smoke_unknown.json"))
    _write_json(out_json, payload)
    _write_json(LATEST_JSON, payload)
    print(
        f"[MOIRAI_UNI2TS_SMOKE] status={payload.get('status')} "
        f"state={payload.get('smoke_state')} reason={payload.get('reason')}"
    )
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
