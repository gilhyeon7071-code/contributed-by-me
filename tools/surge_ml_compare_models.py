from __future__ import annotations

import csv
import importlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
OUTPUT_JSON = LOG_DIR / "surge_ml_compare_models_latest.json"
OUTPUT_CSV = LOG_DIR / "surge_ml_compare_models_latest.csv"
TRAIN_STATUS_PATH = LOG_DIR / "surge_ml_train_status_latest.json"
MODEL_PATH = ROOT / "_cache" / "surge_ml_model.pkl"

surge_train = importlib.import_module("tools.surge_ml_train")


def _feature_columns() -> List[str]:
    return [
        "ret1",
        "ret3",
        "ret5",
        "ret10",
        "range_pct",
        "body_pct",
        "upper_shadow_pct",
        "vol_ratio20",
        "vol_ratio5",
        "atr14_pct",
        "close_to_high_20d",
    ]


def _optional_import(module_name: str) -> Tuple[Any | None, str | None]:
    try:
        return importlib.import_module(module_name), None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _prepare_dataset() -> Tuple[pd.DataFrame, List[str], Dict[str, Any]]:
    raw_params = surge_train._load_surge_params()
    label_thr = surge_train._load_label_thr_from_params()
    label_config = surge_train._load_label_config_from_params(label_thr)
    px = surge_train._load_all_prices()
    df = surge_train._build_features(px, label_thr, label_config)

    features = _feature_columns()
    df[features] = df[features].apply(lambda s: pd.to_numeric(s, errors="coerce"))
    df[features] = df[features].where(np.isfinite(df[features]), np.nan)
    train_df = df.dropna(subset=features + ["label_surge"]).copy()

    eval_start_ymd = str(
        os.getenv("SURGE_EVAL_START_YMD", surge_train.DEFAULT_EVAL_START_YMD)
        or surge_train.DEFAULT_EVAL_START_YMD
    ).strip()
    if eval_start_ymd:
        train_df = train_df[train_df["date"].astype(str) >= eval_start_ymd].copy()

    min_rows = int(surge_train._to_float_env("SURGE_ML_MIN_ROWS", 2000))
    if len(train_df) < min_rows:
        raise RuntimeError(f"INSUFFICIENT_ROWS: rows={len(train_df)} min_rows={min_rows}")

    metadata = {
        "raw_params": raw_params,
        "label_thr_ret5": float(label_thr),
        "label_config": label_config,
        "eval_start_ymd": eval_start_ymd,
        "eval_end_ymd": str(train_df["date"].astype(str).max()) if len(train_df) else "",
        "rows_total": int(len(train_df)),
    }
    return train_df, features, metadata


def _main_split(train_df: pd.DataFrame, metadata: Dict[str, Any]) -> Dict[str, Any]:
    dates = sorted(train_df["date"].astype(str).unique().tolist())
    raw_params = metadata.get("raw_params") or {}
    label_config = metadata.get("label_config") or {}
    embargo_days = int(
        surge_train._to_float_env(
            "SURGE_ML_EMBARGO_DAYS",
            float(raw_params.get("ml_embargo_days", label_config.get("horizon_days", 5)) or label_config.get("horizon_days", 5)),
        )
    )
    split_idx = max(1, int(len(dates) * 0.8))
    split_date = str(dates[split_idx - 1])
    valid_start_idx = min(len(dates), split_idx + max(0, int(embargo_days)))
    valid_start_date = str(dates[valid_start_idx]) if valid_start_idx < len(dates) else ""
    tr = train_df[train_df["date"].astype(str) <= split_date].copy()
    va = train_df[train_df["date"].astype(str) >= valid_start_date].copy() if valid_start_date else pd.DataFrame()
    if len(va) == 0:
        va = tr.tail(min(1000, len(tr))).copy()
    return {
        "dates": dates,
        "train": tr,
        "valid": va,
        "split_date": split_date,
        "valid_start_date": valid_start_date,
        "embargo_days": int(max(0, embargo_days)),
    }


def _fit_sklearn_hgb(X_tr: pd.DataFrame, y_tr: pd.Series, use_balanced: bool) -> Any:
    from sklearn.ensemble import HistGradientBoostingClassifier

    model = HistGradientBoostingClassifier(
        learning_rate=0.05,
        max_depth=5,
        max_iter=int(surge_train._to_float_env("SURGE_ML_COMPARE_MAX_ITER", 200)),
        min_samples_leaf=50,
        random_state=42,
    )
    if use_balanced:
        from sklearn.utils.class_weight import compute_sample_weight

        model.fit(X_tr, y_tr, sample_weight=compute_sample_weight(class_weight="balanced", y=y_tr))
    else:
        model.fit(X_tr, y_tr)
    return model


def _fit_lightgbm(X_tr: pd.DataFrame, y_tr: pd.Series, use_balanced: bool) -> Any:
    lightgbm, err = _optional_import("lightgbm")
    if lightgbm is None:
        raise ImportError(err or "lightgbm is not installed")
    model = lightgbm.LGBMClassifier(
        n_estimators=int(surge_train._to_float_env("SURGE_ML_COMPARE_LGB_ESTIMATORS", 400)),
        learning_rate=surge_train._to_float_env("SURGE_ML_COMPARE_LGB_LR", 0.05),
        max_depth=int(surge_train._to_float_env("SURGE_ML_COMPARE_LGB_MAX_DEPTH", 5)),
        num_leaves=int(surge_train._to_float_env("SURGE_ML_COMPARE_LGB_NUM_LEAVES", 31)),
        min_child_samples=50,
        subsample=0.9,
        colsample_bytree=0.9,
        random_state=42,
        class_weight="balanced" if use_balanced else None,
        verbosity=-1,
    )
    model.fit(X_tr, y_tr)
    return model


def _fit_xgboost(X_tr: pd.DataFrame, y_tr: pd.Series, use_balanced: bool) -> Any:
    xgboost, err = _optional_import("xgboost")
    if xgboost is None:
        raise ImportError(err or "xgboost is not installed")
    pos = float((y_tr == 1).sum())
    neg = float((y_tr == 0).sum())
    scale_pos_weight = max(1.0, neg / max(1.0, pos)) if use_balanced else 1.0
    model = xgboost.XGBClassifier(
        n_estimators=int(surge_train._to_float_env("SURGE_ML_COMPARE_XGB_ESTIMATORS", 400)),
        learning_rate=surge_train._to_float_env("SURGE_ML_COMPARE_XGB_LR", 0.05),
        max_depth=int(surge_train._to_float_env("SURGE_ML_COMPARE_XGB_MAX_DEPTH", 5)),
        subsample=0.9,
        colsample_bytree=0.9,
        objective="binary:logistic",
        eval_metric="logloss",
        tree_method="hist",
        random_state=42,
        n_jobs=int(surge_train._to_float_env("SURGE_ML_COMPARE_XGB_N_JOBS", 2)),
        scale_pos_weight=scale_pos_weight,
    )
    model.fit(X_tr, y_tr)
    return model


def _predict_proba(model: Any, X_va: pd.DataFrame) -> np.ndarray:
    if hasattr(model, "predict_proba"):
        return np.asarray(model.predict_proba(X_va)[:, 1], dtype=float)
    pred = np.asarray(model.predict(X_va), dtype=float)
    return np.clip(pred, 0.0, 1.0)


def _score_probs(y_true: pd.Series, probs: np.ndarray, top_k: int) -> Dict[str, Any]:
    return {
        "valid_auc": surge_train._roc_auc_safe(y_true, probs),
        "valid_pr_auc": surge_train._pr_auc_safe(y_true, probs),
        "precision_at_k": surge_train._precision_at_k_safe(y_true, probs, top_k),
        "precision_top_k": int(min(max(1, top_k), len(y_true))) if len(y_true) else int(top_k),
        "valid_pos_rate": float(y_true.astype(int).mean()) if len(y_true) else 0.0,
    }


def _walk_forward_for_model(
    model_name: str,
    fit_fn: Callable[[pd.DataFrame, pd.Series, bool], Any],
    train_df: pd.DataFrame,
    dates: List[str],
    features: List[str],
    use_balanced: bool,
    top_k: int,
    embargo_days: int,
) -> Dict[str, Any]:
    if str(os.getenv("SURGE_ML_WALK_FORWARD", "1") or "1").strip().lower() in {"0", "false", "no", "off"}:
        return {"enabled": False, "reason": "disabled"}
    folds = surge_train._walk_forward_splits(
        dates,
        folds=int(surge_train._to_float_env("SURGE_ML_WF_FOLDS", 3)),
        embargo_days=embargo_days,
    )
    if not folds:
        return {"enabled": True, "status": "SKIP", "reason": "insufficient_dates", "folds": [], "embargo_days": int(embargo_days)}
    max_rows = int(surge_train._to_float_env("SURGE_ML_WF_MAX_TRAIN_ROWS", 250000))
    rows: List[Dict[str, Any]] = []
    for spec in folds:
        tr = train_df[train_df["date"].astype(str) <= spec["train_end"]].copy()
        va = train_df[(train_df["date"].astype(str) >= spec["valid_start"]) & (train_df["date"].astype(str) <= spec["valid_end"])].copy()
        if len(tr) == 0 or len(va) == 0:
            continue
        if max_rows > 0 and len(tr) > max_rows:
            tr = tr.sort_values("date").tail(max_rows).copy()
        X_tr = tr[features].astype(float)
        y_tr = tr["label_surge"].astype(int)
        X_va = va[features].astype(float)
        y_va = va["label_surge"].astype(int)
        model = fit_fn(X_tr, y_tr, use_balanced)
        probs = _predict_proba(model, X_va)
        rows.append(
            {
                **spec,
                "rows_train": int(len(tr)),
                "rows_valid": int(len(va)),
                **_score_probs(y_va, probs, top_k),
                "trainer": model_name,
            }
        )
    aucs = [float(r["valid_auc"]) for r in rows if r.get("valid_auc") is not None]
    pr_aucs = [float(r["valid_pr_auc"]) for r in rows if r.get("valid_pr_auc") is not None]
    p_at_ks = [float(r["precision_at_k"]) for r in rows if r.get("precision_at_k") is not None]
    return {
        "enabled": True,
        "status": "OK" if aucs else "WARN",
        "folds": rows,
        "fold_count": int(len(rows)),
        "median_auc": float(np.median(aucs)) if aucs else None,
        "median_pr_auc": float(np.median(pr_aucs)) if pr_aucs else None,
        "median_precision_at_k": float(np.median(p_at_ks)) if p_at_ks else None,
        "precision_top_k": int(top_k),
        "embargo_days": int(embargo_days),
        "min_auc": float(np.min(aucs)) if aucs else None,
        "max_train_rows": int(max_rows),
    }


def _run_model(
    model_name: str,
    fit_fn: Callable[[pd.DataFrame, pd.Series, bool], Any],
    split: Dict[str, Any],
    train_df: pd.DataFrame,
    features: List[str],
    use_balanced: bool,
    top_k: int,
) -> Dict[str, Any]:
    try:
        tr = split["train"]
        va = split["valid"]
        X_tr = tr[features].astype(float)
        y_tr = tr["label_surge"].astype(int)
        X_va = va[features].astype(float)
        y_va = va["label_surge"].astype(int)
        model = fit_fn(X_tr, y_tr, use_balanced)
        probs = _predict_proba(model, X_va)
        walk_forward = _walk_forward_for_model(
            model_name,
            fit_fn,
            train_df,
            split["dates"],
            features,
            use_balanced,
            top_k,
            int(split["embargo_days"]),
        )
        return {
            "model": model_name,
            "status": "OK",
            "skip_reason": "",
            "rows_train": int(len(tr)),
            "rows_valid": int(len(va)),
            **_score_probs(y_va, probs, top_k),
            "walk_forward": walk_forward,
        }
    except ImportError as exc:
        return {"model": model_name, "status": "SKIP", "skip_reason": str(exc)}
    except Exception as exc:
        return {"model": model_name, "status": "ERROR", "skip_reason": f"{type(exc).__name__}: {exc}"}


def _write_csv(rows: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "model",
        "status",
        "skip_reason",
        "valid_auc",
        "valid_pr_auc",
        "precision_at_k",
        "precision_top_k",
        "valid_pos_rate",
        "walk_forward_status",
        "wf_median_auc",
        "wf_min_auc",
        "wf_median_pr_auc",
        "wf_median_precision_at_k",
        "rows_train",
        "rows_valid",
    ]
    with OUTPUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            wf = row.get("walk_forward") or {}
            writer.writerow(
                {
                    "model": row.get("model"),
                    "status": row.get("status"),
                    "skip_reason": row.get("skip_reason", ""),
                    "valid_auc": row.get("valid_auc"),
                    "valid_pr_auc": row.get("valid_pr_auc"),
                    "precision_at_k": row.get("precision_at_k"),
                    "precision_top_k": row.get("precision_top_k"),
                    "valid_pos_rate": row.get("valid_pos_rate"),
                    "walk_forward_status": wf.get("status"),
                    "wf_median_auc": wf.get("median_auc"),
                    "wf_min_auc": wf.get("min_auc"),
                    "wf_median_pr_auc": wf.get("median_pr_auc"),
                    "wf_median_precision_at_k": wf.get("median_precision_at_k"),
                    "rows_train": row.get("rows_train"),
                    "rows_valid": row.get("rows_valid"),
                }
            )


def main() -> int:
    ts = datetime.now().isoformat(timespec="seconds")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    train_status_mtime_before = TRAIN_STATUS_PATH.stat().st_mtime if TRAIN_STATUS_PATH.exists() else None
    model_mtime_before = MODEL_PATH.stat().st_mtime if MODEL_PATH.exists() else None

    train_df, features, metadata = _prepare_dataset()
    split = _main_split(train_df, metadata)
    raw_params = metadata.get("raw_params") or {}
    top_k = int(surge_train._to_float_env("SURGE_ML_PRECISION_TOP_K", float(raw_params.get("ml_precision_top_k", 50) or 50)))
    use_balanced = str(os.getenv("SURGE_ML_CLASS_BALANCED", "") or "").strip().lower() in {"1", "true", "yes", "on"}

    model_specs: List[Tuple[str, Callable[[pd.DataFrame, pd.Series, bool], Any]]] = [
        ("sklearn_hgb", _fit_sklearn_hgb),
        ("lightgbm", _fit_lightgbm),
        ("xgboost", _fit_xgboost),
    ]
    rows = [_run_model(name, fit_fn, split, train_df, features, use_balanced, top_k) for name, fit_fn in model_specs]

    train_status_mtime_after = TRAIN_STATUS_PATH.stat().st_mtime if TRAIN_STATUS_PATH.exists() else None
    model_mtime_after = MODEL_PATH.stat().st_mtime if MODEL_PATH.exists() else None
    payload: Dict[str, Any] = {
        "ts": ts,
        "status": "OK" if any(r.get("status") == "OK" for r in rows) else "WARN",
        "purpose": "compare optional model libraries without overwriting the production surge ML model",
        "outputs": {"json": str(OUTPUT_JSON), "csv": str(OUTPUT_CSV)},
        "production_artifacts_unchanged": {
            "train_status_path": str(TRAIN_STATUS_PATH),
            "train_status_mtime_before": train_status_mtime_before,
            "train_status_mtime_after": train_status_mtime_after,
            "train_status_unchanged": train_status_mtime_before == train_status_mtime_after,
            "model_path": str(MODEL_PATH),
            "model_mtime_before": model_mtime_before,
            "model_mtime_after": model_mtime_after,
            "model_unchanged": model_mtime_before == model_mtime_after,
        },
        "policy": {
            "label_method": metadata.get("label_config", {}).get("method"),
            "label_config": metadata.get("label_config"),
            "embargo_days": int(split["embargo_days"]),
            "precision_top_k": int(top_k),
            "operational_model_replacement": "forbidden_by_this_script",
        },
        "dataset": {
            "rows_total": metadata.get("rows_total"),
            "eval_start_ymd": metadata.get("eval_start_ymd"),
            "eval_end_ymd": metadata.get("eval_end_ymd"),
            "split_date": split.get("split_date"),
            "valid_start_date": split.get("valid_start_date"),
            "features": features,
            "class_balanced": bool(use_balanced),
        },
        "results": rows,
    }
    OUTPUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(rows)
    return 0 if any(r.get("status") == "OK" for r in rows) else 2


if __name__ == "__main__":
    raise SystemExit(main())
