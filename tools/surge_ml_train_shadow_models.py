from __future__ import annotations

import json
import os
import pickle
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools import surge_ml_compare_models as compare

LOG_DIR = ROOT / "2_Logs"
SHADOW_MODEL_PATH = ROOT / "_cache" / "surge_ml_shadow_models.pkl"
SHADOW_STATUS_PATH = LOG_DIR / "surge_ml_shadow_models_status_latest.json"
PROD_MODEL_PATH = ROOT / "_cache" / "surge_ml_model.pkl"
PROD_STATUS_PATH = LOG_DIR / "surge_ml_train_status_latest.json"


def _fit_optional_shadow_models(
    train_df: pd.DataFrame,
    split: Dict[str, Any],
    features: List[str],
    use_balanced: bool,
) -> Dict[str, Dict[str, Any]]:
    tr = split["train"]
    X_tr = tr[features].astype(float)
    y_tr = tr["label_surge"].astype(int)
    specs = {
        "lightgbm": compare._fit_lightgbm,
        "xgboost": compare._fit_xgboost,
    }
    models: Dict[str, Dict[str, Any]] = {}
    for name, fit_fn in specs.items():
        try:
            models[name] = {
                "status": "OK",
                "model": fit_fn(X_tr, y_tr, use_balanced),
                "skip_reason": "",
            }
        except ImportError as exc:
            models[name] = {"status": "SKIP", "model": None, "skip_reason": str(exc)}
        except Exception as exc:
            models[name] = {"status": "ERROR", "model": None, "skip_reason": f"{type(exc).__name__}: {exc}"}
    return models


def main() -> int:
    ts = datetime.now().isoformat(timespec="seconds")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    SHADOW_MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    prod_model_mtime_before = PROD_MODEL_PATH.stat().st_mtime if PROD_MODEL_PATH.exists() else None
    prod_status_mtime_before = PROD_STATUS_PATH.stat().st_mtime if PROD_STATUS_PATH.exists() else None

    train_df, features, metadata = compare._prepare_dataset()
    split = compare._main_split(train_df, metadata)
    use_balanced = os.getenv("SURGE_ML_CLASS_BALANCED", "").strip().lower() in {"1", "true", "yes", "on"}
    models = _fit_optional_shadow_models(train_df, split, features, use_balanced)

    bundle = {
        "bundle_type": "surge_ml_shadow_models",
        "trained_at": ts,
        "features": features,
        "label_config": metadata.get("label_config"),
        "label_thr_ret5": metadata.get("label_thr_ret5"),
        "eval_start_ymd": metadata.get("eval_start_ymd"),
        "eval_end_ymd": metadata.get("eval_end_ymd"),
        "split_date": split.get("split_date"),
        "valid_start_date": split.get("valid_start_date"),
        "embargo_days": split.get("embargo_days"),
        "class_balanced": bool(use_balanced),
        "models": models,
    }
    with SHADOW_MODEL_PATH.open("wb") as f:
        pickle.dump(bundle, f)

    prod_model_mtime_after = PROD_MODEL_PATH.stat().st_mtime if PROD_MODEL_PATH.exists() else None
    prod_status_mtime_after = PROD_STATUS_PATH.stat().st_mtime if PROD_STATUS_PATH.exists() else None
    status = {
        "ts": ts,
        "status": "OK" if any(v.get("status") == "OK" for v in models.values()) else "WARN",
        "shadow_model_path": str(SHADOW_MODEL_PATH),
        "rows_train": int(len(split["train"])),
        "rows_total": int(len(train_df)),
        "features": features,
        "label_config": metadata.get("label_config"),
        "embargo_days": split.get("embargo_days"),
        "models": {k: {"status": v.get("status"), "skip_reason": v.get("skip_reason", "")} for k, v in models.items()},
        "production_artifacts_unchanged": {
            "prod_model_path": str(PROD_MODEL_PATH),
            "prod_model_mtime_before": prod_model_mtime_before,
            "prod_model_mtime_after": prod_model_mtime_after,
            "prod_model_unchanged": prod_model_mtime_before == prod_model_mtime_after,
            "prod_status_path": str(PROD_STATUS_PATH),
            "prod_status_mtime_before": prod_status_mtime_before,
            "prod_status_mtime_after": prod_status_mtime_after,
            "prod_status_unchanged": prod_status_mtime_before == prod_status_mtime_after,
        },
    }
    SHADOW_STATUS_PATH.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0 if status["status"] == "OK" else 2


if __name__ == "__main__":
    raise SystemExit(main())
