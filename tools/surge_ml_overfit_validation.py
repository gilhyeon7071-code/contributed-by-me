from __future__ import annotations

import csv
import json
import math
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
INPUT_JSON = LOG_DIR / "surge_ml_compare_models_latest.json"
OUTPUT_JSON = LOG_DIR / "surge_ml_overfit_validation_latest.json"
OUTPUT_CSV = LOG_DIR / "surge_ml_overfit_validation_latest.csv"
TRAIN_STATUS_PATH = LOG_DIR / "surge_ml_train_status_latest.json"
MODEL_PATH = ROOT / "_cache" / "surge_ml_model.pkl"


def _to_float(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _fold_values(row: Dict[str, Any], metric: str) -> List[float]:
    wf = row.get("walk_forward") or {}
    vals: List[float] = []
    for fold in wf.get("folds") or []:
        val = _to_float(fold.get(metric))
        if val is not None:
            vals.append(val)
    return vals


def _paired_diffs(candidate: Dict[str, Any], baseline: Dict[str, Any], metric: str) -> List[float]:
    cand_vals = _fold_values(candidate, metric)
    base_vals = _fold_values(baseline, metric)
    return [float(c - b) for c, b in zip(cand_vals, base_vals) if math.isfinite(c) and math.isfinite(b)]


def _mean(values: List[float]) -> float | None:
    return float(np.mean(values)) if values else None


def _median(values: List[float]) -> float | None:
    return float(np.median(values)) if values else None


def _std(values: List[float]) -> float | None:
    if len(values) < 2:
        return None
    out = float(np.std(values, ddof=1))
    return out if math.isfinite(out) else None


def _sharpe_like(values: List[float]) -> float | None:
    sd = _std(values)
    if sd is None or sd <= 1e-12:
        return None
    return float(np.mean(values) / sd * math.sqrt(len(values)))


def _normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _dsr_proxy(values: List[float], *, n_trials: int) -> Dict[str, Any]:
    n_obs = len(values)
    sr = _sharpe_like(values)
    if n_obs < 2 or sr is None:
        return {
            "deflated_sharpe_ratio": None,
            "sharpe_like": sr,
            "n_obs": int(n_obs),
            "n_trials": int(max(1, n_trials)),
            "reason": "insufficient_or_invalid_metric_diffs",
        }
    penalty = math.sqrt(2.0 * math.log(max(2, int(n_trials)))) / max(1.0, math.sqrt(n_obs))
    adjusted = float(sr) - float(penalty)
    return {
        "deflated_sharpe_ratio": float(_normal_cdf(adjusted)),
        "sharpe_like": float(sr),
        "trial_penalty": float(penalty),
        "n_obs": int(n_obs),
        "n_trials": int(max(1, n_trials)),
    }


def _pbo_proxy(values: List[float], *, min_obs: int) -> Dict[str, Any]:
    n_obs = len(values)
    if n_obs == 0:
        return {
            "status": "FAIL_CLOSED",
            "passed": False,
            "reason": "no_walk_forward_diffs",
            "n_obs": 0,
            "min_obs": int(min_obs),
        }
    underperf = float(np.mean(np.asarray(values, dtype=float) <= 0.0))
    median_diff = _median(values)
    enough = n_obs >= int(min_obs)
    passed = bool(enough and underperf <= 0.50 and (median_diff is not None and median_diff >= 0.0))
    return {
        "status": "PASS" if passed else ("FAIL" if enough else "FAIL_CLOSED"),
        "passed": passed,
        "reason": "" if enough else "insufficient_walk_forward_folds_for_adoption_grade_pbo",
        "pbo_underperformance_rate": underperf,
        "median_diff": median_diff,
        "mean_diff": _mean(values),
        "n_obs": int(n_obs),
        "min_obs": int(min_obs),
    }


def _reality_check(candidate_diffs: Dict[str, List[float]], *, bootstrap_runs: int, min_obs: int) -> Dict[str, Any]:
    usable = {k: v for k, v in candidate_diffs.items() if len(v) > 0}
    if not usable:
        return {
            "status": "FAIL_CLOSED",
            "passed": False,
            "reason": "no_candidate_diffs",
            "n_obs": 0,
            "min_obs": int(min_obs),
        }
    n_obs = min(len(v) for v in usable.values())
    names = sorted(usable)
    mat = np.asarray([usable[name][:n_obs] for name in names], dtype=float)
    observed_means = mat.mean(axis=1)
    observed_best = float(np.max(observed_means))
    best_model = names[int(np.argmax(observed_means))]
    rng = np.random.default_rng(int(os.getenv("SURGE_ML_REALITY_CHECK_SEED", "42") or "42"))
    centered = mat - observed_means[:, None]
    max_centered_means = []
    runs = max(100, int(bootstrap_runs))
    for _ in range(runs):
        idx = rng.integers(0, n_obs, size=n_obs)
        max_centered_means.append(float(centered[:, idx].mean(axis=1).max()))
    threshold = max(0.0, observed_best)
    p_value = float(np.mean(np.asarray(max_centered_means, dtype=float) >= threshold))
    enough = n_obs >= int(min_obs)
    passed = bool(enough and observed_best > 0.0 and p_value <= 0.10)
    return {
        "status": "PASS" if passed else ("FAIL" if enough else "FAIL_CLOSED"),
        "passed": passed,
        "reason": "" if enough else "insufficient_walk_forward_folds_for_adoption_grade_reality_check",
        "best_model": best_model,
        "observed_best_mean_diff": observed_best,
        "p_value": p_value,
        "bootstrap_runs": int(runs),
        "n_obs": int(n_obs),
        "min_obs": int(min_obs),
        "candidate_mean_diffs": {name: float(np.mean(usable[name][:n_obs])) for name in names},
    }


def _model_lookup(payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in payload.get("results") or []:
        name = str(row.get("model") or "").strip()
        if name:
            out[name] = row
    return out


def _validate(payload: Dict[str, Any]) -> Dict[str, Any]:
    models = _model_lookup(payload)
    if "sklearn_hgb" not in models:
        raise RuntimeError("baseline sklearn_hgb is missing from comparison output")
    baseline = models["sklearn_hgb"]
    candidates = {name: row for name, row in models.items() if name != "sklearn_hgb" and row.get("status") == "OK"}
    min_obs = int(float(os.getenv("SURGE_ML_OVERFIT_MIN_FOLDS", "20") or "20"))
    bootstrap_runs = int(float(os.getenv("SURGE_ML_REALITY_CHECK_BOOTSTRAPS", "2000") or "2000"))
    min_dsr = float(os.getenv("SURGE_ML_MIN_DSR", "0.10") or "0.10")

    candidate_rows: List[Dict[str, Any]] = []
    candidate_pr_diffs: Dict[str, List[float]] = {}
    for name, row in sorted(candidates.items()):
        pr_diffs = _paired_diffs(row, baseline, "valid_pr_auc")
        auc_diffs = _paired_diffs(row, baseline, "valid_auc")
        p_at_k_diffs = _paired_diffs(row, baseline, "precision_at_k")
        dsr = _dsr_proxy(pr_diffs, n_trials=max(1, len(candidates)))
        dsr_enough = len(pr_diffs) >= min_obs
        dsr_passed = bool(
            dsr_enough
            and dsr.get("deflated_sharpe_ratio") is not None
            and float(dsr["deflated_sharpe_ratio"]) >= min_dsr
            and (_mean(pr_diffs) is not None and float(_mean(pr_diffs) or 0.0) > 0.0)
        )
        pbo = _pbo_proxy(pr_diffs, min_obs=min_obs)
        main_pr_diff = None
        cand_main = _to_float(row.get("valid_pr_auc"))
        base_main = _to_float(baseline.get("valid_pr_auc"))
        if cand_main is not None and base_main is not None:
            main_pr_diff = cand_main - base_main
        candidate_rows.append(
            {
                "model": name,
                "main_valid_pr_auc": cand_main,
                "main_pr_auc_diff_vs_baseline": main_pr_diff,
                "main_precision_at_k": _to_float(row.get("precision_at_k")),
                "main_auc": _to_float(row.get("valid_auc")),
                "wf_pr_auc_diff_mean": _mean(pr_diffs),
                "wf_pr_auc_diff_median": _median(pr_diffs),
                "wf_auc_diff_mean": _mean(auc_diffs),
                "wf_precision_at_k_diff_mean": _mean(p_at_k_diffs),
                "fold_count": int(len(pr_diffs)),
                "pbo": pbo,
                "dsr": {
                    **dsr,
                    "status": "PASS" if dsr_passed else ("FAIL" if dsr_enough else "FAIL_CLOSED"),
                    "passed": dsr_passed,
                    "min_dsr": float(min_dsr),
                    "reason": "" if dsr_enough else "insufficient_walk_forward_folds_for_adoption_grade_dsr",
                },
            }
        )
        candidate_pr_diffs[name] = pr_diffs

    reality_check = _reality_check(candidate_pr_diffs, bootstrap_runs=bootstrap_runs, min_obs=min_obs)
    any_candidate_passed = any(
        bool(row["pbo"].get("passed")) and bool(row["dsr"].get("passed")) for row in candidate_rows
    )
    overall_passed = bool(any_candidate_passed and reality_check.get("passed"))
    return {
        "baseline_model": "sklearn_hgb",
        "baseline": {
            "valid_auc": _to_float(baseline.get("valid_auc")),
            "valid_pr_auc": _to_float(baseline.get("valid_pr_auc")),
            "precision_at_k": _to_float(baseline.get("precision_at_k")),
            "walk_forward": baseline.get("walk_forward") or {},
        },
        "candidate_count": int(len(candidate_rows)),
        "min_adoption_grade_folds": int(min_obs),
        "metric_under_test": "walk_forward.valid_pr_auc_diff_vs_sklearn_hgb",
        "candidates": candidate_rows,
        "reality_check": reality_check,
        "overall": {
            "status": "PASS" if overall_passed else "FAIL_CLOSED",
            "passed": overall_passed,
            "reason": "" if overall_passed else "no_candidate_passed_pbo_dsr_reality_check_or_sample_size_guard",
            "operational_model_replacement": "forbidden_by_this_validation",
        },
    }


def _write_csv(validation: Dict[str, Any]) -> None:
    fieldnames = [
        "model",
        "main_valid_pr_auc",
        "main_pr_auc_diff_vs_baseline",
        "wf_pr_auc_diff_mean",
        "wf_pr_auc_diff_median",
        "wf_precision_at_k_diff_mean",
        "fold_count",
        "pbo_status",
        "pbo_underperformance_rate",
        "dsr_status",
        "deflated_sharpe_ratio",
        "dsr_reason",
        "reality_check_status",
        "reality_check_p_value",
    ]
    rc = validation.get("reality_check") or {}
    with OUTPUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in validation.get("candidates") or []:
            pbo = row.get("pbo") or {}
            dsr = row.get("dsr") or {}
            writer.writerow(
                {
                    "model": row.get("model"),
                    "main_valid_pr_auc": row.get("main_valid_pr_auc"),
                    "main_pr_auc_diff_vs_baseline": row.get("main_pr_auc_diff_vs_baseline"),
                    "wf_pr_auc_diff_mean": row.get("wf_pr_auc_diff_mean"),
                    "wf_pr_auc_diff_median": row.get("wf_pr_auc_diff_median"),
                    "wf_precision_at_k_diff_mean": row.get("wf_precision_at_k_diff_mean"),
                    "fold_count": row.get("fold_count"),
                    "pbo_status": pbo.get("status"),
                    "pbo_underperformance_rate": pbo.get("pbo_underperformance_rate"),
                    "dsr_status": dsr.get("status"),
                    "deflated_sharpe_ratio": dsr.get("deflated_sharpe_ratio"),
                    "dsr_reason": dsr.get("reason"),
                    "reality_check_status": rc.get("status"),
                    "reality_check_p_value": rc.get("p_value"),
                }
            )


def main() -> int:
    ts = datetime.now().isoformat(timespec="seconds")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    train_status_mtime_before = TRAIN_STATUS_PATH.stat().st_mtime if TRAIN_STATUS_PATH.exists() else None
    model_mtime_before = MODEL_PATH.stat().st_mtime if MODEL_PATH.exists() else None
    payload = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
    validation = _validate(payload)
    train_status_mtime_after = TRAIN_STATUS_PATH.stat().st_mtime if TRAIN_STATUS_PATH.exists() else None
    model_mtime_after = MODEL_PATH.stat().st_mtime if MODEL_PATH.exists() else None
    out = {
        "ts": ts,
        "status": validation["overall"]["status"],
        "source": str(INPUT_JSON),
        "outputs": {"json": str(OUTPUT_JSON), "csv": str(OUTPUT_CSV)},
        "purpose": "fail-closed overfit and data-snooping validation for optional surge ML model adoption",
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
        "source_policy": payload.get("policy") or {},
        "validation": validation,
    }
    OUTPUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(validation)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
