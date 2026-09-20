from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"

DEFAULT_PREVIEW_META = LOGS / "future_signal_preview_latest.json"
DEFAULT_VALIDATION_META = LOGS / "future_signal_validation_latest.json"
DEFAULT_BASELINE_META = LOGS / "future_signal_baseline_latest.json"
LATEST_JSON = LOGS / "future_signal_calibration_latest.json"


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path)


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _calibration_bins(df: pd.DataFrame, bins: int = 5) -> List[Dict[str, Any]]:
    work = df.copy()
    work["pred_up_prob"] = pd.to_numeric(work["pred_up_prob"], errors="coerce")
    work["actual_up"] = pd.to_numeric(work["actual_up"], errors="coerce")
    work = work.dropna(subset=["pred_up_prob", "actual_up"])
    if len(work) == 0:
        return []
    work["bin"] = pd.cut(work["pred_up_prob"], bins=bins, include_lowest=True, duplicates="drop")
    rows: List[Dict[str, Any]] = []
    for b, g in work.groupby("bin", observed=False):
        rows.append(
            {
                "bin": str(b),
                "rows": int(len(g)),
                "avg_pred_up_prob": round(float(g["pred_up_prob"].mean()), 6),
                "actual_up_rate": round(float(g["actual_up"].mean()), 6),
                "abs_gap": round(abs(float(g["pred_up_prob"].mean()) - float(g["actual_up"].mean())), 6),
            }
        )
    return rows


def _realized_calibration_metrics(realized: pd.DataFrame) -> Dict[str, Any]:
    if "realized_return" not in realized.columns or "pred_up_prob" not in realized.columns:
        return {"available": False, "reason": "REALIZED_ROW_DATA_MISSING"}
    work = realized.copy()
    work["actual_up"] = pd.to_numeric(work["realized_return"], errors="coerce").map(lambda v: 1.0 if v > 0 else 0.0)
    work["pred_up_prob"] = pd.to_numeric(work["pred_up_prob"], errors="coerce")
    work = work.dropna(subset=["pred_up_prob", "actual_up"])
    if len(work) == 0:
        return {"available": False, "reason": "CALIBRATION_ROWS_ZERO"}
    brier = float(((work["pred_up_prob"] - work["actual_up"]) ** 2).mean())
    horizon_types: List[str] = []
    if "horizon_type" in work.columns:
        horizon_types = sorted(str(x) for x in work["horizon_type"].dropna().astype(str).unique())
    bins = _calibration_bins(work)
    return {
        "available": True,
        "reason": "ok",
        "rows": int(len(work)),
        "horizon_types": horizon_types,
        "brier_score": round(brier, 6),
        "bin_count": int(len(bins)),
        "bins": bins,
    }


def validate_calibration(
    *,
    preview_meta_path: Path,
    validation_meta_path: Path,
    baseline_meta_path: Path,
    output_dir: Path,
) -> Dict[str, Any]:
    preview_meta = _load_json(preview_meta_path)
    validation_meta = _load_json(validation_meta_path)
    baseline_meta = _load_json(baseline_meta_path)

    d = str(preview_meta.get("D") or validation_meta.get("D") or "")
    out_json = output_dir / f"future_signal_calibration_{d or 'unknown'}.json"
    calibrated = bool(baseline_meta.get("calibrated", False))
    trading_approved = bool(baseline_meta.get("trading_approved", False))

    base_payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "D": d,
        "model_version": str(preview_meta.get("model_version") or baseline_meta.get("model_version") or ""),
        "calibration_version": str(baseline_meta.get("calibration_version") or ""),
        "calibrated": calibrated,
        "trading_approved": trading_approved,
        "preview_meta": str(preview_meta_path),
        "validation_meta": str(validation_meta_path),
        "baseline_meta": str(baseline_meta_path),
        "outputs": {
            "json": str(out_json),
            "latest_json": str(LATEST_JSON),
        },
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

    if not preview_meta:
        payload = {**base_payload, "status": "FAIL", "calibration_state": "BLOCKED", "reason": "PREVIEW_META_MISSING"}
        _write_json(out_json, payload)
        _write_json(LATEST_JSON, payload)
        return payload
    if not baseline_meta:
        payload = {**base_payload, "status": "FAIL", "calibration_state": "BLOCKED", "reason": "BASELINE_META_MISSING"}
        _write_json(out_json, payload)
        _write_json(LATEST_JSON, payload)
        return payload

    validation_state = str(validation_meta.get("validation_state", "UNKNOWN"))
    if validation_state != "EVALUATED":
        partial_preview: Dict[str, Any] = {
            "available": False,
            "scope": "REALIZED_ROWS_DIAGNOSTIC_ONLY",
            "used_for_trading": False,
            "reason": f"VALIDATION_STATE_NOT_EVALUATED:{validation_state}",
        }
        if validation_state == "EVALUATED_PARTIAL":
            realized_csv = Path(str((validation_meta.get("outputs") or {}).get("realized_csv", "")))
            if realized_csv.exists():
                realized_all = _read_csv(realized_csv)
                partial_preview = {
                    "scope": "REALIZED_ROWS_DIAGNOSTIC_ONLY",
                    "used_for_trading": False,
                    "source": str(realized_csv),
                    **_realized_calibration_metrics(realized_all),
                }
                # INTRADAY-only calibration path: when multi-horizon EVALUATED is structurally
                # unreachable, use INTRADAY rows (horizon==0) as a partial calibration signal.
                if "horizon_type" in realized_all.columns or "horizon" in realized_all.columns:
                    if "horizon" in realized_all.columns:
                        intraday_rows = realized_all[pd.to_numeric(realized_all["horizon"], errors="coerce").fillna(-1) == 0].copy()
                    else:
                        intraday_rows = realized_all[realized_all["horizon_type"].astype(str).str.upper() == "INTRADAY"].copy()
                    intraday_metrics = _realized_calibration_metrics(intraday_rows)
                    if bool(intraday_metrics.get("available")) and int(intraday_metrics.get("rows", 0)) >= 3:
                        payload = {
                            **base_payload,
                            "status": "PASS",
                            "calibration_state": "INTRADAY_EVALUATED",
                            "reason": "INTRADAY_ONLY_PARTIAL_CALIBRATION",
                            "brier_score": intraday_metrics.get("brier_score"),
                            "bin_count": int(intraday_metrics.get("bin_count") or 0),
                            "bins": intraday_metrics.get("bins") or [],
                            "intraday_rows": int(intraday_metrics.get("rows") or 0),
                            "partial_calibration_preview": partial_preview,
                        }
                        _write_json(out_json, payload)
                        _write_json(LATEST_JSON, payload)
                        return payload
            else:
                partial_preview = {
                    **partial_preview,
                    "reason": "REALIZED_CSV_MISSING",
                }
        payload = {
            **base_payload,
            "status": "FAIL",
            "calibration_state": "WAITING",
            "reason": f"REALIZED_VALIDATION_NOT_EVALUATED:{validation_state}",
            "brier_score": None,
            "bin_count": 0,
            "bins": [],
            "partial_calibration_preview": partial_preview,
        }
        _write_json(out_json, payload)
        _write_json(LATEST_JSON, payload)
        return payload

    if not calibrated:
        payload = {
            **base_payload,
            "status": "FAIL",
            "calibration_state": "UNCALIBRATED",
            "reason": "CALIBRATION_NOT_FIT",
            "brier_score": None,
            "bin_count": 0,
            "bins": [],
        }
        _write_json(out_json, payload)
        _write_json(LATEST_JSON, payload)
        return payload

    realized_csv = Path(str((validation_meta.get("outputs") or {}).get("realized_csv", "")))
    if not realized_csv.exists():
        payload = {**base_payload, "status": "FAIL", "calibration_state": "BLOCKED", "reason": "REALIZED_CSV_MISSING"}
        _write_json(out_json, payload)
        _write_json(LATEST_JSON, payload)
        return payload

    realized = _read_csv(realized_csv)
    if "realized_return" not in realized.columns or "pred_up_prob" not in realized.columns:
        payload = {
            **base_payload,
            "status": "FAIL",
            "calibration_state": "BLOCKED",
            "reason": "REALIZED_ROW_DATA_MISSING",
            "brier_score": None,
            "bin_count": 0,
            "bins": [],
        }
        _write_json(out_json, payload)
        _write_json(LATEST_JSON, payload)
        return payload

    metrics = _realized_calibration_metrics(realized)
    if not bool(metrics.get("available")):
        payload = {
            **base_payload,
            "status": "FAIL",
            "calibration_state": "BLOCKED",
            "reason": str(metrics.get("reason") or "CALIBRATION_ROWS_ZERO"),
            "brier_score": None,
            "bin_count": 0,
            "bins": [],
        }
        _write_json(out_json, payload)
        _write_json(LATEST_JSON, payload)
        return payload

    payload = {
        **base_payload,
        "status": "PASS",
        "calibration_state": "EVALUATED",
        "reason": "ok",
        "rows": int(metrics.get("rows") or 0),
        "brier_score": metrics.get("brier_score"),
        "bin_count": int(metrics.get("bin_count") or 0),
        "bins": metrics.get("bins") or [],
    }
    _write_json(out_json, payload)
    _write_json(LATEST_JSON, payload)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate future signal probability calibration when realized data exists.")
    ap.add_argument("--preview-meta", default=str(DEFAULT_PREVIEW_META))
    ap.add_argument("--validation-meta", default=str(DEFAULT_VALIDATION_META))
    ap.add_argument("--baseline-meta", default=str(DEFAULT_BASELINE_META))
    ap.add_argument("--output-dir", default=str(LOGS))
    args = ap.parse_args()
    payload = validate_calibration(
        preview_meta_path=Path(args.preview_meta),
        validation_meta_path=Path(args.validation_meta),
        baseline_meta_path=Path(args.baseline_meta),
        output_dir=Path(args.output_dir),
    )
    print(
        f"[FUTURE_CALIBRATION] status={payload.get('status')} state={payload.get('calibration_state')} "
        f"reason={payload.get('reason')} D={payload.get('D')} calibrated={payload.get('calibrated')} "
        f"brier={payload.get('brier_score')}"
    )
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
