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
DEFAULT_CALIBRATION_META = LOGS / "future_signal_calibration_latest.json"
HISTORY_CSV = LOGS / "future_signal_preview_history.csv"
LATEST_JSON = LOGS / "future_signal_shadow_history_latest.json"

HISTORY_COLUMNS = [
    "recorded_at",
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
    "calibration_version",
    "preview_status",
    "validation_status",
    "validation_state",
    "validation_reason",
    "target_date",
    "realized_rows",
    "calibration_status",
    "calibration_state",
    "calibration_reason",
    "calibrated",
    "trading_approved",
]


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc, dtype=str)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype=str)


def _norm_code(v: Any) -> str:
    s = str(v or "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(6) if s.isdigit() else s


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _bool_text(v: Any) -> str:
    return "true" if bool(v) else "false"


def update_history(
    *,
    preview_meta_path: Path,
    validation_meta_path: Path,
    calibration_meta_path: Path,
    history_path: Path,
    output_json: Path,
) -> Dict[str, Any]:
    preview_meta = _load_json(preview_meta_path)
    validation_meta = _load_json(validation_meta_path)
    calibration_meta = _load_json(calibration_meta_path)

    if not preview_meta:
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "status": "FAIL",
            "reason": "PREVIEW_META_MISSING",
            "rows_added_or_updated": 0,
        }

    preview_csv = Path(str((preview_meta.get("outputs") or {}).get("csv", "")))
    if not preview_csv.exists():
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "status": "FAIL",
            "reason": "PREVIEW_CSV_MISSING",
            "rows_added_or_updated": 0,
        }

    preview = _read_csv(preview_csv)
    target_dates = validation_meta.get("target_dates") if isinstance(validation_meta.get("target_dates"), dict) else {}
    now = datetime.now().isoformat(timespec="seconds")
    rows: List[Dict[str, Any]] = []
    for _, r in preview.iterrows():
        rows.append(
            {
                "recorded_at": now,
                "as_of": r.get("as_of", preview_meta.get("as_of", "")),
                "run_id": r.get("run_id", preview_meta.get("run_id", "")),
                "D": r.get("D", preview_meta.get("D", "")),
                "code": str(r.get("code", "")).zfill(6),
                "name": r.get("name", ""),
                "horizon_type": r.get("horizon_type", ""),
                "horizon": r.get("horizon", ""),
                "horizon_days": r.get("horizon_days", r.get("horizon", "")),
                "pred_up_prob": r.get("pred_up_prob", ""),
                "expected_return": r.get("expected_return", ""),
                "downside_q10": r.get("downside_q10", ""),
                "confidence": r.get("confidence", ""),
                "abstain": r.get("abstain", ""),
                "reason": r.get("reason", ""),
                "news_score": r.get("news_score", ""),
                "news_weight": r.get("news_weight", ""),
                "news_contribution": r.get("news_contribution", ""),
                "model_version": r.get("model_version", preview_meta.get("model_version", "")),
                "feature_version": r.get("feature_version", preview_meta.get("feature_version", "")),
                "feature_asof": r.get("feature_asof", preview_meta.get("feature_asof", "")),
                "calibration_version": r.get("calibration_version", calibration_meta.get("calibration_version", "")),
                "preview_status": preview_meta.get("status", ""),
                "validation_status": validation_meta.get("status", ""),
                "validation_state": validation_meta.get("validation_state", ""),
                "validation_reason": validation_meta.get("reason", ""),
                "target_date": target_dates.get(str(r.get("horizon_type", "")), validation_meta.get("target_date", "")),
                "realized_rows": validation_meta.get("realized_rows", ""),
                "calibration_status": calibration_meta.get("status", ""),
                "calibration_state": calibration_meta.get("calibration_state", ""),
                "calibration_reason": calibration_meta.get("reason", ""),
                "calibrated": _bool_text(calibration_meta.get("calibrated", False)),
                "trading_approved": _bool_text(calibration_meta.get("trading_approved", False)),
            }
        )

    new_df = pd.DataFrame(rows, columns=HISTORY_COLUMNS)
    if history_path.exists():
        old = _read_csv(history_path)
        for col in HISTORY_COLUMNS:
            if col not in old.columns:
                old[col] = ""
        combined = pd.concat([old[HISTORY_COLUMNS], new_df], ignore_index=True)
    else:
        combined = new_df

    before_rows = int(len(combined))
    if len(combined):
        combined["D"] = combined["D"].astype(str).str.strip()
        combined["code"] = combined["code"].map(_norm_code)
        combined["horizon_type"] = combined["horizon_type"].astype(str).str.strip()
        combined["horizon"] = combined["horizon"].astype(str).str.strip()
    combined = combined.drop_duplicates(subset=["D", "code", "horizon_type", "horizon"], keep="last")
    combined = combined.sort_values(["D", "run_id", "code", "horizon_type", "horizon"]).reset_index(drop=True)
    _write_csv(history_path, combined[HISTORY_COLUMNS])

    unique_days = int(pd.Series(combined["D"]).dropna().astype(str).nunique()) if len(combined) else 0
    payload = {
        "generated_at": now,
        "status": "PASS",
        "reason": "ok",
        "history_csv": str(history_path),
        "rows_added_or_updated": int(len(new_df)),
        "history_rows": int(len(combined)),
        "deduped_rows_removed": int(before_rows - len(combined)),
        "unique_D": unique_days,
        "latest_D": str(combined["D"].max()) if len(combined) else "",
        "latest_run_id": str(preview_meta.get("run_id", "")),
        "upsert_key": ["D", "code", "horizon_type", "horizon"],
        "preview_status": preview_meta.get("status", ""),
        "validation_state": validation_meta.get("validation_state", ""),
        "calibration_state": calibration_meta.get("calibration_state", ""),
        "trading_approved": bool(calibration_meta.get("trading_approved", False)),
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
    _write_json(output_json, payload)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Upsert future signal preview rows into shadow history.")
    ap.add_argument("--preview-meta", default=str(DEFAULT_PREVIEW_META))
    ap.add_argument("--validation-meta", default=str(DEFAULT_VALIDATION_META))
    ap.add_argument("--calibration-meta", default=str(DEFAULT_CALIBRATION_META))
    ap.add_argument("--history", default=str(HISTORY_CSV))
    ap.add_argument("--output-json", default=str(LATEST_JSON))
    args = ap.parse_args()

    payload = update_history(
        preview_meta_path=Path(args.preview_meta),
        validation_meta_path=Path(args.validation_meta),
        calibration_meta_path=Path(args.calibration_meta),
        history_path=Path(args.history),
        output_json=Path(args.output_json),
    )
    print(
        f"[FUTURE_SHADOW_HISTORY] status={payload.get('status')} reason={payload.get('reason')} "
        f"history_rows={payload.get('history_rows')} unique_D={payload.get('unique_D')} "
        f"history={payload.get('history_csv', '')}"
    )
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
