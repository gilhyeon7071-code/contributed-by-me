from __future__ import annotations

import argparse
import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
DEFAULT_HISTORY = LOG_DIR / "future_signal_preview_history.csv"
LATEST_JSON = LOG_DIR / "e_detector_shadow_latest.json"
LATEST_CSV = LOG_DIR / "e_detector_shadow_latest.csv"


def _now_ts() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc, dtype={"code": str})
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype={"code": str})


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _norm_date8(value: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(value or ""))
    return s[:8] if len(s) >= 8 else ""


def _norm_code(value: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(value or ""))
    return s.zfill(6)[-6:] if s else ""


def _bool_like(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def _mad(values: pd.Series) -> float:
    med = float(values.median())
    mad = float((values - med).abs().median())
    return mad * 1.4826


def _quantile(values: pd.Series, q: float) -> float:
    if values.empty:
        return math.nan
    return float(values.quantile(q, interpolation="higher"))


def _calibration_stats(cal: pd.DataFrame, target_col: str) -> Dict[str, Any]:
    vals = pd.to_numeric(cal[target_col], errors="coerce").dropna()
    if vals.empty:
        return {"ok": False, "reason": "NO_CALIBRATION_VALUES"}
    center = float(vals.median())
    scale = _mad(vals)
    if not math.isfinite(scale) or scale <= 0:
        scale = float(vals.std(ddof=1))
    if not math.isfinite(scale) or scale <= 0:
        return {"ok": False, "reason": "ZERO_CALIBRATION_SCALE"}
    return {
        "ok": True,
        "center": center,
        "scale": scale,
        "rows": int(len(vals)),
    }


def _prepare_history(history_path: Path, target_col: str) -> Tuple[pd.DataFrame, List[str]]:
    df = _read_csv(history_path)
    required = {"D", "code", target_col}
    missing = sorted(required - set(df.columns))
    if missing:
        return pd.DataFrame(), [f"MISSING_COLUMNS:{','.join(missing)}"]
    work = df.copy()
    work["D"] = work["D"].map(_norm_date8)
    work["code"] = work["code"].map(_norm_code)
    work[target_col] = pd.to_numeric(work[target_col], errors="coerce")
    if "horizon_type" not in work.columns:
        work["horizon_type"] = "ALL"
    work["horizon_type"] = work["horizon_type"].fillna("ALL").astype(str)
    if "abstain" in work.columns:
        work["abstain_bool"] = work["abstain"].map(_bool_like)
    else:
        work["abstain_bool"] = False
    work = work.dropna(subset=[target_col])
    work = work[(work["D"].str.len() == 8) & (work["code"].str.len() == 6)].copy()
    work = work.sort_values(["D", "code", "horizon_type"]).reset_index(drop=True)
    if work.empty:
        return work, ["NO_VALID_HISTORY_ROWS"]
    return work, []


def _build_rows(
    work: pd.DataFrame,
    *,
    target_col: str,
    d: str,
    calibration_days: int,
    min_calibration_rows: int,
    betting_alpha: float,
    target_arl: int,
    e_value_cap: float,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    latest = work[work["D"] == d].copy()
    cutoff_dates = sorted(x for x in work["D"].dropna().unique().tolist() if x < d)
    cal_dates = cutoff_dates[-calibration_days:] if calibration_days > 0 else cutoff_dates
    cal = work[work["D"].isin(cal_dates)].copy()
    stats_global = _calibration_stats(cal, target_col)
    rows: List[Dict[str, Any]] = []
    threshold_log = math.log(float(target_arl))
    e_win = 1.0 / float(betting_alpha)
    e_win = min(float(e_win), float(e_value_cap))

    for _, row in latest.iterrows():
        htype = str(row.get("horizon_type") or "ALL")
        hcal = cal[cal["horizon_type"].astype(str) == htype].copy()
        stats = _calibration_stats(hcal, target_col)
        cal_scope = "horizon_type"
        if not stats.get("ok") or int(stats.get("rows") or 0) < min_calibration_rows:
            stats = stats_global
            cal_scope = "global"
        if not stats.get("ok") or int(stats.get("rows") or 0) < min_calibration_rows:
            rows.append(
                {
                    "D": d,
                    "code": row["code"],
                    "name": row.get("name", ""),
                    "horizon_type": htype,
                    "target_col": target_col,
                    "target_value": row[target_col],
                    "status": "NOT_READY",
                    "reason": stats.get("reason") or "INSUFFICIENT_CALIBRATION",
                    "calibration_scope": cal_scope,
                    "calibration_rows": int(stats.get("rows") or 0),
                    "score": None,
                    "score_threshold": None,
                    "betting_alpha": betting_alpha,
                    "e_value": None,
                    "threshold_log": threshold_log,
                    "alert": False,
                    "abstain": bool(row.get("abstain_bool", False)),
                }
            )
            continue

        center = float(stats["center"])
        scale = float(stats["scale"])
        score_cal = (pd.to_numeric((hcal if cal_scope == "horizon_type" else cal)[target_col], errors="coerce") - center).abs() / scale
        score_threshold = _quantile(score_cal.dropna(), 1.0 - betting_alpha)
        score = abs(float(row[target_col]) - center) / scale
        hit = bool(math.isfinite(score_threshold) and score >= score_threshold)
        e_value = e_win if hit else 0.0
        rows.append(
            {
                "D": d,
                "code": row["code"],
                "name": row.get("name", ""),
                "horizon_type": htype,
                "target_col": target_col,
                "target_value": float(row[target_col]),
                "status": "PASS",
                "reason": "E_VALUE_READY",
                "calibration_scope": cal_scope,
                "calibration_rows": int(stats["rows"]),
                "center": center,
                "scale": scale,
                "score": round(float(score), 8),
                "score_threshold": round(float(score_threshold), 8) if math.isfinite(score_threshold) else None,
                "betting_alpha": betting_alpha,
                "e_value": round(float(e_value), 8),
                "threshold_log": round(float(threshold_log), 8),
                "alert": False,
                "abstain": bool(row.get("abstain_bool", False)),
            }
        )

    out = pd.DataFrame(rows)
    ready = out[out["status"] == "PASS"].copy()
    if ready.empty:
        return out, {"status": "NOT_READY", "reason": "NO_READY_ROWS"}
    daily_e = float(pd.to_numeric(ready["e_value"], errors="coerce").fillna(0.0).mean())
    log_e_sr = math.log(daily_e) if daily_e > 0 else 0.0
    alert = bool(log_e_sr >= threshold_log)
    out.loc[out["status"] == "PASS", "daily_mixture_e_value"] = round(daily_e, 8)
    out.loc[out["status"] == "PASS", "log_e_sr"] = round(log_e_sr, 8)
    out.loc[out["status"] == "PASS", "alert"] = alert
    return out, {
        "status": "PASS",
        "reason": "ok",
        "latest_D": d,
        "target_arl": int(target_arl),
        "threshold_log": round(float(threshold_log), 8),
        "daily_mixture_e_value": round(daily_e, 8),
        "log_e_sr": round(log_e_sr, 8),
        "alert": alert,
        "ready_rows": int(len(ready)),
        "alert_rows": int((pd.to_numeric(ready["e_value"], errors="coerce").fillna(0.0) > 0).sum()),
        "not_ready_rows": int((out["status"] != "PASS").sum()),
    }


def run(args: argparse.Namespace) -> Dict[str, Any]:
    history_path = Path(args.history_csv)
    output_json = Path(args.output_json)
    output_csv = Path(args.output_csv)
    base_policy = {
        "read_only_for_trading": True,
        "screening_only": True,
        "confirmatory_gate_required": True,
        "orders_modified": False,
        "fills_modified": False,
        "ledger_modified": False,
        "stats_modified": False,
        "gate_modified": False,
        "risk_lock_modified": False,
        "score_modified": False,
        "threshold_applied": False,
        "runtime_config_modified": False,
        "trading_approved": False,
    }
    payload_base: Dict[str, Any] = {
        "generated_at": _now_ts(),
        "input": str(history_path),
        "outputs": {"json": str(output_json), "csv": str(output_csv), "latest_json": str(LATEST_JSON), "latest_csv": str(LATEST_CSV)},
        "method": {
            "name": "shadow_e_detector_v0",
            "target_col": str(args.target_col),
            "target_arl": int(args.target_arl),
            "threshold_rule": "log(target_arl)",
            "betting_alpha": float(args.betting_alpha),
            "calibration_days": int(args.calibration_days),
            "min_calibration_rows": int(args.min_calibration_rows),
            "notes": [
                "nonparametric split-calibrated indicator e-values",
                "daily row e-values are averaged as a mixture before e-SR screening",
                "shadow output only; not connected to order, score, gate, or lock policy",
            ],
        },
        "policy": base_policy,
    }
    if not history_path.exists():
        payload = {**payload_base, "status": "FAIL", "reason": "HISTORY_MISSING"}
        _write_json(output_json, payload)
        _write_json(LATEST_JSON, payload)
        return payload
    work, reasons = _prepare_history(history_path, str(args.target_col))
    if reasons:
        payload = {**payload_base, "status": "FAIL", "reason": ";".join(reasons)}
        _write_json(output_json, payload)
        _write_json(LATEST_JSON, payload)
        return payload
    d = _norm_date8(args.d) or str(work["D"].max())
    rows, summary = _build_rows(
        work,
        target_col=str(args.target_col),
        d=d,
        calibration_days=int(args.calibration_days),
        min_calibration_rows=int(args.min_calibration_rows),
        betting_alpha=float(args.betting_alpha),
        target_arl=int(args.target_arl),
        e_value_cap=float(args.e_value_cap),
    )
    _write_csv(output_csv, rows)
    _write_csv(LATEST_CSV, rows)
    payload = {
        **payload_base,
        **summary,
        "D": d,
        "rows": int(len(rows)),
        "history_rows": int(len(work)),
        "history_D_min": str(work["D"].min()),
        "history_D_max": str(work["D"].max()),
    }
    _write_json(output_json, payload)
    _write_json(LATEST_JSON, payload)
    return payload


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Build read-only e-detector shadow screening artifacts.")
    p.add_argument("--history-csv", default=str(DEFAULT_HISTORY))
    p.add_argument("--target-col", default="expected_return")
    p.add_argument("--d", default="")
    p.add_argument("--target-arl", type=int, default=300)
    p.add_argument("--betting-alpha", type=float, default=0.05)
    p.add_argument("--calibration-days", type=int, default=20)
    p.add_argument("--min-calibration-rows", type=int, default=30)
    p.add_argument("--e-value-cap", type=float, default=100.0)
    p.add_argument("--output-json", default=str(LOG_DIR / "e_detector_shadow.json"))
    p.add_argument("--output-csv", default=str(LOG_DIR / "e_detector_shadow.csv"))
    return p


def main() -> int:
    payload = run(build_parser().parse_args())
    print(json.dumps({"status": payload.get("status"), "reason": payload.get("reason"), "D": payload.get("D"), "alert": payload.get("alert")}, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
