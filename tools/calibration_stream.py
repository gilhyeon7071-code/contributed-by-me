from __future__ import annotations

import datetime as _dt
import json
import math
import os
import pickle
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


ROOT_A = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT_A / "2_Logs"
ROOT_B = ROOT_A.parent / "vibe" / "buffett"
RUNS_B = ROOT_B / "runs"
CALIB_ROOT = ROOT_B / "data" / "calib"
PTR_PATH = RUNS_B / "SSOT_TODAY_FINAL.json"
OBS_LAST_PATH = RUNS_B / "observer_state_last.json"

JOINED_FINAL_PATH = LOG_DIR / "joined_trades_final_latest.csv"
AFTER_CLOSE_LAST_PATH = LOG_DIR / "after_close_summary_last.json"


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _norm_ymd(v: Any) -> str:
    s = "".join(ch for ch in str(v or "") if ch.isdigit())
    return s[:8]


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _raw_prob_from_row(row: pd.Series, score_scale: float = 4.0) -> float:
    for k in ("p_raw", "raw_prob", "raw_proba", "pred_prob"):
        if k in row.index:
            p = _to_float(row.get(k), 0.5)
            return max(1e-6, min(1 - 1e-6, p))
    fs = _to_float(row.get("final_score"), 0.0)
    p = _sigmoid(fs * score_scale)
    return max(1e-6, min(1 - 1e-6, p))


def _label_from_row(row: pd.Series) -> Optional[int]:
    for k in ("y", "label", "target", "success"):
        if k in row.index:
            v = row.get(k)
            if pd.isna(v):
                return None
            try:
                return int(float(v) > 0.5)
            except Exception:
                return None
    for k in ("pnl_pct", "net_ret", "gross_ret", "pnl_krw_net", "pnl_krw"):
        if k in row.index:
            v = row.get(k)
            if pd.isna(v):
                continue
            return int(_to_float(v, 0.0) > 0.0)
    return None


def _extract_regime_default() -> str:
    if not AFTER_CLOSE_LAST_PATH.exists():
        return "UNKNOWN"
    try:
        obj = json.loads(AFTER_CLOSE_LAST_PATH.read_text(encoding="utf-8"))
    except Exception:
        return "UNKNOWN"
    return str(obj.get("market_regime") or "UNKNOWN").upper()


def _pick_date_col(df: pd.DataFrame) -> Optional[str]:
    for c in ("exit_date", "date", "entry_date"):
        if c in df.columns:
            return c
    return None


def _fit_pav(scores: List[float], labels: List[int]) -> Dict[str, Any]:
    pairs = sorted(zip(scores, labels), key=lambda x: x[0])
    blocks: List[Dict[str, Any]] = []
    for s, y in pairs:
        blocks.append({"sum_w": 1.0, "sum_y": float(y), "min_s": float(s), "max_s": float(s)})
        while len(blocks) >= 2:
            a = blocks[-2]
            b = blocks[-1]
            av = a["sum_y"] / a["sum_w"]
            bv = b["sum_y"] / b["sum_w"]
            if av <= bv:
                break
            merged = {
                "sum_w": a["sum_w"] + b["sum_w"],
                "sum_y": a["sum_y"] + b["sum_y"],
                "min_s": a["min_s"],
                "max_s": b["max_s"],
            }
            blocks = blocks[:-2] + [merged]
    boundaries = [float(b["max_s"]) for b in blocks]
    values = [float(b["sum_y"] / b["sum_w"]) for b in blocks]
    return {"method": "PAV_ISOTONIC_PROXY", "boundaries": boundaries, "values": values}


def _predict_pav(model: Dict[str, Any], scores: List[float]) -> List[float]:
    bs = [float(x) for x in (model.get("boundaries") or [])]
    vs = [float(x) for x in (model.get("values") or [])]
    if not bs or not vs or len(bs) != len(vs):
        return [0.5 for _ in scores]
    out: List[float] = []
    for s in scores:
        idx = 0
        while idx < len(bs) and s > bs[idx]:
            idx += 1
        if idx >= len(vs):
            idx = len(vs) - 1
        out.append(max(1e-6, min(1 - 1e-6, float(vs[idx]))))
    return out


def _compute_ece(y: List[int], p: List[float], n_bins: int = 10) -> float:
    if not y or not p or len(y) != len(p):
        return 0.0
    n = len(y)
    ece = 0.0
    for b in range(n_bins):
        lo = b / n_bins
        hi = (b + 1) / n_bins
        idxs = [i for i, pi in enumerate(p) if (pi >= lo and (pi < hi or (b == n_bins - 1 and pi <= hi)))]
        if not idxs:
            continue
        conf = sum(p[i] for i in idxs) / len(idxs)
        acc = sum(y[i] for i in idxs) / len(idxs)
        ece += (len(idxs) / n) * abs(acc - conf)
    return float(ece)


def _compute_brier(y: List[int], p: List[float]) -> float:
    if not y or not p or len(y) != len(p):
        return 0.0
    n = len(y)
    return float(sum((p[i] - y[i]) ** 2 for i in range(n)) / n)


def _status_from_metrics(ece: float, brier: float, ece_warn: float, ece_fail: float, brier_warn: float, brier_fail: float) -> str:
    if ece > ece_fail or brier > brier_fail:
        return "FAIL"
    if ece > ece_warn or brier > brier_warn:
        return "WARN"
    return "PASS"


def _load_window_df(window_days: int) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    meta: Dict[str, Any] = {"source": str(JOINED_FINAL_PATH)}
    if not JOINED_FINAL_PATH.exists():
        return pd.DataFrame(), {**meta, "error": "joined_trades_final_latest.csv missing"}
    try:
        df = pd.read_csv(JOINED_FINAL_PATH, dtype={"code": str})
    except Exception as e:
        return pd.DataFrame(), {**meta, "error": f"read_csv failed: {type(e).__name__}: {e}"}
    if df.empty:
        return df, {**meta, "error": "empty"}

    date_col = _pick_date_col(df)
    if not date_col:
        return pd.DataFrame(), {**meta, "error": "date column missing(exit_date/date/entry_date)"}

    df = df.copy()
    df["_ymd"] = df[date_col].map(_norm_ymd)
    df = df[df["_ymd"].str.len() == 8].copy()
    if df.empty:
        return df, {**meta, "error": "no valid ymd"}

    dates = sorted(df["_ymd"].unique().tolist())
    keep_dates = set(dates[-int(max(1, window_days)):])
    df = df[df["_ymd"].isin(keep_dates)].copy()

    if "regime" not in df.columns:
        df["regime"] = _extract_regime_default()
    df["regime"] = df["regime"].fillna("UNKNOWN").astype(str).str.upper()
    df["_p_raw"] = df.apply(_raw_prob_from_row, axis=1)
    df["_y"] = df.apply(_label_from_row, axis=1)
    df = df[df["_y"].isin([0, 1])].copy()
    df["_y"] = df["_y"].astype(int)

    meta.update(
        {
            "window_days": int(window_days),
            "rows_after_window": int(len(df)),
            "date_min": min(keep_dates) if keep_dates else None,
            "date_max": max(keep_dates) if keep_dates else None,
            "regimes": sorted(df["regime"].unique().tolist()) if not df.empty else [],
        }
    )
    return df, meta


def _save_model(regime: str, model: Dict[str, Any]) -> str:
    regime_dir = CALIB_ROOT / regime
    regime_dir.mkdir(parents=True, exist_ok=True)
    path = regime_dir / "ivap.pkl"
    with path.open("wb") as f:
        pickle.dump(model, f)
    return str(path)


def _update_observer_state(cal_status: str, detail: str, evidence: str) -> None:
    if not OBS_LAST_PATH.exists():
        return
    try:
        obj = json.loads(OBS_LAST_PATH.read_text(encoding="utf-8"))
    except Exception:
        return
    reasons = obj.get("reasons")
    if not isinstance(reasons, list):
        reasons = []
    reasons = [r for r in reasons if str((r or {}).get("code") or "").upper() not in {"CALIBRATION_DRIFT_WARN", "CALIBRATION_DRIFT_FAIL"}]
    if cal_status == "FAIL":
        reasons.append(
            {
                "level": "FAIL",
                "code": "CALIBRATION_DRIFT_FAIL",
                "detail": detail,
                "evidence": evidence,
            }
        )
        obj["status"] = "FAIL"
        state = obj.get("state") if isinstance(obj.get("state"), dict) else {}
        state["strategy_state_label"] = "RISK"
        state["account_state_label"] = "RISK"
        state["exposure_policy"] = "HALT"
        obj["state"] = state
    elif cal_status == "WARN":
        reasons.append(
            {
                "level": "CAUTION",
                "code": "CALIBRATION_DRIFT_WARN",
                "detail": detail,
                "evidence": evidence,
            }
        )
        if str(obj.get("status") or "").upper() == "PASS":
            obj["status"] = "CAUTION"
            state = obj.get("state") if isinstance(obj.get("state"), dict) else {}
            state["strategy_state_label"] = "CAUTION"
            state["account_state_label"] = "CAUTION"
            state["exposure_policy"] = "REDUCED"
            obj["state"] = state
    obj["reasons"] = reasons
    OBS_LAST_PATH.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_snapshot_report(report: Dict[str, Any]) -> Optional[str]:
    if not PTR_PATH.exists():
        return None
    try:
        ptr = json.loads(PTR_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    snap = Path(str(ptr.get("latest_snapshot") or ""))
    if not snap.exists() or not snap.is_dir():
        return None
    out = snap / "calib_report.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(out)


def main() -> int:
    window_days = int(os.getenv("CALIB_WINDOW_DAYS", "21") or "21")
    min_samples = int(os.getenv("CALIB_MIN_SAMPLES", "30") or "30")
    ece_warn = _to_float(os.getenv("CALIB_ECE_WARN", "0.04"), 0.04)
    ece_fail = _to_float(os.getenv("CALIB_ECE_FAIL", "0.06"), 0.06)
    brier_warn = _to_float(os.getenv("CALIB_BRIER_WARN", "0.22"), 0.22)
    brier_fail = _to_float(os.getenv("CALIB_BRIER_FAIL", "0.25"), 0.25)

    now_ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    as_of_ymd = _dt.datetime.now().strftime("%Y%m%d")

    df, load_meta = _load_window_df(window_days)
    if df.empty:
        report = {
            "as_of_ymd": as_of_ymd,
            "generated_at": _dt.datetime.now().isoformat(timespec="seconds"),
            "status": "WARN",
            "new_orders_allowed": True,
            "reason": "insufficient_labeled_data",
            "metrics": {"ece": None, "brier": None, "rows": 0},
            "thresholds": {
                "ece_warn": ece_warn,
                "ece_fail": ece_fail,
                "brier_warn": brier_warn,
                "brier_fail": brier_fail,
            },
            "window": load_meta,
            "regime_reports": [],
            "recalib_required": False,
            "model_paths": {},
        }
        dated = LOG_DIR / f"calibration_stream_{now_ts}.json"
        latest = LOG_DIR / "calibration_stream_latest.json"
        dated.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        latest.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        snap = _write_snapshot_report(report)
        if snap:
            report["snapshot_report_path"] = snap
            latest.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            dated.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[CALIB] status={report['status']} rows=0 reason=insufficient_labeled_data latest={latest}")
        return 0

    global_scores = [float(x) for x in df["_p_raw"].tolist()]
    global_labels = [int(x) for x in df["_y"].tolist()]
    global_model = _fit_pav(global_scores, global_labels)
    global_model.update(
        {
            "regime": "GLOBAL",
            "window_days": window_days,
            "samples": len(global_scores),
            "generated_at": _dt.datetime.now().isoformat(timespec="seconds"),
        }
    )
    global_path = _save_model("GLOBAL", global_model)

    regime_reports: List[Dict[str, Any]] = []
    model_paths: Dict[str, str] = {"GLOBAL": global_path}

    y_all: List[int] = []
    p_all: List[float] = []
    for regime in sorted(df["regime"].unique().tolist()):
        sub = df[df["regime"] == regime].copy()
        scores = [float(x) for x in sub["_p_raw"].tolist()]
        labels = [int(x) for x in sub["_y"].tolist()]
        if len(scores) >= min_samples:
            model = _fit_pav(scores, labels)
            model.update(
                {
                    "regime": regime,
                    "window_days": window_days,
                    "samples": len(scores),
                    "generated_at": _dt.datetime.now().isoformat(timespec="seconds"),
                }
            )
            model_path = _save_model(regime, model)
        else:
            model = global_model
            model_path = global_path
        model_paths[regime] = model_path
        p_cal = _predict_pav(model, scores)
        ece = _compute_ece(labels, p_cal)
        brier = _compute_brier(labels, p_cal)
        status = _status_from_metrics(ece, brier, ece_warn, ece_fail, brier_warn, brier_fail)
        regime_reports.append(
            {
                "regime": regime,
                "samples": len(scores),
                "method": str(model.get("method") or "PAV_ISOTONIC_PROXY"),
                "model_path": model_path,
                "ece": round(ece, 6),
                "brier": round(brier, 6),
                "status": status,
                "used_global_model": bool(model is global_model),
            }
        )
        y_all.extend(labels)
        p_all.extend(p_cal)

    ece_all = _compute_ece(y_all, p_all)
    brier_all = _compute_brier(y_all, p_all)
    status_metrics = _status_from_metrics(ece_all, brier_all, ece_warn, ece_fail, brier_warn, brier_fail)

    # 데이터 신선도 (PLANS 2026-08-21 (25)).
    # _load_window_df 의 창은 `dates[-window_days:]` - 달력이 아니라 파일에 있는 날짜 중 마지막 N개다.
    # 상류(joined_trades_final_latest.csv)가 멈추면 창은 영원히 과거를 가리키면서도
    # `window_days: 21` 이라고 보고한다. 기준선을 달력으로 분리해 나이를 실제로 잰다.
    max_age_days = int(os.getenv("CALIB_MAX_AGE_DAYS", "30") or "30")
    data_age_days = None
    freshness_status = "UNKNOWN"
    _dmax = str(load_meta.get("date_max") or "").strip()
    if len(_dmax) == 8 and _dmax.isdigit():
        try:
            _d = _dt.date(int(_dmax[0:4]), int(_dmax[4:6]), int(_dmax[6:8]))
            data_age_days = int((_dt.date.today() - _d).days)
            freshness_status = "STALE" if data_age_days > max_age_days else "FRESH"
        except Exception:
            data_age_days = None
            freshness_status = "UNKNOWN"
    load_meta = {
        **load_meta,
        "data_age_days": data_age_days,
        "max_age_days": max_age_days,
        "freshness_status": freshness_status,
    }

    # STALE 이면 최소 WARN. 드리프트 FAIL 이 있으면 그쪽이 우선한다(더 나쁜 상태를 덮지 않는다)
    status_all = status_metrics
    if freshness_status == "STALE" and status_all == "PASS":
        status_all = "WARN"

    _reason_parts = []
    if status_metrics in {"WARN", "FAIL"}:
        _reason_parts.append("calibration_drift")
    if freshness_status == "STALE":
        _reason_parts.append("calibration_stale")
    _reason = "+".join(_reason_parts) if _reason_parts else "ok"

    recalib_required = (
        status_all in {"WARN", "FAIL"}
        or freshness_status == "STALE"
        or any(r["status"] in {"WARN", "FAIL"} for r in regime_reports)
    )

    report = {
        "as_of_ymd": as_of_ymd,
        "generated_at": _dt.datetime.now().isoformat(timespec="seconds"),
        "status": status_all,
        "status_from_metrics": status_metrics,
        "new_orders_allowed": False if status_all == "FAIL" else True,
        "reason": _reason,
        "metrics": {
            "rows": len(y_all),
            "ece": round(ece_all, 6),
            "brier": round(brier_all, 6),
        },
        "thresholds": {
            "ece_warn": ece_warn,
            "ece_fail": ece_fail,
            "brier_warn": brier_warn,
            "brier_fail": brier_fail,
        },
        "window": load_meta,
        "regime_reports": regime_reports,
        "recalib_required": bool(recalib_required),
        "model_paths": model_paths,
        "observer_gate_code": (
            "CALIBRATION_DRIFT_FAIL"
            if status_all == "FAIL"
            else "CALIBRATION_DRIFT_WARN"
            if status_all == "WARN"
            else "CALIBRATION_OK"
        ),
    }

    dated = LOG_DIR / f"calibration_stream_{now_ts}.json"
    latest = LOG_DIR / "calibration_stream_latest.json"
    dated.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    latest.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    snap_report = _write_snapshot_report(report)
    if snap_report:
        report["snapshot_report_path"] = snap_report
        latest.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        dated.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    if status_all in {"WARN", "FAIL"}:
        detail = (
            f"calibration status={status_all} "
            f"(ece={report['metrics']['ece']} brier={report['metrics']['brier']} "
            f"thr_ece_fail={ece_fail} thr_brier_fail={brier_fail})"
        )
        _update_observer_state(status_all, detail, str(latest))

    print(
        f"[CALIB] status={status_all} rows={len(y_all)} "
        f"ece={report['metrics']['ece']} brier={report['metrics']['brier']} latest={latest}"
    )
    return 3 if status_all == "FAIL" else 0


if __name__ == "__main__":
    raise SystemExit(main())

