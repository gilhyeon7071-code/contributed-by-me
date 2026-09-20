from __future__ import annotations

import argparse
import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"

DEFAULT_PREVIEW_META = LOGS / "future_signal_preview_latest.json"
LATEST_JSON = LOGS / "future_signal_validation_latest.json"
INTRADAY_STATUS_LATEST = LOGS / "intraday_prices_status_latest.json"
INTRADAY_STATUS_DATED_TEMPLATE = "intraday_prices_status_{d}.json"


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


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _norm_date8(v: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s[:8] if len(s) >= 8 else ""


def _norm_code(v: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s.zfill(6)[-6:] if s else ""


def _codes_from_list(v: Any) -> List[str]:
    if not isinstance(v, list):
        return []
    return sorted({_norm_code(x) for x in v if _norm_code(x)})


def _date8_from_text(v: Any) -> str:
    m = re.search(r"(\d{8})", str(v or ""))
    return m.group(1) if m else ""


def _intraday_status_for_d(d: str) -> Dict[str, Any]:
    dated_path = LOGS / INTRADAY_STATUS_DATED_TEMPLATE.format(d=d)
    status_path = dated_path if dated_path.exists() else INTRADAY_STATUS_LATEST
    status = _load_json(status_path)
    status_d = _date8_from_text(status.get("history_csv")) or _date8_from_text(status.get("ts"))
    if not status:
        return {
            "available": False,
            "status_path": str(status_path),
            "preferred_status_path": str(dated_path),
            "fallback_status_path": str(INTRADAY_STATUS_LATEST),
            "status_D": "",
            "validation_D": d,
            "reason": "INTRADAY_STATUS_MISSING",
            "requested_codes": [],
        }
    if status_d != d:
        return {
            "available": False,
            "status_path": str(status_path),
            "preferred_status_path": str(dated_path),
            "fallback_status_path": str(INTRADAY_STATUS_LATEST),
            "status_D": status_d,
            "validation_D": d,
            "reason": "RETROSPECTIVE_HISTORY_ONLY",
            "mode": "RETROSPECTIVE_HISTORY_ONLY",
            "requested_codes": _codes_from_list(status.get("codes_requested_list")),
            "future_signal_codes": _codes_from_list(status.get("future_signal_preview_codes")),
        }
    return {
        "available": True,
        "status_path": str(status_path),
        "preferred_status_path": str(dated_path),
        "fallback_status_path": str(INTRADAY_STATUS_LATEST),
        "status_D": status_d,
        "validation_D": d,
        "reason": "ok",
        "mode": "STATUS_MATCHED_REQUEST_AUDIT",
        "requested_codes": _codes_from_list(status.get("codes_requested_list")),
        "future_signal_codes": _codes_from_list(status.get("future_signal_preview_codes")),
    }


def _candidate_price_files() -> Dict[str, Path]:
    out: Dict[str, Path] = {}
    for p in LOGS.glob("candidates_v41_1_*.csv"):
        m = re.search(r"(\d{8})", p.name)
        if not m:
            continue
        out[m.group(1)] = p
    return out


def _intraday_price_files() -> Dict[str, Path]:
    out: Dict[str, Path] = {}
    for p in LOGS.glob("intraday_prices_history_*.csv"):
        m = re.search(r"(\d{8})", p.name)
        if not m:
            continue
        out[m.group(1)] = p
    return out


def _target_date(d: str, horizon: int, dates: List[str]) -> Optional[str]:
    ordered = sorted(x for x in dates if x >= d)
    if d not in ordered:
        return None
    idx = ordered.index(d) + int(horizon)
    if idx >= len(ordered):
        return None
    return ordered[idx]


def _price_map(path: Path) -> Dict[str, float]:
    df = _read_csv(path)
    if "code" not in df.columns or "close" not in df.columns:
        return {}
    out: Dict[str, float] = {}
    for _, r in df.iterrows():
        code = _norm_code(r.get("code"))
        try:
            close = float(r.get("close"))
        except Exception:
            continue
        if code and close > 0:
            out[code] = close
    return out


def _intraday_return_map(path: Path) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    df = _read_csv(path)
    required = {"code", "ts", "current_price"}
    if not required.issubset(set(df.columns)):
        return {}, {"source": str(path), "status": "FAIL", "reason": "INTRADAY_REQUIRED_COLUMN_MISSING"}
    work = df.copy()
    work["code_norm"] = work["code"].map(_norm_code)
    work["price_num"] = pd.to_numeric(work["current_price"], errors="coerce")
    work["ts_str"] = work["ts"].astype(str)
    work = work[(work["code_norm"].astype(str).str.len() == 6) & (work["price_num"] > 0)]
    if len(work) == 0:
        return {}, {"source": str(path), "status": "FAIL", "reason": "INTRADAY_VALID_ROW_ZERO"}
    out: Dict[str, Dict[str, Any]] = {}
    for code, g in work.sort_values("ts_str").groupby("code_norm", dropna=False):
        if len(g) < 2:
            continue
        first = g.iloc[0]
        last = g.iloc[-1]
        base = float(first["price_num"])
        target = float(last["price_num"])
        if base <= 0 or target <= 0:
            continue
        out[str(code)] = {
            "return": (target / base) - 1.0,
            "base_price": base,
            "target_price": target,
            "base_ts": str(first["ts_str"]),
            "target_ts": str(last["ts_str"]),
            "observations": int(len(g)),
        }
    return out, {
        "source": str(path),
        "status": "PASS",
        "rows": int(len(df)),
        "valid_rows": int(len(work)),
        "codes_with_return": int(len(out)),
    }


def _safe_corr(df: pd.DataFrame, left: str, right: str) -> Optional[float]:
    if len(df) < 3:
        return None
    x = pd.to_numeric(df[left], errors="coerce")
    y = pd.to_numeric(df[right], errors="coerce")
    work = pd.DataFrame({"x": x, "y": y}).dropna()
    if len(work) < 3:
        return None
    val = float(work["x"].corr(work["y"], method="spearman"))
    if math.isnan(val) or math.isinf(val):
        return None
    return round(val, 6)


def _topk_metrics(df: pd.DataFrame, k: int) -> Dict[str, Any]:
    if len(df) == 0:
        return {"k": k, "rows": 0}
    k_eff = min(int(k), len(df))
    top = df.sort_values(["pred_up_prob", "confidence"], ascending=[False, False]).head(k_eff)
    return {
        "k": int(k),
        "rows": int(len(top)),
        "mean_realized_return": round(float(pd.to_numeric(top["realized_return"], errors="coerce").mean()), 6),
        "hit_rate": round(float((pd.to_numeric(top["realized_return"], errors="coerce") > 0).mean()), 6),
    }


def validate_preview(*, preview_meta_path: Path, output_dir: Path) -> Dict[str, Any]:
    preview_meta = _load_json(preview_meta_path)
    if not preview_meta:
        payload = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "status": "FAIL",
            "validation_state": "BLOCKED",
            "reason": "PREVIEW_META_MISSING",
            "rows": 0,
            "outputs": {},
        }
        return payload

    preview_csv = Path(str((preview_meta.get("outputs") or {}).get("csv", "")))
    if not preview_csv.exists():
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "status": "FAIL",
            "validation_state": "BLOCKED",
            "reason": "PREVIEW_CSV_MISSING",
            "rows": 0,
            "outputs": {},
        }

    preview = _read_csv(preview_csv)
    d = _norm_date8(preview_meta.get("D"))
    horizon_vals = pd.to_numeric(preview.get("horizon", pd.Series([5])), errors="coerce").dropna()
    horizon = int(horizon_vals.iloc[0]) if len(horizon_vals) else 5

    files = _candidate_price_files()
    intraday_files = _intraday_price_files()
    out_json = output_dir / f"future_signal_validation_{d or 'unknown'}.json"
    out_realized_csv = output_dir / f"future_signal_validation_rows_{d or 'unknown'}.csv"

    base_payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "D": d,
        "horizon": horizon,
        "model_version": str(preview_meta.get("model_version") or ""),
        "feature_version": str(preview_meta.get("feature_version") or ""),
        "preview_csv": str(preview_csv),
        "available_price_dates_min": min(files.keys()) if files else "",
        "available_price_dates_max": max(files.keys()) if files else "",
        "available_intraday_dates_min": min(intraday_files.keys()) if intraday_files else "",
        "available_intraday_dates_max": max(intraday_files.keys()) if intraday_files else "",
        "target_date": "",
        "rows": int(len(preview)),
        "outputs": {
            "json": str(out_json),
            "latest_json": str(LATEST_JSON),
            "realized_csv": str(out_realized_csv),
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

    if not d:
        payload = {**base_payload, "status": "FAIL", "validation_state": "BLOCKED", "reason": "D_MISSING"}
        _write_json(out_json, payload)
        _write_json(LATEST_JSON, payload)
        return payload
    if not files or d not in files:
        payload = {
            **base_payload,
            "status": "FAIL",
            "validation_state": "WAITING",
            "reason": "TARGET_DATE_NOT_AVAILABLE",
            "realized_rows": 0,
            "metrics": {},
        }
        _write_json(out_json, payload)
        _write_json(LATEST_JSON, payload)
        return payload

    d_prices = _price_map(files[d])
    intraday_returns: Dict[str, Dict[str, Any]] = {}
    intraday_source_meta: Dict[str, Any] = {}
    if d in intraday_files:
        intraday_returns, intraday_source_meta = _intraday_return_map(intraday_files[d])
    rows: List[Dict[str, Any]] = []
    waiting_rows = 0
    missing_price_rows = 0
    intraday_missing_rows = 0
    intraday_not_requested_rows = 0
    intraday_requested_but_missing_price_rows = 0
    intraday_retrospective_history_missing_rows = 0
    intraday_missing_codes_by_reason: Dict[str, List[str]] = {
        "retrospective_history_missing": [],
        "not_requested": [],
        "requested_but_missing_price": [],
        "unknown": [],
    }
    intraday_status_meta = _intraday_status_for_d(d)
    intraday_requested_codes = set(_codes_from_list(intraday_status_meta.get("requested_codes")))
    target_dates: Dict[str, str] = {}
    realized_price_sources: Dict[str, Any] = {}
    for _, r in preview.iterrows():
        code = _norm_code(r.get("code"))
        try:
            row_horizon = int(float(r.get("horizon", horizon)))
        except Exception:
            row_horizon = int(horizon)
        horizon_type = str(r.get("horizon_type", "") or f"D{row_horizon}")
        if row_horizon == 0:
            if d not in intraday_files:
                waiting_rows += 1
                continue
            target_dates[horizon_type] = d
            realized_price_sources[horizon_type] = {
                "type": "intraday_first_to_last_current_price",
                **intraday_source_meta,
            }
            realized_info = intraday_returns.get(code)
            if not code or not realized_info:
                missing_price_rows += 1
                intraday_missing_rows += 1
                if not bool(intraday_status_meta.get("available")) and intraday_status_meta.get("reason") == "RETROSPECTIVE_HISTORY_ONLY":
                    intraday_retrospective_history_missing_rows += 1
                    if code:
                        intraday_missing_codes_by_reason["retrospective_history_missing"].append(code)
                elif intraday_requested_codes and code not in intraday_requested_codes:
                    intraday_not_requested_rows += 1
                    if code:
                        intraday_missing_codes_by_reason["not_requested"].append(code)
                elif intraday_requested_codes and code in intraday_requested_codes:
                    intraday_requested_but_missing_price_rows += 1
                    if code:
                        intraday_missing_codes_by_reason["requested_but_missing_price"].append(code)
                else:
                    if code:
                        intraday_missing_codes_by_reason["unknown"].append(code)
                continue
            ret = float(realized_info["return"])
            rows.append(
                {
                    "code": code,
                    "horizon_type": horizon_type,
                    "horizon": row_horizon,
                    "target_date": d,
                    "price_source": "intraday_first_to_last_current_price",
                    "base_price": realized_info.get("base_price"),
                    "target_price": realized_info.get("target_price"),
                    "base_ts": realized_info.get("base_ts"),
                    "target_ts": realized_info.get("target_ts"),
                    "price_observations": realized_info.get("observations"),
                    "pred_up_prob": r.get("pred_up_prob"),
                    "expected_return": r.get("expected_return"),
                    "confidence": r.get("confidence"),
                    "abstain": str(r.get("abstain", "")).strip().lower() in {"1", "true", "yes"},
                    "realized_return": ret,
                }
            )
            continue
        target = _target_date(d, row_horizon, list(files.keys()))
        if target is None:
            waiting_rows += 1
            continue
        target_dates[horizon_type] = target
        realized_price_sources[horizon_type] = {
            "type": "candidate_close_to_candidate_close",
            "base_file": str(files[d]),
            "target_file": str(files[target]),
        }
        t_prices = _price_map(files[target])
        if not code or code not in d_prices or code not in t_prices:
            missing_price_rows += 1
            continue
        ret = (float(t_prices[code]) / float(d_prices[code])) - 1.0
        rows.append(
            {
                "code": code,
                "horizon_type": horizon_type,
                "horizon": row_horizon,
                "target_date": target,
                "price_source": "candidate_close_to_candidate_close",
                "base_price": d_prices.get(code),
                "target_price": t_prices.get(code),
                "base_ts": "",
                "target_ts": "",
                "price_observations": "",
                "pred_up_prob": r.get("pred_up_prob"),
                "expected_return": r.get("expected_return"),
                "confidence": r.get("confidence"),
                "abstain": str(r.get("abstain", "")).strip().lower() in {"1", "true", "yes"},
                "realized_return": ret,
            }
        )
    realized = pd.DataFrame(rows)
    if len(realized):
        _write_csv(out_realized_csv, realized)
    horizon_summary: List[Dict[str, Any]] = []
    if len(realized):
        for htype, g in realized.groupby("horizon_type", dropna=False):
            horizon_summary.append({
                "horizon_type": str(htype),
                "horizon": int(pd.to_numeric(g["horizon"], errors="coerce").dropna().iloc[0]) if len(pd.to_numeric(g["horizon"], errors="coerce").dropna()) else None,
                "target_date": str(g["target_date"].iloc[0]) if "target_date" in g.columns and len(g) else "",
                "realized_rows": int(len(g)),
                "mean_realized_return": round(float(pd.to_numeric(g["realized_return"], errors="coerce").mean()), 6),
                "hit_rate": round(float((pd.to_numeric(g["realized_return"], errors="coerce") > 0).mean()), 6),
                "spearman_pred_up_prob": _safe_corr(g, "pred_up_prob", "realized_return"),
                "spearman_expected_return": _safe_corr(g, "expected_return", "realized_return"),
            })
    metrics = {
        "realized_rows": int(len(realized)),
        "coverage": round(float(len(realized)) / float(max(1, len(preview))), 6),
        "waiting_rows": int(waiting_rows),
        "missing_price_rows": int(missing_price_rows),
        "intraday_missing_rows": int(intraday_missing_rows),
        "intraday_not_requested_rows": int(intraday_not_requested_rows),
        "intraday_requested_but_missing_price_rows": int(intraday_requested_but_missing_price_rows),
        "intraday_status_mismatch_rows": int(intraday_retrospective_history_missing_rows),
        "intraday_retrospective_history_missing_rows": int(intraday_retrospective_history_missing_rows),
        "intraday_coverage": {
            "status_path": intraday_status_meta.get("status_path"),
            "preferred_status_path": intraday_status_meta.get("preferred_status_path"),
            "fallback_status_path": intraday_status_meta.get("fallback_status_path"),
            "status_D": intraday_status_meta.get("status_D"),
            "validation_D": intraday_status_meta.get("validation_D"),
            "status_matches_validation_D": bool(intraday_status_meta.get("available")),
            "mode": intraday_status_meta.get("mode") or "",
            "reason": intraday_status_meta.get("reason"),
            "requested_code_count": int(len(intraday_requested_codes)),
            "missing_codes_by_reason": {
                k: sorted(set(v)) for k, v in intraday_missing_codes_by_reason.items() if v
            },
        },
        "mean_realized_return": round(float(pd.to_numeric(realized["realized_return"], errors="coerce").mean()), 6) if len(realized) else None,
        "hit_rate": round(float((pd.to_numeric(realized["realized_return"], errors="coerce") > 0).mean()), 6) if len(realized) else None,
        "spearman_pred_up_prob": _safe_corr(realized, "pred_up_prob", "realized_return") if len(realized) else None,
        "spearman_expected_return": _safe_corr(realized, "expected_return", "realized_return") if len(realized) else None,
        "top3": _topk_metrics(realized, 3),
        "top5": _topk_metrics(realized, 5),
        "by_horizon": horizon_summary,
    }
    status = "PASS" if int(metrics["realized_rows"]) > 0 else "FAIL"
    validation_state = "EVALUATED" if status == "PASS" and waiting_rows == 0 else ("EVALUATED_PARTIAL" if status == "PASS" else "WAITING")
    reason = "ok" if validation_state == "EVALUATED" else ("PARTIAL_TARGET_DATE_NOT_AVAILABLE" if status == "PASS" else "TARGET_DATE_NOT_AVAILABLE")
    payload = {
        **base_payload,
        "status": status,
        "validation_state": validation_state,
        "reason": reason,
        "target_dates": target_dates,
        "realized_price_sources": realized_price_sources,
        "realized_rows": int(metrics["realized_rows"]),
        "metrics": metrics,
    }
    _write_json(out_json, payload)
    _write_json(LATEST_JSON, payload)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate future signal preview against realized future prices when available.")
    ap.add_argument("--preview-meta", default=str(DEFAULT_PREVIEW_META))
    ap.add_argument("--output-dir", default=str(LOGS))
    args = ap.parse_args()
    payload = validate_preview(preview_meta_path=Path(args.preview_meta), output_dir=Path(args.output_dir))
    print(
        f"[FUTURE_VALIDATION] status={payload.get('status')} state={payload.get('validation_state')} "
        f"reason={payload.get('reason')} D={payload.get('D')} horizon={payload.get('horizon')} "
        f"target={payload.get('target_date')} rows={payload.get('rows')} realized_rows={payload.get('realized_rows', 0)}"
    )
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
