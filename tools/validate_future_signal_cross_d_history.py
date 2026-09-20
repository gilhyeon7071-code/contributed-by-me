from __future__ import annotations

import argparse
import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"

DEFAULT_HISTORY = LOGS / "future_signal_preview_history.csv"
LATEST_JSON = LOGS / "future_signal_cross_d_validation_latest.json"
LATEST_CSV = LOGS / "future_signal_cross_d_validation_latest.csv"
OHLCV_PAPER_PARQUET = ROOT / "paper" / "prices" / "ohlcv_paper.parquet"
MARKET_EVENT_LOG_GLOB = "market_event_log_*.json"

SAMPLE_COLLECTION_POLICY = {
    "policy_name": "FUTURE_SIGNAL_SAMPLE_COLLECTION_V0",
    "scope": "review_readiness_only",
    "used_for_trading": False,
    "min_unique_signal_days": 20,
    "min_realized_rows_total": 100,
    "min_coverage": 0.25,
    "min_realized_rows_by_horizon": {
        "INTRADAY": 40,
        "SHORT": 30,
        "SWING": 30,
        "MID": 20,
        "LONG": 20,
    },
}


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc, dtype=str)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype=str)


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8-sig"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _norm_date8(v: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s[:8] if len(s) >= 8 else ""


def _market_event_days() -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for path in LOGS.glob(MARKET_EVENT_LOG_GLOB):
        if path.name.endswith("_latest.json"):
            continue
        doc = _read_json(path)
        d = _norm_date8(doc.get("D") or path.name)
        if not d:
            continue
        sidecar = bool(doc.get("sidecar", False))
        circuit = bool(doc.get("circuit_breaker", False))
        if sidecar or circuit:
            out[d] = {
                "D": d,
                "sidecar": sidecar,
                "circuit_breaker": circuit,
                "source": str(path),
            }
    return out


def _norm_code(v: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s.zfill(6)[-6:] if s else ""


def _num(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
    except Exception:
        return default
    if math.isnan(x) or math.isinf(x):
        return default
    return x


def _candidate_price_files() -> Dict[str, Path]:
    out: Dict[str, Path] = {}
    for p in LOGS.glob("candidates_v41_1_*.csv"):
        m = re.search(r"(\d{8})", p.name)
        if m:
            out[m.group(1)] = p
    return out


def _intraday_price_files() -> Dict[str, Path]:
    out: Dict[str, Path] = {}
    for p in LOGS.glob("intraday_prices_history_*.csv"):
        m = re.search(r"(\d{8})", p.name)
        if m:
            out[m.group(1)] = p
    return out


def _intraday_return_map(path: Path) -> Dict[str, float]:
    """Returns {code: first_to_last_return} from intraday history CSV."""
    df = _read_csv(path)
    required = {"code", "ts", "current_price"}
    if not required.issubset(set(df.columns)):
        return {}
    work = df.copy()
    work["code_norm"] = work["code"].map(_norm_code)
    work["price_num"] = pd.to_numeric(work["current_price"], errors="coerce")
    work["ts_str"] = work["ts"].astype(str)
    work = work[(work["code_norm"].astype(str).str.len() == 6) & (work["price_num"] > 0)]
    out: Dict[str, float] = {}
    for code, g in work.sort_values("ts_str").groupby("code_norm", dropna=False):
        if len(g) < 2:
            continue
        base = float(g.iloc[0]["price_num"])
        target = float(g.iloc[-1]["price_num"])
        if base > 0 and target > 0:
            out[str(code)] = (target / base) - 1.0
    return out


def _intraday_missing_reason(path: Path, code: str) -> str:
    df = _read_csv(path)
    required = {"code", "ts", "current_price"}
    if not required.issubset(set(df.columns)):
        return "intraday_file_schema_missing"
    work = df.copy()
    work["code_norm"] = work["code"].map(_norm_code)
    code_rows = work[work["code_norm"] == code].copy()
    if len(code_rows) == 0:
        return "intraday_code_absent"
    prices = pd.to_numeric(code_rows["current_price"], errors="coerce")
    valid_prices = prices[prices > 0]
    if len(valid_prices) == 0:
        return "intraday_no_valid_price"
    if len(valid_prices) == 1:
        return "intraday_single_valid_tick"
    return "intraday_return_unavailable"


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
        close = _num(r.get("close"), 0.0)
        if code and close > 0:
            out[code] = close
    return out


def _ohlcv_paper_close_maps(path: Path = OHLCV_PAPER_PARQUET) -> Dict[str, Dict[str, float]]:
    if not path.exists():
        return {}
    try:
        df = pd.read_parquet(path)
    except Exception:
        return {}
    cols = {str(c).lower(): c for c in df.columns}
    date_col = next((cols[k] for k in ("date", "ymd", "datetime", "dt") if k in cols), None)
    code_col = next((cols[k] for k in ("code", "ticker", "symbol") if k in cols), None)
    close_col = next((cols[k] for k in ("close", "adj_close") if k in cols), None)
    if not date_col or not code_col or not close_col:
        return {}
    work = df[[date_col, code_col, close_col]].copy()
    work.columns = ["date", "code", "close"]
    work["date"] = work["date"].map(_norm_date8)
    work["code"] = work["code"].map(_norm_code)
    work["close"] = pd.to_numeric(work["close"], errors="coerce")
    work = work[(work["date"].astype(str).str.len() == 8) & (work["code"].astype(str).str.len() == 6) & (work["close"] > 0)]
    out: Dict[str, Dict[str, float]] = {}
    for date, g in work.sort_values(["date", "code"]).groupby("date", dropna=False):
        out[str(date)] = {str(r.code): float(r.close) for r in g[["code", "close"]].itertuples(index=False)}
    return out


def _safe_corr(df: pd.DataFrame, left: str, right: str) -> Optional[float]:
    if len(df) < 3:
        return None
    work = pd.DataFrame({
        "x": pd.to_numeric(df[left], errors="coerce"),
        "y": pd.to_numeric(df[right], errors="coerce"),
    }).dropna()
    if len(work) < 3:
        return None
    val = float(work["x"].corr(work["y"], method="spearman"))
    if math.isnan(val) or math.isinf(val):
        return None
    return round(val, 6)


def _canonical_horizon_type(v: Any) -> str:
    s = str(v or "").upper().strip()
    if s == "D0":
        return "INTRADAY"
    if s == "D2":
        return "SHORT"
    if s == "D5":
        return "SWING"
    if s == "D15":
        return "MID"
    if s == "D30":
        return "LONG"
    return s


def _sample_collection_state(history: pd.DataFrame, realized: pd.DataFrame) -> Dict[str, Any]:
    policy = SAMPLE_COLLECTION_POLICY
    unique_signal_days = int(history["D"].nunique()) if "D" in history.columns else 0
    history_rows = int(len(history))
    realized_rows = int(len(realized))
    coverage = float(realized_rows) / float(max(1, history_rows))
    by_horizon: Dict[str, int] = {str(k): 0 for k in policy["min_realized_rows_by_horizon"].keys()}
    if len(realized) and "horizon_type" in realized.columns:
        h = realized["horizon_type"].map(_canonical_horizon_type)
        counts = h.value_counts().to_dict()
        for key in by_horizon:
            by_horizon[key] = int(counts.get(key, 0))

    unmet: List[str] = []
    if unique_signal_days < int(policy["min_unique_signal_days"]):
        unmet.append(f"unique_signal_days:{unique_signal_days}<{policy['min_unique_signal_days']}")
    if realized_rows < int(policy["min_realized_rows_total"]):
        unmet.append(f"realized_rows:{realized_rows}<{policy['min_realized_rows_total']}")
    if coverage < float(policy["min_coverage"]):
        unmet.append(f"coverage:{coverage:.6f}<{policy['min_coverage']}")
    for horizon, minimum in policy["min_realized_rows_by_horizon"].items():
        actual = int(by_horizon.get(str(horizon), 0))
        if actual < int(minimum):
            unmet.append(f"{horizon}_rows:{actual}<{minimum}")

    return {
        "state": "COLLECTING" if unmet else "REVIEW_READY",
        "review_ready": not bool(unmet),
        "used_for_trading": False,
        "unique_signal_days": unique_signal_days,
        "history_rows": history_rows,
        "realized_rows": realized_rows,
        "coverage": round(coverage, 6),
        "realized_rows_by_horizon": by_horizon,
        "unmet_requirements": unmet,
    }


def _add_missing_price_diag(diag: Dict[str, Dict[str, Any]], reason: str, row: Dict[str, Any]) -> None:
    item = diag.setdefault(reason, {"rows": 0, "samples": []})
    item["rows"] = int(item.get("rows", 0)) + 1
    samples = item.setdefault("samples", [])
    if len(samples) < 20:
        samples.append(row)


def _bump_nested_count(container: Dict[str, Any], key: str, subkey: str) -> None:
    bucket = container.setdefault(key, {})
    bucket[subkey] = int(bucket.get(subkey, 0)) + 1


def _dedupe_history(history: pd.DataFrame) -> pd.DataFrame:
    work = history.copy()
    if "D" not in work.columns or "code" not in work.columns:
        return pd.DataFrame()
    work["D"] = work["D"].map(_norm_date8)
    work["code"] = work["code"].map(_norm_code)
    if "horizon" not in work.columns:
        work["horizon"] = ""
    if "horizon_type" not in work.columns:
        work["horizon_type"] = ""
    work["horizon_num"] = pd.to_numeric(work["horizon"], errors="coerce").fillna(5).astype(int)
    work["horizon_type_norm"] = work["horizon_type"].fillna("").astype(str).str.upper()
    work.loc[work["horizon_type_norm"].isin({"", "NAN", "NONE"}), "horizon_type_norm"] = (
        "D" + work["horizon_num"].astype(str)
    )
    if "recorded_at" in work.columns:
        work["_recorded_at_sort"] = work["recorded_at"].astype(str)
    else:
        work["_recorded_at_sort"] = ""
    work = work[(work["D"].astype(str).str.len() == 8) & (work["code"].astype(str).str.len() == 6)]
    work = work.sort_values(["D", "code", "horizon_num", "horizon_type_norm", "_recorded_at_sort"])
    return work.drop_duplicates(subset=["D", "code", "horizon_num", "horizon_type_norm"], keep="last")


def build_cross_d_validation(*, history_path: Path, output_json: Path, output_csv: Path) -> Dict[str, Any]:
    if not history_path.exists():
        payload = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "status": "FAIL",
            "reason": "HISTORY_CSV_MISSING",
            "history_csv": str(history_path),
            "rows": 0,
            "policy": {
                "diagnostic_only": True,
                "used_for_trading": False,
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

    history = _dedupe_history(_read_csv(history_path))
    price_files = _candidate_price_files()
    price_dates = sorted(price_files)
    intraday_files = _intraday_price_files()
    ohlcv_paper_prices = _ohlcv_paper_close_maps()
    market_event_days = _market_event_days()
    price_cache: Dict[str, Dict[str, float]] = {}
    intraday_cache: Dict[str, Dict[str, float]] = {}
    rows: List[Dict[str, Any]] = []
    waiting_rows = 0
    missing_price_rows = 0
    unsupported_horizon_rows = 0
    missing_price_breakdown: Dict[str, Dict[str, Any]] = {}
    missing_price_by_date: Dict[str, Any] = {}
    intraday_history_coverage_by_date: Dict[str, Any] = {}
    realized_price_source_counts: Dict[str, int] = {}

    for _, r in history.iterrows():
        d = _norm_date8(r.get("D"))
        code = _norm_code(r.get("code"))
        horizon = int(_num(r.get("horizon_num"), 5.0))
        horizon_type = str(r.get("horizon_type_norm") or f"D{horizon}")
        if horizon < 0:
            unsupported_horizon_rows += 1
            continue
        # INTRADAY (horizon==0): use intraday first-to-last return
        if horizon == 0:
            if d not in intraday_files:
                waiting_rows += 1
                continue
            if d not in intraday_cache:
                intraday_cache[d] = _intraday_return_map(intraday_files[d])
                intraday_history_coverage_by_date[d] = {
                    "history_csv": str(intraday_files[d]),
                    "codes_with_return": int(len(intraday_cache[d])),
                    "missing_rows": 0,
                    "missing_by_reason": {},
                }
            ret_val = intraday_cache[d].get(code)
            if ret_val is None:
                missing_price_rows += 1
                reason = _intraday_missing_reason(intraday_files[d], code)
                _bump_nested_count(missing_price_by_date, d, reason)
                intraday_diag = intraday_history_coverage_by_date.setdefault(
                    d,
                    {
                        "history_csv": str(intraday_files[d]),
                        "codes_with_return": int(len(intraday_cache.get(d, {}))),
                        "missing_rows": 0,
                        "missing_by_reason": {},
                    },
                )
                intraday_diag["missing_rows"] = int(intraday_diag.get("missing_rows", 0)) + 1
                intraday_diag["missing_by_reason"][reason] = int(intraday_diag["missing_by_reason"].get(reason, 0)) + 1
                _add_missing_price_diag(
                    missing_price_breakdown,
                    reason,
                    {"D": d, "code": code, "horizon_type": horizon_type, "horizon": horizon},
                )
                continue
            realized_return = float(ret_val)
            rows.append({
                "D": d,
                "code": code,
                "name": str(r.get("name") or ""),
                "horizon_type": horizon_type,
                "horizon": horizon,
                "target_date": d,
                "pred_up_prob": r.get("pred_up_prob"),
                "expected_return": r.get("expected_return"),
                "confidence": r.get("confidence"),
                "abstain": r.get("abstain"),
                "base_price": None,
                "target_price": None,
                "realized_return": round(realized_return, 8),
                "actual_up": 1 if realized_return > 0 else 0,
                "exclude_market_event_day": bool(d in market_event_days),
                "market_event_sidecar": bool(market_event_days.get(d, {}).get("sidecar", False)),
                "market_event_circuit_breaker": bool(market_event_days.get(d, {}).get("circuit_breaker", False)),
                "used_for_trading": False,
            })
            continue
        target = _target_date(d, horizon, price_dates)
        if target is None:
            waiting_rows += 1
            continue
        if d not in price_cache:
            price_cache[d] = _price_map(price_files[d])
        if target not in price_cache:
            price_cache[target] = _price_map(price_files[target])
        base_price = price_cache[d].get(code)
        target_price = price_cache[target].get(code)
        price_source = "candidate_close_to_candidate_close"
        if not base_price and code in ohlcv_paper_prices.get(d, {}):
            base_price = ohlcv_paper_prices[d].get(code)
            price_source = "ohlcv_paper_close_fallback"
        if not target_price and code in ohlcv_paper_prices.get(target, {}):
            target_price = ohlcv_paper_prices[target].get(code)
            price_source = "ohlcv_paper_close_fallback"
        if not base_price or not target_price:
            missing_price_rows += 1
            if not base_price and not target_price:
                reason = "base_and_target_daily_price_missing"
            elif not base_price:
                reason = "base_daily_price_missing"
            else:
                reason = "target_daily_price_missing"
            _bump_nested_count(missing_price_by_date, d, reason)
            _add_missing_price_diag(
                missing_price_breakdown,
                reason,
                {"D": d, "target_date": target, "code": code, "horizon_type": horizon_type, "horizon": horizon},
            )
            continue
        realized_price_source_counts[price_source] = int(realized_price_source_counts.get(price_source, 0)) + 1
        realized_return = (float(target_price) / float(base_price)) - 1.0
        rows.append({
            "D": d,
            "code": code,
            "name": str(r.get("name") or ""),
            "horizon_type": horizon_type,
            "horizon": horizon,
            "target_date": target,
            "pred_up_prob": r.get("pred_up_prob"),
            "expected_return": r.get("expected_return"),
            "confidence": r.get("confidence"),
            "abstain": r.get("abstain"),
            "base_price": round(float(base_price), 6),
            "target_price": round(float(target_price), 6),
            "price_source": price_source,
            "realized_return": round(float(realized_return), 8),
            "actual_up": 1 if realized_return > 0 else 0,
            "exclude_market_event_day": bool(d in market_event_days),
            "market_event_sidecar": bool(market_event_days.get(d, {}).get("sidecar", False)),
            "market_event_circuit_breaker": bool(market_event_days.get(d, {}).get("circuit_breaker", False)),
            "used_for_trading": False,
        })

    realized = pd.DataFrame(rows)
    if len(realized):
        _write_csv(output_csv, realized)
    by_horizon: List[Dict[str, Any]] = []
    if len(realized):
        for htype, g in realized.groupby("horizon_type", dropna=False):
            by_horizon.append({
                "horizon_type": str(htype),
                "rows": int(len(g)),
                "mean_realized_return": round(float(pd.to_numeric(g["realized_return"], errors="coerce").mean()), 6),
                "hit_rate": round(float((pd.to_numeric(g["realized_return"], errors="coerce") > 0).mean()), 6),
                "spearman_pred_up_prob": _safe_corr(g, "pred_up_prob", "realized_return"),
                "spearman_expected_return": _safe_corr(g, "expected_return", "realized_return"),
            })
    sample_state = _sample_collection_state(history, realized)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS" if len(realized) else "WAITING",
        "reason": "ok" if len(realized) else "NO_REALIZED_ROWS_AVAILABLE",
        "history_csv": str(history_path),
        "outputs": {
            "json": str(output_json),
            "csv": str(output_csv) if len(realized) else "",
        },
        "history_rows": int(len(history)),
        "realized_rows": int(len(realized)),
        "market_event_days": sorted(market_event_days),
        "market_event_log_count": int(len(market_event_days)),
        "exclude_market_event_day_rows": int(realized["exclude_market_event_day"].astype(bool).sum()) if len(realized) and "exclude_market_event_day" in realized.columns else 0,
        "waiting_rows": int(waiting_rows),
        "missing_price_rows": int(missing_price_rows),
        "missing_price_breakdown": missing_price_breakdown,
        "missing_price_by_date": missing_price_by_date,
        "intraday_history_coverage_by_date": intraday_history_coverage_by_date,
        "daily_price_fallback": {
            "source": str(OHLCV_PAPER_PARQUET),
            "available": bool(ohlcv_paper_prices),
            "date_count": int(len(ohlcv_paper_prices)),
            "used_rows": int(realized_price_source_counts.get("ohlcv_paper_close_fallback", 0)),
            "candidate_price_rows": int(realized_price_source_counts.get("candidate_close_to_candidate_close", 0)),
            "used_for_trading": False,
        },
        "unsupported_horizon_rows": int(unsupported_horizon_rows),
        "available_price_dates_min": min(price_dates) if price_dates else "",
        "available_price_dates_max": max(price_dates) if price_dates else "",
        "available_intraday_dates_min": min(intraday_files.keys()) if intraday_files else "",
        "available_intraday_dates_max": max(intraday_files.keys()) if intraday_files else "",
        "metrics": {
            "realized_rows": int(len(realized)),
            "coverage": round(float(len(realized)) / float(max(1, len(history))), 6),
            "mean_realized_return": round(float(pd.to_numeric(realized["realized_return"], errors="coerce").mean()), 6) if len(realized) else None,
            "hit_rate": round(float((pd.to_numeric(realized["realized_return"], errors="coerce") > 0).mean()), 6) if len(realized) else None,
            "spearman_pred_up_prob": _safe_corr(realized, "pred_up_prob", "realized_return") if len(realized) else None,
            "spearman_expected_return": _safe_corr(realized, "expected_return", "realized_return") if len(realized) else None,
            "by_horizon": by_horizon,
        },
        "sample_collection_policy": SAMPLE_COLLECTION_POLICY,
        "sample_collection_state": sample_state,
        "policy": {
            "diagnostic_only": True,
            "used_for_trading": False,
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
    ap = argparse.ArgumentParser(description="Diagnostic cross-D validation for future-signal preview history.")
    ap.add_argument("--history", default=str(DEFAULT_HISTORY))
    ap.add_argument("--output-json", default=str(LATEST_JSON))
    ap.add_argument("--output-csv", default=str(LATEST_CSV))
    args = ap.parse_args()
    payload = build_cross_d_validation(
        history_path=Path(args.history),
        output_json=Path(args.output_json),
        output_csv=Path(args.output_csv),
    )
    print(
        f"[FUTURE_CROSS_D_VALIDATION] status={payload.get('status')} reason={payload.get('reason')} "
        f"history_rows={payload.get('history_rows', 0)} realized_rows={payload.get('realized_rows', 0)} "
        f"waiting_rows={payload.get('waiting_rows', 0)}"
    )
    return 0 if payload.get("status") in {"PASS", "WAITING"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
