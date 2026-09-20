from __future__ import annotations

import argparse
import json
import math
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

LATEST_JSON = LOG_DIR / "surge_followthrough_validation_latest.json"
LATEST_CSV = LOG_DIR / "surge_followthrough_validation_latest.csv"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _dated_json_path(d: str, out_dir: Path = LOG_DIR) -> Path:
    return out_dir / f"surge_followthrough_validation_{d}_{_stamp()}.json"


def _dated_csv_path(d: str, out_dir: Path = LOG_DIR) -> Path:
    return out_dir / f"surge_followthrough_validation_{d}_{_stamp()}.csv"


def _norm_code(v: Any) -> str:
    digits = re.sub(r"[^0-9]", "", str(v or ""))
    return digits[-6:].zfill(6) if digits else ""


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None or str(v).strip() == "":
            return None
        x = float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def _safe_bool(v: Any) -> bool:
    return str(v).strip().lower() in {"true", "1", "yes", "y"}


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc, dtype=str).fillna("")
        except pd.errors.EmptyDataError:
            return pd.DataFrame()
        except Exception:
            continue
    return pd.DataFrame()


def _snapshot_meta(path: Path) -> Optional[Tuple[str, str, datetime]]:
    m = re.search(r"surge_realtime_(\d{8})_(\d{6})\.csv$", path.name)
    if not m:
        return None
    ymd, hms = m.group(1), m.group(2)
    try:
        dt = datetime.strptime(ymd + hms, "%Y%m%d%H%M%S")
    except ValueError:
        return None
    return ymd, hms, dt


def _available_snapshot_dates() -> List[str]:
    dates = set()
    for path in LOG_DIR.glob("surge_realtime_*.csv"):
        if path.name == "surge_realtime_latest.csv":
            continue
        meta = _snapshot_meta(path)
        if meta is not None:
            dates.add(meta[0])
    return sorted(dates)


def _default_eval_date_from_files() -> str:
    dates = _available_snapshot_dates()
    if not dates:
        return ""
    today = datetime.now().strftime("%Y%m%d")
    completed = [d for d in dates if d < today]
    return completed[-1] if completed else dates[-1]


def _load_snapshots(date_filter: str = "") -> Tuple[pd.DataFrame, List[str]]:
    frames: List[pd.DataFrame] = []
    skipped: List[str] = []
    for path in sorted(LOG_DIR.glob("surge_realtime_*.csv")):
        if path.name == "surge_realtime_latest.csv":
            continue
        meta = _snapshot_meta(path)
        if meta is None:
            continue
        ymd, hms, dt = meta
        if date_filter and ymd != date_filter:
            continue
        df = _read_csv(path)
        if df.empty:
            skipped.append(str(path))
            continue
        df = df.copy()
        df["_snapshot_date"] = ymd
        df["_snapshot_hms"] = hms
        df["_snapshot_dt"] = dt
        df["_source_file"] = str(path)
        frames.append(df)
    if not frames:
        return pd.DataFrame(), skipped
    out = pd.concat(frames, ignore_index=True)
    if "code" in out.columns:
        out["code"] = out["code"].map(_norm_code)
    for col in (
        "current_price",
        "change_pct",
        "rvol20",
        "trading_value",
        "surge_score",
        "surge_score_final",
        "spread_bps",
        "order_imbalance_l1",
    ):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in ("surge_flag", "is_realtime_surge", "excluded_by_policy", "lob_available"):
        if col in out.columns:
            out[col] = out[col].map(_safe_bool)
    return out.sort_values(["_snapshot_dt", "code"]).reset_index(drop=True), skipped


def _intraday_history_path(d: str) -> Path:
    return LOG_DIR / f"intraday_prices_history_{d}.csv"


def _load_intraday_history(d: str) -> pd.DataFrame:
    path = _intraday_history_path(d)
    df = _read_csv(path)
    if df.empty or not {"ts", "code", "current_price"}.issubset(set(df.columns)):
        return pd.DataFrame()
    out = df.copy()
    out["code"] = out["code"].map(_norm_code)
    out["current_price"] = pd.to_numeric(out["current_price"], errors="coerce")
    out["_snapshot_dt"] = pd.to_datetime(out["ts"], errors="coerce")
    out["_snapshot_date"] = out.get("date", d).astype(str).str.replace(r"\D", "", regex=True).str[:8]
    out["_source_file"] = str(path)
    out["_price_source"] = "intraday_prices_history"
    out = out[
        out["code"].astype(str).str.len().eq(6)
        & out["current_price"].gt(0)
        & out["_snapshot_dt"].notna()
    ].copy()
    return out.sort_values(["_snapshot_dt", "code"]).reset_index(drop=True)


def _default_eval_date(snapshots: pd.DataFrame) -> str:
    if snapshots.empty or "_snapshot_date" not in snapshots.columns:
        return ""
    dates = sorted({str(x) for x in snapshots["_snapshot_date"].dropna().astype(str) if re.fullmatch(r"\d{8}", str(x))})
    if not dates:
        return ""
    today = datetime.now().strftime("%Y%m%d")
    completed = [d for d in dates if d < today]
    return completed[-1] if completed else dates[-1]


def _ret(price: Any, entry: float) -> Optional[float]:
    px = _safe_float(price)
    if px is None or entry <= 0:
        return None
    return float(px / entry - 1.0)


def _first_at_or_after(hist: pd.DataFrame, target_dt: datetime) -> Optional[pd.Series]:
    cand = hist[hist["_snapshot_dt"] >= target_dt].sort_values("_snapshot_dt")
    if cand.empty:
        return None
    return cand.iloc[0]


def _history_for_code(
    *,
    code: str,
    first_dt: datetime,
    d: str,
    snapshots: pd.DataFrame,
    intraday_history: pd.DataFrame,
) -> Tuple[pd.DataFrame, str]:
    if not intraday_history.empty:
        ih = intraday_history[
            (intraday_history["code"].astype(str) == code)
            & (intraday_history["_snapshot_date"].astype(str) == d)
            & (intraday_history["_snapshot_dt"] > first_dt)
        ].copy()
        if not ih.empty:
            return ih.sort_values("_snapshot_dt"), "intraday_prices_history"
    hist = snapshots[
        (snapshots["code"].astype(str) == code)
        & (snapshots["_snapshot_dt"] > first_dt)
        & pd.to_numeric(snapshots["current_price"], errors="coerce").gt(0)
    ].copy()
    if "_price_source" not in hist.columns:
        hist["_price_source"] = "surge_realtime_snapshots"
    return hist.sort_values("_snapshot_dt"), "surge_realtime_snapshots"


def _classify_path(hist_after: pd.DataFrame, entry: float, take_profit: float, stop_loss: float) -> Tuple[str, str, Optional[str], Optional[str]]:
    if hist_after.empty:
        return "INSUFFICIENT", "no_future_snapshot_after_detection", None, None
    work = hist_after.copy()
    work["_ret"] = pd.to_numeric(work["current_price"], errors="coerce") / entry - 1.0
    take = work[work["_ret"] >= take_profit].sort_values("_snapshot_dt")
    stop = work[work["_ret"] <= stop_loss].sort_values("_snapshot_dt")
    take_ts = None if take.empty else take.iloc[0]["_snapshot_dt"].isoformat(timespec="seconds")
    stop_ts = None if stop.empty else stop.iloc[0]["_snapshot_dt"].isoformat(timespec="seconds")
    if not take.empty and stop.empty:
        return "SUCCESS", "take_profit_reached_without_stop_loss", take_ts, stop_ts
    if take.empty and not stop.empty:
        return "RISK", "stop_loss_reached_without_take_profit", take_ts, stop_ts
    if not take.empty and not stop.empty:
        if take.iloc[0]["_snapshot_dt"] <= stop.iloc[0]["_snapshot_dt"]:
            return "MIXED_SUCCESS_THEN_RISK", "take_profit_reached_before_stop_loss", take_ts, stop_ts
        return "RISK_THEN_SUCCESS", "stop_loss_reached_before_take_profit", take_ts, stop_ts
    return "WEAK", "neither_take_profit_nor_stop_loss_reached", take_ts, stop_ts


def _build_rows(snapshots: pd.DataFrame, intraday_history: pd.DataFrame, d: str, take_profit: float, stop_loss: float) -> pd.DataFrame:
    required = {"code", "current_price", "surge_flag", "is_realtime_surge", "excluded_by_policy", "_snapshot_dt", "_snapshot_date"}
    if snapshots.empty or not required.issubset(set(snapshots.columns)):
        return pd.DataFrame()

    signal = snapshots[
        (snapshots["_snapshot_date"].astype(str) == d)
        & (snapshots["surge_flag"] == True)
        & (snapshots["is_realtime_surge"] == True)
        & (snapshots["excluded_by_policy"] == False)
        & snapshots["code"].astype(str).str.len().eq(6)
        & pd.to_numeric(snapshots["current_price"], errors="coerce").gt(0)
    ].copy()
    if signal.empty:
        return pd.DataFrame()

    signal = signal.sort_values(["code", "_snapshot_dt"])
    first = signal.groupby("code", as_index=False).first()
    rows: List[Dict[str, Any]] = []
    for _, sig in first.iterrows():
        code = str(sig.get("code") or "")
        first_dt = sig["_snapshot_dt"]
        entry = float(sig["current_price"])
        hist, price_source = _history_for_code(
            code=code,
            first_dt=first_dt,
            d=d,
            snapshots=snapshots,
            intraday_history=intraday_history,
        )
        same_day = hist[hist["_snapshot_date"].astype(str) == d].copy()
        next_day = snapshots[
            (snapshots["code"].astype(str) == code)
            & (snapshots["_snapshot_dt"] > first_dt)
            & (snapshots["_snapshot_date"].astype(str) > d)
            & pd.to_numeric(snapshots["current_price"], errors="coerce").gt(0)
        ].sort_values("_snapshot_dt").copy()

        label, label_reason, take_ts, stop_ts = _classify_path(same_day, entry, take_profit, stop_loss)

        horizon_rets: Dict[str, Optional[float]] = {}
        horizon_sources: Dict[str, str] = {}
        for minutes in (5, 15, 30):
            rr = _first_at_or_after(same_day, first_dt + timedelta(minutes=minutes))
            key = f"ret_{minutes}m"
            horizon_rets[key] = _ret(rr.get("current_price"), entry) if rr is not None else None
            horizon_sources[f"{key}_source_ts"] = rr["_snapshot_dt"].isoformat(timespec="seconds") if rr is not None else ""

        max_ret = _ret(same_day["current_price"].max(), entry) if not same_day.empty else None
        min_ret = _ret(same_day["current_price"].min(), entry) if not same_day.empty else None
        last_ret = _ret(same_day.iloc[-1]["current_price"], entry) if not same_day.empty else None
        next_open = next_day.iloc[0] if not next_day.empty else None
        next_open_ret = _ret(next_open.get("current_price"), entry) if next_open is not None else None

        rows.append(
            {
                "validation_D": d,
                "code": code,
                "first_detection_ts": first_dt.isoformat(timespec="seconds"),
                "first_detection_file": str(sig.get("_source_file") or ""),
                "surge_type": str(sig.get("surge_type") or ""),
                "entry_price": entry,
                "change_pct": _safe_float(sig.get("change_pct")),
                "rvol20": _safe_float(sig.get("rvol20")),
                "trading_value": _safe_float(sig.get("trading_value")),
                "surge_score": _safe_float(sig.get("surge_score")),
                "surge_score_final": _safe_float(sig.get("surge_score_final")),
                "lob_available": bool(sig.get("lob_available", False)),
                "lob_status": str(sig.get("lob_status") or ""),
                "price_validation_source": price_source,
                "same_day_future_points": int(len(same_day)),
                "max_ret_same_day": max_ret,
                "min_ret_same_day": min_ret,
                "last_ret_same_day": last_ret,
                "next_snapshot_ret": next_open_ret,
                "next_snapshot_ts": next_open["_snapshot_dt"].isoformat(timespec="seconds") if next_open is not None else "",
                "validation_label": label,
                "validation_reason": label_reason,
                "take_profit_first_ts": take_ts or "",
                "stop_loss_first_ts": stop_ts or "",
                **horizon_rets,
                **horizon_sources,
            }
        )
    return pd.DataFrame(rows).sort_values(["validation_label", "max_ret_same_day", "code"], ascending=[True, False, True])


def _summary(rows: pd.DataFrame) -> Dict[str, Any]:
    if rows.empty:
        return {"rows": 0, "labels": {}}
    labels = {str(k): int(v) for k, v in rows["validation_label"].value_counts().to_dict().items()}
    out: Dict[str, Any] = {
        "rows": int(len(rows)),
        "labels": labels,
        "success_like_rows": int(rows["validation_label"].isin(["SUCCESS", "MIXED_SUCCESS_THEN_RISK"]).sum()),
        "risk_like_rows": int(rows["validation_label"].isin(["RISK", "RISK_THEN_SUCCESS"]).sum()),
        "weak_rows": int((rows["validation_label"] == "WEAK").sum()),
        "insufficient_rows": int((rows["validation_label"] == "INSUFFICIENT").sum()),
        "price_source_counts": {
            str(k): int(v) for k, v in rows.get("price_validation_source", pd.Series(dtype=str)).value_counts().to_dict().items()
        },
    }
    for col in ("ret_5m", "ret_15m", "ret_30m", "max_ret_same_day", "min_ret_same_day", "last_ret_same_day", "next_snapshot_ret"):
        vals = pd.to_numeric(rows.get(col), errors="coerce").dropna()
        out[col] = {
            "valid_rows": int(len(vals)),
            "avg_pct": round(float(vals.mean() * 100.0), 4) if len(vals) else None,
            "median_pct": round(float(vals.median() * 100.0), 4) if len(vals) else None,
        }
    return out


def _json_safe(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if pd.isna(obj) if not isinstance(obj, (list, dict, tuple)) else False:
        return None
    return obj


def _newest_detection_age_minutes(rows: pd.DataFrame) -> Optional[float]:
    if rows.empty or "first_detection_ts" not in rows.columns:
        return None
    ts = pd.to_datetime(rows["first_detection_ts"], errors="coerce").dropna()
    if ts.empty:
        return None
    age = datetime.now() - ts.max().to_pydatetime()
    return max(0.0, age.total_seconds() / 60.0)


def build_validation(d: str, take_profit: float, stop_loss: float, min_age_minutes: float = 0.0) -> Tuple[Dict[str, Any], pd.DataFrame]:
    if not d:
        d = _default_eval_date_from_files()
    snapshots, skipped = _load_snapshots(d)
    intraday_history = _load_intraday_history(d) if d else pd.DataFrame()
    rows = _build_rows(snapshots, intraday_history, d, take_profit, stop_loss)
    min_age_minutes = max(0.0, float(min_age_minutes or 0.0))
    newest_detection_age_minutes = _newest_detection_age_minutes(rows)
    waiting_for_window = (
        min_age_minutes > 0.0
        and newest_detection_age_minutes is not None
        and newest_detection_age_minutes < min_age_minutes
    )
    if not d:
        status = "FAIL"
        state = "BLOCKED"
        reason = "VALIDATION_DATE_MISSING"
    elif snapshots.empty:
        status = "FAIL"
        state = "BLOCKED"
        reason = "SURGE_SNAPSHOT_HISTORY_MISSING"
    elif rows.empty:
        status = "WARN"
        state = "NOT_EVALUABLE"
        reason = "NO_EVALUABLE_SURGE_DETECTIONS"
    elif waiting_for_window:
        status = "WARN"
        state = "DEFERRED"
        reason = "WAITING_FOR_FOLLOWTHROUGH_WINDOW"
    elif int((rows["validation_label"] == "INSUFFICIENT").sum()) > 0:
        status = "PASS"
        state = "EVALUATED_PARTIAL"
        reason = "PARTIAL_FUTURE_SNAPSHOTS"
    else:
        status = "PASS"
        state = "EVALUATED"
        reason = "ok"

    payload = {
        "ts": _now(),
        "status": status,
        "validation_state": state,
        "reason": reason,
        "validation_D": d,
        "purpose": "post-detection follow-through validation for realtime surge candidates; no order/fill mutation",
        "criteria": {
            "take_profit_pct": take_profit,
            "stop_loss_pct": stop_loss,
            "min_detection_age_minutes": min_age_minutes,
            "labels": {
                "SUCCESS": "take profit reached and stop loss not reached on same-day later snapshots",
                "WEAK": "neither take profit nor stop loss reached",
                "RISK": "stop loss reached and take profit not reached",
                "MIXED_SUCCESS_THEN_RISK": "take profit reached before later stop loss",
                "RISK_THEN_SUCCESS": "stop loss reached before later take profit",
                "INSUFFICIENT": "no later same-day snapshot for the detected code",
            },
        },
        "inputs": {
            "log_dir": str(LOG_DIR),
            "snapshot_rows": int(len(snapshots)),
            "snapshot_files": int(snapshots["_source_file"].nunique()) if not snapshots.empty else 0,
            "intraday_history_path": str(_intraday_history_path(d)) if d else "",
            "intraday_history_rows": int(len(intraday_history)),
            "intraday_history_codes": int(intraday_history["code"].nunique()) if not intraday_history.empty else 0,
            "skipped_empty_files": skipped[:50],
        },
        "evaluation_window": {
            "min_detection_age_minutes": min_age_minutes,
            "newest_detection_age_minutes": round(float(newest_detection_age_minutes), 2) if newest_detection_age_minutes is not None else None,
            "is_mature": not waiting_for_window,
        },
        "summary": _summary(rows),
        "outputs": {
            "latest_json": str(LATEST_JSON),
            "latest_csv": str(LATEST_CSV),
        },
    }
    return payload, rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate realtime surge detections with later price snapshots.")
    parser.add_argument("--date", default="", help="Validation date in YYYYMMDD. Defaults to latest completed snapshot date.")
    parser.add_argument("--take-profit-pct", type=float, default=0.02)
    parser.add_argument("--stop-loss-pct", type=float, default=-0.015)
    parser.add_argument("--min-age-minutes", type=float, default=0.0, help="Defer final judgment until newest detection is at least this old.")
    parser.add_argument("--out-json", default=str(LATEST_JSON), help="Latest JSON output path.")
    parser.add_argument("--out-csv", default=str(LATEST_CSV), help="Latest CSV output path.")
    parser.add_argument("--history-dir", default=str(LOG_DIR), help="Directory for timestamped validation outputs.")
    args = parser.parse_args()

    latest_json = Path(args.out_json)
    latest_csv = Path(args.out_csv)
    history_dir = Path(args.history_dir)
    latest_json.parent.mkdir(parents=True, exist_ok=True)
    latest_csv.parent.mkdir(parents=True, exist_ok=True)
    history_dir.mkdir(parents=True, exist_ok=True)
    payload, rows = build_validation(
        args.date.strip(),
        float(args.take_profit_pct),
        float(args.stop_loss_pct),
        float(args.min_age_minutes),
    )
    d = str(payload.get("validation_D") or "unknown")
    dated_json = _dated_json_path(d, history_dir)
    dated_csv = _dated_csv_path(d, history_dir)
    payload["outputs"]["latest_json"] = str(latest_json)
    payload["outputs"]["latest_csv"] = str(latest_csv)
    payload["outputs"]["history_json"] = str(dated_json)
    payload["outputs"]["history_csv"] = str(dated_csv)

    latest_json.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    dated_json.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    rows.to_csv(latest_csv, index=False, encoding="utf-8-sig")
    rows.to_csv(dated_csv, index=False, encoding="utf-8-sig")

    print(
        json.dumps(
            {
                "status": payload["status"],
                "validation_state": payload["validation_state"],
                "validation_D": payload["validation_D"],
                "rows": int(len(rows)),
                "latest_json": str(latest_json),
                "latest_csv": str(latest_csv),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
