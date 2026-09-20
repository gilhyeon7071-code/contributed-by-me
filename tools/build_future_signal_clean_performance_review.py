import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"

DEFAULT_CROSS_D_CSV = LOGS / "future_signal_cross_d_validation_latest.csv"
DEFAULT_INTEGRITY_JSON = LOGS / "future_signal_history_integrity_audit_latest.json"
DEFAULT_STALE_CSV = LOGS / "future_signal_history_integrity_stale_rows_latest.csv"
LATEST_JSON = LOGS / "future_signal_clean_performance_review_latest.json"
LATEST_CSV = LOGS / "future_signal_clean_performance_review_latest.csv"
DETAIL_CSV = LOGS / "future_signal_clean_performance_review_details_latest.csv"


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc, dtype=str)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype=str)


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _norm_code(v: Any) -> str:
    s = str(v or "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(6) if s.isdigit() else s


def _norm_ymd(v: Any) -> str:
    s = str(v or "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s[:8]


def _norm_horizon(v: Any) -> str:
    s = str(v or "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def _key(row: pd.Series) -> Tuple[str, str, str]:
    return (_norm_ymd(row.get("D")), _norm_code(row.get("code")), _norm_horizon(row.get("horizon")))


def _stale_keys(stale: pd.DataFrame) -> Set[Tuple[str, str, str]]:
    if stale.empty:
        return set()
    return {_key(r) for _, r in stale.iterrows()}


def _safe_corr(df: pd.DataFrame, x: str, y: str) -> Any:
    if df.empty or x not in df.columns or y not in df.columns:
        return None
    xs = pd.to_numeric(df[x], errors="coerce")
    ys = pd.to_numeric(df[y], errors="coerce")
    valid = xs.notna() & ys.notna()
    if int(valid.sum()) < 3 or xs[valid].nunique() < 2 or ys[valid].nunique() < 2:
        return None
    return round(float(xs[valid].corr(ys[valid], method="spearman")), 6)


def _metrics(label: str, df: pd.DataFrame) -> Dict[str, Any]:
    ret = pd.to_numeric(df.get("realized_return", pd.Series(dtype=float)), errors="coerce")
    pred = pd.to_numeric(df.get("pred_up_prob", pd.Series(dtype=float)), errors="coerce")
    out: Dict[str, Any] = {
        "sample": label,
        "rows": int(len(df)),
        "mean_return": round(float(ret.mean()), 6) if len(df) else None,
        "hit_rate": round(float((ret > 0).mean()), 6) if len(df) else None,
        "spearman_pred_up_prob": _safe_corr(df, "pred_up_prob", "realized_return"),
        "mean_pred_up_prob": round(float(pred.mean()), 6) if len(df) else None,
    }
    return out


def _metric_rows(label: str, df: pd.DataFrame) -> List[Dict[str, Any]]:
    rows = [_metrics(label, df)]
    if len(df) and "horizon_type" in df.columns:
        for horizon, g in df.groupby("horizon_type", dropna=False):
            item = _metrics(f"{label}:{horizon}", g)
            item["horizon_type"] = str(horizon)
            rows.append(item)
    return rows


def _bool_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    return df[col].astype(str).str.strip().str.lower().isin({"1", "1.0", "true", "yes", "y"})


def build_review(cross_d_csv: Path, stale_csv: Path, integrity_json: Path, output_json: Path, output_csv: Path, detail_csv: Path) -> Dict[str, Any]:
    generated_at = datetime.now().isoformat(timespec="seconds")
    if not cross_d_csv.exists():
        payload = {
            "generated_at": generated_at,
            "status": "FAIL",
            "reason": "CROSS_D_CSV_MISSING",
            "cross_d_csv": str(cross_d_csv),
            "policy": _policy(),
        }
        _write_json(output_json, payload)
        return payload
    if not stale_csv.exists():
        payload = {
            "generated_at": generated_at,
            "status": "FAIL",
            "reason": "STALE_ROWS_CSV_MISSING",
            "stale_csv": str(stale_csv),
            "policy": _policy(),
        }
        _write_json(output_json, payload)
        return payload

    realized = _read_csv(cross_d_csv)
    stale = _read_csv(stale_csv)
    stale_key_set = _stale_keys(stale)
    if realized.empty:
        payload = {
            "generated_at": generated_at,
            "status": "WAITING",
            "reason": "NO_REALIZED_ROWS",
            "cross_d_csv": str(cross_d_csv),
            "policy": _policy(),
        }
        _write_json(output_json, payload)
        return payload

    for col in ("D", "code", "horizon", "horizon_type"):
        if col not in realized.columns:
            realized[col] = ""
    realized["_clean_key"] = realized.apply(_key, axis=1)
    realized["excluded_by_history_integrity"] = realized["_clean_key"].map(lambda k: k in stale_key_set)
    realized["exclude_market_event_day"] = _bool_series(realized, "exclude_market_event_day")
    clean = realized[~realized["excluded_by_history_integrity"]].copy()
    excluded = realized[realized["excluded_by_history_integrity"]].copy()
    clean_excl_market_events = clean[~clean["exclude_market_event_day"]].copy()
    market_event_excluded = clean[clean["exclude_market_event_day"]].copy()

    summary_rows = (
        _metric_rows("raw", realized)
        + _metric_rows("clean", clean)
        + _metric_rows("clean_excl_market_events", clean_excl_market_events)
        + _metric_rows("excluded_by_history_integrity", excluded)
        + _metric_rows("excluded_market_event_days", market_event_excluded)
    )
    summary = pd.DataFrame(summary_rows)
    detail = realized.drop(columns=["_clean_key"], errors="ignore").copy()
    _write_csv(output_csv, summary)
    _write_csv(detail_csv, detail)

    integrity = _read_json(integrity_json)
    payload = {
        "generated_at": generated_at,
        "status": "PASS",
        "reason": "ok",
        "cross_d_csv": str(cross_d_csv),
        "stale_csv": str(stale_csv),
        "integrity_json": str(integrity_json),
        "raw_realized_rows": int(len(realized)),
        "clean_realized_rows": int(len(clean)),
        "clean_excl_market_events_rows": int(len(clean_excl_market_events)),
        "excluded_realized_rows": int(len(excluded)),
        "excluded_realized_rate": round(float(len(excluded)) / float(max(1, len(realized))), 6),
        "excluded_market_event_day_rows": int(len(market_event_excluded)),
        "excluded_market_event_day_rate_from_clean": round(float(len(market_event_excluded)) / float(max(1, len(clean))), 6),
        "history_integrity_stale_or_unmatched_rows": integrity.get("stale_or_unmatched_rows"),
        "history_integrity_row_reason_breakdown": integrity.get("row_reason_breakdown"),
        "metrics": {
            "raw": _metrics("raw", realized),
            "clean": _metrics("clean", clean),
            "clean_history_integrity": _metrics("clean_history_integrity", clean),
            "clean_excl_market_events": _metrics("clean_excl_market_events", clean_excl_market_events),
            "excluded_by_history_integrity": _metrics("excluded_by_history_integrity", excluded),
            "excluded_market_event_days": _metrics("excluded_market_event_days", market_event_excluded),
        },
        "outputs": {
            "json": str(output_json),
            "csv": str(output_csv),
            "details_csv": str(detail_csv),
        },
        "policy": _policy(),
    }
    _write_json(output_json, payload)
    return payload


def _policy() -> Dict[str, Any]:
    return {
        "diagnostic_only": True,
        "used_for_trading": False,
        "cross_d_validation_modified": False,
        "history_modified": False,
        "orders_modified": False,
        "fills_modified": False,
        "ledger_modified": False,
        "stats_modified": False,
        "gate_modified": False,
        "risk_lock_modified": False,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Compare raw future-signal realized performance with history-integrity-clean performance.")
    ap.add_argument("--cross-d-csv", default=str(DEFAULT_CROSS_D_CSV))
    ap.add_argument("--stale-csv", default=str(DEFAULT_STALE_CSV))
    ap.add_argument("--integrity-json", default=str(DEFAULT_INTEGRITY_JSON))
    ap.add_argument("--output-json", default=str(LATEST_JSON))
    ap.add_argument("--output-csv", default=str(LATEST_CSV))
    ap.add_argument("--detail-csv", default=str(DETAIL_CSV))
    args = ap.parse_args()

    payload = build_review(
        cross_d_csv=Path(args.cross_d_csv),
        stale_csv=Path(args.stale_csv),
        integrity_json=Path(args.integrity_json),
        output_json=Path(args.output_json),
        output_csv=Path(args.output_csv),
        detail_csv=Path(args.detail_csv),
    )
    print(
        f"[FUTURE_SIGNAL_CLEAN_PERF] status={payload.get('status')} reason={payload.get('reason')} "
        f"raw_rows={payload.get('raw_realized_rows', 0)} clean_rows={payload.get('clean_realized_rows', 0)} "
        f"excluded_rows={payload.get('excluded_realized_rows', 0)}"
    )
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
