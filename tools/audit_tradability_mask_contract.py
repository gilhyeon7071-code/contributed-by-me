# -*- coding: utf-8 -*-
"""Read-only audit for tradability-mask style protections.

This does not change trading policy or write operational inputs.  It inspects
the latest RootA artifacts and reports whether daily candidates and realtime
surge paths expose enough evidence to avoid using non-tradable prices.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        return {"_read_error": f"{type(exc).__name__}:{exc}"}


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")
    except Exception:
        try:
            return pd.read_csv(path, dtype=str).fillna("")
        except Exception:
            return pd.DataFrame()


def _bool_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([False] * len(df), index=df.index, dtype=bool)
    return df[col].astype(str).str.strip().str.lower().isin({"1", "true", "t", "yes", "y", "on"})


def _num(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    if col not in df.columns:
        return pd.Series([default] * len(df), index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors="coerce").fillna(default)


def _bounded_krx_glob() -> list[Path]:
    # Bounded, non-recursive scan (mirrors p0_daily_check.py:_krx_clean_files())
    # so this audit does not also pick up unrelated backup/tmp copies via rglob.
    out: list[Path] = []
    seen: set[str] = set()
    for d in (ROOT / "_krx_manual", ROOT / "krx_daily_archive", ROOT):
        if not d.exists() or not d.is_dir():
            continue
        for p in d.glob("krx_daily_*_clean.parquet"):
            key = str(p.resolve())
            if key in seen:
                continue
            seen.add(key)
            out.append(p)
    return out


def _latest_krx_clean_columns(limit: int = 4) -> dict[str, Any]:
    paths = sorted(_bounded_krx_glob(), key=lambda p: p.stat().st_mtime, reverse=True)[:limit]
    out: list[dict[str, Any]] = []
    tradability_terms = {
        "halt",
        "suspend",
        "suspended",
        "tradable",
        "is_tradable",
        "trading_status",
        "status",
        "limit_up",
        "limit_down",
        "upper_limit",
        "lower_limit",
        "tradability_checked",
        "tradability_blocked",
        "tradability_reason",
        "tradability_source",
    }
    found_terms: set[str] = set()
    for path in paths:
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            out.append({"path": str(path), "read_error": type(exc).__name__})
            continue
        cols = [str(c) for c in df.columns]
        lowered = {c.lower() for c in cols}
        matched = sorted(c for c in lowered if c in tradability_terms)
        found_terms.update(matched)
        entry: dict[str, Any] = {
            "path": str(path),
            "rows": int(len(df)),
            "columns": cols,
            "tradability_columns": matched,
        }
        if "date" in df.columns:
            dates = pd.to_datetime(df["date"], errors="coerce")
            entry["date_min"] = str(dates.min().date()) if dates.notna().any() else None
            entry["date_max"] = str(dates.max().date()) if dates.notna().any() else None
        out.append(entry)
    return {
        "files_checked": out,
        "tradability_columns_found": sorted(found_terms),
        "has_explicit_tradability_columns": bool(found_terms),
    }


def _audit_krx_integrity_sidecar() -> dict[str, Any]:
    path = LOG_DIR / "krx_price_integrity_status_latest.json"
    data = _read_json(path)
    examples = data.get("blocked_examples", [])
    reason_counts: dict[str, int] = {}
    if isinstance(examples, list):
        for row in examples:
            if not isinstance(row, dict):
                continue
            reasons = row.get("reasons", [])
            if isinstance(reasons, str):
                reasons = [reasons]
            if not isinstance(reasons, list):
                continue
            for reason in reasons:
                key = str(reason)
                reason_counts[key] = reason_counts.get(key, 0) + 1
    return {
        "path": str(path),
        "exists": path.exists(),
        "read_error": data.get("_read_error"),
        "generated_at": data.get("generated_at"),
        "status": data.get("status"),
        "range_from": data.get("range_from"),
        "range_to": data.get("range_to"),
        "latest_source": data.get("latest_source"),
        "out_path": data.get("out_path"),
        "blocked_rows": data.get("blocked_rows"),
        "tolerated_zero_ohlc_blocks": data.get("tolerated_zero_ohlc_blocks"),
        "soft_skipped_days_count": len(data.get("soft_skipped_days", []) or []),
        "blocked_example_reason_counts": reason_counts,
    }


def _audit_krx_watchlist_snapshot() -> dict[str, Any]:
    latest = ROOT / "_cache" / "krx_watchlist_latest.csv"
    latest_df = _read_csv(latest)
    as_of_paths = sorted((ROOT / "_cache").glob("krx_watchlist_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)[:5]
    snapshots: list[dict[str, Any]] = []
    for path in as_of_paths:
        df = _read_csv(path)
        snapshots.append({
            "path": str(path),
            "rows": int(len(df)),
            "columns": [str(c) for c in df.columns],
        })
    return {
        "latest_path": str(latest),
        "latest_exists": latest.exists(),
        "latest_rows": int(len(latest_df)),
        "latest_flag_counts": {
            "krx_admin": int(_bool_series(latest_df, "krx_admin").sum()) if len(latest_df) else 0,
            "krx_warning": int(_bool_series(latest_df, "krx_warning").sum()) if len(latest_df) else 0,
            "krx_risk": int(_bool_series(latest_df, "krx_risk").sum()) if len(latest_df) else 0,
            "krx_caution": int(_bool_series(latest_df, "krx_caution").sum()) if len(latest_df) else 0,
        },
        "recent_snapshots": snapshots,
    }


def _codes_from_csv(path: Path) -> list[str]:
    df = _read_csv(path)
    if "code" not in df.columns:
        return []
    return sorted(set(str(v).strip().zfill(6) for v in df["code"] if str(v).strip()))


def _open_position_codes() -> list[str]:
    state = _read_json(ROOT / "paper" / "paper_state.json")
    pos = state.get("open_positions", []) if isinstance(state, dict) else []
    if not isinstance(pos, list):
        return []
    out: list[str] = []
    for row in pos:
        if not isinstance(row, dict):
            continue
        raw = str(row.get("code", "")).strip()
        digits = "".join(ch for ch in raw if ch.isdigit())
        if digits:
            out.append(digits.zfill(6))
    return sorted(set(out))


def _positive_history_for_codes(history_path: Path, codes: list[str]) -> dict[str, Any]:
    hist = _read_csv(history_path)
    if hist.empty or "code" not in hist.columns or "current_price" not in hist.columns:
        return {"path": str(history_path), "exists": history_path.exists(), "by_code": {}}
    hist = hist.copy()
    hist["code"] = hist["code"].astype(str).str.strip().str.zfill(6)
    hist["_current_price_num"] = pd.to_numeric(hist["current_price"], errors="coerce").fillna(0)
    out: dict[str, Any] = {}
    for code in codes:
        sub = hist[(hist["code"] == code) & (hist["_current_price_num"] > 0)].copy()
        last = sub.tail(1)
        out[code] = {
            "positive_rows": int(len(sub)),
            "last_ts": str(last["ts"].iloc[0]) if len(last) and "ts" in last.columns else None,
            "last_current_price": int(last["_current_price_num"].iloc[0]) if len(last) else None,
        }
    return {"path": str(history_path), "exists": history_path.exists(), "by_code": out}


def _classify_intraday_error(error: str) -> str:
    text = str(error or "")
    if "EGW00201" in text:
        return "kis_per_second_rate_limit"
    if "timeout" in text.lower() or "timed out" in text.lower():
        return "timeout"
    if "token" in text.lower() or "authorization" in text.lower() or "unauthorized" in text.lower():
        return "token_or_auth"
    if "parse" in text.lower() or "KeyError" in text or "ValueError" in text:
        return "parser_or_response_shape"
    if text.strip():
        return "other_api_error"
    return "unknown"


def _count_error_classes(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        cls = _classify_intraday_error(str(row.get("error", "")))
        counts[cls] = counts.get(cls, 0) + 1
    return counts


def _audit_daily_candidates() -> dict[str, Any]:
    cand = _read_csv(LOG_DIR / "candidates_latest_data.csv")
    meta = _read_json(LOG_DIR / "candidates_latest_meta.json")
    close = _num(cand, "close")
    value = _num(cand, "value")
    ret = _num(cand, "ret1_pct")
    krx_flag_cols = [c for c in ["krx_admin", "krx_warning", "krx_risk", "krx_caution"] if c in cand.columns]
    flagged_rows = int(pd.concat([_bool_series(cand, c) for c in krx_flag_cols], axis=1).any(axis=1).sum()) if krx_flag_cols else 0
    limit_like_rows = int((ret.abs() >= 29.0).sum()) if len(cand) else 0
    return {
        "path": str(LOG_DIR / "candidates_latest_data.csv"),
        "rows": int(len(cand)),
        "latest_date": meta.get("latest_date"),
        "as_of_select": meta.get("as_of_select"),
        "close_nonpositive": int((close <= 0).sum()) if len(cand) else 0,
        "value_nonpositive": int((value <= 0).sum()) if len(cand) else 0,
        "krx_flag_columns": krx_flag_cols,
        "krx_flagged_rows": flagged_rows,
        "limit_like_abs_ret_ge_29pct_rows": limit_like_rows,
        "execution_pool_false_rows": int((~_bool_series(cand, "execution_pool")).sum()) if "execution_pool" in cand.columns else None,
    }


def _audit_intraday_prices() -> dict[str, Any]:
    df = _read_csv(LOG_DIR / "intraday_prices_latest.csv")
    status = _read_json(LOG_DIR / "intraday_prices_status_latest.json")
    current = _num(df, "current_price")
    ask = _num(df, "ask1")
    bid = _num(df, "bid1")
    failed_codes = df.loc[current <= 0, "code"].astype(str).str.strip().str.zfill(6).tolist() if "code" in df.columns else []
    ok_codes = df.loc[current > 0, "code"].astype(str).str.strip().str.zfill(6).tolist() if "code" in df.columns else []
    history_path = Path(str(status.get("history_csv") or (LOG_DIR / f"intraday_prices_history_{datetime.now().strftime('%Y%m%d')}.csv")))
    history = _positive_history_for_codes(history_path, failed_codes)
    candidate_codes = set(_codes_from_csv(LOG_DIR / "candidates_latest_data.csv"))
    future_codes = set(str(v).strip().zfill(6) for v in (status.get("future_signal_preview_codes") or []) if str(v).strip())
    realtime_codes = set(str(v).strip().zfill(6) for v in (status.get("realtime_surge_codes") or []) if str(v).strip())
    open_codes = set(_open_position_codes())
    source_buckets = {
        code: sorted(name for name, codes in {
            "daily_candidates": candidate_codes,
            "future_signal_preview": future_codes,
            "realtime_surge": realtime_codes,
            "open_positions": open_codes,
        }.items() if code in codes)
        for code in failed_codes
    }
    slow_top_failed = [
        row for row in (status.get("slow_top") or [])
        if isinstance(row, dict) and not bool(row.get("ok", False))
    ]
    failed_perf_rows = [
        row for row in (status.get("failed_perf_rows") or [])
        if isinstance(row, dict)
    ]
    failed_error_counts = status.get("failed_error_counts", {})
    if not isinstance(failed_error_counts, dict):
        failed_error_counts = {}
    hoga_fallback_error_rows = [
        row for row in (status.get("hoga_fallback_error_rows") or [])
        if isinstance(row, dict)
    ]
    failed_error_class_counts = _count_error_classes(failed_perf_rows)
    history_by_code = history.get("by_code", {})
    failed_with_positive_history = [
        code for code in failed_codes
        if int((history_by_code.get(code) or {}).get("positive_rows") or 0) > 0
    ]
    return {
        "path": str(LOG_DIR / "intraday_prices_latest.csv"),
        "rows": int(len(df)),
        "status_path": str(LOG_DIR / "intraday_prices_status_latest.json"),
        "status_ts": status.get("ts"),
        "codes_requested": status.get("codes_requested"),
        "codes_ok": status.get("codes_ok"),
        "codes_failed": status.get("codes_failed"),
        "elapsed_total_sec": status.get("elapsed_total_sec"),
        "kis_timeout_sec": status.get("kis_timeout_sec"),
        "workers": status.get("workers"),
        "request_interval_sec": status.get("request_interval_sec"),
        "rate_limit_retry_max": status.get("rate_limit_retry_max"),
        "rate_limit_retry_sleep": status.get("rate_limit_retry_sleep"),
        "failed_codes": failed_codes,
        "ok_codes": ok_codes,
        "failed_code_source_buckets": source_buckets,
        "failed_codes_with_positive_history": failed_with_positive_history,
        "positive_history_for_failed_codes": history,
        "slow_top_failed": slow_top_failed,
        "failed_perf_rows": failed_perf_rows,
        "failed_error_counts": failed_error_counts,
        "failed_error_class_counts": failed_error_class_counts,
        "dominant_failed_error_class": max(failed_error_class_counts, key=failed_error_class_counts.get) if failed_error_class_counts else None,
        "failed_error_rows_available": bool(failed_perf_rows),
        "hoga_fallback_error_rows": hoga_fallback_error_rows,
        "fallback_mode": status.get("fallback_mode"),
        "hoga_fallback_mode": status.get("hoga_fallback_mode"),
        "hoga_fallback_scope": status.get("hoga_fallback_scope"),
        "current_nonpositive_rows": int((current <= 0).sum()) if len(df) else 0,
        "ask_nonpositive_rows": int((ask <= 0).sum()) if len(df) else 0,
        "bid_nonpositive_rows": int((bid <= 0).sum()) if len(df) else 0,
        "bid_ask_unavailable_reason": (
            "hoga_fallback_disabled"
            if len(df) and int((ask <= 0).sum()) == len(df) and int((bid <= 0).sum()) == len(df) and status.get("hoga_fallback_mode") == "off"
            else None
        ),
    }


def _audit_surge_lob() -> dict[str, Any]:
    lob = _read_csv(LOG_DIR / "surge_lob_latest.csv")
    status = _read_json(LOG_DIR / "surge_lob_latest.json")
    lob_available = _bool_series(lob, "lob_available")
    return {
        "path": str(LOG_DIR / "surge_lob_latest.csv"),
        "rows": int(len(lob)),
        "json_path": str(LOG_DIR / "surge_lob_latest.json"),
        "json_status": status.get("status"),
        "lob_coverage_pct": status.get("lob_coverage_pct"),
        "codes_no_lob": status.get("codes_no_lob"),
        "lob_available_rows": int(lob_available.sum()) if len(lob) else 0,
        "lob_status_counts": lob.get("lob_status", pd.Series(dtype=str)).astype(str).value_counts().to_dict(),
    }


def _audit_surge_realtime() -> dict[str, Any]:
    df = _read_csv(LOG_DIR / "surge_realtime_latest.csv")
    status_path = LOG_DIR / "surge_realtime_latest.json"
    status = _read_json(status_path)
    thresholds = status.get("thresholds", {}) if isinstance(status.get("thresholds"), dict) else {}
    no_lob_block = df.get("exclude_reasons", pd.Series(dtype=str)).astype(str).str.contains("NO_LOB_BLOCK", regex=False)
    is_realtime = _bool_series(df, "is_realtime_surge")
    lob_available = _bool_series(df, "lob_available")
    excluded = _bool_series(df, "excluded_by_policy")
    no_lob_probe_allowed = _bool_series(df, "no_lob_probe_allowed")
    risky_open = is_realtime & (~lob_available) & (~excluded) & (~no_lob_probe_allowed)
    allowed_no_lob_probe = is_realtime & (~lob_available) & (~excluded) & no_lob_probe_allowed
    risky_no_lob_codes = (
        df.loc[risky_open, "code"].astype(str).str.strip().str.zfill(6).tolist()
        if len(df) and "code" in df.columns
        else []
    )
    allowed_no_lob_probe_codes = (
        df.loc[allowed_no_lob_probe, "code"].astype(str).str.strip().str.zfill(6).tolist()
        if len(df) and "code" in df.columns
        else []
    )
    return {
        "path": str(LOG_DIR / "surge_realtime_latest.csv"),
        "status_json_path": str(status_path),
        "status_ts": status.get("ts"),
        "status_lock_status": status.get("lock_status"),
        "rows": int(len(df)),
        "is_realtime_surge_rows": int(is_realtime.sum()) if len(df) else 0,
        "excluded_rows": int(excluded.sum()) if len(df) else 0,
        "no_lob_block_rows": int(no_lob_block.sum()) if len(df) else 0,
        "realtime_without_lob_not_excluded_rows": int(risky_open.sum()) if len(df) else 0,
        "allowed_no_lob_probe_rows": int(allowed_no_lob_probe.sum()) if len(df) else 0,
        "risky_no_lob_codes": risky_no_lob_codes,
        "allowed_no_lob_probe_codes": allowed_no_lob_probe_codes,
        "no_lob_probe_config": {
            "enabled": bool(thresholds.get("no_lob_probe_enabled", False)),
            "max_change_pct": thresholds.get("no_lob_probe_max_change_pct"),
            "min_rvol20": thresholds.get("no_lob_probe_min_rvol20"),
            "min_trading_value": thresholds.get("no_lob_probe_min_trading_value"),
            "min_score_final": thresholds.get("no_lob_probe_min_score_final"),
            "paper_data_collection_enabled": bool(thresholds.get("paper_data_collection_enabled", False)),
        },
    }


def build_report() -> dict[str, Any]:
    krx = _latest_krx_clean_columns()
    integrity = _audit_krx_integrity_sidecar()
    watchlist = _audit_krx_watchlist_snapshot()
    daily = _audit_daily_candidates()
    intraday = _audit_intraday_prices()
    lob = _audit_surge_lob()
    surge = _audit_surge_realtime()
    issues: list[str] = []

    if not krx["has_explicit_tradability_columns"]:
        if integrity["exists"] and integrity["status"] == "PASS":
            issues.append("daily_tradability_mask_sidecar_only_not_in_clean_parquet")
        else:
            issues.append("daily_krx_clean_has_no_explicit_tradability_mask_columns")
    if daily["close_nonpositive"] or daily["value_nonpositive"]:
        issues.append("daily_candidates_include_nonpositive_price_or_value")
    if surge["realtime_without_lob_not_excluded_rows"]:
        issues.append("surge_realtime_has_unexcluded_no_lob_rows")
    if intraday["codes_failed"]:
        if intraday["failed_codes"] and len(intraday["failed_codes_with_positive_history"]) == len(intraday["failed_codes"]):
            issues.append("intraday_snapshot_partial_fetch_failures_with_history_available")
        else:
            issues.append("intraday_price_snapshot_has_failed_codes")
        if not intraday["failed_error_rows_available"]:
            issues.append("intraday_failed_error_rows_not_available_in_latest_status")
        elif intraday["dominant_failed_error_class"]:
            issues.append(f"intraday_failed_error_class_{intraday['dominant_failed_error_class']}")

    status = "PASS" if not issues else "WARN"
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "scope": "read_only_tradability_mask_contract_audit",
        "issues": issues,
        "daily_krx_source": krx,
        "daily_krx_integrity_sidecar": integrity,
        "krx_watchlist_snapshot": watchlist,
        "daily_candidates": daily,
        "intraday_prices": intraday,
        "surge_lob": lob,
        "surge_realtime": surge,
        "policy_effect": False,
        "trading_effect": False,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Read-only tradability-mask contract audit")
    ap.add_argument("--out-json", default=str(LOG_DIR / "tradability_mask_contract_audit_latest.json"))
    args = ap.parse_args()

    report = build_report()
    out = Path(args.out_json)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": report["status"], "issues": report["issues"], "out_json": str(out)}, ensure_ascii=False))
    return 0 if report["status"] in {"PASS", "WARN"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
