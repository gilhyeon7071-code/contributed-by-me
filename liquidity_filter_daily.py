# -*- coding: utf-8 -*-
"""
liquidity_filter_daily.py
- 목적: 후보(candidates_latest_data.csv)에 대해
  (1) 최소 거래대금(min_trading_value_krw) 미달 제외
  (2) 상한가 근접(일간 수익률 day_ret_pct >= limit_up_ban_pct) 제외
- 산출물:
  - 2_Logs/liquidity_filter_daily_YYYYMMDD_HHMMSS.json
  - 2_Logs/liquidity_filter_daily_last.json
  - 2_Logs/candidates_latest_data.filtered.csv
  - 2_Logs/candidates_latest_data.bak_YYYYMMDD_HHMMSS.csv (백업)
  - 2_Logs/candidates_latest_data.csv (필터 반영본으로 덮어쓰기)
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, Tuple, Any, List, Optional

import pandas as pd
from utils.common import norm_code


BASE_DIR = Path(os.environ.get("BASE_DIR", r"E:\1_Data"))
LOG_DIR = BASE_DIR / "2_Logs"

DEFAULT_MIN_TRADING_VALUE_KRW = 1_000_000_000  # 10억
DEFAULT_LIMIT_UP_BAN_PCT = 28.0                # 28% 이상 진입 금지 (상한가 근접 ban)

CAND_LATEST = LOG_DIR / "candidates_latest_data.csv"


def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _jsave(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _to_yyyymmdd(x: Any) -> Optional[str]:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, (pd.Timestamp, datetime)):
        return x.strftime("%Y%m%d")
    s = str(x).strip()
    if not s:
        return None
    # allow 'YYYY-MM-DD'
    m = re.fullmatch(r"(\d{4})[-/](\d{2})[-/](\d{2})", s)
    if m:
        return f"{m.group(1)}{m.group(2)}{m.group(3)}"
    # allow 'YYYYMMDD'
    m = re.fullmatch(r"\d{8}", s)
    if m:
        return s
    return None




def _prev_weekday_yyyymmdd(ymd: str) -> str:
    """prev weekday for a given YYYYMMDD (Mon->Fri prev is Fri)."""
    d = datetime.strptime(ymd, "%Y%m%d").date()
    d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def _find_krx_clean_parquets(base_dir: Path) -> List[Path]:
    # expected pattern: krx_daily_YYYYMMDD_YYYYMMDD_clean.parquet
    pats = list(base_dir.glob("krx_daily_*_clean.parquet"))
    pats.sort()
    return pats


def _load_px_map_for_codes_dates(
    parquets: List[Path],
    codes: List[str],
    dates: List[str],
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    Build a mapping (code, yyyymmdd) -> {'close': float|None, 'trading_value': float|None}
    by scanning krx_clean parquets and extracting only needed rows.
    """
    need_codes = set(codes)
    need_dates = set(dates)
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}

    if not parquets:
        return out

    cols_try = [
        ["date", "code", "close", "trading_value"],
        ["date", "code", "종가", "거래대금"],
    ]

    for pq in parquets:
        df = None
        for cols in cols_try:
            try:
                df = pd.read_parquet(pq, columns=cols)
                break
            except Exception:
                df = None
        if df is None or df.empty:
            continue

        # normalize columns
        if "종가" in df.columns and "close" not in df.columns:
            df = df.rename(columns={"종가": "close"})
        if "거래대금" in df.columns and "trading_value" not in df.columns:
            df = df.rename(columns={"거래대금": "trading_value"})

        # date normalization
        if "date" not in df.columns or "code" not in df.columns:
            continue

        # convert date to yyyymmdd str
        if pd.api.types.is_datetime64_any_dtype(df["date"]):
            df["date"] = df["date"].dt.strftime("%Y%m%d")
        else:
            df["date"] = df["date"].astype(str).str.replace(r"\.0$", "", regex=True).str.replace("-", "", regex=False)

        # code normalization
        df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)

        df = df[df["code"].isin(need_codes) & df["date"].isin(need_dates)]
        if df.empty:
            continue

        for _, r in df.iterrows():
            key = (str(r["code"]).zfill(6), str(r["date"]))
            out[key] = {
                "close": None if pd.isna(r.get("close")) else float(r.get("close")),
                "trading_value": None if pd.isna(r.get("trading_value")) else float(r.get("trading_value")),
                "src": str(pq.name),
            }

    return out


def main() -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    min_tv = int(os.environ.get("MIN_TRADING_VALUE_KRW", DEFAULT_MIN_TRADING_VALUE_KRW))
    limit_up_ban_pct = float(os.environ.get("LIMIT_UP_BAN_PCT", DEFAULT_LIMIT_UP_BAN_PCT))

    ts = _now_ts()
    out_json = LOG_DIR / f"liquidity_filter_daily_{ts}.json"
    out_last = LOG_DIR / "liquidity_filter_daily_last.json"
    out_filtered = LOG_DIR / "candidates_latest_data.filtered.csv"

    if (not CAND_LATEST.exists()) or CAND_LATEST.stat().st_size == 0:
        rep = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "status": "SKIP",
            "reason": "missing_or_empty_candidates_latest_data.csv",
            "min_trading_value_krw": min_tv,
            "limit_up_ban_pct": limit_up_ban_pct,
        }
        _jsave(out_json, rep)
        _jsave(out_last, rep)
        print("[LIQ_FILTER] skipped: missing or empty candidates_latest_data.csv")
        return 0

    df = pd.read_csv(CAND_LATEST, encoding="utf-8-sig")
    if df.empty or ("code" not in df.columns) or ("date" not in df.columns):
        rep = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "status": "SKIP",
            "reason": "missing_required_columns(code/date) or empty_df",
            "min_trading_value_krw": min_tv,
            "limit_up_ban_pct": limit_up_ban_pct,
        }
        _jsave(out_json, rep)
        _jsave(out_last, rep)
        print("[LIQ_FILTER] skipped: missing code/date columns or empty df")
        return 0

    # normalize
    df["code"] = df["code"].apply(norm_code)
    df["date_yyyymmdd"] = df["date"].apply(_to_yyyymmdd)
    df = df[df["date_yyyymmdd"].notna()].copy()
    df["date_yyyymmdd"] = df["date_yyyymmdd"].astype(str)

    if df.empty:
        rep = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "status": "SKIP",
            "reason": "no_valid_date_rows_after_normalization",
            "min_trading_value_krw": min_tv,
            "limit_up_ban_pct": limit_up_ban_pct,
        }
        _jsave(out_json, rep)
        _jsave(out_last, rep)
        print("[LIQ_FILTER] skipped: no valid date rows")
        return 0

    # Ensure we have trading_value + day_ret_pct, fill from krx_clean if missing
    notes: List[str] = []
    if "trading_value" not in df.columns:
        df["trading_value"] = pd.NA
    if "day_ret_pct" not in df.columns:
        df["day_ret_pct"] = pd.NA

    need_codes = sorted(set(df["code"].tolist()))
    need_dates = sorted(set(df["date_yyyymmdd"].tolist()))
    need_prev_dates = sorted({_prev_weekday_yyyymmdd(d) for d in need_dates})
    px_dates = sorted(set(need_dates) | set(need_prev_dates))

    parquets = _find_krx_clean_parquets(BASE_DIR)
    px_map = _load_px_map_for_codes_dates(parquets, need_codes, px_dates)

    def _get_px(code: str, d: str) -> Dict[str, Any]:
        return px_map.get((code, d), {})

    # Fill trading_value if missing
    tv_filled = 0
    ret_filled = 0
    for i, r in df.iterrows():
        code = r["code"]
        d = r["date_yyyymmdd"]

        tv = r.get("trading_value")
        if tv is None or (isinstance(tv, float) and pd.isna(tv)) or pd.isna(tv):
            info = _get_px(code, d)
            if "trading_value" in info and info["trading_value"] is not None:
                df.at[i, "trading_value"] = float(info["trading_value"])
                tv_filled += 1

        dr = r.get("day_ret_pct")
        if dr is None or (isinstance(dr, float) and pd.isna(dr)) or pd.isna(dr):
            # compute from closes
            info_t = _get_px(code, d)
            pd_ = _prev_weekday_yyyymmdd(d)
            info_p = _get_px(code, pd_)
            ct = info_t.get("close")
            cp = info_p.get("close")
            if (ct is not None) and (cp is not None) and (cp != 0):
                df.at[i, "day_ret_pct"] = (float(ct) / float(cp) - 1.0) * 100.0
                ret_filled += 1
            else:
                notes.append(f"missing day_ret_pct for {code} (close_today={ct}, close_prev={cp}, date={d}, prev={pd_})")

    if tv_filled:
        notes.append(f"filled_trading_value_from_krx_clean={tv_filled}")
    if ret_filled:
        notes.append(f"filled_day_ret_pct_from_krx_clean={ret_filled}")

    # Apply filters
    removed: List[Dict[str, Any]] = []
    keep_mask = []
    for _, r in df.iterrows():
        code = r["code"]
        d = r["date_yyyymmdd"]
        tv = r.get("trading_value")
        dr = r.get("day_ret_pct")
        reasons = []

        tv_num = None
        if tv is not None and not pd.isna(tv):
            try:
                tv_num = float(tv)
            except Exception:
                tv_num = None
        dr_num = None
        if dr is not None and not pd.isna(dr):
            try:
                dr_num = float(dr)
            except Exception:
                dr_num = None

        if tv_num is not None and tv_num < float(min_tv):
            reasons.append(f"trading_value_below_min({int(tv_num)}<{min_tv})")

        if dr_num is not None and dr_num >= float(limit_up_ban_pct):
            reasons.append(f"limit_up_near(day_ret_pct={dr_num:.2f}>= {limit_up_ban_pct})")

        if reasons:
            removed.append({
                "code": code,
                "date": d,
                "trading_value": tv_num,
                "day_ret_pct": dr_num,
                "reasons": reasons,
            })
            keep_mask.append(False)
        else:
            keep_mask.append(True)

    filtered = df[keep_mask].copy()

    # Save CSV outputs (preserve expected columns; keep original date column as-is, but normalize code)
    # Backup current
    bak = LOG_DIR / f"candidates_latest_data.bak_{ts}.csv"
    try:
        import shutil
        shutil.copy2(CAND_LATEST, bak)
        notes.append(f"backup_saved={bak.name}")
    except Exception:
        # fallback copy
        df0 = pd.read_csv(bak, encoding="utf-8-sig") if bak.exists() else None
        notes.append("backup_replace_failed_used_copy")
        import shutil
        shutil.copy2(CAND_LATEST, bak)

    # Rebuild for save: restore 'date' as YYYY-MM-DD to match prior style if possible
    # but do not force; keep original 'date' column from file if present.
    # Ensure code is zero padded
    if "code" in filtered.columns:
        filtered["code"] = filtered["code"].apply(norm_code)
    # If date column exists but not ISO, keep it; also write helper date_yyyymmdd
    filtered.to_csv(out_filtered, index=False, encoding="utf-8-sig")
    notes.append("filtered_saved=candidates_latest_data.filtered.csv")

    # overwrite latest_data with filtered
    tmp_latest = CAND_LATEST.with_suffix(CAND_LATEST.suffix + ".tmp")
    filtered.drop(columns=["date_yyyymmdd"], errors="ignore").to_csv(tmp_latest, index=False, encoding="utf-8-sig")
    tmp_latest.replace(CAND_LATEST)

    rep = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS",
        "min_trading_value_krw": min_tv,
        "limit_up_ban_pct": float(limit_up_ban_pct),
        "candidates_before": int(len(df)),
        "candidates_after": int(len(filtered)),
        "removed": removed,
        "kept": filtered["code"].tolist() if "code" in filtered.columns else [],
        "notes": notes,
    }

    _jsave(out_json, rep)
    _jsave(out_last, rep)
    print(f"[LIQ_FILTER] wrote: {out_json}")
    print(f"[LIQ_FILTER] status=PASS before={len(df)} after={len(filtered)} removed={len(removed)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
