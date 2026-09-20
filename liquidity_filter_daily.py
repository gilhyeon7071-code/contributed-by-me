# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from utils.common import norm_code


_DEFAULT_BASE_DIR = Path(__file__).resolve().parent
BASE_DIR = Path(os.environ.get("BASE_DIR") or os.environ.get("ROOTA") or str(_DEFAULT_BASE_DIR))
LOG_DIR = BASE_DIR / "2_Logs"

DEFAULT_MIN_TRADING_VALUE_KRW = 1_000_000_000
DEFAULT_LIMIT_UP_BAN_PCT = 28.0

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
    m = re.fullmatch(r"(\d{4})[-/](\d{2})[-/](\d{2})", s)
    if m:
        return f"{m.group(1)}{m.group(2)}{m.group(3)}"
    if re.fullmatch(r"\d{8}", s):
        return s
    return None


def _prev_weekday_yyyymmdd(ymd: str) -> str:
    d = datetime.strptime(ymd, "%Y%m%d").date()
    d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def _find_krx_clean_parquets(base_dir: Path) -> List[Path]:
    roots = [
        base_dir / "krx_daily_archive",
        base_dir / "_krx_manual",
        base_dir / "_krx_seed_full",
    ]
    pats: List[Path] = []
    seen: set[str] = set()
    for root in roots:
        if not root.exists():
            continue
        for pq in root.glob("krx_daily_*_clean.parquet"):
            key = str(pq.resolve()).lower()
            if key in seen:
                continue
            seen.add(key)
            pats.append(pq)
    pats.sort()
    return pats


def _parquet_date_range(pq: Path) -> Tuple[Optional[str], Optional[str]]:
    dates = re.findall(r"\d{8}", pq.name)
    if len(dates) < 2:
        return None, None
    return dates[-2], dates[-1]


def _filter_parquets_for_dates(parquets: List[Path], dates: List[str]) -> List[Path]:
    if not dates:
        return parquets
    need_min = min(dates)
    need_max = max(dates)
    out: List[Path] = []
    for pq in parquets:
        start, end = _parquet_date_range(pq)
        if not start or not end:
            out.append(pq)
            continue
        if end < need_min or start > need_max:
            continue
        out.append(pq)
    return out


def _normalize_parquet_cols(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "醫낃?": "close",
        "CLSPRC_IDX": "close",
        "value": "trading_value",
        "ACC_TRDVAL": "trading_value",
        "ACC_TRDVOL": "volume",
    }
    for src, dst in rename_map.items():
        if src in df.columns and dst not in df.columns:
            df = df.rename(columns={src: dst})
    return df


def _read_needed_rows(pq: Path, need_codes: set[str], need_dates: set[str]) -> pd.DataFrame:
    candidates = [
        ["date", "code", "close", "trading_value"],
        ["date", "code", "close", "value"],
        ["date", "code", "CLSPRC_IDX", "ACC_TRDVAL"],
    ]
    df = None
    for cols in candidates:
        try:
            df = pd.read_parquet(pq, columns=cols)
            break
        except Exception:
            df = None
    if df is None or df.empty:
        return pd.DataFrame()

    df = _normalize_parquet_cols(df)
    if "date" not in df.columns or "code" not in df.columns:
        return pd.DataFrame()

    if pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = df["date"].dt.strftime("%Y%m%d")
    else:
        df["date"] = (
            df["date"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.replace("-", "", regex=False)
            .str.replace("/", "", regex=False)
        )
    df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
    return df[df["code"].isin(need_codes) & df["date"].isin(need_dates)].copy()


def _load_px_map_for_codes_dates(
    parquets: List[Path],
    codes: List[str],
    dates: List[str],
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    if not parquets:
        return out

    need_codes = set(codes)
    need_dates = set(dates)
    for pq in parquets:
        df = _read_needed_rows(pq, need_codes, need_dates)
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
            "schema_contract": {
                "required_columns": ["code", "date"],
                "missing_required_columns": ["code", "date"],
                "input_columns": [],
                "output_columns": [],
                "schema_match": False,
            },
        }
        _jsave(out_json, rep)
        _jsave(out_last, rep)
        print("[LIQ_FILTER] skipped: missing or empty candidates_latest_data.csv")
        return 0

    df = pd.read_csv(CAND_LATEST, encoding="utf-8-sig")
    input_rows_raw = int(len(df))
    input_columns = [str(c) for c in df.columns.tolist()]
    required_columns = ["code", "date"]
    missing_required_columns = [c for c in required_columns if c not in df.columns]
    if df.empty or missing_required_columns:
        rep = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "status": "SKIP",
            "reason": "missing_required_columns(code/date) or empty_df",
            "min_trading_value_krw": min_tv,
            "limit_up_ban_pct": limit_up_ban_pct,
            "schema_contract": {
                "required_columns": required_columns,
                "missing_required_columns": missing_required_columns,
                "input_columns": input_columns,
                "output_columns": [],
                "schema_match": False,
            },
        }
        _jsave(out_json, rep)
        _jsave(out_last, rep)
        print("[LIQ_FILTER] skipped: missing code/date columns or empty df")
        return 0

    cols_added_for_contract: List[str] = []

    df["code"] = df["code"].apply(norm_code)
    rows_before_date_norm = int(len(df))
    df["date_yyyymmdd"] = df["date"].apply(_to_yyyymmdd)
    df = df[df["date_yyyymmdd"].notna()].copy()
    rows_after_date_norm = int(len(df))
    df["date_yyyymmdd"] = df["date_yyyymmdd"].astype(str)
    dup_key_detected = int(df.duplicated(subset=["code", "date_yyyymmdd"]).sum())
    rows_before_dedup = int(len(df))
    if dup_key_detected > 0:
        # Keep latest row by file order for the same strategy key(code+date).
        df = df.drop_duplicates(subset=["code", "date_yyyymmdd"], keep="last").copy()
    rows_after_dedup = int(len(df))
    dup_key_removed = rows_before_dedup - rows_after_dedup

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

    notes: List[str] = []
    if "trading_value" not in df.columns:
        df["trading_value"] = pd.NA
        cols_added_for_contract.append("trading_value")
    if "day_ret_pct" not in df.columns:
        df["day_ret_pct"] = pd.NA
        cols_added_for_contract.append("day_ret_pct")

    missing_before_fill = {
        "code": int(df["code"].isna().sum()),
        "date_yyyymmdd": int(df["date_yyyymmdd"].isna().sum()),
        "trading_value": int(df["trading_value"].isna().sum()),
        "day_ret_pct": int(df["day_ret_pct"].isna().sum()),
    }

    need_codes = sorted(set(df["code"].tolist()))
    need_dates = sorted(set(df["date_yyyymmdd"].tolist()))
    need_prev_dates = sorted({_prev_weekday_yyyymmdd(d) for d in need_dates})
    px_dates = sorted(set(need_dates) | set(need_prev_dates))

    parquets_all = _find_krx_clean_parquets(BASE_DIR)
    parquets = _filter_parquets_for_dates(parquets_all, px_dates)
    px_map = _load_px_map_for_codes_dates(parquets, need_codes, px_dates)
    notes.append(f"krx_clean_parquets_scanned={len(parquets)}/{len(parquets_all)}")

    def _get_px(code: str, d: str) -> Dict[str, Any]:
        return px_map.get((code, d), {})

    tv_filled = 0
    ret_filled = 0
    for i, r in df.iterrows():
        code = r["code"]
        d = r["date_yyyymmdd"]

        tv = r.get("trading_value")
        if tv is None or (isinstance(tv, float) and pd.isna(tv)) or pd.isna(tv):
            cur_value = r.get("value")
            if cur_value is not None and not pd.isna(cur_value):
                try:
                    df.at[i, "trading_value"] = float(cur_value)
                    tv_filled += 1
                except Exception:
                    pass
            else:
                info = _get_px(code, d)
                if "trading_value" in info and info["trading_value"] is not None:
                    df.at[i, "trading_value"] = float(info["trading_value"])
                    tv_filled += 1

        dr = r.get("day_ret_pct")
        if dr is None or (isinstance(dr, float) and pd.isna(dr)) or pd.isna(dr):
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
        notes.append(f"filled_trading_value={tv_filled}")
    if ret_filled:
        notes.append(f"filled_day_ret_pct_from_krx_clean={ret_filled}")
    if dup_key_detected:
        notes.append(f"dedup_code_date={dup_key_detected}")
    missing_after_fill = {
        "code": int(df["code"].isna().sum()),
        "date_yyyymmdd": int(df["date_yyyymmdd"].isna().sum()),
        "trading_value": int(df["trading_value"].isna().sum()),
        "day_ret_pct": int(df["day_ret_pct"].isna().sum()),
    }

    removed: List[Dict[str, Any]] = []
    keep_mask = []
    for _, r in df.iterrows():
        code = r["code"]
        d = r["date_yyyymmdd"]

        tv_num = pd.to_numeric(pd.Series([r.get("trading_value")]), errors="coerce").iloc[0]
        dr_num = pd.to_numeric(pd.Series([r.get("day_ret_pct")]), errors="coerce").iloc[0]
        reasons = []

        if not pd.isna(tv_num) and float(tv_num) < float(min_tv):
            reasons.append(f"trading_value_below_min({int(float(tv_num))}<{min_tv})")
        if not pd.isna(dr_num) and float(dr_num) >= float(limit_up_ban_pct):
            reasons.append(f"limit_up_near(day_ret_pct={float(dr_num):.2f}>={limit_up_ban_pct})")

        if reasons:
            removed.append(
                {
                    "code": code,
                    "date": d,
                    "trading_value": None if pd.isna(tv_num) else float(tv_num),
                    "day_ret_pct": None if pd.isna(dr_num) else float(dr_num),
                    "reasons": reasons,
                }
            )
            keep_mask.append(False)
        else:
            keep_mask.append(True)

    filtered = df[keep_mask].copy()
    rows_after_rule_filter = int(len(filtered))
    dropped_by_rule_filter = int(len(df) - len(filtered))

    bak = LOG_DIR / f"candidates_latest_data.bak_{ts}.csv"
    shutil.copy2(CAND_LATEST, bak)
    notes.append(f"backup_saved={bak.name}")

    if "code" in filtered.columns:
        filtered["code"] = filtered["code"].apply(norm_code)
    filtered.to_csv(out_filtered, index=False, encoding="utf-8-sig")
    notes.append("filtered_saved=candidates_latest_data.filtered.csv")

    output_df = filtered.drop(columns=["date_yyyymmdd"], errors="ignore").copy()
    expected_output_columns = input_columns.copy()
    for c in cols_added_for_contract:
        if c not in expected_output_columns:
            expected_output_columns.append(c)
    output_columns = [str(c) for c in output_df.columns.tolist()]
    schema_match = output_columns == expected_output_columns
    schema_missing_in_output = [c for c in expected_output_columns if c not in output_columns]
    schema_unexpected_in_output = [c for c in output_columns if c not in expected_output_columns]

    tmp_latest = CAND_LATEST.with_suffix(CAND_LATEST.suffix + ".tmp")
    output_df.to_csv(tmp_latest, index=False, encoding="utf-8-sig")
    tmp_latest.replace(CAND_LATEST)

    missing_output = {
        "code": int(output_df["code"].isna().sum()) if "code" in output_df.columns else None,
        "date": int(output_df["date"].isna().sum()) if "date" in output_df.columns else None,
        "trading_value": int(output_df["trading_value"].isna().sum()) if "trading_value" in output_df.columns else None,
        "day_ret_pct": int(output_df["day_ret_pct"].isna().sum()) if "day_ret_pct" in output_df.columns else None,
    }
    dropped_by_invalid_date = int(rows_before_date_norm - rows_after_date_norm)
    dropped_total = int(input_rows_raw - rows_after_rule_filter)

    rep = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS" if schema_match else "FAIL",
        "reason": "ok" if schema_match else "schema_contract_mismatch",
        "min_trading_value_krw": min_tv,
        "limit_up_ban_pct": float(limit_up_ban_pct),
        "dup_key_rule": "drop_duplicates(code,date_yyyymmdd,keep=last)",
        "dup_key_detected": dup_key_detected,
        "dup_key_removed": dup_key_removed,
        "rows_summary": {
            "input_rows_raw": input_rows_raw,
            "rows_before_date_norm": rows_before_date_norm,
            "rows_after_date_norm": rows_after_date_norm,
            "rows_before_dedup": rows_before_dedup,
            "rows_after_dedup": rows_after_dedup,
            "rows_after_rule_filter": rows_after_rule_filter,
            "dropped_by_invalid_date": dropped_by_invalid_date,
            "dropped_by_duplicate": dup_key_removed,
            "dropped_by_rule_filter": dropped_by_rule_filter,
            "dropped_total": dropped_total,
        },
        "missing_summary": {
            "before_fill": missing_before_fill,
            "after_fill": missing_after_fill,
            "output": missing_output,
            "filled_counts": {
                "trading_value": tv_filled,
                "day_ret_pct": ret_filled,
            },
        },
        "schema_contract": {
            "required_columns": required_columns,
            "missing_required_columns": [],
            "input_columns": input_columns,
            "expected_output_columns": expected_output_columns,
            "output_columns": output_columns,
            "added_columns_for_contract": cols_added_for_contract,
            "missing_in_output": schema_missing_in_output,
            "unexpected_in_output": schema_unexpected_in_output,
            "schema_match": schema_match,
        },
        "candidates_before": rows_before_dedup,
        "candidates_after": rows_after_rule_filter,
        "removed": removed,
        "kept": filtered["code"].tolist() if "code" in filtered.columns else [],
        "notes": notes,
    }

    _jsave(out_json, rep)
    _jsave(out_last, rep)
    print(f"[LIQ_FILTER] wrote: {out_json}")
    print(
        "[LIQ_FILTER] status={status} input_rows={input_rows} output_rows={output_rows} "
        "dropped_total={dropped_total} dropped_invalid_date={dropped_invalid_date} "
        "dropped_duplicate={dropped_duplicate} dropped_rule={dropped_rule} "
        "dup_rule={dup_rule} dup_detected={dup_detected} dup_removed={dup_removed}".format(
            status=rep["status"],
            input_rows=input_rows_raw,
            output_rows=rows_after_rule_filter,
            dropped_total=dropped_total,
            dropped_invalid_date=dropped_by_invalid_date,
            dropped_duplicate=dup_key_removed,
            dropped_rule=dropped_by_rule_filter,
            dup_rule=rep["dup_key_rule"],
            dup_detected=dup_key_detected,
            dup_removed=dup_key_removed,
        )
    )
    if not schema_match:
        print(
            f"[LIQ_FILTER][FAIL] schema mismatch missing={schema_missing_in_output} "
            f"unexpected={schema_unexpected_in_output}"
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
