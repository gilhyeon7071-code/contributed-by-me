from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
TOOLS_DIR = ROOT / "tools"
LOG_DIR = ROOT / "2_Logs"
KRX_DIR = ROOT / "_krx_manual"

sys.path.insert(0, str(TOOLS_DIR))
from kis_order_client import KISOrderClient  # noqa: E402


def _norm_ymd(v: object) -> str:
    return str(v or "").replace("-", "").strip()[:8]


def _norm_code(v: object) -> str:
    digits = "".join(ch for ch in str(v or "") if ch.isdigit())
    return digits[-6:].zfill(6)


def _to_num(v: object) -> float:
    try:
        return float(str(v or "0").replace(",", "").strip() or 0)
    except Exception:
        return 0.0


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8-sig"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _safe_write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _parquet_date_max(path: Path) -> str:
    try:
        df = pd.read_parquet(path, columns=["date"])
    except Exception:
        return ""
    if df.empty:
        return ""
    return str(df["date"].astype(str).str.replace("-", "", regex=False).str[:8].max())


def _latest_clean_path(base_dir: Path) -> Path:
    candidates: List[tuple[str, float, Path]] = []
    for path in base_dir.glob("krx_daily_*_clean.parquet"):
        mx = _parquet_date_max(path)
        if mx:
            candidates.append((mx, path.stat().st_mtime, path))
    if not candidates:
        raise FileNotFoundError(f"no krx_daily_*_clean.parquet under {base_dir}")
    candidates.sort()
    return candidates[-1][2]


def _load_code_pool(path: Path, max_codes: int = 0) -> pd.DataFrame:
    cols = ["date", "code"]
    try:
        schema_cols = pd.read_parquet(path, columns=[]).columns.tolist()
    except Exception:
        schema_cols = []
    if "market" in schema_cols:
        cols.append("market")
    try:
        df = pd.read_parquet(path, columns=cols)
    except Exception:
        df = pd.read_parquet(path)
    if df.empty or "code" not in df.columns:
        raise ValueError(f"base clean has no code rows: {path}")
    if "date" in df.columns:
        mx = df["date"].astype(str).str.replace("-", "", regex=False).str[:8].max()
        df = df.loc[df["date"].astype(str).str.replace("-", "", regex=False).str[:8].eq(mx)].copy()
    df["code"] = df["code"].map(_norm_code)
    if "market" not in df.columns:
        df["market"] = ""
    out = df[["code", "market"]].drop_duplicates(subset=["code"], keep="last")
    out = out[out["code"].str.match(r"^\d{6}$", na=False)].sort_values(["market", "code"])
    if int(max_codes or 0) > 0:
        out = out.head(int(max_codes))
    return out.reset_index(drop=True)


def _fetch_daily_chart(client: KISOrderClient, code: str, start: str, end: str) -> List[Dict[str, Any]]:
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": str(code).zfill(6),
        "FID_INPUT_DATE_1": str(start),
        "FID_INPUT_DATE_2": str(end),
        "FID_PERIOD_DIV_CODE": "D",
        "FID_ORG_ADJ_PRC": "0",
    }
    headers = client._auth_headers(tr_id="FHKST03010100")
    body, _ = client._request_json(
        "GET",
        "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        headers=headers,
        params=params,
    )
    rt_cd = str(body.get("rt_cd", "")).strip()
    if rt_cd and rt_cd != "0":
        raise RuntimeError(f"KIS daily chart rejected code={code} rt_cd={rt_cd} msg={body.get('msg1')}")
    rows = body.get("output2", []) or []
    if isinstance(rows, dict):
        rows = [rows]
    return [r for r in rows if isinstance(r, dict)]


def _fetch_daily_chart_chunked(
    client: KISOrderClient,
    code: str,
    start: str,
    end: str,
    *,
    chunk_days: int = 90,
) -> List[Dict[str, Any]]:
    start_dt = dt.datetime.strptime(start, "%Y%m%d").date()
    end_dt = dt.datetime.strptime(end, "%Y%m%d").date()
    if start_dt > end_dt:
        return []

    rows: List[Dict[str, Any]] = []
    cur = start_dt
    step_days = max(1, int(chunk_days))
    while cur <= end_dt:
        chunk_end = min(end_dt, cur + dt.timedelta(days=step_days - 1))
        got = _fetch_daily_chart(
            client,
            code,
            cur.strftime("%Y%m%d"),
            chunk_end.strftime("%Y%m%d"),
        )
        rows.extend(got)
        cur = chunk_end + dt.timedelta(days=1)

    dedup: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        ymd = _norm_ymd(row.get("stck_bsop_date"))
        if ymd:
            dedup[ymd] = row
    return [dedup[k] for k in sorted(dedup.keys(), reverse=True)]


def _normalise_rows(code: str, market: str, rows: List[Dict[str, Any]], start: str, end: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        ymd = _norm_ymd(row.get("stck_bsop_date"))
        if not ymd or ymd < start or ymd > end:
            continue
        close = _to_num(row.get("stck_clpr"))
        volume = _to_num(row.get("acml_vol"))
        out.append(
            {
                "date": ymd,
                "code": str(code).zfill(6),
                "market": str(market or ""),
                "open": _to_num(row.get("stck_oprc")),
                "high": _to_num(row.get("stck_hgpr")),
                "low": _to_num(row.get("stck_lwpr")),
                "close": close,
                "volume": volume,
                "value": _to_num(row.get("acml_tr_pbmn")) or close * volume,
                "change_rate": pd.NA,
                "source": "kis_daily_itemchartprice",
            }
        )
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Build a fail-closed KRX-clean-compatible snapshot from KIS daily chart data.")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--base-clean", default="")
    ap.add_argument("--max-codes", type=int, default=0)
    ap.add_argument("--min-rows-per-required-date", type=int, default=1800)
    ap.add_argument("--required-dates", default="")
    ap.add_argument("--cache-csv", default="")
    ap.add_argument("--sleep-sec", type=float, default=0.0)
    args = ap.parse_args()

    start = _norm_ymd(args.start)
    end = _norm_ymd(args.end)
    if len(start) != 8 or len(end) != 8 or start > end:
        raise SystemExit("[FATAL] invalid --start/--end")
    required_dates = [_norm_ymd(x) for x in str(args.required_dates or end).split(",") if _norm_ymd(x)]
    min_rows = max(1, int(args.min_rows_per_required_date))
    max_codes = max(0, int(args.max_codes or 0))

    base_clean = Path(args.base_clean) if str(args.base_clean or "").strip() else _latest_clean_path(KRX_DIR)
    pool = _load_code_pool(base_clean, max_codes=max_codes)
    if pool.empty:
        raise SystemExit("[FATAL] empty code pool")

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    cache_csv = Path(args.cache_csv) if str(args.cache_csv or "").strip() else LOG_DIR / f"kis_daily_clean_cache_{start}_{end}.csv"
    status_latest = LOG_DIR / "kis_daily_clean_status_latest.json"
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    status_path = LOG_DIR / f"kis_daily_clean_status_{ts}.json"

    cached = pd.DataFrame()
    if cache_csv.exists():
        try:
            cached = pd.read_csv(cache_csv, dtype=str)
        except Exception:
            cached = pd.DataFrame()
    done_codes = set(cached["code"].astype(str).str.zfill(6).tolist()) if "code" in cached.columns else set()

    client = KISOrderClient.from_env(mock=False)
    rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    total = len(pool)
    for idx, rec in pool.iterrows():
        code = str(rec.get("code", "")).zfill(6)
        market = str(rec.get("market", "") or "")
        if code in done_codes:
            continue
        try:
            got = _fetch_daily_chart_chunked(client, code, start, end)
            rows.extend(_normalise_rows(code, market, got, start, end))
        except Exception as exc:
            errors.append({"code": code, "error": str(exc)[:300]})
        if rows and (len(rows) % 200 == 0):
            new_df = pd.DataFrame(rows)
            base_df = cached if not cached.empty else pd.DataFrame()
            merged = pd.concat([base_df, new_df], ignore_index=True)
            merged = merged.drop_duplicates(subset=["date", "code"], keep="last")
            merged.to_csv(cache_csv, index=False, encoding="utf-8-sig")
            cached = merged
            rows = []
        if idx == 0 or (idx + 1) % 100 == 0 or (idx + 1) == total:
            print(f"[KIS_DAILY] {idx + 1}/{total} cache_rows={len(cached) + len(rows)} errors={len(errors)}", flush=True)
        if float(args.sleep_sec or 0.0) > 0.0:
            time.sleep(float(args.sleep_sec))

    if rows:
        base_df = cached if not cached.empty else pd.DataFrame()
        cached = pd.concat([base_df, pd.DataFrame(rows)], ignore_index=True)
        cached = cached.drop_duplicates(subset=["date", "code"], keep="last")
        cached.to_csv(cache_csv, index=False, encoding="utf-8-sig")

    out_df = cached.copy() if not cached.empty else pd.DataFrame()
    if not out_df.empty:
        out_df["date"] = out_df["date"].astype(str).map(_norm_ymd)
        out_df["code"] = out_df["code"].astype(str).map(_norm_code)
        out_df = out_df[(out_df["date"] >= start) & (out_df["date"] <= end)].copy()
        out_df = out_df.drop_duplicates(subset=["date", "code"], keep="last")
        for col in ["open", "high", "low", "close", "volume", "value"]:
            out_df[col] = pd.to_numeric(out_df[col], errors="coerce")
        out_df = out_df.dropna(subset=["date", "code", "open", "high", "low", "close"])

    counts = out_df["date"].value_counts().sort_index().to_dict() if not out_df.empty else {}
    missing_required = [d for d in required_dates if int(counts.get(d, 0) or 0) < min_rows]
    status = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "status": "PASS" if not missing_required else "FAIL",
        "policy_change": "KIS_DAILY_AS_KRX_CLEAN_COMPAT_FALLBACK",
        "source": "KIS inquire-daily-itemchartprice",
        "base_clean": str(base_clean),
        "cache_csv": str(cache_csv),
        "range_from": start,
        "range_to": end,
        "required_dates": required_dates,
        "min_rows_per_required_date": min_rows,
        "counts_by_date": {str(k): int(v) for k, v in counts.items()},
        "errors_count": len(errors),
        "error_examples": errors[:20],
        "max_codes": max_codes,
        "code_pool_n": int(len(pool)),
    }

    if missing_required:
        status["missing_required_dates"] = missing_required
        _safe_write_json(status_path, status)
        _safe_write_json(status_latest, status)
        print(f"[FATAL] required dates below min rows: {missing_required} counts={counts}")
        return 2

    out_path = KRX_DIR / f"krx_daily_{start}_{end}_clean.parquet"
    if out_path.exists() and out_path.stat().st_size > 0:
        backup = out_path.with_suffix(out_path.suffix + f".bak_{ts}")
        out_path.replace(backup)
        status["replaced_existing_backup"] = str(backup)
    out_df = out_df.sort_values(["date", "code"])
    out_df.to_parquet(out_path, index=False)
    status["out_path"] = str(out_path)
    status["rows"] = int(len(out_df))
    status["date_max"] = str(out_df["date"].max())
    _safe_write_json(status_path, status)
    _safe_write_json(status_latest, status)
    print(f"[OK] wrote {out_path} rows={len(out_df)} counts={counts}")
    print(f"[OK] status {status_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
