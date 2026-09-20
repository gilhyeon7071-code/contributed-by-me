from __future__ import annotations

import argparse
import datetime as dt
import json
import time
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from pykrx import stock


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
KRX_DIR = ROOT / "_krx_manual"


def _norm_ymd(v: object) -> str:
    return str(v or "").replace("-", "").strip()[:8]


def _norm_code(v: object) -> str:
    digits = "".join(ch for ch in str(v or "") if ch.isdigit())
    return digits[-6:].zfill(6)


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8-sig"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _safe_write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _latest_clean_path(base_dir: Path) -> Path:
    candidates: List[tuple[str, float, Path]] = []
    for path in base_dir.glob("krx_daily_*_clean.parquet"):
        try:
            df = pd.read_parquet(path, columns=["date"])
        except Exception:
            continue
        if df.empty:
            continue
        mx = str(df["date"].astype(str).str.replace("-", "", regex=False).str[:8].max())
        if mx:
            candidates.append((mx, path.stat().st_mtime, path))
    if not candidates:
        raise FileNotFoundError(f"no krx_daily_*_clean.parquet under {base_dir}")
    candidates.sort()
    return candidates[-1][2]


def _load_code_pool(path: Path, max_codes: int = 0) -> pd.DataFrame:
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


def _fetch_code_ohlcv(code: str, start: str, end: str, adjusted: bool) -> pd.DataFrame:
    raw = stock.get_market_ohlcv_by_date(start, end, str(code).zfill(6), adjusted=bool(adjusted))
    if raw is None or raw.empty:
        return pd.DataFrame()
    df = raw.reset_index().copy()
    rename = {
        "날짜": "date",
        "시가": "open",
        "고가": "high",
        "저가": "low",
        "종가": "close",
        "거래량": "volume",
        "거래대금": "value",
        "등락률": "change_rate",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    if "date" not in df.columns:
        df = df.rename(columns={df.columns[0]: "date"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y%m%d")
    df["code"] = str(code).zfill(6)
    if "value" not in df.columns:
        df["value"] = pd.to_numeric(df.get("close"), errors="coerce").astype("float64") * pd.to_numeric(df.get("volume"), errors="coerce").astype("float64")
    if "change_rate" not in df.columns:
        df["change_rate"] = pd.NA
    for col in ["open", "high", "low", "close", "volume", "value", "change_rate"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    cols = ["date", "code", "open", "high", "low", "close", "volume", "value", "change_rate"]
    return df[[c for c in cols if c in df.columns]].dropna(subset=["date", "code", "open", "high", "low", "close"])


def main() -> int:
    ap = argparse.ArgumentParser(description="Build a KRX-clean-compatible snapshot from pykrx by-code daily OHLCV.")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--base-clean", default="")
    ap.add_argument("--max-codes", type=int, default=0)
    ap.add_argument("--min-rows-per-required-date", type=int, default=1800)
    ap.add_argument("--required-dates", default="")
    ap.add_argument("--cache-csv", default="")
    ap.add_argument("--sleep-sec", type=float, default=0.0)
    ap.add_argument("--adjusted", default="true", choices=["true", "false"])
    args = ap.parse_args()

    start = _norm_ymd(args.start)
    end = _norm_ymd(args.end)
    if len(start) != 8 or len(end) != 8 or start > end:
        raise SystemExit("[FATAL] invalid --start/--end")
    required_dates = [_norm_ymd(x) for x in str(args.required_dates or end).split(",") if _norm_ymd(x)]
    min_rows = max(1, int(args.min_rows_per_required_date))
    max_codes = max(0, int(args.max_codes or 0))
    adjusted = str(args.adjusted).strip().lower() == "true"

    base_clean = Path(args.base_clean) if str(args.base_clean or "").strip() else _latest_clean_path(KRX_DIR)
    pool = _load_code_pool(base_clean, max_codes=max_codes)
    if pool.empty:
        raise SystemExit("[FATAL] empty code pool")

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    cache_csv = Path(args.cache_csv) if str(args.cache_csv or "").strip() else LOG_DIR / f"pykrx_daily_clean_cache_{start}_{end}.csv"
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    status_path = LOG_DIR / f"pykrx_daily_clean_status_{ts}.json"
    status_latest = LOG_DIR / "pykrx_daily_clean_status_latest.json"

    cached = pd.DataFrame()
    if cache_csv.exists():
        try:
            cached = pd.read_csv(cache_csv, dtype={"code": str, "date": str})
        except Exception:
            cached = pd.DataFrame()
    done_codes = set(cached["code"].astype(str).str.zfill(6).tolist()) if "code" in cached.columns else set()

    frames: List[pd.DataFrame] = []
    errors: List[Dict[str, str]] = []
    total = len(pool)
    for idx, rec in pool.iterrows():
        code = str(rec.get("code", "")).zfill(6)
        market = str(rec.get("market", "") or "")
        if code in done_codes:
            continue
        try:
            df = _fetch_code_ohlcv(code, start, end, adjusted=adjusted)
            if not df.empty:
                df["market"] = market
                df["source"] = "pykrx_get_market_ohlcv_by_date"
                frames.append(df)
        except Exception as exc:
            errors.append({"code": code, "error": str(exc)[:300]})
        if frames and len(frames) % 50 == 0:
            new_df = pd.concat(frames, ignore_index=True)
            base_df = cached if not cached.empty else pd.DataFrame()
            cached = pd.concat([base_df, new_df], ignore_index=True)
            cached["code"] = cached["code"].astype(str).str.zfill(6)
            cached["date"] = cached["date"].astype(str).map(_norm_ymd)
            cached = cached.drop_duplicates(subset=["date", "code"], keep="last")
            cached.to_csv(cache_csv, index=False, encoding="utf-8-sig")
            frames = []
        if idx == 0 or (idx + 1) % 100 == 0 or (idx + 1) == total:
            print(f"[PYKRX_DAILY] {idx + 1}/{total} cache_rows={len(cached) + sum(len(x) for x in frames)} errors={len(errors)}", flush=True)
        if float(args.sleep_sec or 0.0) > 0:
            time.sleep(float(args.sleep_sec))

    if frames:
        new_df = pd.concat(frames, ignore_index=True)
        base_df = cached if not cached.empty else pd.DataFrame()
        cached = pd.concat([base_df, new_df], ignore_index=True)
        cached["code"] = cached["code"].astype(str).str.zfill(6)
        cached["date"] = cached["date"].astype(str).map(_norm_ymd)
        cached = cached.drop_duplicates(subset=["date", "code"], keep="last")
        cached.to_csv(cache_csv, index=False, encoding="utf-8-sig")

    out_df = cached.copy() if not cached.empty else pd.DataFrame()
    if not out_df.empty:
        out_df["date"] = out_df["date"].astype(str).map(_norm_ymd)
        out_df["code"] = out_df["code"].astype(str).map(_norm_code)
        out_df = out_df[(out_df["date"] >= start) & (out_df["date"] <= end)].copy()
        out_df = out_df.drop_duplicates(subset=["date", "code"], keep="last")
        for col in ["open", "high", "low", "close", "volume", "value", "change_rate"]:
            out_df[col] = pd.to_numeric(out_df[col], errors="coerce")
        out_df = out_df.dropna(subset=["date", "code", "open", "high", "low", "close"])
        out_df = out_df[["date", "code", "market", "open", "high", "low", "close", "volume", "value", "change_rate", "source"]]

    counts = out_df["date"].value_counts().sort_index().to_dict() if not out_df.empty else {}
    missing_required = [d for d in required_dates if int(counts.get(d, 0) or 0) < min_rows]
    status = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "status": "PASS" if not missing_required else "FAIL",
        "source": "pykrx get_market_ohlcv_by_date",
        "adjusted": bool(adjusted),
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

    out_path = KRX_DIR / f"pykrx_daily_{start}_{end}_clean.parquet"
    if out_path.exists() and out_path.stat().st_size > 0:
        backup = out_path.with_suffix(out_path.suffix + f".bak_{ts}")
        out_path.replace(backup)
        status["replaced_existing_backup"] = str(backup)
    out_df = out_df.sort_values(["date", "code"])
    out_df.to_parquet(out_path, index=False)
    status["out_path"] = str(out_path)
    status["rows"] = int(len(out_df))
    status["date_max"] = str(out_df["date"].max()) if not out_df.empty else ""
    _safe_write_json(status_path, status)
    _safe_write_json(status_latest, status)
    print(f"[OK] wrote {out_path} rows={len(out_df)} counts={counts}")
    print(f"[OK] status {status_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
