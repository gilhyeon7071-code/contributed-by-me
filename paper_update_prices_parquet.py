from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
import json
import multiprocessing as mp
import re

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "2_Logs"
PAPER_PRICE_DIR = BASE_DIR / "paper" / "prices"
OUT_PARQUET = PAPER_PRICE_DIR / "ohlcv_paper.parquet"
CAND = LOG_DIR / "candidates_latest_data.csv"
CFG_PATH = BASE_DIR / "paper" / "paper_engine_config.json"

PAPER_TRADES = BASE_DIR / "paper" / "trades.csv"
PAPER_FILLS = BASE_DIR / "paper" / "fills.csv"

KOR_OHLCV_COLS = {
    "open": "시가",
    "high": "고가",
    "low": "저가",
    "close": "종가",
    "volume": "거래량",
}
FETCH_TIMEOUT_SEC = 15
FETCH_RETRY_COUNT = 1
ROWS_WARN_RATIO = 0.75
ROWS_FAIL_RATIO = 0.50
NCODE_WARN_RATIO = 0.85
NCODE_FAIL_RATIO = 0.70


def ymd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_read_csv(p: Path) -> pd.DataFrame:
    try:
        if p.exists():
            return pd.read_csv(p)
    except Exception:
        pass
    return pd.DataFrame()


def _normalize_ymd_series(s: pd.Series) -> list[str]:
    # accept: 2025-12-26, 20251226, 20251226.0, etc.
    vals = []
    for x in s.astype(str).tolist():
        x = x.strip()
        if not x:
            continue
        x = x.replace("-", "")
        m = re.search(r"(\d{8})", x)
        if m:
            vals.append(m.group(1))
    vals = [v for v in vals if re.fullmatch(r"\d{8}", v)]
    return vals


def _extract_codes_from_df(df: pd.DataFrame) -> list[str]:
    if df is None or df.empty:
        return []
    for col in ["code", "ticker"]:
        if col in df.columns:
            try:
                return sorted(
                    set(
                        df[col]
                        .astype(str)
                        .str.replace(".0", "", regex=False)
                        .str.strip()
                        .str.zfill(6)
                        .tolist()
                    )
                )
            except Exception:
                continue
    return []


def _merge_codes(*code_lists: list[str]) -> list[str]:
    merged = []
    seen = set()
    for codes in code_lists:
        for code in codes or []:
            norm = str(code or "").strip().zfill(6)
            if not norm or norm in seen:
                continue
            seen.add(norm)
            merged.append(norm)
    return sorted(merged)


def _resolve_ohlcv_column(df: pd.DataFrame, key: str, fallbacks: list[str]) -> str | None:
    candidates = []
    preferred = KOR_OHLCV_COLS.get(key)
    if preferred:
        candidates.append(preferred)
    candidates.extend(fallbacks)
    for name in candidates:
        if name in df.columns:
            return name
    return None


def _fetch_ohlcv_worker(start: str, end: str, code: str, queue: mp.Queue) -> None:
    try:
        from pykrx import stock  # type: ignore

        df = stock.get_market_ohlcv(start, end, code)
        if df is None:
            queue.put({"ok": True, "empty": True, "columns": [], "records": []})
            return
        x = df.reset_index()
        queue.put(
            {
                "ok": True,
                "empty": bool(x.empty),
                "columns": [str(c) for c in x.columns],
                "records": x.to_dict("records"),
            }
        )
    except Exception as e:
        queue.put({"ok": False, "error": f"{type(e).__name__}: {e}"})


def _fetch_ohlcv_with_timeout(start: str, end: str, code: str) -> tuple[pd.DataFrame | None, str | None]:
    last_error = None
    for attempt in range(1, FETCH_RETRY_COUNT + 2):
        queue: mp.Queue = mp.Queue()
        proc = mp.Process(target=_fetch_ohlcv_worker, args=(start, end, code, queue))
        proc.start()
        proc.join(FETCH_TIMEOUT_SEC)

        if proc.is_alive():
            proc.terminate()
            proc.join(2)
            if proc.is_alive():
                proc.kill()
                proc.join(1)
            last_error = f"timeout:{FETCH_TIMEOUT_SEC}s attempt={attempt}"
            print(f"[PRICE_UPDATE][TIMEOUT] code={code} {last_error}", flush=True)
            continue

        try:
            payload = queue.get_nowait()
        except Exception:
            payload = {"ok": False, "error": f"no_payload attempt={attempt}"}

        if not payload.get("ok"):
            last_error = str(payload.get("error") or f"unknown_error attempt={attempt}")
            continue

        if payload.get("empty"):
            return pd.DataFrame(), None

        records = payload.get("records") or []
        columns = payload.get("columns") or []
        if not records:
            return pd.DataFrame(columns=columns), None
        return pd.DataFrame.from_records(records, columns=columns), None

    return None, last_error


def _derive_expected_date_from_fills(fills_path: Path) -> tuple[str, str]:
    fdf = _safe_read_csv(fills_path)
    if fdf is None or fdf.empty:
        return "", "fills_empty"

    if "side" in fdf.columns and "date" in fdf.columns:
        try:
            side = fdf["side"].astype(str).str.upper()
            buy = fdf.loc[side == "BUY", "date"].astype(str)
            buy_ymd = _normalize_ymd_series(buy)
            if buy_ymd:
                return max(buy_ymd), "buy_date"
        except Exception:
            pass

    if "datetime" in fdf.columns:
        try:
            dt_ymd = _normalize_ymd_series(fdf["datetime"].astype(str).str.slice(0, 8))
            if dt_ymd:
                return max(dt_ymd), "datetime_prefix"
        except Exception:
            pass

    return "", "unavailable"


def _safe_lag_days(expected_ymd: str, max_ymd: str) -> int | None:
    try:
        if not expected_ymd or not max_ymd:
            return None
        d1 = datetime.strptime(expected_ymd, "%Y%m%d")
        d2 = datetime.strptime(max_ymd, "%Y%m%d")
        return (d1 - d2).days
    except Exception:
        return None


def _write_price_update_status(payload: dict) -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    dated = LOG_DIR / f"price_update_status_{ts}.json"
    latest = LOG_DIR / "price_update_status_latest.json"
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    dated.write_text(text, encoding="utf-8")
    latest.write_text(text, encoding="utf-8")


def _ensure_parquet_cfg() -> None:
    if CFG_PATH.exists():
        cfg = json.loads(CFG_PATH.read_text(encoding="utf-8"))
    else:
        cfg = {}
    target_updates = {
        "parquet_root": str(PAPER_PRICE_DIR),
        "parquet_top_n_recent": 10,
        "parquet_max_open_files": 5,
    }
    changed = any(cfg.get(k) != v for k, v in target_updates.items())
    if changed:
        cfg.update(target_updates)
        CFG_PATH.parent.mkdir(parents=True, exist_ok=True)
        CFG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"[OK] set parquet_root -> {cfg['parquet_root']}")
    else:
        print(f"[OK] parquet config unchanged -> {cfg.get('parquet_root', str(PAPER_PRICE_DIR))}")


def main() -> int:
    summary: dict = {
        "status": "FAIL",
        "reason": "uninitialized",
        "started_at": _utc_now_iso(),
        "paths": {
            "out_parquet": str(OUT_PARQUET),
            "fills_csv": str(PAPER_FILLS),
            "candidates_csv": str(CAND),
        },
        "freshness": {
            "expected_date": "",
            "expected_date_source": "",
            "max_date": "",
            "lag_days": None,
        },
        "rows": {
            "current_rows": 0,
            "baseline_rows": 0,
            "ratio": None,
            "warn_ratio": ROWS_WARN_RATIO,
            "fail_ratio": ROWS_FAIL_RATIO,
        },
        "ncode": {
            "current_ncode": 0,
            "baseline_ncode": 0,
            "ratio": None,
            "warn_ratio": NCODE_WARN_RATIO,
            "fail_ratio": NCODE_FAIL_RATIO,
        },
        "drop_guard": {
            "rows_warn": False,
            "rows_fail": False,
            "ncode_warn": False,
            "ncode_fail": False,
        },
        "failed_codes": {"count": 0, "sample": []},
        "rc": 1,
    }

    def finish(rc: int, status: str, reason: str) -> int:
        summary["rc"] = int(rc)
        summary["status"] = str(status)
        summary["reason"] = str(reason)
        summary["ended_at"] = _utc_now_iso()
        _write_price_update_status(summary)
        print(
            f"[PRICE_UPDATE_STATUS] status={summary['status']} reason={summary['reason']} "
            f"expected_date={summary['freshness']['expected_date']} max_date={summary['freshness']['max_date']} "
            f"lag_days={summary['freshness']['lag_days']} rows={summary['rows']['current_rows']} "
            f"ncode={summary['ncode']['current_ncode']} rc={summary['rc']}"
        )
        return rc

    try:
        pass
    except Exception as e:
        print(f"[FATAL] pykrx import failed: {type(e).__name__}: {e}")
        return finish(2, "FAIL", f"pykrx_import_failed:{type(e).__name__}")

    if not CAND.exists():
        print(f"[WARN] missing candidates file: {CAND} (will fallback to existing paper data)")
        cdf = pd.DataFrame()
    else:
        cdf = _safe_read_csv(CAND)

    tdf = _safe_read_csv(PAPER_TRADES)
    fdf = _safe_read_csv(PAPER_FILLS)
    parquet_codes: list[str] = []
    parquet_date_max: str = ""
    code_date_max: dict[str, str] = {}
    if OUT_PARQUET.exists():
        try:
            pdf = pd.read_parquet(OUT_PARQUET, columns=["code", "date"])
            parquet_codes = _extract_codes_from_df(pdf)
            summary["rows"]["baseline_rows"] = int(len(pdf))
            summary["ncode"]["baseline_ncode"] = int(pdf["code"].astype(str).nunique()) if "code" in pdf.columns else 0
            if pdf is not None and not pdf.empty and "date" in pdf.columns:
                mx = str(pdf["date"].astype(str).max())
                mx = mx.replace("-", "")[:8]
                if re.fullmatch(r"\d{8}", mx):
                    parquet_date_max = mx
                try:
                    tmp = pdf[["code", "date"]].copy()
                    tmp["code"] = (
                        tmp["code"]
                        .astype(str)
                        .str.replace(".0", "", regex=False)
                        .str.strip()
                        .str.zfill(6)
                    )
                    tmp["date"] = tmp["date"].astype(str).str.replace("-", "", regex=False).str[:8]
                    tmp = tmp[tmp["date"].str.match(r"^\d{8}$", na=False)]
                    if not tmp.empty:
                        g = tmp.groupby("code", as_index=False)["date"].max()
                        code_date_max = {str(r["code"]): str(r["date"]) for _, r in g.iterrows()}
                except Exception:
                    code_date_max = {}
        except Exception:
            parquet_codes = []
            parquet_date_max = ""
            code_date_max = {}
            summary["rows"]["baseline_rows"] = 0
            summary["ncode"]["baseline_ncode"] = 0

    codes = _merge_codes(
        _extract_codes_from_df(cdf),
        _extract_codes_from_df(tdf),
        _extract_codes_from_df(fdf),
        parquet_codes,
    )

    if not codes:
        print("[FATAL] no codes to update (candidates/trades/fills/parquet all empty or missing)")
        return finish(2, "FAIL", "no_codes_to_update")

    expected_date, expected_source = _derive_expected_date_from_fills(PAPER_FILLS)
    summary["freshness"]["expected_date"] = expected_date
    summary["freshness"]["expected_date_source"] = expected_source
    today_ymd = expected_date or ymd(datetime.now())
    missing_codes = sorted(set(codes) - set(parquet_codes))
    stale_codes = sorted(
        c for c in codes
        if c in code_date_max and str(code_date_max.get(c, "")) < today_ymd
    )
    if OUT_PARQUET.exists() and parquet_date_max == today_ymd and not missing_codes and not stale_codes:
        summary["freshness"]["max_date"] = parquet_date_max
        summary["freshness"]["lag_days"] = _safe_lag_days(expected_date, parquet_date_max)
        summary["rows"]["current_rows"] = int(summary["rows"]["baseline_rows"])
        summary["ncode"]["current_ncode"] = int(summary["ncode"]["baseline_ncode"])
        print(
            f"[PRICE_UPDATE] fast-skip: already up-to-date date={parquet_date_max} codes={len(codes)}",
            flush=True,
        )
        _ensure_parquet_cfg()
        return finish(0, "PASS", "fast_skip_uptodate")

    signal_dates: list[str] = []
    if cdf is not None and not cdf.empty:
        if "date" in cdf.columns:
            signal_dates = _normalize_ymd_series(cdf["date"])
        elif "signal_date" in cdf.columns:
            signal_dates = _normalize_ymd_series(cdf["signal_date"])
        else:
            signal_dates = [ymd(datetime.now())]

    if not signal_dates and OUT_PARQUET.exists():
        try:
            pdf = pd.read_parquet(OUT_PARQUET, columns=["date"])
            if pdf is not None and not pdf.empty and "date" in pdf.columns:
                mx = str(pdf["date"].astype(str).max())
                mx = mx.replace("-", "")[:8]
                if re.fullmatch(r"\d{8}", mx):
                    signal_dates = [mx]
        except Exception:
            pass

    base_ymd = min(signal_dates) if signal_dates else ymd(datetime.now())
    try:
        dt_start = datetime.strptime(base_ymd, "%Y%m%d") - timedelta(days=60)
    except Exception:
        dt_start = datetime.now() - timedelta(days=60)
    dt_end = datetime.now()

    start = ymd(dt_start)
    end = ymd(dt_end)

    rows = []
    failed = []

    name_map = {}
    if cdf is not None and not cdf.empty and "name" in cdf.columns and "code" in cdf.columns:
        try:
            for _, r in cdf.iterrows():
                code = str(r.get("code", "")).replace(".0", "").strip().zfill(6)
                nm = str(r.get("name", "") or "").strip()
                if code and nm:
                    name_map[code] = nm
        except Exception:
            name_map = {}

    total_codes = len(codes)
    print(f"[PRICE_UPDATE] codes={total_codes} range={start}~{end}", flush=True)

    for idx, code in enumerate(codes, start=1):
        try:
            df, fetch_error = _fetch_ohlcv_with_timeout(start, end, code)
            if fetch_error:
                failed.append((code, fetch_error))
                if idx % 10 == 0:
                    print(
                        f"[PRICE_UPDATE] progress={idx}/{total_codes} rows={len(rows)} failed={len(failed)}",
                        flush=True,
                    )
                continue
            if df is None or df.empty:
                failed.append((code, "empty"))
                if idx % 10 == 0:
                    print(
                        f"[PRICE_UPDATE] progress={idx}/{total_codes} rows={len(rows)} failed={len(failed)}",
                        flush=True,
                    )
                continue

            col_open = _resolve_ohlcv_column(df, "open", [])
            col_high = _resolve_ohlcv_column(df, "high", [])
            col_low = _resolve_ohlcv_column(df, "low", [])
            col_close = _resolve_ohlcv_column(df, "close", [])
            col_volume = _resolve_ohlcv_column(df, "volume", [])

            missing_cols = [
                name
                for name in [col_open, col_high, col_low, col_close, col_volume]
                if name is None or name not in df.columns
            ]
            if missing_cols:
                failed.append((code, f"missing_col={missing_cols}"))
                continue

            date_series = pd.to_datetime(df.iloc[:, 0], errors="coerce")

            out = pd.DataFrame(
                {
                    "date": date_series.dt.strftime("%Y%m%d"),
                    "code": code,
                    "open": pd.to_numeric(df[col_open], errors="coerce"),
                    "high": pd.to_numeric(df[col_high], errors="coerce"),
                    "low": pd.to_numeric(df[col_low], errors="coerce"),
                    "close": pd.to_numeric(df[col_close], errors="coerce"),
                    "volume": pd.to_numeric(df[col_volume], errors="coerce"),
                }
            )

            out = out.dropna(subset=["open", "high", "low", "close", "volume"])
            out = out[
                (out["open"] > 0)
                & (out["high"] > 0)
                & (out["low"] > 0)
                & (out["close"] > 0)
                & (out["volume"] >= 0)
            ]

            # Name lookup from external source can intermittently stall the whole batch.
            # Keep only names already available from current candidate data.
            out["name"] = name_map.get(code, "")

            if not out.empty:
                rows.append(out)
            else:
                failed.append((code, "all_filtered_zero_or_nan"))
        except Exception as e:
            failed.append((code, f"{type(e).__name__}: {e}"))

        if idx % 10 == 0:
            print(
                f"[PRICE_UPDATE] progress={idx}/{total_codes} rows={len(rows)} failed={len(failed)}",
                flush=True,
            )

    if not rows:
        print("[FATAL] no data written. failed sample:", failed[:10])
        summary["failed_codes"]["count"] = int(len(failed))
        summary["failed_codes"]["sample"] = [list(x) for x in failed[:10]]
        return finish(2, "FAIL", "no_data_written")

    px = pd.concat(rows, ignore_index=True)
    px = px.sort_values(["code", "date"]).drop_duplicates(["code", "date"], keep="last").reset_index(drop=True)

    PAPER_PRICE_DIR.mkdir(parents=True, exist_ok=True)
    px.to_parquet(OUT_PARQUET, index=False)

    current_rows = int(len(px))
    current_ncode = int(px["code"].nunique())
    max_date = str(px["date"].max()) if "date" in px.columns and not px.empty else ""
    min_date = str(px["date"].min()) if "date" in px.columns and not px.empty else ""
    summary["freshness"]["max_date"] = max_date
    summary["freshness"]["lag_days"] = _safe_lag_days(expected_date, max_date)
    summary["rows"]["current_rows"] = current_rows
    summary["ncode"]["current_ncode"] = current_ncode

    base_rows = int(summary["rows"]["baseline_rows"])
    base_ncode = int(summary["ncode"]["baseline_ncode"])
    rows_ratio = (float(current_rows) / float(base_rows)) if base_rows > 0 else None
    ncode_ratio = (float(current_ncode) / float(base_ncode)) if base_ncode > 0 else None
    summary["rows"]["ratio"] = rows_ratio
    summary["ncode"]["ratio"] = ncode_ratio
    summary["drop_guard"]["rows_warn"] = bool(rows_ratio is not None and rows_ratio < ROWS_WARN_RATIO)
    summary["drop_guard"]["rows_fail"] = bool(rows_ratio is not None and rows_ratio < ROWS_FAIL_RATIO)
    summary["drop_guard"]["ncode_warn"] = bool(ncode_ratio is not None and ncode_ratio < NCODE_WARN_RATIO)
    summary["drop_guard"]["ncode_fail"] = bool(ncode_ratio is not None and ncode_ratio < NCODE_FAIL_RATIO)
    summary["failed_codes"]["count"] = int(len(failed))
    summary["failed_codes"]["sample"] = [list(x) for x in failed[:10]]

    print(f"[OK] wrote: {OUT_PARQUET}")
    print(
        f"[OK] rows={current_rows} codes={current_ncode} date_max={max_date} date_min={min_date} "
        f"expected_date={expected_date or 'NA'} lag_days={summary['freshness']['lag_days']}"
    )
    print(
        f"[PRICE_UPDATE_GUARD] baseline_rows={base_rows} current_rows={current_rows} rows_ratio={rows_ratio} "
        f"baseline_ncode={base_ncode} current_ncode={current_ncode} ncode_ratio={ncode_ratio} "
        f"rows_warn={summary['drop_guard']['rows_warn']} rows_fail={summary['drop_guard']['rows_fail']} "
        f"ncode_warn={summary['drop_guard']['ncode_warn']} ncode_fail={summary['drop_guard']['ncode_fail']}"
    )
    if failed:
        print(f"[WARN] failed_codes={len(failed)} sample={failed[:10]}")

    _ensure_parquet_cfg()
    if expected_date and max_date and max_date < expected_date:
        return finish(3, "FAIL", f"freshness_lag:max_date<{expected_date}")
    if summary["drop_guard"]["rows_fail"] or summary["drop_guard"]["ncode_fail"]:
        return finish(4, "FAIL", "drop_guard_failed")
    if summary["drop_guard"]["rows_warn"] or summary["drop_guard"]["ncode_warn"]:
        return finish(0, "WARN", "drop_guard_warn")
    return finish(0, "PASS", "ok")


if __name__ == "__main__":
    mp.freeze_support()
    raise SystemExit(main())
