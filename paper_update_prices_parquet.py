from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import json
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


def ymd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


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
                return sorted(set(df[col].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(6).tolist()))
            except Exception:
                continue
    return []


def main() -> int:
    try:
        from pykrx import stock
    except Exception as e:
        print(f"[FATAL] pykrx import failed: {type(e).__name__}: {e}")
        return 2

    # candidates_latest_data.csv는 비어있을 수 있음(테스트/후보0건 등) → 크래시 방지
    if not CAND.exists():
        print(f"[WARN] missing candidates file: {CAND} (will fallback to existing paper data)")
        cdf = pd.DataFrame()
    else:
        cdf = _safe_read_csv(CAND)

    # codes: candidates → paper(trades/fills) → existing parquet 순으로 fallback
    codes = _extract_codes_from_df(cdf)

    if not codes:
        tdf = _safe_read_csv(PAPER_TRADES)
        codes = _extract_codes_from_df(tdf)

    if not codes:
        fdf = _safe_read_csv(PAPER_FILLS)
        codes = _extract_codes_from_df(fdf)

    if not codes and OUT_PARQUET.exists():
        try:
            pdf = pd.read_parquet(OUT_PARQUET, columns=["code"])
            codes = _extract_codes_from_df(pdf)
        except Exception:
            codes = []

    if not codes:
        print("[FATAL] no codes to update (candidates/trades/fills/parquet all empty or missing)")
        return 2

    # signal_date range 결정: candidates의 date/signal_date → 기존 parquet date_max → 오늘
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

    # name map from candidates (if present)
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

    for code in codes:
        try:
            df = stock.get_market_ohlcv(start, end, code)
            if df is None or df.empty:
                failed.append((code, "empty"))
                continue

            for req in ["시가", "고가", "저가", "종가"]:
                if req not in df.columns:
                    failed.append((code, f"missing_col={req}"))
                    df = None
                    break
            if df is None:
                continue

            out = pd.DataFrame(
                {
                    "date": df.index.strftime("%Y%m%d"),
                    "code": code,
                    "open": pd.to_numeric(df["시가"], errors="coerce"),
                    "high": pd.to_numeric(df["고가"], errors="coerce"),
                    "low": pd.to_numeric(df["저가"], errors="coerce"),
                    "close": pd.to_numeric(df["종가"], errors="coerce"),
                }
            )

            out = out.dropna(subset=["open", "high", "low", "close"])
            out = out[
                (out["open"] > 0)
                & (out["high"] > 0)
                & (out["low"] > 0)
                & (out["close"] > 0)
            ]

            nm = name_map.get(code, "")
            if not nm:
                try:
                    nm = stock.get_market_ticker_name(code) or ""
                except Exception:
                    nm = ""
            out["name"] = nm

            if not out.empty:
                rows.append(out)
            else:
                failed.append((code, "all_filtered_zero_or_nan"))
        except Exception as e:
            failed.append((code, f"{type(e).__name__}: {e}"))

    if not rows:
        print("[FATAL] no data written. failed sample:", failed[:10])
        return 2

    px = pd.concat(rows, ignore_index=True)
    px = (
        px.sort_values(["code", "date"])
        .drop_duplicates(["code", "date"], keep="last")
        .reset_index(drop=True)
    )

    PAPER_PRICE_DIR.mkdir(parents=True, exist_ok=True)
    px.to_parquet(OUT_PARQUET, index=False)

    print(f"[OK] wrote: {OUT_PARQUET}")
    print(
        f"[OK] rows={len(px)} codes={px['code'].nunique()} date_max={px['date'].max()} date_min={px['date'].min()}"
    )
    if failed:
        print(f"[WARN] failed_codes={len(failed)} sample={failed[:10]}")

    # paper_engine_config.json의 parquet_root를 paper/prices로 고정(자동)
    if CFG_PATH.exists():
        cfg = json.loads(CFG_PATH.read_text(encoding="utf-8"))
    else:
        cfg = {}
    cfg["parquet_root"] = str(PAPER_PRICE_DIR)
    cfg["parquet_top_n_recent"] = 10
    cfg["parquet_max_open_files"] = 5
    CFG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CFG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] set parquet_root -> {cfg['parquet_root']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
