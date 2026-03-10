from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

EXCLUDE_DIR_HINTS = {
    ".venv",
    "venv",
    "docker_export",
    "node_modules",
    "__pycache__",
    "_bad",
}

CORE6_BASENAMES = [
    "krx_daily_20200101_20201231_clean.parquet",
    "krx_daily_20210101_20211231_clean.parquet",
    "krx_daily_20220101_20221231_clean.parquet",
    "krx_daily_20230101_20231231_clean.parquet",
    "krx_daily_20240101_20241231_clean.parquet",
    "krx_daily_20250820_20251225_clean.parquet",
]


def _is_excluded(path: Path) -> bool:
    low_parts = {p.lower() for p in path.parts}
    return any(h.lower() in low_parts for h in EXCLUDE_DIR_HINTS)


def _find_parquet_files(root: Path) -> List[Path]:
    cands: List[Path] = []
    for p in root.rglob("*.parquet"):
        if _is_excluded(p):
            continue
        name = p.name.lower()
        if "krx_daily" not in name:
            continue
        if "clean" not in name and "valuefix" not in name:
            continue
        try:
            if p.stat().st_size < 4096:
                continue
        except Exception:
            continue
        cands.append(p)
    cands.sort(key=lambda x: x.stat().st_mtime if x.exists() else 0)
    return cands


def _filter_by_allowed_basenames(files: List[Path], allowed_basenames: List[str]) -> List[Path]:
    if not allowed_basenames:
        return files
    allow = {str(x).strip().lower() for x in allowed_basenames if str(x).strip()}
    out = [p for p in files if p.name.lower() in allow]
    out.sort(key=lambda x: x.stat().st_mtime if x.exists() else 0)
    return out


def _find_col(cols: Iterable[str], candidates: List[str]) -> Optional[str]:
    cmap = {str(c).lower(): str(c) for c in cols}
    for c in candidates:
        if c.lower() in cmap:
            return cmap[c.lower()]
    return None


def _normalize_chunk(df: pd.DataFrame) -> pd.DataFrame:
    dcol = _find_col(df.columns, ["date", "ymd", "trade_date", "dt"])
    ocol = _find_col(df.columns, ["open", "o", "stck_oprc"])
    hcol = _find_col(df.columns, ["high", "h", "stck_hgpr"])
    lcol = _find_col(df.columns, ["low", "l", "stck_lwpr"])
    ccol = _find_col(df.columns, ["close", "c", "stck_clpr"])
    code_col = _find_col(df.columns, ["code", "ticker", "symbol", "pdno"])

    if not dcol or not ccol:
        raise ValueError("required columns missing: date/close")

    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df[dcol], errors="coerce")

    if ocol:
        out["open"] = pd.to_numeric(df[ocol], errors="coerce")
    else:
        out["open"] = pd.to_numeric(df[ccol], errors="coerce")

    if hcol:
        out["high"] = pd.to_numeric(df[hcol], errors="coerce")
    else:
        out["high"] = pd.to_numeric(df[ccol], errors="coerce")

    if lcol:
        out["low"] = pd.to_numeric(df[lcol], errors="coerce")
    else:
        out["low"] = pd.to_numeric(df[ccol], errors="coerce")

    out["close"] = pd.to_numeric(df[ccol], errors="coerce")

    if code_col:
        out["code"] = df[code_col].astype(str).str.strip().str.zfill(6)
    else:
        out["code"] = "__NA__"

    out = out.dropna(subset=["date", "close"])
    return out


def build_market_csv(parquet_files: List[Path], max_files: int = 32) -> tuple[pd.DataFrame, dict]:
    if not parquet_files:
        raise FileNotFoundError("no candidate parquet files found")

    use = parquet_files[-max(1, int(max_files)) :]
    frames: List[pd.DataFrame] = []
    used_files: List[str] = []

    for p in use:
        try:
            raw = pd.read_parquet(p)
            chunk = _normalize_chunk(raw)
            if len(chunk) == 0:
                continue
            frames.append(chunk)
            used_files.append(str(p))
        except Exception:
            continue

    if not frames:
        raise RuntimeError("failed to parse any parquet into market OHLC")

    all_df = pd.concat(frames, ignore_index=True)
    all_df = all_df.dropna(subset=["date", "open", "high", "low", "close"]).copy()
    all_df = all_df[(all_df["open"] > 0) & (all_df["high"] > 0) & (all_df["low"] > 0) & (all_df["close"] > 0)].copy()

    grp = (
        all_df.groupby("date", as_index=False)
        .agg(
            open=("open", "mean"),
            high=("high", "mean"),
            low=("low", "mean"),
            close=("close", "mean"),
            n_symbols=("code", "nunique"),
        )
        .sort_values("date")
    )
    grp = grp[(grp["open"] > 0) & (grp["high"] > 0) & (grp["low"] > 0) & (grp["close"] > 0)].copy()

    meta = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "source_file_count": len(used_files),
        "source_files": used_files,
        "rows": int(len(grp)),
        "date_min": grp["date"].min().strftime("%Y-%m-%d") if len(grp) else None,
        "date_max": grp["date"].max().strftime("%Y-%m-%d") if len(grp) else None,
    }
    return grp, meta


def main() -> int:
    ap = argparse.ArgumentParser(description="Build market OHLC CSV for backtest validation")
    ap.add_argument("--out-csv", default="")
    ap.add_argument("--out-json", default="")
    ap.add_argument("--max-files", type=int, default=32)
    ap.add_argument("--use-core6", action="store_true", help="use only predefined 6 parquet basenames")
    ap.add_argument("--only-basenames", default="", help="comma-separated parquet basenames to include")
    args = ap.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    files = _find_parquet_files(ROOT)
    if args.use_core6:
        files = _filter_by_allowed_basenames(files, CORE6_BASENAMES)
    if args.only_basenames:
        custom = [x.strip() for x in str(args.only_basenames).split(",")]
        files = _filter_by_allowed_basenames(files, custom)

    market_df, meta = build_market_csv(files, max_files=args.max_files)

    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = Path(args.out_csv) if args.out_csv else (LOG_DIR / f"backtest_market_ohlc_{stamp}.csv")
    out_latest_csv = LOG_DIR / "backtest_market_ohlc_latest.csv"
    out_json = Path(args.out_json) if args.out_json else (LOG_DIR / f"backtest_market_ohlc_{stamp}.json")
    out_latest_json = LOG_DIR / "backtest_market_ohlc_latest.json"

    market_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    market_df.to_csv(out_latest_csv, index=False, encoding="utf-8-sig")

    out_json.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    out_latest_json.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] csv={out_csv}")
    print(f"[OK] latest_csv={out_latest_csv}")
    print(f"[OK] json={out_json}")
    print(f"[OK] latest_json={out_latest_json}")
    print(f"[OK] rows={meta['rows']} files={meta['source_file_count']} range={meta['date_min']}..{meta['date_max']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

