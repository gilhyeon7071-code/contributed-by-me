from __future__ import annotations

import argparse
import datetime as dt
import json
import re
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CACHE_DIR = ROOT / "_cache"
KRX_MANUAL_DIR = ROOT / "_krx_manual"

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


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace(",", "", regex=False), errors="coerce")


def _normalize_chunk(df: pd.DataFrame) -> pd.DataFrame:
    dcol = _find_col(df.columns, ["date", "ymd", "trade_date", "dt"])
    code_col = _find_col(df.columns, ["code", "ticker", "symbol", "pdno", "종목코드"])
    name_col = _find_col(df.columns, ["name", "corp_name", "종목명"])
    market_col = _find_col(df.columns, ["market", "mkt", "시장구분", "소속부", "소속"])
    ocol = _find_col(df.columns, ["open", "o", "stck_oprc", "시가"])
    hcol = _find_col(df.columns, ["high", "h", "stck_hgpr", "고가"])
    lcol = _find_col(df.columns, ["low", "l", "stck_lwpr", "저가"])
    ccol = _find_col(df.columns, ["close", "c", "stck_clpr", "종가"])
    vcol = _find_col(df.columns, ["volume", "vol", "stck_vol", "거래량"])
    val_col = _find_col(df.columns, ["value", "turnover", "거래대금", "trading_value", "acc_trdval"])
    cap_col = _find_col(df.columns, ["market_cap", "시가총액"])
    shares_col = _find_col(df.columns, ["listed_shares", "상장주식수"])

    if not dcol or not ccol or not code_col:
        raise ValueError("required columns missing: date/close/code")

    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df[dcol], errors="coerce")
    out["code"] = df[code_col].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(6)
    out["name"] = df[name_col].astype(str).str.strip() if name_col else ""
    out["market"] = df[market_col].astype(str).str.strip() if market_col else ""

    out["open"] = _to_num(df[ocol]) if ocol else _to_num(df[ccol])
    out["high"] = _to_num(df[hcol]) if hcol else _to_num(df[ccol])
    out["low"] = _to_num(df[lcol]) if lcol else _to_num(df[ccol])
    out["close"] = _to_num(df[ccol])
    out["volume"] = _to_num(df[vcol]) if vcol else pd.NA
    out["value"] = _to_num(df[val_col]) if val_col else pd.NA
    out["market_cap"] = _to_num(df[cap_col]) if cap_col else pd.NA
    out["listed_shares"] = _to_num(df[shares_col]) if shares_col else pd.NA

    out = out.dropna(subset=["date", "code", "close"])
    out = out[(out["close"] > 0) & (out["open"] > 0) & (out["high"] > 0) & (out["low"] > 0)]
    return out


def _read_csv_any(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)


def _load_sector_map() -> pd.DataFrame:
    files = list(CACHE_DIR.glob("krx_sector_master_*.csv")) + list(CACHE_DIR.glob("sector_ssot*.csv"))
    files = [p for p in files if p.exists()]
    files.sort(key=lambda x: x.stat().st_mtime if x.exists() else 0, reverse=True)
    if not files:
        return pd.DataFrame(columns=["code", "sector"])

    for p in files:
        try:
            raw = _read_csv_any(p)
        except Exception:
            continue

        code_col = _find_col(raw.columns, ["code", "ticker", "종목코드", "code6"])
        sector_col = _find_col(raw.columns, ["krx_sector", "sector", "industry", "gics", "업종"])
        if not code_col or not sector_col:
            continue

        out = pd.DataFrame()
        out["code"] = raw[code_col].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(6)
        out["sector"] = raw[sector_col].astype(str).str.strip()
        out = out.dropna(subset=["code"]).drop_duplicates(["code"], keep="last")
        out = out[out["sector"].astype(str).str.len() > 0]
        if len(out):
            return out[["code", "sector"]]

    return pd.DataFrame(columns=["code", "sector"])


def _load_market_cap_snapshot() -> pd.DataFrame:
    files = sorted(list(KRX_MANUAL_DIR.glob("krx_kospi_*.csv")) + list(KRX_MANUAL_DIR.glob("krx_kosdaq_*.csv")))
    files += sorted(list((KRX_MANUAL_DIR / "_inbox").glob("krx_kospi_*.csv")) + list((KRX_MANUAL_DIR / "_inbox").glob("krx_kosdaq_*.csv")))
    files = [p for p in files if p.exists()]
    if not files:
        return pd.DataFrame(columns=["code", "market_cap", "listed_shares", "cap_asof"])

    parts: List[pd.DataFrame] = []
    for p in files[-120:]:
        m = re.search(r"(\d{8})", p.name)
        asof = m.group(1) if m else ""
        try:
            raw = _read_csv_any(p)
        except Exception:
            continue
        code_col = _find_col(raw.columns, ["종목코드", "code", "ticker"])
        cap_col = _find_col(raw.columns, ["시가총액", "market_cap", "MarketCap"])
        sh_col = _find_col(raw.columns, ["상장주식수", "listed_shares", "ListedShares"])
        if not code_col or not cap_col:
            continue

        x = pd.DataFrame()
        x["code"] = raw[code_col].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(6)
        x["market_cap"] = _to_num(raw[cap_col])
        x["listed_shares"] = _to_num(raw[sh_col]) if sh_col else pd.NA
        x["cap_asof"] = pd.to_datetime(asof, format="%Y%m%d", errors="coerce")
        x = x.dropna(subset=["code", "market_cap"])
        if len(x):
            parts.append(x)

    if not parts:
        return pd.DataFrame(columns=["code", "market_cap", "listed_shares", "cap_asof"])

    z = pd.concat(parts, ignore_index=True)
    z = z.sort_values(["code", "cap_asof"]).drop_duplicates(["code"], keep="last")
    return z[["code", "market_cap", "listed_shares", "cap_asof"]]


def build_symbol_panel(parquet_files: List[Path], max_files: int = 32) -> Tuple[pd.DataFrame, dict]:
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
        raise RuntimeError("failed to parse any parquet into symbol panel")

    panel = pd.concat(frames, ignore_index=True)
    panel = panel.drop_duplicates(["date", "code"], keep="last").sort_values(["date", "code"]).copy()

    sector_map = _load_sector_map()
    cap_map = _load_market_cap_snapshot()

    if len(sector_map):
        panel = panel.merge(sector_map, on="code", how="left")
    else:
        panel["sector"] = pd.NA

    if len(cap_map):
        panel = panel.merge(cap_map, on="code", how="left", suffixes=("", "_snap"))
        if "market_cap_snap" in panel.columns:
            panel["market_cap"] = pd.to_numeric(panel["market_cap"], errors="coerce")
            panel["market_cap"] = panel["market_cap"].fillna(pd.to_numeric(panel["market_cap_snap"], errors="coerce"))
            panel = panel.drop(columns=["market_cap_snap"])
        if "listed_shares_snap" in panel.columns:
            panel["listed_shares"] = pd.to_numeric(panel["listed_shares"], errors="coerce")
            panel["listed_shares"] = panel["listed_shares"].fillna(pd.to_numeric(panel["listed_shares_snap"], errors="coerce"))
            panel = panel.drop(columns=["listed_shares_snap"])
    panel["market_cap"] = pd.to_numeric(panel["market_cap"], errors="coerce")
    panel["listed_shares"] = pd.to_numeric(panel["listed_shares"], errors="coerce")

    out_cols = [
        "date",
        "code",
        "name",
        "market",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
        "sector",
        "market_cap",
        "listed_shares",
    ]
    for c in out_cols:
        if c not in panel.columns:
            panel[c] = pd.NA
    panel = panel[out_cols]

    meta = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "source_file_count": len(used_files),
        "source_files": used_files,
        "rows": int(len(panel)),
        "date_min": panel["date"].min().strftime("%Y-%m-%d") if len(panel) else None,
        "date_max": panel["date"].max().strftime("%Y-%m-%d") if len(panel) else None,
        "n_symbols": int(panel["code"].nunique()) if len(panel) else 0,
        "sector_coverage": float(panel["sector"].notna().mean()) if len(panel) else 0.0,
        "market_cap_coverage": float(panel["market_cap"].notna().mean()) if len(panel) else 0.0,
    }
    return panel, meta


def main() -> int:
    ap = argparse.ArgumentParser(description="Build symbol-level panel csv for backtest structure checks")
    ap.add_argument("--out-csv", default="")
    ap.add_argument("--out-json", default="")
    ap.add_argument("--max-files", type=int, default=32)
    ap.add_argument("--use-core6", action="store_true")
    ap.add_argument("--only-basenames", default="", help="comma-separated parquet basenames to include")
    args = ap.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    files = _find_parquet_files(ROOT)
    if args.use_core6:
        files = _filter_by_allowed_basenames(files, CORE6_BASENAMES)
    if args.only_basenames:
        custom = [x.strip() for x in str(args.only_basenames).split(",") if x.strip()]
        files = _filter_by_allowed_basenames(files, custom)

    panel_df, meta = build_symbol_panel(files, max_files=args.max_files)

    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = Path(args.out_csv) if args.out_csv else (LOG_DIR / f"backtest_symbol_panel_{stamp}.csv")
    out_latest_csv = LOG_DIR / "backtest_symbol_panel_latest.csv"
    out_json = Path(args.out_json) if args.out_json else (LOG_DIR / f"backtest_symbol_panel_{stamp}.json")
    out_latest_json = LOG_DIR / "backtest_symbol_panel_latest.json"

    z = panel_df.copy()
    z["date"] = pd.to_datetime(z["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    z.to_csv(out_csv, index=False, encoding="utf-8-sig")
    z.to_csv(out_latest_csv, index=False, encoding="utf-8-sig")

    out_json.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    out_latest_json.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[BTPANEL] csv={out_csv}")
    print(f"[BTPANEL] latest_csv={out_latest_csv}")
    print(f"[BTPANEL] json={out_json}")
    print(f"[BTPANEL] latest_json={out_latest_json}")
    print(
        f"[BTPANEL] rows={meta['rows']} symbols={meta['n_symbols']} "
        f"sector_cov={meta['sector_coverage']:.3f} mcap_cov={meta['market_cap_coverage']:.3f} "
        f"range={meta['date_min']}..{meta['date_max']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
