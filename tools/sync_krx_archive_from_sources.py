from __future__ import annotations

from pathlib import Path
from datetime import datetime
from typing import List, Tuple
import pandas as pd
import logging


ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = ROOT / "Raw" / "krx_daily_20221001_20251224.parquet"
MANUAL_DIR = ROOT / "_krx_manual"
ARCHIVE_DIR = ROOT / "krx_daily_archive"
OPTIONAL_TRADABILITY_COLUMNS = [
    "tradability_checked",
    "tradability_blocked",
    "tradability_reason",
    "tradability_source",
]
NEEDED = [
    "date",
    "code",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "value",
    *OPTIONAL_TRADABILITY_COLUMNS,
]




logger = logging.getLogger(__name__)

def _log_print(*args, **kwargs):
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")
    sep = kwargs.get("sep", " ")
    try:
        msg = sep.join(str(a) for a in args)
    except Exception:
        msg = " ".join(str(a) for a in args)
    logger.info(msg)
def _norm_date8(v: object) -> str:
    s = str(v or "").replace("-", "").strip()
    return s[:8]


def _norm_code6(v: object) -> str:
    s = "".join(ch for ch in str(v or "") if ch.isdigit())
    return s[-6:].zfill(6)


def _load_any_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    cols = [c for c in NEEDED if c in df.columns]
    if "date" not in cols or "code" not in cols:
        return pd.DataFrame()
    out = df[cols].copy()
    out["date"] = out["date"].map(_norm_date8)
    out["code"] = out["code"].map(_norm_code6)
    out = out[out["date"].str.len() == 8]
    out = out[out["code"].str.len() == 6]
    out = out.drop_duplicates(subset=["date", "code"], keep="last")
    return out


def _collect_sources() -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    raw = _load_any_parquet(RAW_PATH)
    if not raw.empty:
        frames.append(raw)
    for p in sorted(MANUAL_DIR.glob("krx_daily_*_clean.parquet")):
        df = _load_any_parquet(p)
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=NEEDED)
    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset=["date", "code"], keep="last")
    return out


def _latest_archive_end() -> str:
    mx = ""
    for p in ARCHIVE_DIR.glob("krx_daily_*_clean.parquet"):
        name = p.name
        parts = name.replace("krx_daily_", "").replace("_clean.parquet", "").split("_")
        if len(parts) == 2 and len(parts[1]) == 8 and parts[1].isdigit():
            if parts[1] > mx:
                mx = parts[1]
    return mx


def _write_chunk(df_all: pd.DataFrame, start: str, end: str) -> Tuple[bool, str]:
    if not start or not end or start > end:
        return False, ""
    chunk = df_all[(df_all["date"] >= start) & (df_all["date"] <= end)].copy()
    if chunk.empty:
        return False, ""
    chunk = chunk.sort_values(["date", "code"]).drop_duplicates(subset=["date", "code"], keep="last")
    empty_optional = [
        c for c in OPTIONAL_TRADABILITY_COLUMNS
        if c in chunk.columns and chunk[c].isna().all()
    ]
    if empty_optional:
        chunk = chunk.drop(columns=empty_optional)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    out = ARCHIVE_DIR / f"krx_daily_{start}_{end}_clean.parquet"
    if out.exists() and out.stat().st_size > 0:
        bak = out.with_suffix(out.suffix + f".bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        out.replace(bak)
    chunk.to_parquet(out, index=False)
    return True, str(out)


def main() -> int:
    df_all = _collect_sources()
    if df_all.empty:
        _log_print("[SYNC_KRX_ARCHIVE] no source rows")
        return 2
    date_min = str(df_all["date"].min())
    date_max = str(df_all["date"].max())
    archive_max = _latest_archive_end()
    _log_print(f"[SYNC_KRX_ARCHIVE] source_date_min={date_min} source_date_max={date_max} archive_max={archive_max}")

    wrote: List[str] = []

    # Fill known archive gap in 2025-01-01~2025-08-19 from Raw.
    ok, path = _write_chunk(df_all, "20250101", "20250819")
    if ok:
        wrote.append(path)

    # Extend archive tail using latest _krx_manual updates.
    if archive_max and date_max > archive_max:
        start = (pd.to_datetime(archive_max, format="%Y%m%d") + pd.Timedelta(days=1)).strftime("%Y%m%d")
        ok2, path2 = _write_chunk(df_all, start, date_max)
        if ok2:
            wrote.append(path2)

    if not wrote:
        _log_print("[SYNC_KRX_ARCHIVE] no new chunks written")
        return 0

    for p in wrote:
        _log_print(f"[SYNC_KRX_ARCHIVE] wrote {p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


