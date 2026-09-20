from __future__ import annotations

from datetime import datetime
from pathlib import Path
import shutil
import pandas as pd
import logging

KRX_DIR = Path(str(Path(__file__).resolve().parents[1] / "_krx_manual"))
OUT = Path(str(Path(__file__).resolve().parents[1] / "paper" / "prices" / "ohlcv_paper.parquet"))
OUT.parent.mkdir(parents=True, exist_ok=True)

NEEDED = ["date","code","open","high","low","close","volume","value"]



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
def _norm_date8(x) -> str:
    s = str(x).replace("-", "").strip()
    return s[:8]

def _norm_code6(x) -> str:
    s = "".join(ch for ch in str(x) if ch.isdigit())
    s = s[-6:].zfill(6)
    return s

def main() -> int:
    files = sorted(KRX_DIR.glob("krx_daily_*_clean.parquet"))
    if not files:
        raise SystemExit(f"[FATAL] no krx_clean parquet in {KRX_DIR}")

    parts = []
    for p in files:
        df = pd.read_parquet(p)
        if df is None or len(df) == 0:
            continue
        cols = [c for c in NEEDED if c in df.columns]
        if "date" not in cols or "code" not in cols:
            continue
        df = df[cols].copy()
        df["date"] = df["date"].map(_norm_date8)
        df["code"] = df["code"].map(_norm_code6)
        parts.append(df)

    if not parts:
        raise SystemExit("[FATAL] no usable rows from krx_clean parquet(s)")

    new = pd.concat(parts, ignore_index=True).dropna(subset=["date","code"])
    new = new.drop_duplicates(subset=["date","code"], keep="last")

    if OUT.exists() and OUT.stat().st_size > 0:
        old = pd.read_parquet(OUT)
        merged = pd.concat([old, new], ignore_index=True).drop_duplicates(subset=["date","code"], keep="last")
    else:
        merged = new

    dm = merged["date"].astype(str).max()
    nd = merged["date"].astype(str).nunique()
    nc = merged["code"].astype(str).nunique()

    # backup + atomic-ish write
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if OUT.exists() and OUT.stat().st_size > 0:
        bak = OUT.with_suffix(OUT.suffix + f".bak_{ts}")
        shutil.copy2(OUT, bak)

    tmp = OUT.with_suffix(OUT.suffix + f".tmp_{ts}")
    merged.to_parquet(tmp, index=False)
    tmp.replace(OUT)

    _log_print(f"[PRICES_FROM_KRX] wrote: {OUT} rows={len(merged)} date_max={dm} nunique_dates={nd} nunique_code={nc}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

