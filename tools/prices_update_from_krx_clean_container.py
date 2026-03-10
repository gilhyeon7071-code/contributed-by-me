from __future__ import annotations
from datetime import datetime
from pathlib import Path
import shutil
import pandas as pd

KRX_DIR = Path(r"/app/1_Data/_krx_manual")              # bind mount
OUT     = Path(r"/app/1_Data/paper/prices/ohlcv_paper.parquet")  # named volume
OUT.parent.mkdir(parents=True, exist_ok=True)

NEEDED = ["date","code","open","high","low","close","volume","value"]

def _norm_date8(x) -> str:
    s = str(x).replace("-", "").strip()
    return s[:8]

def _norm_code6(x) -> str:
    s = "".join(ch for ch in str(x) if ch.isdigit())
    return s[-6:].zfill(6)

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
        d = df[cols].copy()
        d["date"] = d["date"].map(_norm_date8)
        d["code"] = d["code"].map(_norm_code6)
        parts.append(d)

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
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if OUT.exists() and OUT.stat().st_size > 0:
        shutil.copy2(OUT, OUT.with_suffix(OUT.suffix + f".bak_{ts}"))

    tmp = OUT.with_suffix(OUT.suffix + f".tmp_{ts}")
    merged.to_parquet(tmp, index=False)
    tmp.replace(OUT)

    print(f"[PRICES_FROM_KRX_CONTAINER] wrote: {OUT} rows={len(merged)} date_max={dm}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
