# -*- coding: utf-8 -*-
"""
backfill_sector_trades.py
  - joined_trades_final_latest.csv에 sector_code 역채움 (one-shot, idempotent)
  - sector(krx_sector) 컬럼이 이미 있으면 MAP 조인으로 sector_code 추가
  - sector 없는 행은 sector_ssot.csv에서 code 기준으로 보완
  - source 컬럼: FAIL_SOFT → sector_mapping_backfill (sector_code 채워진 행)
"""
from pathlib import Path
import pandas as pd

ROOTA = Path(r"E:\1_Data")
LOGS  = ROOTA / "2_Logs"
CACHE = ROOTA / "_cache"

TRADES_FINAL = LOGS / "joined_trades_final_latest.csv"
SSOT = CACHE / "sector_ssot.csv"
MAP  = CACHE / "krx_sector_to_sector_code_SSOT_v1_hotfix.csv"


def _read(path: Path, **kw) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc, **kw)
        except Exception:
            continue
    raise RuntimeError(f"Cannot read: {path}")


def main() -> int:
    if not TRADES_FINAL.exists():
        raise SystemExit(f"FATAL missing: {TRADES_FINAL}")
    if not SSOT.exists():
        raise SystemExit(f"FATAL missing: {SSOT}")
    if not MAP.exists():
        raise SystemExit(f"FATAL missing: {MAP}")

    df   = _read(TRADES_FINAL, dtype={"code": str})
    ssot = _read(SSOT,         dtype={"code": str, "krx_sector": str})
    mp   = _read(MAP,          dtype={"krx_sector": str, "sector_code": str})

    df["code"]         = df["code"].astype(str).str.zfill(6)
    ssot["code"]       = ssot["code"].astype(str).str.zfill(6)
    ssot["krx_sector"] = ssot["krx_sector"].astype(str).str.strip()
    mp["krx_sector"]   = mp["krx_sector"].astype(str).str.strip()
    mp["sector_code"]  = mp["sector_code"].astype(str).str.strip()

    before_rows = len(df)

    # 1) sector 컬럼 보완: 비어있으면 ssot에서 code 기준으로 채움
    df = df.merge(
        ssot[["code", "krx_sector"]].rename(columns={"krx_sector": "_ssot_sector"}),
        on="code", how="left"
    )
    no_sector = df["sector"].isna() | (df["sector"].astype(str).str.strip() == "")
    df.loc[no_sector, "sector"] = df.loc[no_sector, "_ssot_sector"]
    df.drop(columns=["_ssot_sector"], inplace=True)

    # 2) sector(krx_sector) → sector_code (MAP 조인)
    df["_join_key"] = df["sector"].astype(str).str.strip()
    df = df.merge(
        mp[["krx_sector", "sector_code"]].rename(columns={"krx_sector": "_join_key"}),
        on="_join_key", how="left"
    )
    df.drop(columns=["_join_key"], inplace=True)

    # sector_code 컬럼이 이미 있었으면 덮어쓰기, 없으면 신규
    if "sector_code_x" in df.columns:
        df["sector_code"] = df["sector_code_y"].fillna(df["sector_code_x"])
        df.drop(columns=["sector_code_x", "sector_code_y"], inplace=True)

    # 3) source 업데이트
    filled = (
        df["sector_code"].notna()
        & (df["sector_code"].astype(str).str.strip() != "")
        & (df["sector_code"].astype(str).str.strip() != "nan")
    )
    if "source" in df.columns:
        df.loc[filled & (df["source"] == "FAIL_SOFT"), "source"] = "sector_mapping_backfill"

    # 통계
    print(f"[BACKFILL] rows={len(df)} (before={before_rows})")
    print(f"[BACKFILL] sector_code filled: {filled.sum()}/{len(df)}")
    if "source" in df.columns:
        print(f"[BACKFILL] source counts: {df['source'].value_counts().to_dict()}")
    if "sector_code" in df.columns:
        print(f"[BACKFILL] sector_code sample: {df[['code','sector','sector_code']].head(5).to_string()}")

    df.to_csv(TRADES_FINAL, index=False, encoding="utf-8-sig")
    print(f"[WROTE] {TRADES_FINAL}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
