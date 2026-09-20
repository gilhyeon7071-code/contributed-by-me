# -*- coding: utf-8 -*-
"""krx_daily_archive + Raw/RAW_WIDE 를 합친 가격 패널.

2026-08-24: krx_daily_archive 만 보고 "2022-10~2024-12 코스닥 결손" 이라고
단정했다가 뒤집혔다. `Raw/krx_daily_20221001_20251224.parquet` 이 그 구간을
코스닥까지 덮고 있었고, tools/fetch_kosdaq_backfill.py 헤더에 명시돼 있었다.

커버리지 실측:
    archive  6,018,260행  2015-01-02 ~ 2026-08-24   (2025 이전은 대체로 코스피 위주)
    raw      3,158,510행  2022-10-01 ~ 2025-12-24   (KOSDAQ 203만 / KOSPI 113만)
    겹침     1,506,108    raw 에만 1,652,402    합집합 7,670,447

두 소스를 (date, code) 로 합치고 중복은 **raw 우선**으로 남긴다.
raw 는 market 컬럼이 있고 거래대금이 원천값이다.
"""
from __future__ import annotations

import glob
from pathlib import Path

import pandas as pd

ROOT = Path(r"E:\1_Data")
ARCHIVE = ROOT / "krx_daily_archive"
RAW_WIDE = ROOT / "Raw" / "krx_daily_20221001_20251224.parquet"

COLS = ["date", "code", "close", "value"]


def _norm(d: pd.DataFrame) -> pd.DataFrame:
    d = d.copy()
    d["date"] = d["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    d["code"] = d["code"].astype(str).str.zfill(6)
    for c in ("close", "value"):
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")
    return d


def load_merged(cols: list[str] | None = None, with_market: bool = False) -> pd.DataFrame:
    """합친 패널. close>0 만 남기고 (date,code) 중복은 raw 우선."""
    use = list(cols or COLS)
    need = list(dict.fromkeys(use + (["market"] if with_market else [])))

    frames = []
    for f in sorted(glob.glob(str(ARCHIVE / "*_clean.parquet"))):
        try:
            df = pd.read_parquet(f, columns=[c for c in need if c != "market"])
        except Exception:
            continue
        if with_market:
            df["market"] = pd.NA
        df["_src"] = 0                     # archive
        frames.append(_norm(df))
    if not frames:
        raise RuntimeError("archive 에서 읽은 것이 없다")

    if RAW_WIDE.exists():
        rw = pd.read_parquet(RAW_WIDE, columns=need)
        rw["_src"] = 1                     # raw 우선
        frames.append(_norm(rw))

    d = pd.concat(frames, ignore_index=True)
    d = d[d["close"] > 0]
    # _src 오름차순 정렬 후 keep="last" -> 중복 시 raw(1) 가 남는다
    d = d.sort_values(["code", "date", "_src"]).drop_duplicates(
        subset=["date", "code"], keep="last"
    )
    d = _mask_corrupt_value_days(d)
    return d.drop(columns=["_src"]).sort_values(["code", "date"]).reset_index(drop=True)


def _mask_corrupt_value_days(d: pd.DataFrame) -> pd.DataFrame:
    """거래대금이 손상된 **날짜 전체**의 `value` 를 NaN 으로 만든다.

    [2026-09-10] 실측으로 찾은 결함:
    ```
    krx_daily_20260113_20260403_clean.parquet
      2026-01-22 ~ 2026-02-24 (21거래일)  value 범위 **±2.147e9**
      2,147,483,647 = 2^31-1  ->  **상류에서 int32 로 감싼 것**
      거래대금이 21.47억을 넘으면 음수로 감긴다
    ```
    **행 단위 가드로는 부족하다.** `utils/price_history_contract.py:51` 이
    `value > 0` 을 요구하지만, 감겨서 **양수 작은 값**이 된 행은 통과한다 -
    삼성전자 5조가 +14억으로 보이면 "유동성 낮은 종목" 이 될 뿐 검출되지 않는다.
    그날 대형주가 통째로 유니버스에서 빠진다.

    그래서 **날짜 단위**로 막는다. 거래대금 음수는 물리적으로 불가능하므로
    하루에 하나라도 음수가 있으면 그날 `value` 전체를 믿지 않는다.

    복구는 하지 않는다. 5조는 2^32 를 1,164번 감아 원값을 되돌릴 수 없다.
    NaN 으로 두면 유동성 필터가 "거래가능 아님" 으로 처리해 조용히 틀리지 않는다.
    """
    if "value" not in d.columns:
        return d
    v = pd.to_numeric(d["value"], errors="coerce")
    bad_days = d.loc[v < 0, "date"].unique()
    if len(bad_days) == 0:
        return d
    m = d["date"].isin(bad_days)
    print("[PANEL] **거래대금 손상일 %d일 발견 - 그날 value 를 통째로 NaN 처리해요** "
          "(%s ~ %s, 영향 %d행). int32 오버플로 흔적이에요."
          % (len(bad_days), min(bad_days), max(bad_days), int(m.sum())))
    d = d.copy()
    d.loc[m, "value"] = float("nan")
    return d


def add_vol60(d: pd.DataFrame, window: int = 60, min_periods: int = 45) -> pd.DataFrame:
    g = d.groupby("code")
    r = d["close"] / g["close"].shift(1) - 1.0
    d["vol60"] = r.groupby(d["code"]).transform(
        lambda s: s.rolling(window, min_periods=min_periods).std()
    ) * 100
    return d


if __name__ == "__main__":
    d = load_merged(with_market=True)
    print("합친 패널 %d행  %s ~ %s  고유종목 %d"
          % (len(d), d["date"].min(), d["date"].max(), d["code"].nunique()))
    d["yr"] = d["date"].str[:4]
    print("연도별 일평균 종목수:")
    for y, g in d.groupby("yr"):
        per = g.groupby("date").size()
        print("  %s  %6.0f" % (y, per.mean()))
