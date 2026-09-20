# -*- coding: utf-8 -*-
"""임시: 타겟 구간 레짐 분포 추출 (read-only)."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(r"E:\1_Data")
ARCHIVE = ROOT / "krx_daily_archive"
OUT_DIR = ROOT / "2_Logs"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def load_daily() -> pd.DataFrame:
    # 연도별 parquet + 최신 데일리 parquet을 병합
    files = sorted(ARCHIVE.glob("krx_daily_*_clean.parquet"))
    frames = []
    for f in files:
        try:
            df = pd.read_parquet(f)
        except Exception as exc:
            print(f"[SKIP] {f.name}: {exc}")
            continue
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["code"] = df["code"].astype(str).str.strip().str.zfill(6)
        df = df.dropna(subset=["date", "code", "close"])
        if not df.empty:
            frames.append(df)
    if not frames:
        raise RuntimeError("no parquet data loaded")
    df = pd.concat(frames, ignore_index=True)
    # 중복 제거: 같은 date+code면 마지막 것 사용
    df = df.sort_values(["code", "date"]).drop_duplicates(["code", "date"], keep="last")
    return df


def assign_regime(factors: pd.DataFrame) -> pd.Series:
    """report_backtest_v41_1._assign_report_research_regime 재현."""
    d = factors.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce").dt.normalize()
    idx = d.groupby("date")["close"].mean().sort_index()
    ret20 = idx.pct_change(20)
    ma60 = idx.rolling(60, min_periods=60).mean()
    vol20 = idx.pct_change().rolling(20, min_periods=20).std()

    def _safe_breadth(g: pd.DataFrame) -> float:
        close = pd.to_numeric(g["close"], errors="coerce")
        ma = pd.to_numeric(g["ma60"], errors="coerce") if "ma60" in g.columns else pd.Series(np.nan, index=g.index)
        valid = close.notna() & ma.notna()
        if not bool(valid.any()):
            return float("nan")
        return float((close[valid] > ma[valid]).mean())

    # 종목별 ma60이 필요함: 시가총앝 평균 close 기준 breadth 대신, 개별 종목 ma60 비율 사용
    d["ma60"] = d.groupby("code")["close"].transform(lambda s: s.rolling(60, min_periods=60).mean())
    breadth = d.groupby("date").apply(_safe_breadth, include_groups=False)

    out = pd.DataFrame({"ret20": ret20, "ma60": ma60, "vol20": vol20, "breadth": breadth})
    out["above_ma60"] = idx > out["ma60"]
    regime = np.full(len(out), "TRANSITION", dtype=object)
    stress = (out["ret20"] <= -0.10) | ((out["vol20"] >= 0.04) & (out["ret20"] < 0))
    bull = (~stress) & out["above_ma60"] & (out["ret20"] >= 0.05) & (out["breadth"] >= 0.55)
    bear = (~stress) & (~out["above_ma60"]) & (out["ret20"] <= -0.02) & (out["breadth"] <= 0.45)
    sideways = (~stress) & (out["ret20"].abs() <= 0.05) & out["breadth"].between(0.40, 0.60, inclusive="both")
    regime[stress.fillna(False).to_numpy()] = "STRESS"
    regime[bull.fillna(False).to_numpy()] = "BULL"
    regime[bear.fillna(False).to_numpy()] = "BEAR"
    regime[sideways.fillna(False).to_numpy()] = "SIDEWAYS"
    return pd.Series(regime, index=out.index, name="market_regime_research")


def main() -> int:
    df = load_daily()
    print(f"[LOAD] rows={len(df):,} dates={df['date'].nunique()} codes={df['code'].nunique()}")

    regime = assign_regime(df)
    daily_index = regime.reset_index()
    daily_index.columns = ["date", "primary_regime"]

    # KOSPI-like market index = equal-weighted mean close
    market = df.groupby("date").agg(mean_close=("close", "mean"), total_codes=("code", "nunique")).reset_index()
    market = market.merge(daily_index, on="date", how="left")
    market["ret20"] = market["mean_close"].pct_change(20)
    market["ret60"] = market["mean_close"].pct_change(60)

    windows = [
        ("2020-08-07", "2021-08-06", "상승장1"),
        ("2021-08-07", "2022-08-06", "조정1"),
        ("2022-08-07", "2023-08-06", "하락1"),
        ("2023-08-07", "2024-08-06", "상승장2"),
        ("2024-08-07", "2025-08-06", "횡보"),
        ("2025-08-07", "2026-08-06", "상승장3"),
    ]

    summary = []
    for start, end, label in windows:
        s, e = pd.Timestamp(start), pd.Timestamp(end)
        w = market[(market["date"] >= s) & (market["date"] <= e)].copy()
        if w.empty:
            continue
        counts = w["primary_regime"].value_counts().to_dict()
        total = len(w)
        start_close = w["mean_close"].iloc[0]
        end_close = w["mean_close"].iloc[-1]
        summary.append({
            "window": label,
            "start": start,
            "end": end,
            "total_days": total,
            "start_close": round(start_close, 4),
            "end_close": round(end_close, 4),
            "return_pct": round((end_close / start_close - 1) * 100, 2),
            "regime_days": {k: int(v) for k, v in counts.items()},
            "regime_share": {k: round(v / total, 3) for k, v in counts.items()},
        })

    out_json = OUT_DIR / "regime_distribution_target_windows_latest.json"
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] wrote {out_json}")

    # 일별 시장 레짐 CSV도 저장
    market_out = OUT_DIR / "regime_daily_market_index_latest.csv"
    market.to_csv(market_out, index=False, encoding="utf-8-sig")
    print(f"[OK] wrote {market_out}")

    print("\n=== 타겟 구간 레짐 분포 ===")
    for row in summary:
        print(f"\n{row['window']} ({row['start']} ~ {row['end']})")
        print(f"  시장 수익률: {row['return_pct']}%")
        print(f"  레짐 일수: {row['regime_days']}")
        print(f"  레짐 비중: {row['regime_share']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
