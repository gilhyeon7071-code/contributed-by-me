# -*- coding: utf-8 -*-
"""KRX 수동 CSV -> 생산이 읽는 _cache/pykrx_fundamental_latest.csv 로 변환한다.

왜 필요한가 (2026-08-22, PLANS 51/52):
  generate_candidates_v41_1.py 의 _load_pykrx_fundamental_snapshot() 은
  아래 순서로 **로컬 캐시를 먼저** 읽는다.
      _cache/pykrx_fundamental_<YMD>.csv
      _cache/pykrx_fundamental_latest.csv     <- 현재 12종목 / 2026-06-24
      2_Logs/pykrx_fundamental_latest.csv
  그리고 `FUND_PYKRX_REFRESH` 기본값이 "0" 이라 **pykrx 는 호출조차 되지 않는다.**
  즉 생산의 market_cap 결측은 pykrx 사망 때문이 아니라 이 캐시가 비어 있기 때문이다.

  market_cap 이 없으면 pricing_engine.resolve_slippage_pct_tiered() 가
  `float(market_cap or 0)` 에서 0 으로 떨어져 **항상 small_slip_pct=1.0%** 를 고른다.
  왕복 비용이 계약 0.358% 가 아니라 2.158% 가 된다(6배).

  KRX 정보데이터시스템은 프로그램 접근을 막는다(OTP 요청에 'LOGOUT' 응답, pykrx 도 같은 이유로 사망).
  그래서 사람이 받은 CSV 를 넣는 경로가 유일하다.

입력 (둘 다 선택. 있는 만큼만 쓴다)
  _krx_manual/_inbox/krx_kospi_<YMD>.csv     [12016] 전종목 시세  -> 시가총액 / 상장주식수
  _krx_manual/_inbox/krx_kosdaq_<YMD>.csv
  _krx_manual/_inbox/krx_perpbr_<YMD>.csv    [12021] PER/PBR/배당수익률 -> PER/PBR/BPS/EPS/DIV/DPS
  (`_krx_manual/` 바로 아래에 두어도 읽는다)

출력
  _cache/pykrx_fundamental_<YMD>.csv
  _cache/pykrx_fundamental_latest.csv   (기존 파일은 .bak_<TS> 로 백업)

사용
  python tools/build_fundamental_from_krx_manual.py            # 가장 최신 날짜 자동
  python tools/build_fundamental_from_krx_manual.py --date 20260822
  python tools/build_fundamental_from_krx_manual.py --dry-run  # 쓰지 않고 결과만 보고
"""
from __future__ import annotations

import argparse
import datetime as dt
import re
import shutil
import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
MANUAL_DIRS = [ROOT / "_krx_manual" / "_inbox", ROOT / "_krx_manual"]
CACHE_DIR = ROOT / "_cache"

OUT_COLS = ["code", "PER", "PBR", "BPS", "EPS", "DIV", "DPS", "market_cap", "listed_shares"]

# KRX 한글 컬럼 -> 우리 스키마
COL_MAP = {
    "market_cap": ["시가총액", "market_cap", "MarketCap"],
    "listed_shares": ["상장주식수", "listed_shares", "ListedShares"],
    "PER": ["PER"],
    "PBR": ["PBR"],
    "BPS": ["BPS"],
    "EPS": ["EPS"],
    "DPS": ["DPS", "주당배당금"],
    "DIV": ["DIV", "배당수익률"],
}


def _read_csv_any(path: Path) -> pd.DataFrame:
    """KRX 다운로드는 보통 cp949 다. 인코딩을 순서대로 시도한다."""
    last = None
    for enc in ("cp949", "utf-8-sig", "utf-8", "euc-kr"):
        try:
            return pd.read_csv(path, encoding=enc, dtype=str, thousands=",")
        except Exception as exc:  # noqa: BLE001
            last = exc
    raise RuntimeError(f"읽기 실패 {path.name}: {last}")


def _pick(cols, names) -> Optional[str]:
    low = {str(c).strip().lower(): c for c in cols}
    for n in names:
        if n in cols:
            return n
        if str(n).lower() in low:
            return low[str(n).lower()]
    return None


def _norm_code6(v) -> Optional[str]:
    s = re.sub(r"[^0-9A-Za-z]", "", str(v or "").strip())
    if not s:
        return None
    return s.zfill(6)[:6] if len(s) <= 6 else s[:6]


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(
        s.astype(str).str.replace(",", "", regex=False).str.replace("%", "", regex=False).str.strip(),
        errors="coerce",
    )


def _find_files(pattern: str) -> List[Path]:
    out: List[Path] = []
    for d in MANUAL_DIRS:
        if d.exists():
            out += sorted(d.glob(pattern))
    return out


def _ymd_of(p: Path) -> str:
    m = re.search(r"(\d{8})", p.name)
    return m.group(1) if m else ""


def _load_group(patterns: List[str], ymd: str, label: str) -> pd.DataFrame:
    frames = []
    for pat in patterns:
        for p in _find_files(pat):
            if _ymd_of(p) != ymd:
                continue
            raw = _read_csv_any(p)
            code_col = _pick(raw.columns, ["종목코드", "code", "ticker", "단축코드"])
            if code_col is None:
                print(f"  [SKIP] {p.name}: 종목코드 컬럼 없음 (cols={list(raw.columns)[:6]})")
                continue
            x = pd.DataFrame({"code": raw[code_col].map(_norm_code6)})
            got = []
            for key, aliases in COL_MAP.items():
                c = _pick(raw.columns, aliases)
                if c is not None:
                    x[key] = _to_num(raw[c])
                    got.append(key)
            x = x.dropna(subset=["code"])
            print(f"  [OK] {p.name}: {len(x)}행, 컬럼 {got}")
            frames.append(x)
    if not frames:
        print(f"  [NONE] {label} 파일 없음")
        return pd.DataFrame(columns=["code"])
    z = pd.concat(frames, ignore_index=True)
    return z.drop_duplicates("code", keep="last")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="", help="YYYYMMDD. 생략하면 가장 최신 날짜")
    ap.add_argument("--dry-run", action="store_true", help="쓰지 않고 결과만 보고")
    args = ap.parse_args()

    all_files = _find_files("krx_*.csv")
    if not all_files:
        print(f"[FATAL] 수동 CSV 가 없다. 넣을 자리: {MANUAL_DIRS[0]}")
        return 2

    ymds = sorted({_ymd_of(p) for p in all_files if _ymd_of(p)})
    ymd = args.date.strip() or (ymds[-1] if ymds else "")
    if not ymd:
        print("[FATAL] 파일명에서 YYYYMMDD 를 찾지 못했다")
        return 2
    print(f"[DATE] {ymd}   (발견된 날짜: {', '.join(ymds[-6:])})\n")

    print("[1] 전종목 시세 (시가총액 / 상장주식수)")
    px = _load_group(["krx_kospi_*.csv", "krx_kosdaq_*.csv", "krx_price_*.csv"], ymd, "시세")
    print("\n[2] PER/PBR/배당수익률")
    fx = _load_group(["krx_perpbr_*.csv", "krx_per_*.csv", "krx_fundamental_*.csv"], ymd, "PER/PBR")

    if px.empty and fx.empty:
        print(f"\n[FATAL] {ymd} 날짜의 읽을 수 있는 파일이 없다")
        return 3

    out = px if not px.empty else pd.DataFrame(columns=["code"])
    if not fx.empty:
        out = fx if out.empty else out.merge(fx, on="code", how="outer", suffixes=("", "_f"))
        for c in list(out.columns):
            if c.endswith("_f"):
                base = c[:-2]
                if base in out.columns:
                    out[base] = out[base].fillna(out[c])
                out = out.drop(columns=[c])

    for c in OUT_COLS:
        if c not in out.columns:
            out[c] = pd.NA
    out = out[OUT_COLS].dropna(subset=["code"]).drop_duplicates("code")

    print("\n[3] 결과")
    print(f"  종목 {len(out)}개")
    for c in OUT_COLS[1:]:
        n = int(pd.to_numeric(out[c], errors="coerce").notna().sum())
        print(f"    {c:14s} {n:5d} / {len(out)}  ({n / max(1, len(out)) * 100:5.1f}%)")

    # 비용 티어가 실제로 갈리는지 - 이 변환의 목적이다
    mc = pd.to_numeric(out["market_cap"], errors="coerce")
    if mc.notna().any():
        large = int((mc >= 1_000_000_000_000).sum())
        mid = int(((mc >= 100_000_000_000) & (mc < 1_000_000_000_000)).sum())
        small = int((mc < 100_000_000_000).sum())
        print(f"\n  비용 티어 분포 (paper_engine_config 기준)")
        print(f"    large  >=1조     {large:5d}  슬리피지 0.3%")
        print(f"    mid    >=1000억  {mid:5d}  슬리피지 0.5%")
        print(f"    small  <1000억   {small:5d}  슬리피지 1.0%")
        print(f"    -> 지금은 market_cap 결측이라 **전 종목이 small(1.0%)** 로 떨어진다")

    # 오늘 후보와의 교집합 - 실제로 닿는지
    cand = ROOT / "2_Logs" / "candidates_latest_data.with_final_score.csv"
    if cand.exists():
        try:
            cdf = pd.read_csv(cand, dtype={"code": str})
            if "code" in cdf.columns:
                codes = set(cdf["code"].map(_norm_code6).dropna())
                hit = len(codes & set(out["code"]))
                print(f"\n  최신 후보 {len(codes)}종목 중 커버 {hit}종목 ({hit / max(1, len(codes)) * 100:.0f}%)")
        except Exception as exc:  # noqa: BLE001
            print(f"  [WARN] 후보 대조 실패: {exc}")

    if args.dry_run:
        print("\n[DRY-RUN] 아무것도 쓰지 않았다")
        return 0

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    dated = CACHE_DIR / f"pykrx_fundamental_{ymd}.csv"
    latest = CACHE_DIR / "pykrx_fundamental_latest.csv"
    if latest.exists():
        bak = latest.with_name(latest.name + ".bak_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S"))
        shutil.copy2(latest, bak)
        print(f"\n[BACKUP] {bak.name}")
    out.to_csv(dated, index=False, encoding="utf-8-sig")
    out.to_csv(latest, index=False, encoding="utf-8-sig")
    print(f"[WROTE] {dated}")
    print(f"[WROTE] {latest}")
    print("\n다음 후보 생성부터 반영된다. 확인: 후보 파일의 market_cap 이 채워지는지.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
