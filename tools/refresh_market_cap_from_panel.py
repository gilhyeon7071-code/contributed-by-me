# -*- coding: utf-8 -*-
"""시가총액을 로컬 일별 패널로 다시 계산해 fundamental 캐시에 반영한다.

왜 필요한가
-----------
`_cache/pykrx_fundamental_latest.csv` 는 pykrx API 산물이 아니라 **수동 변환분**이고,
2026-08-22 이후 갱신이 멈춰 있었다. 그 사이 pykrx 는 완전히 깨졌다 - 2026-09-09 실측:

    get_market_fundamental_by_ticker(d, market='ALL')   모든 날짜 KeyError
    get_market_cap_by_ticker(d, market='ALL')           동일
    (KRX 응답 컬럼이 바뀌었다. pykrx 1.2.3)

그런데 **시가총액은 외부 API 없이 구할 수 있다.** market_cap = 종가 x 상장주식수 이고
둘 다 로컬에 있다(clean parquet 의 close, 캐시의 listed_shares).

검증 (2026-09-09)
    20260821 종가 x listed_shares  vs  같은 날 스냅샷의 market_cap
    2,578종목 전부 오차 0.1% 이내, 중앙 오차 **0.000000%**  -> 산식이 원본을 재현한다

왜 중요한가 - 시총은 판정 문턱에 쓰인다
    generate_candidates_v41_1.py:1407  KOSDAQ and market_cap <= 3,000억
    generate_candidates_v41_1.py:1444  market_cap >= 1조 -> hard_threshold 상향
    2026-09-09 실측: 08-21 값 대비 551종목이 10% 초과, 112종목이 30% 초과 움직였고
    **문턱 판정이 뒤집히는 종목이 48개**였다(3,000억 39 / 1조 9).

하는 일 / 안 하는 일
--------------------
    한다     market_cap 열만 다시 계산해 덮는다. listed_shares 는 그대로 둔다
    안 한다  PER/PBR/BPS/EPS/DIV/DPS 는 **손대지 않는다.**
             재무제표에서 오는 값이라 종가로 만들 수 없다. pykrx 가 고쳐져야 갱신된다
             -> 이 파일의 mtime 이 새로워져도 그 여섯 열은 여전히 낡았다.
                그 사실을 숨기지 않으려고 열 두 개를 함께 쓴다:
                  market_cap_as_of        시총을 계산한 거래일
                  fundamental_frozen_at   재무 열이 마지막으로 갱신된 날 (보존)

[2026-09-09] .agent/PLANS.md (274)
"""
from __future__ import annotations

import argparse
import glob
import io
import json
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / "_cache"
FUND = CACHE / "pykrx_fundamental_latest.csv"
PANEL_DIR = ROOT / "krx_daily_archive"
FIN_COLS = ["PER", "PBR", "BPS", "EPS", "DIV", "DPS"]

# 문턱은 소비 코드에 박혀 있다. 여기서는 영향 보고에만 쓴다(판정하지 않는다).
THRESHOLDS = [(300_000_000_000, "3,000억 (KOSDAQ junk 판정)"),
              (1_000_000_000_000, "1조 (hard_threshold)")]


def _latest_panel() -> Path | None:
    files = sorted(glob.glob(str(PANEL_DIR / "krx_daily_*_clean.parquet")))
    return Path(files[-1]) if files else None


def _panel_close(p: Path) -> pd.Series:
    px = pd.read_parquet(p)
    if "code" not in px.columns or "close" not in px.columns:
        raise SystemExit("[STOP] 패널에 code/close 가 없다: %s" % p.name)
    px = px.copy()
    px["code"] = px["code"].astype(str).str.zfill(6)
    # [주의] krx_daily parquet 은 휴장일을 close=0 으로 채운다. 거르지 않으면 시총이 0이 된다.
    #   ([[project_1data_price_panel_zero_padding]] - 이 함정이 과거에 결론을 뒤집은 적이 있다)
    close = pd.to_numeric(px["close"], errors="coerce").fillna(0.0)
    px = px[close > 0]
    if px.empty:
        raise SystemExit("[STOP] close>0 인 행이 없다: %s" % p.name)
    return px.groupby("code")["close"].last()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="없으면 계획만 출력한다")
    ap.add_argument("--panel", default="", help="쓸 clean parquet 경로(기본: 가장 최근)")
    ap.add_argument("--max-drift", type=float, default=5.0,
                    help="이 배수를 넘는 시총 변화는 갱신에서 제외한다(상장주식수 오류 방어)")
    ap.add_argument("--max-wild-frac", type=float, default=0.02,
                    help="제외 비율이 이보다 크면 계통 문제로 보고 멈춘다")
    a = ap.parse_args()

    if not FUND.exists():
        print("[STOP] 캐시가 없다: %s" % FUND)
        return 2
    panel = Path(a.panel) if a.panel else _latest_panel()
    if panel is None or not panel.exists():
        print("[STOP] clean parquet 을 찾지 못했다: %s" % PANEL_DIR)
        return 2

    m = re.search(r"(\d{8})", panel.name)
    as_of = m.group(1) if m else ""
    close = _panel_close(panel)
    fu = pd.read_csv(FUND, encoding="utf-8-sig", dtype={"code": str})
    fu["code"] = fu["code"].astype(str).str.zfill(6)
    if "listed_shares" not in fu.columns or "market_cap" not in fu.columns:
        print("[STOP] listed_shares/market_cap 열이 없다")
        return 2

    print("[SRC] 패널 %s (as_of=%s, %d종목) / 캐시 %d종목"
          % (panel.name, as_of, len(close), len(fu)))

    j = fu.merge(close.rename("_close"), left_on="code", right_index=True, how="left")
    have = j["_close"].notna() & pd.to_numeric(j["listed_shares"], errors="coerce").fillna(0).gt(0)
    calc = j["_close"] * pd.to_numeric(j["listed_shares"], errors="coerce")
    old = pd.to_numeric(j["market_cap"], errors="coerce")

    drift = (calc / old).replace([float("inf"), float("-inf")], pd.NA)
    # [2026-09-09] 튀는 종목은 **건너뛴다**(전체를 멈추지 않는다).
    #   실측: 5배 넘게 튀는 14종목은 전부 08-21 패널에도 없던 코드였다.
    #   즉 가격이 움직인 것이 아니라 그 코드의 listed_shares 를 믿을 수 없는 것이다
    #   (우선주/스팩 등 수동 스냅샷의 코드-주식수 대응 문제).
    #   그런 종목에 계산값을 밀어 넣는 것은 낡은 값을 두는 것보다 나쁘다 -> 원래 값을 유지한다.
    #   다만 **비율이 크면 개별 문제가 아니라 계통 문제**이므로 그때는 멈춘다.
    wild_mask = have & (drift.gt(a.max_drift) | drift.lt(1.0 / a.max_drift))
    wild = int(wild_mask.sum())
    if wild:
        frac = wild / max(1, int(have.sum()))
        print("[SKIP] 시총이 %g배 넘게 튀는 종목 %d개 (%.2f%%) - 갱신에서 제외하고 기존 값을 유지한다"
              % (a.max_drift, wild, 100 * frac))
        bad = j[wild_mask]
        print(bad[["code", "listed_shares", "market_cap", "_close"]].head(10).to_string())
        if frac > a.max_wild_frac:
            print("[STOP] 제외 비율이 %.2f%% > 한도 %.2f%% 다. 개별이 아니라 계통 문제로 본다"
                  % (100 * frac, 100 * a.max_wild_frac))
            return 2
        have = have & (~wild_mask)

    n_upd = int(have.sum())
    n_skip = int((~have).sum())
    ratio = (calc[have] / old[have] - 1.0)
    print("[CALC] 갱신 대상 %d종목 / 값 없음 %d종목 (건드리지 않는다)" % (n_upd, n_skip))
    print("       변화율 중앙 %+.2f%%  |변화|>10%%: %d  |변화|>30%%: %d"
          % (100 * float(ratio.median()), int((ratio.abs() > 0.10).sum()),
             int((ratio.abs() > 0.30).sum())))
    for thr, label in THRESHOLDS:
        flip = int(((old[have] <= thr) != (calc[have] <= thr)).sum())
        print("       문턱 %-26s 판정이 뒤집히는 종목 %d개" % (label, flip))

    if not a.apply:
        print("[DRY] --apply 가 없어 쓰지 않았다")
        return 0

    out = fu.copy()
    out.loc[have.values, "market_cap"] = calc[have].round().astype("int64").values
    out["market_cap_as_of"] = as_of
    if "fundamental_frozen_at" not in out.columns:
        # 재무 열이 마지막으로 갱신된 시점을 한 번만 박아 둔다(파일 mtime 은 곧 덮이므로)
        out["fundamental_frozen_at"] = datetime.fromtimestamp(FUND.stat().st_mtime).strftime("%Y%m%d")

    bdir = ROOT / "backup" / "20260909_market_cap_refresh"
    bdir.mkdir(parents=True, exist_ok=True)
    bak = bdir / ("pykrx_fundamental_latest.csv.bak_%s" % datetime.now().strftime("%Y%m%d_%H%M%S"))
    shutil.copy2(FUND, bak)

    tmp = FUND.with_suffix(".csv.tmp")
    out.to_csv(tmp, index=False, encoding="utf-8-sig", lineterminator="\n")
    os.replace(tmp, FUND)
    print("[OUT] %s  (백업 %s)" % (FUND, bak.name))
    print("[NOTE] PER/PBR/BPS/EPS/DIV/DPS 는 갱신되지 않았다. fundamental_frozen_at 열을 볼 것")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
