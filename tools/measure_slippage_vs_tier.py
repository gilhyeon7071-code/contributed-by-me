# -*- coding: utf-8 -*-
"""설정된 슬리피지 티어가 실제 호가 폭과 얼마나 벌어져 있는지 잰다.

2026-08-25 신규. (106) 이후 열려 있던 비용 결정 (가) - "티어 슬리피지가 실측의 2.8배"
를 닫기 위한 측정이다. 그동안은 거래 2건에서 역산한 값뿐이라 정할 수 없었다.

## 무엇을 기준으로 삼나

슬리피지의 물리적 하한은 **호가 폭의 절반**이다. 최우선호가에 걸어 체결되면
중간가 대비 half-spread 를 지불한다. 호가를 넘겨 치면 full spread 를 지불한다.
시장충격은 이 장부에서 무시할 만하다 - 슬롯이 198,019원이고 대상 종목의
일 거래대금이 최소 10억이라 하루 물량의 0.02% 도 안 된다.

그래서 **half-spread <= 합리적 슬리피지 <= full spread** 가 실측 기반 범위다.
설정된 티어가 이 범위에서 몇 배 떨어져 있는지 본다.

## 시총은 다시 계산한다

`_cache/krx_current_industry_master_*.csv` 의 `market_cap` 은 그 파일 날짜 기준이라
낡았다. `listed_shares` 는 잘 안 변하므로 **상장주식수 x 오늘 종가**로 다시 만든다.
티어 경계가 1조 / 1000억이라 웬만한 오차로는 티어가 안 바뀐다.

읽기 전용이다. 설정을 바꾸지 않는다 - 바꾸려면 잠금 도구를 거쳐야 한다
([[feedback_check_lock_before_editing_config]]).
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(r"E:\1_Data")
sys.path.insert(0, str(ROOT / "tools"))
from load_merged_panel import load_merged, add_vol60          # noqa: E402
from rebalance_portfolio import build_target                  # noqa: E402

OUT_DIR = ROOT / "2_Logs" / "measure"


def load_listed_shares() -> pd.Series:
    """상장주식수. **생산이 쓰는 파일을 먼저 본다.**

    [2026-08-27] 처음엔 `_cache/krx_current_industry_master_*.csv` 만 봤는데
    그건 2026-07-15 `_partial` 스냅샷이고 **생산 경로의 산출물이 아니다**(42일 낡음).
    생산(`generate_candidates_v41_1.py:_load_pykrx_fundamental_snapshot`)은
    `_cache/pykrx_fundamental_latest.csv` 를 읽는다 - market_cap/listed_shares 가
    2,873종목 100% 채워져 있다.

    두 소스로 티어를 매겨 비교했더니 508종목 중 **6종목(1.2%)만 갈렸고 전부 경계선**이었다.
    그래도 근거는 생산과 같은 것을 쓰는 편이 낫다.

    시총은 여기서 직접 읽지 않고 **상장주식수 x 오늘 종가**로 다시 만든다.
    파일의 market_cap 은 그 파일 날짜의 가격이라 가격이 낡는다. 주식수는 잘 안 변한다.
    """
    prod = ROOT / "_cache" / "pykrx_fundamental_latest.csv"
    if prod.exists():
        d = pd.read_csv(prod, dtype=str, encoding="utf-8-sig")
        d["code"] = d["code"].astype(str).str.zfill(6)
        d["listed_shares"] = pd.to_numeric(d.get("listed_shares"), errors="coerce")
        d = d[d["listed_shares"] > 0].drop_duplicates("code", keep="last")
        if len(d) > 1000:
            age = (dt.datetime.now() - dt.datetime.fromtimestamp(prod.stat().st_mtime)).days
            print("  상장주식수 원본 %s (%d종목, %d일 전) [생산 소스]" % (prod.name, len(d), age))
            return d.set_index("code")["listed_shares"]

    files = sorted(glob.glob(str(ROOT / "_cache" / "krx_current_industry_master_*.csv")))
    if not files:
        raise RuntimeError("상장주식수 원본이 없다 (pykrx_fundamental_latest / industry_master 모두)")
    d = pd.read_csv(files[-1], dtype=str, encoding="utf-8-sig")
    d["code"] = d["code"].astype(str).str.zfill(6)
    d["listed_shares"] = pd.to_numeric(d["listed_shares"], errors="coerce")
    d = d[d["listed_shares"] > 0].drop_duplicates("code", keep="last")
    age = (dt.datetime.now() - dt.datetime.fromtimestamp(Path(files[-1]).stat().st_mtime)).days
    print("  [폴백] 상장주식수 원본 %s (%d종목, %d일 전)" % (Path(files[-1]).name, len(d), age))
    return d.set_index("code")["listed_shares"]


def main() -> int:
    ap = argparse.ArgumentParser(description="슬리피지 티어 vs 실제 호가 폭")
    ap.add_argument("--capital", type=float, default=100_000_000)
    ap.add_argument("--exclude-pct", type=float, default=0.20)
    ap.add_argument("--min-value", type=float, default=1e9)
    ap.add_argument("--config", default=str(ROOT / "paper" / "paper_engine_config.json"))
    ap.add_argument("--out", default=str(OUT_DIR / "slippage_vs_tier_latest.csv"))
    args = ap.parse_args()

    cfg = json.loads(Path(args.config).read_text(encoding="utf-8-sig"))
    t = cfg.get("tiered_slippage", {}) or {}
    LARGE, MID = float(t.get("large_cap_krw", 1e12)), float(t.get("mid_cap_krw", 1e11))
    # [2026-09-10] 기본값을 실측치로. 0.003/0.005/0.010 은 아무도 재지 않은 코드 상수였다(C8).
    SL = {"large": float(t.get("large_slip_pct", 0.00139)),
          "mid": float(t.get("mid_slip_pct", 0.00178)),
          "small": float(t.get("small_slip_pct", 0.01))}
    print("[TIER] 설정  large>=%.0f억 %.2f%%   mid>=%.0f억 %.2f%%   small %.2f%%"
          % (LARGE / 1e8, 100 * SL["large"], MID / 1e8, 100 * SL["mid"], 100 * SL["small"]))
    print("       fee_pct=%.4f  sell_tax_pct=%.4f"
          % (float(cfg.get("fee_pct", 0)), float(cfg.get("sell_tax_pct", 0))))

    d = add_vol60(load_merged())
    as_of = str(d["date"].max())
    target, meta = build_target(d, as_of, args.capital, args.exclude_pct, args.min_value)
    shares = load_listed_shares()
    tg = target.copy()
    tg["listed_shares"] = tg["code"].map(shares)
    tg = tg.dropna(subset=["listed_shares"])
    tg["market_cap"] = tg["listed_shares"] * tg["close"]
    tg["tier"] = np.where(tg["market_cap"] >= LARGE, "large",
                          np.where(tg["market_cap"] >= MID, "mid", "small"))
    print("[TIER] as_of=%s  목표 %d종목 중 상장주식수 확보 %d종목"
          % (as_of, meta["target_count"], len(tg)))

    tmp = Path(tempfile.gettempdir()) / ("slip_q_%s.csv" % dt.datetime.now().strftime("%H%M%S"))
    r = subprocess.run([sys.executable, str(ROOT / "tools" / "kis_quote_poll.py"),
                        "--codes", ",".join(tg["code"].tolist()), "--mock", "false",
                        "--iterations", "1", "--interval-sec", "0.3", "--out-csv", str(tmp)],
                       capture_output=True, text=True, timeout=2400)
    if not tmp.exists():
        print("[STOP] 호가 조회 실패: %s" % (r.stderr or "")[-200:])
        return 2

    q = pd.read_csv(tmp, dtype=str, encoding="utf-8-sig")
    q["code"] = q["code"].astype(str).str.zfill(6)
    for c in ("ask1", "bid1"):
        q[c] = pd.to_numeric(q[c], errors="coerce")
    m = q.merge(tg[["code", "close", "market_cap", "tier", "value"]], on="code", how="inner")
    ok = m[(m["status"] == "OK") & (m["ask1"] > 0) & (m["bid1"] > 0)].copy()
    if len(ok) < 30:
        print("[STOP] 유효 호가가 %d개뿐" % len(ok))
        return 2
    ok["mid_px"] = (ok["ask1"] + ok["bid1"]) / 2
    ok["spread_pct"] = (ok["ask1"] - ok["bid1"]) / ok["mid_px"]
    ok["half_spread_pct"] = ok["spread_pct"] / 2
    ok["tier_slip_pct"] = ok["tier"].map(SL)
    ok["ratio_vs_half"] = ok["tier_slip_pct"] / ok["half_spread_pct"].replace(0, np.nan)
    ok["ratio_vs_full"] = ok["tier_slip_pct"] / ok["spread_pct"].replace(0, np.nan)

    print()
    print("%-7s %6s %12s %11s %11s %11s %9s %9s"
          % ("티어", "종목", "시총중앙(억)", "half중앙", "full중앙", "설정슬립", "/half", "/full"))
    for tier in ("large", "mid", "small"):
        g = ok[ok["tier"] == tier]
        if not len(g):
            print("%-7s %6d  (해당 없음)" % (tier, 0))
            continue
        print("%-7s %6d %12.0f %10.3f%% %10.3f%% %10.3f%% %8.1fx %8.1fx"
              % (tier, len(g), g["market_cap"].median() / 1e8,
                 100 * g["half_spread_pct"].median(), 100 * g["spread_pct"].median(),
                 100 * SL[tier], g["ratio_vs_half"].median(), g["ratio_vs_full"].median()))

    tax = float(cfg.get("sell_tax_pct", 0))
    fee = float(cfg.get("fee_pct", 0))
    print()
    print("왕복 비용 비교 (슬리피지x2 + 수수료x2 + 매도세)")
    print("  %-34s %8s" % ("근거", "왕복"))
    for tier in ("large", "mid", "small"):
        g = ok[ok["tier"] == tier]
        if not len(g):
            continue
        print("  %-34s %7.3f%%" % ("설정 티어 (%s)" % tier, 100 * (SL[tier] * 2 + fee * 2 + tax)))
    hs = ok["half_spread_pct"].median()
    fs = ok["spread_pct"].median()
    print("  %-34s %7.3f%%" % ("half-spread 기준(호가에 걸기)", 100 * (hs * 2 + fee * 2 + tax)))
    print("  %-34s %7.3f%%" % ("full-spread 기준(호가를 넘겨 치기)", 100 * (fs * 2 + fee * 2 + tax)))
    print("  %-34s %7.3f%%" % ("승인된 비용 원장", 0.358))
    print("  %-34s %7.3f%%" % ("실주문 실측(2026-08-25, 2종목)", 0.4297))

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    keep = ["code", "tier", "market_cap", "close", "value", "bid1", "ask1",
            "spread_pct", "half_spread_pct", "tier_slip_pct", "ratio_vs_half", "ratio_vs_full"]
    ok[keep].to_csv(out, index=False, encoding="utf-8-sig")
    stamped = out.with_name(out.name.replace("_latest", "_%s" % dt.datetime.now().strftime("%Y%m%d_%H%M%S")))
    ok[keep].to_csv(stamped, index=False, encoding="utf-8-sig")
    print("\n  wrote %s (%d행)" % (out, len(ok)))
    try:
        tmp.unlink()
    except OSError:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
