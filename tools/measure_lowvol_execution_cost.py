# -*- coding: utf-8 -*-
"""저변동성(`vol_20`) 십분위별 **실호가 스프레드**를 재서 집행 비용 가정을 검정한다.

## 왜 만드나

2026-09-10 H007 에서 `vol_20` 저변동성 단조성이 봉인 구간에서 재현됐다
(rho -0.903, D0~D3 +1.14%p, 스프레드 3.78%p). 이 시스템의 첫 표본 외 증거다.

그런데 그건 **패널 측정**이다. 집행 슬리피지와 호가 충격이 안 들어갔다.
`feedback_three_verification_questions`: 신호가 있느냐와 매매가 되느냐는 다른 질문이다.

## 무엇을 검정하나

```
비용 0.400% 왕복의 구성
  거래세     0.200%  (매도만, 고정. 브로커 정산으로 확인됨)
  슬리피지   0.100% x 2
  -> 슬리피지 예산은 편도 **10bps**, 즉 **half-spread <= 10bps** 여야 한다

예측  저변동성 종목은 대체로 크고 유동성이 좋다.
      D0~D3 의 half-spread 가 유니버스 평균보다 낮고 10bps 안에 들어온다
반증  half-spread 가 10bps 를 넘으면 0.400% 가정이 이 전략에 맞지 않는다
```

십분위 **전체**를 재는 이유: 스프레드가 십분위를 따라 같이 움직이면
H007 의 알파가 유동성 프리미엄일 수 있다. 그건 알파 해석 자체를 바꾼다.

## 읽기 전용

주문을 넣지 않는다. `kis_quote_poll.py` 로 호가만 조회한다.

[2026-09-10] 신설.
"""
from __future__ import annotations

import argparse
import datetime as dt
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tools"))
from design_signal_inventory import load_merged, CA_HI, CA_LO  # noqa: E402


def build_deciles(as_of: str | None, min_value: float, vol_win: int) -> pd.DataFrame:
    """기준일의 `vol_<win>` 십분위. 거래가능(min_value 이상)만 대상."""
    d = load_merged()
    px = d.pivot_table(index="date", columns="code", values="close", aggfunc="last")
    val = d.pivot_table(index="date", columns="code", values="value", aggfunc="last")
    px.index = px.index.astype(str)
    val = val.reindex(index=px.index, columns=px.columns)

    t = str(as_of).replace("-", "") if as_of else str(px.index.max())
    if t not in px.index:
        t = max(x for x in px.index if x <= t)
    print("  기준일 %s (패널 최신 %s)" % (t, px.index.max()))

    r1 = px / px.shift(1) - 1.0
    r1 = r1.where((r1 <= CA_HI - 1) & (r1 >= CA_LO - 1))   # 기업행위/가격제한 정제
    vol = r1.rolling(vol_win, min_periods=max(5, vol_win * 3 // 4)).std()

    row_v = vol.loc[t]
    row_val = val.loc[t]
    row_px = px.loc[t]
    m = (row_val >= min_value) & row_px.notna() & row_v.notna()
    x = row_v[m]
    print("  거래가능 %d종목 (거래대금 %.0f억 이상)" % (len(x), min_value / 1e8))
    q = pd.qcut(x.rank(method="first"), 10, labels=False, duplicates="drop")
    out = pd.DataFrame({"code": x.index, "vol": x.values, "decile": q.values})
    out["close"] = row_px.reindex(out["code"]).values
    out["value"] = row_val.reindex(out["code"]).values
    out["as_of"] = t
    return out.sort_values(["decile", "vol"]).reset_index(drop=True)


def poll_quotes(codes: list[str], mock: str) -> pd.DataFrame:
    tmp = Path(tempfile.gettempdir()) / ("lowvol_q_%s.csv" % dt.datetime.now().strftime("%H%M%S"))
    cmd = [sys.executable, str(ROOT / "tools" / "kis_quote_poll.py"),
           "--codes", ",".join(codes), "--mock", mock, "--iterations", "1",
           "--interval-sec", "0.3", "--out-csv", str(tmp)]
    print("  호가 조회 %d종목 (약 %d초)" % (len(codes), int(len(codes) * 0.35) + 20))
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=2400)
    if not tmp.exists():
        print("[STOP] 호가 조회 실패: %s" % (r.stderr or r.stdout or "")[-300:])
        return pd.DataFrame()
    q = pd.read_csv(tmp, dtype=str, encoding="utf-8-sig")
    q["code"] = q["code"].astype(str).str.zfill(6)
    for c in ("ask1", "bid1"):
        q[c] = pd.to_numeric(q.get(c), errors="coerce")
    return q


def main() -> int:
    ap = argparse.ArgumentParser(description="저변동성 십분위별 실호가 스프레드 (H007 후속)")
    ap.add_argument("--as-of", default=None, help="기준일 YYYYMMDD. 비우면 패널 최신")
    ap.add_argument("--min-value", type=float, default=2e9, help="사전등록 유니버스 하한")
    ap.add_argument("--vol-win", type=int, default=20, help="vol_20 이 H007 에서 재현된 축")
    ap.add_argument("--per-decile", type=int, default=30, help="십분위당 표본 수")
    ap.add_argument("--capital", type=float, default=100_000_000)
    ap.add_argument("--positions", type=int, default=50)
    ap.add_argument("--hold", type=int, default=20, help="H007 의 지평")
    ap.add_argument("--budget-bps", type=float, default=10.0,
                    help="편도 슬리피지 예산(bps). 0.400%% 왕복 - 거래세 0.2%% 의 절반")
    ap.add_argument("--mock", default="false", choices=["auto", "true", "false"])
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--out", default="2_Logs/design/lowvol_exec_cost_latest.csv")
    a = ap.parse_args()

    print("저변동성 집행 비용 측정 (읽기 전용, 주문 없음)")
    dec = build_deciles(a.as_of, a.min_value, a.vol_win)

    rng = np.random.default_rng(a.seed)
    picks = []
    for dv, g in dec.groupby("decile"):
        n = min(a.per_decile, len(g))
        picks.append(g.iloc[rng.choice(len(g), n, replace=False)])
    smp = pd.concat(picks, ignore_index=True)
    print("  표본 %d종목 (십분위당 최대 %d)" % (len(smp), a.per_decile))

    q = poll_quotes(smp["code"].tolist(), a.mock)
    if q.empty:
        return 2
    m = smp.merge(q[[c for c in ("code", "ask1", "bid1", "status") if c in q.columns]],
                  on="code", how="left")
    ok = m[(m.get("status", "OK") == "OK") & (m["ask1"] > 0) & (m["bid1"] > 0)].copy()
    print("  유효 호가 %d / %d" % (len(ok), len(m)))
    if len(ok) < 30:
        print("[STOP] 유효 호가가 너무 적어요")
        return 2

    ok["mid"] = (ok["ask1"] + ok["bid1"]) / 2.0
    ok["spread_bps"] = (ok["ask1"] - ok["bid1"]) / ok["mid"] * 10000.0
    ok["half_bps"] = ok["spread_bps"] / 2.0
    # 1포지션 금액이 일 거래대금에서 차지하는 비중 (시장충격 대리)
    slot = a.capital / a.positions
    ok["slot_krw"] = slot
    ok["adv_frac_pct"] = slot / ok["value"] * 100.0

    print()
    print("%6s %6s %10s %10s %10s %10s %12s" %
          ("십분위", "n", "half(중앙)", "half(평균)", "full(중앙)", "예산초과%", "슬롯/거래대금"))
    rows = []
    for dv, g in ok.groupby("decile"):
        over = (g["half_bps"] > a.budget_bps).mean() * 100
        rows.append({
            "as_of": g["as_of"].iloc[0], "decile": int(dv), "n": len(g),
            "half_bps_median": g["half_bps"].median(), "half_bps_mean": g["half_bps"].mean(),
            "spread_bps_median": g["spread_bps"].median(),
            "over_budget_pct": over, "adv_frac_pct_median": g["adv_frac_pct"].median(),
            "vol_median": g["vol"].median(), "value_median_krw": g["value"].median(),
        })
        print("%6d %6d %10.2f %10.2f %10.2f %9.0f%% %11.3f%%" %
              (dv, len(g), g["half_bps"].median(), g["half_bps"].mean(),
               g["spread_bps"].median(), over, g["adv_frac_pct"].median()))

    res = pd.DataFrame(rows)
    low = ok[ok["decile"] <= 3]
    hi = ok[ok["decile"] >= 8]
    turns = 247.0 / a.hold
    print()
    print("=== 전략 대상 D0~D3 (%d종목 표본) ===" % len(low))
    print("  half-spread 중앙 %.2f bps / 평균 %.2f bps   (예산 %.1f bps)"
          % (low["half_bps"].median(), low["half_bps"].mean(), a.budget_bps))
    print("  예산 초과 종목 비율 %.0f%%" % ((low["half_bps"] > a.budget_bps).mean() * 100))
    print("  1포지션 {:,.0f}원 = 일 거래대금의 중앙 {:.3f}%".format(
        slot, low["adv_frac_pct"].median()))
    rt = (low["half_bps"].median() * 2) / 100.0 + 0.200        # 왕복 슬리피지% + 거래세%
    print("  -> 실측 기반 왕복 비용 **%.3f%%**  (가정 0.400%%)" % rt)
    print("     연 %.1f회전 -> 연 비용 **%.1f%%**  (가정으로는 %.1f%%)"
          % (turns, rt * turns, 0.400 * turns))
    print()
    print("=== 대조 D8~D9 (%d종목) ===" % len(hi))
    print("  half-spread 중앙 %.2f bps" % hi["half_bps"].median())
    print()
    if low["half_bps"].median() < hi["half_bps"].median():
        print("  주의: 저변동성 쪽이 스프레드도 낮아요. H007 의 알파가")
        print("        **유동성 프리미엄과 섞여 있을 수 있어요.** 분리는 별도 측정이 필요해요")

    out = ROOT / a.out
    out.parent.mkdir(parents=True, exist_ok=True)
    res.to_csv(out, index=False, encoding="utf-8-sig")
    stamp = out.with_name(out.name.replace("_latest", "_%s" % dt.datetime.now().strftime("%Y%m%d_%H%M%S")))
    res.to_csv(stamp, index=False, encoding="utf-8-sig")
    ok.to_csv(str(stamp).replace(".csv", "_detail.csv"), index=False, encoding="utf-8-sig")
    print("  wrote %s" % out)
    print("  wrote %s (+_detail)" % stamp)
    print()
    print("  주의: 한 시점의 호가예요. 장중 시각·변동성에 따라 움직여요.")
    print("        하루로 결론내지 말고 며칠 모아야 해요(measure_spread_passrate 와 같은 성격).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
