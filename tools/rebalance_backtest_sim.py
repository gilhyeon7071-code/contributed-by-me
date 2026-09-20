# -*- coding: utf-8 -*-
"""리밸런싱 실행기로 11.4년을 돌린다. 실운용 경로와 같은 구조로.

2026-08-24 신규 / **2026-08-25 전면 수정**.

재구현하지 않는다 - tools/rebalance_portfolio.py 의 함수를 그대로 임포트해서 쓴다.
그래야 차이가 나면 구현 차이가 아니라 실제 결함이라고 말할 수 있다.

## 2026-08-25 에 무엇이 바뀌었나 - 첫 판 1.79배는 폐기한다

(110) 에서 실장부 보고서를 만들며 결함 3개가 드러났고, **셋 다 이 파일에도 있었다.**

```
(1) 1봉 선견     같은 봉 종가로 고르고 그 종가에 샀다
(2) 0원 평가     그날 종가가 없는 보유를 0원으로 셌다
(3) 체결가 무검사 부패한 패널 종가로 그대로 사고팔았다
```

(3) 은 시험 장부에서 000150 을 7,730원에 사서 2,203,000원에 "팔아"
**100M 장부에 +30.7M 가짜 차익**을 만들었다 - 그 장부 수익률 전체였다.

그래서 리밸런싱일만 도는 루프를 버리고 **매 거래일을 도는 루프**로 바꿨다.
그래야 일별 평가·기업행위·데이터 결함을 실장부와 같은 방식으로 처리할 수 있고,
`2_Logs/rebalance/rebal_equity.csv` 와 나란히 놓고 읽을 수 있다.

```
신호일 D 의 종가로 목표를 정하고, 그 다음 거래일 D+1 의 종가로 체결한다
```
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from rebalance_portfolio import load_panel, add_vol60, build_target  # noqa: E402
from load_merged_panel import load_merged  # noqa: E402

ROOT = Path(r"E:\1_Data")
OUT = ROOT / "2_Logs" / "rebalance"

CA_HI, CA_LO = 1.305, 0.695        # 일간 가격제한 +-30% 밖 = 정상 등락이 아니다
PLAUS_HI, PLAUS_LO = 10.0, 0.1     # 분할·병합으로 설명 가능한 배율의 한계
LEVEL_HI, LEVEL_LO = 10.0, 0.1     # 1년 중앙 종가 대비 체결가 허용 범위


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--capital", type=float, default=100_000_000)
    ap.add_argument("--exclude-pct", type=float, default=0.20)
    ap.add_argument("--min-value", type=float, default=1e9)
    ap.add_argument("--step", type=int, default=10, help="리밸런싱 간격(거래일)")
    ap.add_argument("--fee-pct", type=float, default=0.00179, help="편도. 왕복 0.358%%의 절반")
    ap.add_argument("--start", default="", help="YYYYMMDD. 비우면 vol60 이 처음 서는 날")
    ap.add_argument("--panel", choices=("archive", "merged"), default="archive",
                    help="archive=krx_daily_archive 만(기존 동작). "
                         "merged=archive+Raw/RAW_WIDE 통합. "
                         "archive 단독은 2022-10~2024-12 코스닥이 비어 있다 - PLANS (134)")
    ap.add_argument("--out", default=str(OUT / "sim_equity.csv"))
    args = ap.parse_args()

    # [2026-08-28] --panel merged 면 Raw/RAW_WIDE 를 합친 패널을 쓴다.
    #   archive 단독은 2022-10~2024-12 에 코스닥이 통째로 없어서
    #   그 두 해 포트 코스닥 비중이 정확히 0.0% 가 된다 (PLANS (134)).
    #   기본값은 archive 로 두어 기존 산출물과의 비교 가능성을 유지한다.
    _panel = load_merged() if args.panel == "merged" else load_panel()
    print("[PANEL] %s rows=%d codes=%d" % (args.panel, len(_panel), _panel["code"].nunique()))
    full = add_vol60(_panel)                # 가격 조회용 - 필터를 걸지 않는다
    d = full.dropna(subset=["vol60"])
    d = d[d["value"] >= args.min_value]      # 유니버스 선정용

    cal = sorted(str(x) for x in full["date"].unique())
    uni_dates = sorted(str(x) for x in d["date"].unique())
    if args.start:
        uni_dates = [x for x in uni_dates if x >= args.start]
    sig_dates = uni_dates[:: args.step]

    # 신호일 D -> 집행일 D+1 (선견 없음). 마지막 신호일은 그 다음 날이 없으면 버린다.
    nxt = {c: cal[i + 1] for i, c in enumerate(cal[:-1])}
    plan = [(s, nxt[s]) for s in sig_dates if s in nxt]
    exec_of = dict(plan)
    exec_days = {e: s for s, e in plan}

    run_from = plan[0][1]
    days = [c for c in cal if c >= run_from]
    print("패널 %d행  거래일 %d  리밸런싱 %d회 (간격 %d일)"
          % (len(full), len(days), len(plan), args.step))
    print("  신호 %s -> 집행 %s  ...  신호 %s -> 집행 %s"
          % (plan[0][0], plan[0][1], plan[-1][0], plan[-1][1]))

    slim = d[d["date"].isin(sig_dates)].copy()          # 목표 산출은 신호일만 필요
    # [2026-08-24] 가격 조회는 min_value 필터가 걸리지 않은 패널에서 한다.
    #   필터된 패널로 조회하면 거래대금이 줄어든 보유 종목을 "종가 없음"으로 보고
    #   영원히 못 팔아 보유가 1464종목까지 쌓였다.
    close_by = {dt_: g.drop_duplicates("code").set_index("code")["close"]
                for dt_, g in full[full["date"].isin(days)].groupby("date")}

    # 체결가 수준 검사용 1년 중앙 종가 (집행일마다)
    med_by = {}
    for s, e in plan:
        lo = (pd.Timestamp(e) - pd.Timedelta(days=400)).strftime("%Y%m%d")
        h = full[(full["date"] < e) & (full["date"] >= lo)]
        med_by[e] = h.groupby("code")["close"].median()

    cash = float(args.capital)
    pos: dict[str, float] = {}
    prev_px: dict[str, float] = {}
    prev_day: dict[str, str] = {}      # 그 종가가 언제 것인지
    rows = []
    n_ca = n_bad = n_blocked = n_noqte = n_gap = 0

    for i, dt_ in enumerate(days):
        px = close_by.get(dt_, pd.Series(dtype=float))
        yday = days[i - 1] if i > 0 else ""

        # 1) 기업행위·데이터결함. 체결보다 먼저 판정한다.
        frozen: dict[str, float] = {}
        for code in list(pos.keys()):
            p_now = float(px.get(code, np.nan))
            p_old = prev_px.get(code, np.nan)
            if not (p_now > 0) or not (p_old > 0):
                continue
            if prev_day.get(code) != yday:
                # [2026-08-25] 거래정지로 며칠 건너뛴 비교에는 가격제한이 안 걸린다.
                #   5일 쉬고 40% 오른 것과 액면분할을 구분할 수 없다.
                #   구분이 안 되면 조정을 지어내지 않는다 - 그대로 받고 센다.
                n_gap += 1
                continue
            r = p_now / p_old
            if CA_LO <= r <= CA_HI:
                continue
            if PLAUS_LO <= r <= PLAUS_HI:
                pos[code] = pos[code] / r          # 분할·병합을 수량으로 흡수
                n_ca += 1
            else:
                frozen[code] = p_old               # 설명 불가 = 데이터 결함. 평가에서 뺀다
                n_bad += 1

        def mark(code: str) -> float:
            if code in frozen:
                return frozen[code]
            p = float(px.get(code, np.nan))
            if p > 0:
                return p
            return float(prev_px.get(code, 0.0) or 0.0)   # 거래정지는 0원이 아니다

        # 2) 집행일이면 매매한다
        if dt_ in exec_days:
            sig = exec_days[dt_]
            try:
                target, meta = build_target(slim, sig, args.capital,
                                            args.exclude_pct, args.min_value)
            except Exception:
                target, meta = None, None
            if target is not None:
                med = med_by.get(dt_, pd.Series(dtype=float))
                want = target.set_index("code")["target_qty"].to_dict()

                def tradable(code: str) -> float:
                    """체결 가능한 가격. 정상성 검사를 통과하지 못하면 0."""
                    nonlocal n_blocked, n_noqte
                    p = float(px.get(code, 0.0) or 0.0)
                    if p <= 0:
                        n_noqte += 1
                        return 0.0
                    if code in frozen:
                        n_blocked += 1
                        return 0.0
                    pp = float(prev_px.get(code, 0.0) or 0.0)
                    if pp > 0 and prev_day.get(code) == yday and not (CA_LO <= p / pp <= CA_HI):
                        n_blocked += 1
                        return 0.0
                    pm = float(med.get(code, 0.0) or 0.0)
                    if pm > 0 and not (LEVEL_LO <= p / pm <= LEVEL_HI):
                        n_blocked += 1
                        return 0.0
                    return p

                for code in list(pos.keys()):                 # 매도 먼저(현금 확보)
                    tgt = float(want.get(code, 0))
                    have = float(pos[code])
                    if tgt >= have:
                        continue
                    p = tradable(code)
                    if p <= 0:
                        continue
                    q = have - tgt
                    gross = p * q
                    cash += gross - gross * args.fee_pct
                    pos[code] = have - q
                    if pos[code] <= 1e-9:
                        pos.pop(code, None)

                for code, tgt in want.items():
                    have = float(pos.get(code, 0.0))
                    if tgt <= have:
                        continue
                    p = tradable(code)
                    if p <= 0:
                        continue
                    q = float(tgt) - have
                    need = p * q * (1 + args.fee_pct)
                    if need > cash:
                        q = cash / (p * (1 + args.fee_pct))
                        if q <= 0:
                            continue
                        need = p * q * (1 + args.fee_pct)
                    cash -= need
                    pos[code] = have + q

        # 3) 평가
        mv = sum(q * mark(c) for c, q in pos.items())
        rows.append({"date": dt_, "cash": cash, "mv": mv, "equity": cash + mv,
                     "n_pos": len(pos), "is_exec": int(dt_ in exec_days)})

        for code in pos:
            p = float(px.get(code, np.nan))
            if p > 0 and code not in frozen:
                prev_px[code] = p
                prev_day[code] = dt_
        if (i + 1) % 500 == 0:
            print("  %s  자산 %s원  보유 %d" % (dt_, format(int(cash + mv), ","), len(pos)))

    eq = pd.DataFrame(rows)
    if eq.empty:
        print("[STOP] 결과 없음")
        return 1
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    eq.to_csv(args.out, index=False, encoding="utf-8-sig")

    yrs = len(eq) / 247.0
    total = eq["equity"].iloc[-1] / args.capital
    cagr = (total ** (1 / yrs) - 1) * 100
    r = eq["equity"].pct_change().dropna()
    dd = (eq["equity"] / eq["equity"].cummax() - 1).min()
    print()
    print("=== 실행기 시뮬레이션 (선견 제거 / 일별 평가 / 체결가 검사) ===")
    print("  기간 %s ~ %s  (%.1f년, 거래일 %d, 리밸런싱 %d회)"
          % (eq["date"].iloc[0], eq["date"].iloc[-1], yrs, len(eq), int(eq["is_exec"].sum())))
    print("  최종자산 %s원  누적 %.2f배  CAGR %.2f%%"
          % (format(int(eq["equity"].iloc[-1]), ","), total, cagr))
    print("  일변동성 %.2f%%  연환산 %.1f%%  최대낙폭 %.1f%%"
          % (100 * r.std(), 100 * r.std() * np.sqrt(252), 100 * dd))
    print("  평균 보유 %.0f종목" % eq["n_pos"].mean())
    print("  기업행위 흡수 %d건 / 데이터결함 %d건 / 체결 차단 %d건"
          % (n_ca, n_bad, n_blocked))
    print("  종가없어 못 산 주문 %d건 / 거래공백이라 판정 보류 %d건" % (n_noqte, n_gap))
    print("  wrote %s" % args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
