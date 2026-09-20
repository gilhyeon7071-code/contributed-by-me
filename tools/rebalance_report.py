# -*- coding: utf-8 -*-
"""리밸런싱 실장부의 자산곡선과 성적을 뽑는다.

2026-08-25 신규. 장부는 개시했는데(PLANS 109) **읽는 도구가 없었다** -
`rebal_state.json` / `rebal_trades.csv` 를 참조하는 코드가 생산자 자신뿐이었다.
쌓기만 하고 아무도 안 읽는 상태를 첫날에 막는다.

## 왜 원장을 재생하는가

`rebal_state.json` 의 `history` 는 **체결일에만** 한 줄 생긴다.
리밸런싱 간격이 10거래일이면 그 사이 9일의 자산을 알 수 없다.
그래서 `rebal_trades.csv` 를 날짜순으로 재생해 **매 거래일 보유를 복원하고
그날 종가로 평가**한다. 체결일 자산은 `history` 와 일치해야 한다(검증에 쓴다).

## 기업행위 - 이걸 안 하면 몇 달 뒤 가짜 손실이 쌓인다

krx 패널은 **분할·병합 미조정**이다. 액면분할이 나면 종가가 반토막인데
보유 수량은 그대로라 평가액이 반토막 난다 - 실제로는 수량이 두 배가 된다.
한국 일간 가격제한이 +-30% 이므로 **연속 거래일 종가비가 +-30.5% 를 넘으면
정상 등락으로 설명되지 않는다** -> 기업행위로 보고 수량을 역수로 조정한다.
([[project_1data_panel_methodology]] 의 정제 규칙과 같은 기준)

조정한 건은 전부 화면과 CSV 에 남긴다. 조용히 넘어가지 않는다.

## 벤치마크

개시일 적격 유니버스(거래대금 하한만 통과, vol60 배제 전)를 **동일가중 매수보유**한다.
이 장부의 선정 규칙은 "저변동 80% 중 슬롯 이하 가격"이므로,
벤치마크와의 차이가 곧 **그 필터가 값을 하는가**에 대한 답이다.

읽기 전용이다. 상태도 원장도 건드리지 않는다.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(r"E:\1_Data")
sys.path.insert(0, str(ROOT / "tools"))
from load_merged_panel import load_merged, add_vol60          # noqa: E402

DEFAULT_DIR = ROOT / "2_Logs" / "rebalance"
# 가격제한 +-30% 를 넘는 종가비 = 기업행위.
# [2026-08-25] 상한과 하한은 대칭이 아니다. 하한가는 비율 0.70 이지 1/1.305=0.766 이 아니다.
#   처음에 1/1.305 로 두었더니 **하한가 종목이 전부 기업행위로 잡혔다**
#   (시험 원장 68건 중 중앙 ratio 0.7306 = 하한가). 수량이 1.37배로 부풀어
#   자산이 계속 과대 계상됐다. history 대조가 이걸 잡아냈다.
CA_HI = 1.305
CA_LO = 0.695
# [2026-08-25] 기업행위로 설명 가능한 배율의 한계. 밖이면 데이터 결함으로 본다.
#   실제로 000150 이 7,140 -> 1,600,000 (224배) 로 튀어 하루 +24.6% 를 만들었다.
#   액면분할·병합은 이 범위를 벗어나지 않는다.
CA_PLAUSIBLE_HI = 10.0
CA_PLAUSIBLE_LO = 0.1


def load_ledger(p: Path) -> pd.DataFrame:
    if not p.exists():
        return pd.DataFrame(columns=["exec_date", "side", "code", "qty", "price", "gross", "fee"])
    d = pd.read_csv(p, dtype=str, encoding="utf-8-sig")
    d["code"] = d["code"].astype(str).str.zfill(6)
    d["exec_date"] = d["exec_date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    for c in ("qty", "price", "gross", "fee"):
        d[c] = pd.to_numeric(d.get(c), errors="coerce").fillna(0.0)
    d["qty"] = d["qty"].astype(int)
    return d.sort_values("exec_date").reset_index(drop=True)


def replay(led: pd.DataFrame, closes: dict[str, pd.Series], dates: list[str],
           capital: float) -> tuple[pd.DataFrame, list[dict], list[dict]]:
    """원장을 재생해 매 거래일 자산을 만든다.

    기업행위는 **수량이 아니라 가격 배수로** 흡수한다.
    처음엔 수량을 나눴는데 `round(14/224.09)=0` 이 되는 순간 조정을 통째로 건너뛰어
    오염된 가격이 그대로 평가에 들어갔다(하루 +24.6%). 정수 반올림이 개입하지 않는
    가격 배수라야 평가액이 연속으로 이어진다.
    """
    pos: dict[str, int] = {}
    adj: dict[str, float] = {}          # 기업행위 누적 가격 배수
    cash = float(capital)
    rows, ca_log, bad_log = [], [], []
    n_gap = 0
    prev_px: dict[str, float] = {}      # 조정 전 원본 종가(배율 판정용)
    prev_day: dict[str, str] = {}       # 그 종가가 언제 것인지
    by_date = {d_: g for d_, g in led.groupby("exec_date")}

    for i_, d_ in enumerate(dates):
        px = closes.get(d_, pd.Series(dtype=float))
        yday = dates[i_ - 1] if i_ > 0 else ""

        # 1) 기업행위·데이터결함 판정을 체결보다 먼저 한다
        for code in list(pos.keys()):
            p_now = float(px.get(code, np.nan))
            p_old = prev_px.get(code, np.nan)
            if not (p_now > 0) or not (p_old > 0):
                continue
            if prev_day.get(code) != yday:
                # [2026-08-25] 거래정지로 며칠 건너뛴 비교에는 가격제한이 안 걸린다.
                #   5일 쉬고 40% 오른 것과 액면분할을 구분할 수 없다.
                #   구분이 안 되면 조정을 지어내지 않는다.
                n_gap += 1
                continue
            ratio = p_now / p_old
            if CA_LO <= ratio <= CA_HI:
                continue
            rec = {"date": d_, "code": code, "prev_close": round(p_old, 2),
                   "close": round(p_now, 2), "ratio": round(ratio, 4),
                   "qty": int(pos[code])}
            if CA_PLAUSIBLE_LO <= ratio <= CA_PLAUSIBLE_HI:
                adj[code] = adj.get(code, 1.0) / ratio
                rec["adj_after"] = round(adj[code], 6)
                ca_log.append(rec)
            else:
                # 분할·병합으로 설명 불가. 오염된 가격을 평가에 넣지 않는다.
                rec["action"] = "freeze"
                bad_log.append(rec)
                px = px.copy()
                px[code] = p_old

        # 2) 그날 체결 반영
        g = by_date.get(d_)
        if g is not None:
            for r in g.itertuples():
                if r.side == "BUY":
                    cash -= (r.gross + r.fee)
                    pos[r.code] = int(pos.get(r.code, 0)) + int(r.qty)
                else:
                    cash += (r.gross - r.fee)
                    left = int(pos.get(r.code, 0)) - int(r.qty)
                    if left > 0:
                        pos[r.code] = left
                    else:
                        pos.pop(r.code, None)

        # 3) 평가. 종가가 없는 종목(거래정지·상폐)은 마지막 값을 쓰고 센다
        mv = 0.0
        stale = 0
        for code, q in pos.items():
            p = float(px.get(code, np.nan))
            if not (p > 0):
                # 거래정지·상폐는 0원이 아니다. 마지막 종가로 평가하고 센다.
                p = float(prev_px.get(code, 0.0) or 0.0)
                if p > 0:
                    stale += 1
            mv += q * p * adj.get(code, 1.0)
        rows.append({"date": d_, "cash": cash, "mv": mv, "equity": cash + mv,
                     "n_pos": len(pos), "stale_px": stale})

        for code in pos:
            p = float(px.get(code, np.nan))
            if p > 0:
                prev_px[code] = p
                prev_day[code] = d_

    if n_gap:
        print("  거래공백이라 기업행위 판정을 보류한 건 %d" % n_gap)
    return pd.DataFrame(rows), ca_log, bad_log


def benchmark(panel: pd.DataFrame, codes: list[str], dates: list[str]) -> pd.Series:
    """개시일 동일가중 매수보유. 기업행위는 수익률에서 중립화한다."""
    w = panel[panel["code"].isin(codes) & panel["date"].isin(dates)]
    piv = w.pivot_table(index="date", columns="code", values="close", aggfunc="last")
    piv = piv.reindex(dates).ffill()
    ret = piv / piv.shift(1)
    ret = ret.where((ret <= CA_HI) & (ret >= CA_LO), 1.0)   # 기업행위 중립화
    ret.iloc[0] = 1.0
    cum = ret.cumprod()
    return cum.mean(axis=1)          # 동일가중


def main() -> int:
    ap = argparse.ArgumentParser(description="리밸런싱 실장부 성적")
    ap.add_argument("--state", default=str(DEFAULT_DIR / "rebal_state.json"))
    ap.add_argument("--ledger", default=str(DEFAULT_DIR / "rebal_trades.csv"))
    ap.add_argument("--capital", type=float, default=100_000_000)
    ap.add_argument("--min-value", type=float, default=1e9, help="벤치마크 유니버스 하한")
    ap.add_argument("--out", default=str(DEFAULT_DIR / "rebal_equity.csv"))
    ap.add_argument("--ca-out", default=str(DEFAULT_DIR / "rebal_corporate_actions.csv"))
    args = ap.parse_args()

    led = load_ledger(Path(args.ledger))
    if led.empty:
        print("[REBAL_REPORT] 원장이 비어 있다. 아직 체결이 없다.")
        print("  마감 후 rebalance_paper_fill.py --apply 를 돌리면 생긴다.")
        return 3

    start = led["exec_date"].min()
    panel = add_vol60(load_merged())
    dates = sorted(x for x in panel["date"].unique() if x >= start)
    if not dates:
        print("[STOP] 개시일 %s 이후 거래일이 패널에 없다" % start)
        return 2
    closes = {d_: g.drop_duplicates("code").set_index("code")["close"]
              for d_, g in panel[panel["date"].isin(dates)].groupby("date")}

    eq, ca, bad = replay(led, closes, dates, args.capital)

    # 벤치마크 유니버스 = 개시일 적격 종목(vol60 배제 전)
    s0 = panel[(panel["date"] == start) & (panel["value"] >= args.min_value)].dropna(subset=["vol60"])
    bench = benchmark(panel, s0["code"].tolist(), dates)
    eq["bench"] = bench.reindex(eq["date"]).to_numpy()
    eq["equity_idx"] = eq["equity"] / args.capital
    eq["excess_pp"] = (eq["equity_idx"] - eq["bench"]) * 100

    last = eq.iloc[-1]
    days = len(eq)
    print("[REBAL_REPORT] 개시 %s -> %s  (거래일 %d)" % (start, last["date"], days))
    print("  자산 %s원   현금 %s원 (%.1f%%)   보유 %d종목"
          % (format(int(last["equity"]), ","), format(int(last["cash"]), ","),
             100 * last["cash"] / max(last["equity"], 1), int(last["n_pos"])))
    print("  누적수익 %+.2f%%    벤치마크(동일가중 %d종목) %+.2f%%    초과 %+.2f%%p"
          % (100 * (last["equity_idx"] - 1), len(s0), 100 * (last["bench"] - 1), last["excess_pp"]))

    # 원장에 이미 들어온 체결가 검사. 과거는 못 고치니 보이게라도 만든다.
    _chk = led.merge(panel[["date", "code", "close"]].rename(columns={"date": "exec_date"}),
                     on=["exec_date", "code"], how="left")
    _chk["gap"] = (_chk["price"] / _chk["close"] - 1.0).abs()
    _odd = _chk[_chk["gap"] > 0.01]
    if len(_odd):
        print("  [주의] 체결가가 그날 종가와 다른 건 %d (원장 %d건 중)" % (len(_odd), len(led)))
    # 극단 체결가. 기준은 **체결일 이전 1년 중앙 종가**여야 한다.
    #   [2026-08-25] 처음엔 전체기간(11.4년) 중앙값을 썼더니 11년간 정상 성장한 종목이
    #   64건이나 잡혔다(086520 이 5,720 -> 147,100 은 오염이 아니라 주가 상승이다).
    #   기준선은 체결 시점 근처에서 뽑아야 한다. rebalance_paper_fill.py 의 가드와 같은 기준.
    _rows = []
    for _d, _g in led.groupby("exec_date"):
        _lo = (pd.Timestamp(_d) - pd.Timedelta(days=400)).strftime("%Y%m%d")
        _h = panel[(panel["date"] < _d) & (panel["date"] >= _lo)]
        _m = _h.groupby("code")["close"].median()
        _t = _g.copy()
        _t["med"] = _t["code"].map(_m)
        _rows.append(_t)
    _chk2 = pd.concat(_rows, ignore_index=True) if _rows else _chk
    _wild = _chk2[(_chk2["med"] > 0) & ((_chk2["price"] / _chk2["med"] > 10) |
                                        (_chk2["price"] / _chk2["med"] < 0.1))]
    if len(_wild):
        print("  [원장오염] 종목 중앙 종가의 10배 밖에서 체결된 건 %d" % len(_wild))
        for r in _wild.head(5).itertuples():
            print("    %s %s %s  체결 %s  /  중앙 %s  (x%.1f)"
                  % (r.exec_date, r.side, r.code, format(int(r.price), ","),
                     format(int(r.med), ","), r.price / r.med))
        print("    -> 이 건들이 만든 손익은 실재하지 않는다. 성적에서 빼고 읽을 것.")

    fees = float(led["fee"].sum())
    buy = float(led.loc[led["side"] == "BUY", "gross"].sum())
    sell = float(led.loc[led["side"] == "SELL", "gross"].sum())
    print("  누적 매수 %s원  매도 %s원  비용 %s원 (거래대금의 %.3f%%)"
          % (format(int(buy), ","), format(int(sell), ","), format(int(fees), ","),
             100 * fees / max(buy + sell, 1)))

    if days >= 2:
        r = eq["equity_idx"].pct_change().dropna()
        if len(r) >= 2 and r.std() > 0:
            print("  일변동성 %.2f%%   연환산 %.1f%%   최대낙폭 %.2f%%"
                  % (100 * r.std(), 100 * r.std() * np.sqrt(252),
                     100 * (eq["equity_idx"] / eq["equity_idx"].cummax() - 1).min()))
    else:
        print("  변동성·낙폭은 거래일 2일부터 나온다")

    if int(last["stale_px"]):
        print("  [주의] 종가가 없어 직전값으로 평가한 종목 %d개(거래정지·상폐 가능)"
              % int(last["stale_px"]))

    if ca:
        print("  [기업행위] %d건을 가격 배수로 흡수했다 (종가비가 +-30.5%% 밖)" % len(ca))
        for c in ca[-5:]:
            print("    %s %s  %s -> %s (x%.3f)  보유 %d주, 누적배수 %.4f"
                  % (c["date"], c["code"], format(int(c["prev_close"]), ","),
                     format(int(c["close"]), ","), c["ratio"], c["qty"], c["adj_after"]))
        pd.DataFrame(ca).to_csv(args.ca_out, index=False, encoding="utf-8-sig")
        print("    wrote %s" % args.ca_out)
    else:
        print("  기업행위 감지 0건")

    if bad:
        print("  [데이터결함] 분할·병합으로 설명 안 되는 종가 %d건 - 평가에서 제외(직전값 고정)" % len(bad))
        for c in bad[:5]:
            print("    %s %s  %s -> %s (x%.1f)"
                  % (c["date"], c["code"], format(int(c["prev_close"]), ","),
                     format(int(c["close"]), ","), c["ratio"]))
        pd.DataFrame(bad).to_csv(str(args.ca_out).replace(".csv", "_baddata.csv"),
                                 index=False, encoding="utf-8-sig")

    # history 대조 - 재생이 생산자와 어긋나면 둘 중 하나가 틀린 것이다
    sp = Path(args.state)
    if sp.exists():
        st = json.loads(sp.read_text(encoding="utf-8"))
        idx = eq.set_index("date")["equity"]
        bad = []
        for h in (st.get("history") or []):
            d_ = str(h.get("exec_date"))
            if d_ in idx.index:
                diff = abs(float(idx[d_]) - float(h.get("equity", 0)))
                if diff > max(1000.0, 0.0001 * float(h.get("equity", 1))):
                    bad.append((d_, float(h["equity"]), float(idx[d_]), diff))
        if bad:
            print("  [불일치] 원장 재생과 state.history 자산이 다르다:")
            for d_, a, b, df in bad:
                print("    %s  history %s / 재생 %s  차이 %s"
                      % (d_, format(int(a), ","), format(int(b), ","), format(int(df), ",")))
        else:
            print("  history 대조 일치 (%d개 체결일)" % len(st.get("history") or []))

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    eq.to_csv(out, index=False, encoding="utf-8-sig")
    print("  wrote %s (%d행)" % (out, len(eq)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
