"""D6·D7 을 **생산 코드와 무관하게** 원자료에서 다시 계산해 대조 기록을 만든다 (2026-09-21 신설).

왜: 09-28 가부 기준 2번은 "D6·D7 이 고정 입력에서 손계산과 일치한다" 인데,
지금까지 그 대조는 **사람이 '맞다' 고 말한 것**뿐이라 기계가 읽을 증거가 없었다.
그래서 가부 판정기가 이 기준을 UNKNOWN 으로 둔다 — 그리고 UNKNOWN 은 통과가 아니다.

**순환을 피하는 것이 이 파일의 전부다.**
`src/targets.py` 를 import 하지 않는다. `spec/D7_target_portfolio.md` 의 글만 보고 다시 짠다.
생산 코드로 기대값을 만들면 자기 자신과 비교하는 것이라 언제나 통과한다.

쓰는 것: 원자료 `input_<선정일>.csv` 하나. 읽는 것: 명세 문서.
결과: `handcalc_<선정일>.json` (기대값 + 산식 + 불일치 목록).
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import sys
from pathlib import Path

V2 = Path(__file__).resolve().parents[1]

CAP = 0.20                  # 종목 비중 상한 (명세 1. 상한 C6)
COST_RESERVE = 0.005        # 비용 준비금 0.5% (명세 2. 금액)
N_NAMES = 50                # 시총 상위 50 (WHAT_IT_DOES 조항 3)


# ---------------------------------------------------------------- D2 -> D6
def eligible(rows):
    """KOSPI 보통주만 + 제도 배제(거래정지·정리매매·관리종목). WHAT_IT_DOES 조항 1·2."""
    out = []
    for r in rows:
        if r.get("market") != "KOSPI":
            continue
        if r.get("share_type") != "보통주" or r.get("security_group") != "주권":
            continue
        if any(str(r.get(k, "")).strip().upper() == "O" for k in ("trading_halt", "liquidation", "administrative")):
            continue
        if str(r.get("mcap_check", "")).strip() != "OK":
            continue
        try:
            r["_mcap"] = int(r["market_cap"])
            r["_close"] = int(r["close"])
        except (ValueError, KeyError, TypeError):
            continue
        if r["_mcap"] <= 0 or r["_close"] <= 0:
            continue
        out.append(r)
    return out


def select_top(rows, n=N_NAMES):
    """시총 내림차순 상위 n. 동률은 코드 오름차순(명세의 타이브레이크 관례)."""
    return sorted(rows, key=lambda r: (-r["_mcap"], r["code"]))[:n]


# ---------------------------------------------------------------- D7 C6
def cap_weights(mcaps, cap=CAP, tol=1e-12, max_rounds=100):
    """20% 초과분을 나머지에 **시총 비율로** 재배분, 새로 넘으면 반복 (명세 1)."""
    total = float(sum(mcaps))
    w = [m / total for m in mcaps]
    capped = [False] * len(w)
    rounds = 0
    while rounds < max_rounds:
        over = [i for i, x in enumerate(w) if not capped[i] and x > cap + tol]
        if not over:
            break
        rounds += 1
        for i in over:
            capped[i] = True
            w[i] = cap
        free = 1.0 - sum(w[i] for i in range(len(w)) if capped[i])
        rest = [i for i in range(len(w)) if not capped[i]]
        rest_mcap = float(sum(mcaps[i] for i in rest))
        for i in rest:
            w[i] = free * (mcaps[i] / rest_mcap)
    return w, rounds, sum(capped)


# ---------------------------------------------------------------- D7 C7
def integerize(weights, prices, allocable, exposure, cap=CAP):
    """내림에서 시작해, 1주 추가가 오차를 줄이고 예산·종목상한을 지키는 것 중
    개선액 큰 순(같으면 시총 큰 순 -> 여기서는 입력 순서, 그다음 코드 오름차순)으로 1주씩 (명세 3)."""
    budget = allocable * exposure
    tv = [allocable * w * exposure for w in weights]
    qty = [int(math.floor(t / p + 1e-9)) for t, p in zip(tv, prices)]   # 명세의 +1e-9 수리
    floor_qty = list(qty)
    spent = sum(q * p for q, p in zip(qty, prices))

    while True:
        best = None
        for i, p in enumerate(prices):
            if spent + p > budget + 1e-9:
                continue
            new_val = (qty[i] + 1) * p
            if new_val > budget * cap + 1e-9:            # 종목 상한
                continue
            cur_err = abs(tv[i] - qty[i] * p)
            new_err = abs(tv[i] - new_val)
            gain = cur_err - new_err
            if gain <= 1e-9:
                continue
            key = (-gain, i)
            if best is None or key < best[0]:
                best = (key, i)
        if best is None:
            break
        i = best[1]
        qty[i] += 1
        spent += prices[i]
    return floor_qty, qty, spent


# ---------------------------------------------------------------- 대조
def main(argv=None):
    ap = argparse.ArgumentParser(description="D6·D7 독립 재계산 + 생산 산출물 대조")
    ap.add_argument("--run-dir", required=True, help="plan_runs/<이름>")
    ap.add_argument("--selection-date", required=True)
    ap.add_argument("--capital", type=float, default=60_000_000.0)
    ap.add_argument("--exposure", type=float, default=1.0)
    ap.add_argument("--write", action="store_true", help="handcalc_<선정일>.json 을 쓴다")
    a = ap.parse_args(argv)

    run = Path(a.run_dir)
    d = a.selection_date
    rows = list(csv.DictReader((run / f"input_{d}.csv").open(encoding="utf-8-sig")))
    sel = select_top(eligible(rows))
    mcaps = [r["_mcap"] for r in sel]
    prices = [float(r["_close"]) for r in sel]

    w, rounds, n_capped = cap_weights(mcaps)
    allocable = a.capital * (1.0 - COST_RESERVE)
    floor_q, qty, spent = integerize(w, prices, allocable, a.exposure)

    mine = {r["code"]: {"rank": i + 1, "basket_weight": w[i], "target_qty": qty[i], "floor_qty": floor_q[i]}
            for i, r in enumerate(sel)}
    expected = {
        "n_names": len(sel),
        "cap_rounds": rounds,
        "capped_names": n_capped,
        "basket_weight_sum": round(sum(w), 12),
        "allocable": allocable,
        "invested": round(spent, 2),
        "floor_qty_total": sum(floor_q),
        "added_qty_total": sum(qty) - sum(floor_q),
        "top1_code": sel[0]["code"],
        "top1_weight": w[0],
    }

    # 생산 산출물과 대조
    prod_rows = list(csv.DictReader((run / f"target_portfolio_{d}.csv").open(encoding="utf-8-sig")))
    prod = {r["code"]: r for r in prod_rows}
    diffs = []
    if set(prod) != set(mine):
        diffs.append({"what": "종목 집합", "only_prod": sorted(set(prod) - set(mine)),
                      "only_hand": sorted(set(mine) - set(prod))})
    for code, m in mine.items():
        p = prod.get(code)
        if not p:
            continue
        if abs(float(p["basket_weight"]) - m["basket_weight"]) > 1e-9:
            diffs.append({"what": "basket_weight", "code": code,
                          "prod": float(p["basket_weight"]), "hand": m["basket_weight"]})
        if int(p["target_qty"]) != m["target_qty"]:
            diffs.append({"what": "target_qty", "code": code,
                          "prod": int(p["target_qty"]), "hand": m["target_qty"]})

    summ = json.loads((run / f"target_{d}_summary.json").read_text(encoding="utf-8"))["d7"]
    for k in ("cap_rounds", "capped_names", "floor_qty_total", "added_qty_total"):
        if summ.get(k) != expected[k]:
            diffs.append({"what": f"summary.{k}", "prod": summ.get(k), "hand": expected[k]})
    if abs(float(summ.get("invested", 0)) - expected["invested"]) > 1.0:
        diffs.append({"what": "summary.invested", "prod": summ.get("invested"), "hand": expected["invested"]})

    rec = {
        "selection_date": d,
        "generated_at": dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "method": "spec/D7_target_portfolio.md 의 글만 보고 원자료 input_<날짜>.csv 에서 독립 재계산. "
                  "src/targets.py 를 import 하지 않는다(순환 방지).",
        "inputs": {"capital": a.capital, "exposure": a.exposure,
                   "cost_reserve": COST_RESERVE, "cap": CAP, "n_names": N_NAMES},
        "expected": expected,
        "mismatches": diffs,
        "verdict": "MATCH" if not diffs else "MISMATCH",
    }
    print(json.dumps({k: rec[k] for k in ("selection_date", "verdict", "expected")}, ensure_ascii=False, indent=2))
    if diffs:
        print("\n불일치 %d건:" % len(diffs))
        for x in diffs[:20]:
            print("  ", json.dumps(x, ensure_ascii=False))
    if a.write:
        out = run / f"handcalc_{d}.json"
        out.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
        print("\n기록:", out)
    return 0 if not diffs else 1


if __name__ == "__main__":
    sys.exit(main())
