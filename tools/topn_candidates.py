# -*- coding: utf-8 -*-
"""RD_20260901_topn 1단계 하네스 - 횡단면 상위 N 후보 생성

사전등록  docs/references/PREREG_RD_20260901_TOPN.md (이 파일은 그 설계의 구현이다)
목적      매일 살 종목이 나오게 한다. 수익률은 1단계 판정 기준이 아니다.

유니버스  집행 제약으로 정의한다. 알파 주장이 아니다.
  base_value(전일까지 20일 평균 거래대금) >= 20억   포지션 1,670만 / 참여율 1% x 안전계수 2
  종가 > 1,000원                                  호가단위 비중 과대 배제
  상장 126거래일 이상                               지표 계산 이력
  당일 가격제한 도달 제외                            지정가 체결 불가
  tradability_blocked 제외                        제도 (패널에 있을 때만)

ARM 3갈래
  ARM_RANDOM  무작위 N.  상설 대조군. 날짜 시드로 재현 가능
  ARM_LIQ     base_value 상위 N
  ARM_SCORE   z(rs)+z(v_accel)-z(stretch)-z(atr14_pct), 가중치 전부 1. 튜닝 금지

홀드아웃  2025-01-01 이후는 봉인. --unseal 없이는 산출하지 않는다
출력      2_Logs/topn/ 아래에만 쓴다. 기존 SSOT 미접촉
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
OUT_DIR = ROOT / "2_Logs" / "topn"

SEAL_START = pd.Timestamp("2025-01-01")
DESIGN_START = pd.Timestamp("2020-01-01")
VALUE_FLOOR = 2.0e9
PRICE_FLOOR = 1000.0
MIN_LISTING_DAYS = 126.0
LIMIT_PCT = 0.295
N_DEFAULT = 20
ARMS = ("ARM_RANDOM", "ARM_LIQ", "ARM_SCORE")


def _z(s):
    v = pd.to_numeric(s, errors="coerce").astype("float64")
    m = v.mean()
    sd = v.std(ddof=0)
    if not np.isfinite(sd) or sd == 0:
        return pd.Series(0.0, index=v.index)
    return (v.fillna(m) - m) / sd


def load_panel(start):
    import optimize_params_v41_1 as O

    df = O.load_data(O.BASE_DIR)
    df = df[df["date"] >= start].copy()
    df = O.compute_factors(df)
    keep = [
        "date", "code", "name", "market", "close", "value",
        "change_pct", "change_rate", "rs", "v_accel", "stretch",
        "atr14_pct", "listing_days", "tradability_blocked",
    ]
    df = df[[c for c in keep if c in df.columns]].copy()
    for c in df.columns:
        if str(df[c].dtype) == "float64":
            df[c] = df[c].astype("float32")
    return df.sort_values(["code", "date"])


def build_universe(df):
    df = df.copy()
    df["base_value"] = df.groupby("code")["value"].transform(
        lambda s: s.shift(1).rolling(20, min_periods=15).mean()
    )
    n0 = len(df)
    steps = []

    m = df["base_value"].notna() & (df["base_value"] >= VALUE_FLOOR)
    steps.append(("value_floor_20e", int(m.sum())))

    m = m & (pd.to_numeric(df["close"], errors="coerce") > PRICE_FLOOR)
    steps.append(("price_floor_1000", int(m.sum())))

    if "listing_days" in df.columns:
        m = m & (pd.to_numeric(df["listing_days"], errors="coerce").fillna(0.0) >= MIN_LISTING_DAYS)
        steps.append(("listing_days_126", int(m.sum())))

    # [2026-09-07] 상한/하한 배제가 **fail-open** 이었다. 실측:
    #   change_pct 결측 98.92% (전진 5거래일은 **100%**)
    #   NaN -> (chg >= 0.295) 가 False -> ~False = True -> 그대로 통과
    #   결과: 08-31~09-04 에 상하한 종목 4건이 후보로 뽑혔고 **4건 전부 ARM_SCORE** 였다
    #        (08-31 054940 1위 / 09-03 162300 2위 = 둘 다 진입 대상)
    #   점수식에 +z(v_accel) 이 있어 상한가가 구조적으로 상위에 온다. 우연이 아니다.
    #   이대로 두면 A4(체결률)/A5(슬리피지)가 떨어지고 B4 "점수 계산 결함" 으로 오귀인된다.
    # change_pct 는 옵티마이저도 생산 후보생성기도 만들지 않는 원자료 컬럼이라 채울 주체가 없다.
    # 그래서 close 에서 유도한다. 유도값 검증: 둘 다 있는 18,693행에서 99.65% 가 0.01%p 이내.
    # 사전등록 변경이 아니다 - 설계서가 [구현됨]이라 적은 규칙을 실제로 작동시키는 것이다.
    reported = None
    for c in ("change_pct", "change_rate"):
        if c in df.columns:
            v = pd.to_numeric(df[c], errors="coerce").abs()
            mx = float(v.max()) if np.isfinite(v.max()) else 0.0
            reported = (v / 100.0) if mx > 1.5 else v
            break

    close_num = pd.to_numeric(df["close"], errors="coerce")
    prev_close = close_num.groupby(df["code"]).shift(1)
    derived = (close_num / prev_close - 1.0).abs()

    if reported is None:
        chg = derived
    else:
        chg = reported.where(reported.notna(), derived)

    unknown = chg.isna()
    n_unknown = int((m & unknown).sum())
    if n_unknown:
        # 판정 불가를 통과시키지 않는다(fail-closed). 조용히 넘기면 지금 고친 결함이 되돌아온다.
        print("[UNIV][WARN] 등락 판정 불가 %d 행을 제외한다 (close 이력 부재)" % n_unknown, flush=True)
    m = m & ~(chg >= LIMIT_PCT) & ~unknown
    steps.append(("price_limit_excl", int(m.sum())))
    steps.append(("  ^ 유도 사용 행", int(reported.isna().sum()) if reported is not None else len(df)))

    if "tradability_blocked" in df.columns:
        blocked = df["tradability_blocked"].astype(str).str.strip().str.lower()
        m = m & ~blocked.isin(["1", "true", "yes", "y"])
        steps.append(("tradability_blocked_excl", int(m.sum())))

    # [2026-09-01] 상품구분 배제. market 컬럼은 52%가 UNKNOWN 이라 쓸 수 없어
    # KRX 종목코드 규약으로 거른다. 실측 영향 547 -> 534 (2.4%).
    #   영문 포함     신형우선주 (예: 0126Z0 이 ARM_SCORE 6위로 뽑혔다)
    #   끝자리 != 0   우선주(5) / 2우B(7) 등
    # 보통주 대표 종목(005930 / 000660)이 생존함을 확인했다.
    code_s = df["code"].astype(str)
    m = m & code_s.str.fullmatch(r"[0-9]{6}").fillna(False) & code_s.str.endswith("0")
    steps.append(("instrument_type_excl", int(m.sum())))

    out = df[m].copy()
    print("[UNIV] %d -> %d (%.2f%%)" % (n0, len(out), 100.0 * len(out) / max(1, n0)), flush=True)
    for name, cnt in steps:
        print("       %-26s %d" % (name, cnt), flush=True)
    return out


def pick(day, n, seed):
    d = day.reset_index(drop=True)
    k = min(n, len(d))
    res = {}
    res["ARM_LIQ"] = d.nlargest(k, "base_value")["code"].tolist()
    rng = np.random.default_rng(seed)
    idx = rng.permutation(len(d))[:k]
    res["ARM_RANDOM"] = d.loc[idx, "code"].tolist()
    score = _z(d["rs"]) + _z(d["v_accel"]) - _z(d["stretch"]) - _z(d["atr14_pct"])
    d = d.assign(_score=score)
    res["ARM_SCORE"] = d.nlargest(k, "_score")["code"].tolist()
    return res, d.set_index("code")["_score"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="", help="YYYYMMDD 단일일")
    ap.add_argument("--range", default="", help="YYYYMMDD:YYYYMMDD")
    ap.add_argument("--forward", action="store_true",
                    help="전진 운용. 패널의 최신 거래일 1일만 산출한다. 봉인과 무관하다")
    ap.add_argument("-n", "--top-n", type=int, default=N_DEFAULT)
    ap.add_argument("--unseal", action="store_true", help="봉인 구간 산출 허용. 원장에 기록된다")
    ap.add_argument("--unseal-reason", default="")
    ap.add_argument("--out-dir", default=str(OUT_DIR))
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.unseal and not args.unseal_reason.strip():
        print("[STOP] --unseal 은 --unseal-reason 을 요구한다", flush=True)
        return 2

    # 전진 운용은 봉인과 성격이 다르다.
    #   봉인   = 과거 2025-01 이후 구간을 **설계에 쓰는 것**을 막는다 (표본 외 보호)
    #   전진   = 오늘 이후를 실시간으로 산출한다. 결과를 보고 설계를 고치지 않는 한 오염이 아니다
    # 그래서 --forward 는 최신 거래일 **1일만** 낸다. 범위 조회는 허용하지 않는다.
    if args.forward:
        if args.range or args.date:
            print("[STOP] --forward 는 --date/--range 와 함께 쓸 수 없다", flush=True)
            return 2
        df_f = load_panel(SEAL_START - pd.Timedelta(days=400))
        uni_f = build_universe(df_f)
        latest = pd.Timestamp(max(pd.to_datetime(uni_f["date"]).unique()))
        print("[FWD] 전진 운용. 최신 거래일 %s 1일만 산출한다" % latest.date(), flush=True)
        return _emit(uni_f[uni_f["date"] == latest], [latest], latest, latest,
                     args.top_n, out_dir, unsealed=False, forward=True)

    if args.range:
        a, b = args.range.split(":")
        lo, hi = pd.Timestamp(a), pd.Timestamp(b)
    elif args.date:
        lo = hi = pd.Timestamp(args.date)
    else:
        lo, hi = DESIGN_START, SEAL_START - pd.Timedelta(days=1)

    if hi >= SEAL_START and not args.unseal:
        print("[SEAL] 요청 구간이 봉인 구간(%s 이후)을 포함한다. 잘라낸다." % SEAL_START.date(), flush=True)
        hi = SEAL_START - pd.Timedelta(days=1)
    if args.unseal:
        with (out_dir / "unseal_log.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(json.dumps({
                "ts": pd.Timestamp.now().isoformat(),
                "range": [str(lo.date()), str(hi.date())],
                "reason": args.unseal_reason,
            }, ensure_ascii=False) + "\n")
        print("[SEAL] 봉인 해제됨. unseal_log.jsonl 에 기록. 이 구간은 더 이상 표본 외가 아니다", flush=True)

    if lo > hi:
        print("[STOP] 유효 구간이 없다", flush=True)
        return 2

    df = load_panel(min(lo, DESIGN_START))
    uni = build_universe(df)
    uni = uni[(uni["date"] >= lo) & (uni["date"] <= hi)]
    dates = sorted(pd.to_datetime(uni["date"]).unique())
    print("[RUN] %s ~ %s  거래일 %d  N=%d" % (lo.date(), hi.date(), len(dates), args.top_n), flush=True)
    return _emit(uni, dates, lo, hi, args.top_n, out_dir,
                 unsealed=bool(args.unseal), forward=False)


def _emit(uni, dates, lo, hi, top_n, out_dir, unsealed, forward):
    rows = []
    daily = []
    for dt in dates:
        day = uni[uni["date"] == dt]
        seed = int(pd.Timestamp(dt).strftime("%Y%m%d"))
        picks, score_by_code = pick(day, top_n, seed)
        ymd = pd.Timestamp(dt).strftime("%Y-%m-%d")
        for arm in ARMS:
            for rank, code in enumerate(picks[arm], 1):
                rows.append({
                    "date": ymd, "arm": arm, "rank": rank, "code": code,
                    "score": float(score_by_code.get(code, np.nan)),
                })
        daily.append({"date": ymd, "universe": int(len(day)),
                      **{arm: len(picks[arm]) for arm in ARMS}})

    cand = pd.DataFrame(rows)
    dly = pd.DataFrame(daily)
    if forward:
        tag = hi.strftime("%Y%m%d")
        cand.to_csv(out_dir / ("candidates_%s.csv" % tag), index=False, encoding="utf-8-sig")
        # 전진 원장에 누적한다. 하루치가 한 번만 들어가도록 같은 날짜는 덮어쓴다
        led = out_dir / "forward_ledger.csv"
        if led.exists():
            old = pd.read_csv(led, dtype={"code": str}, encoding="utf-8-sig")
            old = old[old["date"] != cand["date"].iloc[0]]
            cand = pd.concat([old, cand], ignore_index=True)
        cand.to_csv(led, index=False, encoding="utf-8-sig")
        dly.to_csv(out_dir / ("daily_counts_%s.csv" % tag), index=False, encoding="utf-8-sig")
    else:
        tag = "%s_%s" % (lo.strftime("%Y%m%d"), hi.strftime("%Y%m%d"))
        cand.to_csv(out_dir / ("candidates_%s.csv" % tag), index=False, encoding="utf-8-sig")
        dly.to_csv(out_dir / ("daily_counts_%s.csv" % tag), index=False, encoding="utf-8-sig")

    n = top_n
    total = len(dly)
    a1 = {arm: int((dly[arm] == n).sum()) for arm in ARMS}
    status = {
        "round": "RD_20260901_topn",
        "stage": 1,
        "generated_at": pd.Timestamp.now().isoformat(),
        "range": [str(lo.date()), str(hi.date())],
        "trading_days": total,
        "N": n,
        "universe_median": float(dly["universe"].median()) if total else None,
        "universe_min": int(dly["universe"].min()) if total else None,
        "A1_days_with_exactly_N": a1,
        "A1_pass": bool(total > 0 and all(v == total for v in a1.values())),
        "A2_relax_ladder_calls": 0,
        "A2_pass": True,
        "A2_note": "이 하네스는 완화 사다리를 호출하지 않는다 (코드에 경로가 없다)",
        "A3_A4_A5": "전진 운용 필요. 이 실행에서는 판정하지 않는다",
        "sealed_from": str(SEAL_START.date()),
        "unsealed": bool(unsealed),
        "mode": "forward" if forward else "design",
    }
    name = "stage1_status_forward.json" if forward else "stage1_status.json"
    (out_dir / name).write_text(
        json.dumps(status, ensure_ascii=False, indent=1), encoding="utf-8")

    print("", flush=True)
    print("[A1] 거래일 %d / N=%d" % (total, n), flush=True)
    for arm in ARMS:
        print("     %-11s 정확히 N개인 날 %d / %d  %s" % (
            arm, a1[arm], total, "PASS" if a1[arm] == total else "**FAIL**"), flush=True)
    print("[A2] 완화 사다리 호출 0건 (경로 없음) PASS", flush=True)
    if total:
        print("[UNIV] 일별 중앙 %.0f / 최소 %d" % (
            status["universe_median"], status["universe_min"]), flush=True)
    print("", flush=True)
    print("출력 %s" % out_dir, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
