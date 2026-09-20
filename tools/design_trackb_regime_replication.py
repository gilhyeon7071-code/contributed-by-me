# -*- coding: utf-8 -*-
"""Track B 레짐 탐지기 재측정 — KOSPI 재현 검증 + KOSDAQ 전이 시험.

왜 이 파일이 있나
    PLANS (138) 이 "이 프로젝트에서 지수를 이긴 첫 숫자" 를 냈고, 그것이 Track B 를
    다음 재료로 지목한 유일한 근거였다. 그런데 **(138) 은 코드도 산출물도 남기지 않았다.**
    "코스닥 재현 실패" 도 요약 두 줄뿐이고 수치가 없다.
    2026-08-30 재측정 결과 (138) 의 전략 수치는 재현되지 않는다. 상세 PLANS (144).
    이 파일은 그 재측정을 재현 가능하게 남기기 위한 것이다. 결과는 남기고 판단은 문서에 있다.

규칙 (PLANS 138 기술 그대로)
    월초 판정. idx_mom60(지수 60거래일 수익률), breadth_ma60(종목 중 MA60 위 비율)이
    각각 **확장 분위수**(그 시점까지의 과거만)를 넘으면 ON. ON 이면 다음 달 지수 보유,
    아니면 현금. burn-in 18개월(데이터 시작점 기준), 평가창 2016-12~2026-07 (116개월).

주의
    burn-in 은 데이터 시작점부터이고 평가창은 그 뒤다. 평가창 안에서 18개월을 버리면
    n=98 이 되어 기록(116)과 어긋난다 - 첫 시도에서 실제로 그렇게 틀렸다.

산출물
    2_Logs/design/trackb_kospi_kosdaq.csv
"""

import sys, numpy as np, pandas as pd
sys.path.insert(0, r"E:\1_Data\tools")
from load_merged_panel import load_merged

BURN = 18
START, END = "201612", "202607"

print("[1] 지수")
ix = pd.read_csv(r"E:\1_Data\2_Logs\index_daily_history.csv", dtype=str)
ix["close"] = pd.to_numeric(ix["close"], errors="coerce")
ixw = ix.pivot(index="date", columns="index_code", values="close").sort_index()
ixw.index = pd.to_datetime(ixw.index, format="%Y%m%d")

print("[2] 종목 패널 + market 지도")
p = load_merged(cols=["date", "code", "close", "value"])
mm = {}
for path in [r"E:\1_Data\krx_daily_archive\krx_daily_20150102_20191230_backfill_clean.parquet",
             r"E:\1_Data\krx_daily_archive\krx_daily_20200102_20220930_kosdaq_clean.parquet",
             r"E:\1_Data\Raw\krx_daily_20221001_20251224.parquet"]:
    d = pd.read_parquet(path, columns=["code", "market"]).dropna()
    d["code"] = d["code"].astype(str).str.zfill(6)
    for c, m in d.drop_duplicates("code")[["code", "market"]].itertuples(index=False):
        mm[c] = str(m).upper().strip()
close = p.pivot(index="date", columns="code", values="close").sort_index()
close.index = pd.to_datetime(close.index, format="%Y%m%d")
above = close > close.rolling(60, min_periods=40).mean()
valid = close.rolling(60, min_periods=40).mean().notna()
isk = pd.Series({c: ("KOSDAQ" in mm.get(c, "")) for c in close.columns})
kq, kp = isk[isk].index, isk[~isk].index
print("    KOSDAQ %d / KOSPI %d / 전체 %d" % (len(kq), len(kp), close.shape[1]))

def breadth(cols):
    a = above[cols].sum(axis=1); v = valid[cols].sum(axis=1)
    return (a / v.replace(0, np.nan))

BR = {"ALL": breadth(close.columns), "KOSPI": breadth(kp), "KOSDAQ": breadth(kq)}

# 월말 스냅샷
def monthly(s):
    return s.groupby(s.index.to_period("M")).last()

res_rows, blocks = [], {}
for mkt, code, br_key in (("KOSPI", "0001", "KOSPI"), ("KOSDAQ", "1001", "KOSDAQ")):
    idx = ixw[code].dropna()
    mom60 = idx / idx.shift(60) - 1
    m_idx = monthly(idx)
    m_mom = monthly(mom60)
    for br_lbl in ("ALL", br_key):
        m_br = monthly(BR[br_lbl].reindex(idx.index).ffill())
        df = pd.DataFrame({"idx": m_idx, "mom": m_mom, "br": m_br}).dropna()
        df["fwd"] = df["idx"].pct_change().shift(-1)          # 다음 달 수익
        df = df.dropna()
        # [2026-08-30] burn-in 은 **데이터 시작점부터** 이고 평가창은 그 뒤다.
        #   처음엔 평가창을 먼저 자르고 그 안에서 18개월을 버려 n=98 이 됐다(기록은 116).
        #   확장 분위수는 가진 이력 전체를 쓰고, 평가만 창으로 제한한다.
        win = (df.index >= pd.Period(START, "M")) & (df.index <= pd.Period(END, "M"))
        burn_ok = pd.Series(range(len(df)), index=df.index) >= BURN
        keep = win & burn_ok
        for q in (0.30, 0.50):
            on_mom = df["mom"] > df["mom"].expanding().quantile(q)
            on_br = df["br"] > df["br"].expanding().quantile(q)
            for cond, on in (("MOM", on_mom), ("BR", on_br),
                             ("AND", on_mom & on_br), ("OR", on_mom | on_br)):
                d = df[keep].copy()
                o = on[keep]
                r = d["fwd"].where(o, 0.0)
                bh = d["fwd"]
                yrs = len(d) / 12.0
                def st(x):
                    cum = float((1 + x).prod()); eq = (1 + x).cumprod()
                    return (cum ** (1 / yrs) - 1, float((eq / eq.cummax() - 1).min()),
                            float(x.mean() / x.std() * np.sqrt(12)) if x.std() > 0 else np.nan)
                c1, m1, s1 = st(r); c0, m0, s0 = st(bh)
                on_m, off_m = d["fwd"][o], d["fwd"][~o]
                if len(on_m) > 3 and len(off_m) > 3:
                    sp = on_m.mean() - off_m.mean()
                    se = np.sqrt(on_m.var(ddof=1) / len(on_m) + off_m.var(ddof=1) / len(off_m))
                    t = sp / se if se > 0 else np.nan
                else:
                    sp = t = np.nan
                res_rows.append(dict(시장=mkt, breadth=br_lbl, 조건=cond, q=q, n=len(d),
                                     ON률=100 * o.mean(), CAGR=100 * c1, MDD=100 * m1, 샤프=s1,
                                     보유CAGR=100 * c0, 보유MDD=100 * m0, 보유샤프=s0,
                                     ON평균=100 * on_m.mean() if len(on_m) else np.nan,
                                     OFF평균=100 * off_m.mean() if len(off_m) else np.nan,
                                     차이pp=100 * sp, t=t))

o = pd.DataFrame(res_rows)
o.to_csv(r"E:\1_Data\2_Logs\design\trackb_kospi_kosdaq.csv", index=False)
pd.set_option("display.width", 260)
f = lambda x: "%.2f" % x
print("\n=== KOSPI 재현 (PLANS 138 대조: BR q=0.30 -> CAGR 15.74 / MDD -30.1 / 샤프 0.81, 보유 12.62/-34.4/0.59) ===")
print(o[(o.시장 == "KOSPI") & (o.breadth == "ALL")][["조건","q","n","ON률","CAGR","MDD","샤프","보유CAGR","보유MDD","보유샤프","차이pp","t"]].to_string(index=False, float_format=f))
