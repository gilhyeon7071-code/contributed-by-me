# H009 측정 — 등록 2026-09-19 15:05:56. 정의는 등록 파일 그대로.
import sys, json, numpy as np, pandas as pd
sys.path.insert(0, r"E:\1_Data\tools")
from load_merged_panel import load_merged
OUT = sys.argv[1]
d = load_merged(["date", "code", "close", "value"])
d = d.sort_values(["code", "date"]).reset_index(drop=True)
for c in ("close", "value"): d[c] = pd.to_numeric(d[c], errors="coerce").astype(float)
g = d.groupby("code", sort=False)
d["prev"] = g["close"].shift(1)
d["ret"] = d["close"] / d["prev"] - 1
d = d[d["ret"].abs() <= 0.305].copy()           # 기업행위 제거 (행 단위)
g = d.groupby("code", sort=False)
d["vol"] = d["value"] / d["close"]
d["vavg20"] = g["vol"].transform(lambda s: s.shift(1).rolling(20, min_periods=20).mean())
d["rv"] = d["vol"] / d["vavg20"]
for h in (1, 3, 5):
    d[f"f{h}"] = g["close"].shift(-h) / d["close"] - 1
    # 다음 h 거래일 안에 |일간|>30.5% 행이 빠져 있으면 결측 처리됨(행 제거로 shift 가 건너뛸 수 있어 날짜 간격 확인)
    d[f"dh{h}"] = g["date"].shift(-h)
elig = d["value"].ge(1e9) & d["prev"].gt(0) & d["f1"].notna()
surge = elig & d["ret"].ge(0.05) & d["rv"].ge(2.0)
elig = elig.fillna(False).astype(bool); surge = surge.fillna(False).astype(bool)
limit = surge & d["ret"].ge(0.22).fillna(False).astype(bool)
d["grp"] = np.where(surge, np.where(limit, "limit", "surge"), np.where(elig, "univ", "x"))
x = d[d["grp"] != "x"]
res = {}
cal = sorted(d["date"].unique())
nxt = {a: b for a, b in zip(cal[:-1], cal[1:])}
x = x[x["dh1"] == x["date"].map(nxt)]          # 다음 행이 실제 다음 거래일인 것만
for h in (1, 3, 5):
    col = f"f{h}"
    b = x.groupby(["date", "grp"])[col].mean().unstack()
    for kind in ("surge", "limit"):
        if kind not in b: continue
        e = (b[kind] - b["univ"]).dropna()
        yrs = e.groupby(e.index.str[:4]).mean()
        def seg(s):
            n = len(s); m = s.mean(); sd = s.std()
            return {"days": n, "mean_excess_pct": m * 100, "sd_pct": sd * 100, "t": m / (sd / np.sqrt(n)) if n > 1 else None}
        res[f"{kind}_h{h}"] = {"all": seg(e), "explore_2015_2021": seg(e[e.index < "20220101"]),
                               "validate_2022_2023": seg(e[(e.index >= "20220101") & (e.index < "20240101")]),
                               "oos_2024_": seg(e[e.index >= "20240101"]),
                               "years_pos": int((yrs > 0).sum()), "years": int(len(yrs)),
                               "by_year_pct": (yrs * 100).round(3).to_dict(),
                               "cand_mean_raw_pct": float(b[kind].dropna().mean() * 100)}
# 선택편향: 신호일 vs 비신호일 유니버스 h1 수익
bu = x[x.grp == "univ"].groupby("date")["f1"].mean()
sd_days = set(x[x.grp == "surge"]["date"])
res["selection_bias_univ_h1"] = {"signal_days_mean_pct": float(bu[bu.index.isin(sd_days)].mean() * 100),
                                 "nonsignal_days_mean_pct": float(bu[~bu.index.isin(sd_days)].mean() * 100),
                                 "signal_days": int(bu.index.isin(sd_days).sum()), "all_days": int(len(bu))}
res["counts"] = {"surge_rows": int((x.grp == "surge").sum()), "limit_rows": int((x.grp == "limit").sum()),
                 "panel_first": str(d.date.min()), "panel_last": str(d.date.max())}
res["surge_per_day_median"] = float(x[x.grp == "surge"].groupby("date").size().median())
json.dump(res, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1, default=float)
print(json.dumps({k: v for k, v in res.items() if k in ("counts", "selection_bias_univ_h1", "surge_per_day_median")}, ensure_ascii=False, indent=1, default=float))
for k in ("surge_h1", "limit_h1", "surge_h3", "surge_h5"):
    if k in res:
        r = res[k]; print(k, {s: {kk: round(vv, 3) if isinstance(vv, float) else vv for kk, vv in r[s].items()} for s in ("all", "explore_2015_2021", "validate_2022_2023", "oos_2024_")}, "years+", r["years_pos"], "/", r["years"], "raw", round(r["cand_mean_raw_pct"], 3))
print("by_year surge_h1", res["surge_h1"]["by_year_pct"])
