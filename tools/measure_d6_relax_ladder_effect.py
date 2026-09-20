# -*- coding: utf-8 -*-
"""D6 수리(완화 사다리 rs_lim 방향) 효과 측정. **읽기 전용, 주문 없음.**

옛 사다리(`max(rs_lim * m, -0.10)`)와 새 사다리(`_relax_rs_lim`)를 같은 자료에
각각 적용해 **단계별로 몇 종목이 게이트를 통과하는지** 비교한다.

왜 필요한가: D6 은 살아 있는 매매 로직 변경이다. "부호가 맞아졌다" 만으로는
얼마나 열리는지 알 수 없다. 방향이 맞는 것과 크기가 타당한 것은 다른 질문이다.

[2026-09-10] 신설.
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

import generate_candidates_v41_1 as gen  # noqa: E402


def _old_ladder(p0: dict) -> list:
    """수정 전 사다리를 그대로 재현한다(비교 기준선)."""
    lad = []
    lad.append(("L0", dict(p0)))
    p1 = dict(p0)
    p1["atr_max"] = max(float(p1["atr_max"]), 0.10)
    p1["v_accel_lim"] = max(float(p1["v_accel_lim"]) * 0.90, 1.20)
    lad.append(("L1", p1))
    p2 = dict(p1)
    p2["atr_max"] = max(float(p2["atr_max"]), 0.12)
    p2["v_accel_lim"] = max(float(p2["v_accel_lim"]) * 0.90, 1.20)
    p2["rs_lim"] = max(float(p2["rs_lim"]) * 0.95, -0.10)      # 옛 계산
    lad.append(("L2", p2))
    p3 = dict(p2)
    p3["atr_max"] = max(float(p3["atr_max"]), 0.15)
    p3["v_accel_lim"] = max(float(p3["v_accel_lim"]) * 0.90, 1.20)
    p3["rs_lim"] = max(float(p3["rs_lim"]) * 0.95, -0.10)
    p3["stretch_max"] = min(float(p3["stretch_max"]) + 0.03, 1.30)
    p3["value_min"] = max(float(p3["value_min"]) * 0.85, 1_000_000_000.0)
    lad.append(("L3", p3))
    p4 = dict(p3)
    p4["atr_max"] = max(float(p4["atr_max"]), 0.18)
    p4["v_accel_lim"] = max(float(p4["v_accel_lim"]) * 0.90, 1.15)
    p4["rs_lim"] = max(float(p4["rs_lim"]) * 0.93, -0.10)
    p4["stretch_max"] = min(float(p4["stretch_max"]) + 0.03, 1.35)
    p4["value_min"] = max(float(p4["value_min"]) * 0.70, 1_000_000_000.0)
    lad.append(("L4", p4))
    p5 = dict(p4)
    p5["atr_max"] = max(float(p5["atr_max"]), 0.22)
    p5["v_accel_lim"] = max(float(p5["v_accel_lim"]) * 0.88, 1.10)
    p5["rs_lim"] = max(float(p5["rs_lim"]) * 0.92, -0.10)
    p5["stretch_max"] = min(float(p5["stretch_max"]) + 0.04, 1.40)
    p5["value_min"] = max(float(p5["value_min"]) * 0.55, 1_000_000_000.0)
    lad.append(("L5", p5))
    p6 = dict(p5)
    p6["atr_max"] = max(float(p6["atr_max"]), 0.25)
    p6["v_accel_lim"] = max(float(p6["v_accel_lim"]) * 0.85, 1.05)
    p6["rs_lim"] = max(float(p6["rs_lim"]) * 0.90, -0.10)
    p6["stretch_max"] = min(float(p6["stretch_max"]) + 0.05, 1.45)
    p6["value_min"] = max(float(p6["value_min"]) * 0.40, 1_000_000_000.0)
    lad.append(("L6", p6))
    return lad


def _pass_mask(d: pd.DataFrame, p: dict) -> pd.Series:
    m = (
        (d["rs"] > float(p["rs_lim"]))
        & (d["v_accel"] > float(p["v_accel_lim"]))
        & (d["stretch"] < float(p["stretch_max"]))
        & (d["value"] > float(p["value_min"]))
        & (d["atr14_pct"] < float(p["atr_max"]))
        & (d["rsi14"] < float(p["rsi_max"]))
        & (d["vol_close_corr20"] >= float(p["vol_close_corr_min"]))
        & (d["high_52w_gap"] <= float(p["near_52w_high_gap_max"]))
        & (d["listing_days"] >= float(p["min_listing_days"]))
    )
    if float(p.get("require_macd_golden", 0.0) or 0.0) >= 0.5:
        m = m & (d["macd_golden"] == True)  # noqa: E712
    return m.fillna(False).astype(bool)


def main() -> int:
    ap = argparse.ArgumentParser(description="D6 완화 사다리 수리 효과 (읽기 전용)")
    ap.add_argument("--lookback-days", type=int, default=120)
    ap.add_argument("--min-universe", type=int, default=2000)
    a = ap.parse_args()

    raw = gen._load_data()
    if raw.empty:
        print("[ERR] no raw data")
        return 2
    df, _latest, _ = gen._compute_factors(raw)
    if df.empty:
        print("[ERR] no factor data")
        return 2

    max_d = pd.to_datetime(df["date"].max())
    dx = df[df["date"] >= max_d - pd.Timedelta(days=int(a.lookback_days))].copy()
    uni = dx.groupby("date")["code"].nunique()
    dx = dx[dx["date"].isin(uni[uni >= int(a.min_universe)].index)].copy()
    if dx.empty:
        print("[ERR] empty after filter")
        return 3

    params = gen._normalize_params(gen.read_json(ROOT / "12_Risk_Controlled" / "stable_params_v41_1.json") or {})
    old = _old_ladder(params)
    new = [(n, p) for n, p in gen._relax_ladder(params) if n in {x[0] for x in old}]

    dates = sorted(dx["date"].unique())
    print("창 %s ~ %s (%d거래일, %d행)" % (
        pd.Timestamp(dates[0]).date(), pd.Timestamp(dates[-1]).date(), len(dates), len(dx)))
    print()
    print("%-4s %-10s %-10s %-9s %-9s %-9s %s" % (
        "단계", "rs_lim(옛)", "rs_lim(새)", "통과(옛)", "통과(새)", "증가", "0후보일(옛->새)"))

    rows = []
    for (n_o, p_o), (n_n, p_n) in zip(old, new):
        assert n_o == n_n
        mo = _pass_mask(dx, p_o)
        mn = _pass_mask(dx, p_n)
        by_o = mo.groupby(dx["date"]).sum()
        by_n = mn.groupby(dx["date"]).sum()
        zo = int((by_o == 0).sum())
        zn = int((by_n == 0).sum())
        print("%-4s %+10.5f %+10.5f %9d %9d %+9d   %d -> %d" % (
            n_o, float(p_o["rs_lim"]), float(p_n["rs_lim"]),
            int(mo.sum()), int(mn.sum()), int(mn.sum()) - int(mo.sum()), zo, zn))
        rows.append({
            "level": n_o, "rs_lim_old": float(p_o["rs_lim"]), "rs_lim_new": float(p_n["rs_lim"]),
            "pass_old": int(mo.sum()), "pass_new": int(mn.sum()),
            "zero_days_old": zo, "zero_days_new": zn, "total_days": len(dates),
        })

    print()
    print("=== rs 게이트 단독 (다른 게이트 무시) ===")
    for (n_o, p_o), (_n, p_n) in zip(old, new):
        ro = int((dx["rs"] > float(p_o["rs_lim"])).sum())
        rn = int((dx["rs"] > float(p_n["rs_lim"])).sum())
        print("  %-3s  %7d -> %7d  (%+.3f%%p 통과율)" % (
            n_o, ro, rn, (rn - ro) / max(1, len(dx)) * 100.0))

    # === 선택 단계 시뮬 ===
    # 생산은 **후보가 나오는 첫 단계에서 break** 한다(generate_candidates_v41_1.py:2190).
    # rs 가 느슨해지면 **더 이른 단계에서 멈추고**, 그 단계는 value_min 등이 더 빡빡하다.
    # 그래서 날에 따라 후보가 **줄 수도** 있다. 이것이 판단에 필요한 숫자다.
    print()
    print("=== 선택 단계 시뮬 (첫 통과 단계에서 멈춘다) ===")
    def _chosen(lad):
        per = {}
        for n, p in lad:
            per[n] = _pass_mask(dx, p).groupby(dx["date"]).sum()
        res = {}
        for d in dates:
            for n, _p in lad:
                c = int(per[n].get(d, 0))
                if c > 0:
                    res[d] = (n, c); break
            else:
                res[d] = ("NONE", 0)
        return res
    co, cn = _chosen(old), _chosen(new)
    import collections
    lvl_o = collections.Counter(v[0] for v in co.values())
    lvl_n = collections.Counter(v[0] for v in cn.values())
    order = [x[0] for x in old] + ["NONE"]
    print("  단계별 선택 일수:")
    for n in order:
        print("    %-5s  옛 %3d일  ->  새 %3d일" % (n, lvl_o.get(n, 0), lvl_n.get(n, 0)))
    so = sum(v[1] for v in co.values()); sn = sum(v[1] for v in cn.values())
    zo = sum(1 for v in co.values() if v[1] == 0); zn = sum(1 for v in cn.values() if v[1] == 0)
    print("  후보 총합   옛 %d -> 새 %d  (%+d)" % (so, sn, sn - so))
    print("  0후보일     옛 %d -> 새 %d  (%+d)" % (zo, zn, zn - zo))
    up = sum(1 for d in dates if cn[d][1] > co[d][1])
    dn = sum(1 for d in dates if cn[d][1] < co[d][1])
    sm = len(dates) - up - dn
    print("  날짜별      늘어난 날 %d / 줄어든 날 %d / 같은 날 %d" % (up, dn, sm))
    if dn:
        ex = [(str(pd.Timestamp(d).date()), co[d], cn[d]) for d in dates if cn[d][1] < co[d][1]][:6]
        print("  줄어든 예 (날짜, 옛(단계,수), 새(단계,수)):")
        for e in ex: print("    ", e)

    # 날짜별 원자료를 CSV 로 남긴다. 집계만 남기면 나중에 되짚을 수 없다.
    det = pd.DataFrame([{
        "date": str(pd.Timestamp(d).date()),
        "chosen_level_old": co[d][0], "cand_old": co[d][1],
        "chosen_level_new": cn[d][0], "cand_new": cn[d][1],
        "delta": cn[d][1] - co[d][1],
    } for d in dates])
    det_path = ROOT / "2_Logs" / "d6_relax_ladder_effect_by_date_latest.csv"
    det.to_csv(det_path, index=False, encoding="utf-8-sig")
    print("[OK] %s (%d행)" % (det_path.name, len(det)))

    out = ROOT / "2_Logs" / "d6_relax_ladder_effect_latest.json"
    out.write_text(json.dumps({
        "measured_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "window": {"start": str(pd.Timestamp(dates[0]).date()), "end": str(pd.Timestamp(dates[-1]).date()),
                   "days": len(dates), "rows": int(len(dx))},
        "stable_rs_lim": float(params["rs_lim"]),
        "levels": rows,
        "chosen_level_sim": {
            "note": "생산은 첫 통과 단계에서 break 한다. 게이트 마스크만 쓴 근사다(섹터 유니온 등 제외)",
            "days_by_level_old": {k: int(v) for k, v in lvl_o.items()},
            "days_by_level_new": {k: int(v) for k, v in lvl_n.items()},
            "cand_total_old": int(so), "cand_total_new": int(sn),
            "zero_days_old": int(zo), "zero_days_new": int(zn),
            "days_up": int(up), "days_down": int(dn), "days_same": int(sm),
        },
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print()
    print("[OK] %s" % out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
