#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Technical indicator diagnostic for STOC v41.1 factors.

Outputs (2_Logs):
- indicator_diag_summary[_<tag>]_YYYYMMDD_HHMMSS.json (+ latest pointer)
- indicator_diag_continuous[_<tag>]_YYYYMMDD_HHMMSS.csv (+ latest pointer)
- indicator_diag_binary[_<tag>]_YYYYMMDD_HHMMSS.csv (+ latest pointer)
- indicator_diag_daily_combo[_<tag>]_YYYYMMDD_HHMMSS.csv (+ latest pointer)
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import sys
from typing import Dict, List

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
RISK_DIR = ROOT / "12_Risk_Controlled"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import generate_candidates_v41_1 as gen  # noqa: E402
from utils.price_history_contract import add_exact_session_forward_returns  # noqa: E402
import logging




logger = logging.getLogger(__name__)

def _log_print(*args, **kwargs):
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")
    sep = kwargs.get("sep", " ")
    try:
        msg = sep.join(str(a) for a in args)
    except Exception:
        msg = " ".join(str(a) for a in args)
    logger.info(msg)
def _ts_now() -> str:
    return pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")


def _safe_qcut_ranked(s: pd.Series, q: int = 5) -> pd.Series:
    r = s.rank(method="first")
    return pd.qcut(r, q=q, labels=False) + 1


def _build_pass_columns(df: pd.DataFrame, p: Dict[str, float]) -> pd.DataFrame:
    out = df.copy()
    out["rs_pass"] = out["rs"] > float(p["rs_lim"])
    out["v_accel_pass"] = out["v_accel"] > float(p["v_accel_lim"])
    out["stretch_pass"] = out["stretch"] < float(p["stretch_max"])
    out["value_pass"] = out["value"] > float(p["value_min"])
    out["atr_pass"] = out["atr14_pct"] < float(p["atr_max"])
    out["rsi_pass"] = out["rsi14"] < float(p["rsi_max"])
    out["volcorr_pass"] = out["vol_close_corr20"] >= float(p["vol_close_corr_min"])
    out["high52_pass"] = out["high_52w_gap"] <= float(p["near_52w_high_gap_max"])
    out["listing_pass"] = out["listing_days"] >= float(p["min_listing_days"])

    require_macd = float(p.get("require_macd_golden", 0.0) or 0.0) >= 0.5
    if require_macd:
        out["all_pass"] = (
            out["rs_pass"]
            & out["v_accel_pass"]
            & out["stretch_pass"]
            & out["value_pass"]
            & out["atr_pass"]
            & out["rsi_pass"]
            & out["volcorr_pass"]
            & out["high52_pass"]
            & out["listing_pass"]
            & (out["macd_golden"] == True)
        )
    else:
        out["all_pass"] = (
            out["rs_pass"]
            & out["v_accel_pass"]
            & out["stretch_pass"]
            & out["value_pass"]
            & out["atr_pass"]
            & out["rsi_pass"]
            & out["volcorr_pass"]
            & out["high52_pass"]
            & out["listing_pass"]
        )

    # [2026-09-10] nullable(masked) dtype 방어.
    # 원천 지표가 Float64/Int64 로 오면 비교 결과가 `boolean` + pd.NA 가 되고
    # `&` 로 전파돼 소비자의 astype(bool) 이 ValueError 로 죽는다(2026-07-21 이후 7주간 무산출).
    # 일반 float64 였다면 `nan > x` 가 False 라 결측=불합격으로 이미 동작한다.
    # 여기서 NA->False 는 의미를 바꾸는 게 아니라 어긋난 컬럼을 그 규약에 맞추는 것이다.
    for _c in [c for c in out.columns if c.endswith("_pass")] + ["all_pass"]:
        out[_c] = out[_c].fillna(False).astype(bool)
    return out


def _build_pass_columns_effective(df: pd.DataFrame, params: Dict[str, float],
                                  mode: str = "auto") -> tuple:
    """**생산이 실제로 쓰는 완화 단계**에서 게이트 통과 여부를 만든다. (D4 수리)

    [2026-09-10] 종전에는 stable_params 를 그대로 써서 **L0 게이트만** 쟀다.
      그런데 생산은 완화 사다리를 타고, 남은 16일 실측 기준 **중앙값이 L6** 이며
      **L0 은 한 번도 안 쓰였다**(L3 x1 / L4 x2 / L5 x2 / L6 x7 / L7 x2 / NONE x1).
      즉 매일 내던 "게이트별 통과/탈락" 표는 **한 번도 생산 게이트를 설명한 적이 없다.**
      이 출력이 indicator_diag_and_recommend 의 파라미터 권고로 이어진다 = 측정 오염.
      BROKEN_WINDOW_REGISTER **D4**.

    mode:
      "auto"   날짜마다 **첫 통과 단계**를 고른다. 생산과 같은 규칙이다
               (generate_candidates_v41_1.py:2186 의 `if not cand.empty: break`)
      "stable" 종전 동작. stable_params 그대로(= L0). 비교용으로 남긴다
      "L0".."L9"  특정 단계로 고정

    **한계 - 근사다.** 생산의 `_select_candidates` 는 이 9개 게이트 말고도
    섹터 유니온·junk_risk·감시종목 등을 더 본다. 여기서는 **게이트만** 재현한다.
    그래서 고른 단계가 생산과 다를 수 있다. 그래도 L0 고정보다는 훨씬 가깝다.
    """
    import generate_candidates_v41_1 as _gen

    if mode == "stable":
        out = _build_pass_columns(df, params)
        out["gate_level"] = "STABLE"
        return out, {"mode": "stable", "levels_used": {"STABLE": int(df["date"].nunique())}}

    ladder = _gen._relax_ladder(dict(params))
    if mode != "auto":
        want = str(mode).upper()
        sel = [(n, q) for n, q in ladder if str(n).upper() == want]
        if not sel:
            raise ValueError("알 수 없는 단계: %s" % mode)
        out = _build_pass_columns(df, sel[0][1])
        out["gate_level"] = want
        return out, {"mode": want, "levels_used": {want: int(df["date"].nunique())}}

    # auto: 단계별 **마스크만** 만든다.
    #   _build_pass_columns 는 df.copy() 를 하므로 10단계면 프레임이 10벌이 된다.
    #   게이트 입력 컬럼만 써서 불리언 배열만 남긴다(메모리 ~10분의 1).
    GATE_SRC = ["rs", "v_accel", "stretch", "value", "atr14_pct", "rsi14",
                "vol_close_corr20", "high_52w_gap", "listing_days", "macd_golden"]
    src = df[[c for c in GATE_SRC if c in df.columns]]
    per_level = []
    for name, q in ladder:
        blk = _build_pass_columns(src, q)
        pcols = [c for c in blk.columns if c.endswith("_pass")] + ["all_pass"]
        per_level.append((name, q, {c: blk[c].to_numpy(dtype=bool) for c in pcols}))
        del blk

    dates = df["date"].to_numpy()
    uniq = pd.unique(dates)
    date_idx = {d: (dates == d) for d in uniq}
    chosen_name = {}
    for d in uniq:
        m = date_idx[d]
        for name, _q, masks in per_level:
            if bool(masks["all_pass"][m].any()):
                chosen_name[d] = name
                break
        else:
            chosen_name[d] = ladder[-1][0]      # 어느 단계에서도 0 -> 가장 느슨한 단계로 관측

    lvl_arr = np.empty(len(df), dtype=object)
    for d in uniq:
        lvl_arr[date_idx[d]] = chosen_name[d]

    pass_cols = list(per_level[0][2].keys())
    out = df.copy()
    for c in pass_cols:
        out[c] = False
    for name, _q, masks in per_level:
        sel = (lvl_arr == name)
        if not sel.any():
            continue
        for c in pass_cols:
            col = out[c].to_numpy(dtype=bool)
            col[sel] = masks[c][sel]
            out[c] = col
    out["gate_level"] = lvl_arr

    used = {}
    for d, n in chosen_name.items():
        used[n] = used.get(n, 0) + 1
    return out, {"mode": "auto", "levels_used": used,
                 "note": "날짜마다 첫 통과 단계. 게이트 9개만 재현한 근사다"}


def _continuous_diag(df: pd.DataFrame, factors: List[str], target: str) -> pd.DataFrame:
    rows = []
    for col in factors:
        x = df[[col, target]].dropna()
        n = len(x)
        if n < 200:
            rows.append(
                {
                    "factor": col,
                    "n": n,
                    "spearman": np.nan,
                    "q1_mean_bps": np.nan,
                    "q5_mean_bps": np.nan,
                    "q5_minus_q1_bps": np.nan,
                    "q1_winrate": np.nan,
                    "q5_winrate": np.nan,
                }
            )
            continue

        # Spearman without scipy dependency (rank correlation via Pearson on ranks)
        spearman = float(x[col].rank(method="average").corr(x[target].rank(method="average")))

        try:
            q = _safe_qcut_ranked(x[col], q=5)
            q1 = x.loc[q == 1, target]
            q5 = x.loc[q == 5, target]
            q1_mean = float(q1.mean() * 10000.0)
            q5_mean = float(q5.mean() * 10000.0)
            q1_win = float((q1 > 0).mean())
            q5_win = float((q5 > 0).mean())
            delta = q5_mean - q1_mean
        except Exception:
            q1_mean = np.nan
            q5_mean = np.nan
            q1_win = np.nan
            q5_win = np.nan
            delta = np.nan

        rows.append(
            {
                "factor": col,
                "n": n,
                "spearman": spearman,
                "q1_mean_bps": q1_mean,
                "q5_mean_bps": q5_mean,
                "q5_minus_q1_bps": delta,
                "q1_winrate": q1_win,
                "q5_winrate": q5_win,
            }
        )

    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values("q5_minus_q1_bps", ascending=False, na_position="last")
    return out


def _binary_diag(df: pd.DataFrame, cols: List[str], target: str) -> pd.DataFrame:
    rows = []
    for col in cols:
        x = df[[col, target]].dropna()
        if x.empty:
            continue
        x = x.copy()
        x[col] = x[col].astype(bool)
        pass_n = int(x[col].sum())
        fail_n = int((~x[col]).sum())
        n = int(len(x))

        pass_rate = float(pass_n / n) if n else np.nan
        pass_ret = float(x.loc[x[col], target].mean() * 10000.0) if pass_n else np.nan
        fail_ret = float(x.loc[~x[col], target].mean() * 10000.0) if fail_n else np.nan
        delta = pass_ret - fail_ret if (not np.isnan(pass_ret) and not np.isnan(fail_ret)) else np.nan
        pass_win = float((x.loc[x[col], target] > 0).mean()) if pass_n else np.nan
        fail_win = float((x.loc[~x[col], target] > 0).mean()) if fail_n else np.nan

        rows.append(
            {
                "filter": col,
                "n": n,
                "pass_n": pass_n,
                "fail_n": fail_n,
                "pass_rate": pass_rate,
                "pass_mean_bps": pass_ret,
                "fail_mean_bps": fail_ret,
                "pass_minus_fail_bps": delta,
                "pass_winrate": pass_win,
                "fail_winrate": fail_win,
            }
        )

    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values("pass_minus_fail_bps", ascending=False, na_position="last")
    return out


def _alpha_dispersion(daily_combo: pd.DataFrame) -> dict:
    """alpha 의 흩어짐과 표본 수. avg_alpha_bps 를 혼자 읽지 못하게 붙여 보낸다."""
    a = pd.to_numeric(daily_combo.get("alpha_bps"), errors="coerce").dropna() if len(daily_combo) else pd.Series(dtype=float)
    n = int(len(a))
    if n < 2:
        return {"alpha_n": n, "alpha_std_bps": np.nan, "alpha_se_bps": np.nan,
                "alpha_t": np.nan, "alpha_max_abs_bps": (float(a.iloc[0]) if n == 1 else np.nan)}
    sd = float(a.std(ddof=1))
    se = sd / np.sqrt(n)
    return {
        "alpha_n": n,
        "alpha_std_bps": sd,
        "alpha_se_bps": float(se),
        "alpha_t": float(a.mean() / se) if se > 0 else np.nan,
        # 한 건이 평균을 끌고 가는지 바로 보이게 한다
        "alpha_max_abs_bps": float(a.iloc[a.abs().values.argmax()]),
    }


def _daily_combo_diag(df: pd.DataFrame, target: str) -> pd.DataFrame:
    rows = []
    for d, x in df.groupby("date", sort=True):
        t = pd.to_numeric(x[target], errors="coerce")
        sel = x["all_pass"].astype(bool)
        t_sel = pd.to_numeric(x.loc[sel, target], errors="coerce")
        rows.append(
            {
                "date": d,
                "universe_n": int(len(x)),
                "selected_n": int(sel.sum()),
                "universe_mean_bps": float(t.mean() * 10000.0) if t.notna().any() else np.nan,
                "selected_mean_bps": float(t_sel.mean() * 10000.0) if t_sel.notna().any() else np.nan,
            }
        )
    g = pd.DataFrame(rows)
    g["alpha_bps"] = g["selected_mean_bps"] - g["universe_mean_bps"]
    return g


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback-days", type=int, default=120)
    ap.add_argument("--min-universe", type=int, default=2000)
    ap.add_argument("--target-horizon", type=int, default=1, choices=[1, 2, 5])
    ap.add_argument("--tag", type=str, default="")
    # [2026-09-10] D4 수리. 기본을 auto 로 둔다 - 생산이 쓰는 단계를 재야 의미가 있다.
    ap.add_argument("--gate-level", type=str, default="auto",
                    help="auto(날짜별 첫 통과 단계) / stable(종전=L0) / L0..L9")
    args = ap.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    raw = gen._load_data()
    if raw.empty:
        _log_print("[ERR] no raw data")
        return 2

    df, latest_dt, _ = gen._compute_factors(raw)
    if df.empty:
        _log_print("[ERR] no factor data")
        return 2

    df = df.sort_values(["code", "date"]).reset_index(drop=True)
    df = add_exact_session_forward_returns(df, (1, 2, 5))

    max_d = pd.to_datetime(df["date"].max())
    start_d = max_d - pd.Timedelta(days=int(args.lookback_days))
    dx = df[df["date"] >= start_d].copy()

    uni = dx.groupby("date")["code"].nunique()
    valid_dates = uni[uni >= int(args.min_universe)].index
    dx = dx[dx["date"].isin(valid_dates)].copy()
    if dx.empty:
        _log_print("[ERR] no rows after lookback/min-universe filter")
        return 3

    stable_path = RISK_DIR / "stable_params_v41_1.json"
    params_raw = gen.read_json(stable_path) or {}
    params = gen._normalize_params(params_raw)
    # [2026-09-10] 이 진단은 _select_candidates 의 9개 게이트만 재현한다.
    #   생산은 rule_e(mkt_ret20 / mkt_ret60 / sector_rs)도 적용한다.
    #   2026-09-10 현재 셋 다 -1.0(비활성)이라 _rule_e_threshold 가 None 을 낸다.
    #   그래서 지금은 일치하지만, **다시 켜지면 이 진단이 조용히 과대평가한다.**
    #   (기록상 rule_e 는 2026-08-14 에 후보 발생일을 29% -> 5.6% 로 줄인 적이 있다)
    _rule_e_on = [k for k in ("mkt_ret20_min", "mkt_ret60_min", "sector_rs_min")
                  if gen._rule_e_threshold(params, k) is not None]
    if _rule_e_on:
        _log_print("[WARN] rule_e 가 켜져 있다(%s). 이 진단은 rule_e 를 모델링하지 않으므로 "
                   "all_pass 가 생산보다 **과대**하다. 게이트 표를 그대로 인용하지 말 것."
                   % ", ".join(_rule_e_on))

    dx, gate_info = _build_pass_columns_effective(dx, params, mode=str(args.gate_level))
    _log_print("[GATE_LEVEL] mode=%s levels_used=%s"
               % (gate_info.get("mode"), gate_info.get("levels_used")))

    target = f"fwd_ret_{int(args.target_horizon)}d"

    cont_cols = [
        "rs",
        "rs_slope",
        "v_accel",
        "stretch",
        "atr14_pct",
        "rsi14",
        "vol_close_corr20",
        "high_52w_gap",
        "listing_days",
    ]
    bin_cols = [
        "rs_pass",
        "v_accel_pass",
        "stretch_pass",
        "value_pass",
        "atr_pass",
        "rsi_pass",
        "volcorr_pass",
        "high52_pass",
        "listing_pass",
        "macd_golden",
        "all_pass",
    ]

    cont = _continuous_diag(dx, cont_cols, target=target)
    binary = _binary_diag(dx, bin_cols, target=target)
    daily_combo = _daily_combo_diag(dx, target=target)

    ts = _ts_now()
    tag = (args.tag or "").strip()
    tag_suffix = f"_{tag}" if tag else ""

    cont_path = LOG_DIR / f"indicator_diag_continuous{tag_suffix}_{ts}.csv"
    bin_path = LOG_DIR / f"indicator_diag_binary{tag_suffix}_{ts}.csv"
    combo_path = LOG_DIR / f"indicator_diag_daily_combo{tag_suffix}_{ts}.csv"
    summary_path = LOG_DIR / f"indicator_diag_summary{tag_suffix}_{ts}.json"

    cont.to_csv(cont_path, index=False, encoding="utf-8-sig")
    binary.to_csv(bin_path, index=False, encoding="utf-8-sig")
    daily_combo.to_csv(combo_path, index=False, encoding="utf-8-sig")

    selected_days = int((daily_combo["selected_n"] > 0).sum())
    summary = {
        "as_of": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "latest_date": str(pd.to_datetime(latest_dt).date()),
        "window": {
            "start_date": str(pd.to_datetime(dx["date"].min()).date()),
            "end_date": str(pd.to_datetime(dx["date"].max()).date()),
            "lookback_days": int(args.lookback_days),
            "min_universe": int(args.min_universe),
            "rows": int(len(dx)),
            "dates": int(dx["date"].nunique()),
            "codes": int(dx["code"].nunique()),
        },
        "target": target,
        "target_horizon_days": int(args.target_horizon),
        "target_alignment": "actual_trading_session_exact",
        "price_history_integrity": raw.attrs.get("price_history_integrity", {}),
        "tag": tag,
        "params": {
            "rs_lim": float(params["rs_lim"]),
            "v_accel_lim": float(params["v_accel_lim"]),
            "stretch_max": float(params["stretch_max"]),
            "value_min": float(params["value_min"]),
            "atr_max": float(params["atr_max"]),
            "rsi_max": float(params["rsi_max"]),
            "require_macd_golden": float(params["require_macd_golden"]),
            "vol_close_corr_min": float(params["vol_close_corr_min"]),
            "near_52w_high_gap_max": float(params["near_52w_high_gap_max"]),
            "min_listing_days": float(params["min_listing_days"]),
        },
        # [2026-09-10] 어느 단계에서 잰 것인지 산출물이 스스로 말하게 한다.
        #   종전 산출물은 L0 로 쟀으면서 그 사실을 어디에도 적지 않았다.
        "gate_level": dict(gate_info, rule_e_active=_rule_e_on,
                           modeled_gates=9,
                           unmodeled_note=("rule_e 는 모델링하지 않는다. 비활성일 때만 생산과 일치한다")),
        "combo": {
            "selected_days": selected_days,
            "total_days": int(len(daily_combo)),
            "selected_day_ratio": float(selected_days / len(daily_combo)) if len(daily_combo) else 0.0,
            "avg_selected_n": float(daily_combo["selected_n"].mean()) if len(daily_combo) else 0.0,
            "avg_alpha_bps": float(daily_combo["alpha_bps"].mean(skipna=True)) if len(daily_combo) else np.nan,
            "positive_alpha_day_ratio": float((daily_combo["alpha_bps"] > 0).mean(skipna=True)) if len(daily_combo) else np.nan,
            # [2026-09-10] avg_alpha_bps 단독은 위험하다.
            #   실측 2026-09-10: h5 avg_alpha_bps +846bps 인데 표본이 8일 x 1종목이고,
            #   그중 한 건(+9883bps)을 빼면 나머지 7건 평균이 -445bps 로 부호가 뒤집힌다.
            #   흩어짐과 표본 수를 같이 내보내지 않으면 읽는 쪽이 판단할 수 없다.
            **_alpha_dispersion(daily_combo),
        },
        "top_continuous": cont.head(5).to_dict(orient="records"),
        "top_binary": binary.head(5).to_dict(orient="records"),
        "paths": {
            "continuous_csv": str(cont_path),
            "binary_csv": str(bin_path),
            "daily_combo_csv": str(combo_path),
            "summary_json": str(summary_path),
        },
    }

    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    if tag:
        shutil.copyfile(cont_path, LOG_DIR / f"indicator_diag_continuous_latest_{tag}.csv")
        shutil.copyfile(bin_path, LOG_DIR / f"indicator_diag_binary_latest_{tag}.csv")
        shutil.copyfile(combo_path, LOG_DIR / f"indicator_diag_daily_combo_latest_{tag}.csv")
        shutil.copyfile(summary_path, LOG_DIR / f"indicator_diag_summary_latest_{tag}.json")
    else:
        shutil.copyfile(cont_path, LOG_DIR / "indicator_diag_continuous_latest.csv")
        shutil.copyfile(bin_path, LOG_DIR / "indicator_diag_binary_latest.csv")
        shutil.copyfile(combo_path, LOG_DIR / "indicator_diag_daily_combo_latest.csv")
        shutil.copyfile(summary_path, LOG_DIR / "indicator_diag_summary_latest.json")

    _log_print(f"[OK] continuous={cont_path}")
    _log_print(f"[OK] binary={bin_path}")
    _log_print(f"[OK] combo={combo_path}")
    _log_print(f"[OK] summary={summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



