"""max_pos sweep — standalone, no module import needed."""
from __future__ import annotations
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
PRICES_PATH = ROOT / "paper" / "prices" / "ohlcv_paper.parquet"
STABLE_PATH = ROOT / "12_Risk_Controlled" / "stable_params_v41_1.json"

TRAIN_END = pd.Timestamp("2023-12-31")
VAL_END   = pd.Timestamp("2024-12-31")
# [2026-09-10] 라이브 설정에 맞춰 교정. 이 상수들은 **편도**이고
#   왕복 = 2*FEE + 2*SLIPPAGE + SELL_TAX 로 쓰인다.
#   종전 0.005 는 왕복 1.0~1.4% 로 실제의 2.5~3.5배였다(BROKEN_WINDOW_REGISTER C6).
#   브로커 실측(tr_id TTTC8715R): 매도대금 88,730 / **수수료 0** / 제세금 175 = 0.19723%.
#   라이브 왕복 = 0.0*2 + 0.001*2 + 0.002 = 0.00400 (optimize_params_v41_1.DEFAULT_FEE 와 동일).
#   **이 파일의 과거 산출물은 더 비싼 세계의 값이라 새 실행과 직접 비교할 수 없다.**
#   COST 는 왕복 일괄값이다(params['fee'] 로 시뮬에 전달). 라이브 왕복 0.00400 에 맞춘다.
COST      = 0.00400
MIN_TRADES_TOTAL = 120
MIN_TRADES_PER_WINDOW = 15
PF_STD_PENALTY = 0.35

_PRICE_CACHE = None

def _build_price_cache(df: pd.DataFrame) -> dict:
    slices = {}
    for code, g in df.groupby("code", sort=False):
        g = g.sort_values("date")
        slices[str(code)] = {
            "dates": g["date"].to_numpy(dtype="datetime64[ns]"),
            "open":  g["open"].to_numpy(dtype="float64"),
            "high":  g["high"].to_numpy(dtype="float64"),
            "low":   g["low"].to_numpy(dtype="float64"),
            "close": g["close"].to_numpy(dtype="float64"),
        }
    return {"slices": slices}

def _select_signals(day_df: pd.DataFrame, p: dict) -> pd.DataFrame:
    cond = (
        (day_df["rs"] >= float(p["rs_lim"]))
        & (day_df["v_accel"] >= float(p["v_accel_lim"]))
        & (day_df["stretch"] <= float(p["stretch_max"]))
        & (day_df["value"] >= float(p["value_min"]))
        & (day_df["atr14_pct"] <= float(p["atr_max"]))
        & (day_df["rsi14"] < float(p["rsi_max"]))
        & (day_df["vol_close_corr20"] >= float(p.get("vol_close_corr_min", 0.0)))
        & (day_df["high_52w_gap"] <= float(p.get("near_52w_high_gap_max", 0.05)))
        & (day_df["listing_days"] >= float(p.get("min_listing_days", 126.0)))
    )
    if float(p.get("require_macd_golden", 0.0) or 0.0) >= 0.5:
        cond = cond & (day_df["macd_golden"] == True)
    return day_df[cond].copy()

def simulate_window(df: pd.DataFrame, params: dict) -> list[float]:
    global _PRICE_CACHE
    if df is None or df.empty:
        return []
    _PRICE_CACHE = _build_price_cache(df)
    pc = _PRICE_CACHE
    slices = pc["slices"]

    rs_lim = float(params.get("rs_lim", 1.0))
    v_accel_lim = float(params.get("v_accel_lim", 1.0))
    stretch_max = float(params.get("stretch_max", 9.9))
    value_min = float(params.get("value_min", 0.0))
    atr_max = float(params.get("atr_max", 9.9))
    rsi_max = float(params.get("rsi_max", 70.0))
    require_macd = float(params.get("require_macd_golden", 0.0) or 0.0) >= 0.5
    vol_close_corr_min = float(params.get("vol_close_corr_min", 0.0))
    near_52w_high_gap_max = float(params.get("near_52w_high_gap_max", 0.05))
    min_listing_days = float(params.get("min_listing_days", 126.0))
    use_relax_ladder = float(params.get("use_relax_ladder", 1.0) or 1.0) >= 0.5
    gap_up_max_pct = float(params.get("gap_up_max_pct", 0.0) or 0.0)
    entry_gap_down_stop_pct = float(params.get("entry_gap_down_stop_pct", 0.0) or 0.0)
    gap_limit = float(params.get("gap_limit", 0.0) or 0.0)
    w_rs = float(params.get("w_rs", 0.2))
    w_sl = float(params.get("w_rs_slope", 0.55))
    w_va = float(params.get("w_v_accel", 0.25))
    hold_days = int(params.get("hold", 10))
    max_pos = int(params.get("max_pos", 8))
    fee = float(params.get("fee", COST))

    sl_raw = params.get("stop_loss", -0.05)
    stop_loss = float(sl_raw) if sl_raw is not None else None
    take_profit = params.get("take_profit", None)
    trail_pct = params.get("trail_pct", None)

    req_cols = ["date","code","rs","rs_slope","stretch","v_accel","value","atr14_pct","rsi14","vol_close_corr20","high_52w_gap","listing_days","macd_golden"]
    for c in req_cols:
        if c not in df.columns:
            return []

    p0 = {"rs_lim": rs_lim, "v_accel_lim": v_accel_lim, "stretch_max": stretch_max,
          "value_min": value_min, "atr_max": atr_max, "rsi_max": rsi_max,
          "require_macd_golden": float(require_macd), "vol_close_corr_min": vol_close_corr_min,
          "near_52w_high_gap_max": near_52w_high_gap_max, "min_listing_days": min_listing_days}

    # Relax ladders
    relax_levels = []
    if use_relax_ladder:
        for mul_rs, mul_va, mul_val in [(0.97,0.95,0.90),(0.93,0.88,0.75),(0.88,0.80,0.60)]:
            pr = dict(p0)
            pr["rs_lim"] = max(float(p0["rs_lim"]) * mul_rs, 1.00)
            pr["v_accel_lim"] = max(float(p0["v_accel_lim"]) * mul_va, 1.05)
            pr["value_min"] = max(float(p0["value_min"]) * mul_val, 1_000_000_000.0)
            relax_levels.append(pr)

    # Build signal list for this window
    all_sigs = []
    for d, gdf in df.groupby("date", sort=True):
        sel = _select_signals(gdf, p0)
        if sel.empty and relax_levels:
            for rp in relax_levels:
                sel = _select_signals(gdf, rp)
                if not sel.empty:
                    break
        if sel.empty:
            continue
        sel = sel.copy()
        sel["_score"] = (
            sel["rs"].astype(float) * w_rs
            + sel.get("rs_slope", pd.Series(0.0, index=sel.index)).astype(float) * w_sl
            + sel["v_accel"].astype(float) * w_va
        )
        sel = sel.sort_values("_score", ascending=False)
        for _, row in sel.iterrows():
            code = str(row["code"])
            if gap_limit > 0:
                gap_val = float(row.get("gap_limit", 0.0) or 0.0)
                if gap_val > gap_limit:
                    continue
            all_sigs.append((d, code, float(row["_score"])))

    if not all_sigs:
        return []

    sig_dates = [s[0] for s in all_sigs]
    sig_codes = [s[1] for s in all_sigs]
    n = len(all_sigs)
    rets = []
    active = []   # list of (exit_date, code)
    active_codes: set = set()
    i = 0
    while i < n:
        d = sig_dates[i]
        # expire finished
        if active:
            still_active = []
            for ex_d, ex_c in active:
                if ex_d > d:
                    still_active.append((ex_d, ex_c))
                else:
                    active_codes.discard(ex_c)
            active = still_active

        if len(active) >= max_pos:
            j = i + 1
            while j < n and sig_dates[j] == d:
                j += 1
            i = j
            continue

        j = i
        while j < n and sig_dates[j] == d:
            if len(active) >= max_pos:
                break
            code = str(sig_codes[j])
            j += 1
            if code in active_codes:
                continue
            sl_info = slices.get(code)
            if sl_info is None:
                continue
            dates_arr = sl_info["dates"]
            open_arr  = sl_info["open"]
            high_arr  = sl_info["high"]
            low_arr   = sl_info["low"]
            close_arr = sl_info["close"]
            # find entry index (next day after d)
            d_ts = np.datetime64(d, "ns")
            idx_arr = np.searchsorted(dates_arr, d_ts, side="right")
            if idx_arr >= len(dates_arr):
                continue
            entry_d = dates_arr[idx_arr]
            entry_p = float(open_arr[idx_arr])
            if not np.isfinite(entry_p) or entry_p <= 0:
                continue
            # gap checks
            if gap_up_max_pct > 0:
                prev_c = float(close_arr[idx_arr - 1]) if idx_arr > 0 else entry_p
                if prev_c > 0 and (entry_p - prev_c) / prev_c > gap_up_max_pct:
                    continue
            if entry_gap_down_stop_pct > 0:
                prev_c2 = float(close_arr[idx_arr - 1]) if idx_arr > 0 else entry_p
                if prev_c2 > 0 and (entry_p - prev_c2) / prev_c2 < -entry_gap_down_stop_pct:
                    continue
            # simulate hold
            trail_p = None
            exit_p = float(close_arr[min(idx_arr + hold_days, len(close_arr) - 1)])
            exit_d = dates_arr[min(idx_arr + hold_days, len(dates_arr) - 1)]
            for k in range(idx_arr, min(idx_arr + hold_days + 1, len(dates_arr))):
                hi_p = float(high_arr[k])
                lo_p = float(low_arr[k])
                cl_p = float(close_arr[k])
                if stop_loss is not None and np.isfinite(lo_p):
                    sl_price = entry_p * (1.0 + stop_loss)
                    if lo_p <= sl_price:
                        exit_p = max(sl_price, lo_p)
                        exit_d = dates_arr[k]
                        break
                if take_profit is not None and np.isfinite(hi_p):
                    tp_price = entry_p * (1.0 + take_profit)
                    if hi_p >= tp_price:
                        exit_p = tp_price
                        exit_d = dates_arr[k]
                        break
                if trail_pct is not None and np.isfinite(hi_p):
                    new_trail = hi_p * (1.0 - trail_pct)
                    trail_p = max(trail_p, new_trail) if trail_p is not None else new_trail
                if trail_p is not None and np.isfinite(lo_p) and lo_p <= trail_p:
                    exit_p = float(trail_p)
                    exit_d = dates_arr[k]
                    break
            pnl_pct = (exit_p / entry_p) - 1.0 - fee
            if np.isfinite(pnl_pct):
                rets.append(float(pnl_pct))
            active.append((exit_d, code))
            active_codes.add(code)
        while j < n and sig_dates[j] == d:
            j += 1
        i = j
    return rets

def build_windows(df: pd.DataFrame, years_back: int = 10):
    end = df["date"].max().normalize()
    windows = []
    for k in range(years_back):
        w_end = end - pd.DateOffset(years=k)
        w_start = w_end - pd.DateOffset(years=1) + pd.DateOffset(days=1)
        windows.append((w_start, w_end))
    windows = sorted(windows, key=lambda x: x[0])
    min_d = df["date"].min().normalize()
    out = []
    for s, e in windows:
        s2 = max(s, min_d)
        e2 = min(e, end)
        if s2 >= e2:
            continue
        out.append((s2, e2))
    return out

def eval_params(df: pd.DataFrame, windows, params: dict):
    pfs, means, total_n = [], [], 0
    results = []
    for s, e in windows:
        wdf = df[(df["date"] >= s) & (df["date"] <= e)].copy()
        rets_list = simulate_window(wdf, params)
        rets = np.asarray(rets_list, dtype="float64")
        if rets.size:
            rets = rets[np.isfinite(rets)]
        n = int(rets.size)
        total_n += n
        if n < 5:
            pf, mean_ret = 0.0, -1.0
        else:
            pos = rets[rets > 0].sum()
            neg = abs(rets[rets < 0].sum())
            pf = float(pos / (neg + 1e-9))
            mean_ret = float(np.mean(rets))
        split = "IS" if e <= TRAIN_END else ("VAL" if e <= VAL_END else "OOS")
        results.append({"split": split, "n": n, "pf": pf, "mean_ret": mean_ret,
                        "start": s.strftime("%Y-%m-%d"), "end": e.strftime("%Y-%m-%d")})
        pfs.append(pf)
        means.append(mean_ret)

    if total_n <= 0:
        return -1e9, results

    penalty_n = (max(0, MIN_TRADES_TOTAL - total_n) / max(1, MIN_TRADES_TOTAL)) * 0.8
    low_windows = sum(1 for r in results if r["n"] < MIN_TRADES_PER_WINDOW)
    penalty_n += low_windows * 0.15
    mean_pf = float(np.mean(pfs)) if pfs else 0.0
    std_pf = float(np.std(pfs)) if pfs else 0.0
    mean_ret = float(np.mean(means)) if means else -1.0

    def _cap(x): return max(0.0, min(float(x) if np.isfinite(float(x)) else 5.0, 5.0))
    oos_res = [r for r in results if r["split"] == "OOS"]
    val_res = [r for r in results if r["split"] == "VAL"]
    is_res  = [r for r in results if r["split"] == "IS"]

    def _avg_pf(arr):  return float(np.mean([r["pf"] for r in arr])) if arr else 0.0
    def _avg_ret(arr): return float(np.mean([r["mean_ret"] for r in arr])) if arr else 0.0
    def _sum_n(arr):   return int(sum(r["n"] for r in arr))

    oos_pf  = _avg_pf(oos_res); val_pf = _avg_pf(val_res); is_pf = _avg_pf(is_res)
    oos_ret = _avg_ret(oos_res); val_ret = _avg_ret(val_res); is_ret = _avg_ret(is_res)
    oos_n = _sum_n(oos_res)
    penalty_oos = (max(0, MIN_TRADES_PER_WINDOW - oos_n) / max(1, MIN_TRADES_PER_WINDOW)) * 0.8
    if oos_n <= 0: penalty_oos += 1.0

    score = (
        (_cap(oos_pf) * 1.6 + _cap(val_pf) * 0.6 + _cap(is_pf) * 0.2)
        + (oos_ret * 40.0 + val_ret * 10.0 + is_ret * 2.0)
        - (PF_STD_PENALTY * std_pf)
        - penalty_n - penalty_oos
    )
    return float(score), results

# ── main ──────────────────────────────────────────────────────────
df = pd.read_parquet(PRICES_PATH)
df["date"] = pd.to_datetime(df["date"].astype(str).str.replace(r"[^0-9]","",regex=True).str[:8], format="%Y%m%d", errors="coerce")
df = df.dropna(subset=["date"])
df["code"] = df["code"].astype(str).str.zfill(6)
for c in ["open","high","low","close","volume"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")
df = df.dropna(subset=["open","high","low","close","volume"])

stable = json.load(open(STABLE_PATH, encoding="utf-8-sig"))
base = {
    "rs_lim": stable["rs_lim"],
    "v_accel_lim": stable["v_accel_lim"],
    "stretch_max": stable["stretch_max"],
    "value_min": stable["value_min"],
    "atr_max": stable["atr_max"],
    "gap_limit": stable.get("gap_limit", 0.04),
    "gap_up_max_pct": stable.get("gap_up_max_pct", 0.03),
    "entry_gap_down_stop_pct": stable.get("entry_gap_down_stop_pct", 0.03),
    "rsi_max": stable.get("rsi_max", 70.0),
    "require_macd_golden": stable.get("require_macd_golden", 0.0),
    "vol_close_corr_min": stable.get("vol_close_corr_min", 0.0),
    "near_52w_high_gap_max": stable.get("near_52w_high_gap_max", 0.05),
    "min_listing_days": stable.get("min_listing_days", 126.0),
    "use_relax_ladder": stable.get("use_relax_ladder", 1.0),
    "w_rs": stable["w_rs"],
    "w_rs_slope": stable["w_rs_slope"],
    "w_v_accel": stable["w_v_accel"],
    "stop_loss": stable.get("stop_loss", -0.05),
    "take_profit": stable.get("take_profit"),
    "trail_pct": stable.get("trail_pct"),
    "hold": stable["hold"],
    "fee": COST,
}

windows = build_windows(df, years_back=10)
print(f"Data: {df['date'].min().date()} ~ {df['date'].max().date()}, windows={len(windows)}")
print(f"Params: hold={base['hold']} stop={base['stop_loss']} rs_lim={base['rs_lim']}")
print()
print(f"{'mp':>5} {'score':>8} {'oos_pf':>7} {'oos_ret%':>9} {'oos_n':>6} {'val_pf':>7} {'is_pf':>7}")
print("-"*55)

rows = []
for mp in [4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 20]:
    p = dict(base)
    p["max_pos"] = mp
    score, results = eval_params(df, windows, p)
    oos = [r for r in results if r["split"]=="OOS"]
    val = [r for r in results if r["split"]=="VAL"]
    is_ = [r for r in results if r["split"]=="IS"]
    oos_pf  = np.mean([r["pf"] for r in oos]) if oos else 0.0
    oos_ret = np.mean([r["mean_ret"] for r in oos])*100 if oos else 0.0
    oos_n   = sum(r["n"] for r in oos)
    val_pf  = np.mean([r["pf"] for r in val]) if val else 0.0
    is_pf   = np.mean([r["pf"] for r in is_]) if is_ else 0.0
    marker  = " <-- current" if mp == 8 else ""
    print(f"{mp:>5} {score:>8.3f} {oos_pf:>7.3f} {oos_ret:>9.4f} {oos_n:>6} {val_pf:>7.3f} {is_pf:>7.3f}{marker}")
    rows.append({"mp": mp, "score": score, "oos_pf": oos_pf, "oos_ret": oos_ret, "oos_n": oos_n, "val_pf": val_pf})

best = max(rows, key=lambda r: r["score"])
print()
print(f"Best: max_pos={best['mp']}  score={best['score']:.3f}  oos_pf={best['oos_pf']:.3f}  oos_ret={best['oos_ret']:.4f}%")
