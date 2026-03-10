# -*- coding: utf-8 -*-
"""v41.1 historical backtest report (operational-rule aligned)."""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
RC_DIR = BASE_DIR / "12_Risk_Controlled"
STABLE_PARAMS = RC_DIR / "stable_params_v41_1.json"
PAPER_ENGINE_CONFIG = BASE_DIR / "paper" / "paper_engine_config.json"

CACHE_LISTING = BASE_DIR / "_cache" / "krx_listing.csv"
PARQUET_GLOB_1 = "krx_daily_*_clean.parquet"
PARQUET_GLOB_2 = "krx_daily_*.parquet"

TRAIN_END = pd.Timestamp("2023-12-31")
VAL_END = pd.Timestamp("2024-12-31")

DEFAULT_SL = -0.05
MIN_SL_CAP = -0.30


def _fix_stop_loss(val):
    if val is None:
        return DEFAULT_SL
    try:
        v = float(val)
    except Exception:
        return DEFAULT_SL
    if v >= 0:
        return DEFAULT_SL
    if v < MIN_SL_CAP:
        return MIN_SL_CAP
    return v


def _parse_mixed_date_series(s: pd.Series) -> pd.Series:
    if np.issubdtype(s.dtype, np.datetime64):
        return pd.to_datetime(s, errors="coerce")
    d = s.astype(str).str.strip()
    dt = pd.to_datetime(d, errors="coerce", format="mixed")
    mask = dt.isna() & d.str.match(r"^\d{8}$", na=False)
    if mask.any():
        dt2 = pd.to_datetime(d[mask], errors="coerce", format="%Y%m%d")
        dt.loc[mask] = dt2
    return dt


def _load_paper_gap_policy() -> tuple[float, float]:
    if not PAPER_ENGINE_CONFIG.exists():
        return 0.0, 0.0
    try:
        j = json.loads(PAPER_ENGINE_CONFIG.read_text(encoding="utf-8"))
        gu = float(j.get("gap_up_max_pct", 0.0) or 0.0)
        gd = float(j.get("entry_gap_down_stop_pct", 0.0) or 0.0)
        return gu, gd
    except Exception:
        return 0.0, 0.0


@dataclass
class Params:
    rs_lim: float
    v_accel_lim: float
    stretch_max: float
    value_min: float
    atr_max: Optional[float] = None
    gap_limit: Optional[float] = None
    gap_up_max_pct: float = 0.0
    entry_gap_down_stop_pct: float = 0.0
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    trail_pct: Optional[float] = None
    hold: int = 10
    max_pos: int = 20
    w_rs: float = 0.2
    w_rs_slope: float = 0.55
    w_v_accel: float = 0.25
    rsi_max: float = 70.0
    require_macd_golden: bool = False
    vol_close_corr_min: float = 0.0
    near_52w_high_gap_max: float = 0.05
    min_listing_days: float = 126.0
    use_relax_ladder: bool = True
    cost: float = 0.005
    slippage: float = 0.001


def load_params() -> Params:
    if not STABLE_PARAMS.exists():
        raise FileNotFoundError(f"missing: {STABLE_PARAMS}")

    p = json.loads(STABLE_PARAMS.read_text(encoding="utf-8"))

    def g(k, default=None):
        return p.get(k, default)

    gu_cfg, gd_cfg = _load_paper_gap_policy()

    params = Params(
        rs_lim=float(g("rs_lim", 1.7)),
        v_accel_lim=float(g("v_accel_lim", 2.5)),
        stretch_max=float(g("stretch_max", 1.19)),
        value_min=float(g("value_min", 105_000_000_000.0)),
        atr_max=(float(g("atr_max")) if g("atr_max") is not None else None),
        gap_limit=(float(g("gap_limit")) if g("gap_limit") is not None else None),
        gap_up_max_pct=float(g("gap_up_max_pct", gu_cfg)),
        entry_gap_down_stop_pct=float(g("entry_gap_down_stop_pct", gd_cfg)),
        stop_loss=(float(g("stop_loss")) if g("stop_loss") is not None else None),
        take_profit=(float(g("take_profit")) if g("take_profit") is not None else None),
        trail_pct=(float(g("trail_pct")) if g("trail_pct") is not None else None),
        hold=int(g("hold", 10)),
        max_pos=int(g("max_pos", 20)),
        w_rs=float(g("w_rs", 0.2)),
        w_rs_slope=float(g("w_rs_slope", 0.55)),
        w_v_accel=float(g("w_v_accel", 0.25)),
        rsi_max=float(g("rsi_max", 70.0)),
        require_macd_golden=(float(g("require_macd_golden", 0.0) or 0.0) >= 0.5),
        vol_close_corr_min=float(g("vol_close_corr_min", 0.0)),
        near_52w_high_gap_max=float(g("near_52w_high_gap_max", 0.05)),
        min_listing_days=float(g("min_listing_days", 126.0)),
        use_relax_ladder=(float(g("use_relax_ladder", 1.0) or 1.0) >= 0.5),
    )

    params.take_profit = None
    params.trail_pct = None
    params.stop_loss = _fix_stop_loss(params.stop_loss)

    print(
        "[PARAM] exit_policy enforced: "
        f"take_profit=None trail_pct=None stop_loss={params.stop_loss}"
    )
    return params


def find_parquets() -> List[Path]:
    files = list(BASE_DIR.rglob(PARQUET_GLOB_1))
    if files:
        return sorted(files)
    return sorted(list(BASE_DIR.rglob(PARQUET_GLOB_2)))


def load_data() -> pd.DataFrame:
    files = find_parquets()
    if not files:
        raise FileNotFoundError(f"no parquet files under {BASE_DIR}")

    use_cols = ["date", "code", "market", "open", "high", "low", "close", "volume", "value"]
    dfs = [pd.read_parquet(f, columns=use_cols) for f in files]
    df = pd.concat(dfs, ignore_index=True)

    df["date"] = _parse_mixed_date_series(df["date"])
    df = df.dropna(subset=["date"]).copy()
    df["code"] = df["code"].astype(str).str.zfill(6)

    for c in ["open", "high", "low", "close", "volume", "value"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
            df.loc[df[c] == 0, c] = np.nan

    df = df.sort_values(["code", "date"]).dropna(subset=["close", "open", "high", "low", "value"]) 
    return df


def compute_factors(df: pd.DataFrame) -> pd.DataFrame:
    m_ret_20 = df.groupby("date")["close"].mean().pct_change(20)
    df["m_ret_20"] = df["date"].map(m_ret_20)
    df["ret_20"] = df.groupby("code")["close"].pct_change(20)
    df["rs"] = df["ret_20"] / (df["m_ret_20"] + 1e-9)
    df["rs_slope"] = df.groupby("code")["rs"].diff(5)
    df["ma5"] = df.groupby("code")["close"].transform(lambda x: x.rolling(5, min_periods=5).mean())
    df["stretch"] = df["close"] / (df["ma5"] + 1e-9)
    df["v_ma5"] = df.groupby("code")["value"].transform(lambda x: x.rolling(5, min_periods=5).mean())
    df["v_accel"] = df["value"] / (df.groupby("code")["v_ma5"].shift(1) + 1e-9)

    delta = df.groupby("code")["close"].diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.groupby(df["code"]).transform(lambda x: x.rolling(14, min_periods=14).mean())
    avg_loss = loss.groupby(df["code"]).transform(lambda x: x.rolling(14, min_periods=14).mean())
    rs = avg_gain / (avg_loss + 1e-9)
    df["rsi14"] = 100.0 - (100.0 / (1.0 + rs))

    ema12 = df.groupby("code")["close"].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    ema26 = df.groupby("code")["close"].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    df["macd_line"] = ema12 - ema26
    df["macd_signal"] = df.groupby("code")["macd_line"].transform(lambda x: x.ewm(span=9, adjust=False).mean())
    prev_macd = df.groupby("code")["macd_line"].shift(1)
    prev_sig = df.groupby("code")["macd_signal"].shift(1)
    df["macd_golden"] = (df["macd_line"] > df["macd_signal"]) & (prev_macd <= prev_sig)

    df["vol_close_corr20"] = (
        df.groupby("code", group_keys=False)[["close", "volume"]]
        .apply(lambda g: g["close"].rolling(20, min_periods=20).corr(g["volume"]))
        .reset_index(level=0, drop=True)
    )
    df["high_52w"] = df.groupby("code")["close"].transform(lambda x: x.rolling(252, min_periods=60).max())
    df["high_52w_gap"] = ((df["high_52w"] - df["close"]) / (df["high_52w"] + 1e-9)).clip(lower=0.0)
    first_date = df.groupby("code")["date"].transform("min")
    df["listing_days"] = (df["date"] - first_date).dt.days

    prev_close = df.groupby("code")["close"].shift(1)
    tr = pd.concat(
        [
            (df["high"] - df["low"]).abs(),
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["tr"] = tr
    df["atr14"] = df.groupby("code")["tr"].transform(lambda x: x.rolling(14, min_periods=14).mean())
    df["atr_pct"] = df["atr14"] / (df["close"] + 1e-9)

    for col in ["open", "high", "low", "close"]:
        df[f"n_{col}"] = df.groupby("code")[col].shift(-1)
    return df


def _params_to_filter_dict(params: Params) -> Dict[str, float]:
    return {
        "rs_lim": float(params.rs_lim),
        "v_accel_lim": float(params.v_accel_lim),
        "stretch_max": float(params.stretch_max),
        "value_min": float(params.value_min),
        "atr_max": float(params.atr_max) if params.atr_max is not None else 9.9,
        "rsi_max": float(params.rsi_max),
        "require_macd_golden": 1.0 if bool(params.require_macd_golden) else 0.0,
        "vol_close_corr_min": float(params.vol_close_corr_min),
        "near_52w_high_gap_max": float(params.near_52w_high_gap_max),
        "min_listing_days": float(params.min_listing_days),
    }


def _relax_ladder(p0: Dict[str, float]) -> List[Tuple[str, Dict[str, float]]]:
    lv: List[Tuple[str, Dict[str, float]]] = []
    lv.append(("L0", dict(p0)))

    p1 = dict(p0)
    p1["v_accel_lim"] = max(float(p1["v_accel_lim"]) * 0.90, 1.20)
    lv.append(("L1", p1))

    p2 = dict(p1)
    p2["rs_lim"] = max(float(p2["rs_lim"]) * 0.95, 1.10)
    p2["v_accel_lim"] = max(float(p2["v_accel_lim"]) * 0.90, 1.20)
    lv.append(("L2", p2))

    p3 = dict(p2)
    p3["rs_lim"] = max(float(p3["rs_lim"]) * 0.95, 1.05)
    p3["v_accel_lim"] = max(float(p3["v_accel_lim"]) * 0.90, 1.20)
    p3["value_min"] = max(float(p3["value_min"]) * 0.85, 5_000_000_000.0)
    p3["stretch_max"] = min(float(p3["stretch_max"]) + 0.03, 1.30)
    p3["atr_max"] = min(float(p3["atr_max"]) + 0.01, 0.25)
    lv.append(("L3", p3))

    p4 = dict(p3)
    p4["rs_lim"] = max(float(p4["rs_lim"]) * 0.93, 1.02)
    p4["v_accel_lim"] = max(float(p4["v_accel_lim"]) * 0.90, 1.15)
    p4["value_min"] = max(float(p4["value_min"]) * 0.70, 3_000_000_000.0)
    p4["stretch_max"] = min(float(p4["stretch_max"]) + 0.03, 1.35)
    p4["atr_max"] = min(float(p4["atr_max"]) + 0.03, 0.30)
    lv.append(("L4", p4))

    p5 = dict(p4)
    p5["rs_lim"] = max(float(p5["rs_lim"]) * 0.92, 1.00)
    p5["v_accel_lim"] = max(float(p5["v_accel_lim"]) * 0.88, 1.10)
    p5["value_min"] = max(float(p5["value_min"]) * 0.55, 2_000_000_000.0)
    p5["stretch_max"] = min(float(p5["stretch_max"]) + 0.04, 1.40)
    p5["atr_max"] = min(float(p5["atr_max"]) + 0.04, 0.35)
    lv.append(("L5", p5))

    p6 = dict(p5)
    p6["rs_lim"] = max(float(p6["rs_lim"]) * 0.90, 1.00)
    p6["v_accel_lim"] = max(float(p6["v_accel_lim"]) * 0.85, 1.05)
    p6["value_min"] = max(float(p6["value_min"]) * 0.50, 1_000_000_000.0)
    p6["stretch_max"] = min(float(p6["stretch_max"]) + 0.05, 1.45)
    p6["atr_max"] = min(float(p6["atr_max"]) + 0.05, 0.40)
    lv.append(("L6", p6))

    return lv


def _select_candidates(day_df: pd.DataFrame, p: Dict[str, float]) -> pd.DataFrame:
    cond = (
        (day_df["rs"] > float(p["rs_lim"]))
        & (day_df["v_accel"] > float(p["v_accel_lim"]))
        & (day_df["stretch"] < float(p["stretch_max"]))
        & (day_df["value"] > float(p["value_min"]))
        & (day_df["atr_pct"] < float(p["atr_max"]))
        & (day_df["rsi14"] < float(p["rsi_max"]))
        & (day_df["vol_close_corr20"] >= float(p["vol_close_corr_min"]))
        & (day_df["high_52w_gap"] <= float(p["near_52w_high_gap_max"]))
        & (day_df["listing_days"] >= float(p["min_listing_days"]))
    )
    if float(p.get("require_macd_golden", 0.0) or 0.0) >= 0.5:
        cond = cond & (day_df["macd_golden"] == True)
    return day_df[cond].copy()


def load_listing() -> Optional[pd.DataFrame]:
    if not CACHE_LISTING.exists():
        return None
    m = pd.read_csv(CACHE_LISTING, dtype={"code": str})
    m["code"] = m["code"].astype(str).str.zfill(6)
    if "name" not in m.columns:
        return None
    return m[["code", "name"]].drop_duplicates("code")


def score_day(day_df: pd.DataFrame, params: Params) -> pd.DataFrame:
    rs_r = day_df["rs"].rank(pct=True)
    slope_r = day_df["rs_slope"].rank(pct=True)
    v_r = day_df["v_accel"].rank(pct=True)
    day_df["score"] = rs_r * params.w_rs + slope_r * params.w_rs_slope + v_r * params.w_v_accel
    return day_df


def simulate_trades(df: pd.DataFrame, params: Params) -> pd.DataFrame:
    cols = [
        "date", "code", "market", "open", "high", "low", "close", "value",
        "rs", "rs_slope", "stretch", "v_accel", "atr_pct", "rsi14", "macd_golden",
        "vol_close_corr20", "high_52w_gap", "listing_days", "n_open", "n_high", "n_low", "n_close",
    ]
    sig = df[cols].copy()
    sig = sig.dropna(subset=["n_open", "n_high", "n_low", "n_close"])
    if sig.empty:
        return pd.DataFrame(columns=["entry_date", "exit_date", "code", "market", "entry_px", "exit_px", "ret", "exit_reason", "signal_date", "score", "relax_level"])

    base_filter = _params_to_filter_dict(params)
    ladder = _relax_ladder(base_filter) if bool(params.use_relax_ladder) else [("L0", base_filter)]

    out_rows = []
    active: Dict[str, pd.Timestamp] = {}
    df_code = {c: g for c, g in df.groupby("code", sort=False)}

    for d, g in sig.groupby("date", sort=True):
        for c in list(active.keys()):
            if active[c] <= d:
                del active[c]

        selected = pd.DataFrame()
        chosen_level = "NONE"
        for level, p_try in ladder:
            cand = _select_candidates(g, p_try)
            if not cand.empty:
                selected = cand
                chosen_level = str(level)
                break

        if selected.empty:
            continue

        day = score_day(selected.copy(), params).sort_values("score", ascending=False)
        slots = max(params.max_pos - len(active), 0)
        if slots <= 0:
            continue

        for _, r in day.iterrows():
            code = r["code"]
            if code in active:
                continue
            if slots <= 0:
                break

            entry_open = float(r["n_open"])
            entry_px = entry_open * (1.0 + params.slippage)

            prev_close = float(r["close"])
            gap = (entry_open - prev_close) / (prev_close + 1e-9)

            gu = float(params.gap_up_max_pct or 0.0)
            gd = float(params.entry_gap_down_stop_pct or 0.0)
            if gu > 0 and gap > gu:
                continue
            if gd > 0 and gap <= -abs(gd):
                continue

            # backward-compat fallback for legacy stable params
            if gu <= 0 and gd <= 0 and params.gap_limit is not None:
                if abs(gap) > float(params.gap_limit):
                    continue

            cdf = df_code.get(code)
            if cdf is None:
                continue
            future = cdf[cdf["date"] > d].head(params.hold).copy()
            if future.empty:
                continue

            exit_reason = "HOLD"
            exit_px = float(future.iloc[-1]["close"]) * (1.0 - params.slippage)

            stop = params.stop_loss
            for _, fr in future.iterrows():
                lo = float(fr["low"])
                if stop is not None:
                    stop_px = entry_px * (1.0 + stop)
                    if lo <= stop_px:
                        exit_reason = "STOP"
                        exit_px = stop_px * (1.0 - params.slippage)
                        break

            ret = (exit_px / (entry_px + 1e-9) - 1.0) - params.cost
            exit_date = pd.to_datetime(future.iloc[-1]["date"])
            active[code] = exit_date

            out_rows.append(
                {
                    "signal_date": pd.to_datetime(d).date().isoformat(),
                    "entry_date": pd.to_datetime(future.iloc[0]["date"]).date().isoformat(),
                    "exit_date": exit_date.date().isoformat(),
                    "code": code,
                    "market": r.get("market", ""),
                    "entry_px": round(entry_px, 6),
                    "exit_px": round(exit_px, 6),
                    "ret": float(ret),
                    "exit_reason": exit_reason,
                    "score": float(r["score"]),
                    "relax_level": chosen_level,
                }
            )
            slots -= 1

    return pd.DataFrame(out_rows)


def summarize(rets: np.ndarray) -> Dict:
    if rets.size == 0:
        return {"n": 0, "win_rate": 0.0, "mean": 0.0, "median": 0.0, "p05": 0.0, "p95": 0.0, "pf": 0.0}
    wins = rets[rets > 0]
    losses = rets[rets < 0]
    pf = float(wins.sum() / (abs(losses.sum()) + 1e-9)) if losses.size else float("inf")
    return {
        "n": int(rets.size),
        "win_rate": float((rets > 0).mean()),
        "mean": float(rets.mean()),
        "median": float(np.median(rets)),
        "p05": float(np.percentile(rets, 5)),
        "p95": float(np.percentile(rets, 95)),
        "pf": pf if np.isfinite(pf) else 999.0,
    }


def main() -> int:
    params = load_params()
    print("[REPORT] loading data ...")
    df = load_data()
    print(f"[REPORT] rows={len(df):,} codes={df['code'].nunique():,} dates={df['date'].nunique():,}")
    print("[REPORT] compute factors ...")
    df = compute_factors(df)
    listing = load_listing()
    print("[REPORT] simulate trades ...")
    trades = simulate_trades(df, params)

    if not trades.empty and listing is not None:
        trades = trades.merge(listing, on="code", how="left")
        cols = [
            "signal_date", "entry_date", "exit_date", "code", "name", "market", "entry_px", "exit_px", "ret", "exit_reason", "score", "relax_level"
        ]
        trades = trades[cols]

    RC_DIR.mkdir(parents=True, exist_ok=True)
    out_trades = RC_DIR / "report_backtest_trades_v41_1.csv"
    trades.to_csv(out_trades, index=False, encoding="utf-8-sig")

    if trades.empty:
        splits_df = pd.DataFrame([{"split": "TRAIN", "n": 0}, {"split": "VAL", "n": 0}, {"split": "OOS", "n": 0}])
        yearly_df = pd.DataFrame(columns=["year", "n", "win_rate", "mean", "median", "p05", "p95", "pf"])
        summary = {"params": params.__dict__, "splits": {}, "yearly": []}
    else:
        entry_dt = pd.to_datetime(trades["entry_date"])
        split = np.where(entry_dt <= TRAIN_END, "TRAIN", np.where(entry_dt <= VAL_END, "VAL", "OOS"))
        trades["split"] = split

        splits = []
        for sp in ["TRAIN", "VAL", "OOS"]:
            r = trades.loc[trades["split"] == sp, "ret"].to_numpy(dtype=float)
            s = summarize(r)
            s["split"] = sp
            splits.append(s)
        splits_df = pd.DataFrame(splits)

        years = pd.to_datetime(trades["entry_date"]).dt.year
        trades["year"] = years
        ys = []
        for y in sorted(trades["year"].unique()):
            r = trades.loc[trades["year"] == y, "ret"].to_numpy(dtype=float)
            s = summarize(r)
            s["year"] = int(y)
            ys.append(s)
        yearly_df = pd.DataFrame(ys)[["year", "n", "win_rate", "mean", "median", "p05", "p95", "pf"]]

        summary = {
            "as_of": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
            "params": params.__dict__,
            "splits": {row["split"]: {k: row[k] for k in row.index if k != "split"} for _, row in splits_df.iterrows()},
            "yearly": yearly_df.to_dict(orient="records"),
            "trades_file": str(out_trades),
        }

    out_splits = RC_DIR / "report_backtest_splits_v41_1.csv"
    out_yearly = RC_DIR / "report_backtest_yearly_v41_1.csv"
    out_summary = RC_DIR / "report_backtest_summary_v41_1.json"
    out_summary.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    splits_df.to_csv(out_splits, index=False, encoding="utf-8-sig")
    yearly_df.to_csv(out_yearly, index=False, encoding="utf-8-sig")

    print("[OK] saved:")
    print(f" - {out_summary}")
    print(f" - {out_splits}")
    print(f" - {out_yearly}")
    print(f" - {out_trades}")

    if not splits_df.empty:
        print("\n[SUMMARY] splits")
        print(splits_df.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
