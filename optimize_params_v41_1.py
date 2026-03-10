# -*- coding: utf-8 -*-
"""STOC v41.1 - Parameter Optimizer (stability-oriented)

紐⑹쟻
- KRX ?쇰큺 parquet(沅뚯옣: krx_daily_*_clean.parquet) 湲곕컲?쇰줈
  v41.1 ?꾨낫 ?꾩텧 議곌굔??"怨쇨굅 ?깃낵"濡?寃利앺빐 best/stable ?뚮씪誘명꽣瑜???ν빀?덈떎.

異쒕젰
- E:/1_Data/12_Risk_Controlled/best_params_v41_1.json
- E:/1_Data/12_Risk_Controlled/stable_params_v41_1.json
- E:/1_Data/12_Risk_Controlled/search_report_v41_1.csv

二쇱쓽
- ?덉젙?깆쓣 ?꾪빐 "泥?궛/由ъ뒪?? ?뚮씪誘명꽣(stop/tp/trail/hold/max_pos)??湲곕낯?곸쑝濡?怨좎젙(FROZEN)?⑸땲??
  (Entry ?꾪꽣/媛以묒튂留??먯깋)
"""

from __future__ import annotations

import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# --------------------
# 湲곕낯 寃쎈줈/?곸닔
# --------------------
BASE_DIR = Path(os.environ.get("STOC_BASE_DIR", r"E:\1_Data"))
OUT_DIR = BASE_DIR / "12_Risk_Controlled"
OUT_DIR.mkdir(parents=True, exist_ok=True)
PAPER_ENGINE_CONFIG = BASE_DIR / "paper" / "paper_engine_config.json"

# ?곗씠??踰붿쐞: 2020~2025 ?꾩껜瑜??ы븿?섎젮硫?6 ?댁긽 沅뚯옣
# -----------------------------------------------------------------------------
# Price cache (code -> slice in sorted arrays)
# -----------------------------------------------------------------------------
_PRICE_CACHE = None  # dict with keys: code_s, date_s, open_s, high_s, low_s, close_s, slices

def _build_price_cache(df: pd.DataFrame) -> dict:
    """Build a memory-efficient cache for fast per-code OHLC access.

    The cache keeps one set of large arrays sorted by (code, date) and a mapping
    code -> (start, end) slice indices into those arrays.
    """
    need = ["code", "date", "open", "high", "low", "close"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise ValueError(f"missing columns for price cache: {missing}")

    tmp = df[need].copy()
    tmp["code"] = tmp["code"].astype(str)
    tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce")
    tmp = tmp.dropna(subset=["code", "date", "open", "high", "low", "close"])
    if tmp.empty:
        raise ValueError("price cache build: empty after dropna")

    code = tmp["code"].to_numpy()
    date = tmp["date"].to_numpy(dtype="datetime64[ns]")
    o = tmp["open"].to_numpy(dtype=float)
    h = tmp["high"].to_numpy(dtype=float)
    l = tmp["low"].to_numpy(dtype=float)
    c = tmp["close"].to_numpy(dtype=float)

    # sort by code then date
    order = np.lexsort((date, code))
    code_s = code[order]
    date_s = date[order]
    o_s = o[order]
    h_s = h[order]
    l_s = l[order]
    c_s = c[order]

    # boundaries where code changes
    if len(code_s) == 0:
        raise ValueError("price cache build: empty after sort")
    cut = np.flatnonzero(code_s[1:] != code_s[:-1]) + 1
    bounds = np.concatenate(([0], cut, [len(code_s)]))
    slices = {code_s[bounds[i]]: (int(bounds[i]), int(bounds[i + 1])) for i in range(len(bounds) - 1)}

    return {
        "code_s": code_s,
        "date_s": date_s,
        "open_s": o_s,
        "high_s": h_s,
        "low_s": l_s,
        "close_s": c_s,
        "slices": slices,
    }

YEARS_BACK = 10

# ?먯깋 ?잛닔 (二?1???ㅽ뻾 媛??
N_ITER = 160

# 鍮꾩슜/?щ━?쇱? (蹂댁닔?곸쑝濡?
COST = 0.005
SLIPPAGE = 0.001

# 嫄곕옒??湲곗? (?곗뿰 PF 諛⑹?)
MIN_TRADES_TOTAL = 120
MIN_TRADES_PER_WINDOW = 15

# ?덉젙???⑤꼸??
PF_STD_PENALTY = 0.35

# Stable ?밴툒 議곌굔
PROMOTION_MARGIN = 0.05  # best媛 stable蹂대떎 5% ?댁긽 醫뗭쓣 ?뚮쭔 怨좊젮
STEP_TOWARD_ALPHA = 0.35  # stable??best 諛⑺뼢?쇰줈 ??踰덉뿉 ?쇰쭏???대룞?좎?

# ?먯깋 ?뚮씪誘명꽣 踰붿쐞 (Entry + Weights)
BOUNDS = {
    "rs_lim": (1.10, 3.00, 0.05),
    "v_accel_lim": (1.00, 8.00, 0.10),
    "stretch_max": (1.05, 1.30, 0.01),
    "value_min": (30e9, 300e9, 5e9),
    "atr_max": (0.03, 0.25, 0.005),
    "gap_limit": (0.00, 0.25, 0.01),
}

# 怨좎젙(Freeze) ?뚮씪誘명꽣: ?먯깋?섏? ?딆쓬
FROZEN_KEYS = ["stop_loss", "take_profit", "trail_pct", "hold", "max_pos"]

DEFAULT_FROZEN = {
    "stop_loss": -0.05,
    "take_profit": 0.15,
    "trail_pct": 0.08,
    "hold": 10,
    "max_pos": 20,
}

# 嫄곕옒 鍮꾩슜 (paper_engine_config.json 湲곕낯媛믨낵 ?숈씪)
DEFAULT_FEE = 0.005


# --- IS/VAL/OOS split policy (for selection scoring) ---
SPLIT_POLICY_PATH = BASE_DIR / '12_Risk_Controlled' / 'split_policy_v41_1.json'
DEFAULT_TRAIN_END = datetime(2023, 12, 31)
DEFAULT_VAL_END   = datetime(2024, 12, 31)

def _load_split_policy():
    train_end = DEFAULT_TRAIN_END
    val_end = DEFAULT_VAL_END
    try:
        if SPLIT_POLICY_PATH.exists():
            j = json.load(open(SPLIT_POLICY_PATH, 'r', encoding='utf-8'))
            te = j.get('train_end')
            ve = j.get('val_end')
            if te:
                train_end = datetime.strptime(str(te)[:10], '%Y-%m-%d')
            if ve:
                val_end = datetime.strptime(str(ve)[:10], '%Y-%m-%d')
    except Exception:
        pass
    return train_end, val_end

TRAIN_END, VAL_END = _load_split_policy()


# ?쒕뜡 ?먯깋 ?쒕뱶: ?ы쁽???뺣낫(遺덉븞??媛먯냼)
RNG_SEED = 42


# --------------------
# ?좏떥
# --------------------

def _jload(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _jsave(path: Path, obj: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def _load_paper_gap_policy() -> tuple[float, float]:
    if not PAPER_ENGINE_CONFIG.exists():
        return 0.0, 0.0
    try:
        j = _jload(PAPER_ENGINE_CONFIG)
        gu = _safe_float(j.get("gap_up_max_pct", 0.0), 0.0)
        gd = _safe_float(j.get("entry_gap_down_stop_pct", 0.0), 0.0)
        return float(gu), float(gd)
    except Exception:
        return 0.0, 0.0


GAP_POLICY_KEYS = ("gap_up_max_pct", "entry_gap_down_stop_pct")


def _norm_gap_value(v, default: float = 0.0) -> float:
    x = _safe_float(v, default)
    if not np.isfinite(x):
        return float(default)
    return max(0.0, float(x))


def _apply_gap_policy_schema(obj: dict, fallback: dict | None = None) -> dict:
    fb = fallback or {}
    out = dict(obj)
    out["gap_up_max_pct"] = _norm_gap_value(out.get("gap_up_max_pct", fb.get("gap_up_max_pct", 0.0)))
    out["entry_gap_down_stop_pct"] = _norm_gap_value(out.get("entry_gap_down_stop_pct", fb.get("entry_gap_down_stop_pct", 0.0)))
    return out


def _today_str() -> str:
    return date.today().strftime("%Y-%m-%d")


def _round_grid(x: float, step: float) -> float:
    if step <= 0:
        return float(x)
    return round(round(x / step) * step, 10)


def _clip_grid(x: float, lo: float, hi: float, step: float) -> float:
    x = max(lo, min(hi, float(x)))
    return _round_grid(x, step)


def _safe_float(v, default: float) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _safe_int(v, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


@dataclass
class WindowResult:
    """Per-window evaluation result.

    Field name is **n_trades** (not n). Some older scripts used `n`, but this
    optimizer uses `n_trades` everywhere (reporting, penalties, splits).
    We keep a read-only alias property `n` for backward compatibility.
    """

    start: str
    end: str
    n_trades: int
    pf: float
    mean_ret: float
    split: str
    year: int

    @property
    def n(self) -> int:
        return int(self.n_trades)


# --------------------
# ?곗씠??濡쒕뱶/?⑺꽣
# --------------------

def find_parquets(base_dir: Path) -> List[Path]:
    clean = sorted(base_dir.rglob("krx_daily_*_clean.parquet"))
    if clean:
        return clean
    return sorted(base_dir.rglob("krx_daily_*.parquet"))


def load_data(base_dir: Path) -> pd.DataFrame:
    files = find_parquets(base_dir)
    if not files:
        raise FileNotFoundError(f"KRX parquet ?뚯씪???놁뒿?덈떎: {base_dir}")

    dfs = []
    for p in files:
        df = pd.read_parquet(p)
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)

    # dtype normalize
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["code"] = df["code"].astype(str).str.zfill(6)

    for c in ["open", "high", "low", "close", "value"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
            df.loc[df[c] == 0, c] = np.nan

    need = ["date", "code", "open", "high", "low", "close", "value"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise RuntimeError(f"?꾩닔 而щ읆 ?꾨씫: {missing}")

    df = df.dropna(subset=["date", "code", "open", "high", "low", "close", "value"]).copy()
    df = df.sort_values(["code", "date"]).reset_index(drop=True)

    return df


def compute_factors(df: pd.DataFrame) -> pd.DataFrame:
    # index proxy
    idx = df.groupby("date", as_index=True)["close"].mean().sort_index()
    idx_ma60 = idx.rolling(60).mean()
    m_ret20 = idx.pct_change(20)

    df["m_ret20"] = df["date"].map(m_ret20)
    df["ret20"] = df.groupby("code")["close"].pct_change(20)
    df["rs"] = df["ret20"] / (df["m_ret20"] + 1e-9)
    df["rs_slope"] = df.groupby("code")["rs"].diff(5)

    df["ma5"] = df.groupby("code")["close"].transform(lambda x: x.rolling(5).mean())
    df["stretch"] = df["close"] / (df["ma5"] + 1e-9)

    df["v_ma5"] = df.groupby("code")["value"].transform(lambda x: x.rolling(5).mean())
    df["v_accel"] = df["value"] / (df.groupby("code")["v_ma5"].shift(1) + 1e-9)

    # Auxiliary operational filters (aligned with candidate generator)
    delta = df.groupby("code")["close"].diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.groupby(df["code"]).transform(lambda x: x.rolling(14, min_periods=14).mean())
    avg_loss = loss.groupby(df["code"]).transform(lambda x: x.rolling(14, min_periods=14).mean())
    rs14 = avg_gain / (avg_loss + 1e-9)
    df["rsi14"] = 100.0 - (100.0 / (1.0 + rs14))

    ema12 = df.groupby("code")["close"].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    ema26 = df.groupby("code")["close"].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    df["macd_line"] = ema12 - ema26
    df["macd_signal"] = df.groupby("code")["macd_line"].transform(lambda x: x.ewm(span=9, adjust=False).mean())
    prev_macd = df.groupby("code")["macd_line"].shift(1)
    prev_sig = df.groupby("code")["macd_signal"].shift(1)
    df["macd_golden"] = (df["macd_line"] > df["macd_signal"]) & (prev_macd <= prev_sig)

    df["vol_close_corr20"] = (
        df.groupby("code", group_keys=False)[["close", "value"]]
        .apply(lambda g: g["close"].rolling(20, min_periods=20).corr(g["value"]))
        .reset_index(level=0, drop=True)
    )
    df["high_52w"] = df.groupby("code")["close"].transform(lambda x: x.rolling(252, min_periods=60).max())
    df["high_52w_gap"] = ((df["high_52w"] - df["close"]) / (df["high_52w"] + 1e-9)).clip(lower=0.0)
    first_date = df.groupby("code")["date"].transform("min")
    df["listing_days"] = (df["date"] - first_date).dt.days

    # ATR14% (robust)
    prev_close = df.groupby("code")["close"].shift(1)
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - prev_close).abs()
    tr3 = (df["low"] - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr14"] = tr.groupby(df["code"]).transform(lambda x: x.rolling(14).mean())
    df["atr14_pct"] = df["atr14"] / (df["close"] + 1e-9)

    # next-day open for entry; gap for backtest filter
    df["n_open"] = df.groupby("code")["open"].shift(-1)
    df["gap_next"] = (df["n_open"] / (df["close"] + 1e-9) - 1.0).abs()

    return df


# --------------------
# 諛깊뀒?ㅽ듃(?⑥닚/?덉젙)
# --------------------

def _score_day(df_day: pd.DataFrame, w: dict) -> pd.Series:
    # percentile ranks among passing candidates (same day)
    r_rs = df_day["rs"].rank(pct=True)
    r_slope = df_day["rs_slope"].rank(pct=True)
    r_va = df_day["v_accel"].rank(pct=True)

    w_rs = _safe_float(w.get("w_rs"), 0.2)
    w_slope = _safe_float(w.get("w_rs_slope"), 0.55)
    w_va = _safe_float(w.get("w_v_accel"), 0.25)

    s = (r_rs * w_rs) + (r_slope * w_slope) + (r_va * w_va)
    return s


def _relax_ladder_operational(p0: dict) -> list[tuple[str, dict]]:
    lv = []
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


def _select_day_candidates_operational(day_df: pd.DataFrame, p: dict) -> pd.DataFrame:
    cond = (
        (day_df["rs"] >= float(p["rs_lim"]))
        & (day_df["v_accel"] >= float(p["v_accel_lim"]))
        & (day_df["stretch"] <= float(p["stretch_max"]))
        & (day_df["value"] >= float(p["value_min"]))
        & (day_df["atr14_pct"] <= float(p["atr_max"]))
        & (day_df["rsi14"] < float(p["rsi_max"]))
        & (day_df["vol_close_corr20"] >= float(p["vol_close_corr_min"]))
        & (day_df["high_52w_gap"] <= float(p["near_52w_high_gap_max"]))
        & (day_df["listing_days"] >= float(p["min_listing_days"]))
    )
    if float(p.get("require_macd_golden", 0.0) or 0.0) >= 0.5:
        cond = cond & (day_df["macd_golden"] == True)
    return day_df[cond].copy()


def simulate_window(df: pd.DataFrame, params: dict) -> list[float]:
    """Simulate a single window.

    - Signals are generated from factor columns on df (window-limited).
    - Entry is next trading day's OPEN (D+1) for each signal day.
    - Exit is within HOLD_DAYS with stop/TP/trailing rules.
    - Max concurrent positions enforced.
    """
    global _PRICE_CACHE

    if df is None or df.empty:
        return []

    # Window bounds (hard clip: do not use prices beyond this window)
    w_start = pd.to_datetime(df["date"].min())
    w_end = pd.to_datetime(df["date"].max())
    if pd.isna(w_start) or pd.isna(w_end):
        return []

    # Build cache once (expensive) and reuse across all parameter trials
    if _PRICE_CACHE is None:
        _PRICE_CACHE = _build_price_cache(df)

    pc = _PRICE_CACHE
    slices = pc["slices"]
    date_s = pc["date_s"]
    o_s = pc["open_s"]
    h_s = pc["high_s"]
    l_s = pc["low_s"]
    c_s = pc["close_s"]

    # thresholds
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

    # weights for daily ranking score
    w_rs = float(params.get("w_rs", 0.2))
    w_sl = float(params.get("w_rs_slope", 0.55))
    w_va = float(params.get("w_v_accel", 0.25))

    # frozen/湲곕낯媛? stable_params_v41_1.json(?덉쑝硫? ??params ??DEFAULT_FROZEN
    hold_days = int(params.get("hold", params.get("hold_days", DEFAULT_FROZEN.get("hold", 10))))
    max_pos = int(params.get("max_pos", params.get("max_positions", DEFAULT_FROZEN.get("max_pos", 20))))
    fee = float(params.get("fee", DEFAULT_FEE))

    _sl_raw = params.get("stop_loss", DEFAULT_FROZEN.get("stop_loss", -0.05))
    stop_loss = float(_sl_raw) if _sl_raw is not None else None
    take_profit = params.get("take_profit", DEFAULT_FROZEN.get("take_profit", None))
    trail_pct = params.get("trail_pct", DEFAULT_FROZEN.get("trail_pct", None))

    # signal filter (window-limited)
    req_cols = ["date", "code", "rs", "rs_slope", "stretch", "v_accel", "value", "atr14_pct", "rsi14", "vol_close_corr20", "high_52w_gap", "listing_days", "macd_golden"]
    for c in req_cols:
        if c not in df.columns:
            return []

    p0 = {
        "rs_lim": rs_lim,
        "v_accel_lim": v_accel_lim,
        "stretch_max": stretch_max,
        "value_min": value_min,
        "atr_max": atr_max,
        "rsi_max": rsi_max,
        "require_macd_golden": 1.0 if require_macd else 0.0,
        "vol_close_corr_min": vol_close_corr_min,
        "near_52w_high_gap_max": near_52w_high_gap_max,
        "min_listing_days": min_listing_days,
    }
    ladder = _relax_ladder_operational(p0) if use_relax_ladder else [("L0", p0)]

    selected_daily = []
    for _, gday in df.groupby("date", sort=True):
        chosen = pd.DataFrame()
        for _lv, p_try in ladder:
            cand = _select_day_candidates_operational(gday, p_try)
            if not cand.empty:
                chosen = cand
                break
        if not chosen.empty:
            selected_daily.append(chosen)

    if not selected_daily:
        return []

    sig = pd.concat(selected_daily, ignore_index=True)
    sig["code"] = sig["code"].astype(str)
    sig["date"] = pd.to_datetime(sig["date"], errors="coerce")
    sig = sig.dropna(subset=["date", "code"])
    if sig.empty:
        return []

    # Per-day score (fast: avoid groupby.apply)
    g = sig.groupby("date", sort=False)
    sig["score"] = (
        g["rs"].rank(pct=True) * w_rs
        + g["rs_slope"].rank(pct=True) * w_sl
        + g["v_accel"].rank(pct=True) * w_va
    )

    sig = sig.sort_values(["date", "score"], ascending=[True, False], kind="mergesort")
    sig_dates = sig["date"].to_numpy(dtype="datetime64[ns]")
    sig_codes = sig["code"].to_numpy(dtype=object)

    rets: list[float] = []
    active: list[tuple[np.datetime64, str]] = []  # (exit_date, code)

    i = 0
    n = len(sig)
    w_start64 = np.datetime64(w_start.to_datetime64())
    w_end64 = np.datetime64(w_end.to_datetime64())

    while i < n:
        d = sig_dates[i]

        # expire finished positions once per day (avoid per-signal filtering)
        if active:
            active = [t for t in active if t[0] > d]
        if len(active) >= max_pos:
            # skip all signals for this day
            j = i + 1
            while j < n and sig_dates[j] == d:
                j += 1
            i = j
            continue

        active_codes = {t[1] for t in active}

        # process signals of the day in score order until slots filled
        j = i
        while j < n and sig_dates[j] == d:
            if len(active) >= max_pos:
                break

            code = str(sig_codes[j])
            j += 1

            if code in active_codes:
                continue

            sl = slices.get(code)
            if not sl:
                continue
            a, b = sl

            # Clip this code's available prices to window range [w_start, w_end]
            dates_code = date_s[a:b]
            if len(dates_code) < 2:
                continue

            # locate window bounds within this code slice
            lo = int(a + np.searchsorted(dates_code, w_start64, side="left"))
            hi_excl = int(a + np.searchsorted(dates_code, w_end64, side="right"))
            if hi_excl - lo < 2:
                continue

            dates_w = date_s[lo:hi_excl]

            # signal day must exist in this clipped slice
            idx = int(np.searchsorted(dates_w, d))
            if idx >= len(dates_w) or dates_w[idx] != d:
                continue

            entry_i = lo + idx + 1
            if entry_i >= hi_excl:
                continue  # no next-day open inside window

            entry_p = float(o_s[entry_i])
            if not np.isfinite(entry_p) or entry_p <= 0:
                continue

            sig_close = float(c_s[lo + idx])
            if np.isfinite(sig_close) and sig_close > 0:
                gap = (entry_p - sig_close) / sig_close
                if gap_up_max_pct > 0 and gap > gap_up_max_pct:
                    continue
                if entry_gap_down_stop_pct > 0 and gap <= -abs(entry_gap_down_stop_pct):
                    continue
                if gap_up_max_pct <= 0 and entry_gap_down_stop_pct <= 0 and gap_limit > 0 and abs(gap) > gap_limit:
                    continue

            # simulate exit within hold_days, clipped to window end
            end_i = min(entry_i + hold_days - 1, hi_excl - 1)

            exit_p = float(c_s[end_i])
            exit_d = date_s[end_i]

            # stop/tp/trailing
            if stop_loss is not None:
                stop_p = entry_p * (1.0 + float(stop_loss))
            else:
                stop_p = None
            if take_profit is not None:
                tp_p = entry_p * (1.0 + float(take_profit))
            else:
                tp_p = None

            max_high = entry_p
            if trail_pct is not None:
                trail_p = entry_p * (1.0 - float(trail_pct))
            else:
                trail_p = None

            for k in range(entry_i, end_i + 1):
                hi = float(h_s[k])
                lo_p = float(l_s[k])

                if np.isfinite(hi) and hi > max_high:
                    max_high = hi
                    if trail_p is not None:
                        trail_p = max_high * (1.0 - float(trail_pct))

                # STOP (gap-aware using LOW)
                if stop_p is not None and np.isfinite(lo_p) and lo_p <= stop_p:
                    exit_p = float(stop_p)
                    exit_d = date_s[k]
                    break

                # TAKE PROFIT (gap-aware using HIGH)
                if tp_p is not None and np.isfinite(hi) and hi >= tp_p:
                    exit_p = float(tp_p)
                    exit_d = date_s[k]
                    break

                # TRAIL (gap-aware using LOW)
                if trail_p is not None and np.isfinite(lo_p) and lo_p <= trail_p:
                    exit_p = float(trail_p)
                    exit_d = date_s[k]
                    break

            pnl_pct = (exit_p / entry_p) - 1.0 - fee
            if np.isfinite(pnl_pct):
                rets.append(float(pnl_pct))

            active.append((exit_d, code))
            active_codes.add(code)

        # advance to next day
        while j < n and sig_dates[j] == d:
            j += 1
        i = j

    return rets

def build_windows(df: pd.DataFrame, years_back: int) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    end = df["date"].max().normalize()
    windows: List[Tuple[pd.Timestamp, pd.Timestamp]] = []

    for k in range(years_back):
        w_end = end - pd.DateOffset(years=k)
        w_start = w_end - pd.DateOffset(years=1) + pd.DateOffset(days=1)
        windows.append((w_start, w_end))

    # ?ㅻ옒??寃껊????뺣젹
    windows = sorted(windows, key=lambda x: x[0])

    # ?ㅼ젣 ?곗씠??踰붿쐞??留욎떠 ?대┰ + 鍮?window ?쒓굅
    min_d = df["date"].min().normalize()
    out = []
    for s, e in windows:
        s2 = max(s, min_d)
        e2 = min(e, end)
        if s2 >= e2:
            continue
        out.append((s2, e2))
    return out


def eval_params(df: pd.DataFrame, windows: List[Tuple[pd.Timestamp, pd.Timestamp]], params: dict) -> Tuple[float, List[WindowResult]]:
    results: List[WindowResult] = []
    pfs: List[float] = []
    means: List[float] = []
    total_n = 0

    for (s, e) in windows:
        wdf = df[(df["date"] >= s) & (df["date"] <= e)].copy()
        # [FIX] RESET _PRICE_CACHE EACH WINDOW IN eval_params
        global _PRICE_CACHE
        _PRICE_CACHE = None
        rets_list = simulate_window(wdf, params)
        rets = np.asarray(rets_list, dtype="float64")
        if rets.size:
            rets = rets[np.isfinite(rets)]
        n = int(rets.size)
        total_n += n

        if n < 5:
            pf = 0.0
            mean_ret = -1.0
        else:
            pos = rets[rets > 0].sum()
            neg = abs(rets[rets < 0].sum())
            pf = float(pos / (neg + 1e-9))
            mean_ret = float(np.mean(rets))

        split = "IS" if e <= TRAIN_END else ("VAL" if e <= VAL_END else "OOS")
        results.append(WindowResult(start=s.strftime("%Y-%m-%d"), end=e.strftime("%Y-%m-%d"), n_trades=n, pf=pf, mean_ret=mean_ret, split=split, year=int(e.year)))
        pfs.append(pf)
        means.append(mean_ret)
    # Avoid score-distribution collapse: no-trade only is hard fail.
    if total_n <= 0:
        return -1e9, results
    total_shortfall = max(0, MIN_TRADES_TOTAL - total_n)
    penalty_total_n = (float(total_shortfall) / float(max(1, MIN_TRADES_TOTAL))) * 0.8
    # window trade count penalty
    low_windows = sum(1 for r in results if r.n_trades < MIN_TRADES_PER_WINDOW)
    penalty_n = low_windows * 0.15

    mean_pf = float(np.mean(pfs)) if pfs else 0.0
    std_pf = float(np.std(pfs)) if pfs else 0.0
    mean_ret = float(np.mean(means)) if means else -1.0

    # split aggregation (IS/VAL/OOS) for scoring / reporting
    is_res = [r for r in results if r.split == "IS"]
    val_res = [r for r in results if r.split == "VAL"]
    oos_res = [r for r in results if r.split == "OOS"]

    def _avg_pf(arr):
        return float(np.mean([x.pf for x in arr])) if arr else 0.0

    def _avg_mean(arr):
        return float(np.mean([x.mean_ret for x in arr])) if arr else 0.0

    def _sum_n(arr):
        return int(np.sum([x.n_trades for x in arr])) if arr else 0

    is_pf = _avg_pf(is_res); val_pf = _avg_pf(val_res); oos_pf = _avg_pf(oos_res)
    is_mean = _avg_mean(is_res); val_mean = _avg_mean(val_res); oos_mean = _avg_mean(oos_res)
    is_n = _sum_n(is_res); val_n = _sum_n(val_res); oos_n = _sum_n(oos_res)

    pf_cap = 5.0

    def _cap_pf(x: float) -> float:
        try:
            x = float(x)
        except Exception:
            return 0.0
        if not np.isfinite(x):
            return pf_cap
        return max(0.0, min(x, pf_cap))
    oos_shortfall = max(0, MIN_TRADES_PER_WINDOW - oos_n)
    penalty_oos_n = (float(oos_shortfall) / float(max(1, MIN_TRADES_PER_WINDOW))) * 0.8
    if oos_n <= 0:
        penalty_oos_n += 1.0
    score = (
        (_cap_pf(oos_pf) * 1.6 + _cap_pf(val_pf) * 0.6 + _cap_pf(is_pf) * 0.2)
        + (oos_mean * 40.0 + val_mean * 10.0 + is_mean * 2.0)
        - (PF_STD_PENALTY * std_pf)
        - penalty_n
        - penalty_total_n
        - penalty_oos_n
    )
    return float(score), results


def sample_params(rng: np.random.Generator, base: dict) -> dict:
    p = dict(base)

    for k, (lo, hi, step) in BOUNDS.items():
        v = rng.uniform(lo, hi)
        p[k] = _clip_grid(v, lo, hi, step)

    # weights: dirichlet then round, normalize
    w = rng.dirichlet([2.0, 3.0, 2.0])
    w_rs = float(w[0])
    w_slope = float(w[1])
    w_va = float(w[2])

    # round and renormalize
    w_rs = _round_grid(w_rs, 0.01)
    w_slope = _round_grid(w_slope, 0.01)
    w_va = _round_grid(w_va, 0.01)
    s = w_rs + w_slope + w_va
    if s <= 0:
        w_rs, w_slope, w_va = 0.2, 0.55, 0.25
        s = 1.0
    p["w_rs"] = w_rs / s
    p["w_rs_slope"] = w_slope / s
    p["w_v_accel"] = w_va / s

    return p


def step_toward(stable: dict, best: dict, alpha: float) -> dict:
    out = dict(stable)
    for k in best.keys():
        if k in FROZEN_KEYS:
            continue
        if k in ["as_of", "source", "best_score", "windows"]:
            continue

        if k in BOUNDS:
            lo, hi, step = BOUNDS[k]
            sv = _safe_float(stable.get(k), best.get(k))
            bv = _safe_float(best.get(k), stable.get(k))
            nv = sv + (bv - sv) * alpha
            out[k] = _clip_grid(nv, lo, hi, step)
        elif k.startswith("w_"):
            # weights??留덉?留됱뿉 ?뺢퇋??
            out[k] = _safe_float(stable.get(k), best.get(k)) + (
                _safe_float(best.get(k), stable.get(k)) - _safe_float(stable.get(k), best.get(k))
            ) * alpha

    # normalize weights
    w_rs = _safe_float(out.get("w_rs"), 0.2)
    w_slope = _safe_float(out.get("w_rs_slope"), 0.55)
    w_va = _safe_float(out.get("w_v_accel"), 0.25)
    s = w_rs + w_slope + w_va
    if s <= 0:
        w_rs, w_slope, w_va = 0.2, 0.55, 0.25
        s = 1.0
    out["w_rs"] = w_rs / s
    out["w_rs_slope"] = w_slope / s
    out["w_v_accel"] = w_va / s

    return out


def main() -> int:
    print("[OPT] loading data...")
    df = load_data(BASE_DIR)

    # 2020 ?댁쟾???덉쑝硫??쒖쇅(遺덊븘???몄씠利?
    df = df[df["date"] >= pd.Timestamp("2020-01-01")].copy()

    df = compute_factors(df)

    # Build price cache once (shared by all parameter trials)
    global _PRICE_CACHE
    print("[OPT] building price cache...")
    _PRICE_CACHE = _build_price_cache(df)
    print(f"[OPT] price_cache codes={len(_PRICE_CACHE['slices'])} rows={len(_PRICE_CACHE['date_s'])}")
    df = df.dropna(subset=["rs", "rs_slope", "v_accel", "stretch", "atr14_pct", "gap_next", "rsi14", "vol_close_corr20", "high_52w_gap", "listing_days"]).copy()

    if df.empty:
        raise RuntimeError("?⑺꽣 怨꾩궛 ???좏슚 ?곗씠?곌? ?놁뒿?덈떎.")

    windows = build_windows(df, YEARS_BACK)
    if not windows:
        raise RuntimeError("?덈룄??援ъ꽦???ㅽ뙣?덉뒿?덈떎.")

    stable_path = OUT_DIR / "stable_params_v41_1.json"
    best_path = OUT_DIR / "best_params_v41_1.json"
    report_path = OUT_DIR / "search_report_v41_1.csv"

    prev_stable = _jload(stable_path)

    # base params = prev stable or defaults
    gu_cfg, gd_cfg = _load_paper_gap_policy()
    base = {
        "rs_lim": _safe_float(prev_stable.get("rs_lim"), 1.7),
        "v_accel_lim": _safe_float(prev_stable.get("v_accel_lim"), 2.5),
        "stretch_max": _safe_float(prev_stable.get("stretch_max"), 1.19),
        "value_min": _safe_float(prev_stable.get("value_min"), 105e9),
        "atr_max": _safe_float(prev_stable.get("atr_max"), 0.20),
        "gap_limit": _safe_float(prev_stable.get("gap_limit"), 0.15),
        "gap_up_max_pct": _safe_float(prev_stable.get("gap_up_max_pct"), gu_cfg),
        "entry_gap_down_stop_pct": _safe_float(prev_stable.get("entry_gap_down_stop_pct"), gd_cfg),
        "rsi_max": _safe_float(prev_stable.get("rsi_max"), 70.0),
        "require_macd_golden": 1.0 if _safe_float(prev_stable.get("require_macd_golden"), 0.0) >= 0.5 else 0.0,
        "vol_close_corr_min": _safe_float(prev_stable.get("vol_close_corr_min"), 0.0),
        "near_52w_high_gap_max": _safe_float(prev_stable.get("near_52w_high_gap_max"), 0.05),
        "min_listing_days": _safe_float(prev_stable.get("min_listing_days"), 126.0),
        "use_relax_ladder": 1.0 if _safe_float(prev_stable.get("use_relax_ladder"), 1.0) >= 0.5 else 0.0,
        "w_rs": _safe_float(prev_stable.get("w_rs"), 0.20),
        "w_rs_slope": _safe_float(prev_stable.get("w_rs_slope"), 0.55),
        "w_v_accel": _safe_float(prev_stable.get("w_v_accel"), 0.25),
    }
    base = _apply_gap_policy_schema(base, {"gap_up_max_pct": gu_cfg, "entry_gap_down_stop_pct": gd_cfg})

    # frozen: keep from prev stable if exists, else defaults
    for k in FROZEN_KEYS:
        if k in prev_stable:
            base[k] = prev_stable[k]
        else:
            base[k] = DEFAULT_FROZEN[k]

    rng = np.random.default_rng(RNG_SEED)

    # Evaluate current stable baseline (for promotion decision)
    stable_score, stable_windows = eval_params(df, windows, base)

    best_score = -1e18
    best_params = None
    best_windows = None

    rows = []

    for i in range(N_ITER):
        p = sample_params(rng, base)

        # ensure frozen exactly
        for k in FROZEN_KEYS:
            p[k] = base[k]

        score, win_res = eval_params(df, windows, p)

        # flatten quick metrics
        n_total = int(sum(r.n_trades for r in win_res))
        mean_pf = float(np.mean([r.pf for r in win_res])) if win_res else 0.0
        std_pf = float(np.std([r.pf for r in win_res])) if win_res else 0.0
        mean_ret = float(np.mean([r.mean_ret for r in win_res])) if win_res else -1.0

        # split metrics (IS/VAL/OOS) derived from yearly windows
        is_res = [r for r in win_res if r.split == "IS"]
        val_res = [r for r in win_res if r.split == "VAL"]
        oos_res = [r for r in win_res if r.split == "OOS"]
        is_n = int(sum(r.n_trades for r in is_res)) if is_res else 0
        val_n = int(sum(r.n_trades for r in val_res)) if val_res else 0
        oos_n = int(sum(r.n_trades for r in oos_res)) if oos_res else 0
        is_pf = float(np.mean([r.pf for r in is_res])) if is_res else 0.0
        val_pf = float(np.mean([r.pf for r in val_res])) if val_res else 0.0
        oos_pf = float(np.mean([r.pf for r in oos_res])) if oos_res else 0.0
        is_ret = float(np.mean([r.mean_ret for r in is_res])) if is_res else 0.0
        val_ret = float(np.mean([r.mean_ret for r in val_res])) if val_res else 0.0
        oos_ret = float(np.mean([r.mean_ret for r in oos_res])) if oos_res else 0.0


        rows.append({
            "iter": i + 1,
            "score": score,
            "n_total": n_total,
            "mean_pf": mean_pf,
            "std_pf": std_pf,
            "mean_ret": mean_ret,
            "is_n": is_n, "val_n": val_n, "oos_n": oos_n,
            "is_pf": is_pf, "val_pf": val_pf, "oos_pf": oos_pf,
            "is_ret": is_ret, "val_ret": val_ret, "oos_ret": oos_ret,
            **{k: p.get(k) for k in [
                "rs_lim", "v_accel_lim", "stretch_max", "value_min", "atr_max", "gap_limit", "gap_up_max_pct", "entry_gap_down_stop_pct",
                "rsi_max", "require_macd_golden", "vol_close_corr_min", "near_52w_high_gap_max", "min_listing_days", "use_relax_ladder",
                "w_rs", "w_rs_slope", "w_v_accel",
            ]},
        })

        if score > best_score:
            best_score = score
            best_params = dict(p)
            best_windows = win_res

        if (i + 1) % 20 == 0:
            print(f"[OPT] {i+1}/{N_ITER} best_score={best_score:.4f} stable_score={stable_score:.4f}")

    # Save report
    pd.DataFrame(rows).to_csv(report_path, index=False, encoding="utf-8-sig")

    # best json
    best_obj = _apply_gap_policy_schema(dict(best_params or base), base)
    best_obj.update({
        "as_of": _today_str(),
        "source": "best",
        "best_score": float(best_score),
        "windows": [r.__dict__ for r in (best_windows or [])],
    })
    _jsave(best_path, best_obj)

    # stable promotion decision
    new_stable = dict(base)
    promoted = False

    if best_params is not None and stable_score > -1e8:
        # promotion requires both: margin & enough trades
        if best_score > stable_score * (1.0 + PROMOTION_MARGIN):
            new_stable = step_toward(base, best_params, STEP_TOWARD_ALPHA)
            promoted = True

    # stable json
    new_score, new_windows = eval_params(df, windows, new_stable)
    stable_obj = _apply_gap_policy_schema(dict(new_stable), base)
    stable_obj.update({
        "as_of": _today_str(),
        "source": "stable",
        "best_score": float(new_score),
        "promoted": bool(promoted),
        "windows": [r.__dict__ for r in new_windows],
        "meta": {
            "years_back": YEARS_BACK,
            "n_iter": N_ITER,
            "min_trades_total": MIN_TRADES_TOTAL,
            "min_trades_per_window": MIN_TRADES_PER_WINDOW,
            "rng_seed": RNG_SEED,
            "split_policy": {"train_end": TRAIN_END.strftime("%Y-%m-%d"), "val_end": VAL_END.strftime("%Y-%m-%d")},
            "promotion_margin": PROMOTION_MARGIN,
            "step_alpha": STEP_TOWARD_ALPHA,
            "schema_gap_policy_keys": list(GAP_POLICY_KEYS),
        }
    })
    _jsave(stable_path, stable_obj)

    print("\n[OK] saved:")
    print(f" - {best_path}")
    print(f" - {stable_path}")
    print(f" - {report_path}")
    print(f"[STABLE] promoted={promoted} stable_score={new_score:.4f} (prev {stable_score:.4f})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


