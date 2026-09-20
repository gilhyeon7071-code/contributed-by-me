from __future__ import annotations

import json
import math
import os
import pickle
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PRICES_PATH = ROOT / "paper" / "prices" / "ohlcv_paper.parquet"
MODEL_PATH = ROOT / "_cache" / "surge_ml_model.pkl"
SURGE_PARAMS_PATH = ROOT / "paper" / "surge_params.json"
CONFIG_PATH = ROOT / "paper" / "paper_engine_config.json"
OUT_JSON = ROOT / "2_Logs" / "surge_reversal_compare_latest.json"
OUT_CSV = ROOT / "2_Logs" / "surge_reversal_compare_trades_latest.csv"
OUT_SENS_JSON = ROOT / "2_Logs" / "surge_reversal_sensitivity_latest.json"
DEFAULT_EVAL_START_YMD = "20260301"

DEFAULT_CANDIDATE_CFG = {
    "name": "A",
    "description": "baseline + exit next open when 2+ reversal signals fire",
    "signals": [
        "VOLUME_EXHAUSTION",
        "HIGH_REJECTION",
        "RSI_BREAKDOWN",
        "BOLLINGER_REENTRY",
    ],
    "trigger_count": 2,
    "volume_exhaustion_ratio_max": 0.5,
    "high_rejection_min_drawdown_pct": 0.0,
}


def _to_float_env(name: str, default: float) -> float:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _safe_ratio(num: float, den: float) -> float:
    if den <= 0:
        return 0.0
    return float(num) / float(den)


def _pct01(v: Any, default: float) -> float:
    try:
        x = float(v)
    except Exception:
        x = float(default)
    if abs(x) > 1.0:
        x = x / 100.0
    return float(x)


def _load_json(path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
    if not path.exists():
        return dict(default)
    try:
        obj = json.loads(path.read_text(encoding="utf-8-sig"))
        return obj if isinstance(obj, dict) else dict(default)
    except Exception:
        return dict(default)


def _calc_rsi(closes: Iterable[float], period: int = 14) -> float:
    seq = [float(x) for x in closes]
    if len(seq) < period + 1:
        return 50.0
    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, len(seq)):
        diff = seq[i] - seq[i - 1]
        if diff > 0:
            gains.append(diff)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(diff))
    avg_gain = sum(gains[-period:]) / float(period)
    avg_loss = sum(losses[-period:]) / float(period)
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100.0 - (100.0 / (1.0 + rs)))


def _bollinger_upper(closes: List[float], window: int = 20, num_std: float = 2.0) -> Optional[float]:
    if len(closes) < window:
        return None
    s = pd.Series(closes[-window:], dtype="float64")
    ma = float(s.mean())
    std = float(s.std(ddof=1))
    return ma + num_std * std


def _load_surge_params() -> Dict[str, Any]:
    p = _load_json(SURGE_PARAMS_PATH, {})
    return {
        "pct_min": float(p.get("pct_min", 0.07)),
        "rvol20_min": float(p.get("rvol20_min", 2.0)),
        "range_min": float(p.get("range_min", 0.08)),
        "bt_ml_prob_min": float(p.get("bt_ml_prob_min", 0.40)),
        "bt_ml_topq": float(p.get("bt_ml_topq", 0.90)),
    }


def _load_config() -> Dict[str, Any]:
    cfg = _load_json(CONFIG_PATH, {})
    return cfg if isinstance(cfg, dict) else {}


def _prepare_price_features() -> pd.DataFrame:
    px = pd.read_parquet(PRICES_PATH).copy()
    px["date"] = px["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str.slice(0, 8)
    px["code"] = px["code"].astype(str).str.zfill(6)
    for c in ["open", "high", "low", "close", "volume"]:
        px[c] = pd.to_numeric(px[c], errors="coerce")
    px = px.dropna(subset=["open", "high", "low", "close", "volume"]).sort_values(["code", "date"]).copy()

    g = px.groupby("code", group_keys=False)
    px["ret1"] = g["close"].pct_change(1)
    px["ret3"] = g["close"].pct_change(3)
    px["ret5"] = g["close"].pct_change(5)
    px["ret10"] = g["close"].pct_change(10)
    px["range_pct"] = (px["high"] - px["low"]) / px["close"].replace(0, pd.NA)
    px["body_pct"] = (px["close"] - px["open"]) / px["open"].replace(0, pd.NA)
    px["upper_shadow_pct"] = (px["high"] - np.maximum(px["open"], px["close"])) / px["open"].replace(0, pd.NA)
    px["vol_ma5"] = g["volume"].rolling(5, min_periods=3).mean().reset_index(level=0, drop=True)
    px["vol_ma20"] = g["volume"].rolling(20, min_periods=5).mean().reset_index(level=0, drop=True)
    px["vol_ratio5"] = px["volume"] / px["vol_ma5"].replace(0, pd.NA)
    px["vol_ratio20"] = px["volume"] / px["vol_ma20"].replace(0, pd.NA)
    prev_close = g["close"].shift(1)
    tr1 = (px["high"] - px["low"]).abs()
    tr2 = (px["high"] - prev_close).abs()
    tr3 = (px["low"] - prev_close).abs()
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    px["atr14"] = true_range.groupby(px["code"]).rolling(14, min_periods=5).mean().reset_index(level=0, drop=True)
    px["atr14_pct"] = px["atr14"] / px["close"].replace(0, pd.NA)
    px["high_20d"] = g["high"].rolling(20, min_periods=5).max().reset_index(level=0, drop=True)
    px["close_to_high_20d"] = px["close"] / px["high_20d"].replace(0, pd.NA)
    return px


def _generate_entry_signals(px: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    data = px.dropna(subset=["ret1", "ret5", "range_pct", "vol_ratio20"]).copy()
    data["sig_rule_only"] = (
        (data["ret1"] >= float(params["pct_min"]))
        & ((data["vol_ratio20"] >= float(params["rvol20_min"])) | (data["range_pct"] >= float(params["range_min"])))
    ).astype(int)
    data["sig_rule_ml"] = 0

    if not MODEL_PATH.exists():
        return data

    with MODEL_PATH.open("rb") as f:
        bundle = pickle.load(f)
    model_type = str(bundle.get("model_type", ""))
    model = bundle.get("model")
    features = list(bundle.get("features") or [])
    x = pd.DataFrame(
        {
            "ret1": data["ret1"],
            "ret3": data["ret3"],
            "ret5": data["ret5"],
            "ret10": data["ret10"],
            "range_pct": data["range_pct"],
            "body_pct": data["body_pct"],
            "upper_shadow_pct": data["upper_shadow_pct"],
            "vol_ratio5": data["vol_ratio5"],
            "vol_ratio20": data["vol_ratio20"],
            "atr14_pct": data["atr14_pct"],
            "close_to_high_20d": data["close_to_high_20d"],
        }
    ).reindex(data.index)
    for feat in features:
        if feat not in x.columns:
            x[feat] = 0.0
    x = x[features].apply(pd.to_numeric, errors="coerce").fillna(0.0).astype(float)

    if model_type == "linear_stat":
        coef_map = bundle.get("coef") if isinstance(bundle.get("coef"), dict) else {}
        coef = pd.Series({k: float(coef_map.get(k, 0.0)) for k in features}, dtype=float)
        intercept = float(bundle.get("intercept", 0.0) or 0.0)
        raw = x.mul(coef, axis=1).sum(axis=1) + intercept
        prob = (1.0 / (1.0 + np.exp(-np.clip(raw.to_numpy(dtype=float), -50.0, 50.0)))).astype(float)
    elif model_type == "linear_logreg_fallback":
        coef_map = bundle.get("coef") if isinstance(bundle.get("coef"), dict) else {}
        mean_map = bundle.get("feature_mean") if isinstance(bundle.get("feature_mean"), dict) else {}
        std_map = bundle.get("feature_std") if isinstance(bundle.get("feature_std"), dict) else {}
        coef = pd.Series({k: float(coef_map.get(k, 0.0)) for k in features}, dtype=float)
        f_mean = pd.Series({k: float(mean_map.get(k, 0.0)) for k in features}, dtype=float)
        f_std = pd.Series({k: float(std_map.get(k, 1.0)) for k in features}, dtype=float).replace(0.0, 1.0)
        intercept = float(bundle.get("intercept", 0.0) or 0.0)
        x_norm = (x - f_mean) / f_std
        raw = x_norm.mul(coef, axis=1).sum(axis=1) + intercept
        prob = (1.0 / (1.0 + np.exp(-np.clip(raw.to_numpy(dtype=float), -50.0, 50.0)))).astype(float)
    else:
        prob = model.predict_proba(x)[:, 1]

    data["surge_ml_prob"] = prob
    thr = _to_float_env("SURGE_BT_ML_PROB_MIN", float(params["bt_ml_prob_min"]))
    top_q = _to_float_env("SURGE_BT_ML_TOPQ", float(params["bt_ml_topq"]))
    data["sig_rule_ml"] = ((data["sig_rule_only"] == 1) & (data["surge_ml_prob"] >= thr)).astype(int)
    if int(data["sig_rule_ml"].sum()) == 0:
        cand = data[data["sig_rule_only"] == 1].copy()
        if len(cand) > 0:
            qv = float(cand["surge_ml_prob"].quantile(max(0.5, min(0.99, top_q))))
            data["sig_rule_ml"] = ((data["sig_rule_only"] == 1) & (data["surge_ml_prob"] >= qv)).astype(int)
    return data


def _resolve_dynamic_stop_loss_pct(
    base_stop_pct: float,
    current_profit_pct: float,
    hold_days_trading: int,
    atr14_pct: float,
    cfg: Dict[str, Any],
) -> float:
    dcfg = cfg.get("dynamic_stop_loss", {}) if isinstance(cfg.get("dynamic_stop_loss"), dict) else {}
    if not bool(dcfg.get("enabled", True)):
        return float(base_stop_pct)
    eff = float(base_stop_pct)
    be_trigger_pct = float(dcfg.get("break_even_trigger_profit_pct", 4.0) or 4.0)
    be_stop_pct = _pct01(dcfg.get("break_even_stop_pct", -0.002), -0.002)
    if float(current_profit_pct) >= be_trigger_pct:
        eff = max(eff, be_stop_pct)
    tiers = dcfg.get("profit_lock_tiers", [])
    if isinstance(tiers, list):
        for t in tiers:
            if not isinstance(t, dict):
                continue
            trig = float(t.get("trigger_profit_pct", 0.0) or 0.0)
            stop_t = _pct01(t.get("stop_pct", eff), eff)
            if float(current_profit_pct) >= trig:
                eff = max(eff, stop_t)
    decay_start = int(dcfg.get("time_decay_start_days", 3) or 3)
    decay_step = _pct01(dcfg.get("time_decay_tighten_step_pct", 0.003), 0.003)
    decay_cap = _pct01(dcfg.get("time_decay_tighten_cap_pct", 0.03), 0.03)
    if hold_days_trading >= decay_start and decay_step > 0:
        tighten = min(decay_cap, (hold_days_trading - decay_start + 1) * decay_step)
        eff = max(eff, float(base_stop_pct) + float(tighten))
    atr_ref = _pct01(dcfg.get("atr_ref_pct", 0.04), 0.04)
    atr_scale = float(dcfg.get("atr_tighten_scale", 0.50) or 0.50)
    atr_cap = _pct01(dcfg.get("atr_tighten_cap_pct", 0.03), 0.03)
    if float(atr14_pct or 0.0) > atr_ref and atr_scale > 0:
        atr_tighten = min(atr_cap, (float(atr14_pct) - atr_ref) * atr_scale)
        eff = max(eff, float(base_stop_pct) + float(atr_tighten))
    min_stop = _pct01(dcfg.get("min_stop_pct", -0.30), -0.30)
    max_stop = _pct01(dcfg.get("max_stop_pct", -0.001), -0.001)
    eff = min(max(eff, min_stop), max_stop)
    return float(eff)


@dataclass
class ExitResult:
    exit_date: str
    exit_price: float
    ret: float
    reason: str
    reversal_count: int = 0


def _calc_reversal_signals(
    close_hist: List[float],
    volume_hist: List[float],
    surge_volume: float,
    high_since_entry: float,
    candidate_cfg: Dict[str, Any],
) -> List[str]:
    signals: List[str] = []
    volume_ratio_max = float(candidate_cfg.get("volume_exhaustion_ratio_max", 0.5) or 0.5)
    high_rej_min_dd = _pct01(candidate_cfg.get("high_rejection_min_drawdown_pct", 0.0), 0.0)
    if len(volume_hist) >= 4 and surge_volume > 0:
        post_avg = float(pd.Series(volume_hist[1:][-3:], dtype="float64").mean())
        if post_avg / surge_volume < volume_ratio_max:
            signals.append("VOLUME_EXHAUSTION")
    if len(close_hist) >= 4:
        recent = pd.Series(close_hist[-3:], dtype="float64")
        drawdown = (float(recent.iloc[-1]) / float(high_since_entry)) - 1.0 if float(high_since_entry) > 0 else 0.0
        if (
            bool((recent.diff().dropna() < 0).all())
            and float(recent.iloc[-1]) < float(high_since_entry)
            and drawdown <= -abs(high_rej_min_dd)
        ):
            signals.append("HIGH_REJECTION")
    if len(close_hist) >= 16:
        rsi_prev = _calc_rsi(close_hist[:-1], 14)
        rsi_curr = _calc_rsi(close_hist, 14)
        if rsi_prev >= 70.0 and rsi_curr < 70.0:
            signals.append("RSI_BREAKDOWN")
    if len(close_hist) >= 21:
        upper_prev = _bollinger_upper(close_hist[:-1], 20, 2.0)
        upper_curr = _bollinger_upper(close_hist, 20, 2.0)
        if upper_prev is not None and upper_curr is not None:
            if float(close_hist[-2]) > float(upper_prev) and float(close_hist[-1]) <= float(upper_curr):
                signals.append("BOLLINGER_REENTRY")
    return signals


def _simulate_trade(
    g: pd.DataFrame,
    entry_idx: int,
    cfg: Dict[str, Any],
    mode: str,
    candidate_cfg: Optional[Dict[str, Any]] = None,
) -> Optional[ExitResult]:
    if entry_idx >= len(g) - 1:
        return None
    row = g.iloc[entry_idx]
    entry_date = str(row["date"])
    entry_price = float(row["close"])
    if entry_price <= 0:
        return None

    candidate_cfg = dict(DEFAULT_CANDIDATE_CFG if candidate_cfg is None else candidate_cfg)
    surge_cfg = cfg.get("surge_exit_policy", {}) if isinstance(cfg.get("surge_exit_policy"), dict) else {}
    sell_rules = cfg.get("sell_rules", {}) if isinstance(cfg.get("sell_rules"), dict) else {}
    stop_cfg = sell_rules.get("stop_loss", {}) if isinstance(sell_rules.get("stop_loss"), dict) else {}
    max_hold_days = int(surge_cfg.get("max_hold_days", 5) or 5)
    base_stop_pct = max(_pct01(surge_cfg.get("stop_loss_pct", -0.05), -0.05), -1.0)
    tp_pct = _pct01(surge_cfg.get("take_profit_pct", 0.10), 0.10)
    trail_pct = _pct01(stop_cfg.get("trailing_stop_pct", -10), -10)
    trail_activation_pct = _pct01(stop_cfg.get("trailing_stop_activation_profit_pct", 12), 12)
    preemptive_pct = _pct01(stop_cfg.get("preemptive_close_pct", -3.5), -3.5)
    min_hold_days = int(cfg.get("min_hold_days", 2) or 2)
    min_hold_protect_stop_loss = bool(cfg.get("min_hold_protect_stop_loss", False))

    max_close = entry_price
    high_since_entry = entry_price
    close_hist = [entry_price]
    volume_hist = [float(row.get("volume", 0.0) or 0.0)]
    surge_volume = float(row.get("volume", 0.0) or 0.0)
    pending_reversal_exit = False
    pending_reversal_count = 0

    last_day = min(len(g) - 1, entry_idx + max_hold_days)
    for pos in range(entry_idx + 1, last_day + 1):
        day = g.iloc[pos]
        d = str(day["date"])
        o = float(day["open"])
        h = float(day["high"])
        l = float(day["low"])
        c = float(day["close"])
        vol = float(day.get("volume", 0.0) or 0.0)
        _atr14_raw = pd.to_numeric(day.get("atr14_pct", 0.0), errors="coerce")
        atr14_pct = 0.0 if pd.isna(_atr14_raw) else float(_atr14_raw)
        hold_days = pos - entry_idx

        if pending_reversal_exit and o > 0:
            return ExitResult(d, o, (o / entry_price) - 1.0, "REVERSAL_NEXT_OPEN", pending_reversal_count)

        current_profit_pct = ((c - entry_price) / entry_price) * 100.0 if entry_price > 0 else 0.0
        dynamic_stop = _resolve_dynamic_stop_loss_pct(base_stop_pct, current_profit_pct, hold_days, atr14_pct, cfg)
        stop_price = entry_price * (1.0 + dynamic_stop)
        trail_price = None
        peak_profit_pct = ((max_close - entry_price) / entry_price) if entry_price > 0 else 0.0
        if peak_profit_pct >= trail_activation_pct:
            trail_price = max_close * (1.0 + trail_pct)

        in_protected = bool(min_hold_days > 0 and hold_days <= min_hold_days)
        protect_stop = bool(in_protected and min_hold_protect_stop_loss)
        trail_is_binding = trail_price is not None and float(trail_price) > float(stop_price)

        if not protect_stop and trail_is_binding:
            if o <= float(trail_price):
                return ExitResult(d, o, (o / entry_price) - 1.0, "TRAIL_GAP")
            if l <= float(trail_price):
                return ExitResult(d, float(trail_price), (float(trail_price) / entry_price) - 1.0, "TRAIL")
            if o <= stop_price:
                return ExitResult(d, o, (o / entry_price) - 1.0, "STOP_GAP")
            if l <= stop_price:
                return ExitResult(d, stop_price, (stop_price / entry_price) - 1.0, "STOP")
        elif not protect_stop:
            if o <= stop_price:
                return ExitResult(d, o, (o / entry_price) - 1.0, "STOP_GAP")
            if l <= stop_price:
                return ExitResult(d, stop_price, (stop_price / entry_price) - 1.0, "STOP")
            if trail_price is not None:
                if o <= float(trail_price):
                    return ExitResult(d, o, (o / entry_price) - 1.0, "TRAIL_GAP")
                if l <= float(trail_price):
                    return ExitResult(d, float(trail_price), (float(trail_price) / entry_price) - 1.0, "TRAIL")

        if (not protect_stop) and c <= entry_price * (1.0 + preemptive_pct):
            return ExitResult(d, c, (c / entry_price) - 1.0, "STOP_PREEMPTIVE_CLOSE")

        if (not in_protected) and tp_pct > 0:
            tp_price = entry_price * (1.0 + tp_pct)
            if o >= tp_price:
                return ExitResult(d, o, (o / entry_price) - 1.0, "SURGE_TP_GAP")
            if h >= tp_price:
                return ExitResult(d, tp_price, (tp_price / entry_price) - 1.0, "SURGE_TP")

        high_since_entry = max(high_since_entry, h, c)
        close_hist.append(c)
        volume_hist.append(vol)
        max_close = max(max_close, c)

        if mode == "candidate_a":
            reversal_signals = _calc_reversal_signals(close_hist, volume_hist, surge_volume, high_since_entry, candidate_cfg)
            trigger_count = int(candidate_cfg.get("trigger_count", 2) or 2)
            if len(reversal_signals) >= trigger_count:
                pending_reversal_exit = True
                pending_reversal_count = len(reversal_signals)

        if hold_days >= max_hold_days:
            return ExitResult(d, c, (c / entry_price) - 1.0, "TIME")

    end_row = g.iloc[last_day]
    return ExitResult(str(end_row["date"]), float(end_row["close"]), (float(end_row["close"]) / entry_price) - 1.0, "TIME")


def _summarize(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {
            "trades": 0,
            "win_rate": 0.0,
            "avg_ret": 0.0,
            "median_ret": 0.0,
            "cumulative_return": 0.0,
            "max_drawdown": 0.0,
            "exit_reasons": {},
        }
    rets = pd.to_numeric(df["ret"], errors="coerce").fillna(0.0)
    eq = (1.0 + rets).cumprod()
    dd = eq / eq.cummax() - 1.0
    reasons = Counter(df["exit_reason"].astype(str).tolist())
    return {
        "trades": int(len(df)),
        "win_rate": float((rets > 0).mean()),
        "avg_ret": float(rets.mean()),
        "median_ret": float(rets.median()),
        "cumulative_return": float(eq.iloc[-1] - 1.0),
        "max_drawdown": float(dd.min()),
        "exit_reasons": dict(reasons),
    }


def _delta_summary(base_summary: Dict[str, Any], cand_summary: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "trades": int(cand_summary["trades"] - base_summary["trades"]),
        "win_rate": float(cand_summary["win_rate"] - base_summary["win_rate"]),
        "avg_ret": float(cand_summary["avg_ret"] - base_summary["avg_ret"]),
        "median_ret": float(cand_summary["median_ret"] - base_summary["median_ret"]),
        "cumulative_return": float(cand_summary["cumulative_return"] - base_summary["cumulative_return"]),
        "max_drawdown": float(cand_summary["max_drawdown"] - base_summary["max_drawdown"]),
    }


def _contribution_summary(base_df: pd.DataFrame, cand_df: pd.DataFrame) -> Dict[str, Any]:
    if base_df.empty or cand_df.empty:
        return {
            "reversal_exit_count": 0,
            "avg_ret_delta_on_reversal_exits": 0.0,
            "better_than_baseline_count": 0,
            "worse_than_baseline_count": 0,
            "same_as_baseline_count": 0,
        }
    merged = cand_df.merge(
        base_df[["code", "entry_date", "ret", "exit_reason"]],
        on=["code", "entry_date"],
        how="left",
        suffixes=("_cand", "_base"),
    )
    rev = merged[merged["exit_reason_cand"] == "REVERSAL_NEXT_OPEN"].copy()
    if rev.empty:
        return {
            "reversal_exit_count": 0,
            "avg_ret_delta_on_reversal_exits": 0.0,
            "better_than_baseline_count": 0,
            "worse_than_baseline_count": 0,
            "same_as_baseline_count": 0,
        }
    rev["ret_delta"] = pd.to_numeric(rev["ret_cand"], errors="coerce").fillna(0.0) - pd.to_numeric(rev["ret_base"], errors="coerce").fillna(0.0)
    better = int((rev["ret_delta"] > 0).sum())
    worse = int((rev["ret_delta"] < 0).sum())
    same = int((rev["ret_delta"] == 0).sum())
    return {
        "reversal_exit_count": int(len(rev)),
        "avg_ret_delta_on_reversal_exits": float(rev["ret_delta"].mean()),
        "better_than_baseline_count": better,
        "worse_than_baseline_count": worse,
        "same_as_baseline_count": same,
    }


def _run_single_compare(
    px: pd.DataFrame,
    entries: pd.DataFrame,
    cfg: Dict[str, Any],
    candidate_cfg: Dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any], Dict[str, Any]]:
    baseline_rows: List[Dict[str, Any]] = []
    candidate_rows: List[Dict[str, Any]] = []
    for code, grp in px.groupby("code", sort=False):
        g = grp.reset_index(drop=True)
        entry_dates = set(entries.loc[entries["code"] == code, "date"].astype(str).tolist())
        if not entry_dates:
            continue
        for idx, row in g.iterrows():
            d = str(row["date"])
            if d not in entry_dates:
                continue
            base_res = _simulate_trade(g, idx, cfg, "baseline", candidate_cfg)
            cand_res = _simulate_trade(g, idx, cfg, "candidate_a", candidate_cfg)
            if base_res is None or cand_res is None:
                continue
            common = {
                "code": str(code),
                "entry_date": d,
                "entry_price": float(row["close"]),
            }
            baseline_rows.append(
                {
                    **common,
                    "policy": "baseline",
                    "exit_date": base_res.exit_date,
                    "exit_price": base_res.exit_price,
                    "ret": base_res.ret,
                    "exit_reason": base_res.reason,
                    "reversal_count": base_res.reversal_count,
                }
            )
            candidate_rows.append(
                {
                    **common,
                    "policy": "candidate_a",
                    "exit_date": cand_res.exit_date,
                    "exit_price": cand_res.exit_price,
                    "ret": cand_res.ret,
                    "exit_reason": cand_res.reason,
                    "reversal_count": cand_res.reversal_count,
                }
            )
    base_df = pd.DataFrame(baseline_rows)
    cand_df = pd.DataFrame(candidate_rows)
    return base_df, cand_df, _summarize(base_df), _summarize(cand_df)


def _run_sensitivity(px: pd.DataFrame, entries: pd.DataFrame, cfg: Dict[str, Any], baseline_summary: Dict[str, Any]) -> Dict[str, Any]:
    cases: List[Dict[str, Any]] = []
    for trigger_count in [1, 2, 3]:
        for volume_ratio_max in [0.4, 0.5, 0.6]:
            for high_rejection_min_drawdown_pct in [0.0, 0.02, 0.04]:
                candidate_cfg = {
                    **DEFAULT_CANDIDATE_CFG,
                    "trigger_count": trigger_count,
                    "volume_exhaustion_ratio_max": volume_ratio_max,
                    "high_rejection_min_drawdown_pct": high_rejection_min_drawdown_pct,
                }
                _, cand_df, _, cand_summary = _run_single_compare(px, entries, cfg, candidate_cfg)
                cases.append(
                    {
                        "trigger_count": trigger_count,
                        "volume_exhaustion_ratio_max": volume_ratio_max,
                        "high_rejection_min_drawdown_pct": high_rejection_min_drawdown_pct,
                        "summary": cand_summary,
                        "delta_vs_baseline": _delta_summary(baseline_summary, cand_summary),
                        "reversal_exit_count": int((cand_df["exit_reason"] == "REVERSAL_NEXT_OPEN").sum()) if not cand_df.empty else 0,
                    }
                )
    best_avg_ret = max(cases, key=lambda x: (x["delta_vs_baseline"]["avg_ret"], x["delta_vs_baseline"]["win_rate"]))
    best_win_rate = max(cases, key=lambda x: (x["delta_vs_baseline"]["win_rate"], x["delta_vs_baseline"]["avg_ret"]))
    out = {
        "case_count": int(len(cases)),
        "best_avg_ret_case": best_avg_ret,
        "best_win_rate_case": best_win_rate,
        "cases": cases,
    }
    OUT_SENS_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def run_compare() -> Dict[str, Any]:
    params = _load_surge_params()
    cfg = _load_config()
    px = _prepare_price_features()
    sig = _generate_entry_signals(px, params)
    entries = sig[sig["sig_rule_ml"] == 1].copy()
    eval_start_ymd = str(os.getenv("SURGE_EVAL_START_YMD", DEFAULT_EVAL_START_YMD) or DEFAULT_EVAL_START_YMD).strip()
    if eval_start_ymd:
        entries = entries[entries["date"].astype(str) >= eval_start_ymd].copy()
    eval_dates = sorted(entries["date"].astype(str).unique().tolist())
    candidate_cfg = dict(DEFAULT_CANDIDATE_CFG)
    base_df, cand_df, base_summary, cand_summary = _run_single_compare(px, entries, cfg, candidate_cfg)
    all_df = pd.concat([base_df, cand_df], ignore_index=True) if len(base_df) or len(cand_df) else pd.DataFrame()
    all_df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    delta = _delta_summary(base_summary, cand_summary)
    contribution = _contribution_summary(base_df, cand_df)
    sensitivity = _run_sensitivity(px, entries, cfg, base_summary)

    out = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "entry_policy": "current_rule_plus_ml",
        "eval_start_ymd": eval_start_ymd,
        "eval_end_ymd": (str(eval_dates[-1]) if eval_dates else ""),
        "entry_count": int(len(entries)),
        "candidate_policy": candidate_cfg,
        "baseline": base_summary,
        "candidate_a": cand_summary,
        "delta_candidate_minus_baseline": delta,
        "contribution": contribution,
        "sensitivity": {
            "out_json": str(OUT_SENS_JSON),
            "case_count": int(sensitivity["case_count"]),
            "best_avg_ret_case": sensitivity["best_avg_ret_case"],
            "best_win_rate_case": sensitivity["best_win_rate_case"],
        },
        "files": {
            "out_csv": str(OUT_CSV),
            "sensitivity_out_json": str(OUT_SENS_JSON),
            "prices": str(PRICES_PATH),
            "config": str(CONFIG_PATH),
            "surge_params": str(SURGE_PARAMS_PATH),
        },
    }
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def main() -> int:
    if not PRICES_PATH.exists():
        OUT_JSON.write_text(
            json.dumps({"generated_at": datetime.now().isoformat(timespec="seconds"), "status": "MISSING_PRICES"}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return 1
    run_compare()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
