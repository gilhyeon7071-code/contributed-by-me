from __future__ import annotations

import json
import os
import pickle
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PRICES_PATH = ROOT / "paper" / "prices" / "ohlcv_paper.parquet"
MODEL_PATH = ROOT / "_cache" / "surge_ml_model.pkl"
PARAMS_PATH = ROOT / "paper" / "surge_params.json"
OUT_JSON = ROOT / "2_Logs" / "surge_backtest_report_latest.json"
DEFAULT_EVAL_START_YMD = "20260301"


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


def _load_params() -> Dict[str, Any]:
    if not PARAMS_PATH.exists():
        return {}
    try:
        obj = json.loads(PARAMS_PATH.read_text(encoding="utf-8-sig"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _summary(df: pd.DataFrame, sig_col: str) -> Dict[str, Any]:
    d = df[df[sig_col] == 1].copy()
    if len(d) == 0:
        return {
            "signals": 0,
            "win_rate": 0.0,
            "avg_ret5": 0.0,
            "avg_ret5_net": 0.0,
            "median_ret5": 0.0,
            "median_ret5_net": 0.0,
            "sharpe_annualized": 0.0,
            "mdd": 0.0,
            "mdd_return_floor_count": 0,
            "cost": 0.0,
        }
    r = pd.to_numeric(d["fwd_ret5"], errors="coerce").fillna(0.0)
    cost = _to_float_env("SURGE_BACKTEST_ROUND_TRIP_COST", 0.0023)
    r_net = r - cost
    std_net = float(r_net.std())
    periods_per_year = 250.0 / 5.0
    sharpe = float(r_net.mean() / std_net * np.sqrt(periods_per_year)) if std_net > 0 else 0.0
    mdd_return_floor_count = int((r_net < -1.0).sum())
    equity = (1.0 + r_net.clip(lower=-1.0)).cumprod()
    peak = equity.cummax()
    if len(equity) > 0:
        drawdown = ((equity - peak) / peak.replace(0.0, np.nan)).fillna(-1.0)
        mdd = float(drawdown.min())
    else:
        mdd = 0.0
    return {
        "signals": int(len(d)),
        "win_rate": float((r > 0).mean()),
        "avg_ret5": float(r.mean()),
        "avg_ret5_net": float(r_net.mean()),
        "median_ret5": float(r.median()),
        "median_ret5_net": float(r_net.median()),
        "sharpe_annualized": round(sharpe, 3),
        "mdd": round(mdd, 4),
        "mdd_return_floor_count": mdd_return_floor_count,
        "cost": float(cost),
    }


def main() -> int:
    ts = datetime.now().isoformat(timespec="seconds")
    if not PRICES_PATH.exists():
        OUT_JSON.write_text(
            json.dumps({"ts": ts, "status": "MISSING_PRICES", "path": str(PRICES_PATH)}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return 1

    px = pd.read_parquet(PRICES_PATH).copy()
    px["date"] = px["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str.slice(0, 8)
    px["code"] = px["code"].astype(str).str.zfill(6)
    for c in ["open", "high", "low", "close", "volume"]:
        px[c] = pd.to_numeric(px[c], errors="coerce")
    px = px.dropna(subset=["open", "high", "low", "close", "volume"]).sort_values(["code", "date"])

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
    px["fwd_ret5"] = g["close"].shift(-5) / px["close"] - 1.0
    px = px.dropna(subset=["ret1", "ret5", "range_pct", "vol_ratio20", "fwd_ret5"]).copy()
    eval_start_ymd = str(os.getenv("SURGE_EVAL_START_YMD", DEFAULT_EVAL_START_YMD) or DEFAULT_EVAL_START_YMD).strip()
    if eval_start_ymd:
        px = px[px["date"].astype(str) >= eval_start_ymd].copy()
    eval_dates = sorted(px["date"].astype(str).unique().tolist())

    p = _load_params()
    bt_pct_min = float(p.get("pct_min", 0.07))
    bt_rvol20_min = float(p.get("rvol20_min", 2.0))
    bt_range_min = float(p.get("range_min", 0.08))

    px["sig_rule_only"] = ((px["ret1"] >= bt_pct_min) & ((px["vol_ratio20"] >= bt_rvol20_min) | (px["range_pct"] >= bt_range_min))).astype(int)
    px["sig_rule_ml"] = 0
    ml_status = "MISSING_MODEL"
    ml_prob_thr = _to_float_env("SURGE_BT_ML_PROB_MIN", float(p.get("bt_ml_prob_min", 0.40)))
    ml_top_q = _to_float_env("SURGE_BT_ML_TOPQ", float(p.get("bt_ml_topq", 0.90)))
    ml_fallback_used = False
    if MODEL_PATH.exists():
        with MODEL_PATH.open("rb") as f:
            bundle = pickle.load(f)
        model_type = str(bundle.get("model_type", ""))
        model = bundle.get("model")
        features = list(bundle.get("features") or [])
        g2 = px.groupby("code", group_keys=False)
        x = pd.DataFrame(
            {
                "ret1": px["ret1"],
                "ret3": px["ret3"],
                "ret5": px["ret5"],
                "ret10": px["ret10"],
                "range_pct": px["range_pct"],
                "body_pct": px["body_pct"],
                "upper_shadow_pct": px["upper_shadow_pct"],
                "vol_ratio5": px["vol_ratio5"],
                "vol_ratio20": px["vol_ratio20"],
                "atr14_pct": px["atr14_pct"],
                "close_to_high_20d": px["close_to_high_20d"],
            }
        ).reindex(px.index)
        for feat in features:
            if feat not in x.columns:
                x[feat] = 0.0
        x = x[features].apply(pd.to_numeric, errors="coerce").fillna(0.0)
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
        px["surge_ml_prob"] = prob
        px["sig_rule_ml"] = ((px["sig_rule_only"] == 1) & (px["surge_ml_prob"] >= ml_prob_thr)).astype(int)
        if int(px["sig_rule_ml"].sum()) == 0:
            cand = px[px["sig_rule_only"] == 1].copy()
            if len(cand) > 0:
                qv = float(cand["surge_ml_prob"].quantile(max(0.5, min(0.99, ml_top_q))))
                px["sig_rule_ml"] = ((px["sig_rule_only"] == 1) & (px["surge_ml_prob"] >= qv)).astype(int)
                ml_fallback_used = True
        ml_status = "OK"

    payload = {
        "ts": ts,
        "status": "OK",
        "ml_status": ml_status,
        "ml_prob_threshold": ml_prob_thr,
        "ml_topq_fallback": {"used": ml_fallback_used, "topq": ml_top_q},
        "eval_start_ymd": eval_start_ymd,
        "eval_end_ymd": (str(eval_dates[-1]) if eval_dates else ""),
        "rule_only": _summary(px, "sig_rule_only"),
        "rule_plus_ml": _summary(px, "sig_rule_ml"),
        "rows_eval": int(len(px)),
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
