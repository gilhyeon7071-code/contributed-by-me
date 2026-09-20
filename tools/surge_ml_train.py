from __future__ import annotations

import json
import os
import pickle
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PRICES_PATH   = ROOT / "paper" / "prices" / "ohlcv_paper.parquet"
ARCHIVE_DIR   = ROOT / "krx_daily_archive"
MODEL_PATH    = ROOT / "_cache" / "surge_ml_model.pkl"
STATUS_PATH   = ROOT / "2_Logs" / "surge_ml_train_status_latest.json"
SURGE_PARAMS_PATH = ROOT / "paper" / "surge_params.json"
DEFAULT_EVAL_START_YMD = "20260301"

NEEDED_COLS = ["date", "code", "open", "high", "low", "close", "volume"]


def _load_surge_params() -> Dict[str, Any]:
    try:
        raw = json.loads(SURGE_PARAMS_PATH.read_text(encoding="utf-8-sig"))
        if isinstance(raw, dict):
            return raw
    except Exception:
        pass
    return {}


def _load_all_prices() -> pd.DataFrame:
    """krx_daily_archive 전체 + ohlcv_paper 합산. 중복(code+date)은 archive 우선."""
    frames: List[pd.DataFrame] = []

    # 1) archive annual/daily parquet 로드
    if ARCHIVE_DIR.exists():
        for f in sorted(ARCHIVE_DIR.glob("krx_daily_*_clean.parquet")):
            try:
                df = pd.read_parquet(f, columns=[c for c in NEEDED_COLS if c != "name"])
                frames.append(df)
            except Exception:
                pass

    # 2) paper parquet (최근 보완용)
    if PRICES_PATH.exists():
        try:
            df = pd.read_parquet(PRICES_PATH)
            frames.append(df[[c for c in NEEDED_COLS + ["name"] if c in df.columns]])
        except Exception:
            pass

    if not frames:
        return pd.DataFrame(columns=NEEDED_COLS)

    px = pd.concat(frames, ignore_index=True)
    px["date"] = px["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    px["code"] = px["code"].astype(str).str.zfill(6)
    for c in ["open", "high", "low", "close", "volume"]:
        px[c] = pd.to_numeric(px[c], errors="coerce")
    px = px.dropna(subset=["open", "high", "low", "close", "volume"])
    # 중복 제거: 같은 code+date면 마지막(paper) 우선
    px = px.drop_duplicates(subset=["code", "date"], keep="last")
    px = px.sort_values(["code", "date"]).reset_index(drop=True)
    return px


def _load_label_thr_from_params() -> float:
    """Load return-threshold label fallback from surge_params.json or env."""
    default = 0.15
    v = _load_surge_params().get("ml_label_thr_ret5")
    if isinstance(v, (int, float)) and float(v) > 0:
        default = float(v)
    return _to_float_env("SURGE_ML_LABEL_RET5_MIN", default)

def _to_float_env(name: str, default: float) -> float:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _load_label_config_from_params(label_thr: float) -> Dict[str, Any]:
    raw = _load_surge_params()
    method = str(raw.get("ml_label_method", "triple_barrier") or "triple_barrier").strip().lower()
    method = str(os.getenv("SURGE_ML_LABEL_METHOD", method) or method).strip().lower()
    horizon = int(_to_float_env("SURGE_ML_LABEL_HORIZON_DAYS", float(raw.get("ml_label_horizon_days", 5) or 5)))
    take_profit = _to_float_env("SURGE_ML_TB_TAKE_PROFIT_PCT", float(raw.get("ml_tb_take_profit_pct", label_thr) or label_thr))
    stop_loss = _to_float_env("SURGE_ML_TB_STOP_LOSS_PCT", float(raw.get("ml_tb_stop_loss_pct", -0.05) or -0.05))
    if method not in {"triple_barrier", "ret5_threshold"}:
        method = "triple_barrier"
    return {
        "method": method,
        "horizon_days": max(1, int(horizon)),
        "take_profit_pct": float(take_profit),
        "stop_loss_pct": -abs(float(stop_loss)),
    }


def _triple_barrier_labels(group: pd.DataFrame, horizon_days: int, take_profit_pct: float, stop_loss_pct: float) -> pd.Series:
    closes = pd.to_numeric(group["close"], errors="coerce").to_numpy(dtype=float)
    highs = pd.to_numeric(group["high"], errors="coerce").to_numpy(dtype=float)
    lows = pd.to_numeric(group["low"], errors="coerce").to_numpy(dtype=float)
    labels = np.zeros(len(group), dtype=int)
    h = max(1, int(horizon_days))
    tp = max(0.0, float(take_profit_pct))
    sl = -abs(float(stop_loss_pct))
    for i, close in enumerate(closes):
        if not np.isfinite(close) or close <= 0:
            continue
        upper = close * (1.0 + tp)
        lower = close * (1.0 + sl)
        end = min(len(group), i + h + 1)
        for j in range(i + 1, end):
            hit_up = bool(np.isfinite(highs[j]) and highs[j] >= upper)
            hit_down = bool(np.isfinite(lows[j]) and lows[j] <= lower)
            if hit_up and not hit_down:
                labels[i] = 1
                break
            if hit_down or hit_up:
                break
    return pd.Series(labels, index=group.index, dtype="int64")


def _build_features(px: pd.DataFrame, label_thr: float, label_config: Dict[str, Any]) -> pd.DataFrame:
    px = px.copy().sort_values(["code", "date"])
    g = px.groupby("code", group_keys=False)

    # 기존 피처
    px["ret1"] = g["close"].pct_change(1)
    px["ret3"] = g["close"].pct_change(3)
    px["ret5"] = g["close"].pct_change(5)
    px["range_pct"] = (px["high"] - px["low"]) / px["close"].replace(0, pd.NA)
    px["body_pct"] = (px["close"] - px["open"]) / px["open"].replace(0, pd.NA)
    px["vol_ma20"] = g["volume"].rolling(20, min_periods=5).mean().reset_index(level=0, drop=True)
    px["vol_ratio20"] = px["volume"] / px["vol_ma20"].replace(0, pd.NA)

    # 확장 피처
    # 중기 모멘텀
    px["ret10"] = g["close"].pct_change(10)
    # 단기 거래량 가속 (5일 평균 대비)
    px["vol_ma5"] = g["volume"].rolling(5, min_periods=2).mean().reset_index(level=0, drop=True)
    px["vol_ratio5"] = px["volume"] / px["vol_ma5"].replace(0, pd.NA)
    # 14-day ATR based on True Range.
    prev_close = g["close"].shift(1)
    tr1 = (px["high"] - px["low"]).abs()
    tr2 = (px["high"] - prev_close).abs()
    tr3 = (px["low"] - prev_close).abs()
    px["atr14"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    px["atr14_roll"] = g["atr14"].rolling(14, min_periods=5).mean().reset_index(level=0, drop=True)
    px["atr14_pct"] = px["atr14_roll"] / px["close"].replace(0, pd.NA)
    # 20일 최고가 대비 위치 (돌파 측정)
    px["high_20d"] = g["high"].rolling(20, min_periods=5).max().reset_index(level=0, drop=True)
    px["close_to_high_20d"] = px["close"] / px["high_20d"].replace(0, pd.NA)
    # 위꼬리 비율 (매도 압력: 고가에서 종가까지 하락폭)
    px["upper_shadow_pct"] = (px["high"] - px[["open", "close"]].max(axis=1)) / px["close"].replace(0, pd.NA)

    px["fwd_ret5"] = g["close"].shift(-5) / px["close"] - 1.0
    if str(label_config.get("method", "")).lower() == "triple_barrier":
        px["label_surge"] = (
            px.groupby("code", group_keys=False)[["open", "high", "low", "close", "volume"]]
              .apply(
                  lambda z: _triple_barrier_labels(
                      z,
                      int(label_config.get("horizon_days", 5) or 5),
                      float(label_config.get("take_profit_pct", label_thr) or label_thr),
                      float(label_config.get("stop_loss_pct", -0.05) or -0.05),
                  )
              )
              .astype(int)
        )
    else:
        px["label_surge"] = (px["fwd_ret5"] >= float(label_thr)).fillna(False).astype(int)
    return px


def _roc_auc_safe(y_true: pd.Series, y_score: np.ndarray) -> float | None:
    y = pd.to_numeric(y_true, errors="coerce").fillna(0).astype(int).to_numpy()
    s = np.asarray(y_score, dtype=float)
    pos = y == 1
    neg = y == 0
    n_pos = int(pos.sum())
    n_neg = int(neg.sum())
    if n_pos == 0 or n_neg == 0:
        return None
    ranks = pd.Series(s).rank(method="average").to_numpy()
    auc = (ranks[pos].sum() - (n_pos * (n_pos + 1) / 2.0)) / float(n_pos * n_neg)
    return float(auc)


def _pr_auc_safe(y_true: pd.Series, y_score: np.ndarray) -> float | None:
    y = pd.to_numeric(y_true, errors="coerce").fillna(0).astype(int).to_numpy()
    s = np.asarray(y_score, dtype=float)
    if int((y == 1).sum()) == 0:
        return None
    order = np.argsort(-s, kind="mergesort")
    y_sorted = y[order]
    tp = np.cumsum(y_sorted == 1)
    precision = tp / np.arange(1, len(y_sorted) + 1, dtype=float)
    return float(precision[y_sorted == 1].mean())


def _precision_at_k_safe(y_true: pd.Series, y_score: np.ndarray, k: int) -> float | None:
    y = pd.to_numeric(y_true, errors="coerce").fillna(0).astype(int).to_numpy()
    s = np.asarray(y_score, dtype=float)
    if len(y) == 0:
        return None
    kk = min(max(1, int(k)), len(y))
    order = np.argsort(-s, kind="mergesort")[:kk]
    return float((y[order] == 1).mean())


def _walk_forward_splits(dates: List[str], folds: int = 3, embargo_days: int = 0) -> List[Dict[str, str]]:
    if len(dates) < 40:
        return []
    folds = max(1, int(folds))
    start_valid = max(1, int(len(dates) * 0.55))
    step = max(1, int((len(dates) - start_valid) / folds))
    out: List[Dict[str, str]] = []
    for i in range(folds):
        train_end_idx = min(len(dates) - 2, start_valid + i * step - 1)
        valid_start_idx = train_end_idx + 1 + max(0, int(embargo_days))
        valid_end_idx = min(len(dates) - 1, valid_start_idx + step - 1)
        if train_end_idx < 10 or valid_end_idx <= train_end_idx:
            continue
        if valid_start_idx >= len(dates):
            continue
        out.append(
            {
                "train_end": str(dates[train_end_idx]),
                "valid_start": str(dates[valid_start_idx]),
                "valid_end": str(dates[valid_end_idx]),
            }
        )
    return out


def _fit_predict_prob(train: pd.DataFrame, valid: pd.DataFrame, features: List[str], use_balanced: bool, max_rows: int) -> tuple[np.ndarray, str]:
    tr = train.copy()
    va = valid.copy()
    if max_rows > 0 and len(tr) > max_rows:
        tr = tr.sort_values("date").tail(max_rows).copy()
    X_tr = tr[features].astype(float)
    y_tr = tr["label_surge"].astype(int)
    X_va = va[features].astype(float)
    try:
        from sklearn.ensemble import HistGradientBoostingClassifier

        model = HistGradientBoostingClassifier(
            learning_rate=0.05,
            max_depth=5,
            max_iter=int(_to_float_env("SURGE_ML_WF_MAX_ITER", 80)),
            min_samples_leaf=50,
            random_state=42,
        )
        if use_balanced:
            from sklearn.utils.class_weight import compute_sample_weight

            sample_weight = compute_sample_weight(class_weight="balanced", y=y_tr)
            model.fit(X_tr, y_tr, sample_weight=sample_weight)
        else:
            model.fit(X_tr, y_tr)
        return model.predict_proba(X_va)[:, 1], "sklearn_hgb"
    except Exception:
        x_tr = X_tr.to_numpy(dtype=float)
        x_va = X_va.to_numpy(dtype=float)
        y = y_tr.to_numpy(dtype=float)
        mu = np.nanmean(x_tr, axis=0)
        sigma = np.nanstd(x_tr, axis=0)
        sigma = np.where(np.isfinite(sigma) & (sigma > 1e-9), sigma, 1.0)
        x_tr_n = np.nan_to_num((x_tr - mu) / sigma, nan=0.0, posinf=0.0, neginf=0.0)
        x_va_n = np.nan_to_num((x_va - mu) / sigma, nan=0.0, posinf=0.0, neginf=0.0)
        pos_n = float((y == 1.0).sum())
        neg_n = float((y == 0.0).sum())
        pos_rate = pos_n / max(1.0, pos_n + neg_n)
        bias = float(np.log(max(1e-6, pos_rate) / max(1e-6, 1.0 - pos_rate)))
        w = np.zeros(x_tr_n.shape[1], dtype=float)
        lr = _to_float_env("SURGE_ML_FALLBACK_LR", 0.05)
        l2 = _to_float_env("SURGE_ML_FALLBACK_L2", 1e-3)
        steps = int(_to_float_env("SURGE_ML_WF_FALLBACK_STEPS", 250))
        pos_scale = 1.0
        if use_balanced and pos_n > 0 and neg_n > 0:
            pos_scale = max(1.0, neg_n / pos_n)
        sw = np.where(y > 0.5, pos_scale, 1.0).astype(float)
        n = max(1, x_tr_n.shape[0])
        for _ in range(max(50, steps)):
            raw = np.clip(x_tr_n @ w + bias, -50.0, 50.0)
            p = 1.0 / (1.0 + np.exp(-raw))
            err = (p - y) * sw
            w = w - lr * ((x_tr_n.T @ err) / float(n) + l2 * w)
            bias = bias - lr * float(err.mean())
        return (1.0 / (1.0 + np.exp(-np.clip(x_va_n @ w + bias, -50.0, 50.0)))).astype(float), "linear_logreg_fallback"


def _walk_forward_validate(train_df: pd.DataFrame, dates: List[str], features: List[str], use_balanced: bool) -> Dict[str, object]:
    if str(os.getenv("SURGE_ML_WALK_FORWARD", "1") or "1").strip().lower() in {"0", "false", "no", "off"}:
        return {"enabled": False, "reason": "disabled"}
    raw = _load_surge_params()
    embargo_days = int(_to_float_env("SURGE_ML_EMBARGO_DAYS", float(raw.get("ml_embargo_days", 5) or 5)))
    top_k = int(_to_float_env("SURGE_ML_PRECISION_TOP_K", float(raw.get("ml_precision_top_k", 50) or 50)))
    folds = _walk_forward_splits(dates, folds=int(_to_float_env("SURGE_ML_WF_FOLDS", 3)), embargo_days=embargo_days)
    if not folds:
        return {"enabled": True, "status": "SKIP", "reason": "insufficient_dates", "folds": [], "embargo_days": int(embargo_days)}
    max_rows = int(_to_float_env("SURGE_ML_WF_MAX_TRAIN_ROWS", 250000))
    rows: List[Dict[str, object]] = []
    for spec in folds:
        tr = train_df[train_df["date"] <= spec["train_end"]].copy()
        va = train_df[(train_df["date"] >= spec["valid_start"]) & (train_df["date"] <= spec["valid_end"])].copy()
        if len(tr) == 0 or len(va) == 0:
            continue
        prob, trainer = _fit_predict_prob(tr, va, features, use_balanced, max_rows=max_rows)
        auc = _roc_auc_safe(va["label_surge"].astype(int), prob)
        pr_auc = _pr_auc_safe(va["label_surge"].astype(int), prob)
        precision_at_k = _precision_at_k_safe(va["label_surge"].astype(int), prob, top_k)
        rows.append(
            {
                **spec,
                "rows_train": int(min(len(tr), max_rows) if max_rows > 0 else len(tr)),
                "rows_valid": int(len(va)),
                "valid_auc": auc,
                "valid_pr_auc": pr_auc,
                "precision_at_k": precision_at_k,
                "precision_top_k": int(min(max(1, top_k), len(va))),
                "valid_pos_rate": float(va["label_surge"].astype(int).mean()) if len(va) else 0.0,
                "trainer": trainer,
            }
        )
    aucs = [float(r["valid_auc"]) for r in rows if r.get("valid_auc") is not None]
    pr_aucs = [float(r["valid_pr_auc"]) for r in rows if r.get("valid_pr_auc") is not None]
    p_at_ks = [float(r["precision_at_k"]) for r in rows if r.get("precision_at_k") is not None]
    return {
        "enabled": True,
        "status": "OK" if aucs else "WARN",
        "folds": rows,
        "fold_count": int(len(rows)),
        "median_auc": float(np.median(aucs)) if aucs else None,
        "median_pr_auc": float(np.median(pr_aucs)) if pr_aucs else None,
        "median_precision_at_k": float(np.median(p_at_ks)) if p_at_ks else None,
        "precision_top_k": int(top_k),
        "embargo_days": int(embargo_days),
        "min_auc": float(np.min(aucs)) if aucs else None,
        "max_train_rows": int(max_rows),
    }


def main() -> int:
    ts = datetime.now().isoformat(timespec="seconds")
    if not PRICES_PATH.exists() and not ARCHIVE_DIR.exists():
        STATUS_PATH.write_text(
            json.dumps({"ts": ts, "status": "MISSING_PRICES", "path": str(PRICES_PATH)}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return 1

    label_thr = _load_label_thr_from_params()
    label_config = _load_label_config_from_params(label_thr)
    min_rows = int(_to_float_env("SURGE_ML_MIN_ROWS", 2000))

    px = _load_all_prices()

    df = _build_features(px, label_thr, label_config)
    features: List[str] = [
        "ret1", "ret3", "ret5", "ret10",
        "range_pct", "body_pct", "upper_shadow_pct",
        "vol_ratio20", "vol_ratio5",
        "atr14_pct", "close_to_high_20d",
    ]
    df[features] = df[features].apply(lambda s: pd.to_numeric(s, errors="coerce"))
    df[features] = df[features].where(np.isfinite(df[features]), np.nan)
    train_df = df.dropna(subset=features + ["label_surge"]).copy()
    eval_start_ymd = str(os.getenv("SURGE_EVAL_START_YMD", DEFAULT_EVAL_START_YMD) or DEFAULT_EVAL_START_YMD).strip()
    if eval_start_ymd:
        train_df = train_df[train_df["date"].astype(str) >= eval_start_ymd].copy()
    eval_dates = sorted(train_df["date"].astype(str).unique().tolist())
    if len(train_df) < min_rows:
        STATUS_PATH.write_text(
            json.dumps(
                {
                    "ts": ts,
                    "status": "INSUFFICIENT_ROWS",
                    "eval_start_ymd": eval_start_ymd,
                    "eval_end_ymd": (str(eval_dates[-1]) if eval_dates else ""),
                    "rows": int(len(train_df)),
                    "min_rows": int(min_rows),
                    "label_thr": float(label_thr),
                    "label_config": label_config,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return 2

    dates = sorted(train_df["date"].unique().tolist())
    raw_params = _load_surge_params()
    embargo_days = int(
        _to_float_env(
            "SURGE_ML_EMBARGO_DAYS",
            float(raw_params.get("ml_embargo_days", label_config.get("horizon_days", 5)) or label_config.get("horizon_days", 5)),
        )
    )
    top_k = int(_to_float_env("SURGE_ML_PRECISION_TOP_K", float(raw_params.get("ml_precision_top_k", 50) or 50)))
    split_idx = max(1, int(len(dates) * 0.8))
    split_date = str(dates[split_idx - 1])
    valid_start_idx = min(len(dates), split_idx + max(0, int(embargo_days)))

    tr = train_df[train_df["date"] <= split_date].copy()
    va = train_df[train_df["date"] >= str(dates[valid_start_idx])].copy() if valid_start_idx < len(dates) else pd.DataFrame()
    if len(va) == 0:
        va = tr.tail(min(1000, len(tr))).copy()

    X_tr = tr[features].astype(float)
    y_tr = tr["label_surge"].astype(int)
    X_va = va[features].astype(float)
    y_va = va["label_surge"].astype(int)

    trainer = "linear_stat_fallback"
    use_balanced = str(os.getenv("SURGE_ML_CLASS_BALANCED", "") or "").strip().lower() in {"1", "true", "yes", "on"}
    try:
        from sklearn.ensemble import HistGradientBoostingClassifier

        model = HistGradientBoostingClassifier(
            learning_rate=0.05,
            max_depth=5,
            max_iter=200,
            min_samples_leaf=50,
            random_state=42,
        )
        if use_balanced:
            from sklearn.utils.class_weight import compute_sample_weight

            sample_weight = compute_sample_weight(class_weight="balanced", y=y_tr)
            model.fit(X_tr, y_tr, sample_weight=sample_weight)
        else:
            model.fit(X_tr, y_tr)
        va_prob = model.predict_proba(X_va)[:, 1]
        bundle: Dict[str, object] = {
            "model_type": "sklearn_hgb",
            "model": model,
            "features": features,
            "label_thr_ret5": float(label_thr),
            "label_config": label_config,
            "split_date": split_date,
            "trained_at": ts,
        }
        trainer = "sklearn_hgb"
    except Exception:
        x_tr = X_tr.to_numpy(dtype=float)
        x_va = X_va.to_numpy(dtype=float)
        y = y_tr.to_numpy(dtype=float)
        pos_n = float((y == 1.0).sum())
        neg_n = float((y == 0.0).sum())

        mu = np.nanmean(x_tr, axis=0)
        sigma = np.nanstd(x_tr, axis=0)
        sigma = np.where(np.isfinite(sigma) & (sigma > 1e-9), sigma, 1.0)
        x_tr_n = np.nan_to_num((x_tr - mu) / sigma, nan=0.0, posinf=0.0, neginf=0.0)
        x_va_n = np.nan_to_num((x_va - mu) / sigma, nan=0.0, posinf=0.0, neginf=0.0)

        w = np.zeros(x_tr_n.shape[1], dtype=float)
        pos_rate = pos_n / max(1.0, pos_n + neg_n)
        bias = float(np.log(max(1e-6, pos_rate) / max(1e-6, 1.0 - pos_rate)))

        lr = _to_float_env("SURGE_ML_FALLBACK_LR", 0.05)
        l2 = _to_float_env("SURGE_ML_FALLBACK_L2", 1e-3)
        steps = int(_to_float_env("SURGE_ML_FALLBACK_STEPS", 600))
        pos_scale = _to_float_env("SURGE_ML_FALLBACK_POS_SCALE", 1.0)
        if use_balanced and pos_n > 0 and neg_n > 0:
            pos_scale = max(1.0, neg_n / pos_n)
        sw = np.where(y > 0.5, pos_scale, 1.0).astype(float)

        n = max(1, x_tr_n.shape[0])
        for _ in range(max(50, steps)):
            raw = np.clip(x_tr_n @ w + bias, -50.0, 50.0)
            p = 1.0 / (1.0 + np.exp(-raw))
            err = (p - y) * sw
            grad_w = (x_tr_n.T @ err) / float(n) + l2 * w
            grad_b = float(err.mean())
            w = w - lr * grad_w
            bias = bias - lr * grad_b

        raw_va = np.clip(x_va_n @ w + bias, -50.0, 50.0)
        va_prob = (1.0 / (1.0 + np.exp(-raw_va))).astype(float)
        bundle = {
            "model_type": "linear_logreg_fallback",
            "coef": {k: float(v) for k, v in zip(features, w)},
            "intercept": float(bias),
            "feature_mean": {k: float(v) for k, v in zip(features, mu)},
            "feature_std": {k: float(v) for k, v in zip(features, sigma)},
            "features": features,
            "label_thr_ret5": float(label_thr),
            "label_config": label_config,
            "split_date": split_date,
            "trained_at": ts,
            "fallback_steps": int(max(50, steps)),
            "fallback_lr": float(lr),
            "fallback_l2": float(l2),
            "fallback_pos_scale": float(pos_scale),
        }
        trainer = "linear_logreg_fallback"

    auc = _roc_auc_safe(y_va, va_prob)
    pr_auc = _pr_auc_safe(y_va, va_prob)
    precision_at_k = _precision_at_k_safe(y_va, va_prob, top_k)
    walk_forward = _walk_forward_validate(train_df, dates, features, use_balanced)

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    with MODEL_PATH.open("wb") as f:
        pickle.dump(bundle, f)

    payload: Dict[str, object] = {
        "ts": ts,
        "status": "OK",
        "model_path": str(MODEL_PATH),
        "rows_total": int(len(train_df)),
        "eval_start_ymd": eval_start_ymd,
        "eval_end_ymd": (str(dates[-1]) if dates else ""),
        "rows_train": int(len(tr)),
        "rows_valid": int(len(va)),
        "split_date": split_date,
        "valid_start_date": (str(dates[valid_start_idx]) if valid_start_idx < len(dates) else ""),
        "embargo_days": int(max(0, embargo_days)),
        "label_config": label_config,
        "label_thr_ret5": float(label_thr),
        "valid_auc": auc,
        "valid_pr_auc": pr_auc,
        "precision_at_k": precision_at_k,
        "precision_top_k": int(min(max(1, top_k), len(va))) if len(va) else int(top_k),
        "valid_pos_rate": float(y_va.mean()) if len(y_va) else 0.0,
        "features": features,
        "trainer": trainer,
        "class_balanced": bool(use_balanced),
        "walk_forward": walk_forward,
    }
    STATUS_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
