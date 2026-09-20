from __future__ import annotations

import json
import pickle
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from holiday_manager import HolidayManager

LOG_DIR = ROOT / "2_Logs"
PRICES_PATH = ROOT / "paper" / "prices" / "ohlcv_paper.parquet"
MODEL_PATH = ROOT / "_cache" / "surge_ml_model.pkl"
SHADOW_MODEL_PATH = ROOT / "_cache" / "surge_ml_shadow_models.pkl"
INTRADAY_PRICES = LOG_DIR / "intraday_prices_latest.csv"
OUT_CSV = LOG_DIR / "surge_ml_score_latest.csv"
OUT_JSON = LOG_DIR / "surge_ml_score_latest.json"
P0_GLOB = "p0_daily_check_*.json"


def _replace_atomic(tmp_path: Path, final_path: Path) -> None:
    tmp_path.replace(final_path)


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _replace_atomic(tmp_path, path)


def _write_csv_atomic(path: Path, df: pd.DataFrame) -> None:
    tmp_path = path.with_name(f"{path.name}.tmp")
    df.to_csv(tmp_path, index=False, encoding="utf-8-sig")
    _replace_atomic(tmp_path, path)


def _safe_ratio(num: float, den: float) -> float:
    if den <= 0:
        return 0.0
    return float(num) / float(den)


def _prev_weekday_ymd(today_ymd: str) -> str:
    return HolidayManager().previous_trading_day(str(today_ymd))


def _load_expected_prev_weekday(today_ymd: str) -> str:
    expected = _prev_weekday_ymd(today_ymd)
    try:
        p0_files = sorted(LOG_DIR.glob(P0_GLOB), key=lambda p: p.stat().st_mtime, reverse=True)
    except Exception:
        p0_files = []
    if p0_files:
        try:
            p0 = json.loads(p0_files[0].read_text(encoding="utf-8-sig"))
            prices = p0.get("prices") if isinstance(p0.get("prices"), dict) else {}
            ymd = str(prices.get("prev_weekday") or "").strip()
            ymd = "".join(ch for ch in ymd if ch.isdigit())[:8]
            if len(ymd) == 8 and ymd == expected:
                return ymd
        except Exception:
            pass
    return expected


def _load_rt() -> pd.DataFrame:
    rt = pd.read_csv(INTRADAY_PRICES, dtype={"code": str})
    rt = rt.copy()
    rt["code"] = rt["code"].astype(str).str.zfill(6)
    rt["date"] = rt["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str.slice(0, 8)
    for c in ["current_price", "open", "high", "low", "volume"]:
        if c not in rt.columns:
            rt[c] = 0
        rt[c] = pd.to_numeric(rt[c], errors="coerce").fillna(0.0)
    return rt


def _load_px(today_ymd: str) -> pd.DataFrame:
    px = pd.read_parquet(PRICES_PATH)
    px = px.copy()
    px["date"] = px["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str.slice(0, 8)
    px["code"] = px["code"].astype(str).str.zfill(6)
    for c in ["open", "high", "low", "close", "volume"]:
        px[c] = pd.to_numeric(px[c], errors="coerce").fillna(0.0)
    return px[px["date"] < str(today_ymd)].copy()


def _load_shadow_bundle() -> tuple[Dict[str, Any] | None, str]:
    if not SHADOW_MODEL_PATH.exists():
        return None, "MISSING_SHADOW_BUNDLE"
    try:
        with SHADOW_MODEL_PATH.open("rb") as f:
            bundle = pickle.load(f)
        if not isinstance(bundle, dict):
            return None, "INVALID_SHADOW_BUNDLE"
        return bundle, "OK"
    except ModuleNotFoundError as e:
        return None, f"MISSING_SHADOW_RUNTIME_DEP:{e}"
    except Exception as e:
        return None, f"SHADOW_LOAD_ERROR:{type(e).__name__}:{e}"


def _predict_shadow_scores(shadow_bundle: Dict[str, Any] | None, feat: Dict[str, float]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "surge_ml_prob_lgbm_shadow": np.nan,
        "surge_ml_prob_xgb_shadow": np.nan,
        "shadow_model_status": "MISSING_SHADOW_BUNDLE" if shadow_bundle is None else "OK",
    }
    if shadow_bundle is None:
        return out
    features: List[str] = list(shadow_bundle.get("features") or [])
    models = shadow_bundle.get("models") if isinstance(shadow_bundle.get("models"), dict) else {}
    if not features:
        out["shadow_model_status"] = "INVALID_SHADOW_FEATURES"
        return out
    x = pd.DataFrame([{k: float(feat.get(k, 0.0)) for k in features}])[features]
    status_parts: List[str] = []
    for model_key, out_col in [
        ("lightgbm", "surge_ml_prob_lgbm_shadow"),
        ("xgboost", "surge_ml_prob_xgb_shadow"),
    ]:
        entry = models.get(model_key) if isinstance(models.get(model_key), dict) else {}
        model_status = str(entry.get("status") or "MISSING").strip()
        model = entry.get("model")
        if model_status != "OK" or model is None:
            status_parts.append(f"{model_key}={model_status}")
            continue
        try:
            out[out_col] = round(float(model.predict_proba(x)[0, 1]), 6)
            status_parts.append(f"{model_key}=OK")
        except Exception as e:
            status_parts.append(f"{model_key}=ERROR:{type(e).__name__}")
    out["shadow_model_status"] = ";".join(status_parts) if status_parts else "NO_SHADOW_MODELS"
    return out


def main() -> int:
    ts = datetime.now().isoformat(timespec="seconds")
    if not MODEL_PATH.exists():
        _write_json_atomic(OUT_JSON, {"ts": ts, "status": "MISSING_MODEL", "model_path": str(MODEL_PATH)})
        _write_csv_atomic(OUT_CSV, pd.DataFrame())
        return 0
    if not INTRADAY_PRICES.exists() or not PRICES_PATH.exists():
        _write_json_atomic(
            OUT_JSON,
            {"ts": ts, "status": "MISSING_INPUT", "intraday": str(INTRADAY_PRICES), "prices": str(PRICES_PATH)},
        )
        _write_csv_atomic(OUT_CSV, pd.DataFrame())
        return 0

    try:
        with MODEL_PATH.open("rb") as f:
            bundle = pickle.load(f)
    except ModuleNotFoundError as e:
        _write_json_atomic(
            OUT_JSON,
            {
                "ts": ts,
                "status": "MISSING_RUNTIME_DEP",
                "detail": f"{type(e).__name__}: {e}",
                "model_path": str(MODEL_PATH),
            },
        )
        _write_csv_atomic(OUT_CSV, pd.DataFrame())
        return 0
    model_type = str(bundle.get("model_type", ""))
    model = bundle.get("model")
    features: List[str] = list(bundle.get("features") or [])
    shadow_bundle, shadow_load_status = _load_shadow_bundle()

    rt = _load_rt()
    today_ymd = str(rt["date"].astype(str).max()) if len(rt) else datetime.now().strftime("%Y%m%d")
    expected_prev_weekday = _load_expected_prev_weekday(today_ymd)
    max_krx_daily_move_pct = 0.305
    px = _load_px(today_ymd)

    rows: List[Dict[str, Any]] = []
    stale_ref_skip_count = 0
    impossible_move_skip_count = 0
    for _, rr in rt.iterrows():
        code = str(rr.get("code", "") or "").zfill(6)
        cur = float(rr.get("current_price", 0.0) or 0.0)
        opn = float(rr.get("open", 0.0) or 0.0)
        hi = float(rr.get("high", 0.0) or 0.0)
        lo = float(rr.get("low", 0.0) or 0.0)
        vol = float(rr.get("volume", 0.0) or 0.0)
        if cur <= 0:
            continue

        g = px[px["code"] == code].sort_values("date")
        closes = g["close"].astype(float).tolist()
        highs  = g["high"].astype(float).tolist()
        lows   = g["low"].astype(float).tolist()
        vols   = g["volume"].astype(float).tolist()
        ref_latest_date = str(g["date"].iloc[-1]) if len(g) > 0 else ""
        if len(closes) < 20:
            continue
        if expected_prev_weekday and ref_latest_date and ref_latest_date != expected_prev_weekday:
            stale_ref_skip_count += 1
            continue
        prev_close  = float(closes[-1])
        ret1 = _safe_ratio(cur - prev_close, prev_close)
        if ret1 > max_krx_daily_move_pct:
            impossible_move_skip_count += 1
            continue
        close_3     = float(closes[-3])  if len(closes) >= 3  else prev_close
        close_5     = float(closes[-5])  if len(closes) >= 5  else prev_close
        close_10    = float(closes[-10]) if len(closes) >= 10 else prev_close
        vol_ma20    = float(sum(vols[-20:]) / max(1, len(vols[-20:])))
        vol_ma5     = float(sum(vols[-5:])  / max(1, len(vols[-5:])))
        # atr14: 14-day True Range average / latest prev_close.
        prev_closes_14 = closes[-15:-1]
        tr_list     = [
            max(abs(h - l), abs(h - pc), abs(l - pc))
            for h, l, pc in zip(highs[-14:], lows[-14:], prev_closes_14)
        ]
        atr14_roll  = float(sum(tr_list) / max(1, len(tr_list))) if tr_list else 0.0
        atr14_pct   = _safe_ratio(atr14_roll, max(prev_close, 1e-9))
        # close_to_high_20d: 현재가 / 20일 최고가
        high_20d    = max(highs[-20:]) if highs else cur
        close_to_high_20d = _safe_ratio(cur, max(high_20d, 1e-9))
        # upper_shadow_pct: 위꼬리 / 현재가
        upper_shadow_pct  = _safe_ratio(max(0.0, hi - max(opn, cur)), max(cur, 1e-9))

        feat = {
            "ret1":              ret1,
            "ret3":              _safe_ratio(cur - close_3,    close_3),
            "ret5":              _safe_ratio(cur - close_5,    close_5),
            "ret10":             _safe_ratio(cur - close_10,   close_10),
            "range_pct":         _safe_ratio(max(0.0, hi - lo), max(cur, 1e-9)),
            "body_pct":          _safe_ratio(cur - opn, max(opn, 1e-9)),
            "upper_shadow_pct":  upper_shadow_pct,
            "vol_ratio20":       _safe_ratio(vol, vol_ma20),
            "vol_ratio5":        _safe_ratio(vol, vol_ma5),
            "atr14_pct":         atr14_pct,
            "close_to_high_20d": close_to_high_20d,
        }
        x = pd.DataFrame([{k: float(feat.get(k, 0.0)) for k in features}])[features]
        if model_type == "linear_stat":
            coef_map = bundle.get("coef") if isinstance(bundle.get("coef"), dict) else {}
            coef = pd.Series({k: float(coef_map.get(k, 0.0)) for k in features}, dtype=float)
            intercept = float(bundle.get("intercept", 0.0) or 0.0)
            raw = float(x.mul(coef, axis=1).sum(axis=1).iloc[0] + intercept)
            prob = float(1.0 / (1.0 + np.exp(-np.clip(raw, -50.0, 50.0))))
        elif model_type == "linear_logreg_fallback":
            coef_map = bundle.get("coef") if isinstance(bundle.get("coef"), dict) else {}
            mean_map = bundle.get("feature_mean") if isinstance(bundle.get("feature_mean"), dict) else {}
            std_map = bundle.get("feature_std") if isinstance(bundle.get("feature_std"), dict) else {}
            coef = pd.Series({k: float(coef_map.get(k, 0.0)) for k in features}, dtype=float)
            f_mean = pd.Series({k: float(mean_map.get(k, 0.0)) for k in features}, dtype=float)
            f_std = pd.Series({k: float(std_map.get(k, 1.0)) for k in features}, dtype=float).replace(0.0, 1.0)
            intercept = float(bundle.get("intercept", 0.0) or 0.0)
            x_norm = (x - f_mean) / f_std
            raw = float(x_norm.mul(coef, axis=1).sum(axis=1).iloc[0] + intercept)
            prob = float(1.0 / (1.0 + np.exp(-np.clip(raw, -50.0, 50.0))))
        else:
            prob = float(model.predict_proba(x)[0, 1])
        shadow_scores = _predict_shadow_scores(shadow_bundle, feat)
        if shadow_load_status != "OK":
            shadow_scores["shadow_model_status"] = shadow_load_status
        rows.append(
            {
                "ts": ts,
                "date": today_ymd,
                "code": code,
                "reference_latest_date": ref_latest_date,
                "expected_prev_weekday": expected_prev_weekday,
                "surge_ml_prob": round(prob, 6),
                **shadow_scores,
                **{k: round(float(feat[k]), 6) for k in feat.keys()},
            }
        )

    out_df = pd.DataFrame(rows)
    if len(out_df) > 0:
        out_df = out_df.sort_values(["surge_ml_prob"], ascending=[False])
    _write_csv_atomic(OUT_CSV, out_df)

    payload = {
        "ts": ts,
        "status": "OK",
        "model_path": str(MODEL_PATH),
        "rows": int(len(out_df)),
        "expected_prev_weekday": expected_prev_weekday,
        "stale_ref_skip_count": int(stale_ref_skip_count),
        "impossible_move_skip_count": int(impossible_move_skip_count),
        "top_codes": out_df.head(5)["code"].astype(str).tolist() if len(out_df) else [],
        "shadow_model_path": str(SHADOW_MODEL_PATH),
        "shadow_load_status": shadow_load_status,
        "shadow_columns": [
            "surge_ml_prob_lgbm_shadow",
            "surge_ml_prob_xgb_shadow",
            "shadow_model_status",
        ],
        "out_csv": str(OUT_CSV),
    }
    _write_json_atomic(OUT_JSON, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
