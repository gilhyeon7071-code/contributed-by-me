from __future__ import annotations

import json
import hashlib
import os
import pickle
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from holiday_manager import HolidayManager
from utils.gate_audit import log_active_response_event

LOG_DIR = ROOT / "2_Logs"
SURGE_PARAMS_PATH = ROOT / "paper" / "surge_params.json"
PAPER_ENGINE_CONFIG_PATH = ROOT / "paper" / "paper_engine_config.json"
SURGE_POLICY_PROFILE_LOCK_PATH = ROOT / "paper" / "surge_policy_profile.lock.json"
PAPER_PRICES = ROOT / "paper" / "prices" / "ohlcv_paper.parquet"
INTRADAY_PRICES = LOG_DIR / "intraday_prices_latest.csv"
NEWS_SCORE_PATH = LOG_DIR / "candidates_latest_data.with_news_score.csv"
NEWS_DB_PATH = ROOT / "news_trading" / "data" / "trading.db"
ML_SCORE_PATH = LOG_DIR / "surge_ml_score_latest.csv"
ML_MODEL_PATH = ROOT / "_cache" / "surge_ml_model.pkl"
LOB_PATH = LOG_DIR / "surge_lob_latest.csv"
MARKET_RISING_PATH = LOG_DIR / "market_rising_latest.csv"
OUT_JSON = LOG_DIR / "surge_realtime_latest.json"
OUT_CSV = LOG_DIR / "surge_realtime_latest.csv"
CANDIDATES_FINAL_PATH = LOG_DIR / "candidates_latest_data.with_final_score.csv"
KRX_WATCHLIST_PATH = ROOT / "_cache" / "krx_watchlist_latest.csv"
P0_GLOB = "p0_daily_check_*.json"


def _history_paths(ts: str) -> Dict[str, Path]:
    try:
        stamp = datetime.fromisoformat(str(ts or "")).strftime("%Y%m%d_%H%M%S")
    except Exception:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return {
        "json": LOG_DIR / f"surge_realtime_{stamp}.json",
        "csv": LOG_DIR / f"surge_realtime_{stamp}.csv",
    }


def _write_outputs(payload: Dict[str, Any], out_df: pd.DataFrame, ts: str) -> None:
    paths = _history_paths(ts)
    payload["out_csv"] = str(OUT_CSV)
    payload["history_json"] = str(paths["json"])
    payload["history_csv"] = str(paths["csv"])
    json_text = json.dumps(payload, ensure_ascii=False, indent=2)
    _write_text_replace_with_retry(OUT_JSON, json_text, encoding="utf-8")
    paths["json"].write_text(json_text, encoding="utf-8")
    _write_csv_replace_with_retry(OUT_CSV, out_df, encoding="utf-8-sig")
    out_df.to_csv(paths["csv"], index=False, encoding="utf-8-sig")


def _replace_with_retry(tmp_path: Path, dest_path: Path, attempts: int = 12, delay_sec: float = 0.25) -> None:
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            os.replace(tmp_path, dest_path)
            return
        except PermissionError as exc:
            last_exc = exc
            if attempt >= attempts:
                break
            time.sleep(delay_sec)
    try:
        if tmp_path.exists():
            tmp_path.unlink()
    except Exception:
        pass
    if last_exc is not None:
        raise last_exc


def _write_text_replace_with_retry(path: Path, text: str, encoding: str) -> None:
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp_path.write_text(text, encoding=encoding)
    _replace_with_retry(tmp_path, path)


def _write_csv_replace_with_retry(path: Path, df: pd.DataFrame, encoding: str) -> None:
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    df.to_csv(tmp_path, index=False, encoding=encoding)
    _replace_with_retry(tmp_path, path)


def _to_float_env(name: str, default: float) -> float:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _to_int_env(name: str, default: int) -> int:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _to_bool_env(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "") or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "y", "yes", "on"}


def _safe_ratio(num: float, den: float) -> float:
    if den <= 0:
        return 0.0
    return float(num) / float(den)


def _cfg_float(v: Any, default: float) -> float:
    try:
        if v is None:
            return float(default)
        return float(v)
    except Exception:
        return float(default)


def _cfg_int(v: Any, default: int) -> int:
    try:
        if v is None:
            return int(default)
        return int(float(v))
    except Exception:
        return int(default)


def _load_surge_exit_observation_policy() -> Dict[str, Any]:
    try:
        raw = json.loads(PAPER_ENGINE_CONFIG_PATH.read_text(encoding="utf-8-sig"))
    except Exception as e:
        return {
            "source_status": "CONFIG_READ_ERROR",
            "source_path": str(PAPER_ENGINE_CONFIG_PATH),
            "source_error": f"{type(e).__name__}: {e}"[:240],
            "enabled": False,
            "stop_loss_pct": 0.0,
            "take_profit_pct": 0.0,
            "max_hold_days": 0,
            "stop_sell_ratio_pct": 0.0,
            "preemptive_sell_ratio_pct": 0.0,
            "dynamic_exit_ratio_enabled": False,
            "reversal_exit_enabled": False,
            "intraday_reversal_exit_enabled": False,
        }
    cfg = raw.get("surge_exit_policy") if isinstance(raw, dict) else {}
    if not isinstance(cfg, dict):
        cfg = {}
    dynamic_cfg = cfg.get("dynamic_exit_ratio") if isinstance(cfg.get("dynamic_exit_ratio"), dict) else {}
    reversal_cfg = cfg.get("reversal_exit") if isinstance(cfg.get("reversal_exit"), dict) else {}
    intraday_reversal_cfg = cfg.get("intraday_reversal_exit") if isinstance(cfg.get("intraday_reversal_exit"), dict) else {}
    return {
        "source_status": "LOADED",
        "source_path": str(PAPER_ENGINE_CONFIG_PATH),
        "source_error": "",
        "enabled": bool(cfg.get("enabled", False)),
        "stop_loss_pct": _cfg_float(cfg.get("stop_loss_pct"), 0.0),
        "take_profit_pct": _cfg_float(cfg.get("take_profit_pct"), 0.0),
        "max_hold_days": _cfg_int(cfg.get("max_hold_days"), 0),
        "stop_sell_ratio_pct": _cfg_float(cfg.get("stop_sell_ratio_pct"), 0.0),
        "preemptive_sell_ratio_pct": _cfg_float(
            cfg.get("preemptive_sell_ratio_pct", cfg.get("stop_sell_ratio_pct")),
            0.0,
        ),
        "dynamic_exit_ratio_enabled": bool(dynamic_cfg.get("enabled", False)),
        "reversal_exit_enabled": bool(reversal_cfg.get("enabled", False)),
        "intraday_reversal_exit_enabled": bool(intraday_reversal_cfg.get("enabled", False)),
    }


def _surge_policy_profile_lock_status() -> Dict[str, Any]:
    env_keys = [
        "SURGE_RT_PAPER_DATA_COLLECTION",
        "SURGE_RT_PAPER_LIMIT_NEAR_IGNORE_ENTRY_CAPS",
        "SURGE_RT_PAPER_HIGH_REJECTION_ENTRY_BLOCK_PCT",
        "SURGE_RT_PAPER_ALLOW_KRX_CAUTION",
    ]
    env_snapshot = {k: str(os.getenv(k, "") or "").strip() for k in env_keys}
    enabled_requested = env_snapshot.get("SURGE_RT_PAPER_DATA_COLLECTION") == "1"
    if not enabled_requested:
        return {
            "status": "NOT_REQUESTED",
            "enabled_requested": False,
            "path": str(SURGE_POLICY_PROFILE_LOCK_PATH),
            "current_env_sha256": "",
            "approved_env_sha256": "",
            "profile": "",
            "rules": {},
        }
    canonical = json.dumps(env_snapshot, sort_keys=True, separators=(",", ":"))
    cur_sha = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    try:
        lock = json.loads(SURGE_POLICY_PROFILE_LOCK_PATH.read_text(encoding="utf-8-sig"))
    except Exception as e:
        return {
            "status": "MISSING_OR_INVALID",
            "enabled_requested": True,
            "path": str(SURGE_POLICY_PROFILE_LOCK_PATH),
            "current_env_sha256": cur_sha,
            "approved_env_sha256": "",
            "profile": "",
            "reason": f"{type(e).__name__}: {e}"[:240],
        }
    approved_sha = str(lock.get("approved_env_sha256") or "").strip()
    approved_env = lock.get("env") if isinstance(lock.get("env"), dict) else {}
    approved_env_norm = {k: str(approved_env.get(k, "") or "").strip() for k in env_keys}
    approved_canonical = json.dumps(approved_env_norm, sort_keys=True, separators=(",", ":"))
    approved_recalc = hashlib.sha256(approved_canonical.encode("utf-8")).hexdigest()
    ok = bool(approved_sha and cur_sha == approved_sha and approved_recalc == approved_sha)
    return {
        "status": "LOCK_OK" if ok else "LOCK_MISMATCH",
        "enabled_requested": True,
        "path": str(SURGE_POLICY_PROFILE_LOCK_PATH),
        "current_env_sha256": cur_sha,
        "approved_env_sha256": approved_sha,
        "approved_recalc_sha256": approved_recalc,
        "profile": str(lock.get("profile") or ""),
        "rules": lock.get("rules") if isinstance(lock.get("rules"), dict) else {},
    }


def _prev_weekday_ymd(today_ymd: str) -> str:
    return HolidayManager().previous_trading_day(str(today_ymd))


def _is_xkrx_session_ymd(ymd: str) -> bool:
    return HolidayManager().is_market_open(str(ymd))


def _expected_intraday_ymd(system_ymd: str) -> str:
    system_ymd = str(system_ymd or "").strip()
    if len(system_ymd) == 8 and _is_xkrx_session_ymd(system_ymd):
        return system_ymd
    return _prev_weekday_ymd(system_ymd)


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
            if len(ymd) == 8 and ymd == expected and _is_xkrx_session_ymd(ymd):
                return ymd
        except Exception:
            pass
    return expected


def _load_surge_params() -> Dict[str, float]:
    raw: Dict[str, Any] = {}
    if SURGE_PARAMS_PATH.exists():
        try:
            loaded = json.loads(SURGE_PARAMS_PATH.read_text(encoding="utf-8-sig"))
            if isinstance(loaded, dict):
                raw = loaded
        except Exception:
            raw = {}

    def _pick_float(key: str, env_name: str, default: float) -> float:
        v = raw.get(key)
        if isinstance(v, (int, float)):
            return float(v)
        return _to_float_env(env_name, default)

    def _pick_int(key: str, env_name: str, default: int) -> int:
        v = raw.get(key)
        if isinstance(v, int):
            return int(v)
        if isinstance(v, float):
            return int(v)
        return _to_int_env(env_name, default)

    policy_lock = _surge_policy_profile_lock_status()
    paper_lock_ok = policy_lock.get("status") == "LOCK_OK"

    return {
        "pct_min": _pick_float("pct_min", "SURGE_RT_PCT_MIN", 0.05),
        "range_min": _pick_float("range_min", "SURGE_RT_RANGE_PCT_MIN", 0.08),
        "rvol20_min": _pick_float("rvol20_min", "SURGE_RT_RVOL20_MIN", 2.0),
        "trading_value_floor": _pick_float("trading_value_floor", "SURGE_RT_TRADING_VALUE_FLOOR", 1_000_000_000.0),
        "range_rvol20_min": _pick_float("range_rvol20_min", "SURGE_RT_RANGE_RVOL20_MIN", 1.2),
        "reg_short_5d_min": _pick_float("reg_short_5d_min", "SURGE_RT_REG_SHORT_5D_MIN", 0.60),
        "reg_mid_15d_min": _pick_float("reg_mid_15d_min", "SURGE_RT_REG_MID_15D_MIN", 1.00),
        "max_spread_bps": _pick_float("max_spread_bps", "SURGE_RT_MAX_SPREAD_BPS", 80.0),
        "limit_near_pct": _pick_float("limit_near_pct", "SURGE_RT_LIMIT_NEAR_PCT", 0.22),
        "imbalance_bonus_thr": _pick_float("imbalance_bonus_thr", "SURGE_RT_IMBALANCE_BONUS_THR", 0.30),
        "imbalance_bonus_pts": _pick_float("imbalance_bonus_pts", "SURGE_RT_IMBALANCE_BONUS_PTS", 10.0),
        "max_alerts": max(1, _pick_int("max_alerts", "SURGE_RT_MAX_ALERTS", 20)),
        "weight_rule": _pick_float("weight_rule", "SURGE_RT_RULE_WEIGHT", 0.65),
        "weight_news": _pick_float("weight_news", "SURGE_RT_NEWS_WEIGHT", 0.08),
        "weight_ml": _pick_float("weight_ml", "SURGE_RT_ML_WEIGHT", 0.27),
        "news_block_threshold": _pick_float("news_block_threshold", "SURGE_RT_NEWS_BLOCK_THRESHOLD", -0.30),
        "news_bonus_threshold": _pick_float("news_bonus_threshold", "SURGE_RT_NEWS_BONUS_THRESHOLD", 0.30),
        "news_bonus_pts": _pick_float("news_bonus_pts", "SURGE_RT_NEWS_BONUS_PTS", 5.0),
        "max_entry_change_pct": _pick_float("max_entry_change_pct", "SURGE_RT_MAX_ENTRY_CHANGE_PCT", 0.16),
        "max_open_to_entry_chase_pct": _pick_float(
            "max_open_to_entry_chase_pct",
            "SURGE_RT_MAX_OPEN_TO_ENTRY_CHASE_PCT",
            0.0,
        ),
        "high_rejection_entry_block_pct": _pick_float(
            "high_rejection_entry_block_pct",
            "SURGE_RT_HIGH_REJECTION_ENTRY_BLOCK_PCT",
            0.08,
        ),
        "max_rvol20": _pick_float("max_rvol20", "SURGE_RT_MAX_RVOL20", 0.0),
        "overheat_score_min": _pick_float("overheat_score_min", "SURGE_RT_OVERHEAT_SCORE_MIN", 0.0),
        "overheat_rvol20_min": _pick_float("overheat_rvol20_min", "SURGE_RT_OVERHEAT_RVOL20_MIN", 0.0),
        "atr_entry_cap_enabled": bool(raw.get("atr_entry_cap_enabled", _to_bool_env("SURGE_RT_ATR_ENTRY_CAP_ENABLED", False))),
        "entry_change_pct_multiplier": _pick_float("entry_change_pct_multiplier", "SURGE_RT_ENTRY_CHANGE_PCT_MULTIPLIER", 2.0),
        "atr_entry_cap_multiplier": _pick_float("atr_entry_cap_multiplier", "SURGE_RT_ATR_ENTRY_CAP_MULTIPLIER", 3.0),
        "no_lob_probe": raw.get("no_lob_probe") if isinstance(raw.get("no_lob_probe"), dict) else {},
        "paper_probe": raw.get("paper_probe") if isinstance(raw.get("paper_probe"), dict) else {},
        "paper_data_collection_enabled": bool(
            paper_lock_ok and raw.get("paper_data_collection_enabled", _to_bool_env("SURGE_RT_PAPER_DATA_COLLECTION", False))
        ),
        "paper_limit_near_ignore_entry_caps": bool(
            paper_lock_ok
            and raw.get("paper_limit_near_ignore_entry_caps", _to_bool_env("SURGE_RT_PAPER_LIMIT_NEAR_IGNORE_ENTRY_CAPS", False))
        ),
        "paper_high_rejection_entry_block_pct": _pick_float(
            "paper_high_rejection_entry_block_pct",
            "SURGE_RT_PAPER_HIGH_REJECTION_ENTRY_BLOCK_PCT",
            0.0,
        ),
        "paper_allow_krx_caution": bool(
            paper_lock_ok and raw.get("paper_allow_krx_caution", _to_bool_env("SURGE_RT_PAPER_ALLOW_KRX_CAUTION", False))
        ),
        "paper_policy_profile_lock": policy_lock,
        "orderflow_risk_block_threshold": _pick_float("orderflow_risk_block_threshold", "SURGE_RT_ORDERFLOW_RISK_BLOCK_THRESHOLD", 0.0),
        "extreme_imbalance_block_min": _pick_float("extreme_imbalance_block_min", "SURGE_RT_EXTREME_IMBALANCE_BLOCK_MIN", 0.0),
        "kyle_lambda_z_block_min": _pick_float("kyle_lambda_z_block_min", "SURGE_RT_KYLE_LAMBDA_Z_BLOCK_MIN", 0.0),
        "markout_1step_bps_block_max": _pick_float("markout_1step_bps_block_max", "SURGE_RT_MARKOUT_1STEP_BPS_BLOCK_MAX", 0.0),
        "ofi_norm_abs_block_min": _pick_float("ofi_norm_abs_block_min", "SURGE_RT_OFI_NORM_ABS_BLOCK_MIN", 0.0),
        "exclude_krx_admin": bool(raw.get("exclude_krx_admin", _to_bool_env("SURGE_RT_EXCLUDE_KRX_ADMIN", True))),
        "exclude_krx_warning": bool(raw.get("exclude_krx_warning", _to_bool_env("SURGE_RT_EXCLUDE_KRX_WARNING", True))),
        "exclude_krx_risk": bool(raw.get("exclude_krx_risk", _to_bool_env("SURGE_RT_EXCLUDE_KRX_RISK", True))),
        "exclude_krx_caution": bool(raw.get("exclude_krx_caution", _to_bool_env("SURGE_RT_EXCLUDE_KRX_CAUTION", True))),
        "exclude_no_trade_activity": bool(raw.get("exclude_no_trade_activity", _to_bool_env("SURGE_RT_EXCLUDE_NO_TRADE", True))),
        "exclude_junk_score_min": _pick_float("exclude_junk_score_min", "SURGE_RT_EXCLUDE_JUNK_SCORE_MIN", 80.0),
        "exclude_junk_grades": str(raw.get("exclude_junk_grades", os.getenv("SURGE_RT_EXCLUDE_JUNK_GRADES", "HIGH,CRITICAL")) or "HIGH,CRITICAL"),
        "bt_ml_prob_min": _pick_float("bt_ml_prob_min", "SURGE_RT_ML_PROB_MIN", 0.0),
        "max_krx_daily_move_pct": _pick_float("max_krx_daily_move_pct", "SURGE_RT_MAX_KRX_DAILY_MOVE_PCT", 0.305),
    }


def _load_intraday() -> pd.DataFrame:
    df = pd.read_csv(INTRADAY_PRICES, dtype={"code": str})
    for c in ["current_price", "open", "high", "low", "volume", "trading_value"]:
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["date"] = df["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str.slice(0, 8)
    today_ymd = str(df["date"].dropna().astype(str).max()) if len(df) else ""
    rising = _load_market_rising_supplement(today_ymd)
    if not rising.empty:
        df = pd.concat([df, rising], ignore_index=True)
        df = df.drop_duplicates(subset=["code"], keep="last").copy()
    return df


def _load_market_rising_supplement(today_ymd: str) -> pd.DataFrame:
    if len(str(today_ymd or "")) != 8 or not MARKET_RISING_PATH.exists():
        return pd.DataFrame()
    try:
        mdf = pd.read_csv(MARKET_RISING_PATH, dtype={"code": str})
    except Exception:
        return pd.DataFrame()
    if mdf.empty or "code" not in mdf.columns:
        return pd.DataFrame()

    out = pd.DataFrame()
    out["code"] = mdf["code"].astype(str).str.zfill(6)
    out["date"] = str(today_ymd)
    column_map = {
        "current_price": ["current_price", "price"],
        "open": ["open", "open_price", "current_price", "price"],
        "high": ["high", "high_price", "current_price", "price"],
        "low": ["low", "low_price", "current_price", "price"],
        "volume": ["volume", "acml_vol"],
        "trading_value": ["trading_value", "value"],
    }
    for out_col, candidates in column_map.items():
        src = next((c for c in candidates if c in mdf.columns), None)
        if src is None:
            out[out_col] = 0.0
        else:
            out[out_col] = pd.to_numeric(mdf[src], errors="coerce").fillna(0.0)
    tv_zero = pd.to_numeric(out["trading_value"], errors="coerce").fillna(0.0) <= 0
    px = pd.to_numeric(out["current_price"], errors="coerce").fillna(0.0)
    vol = pd.to_numeric(out["volume"], errors="coerce").fillna(0.0)
    out.loc[tv_zero & (px > 0) & (vol > 0), "trading_value"] = px * vol
    out = out[out["code"].str.len() == 6].copy()
    out = out[out["current_price"] > 0].copy()
    return out


def _load_reference(today_ymd: str) -> pd.DataFrame:
    px = pd.read_parquet(PAPER_PRICES)
    px = px.copy()
    px["code"] = px["code"].astype(str).str.zfill(6)
    px["date"] = px["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str.slice(0, 8)
    for c in ("open", "high", "low", "close", "volume"):
        if c not in px.columns:
            px[c] = 0.0
        px[c] = pd.to_numeric(px[c], errors="coerce").fillna(0.0)
    px = px[px["date"] < str(today_ymd)].copy()
    return px


def _build_reference_map(px: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if px.empty:
        return out

    px = px.sort_values(["code", "date"])
    for code, g in px.groupby("code", sort=False):
        gg = g.tail(20)
        prev_close = float(gg["close"].iloc[-1]) if len(gg) > 0 else 0.0
        avg_vol20 = float(gg["volume"].mean()) if len(gg) > 0 else 0.0
        closes = gg["close"].astype(float).tolist()
        highs = gg["high"].astype(float).tolist()
        lows = gg["low"].astype(float).tolist()
        vols = gg["volume"].astype(float).tolist()
        latest_date = str(gg["date"].iloc[-1]) if len(gg) > 0 else ""
        out[str(code)] = {
            "prev_close": prev_close,
            "avg_vol20": avg_vol20,
            "closes": closes,
            "highs": highs,
            "lows": lows,
            "volumes": vols,
            "latest_date": latest_date,
        }
    return out


def _norm_code_text(v: Any) -> str:
    digits = "".join(ch for ch in str(v or "") if ch.isdigit())
    return digits[-6:].zfill(6) if digits else ""


def _load_signal_news_fallback(today_ymd: str, existing_scores: Dict[str, float]) -> Dict[str, float]:
    if not NEWS_DB_PATH.exists():
        return {}
    ymd = "".join(ch for ch in str(today_ymd or "") if ch.isdigit())[:8]
    if len(ymd) != 8:
        return {}
    try:
        con = sqlite3.connect(str(NEWS_DB_PATH))
        try:
            sdf = pd.read_sql_query(
                """
                SELECT code, news_score, headline_count, article_count
                FROM signals_naver_daily
                WHERE date8 = ?
                """,
                con,
                params=(ymd,),
            )
        finally:
            con.close()
    except Exception:
        return {}
    if sdf.empty or "code" not in sdf.columns or "news_score" not in sdf.columns:
        return {}

    sdf = sdf.copy()
    sdf["code"] = sdf["code"].map(_norm_code_text)
    sdf["news_score"] = pd.to_numeric(sdf["news_score"], errors="coerce").fillna(0.0)
    for col in ("headline_count", "article_count"):
        if col not in sdf.columns:
            sdf[col] = 0
        sdf[col] = pd.to_numeric(sdf[col], errors="coerce").fillna(0.0)
    sdf = sdf[sdf["code"].str.len() == 6].copy()
    sdf = sdf.sort_values(["code", "article_count", "headline_count"], ascending=[True, False, False])
    sdf = sdf.drop_duplicates("code", keep="first")
    out: Dict[str, float] = {}
    for _, r in sdf.iterrows():
        code = str(r["code"])
        score = float(r["news_score"])
        old = existing_scores.get(code)
        if old is None or abs(float(old)) <= 1e-9:
            out[code] = score
    return out


def _load_news_score_map(today_ymd: str = "") -> Dict[str, float]:
    out: Dict[str, float] = {}
    if NEWS_SCORE_PATH.exists():
        try:
            ndf = pd.read_csv(NEWS_SCORE_PATH, dtype={"code": str})
        except Exception:
            ndf = pd.DataFrame()
        if not ndf.empty and "code" in ndf.columns and "news_score" in ndf.columns:
            ndf = ndf.copy()
            ndf["code"] = ndf["code"].map(_norm_code_text)
            ndf["news_score"] = pd.to_numeric(ndf["news_score"], errors="coerce").fillna(0.0)
            ndf = ndf[ndf["code"].str.len() == 6].copy()
            out.update({str(r["code"]): float(r["news_score"]) for _, r in ndf.iterrows()})

    fallback = _load_signal_news_fallback(today_ymd=today_ymd, existing_scores=out)
    out.update(fallback)
    return out



def _load_news_implication_map() -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    if not NEWS_SCORE_PATH.exists():
        return out
    try:
        ndf = pd.read_csv(NEWS_SCORE_PATH, dtype={"code": str})
    except Exception:
        return out
    if ndf.empty or "code" not in ndf.columns:
        return out
    action_col = "news_implication_top_actions"
    scope_col = "news_implication_top_scopes"
    rows_col = "news_implication_rows"
    if action_col not in ndf.columns and scope_col not in ndf.columns:
        return out
    work = ndf.copy()
    work["code"] = work["code"].map(_norm_code_text)
    work = work[work["code"].str.len() == 6].copy()
    for _, r in work.iterrows():
        code = str(r.get("code") or "")
        if not code:
            continue
        out[code] = {
            "actions": str(r.get(action_col) or "").strip(),
            "scopes": str(r.get(scope_col) or "").strip(),
            "rows": str(r.get(rows_col) or "0").strip(),
        }
    return out


def _news_implication_has_action(payload: Dict[str, str], action_name: str) -> bool:
    target = str(action_name or "").strip().lower()
    raw = str((payload or {}).get("actions") or "").strip().lower()
    if not target or not raw:
        return False
    for part in raw.split("|"):
        label = part.split(":", 1)[0].strip().lower()
        if label == target:
            return True
    return False

def _load_ml_score_map() -> Dict[str, float]:
    if not ML_SCORE_PATH.exists():
        return {}
    try:
        mdf = pd.read_csv(ML_SCORE_PATH, dtype={"code": str})
    except Exception:
        return {}
    if "code" not in mdf.columns:
        return {}
    score_col = "surge_ml_prob"
    if score_col not in mdf.columns:
        return {}
    mdf = mdf.copy()
    mdf["code"] = mdf["code"].astype(str).str.zfill(6)
    mdf[score_col] = pd.to_numeric(mdf[score_col], errors="coerce").fillna(0.0)
    return {str(r["code"]): float(r[score_col]) for _, r in mdf.iterrows()}


def _load_ml_feature_map() -> Dict[str, Dict[str, float]]:
    if not ML_SCORE_PATH.exists():
        return {}
    try:
        mdf = pd.read_csv(ML_SCORE_PATH, dtype={"code": str})
    except Exception:
        return {}
    if "code" not in mdf.columns:
        return {}
    mdf = mdf.copy()
    mdf["code"] = mdf["code"].astype(str).str.zfill(6)
    for c in ["atr14_pct"]:
        if c not in mdf.columns:
            mdf[c] = 0.0
        mdf[c] = pd.to_numeric(mdf[c], errors="coerce").fillna(0.0)
    return {
        str(r["code"]): {
            "atr14_pct": float(r.get("atr14_pct", 0.0) or 0.0),
        }
        for _, r in mdf.iterrows()
    }


def _load_ml_bundle() -> tuple[Dict[str, Any] | None, str]:
    if not ML_MODEL_PATH.exists():
        return None, "MISSING_MODEL"
    try:
        with ML_MODEL_PATH.open("rb") as f:
            bundle = pickle.load(f)
        if not isinstance(bundle, dict):
            return None, "INVALID_MODEL_BUNDLE"
        return bundle, "OK"
    except ModuleNotFoundError as e:
        return None, f"MISSING_RUNTIME_DEP:{e}"
    except Exception as e:
        return None, f"MODEL_LOAD_ERROR:{type(e).__name__}:{e}"


def _predict_ml_prob_from_features(bundle: Dict[str, Any] | None, feat: Dict[str, float]) -> tuple[float, str]:
    if not bundle:
        return 0.0, "MISSING_MODEL"
    features: List[str] = list(bundle.get("features") or [])
    if not features:
        return 0.0, "INVALID_MODEL_FEATURES"
    try:
        x = pd.DataFrame([{k: float(feat.get(k, 0.0)) for k in features}])[features]
        model_type = str(bundle.get("model_type", ""))
        if model_type == "linear_stat":
            coef_map = bundle.get("coef") if isinstance(bundle.get("coef"), dict) else {}
            coef = pd.Series({k: float(coef_map.get(k, 0.0)) for k in features}, dtype=float)
            intercept = float(bundle.get("intercept", 0.0) or 0.0)
            raw = float(x.mul(coef, axis=1).sum(axis=1).iloc[0] + intercept)
            prob = float(1.0 / (1.0 + np.exp(-np.clip(raw, -50.0, 50.0))))
            return prob, "detector_model_fallback"
        if model_type == "linear_logreg_fallback":
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
            return prob, "detector_model_fallback"
        model = bundle.get("model")
        if model is None:
            return 0.0, "MISSING_MODEL_OBJECT"
        return float(model.predict_proba(x)[0, 1]), "detector_model_fallback"
    except Exception as e:
        return 0.0, f"MODEL_PREDICT_ERROR:{type(e).__name__}"


def _load_lob_map() -> Dict[str, Dict[str, float]]:
    if not LOB_PATH.exists():
        return {}
    try:
        ldf = pd.read_csv(LOB_PATH, dtype={"code": str})
    except Exception:
        return {}
    if "code" not in ldf.columns:
        return {}
    ldf = ldf.copy()
    ldf["code"] = ldf["code"].astype(str).str.zfill(6)
    depth_cols = (
        [f"ask{i}" for i in range(1, 11)]
        + [f"bid{i}" for i in range(1, 11)]
        + [f"askq{i}" for i in range(1, 11)]
        + [f"bidq{i}" for i in range(1, 11)]
        + ["ask_depth_levels"]
    )
    for c in ["spread_bps", "order_imbalance_l1", "ofi_norm", "kyle_lambda_z", "markout_1step_bps", "orderflow_risk_score"] + depth_cols:
        if c not in ldf.columns:
            ldf[c] = 0.0
        ldf[c] = pd.to_numeric(ldf[c], errors="coerce").fillna(0.0)
    # lob_available: ask1>0 & bid1>0 ??written by surge_lob_ingest, fallback to derivation
    if "lob_available" in ldf.columns:
        ldf["lob_available"] = ldf["lob_available"].astype(str).str.lower().isin(["true", "1"])
    else:
        ldf["lob_available"] = (ldf["ask1"] > 0) & (ldf["bid1"] > 0)
    out: Dict[str, Dict[str, float]] = {}
    for _, r in ldf.iterrows():
        rec = {
            "spread_bps": float(r.get("spread_bps", 0.0)),
            "order_imbalance_l1": float(r.get("order_imbalance_l1", 0.0)),
            "ask1": float(r.get("ask1", 0.0)),
            "bid1": float(r.get("bid1", 0.0)),
            "lob_available": bool(r.get("lob_available", False)),
            "ofi_norm": float(r.get("ofi_norm", 0.0)),
            "kyle_lambda_z": float(r.get("kyle_lambda_z", 0.0)),
            "markout_1step_bps": float(r.get("markout_1step_bps", 0.0)),
            "orderflow_risk_score": float(r.get("orderflow_risk_score", 0.0)),
            "orderflow_tag": str(r.get("orderflow_tag", "") or ""),
        }
        for c in depth_cols:
            rec[c] = float(r.get(c, 0.0) or 0.0)
        out[str(r["code"])] = rec
    return out


def _load_candidate_guard_map() -> Dict[str, Dict[str, Any]]:
    if not CANDIDATES_FINAL_PATH.exists():
        return {}
    try:
        cdf = pd.read_csv(CANDIDATES_FINAL_PATH, dtype={"code": str})
    except Exception:
        return {}
    if "code" not in cdf.columns:
        return {}
    cdf = cdf.copy()
    cdf["code"] = cdf["code"].astype(str).str.zfill(6)

    def _as_bool(v: Any) -> bool:
        s = str(v).strip().lower()
        return s in {"1", "true", "t", "yes", "y"}

    out: Dict[str, Dict[str, Any]] = {}
    for _, r in cdf.iterrows():
        code = str(r.get("code", "")).zfill(6)
        if not code:
            continue
        out[code] = {
            "krx_admin": _as_bool(r.get("krx_admin", False)),
            "krx_warning": _as_bool(r.get("krx_warning", False)),
            "krx_risk": _as_bool(r.get("krx_risk", False)),
            "krx_caution": _as_bool(r.get("krx_caution", False)),
            "junk_risk_grade": str(r.get("junk_risk_grade", "") or "").strip().upper(),
            "junk_risk_score": float(pd.to_numeric(r.get("junk_risk_score"), errors="coerce") if r.get("junk_risk_score") is not None else 0.0) if str(r.get("junk_risk_score", "")).strip() != "" else 0.0,
        }
    return out


def _load_krx_watchlist_guard_map() -> Dict[str, Dict[str, Any]]:
    if not KRX_WATCHLIST_PATH.exists():
        return {}
    try:
        wdf = pd.read_csv(KRX_WATCHLIST_PATH, dtype={"code": str})
    except Exception:
        return {}
    if "code" not in wdf.columns:
        return {}
    wdf = wdf.copy()
    wdf["code"] = wdf["code"].astype(str).str.zfill(6)

    def _as_bool(v: Any) -> bool:
        s = str(v).strip().lower()
        return s in {"1", "true", "t", "yes", "y"}

    out: Dict[str, Dict[str, Any]] = {}
    for _, r in wdf.iterrows():
        code = str(r.get("code", "")).zfill(6)
        if not code:
            continue
        out[code] = {
            "krx_admin": _as_bool(r.get("krx_admin", False)),
            "krx_warning": _as_bool(r.get("krx_warning", False)),
            "krx_risk": _as_bool(r.get("krx_risk", False)),
            "krx_caution": _as_bool(r.get("krx_caution", False)),
        }
    return out


def main() -> int:
    ts = datetime.now().isoformat(timespec="seconds")
    if not INTRADAY_PRICES.exists():
        payload = {
            "ts": ts,
            "status": "MISSING_INTRADAY_PRICES",
            "reason": str(INTRADAY_PRICES),
            "alerts_count": 0,
            "alerts": [],
        }
        _write_outputs(payload, pd.DataFrame(), ts)
        # [2026-09-13] **rc=1 인데 출력이 0바이트였다.** 배치 로그에는 'step failed' 만 남고
        #   사유는 산출물 json 을 따로 열어봐야 알 수 있었다. 원인을 지우지 않는다.
        print('[SURGE_DETECTOR_ABORT] status=%s reason=%s'
              % (payload.get('status'), payload.get('reason')), flush=True)
        return 1
    if not PAPER_PRICES.exists():
        payload = {
            "ts": ts,
            "status": "MISSING_REFERENCE_PRICES",
            "reason": str(PAPER_PRICES),
            "alerts_count": 0,
            "alerts": [],
        }
        _write_outputs(payload, pd.DataFrame(), ts)
        # [2026-09-13] **rc=1 인데 출력이 0바이트였다.** 배치 로그에는 'step failed' 만 남고
        #   사유는 산출물 json 을 따로 열어봐야 알 수 있었다. 원인을 지우지 않는다.
        print('[SURGE_DETECTOR_ABORT] status=%s reason=%s'
              % (payload.get('status'), payload.get('reason')), flush=True)
        return 1

    params = _load_surge_params()
    exit_observation_policy = _load_surge_exit_observation_policy()
    pct_min = float(params["pct_min"])
    range_min = float(params["range_min"])
    rvol20_min = float(params["rvol20_min"])
    trading_value_floor = max(0.0, float(params.get("trading_value_floor", 0.0) or 0.0))
    range_rvol20_min = float(params["range_rvol20_min"])
    reg_short_5d_min = float(params["reg_short_5d_min"])
    reg_mid_15d_min = float(params["reg_mid_15d_min"])
    max_spread_bps = float(params["max_spread_bps"])
    limit_near_pct = float(params["limit_near_pct"])
    imbalance_bonus_thr = float(params["imbalance_bonus_thr"])
    imbalance_bonus_pts = float(params["imbalance_bonus_pts"])
    max_alerts = int(params["max_alerts"])
    w_rule = float(params["weight_rule"])
    w_news = float(params["weight_news"])
    w_ml = float(params["weight_ml"])
    news_block_threshold = float(params.get("news_block_threshold", -0.30) or -0.30)
    news_bonus_threshold = float(params.get("news_bonus_threshold", 0.30) or 0.30)
    news_bonus_pts = float(params.get("news_bonus_pts", 5.0) or 5.0)
    max_entry_change_pct = float(params.get("max_entry_change_pct", 0.0) or 0.0)
    max_open_to_entry_chase_pct = float(params.get("max_open_to_entry_chase_pct", 0.0) or 0.0)
    high_rejection_entry_block_pct = abs(float(params.get("high_rejection_entry_block_pct", 0.0) or 0.0))
    max_rvol20 = float(params.get("max_rvol20", 0.0) or 0.0)
    overheat_score_min = float(params.get("overheat_score_min", 0.0) or 0.0)
    overheat_rvol20_min = float(params.get("overheat_rvol20_min", 0.0) or 0.0)
    atr_entry_cap_enabled = bool(params.get("atr_entry_cap_enabled", False))
    entry_change_pct_multiplier = float(params.get("entry_change_pct_multiplier", 2.0) or 2.0)
    atr_entry_cap_multiplier = float(params.get("atr_entry_cap_multiplier", 3.0) or 3.0)
    no_lob_probe_cfg = params.get("no_lob_probe", {}) if isinstance(params.get("no_lob_probe"), dict) else {}
    no_lob_probe_enabled = bool(no_lob_probe_cfg.get("enabled", False))
    no_lob_probe_max_change_pct = float(no_lob_probe_cfg.get("max_change_pct", 0.0) or 0.0)
    no_lob_probe_min_rvol20 = float(no_lob_probe_cfg.get("min_rvol20", 0.0) or 0.0)
    no_lob_probe_min_trading_value = float(no_lob_probe_cfg.get("min_trading_value", 0.0) or 0.0)
    no_lob_probe_min_score_final = float(no_lob_probe_cfg.get("min_score_final", 0.0) or 0.0)
    no_lob_probe_allow_limit_near = bool(no_lob_probe_cfg.get("allow_limit_near", False))
    paper_probe_cfg = params.get("paper_probe", {}) if isinstance(params.get("paper_probe"), dict) else {}
    paper_probe_enabled = bool(paper_probe_cfg.get("enabled", False))
    paper_probe_max_selected = max(0, int(paper_probe_cfg.get("max_selected", 0) or 0))
    paper_probe_min_score_final = float(paper_probe_cfg.get("min_score_final", 0.0) or 0.0)
    paper_probe_allowed_types = {
        str(v).strip().upper()
        for v in (paper_probe_cfg.get("allowed_types") or [])
        if str(v).strip()
    }
    paper_probe_allowed_block_reasons = {
        str(v).strip().upper()
        for v in (paper_probe_cfg.get("allowed_block_reasons") or [])
        if str(v).strip()
    }
    paper_probe_blocked_block_reasons = {
        str(v).strip().upper()
        for v in (paper_probe_cfg.get("blocked_block_reasons") or [])
        if str(v).strip()
    }
    paper_probe_max_markout_negative_bps = float(paper_probe_cfg.get("max_markout_negative_bps", 0.0) or 0.0)
    paper_probe_high_rejection_floor_pct = float(paper_probe_cfg.get("high_rejection_floor_pct", 0.0) or 0.0)
    paper_data_collection_enabled = bool(params.get("paper_data_collection_enabled", False))
    paper_limit_near_ignore_entry_caps = bool(params.get("paper_limit_near_ignore_entry_caps", False))
    paper_high_rejection_entry_block_pct = float(params.get("paper_high_rejection_entry_block_pct", 0.0) or 0.0)
    paper_allow_krx_caution = bool(params.get("paper_allow_krx_caution", False))
    paper_policy_profile_lock = params.get("paper_policy_profile_lock", {})
    if not isinstance(paper_policy_profile_lock, dict):
        paper_policy_profile_lock = {}
    _profile_rules = paper_policy_profile_lock.get("rules", {})
    if not isinstance(_profile_rules, dict):
        _profile_rules = {}
    profile_still_blocked = {
        str(v).strip().upper()
        for v in (_profile_rules.get("still_blocked") or [])
        if str(v).strip()
    }
    no_lob_probe_profile_blocked = "NO_LOB_BLOCK" in profile_still_blocked
    no_lob_probe_lock_ok = paper_policy_profile_lock.get("status") == "LOCK_OK"
    if (not no_lob_probe_lock_ok) or no_lob_probe_profile_blocked:
        no_lob_probe_enabled = False
    orderflow_risk_block_threshold = float(params.get("orderflow_risk_block_threshold", 0.0) or 0.0)
    extreme_imbalance_block_min = float(params.get("extreme_imbalance_block_min", 0.0) or 0.0)
    kyle_lambda_z_block_min = float(params.get("kyle_lambda_z_block_min", 0.0) or 0.0)
    markout_1step_bps_block_max = float(params.get("markout_1step_bps_block_max", 0.0) or 0.0)
    ofi_norm_abs_block_min = float(params.get("ofi_norm_abs_block_min", 0.0) or 0.0)
    exclude_krx_admin = bool(params.get("exclude_krx_admin", True))
    exclude_krx_warning = bool(params.get("exclude_krx_warning", True))
    exclude_krx_risk = bool(params.get("exclude_krx_risk", True))
    exclude_krx_caution = bool(params.get("exclude_krx_caution", True))
    exclude_no_trade_activity = bool(params.get("exclude_no_trade_activity", True))
    exclude_junk_score_min = float(params.get("exclude_junk_score_min", 80.0) or 80.0)
    exclude_junk_grades_raw = str(params.get("exclude_junk_grades", "HIGH,CRITICAL") or "HIGH,CRITICAL")
    exclude_junk_grades = {x.strip().upper() for x in exclude_junk_grades_raw.split(",") if str(x).strip()}
    max_krx_daily_move_pct = float(params.get("max_krx_daily_move_pct", 0.305) or 0.305)

    rt = _load_intraday()
    today_ymd = str(rt["date"].astype(str).max()) if len(rt) else datetime.now().strftime("%Y%m%d")
    system_today = datetime.now().strftime("%Y%m%d")
    expected_intraday_ymd = _expected_intraday_ymd(system_today)
    if today_ymd != expected_intraday_ymd:
        payload = {
            "ts": ts,
            "status": "STALE_INTRADAY_DATE",
            "reason": f"intraday_date={today_ymd} expected_intraday_date={expected_intraday_ymd} system_today={system_today}",
            "intraday_date": today_ymd,
            "expected_intraday_date": expected_intraday_ymd,
            "system_today": system_today,
            "alerts_count": 0,
            "alerts": [],
        }
        _write_outputs(payload, pd.DataFrame(), ts)
        # [2026-09-13] **rc=1 인데 출력이 0바이트였다.** 배치 로그에는 'step failed' 만 남고
        #   사유는 산출물 json 을 따로 열어봐야 알 수 있었다. 원인을 지우지 않는다.
        print('[SURGE_DETECTOR_ABORT] status=%s reason=%s'
              % (payload.get('status'), payload.get('reason')), flush=True)
        return 1
    expected_prev_weekday = _load_expected_prev_weekday(today_ymd)
    ref = _load_reference(today_ymd)
    ref_map = _build_reference_map(ref)
    news_map = _load_news_score_map(today_ymd=today_ymd)
    news_implication_map = _load_news_implication_map()
    ml_map = _load_ml_score_map()
    ml_feature_map = _load_ml_feature_map()
    ml_bundle: Dict[str, Any] | None = None
    ml_bundle_status = "NOT_LOADED"
    lob_map = _load_lob_map()
    guard_map = _load_candidate_guard_map()
    watch_guard_map = _load_krx_watchlist_guard_map()

    rows: List[Dict[str, Any]] = []
    stale_ref_skip_count = 0
    impossible_move_skip_count = 0
    atr14_fallback_count = 0
    ml_prob_fallback_count = 0
    for _, r in rt.iterrows():
        code = str(r.get("code", "") or "").zfill(6)
        cur = float(r.get("current_price", 0.0) or 0.0)
        opn = float(r.get("open", 0.0) or 0.0)
        hi = float(r.get("high", 0.0) or 0.0)
        lo = float(r.get("low", 0.0) or 0.0)
        vol = float(r.get("volume", 0.0) or 0.0)
        trading_value = float(r.get("trading_value", 0.0) or 0.0)
        if cur <= 0:
            continue
        news_score_value = float(news_map.get(code, 0.0))
        news_implication = news_implication_map.get(code, {})
        news_implication_actions = str(news_implication.get("actions", ""))
        news_implication_scopes = str(news_implication.get("scopes", ""))
        try:
            news_implication_rows = int(float(str(news_implication.get("rows", "0") or "0")))
        except Exception:
            news_implication_rows = 0

        ref_info = ref_map.get(code, {})
        prev_close = float(ref_info.get("prev_close", 0.0) or 0.0)
        avg_vol20 = float(ref_info.get("avg_vol20", 0.0) or 0.0)
        closes = ref_info.get("closes") if isinstance(ref_info.get("closes"), list) else []
        highs = ref_info.get("highs") if isinstance(ref_info.get("highs"), list) else []
        lows = ref_info.get("lows") if isinstance(ref_info.get("lows"), list) else []
        vols = ref_info.get("volumes") if isinstance(ref_info.get("volumes"), list) else []
        ref_latest_date = str(ref_info.get("latest_date", "") or "")
        if prev_close <= 0:
            rows.append(
                {
                    "ts": ts,
                    "date": today_ymd,
                    "code": code,
                    "surge_flag": False,
                    "is_realtime_surge": False,
                    "excluded_by_policy": True,
                    "exclude_reasons": "NO_REFERENCE",
                    "surge_type": "NONE",
                    "surge_score": 0.0,
                    "change_pct": 0.0,
                    "day_range_pct": 0.0,
                    "rvol20": 0.0,
                    "trading_value": round(trading_value, 2),
                    "ret_5d": 0.0,
                    "ret_15d": 0.0,
                    "current_price": int(cur),
                    "prev_close": 0,
                    "prev_close_5d": 0,
                    "prev_close_15d": 0,
                    "reference_latest_date": ref_latest_date,
                    "expected_prev_weekday": expected_prev_weekday,
                    "calc_issue": "NO_REFERENCE",
                    "news_score": round(news_score_value, 6),
                }
            )
            continue
        if expected_prev_weekday and ref_latest_date and ref_latest_date != expected_prev_weekday:
            stale_ref_skip_count += 1
            rows.append(
                {
                    "ts": ts,
                    "date": today_ymd,
                    "code": code,
                    "surge_flag": False,
                    "is_realtime_surge": False,
                    "excluded_by_policy": True,
                    "exclude_reasons": f"REF_STALE:{ref_latest_date}!={expected_prev_weekday}",
                    "surge_type": "NONE",
                    "surge_score": 0.0,
                    "change_pct": 0.0,
                    "day_range_pct": 0.0,
                    "rvol20": 0.0,
                    "trading_value": round(trading_value, 2),
                    "ret_5d": 0.0,
                    "ret_15d": 0.0,
                    "current_price": int(cur),
                    "prev_close": int(prev_close),
                    "prev_close_5d": 0,
                    "prev_close_15d": 0,
                    "reference_latest_date": ref_latest_date,
                    "expected_prev_weekday": expected_prev_weekday,
                    "calc_issue": "REF_STALE",
                    "news_score": round(news_score_value, 6),
                }
            )
            continue

        change_pct = _safe_ratio(cur - prev_close, prev_close)
        if change_pct > max_krx_daily_move_pct:
            impossible_move_skip_count += 1
            rows.append(
                {
                    "ts": ts,
                    "date": today_ymd,
                    "code": code,
                    "surge_flag": False,
                    "is_realtime_surge": False,
                    "excluded_by_policy": True,
                    "exclude_reasons": f"CHANGE_PCT_IMPOSSIBLE:{change_pct:.6f}>{max_krx_daily_move_pct:.3f}",
                    "surge_type": "NONE",
                    "surge_score": 0.0,
                    "change_pct": round(change_pct, 6),
                    "day_range_pct": 0.0,
                    "rvol20": 0.0,
                    "trading_value": round(trading_value, 2),
                    "ret_5d": 0.0,
                    "ret_15d": 0.0,
                    "current_price": int(cur),
                    "prev_close": int(prev_close),
                    "prev_close_5d": 0,
                    "prev_close_15d": 0,
                    "reference_latest_date": ref_latest_date,
                    "expected_prev_weekday": expected_prev_weekday,
                    "calc_issue": "CHANGE_PCT_IMPOSSIBLE",
                    "news_score": round(news_score_value, 6),
                }
            )
            continue
        day_range_pct = _safe_ratio(max(0.0, hi - lo), cur)
        open_to_entry_chase_pct = _safe_ratio(cur - opn, opn) if opn > 0 else 0.0
        intraday_high_drawdown_pct = _safe_ratio(cur - hi, hi) if hi > 0 else 0.0
        intraday_low_rebound_pct = _safe_ratio(cur - lo, lo) if lo > 0 else 0.0
        intraday_range_position_pct = _safe_ratio(cur - lo, hi - lo) if hi > lo > 0 else 0.0
        rvol20 = _safe_ratio(vol, avg_vol20)
        prev_close_5 = float(closes[-5]) if len(closes) >= 5 else 0.0
        prev_close_15 = float(closes[-15]) if len(closes) >= 15 else 0.0
        close_3 = float(closes[-3]) if len(closes) >= 3 else prev_close
        close_10 = float(closes[-10]) if len(closes) >= 10 else prev_close
        ret_5d = _safe_ratio(cur - prev_close_5, prev_close_5) if prev_close_5 > 0 else 0.0
        ret_15d = _safe_ratio(cur - prev_close_15, prev_close_15) if prev_close_15 > 0 else 0.0
        highest_15 = max([float(x) for x in closes[-15:]] + [cur]) if len(closes) > 0 else cur
        cond_15d_highest = bool(cur >= highest_15)

        cond_price = change_pct >= pct_min
        cond_range = day_range_pct >= range_min
        cond_vol = rvol20 >= rvol20_min
        cond_trading_value = trading_value >= trading_value_floor if trading_value_floor > 0 else True
        cond_range_vol = rvol20 >= range_rvol20_min
        cond_reg_volume = rvol20 >= 1.0
        cond_reg_short = bool(ret_5d >= reg_short_5d_min and cond_15d_highest and cond_reg_volume)
        cond_reg_mid = bool(ret_15d >= reg_mid_15d_min and cond_15d_highest and cond_reg_volume)
        lob = lob_map.get(code, {})
        spread_bps = float(lob.get("spread_bps", 0.0) or 0.0)
        order_imbalance_l1 = float(lob.get("order_imbalance_l1", 0.0) or 0.0)
        ofi_norm = float(lob.get("ofi_norm", 0.0) or 0.0)
        kyle_lambda_z = float(lob.get("kyle_lambda_z", 0.0) or 0.0)
        markout_1step_bps = float(lob.get("markout_1step_bps", 0.0) or 0.0)
        orderflow_risk_score = max(0.0, min(1.0, float(lob.get("orderflow_risk_score", 0.0) or 0.0)))
        lob_available = bool(lob.get("lob_available", False))
        orderflow_tag = str(lob.get("orderflow_tag", "") or ("NO_HISTORY" if lob_available else "NO_LOB"))
        prev_closes_14 = closes[-15:-1]
        tr_list = [
            max(abs(float(h) - float(l)), abs(float(h) - float(pc)), abs(float(l) - float(pc)))
            for h, l, pc in zip(highs[-14:], lows[-14:], prev_closes_14)
        ]
        atr14_roll = float(sum(tr_list) / max(1, len(tr_list))) if tr_list else 0.0
        fallback_atr14_pct = _safe_ratio(atr14_roll, max(prev_close, 1e-9))
        vol_ma5 = float(sum([float(x) for x in vols[-5:]]) / max(1, len(vols[-5:]))) if vols else avg_vol20
        high_20d = max([float(x) for x in highs[-20:]]) if highs else cur
        ml_fallback_feat = {
            "ret1": change_pct,
            "ret3": _safe_ratio(cur - close_3, close_3),
            "ret5": _safe_ratio(cur - prev_close_5, prev_close_5),
            "ret10": _safe_ratio(cur - close_10, close_10),
            "range_pct": day_range_pct,
            "body_pct": _safe_ratio(cur - opn, max(opn, 1e-9)),
            "upper_shadow_pct": _safe_ratio(max(0.0, hi - max(opn, cur)), max(cur, 1e-9)),
            "vol_ratio20": rvol20,
            "vol_ratio5": _safe_ratio(vol, vol_ma5),
            "atr14_pct": fallback_atr14_pct,
            "close_to_high_20d": _safe_ratio(cur, max(high_20d, 1e-9)),
        }
        ml_features = ml_feature_map.get(code, {})
        ml_prob = float(ml_map.get(code, 0.0) or 0.0)
        ml_prob_source = "surge_ml_score_latest" if code in ml_map else "missing"
        atr14_pct = float(ml_features.get("atr14_pct", 0.0) or 0.0)
        atr14_pct_source = "surge_ml_score_latest" if atr14_pct > 0 else "missing"
        if atr14_pct <= 0 and fallback_atr14_pct > 0:
            atr14_pct = fallback_atr14_pct
            atr14_pct_source = "detector_ohlc_fallback"
            atr14_fallback_count += 1
        if ml_prob <= 0:
            if _to_bool_env("SURGE_RT_ENABLE_DETECTOR_MODEL_FALLBACK", False):
                if ml_bundle_status == "NOT_LOADED":
                    ml_bundle, ml_bundle_status = _load_ml_bundle()
                fallback_prob, fallback_status = _predict_ml_prob_from_features(ml_bundle, ml_fallback_feat)
            else:
                fallback_prob, fallback_status = 0.0, "DETECTOR_MODEL_FALLBACK_DISABLED"
            if fallback_prob > 0:
                ml_prob = fallback_prob
                ml_prob_source = fallback_status
                ml_prob_fallback_count += 1
            else:
                ml_prob_source = fallback_status
        atr_entry_cap_pct = 0.0
        if atr_entry_cap_enabled:
            static_entry_cap_pct = max(0.0, pct_min * entry_change_pct_multiplier)
            if atr14_pct > 0:
                atr_entry_cap_pct = min(
                    static_entry_cap_pct,
                    max(0.0, atr14_pct * atr_entry_cap_multiplier),
                )
            else:
                atr_entry_cap_pct = static_entry_cap_pct
        # NO_LOB is hard-blocked for realtime surge because execution risk is unquantifiable.
        if lob_available:
            cond_spread_ok = bool(spread_bps <= max_spread_bps)
            lob_status = "OK"
        else:
            cond_spread_ok = False
            lob_status = "NO_LOB"

        cond_limit_near = bool(change_pct >= limit_near_pct)
        is_realtime_surge = bool(
            cond_limit_near
            or (cond_price and (cond_vol or (cond_range and cond_range_vol)))
            or cond_reg_short
            or cond_reg_mid
        )
        detected_surge_flag = bool(is_realtime_surge)
        guard = guard_map.get(code, {})
        watch_guard = watch_guard_map.get(code, {})
        guard_krx_admin = bool(guard.get("krx_admin", False) or watch_guard.get("krx_admin", False))
        guard_krx_warning = bool(guard.get("krx_warning", False) or watch_guard.get("krx_warning", False))
        guard_krx_risk = bool(guard.get("krx_risk", False) or watch_guard.get("krx_risk", False))
        guard_krx_caution = bool(guard.get("krx_caution", False) or watch_guard.get("krx_caution", False))
        paper_relaxations: List[str] = []
        rule_score = 0.0
        rule_score += min(max(change_pct / max(pct_min, 1e-9), 0.0), 3.0) * 40.0
        rule_score += min(max(rvol20 / max(rvol20_min, 1e-9), 0.0), 3.0) * 30.0
        rule_score += min(max(day_range_pct / max(range_min, 1e-9), 0.0), 3.0) * 30.0
        if cond_reg_short:
            rule_score = min(100.0, rule_score + 15.0)
        if cond_reg_mid:
            rule_score = min(100.0, rule_score + 20.0)
        if lob_available and order_imbalance_l1 >= imbalance_bonus_thr:
            rule_score = min(100.0, rule_score + imbalance_bonus_pts)
        if lob_available and orderflow_risk_score > 0:
            rule_score = max(0.0, rule_score - orderflow_risk_score * 12.0)
        rule_score = max(0.0, min(100.0, rule_score))
        blocked_reasons: List[str] = []
        if exclude_krx_admin and guard_krx_admin:
            blocked_reasons.append("KRX_ADMIN")
        if exclude_krx_warning and guard_krx_warning:
            blocked_reasons.append("KRX_WARNING")
        if exclude_krx_risk and guard_krx_risk:
            blocked_reasons.append("KRX_RISK")
        if exclude_krx_caution and guard_krx_caution and not (paper_data_collection_enabled and paper_allow_krx_caution):
            blocked_reasons.append("KRX_CAUTION")
        elif exclude_krx_caution and guard_krx_caution:
            paper_relaxations.append("KRX_CAUTION")
        _junk_grade = str(guard.get("junk_risk_grade", "") or "").upper()
        _junk_score = float(guard.get("junk_risk_score", 0.0) or 0.0)
        if _junk_grade in exclude_junk_grades:
            blocked_reasons.append(f"JUNK_GRADE:{_junk_grade}")
        if _junk_score >= exclude_junk_score_min:
            blocked_reasons.append(f"JUNK_SCORE:{_junk_score:.1f}")
        if exclude_no_trade_activity and vol <= 0:
            blocked_reasons.append("NO_TRADE_ACTIVITY")
        regression_only_surge = bool(
            (cond_reg_short or cond_reg_mid)
            and not cond_limit_near
            and not (cond_price and cond_vol)
            and not (cond_price and cond_range and cond_range_vol)
        )
        if is_realtime_surge and regression_only_surge:
            ml_prob_min = float(params.get("bt_ml_prob_min", 0.0) or 0.0)
            ml_feature_ready = bool(ml_prob >= ml_prob_min) if ml_prob_min > 1e-6 else bool(ml_prob > 1e-6)
            if atr14_pct <= 0:
                blocked_reasons.append("INSUFFICIENT_FEATURES:ATR14_MISSING")
            if not ml_feature_ready:
                blocked_reasons.append(f"INSUFFICIENT_FEATURES:ML_PROB<{ml_prob_min:.4f}")
        if is_realtime_surge and not cond_trading_value:
            blocked_reasons.append(f"TRADING_VALUE_FLOOR:{trading_value:.0f}<{trading_value_floor:.0f}")
        paper_limit_near_entry_cap_relaxed = bool(
            paper_data_collection_enabled and paper_limit_near_ignore_entry_caps and cond_limit_near
        )
        if (
            max_entry_change_pct > 0
            and change_pct > max_entry_change_pct
            and is_realtime_surge
            and not cond_limit_near
            and not paper_limit_near_entry_cap_relaxed
        ):
            blocked_reasons.append(f"ENTRY_CHANGE_BLOCK:{change_pct:.4f}>{max_entry_change_pct:.4f}")
        elif max_entry_change_pct > 0 and change_pct > max_entry_change_pct and is_realtime_surge and not cond_limit_near:
            paper_relaxations.append("ENTRY_CHANGE_BLOCK")
        if (
            max_open_to_entry_chase_pct > 0
            and open_to_entry_chase_pct > max_open_to_entry_chase_pct
            and is_realtime_surge
        ):
            blocked_reasons.append(
                f"OPEN_CHASE_BLOCK:{open_to_entry_chase_pct:.4f}>{max_open_to_entry_chase_pct:.4f}"
            )
        if (
            atr_entry_cap_pct > 0
            and change_pct > atr_entry_cap_pct
            and is_realtime_surge
            and not cond_limit_near
            and not paper_limit_near_entry_cap_relaxed
        ):
            blocked_reasons.append(f"ENTRY_ATR_CAP:{change_pct:.4f}>{atr_entry_cap_pct:.4f}")
        elif atr_entry_cap_pct > 0 and change_pct > atr_entry_cap_pct and is_realtime_surge and not cond_limit_near:
            paper_relaxations.append("ENTRY_ATR_CAP")
        effective_high_rejection_entry_block_pct = high_rejection_entry_block_pct
        if paper_data_collection_enabled and paper_high_rejection_entry_block_pct > 0:
            effective_high_rejection_entry_block_pct = paper_high_rejection_entry_block_pct
        if (
            effective_high_rejection_entry_block_pct > 0
            and is_realtime_surge
            and intraday_high_drawdown_pct <= -effective_high_rejection_entry_block_pct
        ):
            blocked_reasons.append(
                f"HIGH_REJECTION_ENTRY_BLOCK:{intraday_high_drawdown_pct:.4f}<=-{effective_high_rejection_entry_block_pct:.4f}"
            )
        elif (
            paper_data_collection_enabled
            and paper_high_rejection_entry_block_pct > 0
            and high_rejection_entry_block_pct > 0
            and is_realtime_surge
            and intraday_high_drawdown_pct <= -high_rejection_entry_block_pct
        ):
            paper_relaxations.append("HIGH_REJECTION_ENTRY_BLOCK")
        entry_execution_quality_ok = bool(
            lob_available
            and cond_spread_ok
            and cond_trading_value
            and intraday_high_drawdown_pct >= 0
            and intraday_range_position_pct >= 0.98
        )
        if max_rvol20 > 0 and is_realtime_surge and rvol20 > max_rvol20:
            if entry_execution_quality_ok:
                paper_relaxations.append("ENTRY_QUALITY_RVOL_OVERHEAT_BLOCK")
            else:
                blocked_reasons.append(f"RVOL_OVERHEAT_BLOCK:{rvol20:.4f}>{max_rvol20:.4f}")
        if (
            overheat_rvol20_min > 0
            and is_realtime_surge
            and rvol20 >= overheat_rvol20_min
            and (max_rvol20 <= 0 or rvol20 <= max_rvol20)
        ):
            if overheat_score_min > 0 and rule_score >= overheat_score_min:
                if entry_execution_quality_ok:
                    paper_relaxations.append("ENTRY_QUALITY_SCORE_RVOL_OVERHEAT_BLOCK")
                else:
                    blocked_reasons.append(
                        f"SCORE_RVOL_OVERHEAT_BLOCK:score={rule_score:.2f}>={overheat_score_min:.2f},"
                        f"rvol20={rvol20:.4f}>={overheat_rvol20_min:.4f}"
                    )
            else:
                if entry_execution_quality_ok:
                    paper_relaxations.append("ENTRY_QUALITY_RVOL_OVERHEAT_BLOCK")
                else:
                    blocked_reasons.append(
                        f"RVOL_OVERHEAT_BLOCK:{rvol20:.4f}>={overheat_rvol20_min:.4f}"
                    )
        no_lob_probe_allowed = bool(
            no_lob_probe_enabled
            and is_realtime_surge
            and not lob_available
            and (no_lob_probe_allow_limit_near or not cond_limit_near)
            and not blocked_reasons
            and (no_lob_probe_max_change_pct <= 0 or change_pct <= no_lob_probe_max_change_pct)
            and (no_lob_probe_min_rvol20 <= 0 or rvol20 >= no_lob_probe_min_rvol20)
            and (no_lob_probe_min_trading_value <= 0 or trading_value >= no_lob_probe_min_trading_value)
            and (no_lob_probe_min_score_final <= 0 or rule_score >= no_lob_probe_min_score_final)
        )
        if is_realtime_surge and not lob_available and not no_lob_probe_allowed:
            blocked_reasons.append("NO_LOB_BLOCK")
        if lob_available and not cond_spread_ok and is_realtime_surge and not cond_limit_near:
            blocked_reasons.append(f"SPREAD_BLOCK:{spread_bps:.1f}>{max_spread_bps:.1f}")
        if lob_available and orderflow_risk_block_threshold > 0 and orderflow_risk_score >= orderflow_risk_block_threshold:
            blocked_reasons.append(f"ORDERFLOW_RISK_BLOCK:{orderflow_risk_score:.3f}>={orderflow_risk_block_threshold:.3f}")
        if orderflow_tag.strip().upper() == "PAUSE":
            blocked_reasons.append("ORDERFLOW_PAUSE")
        if lob_available and extreme_imbalance_block_min > 0 and abs(order_imbalance_l1) >= extreme_imbalance_block_min:
            if entry_execution_quality_ok:
                paper_relaxations.append("ENTRY_QUALITY_ORDER_IMBALANCE_EXTREME")
            else:
                blocked_reasons.append(f"ORDER_IMBALANCE_EXTREME:{order_imbalance_l1:.3f}")
        if lob_available and kyle_lambda_z_block_min > 0 and kyle_lambda_z >= kyle_lambda_z_block_min:
            blocked_reasons.append(f"KYLE_LAMBDA_Z_BLOCK:{kyle_lambda_z:.3f}>={kyle_lambda_z_block_min:.3f}")
        if lob_available and markout_1step_bps_block_max < 0 and markout_1step_bps <= markout_1step_bps_block_max:
            blocked_reasons.append(f"MARKOUT_NEGATIVE_BLOCK:{markout_1step_bps:.2f}<={markout_1step_bps_block_max:.2f}")
        if lob_available and ofi_norm_abs_block_min > 0 and abs(ofi_norm) >= ofi_norm_abs_block_min:
            blocked_reasons.append(f"OFI_NORM_EXTREME:{ofi_norm:.3f}")
        if news_score_value < news_block_threshold:
            blocked_reasons.append(f"NEWS_NEGATIVE:{news_score_value:.3f}<{news_block_threshold:.3f}")
        if _news_implication_has_action(news_implication, "block"):
            blocked_reasons.append("NEWS_IMPLICATION_BLOCK")
        if cond_limit_near:
            candidate_surge_type = "LIMIT_UP_NEAR"
        elif cond_price and cond_vol:
            candidate_surge_type = "PRICE_VOL_BREAKOUT"
        elif cond_price and cond_range and cond_range_vol:
            candidate_surge_type = "PRICE_RANGE_BREAKOUT"
        elif cond_reg_mid:
            candidate_surge_type = "REG_MID_15D100"
        elif cond_reg_short:
            candidate_surge_type = "REG_SHORT_5D60"
        elif bool(cond_vol):
            try:
                rsi_val = float(str(r.get("rsi14", "50.0")))
                if rsi_val < 30.0:
                    candidate_surge_type = "OVERSOLD_REVERSAL"
                else:
                    candidate_surge_type = "NONE"
            except (ValueError, TypeError):
                candidate_surge_type = "NONE"
        else:
            candidate_surge_type = "NONE"
        block_reason_keys = {
            str(reason).split(":", 1)[0].strip().upper()
            for reason in blocked_reasons
            if str(reason).strip()
        }
        paper_probe_candidate = bool(
            paper_data_collection_enabled
            and paper_probe_enabled
            and paper_probe_max_selected > 0
            and is_realtime_surge
            and candidate_surge_type != "NONE"
            and ((not paper_probe_allowed_types) or candidate_surge_type in paper_probe_allowed_types)
            and bool(lob_available)
            and str(orderflow_tag or "").strip().upper() == "OK"
            and spread_bps <= max_spread_bps
            and (not block_reason_keys.isdisjoint(paper_probe_allowed_block_reasons))
            and block_reason_keys.issubset(paper_probe_allowed_block_reasons)
            and block_reason_keys.isdisjoint(paper_probe_blocked_block_reasons)
            and (
                paper_probe_max_markout_negative_bps >= 0
                or markout_1step_bps >= paper_probe_max_markout_negative_bps
            )
            and (
                paper_probe_high_rejection_floor_pct >= 0
                or intraday_high_drawdown_pct >= paper_probe_high_rejection_floor_pct
            )
        )
        orderflow_tag_norm = str(orderflow_tag or "").strip().upper()
        entry_quality_no_history_allowed = bool(entry_execution_quality_ok and orderflow_tag_norm == "NO_HISTORY")
        if entry_quality_no_history_allowed:
            paper_relaxations.append("ENTRY_QUALITY_ORDERFLOW_NO_HISTORY")
        # NO_LOB stays blocked; NO_HISTORY can approve only when current LOB/execution quality is strong.
        cond_execution_ok = bool(
            cond_spread_ok
            and lob_available
            and (orderflow_tag_norm == "OK" or entry_quality_no_history_allowed)
        )
        flag = bool(
            (cond_limit_near and cond_execution_ok)
            or (((cond_price and (cond_vol or (cond_range and cond_range_vol))) or cond_reg_short or cond_reg_mid) and cond_execution_ok)
        )
        if blocked_reasons:
            flag = False
            is_realtime_surge = False
        surge_type = candidate_surge_type

        if not detected_surge_flag:
            entry_decision = "NOT_DETECTED"
            entry_timing = "NO_ACTION"
            entry_reason = "NO_REALTIME_SURGE"
        elif flag:
            entry_decision = "ENTRY_ALLOWED"
            entry_timing = "NOW"
            entry_reason = "ENTRY_CONDITIONS_MET"
        elif blocked_reasons:
            entry_decision = "ENTRY_BLOCKED"
            entry_timing = "BLOCKED"
            entry_reason = str(blocked_reasons[0]).split(":", 1)[0].strip().upper()
        elif not cond_execution_ok:
            entry_decision = "WAIT_EXECUTION"
            entry_timing = "WAIT"
            entry_reason = "EXECUTION_NOT_READY"
        else:
            entry_decision = "WAIT_CONFIRMATION"
            entry_timing = "WAIT"
            entry_reason = "ENTRY_SIGNAL_NOT_CONFIRMED"

        if not detected_surge_flag:
            exit_layer_status = "NO_SURGE"
            exit_timing = "NO_ACTION"
            exit_observation_reason = "NO_REALTIME_SURGE"
        elif flag:
            exit_layer_status = "ARM_AFTER_ENTRY"
            exit_timing = "AFTER_FILL_INTRADAY_MONITOR"
            exit_observation_reason = "POSITION_EXIT_RULES_ARM_AFTER_FILL"
        else:
            exit_layer_status = "OBSERVE_ONLY_NOT_ARMED"
            exit_timing = "NOT_ARMED"
            exit_observation_reason = "ENTRY_NOT_ALLOWED"

        _log_active_response_gate(
            ts=ts,
            today_ymd=today_ymd,
            code=code,
            detected_surge_flag=detected_surge_flag,
            flag=flag,
            blocked_reasons=blocked_reasons,
            entry_decision=entry_decision,
            entry_reason=entry_reason,
            change_pct=change_pct,
            open_to_entry_chase_pct=open_to_entry_chase_pct,
            atr_entry_cap_pct=atr_entry_cap_pct,
            intraday_high_drawdown_pct=intraday_high_drawdown_pct,
            rvol20=rvol20,
            max_rvol20=max_rvol20,
            spread_bps=spread_bps,
            max_spread_bps=max_spread_bps,
            orderflow_tag=orderflow_tag,
            news_score_value=news_score_value,
            news_block_threshold=news_block_threshold,
            paper_relaxations=paper_relaxations,
        )

        score = rule_score

        rows.append(
            {
                "ts": ts,
                "date": today_ymd,
                "code": code,
                "detected_surge_flag": bool(detected_surge_flag),
                "detected_surge_type": surge_type,
                "entry_allowed": bool(flag),
                "entry_blocked": bool(len(blocked_reasons) > 0),
                "entry_decision": entry_decision,
                "entry_timing": entry_timing,
                "entry_reason": entry_reason,
                "exit_layer_status": exit_layer_status,
                "exit_timing": exit_timing,
                "exit_observation_reason": exit_observation_reason,
                "exit_policy_enabled": bool(exit_observation_policy.get("enabled", False)),
                "exit_stop_loss_pct": round(float(exit_observation_policy.get("stop_loss_pct", 0.0) or 0.0), 6),
                "exit_take_profit_pct": round(float(exit_observation_policy.get("take_profit_pct", 0.0) or 0.0), 6),
                "exit_max_hold_days": int(exit_observation_policy.get("max_hold_days", 0) or 0),
                "exit_stop_sell_ratio_pct": round(float(exit_observation_policy.get("stop_sell_ratio_pct", 0.0) or 0.0), 6),
                "exit_preemptive_sell_ratio_pct": round(float(exit_observation_policy.get("preemptive_sell_ratio_pct", 0.0) or 0.0), 6),
                "exit_dynamic_ratio_enabled": bool(exit_observation_policy.get("dynamic_exit_ratio_enabled", False)),
                "exit_reversal_watch_enabled": bool(exit_observation_policy.get("reversal_exit_enabled", False)),
                "exit_intraday_reversal_watch_enabled": bool(exit_observation_policy.get("intraday_reversal_exit_enabled", False)),
                "exit_policy_source_status": str(exit_observation_policy.get("source_status", "")),
                "surge_flag": bool(flag),
                "is_realtime_surge": bool(is_realtime_surge),
                "excluded_by_policy": bool(len(blocked_reasons) > 0),
                "exclude_reasons": "|".join(blocked_reasons),
                "surge_type": surge_type,
                "surge_score": round(score, 2),
                "change_pct": round(change_pct, 6),
                "open_to_entry_chase_pct": round(open_to_entry_chase_pct, 6),
                "day_range_pct": round(day_range_pct, 6),
                "intraday_high_drawdown_pct": round(intraday_high_drawdown_pct, 6),
                "intraday_low_rebound_pct": round(intraday_low_rebound_pct, 6),
                "intraday_range_position_pct": round(intraday_range_position_pct, 6),
                "rvol20": round(rvol20, 6),
                "trading_value": round(trading_value, 2),
                "ret_5d": round(ret_5d, 6),
                "ret_15d": round(ret_15d, 6),
                "atr14_pct": round(atr14_pct, 6),
                "atr14_pct_source": atr14_pct_source,
                "atr_entry_cap_pct": round(atr_entry_cap_pct, 6),
                "current_price": int(cur),
                "prev_close": int(prev_close),
                "prev_close_5d": int(prev_close_5) if prev_close_5 > 0 else 0,
                "prev_close_15d": int(prev_close_15) if prev_close_15 > 0 else 0,
                "reference_latest_date": ref_latest_date,
                "expected_prev_weekday": expected_prev_weekday,
                "volume_now": int(vol),
                "avg_vol20": round(avg_vol20, 2),
                "cond_price": bool(cond_price),
                "cond_vol": bool(cond_vol),
                "cond_trading_value": bool(cond_trading_value),
                "cond_range": bool(cond_range),
                "cond_range_vol": bool(cond_range_vol),
                "cond_reg_short_5d60_core": bool(cond_reg_short),
                "cond_reg_mid_15d100_core": bool(cond_reg_mid),
                "cond_reg_volume": bool(cond_reg_volume),
                "cond_limit_near": bool(cond_limit_near),
                "cond_15d_highest_close": bool(cond_15d_highest),
                "news_score": round(news_score_value, 6),
                "news_implication_rows": int(news_implication_rows),
                "news_implication_top_actions": news_implication_actions,
                "news_implication_top_scopes": news_implication_scopes,
                "news_implication_block": bool(_news_implication_has_action(news_implication, "block")),
                "news_implication_reduce_size": bool(_news_implication_has_action(news_implication, "reduce_size")),
                "news_implication_watch": bool(_news_implication_has_action(news_implication, "watch")),
                "surge_ml_prob": round(ml_prob, 6),
                "surge_ml_prob_source": ml_prob_source,
                "spread_bps": round(spread_bps, 6),
                "order_imbalance_l1": round(order_imbalance_l1, 6),
                "ofi_norm": round(ofi_norm, 6),
                "kyle_lambda_z": round(kyle_lambda_z, 6),
                "markout_1step_bps": round(markout_1step_bps, 6),
                "orderflow_risk_score": round(orderflow_risk_score, 6),
                "orderflow_tag": orderflow_tag,
                "lob_available": bool(lob_available),
                "lob_status": lob_status,
                "no_lob_probe_allowed": bool(no_lob_probe_allowed),
                "paper_probe_candidate": bool(paper_probe_candidate),
                "paper_probe_allowed": False,
                "paper_probe_block_reasons": "|".join(sorted(block_reason_keys)),
                "paper_policy_relaxations": "|".join(paper_relaxations),
                "ask_depth_levels": float(lob.get("ask_depth_levels", 0.0) or 0.0),
                **{
                    f"ask{i}": float(lob.get(f"ask{i}", 0.0) or 0.0)
                    for i in range(1, 11)
                },
                **{
                    f"askq{i}": float(lob.get(f"askq{i}", 0.0) or 0.0)
                    for i in range(1, 11)
                },
                "cond_spread_ok": bool(cond_spread_ok),
            }
        )

    out_df = pd.DataFrame(rows)
    if len(out_df) > 0:
        for c, default in (
            ("spread_bps", 0.0),
            ("order_imbalance_l1", 0.0),
            ("lob_available", False),
            ("lob_status", "NO_LOB"),
            ("ofi_norm", 0.0),
            ("kyle_lambda_z", 0.0),
            ("markout_1step_bps", 0.0),
            ("orderflow_risk_score", 0.0),
            ("intraday_high_drawdown_pct", 0.0),
            ("intraday_low_rebound_pct", 0.0),
            ("intraday_range_position_pct", 0.0),
            ("orderflow_tag", "NO_LOB"),
            ("no_lob_probe_allowed", False),
            ("paper_policy_relaxations", ""),
            ("ask_depth_levels", 0.0),
            ("detected_surge_flag", False),
            ("detected_surge_type", "NONE"),
            ("entry_allowed", False),
            ("entry_blocked", False),
            ("entry_decision", "NOT_DETECTED"),
            ("entry_timing", "NO_ACTION"),
            ("entry_reason", "NO_REALTIME_SURGE"),
            ("exit_layer_status", "NO_SURGE"),
            ("exit_timing", "NO_ACTION"),
            ("exit_observation_reason", "NO_REALTIME_SURGE"),
            ("exit_policy_enabled", False),
            ("exit_stop_loss_pct", 0.0),
            ("exit_take_profit_pct", 0.0),
            ("exit_max_hold_days", 0),
            ("exit_stop_sell_ratio_pct", 0.0),
            ("exit_preemptive_sell_ratio_pct", 0.0),
            ("exit_dynamic_ratio_enabled", False),
            ("exit_reversal_watch_enabled", False),
            ("exit_intraday_reversal_watch_enabled", False),
            ("exit_policy_source_status", "UNKNOWN"),
        ):
            if c not in out_df.columns:
                out_df[c] = default
            else:
                out_df[c] = out_df[c].where(out_df[c].notna(), default)
        for c in [f"ask{i}" for i in range(1, 11)] + [f"askq{i}" for i in range(1, 11)]:
            if c not in out_df.columns:
                out_df[c] = 0.0
        code_keys = out_df["code"].astype(str).str.zfill(6)
        force_fill = out_df["calc_issue"].astype(str).str.strip() != "" if "calc_issue" in out_df.columns else pd.Series(False, index=out_df.index)
        for c in ("spread_bps", "order_imbalance_l1", "ofi_norm", "kyle_lambda_z", "markout_1step_bps", "orderflow_risk_score"):
            mapped = code_keys.map(lambda code: lob_map.get(code, {}).get(c, pd.NA))
            mapped = pd.to_numeric(mapped, errors="coerce")
            current_text = out_df[c].astype(str).str.strip()
            fill_mask = mapped.notna() & (force_fill | pd.isna(out_df[c]) | (current_text == ""))
            out_df.loc[fill_mask, c] = mapped.loc[fill_mask].astype(float)

        mapped_available = code_keys.map(lambda code: lob_map.get(code, {}).get("lob_available", pd.NA))
        current_text = out_df["lob_available"].astype(str).str.strip()
        fill_mask = mapped_available.notna() & (force_fill | pd.isna(out_df["lob_available"]) | (current_text == ""))
        out_df.loc[fill_mask, "lob_available"] = mapped_available.loc[fill_mask].map(bool)

        mapped_status = code_keys.map(
            lambda code: ("OK" if bool(lob_map.get(code, {}).get("lob_available", False)) else "NO_LOB")
            if code in lob_map else pd.NA
        )
        current_text = out_df["lob_status"].astype(str).str.strip()
        fill_mask = mapped_status.notna() & (force_fill | pd.isna(out_df["lob_status"]) | (current_text == ""))
        out_df.loc[fill_mask, "lob_status"] = mapped_status.loc[fill_mask]

        mapped_tag = code_keys.map(
            lambda code: str(lob_map.get(code, {}).get("orderflow_tag", "") or "NO_HISTORY") if code in lob_map else pd.NA
        )
        current_text = out_df["orderflow_tag"].astype(str).str.strip()
        fill_mask = mapped_tag.notna() & (force_fill | pd.isna(out_df["orderflow_tag"]) | (current_text == ""))
        out_df.loc[fill_mask, "orderflow_tag"] = mapped_tag.loc[fill_mask]
        if "news_score" not in out_df.columns:
            out_df["news_score"] = 0.0
        if "surge_ml_prob" not in out_df.columns:
            out_df["surge_ml_prob"] = 0.0
        if "surge_ml_prob_source" not in out_df.columns:
            out_df["surge_ml_prob_source"] = ""
        if "atr14_pct_source" not in out_df.columns:
            out_df["atr14_pct_source"] = ""
        out_df["news_score"] = pd.to_numeric(out_df["news_score"], errors="coerce").fillna(0.0)
        out_df["surge_ml_prob"] = pd.to_numeric(out_df["surge_ml_prob"], errors="coerce").fillna(0.0)
        # News now participates as a proportional signal when it has a non-zero score.
        # Missing/zero news and missing ML are assigned back to rule to avoid penalizing uncovered names.
        total_w = max(1e-9, w_rule + w_news + w_ml)
        w_rule_n = w_rule / total_w
        w_news_n = w_news / total_w
        w_ml_n = w_ml / total_w

        rule_s = pd.to_numeric(out_df["surge_score"], errors="coerce").fillna(0.0)
        news_raw = pd.to_numeric(out_df["news_score"], errors="coerce").fillna(0.0)
        ml_raw = pd.to_numeric(out_df["surge_ml_prob"], errors="coerce").fillna(0.0)
        _ml_prob_min = float(params.get("bt_ml_prob_min", 0.0) or 0.0)
        ml_avail = (ml_raw >= _ml_prob_min) if _ml_prob_min > 1e-6 else (ml_raw > 1e-6)
        news_avail = news_raw.abs() > 1e-6
        eff_w_rule = w_rule_n + w_news_n * (~news_avail).astype(float) + w_ml_n * (~ml_avail).astype(float)
        eff_w_news = w_news_n * news_avail.astype(float)
        eff_w_ml = w_ml_n * ml_avail.astype(float)

        news_norm = news_raw.clip(-1.0, 1.0).add(1.0).mul(50.0)
        ml_norm = ml_raw.clip(0.0, 1.0).mul(100.0)
        news_bonus = (news_raw > news_bonus_threshold).astype(float) * max(0.0, news_bonus_pts)

        out_df["surge_score_final"] = (
            rule_s * eff_w_rule + news_norm * eff_w_news + ml_norm * eff_w_ml + news_bonus
        ).clip(lower=0.0, upper=100.0)
        if paper_probe_enabled and paper_probe_max_selected > 0 and "paper_probe_candidate" in out_df.columns:
            probe_mask = (
                (out_df["paper_probe_candidate"] == True)
                & (pd.to_numeric(out_df["surge_score_final"], errors="coerce").fillna(0.0) >= paper_probe_min_score_final)
            )
            probe_df = out_df[probe_mask].sort_values(["surge_score_final", "change_pct"], ascending=[False, False])
            probe_idx = probe_df.head(paper_probe_max_selected).index
            if len(probe_idx) > 0:
                out_df.loc[probe_idx, "paper_probe_allowed"] = True
                out_df.loc[probe_idx, "surge_flag"] = True
                out_df.loc[probe_idx, "is_realtime_surge"] = True
                out_df.loc[probe_idx, "paper_policy_relaxations"] = out_df.loc[probe_idx, "paper_policy_relaxations"].astype(str).map(
                    lambda v: "PAPER_PROBE" if not v.strip() else f"{v}|PAPER_PROBE"
                )
        out_df = out_df.sort_values(["surge_flag", "surge_score_final", "change_pct"], ascending=[False, False, False])
    alerts: List[Dict[str, Any]] = []
    if len(out_df) > 0:
        alert_df = out_df[
            (out_df["surge_flag"] == True)
            & (out_df["is_realtime_surge"] == True)
        ]
        for _, rr in alert_df.head(max_alerts).iterrows():
            alerts.append(
                {
                    "code": str(rr.get("code", "")),
                    "date": str(rr.get("date", "") or today_ymd),
                    "surge_type": str(rr.get("surge_type", "")),
                    "surge_score": float(rr.get("surge_score", 0.0)),
                    "surge_score_final": float(rr.get("surge_score_final", 0.0)),
                    "change_pct": float(rr.get("change_pct", 0.0)),
                    "rvol20": float(rr.get("rvol20", 0.0)),
                    "trading_value": float(rr.get("trading_value", 0.0)),
                    "day_range_pct": float(rr.get("day_range_pct", 0.0)),
                    "intraday_high_drawdown_pct": float(rr.get("intraday_high_drawdown_pct", 0.0)),
                    "atr14_pct": float(rr.get("atr14_pct", 0.0)),
                    "atr_entry_cap_pct": float(rr.get("atr_entry_cap_pct", 0.0)),
                    "current_price": float(rr.get("current_price", 0.0)),
                    "prev_close": float(rr.get("prev_close", 0.0)),
                    "news_score": float(rr.get("news_score", 0.0)),
                    "surge_ml_prob": float(rr.get("surge_ml_prob", 0.0)),
                    "surge_ml_prob_source": str(rr.get("surge_ml_prob_source", "")),
                    "atr14_pct_source": str(rr.get("atr14_pct_source", "")),
                    "spread_bps": float(rr.get("spread_bps", 0.0)),
                    "order_imbalance_l1": float(rr.get("order_imbalance_l1", 0.0)),
                    "ofi_norm": float(rr.get("ofi_norm", 0.0)),
                    "kyle_lambda_z": float(rr.get("kyle_lambda_z", 0.0)),
                    "markout_1step_bps": float(rr.get("markout_1step_bps", 0.0)),
                    "orderflow_risk_score": float(rr.get("orderflow_risk_score", 0.0)),
                    "orderflow_tag": str(rr.get("orderflow_tag", "")),
                    "lob_available": bool(rr.get("lob_available", False)),
                    "lob_status": str(rr.get("lob_status", "NO_LOB")),
                    "no_lob_probe_allowed": bool(rr.get("no_lob_probe_allowed", False)),
                    "paper_probe_allowed": bool(rr.get("paper_probe_allowed", False)),
                    "paper_probe_block_reasons": str(rr.get("paper_probe_block_reasons", "") or ""),
                    "paper_policy_relaxations": str(rr.get("paper_policy_relaxations", "") or ""),
                    "ask_depth_levels": float(rr.get("ask_depth_levels", 0.0)),
                    "exclude_reasons": str(rr.get("exclude_reasons", "") or ""),
                    **{
                        f"ask{i}": float(rr.get(f"ask{i}", 0.0) or 0.0)
                        for i in range(1, 11)
                    },
                    **{
                        f"askq{i}": float(rr.get(f"askq{i}", 0.0) or 0.0)
                        for i in range(1, 11)
                    },
                }
            )
    detected_alerts: List[Dict[str, Any]] = []
    if len(out_df) > 0 and "detected_surge_flag" in out_df.columns:
        detected_df = out_df[out_df["detected_surge_flag"] == True]
        for _, rr in detected_df.head(max_alerts).iterrows():
            detected_alerts.append(
                {
                    "code": str(rr.get("code", "")),
                    "date": str(rr.get("date", "") or today_ymd),
                    "detected_surge_type": str(rr.get("detected_surge_type", "")),
                    "surge_score": float(rr.get("surge_score", 0.0)),
                    "surge_score_final": float(rr.get("surge_score_final", 0.0)),
                    "change_pct": float(rr.get("change_pct", 0.0)),
                    "rvol20": float(rr.get("rvol20", 0.0)),
                    "trading_value": float(rr.get("trading_value", 0.0)),
                    "day_range_pct": float(rr.get("day_range_pct", 0.0)),
                    "lob_available": bool(rr.get("lob_available", False)),
                    "lob_status": str(rr.get("lob_status", "NO_LOB")),
                    "entry_allowed": bool(rr.get("entry_allowed", False)),
                    "entry_blocked": bool(rr.get("entry_blocked", False)),
                    "entry_decision": str(rr.get("entry_decision", "")),
                    "entry_timing": str(rr.get("entry_timing", "")),
                    "entry_reason": str(rr.get("entry_reason", "")),
                    "exit_layer_status": str(rr.get("exit_layer_status", "")),
                    "exit_timing": str(rr.get("exit_timing", "")),
                    "exit_observation_reason": str(rr.get("exit_observation_reason", "")),
                    "exit_policy_enabled": bool(rr.get("exit_policy_enabled", False)),
                    "exit_stop_loss_pct": float(rr.get("exit_stop_loss_pct", 0.0)),
                    "exit_take_profit_pct": float(rr.get("exit_take_profit_pct", 0.0)),
                    "exit_max_hold_days": int(rr.get("exit_max_hold_days", 0) or 0),
                    "exit_stop_sell_ratio_pct": float(rr.get("exit_stop_sell_ratio_pct", 0.0)),
                    "exit_preemptive_sell_ratio_pct": float(rr.get("exit_preemptive_sell_ratio_pct", 0.0)),
                    "exit_dynamic_ratio_enabled": bool(rr.get("exit_dynamic_ratio_enabled", False)),
                    "exit_reversal_watch_enabled": bool(rr.get("exit_reversal_watch_enabled", False)),
                    "exit_intraday_reversal_watch_enabled": bool(rr.get("exit_intraday_reversal_watch_enabled", False)),
                    "exit_policy_source_status": str(rr.get("exit_policy_source_status", "")),
                    "exclude_reasons": str(rr.get("exclude_reasons", "") or ""),
                }
            )

    lob_ok_count = int(out_df["lob_available"].sum()) if len(out_df) > 0 and "lob_available" in out_df.columns else 0
    lob_no_count = int(len(out_df)) - lob_ok_count if len(out_df) > 0 else 0
    detected_count = int((out_df["detected_surge_flag"] == True).sum()) if len(out_df) > 0 and "detected_surge_flag" in out_df.columns else 0
    entry_decision_counts = (
        {str(k): int(v) for k, v in out_df["entry_decision"].value_counts(dropna=False).items()}
        if len(out_df) > 0 and "entry_decision" in out_df.columns
        else {}
    )
    entry_reason_counts = (
        {str(k): int(v) for k, v in out_df["entry_reason"].value_counts(dropna=False).items()}
        if len(out_df) > 0 and "entry_reason" in out_df.columns
        else {}
    )
    exit_layer_status_counts = (
        {str(k): int(v) for k, v in out_df["exit_layer_status"].value_counts(dropna=False).items()}
        if len(out_df) > 0 and "exit_layer_status" in out_df.columns
        else {}
    )
    exit_observation_reason_counts = (
        {str(k): int(v) for k, v in out_df["exit_observation_reason"].value_counts(dropna=False).items()}
        if len(out_df) > 0 and "exit_observation_reason" in out_df.columns
        else {}
    )
    alert_count_all = int((out_df["surge_flag"] == True).sum()) if len(out_df) > 0 and "surge_flag" in out_df.columns else 0
    alert_count_realtime = int(
        (
            (out_df["surge_flag"] == True)
            & (out_df["is_realtime_surge"] == True)
        ).sum()
    ) if len(out_df) > 0 and {"surge_flag", "is_realtime_surge"}.issubset(out_df.columns) else 0
    payload = {
        "ts": ts,
        "status": "OK",
        "thresholds": {
            "pct_min": pct_min,
            "range_min": range_min,
            "rvol20_min": rvol20_min,
            "trading_value_floor": trading_value_floor,
            "range_rvol20_min": range_rvol20_min,
            "reg_short_5d_min": reg_short_5d_min,
            "reg_mid_15d_min": reg_mid_15d_min,
            "limit_near_pct": limit_near_pct,
            "imbalance_bonus_thr": imbalance_bonus_thr,
            "imbalance_bonus_pts": imbalance_bonus_pts,
            "weight_rule": w_rule,
            "weight_news": w_news,
            "weight_ml": w_ml,
            "news_block_threshold": news_block_threshold,
            "news_bonus_threshold": news_bonus_threshold,
            "news_bonus_pts": news_bonus_pts,
            "max_entry_change_pct": max_entry_change_pct,
            "max_open_to_entry_chase_pct": max_open_to_entry_chase_pct,
            "high_rejection_entry_block_pct": high_rejection_entry_block_pct,
            "max_rvol20": max_rvol20,
            "overheat_score_min": overheat_score_min,
            "overheat_rvol20_min": overheat_rvol20_min,
            "atr_entry_cap_enabled": atr_entry_cap_enabled,
            "entry_change_pct_multiplier": entry_change_pct_multiplier,
            "atr_entry_cap_multiplier": atr_entry_cap_multiplier,
            "no_lob_probe_enabled": no_lob_probe_enabled,
            "no_lob_probe_max_change_pct": no_lob_probe_max_change_pct,
            "no_lob_probe_min_rvol20": no_lob_probe_min_rvol20,
            "no_lob_probe_min_trading_value": no_lob_probe_min_trading_value,
            "no_lob_probe_min_score_final": no_lob_probe_min_score_final,
            "no_lob_probe_allow_limit_near": no_lob_probe_allow_limit_near,
            "no_lob_probe_lock_ok": no_lob_probe_lock_ok,
            "no_lob_probe_profile_blocked": no_lob_probe_profile_blocked,
            "paper_probe_enabled": paper_probe_enabled,
            "paper_probe_max_selected": paper_probe_max_selected,
            "paper_probe_min_score_final": paper_probe_min_score_final,
            "paper_data_collection_enabled": paper_data_collection_enabled,
            "paper_limit_near_ignore_entry_caps": paper_limit_near_ignore_entry_caps,
            "paper_high_rejection_entry_block_pct": paper_high_rejection_entry_block_pct,
            "paper_allow_krx_caution": paper_allow_krx_caution,
            "paper_policy_profile_lock_status": str(paper_policy_profile_lock.get("status", "")),
            "paper_policy_profile_lock_profile": str(paper_policy_profile_lock.get("profile", "")),
            "paper_policy_profile_lock_path": str(paper_policy_profile_lock.get("path", "")),
            "paper_policy_profile_current_env_sha256": str(paper_policy_profile_lock.get("current_env_sha256", "")),
            "paper_policy_profile_approved_env_sha256": str(paper_policy_profile_lock.get("approved_env_sha256", "")),
            "orderflow_risk_block_threshold": orderflow_risk_block_threshold,
            "bt_ml_prob_min": float(params.get("bt_ml_prob_min", 0.0) or 0.0),
            "extreme_imbalance_block_min": extreme_imbalance_block_min,
            "kyle_lambda_z_block_min": kyle_lambda_z_block_min,
            "markout_1step_bps_block_max": markout_1step_bps_block_max,
            "ofi_norm_abs_block_min": ofi_norm_abs_block_min,
            "max_spread_bps": max_spread_bps,
            "max_alerts": max_alerts,
            "max_krx_daily_move_pct": max_krx_daily_move_pct,
        },
        "intraday_date": today_ymd,
        "expected_intraday_date": expected_intraday_ymd,
        "system_today": system_today,
        "expected_prev_weekday": expected_prev_weekday,
        "input_rows": int(len(rt)),
        "evaluated_rows": int(len(out_df)),
        "lob_ok_count": lob_ok_count,
        "lob_no_lob_count": lob_no_count,
        "lob_coverage_pct": round(lob_ok_count / max(len(out_df), 1) * 100.0, 1) if len(out_df) > 0 else 0.0,
        "orderflow_caution_count": int(out_df["orderflow_tag"].isin(["CAUTION", "PAUSE"]).sum()) if len(out_df) > 0 and "orderflow_tag" in out_df.columns else 0,
        "orderflow_pause_count": int((out_df["orderflow_tag"] == "PAUSE").sum()) if len(out_df) > 0 and "orderflow_tag" in out_df.columns else 0,
        "orderflow_max_risk_score": float(pd.to_numeric(out_df["orderflow_risk_score"], errors="coerce").fillna(0.0).max()) if len(out_df) > 0 and "orderflow_risk_score" in out_df.columns else 0.0,
        "stale_ref_skip_count": int(stale_ref_skip_count),
        "impossible_move_skip_count": int(impossible_move_skip_count),
        "alerts_count": int(len(alerts)),
        "alerts_count_realtime": int(alert_count_realtime),
        "alerts_count_all": int(alert_count_all),
        "policy_excluded_count": int((out_df["excluded_by_policy"] == True).sum()) if len(out_df) > 0 and "excluded_by_policy" in out_df.columns else 0,
        "trading_value_floor_block_count": int(out_df["exclude_reasons"].astype(str).str.contains("TRADING_VALUE_FLOOR", regex=False).sum()) if len(out_df) > 0 and "exclude_reasons" in out_df.columns else 0,
        "news_score_map_count": int(len(news_map)),
        "news_score_covered_rows": int(out_df["code"].astype(str).str.zfill(6).isin(set(news_map.keys())).sum()) if len(out_df) > 0 and "code" in out_df.columns else 0,
        "news_score_nonzero_rows": int((pd.to_numeric(out_df["news_score"], errors="coerce").fillna(0.0).abs() > 1e-9).sum()) if len(out_df) > 0 and "news_score" in out_df.columns else 0,
        "news_implication_map_count": int(len(news_implication_map)),
        "news_implication_block_rows": int((out_df["news_implication_block"] == True).sum()) if len(out_df) > 0 and "news_implication_block" in out_df.columns else 0,
        "news_implication_reduce_size_rows": int((out_df["news_implication_reduce_size"] == True).sum()) if len(out_df) > 0 and "news_implication_reduce_size" in out_df.columns else 0,
        "news_implication_watch_rows": int((out_df["news_implication_watch"] == True).sum()) if len(out_df) > 0 and "news_implication_watch" in out_df.columns else 0,
        "ml_model_status": ml_bundle_status,
        "atr14_fallback_count": int(atr14_fallback_count),
        "ml_prob_fallback_count": int(ml_prob_fallback_count),
        "detected_count": detected_count,
        "entry_decision_counts": entry_decision_counts,
        "entry_reason_counts": entry_reason_counts,
        "exit_observation_policy": exit_observation_policy,
        "exit_layer_status_counts": exit_layer_status_counts,
        "exit_observation_reason_counts": exit_observation_reason_counts,
        "detected_alerts": detected_alerts,
        "alerts": alerts,
    }
    _write_outputs(payload, out_df, ts)
    return 0


def _log_active_response_gate(
    *,
    ts: str,
    today_ymd: str,
    code: str,
    detected_surge_flag: bool,
    flag: bool,
    blocked_reasons: List[str],
    entry_decision: str,
    entry_reason: str,
    change_pct: float,
    open_to_entry_chase_pct: float,
    atr_entry_cap_pct: float,
    intraday_high_drawdown_pct: float,
    rvol20: float,
    max_rvol20: float,
    spread_bps: float,
    max_spread_bps: float,
    orderflow_tag: Any,
    news_score_value: float,
    news_block_threshold: float,
    paper_relaxations: List[str],
) -> None:
    if not detected_surge_flag:
        return
    decision = "PASS" if flag else ("RELAX" if paper_relaxations else "BLOCK")
    if flag:
        log_active_response_event(
            code=code,
            gate_name="surge_entry_gate",
            decision="PASS",
            reason_code="ENTRY_CONDITIONS_MET",
            reason_detail=f"entry_decision={entry_decision}",
            date=today_ymd,
            ts=ts,
            extra={"surge_type": entry_reason},
        )
        return
    reasons = list(blocked_reasons) or [entry_reason]
    for reason in reasons:
        reason_code = str(reason).split(":", 1)[0].strip().upper()
        reason_detail = str(reason)
        threshold = None
        actual = None
        if reason_code == "ENTRY_CHANGE_BLOCK":
            actual = float(change_pct)
        elif reason_code == "OPEN_CHASE_BLOCK":
            actual = float(open_to_entry_chase_pct)
        elif reason_code == "ENTRY_ATR_CAP":
            threshold = float(atr_entry_cap_pct)
            actual = float(change_pct)
        elif reason_code == "HIGH_REJECTION_ENTRY_BLOCK":
            actual = float(intraday_high_drawdown_pct)
        elif reason_code.startswith("RVOL_OVERHEAT") or reason_code.startswith("SCORE_RVOL_OVERHEAT"):
            threshold = float(max_rvol20) if max_rvol20 > 0 else None
            actual = float(rvol20)
        elif reason_code == "SPREAD_BLOCK":
            threshold = float(max_spread_bps)
            actual = float(spread_bps)
        elif reason_code == "NEWS_NEGATIVE":
            threshold = float(news_block_threshold)
            actual = float(news_score_value)
        log_active_response_event(
            code=code,
            gate_name="surge_entry_gate",
            decision=decision,
            reason_code=reason_code,
            reason_detail=reason_detail,
            threshold=threshold,
            actual_value=actual,
            date=today_ymd,
            ts=ts,
            extra={
                "entry_decision": entry_decision,
                "orderflow_tag": str(orderflow_tag),
                "paper_relaxations": paper_relaxations,
            },
        )


if __name__ == "__main__":
    raise SystemExit(main())
