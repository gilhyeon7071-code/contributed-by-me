from __future__ import annotations

import json
import os
import re
from bisect import bisect_right
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import logging

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"

IN_NEWS = LOGS / "candidates_latest_data.with_news_score.csv"
IN_POLICY = LOGS / "candidates_latest_data.with_policy_score.csv"
IN_SECTOR = LOGS / "candidates_latest_data.with_sector_score.csv"
IN_FILTERED = LOGS / "candidates_latest_data.filtered.csv"
IN_BASE = LOGS / "candidates_latest_data.csv"
IN_NEWS_CANDIDATES = LOGS / "news_candidates_latest.csv"
IN_NEWS_TOPIC_CANDIDATE_IMPACT = LOGS / "news_topic_candidate_impact_latest.csv"
IN_NEWS_JUDGMENT_L3_LAYER = LOGS / "news_judgment_l3_layer_latest.csv"
IN_NEWS_MEDIUM_ADJUSTMENT = LOGS / "news_medium_adjustment_latest.csv"
OUT = LOGS / "candidates_latest_data.with_final_score.csv"
# [2026-08-24] NEWS_ONLY 템플릿 행 분리 산출물. 순위·진입에는 쓰지 않고 데이터만 보존한다.
NEWS_ONLY_OUT = LOGS / "candidates_latest_data.news_only.csv"
DART_CORP_CODE_MAP = ROOT / "_cache" / "dart_corp_code_map.csv"
FORWARD_ESTIMATE_CSV = ROOT / "_cache" / "forward_estimate_latest.csv"
FORWARD_ESTIMATE_META = ROOT / "_cache" / "forward_estimate_meta.json"
FORECAST_VALIDATION_LATEST = LOGS / "forecast_score_validation_latest.json"
NEWS_SCORE_STATUS_LATEST = LOGS / "news_score_status_latest.json"
MACRO_FEATURE_LATEST = LOGS / "macro_feature_external_latest.json"
MACRO_DEFERRED_OPTIONAL_SERIES = {"NAPM", "NAPMNONMFG", "MANEXIMCN", "DCOILWTICO", "DCOILBRENTEU", "CBOE_PUT_CALL_TOTAL", "BDI_INVESTING_WEB"}
MACRO_TIER_WEIGHT_MULTIPLIERS = {
    "OFFICIAL": 1.0,
    "OFFICIAL_PROXY": 0.6,
    "WEB_FALLBACK": 0.35,
    "MARKET_LEVEL": 0.5,
    "MISSING": 0.0,
}
POLICY_SIGNAL_LATEST = LOGS / "policy_agenda_signal_latest.json"
SURGE_LOB_LATEST = LOGS / "surge_lob_latest.csv"
MARKET_RISING_STATUS_LATEST = LOGS / "market_rising_status_latest.json"
SURGE_REALTIME_LATEST = LOGS / "surge_realtime_latest.json"
GATE_DAILY_GLOB = "gate_daily_*.json"
P0_DAILY_GLOB = "p0_daily_check_*.json"

# policy 5% 추가: news/fundamental 각 소폭 감소로 합계 1.00 유지
SCORE_WEIGHTS_BY_REGIME: Dict[str, Dict[str, float]] = {
    "BULL":     {"sector": 0.28, "regime": 0.16, "fx": 0.14, "news": 0.12, "fundamental": 0.13, "policy": 0.05, "forecast": 0.12},
    "SIDEWAYS": {"sector": 0.24, "regime": 0.21, "fx": 0.18, "news": 0.08, "fundamental": 0.12, "policy": 0.05, "forecast": 0.12},
    "BEAR":     {"sector": 0.15, "regime": 0.21, "fx": 0.29, "news": 0.08, "fundamental": 0.12, "policy": 0.05, "forecast": 0.10},
    "CRASH":    {"sector": 0.11, "regime": 0.17, "fx": 0.34, "news": 0.12, "fundamental": 0.13, "policy": 0.05, "forecast": 0.08},
    "CAUTION":  {"sector": 0.24, "regime": 0.21, "fx": 0.18, "news": 0.08, "fundamental": 0.12, "policy": 0.05, "forecast": 0.12},
}




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
def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)


def _ymd_int(value: Any) -> int:
    text = re.sub(r"\D", "", str(value or ""))[:8]
    return int(text) if len(text) == 8 else 0


def _strict_code6(value: Any) -> str:
    text = re.sub(r"\D", "", str(value or ""))
    return text if len(text) == 6 else ""


def _clean_ref_name(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text.lower() in {"", "nan", "none", "null", "<na>", "nat"} else text


def _load_reference_name_map() -> Dict[str, str]:
    if not DART_CORP_CODE_MAP.exists():
        return {}
    try:
        ref = _read_csv(DART_CORP_CODE_MAP)
    except Exception:
        return {}
    if ref.empty or "code" not in ref.columns or "corp_name" not in ref.columns:
        return {}
    out: Dict[str, str] = {}
    for _, row in ref.iterrows():
        code = _strict_code6(row.get("code"))
        name = _clean_ref_name(row.get("corp_name"))
        if code and name and code not in out:
            out[code] = name
    return out


def _write_latest_if_not_older(
    path: Path,
    payload: Dict[str, Any],
    new_ymd: str,
    existing_key: str,
    *,
    ensure_ascii: bool = False,
) -> bool:
    new_i = _ymd_int(new_ymd)
    try:
        existing = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except Exception:
        existing = {}
    old_i = _ymd_int(existing.get(existing_key)) if isinstance(existing, dict) else 0
    if old_i > new_i:
        _log_print(f"[FINAL_SCORE] skip stale latest overwrite path={path.name} old={old_i} new={new_i}")
        return False
    path.write_text(json.dumps(payload, ensure_ascii=ensure_ascii, indent=2), encoding="utf-8")
    return True


def _merge_missing_reference_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "code" not in out.columns:
        return out
    out["code"] = out["code"].astype(str).str.extract(r"(\d+)")[0].fillna("").str.zfill(6)

    refs = [
        (
            LOGS / "candidates_latest_data.csv",
            [
                "disparity200", "macd_bullish", "PER", "PBR", "BPS", "EPS", "DIV", "DPS",
                "per_percentile", "pbr_percentile", "per_band", "pbr_band",
                "valuation_band", "market_cap", "listed_shares", "credit_able_yn", "margin_rate",
                "loan_remaining_rate", "short_over_yn", "short_sale_available_yn",
                "short_balance_qty", "short_balance_amount", "short_balance_ratio",
                "short_balance_source", "short_balance_status",
                "short_balance_source_tier", "short_balance_granularity",
                "credit_balance_qty", "credit_balance_amount",
                "credit_balance_ratio", "credit_balance_source", "credit_balance_status",
                "credit_balance_as_of", "credit_balance_source_tier", "credit_balance_granularity",
                "lend_balance_qty", "lend_balance_amount", "lend_balance_source",
                "lend_balance_status", "lend_balance_source_tier", "lend_balance_granularity",
                "forward_eps", "eps_revision_delta", "eps_revision_pct",
                "eps_revision_direction", "op_consensus_change", "op_consensus_change_pct",
                "op_consensus_direction", "op_consensus_source", "ttm_opm",
            ],
            "candidates",
        ),
        (
            FORWARD_ESTIMATE_CSV,
            [
                "forward_per", "forward_eps", "eps_revision_delta", "eps_revision_pct",
                "eps_revision_direction", "op_consensus_change", "op_consensus_change_pct",
                "op_consensus_direction", "op_consensus_source", "ttm_opm", "peg_ratio", "current_per_wr", "updated_at",
            ],
            "fwd",
        ),
        (
            ROOT / "_cache" / "pykrx_fundamental_latest.csv",
            [
                "PER", "PBR", "BPS", "EPS", "DIV", "DPS", "market_cap",
                "listed_shares", "per_percentile", "pbr_percentile", "per_band",
                "pbr_band", "valuation_band", "credit_able_yn", "margin_rate", "loan_remaining_rate",
                "short_over_yn", "short_sale_available_yn",
                "short_balance_qty", "short_balance_amount", "short_balance_ratio",
                "short_balance_source", "short_balance_status",
                "short_balance_source_tier", "short_balance_granularity",
                "credit_balance_qty", "credit_balance_amount",
                "credit_balance_ratio", "credit_balance_source", "credit_balance_status",
                "credit_balance_as_of", "credit_balance_source_tier", "credit_balance_granularity",
                "lend_balance_qty", "lend_balance_amount", "lend_balance_source",
                "lend_balance_status", "lend_balance_source_tier", "lend_balance_granularity",
            ],
            "pykrx",
        ),
        (
            LOGS / "pykrx_fundamental_latest.csv",
            [
                "PER", "PBR", "BPS", "EPS", "DIV", "DPS", "market_cap",
                "listed_shares", "per_percentile", "pbr_percentile", "per_band",
                "pbr_band", "valuation_band", "credit_able_yn", "margin_rate", "loan_remaining_rate",
                "short_over_yn", "short_sale_available_yn",
                "short_balance_qty", "short_balance_amount", "short_balance_ratio",
                "short_balance_source", "short_balance_status",
                "short_balance_source_tier", "short_balance_granularity",
                "credit_balance_qty", "credit_balance_amount",
                "credit_balance_ratio", "credit_balance_source", "credit_balance_status",
                "credit_balance_as_of", "credit_balance_source_tier", "credit_balance_granularity",
                "lend_balance_qty", "lend_balance_amount", "lend_balance_source",
                "lend_balance_status", "lend_balance_source_tier", "lend_balance_granularity",
            ],
            "pykrx_log",
        ),
    ]

    for path, cols, suffix in refs:
        if not path.exists():
            continue
        try:
            ref = _read_csv(path)
        except Exception:
            continue
        if ref.empty or "code" not in ref.columns:
            continue
        ref = ref.copy()
        ref["code"] = ref["code"].astype(str).str.extract(r"(\d+)")[0].fillna("").str.zfill(6)
        use_cols = ["code"] + [c for c in cols if c in ref.columns]
        if len(use_cols) <= 1:
            continue
        ref = ref[use_cols].drop_duplicates("code", keep="last")
        tmp = out.merge(ref, on="code", how="left", suffixes=("", f"_{suffix}"))
        for c in use_cols:
            if c == "code":
                continue
            dup = f"{c}_{suffix}"
            if c not in out.columns and c in tmp.columns:
                out[c] = tmp[c]
            elif dup in tmp.columns:
                if c in {"updated_at", "eps_revision_direction", "op_consensus_direction", "op_consensus_source", "credit_able_yn", "short_over_yn", "short_sale_available_yn", "short_balance_source", "short_balance_status", "short_balance_source_tier", "short_balance_granularity", "credit_balance_source", "credit_balance_status", "credit_balance_as_of", "credit_balance_source_tier", "credit_balance_granularity", "lend_balance_source", "lend_balance_status", "lend_balance_source_tier", "lend_balance_granularity", "per_band", "pbr_band", "valuation_band"}:
                    cur_text = out[c].astype(str).str.strip()
                    valid_cur = cur_text.ne("") & cur_text.str.lower().ne("nan")
                    out[c] = out[c].where(valid_cur, tmp[dup])
                else:
                    cur = pd.to_numeric(out[c], errors="coerce")
                    out[c] = out[c].where(cur.notna(), tmp[dup])
    return out


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _norm_date8(v: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s[:8] if len(s) >= 8 else ""


def _safe_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        try:
            return json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:
            return None


def _latest_json_by_glob(pattern: str) -> Optional[Dict[str, Any]]:
    files = sorted(LOGS.glob(pattern), key=lambda p: p.stat().st_mtime)
    for path in reversed(files):
        obj = _safe_json(path)
        if isinstance(obj, dict):
            return obj
    return None


def _dated_input_rank(path: Path) -> tuple[str, float]:
    if not path.exists():
        return ("", -1.0)
    try:
        df = _read_csv(path)
    except Exception:
        return ("", float(path.stat().st_mtime))
    if not isinstance(df, pd.DataFrame) or df.empty:
        return ("", float(path.stat().st_mtime))
    for col in ("date_yyyymmdd", "date", "signal_date"):
        if col in df.columns:
            s = df[col].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
            s = s[s.str.len() == 8]
            if len(s):
                return (str(s.max()), float(path.stat().st_mtime))
    return ("", float(path.stat().st_mtime))


def _candidate_chain_asof() -> str:
    meta = _safe_json(LOGS / "candidates_latest_meta.json") or {}
    for key in ("latest_date", "as_of", "as_of_ymd"):
        d8 = _norm_date8(meta.get(key))
        if d8:
            return d8
    for path in (IN_BASE, IN_FILTERED, IN_SECTOR, IN_NEWS):
        d8, _ = _dated_input_rank(path)
        if d8:
            return d8
    return ""


def _pick_input() -> Path:
    candidates = [IN_POLICY, IN_NEWS, IN_SECTOR, IN_FILTERED, IN_BASE]
    chain_asof = _candidate_chain_asof()
    base_mtime = float(IN_BASE.stat().st_mtime) if IN_BASE.exists() else -1.0
    base_codes: set[str] = set()
    if IN_BASE.exists():
        try:
            base_df = _read_csv(IN_BASE)
            if "code" in base_df.columns:
                base_codes = set(base_df["code"].astype(str).str.extract(r"(\d+)")[0].fillna("").str.zfill(6))
                base_codes.discard("")
        except Exception:
            base_codes = set()
    ranked: list[tuple[str, int, float, Path]] = []
    stage_rank = {path: idx for idx, path in enumerate(reversed(candidates), start=1)}
    for path in candidates:
        date8, mtime = _dated_input_rank(path)
        if path != IN_BASE and base_mtime > 0 and mtime < base_mtime:
            continue
        if chain_asof and date8 and date8 < chain_asof:
            continue
        if path != IN_BASE and base_codes:
            try:
                stage_df = _read_csv(path)
                if "code" in stage_df.columns:
                    stage_codes = set(stage_df["code"].astype(str).str.extract(r"(\d+)")[0].fillna("").str.zfill(6))
                    stage_codes.discard("")
                    if not stage_codes.issubset(base_codes):
                        continue
            except Exception:
                continue
        if date8:
            ranked.append((date8, int(stage_rank.get(path, 0)), mtime, path))
    if ranked:
        ranked.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
        return ranked[0][3]
    if chain_asof:
        for path in (IN_BASE, IN_FILTERED, IN_SECTOR, IN_NEWS):
            if path.exists():
                return path
    for path in candidates:
        if path.exists():
            return path
    return IN_BASE


def _load_policy_score_map() -> Tuple[Dict[str, float], Dict[str, Any]]:
    """policy_agenda_signal_latest.json 에서 policy_score 맵 로드."""
    meta: Dict[str, Any] = {"available": False, "reason": ""}
    if not POLICY_SIGNAL_LATEST.exists():
        meta["reason"] = "signal_missing"
        return {}, meta
    try:
        obj = _safe_json(POLICY_SIGNAL_LATEST) or {}
        agenda_scores = obj.get("agenda_scores") or {}
        sector_signal = obj.get("sector_signal") or {}
        if not sector_signal:
            meta["reason"] = "sector_signal_empty"
            return {}, meta
        meta["available"] = True
        meta["reason"] = "ok"
        meta["government"] = str(obj.get("government") or "")
        meta["generated_at"] = str(obj.get("generated_at") or "")
        meta["agenda_count"] = len(agenda_scores)
        return {str(k): float(v) for k, v in sector_signal.items()}, meta
    except Exception as e:
        meta["reason"] = f"load_fail:{type(e).__name__}"
        return {}, meta


def _news_gate_from_status() -> Dict[str, Any]:
    obj = _safe_json(NEWS_SCORE_STATUS_LATEST) or {}
    collect_mode = str(os.getenv("NEWS_COLLECT_MODE", "production")).strip().lower()
    quality = str(obj.get("quality") or "FAIL").upper()
    reason = str((obj.get("meta") or {}).get("reason") or obj.get("reason") or "")
    mapped_rate = float(obj.get("mapped_rate") or 0.0)
    nonzero_rate = float(obj.get("nonzero_rate") or 0.0)
    used_lag_days = (obj.get("meta") or {}).get("used_lag_days")
    try:
        used_lag_days_i = int(used_lag_days) if used_lag_days is not None else -1
    except Exception:
        used_lag_days_i = -1
    gate_quality_levels = {
        x.strip().upper()
        for x in str(os.getenv("NEWS_GATE_QUALITY_LEVELS", "PASS,WARN")).split(",")
        if x.strip()
    }
    min_mapped_rate = max(0.0, min(1.0, float(str(os.getenv("NEWS_GATE_MIN_MAPPED_RATE", "0.50")).strip() or "0.50")))
    min_nonzero_rate = max(0.0, min(1.0, float(str(os.getenv("NEWS_GATE_MIN_NONZERO_RATE", "0.05")).strip() or "0.05")))
    stale_block = bool(
        reason in {"signals_stale_lag_3d", "signals_stale_lag_4d"}
        or str(reason).startswith("signals_stale_lag_")
    )
    # Accumulate mode: if coverage is present but candidate-article overlap is zero,
    # allow exploratory gate-open without hard fail on nonzero_rate from status.
    accumulate_soft_open = bool(
        collect_mode == "accumulate"
        and quality == "WARN"
        and "candidate_article_coverage_zero" in str(reason).lower()
        and mapped_rate >= min_mapped_rate
        and not stale_block
    )
    gate_open = bool(
        quality in gate_quality_levels
        and mapped_rate >= min_mapped_rate
        and (nonzero_rate >= min_nonzero_rate or accumulate_soft_open)
        and not stale_block
    )
    if accumulate_soft_open and nonzero_rate < min_nonzero_rate:
        nonzero_rate = float(min_nonzero_rate)
    gate_close_reasons: List[str] = []
    if quality not in gate_quality_levels:
        gate_close_reasons.append(f"quality_not_allowed:{quality}")
    if mapped_rate < min_mapped_rate:
        gate_close_reasons.append(f"mapped_rate_low:{mapped_rate:.4f}<{min_mapped_rate:.4f}")
    if nonzero_rate < min_nonzero_rate:
        gate_close_reasons.append(f"nonzero_rate_low:{nonzero_rate:.4f}<{min_nonzero_rate:.4f}")
    if stale_block:
        gate_close_reasons.append(f"stale_reason:{reason or 'signals_stale'}")
    effective_reason = "ok" if gate_open else (";".join(gate_close_reasons) if gate_close_reasons else (reason or "gate_closed"))
    return {
        "gate_open": gate_open,
        "quality": quality,
        "reason": effective_reason,
        "source_reason": reason,
        "mapped_rate": mapped_rate,
        "nonzero_rate": nonzero_rate,
        "used_lag_days": used_lag_days_i,
        "gate_quality_levels": sorted(gate_quality_levels),
        "gate_min_mapped_rate": min_mapped_rate,
        "gate_min_nonzero_rate": min_nonzero_rate,
    }


def _dynamic_news_weight(base_w_news: float, news_gate: Dict[str, Any], news_nonzero: int) -> Tuple[float, Dict[str, Any]]:
    enabled = str(os.getenv("NEWS_DYNAMIC_WEIGHT_ENABLED", "1")).strip().lower() not in {"0", "false", "no", "off"}
    if not enabled:
        return float(base_w_news), {"enabled": False, "factor": 1.0, "reason": "disabled"}

    if not bool(news_gate.get("gate_open")) or int(news_nonzero) <= 0:
        return 0.0, {"enabled": True, "factor": 0.0, "reason": "gate_closed_or_zero_news"}

    quality = str(news_gate.get("quality") or "FAIL").upper()
    mapped_rate = float(news_gate.get("mapped_rate") or 0.0)
    nonzero_rate = float(news_gate.get("nonzero_rate") or 0.0)
    raw_used_lag_days = news_gate.get("used_lag_days")
    used_lag_days = int(raw_used_lag_days) if raw_used_lag_days is not None else -1

    # base quality factor
    if quality == "PASS":
        q_factor = 1.0
    elif quality == "WARN":
        q_factor = 0.75
    else:
        q_factor = 0.0

    target_mapped = max(0.01, min(1.0, float(str(os.getenv("NEWS_DYNAMIC_TARGET_MAPPED_RATE", "0.80")).strip() or "0.80")))
    target_nonzero = max(0.01, min(1.0, float(str(os.getenv("NEWS_DYNAMIC_TARGET_NONZERO_RATE", "0.20")).strip() or "0.20")))
    mapped_factor = min(1.0, max(0.0, mapped_rate / target_mapped))
    nonzero_factor = min(1.0, max(0.0, nonzero_rate / target_nonzero))

    lag_factor = 1.0
    if used_lag_days >= 2:
        lag_factor = 0.70
    elif used_lag_days == 1:
        lag_factor = 0.85

    factor = max(0.0, min(1.0, q_factor * mapped_factor * nonzero_factor * lag_factor))
    eff = round(float(base_w_news) * float(factor), 6)
    detail = {
        "enabled": True,
        "factor": round(float(factor), 6),
        "quality_factor": round(float(q_factor), 6),
        "mapped_factor": round(float(mapped_factor), 6),
        "nonzero_factor": round(float(nonzero_factor), 6),
        "lag_factor": round(float(lag_factor), 6),
        "target_mapped_rate": target_mapped,
        "target_nonzero_rate": target_nonzero,
        "used_lag_days": used_lag_days,
        "reason": "ok",
    }
    return eff, detail


def _max_date8(df: pd.DataFrame) -> str:
    if "date_yyyymmdd" in df.columns:
        s = df["date_yyyymmdd"].astype(str)
    elif "date" in df.columns:
        s = df["date"].astype(str)
    elif "signal_date" in df.columns:
        s = df["signal_date"].astype(str)
    else:
        return datetime.now().strftime("%Y%m%d")
    d8 = s.map(_norm_date8)
    d8 = d8[d8.str.len() == 8]
    if len(d8):
        return str(d8.max())
    chain_asof = _candidate_chain_asof()
    return chain_asof or datetime.now().strftime("%Y%m%d")


def _regime_score(risk_on: bool, regime: str) -> float:
    r = str(regime or "").upper()
    if not bool(risk_on):
        return -0.20
    if r in {"CRASH", "RISK_OFF", "BEAR"}:
        return -0.10
    if r in {"NORMAL", "BULL"}:
        return 0.20
    return 0.10


def _load_fx_context() -> Dict[str, Any]:
    obj = _safe_json(MACRO_FEATURE_LATEST) or {}
    fx = obj.get("fx_context") if isinstance(obj.get("fx_context"), dict) else {}
    return fx if isinstance(fx, dict) else {}


def _normalize_fundamental_score(v: Any) -> float:
    try:
        x = float(v)
    except Exception:
        return 0.0
    if pd.isna(x):
        return 0.0
    if x > 1.0:
        if x >= 10.0:
            x = x / 100.0
        else:
            x = 1.0
    return round(max(0.0, min(1.0, float(x))), 6)


def _clip01(v: Any) -> float:
    try:
        return float(max(0.0, min(1.0, float(v))))
    except Exception:
        return 0.0


def _scaled_range(v: Any, low: float, high: float) -> float:
    try:
        x = float(v)
    except Exception:
        return 0.0
    if high <= low:
        return 0.0
    return _clip01((x - low) / (high - low))


def _scaled_center(v: Any, center: float, half_range: float) -> float:
    try:
        x = float(v)
    except Exception:
        return 0.0
    if half_range <= 0:
        return 0.0
    return _clip01(0.5 + ((x - center) / (2.0 * half_range)))


def _numeric_series(df: pd.DataFrame, col: str, default: float) -> pd.Series:
    if col not in df.columns:
        return pd.Series([default] * len(df), index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors="coerce").fillna(default)


def _numeric_series_nan(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([pd.NA] * len(df), index=df.index, dtype="Float64")
    return pd.to_numeric(df[col], errors="coerce")


def _percentile_pressure(df: pd.DataFrame, col: str) -> pd.Series:
    s = _numeric_series_nan(df, col).astype(float)
    s = s.where(s <= 1.5, s / 100.0)
    return s.clip(lower=0.0, upper=1.0)


def _band_pressure(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([pd.NA] * len(df), index=df.index, dtype="Float64")
    labels = df[col].fillna("").astype(str).str.upper()

    def conv(v: str) -> Any:
        if not v:
            return pd.NA
        if any(x in v for x in ("CHEAP", "LOW", "UNDER", "DISCOUNT", "저평가", "저렴")):
            return 0.20
        if any(x in v for x in ("EXPENSIVE", "HIGH", "OVER", "PREMIUM", "고평가", "과열")):
            return 0.80
        if any(x in v for x in ("FAIR", "MID", "NORMAL", "NEUTRAL", "적정", "중립")):
            return 0.50
        return pd.NA

    return labels.map(conv).astype("Float64")


def _mean_available(parts: List[pd.Series], default: float) -> Tuple[pd.Series, pd.Series]:
    if not parts:
        base = pd.Series([default] * 0, dtype=float)
        return base, pd.Series([0] * 0, dtype=int)
    frame = pd.concat(parts, axis=1)
    count = frame.notna().sum(axis=1).astype(int)
    mean = frame.mean(axis=1).fillna(default).astype(float)
    return mean, count


def _build_fundamental_prereflection_score(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    out = df.copy()
    fund = pd.to_numeric(out.get("fundamental_score_01", 0.0), errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0)

    valuation_parts = [
        _percentile_pressure(out, "per_percentile"),
        _percentile_pressure(out, "pbr_percentile"),
        _band_pressure(out, "valuation_band"),
        _band_pressure(out, "per_band"),
        _band_pressure(out, "pbr_band"),
    ]
    valuation_pressure, valuation_count = _mean_available(valuation_parts, 0.50)

    peg = _numeric_series_nan(out, "peg_ratio").astype(float)
    peg_pressure = ((peg - 0.8) / (2.5 - 0.8)).clip(lower=0.0, upper=1.0)
    forward_per = _numeric_series_nan(out, "forward_per").astype(float)
    forward_per_pressure = ((forward_per - 8.0) / (35.0 - 8.0)).clip(lower=0.0, upper=1.0)
    price_pressure, price_pressure_count = _mean_available(
        [valuation_pressure.where(valuation_count > 0), peg_pressure, forward_per_pressure],
        0.50,
    )

    eps_revision = _numeric_series_nan(out, "eps_revision_pct").astype(float)
    op_revision = _numeric_series_nan(out, "op_consensus_change_pct").astype(float)
    ttm_opm = _numeric_series_nan(out, "ttm_opm").astype(float)
    support_parts = [
        (0.5 + (eps_revision / 40.0)).clip(lower=0.0, upper=1.0),
        (0.5 + (op_revision / 40.0)).clip(lower=0.0, upper=1.0),
        (ttm_opm / 15.0).clip(lower=0.0, upper=1.0),
    ]
    support_score, support_count = _mean_available(support_parts, 0.50)
    evidence_count = (price_pressure_count + support_count).astype(int)

    adjustment = ((0.5 - price_pressure) * 0.30 + (support_score - 0.5) * 0.20).where(evidence_count > 0, 0.0)
    pre_score = (fund + adjustment).clip(lower=0.0, upper=1.0).round(6)

    label = pd.Series("NEUTRAL", index=out.index, dtype=object)
    label = label.where(~((fund >= 0.50) & (price_pressure <= 0.35) & (support_score >= 0.45)), "UNDER_REFLECTED")
    label = label.where(~((fund >= 0.50) & (price_pressure >= 0.70) & (support_score <= 0.55)), "OVER_REFLECTED")
    label = label.where(evidence_count > 0, "INSUFFICIENT_DATA")

    out["fundamental_prereflection_score"] = pre_score
    out["fundamental_prereflection_label"] = label
    out["fundamental_prereflection_source"] = label.map(
        lambda x: "PREREFLECTION_NEUTRAL_INSUFFICIENT_DATA"
        if str(x) == "INSUFFICIENT_DATA"
        else "FUNDAMENTAL_PREREFLECTION_V1"
    )
    out["fundamental_prereflection_evidence_count"] = evidence_count
    out["fundamental_prereflection_valuation_pressure"] = price_pressure.round(6)
    out["fundamental_prereflection_support_score"] = support_score.round(6)
    out["fundamental_prereflection_adjustment"] = adjustment.round(6)

    meta = {
        "source": "FUNDAMENTAL_PREREFLECTION_V1",
        "method": "fundamental_score adjusted by valuation pressure and support evidence",
        "rows": int(len(out)),
        "evaluable_rows": int((evidence_count > 0).sum()),
        "insufficient_rows": int((evidence_count <= 0).sum()),
        "label_counts": {str(k): int(v) for k, v in label.value_counts().to_dict().items()},
    }
    return out, meta


def _load_lob_map() -> Tuple[Dict[str, Dict[str, float]], Dict[str, Any]]:
    meta: Dict[str, Any] = {"available": False, "path": str(SURGE_LOB_LATEST), "rows": 0, "reason": "missing"}
    if not SURGE_LOB_LATEST.exists():
        return {}, meta
    try:
        lob = _read_csv(SURGE_LOB_LATEST)
    except Exception as e:
        meta["reason"] = f"read_failed:{type(e).__name__}"
        return {}, meta
    if lob.empty or "code" not in lob.columns:
        meta["reason"] = "empty_or_missing_code"
        return {}, meta
    lob = lob.copy()
    lob["code"] = lob["code"].astype(str).str.extract(r"(\d+)")[0].fillna("").str.zfill(6)
    numeric_cols = (
        "spread_bps",
        "order_imbalance_l1",
        "ofi_norm",
        "kyle_lambda_z",
        "markout_1step_bps",
        "orderflow_risk_score",
    )
    for col in numeric_cols:
        if col not in lob.columns:
            lob[col] = 0.0
        lob[col] = pd.to_numeric(lob[col], errors="coerce").fillna(0.0)
    if "lob_available" in lob.columns:
        lob["lob_available"] = lob["lob_available"].astype(str).str.lower().isin(["true", "1", "yes"])
    else:
        lob["lob_available"] = False
    if "lob_status" not in lob.columns:
        lob["lob_status"] = ""
    if "orderflow_tag" not in lob.columns:
        lob["orderflow_tag"] = ""
    out: Dict[str, Dict[str, float]] = {}
    for _, row in lob.drop_duplicates("code", keep="last").iterrows():
        out[str(row["code"])] = {
            "spread_bps": float(row.get("spread_bps", 0.0) or 0.0),
            "order_imbalance_l1": float(row.get("order_imbalance_l1", 0.0) or 0.0),
            "ofi_norm": float(row.get("ofi_norm", 0.0) or 0.0),
            "kyle_lambda_z": float(row.get("kyle_lambda_z", 0.0) or 0.0),
            "markout_1step_bps": float(row.get("markout_1step_bps", 0.0) or 0.0),
            "orderflow_risk_score": float(row.get("orderflow_risk_score", 0.0) or 0.0),
            "lob_available": 1.0 if bool(row.get("lob_available", False)) else 0.0,
            "lob_status": str(row.get("lob_status", "") or ""),
            "orderflow_tag": str(row.get("orderflow_tag", "") or ""),
        }
    meta.update({"available": True, "rows": int(len(lob)), "reason": "ok"})
    return out, meta


def _execution_adjustment_from_lob(lob: Dict[str, Any]) -> Tuple[float, float]:
    if not lob or float(lob.get("lob_available", 0.0) or 0.0) <= 0:
        return 0.0, 0.0
    spread_bps = float(lob.get("spread_bps", 0.0) or 0.0)
    imbalance = float(lob.get("order_imbalance_l1", 0.0) or 0.0)
    orderflow_risk = max(0.0, min(1.0, float(lob.get("orderflow_risk_score", 0.0) or 0.0)))
    spread_score = 1.0 - max(0.0, min(1.0, spread_bps / 120.0))
    imbalance_score = max(-1.0, min(1.0, imbalance))
    exec_score = max(-1.0, min(1.0, (spread_score - 0.5) * 1.2 + imbalance_score * 0.4 - orderflow_risk * 0.8))
    return round(float(exec_score), 6), round(float(exec_score) * 0.03, 6)


def _leader_coupling_for_row(row: pd.Series) -> Tuple[float, float]:
    ret1 = pd.to_numeric(pd.Series([row.get("ret1_pct", 0.0)]), errors="coerce").fillna(0.0).iloc[0] / 100.0
    rs_slope = pd.to_numeric(pd.Series([row.get("rs_slope", 1.0)]), errors="coerce").fillna(1.0).iloc[0]
    v_accel = pd.to_numeric(pd.Series([row.get("v_accel", 1.0)]), errors="coerce").fillna(1.0).iloc[0]
    macd = str(row.get("macd_golden", "")).strip().lower() in {"true", "1", "yes"}
    score = (
        max(-1.0, min(1.0, float(ret1) / 0.05)) * 0.35
        + max(-1.0, min(1.0, (float(rs_slope) - 1.0) / 0.35)) * 0.30
        + max(-1.0, min(1.0, (float(v_accel) - 1.0) / 2.0)) * 0.25
        + (0.10 if macd else 0.0)
    )
    score = max(-1.0, min(1.0, float(score)))
    return round(score, 6), round(score * 0.08, 6)


def _apply_final_sector_leader_fallback(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    out = df.copy()
    for col, default in (
        ("sector_leader_code", ""),
        ("sector_leader_name", ""),
        ("sector_leader_coupling", 0.0),
        ("sector_leader_effect", 0.0),
    ):
        if col not in out.columns:
            out[col] = default
    out["sector_leader_code"] = out["sector_leader_code"].astype(str)
    out["sector_leader_name"] = out["sector_leader_name"].astype(str)
    if out.empty or "sector_code" not in out.columns:
        return out, {"status": "SKIP", "reason": "empty_or_missing_sector_code", "filled_rows": 0}

    work = out.copy()
    work["sector_code"] = work["sector_code"].astype(str).str.strip()
    work = work[(work["sector_code"] != "") & (work["sector_code"].str.lower() != "nan")].copy()
    if work.empty:
        return out, {"status": "SKIP", "reason": "no_sector_rows", "filled_rows": 0}
    if "market_cap" in work.columns:
        work["_leader_size"] = pd.to_numeric(work["market_cap"], errors="coerce").fillna(0.0)
    elif "value" in work.columns:
        work["_leader_size"] = pd.to_numeric(work["value"], errors="coerce").fillna(0.0)
    else:
        work["_leader_size"] = 0.0
    leaders = (
        work.sort_values(["sector_code", "_leader_size", "code"], ascending=[True, False, True])
            .groupby("sector_code", as_index=False, group_keys=False)
            .head(1)
    )
    leader_map = {str(r["sector_code"]): r for _, r in leaders.iterrows()}
    filled = 0
    adjusted = 0
    for idx, row in out.iterrows():
        cur_leader = str(row.get("sector_leader_code", "") or "").strip()
        if cur_leader and cur_leader.lower() != "nan":
            continue
        leader = leader_map.get(str(row.get("sector_code", "") or "").strip())
        if leader is None:
            continue
        coupling, effect = _leader_coupling_for_row(leader)
        out.at[idx, "sector_leader_code"] = str(leader.get("code", "") or "").zfill(6)
        out.at[idx, "sector_leader_name"] = str(leader.get("name", "") or "")
        out.at[idx, "sector_leader_coupling"] = coupling
        out.at[idx, "sector_leader_effect"] = effect
        base = float(pd.to_numeric(pd.Series([row.get("sector_score", 0.0)]), errors="coerce").fillna(0.0).iloc[0])
        out.at[idx, "sector_score"] = round(max(-1.0, min(1.0, base + effect)), 6)
        filled += 1
        if effect != 0:
            adjusted += 1
    return out, {"status": "OK", "groups": int(len(leaders)), "filled_rows": int(filled), "adjusted_rows": int(adjusted)}


def _forecast_label(v: Any) -> str:
    score = _clip01(v)
    if score >= 0.70:
        return "LEAD_STRONG"
    if score >= 0.55:
        return "LEAD_WATCH"
    if score >= 0.40:
        return "NEUTRAL"
    return "WEAK"


def _macro_freshness_guard() -> Dict[str, Any]:
    obj = _safe_json(MACRO_FEATURE_LATEST) or {}
    summary = obj.get("indicator_freshness_summary") if isinstance(obj.get("indicator_freshness_summary"), dict) else {}
    mapping = obj.get("indicator_source_mapping") if isinstance(obj.get("indicator_source_mapping"), list) else []
    stale = int(summary.get("stale") or 0)
    unknown = int(summary.get("unknown") or 0)
    total = int(summary.get("total") or len(mapping) or 0)
    bad = stale + unknown
    bad_items: List[Dict[str, Any]] = []
    deferred_items: List[Dict[str, Any]] = []
    try:
        required_stale_grace_days = max(0, int(str(os.getenv("MACRO_REQUIRED_STALE_GRACE_DAYS", "1")).strip() or "1"))
    except Exception:
        required_stale_grace_days = 1
    for item in mapping:
        if not isinstance(item, dict):
            continue
        freshness = str(item.get("freshness") or "").upper()
        if freshness not in {"STALE", "UNKNOWN"}:
            continue
        row = {
            "indicator": str(item.get("indicator") or ""),
            "series_id": str(item.get("series_id") or ""),
            "freshness": freshness,
            "latest_date": item.get("latest_date"),
            "age_days": item.get("age_days"),
            "max_age_days": item.get("max_age_days"),
        }
        score_role = str(item.get("score_role") or "")
        if str(item.get("series_id") or "") in MACRO_DEFERRED_OPTIONAL_SERIES or (
            score_role and score_role != "required_guard"
        ):
            deferred_items.append({
                **row,
                "score_role": score_role,
                "deferred_reason": "optional_macro_input_no_score_penalty_when_missing",
            })
            continue
        try:
            age_days = int(float(item.get("age_days")))
            max_age_days = int(float(item.get("max_age_days")))
        except Exception:
            age_days = -1
            max_age_days = -1
        if freshness == "STALE" and age_days >= 0 and max_age_days >= 0 and age_days <= max_age_days + required_stale_grace_days:
            deferred_items.append({
                **row,
                "deferred_reason": "required_macro_input_forward_fill_grace",
                "grace_days": int(required_stale_grace_days),
            })
            continue
        bad_items.append(row)
    required_bad = int(len(bad_items))
    deferred_bad = int(len(deferred_items))
    status = "FAIL" if required_bad > 0 else ("PASS" if total > 0 else "FAIL")
    if required_bad > 0:
        reason = "stale_or_unknown_required_macro_inputs"
    elif total > 0 and deferred_bad > 0:
        reason = "ok_with_deferred_optional_macro_inputs"
    elif total > 0:
        reason = "ok"
    else:
        reason = "macro_freshness_missing"
    return {
        "available": bool(obj),
        "as_of_ymd": str(obj.get("as_of_ymd") or ""),
        "generated_for_ymd": str(obj.get("generated_for_ymd") or ""),
        "generated_at": str(obj.get("generated_at") or ""),
        "source_tier_summary": obj.get("indicator_source_tier_summary") if isinstance(obj.get("indicator_source_tier_summary"), dict) else {},
        "ok": int(summary.get("ok") or 0),
        "stale": stale,
        "unknown": unknown,
        "total": total,
        "bad": bad,
        "required_bad": required_bad,
        "deferred_bad": deferred_bad,
        "status": status,
        "reason": reason,
        "bad_items": bad_items,
        "deferred_items": deferred_items,
    }


def _macro_tier_weight_factor(macro_freshness: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    summary = macro_freshness.get("source_tier_summary") if isinstance(macro_freshness.get("source_tier_summary"), dict) else {}
    counts = summary.get("counts") if isinstance(summary.get("counts"), dict) else {}
    total_weight = 0.0
    total_count = 0
    for tier, count in counts.items():
        try:
            c = int(count or 0)
        except Exception:
            c = 0
        if c <= 0:
            continue
        total_count += c
        total_weight += c * float(MACRO_TIER_WEIGHT_MULTIPLIERS.get(str(tier), 0.0))
    factor = (total_weight / total_count) if total_count > 0 else 0.0
    required_bad = int(macro_freshness.get("required_bad") or 0)
    if required_bad > 0:
        factor = 0.0
    return float(max(0.0, min(1.0, factor))), {
        "counts": counts,
        "factor": float(max(0.0, min(1.0, factor))),
        "required_bad": required_bad,
        "multipliers": MACRO_TIER_WEIGHT_MULTIPLIERS,
    }


def _build_forecast_score(
    df: pd.DataFrame,
    *,
    news_gate_open: bool = True,
    macro_forecast_allowed: bool = True,
) -> pd.DataFrame:
    x = df.copy()
    feature_score = _numeric_series(x, "score", 0.0).clip(lower=0.0, upper=1.0)
    macro_score_raw = _numeric_series(x, "regime_score", 0.0).clip(lower=0.0, upper=1.0)
    macro_score = macro_score_raw if bool(macro_forecast_allowed) else pd.Series([0.0] * len(x), index=x.index)
    flow_score = _numeric_series(x, "flow_score", 0.0).clip(lower=0.0, upper=1.0)
    sector_score = _numeric_series(x, "sector_score", 0.0).clip(lower=0.0, upper=1.0)
    news_score_raw = _numeric_series(x, "news_score", 0.0).clip(lower=0.0, upper=1.0)
    news_score = news_score_raw if bool(news_gate_open) else pd.Series([0.0] * len(x), index=x.index)
    fundamental_score = _numeric_series(x, "fundamental_score_01", 0.0).clip(lower=0.0, upper=1.0)
    policy_score = _numeric_series(x, "policy_score", 0.0).clip(lower=0.0, upper=1.0)

    rs_score = _numeric_series(x, "rs", 1.0).map(lambda v: _scaled_range(v, 0.90, 1.60))
    rs_slope_score = _numeric_series(x, "rs_slope", 1.0).map(lambda v: _scaled_range(v, 0.95, 2.20))
    vol_accel_score = _numeric_series(x, "v_accel", 1.0).map(lambda v: _scaled_range(v, 0.90, 4.00))
    adx_score = _numeric_series(x, "adx14", 20.0).map(lambda v: _scaled_range(v, 15.0, 45.0))
    rsi_score = _numeric_series(x, "rsi14", 50.0).map(lambda v: _scaled_center(v, 58.0, 20.0))
    stoch_score = _numeric_series(x, "stoch_k", 50.0).map(lambda v: _scaled_center(v, 62.0, 35.0))
    obv_score = _numeric_series(x, "obv_slope", 0.0).map(lambda v: _scaled_center(v, 0.0, 2_000_000.0))
    trend_score = _numeric_series(x, "sma_ema_trend_ok", 0.0).map(lambda v: 1.0 if float(v) >= 1.0 else 0.0)
    macd_col = "macd_bullish" if "macd_bullish" in x.columns else "macd_golden"
    macd_score = _numeric_series(x, macd_col, 0.0).map(lambda v: 1.0 if float(v) >= 1.0 else 0.0)

    forecast_raw = (
        feature_score * 0.16
        + macro_score * 0.10
        + flow_score * 0.10
        + sector_score * 0.11
        + news_score * 0.07
        + fundamental_score * 0.08
        + policy_score * 0.04
        + rs_score * 0.09
        + rs_slope_score * 0.07
        + vol_accel_score * 0.05
        + adx_score * 0.04
        + rsi_score * 0.03
        + stoch_score * 0.02
        + obv_score * 0.02
        + trend_score * 0.01
        + macd_score * 0.01
    )
    x["forecast_score"] = pd.to_numeric(forecast_raw, errors="coerce").fillna(0.0).astype(float).round(6).clip(lower=0.0, upper=1.0)
    x["forecast_label"] = x["forecast_score"].map(_forecast_label)
    x["forecast_score_source"] = "FORECAST_BLEND_V1"
    x["forecast_news_gate_open"] = bool(news_gate_open)
    x["forecast_macro_freshness_ok"] = bool(macro_forecast_allowed)
    return x


def _safe_corr(df: pd.DataFrame, left: str, right: str, *, invert_right: bool = False) -> Optional[float]:
    cols = [left, right]
    if any(c not in df.columns for c in cols):
        return None
    work = df[cols].copy()
    work[left] = pd.to_numeric(work[left], errors="coerce")
    work[right] = pd.to_numeric(work[right], errors="coerce")
    work = work.dropna()
    if len(work) < 3:
        return None
    if invert_right:
        work[right] = -work[right]
    try:
        val = float(work[left].corr(work[right], method="spearman"))
    except Exception:
        return None
    if pd.isna(val):
        return None
    return round(val, 6)


def _build_forecast_validation(df: pd.DataFrame, score_asof_ymd: str, macro_freshness: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "validation_type": "FORWARD_PROXY_V1",
        "status": "FAIL",
        "reason": "init",
        "score_asof_ymd": str(score_asof_ymd or ""),
        "forward_asof_ymd": "",
        "asof_alignment": "UNKNOWN",
        "source_paths": {
            "score_candidates": str(OUT),
            "forward_estimate_csv": str(FORWARD_ESTIMATE_CSV),
            "forward_estimate_meta": str(FORWARD_ESTIMATE_META),
        },
        "counts": {
            "score_rows": int(len(df)),
            "forward_rows": 0,
            "merged_rows": 0,
            "forward_proxy_rows": 0,
        },
        "coverage": {
            "merge_rate": 0.0,
            "forward_proxy_rate": 0.0,
        },
        "label_counts": {},
        "matched_label_counts": {},
        "correlations": {
            "forecast_vs_ttm_opm_spearman": None,
            "forecast_vs_forward_eps_spearman": None,
            "forecast_vs_forward_per_inverse_spearman": None,
        },
        "top_forecast_rows": [],
        "macro_freshness": macro_freshness or {},
    }

    if not FORWARD_ESTIMATE_CSV.exists():
        result["reason"] = "forward_estimate_missing"
        return result

    try:
        fwd = _read_csv(FORWARD_ESTIMATE_CSV)
    except Exception as e:
        result["reason"] = f"forward_estimate_load_fail:{type(e).__name__}"
        return result

    if "code" not in fwd.columns:
        result["reason"] = "forward_estimate_no_code"
        return result

    fwd = fwd.copy()
    fwd["code"] = fwd["code"].astype(str).str.extract(r"(\d+)")[0].fillna("").str.zfill(6)
    fwd = fwd[fwd["code"].str.len() == 6].drop_duplicates(subset=["code"], keep="last")
    result["counts"]["forward_rows"] = int(len(fwd))

    meta = _safe_json(FORWARD_ESTIMATE_META) or {}
    forward_asof = _norm_date8(meta.get("as_of_ymd"))
    if not forward_asof and "as_of_ymd" in fwd.columns:
        tmp = fwd["as_of_ymd"].astype(str).map(_norm_date8)
        tmp = tmp[tmp.str.len() == 8]
        forward_asof = str(tmp.max()) if len(tmp) else ""
    result["forward_asof_ymd"] = forward_asof

    score_df = df.copy()
    score_df["code"] = score_df["code"].astype(str).str.extract(r"(\d+)")[0].fillna("").str.zfill(6)

    proxy_base_cols = ["forward_per", "forward_eps", "ttm_opm", "peg_ratio", "current_per_wr"]
    use_cols = [c for c in ["code", *proxy_base_cols, "updated_at"] if c in fwd.columns]
    fwd_for_merge = fwd[use_cols].copy()
    fwd_rename = {c: f"{c}_forward_source" for c in use_cols if c != "code"}
    fwd_for_merge = fwd_for_merge.rename(columns=fwd_rename)
    merged = score_df.merge(fwd_for_merge, on="code", how="left")

    for c in proxy_base_cols:
        src_col = f"{c}_forward_source"
        if c in merged.columns and src_col in merged.columns:
            left = pd.to_numeric(merged[c], errors="coerce")
            right = pd.to_numeric(merged[src_col], errors="coerce")
            merged[c] = left.where(left.notna(), right)
        elif c not in merged.columns and src_col in merged.columns:
            merged[c] = pd.to_numeric(merged[src_col], errors="coerce")

    proxy_cols = [c for c in proxy_base_cols if c in merged.columns]
    if proxy_cols:
        proxy_any = merged[proxy_cols].notna().any(axis=1)
    else:
        proxy_any = pd.Series([False] * len(merged), index=merged.index)

    result["counts"]["merged_rows"] = int(merged["code"].notna().sum())
    result["counts"]["forward_proxy_rows"] = int(proxy_any.sum())
    score_rows = max(1, int(len(score_df)))
    result["coverage"]["merge_rate"] = round(float(len(merged)) / float(score_rows), 6)
    result["coverage"]["forward_proxy_rate"] = round(float(proxy_any.sum()) / float(score_rows), 6)
    result["label_counts"] = {
        str(k): int(v) for k, v in merged["forecast_label"].fillna("NEUTRAL").astype(str).value_counts().to_dict().items()
    }
    result["matched_label_counts"] = {
        str(k): int(v)
        for k, v in merged.loc[proxy_any, "forecast_label"].fillna("NEUTRAL").astype(str).value_counts().to_dict().items()
    }

    result["correlations"] = {
        "forecast_vs_ttm_opm_spearman": _safe_corr(merged.loc[proxy_any], "forecast_score", "ttm_opm"),
        "forecast_vs_forward_eps_spearman": _safe_corr(merged.loc[proxy_any], "forecast_score", "forward_eps"),
        "forecast_vs_forward_per_inverse_spearman": _safe_corr(
            merged.loc[proxy_any], "forecast_score", "forward_per", invert_right=True
        ),
    }

    top_cols = [c for c in ["code", "name", "forecast_score", "forecast_label", "final_score", "forward_per", "forward_eps", "ttm_opm", "peg_ratio"] if c in merged.columns]
    top_rows = (
        merged.sort_values(["forecast_score", "final_score"], ascending=[False, False])
        .head(5)[top_cols]
        .fillna("")
        .to_dict(orient="records")
    )
    result["top_forecast_rows"] = top_rows

    if str(score_asof_ymd) and str(forward_asof):
        result["asof_alignment"] = "MATCH" if str(score_asof_ymd) == str(forward_asof) else "MISMATCH"
    else:
        result["asof_alignment"] = "UNKNOWN"

    proxy_rate = float(result["coverage"]["forward_proxy_rate"])
    macro_status = str((macro_freshness or {}).get("status") or "PASS").upper()
    if macro_status != "PASS":
        result["status"] = "FAIL"
        result["reason"] = f"macro_freshness_{str((macro_freshness or {}).get('reason') or 'fail')}"
    elif proxy_rate <= 0.0:
        result["status"] = "FAIL"
        result["reason"] = "forward_proxy_zero_coverage"
    elif result["asof_alignment"] != "MATCH":
        result["status"] = "WARN"
        result["reason"] = "forward_asof_mismatch"
    else:
        result["status"] = "PASS"
        result["reason"] = "ok"
    return result


def _sector_fx_bucket(name: str, sector: str) -> str:
    text = f"{name} {sector}".strip().upper()
    export_keywords = [
        "자동차", "AUTO", "반도체", "SEMICON", "전자", "ELECT", "조선", "SHIP",
        "철강", "STEEL", "기계", "MACH", "방산", "DEFENSE", "정유", "OIL", "에너지", "ENERGY",
        "운송", "SHIPPING", "해상", "물류", "LOGISTICS",
        "일렉트릭", "전력기기", "변압기", "전선", "배전", "전력설비",
        "2차전지", "배터리", "BATTERY", "CELL", "이차전지",
        "바이오", "BIO", "제약", "PHARMA", "의약품", "DRUG", "헬스케어", "HEALTH",
    ]
    domestic_keywords = [
        "유통", "RETAIL", "백화점", "호텔", "여행", "항공", "AIR", "식품", "FOOD",
        "화장품", "COSMETIC", "의류", "패션", "건설", "REIT", "부동산", "전기가스", "UTILITY",
        "소비", "CONSUMER",
        "통신", "TELECOM", "게임", "GAME", "엔터", "ENTERTAIN", "콘텐츠", "CONTENT",
        "화학", "CHEM", "석유화학", "PETRO", "PETROCHEM",
    ]
    financial_keywords = ["금융", "은행", "보험", "증권", "FINANC"]

    if any(k in text for k in export_keywords):
        return "EXPORT"
    if any(k in text for k in domestic_keywords):
        return "DOMESTIC"
    if any(k in text for k in financial_keywords):
        return "FINANCIAL"
    return "NEUTRAL"


def _fx_score_for_row(name: str, sector: str, fx_ctx: Dict[str, Any]) -> Tuple[float, str]:
    if not fx_ctx or not bool(fx_ctx.get("available")):
        return 0.0, "FAIL_SOFT"

    bucket = _sector_fx_bucket(name, sector)
    band = str(fx_ctx.get("level_band") or "UNKNOWN").upper()
    vol_band = str(fx_ctx.get("volatility_band") or "UNKNOWN").upper()
    three_day_extreme = bool(fx_ctx.get("three_day_extreme"))
    trend_direction = str(fx_ctx.get("trend_direction") or "UNKNOWN").upper()
    text = f"{name} {sector}".strip().upper()
    fin_sub = "GENERIC"
    if bucket == "FINANCIAL":
        if "은행" in text or "BANK" in text:
            fin_sub = "BANK"
        elif "증권" in text or "BROKER" in text or "SECURIT" in text:
            fin_sub = "BROKER"
        elif "보험" in text or "INSUR" in text:
            fin_sub = "INSURANCE"
    score = 0.0

    if band in {"OVER_STRONG", "STRONG"}:
        if bucket == "DOMESTIC":
            score = 0.12 if band == "OVER_STRONG" else 0.10
        elif bucket == "EXPORT":
            score = -0.15 if band == "OVER_STRONG" else -0.10
        elif bucket == "FINANCIAL":
            if fin_sub == "BANK":
                score = 0.04 if band == "STRONG" else 0.01
            elif fin_sub == "BROKER":
                score = 0.03
            elif fin_sub == "INSURANCE":
                score = 0.02
            else:
                score = 0.02 if band == "STRONG" else 0.00
    elif band == "FAIR":
        score = 0.0
    elif band == "WEAK":
        if bucket == "EXPORT":
            score = 0.12
        elif bucket == "FINANCIAL":
            if fin_sub == "BANK":
                score = 0.06
            elif fin_sub == "BROKER":
                score = 0.04
            elif fin_sub == "INSURANCE":
                score = 0.03
            else:
                score = 0.04
        elif bucket == "DOMESTIC":
            score = -0.10
        else:
            score = -0.02
    elif band == "CRISIS":
        if bucket == "EXPORT":
            score = 0.08
        elif bucket == "DOMESTIC":
            score = -0.18
        elif bucket == "FINANCIAL":
            if fin_sub == "BANK":
                score = -0.08
            elif fin_sub == "BROKER":
                score = -0.12
            elif fin_sub == "INSURANCE":
                score = -0.11
            else:
                score = -0.10
        else:
            score = -0.08

    if vol_band == "HIGH":
        if score > 0:
            score *= 0.70
        else:
            score -= 0.03
    elif vol_band == "EXTREME":
        if score > 0:
            score *= 0.40
        else:
            score -= 0.08

    if three_day_extreme:
        if score > 0:
            score *= 0.60
        else:
            score -= 0.05

    # 추세 반영: 원화 약세 추세(WEAKENING)에서는 EXPORT 가산, 강세 추세에서는 EXPORT 감산.
    if trend_direction == "WEAKENING":
        if bucket == "EXPORT":
            score += 0.03
        elif bucket == "DOMESTIC":
            score -= 0.01
    elif trend_direction == "STRENGTHENING":
        if bucket == "EXPORT":
            score -= 0.03
        elif bucket == "DOMESTIC":
            score += 0.01

    # 스케일 보정: fx 영향이 과소평가되지 않도록 범위만 확대(캡 적용).
    score = max(-0.30, min(0.20, float(score) * 1.35))
    return round(float(score), 6), f"FX_{band}_{vol_band}_{trend_direction}_{bucket}_{fin_sub}"


def _load_macro_points() -> List[Tuple[str, bool, str]]:
    points: List[Tuple[str, bool, str]] = []
    for p in sorted(LOGS.glob("macro_signal_*.json")):
        obj = _safe_json(p) or {}
        d8 = _norm_date8(obj.get("as_of_ymd"))
        if not d8:
            continue
        ro = bool(obj.get("risk_on", False))
        rg = str(obj.get("regime", "NORMAL") or "NORMAL")
        points.append((d8, ro, rg))

    latest = LOGS / "macro_signal_latest.json"
    if latest.exists():
        obj = _safe_json(latest) or {}
        d8 = _norm_date8(obj.get("as_of_ymd"))
        if d8:
            ro = bool(obj.get("risk_on", False))
            rg = str(obj.get("regime", "NORMAL") or "NORMAL")
            points.append((d8, ro, rg))

    if not points:
        return []

    # dedupe by date (keep latest record per date)
    by_date: Dict[str, Tuple[bool, str]] = {}
    for d8, ro, rg in points:
        by_date[d8] = (ro, rg)

    out = [(d8, v[0], v[1]) for d8, v in by_date.items()]
    out = sorted(out, key=lambda x: x[0])
    return out


def _score_regime() -> str:
    p0 = _latest_json_by_glob(P0_DAILY_GLOB) or {}
    gate = _latest_json_by_glob(GATE_DAILY_GLOB) or {}
    macro_latest = _safe_json(LOGS / "macro_signal_latest.json") or {}

    p0_regime = str(p0.get("market_regime") or "BULL").upper()
    risk_off = bool((p0.get("snapshot") or {}).get("risk_off", {}).get("enabled", False))
    hmm_ctx = macro_latest.get("hmm_context") if isinstance(macro_latest.get("hmm_context"), dict) else {}
    hmm_regime = str(hmm_ctx.get("regime") or "").upper()
    hmm_confidence = float(hmm_ctx.get("confidence", 0.0) or 0.0)

    gate_macro = gate.get("gate_macro") if isinstance(gate.get("gate_macro"), dict) else {}
    gate_msg = str(gate_macro.get("msg") or "")
    gate_macro_status = str(gate_macro.get("status") or "PASS").upper()

    hard_gate_block = any(
        str(((gate.get(k) or {}).get("status") or "PASS")).upper() != "PASS"
        for k in ("gate0", "gate1", "gate2")
    )

    if hard_gate_block or risk_off or gate_macro_status == "BLOCK":
        return "CRASH" if p0_regime == "CRASH" else "BEAR"

    if hmm_confidence >= 0.55 and hmm_regime in {"CRASH", "BEAR"}:
        return hmm_regime

    if p0_regime == "BULL" and "macro_risk_off_soft" in gate_msg.lower():
        return "CAUTION"

    if p0_regime in SCORE_WEIGHTS_BY_REGIME:
        return p0_regime
    return "CAUTION"


def _intraday_market_context() -> Dict[str, Any]:
    rising = _safe_json(MARKET_RISING_STATUS_LATEST) or {}
    surge = _safe_json(SURGE_REALTIME_LATEST) or {}
    rising_rows = int(float(rising.get("selected_rows", 0) or 0))
    surge_alerts = int(float(surge.get("alerts_count_realtime", surge.get("alerts_count", 0)) or 0))
    if rising_rows >= 50 or surge_alerts >= 3:
        bias = "RISING_STRONG"
        forecast_delta = 0.03
        market_mode = "RISK_ON_INTRADAY"
    elif rising_rows <= 15 and surge_alerts <= 0:
        bias = "RISING_WEAK"
        forecast_delta = -0.03
        market_mode = "RISK_OFF_INTRADAY"
    else:
        bias = "NEUTRAL"
        forecast_delta = 0.0
        market_mode = "NEUTRAL_INTRADAY"
    return {
        "market_mode": market_mode,
        "bias": bias,
        "forecast_delta": round(float(forecast_delta), 6),
        "rising_rows": rising_rows,
        "surge_alerts": surge_alerts,
        "rising_ts": str(rising.get("ts") or ""),
        "surge_ts": str(surge.get("ts") or ""),
    }


def _apply_intraday_market_weights(weight_cfg: Dict[str, float], ctx: Dict[str, Any]) -> Dict[str, float]:
    out = {str(k): float(v) for k, v in weight_cfg.items()}
    delta = float(ctx.get("forecast_delta", 0.0) or 0.0)
    if abs(delta) < 1e-12:
        return out
    old_forecast = float(out.get("forecast", 0.0))
    new_forecast = max(0.05, min(0.18, old_forecast + delta))
    actual_delta = new_forecast - old_forecast
    out["forecast"] = new_forecast
    donors = ["sector", "regime", "fx"]
    donor_sum = sum(max(0.0, float(out.get(k, 0.0))) for k in donors)
    if donor_sum > 0:
        for k in donors:
            out[k] = max(0.0, float(out.get(k, 0.0)) - actual_delta * (float(out.get(k, 0.0)) / donor_sum))
    total = sum(float(v) for v in out.values())
    if total > 0:
        out = {k: round(float(v) / total, 6) for k, v in out.items()}
    return out


def _regime_asof(date8: str, points: List[Tuple[str, bool, str]]) -> Tuple[str, float, str]:
    if not points:
        return "FAIL_SOFT", 0.0, "FAIL_SOFT"
    dates = [d for d, _, _ in points]
    i = bisect_right(dates, str(date8)) - 1
    if i >= 0:
        d, ro, rg = points[i]
        return rg, _regime_score(ro, rg), "MACRO_ASOF"
    d, ro, rg = points[0]
    return rg, _regime_score(ro, rg), "MACRO_FORWARD_FILL"


def _row_date8(df: pd.DataFrame) -> pd.Series:
    if "date_yyyymmdd" in df.columns:
        return df["date_yyyymmdd"].astype(str).map(_norm_date8)
    if "date" in df.columns:
        return df["date"].astype(str).map(_norm_date8)
    if "signal_date" in df.columns:
        return df["signal_date"].astype(str).map(_norm_date8)
    return pd.Series([datetime.now().strftime("%Y%m%d")] * len(df), index=df.index)


def _restore_lineage_columns(df: pd.DataFrame) -> pd.DataFrame:
    lineage_cols = ["candidate_origin", "execution_pool", "natural_pass"]
    if not IN_BASE.exists():
        return df
    try:
        base = _read_csv(IN_BASE)
    except Exception:
        return df
    if "code" not in base.columns:
        return df

    for col in lineage_cols:
        if col not in df.columns:
            df[col] = ""
        if col not in base.columns:
            base[col] = ""

    work = df.copy()
    work["code"] = work["code"].astype(str)
    base_work = base.copy()
    base_work["code"] = base_work["code"].astype(str)

    if "date_yyyymmdd" in work.columns:
        work["date_yyyymmdd"] = work["date_yyyymmdd"].astype(str).map(_norm_date8)
    else:
        work["date_yyyymmdd"] = _row_date8(work)
    if "date_yyyymmdd" in base_work.columns:
        base_work["date_yyyymmdd"] = base_work["date_yyyymmdd"].astype(str).map(_norm_date8)
    else:
        base_work["date_yyyymmdd"] = _row_date8(base_work)

    # Prevent row explosion from many-to-many merge on duplicated (code, date_yyyymmdd) keys.
    def _first_non_blank(s: pd.Series) -> str:
        ss = s.astype(str).str.strip()
        ss = ss[(ss != "") & (ss.str.lower() != "nan")]
        return str(ss.iloc[0]) if len(ss) else ""

    base_lineage = (
        base_work[["code", "date_yyyymmdd"] + lineage_cols]
        .groupby(["code", "date_yyyymmdd"], as_index=False)
        .agg({c: _first_non_blank for c in lineage_cols})
        .rename(columns={c: f"{c}_base" for c in lineage_cols})
    )

    restored = work.merge(
        base_lineage,
        on=["code", "date_yyyymmdd"],
        how="left",
    )

    for col in lineage_cols:
        cur = restored[col].astype(str).str.strip()
        src = restored.get(f"{col}_base", "").astype(str).str.strip()
        restored[col] = cur.where(cur != "", src)
        if col in ("execution_pool", "natural_pass"):
            restored[col] = (
                restored[col]
                .astype(str)
                .str.strip()
                .str.lower()
                .map({"true": "True", "false": "False"})
                .fillna("False")
            )

    drop_cols = [c for c in restored.columns if c.endswith("_base")]
    restored = restored.drop(columns=drop_cols, errors="ignore")
    return restored


def _overlay_stage_columns(df: pd.DataFrame, path: Path, cols: List[str]) -> pd.DataFrame:
    if not path.exists() or not cols:
        return df
    try:
        src = _read_csv(path)
    except Exception:
        return df
    if src.empty or "code" not in src.columns:
        return df

    work = df.copy()
    work["code"] = work["code"].astype(str).str.extract(r"(\d+)")[0].fillna("").str.zfill(6)
    src_work = src.copy()
    src_work["code"] = src_work["code"].astype(str).str.extract(r"(\d+)")[0].fillna("").str.zfill(6)

    if "date_yyyymmdd" in work.columns:
        work["date_yyyymmdd"] = work["date_yyyymmdd"].astype(str).map(_norm_date8)
    else:
        work["date_yyyymmdd"] = _row_date8(work)
    if "date_yyyymmdd" in src_work.columns:
        src_work["date_yyyymmdd"] = src_work["date_yyyymmdd"].astype(str).map(_norm_date8)
    else:
        src_work["date_yyyymmdd"] = _row_date8(src_work)

    use_cols = [c for c in cols if c in src_work.columns]
    if not use_cols:
        return work

    src_one = src_work[["code", "date_yyyymmdd"] + use_cols].drop_duplicates(["code", "date_yyyymmdd"], keep="last")
    merged = work.merge(src_one, on=["code", "date_yyyymmdd"], how="left", suffixes=("", "_stage"))
    src_code_one = src_work[["code"] + use_cols].drop_duplicates(["code"], keep="last")
    merged = merged.merge(src_code_one, on=["code"], how="left", suffixes=("", "_code_stage"))
    for col in use_cols:
        stage_col = f"{col}_stage"
        code_stage_col = f"{col}_code_stage"
        if stage_col not in merged.columns and code_stage_col not in merged.columns:
            continue
        if stage_col not in merged.columns:
            merged[stage_col] = merged[code_stage_col]
        elif code_stage_col in merged.columns:
            stage_text = merged[stage_col].astype(str).str.strip()
            code_text = merged[code_stage_col].astype(str).str.strip()
            merged[stage_col] = merged[stage_col].where(
                (stage_text != "") & (stage_text.str.lower() != "nan"),
                merged[code_stage_col].where((code_text != "") & (code_text.str.lower() != "nan"), merged[stage_col]),
            )
        if col not in merged.columns:
            merged[col] = merged[stage_col]
        else:
            src_ser = merged[stage_col]
            if pd.api.types.is_numeric_dtype(src_ser):
                merged[col] = pd.to_numeric(src_ser, errors="coerce").where(pd.to_numeric(src_ser, errors="coerce").notna(), merged[col])
            else:
                src_text = src_ser.astype(str).str.strip()
                valid_src = (src_text != "") & (src_text.str.lower() != "nan")
                merged[col] = src_ser.astype(object).where(valid_src, merged[col].astype(object))
    return merged.drop(columns=[c for c in merged.columns if c.endswith("_stage")], errors="ignore")


def _merge_news_candidates(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    status: Dict[str, Any] = {
        "enabled": False,
        "input": str(IN_NEWS_CANDIDATES),
        "rows_in": 0,
        "updated_existing": 0,
        "added_news_only": 0,
        "rejected_bad_code_rows": 0,
        "rejected_not_in_candidate_rows": 0,
        "rejected_unknown_code_rows": 0,
        "rejected_name_mismatch_rows": 0,
        "reason": "",
    }
    if not IN_NEWS_CANDIDATES.exists():
        status["reason"] = "input_missing"
        if "candidate_origin_hybrid" not in df.columns:
            df["candidate_origin_hybrid"] = "TECH"
        return df, status
    try:
        news_df = _read_csv(IN_NEWS_CANDIDATES)
    except Exception as e:
        status["reason"] = f"read_failed:{type(e).__name__}"
        if "candidate_origin_hybrid" not in df.columns:
            df["candidate_origin_hybrid"] = "TECH"
        return df, status
    if "code" not in news_df.columns:
        status["reason"] = "missing_code"
        if "candidate_origin_hybrid" not in df.columns:
            df["candidate_origin_hybrid"] = "TECH"
        return df, status

    work = df.copy()
    work["code"] = work["code"].map(_strict_code6)
    work = work[work["code"] != ""].copy()
    if work.empty:
        status["enabled"] = True
        status["reason"] = "base_candidates_empty"
        if "candidate_origin_hybrid" not in work.columns:
            work["candidate_origin_hybrid"] = "TECH"
        return work, status
    news_df = news_df.copy()
    raw_news_rows = int(len(news_df))
    news_df["code"] = news_df["code"].map(_strict_code6)
    news_df = news_df[news_df["code"] != ""].drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    status["rejected_bad_code_rows"] = int(raw_news_rows - len(news_df))
    ref_names = _load_reference_name_map()
    if ref_names:
        if "name" not in news_df.columns:
            news_df["name"] = ""
        ref_series = news_df["code"].map(ref_names).fillna("")
        name_series = news_df["name"].map(_clean_ref_name)
        unknown = ref_series.eq("")
        mismatch = ref_series.ne("") & name_series.ne("") & name_series.ne(ref_series)
        status["rejected_unknown_code_rows"] = int(unknown.sum())
        status["rejected_name_mismatch_rows"] = int(mismatch.sum())
        news_df = news_df[~(unknown | mismatch)].copy().reset_index(drop=True)

    status["enabled"] = True
    status["rows_in"] = int(len(news_df))
    if "candidate_origin_hybrid" not in work.columns:
        work["candidate_origin_hybrid"] = "TECH"
    else:
        current = work["candidate_origin_hybrid"].astype(str).str.strip()
        work["candidate_origin_hybrid"] = current.where(current != "", "TECH")

    news_cols = [
        "news_score",
        "news_sentiment",
        "news_article_count",
        "news_freshest_age_hours",
        "news_source",
        "watch_badge",
        "krx_caution",
        "krx_warning",
        "krx_risk",
        "krx_admin",
        "news_source_scope",
    ]
    for col in news_cols:
        if col not in news_df.columns:
            news_df[col] = pd.NA
        if col not in work.columns:
            work[col] = pd.NA

    existing_codes = set(work["code"].astype(str))
    overlap_codes = existing_codes.intersection(set(news_df["code"].astype(str)))
    if overlap_codes:
        news_map = news_df.set_index("code")[news_cols]
        for col in news_cols:
            mapped = work["code"].map(news_map[col].to_dict())
            if col in ["news_score", "news_sentiment", "news_article_count", "news_freshest_age_hours"]:
                current = pd.to_numeric(work[col], errors="coerce")
                incoming = pd.to_numeric(mapped, errors="coerce")
                valid_incoming = incoming.notna() & (incoming != 0)
                work[col] = incoming.where(valid_incoming, current)
            else:
                current = work[col].astype(str).str.strip()
                incoming = mapped.astype(str).str.strip()
                valid_incoming = (incoming != "") & (incoming.str.lower() != "nan")
                work[col] = incoming.where(valid_incoming, current)
        overlap_mask = work["code"].isin(overlap_codes)
        work.loc[overlap_mask, "candidate_origin_hybrid"] = "TECH+NEWS"
        status["updated_existing"] = int(overlap_mask.sum())

    news_only = news_df[~news_df["code"].isin(existing_codes)].copy()
    collect_mode = str(os.getenv("NEWS_COLLECT_MODE", "production")).strip().lower()
    append_news_only_env = str(os.getenv("NEWS_CANDIDATES_APPEND_NEWS_ONLY", "1")).strip() != "0"
    append_news_only = collect_mode == "accumulate" or append_news_only_env
    if not news_only.empty:
        if append_news_only:
            template = {c: pd.NA for c in work.columns}
            add_rows: List[Dict[str, Any]] = []
            for _, row in news_only.iterrows():
                code = _strict_code6(row.get("code", ""))
                if not code:
                    continue
                rec = dict(template)
                rec["code"] = code
                rec["name"] = row.get("name", "")
                rec["candidate_origin_hybrid"] = "NEWS_ONLY"
                for col in news_cols:
                    rec[col] = row.get(col, pd.NA)
                add_rows.append(rec)
            if add_rows:
                add_df = pd.DataFrame(add_rows)
                add_df = add_df.dropna(axis=1, how="all")
                work = pd.concat([work, add_df], ignore_index=True)
                work["code"] = work["code"].map(_strict_code6)
                work = work[work["code"] != ""].copy()
                work = work.sort_values(["code"]).drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
                status["added_news_only"] = int(len(add_rows))
                status["reason"] = "ok_news_only_appended_accumulate" if collect_mode == "accumulate" else "ok_news_only_appended"
            else:
                status["added_news_only"] = 0
                status["reason"] = "ok_news_only_deferred"
        else:
            # Production mode keeps contract-bound behavior.
            status["added_news_only"] = 0
            status["reason"] = "ok_news_only_deferred"

    if not status["reason"]:
        status["reason"] = "ok"
    return work, status


def _news_topic_execution_effect(candidate_effect: Any) -> str:
    effect = str(candidate_effect or "").strip().lower()
    if effect in {"block_review", "avoid_chase"}:
        return "block_order"
    if effect == "reduce_size_context":
        return "reduce_size"
    if effect == "watch_only":
        return "watch_only"
    if effect == "positive_context":
        return "context_only"
    return ""


def _merge_news_judgment_l3_layer(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    status: Dict[str, Any] = {
        "enabled": False,
        "input": str(IN_NEWS_JUDGMENT_L3_LAYER),
        "rows_in": 0,
        "matched_rows": 0,
        "execution_allowed_rows": 0,
        "reason": "",
    }
    work = df.copy()
    if "code" not in work.columns:
        status["reason"] = "candidate_missing_code"
        return work, status
    if not IN_NEWS_JUDGMENT_L3_LAYER.exists():
        status["reason"] = "input_missing"
        return work, status
    try:
        l3_df = pd.read_csv(IN_NEWS_JUDGMENT_L3_LAYER, encoding="utf-8-sig", dtype=str)
    except Exception as e:
        status["reason"] = f"read_failed:{type(e).__name__}"
        return work, status
    if l3_df.empty or "code" not in l3_df.columns:
        status["reason"] = "empty_or_missing_code"
        return work, status
    l3_df = l3_df.copy()
    l3_df["code"] = l3_df["code"].map(_strict_code6)
    l3_df = l3_df[l3_df["code"] != ""].copy()
    l3_df = l3_df.drop_duplicates(subset=["code"], keep="last").reset_index(drop=True)
    cols = [
        "judgment_id",
        "l3_execution_effect",
        "execution_allowed",
        "execution_denied_reason",
        "reviewed_execution_allowed",
        "source_trading_effect",
        "active_for_l3",
        "consumer",
        "processing_state",
    ]
    for col in cols:
        if col not in l3_df.columns:
            l3_df[col] = ""
    add = l3_df[["code"] + cols].rename(
        columns={
            "judgment_id": "news_topic_l3_judgment_id",
            "l3_execution_effect": "news_topic_l3_execution_effect",
            "execution_allowed": "news_topic_l3_execution_allowed",
            "execution_denied_reason": "news_topic_l3_execution_denied_reason",
            "reviewed_execution_allowed": "news_topic_l3_reviewed_execution_allowed",
            "source_trading_effect": "news_topic_l3_source_trading_effect",
            "active_for_l3": "news_topic_l3_active_for_l3",
            "consumer": "news_topic_l3_consumer",
            "processing_state": "news_topic_l3_processing_state",
        }
    )
    add_cols = [c for c in add.columns if c != "code"]
    work = work.drop(columns=[c for c in add_cols if c in work.columns], errors="ignore")
    work["code"] = work["code"].map(_strict_code6)
    work = work[work["code"] != ""].copy()
    work = work.merge(add, on="code", how="left")
    allowed = work.get("news_topic_l3_execution_allowed", pd.Series("", index=work.index)).fillna("").astype(str).str.lower().isin({"true", "1", "yes", "y"})
    matched = work.get("news_topic_l3_judgment_id", pd.Series("", index=work.index)).fillna("").astype(str).str.strip().ne("")
    status.update(
        {
            "enabled": True,
            "rows_in": int(len(l3_df)),
            "matched_rows": int(matched.sum()),
            "execution_allowed_rows": int(allowed.sum()),
            "reason": "ok",
        }
    )
    return work, status


def _merge_medium_news_adjustment(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    status: Dict[str, Any] = {
        "enabled": False,
        "input": str(IN_NEWS_MEDIUM_ADJUSTMENT),
        "rows_in": 0,
        "matched_codes": 0,
        "penalize_codes": 0,
        "boost_codes": 0,
        "avoid_chase_codes": 0,
        "reason": "",
    }
    work = df.copy()
    work["medium_news_adjustment"] = 0.0
    if "code" not in work.columns:
        status["reason"] = "candidate_missing_code"
        return work, status
    if not IN_NEWS_MEDIUM_ADJUSTMENT.exists():
        status["reason"] = "input_missing"
        return work, status
    try:
        adj_df = pd.read_csv(IN_NEWS_MEDIUM_ADJUSTMENT, encoding="utf-8-sig", dtype=str)
    except Exception as e:
        status["reason"] = f"read_failed:{type(e).__name__}"
        return work, status
    if adj_df.empty or "code" not in adj_df.columns:
        status["reason"] = "empty_or_missing_code"
        return work, status
    adj_df = adj_df.copy()
    adj_df["code"] = adj_df["code"].map(_strict_code6)
    adj_df = adj_df[adj_df["code"] != ""].drop_duplicates(subset=["code"], keep="last").reset_index(drop=True)
    adj_df["medium_news_adjustment"] = pd.to_numeric(adj_df.get("medium_news_adjustment", 0), errors="coerce").fillna(0.0)
    adj_map = adj_df.set_index("code")["medium_news_adjustment"].to_dict()
    action_map = adj_df.set_index("code").get("action", pd.Series(dtype=str)).to_dict() if "action" in adj_df.columns else {}
    work["code"] = work["code"].map(_strict_code6)
    work["medium_news_adjustment"] = work["code"].map(adj_map).fillna(0.0)
    matched = (work["medium_news_adjustment"] != 0.0).sum()
    actions = [action_map.get(c, "") for c in work["code"]]
    status.update({
        "enabled": True,
        "rows_in": int(len(adj_df)),
        "matched_codes": int(matched),
        "penalize_codes": int(sum(1 for a in actions if a == "penalize")),
        "boost_codes": int(sum(1 for a in actions if a == "boost")),
        "avoid_chase_codes": int(sum(1 for a in actions if a == "avoid_chase")),
        "reason": "ok",
    })
    return work, status


def _merge_news_topic_candidate_impact(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    status: Dict[str, Any] = {
        "enabled": False,
        "input": str(IN_NEWS_TOPIC_CANDIDATE_IMPACT),
        "rows_in": 0,
        "matched_rows": 0,
        "rejected_bad_code_rows": 0,
        "rejected_unknown_code_rows": 0,
        "rejected_name_mismatch_rows": 0,
        "effect_counts": {},
        "execution_effect_counts": {},
        "reason": "",
    }
    work = df.copy()
    if "code" not in work.columns:
        status["reason"] = "candidate_missing_code"
        return work, status
    if not IN_NEWS_TOPIC_CANDIDATE_IMPACT.exists():
        status["reason"] = "input_missing"
        return work, status
    work["code"] = work["code"].map(_strict_code6)
    work = work[work["code"] != ""].copy()
    candidate_codes = set(work["code"].astype(str))
    candidate_names = (
        work[["code", "name"]].dropna(subset=["code"]).drop_duplicates(subset=["code"], keep="first").set_index("code")["name"].map(_clean_ref_name).to_dict()
        if "name" in work.columns
        else {}
    )
    try:
        for enc in ("utf-8-sig", "utf-8", "cp949"):
            try:
                topic_df = pd.read_csv(IN_NEWS_TOPIC_CANDIDATE_IMPACT, encoding=enc, dtype=str)
                break
            except UnicodeDecodeError:
                continue
        else:
            topic_df = pd.read_csv(IN_NEWS_TOPIC_CANDIDATE_IMPACT, dtype=str)
    except Exception as e:
        status["reason"] = f"read_failed:{type(e).__name__}"
        return work, status
    if "code" not in topic_df.columns:
        status["reason"] = "missing_code"
        return work, status

    raw_rows = int(len(topic_df))
    topic_df = topic_df.copy()
    topic_df["code"] = topic_df["code"].map(_strict_code6)
    topic_df = topic_df[topic_df["code"] != ""].copy()
    status["rejected_bad_code_rows"] = int(raw_rows - len(topic_df))
    in_candidate = topic_df["code"].isin(candidate_codes)
    status["rejected_not_in_candidate_rows"] = int((~in_candidate).sum())
    topic_df = topic_df[in_candidate].copy()
    if candidate_names:
        if "name" not in topic_df.columns:
            topic_df["name"] = ""
        ref_series = topic_df["code"].map(candidate_names).fillna("")
        name_series = topic_df["name"].map(_clean_ref_name)
        mismatch = ref_series.ne("") & name_series.ne("") & name_series.ne(ref_series)
        status["rejected_name_mismatch_rows"] = int(mismatch.sum())
        topic_df = topic_df[~mismatch].copy()

    if topic_df.empty:
        status["enabled"] = True
        status["reason"] = "no_valid_rows"
        return work, status

    if "candidate_effect" not in topic_df.columns:
        topic_df["candidate_effect"] = ""
    topic_df = topic_df.sort_values(["code", "confirm_level", "avg_confidence"], ascending=[True, False, False], kind="mergesort")
    topic_df = topic_df.drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    l3_map = {}
    if IN_NEWS_JUDGMENT_L3_LAYER.exists():
        try:
            l3_df = pd.read_csv(IN_NEWS_JUDGMENT_L3_LAYER, encoding="utf-8-sig", dtype=str)
            if "code" in l3_df.columns:
                l3_df = l3_df.copy()
                l3_df["code"] = l3_df["code"].map(_strict_code6)
                l3_df = l3_df[l3_df["code"] != ""].drop_duplicates(subset=["code"], keep="last")
                l3_map = l3_df.set_index("code").to_dict("index")
        except Exception:
            l3_map = {}
    topic_df["news_topic_execution_effect"] = topic_df["code"].map(
        lambda code: str((l3_map.get(str(code)) or {}).get("l3_execution_effect") or "")
    )

    source_cols = [
        "topic_key",
        "topic_label",
        "topic_state",
        "direction",
        "risk_state",
        "confirm_level",
        "candidate_effect",
        "effect_reason",
        "candidate_actions",
        "avg_strength",
        "avg_confidence",
        "representative_titles",
        "direct_candidate_allowed",
        "trading_effect",
        "news_topic_execution_effect",
    ]
    for col in source_cols:
        if col not in topic_df.columns:
            topic_df[col] = ""
    rename_map = {
        "topic_key": "news_topic_key",
        "topic_label": "news_topic_label",
        "topic_state": "news_topic_state",
        "direction": "news_topic_direction",
        "risk_state": "news_topic_risk_state",
        "confirm_level": "news_topic_confirm_level",
        "candidate_effect": "news_topic_candidate_effect",
        "effect_reason": "news_topic_effect_reason",
        "candidate_actions": "news_topic_candidate_actions",
        "avg_strength": "news_topic_avg_strength",
        "avg_confidence": "news_topic_avg_confidence",
        "representative_titles": "news_topic_representative_titles",
        "direct_candidate_allowed": "news_topic_direct_candidate_allowed",
        "trading_effect": "news_topic_source_trading_effect",
    }
    merge_cols = ["code"] + source_cols
    add = topic_df[merge_cols].rename(columns=rename_map)
    existing_topic_cols = [c for c in add.columns if c != "code"]
    work = work.drop(columns=[c for c in existing_topic_cols if c in work.columns], errors="ignore")
    work = work.merge(add, on="code", how="left")
    matched = 0
    if "news_topic_candidate_effect" in work.columns:
        matched_text = work["news_topic_candidate_effect"].fillna("").astype(str).str.strip()
        matched = int((matched_text != "").sum())

    status["enabled"] = True
    status["rows_in"] = int(len(topic_df))
    status["matched_rows"] = int(matched)
    status["effect_counts"] = {
        str(k): int(v)
        for k, v in topic_df["candidate_effect"].fillna("").astype(str).str.strip().value_counts().to_dict().items()
        if str(k).strip()
    }
    status["execution_effect_counts"] = {
        str(k): int(v)
        for k, v in topic_df["news_topic_execution_effect"].fillna("").astype(str).str.strip().value_counts().to_dict().items()
        if str(k).strip()
    }
    status["reason"] = "ok"
    return work, status


def _apply_collect_mode_defaults() -> str:
    """NEWS_COLLECT_MODE 기반 파라미터 기본값 적용.
    이미 설정된 개별 env var는 덮어쓰지 않음 — 개별 값이 항상 우선."""
    mode = str(os.getenv("NEWS_COLLECT_MODE", "production")).strip().lower()
    if mode not in {"accumulate", "production"}:
        mode = "production"
    presets: Dict[str, Dict[str, str]] = {
        "accumulate": {
            "NEWS_GATE_MIN_MAPPED_RATE": "0.20",
            "NEWS_GATE_MIN_NONZERO_RATE": "0.0",
            "NEWS_GATE_QUALITY_LEVELS": "PASS,WARN",
            "NEWS_DYNAMIC_TARGET_MAPPED_RATE": "0.50",
            "NEWS_DYNAMIC_TARGET_NONZERO_RATE": "0.10",
        },
        "production": {
            "NEWS_GATE_MIN_MAPPED_RATE": "0.50",
            "NEWS_GATE_MIN_NONZERO_RATE": "0.05",
            "NEWS_GATE_QUALITY_LEVELS": "PASS,WARN",
            "NEWS_DYNAMIC_TARGET_MAPPED_RATE": "0.80",
            "NEWS_DYNAMIC_TARGET_NONZERO_RATE": "0.20",
        },
    }
    for k, v in presets[mode].items():
        if not os.getenv(k):
            os.environ[k] = v
    return mode


def main() -> int:
    collect_mode = _apply_collect_mode_defaults()
    in_path = _pick_input()
    if not in_path.exists():
        raise SystemExit(f"[FATAL] missing input candidates file: {in_path}")

    df = _read_csv(in_path)
    if "code" not in df.columns:
        raise SystemExit(f"[FATAL] missing code column: {in_path}")
    df = _merge_missing_reference_columns(df)
    df = _restore_lineage_columns(df)
    df = _overlay_stage_columns(
        df,
        IN_SECTOR,
        [
            "krx_sector",
            "sector_code",
            "sector_action",
            "sector_entry_allowed",
            "sector_entry_weight",
            "sector_score",
            "sector_strength",
            "sector_leader_code",
            "sector_leader_name",
            "sector_leader_coupling",
            "sector_leader_effect",
        ],
    )
    df, sector_leader_final_meta = _apply_final_sector_leader_fallback(df)
    df, news_candidate_status = _merge_news_candidates(df)
    df, news_topic_candidate_impact_status = _merge_news_topic_candidate_impact(df)
    df, news_judgment_l3_status = _merge_news_judgment_l3_layer(df)
    df, medium_news_adj_status = _merge_medium_news_adjustment(df)

    d8 = _row_date8(df)
    points = _load_macro_points()

    regimes: List[str] = []
    regime_scores: List[float] = []
    regime_sources: List[str] = []
    for v in d8.tolist():
        rg, rv, src = _regime_asof(str(v or ""), points)
        regimes.append(rg)
        regime_scores.append(float(rv))
        regime_sources.append(src)

    df["regime"] = regimes
    df["regime_score"] = regime_scores
    df["regime_source"] = regime_sources

    if "sector_score" not in df.columns:
        df["sector_score"] = 0.0
    if "news_score" not in df.columns:
        df["news_score"] = 0.0
    if "fundamental_score" not in df.columns:
        df["fundamental_score"] = 0.0

    # policy_score: with_policy_score.csv 에서 이미 컬럼이 있으면 사용,
    # 없으면 sector_signal 맵으로 sector_code 기반 보완
    policy_signal_map, policy_meta = _load_policy_score_map()
    if "policy_score" not in df.columns:
        df["policy_score"] = 0.0
    if policy_signal_map:
        sc_col = next((c for c in ("sector_code", "krx_sector_code") if c in df.columns), None)
        if sc_col:
            sector_keys = df[sc_col].astype(str).str.strip().map(
                lambda v: v.zfill(3) if v.isdigit() else v
            )
            mapped = sector_keys.map(policy_signal_map).fillna(0.0)
            df["policy_score"] = df["policy_score"].where(
                pd.to_numeric(df["policy_score"], errors="coerce").fillna(0.0) != 0,
                mapped,
            )
    df["policy_score"] = pd.to_numeric(df["policy_score"], errors="coerce").fillna(0.0)

    # Keep TTM quality policy aligned with snapshot builder.
    if {"ttm_revenue", "ttm_operating_profit"}.issubset(df.columns):
        _rev = pd.to_numeric(df["ttm_revenue"], errors="coerce")
        _op = pd.to_numeric(df["ttm_operating_profit"], errors="coerce")
        _bad_op = _rev.gt(0) & _op.abs().gt(_rev.abs() * 1.1)
        df.loc[_bad_op, "ttm_operating_profit"] = pd.NA
    if {"ttm_revenue", "ttm_net_income"}.issubset(df.columns):
        _rev = pd.to_numeric(df["ttm_revenue"], errors="coerce")
        _net = pd.to_numeric(df["ttm_net_income"], errors="coerce")
        _bad_net = _rev.gt(0) & _net.abs().gt(_rev.abs() * 1.1)
        df.loc[_bad_net, "ttm_net_income"] = pd.NA

    df["sector_score"] = pd.to_numeric(df["sector_score"], errors="coerce").fillna(0.0)
    df["news_score"] = pd.to_numeric(df["news_score"], errors="coerce").fillna(0.0)
    df["regime_score"] = pd.to_numeric(df["regime_score"], errors="coerce").fillna(0.0)
    df["fundamental_score_01"] = df["fundamental_score"].map(_normalize_fundamental_score)
    df["fundamental_source"] = df["fundamental_score_01"].map(
        lambda x: "CANDIDATE_FUNDAMENTAL" if float(x) > 0 else "FAIL_SOFT"
    )
    df, prereflection_meta = _build_fundamental_prereflection_score(df)
    fx_ctx = _load_fx_context()
    fx_pairs = [
        _fx_score_for_row(str(r.get("name") or ""), str(r.get("krx_sector") or r.get("sector") or ""), fx_ctx)
        for _, r in df.iterrows()
    ]
    df["fx_score"] = [x[0] for x in fx_pairs]
    df["fx_source"] = [x[1] for x in fx_pairs]
    df["fx_band"] = str(fx_ctx.get("level_band") or "UNKNOWN")
    df["fx_volatility_band"] = str(fx_ctx.get("volatility_band") or "UNKNOWN")
    lob_map, lob_meta = _load_lob_map()
    lob_scores: List[float] = []
    lob_adjustments: List[float] = []
    lob_spreads: List[float] = []
    lob_imbalances: List[float] = []
    lob_statuses: List[str] = []
    lob_orderflow_risks: List[float] = []
    lob_orderflow_tags: List[str] = []
    lob_ofi_norms: List[float] = []
    lob_kyle_lambda_zs: List[float] = []
    lob_markouts: List[float] = []
    for _, r in df.iterrows():
        code = str(r.get("code") or "").zfill(6)
        lob = lob_map.get(code, {})
        score, adjustment = _execution_adjustment_from_lob(lob)
        lob_scores.append(score)
        lob_adjustments.append(adjustment)
        lob_spreads.append(float(lob.get("spread_bps", 0.0) or 0.0))
        lob_imbalances.append(float(lob.get("order_imbalance_l1", 0.0) or 0.0))
        lob_statuses.append(str(lob.get("lob_status", "") or ("OK" if lob else "MISSING")))
        lob_orderflow_risks.append(float(lob.get("orderflow_risk_score", 0.0) or 0.0))
        lob_orderflow_tags.append(str(lob.get("orderflow_tag", "") or ("MISSING" if not lob else "")))
        lob_ofi_norms.append(float(lob.get("ofi_norm", 0.0) or 0.0))
        lob_kyle_lambda_zs.append(float(lob.get("kyle_lambda_z", 0.0) or 0.0))
        lob_markouts.append(float(lob.get("markout_1step_bps", 0.0) or 0.0))
    df["execution_lob_score"] = lob_scores
    df["execution_lob_adjustment"] = lob_adjustments
    df["execution_spread_bps"] = lob_spreads
    df["execution_order_imbalance_l1"] = lob_imbalances
    df["execution_lob_status"] = lob_statuses
    df["execution_orderflow_risk_score"] = lob_orderflow_risks
    df["execution_orderflow_tag"] = lob_orderflow_tags
    df["execution_ofi_norm"] = lob_ofi_norms
    df["execution_kyle_lambda_z"] = lob_kyle_lambda_zs
    df["execution_markout_1step_bps"] = lob_markouts
    if "sector_leader_code" in df.columns:
        df["sector_leader_code"] = (
            df["sector_leader_code"]
            .astype(str)
            .str.extract(r"(\d+)")[0]
            .fillna("")
            .map(lambda x: str(x).zfill(6) if str(x).strip() else "")
        )
    news_nonzero = int((df["news_score"] != 0).sum())
    policy_nonzero = int((df["policy_score"] != 0).sum())
    news_gate = _news_gate_from_status()
    # In accumulate mode, final-score input can include NEWS_ONLY rows merged after
    # news_score status generation. Re-evaluate nonzero coverage from the current df.
    if str(collect_mode).lower() == "accumulate" and len(df) > 0:
        live_nonzero_rate = float(news_nonzero) / float(len(df))
        if live_nonzero_rate > float(news_gate.get("nonzero_rate") or 0.0):
            news_gate["nonzero_rate"] = live_nonzero_rate
        gate_quality_levels = set(news_gate.get("gate_quality_levels") or [])
        min_mapped_rate = float(news_gate.get("gate_min_mapped_rate") or 0.0)
        min_nonzero_rate = float(news_gate.get("gate_min_nonzero_rate") or 0.0)
        quality_ok = str(news_gate.get("quality") or "").upper() in gate_quality_levels
        mapped_ok = float(news_gate.get("mapped_rate") or 0.0) >= min_mapped_rate
        nonzero_ok = float(news_gate.get("nonzero_rate") or 0.0) >= min_nonzero_rate
        news_gate["gate_open"] = bool(quality_ok and mapped_ok and nonzero_ok)
        if news_gate["gate_open"]:
            news_gate["reason"] = "ok"
    news_gate_effective = bool(news_gate.get("gate_open")) and (news_nonzero > 0)
    macro_freshness = _macro_freshness_guard()
    macro_forecast_allowed = str(macro_freshness.get("status") or "FAIL").upper() == "PASS"
    macro_tier_factor, macro_tier_policy = _macro_tier_weight_factor(macro_freshness)
    score_regime = _score_regime()
    intraday_market = _intraday_market_context()
    base_weight_cfg = dict(SCORE_WEIGHTS_BY_REGIME.get(score_regime, SCORE_WEIGHTS_BY_REGIME["CAUTION"]))
    weight_cfg = _apply_intraday_market_weights(base_weight_cfg, intraday_market)
    w_sector = float(weight_cfg["sector"])
    w_regime = float(weight_cfg["regime"])
    w_fx = float(weight_cfg["fx"])
    w_news_base = float(weight_cfg["news"])
    w_news, news_weight_dynamic = _dynamic_news_weight(w_news_base, news_gate, news_nonzero)
    w_fundamental = float(weight_cfg["fundamental"])
    w_prereflection = round(min(0.04, max(0.0, w_fundamental * 0.35)), 6)
    w_fundamental_quality = round(max(0.0, w_fundamental - w_prereflection), 6)
    w_policy_base = float(weight_cfg.get("policy", 0.05))
    w_forecast_base = float(weight_cfg.get("forecast", 0.05))
    w_regime_base = float(w_regime)
    w_fx_base = float(w_fx)
    w_regime = float(w_regime) * float(macro_tier_factor)
    w_fx = float(w_fx) * float(macro_tier_factor)
    w_forecast = float(w_forecast_base) if bool(macro_forecast_allowed) else 0.0
    w_forecast = float(w_forecast) * float(macro_tier_factor)
    w_policy = float(w_policy_base) if policy_nonzero > 0 and policy_meta.get("available") else 0.0

    # Disabled signal weights remain zero; unrelated weights are not scaled upward.
    # [2026-08-24] B4: _w_total 은 계산 후 어디서도 쓰이지 않았다(정규화 의도의 사체).
    #   TECH_FUND_2AXIS_20260820 이후 이 가중치들은 실효 0.0 이므로 합도 0.0 이다. 제거한다.
    w_sector = round(w_sector, 6)
    w_regime = round(w_regime, 6)
    w_news = round(w_news, 6)
    w_fx = round(w_fx, 6)
    w_fundamental = round(w_fundamental, 6)
    w_policy = round(w_policy, 6)
    w_forecast = round(w_forecast, 6)

    # [2026-08-24] 라벨이 적용된 것만 말하게 한다.
    # (80) 이 6축을 격리한 뒤에도 NEWS_ON / FORECAST_ON / PREREF_ON 을 계속 적어 왔고,
    # 이 문자열은 final_score_source -> 아카이브 매니페스트의 axis_source 로 보존된다.
    # 286행 전부가 axis_mode=TECH_FUND_2AXIS 와 같은 행에서 모순됐다.
    # 명목 가중치는 버리지 않고 상태 JSON 의 weights_nominal_unapplied 에 남긴다.
    # 상세: docs/exec-plans/active/20260824_output_self_description_fix.md
    blend_policy_base = (
        f"ASOF_BLEND_{score_regime}"
        f"_{intraday_market.get('market_mode', 'NEUTRAL_INTRADAY')}"
        f"_AXIS2_TECHFUND"
    )

    df = _build_forecast_score(
        df,
        news_gate_open=bool(news_gate_effective),
        macro_forecast_allowed=bool(macro_forecast_allowed),
    )
    for col in ("sector_score", "regime_score", "news_score", "fx_score", "fundamental_score_01", "fundamental_prereflection_score", "policy_score", "forecast_score"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    # [2026-08-20] 8축 가중합 -> 검증된 2축 공식으로 교체.
    #
    # 기존 8축(sector/regime/news/fx/fundamental_quality/fundamental_prereflection/
    # policy/forecast)에는 **기술 점수(score)가 없었다.** 이 파일 전체에서
    # score/tech_score/w_tech 사용처가 0건이었다. 그런데 후보 *선정*은 전부 기술 지표로 한다
    # (v_accel>6.6, rs, high_52w_gap, stretch, atr). 즉 선정은 기술, 순위는 비기술이었고
    # entry.py:6047 cap_signal_top_n 이 순위로 자르므로 기술적으로 가장 강한 종목이
    # 잘려나갈 수 있었다.
    #
    # 대체 공식은 generate_candidates_v41_1.py:1594-1606 이 이미 쓰는 것이며
    # 2026-08-20 에 10행 전부 차이 0.0 으로 재현 검증됐다. 새로 만드는 것이 아니다.
    #
    # 나머지 6축은 컬럼으로 계속 산출하되 여기 반영하지 않는다(observe_only 격리).
    # 제거가 아니라 격리인 이유: 제거하면 나중에 검정할 데이터도 사라진다.
    # 축은 검정을 통과할 때만 다시 편입한다.
    #
    # 근거는 "더 좋다"가 아니라 "전제와 정합하고 분해 가능하다"이다.
    # 상세: .agent/PLANS.md 2026-08-20 (80),
    #       docs/exec-plans/active/20260820_final_score_axis_reduction.md
    # stable_params 에서 가중치를 읽는다. `or` 폴백은 쓰지 않는다 --
    # 설정값 0.0 이 falsy 라 기본값으로 조용히 덮이는 트랩이 있다(2026-07-24 10건 수정 이력).
    def _w_from_stable(key: str, default: float) -> float:
        try:
            sp = json.loads((ROOT / "12_Risk_Controlled" / "stable_params_v41_1.json").read_text(encoding="utf-8-sig"))
            v = sp.get(key)
            return default if v is None else float(v)
        except Exception:
            return default

    _w_tech_raw = _w_from_stable("w_tech_score", 0.75)
    _w_fund_raw = _w_from_stable("w_fundamental_score", 0.25)
    _ws = _w_tech_raw + _w_fund_raw
    if _ws <= 0:
        _w_tech_raw, _w_fund_raw, _ws = 0.75, 0.25, 1.0
    _w_tech, _w_fund = _w_tech_raw / _ws, _w_fund_raw / _ws
    # [2026-08-24] 결측이 점수가 되는 것을 멈춘다.
    #
    # 기존: score 결측 -> 0.0, fundamental_score 결측 -> 50.0 으로 채운 뒤 가중합.
    # 둘 다 결측이면 0*0.75 + 0.5*0.25 = **0.125 가 창작된다.**
    # 아는 것이 하나도 없는 종목이 "중립 재무를 가진 종목"으로 번역된다.
    # 2026-08-24 실측: 22행 중 12행(전부 NEWS_ONLY 템플릿 행)이 이 값이었다.
    #
    # 한쪽만 결측이면 정보가 하나는 있으므로 기존 폴백을 유지한다.
    # 둘 다 결측이면 base 를 만들지 않는다(NaN). 그리고 그 사실을 컬럼에 남긴다.
    # 상세: docs/exec-plans/active/20260824_missing_becomes_score_fix.md
    _tech_raw = pd.to_numeric(df.get("score"), errors="coerce")
    _fund_raw = pd.to_numeric(df.get("fundamental_score"), errors="coerce")
    _both_missing = _tech_raw.isna() & _fund_raw.isna()
    _tech = _tech_raw.fillna(0.0).clip(0.0, 1.0)
    # [2026-08-24] B2: 이 폴백은 문서상 'FAIL-CLOSED' 로 불렸지만 실제로는 fail-OPEN 이다.
    #   재무 결측에 중립 50 점을 주므로 '데이터가 없는 종목'이 '데이터가 나쁜 종목'(예: 30점)을 이긴다.
    #   동작은 그대로 둔다 - 바꾸면 순위가 바뀌고 그것은 매매 동작 변경이다.
    #   두 축이 모두 결측인 행은 아래에서 score_inputs_missing 으로 표시하고 base 를 NA 로 만든다.
    _fund = (_fund_raw.fillna(50.0) / 100.0).clip(0.0, 1.0)
    df["final_score_base"] = (_tech * _w_tech + _fund * _w_fund).clip(0.0, 1.0).round(6)
    df["score_inputs_missing"] = _both_missing
    df.loc[_both_missing, "final_score_base"] = pd.NA
    df["final_score_axis_mode"] = "TECH_FUND_2AXIS_20260820"
    df["execution_lob_adjustment"] = pd.to_numeric(df["execution_lob_adjustment"], errors="coerce").fillna(0.0)
    df["medium_news_adjustment"] = pd.to_numeric(df.get("medium_news_adjustment", 0), errors="coerce").fillna(0.0)
    # [2026-08-20] medium_news_adjustment 를 가산에서 승산으로 바꾼다.
    #
    # build_news_medium_adjustment_daily.py 의 설계 의도는 docstring 그대로
    # "final_score 소폭 조정값"이며, 조정폭 3~8% 는 점수가 0~1 스케일일 때를 가정한 값이다.
    # 그러나 실제 final_score_base 는 0.015~0.120 대역이다(2026-08-19 실측 22행).
    # 그 결과 절대값 가산이 base 중앙(0.0964) 대비 31~83% 가 되어
    # "소폭 조정"이 아니라 지배적 요인이 됐다 -- 2026-08-19 상위 4개 중 3개가
    # 부스트로 만들어졌고, 한 종목은 0.0373 -> 0.0873 으로 5.8배 뛰며 최하위에서 1위가 됐다.
    #
    # 승산은 스케일 불변이므로 base 대역이 앞으로 바뀌어도 의도가 유지된다.
    # boost +0.05 -> x1.05, penalize -0.08 -> x0.92 로 읽는다.
    # execution_lob_adjustment 는 이번 범위가 아니다(가산 유지, 별도 검토).
    # 상세: .agent/PLANS.md 2026-08-20 (74)(78)(79)
    _mn = df["medium_news_adjustment"].clip(lower=-0.95, upper=0.95)
    df["final_score"] = (
        (df["final_score_base"] + df["execution_lob_adjustment"]) * (1.0 + _mn)
    ).clip(lower=0.0, upper=1.0).round(6)

    # 라벨의 뒷부분은 **실제로 곱해지고 더해진** 두 조정항의 적용 여부다.
    _mn_on = bool((_mn != 0).any())
    _lob_on = bool((df["execution_lob_adjustment"] != 0).any())
    blend_policy = (
        f"{blend_policy_base}"
        f"_{'NEWSADJ_ON' if _mn_on else 'NEWSADJ_OFF'}"
        f"_{'LOBADJ_ON' if _lob_on else 'LOBADJ_OFF'}"
    )

    df["final_score_source"] = df["final_score"].map(lambda x: blend_policy if abs(float(x)) > 1e-12 else "FAIL_SOFT")

    # [2026-08-24] 이 컬럼들은 **실효 가중치**를 적는다.
    # (80) 격리 이후 아래 다섯 축은 final_score 산술에 들어가지 않는다 - 전수 조회로 확인했다
    # (산술 참조 0건, .agent/PLANS.md (65)). 명목값을 적으면 아카이브가 그것을 이력으로
    # 보존해 "그날 뉴스 가중치"를 물었을 때 0 이 아닌 값이 나온다.
    # 명목값은 상태 JSON 의 weights_nominal_unapplied 에 이름을 붙여 남긴다.
    df["final_score_w_news"] = 0.0
    df["final_score_w_policy"] = 0.0
    df["final_score_w_forecast"] = 0.0
    df["final_score_w_fundamental_quality"] = 0.0
    df["final_score_w_fundamental_prereflection"] = 0.0
    # 이 항은 실제로 가산된다. 0.03 은 그 조정폭 상한이다
    df["final_score_w_execution_lob_adjustment"] = 0.03

    # [2026-08-24] NEWS_ONLY 템플릿 행을 순위·진입용 산출물에서 분리한다.
    #
    # 이 행들은 :1644 에서 **전 컬럼 NA 템플릿**으로 append 된 것이라 date/score/fundamental 이
    # 전부 없다. 지금까지 진입에 닿지 않은 것은 설계가 아니라 우연이었다 -
    # date NaN -> 'nan' -> 숫자만 남기면 '' 가 되어 entry.py 의 날짜 필터에 걸렸을 뿐이고,
    # 템플릿이 date 를 채우는 순간 그대로 통과한다(.agent/PLANS.md (67)).
    #
    # 데이터는 버리지 않는다 - 별도 산출물로 전 컬럼 보존한다.
    # (58) "격리 유지, 산출 계속" 방침과 같다. 축은 나중에 검정하려면 데이터가 필요하다.
    # 이것은 이미 기록된 계획이다: (57) 남은 것 4번 "NEWS_ONLY 를 별도 산출물로".
    _origin = df.get("candidate_origin_hybrid")
    if _origin is None:
        _news_only_mask = pd.Series(False, index=df.index)
    else:
        _news_only_mask = _origin.astype(str).str.strip().eq("NEWS_ONLY")
    df_news_only = df[_news_only_mask].copy()
    df_main = df[~_news_only_mask].copy()
    if len(df_news_only):
        _write_csv(NEWS_ONLY_OUT, df_news_only)
    logger.info(
        "[FINAL_SCORE] split: main=%d news_only=%d -> %s",
        len(df_main), len(df_news_only), NEWS_ONLY_OUT.name,
    )
    df = df_main

    _write_csv(OUT, df)

    asof_ymd = _max_date8(df)
    status = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "input": str(in_path),
        "output": str(OUT),
        "asof_ymd": asof_ymd,
        "collect_mode": collect_mode,
        "rows": int(len(df)),
        # [2026-08-24] 적용된 것과 계산만 된 것을 분리한다.
        # 예전 "weights" 는 레짐 테이블의 명목값을 그대로 담아서, 읽는 쪽이
        # 그 축들이 점수를 만든다고 오해하게 했다. 명목값은 축 재편입 때 필요하므로 버리지 않는다.
        "axis_mode": "TECH_FUND_2AXIS_20260820",
        "weights": {
            "_note": "final_score 산술에 실제로 들어가는 항만",
            "tech": _w_tech,
            "fundamental": _w_fund,
            "execution_lob_adjustment": 0.03,
            "medium_news_adjustment": "multiplicative",
        },
        "weights_nominal_unapplied": {
            "_note": "레짐 테이블이 준 값. (80) 격리로 final_score 에 반영되지 않는다",
            "sector": w_sector,
            "regime": w_regime,
            "news_base": w_news_base,
            "news": w_news,
            "fx": w_fx,
            "fundamental_axis": w_fundamental,
            "fundamental_quality": w_fundamental_quality,
            "fundamental_prereflection": w_prereflection,
            "policy_base": w_policy_base,
            "policy": w_policy,
            "forecast_base": w_forecast_base,
            "forecast": w_forecast,
        },
        "score_regime": score_regime,
        "base_weights": base_weight_cfg,
        "intraday_market": intraday_market,
        "nonzero_rows": {
            "sector": int((df["sector_score"] != 0).sum()),
            "regime": int((df["regime_score"] != 0).sum()),
            "news": news_nonzero,
            "fx": int((df["fx_score"] != 0).sum()),
            "fundamental": int((df["fundamental_score_01"] != 0).sum()),
            "fundamental_prereflection": int((df["fundamental_prereflection_evidence_count"] > 0).sum()),
            "policy": policy_nonzero,
            "forecast": int((df["forecast_score"] != 0).sum()),
            "execution_lob": int((df["execution_lob_score"] != 0).sum()),
            "final": int((df["final_score"] != 0).sum()),
        },
        "execution_lob": {
            **lob_meta,
            "matched_rows": int((df["execution_lob_status"] != "MISSING").sum()),
            "adjusted_rows": int((df["execution_lob_adjustment"] != 0).sum()),
            "orderflow_caution_rows": int(df["execution_orderflow_tag"].isin(["CAUTION", "PAUSE"]).sum()),
            "orderflow_pause_rows": int((df["execution_orderflow_tag"] == "PAUSE").sum()),
            "orderflow_max_risk_score": float(pd.to_numeric(df["execution_orderflow_risk_score"], errors="coerce").fillna(0.0).max() if len(df) else 0.0),
        },
        "sector_leader_final": sector_leader_final_meta,
        "forecast_score": {
            "source": "FORECAST_BLEND_V1",
            "news_gate_open": bool(news_gate_effective),
            "macro_freshness_ok": bool(macro_forecast_allowed),
            "label_counts": {
                str(k): int(v)
                for k, v in df["forecast_label"].fillna("NEUTRAL").astype(str).value_counts().to_dict().items()
            },
            "nonzero_rows": int((df["forecast_score"] != 0).sum()),
        },
        "fundamental_prereflection": prereflection_meta,
        "policy_signal": policy_meta,
        "macro_points": int(len(points)),
        "macro_freshness": macro_freshness,
        "macro_tier_weight_policy": {
            **macro_tier_policy,
            "regime_base": float(w_regime_base),
            "fx_base": float(w_fx_base),
            "forecast_base_effective_before_tier": float(w_forecast_base) if bool(macro_forecast_allowed) else 0.0,
        },
        "news_gate": {
            "gate": "OPEN" if float(w_news) > 0 else "CLOSED",
            "quality": str(news_gate.get("quality") or "FAIL"),
            "reason": str(news_gate.get("reason") or ""),
            "mapped_rate": float(news_gate.get("mapped_rate") or 0.0),
            "nonzero_rate": float(news_gate.get("nonzero_rate") or 0.0),
            "used_lag_days": int(news_gate.get("used_lag_days")) if news_gate.get("used_lag_days") is not None else -1,
        },
        "news_dynamic_weight": {
            "base_weight": float(w_news_base),
            "effective_weight": float(w_news),
            **news_weight_dynamic,
        },
        "news_candidates": news_candidate_status,
        "news_topic_candidate_impact": news_topic_candidate_impact_status,
        "news_judgment_l3": news_judgment_l3_status,
        "medium_news_adjustment": medium_news_adj_status,
        "policy": blend_policy,
    }

    forecast_validation = _build_forecast_validation(df, asof_ymd, macro_freshness)
    status["forecast_validation"] = {
        "status": str(forecast_validation.get("status") or "FAIL"),
        "reason": str(forecast_validation.get("reason") or ""),
        "score_asof_ymd": str(forecast_validation.get("score_asof_ymd") or ""),
        "forward_asof_ymd": str(forecast_validation.get("forward_asof_ymd") or ""),
        "asof_alignment": str(forecast_validation.get("asof_alignment") or "UNKNOWN"),
        "forward_proxy_rate": float((forecast_validation.get("coverage") or {}).get("forward_proxy_rate") or 0.0),
    }

    st = LOGS / f"final_score_merge_status_{asof_ymd}.json"
    st_latest = LOGS / "final_score_merge_status_latest.json"
    fv = LOGS / f"forecast_score_validation_{asof_ymd}.json"
    st.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_latest_if_not_older(st_latest, status, asof_ymd, "asof_ymd")
    fv.write_text(json.dumps(forecast_validation, ensure_ascii=True, indent=2), encoding="utf-8")
    _write_latest_if_not_older(
        FORECAST_VALIDATION_LATEST,
        forecast_validation,
        asof_ymd,
        "score_asof_ymd",
        ensure_ascii=True,
    )

    _log_print(f"[FINAL_SCORE] input={in_path.name} rows={len(df)} final_nonzero={status['nonzero_rows']['final']}")
    _log_print(f"[FINAL_SCORE] wrote {OUT}")
    _log_print(f"[FINAL_SCORE] status={st}")
    _log_print(f"[FINAL_SCORE] forecast_validation={fv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
