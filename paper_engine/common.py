"""Common type-coercion and small helper utilities for paper_engine.

Split out from the legacy ``paper_engine.py`` to break circular dependencies
between the risk/orchestration modules and the main engine.
"""

from __future__ import annotations

__all__ = [
    "BASE_DIR",
    "HOLIDAYS_PATH",
    "LOG_DIR",
    "RISK_DIR",
    "PAPER_ENGINE_PHASE_TRACE_PATH",
    "DISCLOSURE_RISK_PATH",
    "FX_SCORE_CAUTION_THRESHOLD",
    "RUN_LABEL",
    "PAPER_SESSION_ID",
    "_path_from_env",
    "_to_float",
    "_to_int",
    "_truthy",
    "_entry_truthy",
    "_pct01_from_config",
    "_pct100_from_config",
    "_ask_book_from_row",
    "_get_dict",
    "_get_list",
    "_append_reduction_multiplier",
    "_norm_ymd_text",
    "_norm_ts14",
    "_clean_active_response_cell",
    "_clean_sector_label",
    "_has_source_entry_cell",
    "_source_entry_cell_text",
    "now_ts",
    "_extract_ymd_from_obj",
    "_extract_ymd_from_ts_text",
    "_load_json_with_date",
    "_load_holidays_ymd_cached",
    "_business_day_gap",
    "_next_krx_session_ymd",
    "_parse_policy_dt_text",
    "_position_cost_basis",
    "_clamp_sell_ratio_pct",
    "_pos_list",
    "_calc_sell_qty",
    "_get_asset_type_rule",
    "_calc_ma",
    "_calc_rsi",
    "_bollinger_upper_from_closes",
    "_extract_note_field",
    "_fund_float",
    "_load_fundamentals_db",
    "resolve_slip_pct",
    "_normal_realtime_gap_policy",
    "_severity_from_rank",
    "_estimate_atr14_pct_from_ohlc",
    "_resolve_dynamic_stop_loss_pct",
    "_sig_float",
    "get_ohlc",
    "_severity_rank",
    "_v411_trade_sig",
    "_classify_asset_type",
    "_shift_severity",
    "_parse_note_fields",
    "_adjust_take_profit_ratio",
    "calc_net_ret",
    "_concat_drop_all_na_columns",
    "_stable_digest",
    "next_trading_date",
    "prev_trading_date",
    "_ops_policy",
    "_clamp01",
    "compute_dynamic_probe_floor",
    "compute_participation_slo",
    "_event_doc_rows",
    "_cap_max_new",
    "_parse_hhmm",
    "_finite_float_or_none",
    "_paper_engine_phase_trace",
    "_is_hard_block_reason",
    "_safe_gate_float",
    "_safe_gate_int",
    "_load_sector_db",
    "_load_disclosure_negative_codes_for_entry",
    "_trade_row_for_intraday_residual_guard",
]

import hashlib
import json
import math
import os
import re
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, cast, overload

import pandas as pd

from pricing_engine import calc_net_return, resolve_slippage_pct_tiered
from utils.common import norm_code, is_risk_off_hard_block_reason

BASE_DIR = Path(__file__).resolve().parent.parent
HOLIDAYS_PATH = BASE_DIR / "holidays.json"
LOG_DIR = BASE_DIR / "2_Logs"
RISK_DIR = BASE_DIR / "12_Risk_Controlled"


def _path_from_env(var_name: str, default_path: Path) -> Path:
    raw = str(os.getenv(var_name, "") or "").strip()
    return Path(raw) if raw else default_path


PAPER_ENGINE_PHASE_TRACE_PATH = _path_from_env(
    "PAPER_ENGINE_PHASE_TRACE_PATH",
    LOG_DIR / "paper_engine_phase_trace_latest.jsonl",
)
DISCLOSURE_RISK_PATH = _path_from_env(
    "PAPER_DISCLOSURE_RISK_PATH",
    LOG_DIR / "disclosure_risk_latest.json",
)

FX_SCORE_CAUTION_THRESHOLD = -0.20


@overload
def _to_float(v: Any, d: None = None) -> Optional[float]: ...
@overload
def _to_float(v: Any, d: float) -> float: ...
def _to_float(v: Any, d: Optional[float] = None) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        if d is None:
            return None
        try:
            return float(d)
        except Exception:
            return None


def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return bool(v)
    if isinstance(v, (int, float)):
        try:
            fv = float(v)
            if math.isnan(fv):
                return False
            return fv != 0.0
        except Exception:
            pass
    s = str(v or "").strip().lower()
    try:
        fs = float(s)
        if not math.isnan(fs):
            return fs != 0.0
    except Exception:
        pass
    return s in {"1", "true", "t", "y", "yes", "on"}


def _entry_truthy(v: Any) -> bool:
    return str(v or "").strip().upper() in {"TRUE", "1", "Y", "YES", "T"}


def _pct01_from_config(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
    except Exception:
        x = float(default)
    if not math.isfinite(x):
        x = float(default)
    if abs(x) > 1.0:
        x = x / 100.0
    return float(x)


def _ask_book_from_row(row: Any, levels: int = 10) -> List[Dict[str, float]]:
    out: List[Dict[str, float]] = []
    for i in range(1, int(levels or 0) + 1):
        price = _to_float(row.get(f"ask{i}") if hasattr(row, "get") else None, 0.0) or 0.0
        vol = _to_float(row.get(f"askq{i}") if hasattr(row, "get") else None, 0.0) or 0.0
        if price > 0 and vol > 0:
            out.append({"price": float(price), "vol": float(vol)})
    return out


@overload
def _to_int(v: Any, d: None = None) -> Optional[int]: ...
@overload
def _to_int(v: Any, d: int) -> int: ...
def _to_int(v: Any, d: Optional[int] = None) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        if d is None:
            return None
        try:
            return int(d)
        except Exception:
            return None


def _get_dict(d: Any, key: str, default: Optional[Dict[str, Any]] = None) -> Any:
    """Safely extract a nested dict from a mapping."""
    out: Dict[str, Any] = default if default is not None else {}
    if isinstance(d, dict):
        v = d.get(key)
        if isinstance(v, dict):
            out = cast(Dict[str, Any], v)
    return out


def _get_list(d: Any, key: str, default: Optional[List[Any]] = None) -> List[Any]:
    """Safely extract a nested list from a mapping."""
    out: List[Any] = default if default is not None else []
    if isinstance(d, dict):
        v = d.get(key)
        if isinstance(v, list):
            out = cast(List[Any], v)
    return out


def _append_reduction_multiplier(
    reasons: List[str],
    multipliers: List[float],
    categories: List[str],
    reason: str,
    mult: Any,
    category: str,
) -> None:
    """Append a qty-reduction multiplier if it is in (0, 1)."""
    m = float(_to_float(mult, 1.0))
    m = max(0.0, min(1.0, m))
    if 0.0 < m < 1.0:
        reasons.append(reason)
        multipliers.append(m)
        categories.append(category)


def _norm_ymd_text(value: Any) -> str:
    return re.sub(r"[^0-9]", "", str(value or "")).strip()[:8]


def _clean_active_response_cell(value: Any) -> str:
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value or "").strip()
    return "" if text.lower() in {"nan", "none", "null", "<na>"} else text


def _has_source_entry_cell(value: Any) -> bool:
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass
    text = str(value).strip()
    return bool(text) and text.lower() not in {"nan", "none", "null", "<na>"}


def _source_entry_cell_text(value: Any) -> str:
    if not _has_source_entry_cell(value):
        return ""
    return str(value).strip()


def now_ts() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _extract_ymd_from_obj(obj: Dict[str, Any], keys: List[str]) -> Optional[str]:
    if not isinstance(obj, dict):
        return None
    for key in keys:
        raw = obj.get(key)
        ymd = _norm_ymd_text(raw)
        if len(ymd) == 8:
            return ymd
        if isinstance(raw, str):
            m = re.search(r"(20\d{2})[-/]?(\d{2})[-/]?(\d{2})", raw)
            if m:
                return f"{m.group(1)}{m.group(2)}{m.group(3)}"
    return None


def _load_json_with_date(path: Path, keys: List[str], *, allow_replay_consistency_fallback: bool = True) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "path": str(path),
        "exists": path.exists(),
        "as_of_ymd": None,
        "error": "",
    }
    if not path.exists():
        out["error"] = "missing"
        return out
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        ymd = _extract_ymd_from_obj(obj, keys)
        if allow_replay_consistency_fallback and ymd is None and isinstance(obj, dict):
            _rc = obj.get("replay_consistency")
            rc = cast(Dict[str, Any], _rc) if isinstance(_rc, dict) else {}
            ymd = _extract_ymd_from_obj(rc, ["as_of", "as_of_ymd"])
        out["as_of_ymd"] = ymd
        out["ok"] = bool(ymd)
        return out
    except Exception as exc:
        out["error"] = f"read_fail:{type(exc).__name__}"
        return out


@lru_cache(maxsize=1)
def _load_holidays_ymd_cached() -> set[str]:
    if not HOLIDAYS_PATH.exists():
        return set()
    try:
        obj = json.loads(HOLIDAYS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return set()

    vals: List[str] = []
    if isinstance(obj, dict):
        for k in ["holidays", "krx_holidays", "dates", "holiday_dates"]:
            v = obj.get(k)
            if isinstance(v, list):
                vals.extend(str(x) for x in v)
    elif isinstance(obj, list):
        vals.extend(str(x) for x in obj)

    out: set[str] = set()
    for item in vals:
        ymd = _norm_ymd_text(item)
        if len(ymd) == 8:
            out.add(ymd)
    return out


def _business_day_gap(ymd_a: str, ymd_b: str) -> Optional[int]:
    a = _norm_ymd_text(ymd_a)
    b = _norm_ymd_text(ymd_b)
    if len(a) != 8 or len(b) != 8:
        return None
    try:
        da = datetime.strptime(a, "%Y%m%d")
        db = datetime.strptime(b, "%Y%m%d")
    except Exception:
        return None
    if da == db:
        return 0
    step = 1 if da < db else -1
    cur = da
    gap = 0
    holidays = _load_holidays_ymd_cached()
    while cur != db:
        cur = cur + timedelta(days=step)
        cur_ymd = cur.strftime("%Y%m%d")
        if cur.weekday() >= 5:
            continue
        if cur_ymd in holidays:
            continue
        gap += 1
    return gap


def _next_krx_session_ymd(after_ymd: str, max_lookahead_days: int = 14) -> Optional[str]:
    base_ymd = _norm_ymd_text(after_ymd)
    if len(base_ymd) != 8:
        return None
    try:
        current = datetime.strptime(base_ymd, "%Y%m%d")
    except Exception:
        return None

    holidays = _load_holidays_ymd_cached()
    for offset in range(1, max_lookahead_days + 1):
        cand = current + timedelta(days=offset)
        cand_ymd = cand.strftime("%Y%m%d")
        if cand.weekday() >= 5:
            continue
        if cand_ymd in holidays:
            continue
        return cand_ymd
    return None


def _parse_policy_dt_text(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    text = text.replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y%m%d%H%M%S", "%Y%m%d %H:%M:%S"):
        try:
            return datetime.strptime(text[: len(datetime.now().strftime(fmt))], fmt)
        except Exception:
            continue
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone().replace(tzinfo=None)
        return parsed
    except Exception:
        return None


def _position_cost_basis(pos: Dict[str, Any]) -> float:
    qty = float(_to_float(pos.get("qty"), 0.0) or 0.0)
    price = float(
        _to_float(
            pos.get("entry_price", pos.get("avg_entry_price", pos.get("price"))),
            0.0,
        )
        or 0.0
    )
    return max(0.0, qty * price)



def _pct100_from_config(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
    except Exception:
        x = float(default)
    if abs(x) <= 1.0:
        x = x * 100.0
    return float(x)


def _clamp_sell_ratio_pct(v: Any) -> float:
    try:
        x = float(v)
    except Exception:
        x = 0.0
    return max(0.0, min(100.0, x))


def _pos_list(pos: Dict[str, Any], key: str) -> List[str]:
    raw = pos.get(key, [])
    if isinstance(raw, list):
        return [str(x) for x in raw]
    if raw in (None, "", "None"):
        return []
    return [str(raw)]


def _calc_sell_qty(total_qty: int, sell_ratio_pct: float) -> int:
    qty = max(0, int(total_qty))
    if qty <= 0:
        return 0
    ratio = _clamp_sell_ratio_pct(sell_ratio_pct)
    if ratio >= 100.0:
        return qty
    sell_qty = int(math.floor(qty * (ratio / 100.0)))
    if sell_qty <= 0 and ratio > 0:
        sell_qty = 1
    return min(qty, sell_qty)


def _get_asset_type_rule(asset_type: str, sell_rules: Dict[str, Any]) -> Dict[str, Any]:
    rules = sell_rules.get("asset_type_rules", {}) if isinstance(sell_rules.get("asset_type_rules"), dict) else {}
    out = rules.get(str(asset_type or "").strip(), {})
    return out if isinstance(out, dict) else {}


def _calc_ma(closes: List[float], period: int) -> Optional[float]:
    if period <= 0 or len(closes) < period:
        return None
    window = closes[-period:]
    return float(sum(window) / float(period))


def _calc_rsi(closes: List[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, len(closes)):
        diff = float(closes[i]) - float(closes[i - 1])
        if diff > 0:
            gains.append(diff)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(diff))
    avg_gain = sum(gains[:period]) / float(period)
    avg_loss = sum(losses[:period]) / float(period)
    for gain, loss in zip(gains[period:], losses[period:]):
        avg_gain = ((avg_gain * float(period - 1)) + float(gain)) / float(period)
        avg_loss = ((avg_loss * float(period - 1)) + float(loss)) / float(period)
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100.0 - (100.0 / (1.0 + rs)))


def _bollinger_upper_from_closes(closes: List[float], window: int = 20, num_std: float = 2.0) -> Optional[float]:
    if len(closes) < window:
        return None
    s = pd.Series(closes[-window:], dtype="float64")
    ma = float(s.mean())
    std = float(s.std(ddof=1))
    return ma + num_std * std


def _extract_note_field(note: Any, key: str) -> str:
    if note is None:
        return ""
    try:
        m = re.search(rf"{re.escape(str(key))}=([^;]*)", str(note))
        return str(m.group(1)).strip() if m else ""
    except Exception:
        return ""


def _fund_float(v: Any, default: float = 0.0) -> float:
    try:
        if v in ("", None, "None"):
            return float(default)
        return float(v)
    except Exception:
        return float(default)


def _load_fundamentals_db(sell_rules: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    csv_path = str(sell_rules.get("fundamentals_csv_path", "") or "").strip()
    path = Path(csv_path) if csv_path else (BASE_DIR / "_cache" / "dart_fundamental_latest.csv")
    if not path.exists():
        print(f"[FUND] fundamentals csv missing: {path}")
        return {}
    try:
        df = pd.read_csv(path, dtype={"code": str})
    except Exception as e:
        print(f"[FUND] fundamentals load failed: {type(e).__name__}: {e}")
        return {}
    db: Dict[str, Dict[str, Any]] = {}
    for _, row in df.iterrows():
        code = norm_code(row.get("code", ""))
        if not code:
            continue
        db[code] = {
            "per": _fund_float(row.get("PER"), 0.0),
            "pbr": _fund_float(row.get("PBR"), 0.0),
            "roe": _fund_float(row.get("ROE"), 0.0),
            "debt_ratio": _fund_float(row.get("debt_ratio"), 0.0),
            "operating_margin": _fund_float(row.get("OPM"), 0.0),
            "revenue_growth_yoy": _fund_float(row.get("revenue_growth_yoy") or row.get("revenue_growth"), 0.0),
            "dividend_yield": _fund_float(row.get("dividend_yield"), 0.0),
            "last_updated": str(row.get("dart_updated_at", "") or row.get("as_of_ymd", "") or ""),
        }
    print(f"[FUND] loaded fundamentals rows={len(db)} source={path}")
    return db


def resolve_slip_pct(market_cap: float, cfg: dict, default_slip: float) -> float:
    """Return slippage rate tiered by market cap if tiered_slippage.enabled=True."""
    return resolve_slippage_pct_tiered(
        market_cap=market_cap,
        cfg=cfg if isinstance(cfg, dict) else {},
        default_slippage_pct=default_slip,
    )


def _normal_realtime_gap_policy(cfg: Dict[str, Any]) -> Dict[str, Any]:
    policy = cfg.get("normal_realtime_gap_policy", {}) if isinstance(cfg, dict) else {}
    return policy if isinstance(policy, dict) and bool(policy.get("enabled", True)) else {}


def _severity_from_rank(rank: Any) -> str:
    try:
        idx = int(rank)
    except Exception:
        idx = 1
    idx = max(0, min(3, idx))
    return ["LIGHT", "NORMAL", "HARD", "FORCE"][idx]


def _estimate_atr14_pct_from_ohlc(px: pd.DataFrame, code: str, upto_ymd: str, window: int = 14) -> float:
    if not isinstance(px, pd.DataFrame) or px.empty:
        return 0.0
    need = {"code", "date", "high", "low", "close"}
    if not need.issubset(set(px.columns)):
        return 0.0
    work = px[
        (px["code"].astype(str).str.zfill(6) == str(code).zfill(6))
        & (px["date"].astype(str) <= str(upto_ymd))
    ].copy()
    if len(work) < 2:
        return 0.0
    work = work.sort_values("date")
    for col in ["high", "low", "close"]:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work = work.dropna(subset=["high", "low", "close"])
    work = work[(work["high"] > 0) & (work["low"] > 0) & (work["close"] > 0)]
    if len(work) < 2:
        return 0.0
    prev_close = work["close"].shift(1)
    tr = pd.concat(
        [
            (work["high"] - work["low"]).abs(),
            (work["high"] - prev_close).abs(),
            (work["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1).dropna()
    if tr.empty:
        return 0.0
    atr = float(tr.tail(max(1, int(window))).mean())
    last_close = float(work["close"].iloc[-1])
    if atr <= 0 or last_close <= 0:
        return 0.0
    return float(atr / last_close)


def _resolve_dynamic_stop_loss_pct(
    *,
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
    # Break-even tightening once unrealized profit exceeds threshold.
    be_trigger_pct = float(_to_float(dcfg.get("break_even_trigger_profit_pct"), 4.0))
    be_stop_pct = _pct01_from_config(dcfg.get("break_even_stop_pct"), -0.002)
    if float(current_profit_pct) >= be_trigger_pct:
        eff = max(eff, be_stop_pct)

    # Profit lock tiers: each tier can tighten stop toward zero.
    tiers = dcfg.get("profit_lock_tiers", [])
    if isinstance(tiers, list):
        for t in tiers:
            if not isinstance(t, dict):
                continue
            trig = float(_to_float(t.get("trigger_profit_pct"), 0.0) or 0.0)
            stop_t = _pct01_from_config(t.get("stop_pct"), eff)
            if float(current_profit_pct) >= trig:
                eff = max(eff, stop_t)

    # Time-decay tightening: longer holding period progressively tightens stop.
    decay_start = int(_to_int(dcfg.get("time_decay_start_days"), 3))
    decay_step = _pct01_from_config(dcfg.get("time_decay_tighten_step_pct"), 0.003)
    decay_cap = _pct01_from_config(dcfg.get("time_decay_tighten_cap_pct"), 0.03)
    if hold_days_trading >= decay_start and decay_step > 0:
        tighten = min(decay_cap, (hold_days_trading - decay_start + 1) * decay_step)
        eff = max(eff, float(base_stop_pct) + float(tighten))

    # High ATR regime tightening.
    atr_ref = _pct01_from_config(dcfg.get("atr_ref_pct"), 0.04)
    atr_scale = float(_to_float(dcfg.get("atr_tighten_scale"), 0.50))
    atr_cap = _pct01_from_config(dcfg.get("atr_tighten_cap_pct"), 0.03)
    atr_val = float(atr14_pct or 0.0)
    if atr_val > atr_ref and atr_scale > 0:
        atr_tighten = min(atr_cap, (atr_val - atr_ref) * atr_scale)
        eff = max(eff, float(base_stop_pct) + float(atr_tighten))

    min_stop = _pct01_from_config(dcfg.get("min_stop_pct"), -0.30)
    max_stop = _pct01_from_config(dcfg.get("max_stop_pct"), -0.001)
    eff = min(max(eff, min_stop), max_stop)
    return float(eff)


def _sig_float(x: Any) -> str:
    try:
        v = float(x)
        return f"{v:.8f}"
    except Exception:
        return str(x)


def get_ohlc(px: pd.DataFrame, code: str, day: str) -> Optional[Dict[str, float]]:
    r = px[(px["code"] == code) & (px["date"] == day)]
    if r.empty:
        return None
    x = r.iloc[0]
    return {"open": float(x["open"]), "high": float(x["high"]), "low": float(x["low"]), "close": float(x["close"])}


def _severity_rank(severity: Any) -> int:
    return {
        "LIGHT": 0,
        "NORMAL": 1,
        "HARD": 2,
        "FORCE": 3,
    }.get(str(severity or "").strip().upper(), 1)


def _v411_trade_sig(row: pd.Series) -> str:
    return "|".join([
        str(row.get("code", "")),
        str(row.get("entry_ts", "")),
        str(row.get("exit_ts", "")),
        _sig_float(row.get("entry_price", "")),
        _sig_float(row.get("exit_price", "")),
        _sig_float(row.get("qty", "")),
        _sig_float(row.get("net_ret", "")),
        str(row.get("note", "")),
    ])


def _classify_asset_type(position: Dict[str, Any]) -> str:
    fund = position.get("fundamentals", {}) if isinstance(position.get("fundamentals"), dict) else {}
    sector = str(
        position.get("sector")
        or (fund.get("sector") if isinstance(fund, dict) else "")
        or ""
    ).strip()

    if fund:
        if _fund_float(fund.get("dividend_yield"), 0.0) >= 4.0:
            return "배당주"
        if _fund_float(fund.get("revenue_growth_yoy"), 0.0) >= 20.0 and _fund_float(fund.get("per"), 0.0) >= 25.0:
            return "성장주"
        if 0.0 < _fund_float(fund.get("per"), 0.0) < 12.0 and 0.0 < _fund_float(fund.get("pbr"), 0.0) < 1.0:
            return "가치주"

    if any(k in sector for k in ["금융", "보험", "증권", "은행"]):
        return "가치주"
    if any(k in sector for k in ["반도체", "자동차", "화학", "건설", "기계", "전동기", "발전기", "전기 변환", "제어 장치", "전자부품"]):
        return "경기민감주"
    if any(k in sector for k in ["식품", "바이오", "의약", "통신", "에너지", "전기 통신", "유틸리티", "정밀기기"]):
        return "경기방어주"
    # 섹터 미분류 + 소형주(시가총액 3000억 미만) → 테마주 (단기 모멘텀 전략)
    try:
        market_cap = float(position.get("market_cap") or 0)
    except (TypeError, ValueError):
        market_cap = 0.0
    if 0 < market_cap < 300_000_000_000:
        return "테마주"
    return ""


def _norm_ts14(value: Any) -> str:
    text = re.sub(r"[^0-9]", "", str(value or ""))
    return text[:14]


def _shift_severity(severity: Any, delta: int) -> str:
    return _severity_from_rank(_severity_rank(severity) + int(delta))


def _parse_note_fields(note: Any) -> Dict[str, str]:
    text = str(note or "").strip()
    if not text:
        return {}
    out: Dict[str, str] = {}
    for part in text.split(";"):
        chunk = str(part).strip()
        if not chunk or "=" not in chunk:
            continue
        key, value = chunk.split("=", 1)
        key = str(key).strip()
        if not key:
            continue
        out[key] = str(value).strip()
    return out


def _extract_ymd_from_ts_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    digits = re.sub(r"[^0-9]", "", text)
    return digits[:8] if len(digits) >= 8 else ""


def _adjust_take_profit_ratio(base_ratio_pct: float, asset_type: str, profit_level_pct: float, tp_cfg: Dict[str, Any]) -> float:
    ratio = float(base_ratio_pct)
    adjustments = tp_cfg.get("asset_type_adjustments", {}) if isinstance(tp_cfg.get("asset_type_adjustments"), dict) else {}
    if asset_type == "배당주" and profit_level_pct < 30.0:
        return 0.0
    if asset_type and asset_type in adjustments:
        try:
            ratio *= float(adjustments.get(asset_type) or 0)
        except Exception:
            pass
    return _clamp_sell_ratio_pct(ratio)




def calc_net_ret(entry: float, exit: float, fee_pct: float, slip_pct: float, sell_tax_pct: float = 0.0) -> float:
    # legacy SSOT (matches paper/trades.csv pnl_pct)
    # gross - ((entry+exit)/entry) * (fee+slip)
    return calc_net_return(
        entry_price=entry,
        exit_price=exit,
        fee_pct=fee_pct,
        slippage_pct=slip_pct,
        sell_tax_pct=sell_tax_pct,
    )


RUN_LABEL = str(os.getenv("PAPER_RUN_LABEL", "main") or "main").strip() or "main"
PAPER_SESSION_ID = str(os.getenv("PAPER_SESSION_ID", "") or "").strip()


def _concat_drop_all_na_columns(frames: List[pd.DataFrame], ignore_index: bool = True) -> pd.DataFrame:
    valid_frames = [f for f in frames if isinstance(f, pd.DataFrame)]
    if not valid_frames:
        return pd.DataFrame()

    ordered_cols: List[str] = []
    for frame in valid_frames:
        for col in frame.columns:
            if col not in ordered_cols:
                ordered_cols.append(col)

    trimmed_frames: List[pd.DataFrame] = []
    for frame in valid_frames:
        keep_cols = [c for c in frame.columns if not frame[c].isna().all()]
        if keep_cols:
            trimmed_frames.append(frame[keep_cols].copy())
        else:
            trimmed_frames.append(frame.iloc[:, 0:0].copy())

    out = pd.concat(trimmed_frames, ignore_index=ignore_index, sort=False)
    if ordered_cols:
        out = out.reindex(columns=ordered_cols)
    return out


def _stable_digest(*parts: Any) -> str:
    text = "|".join(str(p or "").strip() for p in parts)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]


def next_trading_date(px: pd.DataFrame, code: str, after_ymd: str) -> Optional[str]:
    # Find next available close row after the given date.
    after_ymd = _norm_ymd_text(after_ymd)
    d = px.loc[(px["code"] == code) & (px["date"] > after_ymd) & (px["close"] > 0), "date"]
    if d.empty:
        return _next_krx_session_ymd(after_ymd)
    return str(d.iloc[0])


def prev_trading_date(px: pd.DataFrame, code: str, before_ymd: str) -> Optional[str]:
    d = px.loc[(px["code"] == code) & (px["date"] < before_ymd) & (px["close"] > 0), "date"]
    if d.empty:
        return None
    return str(d.max())



def _ops_policy(cfg: Dict[str, Any]) -> Dict[str, Any]:
    op = cfg.get("market_ops_policy", {}) if isinstance(cfg, dict) else {}
    return op if isinstance(op, dict) else {}


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def compute_dynamic_probe_floor(base_max_new: int, market_regime: str, regime_info: Dict[str, Any], cfg: Dict[str, Any], risk_off_hard: bool, universe_shrink: bool) -> int:
    op = _ops_policy(cfg)
    if not bool(op.get("enabled", False)):
        return 0
    if risk_off_hard:
        return 0

    day_ret_abs = abs(_to_float((regime_info or {}).get("day_ret"), 0.0))
    base = max(0, int(base_max_new))
    if base <= 0:
        return 0

    reg = str(market_regime or "NORMAL").upper()
    if reg == "RALLY":
        min_n = max(1, _to_int(op.get("probe_rally_min", 1), 1))
        ratio = _to_float(op.get("probe_rally_ratio_base", 0.10), 0.10) + day_ret_abs * _to_float(op.get("probe_rally_ratio_per_ret", 2.0), 2.0)
        ratio = min(_to_float(op.get("probe_rally_ratio_cap", 0.35), 0.35), ratio)
        if universe_shrink:
            ratio += _to_float(op.get("universe_shrink_probe_uplift", 0.05), 0.05)
        ratio = _clamp01(ratio)
        return min(base, max(min_n, int(math.ceil(base * ratio))))

    if reg == "CRASH":
        return 0

    return 0


def compute_participation_slo(market_regime: str, regime_info: Dict[str, Any], cfg: Dict[str, Any]) -> float:
    op = _ops_policy(cfg)
    if not bool(op.get("enabled", False)):
        return 0.0
    reg = str(market_regime or "NORMAL").upper()
    day_ret_abs = abs(_to_float((regime_info or {}).get("day_ret"), 0.0))

    if reg == "RALLY":
        x = _to_float(op.get("slo_rally_base", 0.20), 0.20) + day_ret_abs * _to_float(op.get("slo_rally_per_ret", 2.0), 2.0)
        return _clamp01(min(_to_float(op.get("slo_rally_cap", 0.50), 0.50), x))
    if reg == "CRASH":
        x = _to_float(op.get("slo_crash_base", 0.05), 0.05) + day_ret_abs * _to_float(op.get("slo_crash_per_ret", 0.8), 0.8)
        return _clamp01(min(_to_float(op.get("slo_crash_cap", 0.20), 0.20), x))
    return _clamp01(_to_float(op.get("slo_normal", 0.10), 0.10))

def _event_doc_rows(event_doc: Any) -> List[Dict[str, Any]]:
    if isinstance(event_doc, list):
        return [row for row in event_doc if isinstance(row, dict)]
    if not isinstance(event_doc, dict):
        return []
    rows: List[Dict[str, Any]] = []
    for key in ("events", "items", "rows", "market_events", "event_rows"):
        val = event_doc.get(key)
        if isinstance(val, list):
            rows.extend([row for row in val if isinstance(row, dict)])
    if not rows:
        rows.append(event_doc)
    return rows

def _cap_max_new(current_max_new: int, cap_value: Any) -> int:
    try:
        parsed_cap = int(cap_value)
    except Exception:
        return int(current_max_new)
    parsed_cap = max(0, parsed_cap)
    return min(int(current_max_new), parsed_cap)

def _parse_hhmm(value: Any, default_hhmm: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default_hhmm)

def _finite_float_or_none(v: Any) -> Optional[float]:
    val = _to_float(v, None)
    if val is None:
        return None
    try:
        out = float(val)
        if not math.isfinite(out):
            return None
        return out
    except Exception:
        return None


def _paper_engine_phase_trace(phase: str, **fields: Any) -> None:
    payload: Dict[str, Any] = {"ts": now_ts(), "phase": str(phase)}
    for key, value in fields.items():
        try:
            if isinstance(value, (str, int, float, bool)) or value is None:
                payload[str(key)] = value
            else:
                payload[str(key)] = str(value)[:500]
        except Exception:
            payload[str(key)] = "<unserializable>"
    try:
        PAPER_ENGINE_PHASE_TRACE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with PAPER_ENGINE_PHASE_TRACE_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass
    try:
        print(f"[PE_PHASE] {phase}", flush=True)
    except Exception:
        pass

def _is_hard_block_reason(reason: str) -> bool:
    return is_risk_off_hard_block_reason(reason)

def _safe_gate_float(raw: Any, default: float, *, min_v: Optional[float] = None, max_v: Optional[float] = None) -> tuple[float, bool]:
    fallback = False
    try:
        x = float(raw)
    except Exception:
        x = float(default)
        fallback = True
    if min_v is not None and x < float(min_v):
        x = float(default)
        fallback = True
    if max_v is not None and x > float(max_v):
        x = float(default)
        fallback = True
    return x, fallback

def _safe_gate_int(raw: Any, default: int, *, min_v: Optional[int] = None, max_v: Optional[int] = None) -> tuple[int, bool]:
    fallback = False
    try:
        x = int(float(raw))
    except Exception:
        x = int(default)
        fallback = True
    if min_v is not None and x < int(min_v):
        x = int(default)
        fallback = True
    if max_v is not None and x > int(max_v):
        x = int(default)
        fallback = True
    return x, fallback

def _clean_sector_label(value: Any) -> str:
    sector = str(value if value is not None else "").strip()
    if sector.lower() in {"", "nan", "none", "null", "<na>", "unknown"}:
        return ""
    if "?" in sector or "\ufffd" in sector:
        return ""
    return sector

def _load_sector_db() -> Dict[str, str]:
    path = BASE_DIR / "_cache" / "sector_ssot.csv"
    if not path.exists():
        print(f"[FUND] sector ssot missing: {path}")
        return {}
    try:
        df = pd.read_csv(path, dtype={"code": str})
    except Exception as e:
        print(f"[FUND] sector ssot load failed: {type(e).__name__}: {e}")
        return {}
    db: Dict[str, str] = {}
    for _, row in df.iterrows():
        code = norm_code(row.get("code", ""))
        sector = _clean_sector_label(row.get("krx_sector", ""))
        if code and sector:
            db[code] = sector
    override_path = BASE_DIR / "_cache" / "sector_preferred_share_override.csv"
    if override_path.exists():
        try:
            ov = pd.read_csv(override_path, dtype={"code": str, "krx_sector": str})
            for _, row in ov.iterrows():
                code = norm_code(row.get("code", ""))
                sector = _clean_sector_label(row.get("krx_sector", ""))
                if code and sector:
                    db[code] = sector
        except Exception as e:
            print(f"[FUND] sector preferred override load failed: {type(e).__name__}: {e}")
    print(f"[FUND] loaded sector rows={len(db)} source={path}")
    return db


def _load_disclosure_negative_codes_for_entry() -> Tuple[set[str], str, Optional[datetime], set[str]]:
    """Return (blocked_codes, status_note, generated_at, fundamental_codes).

    fundamental_codes: subset of blocked_codes whose as_of_ymd is >7 days before today.
    These represent long-standing financial disclosures and must always block entry,
    regardless of whether the disclosure file was generated after the surge signal.
    """
    if not DISCLOSURE_RISK_PATH.exists():
        return set(), "disclosure_missing", None, set()
    try:
        obj = json.loads(DISCLOSURE_RISK_PATH.read_text(encoding="utf-8"))
    except Exception:
        try:
            obj = json.loads(DISCLOSURE_RISK_PATH.read_text(encoding="utf-8-sig"))
        except Exception as exc:
            return set(), f"disclosure_read_error:{type(exc).__name__}", None, set()
    generated_at = _parse_policy_dt_text(obj.get("generated_at"))
    items = obj.get("items")
    if not isinstance(items, list):
        return set(), "disclosure_items_missing", generated_at, set()
    today = datetime.now().date()
    blocked: set[str] = set()
    fundamental: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        signal = str(item.get("disclosure_signal", "") or "").strip().upper()
        is_blocked = _truthy(item.get("is_blocked"))
        if signal != "NEGATIVE" and not is_blocked:
            continue
        code = norm_code(item.get("code", ""))
        if not re.fullmatch(r"\d{6}", code):
            continue
        blocked.add(code)
        as_of_raw = str(item.get("as_of_ymd", "") or "").strip()
        if len(as_of_raw) == 8 and as_of_raw.isdigit():
            try:
                as_of_date = datetime.strptime(as_of_raw, "%Y%m%d").date()
                if (today - as_of_date).days >= 7:
                    fundamental.add(code)
            except Exception:
                pass
    gen_note = generated_at.strftime("%Y-%m-%d %H:%M:%S") if generated_at else "unknown"
    return blocked, f"disclosure_negative_codes={len(blocked)} fundamental={len(fundamental)} generated_at={gen_note}", generated_at, fundamental


def _trade_row_for_intraday_residual_guard(row: Any, schema: str) -> Dict[str, Any]:
    values = list(row) if isinstance(row, (list, tuple)) else []
    if str(schema or "").strip().lower() == "legacy":
        note = str(values[9] if len(values) > 9 else "")
        entry_date = _norm_ymd_text(values[2] if len(values) > 2 else "")
        exit_date = _norm_ymd_text(values[4] if len(values) > 4 else "")
        return {
            "trade_id": str(values[0] if len(values) > 0 else ""),
            "code": norm_code(values[1] if len(values) > 1 else ""),
            "entry_date": entry_date,
            "exit_date": exit_date,
            "entry_ts": entry_date,
            "exit_ts": exit_date,
            "sell_qty": _to_int(_extract_note_field(note, "sell_qty"), 0),
            "net_ret": _to_float(values[6] if len(values) > 6 else None, 0.0),
            "exit_reason": str(values[8] if len(values) > 8 else _extract_note_field(note, "exit_reason")).strip().upper(),
            "partial_exit": bool(_truthy(_extract_note_field(note, "partial_exit"))),
            "note": note,
        }

    note = str(values[15] if len(values) > 15 else "")
    entry_ts = str(values[1] if len(values) > 1 else "")
    exit_ts = str(values[2] if len(values) > 2 else "")
    return {
        "trade_id": str(values[0] if len(values) > 0 else ""),
        "code": norm_code(values[3] if len(values) > 3 else ""),
        "entry_date": _extract_ymd_from_ts_text(entry_ts),
        "exit_date": _extract_ymd_from_ts_text(exit_ts),
        "entry_ts": entry_ts,
        "exit_ts": exit_ts,
        "sell_qty": _to_int(values[6] if len(values) > 6 else _extract_note_field(note, "sell_qty"), 0),
        "net_ret": _to_float(values[10] if len(values) > 10 else None, 0.0),
        "exit_reason": str(_extract_note_field(note, "exit_reason")).strip().upper(),
        "partial_exit": bool(_truthy(_extract_note_field(note, "partial_exit"))),
        "note": note,
    }
