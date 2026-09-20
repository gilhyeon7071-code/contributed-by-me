from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

MARKET_RISING_CSV = LOG_DIR / "market_rising_latest.csv"
SURGE_SANITY_CSV = LOG_DIR / "surge_sanity_labeled_latest.csv"
CANDIDATE_DETAIL_CSV = LOG_DIR / "candidates_latest_data.with_final_score.csv"
NEWS_CANDIDATES_CSV = LOG_DIR / "news_candidates_latest.csv"
INTRADAY_HISTORY_CSV = LOG_DIR / "arl_intraday_history_returns_latest.csv"

OUT_JSON = LOG_DIR / "surge_event_money_pullback_screener_latest.json"
OUT_CSV = LOG_DIR / "surge_event_money_pullback_screener_latest.csv"


HARD_EXCLUDE_TOKENS = {
    "KRX_ADMIN",
    "KRX_WARNING",
    "KRX_RISK",
    "NEWS_NEGATIVE",
    "NEWS_IMPLICATION_BLOCK",
    "ORDERFLOW_PAUSE",
    "ORDERFLOW_RISK_BLOCK",
}


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    return []


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        if value in ("", None):
            return default
        return float(value)
    except Exception:
        return default


def _int(value: Any, default: int = 0) -> int:
    v = _float(value, None)
    return default if v is None else int(v)


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def _code(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text else ""


def _tokens(*values: Any) -> set[str]:
    out: set[str] = set()
    for value in values:
        text = str(value or "")
        for token in text.replace("|", ";").replace(",", ";").split(";"):
            token = token.strip().upper()
            if token:
                out.add(token)
    return out


def _by_code(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code and code not in out:
            out[code] = row
    return out


def _history_by_code(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    out: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code:
            out.setdefault(code, []).append(row)
    return out


def _add(reason: list[str], key: str, value: Any = None) -> None:
    if value in ("", None):
        reason.append(key)
    else:
        reason.append(f"{key}={value}")


def _first(*values: Any) -> Any:
    for value in values:
        if value not in ("", None):
            return value
    return ""


def _calc_pullback_fields(row: dict[str, Any]) -> dict[str, float | str]:
    current = _float(_first(row.get("current_price"), row.get("close")), None)
    high = _float(row.get("high_price"), None)
    low = _float(row.get("low_price"), None)
    out: dict[str, float | str] = {
        "calc_high_drawdown_pct": "",
        "calc_low_rebound_pct": "",
        "calc_range_position_pct": "",
    }
    if current is not None and high is not None and high > 0:
        out["calc_high_drawdown_pct"] = round((current - high) / high, 6)
    if current is not None and low is not None and low > 0:
        out["calc_low_rebound_pct"] = round((current - low) / low, 6)
    if current is not None and high is not None and low is not None and high > low:
        out["calc_range_position_pct"] = round((current - low) / (high - low), 6)
    return out


def _intraday_points(history_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    prev_volume: float | None = None
    prev_value: float | None = None
    prev_price: float | None = None
    for row in history_rows:
        price = _float(row.get("current_price"), None)
        volume = _float(row.get("volume"), None)
        value = _float(row.get("trading_value"), None)
        if price is None or price <= 0:
            continue
        delta_volume = 0.0
        delta_value = 0.0
        if volume is not None and prev_volume is not None:
            delta_volume = max(0.0, volume - prev_volume)
        if value is not None and prev_value is not None:
            delta_value = max(0.0, value - prev_value)
        ret_pct = 0.0
        if prev_price is not None and prev_price > 0:
            ret_pct = (price - prev_price) / prev_price
        points.append({
            "ts": row.get("ts", ""),
            "price": price,
            "volume": volume or 0.0,
            "value": value or 0.0,
            "delta_volume": delta_volume,
            "delta_value": delta_value,
            "ret_pct": ret_pct,
        })
        prev_price = price
        if volume is not None:
            prev_volume = volume
        if value is not None:
            prev_value = value
    return points


def _avg(values: list[float]) -> float | None:
    clean = [v for v in values if v is not None and v >= 0]
    if not clean:
        return None
    return sum(clean) / len(clean)


def _ratio(num: float | None, den: float | None) -> float | None:
    if num is None or den is None or den <= 0:
        return None
    return num / den


def _event_axis(row: dict[str, Any]) -> tuple[float, str]:
    score = 0.0
    evidence: list[str] = []

    news_score = _float(row.get("news_score"), None)
    if news_score is not None and news_score >= 0.55:
        score += 1.0
        _add(evidence, "news_score", round(news_score, 4))

    if _int(row.get("news_implication_positive_rows")) > 0 or _int(row.get("news_implication_boost_rows")) > 0:
        score += 1.0
        _add(evidence, "positive_news_implication")

    if _int(row.get("news_implication_sector_rows")) > 0 or _int(row.get("news_implication_macro_rows")) > 0:
        score += 0.75
        _add(evidence, "sector_or_macro_news_implication")

    if _truthy(row.get("sector_entry_allowed")):
        score += 0.75
        _add(evidence, "sector_entry_allowed")

    sector_score = _float(row.get("sector_score"), None)
    if sector_score is not None and sector_score >= 0.55:
        score += 0.5
        _add(evidence, "sector_score", round(sector_score, 4))

    policy_score = _float(row.get("policy_score"), None)
    if policy_score is not None and policy_score >= 0.55:
        score += 0.5
        _add(evidence, "policy_score", round(policy_score, 4))

    macro_score = _float(row.get("macro_score"), None)
    if macro_score is not None and macro_score >= 0.55:
        score += 0.5
        _add(evidence, "macro_score", round(macro_score, 4))

    article_count = _int(row.get("news_article_count"), 0)
    if article_count >= 2:
        score += 0.25
        _add(evidence, "news_article_count", article_count)

    directness = _float(row.get("news_implication_directness_score"), None)
    if directness is not None and directness >= 0.55:
        score += 0.5
        _add(evidence, "news_directness", round(directness, 4))

    strength = _float(row.get("news_implication_strength_avg"), None)
    confidence = _float(row.get("news_implication_confidence_avg"), None)
    if strength is not None and confidence is not None and strength >= 0.55 and confidence >= 0.55:
        score += 0.75
        _add(evidence, "news_strength_confidence", f"{round(strength, 4)}/{round(confidence, 4)}")

    source_signal = _float(row.get("news_source_signal_score"), None)
    if source_signal is not None and source_signal >= 0.50:
        score += 0.5
        _add(evidence, "source_signal_score", round(source_signal, 4))

    freshest_age = _float(row.get("news_freshest_age_hours"), None)
    if freshest_age is not None and freshest_age <= 24.0:
        score += 0.25
        _add(evidence, "fresh_news_hours", round(freshest_age, 2))

    if not evidence:
        _add(evidence, "event_evidence_missing")
    return round(score, 3), "|".join(evidence)


def _money_axis(row: dict[str, Any]) -> tuple[float, str]:
    score = 0.0
    evidence: list[str] = []

    trading_value = _float(_first(row.get("trading_value"), row.get("value")), 0.0) or 0.0
    if trading_value >= 50_000_000_000:
        score += 2.0
        _add(evidence, "trading_value_ge_50b", round(trading_value, 0))
    elif trading_value >= 10_000_000_000:
        score += 1.25
        _add(evidence, "trading_value_ge_10b", round(trading_value, 0))
    elif trading_value >= 1_000_000_000:
        score += 0.5
        _add(evidence, "trading_value_ge_1b", round(trading_value, 0))

    rvol = _float(row.get("rvol20"), None)
    if rvol is not None:
        if 1.5 <= rvol <= 5.0:
            score += 1.0
            _add(evidence, "rvol_confirmed", round(rvol, 4))
        elif rvol > 5.0:
            score += 0.5
            _add(evidence, "rvol_overheat_watch", round(rvol, 4))

    v_accel = _float(row.get("v_accel"), None)
    if v_accel is not None:
        if v_accel >= 2.0:
            score += 1.0
            _add(evidence, "v_accel_ge_2", round(v_accel, 4))
        elif v_accel >= 1.0:
            score += 0.5
            _add(evidence, "v_accel_ge_1", round(v_accel, 4))

    flow_score = _float(row.get("flow_score"), None)
    if flow_score is not None and flow_score >= 0.55:
        score += 0.75
        _add(evidence, "flow_score", round(flow_score, 4))

    foreign_net = _float(_first(row.get("foreign_net_20d"), row.get("foreign_net")), None)
    institution_net = _float(_first(row.get("institution_net_20d"), row.get("institution_net")), None)
    if foreign_net is not None and institution_net is not None:
        if foreign_net + institution_net > 0:
            score += 0.75
            _add(evidence, "foreign_institution_net_positive", round(foreign_net + institution_net, 0))
        if foreign_net > 0 and institution_net > 0:
            score += 0.5
            _add(evidence, "foreign_and_institution_both_positive")

    personal_net = _float(_first(row.get("personal_net_20d"), row.get("personal_net")), None)
    if personal_net is not None and foreign_net is not None and institution_net is not None:
        fi_sum = foreign_net + institution_net
        if personal_net > 0 and fi_sum <= 0:
            score -= 0.75
            _add(evidence, "personal_chase_without_fi_support", round(personal_net, 0))

    if not evidence:
        _add(evidence, "money_evidence_missing")
    return round(score, 3), "|".join(evidence)


def _pullback_axis(row: dict[str, Any]) -> tuple[float, str]:
    score = 0.0
    evidence: list[str] = []

    calc = _calc_pullback_fields(row)
    high_drawdown = _float(_first(row.get("intraday_high_drawdown_pct"), calc.get("calc_high_drawdown_pct")), None)
    # market_rising intraday_drawdown_pct is a positive low-to-current rebound metric.
    if high_drawdown is not None and high_drawdown > 1.0:
        high_drawdown = _float(calc.get("calc_high_drawdown_pct"), None)

    low_rebound = _float(_first(row.get("intraday_low_rebound_pct"), calc.get("calc_low_rebound_pct")), None)
    if low_rebound is not None and low_rebound > 1.0:
        low_rebound = low_rebound / 100.0

    range_pos = _float(_first(row.get("intraday_range_position_pct"), calc.get("calc_range_position_pct")), None)
    if range_pos is not None and range_pos > 1.0:
        range_pos = range_pos / 100.0

    change = _float(row.get("change_pct"), 0.0) or 0.0
    if change > 1.0:
        change = change / 100.0
    disparity20 = _float(row.get("disparity20"), None)

    if high_drawdown is not None:
        if -0.08 <= high_drawdown <= -0.02:
            score += 1.0
            _add(evidence, "controlled_pullback_from_high", round(high_drawdown, 5))
        elif -0.02 < high_drawdown <= 0.0:
            score += 0.25
            _add(evidence, "shallow_pullback", round(high_drawdown, 5))
        elif high_drawdown < -0.10:
            score -= 1.0
            _add(evidence, "deep_rejection_risk", round(high_drawdown, 5))

    if low_rebound is not None and low_rebound >= 0.02:
        score += 0.75
        _add(evidence, "rebound_from_low", round(low_rebound, 5))

    if range_pos is not None and range_pos >= 0.50:
        score += 0.5
        _add(evidence, "upper_half_recovery", round(range_pos, 5))

    if disparity20 is not None and 0.98 <= disparity20 <= 1.08:
        score += 0.5
        _add(evidence, "near_20d_reference", round(disparity20, 5))

    if _truthy(row.get("macd_bullish")) or _truthy(row.get("macd_golden")):
        score += 0.5
        _add(evidence, "macd_bullish_or_golden")

    if 0.05 <= change <= 0.16:
        score += 0.5
        _add(evidence, "not_late_chase_change_band", round(change, 5))
    elif change >= 0.24:
        score -= 0.25
        _add(evidence, "late_chase_watch", round(change, 5))

    if not evidence:
        _add(evidence, "pullback_evidence_missing")
    return round(score, 3), "|".join(evidence)


def _methodology_trend_axis(row: dict[str, Any]) -> tuple[float, str]:
    score = 0.0
    evidence: list[str] = []

    if _truthy(row.get("sma_ema_trend_ok")):
        score += 1.0
        _add(evidence, "sma_ema_trend_ok")

    adx = _float(row.get("adx14"), None)
    if adx is not None:
        if adx >= 20:
            score += 1.0
            _add(evidence, "adx_trend", round(adx, 4))
        else:
            score -= 0.25
            _add(evidence, "adx_weak", round(adx, 4))

    disparity20 = _float(row.get("disparity20"), None)
    disparity60 = _float(row.get("disparity60"), None)
    disparity200 = _float(row.get("disparity200"), None)
    if disparity20 is not None:
        if 0.98 <= disparity20 <= 1.12:
            score += 0.5
            _add(evidence, "price_near_or_above_20d", round(disparity20, 4))
        elif disparity20 > 1.2:
            score -= 0.5
            _add(evidence, "short_term_overextended", round(disparity20, 4))
    if disparity60 is not None and disparity60 >= 1.0:
        score += 0.25
        _add(evidence, "above_60d_reference", round(disparity60, 4))
    if disparity200 is not None:
        if disparity200 >= 1.0:
            score += 0.25
            _add(evidence, "above_200d_reference", round(disparity200, 4))
        elif disparity200 < 0.95:
            score -= 0.5
            _add(evidence, "below_200d_reference", round(disparity200, 4))

    rsi = _float(row.get("rsi14"), None)
    if rsi is not None:
        if 40 <= rsi <= 70:
            score += 0.5
            _add(evidence, "rsi_trend_pullback_zone", round(rsi, 4))
        elif rsi < 35:
            score -= 0.5
            _add(evidence, "rsi_trend_support_failed", round(rsi, 4))
        elif rsi > 85:
            score -= 0.25
            _add(evidence, "rsi_overheated", round(rsi, 4))

    if _truthy(row.get("macd_bullish")) or _truthy(row.get("macd_golden")):
        score += 0.5
        _add(evidence, "macd_support")

    if not evidence:
        _add(evidence, "trend_data_limited")
    return round(score, 3), "|".join(evidence)


def _methodology_volume_axis(row: dict[str, Any]) -> tuple[float, str]:
    score = 0.0
    evidence: list[str] = []

    rvol = _float(row.get("rvol20"), None)
    v_accel = _float(row.get("v_accel"), None)
    volume_now = _float(row.get("volume_now"), None)
    avg_vol20 = _float(row.get("avg_vol20"), None)

    if rvol is not None:
        if 0.5 <= rvol <= 1.2:
            score += 0.75
            _add(evidence, "quiet_or_orderly_pullback_volume", round(rvol, 4))
        elif 1.2 < rvol <= 3.0:
            score += 0.5
            _add(evidence, "volume_reaccel_possible", round(rvol, 4))
        elif rvol > 5.0:
            score -= 0.5
            _add(evidence, "volume_overheat", round(rvol, 4))

    if v_accel is not None:
        if 0.4 <= v_accel <= 1.5:
            score += 0.5
            _add(evidence, "v_accel_orderly", round(v_accel, 4))
        elif v_accel > 3.0:
            score -= 0.25
            _add(evidence, "v_accel_overheat", round(v_accel, 4))

    if volume_now is not None and avg_vol20 is not None and avg_vol20 > 0:
        ratio = volume_now / avg_vol20
        if 0.5 <= ratio <= 1.2:
            score += 0.5
            _add(evidence, "volume_now_near_avg20", round(ratio, 4))
        elif ratio > 3.0:
            score -= 0.25
            _add(evidence, "volume_now_hot", round(ratio, 4))

    if not evidence:
        _add(evidence, "pullback_volume_contraction_data_limited")
    return round(score, 3), "|".join(evidence)


def _methodology_reaccel_axis(row: dict[str, Any]) -> tuple[float, str]:
    score = 0.0
    evidence: list[str] = []

    calc = _calc_pullback_fields(row)
    range_pos = _float(_first(row.get("intraday_range_position_pct"), calc.get("calc_range_position_pct")), None)
    if range_pos is not None and range_pos > 1.0:
        range_pos = range_pos / 100.0
    low_rebound = _float(_first(row.get("intraday_low_rebound_pct"), calc.get("calc_low_rebound_pct")), None)
    if low_rebound is not None and low_rebound > 1.0:
        low_rebound = low_rebound / 100.0

    if range_pos is not None:
        if range_pos >= 0.65:
            score += 1.0
            _add(evidence, "upper_range_reclaim", round(range_pos, 4))
        elif range_pos >= 0.50:
            score += 0.5
            _add(evidence, "midrange_recovery", round(range_pos, 4))
        elif range_pos < 0.35:
            score -= 0.5
            _add(evidence, "range_recovery_weak", round(range_pos, 4))

    if low_rebound is not None and low_rebound >= 0.03:
        score += 0.5
        _add(evidence, "low_rebound_confirmed", round(low_rebound, 4))

    entry_decision = str(row.get("entry_decision") or "").strip().upper()
    if entry_decision == "ENTRY_ALLOWED":
        score += 1.0
        _add(evidence, "current_surge_entry_allowed")
    elif entry_decision == "WAIT_EXECUTION":
        score += 0.5
        _add(evidence, "current_surge_wait_execution")
    elif entry_decision == "ENTRY_BLOCKED":
        score -= 0.5
        _add(evidence, "current_surge_entry_blocked")

    if _truthy(row.get("detected_surge_flag")) or _truthy(row.get("is_realtime_surge")):
        score += 0.5
        _add(evidence, "realtime_surge_detected")

    if not evidence:
        _add(evidence, "reaccel_confirmation_data_limited")
    return round(score, 3), "|".join(evidence)


def _methodology_first_pullback_axis(row: dict[str, Any]) -> tuple[float, str]:
    score = 0.0
    evidence: list[str] = []

    ret_5d = _float(row.get("ret_5d"), None)
    ret_15d = _float(row.get("ret_15d"), None)
    high_drawdown = _float(row.get("intraday_high_drawdown_pct"), None)
    if high_drawdown is not None and high_drawdown > 1.0:
        high_drawdown = None
    high_52w_gap = _float(row.get("high_52w_gap"), None)

    if ret_5d is not None and ret_15d is not None:
        if ret_15d > 0.10 and ret_5d > -0.05:
            score += 0.75
            _add(evidence, "recent_leader_after_15d_strength", f"{round(ret_5d, 4)}/{round(ret_15d, 4)}")
        elif ret_15d < -0.05:
            score -= 0.5
            _add(evidence, "not_recent_leader", round(ret_15d, 4))

    if high_drawdown is not None:
        if -0.08 <= high_drawdown <= -0.02:
            score += 0.5
            _add(evidence, "first_pullback_depth_proxy", round(high_drawdown, 4))
        elif high_drawdown < -0.12:
            score -= 0.5
            _add(evidence, "late_or_failed_pullback_depth", round(high_drawdown, 4))

    if high_52w_gap is not None:
        if high_52w_gap <= 0.15:
            score += 0.25
            _add(evidence, "near_leader_high_proxy", round(high_52w_gap, 4))
        elif high_52w_gap > 0.55:
            score -= 0.25
            _add(evidence, "far_from_leader_high", round(high_52w_gap, 4))

    if not evidence:
        _add(evidence, "first_pullback_sequence_data_limited")
    return round(score, 3), "|".join(evidence)


def _methodology_rr_axis(row: dict[str, Any]) -> tuple[float, str, str, str, str]:
    score = 0.0
    evidence: list[str] = []
    current = _float(_first(row.get("current_price"), row.get("close")), None)
    high = _float(row.get("high_price"), None)
    low = _float(row.get("low_price"), None)
    atr_pct = _float(row.get("atr14_pct"), None)

    if current is None or current <= 0:
        return 0.0, "rr_data_limited", "", "", ""

    structural_stop_pct = None
    if low is not None and low > 0 and low < current:
        structural_stop_pct = (current - low) / current
    atr_stop_pct = 1.5 * atr_pct if atr_pct is not None and atr_pct > 0 else None

    stop_pct_candidates = [v for v in (structural_stop_pct, atr_stop_pct) if v is not None and v > 0]
    if not stop_pct_candidates:
        return 0.0, "rr_stop_data_limited", "", "", ""

    stop_pct = max(stop_pct_candidates)
    target_pct = None
    if high is not None and high > current:
        target_pct = (high - current) / current
        _add(evidence, "target_prior_intraday_high", round(target_pct, 4))
    else:
        target_pct = 0.05
        _add(evidence, "target_proxy_5pct")

    rr = target_pct / stop_pct if stop_pct > 0 else 0.0
    if rr >= 2.0:
        score += 1.0
        _add(evidence, "rr_ge_2", round(rr, 4))
    elif rr >= 1.2:
        score += 0.5
        _add(evidence, "rr_ge_1_2", round(rr, 4))
    else:
        score -= 0.5
        _add(evidence, "rr_weak", round(rr, 4))

    _add(evidence, "stop_pct", round(stop_pct, 4))
    if structural_stop_pct is not None:
        _add(evidence, "structural_stop_pct", round(structural_stop_pct, 4))
    if atr_stop_pct is not None:
        _add(evidence, "atr_stop_pct", round(atr_stop_pct, 4))

    stop_price = round(current * (1.0 - stop_pct), 4)
    target_price = round(current * (1.0 + target_pct), 4)
    return round(score, 3), "|".join(evidence), round(rr, 4), stop_price, target_price


def _intraday_volume_structure(history_rows: list[dict[str, str]]) -> dict[str, Any]:
    points = _intraday_points(history_rows)
    if len(points) < 4:
        return {
            "pullback_volume_label": "VOLUME_DATA_LIMITED",
            "pullback_volume_evidence": "intraday_history_points_lt_4",
            "pullback_volume_contraction_ratio": "",
            "reaccel_volume_ratio": "",
            "negative_volume_expansion": "",
        }

    prices = [p["price"] for p in points]
    peak_idx = max(range(len(points)), key=lambda i: prices[i])
    post_peak = points[peak_idx:] if peak_idx < len(points) else points
    trough_rel = min(range(len(post_peak)), key=lambda i: post_peak[i]["price"])
    trough_idx = peak_idx + trough_rel

    rise_values = [p["delta_value"] for p in points[: max(peak_idx + 1, 1)]]
    pullback_values = [p["delta_value"] for p in points[peak_idx + 1: max(trough_idx + 1, peak_idx + 2)]]
    reaccel_values = [p["delta_value"] for p in points[trough_idx + 1:]]

    rise_avg = _avg(rise_values)
    pullback_avg = _avg(pullback_values)
    reaccel_avg = _avg(reaccel_values)
    contraction = _ratio(pullback_avg, rise_avg)
    reaccel_ratio = _ratio(reaccel_avg, pullback_avg)
    negative_expansion = sum(1 for p in points if p["ret_pct"] < -0.003 and p["delta_value"] > (rise_avg or 0.0))

    evidence: list[str] = []
    label = "VOLUME_NEUTRAL"
    if contraction is not None:
        _add(evidence, "pullback_vs_rise_value_ratio", round(contraction, 4))
    if reaccel_ratio is not None:
        _add(evidence, "reaccel_vs_pullback_value_ratio", round(reaccel_ratio, 4))
    if negative_expansion:
        _add(evidence, "negative_volume_expansion_bars", negative_expansion)

    if contraction is not None and contraction <= 0.7 and negative_expansion == 0:
        label = "HEALTHY_VOLUME_CONTRACTION"
    if reaccel_ratio is not None and reaccel_ratio >= 1.25 and contraction is not None and contraction <= 1.0:
        label = "VOLUME_REACCELERATION"
    if contraction is not None and contraction > 1.25:
        label = "PULLBACK_VOLUME_DISTRIBUTION_RISK"
    if negative_expansion >= 2:
        label = "NEGATIVE_VOLUME_EXPANSION_RISK"

    return {
        "pullback_volume_label": label,
        "pullback_volume_evidence": "|".join(evidence) if evidence else "volume_phase_evidence_limited",
        "pullback_volume_contraction_ratio": "" if contraction is None else round(contraction, 4),
        "reaccel_volume_ratio": "" if reaccel_ratio is None else round(reaccel_ratio, 4),
        "negative_volume_expansion": negative_expansion,
    }


def _intraday_first_pullback_structure(row: dict[str, Any], history_rows: list[dict[str, str]]) -> dict[str, Any]:
    points = _intraday_points(history_rows)
    if len(points) < 4:
        return {
            "first_pullback_label": "FIRST_PULLBACK_DATA_LIMITED",
            "first_pullback_evidence_detail": "intraday_history_points_lt_4",
            "pullback_sequence_count_proxy": "",
            "lower_high_proxy": "",
        }

    prices = [p["price"] for p in points]
    peak_idx = max(range(len(points)), key=lambda i: prices[i])
    post_peak = points[peak_idx:]
    trough_rel = min(range(len(post_peak)), key=lambda i: post_peak[i]["price"])
    trough_idx = peak_idx + trough_rel

    local_highs = 0
    lower_highs = 0
    last_high: float | None = None
    for i in range(1, len(points) - 1):
        if points[i]["price"] > points[i - 1]["price"] and points[i]["price"] >= points[i + 1]["price"]:
            local_highs += 1
            if last_high is not None and points[i]["price"] < last_high:
                lower_highs += 1
            last_high = points[i]["price"]

    high_drawdown = _float(row.get("intraday_high_drawdown_pct"), None)
    if high_drawdown is not None and high_drawdown > 1.0:
        high_drawdown = None

    evidence: list[str] = []
    _add(evidence, "peak_idx", peak_idx)
    _add(evidence, "trough_idx", trough_idx)
    _add(evidence, "local_highs", local_highs)
    _add(evidence, "lower_highs", lower_highs)
    if high_drawdown is not None:
        _add(evidence, "high_drawdown", round(high_drawdown, 4))

    label = "FIRST_PULLBACK_UNCONFIRMED"
    if peak_idx <= max(2, len(points) // 3) and lower_highs == 0 and high_drawdown is not None and -0.10 <= high_drawdown <= -0.02:
        label = "FIRST_PULLBACK_CANDIDATE"
    elif lower_highs >= 2:
        label = "LATE_OR_WEAK_PULLBACK"
    elif high_drawdown is not None and high_drawdown < -0.12:
        label = "FAILED_OR_DEEP_PULLBACK"

    return {
        "first_pullback_label": label,
        "first_pullback_evidence_detail": "|".join(evidence),
        "pullback_sequence_count_proxy": local_highs,
        "lower_high_proxy": lower_highs,
    }


def _intraday_reaccel_structure(history_rows: list[dict[str, str]]) -> dict[str, Any]:
    points = _intraday_points(history_rows)
    if len(points) < 4:
        return {
            "reaccel_detail_label": "REACCEL_DATA_LIMITED",
            "reaccel_detail_evidence": "intraday_history_points_lt_4",
            "recent_high_breakout": "",
            "open_reclaim": "",
            "prior_high_reclaim": "",
        }

    latest = points[-1]
    previous = points[:-1]
    recent_window = previous[-5:] if len(previous) >= 5 else previous
    recent_high = max(p["price"] for p in recent_window) if recent_window else latest["price"]
    observed_high = max(p["price"] for p in points)
    open_price = _float(history_rows[-1].get("open"), None)
    if open_price is None:
        open_price = _float(history_rows[0].get("open"), None)

    recent_high_breakout = latest["price"] > recent_high
    open_reclaim = bool(open_price is not None and latest["price"] >= open_price)
    prior_high_reclaim = latest["price"] >= observed_high * 0.985
    recent_value_avg = _avg([p["delta_value"] for p in recent_window])
    latest_value = latest["delta_value"]
    value_reaccel = recent_value_avg is not None and recent_value_avg > 0 and latest_value >= recent_value_avg * 1.2

    evidence: list[str] = []
    _add(evidence, "latest_price", latest["price"])
    _add(evidence, "recent_high", round(recent_high, 4))
    _add(evidence, "observed_high", round(observed_high, 4))
    if open_price is not None:
        _add(evidence, "open", round(open_price, 4))
    _add(evidence, "latest_delta_value", round(latest_value, 0))
    if recent_value_avg is not None:
        _add(evidence, "recent_delta_value_avg", round(recent_value_avg, 0))

    label = "REACCEL_WAIT"
    if recent_high_breakout and (open_reclaim or prior_high_reclaim) and value_reaccel:
        label = "REACCEL_CONFIRMED"
    elif recent_high_breakout or open_reclaim or prior_high_reclaim:
        label = "REACCEL_RECOVERING"
    elif latest["price"] < observed_high * 0.94:
        label = "REACCEL_WEAK"

    return {
        "reaccel_detail_label": label,
        "reaccel_detail_evidence": "|".join(evidence),
        "recent_high_breakout": recent_high_breakout,
        "open_reclaim": open_reclaim,
        "prior_high_reclaim": prior_high_reclaim,
    }


def _enhanced_rr_structure(row: dict[str, Any], history_rows: list[dict[str, str]]) -> dict[str, Any]:
    points = _intraday_points(history_rows)
    current = _float(_first(row.get("current_price"), row.get("close")), None)
    if points:
        current = points[-1]["price"]
    if current is None or current <= 0:
        return {
            "rr_detail_label": "RR_DATA_LIMITED",
            "rr_detail_evidence": "current_price_missing",
            "structure_stop_price": "",
            "atr_stop_price": "",
            "rr_1r_target": "",
            "rr_2r_target": "",
            "slippage_bps_proxy": "",
        }

    observed_low = min((p["price"] for p in points), default=_float(row.get("low_price"), None) or 0.0)
    observed_high = max((p["price"] for p in points), default=_float(row.get("high_price"), None) or 0.0)
    atr_pct = _float(row.get("atr14_pct"), None)
    ask1 = _float(row.get("ask1"), None)
    bid1 = _float(row.get("bid1"), None)
    slippage_bps = 0.0
    if ask1 is not None and bid1 is not None and ask1 > 0 and bid1 > 0 and ask1 >= bid1:
        slippage_bps = ((ask1 - bid1) / current) * 10000.0

    structure_stop_price = observed_low if observed_low and observed_low < current else ""
    atr_stop_price = current * (1.0 - 1.5 * atr_pct) if atr_pct is not None and atr_pct > 0 else ""
    stop_candidates = [p for p in (structure_stop_price, atr_stop_price) if isinstance(p, float) and p > 0 and p < current]
    if not stop_candidates:
        return {
            "rr_detail_label": "RR_STOP_DATA_LIMITED",
            "rr_detail_evidence": "no_valid_structure_or_atr_stop",
            "structure_stop_price": structure_stop_price,
            "atr_stop_price": atr_stop_price,
            "rr_1r_target": "",
            "rr_2r_target": "",
            "slippage_bps_proxy": round(slippage_bps, 4),
        }

    stop_price = min(stop_candidates)
    risk_per_share = current - stop_price
    one_r_target = current + risk_per_share
    two_r_target = current + 2 * risk_per_share
    target_price = observed_high if observed_high > current else two_r_target
    rr = (target_price - current) / risk_per_share if risk_per_share > 0 else 0.0

    label = "RR_WEAK"
    if rr >= 2.0 and slippage_bps <= 30:
        label = "RR_ACCEPTABLE"
    elif rr >= 1.2:
        label = "RR_MARGINAL"

    evidence: list[str] = []
    _add(evidence, "current", round(current, 4))
    _add(evidence, "stop_price", round(stop_price, 4))
    _add(evidence, "target_price", round(target_price, 4))
    _add(evidence, "rr", round(rr, 4))
    _add(evidence, "slippage_bps_proxy", round(slippage_bps, 4))

    return {
        "rr_detail_label": label,
        "rr_detail_evidence": "|".join(evidence),
        "structure_stop_price": "" if structure_stop_price == "" else round(float(structure_stop_price), 4),
        "atr_stop_price": "" if atr_stop_price == "" else round(float(atr_stop_price), 4),
        "rr_1r_target": round(one_r_target, 4),
        "rr_2r_target": round(two_r_target, 4),
        "slippage_bps_proxy": round(slippage_bps, 4),
    }


def _methodology_label(score: float, risk_score: float, trend_score: float, reaccel_score: float, rr_score: float) -> str:
    if risk_score >= 2.0:
        return "METHODOLOGY_RISK_BLOCK"
    if score >= 5.0 and trend_score >= 1.5 and reaccel_score >= 1.0 and rr_score >= 0.5:
        return "PULLBACK_METHOD_STRONG"
    if score >= 3.0 and trend_score >= 0.75:
        return "PULLBACK_METHOD_WATCH"
    if score <= 0.5:
        return "PULLBACK_METHOD_WEAK"
    return "PULLBACK_METHOD_INCOMPLETE"


def _risk_axis(row: dict[str, Any]) -> tuple[float, str]:
    risk = 0.0
    evidence: list[str] = []

    token_hits = sorted(_tokens(row.get("exclude_reasons"), row.get("paper_probe_block_reasons")) & HARD_EXCLUDE_TOKENS)
    if token_hits:
        risk += 3.0
        _add(evidence, "hard_exclude_token", "|".join(token_hits))

    if _truthy(row.get("krx_admin")) or _truthy(row.get("krx_warning")) or _truthy(row.get("krx_risk")):
        risk += 3.0
        _add(evidence, "krx_hard_watch")

    if _truthy(row.get("krx_caution")) or str(row.get("watch_badge") or "").strip().upper() == "CAUTION":
        risk += 1.25
        _add(evidence, "krx_caution")

    news_block_rows = _int(row.get("news_implication_block_rows"), 0)
    if news_block_rows > 0:
        risk += 2.0
        _add(evidence, "news_implication_block_rows", news_block_rows)

    news_negative_rows = _int(row.get("news_implication_negative_rows"), 0)
    if news_negative_rows > 0:
        risk += 0.75
        _add(evidence, "news_negative_rows", news_negative_rows)

    change = _float(row.get("change_pct"), 0.0) or 0.0
    if change > 1.0:
        change = change / 100.0
    if change >= 0.24:
        risk += 0.75
        _add(evidence, "late_chase_change", round(change, 5))

    rvol = _float(row.get("rvol20"), None)
    if rvol is not None and rvol > 5.0:
        risk += 0.75
        _add(evidence, "rvol_overheat", round(rvol, 4))

    high_drawdown = _float(row.get("intraday_high_drawdown_pct"), None)
    if high_drawdown is not None and high_drawdown > 1.0:
        high_drawdown = None
    if high_drawdown is not None and high_drawdown < -0.10:
        risk += 1.0
        _add(evidence, "deep_high_rejection", round(high_drawdown, 5))

    orderflow_tag = str(row.get("orderflow_tag") or row.get("execution_orderflow_tag") or "").strip().upper()
    if orderflow_tag == "PAUSE":
        risk += 2.0
        _add(evidence, "orderflow_pause")
    elif orderflow_tag == "CAUTION":
        risk += 0.75
        _add(evidence, "orderflow_caution")

    if not evidence:
        _add(evidence, "risk_clear_current_inputs")
    return round(risk, 3), "|".join(evidence)


def _quality_label(event_score: float, money_score: float, pullback_score: float, risk_score: float) -> str:
    strong_axes = int(event_score >= 1.75) + int(money_score >= 3.0) + int(pullback_score >= 1.75)
    if risk_score >= 3.0:
        return "HARD_RISK"
    if strong_axes == 3 and risk_score <= 0.75:
        return "THREE_AXIS_STRONG"
    if strong_axes >= 2 and risk_score <= 1.5:
        return "TWO_AXIS_WATCH"
    if money_score >= 3.0 and risk_score <= 1.5:
        return "MONEY_LED_WATCH"
    return "WEAK_OR_RISKY"


def _bucket(row: dict[str, Any]) -> tuple[str, str]:
    event_score = _float(row.get("event_score"), 0.0) or 0.0
    money_score = _float(row.get("money_score"), 0.0) or 0.0
    pullback_score = _float(row.get("pullback_score"), 0.0) or 0.0
    risk_score = _float(row.get("risk_score"), 0.0) or 0.0
    total = event_score + money_score + pullback_score

    if risk_score >= 3.0:
        return "C_TRUE_EXCLUDE", "hard_or_compound_risk"
    if risk_score >= 2.0:
        return "C_TRUE_EXCLUDE", "risk_too_high_for_watch"
    if event_score >= 2.0 and money_score >= 3.0 and pullback_score >= 1.75 and total >= 7.0 and risk_score <= 0.75:
        return "A_BUYABLE_WATCH", "refined_three_axis_confirmed_low_risk"
    if money_score >= 3.0 and (event_score >= 1.25 or pullback_score >= 1.5) and risk_score <= 1.5:
        return "B_EXPLORE_WATCH", "refined_large_money_with_supporting_axis"
    if event_score >= 2.0 and pullback_score >= 1.5 and risk_score <= 1.5:
        return "B_EXPLORE_WATCH", "refined_event_pullback_watch_without_full_money"
    return "C_TRUE_EXCLUDE", "refined_structure_not_confirmed"


def _watch_subtype(row: dict[str, Any]) -> str:
    event_score = _float(row.get("event_score"), 0.0) or 0.0
    money_score = _float(row.get("money_score"), 0.0) or 0.0
    pullback_score = _float(row.get("pullback_score"), 0.0) or 0.0
    if event_score >= 2.0 and money_score >= 3.0 and pullback_score >= 1.75:
        return "event_money_pullback"
    if money_score >= 3.0 and pullback_score >= 1.5:
        return "money_pullback"
    if event_score >= 2.0 and money_score >= 3.0:
        return "event_money_no_pullback"
    if event_score >= 2.0 and pullback_score >= 1.5:
        return "event_pullback_money_weak"
    if money_score >= 3.0:
        return "money_only"
    return "mixed_watch"


def _bucket_display(bucket: str) -> str:
    if bucket == "A_BUYABLE_WATCH":
        return "HIGH_QUALITY_WATCH_NOT_BUY_APPROVAL"
    if bucket == "B_EXPLORE_WATCH":
        return "EXPLORE_WATCH_NOT_BUY_APPROVAL"
    if bucket == "C_TRUE_EXCLUDE":
        return "TRUE_EXCLUDE"
    return bucket


def _merge_sources() -> tuple[dict[str, dict[str, Any]], dict[str, int]]:
    sources = {
        "market_rising": _read_csv(MARKET_RISING_CSV),
        "surge_sanity": _read_csv(SURGE_SANITY_CSV),
        "candidate_detail": _read_csv(CANDIDATE_DETAIL_CSV),
        "news_candidates": _read_csv(NEWS_CANDIDATES_CSV),
    }
    merged: dict[str, dict[str, Any]] = {}
    source_counts: dict[str, int] = {}
    for source_name, rows in sources.items():
        source_counts[source_name] = len(rows)
        for src_row in rows:
            code = _code(src_row.get("code"))
            if not code:
                continue
            row = merged.setdefault(code, {"code": code, "source_tags": []})
            row["source_tags"].append(source_name)
            for key, value in src_row.items():
                if key == "code":
                    continue
                if value not in ("", None):
                    if key not in row or row.get(key) in ("", None):
                        row[key] = value
                    elif source_name == "candidate_detail" and key in {
                        "news_score",
                        "sector_score",
                        "macro_score",
                        "policy_score",
                        "flow_score",
                        "v_accel",
                        "adx14",
                        "rsi14",
                        "atr14_pct",
                        "foreign_net_20d",
                        "institution_net_20d",
                        "disparity20",
                        "disparity60",
                        "disparity200",
                        "high_52w_gap",
                        "sma_ema_trend_ok",
                        "macd_bullish",
                        "macd_golden",
                    }:
                        row[key] = value
    return merged, source_counts


def build() -> dict[str, Any]:
    merged, source_counts = _merge_sources()
    intraday_history_rows = _read_csv(INTRADAY_HISTORY_CSV)
    source_counts["intraday_history"] = len(intraday_history_rows)
    history_by_code = _history_by_code(intraday_history_rows)
    out_rows: list[dict[str, Any]] = []
    asof_values = Counter()

    for code, base in merged.items():
        row = dict(base)
        history_rows = history_by_code.get(code, [])
        event_score, event_evidence = _event_axis(row)
        money_score, money_evidence = _money_axis(row)
        pullback_score, pullback_evidence = _pullback_axis(row)
        risk_score, risk_evidence = _risk_axis(row)
        trend_score, trend_evidence = _methodology_trend_axis(row)
        volume_quality_score, volume_quality_evidence = _methodology_volume_axis(row)
        reaccel_score, reaccel_evidence = _methodology_reaccel_axis(row)
        first_pullback_score, first_pullback_evidence = _methodology_first_pullback_axis(row)
        rr_score, rr_evidence, rr_ratio, stop_price, target_price = _methodology_rr_axis(row)
        volume_structure = _intraday_volume_structure(history_rows)
        first_pullback_structure = _intraday_first_pullback_structure(row, history_rows)
        reaccel_structure = _intraday_reaccel_structure(history_rows)
        rr_structure = _enhanced_rr_structure(row, history_rows)
        methodology_score = round(
            trend_score + volume_quality_score + reaccel_score + first_pullback_score + rr_score,
            3,
        )
        total = round(event_score + money_score + pullback_score, 3)
        row.update({
            "event_score": event_score,
            "event_evidence": event_evidence,
            "money_score": money_score,
            "money_evidence": money_evidence,
            "pullback_score": pullback_score,
            "pullback_evidence": pullback_evidence,
            "risk_score": risk_score,
            "risk_evidence": risk_evidence,
            "methodology_trend_score": trend_score,
            "methodology_trend_evidence": trend_evidence,
            "methodology_volume_quality_score": volume_quality_score,
            "methodology_volume_quality_evidence": volume_quality_evidence,
            "methodology_reaccel_score": reaccel_score,
            "methodology_reaccel_evidence": reaccel_evidence,
            "methodology_first_pullback_score": first_pullback_score,
            "methodology_first_pullback_evidence": first_pullback_evidence,
            "methodology_rr_score": rr_score,
            "methodology_rr_evidence": rr_evidence,
            "methodology_rr_ratio": rr_ratio,
            "methodology_stop_price": stop_price,
            "methodology_target_price": target_price,
            **volume_structure,
            **first_pullback_structure,
            **reaccel_structure,
            **rr_structure,
            "methodology_score": methodology_score,
            "methodology_label": _methodology_label(methodology_score, risk_score, trend_score, reaccel_score, rr_score),
            "quality_label": _quality_label(event_score, money_score, pullback_score, risk_score),
            "total_structure_score": total,
            "source_tags": "|".join(sorted(set(row.get("source_tags", [])))),
            "research_only": True,
            "policy_change": False,
            "entry_approval_changed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_route": False,
            "connection_level": "SIGNAL_QUALITY_ONLY",
            "trading_connection": False,
            "signal_connection": False,
            "execution_connection": False,
            "connection_note": "Signal quality or observation only; not entry approval, order, fill, or ledger.",
        })
        bucket, reason = _bucket(row)
        row["criteria_first_bucket"] = bucket
        row["criteria_first_bucket_display"] = _bucket_display(bucket)
        row["bucket_contract"] = "watch_quality_only_not_entry_approval"
        row["bucket_reason"] = reason
        row["watch_subtype"] = _watch_subtype(row) if bucket != "C_TRUE_EXCLUDE" else ""
        if str(row.get("as_of_ymd") or "").strip():
            asof_values[str(row.get("as_of_ymd")).strip()] += 1
        elif str(row.get("date") or "").strip():
            asof_values[str(row.get("date")).strip().replace("-", "")[:8]] += 1
        out_rows.append(row)

    out_rows.sort(
        key=lambda r: (
            {"A_BUYABLE_WATCH": 0, "B_EXPLORE_WATCH": 1, "C_TRUE_EXCLUDE": 2}.get(str(r.get("criteria_first_bucket")), 9),
            -float(r.get("total_structure_score") or 0.0),
            str(r.get("code") or ""),
        )
    )

    bucket_counts = Counter(str(row.get("criteria_first_bucket") or "") for row in out_rows)
    quality_counts = Counter(str(row.get("quality_label") or "") for row in out_rows)
    methodology_counts = Counter(str(row.get("methodology_label") or "") for row in out_rows)
    subtype_counts = Counter(str(row.get("watch_subtype") or "") for row in out_rows if row.get("watch_subtype"))
    top_rows = [
        {
            "code": row.get("code"),
            "name": row.get("name"),
            "bucket": row.get("criteria_first_bucket"),
            "bucket_display": row.get("criteria_first_bucket_display"),
            "watch_subtype": row.get("watch_subtype"),
            "quality_label": row.get("quality_label"),
            "total_structure_score": row.get("total_structure_score"),
            "event_score": row.get("event_score"),
            "money_score": row.get("money_score"),
            "pullback_score": row.get("pullback_score"),
            "risk_score": row.get("risk_score"),
            "methodology_label": row.get("methodology_label"),
            "methodology_score": row.get("methodology_score"),
            "source_tags": row.get("source_tags"),
            "reason": row.get("bucket_reason"),
        }
        for row in out_rows[:15]
    ]

    result = {
        "generated_at": _now_ts(),
        "status": "OK",
        "scope": "surge_event_money_pullback_screener",
        "source_files": {
            "market_rising": str(MARKET_RISING_CSV),
            "surge_sanity": str(SURGE_SANITY_CSV),
            "candidate_detail": str(CANDIDATE_DETAIL_CSV),
            "news_candidates": str(NEWS_CANDIDATES_CSV),
            "intraday_history": str(INTRADAY_HISTORY_CSV),
        },
        "summary": {
            "source_rows": source_counts,
            "unique_codes_screened": len(out_rows),
            "bucket_counts": dict(sorted(bucket_counts.items())),
            "quality_counts": dict(sorted(quality_counts.items())),
            "methodology_counts": dict(sorted(methodology_counts.items())),
            "watch_subtype_counts": dict(sorted(subtype_counts.items())),
            "asof_values": dict(sorted(asof_values.items())),
            "research_only": True,
            "policy_change": False,
            "entry_approval_changed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_route": False,
            "connection_level": "SIGNAL_QUALITY_ONLY",
            "trading_connection": False,
            "signal_connection": False,
            "execution_connection": False,
            "connection_note": "Signal quality or observation only; not entry approval, order, fill, or ledger.",
        },
        "top_candidates": top_rows,
        "interpretation": (
            "Criteria-first read-only screener. It builds candidates from the broad daily rising, "
            "surge sanity, final-score, and news candidate universe before assigning A/B/C labels. "
            "Labels do not approve orders or relax gates."
        ),
        "artifacts": {"csv": str(OUT_CSV)},
    }

    OUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    fields = [
        "code", "name", "source_tags", "criteria_first_bucket", "criteria_first_bucket_display",
        "bucket_contract", "bucket_reason",
        "watch_subtype", "quality_label", "total_structure_score", "event_score",
        "event_evidence", "money_score", "money_evidence", "pullback_score",
        "pullback_evidence", "risk_score", "risk_evidence", "rank", "date",
        "methodology_label", "methodology_score",
        "methodology_trend_score", "methodology_trend_evidence",
        "methodology_volume_quality_score", "methodology_volume_quality_evidence",
        "methodology_reaccel_score", "methodology_reaccel_evidence",
        "methodology_first_pullback_score", "methodology_first_pullback_evidence",
        "methodology_rr_score", "methodology_rr_evidence", "methodology_rr_ratio",
        "methodology_stop_price", "methodology_target_price",
        "pullback_volume_label", "pullback_volume_evidence",
        "pullback_volume_contraction_ratio", "reaccel_volume_ratio",
        "negative_volume_expansion",
        "first_pullback_label", "first_pullback_evidence_detail",
        "pullback_sequence_count_proxy", "lower_high_proxy",
        "reaccel_detail_label", "reaccel_detail_evidence",
        "recent_high_breakout", "open_reclaim", "prior_high_reclaim",
        "rr_detail_label", "rr_detail_evidence", "structure_stop_price",
        "atr_stop_price", "rr_1r_target", "rr_2r_target",
        "slippage_bps_proxy",
        "as_of_ymd", "market", "detected_surge_type", "surge_sanity_status",
        "entry_decision", "entry_reason", "exclude_reasons", "current_price",
        "close", "change_pct", "rvol20", "trading_value", "value", "v_accel",
        "flow_score", "foreign_net_20d", "institution_net_20d", "personal_net_20d", "news_score",
        "news_article_count", "news_implication_positive_rows",
        "news_implication_boost_rows", "news_implication_block_rows", "sector_score",
        "sector_entry_allowed", "macro_score", "policy_score", "high_price",
        "low_price", "intraday_high_drawdown_pct", "intraday_low_rebound_pct",
        "intraday_range_position_pct", "intraday_drawdown_pct", "disparity20",
        "macd_bullish", "macd_golden", "krx_admin", "krx_warning", "krx_risk",
        "krx_caution", "watch_badge", "krx_watch_note", "research_only",
        "policy_change", "entry_approval_changed", "paper_order_route",
        "broker_order_route", "trading_route", "connection_level", "trading_connection", "signal_connection", "execution_connection", "connection_note",
    ]
    _write_csv(OUT_CSV, out_rows, fields)
    return result


def main() -> int:
    result = build()
    print(json.dumps(result["summary"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
