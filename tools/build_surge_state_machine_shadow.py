from __future__ import annotations

import csv
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

CANDIDATES_CSV = LOG_DIR / "candidates_latest_data.with_final_score.csv"
FALLBACK_CANDIDATES_CSV = LOG_DIR / "candidates_latest_data.csv"
SURGE_REALTIME_CSV = LOG_DIR / "surge_realtime_latest.csv"
READINESS_CSV = LOG_DIR / "surge_ev_probe_readiness_latest.csv"
STAGED_CSV = LOG_DIR / "surge_ev_paper_probe_staged_latest.csv"
OHLCV_PARQUET = ROOT / "paper" / "prices" / "ohlcv_paper.parquet"
INTRADAY_PRICES_CSV = LOG_DIR / "intraday_prices_latest.csv"
MARKET_RISING_CSV = LOG_DIR / "market_rising_latest.csv"

OUT_JSON = LOG_DIR / "surge_state_machine_shadow_latest.json"
OUT_CSV = LOG_DIR / "surge_state_machine_shadow_latest.csv"


HARD_INTRADAY_BLOCKERS = {
    "ENTRY_CHANGE_BLOCK",
    "ENTRY_ATR_CAP",
    "HIGH_REJECTION_ENTRY_BLOCK",
    "RVOL_OVERHEAT_BLOCK",
    "SCORE_RVOL_OVERHEAT_BLOCK",
    "SPREAD_BLOCK",
    "ORDERFLOW_RISK_BLOCK",
    "ORDERFLOW_PAUSE",
    "ORDER_IMBALANCE_EXTREME",
    "KYLE_LAMBDA_Z_BLOCK",
    "MARKOUT_NEGATIVE_BLOCK",
    "OFI_NORM_EXTREME",
    "NEWS_NEGATIVE",
    "NEWS_IMPLICATION_BLOCK",
    "KRX_ADMIN",
    "KRX_WARNING",
    "KRX_RISK",
    "KRX_CAUTION",
}


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        out = float(value)
        if not math.isfinite(out):
            return default
        return out
    except Exception:
        return default


def _to_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _text_has_any(value: Any, needles: Iterable[str]) -> bool:
    text = str(value or "").lower()
    return any(str(needle).lower() in text for needle in needles)


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists() or path.stat().st_size <= 5:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        return list(csv.DictReader(fp))


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "code",
        "name",
        "state",
        "state_reason",
        "next_required_condition",
        "precursor_score",
        "precursor_stage",
        "precursor_reasons",
        "precursor_risk_flags",
        "intraday_score",
        "intraday_stage",
        "intraday_reasons",
        "intraday_risk_flags",
        "surge_score_final",
        "change_pct",
        "open_gap_pct",
        "open_to_current_pct",
        "day_range_pct",
        "rvol20",
        "trading_value",
        "order_imbalance_l1",
        "spread_bps",
        "ask_depth_levels",
        "ask_wall_l1_5_qty",
        "ask_wall_thin_flag",
        "readiness_status",
        "stage_status",
        "source_layers",
        "paper_order_route",
        "broker_order_route",
        "dispatch_enabled",
        "trading_allowed",
        "research_only",
        "must_not_dispatch",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def _index_by_code(rows: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        code = str(row.get("code") or "").zfill(6)
        if code and code != "000000":
            out[code] = row
    return out


def _merge_realtime_context(
    realtime: Dict[str, Dict[str, Any]],
    intraday_prices: Dict[str, Dict[str, Any]],
    market_rising: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for code in set(realtime) | set(intraday_prices) | set(market_rising):
        row: Dict[str, Any] = {}
        if code in market_rising:
            mr = market_rising[code]
            row.update({
                "market_rising_open_gap_pct": mr.get("open_gap_pct"),
                "market_rising_rank": mr.get("rank"),
                "market_rising_high_price": mr.get("high_price"),
                "market_rising_low_price": mr.get("low_price"),
                "market_rising_intraday_drawdown_pct": mr.get("intraday_drawdown_pct"),
                "market_rising_change_pct": mr.get("change_pct"),
                "market_rising_trading_value": mr.get("trading_value"),
            })
            if not row.get("current_price"):
                row["current_price"] = mr.get("current_price")
            if not row.get("trading_value"):
                row["trading_value"] = mr.get("trading_value")
        if code in intraday_prices:
            ip = intraday_prices[code]
            row.update({
                "intraday_open": ip.get("open"),
                "intraday_high": ip.get("high"),
                "intraday_low": ip.get("low"),
                "intraday_volume": ip.get("volume"),
                "intraday_trading_value": ip.get("trading_value"),
                "intraday_ask1": ip.get("ask1"),
                "intraday_bid1": ip.get("bid1"),
                "intraday_askq1": ip.get("askq1"),
                "intraday_bidq1": ip.get("bidq1"),
            })
            row["current_price"] = ip.get("current_price") or row.get("current_price")
            row["trading_value"] = ip.get("trading_value") or row.get("trading_value")
        if code in realtime:
            row.update(realtime[code])
        out[code] = row
    return out


def _split_reasons(value: Any) -> Set[str]:
    out: Set[str] = set()
    for part in str(value or "").replace(",", "|").split("|"):
        key = part.strip()
        if key:
            out.add(key.split(":", 1)[0])
    return out


def _candidate_path() -> Path:
    if CANDIDATES_CSV.exists() and CANDIDATES_CSV.stat().st_size > 5:
        return CANDIDATES_CSV
    return FALLBACK_CANDIDATES_CSV


def _build_ohlcv_context(codes: Set[str]) -> Dict[str, Dict[str, Any]]:
    if not codes or not OHLCV_PARQUET.exists() or OHLCV_PARQUET.stat().st_size <= 5:
        return {}
    try:
        import pandas as pd
    except Exception:
        return {}

    try:
        df = pd.read_parquet(OHLCV_PARQUET)
    except Exception:
        return {}
    if df.empty or "code" not in df.columns or "date" not in df.columns:
        return {}

    needed = {"date", "code", "open", "high", "low", "close", "volume", "name", "value"}
    keep = [col for col in df.columns if col in needed]
    df = df.loc[:, keep].copy()
    df["code"] = df["code"].astype(str).str.zfill(6)
    df = df[df["code"].isin(codes)]
    if df.empty:
        return {}

    df["date"] = df["date"].astype(str)
    for col in ("open", "high", "low", "close", "volume", "value"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    out: Dict[str, Dict[str, Any]] = {}
    for code, group in df.sort_values(["code", "date"]).groupby("code", sort=False):
        g = group.dropna(subset=["close"]).tail(260).copy()
        if g.empty:
            continue
        latest = g.iloc[-1]
        close = _to_float(latest.get("close"))
        if close <= 0:
            continue

        prev_close = _to_float(g.iloc[-2].get("close")) if len(g) >= 2 else 0.0
        close_5 = _to_float(g.iloc[-6].get("close")) if len(g) >= 6 else 0.0
        close_20 = _to_float(g.iloc[-21].get("close")) if len(g) >= 21 else 0.0
        prev_high_20 = _to_float(g.iloc[:-1]["high"].tail(20).max()) if len(g) >= 2 and "high" in g.columns else 0.0
        prev_high_60 = _to_float(g.iloc[:-1]["high"].tail(60).max()) if len(g) >= 2 and "high" in g.columns else 0.0
        prev_low_20 = _to_float(g.iloc[:-1]["low"].tail(20).min()) if len(g) >= 2 and "low" in g.columns else 0.0
        high_52w = _to_float(g["high"].max(), close) if "high" in g.columns else close
        latest_open = _to_float(latest.get("open"))
        latest_high = _to_float(latest.get("high"))
        latest_low = _to_float(latest.get("low"))
        value = _to_float(latest.get("value"))
        volume = _to_float(latest.get("volume"))
        if value <= 0 and volume > 0:
            value = close * volume

        vol_window = g["volume"].dropna().tail(21) if "volume" in g.columns else []
        avg_vol20 = _to_float(vol_window.iloc[:-1].mean()) if len(vol_window) >= 2 else 0.0
        v_accel = (volume / avg_vol20 - 1.0) if volume > 0 and avg_vol20 > 0 else 0.0

        if {"high", "low"}.issubset(g.columns):
            prev = g["close"].shift(1)
            tr = pd.concat([
                (g["high"] - g["low"]).abs(),
                (g["high"] - prev).abs(),
                (g["low"] - prev).abs(),
            ], axis=1).max(axis=1)
            atr14_pct = _to_float(tr.tail(14).mean()) / close if len(tr.dropna()) else 0.0
        else:
            atr14_pct = 0.0

        close_window = g["close"].dropna().tail(20)
        if len(close_window) >= 10:
            mid = _to_float(close_window.mean())
            bb_width = (_to_float(close_window.std()) * 4.0 / mid) if mid > 0 else 0.0
        else:
            bb_width = 0.0

        latest_range = latest_high - latest_low
        body = close - latest_open
        upper_wick = latest_high - max(close, latest_open)
        close_near_high = (latest_high - close) / latest_high if latest_high > 0 else 999.0
        prev_20_value = g.iloc[:-1]["value"].dropna().tail(20) if "value" in g.columns else []
        prev_20_value_avg = _to_float(prev_20_value.mean()) if len(prev_20_value) else 0.0
        limit_up_days_60 = 0
        if len(g) >= 2:
            pct = g["close"].pct_change()
            prior_pct = pct.iloc[:-1].tail(60)
            limit_up_days_60 = int(((prior_pct >= 0.25) & (prior_pct <= 0.305)).sum())

        out[str(code)] = {
            "date": str(latest.get("date") or ""),
            "code": str(code),
            "name": "" if latest.get("name") is None else str(latest.get("name") or ""),
            "value": value,
            "ret1_pct": ((close / prev_close) - 1.0) * 100.0 if prev_close > 0 else 0.0,
            "rs": ((close / close_20) - 1.0) * 100.0 if close_20 > 0 else 0.0,
            "rs_slope": ((close / close_5) - 1.0) * 100.0 if close_5 > 0 else 0.0,
            "v_accel": v_accel,
            "atr14_pct": atr14_pct,
            "bb_width": bb_width,
            "high_52w_gap": ((high_52w - close) / high_52w) if high_52w > 0 else 999.0,
            "prev_high_20_break": close > prev_high_20 if prev_high_20 > 0 else False,
            "prev_high_60_break": close > prev_high_60 if prev_high_60 > 0 else False,
            "box20_upper_break": close > prev_high_20 and prev_high_20 > prev_low_20 and ((prev_high_20 - prev_low_20) / close) <= 0.30 if close > 0 else False,
            "long_bull_candle": latest_range > 0 and body > 0 and body / latest_range >= 0.60,
            "close_near_high_pct": close_near_high,
            "upper_wick_ratio": upper_wick / latest_range if latest_range > 0 else 0.0,
            "value_bottom_rebound_20d": value > 0 and prev_20_value_avg > 0 and value >= prev_20_value_avg * 3.0,
            "value_vs_20d_avg": value / prev_20_value_avg if value > 0 and prev_20_value_avg > 0 else 0.0,
            "limit_up_days_60": limit_up_days_60,
            "context_source": "ohlcv_paper_latest",
        }
    return out


def _precursor_score(row: Dict[str, Any] | None) -> Dict[str, Any]:
    if not row:
        return {
            "score": 0.0,
            "stage": "UNKNOWN",
            "reasons": [],
            "risk_flags": ["NO_DAILY_CANDIDATE_CONTEXT"],
        }

    score = 0.0
    reasons: List[str] = []
    risk_flags: List[str] = []

    value = _to_float(row.get("value"))
    ret1_pct = _to_float(row.get("ret1_pct"))
    rs = _to_float(row.get("rs"))
    rs_slope = _to_float(row.get("rs_slope"))
    v_accel = _to_float(row.get("v_accel"))
    final_score = _to_float(row.get("final_score"))
    high_52w_gap = _to_float(row.get("high_52w_gap"), 999.0)
    bb_width = _to_float(row.get("bb_width"))
    atr14_pct = _to_float(row.get("atr14_pct"))
    news_score = _to_float(row.get("news_score"))
    news_rows = _to_float(row.get("news_implication_rows"))
    news_positive = _to_float(row.get("news_implication_positive_rows"))
    news_negative = _to_float(row.get("news_implication_negative_rows"))
    news_boost = _to_float(row.get("news_implication_boost_rows"))
    news_block = _to_float(row.get("news_implication_block_rows"))
    news_strength = _to_float(row.get("news_implication_strength_avg"))
    news_risk = _to_float(row.get("news_implication_risk_score"))
    sector_score = _to_float(row.get("sector_score"))
    sector_strength = _to_float(row.get("sector_strength"))
    sector_leader_coupling = _to_float(row.get("sector_leader_coupling"))
    sector_leader_effect = _to_float(row.get("sector_leader_effect"))
    junk_risk = _to_float(row.get("junk_risk_score"))
    foreign_net = _to_float(row.get("foreign_net"), _to_float(row.get("foreign_net_20d")))
    institution_net = _to_float(row.get("institution_net"), _to_float(row.get("institution_net_20d")))
    personal_net = _to_float(row.get("personal_net"), _to_float(row.get("personal_net_20d")))
    credit_ratio = _to_float(row.get("credit_balance_ratio"))
    short_ratio = _to_float(row.get("short_balance_ratio"))
    short_amount = _to_float(row.get("short_balance_amount"))
    lend_amount = _to_float(row.get("lend_balance_amount"))
    market_cap = _to_float(row.get("market_cap"))
    listed_shares = _to_float(row.get("listed_shares"))
    listing_days = _to_float(row.get("listing_days"), 999999.0)
    value_vs_20d_avg = _to_float(row.get("value_vs_20d_avg"))
    limit_up_days_60 = _to_float(row.get("limit_up_days_60"))

    if value >= 5_000_000_000:
        score += 15
        reasons.append("TURNOVER_GE_5B")
    elif value >= 1_000_000_000:
        score += 10
        reasons.append("TURNOVER_GE_1B")

    if rs > 0:
        score += 12
        reasons.append("RS_POSITIVE")
    if rs_slope > 0:
        score += 8
        reasons.append("RS_SLOPE_POSITIVE")
    if v_accel >= 0.45:
        score += 12
        reasons.append("VOLUME_ACCELERATION")
    if 0 <= high_52w_gap <= 0.15:
        score += 10
        reasons.append("NEAR_52W_HIGH")
    if _to_bool(row.get("prev_high_20_break")):
        score += 7
        reasons.append("BREAK_PREV_20D_HIGH")
    if _to_bool(row.get("prev_high_60_break")):
        score += 5
        reasons.append("BREAK_PREV_60D_HIGH")
    if _to_bool(row.get("box20_upper_break")):
        score += 6
        reasons.append("BOX20_UPPER_BREAK")
    if _to_bool(row.get("long_bull_candle")):
        score += 5
        reasons.append("LONG_BULL_CANDLE")
    if 0 <= _to_float(row.get("close_near_high_pct"), 999.0) <= 0.03:
        score += 4
        reasons.append("DAILY_CLOSE_NEAR_HIGH")
    if _to_float(row.get("upper_wick_ratio")) >= 0.45:
        risk_flags.append("UPPER_WICK_HEAVY")
        score -= 5
    if 0 < bb_width <= 0.12 and 0 < atr14_pct <= 0.12:
        score += 10
        reasons.append("VOLATILITY_COMPRESSION")
    if _to_float(row.get("boll_mid_pos")) >= 0.85:
        score += 4
        reasons.append("BOLLINGER_UPPER_ZONE")
    if _to_float(row.get("squeeze_momentum")) > 0:
        score += 4
        reasons.append("SQUEEZE_MOMENTUM_POSITIVE")
    if ret1_pct > 0:
        score += 8
        reasons.append("POSITIVE_DAILY_RETURN")
    if _to_bool(row.get("value_bottom_rebound_20d")) or value_vs_20d_avg >= 2.0:
        score += 6
        reasons.append("VALUE_BOTTOM_REBOUND_20D")
    if final_score >= 0.55:
        score += 15
        reasons.append("FINAL_SCORE_STRONG")
    elif final_score >= 0.40:
        score += 8
        reasons.append("FINAL_SCORE_OK")
    if news_score > 0:
        score += 5
        reasons.append("PRE_SIGNAL_NEWS_SCORE")
    if news_rows > 0 and news_positive > news_negative:
        score += 6
        reasons.append("NEWS_IMPLICATION_POSITIVE")
    if news_boost > 0 or news_strength >= 0.55:
        score += 5
        reasons.append("NEWS_IMPLICATION_BOOST")
    if news_block > 0 or news_risk >= 0.65:
        risk_flags.append("NEWS_IMPLICATION_RISK")
        score -= 12
    news_actions = row.get("news_implication_top_actions")
    news_tags = row.get("news_entity_top_tags")
    if _text_has_any(news_actions, ["order", "contract", "supply", "수주", "계약", "공급"]):
        score += 5
        reasons.append("NEWS_CONTRACT_OR_SUPPLY")
    if _text_has_any(news_actions, ["investment", "funding", "투자", "유치"]):
        score += 4
        reasons.append("NEWS_INVESTMENT_FUNDING")
    if _text_has_any(str(news_actions) + " " + str(news_tags), ["patent", "approval", "license", "fda", "특허", "승인", "허가", "인허가"]):
        score += 4
        reasons.append("NEWS_APPROVAL_OR_PATENT")
    if _text_has_any(news_actions, ["buyback", "treasury", "자사주"]):
        score += 3
        reasons.append("NEWS_BUYBACK")
    if _text_has_any(news_actions, ["m&a", "merger", "acquisition", "매각", "인수", "합병"]):
        score += 4
        reasons.append("NEWS_MA")
    if _text_has_any(news_actions, ["new business", "신규사업", "진출"]):
        score += 3
        reasons.append("NEWS_NEW_BUSINESS")
    if _text_has_any(str(row.get("junk_flags")) + " " + str(news_actions), ["audit", "감사", "의견거절", "한정"]):
        risk_flags.append("AUDIT_REPORT_RISK")
        score -= 15
    if sector_score > 0:
        score += 5
        reasons.append("SECTOR_SUPPORT")
    if sector_strength >= 0.55:
        score += 5
        reasons.append("SECTOR_STRENGTH")
    if sector_leader_coupling >= 0.40 or sector_leader_effect > 0:
        score += 5
        reasons.append("SECTOR_LEADER_COUPLING")

    if foreign_net > 0:
        score += 3
        reasons.append("FOREIGN_NET_BUY")
    if institution_net > 0:
        score += 3
        reasons.append("INSTITUTION_NET_BUY")
    if foreign_net > 0 and institution_net > 0:
        score += 4
        reasons.append("FOREIGN_INSTITUTION_COBUY")
    if personal_net > 0 and foreign_net <= 0 and institution_net <= 0:
        risk_flags.append("PERSONAL_ONLY_BUY_PRESSURE")
        score -= 4

    if credit_ratio >= 5:
        risk_flags.append("CREDIT_BALANCE_HIGH")
        score -= 6
    if short_ratio >= 3:
        risk_flags.append("SHORT_BALANCE_HIGH")
        score -= 5
    if short_amount > 0 and value > 0 and short_amount / value >= 3:
        risk_flags.append("SHORT_BALANCE_AMOUNT_HIGH_VS_TURNOVER")
        score -= 5
    if lend_amount > 0 and value > 0 and lend_amount / value >= 3:
        risk_flags.append("LEND_BALANCE_AMOUNT_HIGH_VS_TURNOVER")
        score -= 4

    if 0 < market_cap <= 300_000_000_000:
        score += 4
        reasons.append("SMALL_MARKET_CAP")
    elif market_cap >= 5_000_000_000_000:
        risk_flags.append("LARGE_CAP_LOWER_SURGE_TORQUE")
        score -= 3
    if 0 < listed_shares <= 30_000_000:
        score += 4
        reasons.append("LOW_LISTED_SHARES_PROXY")
    if listing_days <= 180:
        score += 3
        reasons.append("RECENT_LISTING")

    if _to_bool(row.get("krx_admin")) or _to_bool(row.get("krx_warning")) or _to_bool(row.get("krx_risk")):
        risk_flags.append("KRX_HARD_RISK")
        score -= 30
    if _to_bool(row.get("krx_caution")):
        risk_flags.append("KRX_CAUTION")
        score -= 10
    if junk_risk >= 88:
        risk_flags.append("JUNK_RISK_HIGH")
        score -= 20
    if ret1_pct >= 20:
        risk_flags.append("DAILY_MOVE_ALREADY_EXTENDED")
        score -= 10

    score = max(0.0, min(100.0, round(score, 3)))
    if "KRX_HARD_RISK" in risk_flags or "JUNK_RISK_HIGH" in risk_flags:
        stage = "EXCLUDE"
    elif score >= 65:
        stage = "WATCH"
    elif score >= 35:
        stage = "DISCOVERY"
    else:
        stage = "REJECT_PRECURSOR"

    return {
        "score": score,
        "stage": stage,
        "reasons": reasons,
        "risk_flags": risk_flags,
    }


def _intraday_score(row: Dict[str, Any] | None) -> Dict[str, Any]:
    if not row:
        return {
            "score": 0.0,
            "stage": "NO_INTRADAY_CONTEXT",
            "reasons": [],
            "risk_flags": ["NO_INTRADAY_CONTEXT"],
            "open_gap_pct": 0.0,
            "open_to_current_pct": 0.0,
            "day_range_pct": 0.0,
            "order_imbalance_l1": 0.0,
            "ask_wall_l1_5_qty": 0.0,
            "ask_wall_thin_flag": False,
        }

    score = 0.0
    reasons: List[str] = []
    risk_flags: List[str] = []

    surge_score = _to_float(row.get("surge_score_final"), _to_float(row.get("surge_score")))
    change_pct = _to_float(row.get("change_pct"))
    open_gap_pct = _to_float(row.get("market_rising_open_gap_pct")) / 100.0
    current_price = _to_float(row.get("current_price"))
    intraday_open = _to_float(row.get("intraday_open"))
    intraday_high = _to_float(row.get("intraday_high"), _to_float(row.get("market_rising_high_price")))
    day_range_pct = _to_float(row.get("day_range_pct"))
    if day_range_pct <= 0 and current_price > 0:
        intraday_low = _to_float(row.get("intraday_low"), _to_float(row.get("market_rising_low_price")))
        if intraday_high > 0 and intraday_low > 0:
            day_range_pct = (intraday_high - intraday_low) / current_price
    open_to_current_pct = ((current_price / intraday_open) - 1.0) if current_price > 0 and intraday_open > 0 else 0.0
    rvol20 = _to_float(row.get("rvol20"))
    trading_value = _to_float(row.get("trading_value"))
    market_rising_rank = _to_float(row.get("market_rising_rank"), 999999.0)
    spread_bps = _to_float(row.get("spread_bps"), 999.0)
    ask_depth = _to_float(row.get("ask_depth_levels"), -1.0)
    order_imbalance = _to_float(row.get("order_imbalance_l1"))
    orderflow_risk = _to_float(row.get("orderflow_risk_score"))
    orderflow_tag = str(row.get("orderflow_tag") or "").upper()
    ask_wall_l1_5_qty = sum(_to_float(row.get(f"askq{idx}")) for idx in range(1, 6))
    drawdown = _to_float(row.get("intraday_high_drawdown_pct"))
    lob_available = _to_bool(row.get("lob_available"))
    lob_status = str(row.get("lob_status") or "").upper()
    blocked = _to_bool(row.get("entry_blocked")) or _to_bool(row.get("excluded_by_policy"))
    blockers = _split_reasons(row.get("entry_reason")) | _split_reasons(row.get("exclude_reasons")) | _split_reasons(row.get("paper_probe_block_reasons"))
    hard = sorted(blockers & HARD_INTRADAY_BLOCKERS)

    if surge_score >= 90:
        score += 25
        reasons.append("SURGE_SCORE_GE_90")
    elif surge_score >= 80:
        score += 18
        reasons.append("SURGE_SCORE_GE_80")
    elif surge_score >= 70:
        score += 10
        reasons.append("SURGE_SCORE_GE_70")

    if change_pct >= 0.12:
        score += 15
        reasons.append("CHANGE_GE_12PCT")
    elif change_pct >= 0.05:
        score += 8
        reasons.append("CHANGE_GE_5PCT")

    if open_gap_pct >= 0.03:
        score += 5
        reasons.append("OPEN_GAP_GE_3PCT")
    if open_to_current_pct >= 0.04:
        score += 8
        reasons.append("OPEN_TO_CURRENT_GE_4PCT")
    elif open_to_current_pct <= -0.03:
        risk_flags.append("OPEN_TO_CURRENT_FADE")
        score -= 8

    if _to_bool(row.get("cond_15d_highest_close")):
        score += 8
        reasons.append("BREAKS_15D_HIGH_CLOSE")
    if intraday_high > 0 and current_price >= intraday_high * 0.995:
        score += 5
        reasons.append("CURRENT_NEAR_INTRADAY_HIGH")

    if rvol20 >= 3.0:
        score += 15
        reasons.append("RVOL_GE_3")
    elif rvol20 >= 2.0:
        score += 8
        reasons.append("RVOL_GE_2")

    if trading_value >= 2_000_000_000:
        score += 10
        reasons.append("TRADING_VALUE_GE_2B")
    elif trading_value >= 500_000_000:
        score += 5
        reasons.append("TRADING_VALUE_GE_500M")
    if market_rising_rank <= 30:
        score += 5
        reasons.append("MARKET_RISING_VALUE_RANK_TOP30")

    if change_pct >= 0.05 and rvol20 < 1.0:
        risk_flags.append("PRICE_UP_WITH_WEAK_RVOL")
        score -= 12
    if day_range_pct >= 0.12:
        score += 5
        reasons.append("DAY_RANGE_EXPANSION_GE_12PCT")

    if lob_available and lob_status == "OK":
        score += 12
        reasons.append("LOB_OK")
    elif lob_available:
        score += 4
        reasons.append("LOB_AVAILABLE_NOT_OK")
    if orderflow_tag == "OK" and orderflow_risk <= 0.30:
        score += 4
        reasons.append("ORDERFLOW_STRENGTH_OK")
    elif orderflow_risk >= 0.70:
        risk_flags.append("ORDERFLOW_RISK_HIGH")
        score -= 8

    if spread_bps <= 15:
        score += 8
        reasons.append("SPREAD_LE_15BPS")
    elif spread_bps > 40:
        risk_flags.append("WIDE_SPREAD")
        score -= 15

    if ask_depth >= 3:
        score += 5
        reasons.append("ASK_DEPTH_GE_3")
    elif 0 <= ask_depth < 2:
        risk_flags.append("SHALLOW_ASK_DEPTH")
        score -= 5
    else:
        risk_flags.append("ASK_DEPTH_UNKNOWN")

    if order_imbalance >= 0.30:
        score += 6
        reasons.append("BID_IMBALANCE_L1_POSITIVE")
    elif order_imbalance <= -0.50:
        risk_flags.append("ASK_SUPPLY_IMBALANCE_L1")
        score -= 8

    ask_wall_thin = bool(lob_available and ask_wall_l1_5_qty > 0 and trading_value >= 500_000_000 and ask_wall_l1_5_qty * max(current_price, 1.0) <= trading_value * 0.03)
    if ask_wall_thin:
        score += 4
        reasons.append("ASK_WALL_L1_5_THIN")
    if _to_bool(row.get("cond_limit_near")):
        if ask_depth >= 3:
            score += 5
            reasons.append("LIMIT_NEAR_WITH_ASK_DEPTH")
        else:
            risk_flags.append("LIMIT_NEAR_THIN_OR_UNKNOWN_DEPTH")
            score -= 4

    if drawdown <= -0.08:
        risk_flags.append("HIGH_REJECTION_DRAWDOWN")
        score -= 20

    if blocked:
        risk_flags.append("ENTRY_BLOCKED_OR_POLICY_EXCLUDED")
    risk_flags.extend(hard)

    score = max(0.0, min(100.0, round(score, 3)))
    if hard:
        stage = "REJECT_INTRADAY"
    elif score >= 75 and lob_available and lob_status == "OK" and not blocked:
        stage = "READY"
    elif score >= 45:
        stage = "WATCH"
    else:
        stage = "REJECT_INTRADAY" if blocked else "WATCH"

    return {
        "score": score,
        "stage": stage,
        "reasons": reasons,
        "risk_flags": sorted(set(risk_flags)),
        "open_gap_pct": open_gap_pct,
        "open_to_current_pct": open_to_current_pct,
        "day_range_pct": day_range_pct,
        "order_imbalance_l1": order_imbalance,
        "ask_wall_l1_5_qty": ask_wall_l1_5_qty,
        "ask_wall_thin_flag": ask_wall_thin,
    }


def _readiness_by_code(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        codes = [c.strip().zfill(6) for c in str(row.get("candidate_codes") or "").split(",") if c.strip()]
        for code in codes:
            out[code] = row
    return out


def _next_condition(state: str, precursor: Dict[str, Any], intraday: Dict[str, Any]) -> str:
    if state == "REJECT_PRECURSOR":
        return "Improve precursor score without hard KRX/junk risk."
    if state == "DISCOVERY":
        return "Need precursor score >=65 and live intraday confirmation."
    if state == "WATCH":
        if intraday["stage"] in {"NO_INTRADAY_CONTEXT", "WATCH"}:
            return "Need LOB OK, spread acceptable, volume/turnover continuation, and breakout/VWAP hold."
        return "Need EV readiness and paper-probe sizing contract."
    if state == "READY":
        return "Need EV readiness plus staged paper-probe contract; no broker route."
    if state == "PAPER_PROBE_READY_NOT_ROUTED":
        return "Needs explicit separate approval before virtual/paper consumer activation."
    if state == "REJECT_INTRADAY":
        return "Intraday hard blocker must clear; keep observation only."
    return "Observe only."


def _state_for(
    precursor: Dict[str, Any],
    intraday: Dict[str, Any],
    readiness: Dict[str, Any] | None,
    staged: Dict[str, Any] | None,
) -> Dict[str, Any]:
    if staged and str(staged.get("stage_status") or "") == "PAPER_PROBE_STAGED_NOT_ROUTED":
        state = "PAPER_PROBE_READY_NOT_ROUTED"
        reason = "staged paper-probe candidate exists but all routes are disabled"
    elif readiness and str(readiness.get("readiness_status") or "") == "SHADOW_PAPER_PROBE_READY_NOT_ROUTED":
        state = "PAPER_PROBE_READY_NOT_ROUTED"
        reason = "EV readiness contract exists but all routes are disabled"
    elif precursor["stage"] == "EXCLUDE" or precursor["stage"] == "REJECT_PRECURSOR":
        state = "REJECT_PRECURSOR"
        reason = "daily precursor layer rejected or lacks enough setup evidence"
    elif intraday["stage"] == "REJECT_INTRADAY":
        state = "REJECT_INTRADAY"
        reason = "intraday hard blocker or weak live action"
    elif intraday["stage"] == "READY":
        state = "READY"
        reason = "precursor plus intraday confirmation, no route approval"
    elif precursor["stage"] == "WATCH" or intraday["stage"] == "WATCH":
        state = "WATCH"
        reason = "watch candidate; live or EV confirmation incomplete"
    else:
        state = "DISCOVERY"
        reason = "early candidate only"
    return {
        "state": state,
        "state_reason": reason,
        "next_required_condition": _next_condition(state, precursor, intraday),
    }


def main() -> int:
    now = datetime.now().isoformat(timespec="seconds")
    candidate_path = _candidate_path()
    candidates = _index_by_code(_read_csv(candidate_path))
    realtime_raw = _index_by_code(_read_csv(SURGE_REALTIME_CSV))
    intraday_prices = _index_by_code(_read_csv(INTRADAY_PRICES_CSV))
    market_rising = _index_by_code(_read_csv(MARKET_RISING_CSV))
    realtime = _merge_realtime_context(realtime_raw, intraday_prices, market_rising)
    readiness = _readiness_by_code(_read_csv(READINESS_CSV))
    staged = _index_by_code(_read_csv(STAGED_CSV))

    codes_set = set(candidates) | set(realtime) | set(readiness) | set(staged)
    ohlcv_context = _build_ohlcv_context(codes_set)
    codes = sorted(codes_set | set(ohlcv_context))
    rows: List[Dict[str, Any]] = []
    for code in codes:
        cand = {**(ohlcv_context.get(code) or {}), **(candidates.get(code) or {})} or None
        rt = realtime.get(code)
        ready = readiness.get(code)
        stg = staged.get(code)
        precursor = _precursor_score(cand)
        intraday = _intraday_score(rt)
        state = _state_for(precursor, intraday, ready, stg)
        source_layers = []
        if cand:
            source_layers.append(str(cand.get("context_source") or "daily_candidate"))
        if rt:
            source_layers.append("surge_realtime")
        if ready:
            source_layers.append("ev_readiness")
        if stg:
            source_layers.append("paper_probe_staged")
        rows.append({
            "code": code,
            "name": (cand or rt or {}).get("name", ""),
            **state,
            "precursor_score": precursor["score"],
            "precursor_stage": precursor["stage"],
            "precursor_reasons": "|".join(precursor["reasons"]),
            "precursor_risk_flags": "|".join(precursor["risk_flags"]),
            "intraday_score": intraday["score"],
            "intraday_stage": intraday["stage"],
            "intraday_reasons": "|".join(intraday["reasons"]),
            "intraday_risk_flags": "|".join(intraday["risk_flags"]),
            "surge_score_final": _to_float((rt or {}).get("surge_score_final"), _to_float((rt or {}).get("surge_score"))),
            "change_pct": _to_float((rt or stg or {}).get("change_pct")),
            "open_gap_pct": intraday["open_gap_pct"],
            "open_to_current_pct": intraday["open_to_current_pct"],
            "day_range_pct": intraday["day_range_pct"],
            "rvol20": _to_float((rt or stg or {}).get("rvol20")),
            "trading_value": _to_float((rt or {}).get("trading_value")),
            "order_imbalance_l1": intraday["order_imbalance_l1"],
            "spread_bps": _to_float((rt or stg or {}).get("spread_bps"), -1.0),
            "ask_depth_levels": _to_float((rt or stg or {}).get("ask_depth_levels"), -1.0),
            "ask_wall_l1_5_qty": intraday["ask_wall_l1_5_qty"],
            "ask_wall_thin_flag": intraday["ask_wall_thin_flag"],
            "readiness_status": (ready or {}).get("readiness_status", ""),
            "stage_status": (stg or {}).get("stage_status", ""),
            "source_layers": "|".join(source_layers),
            "paper_order_route": False,
            "broker_order_route": False,
            "dispatch_enabled": False,
            "trading_allowed": False,
            "research_only": True,
            "must_not_dispatch": True,
        })

    state_order = {
        "PAPER_PROBE_READY_NOT_ROUTED": 0,
        "READY": 1,
        "WATCH": 2,
        "DISCOVERY": 3,
        "REJECT_INTRADAY": 4,
        "REJECT_PRECURSOR": 5,
    }
    rows.sort(key=lambda row: (state_order.get(str(row.get("state")), 99), -_to_float(row.get("intraday_score")), -_to_float(row.get("precursor_score")), str(row.get("code"))))

    state_counts: Dict[str, int] = {}
    for row in rows:
        state = str(row.get("state") or "UNKNOWN")
        state_counts[state] = state_counts.get(state, 0) + 1

    payload = {
        "ts": now,
        "status": "OK",
        "scope": "surge_state_machine_shadow",
            "source_files": {
            "candidates": str(candidate_path),
            "ohlcv_context": str(OHLCV_PARQUET),
            "surge_realtime": str(SURGE_REALTIME_CSV),
            "intraday_prices": str(INTRADAY_PRICES_CSV),
            "market_rising": str(MARKET_RISING_CSV),
            "readiness": str(READINESS_CSV),
            "staged": str(STAGED_CSV),
        },
        "summary": {
            "rows": len(rows),
            "state_counts": state_counts,
            "ohlcv_context_rows": len(ohlcv_context),
            "paper_order_route": False,
            "broker_order_route": False,
            "dispatch_enabled": False,
            "trading_allowed": False,
        },
        "risk_contract": {
            "shadow_only": True,
            "policy_change": False,
            "entry_approval_changed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "dispatch_enabled": False,
            "trading_allowed": False,
            "research_only": True,
            "must_not_dispatch": True,
        },
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"status": "OK", "rows": len(rows), "state_counts": state_counts, "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
