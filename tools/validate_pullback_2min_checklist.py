from __future__ import annotations

import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, time
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_pullback_methodology_first_screener import _load_daily_history


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CACHE_DIR = ROOT / "_cache"

SOURCE_CSV = LOG_DIR / "four_question_intraday_watch_latest.csv"
OUT_JSON = LOG_DIR / "pullback_2min_checklist_validation_latest.json"
OUT_CSV = LOG_DIR / "pullback_2min_checklist_validation_latest.csv"


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
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _code(value: Any) -> str:
    text = str(value or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6)[-6:] if digits else ""


def _ymd(value: Any) -> str:
    text = str(value or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _float(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value if value is not None else "").strip()
        if not text or text.lower() in {"nan", "none", "<na>"}:
            return default
        return float(text)
    except Exception:
        return default


def _pct(new: float, base: float) -> float:
    return (new / base) - 1.0 if base > 0 else 0.0


def _ema(values: list[float], span: int) -> float:
    if not values:
        return 0.0
    alpha = 2.0 / (span + 1.0)
    ema = values[0]
    for value in values[1:]:
        ema = value * alpha + ema * (1.0 - alpha)
    return ema


def _rsi(values: list[float], period: int = 14) -> float:
    if len(values) <= period:
        return 0.0
    gains: list[float] = []
    losses: list[float] = []
    for prev, cur in zip(values[-period - 1 : -1], values[-period:]):
        diff = cur - prev
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _atr(rows: pd.DataFrame, period: int = 14) -> float:
    if len(rows) < period + 1:
        return 0.0
    ordered = rows.sort_values("date").tail(period + 1)
    tr_values: list[float] = []
    prev_close = float(ordered.iloc[0]["close"])
    for _, row in ordered.iloc[1:].iterrows():
        high = float(row["high"])
        low = float(row["low"])
        tr_values.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        prev_close = float(row["close"])
    return sum(tr_values) / len(tr_values) if tr_values else 0.0


def _supply_history() -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for path in sorted(CACHE_DIR.glob("pykrx_supply_????????.csv")):
        ymd = _ymd(path.name)
        for row in _read_csv(path):
            code = _code(row.get("code"))
            if not code:
                continue
            out[code].append(
                {
                    "ymd": ymd,
                    "foreign_net": _float(row.get("foreign_net")),
                    "institution_net": _float(row.get("institution_net")),
                }
            )
    return out


def _intraday_rows(asof: str, code: str) -> list[dict[str, Any]]:
    path = LOG_DIR / f"intraday_prices_history_{asof}.csv"
    rows = []
    for row in _read_csv(path):
        if _code(row.get("code")) != code:
            continue
        price = _float(row.get("current_price"))
        volume = _float(row.get("volume"))
        value = _float(row.get("trading_value"))
        ts_text = str(row.get("ts") or "")
        rows.append(
            {
                "ts": ts_text,
                "time": _parse_time(ts_text),
                "price": price,
                "volume": volume,
                "value": value,
                "vwap": value / volume if volume > 0 else 0.0,
            }
        )
    rows.sort(key=lambda r: str(r.get("ts") or ""))
    return rows


def _parse_time(value: str) -> time | None:
    try:
        return datetime.fromisoformat(value).time()
    except Exception:
        return None


def _daily_by_code(daily: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if daily.empty:
        return {}
    return {str(code): g.sort_values("date").reset_index(drop=True) for code, g in daily.groupby("code", sort=False)}


def _daily_slice(daily_map: dict[str, pd.DataFrame], code: str, asof: str) -> pd.DataFrame:
    g = daily_map.get(code)
    if g is None or g.empty:
        return pd.DataFrame()
    return g[g["date"].astype(str) <= asof].copy()


def _market_breadth(daily: pd.DataFrame, asof: str) -> dict[str, dict[str, Any]]:
    day = daily[daily["date"].astype(str) == asof].copy() if not daily.empty else pd.DataFrame()
    out: dict[str, dict[str, Any]] = {}
    if day.empty:
        return out
    prior = daily[daily["date"].astype(str) < asof].copy()
    if prior.empty:
        return out
    prior = prior.sort_values(["code", "date"]).groupby("code", as_index=False).tail(1)
    prior_close = dict(zip(prior["code"].astype(str), pd.to_numeric(prior["close"], errors="coerce")))
    day["prev_close"] = day["code"].astype(str).map(prior_close)
    day["close_n"] = pd.to_numeric(day["close"], errors="coerce")
    day["ret"] = (day["close_n"] / day["prev_close"]) - 1.0
    day = day.dropna(subset=["ret"])
    for market, g in day.groupby("market"):
        up = int((g["ret"] > 0).sum())
        down = int((g["ret"] < 0).sum())
        out[str(market)] = {"up_count": up, "down_count": down, "breadth_proxy_pass": up > down}
    return out


def _latest_intraday(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    return rows[-1] if rows else None


def _check_precondition(daily_rows: pd.DataFrame) -> tuple[str, str, dict[str, Any]]:
    if len(daily_rows) < 25:
        return "NA", "daily_history_lt_25", {}
    recent = daily_rows.tail(20)
    high20 = float(recent["high"].max())
    low5 = float(daily_rows.tail(5)["low"].min())
    close = float(daily_rows.iloc[-1]["close"])
    volume3 = float(daily_rows.tail(3)["volume"].mean())
    prev_volume10 = float(daily_rows.iloc[-13:-3]["volume"].mean()) if len(daily_rows) >= 13 else 0.0
    pullback_pct = (high20 - low5) / high20 if high20 > 0 else 0.0
    volume_slowdown = prev_volume10 > 0 and volume3 < prev_volume10
    close_near_recovery = close >= low5 * 1.015
    passed = 0.03 <= pullback_pct <= 0.08 and volume_slowdown and close_near_recovery
    return (
        "PASS" if passed else "FAIL",
        "pullback_3_8pct_volume_slowdown_recovery" if passed else "pullback_definition_not_met",
        {
            "pullback_pct": round(pullback_pct, 6),
            "volume3_vs_prev10": round(volume3 / prev_volume10, 6) if prev_volume10 > 0 else 0.0,
        },
    )


def _check_trend(daily_rows: pd.DataFrame) -> tuple[str, str, dict[str, Any]]:
    if len(daily_rows) < 25:
        return "NA", "daily_history_lt_25", {}
    close_values = [float(v) for v in daily_rows["close"].tail(30).tolist()]
    ema20 = _ema(close_values, 20)
    last = daily_rows.iloc[-1]
    prev = daily_rows.iloc[-2]
    close = float(last["close"])
    higher_high_low = float(last["high"]) > float(prev["high"]) and float(last["low"]) >= float(prev["low"])
    passed = close >= ema20 and higher_high_low
    return (
        "PASS" if passed else "FAIL",
        "close_above_ema20_and_higher_high_low" if passed else "trend_or_higher_high_low_not_confirmed",
        {"close": round(close, 6), "ema20": round(ema20, 6), "higher_high_low": higher_high_low},
    )


def _check_vwap(daily_rows: pd.DataFrame, intraday: list[dict[str, Any]]) -> tuple[str, str, dict[str, Any]]:
    if len(daily_rows) < 25 or not intraday:
        return "NA", "daily_or_intraday_missing", {}
    latest = _latest_intraday(intraday)
    close_values = [float(v) for v in daily_rows["close"].tail(30).tolist()]
    ema20 = _ema(close_values, 20)
    price = float(latest["price"])
    vwap = float(latest["vwap"])
    below_two = len(intraday) >= 2 and all(float(row["price"]) < float(row["vwap"]) for row in intraday[-2:])
    passed = price >= vwap >= ema20 and not below_two
    return (
        "PASS" if passed else "FAIL",
        "price_ge_vwap_ge_ema20" if passed else "vwap_alignment_failed_or_two_bars_below",
        {"price": round(price, 6), "vwap": round(vwap, 6), "ema20": round(ema20, 6), "below_vwap_last_two": below_two},
    )


def _check_supply(code: str, asof: str, supply_map: dict[str, list[dict[str, Any]]]) -> tuple[str, str, dict[str, Any]]:
    rows = [row for row in supply_map.get(code, []) if str(row.get("ymd", "")) <= asof]
    if not rows:
        return "NA", "raw_supply_history_missing", {}
    latest5 = rows[-5:]
    foreign5 = sum(float(row.get("foreign_net") or 0.0) for row in latest5)
    inst5 = sum(float(row.get("institution_net") or 0.0) for row in latest5)
    smart5 = foreign5 + inst5
    # The checklist requires intraday foreign+institution buy ratio >= 55%.
    # This data is not present in the current intraday history, so only the 5-day flow proxy is reported.
    return (
        "NA",
        "intraday_foreign_institution_buy_ratio_missing; five_day_net_flow_proxy_reported",
        {"foreign_net_5d": round(foreign5, 3), "institution_net_5d": round(inst5, 3), "smart_net_5d": round(smart5, 3), "five_day_proxy_pass": smart5 > 0},
    )


def _check_rsi(daily_rows: pd.DataFrame, intraday: list[dict[str, Any]]) -> tuple[str, str, dict[str, Any]]:
    if len(daily_rows) < 16:
        return "NA", "daily_history_lt_16", {}
    closes = [float(v) for v in daily_rows["close"].tolist()]
    rsi = _rsi(closes, 14)
    rebound = bool(intraday and float(intraday[-1]["price"]) >= float(intraday[0]["price"]))
    if 45 <= rsi <= 55 and rebound:
        return "PASS", "rsi_45_55_and_rebound_attempt", {"rsi14": round(rsi, 6), "intraday_rebound": rebound}
    if rsi < 40 and not rebound:
        return "FAIL", "rsi_below_40_and_recovery_failed", {"rsi14": round(rsi, 6), "intraday_rebound": rebound}
    if rsi >= 68:
        return "OBSERVE", "rsi_near_overheat_observation", {"rsi14": round(rsi, 6), "intraday_rebound": rebound}
    return "FAIL", "rsi_band_condition_not_met", {"rsi14": round(rsi, 6), "intraday_rebound": rebound}


def _check_breadth(daily_rows: pd.DataFrame, breadth: dict[str, dict[str, Any]]) -> tuple[str, str, dict[str, Any]]:
    if daily_rows.empty:
        return "NA", "daily_history_missing", {}
    market = str(daily_rows.iloc[-1].get("market") or "")
    info = breadth.get(market)
    if not info:
        return "NA", "market_breadth_missing", {"market": market}
    return (
        "NA",
        "market_up_down_breadth_proxy_available_but_index_5ema_missing",
        {"market": market, **info},
    )


def _check_stop_and_dd(daily_rows: pd.DataFrame, intraday: list[dict[str, Any]]) -> tuple[tuple[str, str, dict[str, Any]], tuple[str, str, dict[str, Any]]]:
    if len(daily_rows) < 16:
        na = ("NA", "daily_history_lt_16", {})
        return na, na
    atr14 = _atr(daily_rows, 14)
    if atr14 <= 0:
        na = ("NA", "atr14_unavailable", {})
        return na, na
    correction_low = float(daily_rows.tail(5)["low"].min())
    stop_price = correction_low - atr14
    entry = float(intraday[-1]["price"]) if intraday else float(daily_rows.iloc[-1]["close"])
    risk_pct = (entry - stop_price) / entry if entry > 0 else 0.0
    stop_status = "PASS" if correction_low > 0 and stop_price > 0 else "FAIL"
    dd_status = "PASS" if 0 < risk_pct <= 0.02 else "FAIL"
    stop = (
        stop_status,
        "stop_reference_defined_with_1atr_below_correction_low" if stop_status == "PASS" else "stop_reference_not_clear",
        {"atr14": round(atr14, 6), "correction_low": round(correction_low, 6), "stop_price": round(stop_price, 6)},
    )
    dd = (
        dd_status,
        "estimated_dd_within_2pct_limit" if dd_status == "PASS" else "estimated_dd_exceeds_2pct_or_invalid",
        {"entry_price": round(entry, 6), "stop_price": round(stop_price, 6), "estimated_dd_pct": round(risk_pct, 6)},
    )
    return stop, dd


def _check_execution_window(intraday: list[dict[str, Any]]) -> tuple[str, str, dict[str, Any]]:
    if len(intraday) < 3:
        return "NA", "intraday_history_insufficient", {}
    window_start = time(9, 30)
    window_end = time(10, 30)
    window = [row for row in intraday if row.get("time") and window_start <= row["time"] <= window_end]
    if not window:
        return "NA", "no_intraday_rows_in_30_90min_window", {}
    reclaim_rows = [row for row in window if float(row["price"]) >= float(row["vwap"]) > 0]
    volume_deltas: list[float] = []
    for prev, cur in zip(intraday[:-1], intraday[1:]):
        delta = float(cur["volume"]) - float(prev["volume"])
        if delta >= 0:
            volume_deltas.append(delta)
    avg_delta = sum(volume_deltas) / len(volume_deltas) if volume_deltas else 0.0
    window_deltas: list[float] = []
    prev_by_ts = {str(row["ts"]): idx for idx, row in enumerate(intraday)}
    for row in window:
        idx = prev_by_ts.get(str(row["ts"]), 0)
        if idx > 0:
            delta = float(intraday[idx]["volume"]) - float(intraday[idx - 1]["volume"])
            if delta >= 0:
                window_deltas.append(delta)
    window_volume_ok = bool(avg_delta > 0 and window_deltas and max(window_deltas) >= avg_delta * 1.2)
    passed = bool(reclaim_rows and window_volume_ok)
    return (
        "PASS" if passed else "FAIL",
        "vwap_reclaim_in_window_with_volume_proxy" if passed else "execution_window_condition_not_met",
        {"window_rows": len(window), "vwap_reclaim_rows": len(reclaim_rows), "volume_proxy_ok": window_volume_ok},
    )


def _overall(item_statuses: list[str]) -> str:
    core = [s for s in item_statuses if s != "NA"]
    if not core:
        return "NOT_EVALUABLE"
    if any(s == "FAIL" for s in core):
        return "FAIL"
    if any(s in {"OBSERVE"} for s in core):
        return "OBSERVE"
    if any(s == "NA" for s in item_statuses):
        return "PARTIAL_PASS_WITH_DATA_GAPS"
    return "PASS"


def _set_item(row: dict[str, Any], idx: int, result: tuple[str, str, dict[str, Any]]) -> str:
    status, reason, details = result
    row[f"item{idx}_status"] = status
    row[f"item{idx}_reason"] = reason
    for key, value in details.items():
        row[f"item{idx}_{key}"] = value
    return status


def build() -> dict[str, Any]:
    source_rows = _read_csv(SOURCE_CSV)
    if not source_rows:
        payload = {
            "status": "FAIL",
            "reason": "SOURCE_MISSING_OR_EMPTY",
            "source": str(SOURCE_CSV),
            "generated_at": _now_ts(),
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
        }
        _write_json(OUT_JSON, payload)
        return payload

    daily = _load_daily_history()
    daily_map = _daily_by_code(daily)
    supply_map = _supply_history()
    asofs = sorted({_ymd(row.get("asof")) for row in source_rows if _ymd(row.get("asof"))})
    breadth_by_asof = {asof: _market_breadth(daily, asof) for asof in asofs}

    out_rows: list[dict[str, Any]] = []
    for source in source_rows:
        code = _code(source.get("code"))
        asof = _ymd(source.get("asof"))
        daily_rows = _daily_slice(daily_map, code, asof)
        intraday = _intraday_rows(asof, code)
        out: dict[str, Any] = {
            "asof": asof,
            "code": code,
            "name": source.get("name", ""),
            "tracking_group": source.get("tracking_group", ""),
            "role_fit_label": source.get("role_fit_label", ""),
            "reaction_label": source.get("reaction_label", ""),
            "intraday_status": source.get("intraday_status", ""),
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
        }
        statuses = [
            _set_item(out, 0, _check_precondition(daily_rows)),
            _set_item(out, 1, _check_trend(daily_rows)),
            _set_item(out, 2, _check_vwap(daily_rows, intraday)),
            _set_item(out, 3, _check_supply(code, asof, supply_map)),
            _set_item(out, 4, _check_rsi(daily_rows, intraday)),
            _set_item(out, 5, _check_breadth(daily_rows, breadth_by_asof.get(asof, {}))),
        ]
        stop, dd = _check_stop_and_dd(daily_rows, intraday)
        statuses.append(_set_item(out, 6, stop))
        statuses.append(_set_item(out, 7, dd))
        statuses.append(_set_item(out, 8, _check_execution_window(intraday)))
        out["overall_checklist_status"] = _overall(statuses)
        out["pass_count"] = sum(1 for status in statuses if status == "PASS")
        out["fail_count"] = sum(1 for status in statuses if status == "FAIL")
        out["na_count"] = sum(1 for status in statuses if status == "NA")
        out["observe_count"] = sum(1 for status in statuses if status == "OBSERVE")
        out_rows.append(out)

    out_rows.sort(key=lambda row: (str(row.get("overall_checklist_status")), str(row.get("tracking_group")), str(row.get("code"))))
    fields = []
    for row in out_rows:
        for key in row.keys():
            if key not in fields:
                fields.append(key)
    if not fields:
        fields = ["asof", "code"]
    _write_csv(OUT_CSV, out_rows, fields)

    item_counts = {
        f"item{idx}": dict(Counter(str(row.get(f"item{idx}_status")) for row in out_rows))
        for idx in range(9)
    }
    payload = {
        "status": "PASS",
        "reason": "ok",
        "generated_at": _now_ts(),
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
        "scope": "read_only_pullback_2min_checklist_validation",
        "source_files": {
            "role_fit_intraday_watch": str(SOURCE_CSV),
            "daily_archive": str(ROOT / "krx_daily_archive"),
            "intraday_price_pattern": str(LOG_DIR / "intraday_prices_history_YYYYMMDD.csv"),
            "supply_cache_pattern": str(CACHE_DIR / "pykrx_supply_YYYYMMDD.csv"),
        },
        "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV)},
        "source_counts": {
            "source_rows": len(source_rows),
            "daily_rows_loaded": int(len(daily)) if not daily.empty else 0,
            "supply_codes_loaded": len(supply_map),
        },
        "overall_status_counts": dict(Counter(str(row.get("overall_checklist_status")) for row in out_rows)),
        "item_status_counts": item_counts,
        "top_rows": out_rows[:30],
        "access_issues": [
            "institution+foreign intraday buy ratio is not present, so item3 remains NA with a five-day net-flow proxy",
            "market index 5EMA is not present, so item5 remains NA with market breadth proxy only",
            "execution-window volume normalization is computed from available snapshot volume deltas, not exact 5-minute bars",
            "this tool is read-only and does not approve orders or change entry gates",
        ],
    }
    _write_json(OUT_JSON, payload)
    return payload


def main() -> int:
    payload = build()
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
