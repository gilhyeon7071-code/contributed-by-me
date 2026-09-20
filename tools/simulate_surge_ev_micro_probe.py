import csv
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CANDIDATES_JSON = LOG_DIR / "surge_ev_shadow_candidates_latest.json"
LEGACY_CANDIDATES_JSON = LOG_DIR / "surge_ev_probe_candidates_latest.json"
CANDIDATES_HISTORY_CSV = LOG_DIR / "surge_ev_shadow_candidates_history.csv"
OUT_JSON = LOG_DIR / "surge_ev_shadow_simulation_latest.json"
OUT_CSV = LOG_DIR / "surge_ev_shadow_simulation_latest.csv"
LEGACY_OUT_JSON = LOG_DIR / "surge_ev_micro_probe_simulation_latest.json"
LEGACY_OUT_CSV = LOG_DIR / "surge_ev_micro_probe_simulation_latest.csv"

STOP_LOSS_PCT = -0.012
TIMEBOX_MINUTES = [5, 15, 30]
PRIMARY_TIMEBOX_MIN = 15
MICRO_POSITION_FACTOR = 0.10
MIN_SLIPPAGE_BPS = 5.0
MAX_SLIPPAGE_BPS = 30.0
REGULAR_MARKET_CLOSE_HOUR = 15
REGULAR_MARKET_CLOSE_MINUTE = 30


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except Exception:
        return default


def _parse_ts(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value).strip())
    except Exception:
        return None


def _candidate_key(candidate: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return (
        str(candidate.get("code") or "").zfill(6),
        str(candidate.get("ts") or ""),
        str(candidate.get("tier") or ""),
        str(candidate.get("source") or ""),
    )


def _history_path_for(ts: datetime | None) -> Path:
    if ts is None:
        return LOG_DIR / f"intraday_prices_history_{datetime.now().strftime('%Y%m%d')}.csv"
    return LOG_DIR / f"intraday_prices_history_{ts.strftime('%Y%m%d')}.csv"


def _market_close_ts(ts: datetime) -> datetime:
    return ts.replace(hour=REGULAR_MARKET_CLOSE_HOUR, minute=REGULAR_MARKET_CLOSE_MINUTE, second=0, microsecond=0)


def _load_candidates() -> Dict[str, Any]:
    source_path = CANDIDATES_JSON if CANDIDATES_JSON.exists() else LEGACY_CANDIDATES_JSON
    if not source_path.exists():
        return {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "candidate_rows": 0,
            "candidates": [],
            "risk_contract": {
                "policy_change": False,
                "entry_approval_changed": False,
                "live_order_allowed": False,
                "entry_signal": False,
                "paper_order_route": False,
                "broker_order_route": False,
                "trading_route": False,
                "research_only": True,
                "must_not_dispatch": True,
            },
        }
    payload = json.loads(source_path.read_text(encoding="utf-8-sig"))
    payload["_source_candidates_json"] = str(source_path)
    return payload


def _load_candidate_history(day: datetime) -> List[Dict[str, Any]]:
    if not CANDIDATES_HISTORY_CSV.exists() or CANDIDATES_HISTORY_CSV.stat().st_size <= 5:
        return []
    rows: List[Dict[str, Any]] = []
    with CANDIDATES_HISTORY_CSV.open("r", encoding="utf-8-sig", newline="") as fp:
        for row in csv.DictReader(fp):
            signal_ts = _parse_ts(row.get("ts"))
            if signal_ts is None or signal_ts.date() != day.date():
                continue
            row["code"] = str(row.get("code") or "").zfill(6)
            rows.append(dict(row))
    return rows


def _merge_candidates(current: List[Dict[str, Any]], history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for candidate in history + current:
        key = _candidate_key(candidate)
        if key[0] and key[1]:
            merged[key] = candidate
    return sorted(
        merged.values(),
        key=lambda x: (
            str(x.get("ts") or ""),
            -_to_float(x.get("surge_score_final")),
            str(x.get("code") or ""),
        ),
    )


def _load_history(path: Path) -> Dict[str, List[Tuple[datetime, float]]]:
    out: Dict[str, List[Tuple[datetime, float]]] = {}
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        for row in csv.DictReader(fp):
            ts = _parse_ts(row.get("ts"))
            code = str(row.get("code") or "").zfill(6)
            px = _to_float(row.get("current_price"))
            if ts and code and px > 0:
                out.setdefault(code, []).append((ts, px))
    for code in out:
        out[code].sort(key=lambda x: x[0])
    return out


def _first_at_or_after(series: List[Tuple[datetime, float]], target: datetime) -> Tuple[datetime, float] | None:
    for ts, px in series:
        if ts >= target:
            return ts, px
    return None


def _slippage_bps(candidate: Dict[str, Any]) -> float:
    spread_bps = max(0.0, _to_float(candidate.get("spread_bps")))
    return max(MIN_SLIPPAGE_BPS, min(MAX_SLIPPAGE_BPS, spread_bps / 2.0))


def _simulate_candidate(candidate: Dict[str, Any], history: Dict[str, List[Tuple[datetime, float]]]) -> Dict[str, Any]:
    code = str(candidate.get("code") or "").zfill(6)
    signal_ts = _parse_ts(candidate.get("ts"))
    base = {
        "code": code,
        "tier": candidate.get("tier", ""),
        "source": candidate.get("source", ""),
        "signal_ts": candidate.get("ts", ""),
        "first_no_lob_ts": candidate.get("first_no_lob_ts", ""),
        "lob_confirm_ts": candidate.get("lob_confirm_ts", ""),
        "lob_confirm_delay_sec": _to_float(candidate.get("lob_confirm_delay_sec"), 0.0),
        "detected_surge_type": candidate.get("detected_surge_type", ""),
        "surge_score_final": _to_float(candidate.get("surge_score_final")),
        "rvol20": _to_float(candidate.get("rvol20")),
        "change_pct": _to_float(candidate.get("change_pct")),
        "day_range_pct": _to_float(candidate.get("day_range_pct")),
        "intraday_high_drawdown_pct": _to_float(candidate.get("intraday_high_drawdown_pct")),
        "intraday_low_rebound_pct": _to_float(candidate.get("intraday_low_rebound_pct")),
        "intraday_range_position_pct": _to_float(candidate.get("intraday_range_position_pct")),
        "lob_status": candidate.get("lob_status", ""),
        "spread_bps": _to_float(candidate.get("spread_bps")),
        "ask_depth_levels": _to_float(candidate.get("ask_depth_levels"), -1.0),
        "micro_position_factor": MICRO_POSITION_FACTOR,
        "stop_loss_pct": STOP_LOSS_PCT,
        "primary_timebox_min": PRIMARY_TIMEBOX_MIN,
        "policy_change": False,
        "entry_approval_changed": False,
        "live_order_allowed": False,
        "entry_signal": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "trading_route": False,
        "research_only": True,
        "must_not_dispatch": True,
    }
    if signal_ts is None:
        return {**base, "status": "NOT_EVALUABLE_BAD_SIGNAL_TS", "exit_reason": "NO_SIMULATION"}
    series = history.get(code, [])
    market_close_ts = _market_close_ts(signal_ts)
    regular_series = [(ts, px) for ts, px in series if ts <= market_close_ts]
    if signal_ts > market_close_ts:
        last_ts = series[-1][0].isoformat(timespec="seconds") if series else ""
        return {
            **base,
            "status": "NOT_EVALUABLE_SIGNAL_AFTER_MARKET_CLOSE",
            "market_close_ts": market_close_ts.isoformat(timespec="seconds"),
            "last_history_ts": last_ts,
            "exit_reason": "NO_SIMULATION_AFTER_MARKET_CLOSE",
        }
    entry = _first_at_or_after(regular_series, signal_ts)
    if entry is None:
        last_regular_ts = regular_series[-1][0].isoformat(timespec="seconds") if regular_series else ""
        last_ts = series[-1][0].isoformat(timespec="seconds") if series else ""
        has_post_close_price = any(ts >= signal_ts and ts > market_close_ts for ts, _ in series)
        return {
            **base,
            "status": (
                "NOT_EVALUABLE_NO_REGULAR_PRICE_AT_OR_AFTER_SIGNAL"
                if has_post_close_price
                else "NOT_EVALUABLE_NO_PRICE_AT_OR_AFTER_SIGNAL"
            ),
            "market_close_ts": market_close_ts.isoformat(timespec="seconds"),
            "last_history_ts": last_regular_ts or last_ts,
            "latest_history_ts": last_ts,
            "exit_reason": "NO_SIMULATION",
        }

    entry_ts, entry_px = entry
    slip_bps = _slippage_bps(candidate)
    entry_px_slip = entry_px * (1.0 + slip_bps / 10000.0)
    future = [(ts, px) for ts, px in regular_series if ts >= entry_ts]
    if not future:
        return {**base, "status": "NOT_EVALUABLE_NO_FUTURE_SERIES", "exit_reason": "NO_SIMULATION"}

    stop_hit = None
    max_fav = -999.0
    max_adv = 999.0
    for ts, px in future:
        ret = px / entry_px_slip - 1.0
        max_fav = max(max_fav, ret)
        max_adv = min(max_adv, ret)
        if stop_hit is None and ret <= STOP_LOSS_PCT:
            stop_hit = (ts, px, ret)

    horizon_results: Dict[str, Any] = {}
    for minutes in TIMEBOX_MINUTES:
        target = entry_ts + timedelta(minutes=minutes)
        point = _first_at_or_after(future, target)
        if point is None:
            horizon_results[f"ret_{minutes}m_pct"] = ""
            horizon_results[f"exit_{minutes}m_ts"] = ""
            horizon_results[f"pending_{minutes}m"] = True
            continue
        exit_ts, exit_px = point
        exit_px_slip = exit_px * (1.0 - slip_bps / 10000.0)
        ret = exit_px_slip / entry_px_slip - 1.0
        horizon_results[f"ret_{minutes}m_pct"] = round(ret * 100.0, 6)
        horizon_results[f"exit_{minutes}m_ts"] = exit_ts.isoformat(timespec="seconds")
        horizon_results[f"pending_{minutes}m"] = False

    primary_key = f"ret_{PRIMARY_TIMEBOX_MIN}m_pct"
    if stop_hit is not None:
        exit_reason = "STOP_LOSS_HIT"
        primary_ret_pct = round(stop_hit[2] * 100.0, 6)
        exit_ts = stop_hit[0].isoformat(timespec="seconds")
        exit_px = stop_hit[1]
    else:
        primary_point = _first_at_or_after(future, entry_ts + timedelta(minutes=PRIMARY_TIMEBOX_MIN))
        if primary_point is None:
            latest_ts = future[-1][0].isoformat(timespec="seconds")
            market_close_ts = _market_close_ts(entry_ts)
            latest_future_ts, latest_future_px = future[-1]
            if entry_ts + timedelta(minutes=PRIMARY_TIMEBOX_MIN) > market_close_ts:
                close_px_slip = latest_future_px * (1.0 - slip_bps / 10000.0)
                close_ret = close_px_slip / entry_px_slip - 1.0
                return {
                    **base,
                    "status": "NOT_EVALUABLE_MARKET_CLOSE_BEFORE_PRIMARY_TIMEBOX",
                    "entry_ts": entry_ts.isoformat(timespec="seconds"),
                    "entry_price": entry_px,
                    "slippage_bps_each_side": round(slip_bps, 6),
                    "entry_price_with_slippage": round(entry_px_slip, 6),
                    "required_primary_exit_after": (entry_ts + timedelta(minutes=PRIMARY_TIMEBOX_MIN)).isoformat(timespec="seconds"),
                    "market_close_ts": market_close_ts.isoformat(timespec="seconds"),
                    "latest_history_ts": latest_ts,
                    "latest_history_price": latest_future_px,
                    "close_markout_ret_pct": round(close_ret * 100.0, 6),
                    "exit_reason": "NO_SIMULATION_MARKET_CLOSE_BEFORE_TIMEBOX",
                    "max_favorable_pct": round(max_fav * 100.0, 6),
                    "max_adverse_pct": round(max_adv * 100.0, 6),
                    **horizon_results,
                }
            return {
                **base,
                "status": "NOT_EVALUABLE_WAITING_PRIMARY_TIMEBOX",
                "entry_ts": entry_ts.isoformat(timespec="seconds"),
                "entry_price": entry_px,
                "slippage_bps_each_side": round(slip_bps, 6),
                "entry_price_with_slippage": round(entry_px_slip, 6),
                "required_primary_exit_after": (entry_ts + timedelta(minutes=PRIMARY_TIMEBOX_MIN)).isoformat(timespec="seconds"),
                "latest_history_ts": latest_ts,
                "exit_reason": "NO_SIMULATION_WAITING_TIMEBOX",
                "max_favorable_pct": round(max_fav * 100.0, 6),
                "max_adverse_pct": round(max_adv * 100.0, 6),
                **horizon_results,
            }
        exit_reason = f"TIMEBOX_{PRIMARY_TIMEBOX_MIN}M"
        primary_ret_pct = horizon_results.get(primary_key)
        exit_ts = horizon_results.get(f"exit_{PRIMARY_TIMEBOX_MIN}m_ts", "")
        exit_px = primary_point[1]

    gross_ret = _to_float(primary_ret_pct) / 100.0
    micro_ret = gross_ret * MICRO_POSITION_FACTOR
    return {
        **base,
        "status": "EVALUATED",
        "entry_ts": entry_ts.isoformat(timespec="seconds"),
        "entry_price": entry_px,
        "slippage_bps_each_side": round(slip_bps, 6),
        "entry_price_with_slippage": round(entry_px_slip, 6),
        "exit_reason": exit_reason,
        "primary_exit_ts": exit_ts,
        "primary_exit_price": exit_px,
        "primary_ret_pct": primary_ret_pct,
        "micro_position_ret_pct": round(micro_ret * 100.0, 6),
        "max_favorable_pct": round(max_fav * 100.0, 6),
        "max_adverse_pct": round(max_adv * 100.0, 6),
        **horizon_results,
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "code",
        "tier",
        "source",
        "status",
        "signal_ts",
        "first_no_lob_ts",
        "lob_confirm_ts",
        "lob_confirm_delay_sec",
        "entry_ts",
        "entry_price",
        "lob_status",
        "ask_depth_levels",
        "day_range_pct",
        "intraday_high_drawdown_pct",
        "intraday_low_rebound_pct",
        "intraday_range_position_pct",
        "slippage_bps_each_side",
        "exit_reason",
        "market_close_ts",
        "primary_exit_ts",
        "primary_ret_pct",
        "close_markout_ret_pct",
        "required_primary_exit_after",
        "latest_history_ts",
        "micro_position_ret_pct",
        "max_favorable_pct",
        "max_adverse_pct",
        "ret_5m_pct",
        "ret_15m_pct",
        "ret_30m_pct",
        "micro_position_factor",
        "stop_loss_pct",
            "policy_change",
            "entry_approval_changed",
            "live_order_allowed",
            "entry_signal",
            "paper_order_route",
            "broker_order_route",
            "trading_route",
            "research_only",
            "must_not_dispatch",
        ]
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fields})


def main() -> int:
    now = datetime.now().isoformat(timespec="seconds")
    src = _load_candidates()
    current_candidates = list(src.get("candidates") or [])
    history_candidates = _load_candidate_history(datetime.now())
    candidates = _merge_candidates(current_candidates, history_candidates)
    first_ts = _parse_ts(candidates[0].get("ts")) if candidates else None
    history_path = _history_path_for(first_ts)
    history = _load_history(history_path)
    rows = [_simulate_candidate(candidate, history) for candidate in candidates]
    evaluated = [r for r in rows if r.get("status") == "EVALUATED"]
    not_eval = [r for r in rows if r.get("status") != "EVALUATED"]
    avg_primary = None
    if evaluated:
        avg_primary = round(sum(_to_float(r.get("primary_ret_pct")) for r in evaluated) / len(evaluated), 6)
    payload = {
        "ts": now,
        "status": "OK",
        "scope": "surge_ev_shadow_simulation",
        "source_candidates_json": str(src.get("_source_candidates_json") or CANDIDATES_JSON),
        "source_candidates_history_csv": str(CANDIDATES_HISTORY_CSV),
        "source_candidates_ts": src.get("ts", ""),
        "current_candidate_rows": len(current_candidates),
        "history_candidate_rows": len(history_candidates),
        "history_file": str(history_path),
        "candidate_rows": len(candidates),
        "evaluated_rows": len(evaluated),
        "not_evaluable_rows": len(not_eval),
        "avg_primary_ret_pct": avg_primary,
        "risk_contract": {
            "policy_change": False,
            "entry_approval_changed": False,
            "live_order_allowed": False,
            "entry_signal": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_route": False,
            "research_only": True,
            "must_not_dispatch": True,
            "purpose": "research shadow expected-value simulation only; no order route",
        },
        "simulation_params": {
            "micro_position_factor": MICRO_POSITION_FACTOR,
            "stop_loss_pct": STOP_LOSS_PCT,
            "primary_timebox_min": PRIMARY_TIMEBOX_MIN,
            "timebox_minutes": TIMEBOX_MINUTES,
            "min_slippage_bps_each_side": MIN_SLIPPAGE_BPS,
            "max_slippage_bps_each_side": MAX_SLIPPAGE_BPS,
        },
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    LEGACY_OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(LEGACY_OUT_CSV, rows)
    print(json.dumps({"status": "OK", "candidate_rows": len(candidates), "evaluated_rows": len(evaluated)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
