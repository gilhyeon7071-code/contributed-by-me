import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SIMULATION_JSON = LOG_DIR / "surge_ev_shadow_simulation_latest.json"
PRICE_HISTORY_CSV = LOG_DIR / f"intraday_prices_history_{datetime.now().strftime('%Y%m%d')}.csv"
OUT_JSON = LOG_DIR / "surge_ev_bucket_decomposition_latest.json"
OUT_CSV = LOG_DIR / "surge_ev_bucket_decomposition_latest.csv"


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


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _load_price_history(path: Path) -> Dict[str, List[Tuple[datetime, float]]]:
    out: Dict[str, List[Tuple[datetime, float]]] = {}
    if not path.exists() or path.stat().st_size <= 5:
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


def _change_bucket(value: float) -> str:
    if value < 0.08:
        return "<8%"
    if value < 0.12:
        return "8-12%"
    if value < 0.16:
        return "12-16%"
    if value < 0.24:
        return "16-24%"
    return ">=24%"


def _spread_bucket(value: float) -> str:
    if value <= 10.0:
        return "<=10bps"
    if value <= 15.0:
        return "10-15bps"
    if value <= 20.0:
        return "15-20bps"
    if value <= 30.0:
        return "20-30bps"
    return ">30bps"


def _rvol_bucket(value: float) -> str:
    if value < 1.5:
        return "<1.5"
    if value < 2.5:
        return "1.5-2.5"
    if value < 3.0:
        return "2.5-3.0"
    if value <= 4.2:
        return "3.0-4.2"
    return ">4.2"


def _lob_delay_bucket(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "UNKNOWN"
    delay_sec = _to_float(text, -1.0)
    if delay_sec < 0:
        return "UNKNOWN"
    if delay_sec < 60.0:
        return "<60s"
    if delay_sec < 120.0:
        return "60-120s"
    if delay_sec < 300.0:
        return "120-300s"
    return ">=300s"


def _session_bucket(status: Any) -> str:
    text = str(status or "")
    if text == "EVALUATED":
        return "REGULAR_PRIMARY_EVALUATED"
    if text == "NOT_EVALUABLE_MARKET_CLOSE_BEFORE_PRIMARY_TIMEBOX":
        return "LATE_SESSION_MARKET_CLOSE_BEFORE_PRIMARY_TIMEBOX"
    if text == "NOT_EVALUABLE_NO_PRICE_AT_OR_AFTER_SIGNAL":
        return "PRICE_HISTORY_COVERAGE_GAP"
    if text == "NOT_EVALUABLE_NO_REGULAR_PRICE_AT_OR_AFTER_SIGNAL":
        return "REGULAR_PRICE_HISTORY_COVERAGE_GAP"
    if text == "NOT_EVALUABLE_SIGNAL_AFTER_MARKET_CLOSE":
        return "AFTER_MARKET_SIGNAL_NOT_EVALUABLE"
    if text == "NOT_EVALUABLE_WAITING_PRIMARY_TIMEBOX":
        return "REGULAR_WAITING_PRIMARY_TIMEBOX"
    return "OTHER_NOT_EVALUABLE"


def _cause_flags(row: Dict[str, Any]) -> List[str]:
    flags: List[str] = []
    entry_drawdown = _to_float(row.get("entry_drawdown_from_high_pct"))
    intraday_high_drawdown = _to_float(row.get("intraday_high_drawdown_pct"))
    low_rebound = _to_float(row.get("intraday_low_rebound_pct"))
    range_position = _to_float(row.get("intraday_range_position_pct"))
    rvol20 = _to_float(row.get("rvol20"))
    spread_bps = _to_float(row.get("spread_bps"))
    ask_depth_levels = _to_float(row.get("ask_depth_levels"), -1.0)
    max_adverse = _to_float(row.get("max_adverse_pct"))
    max_favorable = _to_float(row.get("max_favorable_pct"))
    lob_confirm_delay = _to_float(row.get("lob_confirm_delay_sec"))
    if entry_drawdown >= -1.5:
        flags.append("NEAR_HIGH_CHASE")
    if low_rebound >= 0.05:
        flags.append("LOW_RECOVERY_GE5PCT")
    if range_position >= 0.5:
        flags.append("RANGE_UPPER_HALF")
    if intraday_high_drawdown <= -0.025 and range_position >= 0.5:
        flags.append("HIGH_REJECTION_BUT_RANGE_UPPER_HALF")
    if rvol20 < 2.5:
        flags.append("WEAK_RVOL")
    if spread_bps > 20.0:
        flags.append("WIDE_SPREAD")
    if ask_depth_levels >= 0 and ask_depth_levels < 3.0:
        flags.append("SHALLOW_ASK_DEPTH")
    if max_adverse <= -1.2:
        flags.append("FAST_ADVERSE_MOVE")
    if max_favorable <= 0.5:
        flags.append("LOW_FOLLOWTHROUGH")
    if lob_confirm_delay >= 120.0:
        flags.append("LOB_CONFIRM_DELAY_GE_120S")
    if str(row.get("exit_reason") or "") == "STOP_LOSS_HIT":
        flags.append("STOP_HIT")
    if not flags:
        flags.append("NO_SIMPLE_CAUSE_FLAG")
    return flags


def _enrich(row: Dict[str, Any], history: Dict[str, List[Tuple[datetime, float]]]) -> Dict[str, Any]:
    code = str(row.get("code") or "").zfill(6)
    signal_ts = _parse_ts(row.get("signal_ts"))
    entry_ts = _parse_ts(row.get("entry_ts"))
    series = history.get(code, [])
    delay_min = ""
    signal_to_entry_pct = ""
    entry_drawdown_from_high_pct = ""
    if signal_ts and entry_ts:
        delay_min = round((entry_ts - signal_ts).total_seconds() / 60.0, 6)
        signal_point = _first_at_or_after(series, signal_ts)
        entry_px = _to_float(row.get("entry_price"))
        if signal_point and signal_point[1] > 0 and entry_px > 0:
            signal_to_entry_pct = round((entry_px / signal_point[1] - 1.0) * 100.0, 6)
        upto_entry = [px for ts, px in series if ts <= entry_ts]
        if entry_px > 0 and upto_entry:
            high_to_entry = max(upto_entry)
            if high_to_entry > 0:
                entry_drawdown_from_high_pct = round((entry_px / high_to_entry - 1.0) * 100.0, 6)
    change_pct = _to_float(row.get("change_pct"))
    spread_bps = _to_float(row.get("spread_bps"))
    rvol20 = _to_float(row.get("rvol20"))
    enriched = {
        "code": code,
        "source": row.get("source", ""),
        "tier": row.get("tier", ""),
        "detected_surge_type": row.get("detected_surge_type", ""),
        "signal_ts": row.get("signal_ts", ""),
        "first_no_lob_ts": row.get("first_no_lob_ts", ""),
        "lob_confirm_ts": row.get("lob_confirm_ts", ""),
        "lob_confirm_delay_sec": row.get("lob_confirm_delay_sec", ""),
        "lob_delay_bucket": _lob_delay_bucket(row.get("lob_confirm_delay_sec")),
        "entry_ts": row.get("entry_ts", ""),
        "required_primary_exit_after": row.get("required_primary_exit_after", ""),
        "market_close_ts": row.get("market_close_ts", ""),
        "latest_history_ts": row.get("latest_history_ts", row.get("last_history_ts", "")),
        "close_markout_ret_pct": row.get("close_markout_ret_pct", ""),
        "status": row.get("status", ""),
        "session_bucket": _session_bucket(row.get("status", "")),
        "exit_reason": row.get("exit_reason", ""),
        "primary_ret_pct": row.get("primary_ret_pct", ""),
        "micro_position_ret_pct": row.get("micro_position_ret_pct", ""),
        "max_favorable_pct": row.get("max_favorable_pct", ""),
        "max_adverse_pct": row.get("max_adverse_pct", ""),
        "surge_score_final": row.get("surge_score_final", ""),
        "change_pct": change_pct,
        "day_range_pct": row.get("day_range_pct", ""),
        "intraday_high_drawdown_pct": row.get("intraday_high_drawdown_pct", ""),
        "intraday_low_rebound_pct": row.get("intraday_low_rebound_pct", ""),
        "intraday_range_position_pct": row.get("intraday_range_position_pct", ""),
        "change_bucket": _change_bucket(change_pct),
        "rvol20": rvol20,
        "rvol_bucket": _rvol_bucket(rvol20),
        "lob_status": row.get("lob_status", ""),
        "spread_bps": spread_bps,
        "spread_bucket": _spread_bucket(spread_bps),
        "ask_depth_levels": row.get("ask_depth_levels", ""),
        "delay_min": delay_min,
        "signal_to_entry_pct": signal_to_entry_pct,
        "entry_drawdown_from_high_pct": entry_drawdown_from_high_pct,
    }
    enriched["cause_flags"] = "|".join(_cause_flags(enriched))
    return enriched


def _summarize(rows: List[Dict[str, Any]], group_fields: List[str]) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, ...], List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(str(row.get(field, "")) for field in group_fields)].append(row)
    out: List[Dict[str, Any]] = []
    for key, items in grouped.items():
        evaluated_items = [row for row in items if str(row.get("status") or "") == "EVALUATED"]
        returns = [_to_float(row.get("primary_ret_pct")) for row in evaluated_items]
        stop_hits = [row for row in evaluated_items if str(row.get("exit_reason") or "") == "STOP_LOSS_HIT"]
        positives = [value for value in returns if value > 0.0]
        record = {field: key[idx] for idx, field in enumerate(group_fields)}
        record.update({
            "n": len(items),
            "evaluated_n": len(evaluated_items),
            "avg_primary_ret_pct": round(sum(returns) / len(returns), 6) if returns else None,
            "median_primary_ret_pct": round(median(returns), 6) if returns else None,
            "positive_rows": len(positives),
            "stop_hits": len(stop_hits),
            "codes": ",".join(sorted({str(row.get("code") or "") for row in items})),
        })
        out.append(record)
    return sorted(out, key=lambda x: (-int(x.get("n") or 0), str(x)))


def _cause_summary(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    counter: Counter[str] = Counter()
    for row in rows:
        for flag in str(row.get("cause_flags") or "").split("|"):
            if flag:
                counter[flag] += 1
    return [{"cause": key, "count": int(value)} for key, value in counter.most_common()]


def _pending_delay_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        if row.get("lob_delay_bucket") == "UNKNOWN":
            continue
        if str(row.get("status") or "") == "EVALUATED":
            continue
        out.append({
            "code": row.get("code", ""),
            "source": row.get("source", ""),
            "signal_ts": row.get("signal_ts", ""),
            "lob_delay_bucket": row.get("lob_delay_bucket", ""),
            "lob_confirm_delay_sec": row.get("lob_confirm_delay_sec", ""),
            "status": row.get("status", ""),
            "required_primary_exit_after": row.get("required_primary_exit_after", ""),
            "market_close_ts": row.get("market_close_ts", ""),
            "latest_history_ts": row.get("latest_history_ts", ""),
            "close_markout_ret_pct": row.get("close_markout_ret_pct", ""),
            "cause_flags": row.get("cause_flags", ""),
        })
    return sorted(out, key=lambda x: (str(x.get("status", "")), str(x.get("code", ""))))


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "code",
        "source",
        "tier",
        "detected_surge_type",
        "signal_ts",
        "first_no_lob_ts",
        "lob_confirm_ts",
        "lob_confirm_delay_sec",
        "lob_delay_bucket",
        "entry_ts",
        "required_primary_exit_after",
        "market_close_ts",
        "latest_history_ts",
        "close_markout_ret_pct",
        "status",
        "session_bucket",
        "exit_reason",
        "primary_ret_pct",
        "max_favorable_pct",
        "max_adverse_pct",
        "change_pct",
        "day_range_pct",
        "intraday_high_drawdown_pct",
        "intraday_low_rebound_pct",
        "intraday_range_position_pct",
        "change_bucket",
        "rvol20",
        "rvol_bucket",
        "lob_status",
        "spread_bps",
        "spread_bucket",
        "ask_depth_levels",
        "delay_min",
        "signal_to_entry_pct",
        "entry_drawdown_from_high_pct",
        "cause_flags",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def main() -> int:
    now = datetime.now().isoformat(timespec="seconds")
    simulation = _load_json(SIMULATION_JSON)
    history = _load_price_history(PRICE_HISTORY_CSV)
    rows = [_enrich(row, history) for row in simulation.get("rows") or []]
    evaluated = [row for row in rows if str(row.get("status") or "") == "EVALUATED"]
    watch_condition_rows = [
        row for row in evaluated
        if row.get("source") == "SCORE_RVOL_LOB_CONFIRMED_CLEAN"
        and row.get("change_bucket") == "12-16%"
        and _to_float(row.get("spread_bps")) <= 15.0
        and 3.0 <= _to_float(row.get("rvol20")) <= 4.2
    ]
    no_lob_risk_rows = [
        row for row in evaluated
        if row.get("source") == "NO_LOB_RECHECK_CLEAN"
        and row.get("change_bucket") == "12-16%"
    ]
    payload = {
        "ts": now,
        "status": "OK",
        "scope": "surge_ev_bucket_decomposition",
        "source_simulation_json": str(SIMULATION_JSON),
        "source_price_history_csv": str(PRICE_HISTORY_CSV),
        "simulation_ts": simulation.get("ts", ""),
        "candidate_rows": len(rows),
        "evaluated_rows": len(evaluated),
        "pending_rows": len(rows) - len(evaluated),
        "gradebook_contract": {
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
        "summary": {
            "exit_reason_counts": [{"exit_reason": k, "count": int(v)} for k, v in Counter(row.get("exit_reason", "") for row in evaluated).most_common()],
            "session_buckets_all_rows": _summarize(rows, ["session_bucket"]),
            "session_source_buckets_all_rows": _summarize(rows, ["session_bucket", "source"]),
            "source_change_buckets": _summarize(evaluated, ["source", "change_bucket"]),
            "source_change_spread_rvol_buckets": _summarize(evaluated, ["source", "change_bucket", "spread_bucket", "rvol_bucket"]),
            "lob_delay_buckets": _summarize(evaluated, ["lob_delay_bucket"]),
            "source_lob_delay_buckets": _summarize(evaluated, ["source", "lob_delay_bucket"]),
            "source_change_lob_delay_buckets": _summarize(evaluated, ["source", "change_bucket", "lob_delay_bucket"]),
            "cause_lob_delay_buckets": _summarize(evaluated, ["lob_delay_bucket", "change_bucket", "rvol_bucket", "spread_bucket"]),
            "watch_condition": _summarize(watch_condition_rows, ["source", "change_bucket", "spread_bucket", "rvol_bucket"]),
            "no_lob_12_16_risk": _summarize(no_lob_risk_rows, ["source", "change_bucket"]),
            "no_lob_12_16_cause_counts": _cause_summary(no_lob_risk_rows),
            "watch_condition_cause_counts": _cause_summary(watch_condition_rows),
            "pending_delay_rows": _pending_delay_rows(rows),
        },
        "interpretation_flags": {
            "watch_condition_research_only": True,
            "watch_condition_sample_too_small_for_policy_change": len(watch_condition_rows) < 30,
            "no_lob_recheck_12_16_negative": bool(no_lob_risk_rows and sum(_to_float(row.get("primary_ret_pct")) for row in no_lob_risk_rows) < 0.0),
        },
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    print(json.dumps({
        "status": "OK",
        "evaluated_rows": len(evaluated),
        "watch_condition_rows": len(watch_condition_rows),
        "no_lob_12_16_rows": len(no_lob_risk_rows),
        "out_json": str(OUT_JSON),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
