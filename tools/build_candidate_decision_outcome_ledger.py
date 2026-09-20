"""Build a read-only candidate decision/outcome ledger.

This observer joins candidate, decision, action, intraday price, and trade
artifacts so candidate decisions can be audited beyond one latest snapshot.
It does not create orders and does not change scores, gates, or policy.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER_DIR = ROOT / "paper"

CANDIDATES_CSV = LOG_DIR / "candidates_latest_data.with_final_score.csv"
ENTRY_LAYER_JSON = LOG_DIR / "entry_decision_layers_runtime_latest.json"
ENTRY_LAYER_CSV = LOG_DIR / "entry_decision_layers_runtime_latest.csv"
ENTRY_SIGNAL_CSV = LOG_DIR / "entry_signal_snapshot_latest.csv"
ACTION_QUEUE_CSV = LOG_DIR / "candidate_action_queue_latest.csv"
PENDING_JSON = LOG_DIR / "pending_entry_status_latest.json"
TRADES_CALC_CSV = PAPER_DIR / "trades_calc.csv"
CANDIDATE_PRICE_HISTORY_CSV = LOG_DIR / "candidate_price_history_latest.csv"

OUT_JSON = LOG_DIR / "candidate_decision_outcome_ledger_latest.json"
OUT_CSV = LOG_DIR / "candidate_decision_outcome_ledger_latest.csv"
HISTORY_CSV = LOG_DIR / "candidate_decision_outcome_ledger_history.csv"
HISTORY_JSONL = LOG_DIR / "candidate_decision_outcome_ledger_history.jsonl"


FIELDS = [
    "ledger_key",
    "generated_at",
    "decision_snapshot_ts",
    "code",
    "name",
    "candidate_date",
    "decision_type",
    "strategy_group",
    "decision_reason",
    "candidate_score",
    "rank_score",
    "risk_state",
    "execution_state",
    "source_membership",
    "v_accel_original",
    "normal_intraday_value_ratio",
    "normal_intraday_rechecked_v_accel",
    "normal_intraday_block_v_accel_min",
    "price_at_decision",
    "decision_price_ts",
    "ret_5m_pct",
    "ret_15m_pct",
    "ret_close_or_latest_pct",
    "ret_next_day_pct",
    "actual_trade_id",
    "actual_pnl_pct",
    "actual_entry_ts",
    "actual_exit_ts",
    "outcome_status",
    "outcome_gap",
    "verdict",
    "policy_effect",
    "policy_change_applied",
]


def _now_ts() -> str:
    return dt.datetime.now().replace(microsecond=0).isoformat()


def _today_ymd() -> str:
    return dt.datetime.now().strftime("%Y%m%d")


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def _write_csv(path: Path, rows: Iterable[Dict[str, Any]], fields: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _append_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _norm_code(value: Any) -> str:
    text = str(value or "").strip()
    if not text or text.lower() == "nan":
        return ""
    return text.zfill(6)[-6:]


def _to_float(value: Any) -> float | None:
    try:
        if value in ("", None):
            return None
        text = str(value).strip()
        if not text or text.lower() == "nan":
            return None
        return float(text)
    except Exception:
        return None


def _parse_ts(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y%m%d %H:%M:%S", "%Y%m%d"):
        try:
            return dt.datetime.strptime(text, fmt)
        except Exception:
            continue
    return None


def _index_by_code(rows: Iterable[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        code = _norm_code(row.get("code"))
        if code and code not in out:
            out[code] = row
    return out


def _all_by_code(rows: Iterable[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    out: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        code = _norm_code(row.get("code"))
        if code:
            out.setdefault(code, []).append(row)
    return out


def _classify_decision(row: Dict[str, str]) -> str:
    signal = str(row.get("signal") or "").strip().upper()
    reason = str(row.get("execution_reason") or row.get("reason") or "").strip().upper()
    if signal in {"BUY", "ENTRY", "READY"}:
        return "BUY"
    if "BLOCK" in reason or "CAP_" in reason:
        return "BLOCK"
    if signal == "HOLD":
        return "HOLD"
    return "WATCH"


def _strategy_group(row: Dict[str, str], action_row: Dict[str, str]) -> str:
    if str(row.get("is_surge") or action_row.get("surge_expectancy_buy_ready") or "").strip().lower() in {"1", "true", "yes"}:
        return "surge"
    reason = str(row.get("execution_reason") or row.get("reason") or action_row.get("reason") or "").lower()
    if "intraday" in reason:
        return "normal"
    if "surge" in reason or "lob" in reason:
        return "surge"
    if str(action_row.get("reason") or "").startswith("surge_"):
        return "surge"
    return "normal"


def _source_membership(*items: Tuple[str, bool]) -> str:
    return "|".join(name for name, present in items if present)


def _intraday_history_path() -> Path:
    return LOG_DIR / f"intraday_prices_history_{_today_ymd()}.csv"


def _price_points_by_code() -> Dict[str, List[Dict[str, Any]]]:
    points: Dict[str, List[Dict[str, Any]]] = {}
    for row in _read_csv(_intraday_history_path()):
        code = _norm_code(row.get("code"))
        ts = _parse_ts(row.get("ts"))
        price = _to_float(row.get("current_price") or row.get("current"))
        if not code or ts is None or price is None or price <= 0:
            continue
        points.setdefault(code, []).append({"ts": ts, "price": price})
    for rows in points.values():
        rows.sort(key=lambda x: x["ts"])
    return points


def _markout(points: List[Dict[str, Any]], decision_ts: dt.datetime | None) -> Dict[str, Any]:
    if not points:
        return {
            "price_at_decision": "",
            "decision_price_ts": "",
            "ret_5m_pct": "",
            "ret_15m_pct": "",
            "ret_close_or_latest_pct": "",
            "outcome_status": "NO_INTRADAY_MARKOUT_SOURCE",
            "outcome_gap": "missing_intraday_history",
        }
    if decision_ts is None:
        base = points[0]
        status = "INTRADAY_SERIES_AVAILABLE_NO_DECISION_TS"
        gap = "decision_timestamp_missing"
    else:
        before = [p for p in points if p["ts"] <= decision_ts]
        base = before[-1] if before else points[0]
        status = "INTRADAY_MARKOUT_PARTIAL"
        gap = "post_decision_points_missing" if points[-1]["ts"] <= base["ts"] else ""

    def ret_after(minutes: int) -> str:
        target = base["ts"] + dt.timedelta(minutes=minutes)
        future = [p for p in points if p["ts"] >= target]
        if not future:
            return ""
        return str(round((future[0]["price"] / base["price"] - 1.0) * 100.0, 6))

    latest_ret = round((points[-1]["price"] / base["price"] - 1.0) * 100.0, 6)
    return {
        "price_at_decision": base["price"],
        "decision_price_ts": base["ts"].isoformat(timespec="seconds"),
        "ret_5m_pct": ret_after(5),
        "ret_15m_pct": ret_after(15),
        "ret_close_or_latest_pct": str(latest_ret),
        "outcome_status": status,
        "outcome_gap": gap,
    }


def _date8(value: Any) -> str:
    text = str(value or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _note_field(note: str, field: str) -> str:
    prefix = f"{field}="
    for part in str(note or "").replace("|", ";").split(";"):
        text = part.strip()
        if text.startswith(prefix):
            return text[len(prefix):].strip()
    return ""


def _trades_by_code_and_signal(rows: Iterable[Dict[str, str]]) -> Dict[Tuple[str, str], Dict[str, str]]:
    out: Dict[Tuple[str, str], Dict[str, str]] = {}
    for row in rows:
        code = _norm_code(row.get("code"))
        signal_date = _date8(_note_field(str(row.get("note") or ""), "signal_date"))
        if not code or not signal_date:
            continue
        key = (code, signal_date)
        prev = out.get(key)
        if prev is None or str(row.get("exit_ts") or "") > str(prev.get("exit_ts") or ""):
            out[key] = row
    return out


def _verdict(decision_type: str, mark: Dict[str, Any], trade: Dict[str, str]) -> str:
    pnl = _to_float(trade.get("net_ret")) if trade else None
    latest_ret = _to_float(mark.get("ret_close_or_latest_pct"))
    if pnl is not None:
        return "TRADED_WIN" if pnl > 0 else "TRADED_LOSS"
    if latest_ret is None:
        return "OUTCOME_PENDING"
    if decision_type == "BLOCK" and latest_ret > 0.3:
        return "POSSIBLE_MISSED_UPSIDE"
    if decision_type == "BLOCK" and latest_ret < -0.3:
        return "POSSIBLE_AVOIDED_LOSS"
    if decision_type in {"HOLD", "WATCH"}:
        return "OBSERVE_ONLY_NEUTRAL" if abs(latest_ret) <= 0.3 else "OBSERVE_ONLY_MOVED"
    return "OUTCOME_REVIEW"


def _history_rows_by_key(path: Path) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        return {}
    rows: Dict[str, Dict[str, str]] = {}
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                key = str(row.get("ledger_key") or "").strip()
                if key:
                    rows[key] = row
    except Exception:
        return {}
    return rows


def main() -> int:
    generated_at = _now_ts()
    entry_meta = _read_json(ENTRY_LAYER_JSON)
    pending = _read_json(PENDING_JSON)
    decision_snapshot_ts = str(entry_meta.get("generated_at") or pending.get("generated_at") or generated_at)
    decision_ts = _parse_ts(decision_snapshot_ts)

    candidates = _index_by_code(_read_csv(CANDIDATES_CSV))
    entry_layers = _index_by_code(_read_csv(ENTRY_LAYER_CSV))
    entry_signals = _index_by_code(_read_csv(ENTRY_SIGNAL_CSV))
    action_by_code = _all_by_code(_read_csv(ACTION_QUEUE_CSV))
    trades_by_signal = _trades_by_code_and_signal(_read_csv(TRADES_CALC_CSV))
    price_points = _price_points_by_code()

    codes = set(candidates) | set(entry_layers) | set(entry_signals) | set(action_by_code)
    rows: List[Dict[str, Any]] = []
    for code in sorted(codes):
        candidate = candidates.get(code, {})
        entry = entry_layers.get(code) or entry_signals.get(code) or {}
        action_rows = action_by_code.get(code, [])
        action = action_rows[0] if action_rows else {}
        signal_date_candidates = [
            _date8(entry.get("signal_date")),
            _date8(candidate.get("date")),
            _date8(candidate.get("date_yyyymmdd")),
            _date8(action.get("signal_date")),
        ]
        trade = {}
        for signal_date in signal_date_candidates:
            if signal_date and (code, signal_date) in trades_by_signal:
                trade = trades_by_signal[(code, signal_date)]
                break
        decision_type = _classify_decision(entry or action)
        reason = (
            entry.get("execution_reason")
            or entry.get("reason")
            or action.get("reason")
            or action.get("surge_intent_reason")
            or ""
        )
        mark = _markout(price_points.get(code, []), decision_ts)
        row = {
            "generated_at": generated_at,
            "decision_snapshot_ts": decision_snapshot_ts,
            "code": code,
            "name": entry.get("name") or candidate.get("name") or action.get("name") or "",
            "candidate_date": candidate.get("date") or candidate.get("date_yyyymmdd") or entry.get("signal_date") or "",
            "decision_type": decision_type,
            "strategy_group": _strategy_group(entry, action),
            "decision_reason": reason,
            "candidate_score": candidate.get("final_score") or action.get("final_score") or "",
            "rank_score": entry.get("rank_score") or "",
            "risk_state": entry.get("risk_reason") or entry.get("entry_gate_reason") or "",
            "execution_state": entry.get("execution_reason") or reason,
            "source_membership": _source_membership(
                ("candidate", bool(candidate)),
                ("entry_layer", bool(entry_layers.get(code))),
                ("entry_signal", bool(entry_signals.get(code))),
                ("action_queue", bool(action_rows)),
                ("trade", bool(trade)),
                ("intraday_history", bool(price_points.get(code))),
            ),
            "v_accel_original": candidate.get("v_accel") or "",
            "normal_intraday_value_ratio": entry.get("normal_intraday_value_ratio") or "",
            "normal_intraday_rechecked_v_accel": entry.get("normal_intraday_rechecked_v_accel") or "",
            "normal_intraday_block_v_accel_min": entry.get("normal_intraday_block_v_accel_min") or "",
            "price_at_decision": mark["price_at_decision"],
            "decision_price_ts": mark["decision_price_ts"],
            "ret_5m_pct": mark["ret_5m_pct"],
            "ret_15m_pct": mark["ret_15m_pct"],
            "ret_close_or_latest_pct": mark["ret_close_or_latest_pct"],
            "ret_next_day_pct": "",
            "actual_trade_id": trade.get("trade_id") or "",
            "actual_pnl_pct": str(round(float(trade.get("net_ret")) * 100.0, 6)) if _to_float(trade.get("net_ret")) is not None else "",
            "actual_entry_ts": trade.get("entry_ts") or "",
            "actual_exit_ts": trade.get("exit_ts") or "",
            "outcome_status": mark["outcome_status"],
            "outcome_gap": mark["outcome_gap"],
            "policy_effect": "false",
            "policy_change_applied": "false",
        }
        row["verdict"] = _verdict(decision_type, mark, trade)
        row["ledger_key"] = "|".join([
            decision_snapshot_ts,
            code,
            row["decision_type"],
            str(row["decision_reason"])[:120],
        ])
        rows.append(row)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    _write_csv(OUT_CSV, rows, FIELDS)
    dated_csv = LOG_DIR / f"candidate_decision_outcome_ledger_{_today_ymd()}_{dt.datetime.now().strftime('%H%M%S')}.csv"
    _write_csv(dated_csv, rows, FIELDS)

    existing = _history_rows_by_key(HISTORY_CSV)
    new_rows = [row for row in rows if str(row.get("ledger_key") or "") not in existing]
    updated_rows = [row for row in rows if str(row.get("ledger_key") or "") in existing]
    merged: Dict[str, Dict[str, Any]] = {key: value for key, value in existing.items()}
    for row in rows:
        merged[str(row.get("ledger_key") or "")] = row
    merged_rows = [row for key, row in merged.items() if key]
    _write_csv(HISTORY_CSV, merged_rows, FIELDS)
    HISTORY_JSONL.write_text("", encoding="utf-8")
    _append_jsonl(HISTORY_JSONL, merged_rows)

    counts: Dict[str, int] = {}
    verdict_counts: Dict[str, int] = {}
    for row in rows:
        counts[row["decision_type"]] = counts.get(row["decision_type"], 0) + 1
        verdict_counts[row["verdict"]] = verdict_counts.get(row["verdict"], 0) + 1
    payload = {
        "generated_at": generated_at,
        "status": "PASS",
        "schema_version": "candidate_decision_outcome_ledger_v1",
        "rows": len(rows),
        "history_appended_rows": len(new_rows),
        "history_updated_rows": len(updated_rows),
        "decision_type_counts": counts,
        "verdict_counts": verdict_counts,
        "source_files": {
            "candidates": str(CANDIDATES_CSV),
            "entry_layers": str(ENTRY_LAYER_CSV),
            "entry_signal": str(ENTRY_SIGNAL_CSV),
            "action_queue": str(ACTION_QUEUE_CSV),
            "pending": str(PENDING_JSON),
            "intraday_history": str(_intraday_history_path()),
            "trades_calc": str(TRADES_CALC_CSV),
        },
        "outputs": {
            "latest_csv": str(OUT_CSV),
            "latest_json": str(OUT_JSON),
            "dated_csv": str(dated_csv),
            "history_csv": str(HISTORY_CSV),
            "history_jsonl": str(HISTORY_JSONL),
        },
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "notes": [
            "Read-only ledger; no orders, fills, scores, gates, or policy are modified.",
            "Rows with OUTCOME_PENDING need later intraday or next-day price observations.",
        ],
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": "PASS", "rows": len(rows), "history_appended_rows": len(new_rows), "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
