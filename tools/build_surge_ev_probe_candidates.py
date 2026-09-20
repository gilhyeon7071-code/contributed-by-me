import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
RECHECK_CSV = LOG_DIR / "surge_no_lob_recheck_queue_latest.csv"
SCORE_RVOL_CSV = LOG_DIR / "surge_score_rvol_conditional_recheck_queue_latest.csv"
OBSERVE_JSON = LOG_DIR / "surge_wait_lob_hoga_observe_latest.json"
OUT_JSON = LOG_DIR / "surge_ev_shadow_candidates_latest.json"
OUT_CSV = LOG_DIR / "surge_ev_shadow_candidates_latest.csv"
HISTORY_CSV = LOG_DIR / "surge_ev_shadow_candidates_history.csv"
LEGACY_OUT_JSON = LOG_DIR / "surge_ev_probe_candidates_latest.json"
LEGACY_OUT_CSV = LOG_DIR / "surge_ev_probe_candidates_latest.csv"
REGULAR_MARKET_CLOSE_HOUR = 15
REGULAR_MARKET_CLOSE_MINUTE = 30

RESOLVABLE_BLOCKERS = {"NO_LOB_BLOCK"}
HARD_BLOCKERS = {
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
    "KRX_ADMIN",
    "KRX_WARNING",
    "KRX_RISK",
    "KRX_CAUTION",
    "TRADING_VALUE_BLOCK",
    "TRADING_VALUE_FLOOR",
    "LOW_TRADING_VALUE",
    "NEWS_NEGATIVE",
    "NEWS_IMPLICATION_BLOCK",
    "FEATURE_BLOCK",
    "FEATURE_MISSING_BLOCK",
    "INSUFFICIENT_FEATURES",
}
MICRO_PROBE_WATCH_BLOCKERS = {
    "ENTRY_CHANGE_BLOCK",
    "ENTRY_ATR_CAP",
    "HIGH_REJECTION_ENTRY_BLOCK",
    "RVOL_OVERHEAT_BLOCK",
    "SCORE_RVOL_OVERHEAT_BLOCK",
}
POLICY_HARD_BLOCKERS = HARD_BLOCKERS - MICRO_PROBE_WATCH_BLOCKERS


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except Exception:
        return default


def _to_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _parse_ts(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value or "").strip())
    except Exception:
        return None


def _is_regular_session_signal(value: Any) -> bool:
    ts = _parse_ts(value)
    if ts is None:
        return False
    close_ts = ts.replace(hour=REGULAR_MARKET_CLOSE_HOUR, minute=REGULAR_MARKET_CLOSE_MINUTE, second=0, microsecond=0)
    return ts <= close_ts


def _reason_keys(*values: Any) -> Set[str]:
    keys: Set[str] = set()
    for value in values:
        for part in str(value or "").split("|"):
            part = part.strip()
            if part:
                keys.add(part.split(":", 1)[0].strip())
    return keys


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists() or path.stat().st_size <= 5:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        return list(csv.DictReader(fp))


def _load_observe() -> Dict[str, Any]:
    if not OBSERVE_JSON.exists():
        return {}
    return json.loads(OBSERVE_JSON.read_text(encoding="utf-8-sig"))


def _remaining_hard(row: Dict[str, Any]) -> List[str]:
    keys = _reason_keys(row.get("exclude_reasons"), row.get("entry_reason"), row.get("paper_probe_block_reasons"))
    return sorted(k for k in keys if k in HARD_BLOCKERS and k not in RESOLVABLE_BLOCKERS)


def _remaining_policy_hard(row: Dict[str, Any]) -> List[str]:
    keys = _reason_keys(row.get("exclude_reasons"), row.get("entry_reason"), row.get("paper_probe_block_reasons"))
    return sorted(k for k in keys if k in POLICY_HARD_BLOCKERS and k not in RESOLVABLE_BLOCKERS)


def _candidate_from_no_lob(row: Dict[str, Any]) -> Dict[str, Any] | None:
    remaining = _remaining_hard(row)
    if remaining:
        return None
    if not _to_bool(row.get("lob_available")):
        return None
    if str(row.get("lob_status") or "").upper() != "OK":
        return None
    spread_bps = _to_float(row.get("spread_bps"), 999999.0)
    if spread_bps > 40.0:
        return None
    score = _to_float(row.get("surge_score_final"))
    rvol20 = _to_float(row.get("rvol20"))
    change_pct = _to_float(row.get("change_pct"))
    return {
        "tier": "EV_SHADOW_CANDIDATE_ONLY",
        "source": "NO_LOB_RECHECK_CLEAN",
        "code": str(row.get("code") or "").zfill(6),
        "ts": row.get("ts", ""),
        "first_no_lob_ts": row.get("first_no_lob_ts", row.get("ts", "")),
        "lob_confirm_ts": row.get("lob_confirm_ts", ""),
        "lob_confirm_delay_sec": _to_float(row.get("lob_confirm_delay_sec")),
        "detected_surge_type": row.get("detected_surge_type", ""),
        "surge_score_final": round(score, 6),
        "change_pct": round(change_pct, 6),
        "day_range_pct": _to_float(row.get("day_range_pct")),
        "intraday_high_drawdown_pct": _to_float(row.get("intraday_high_drawdown_pct")),
        "intraday_low_rebound_pct": _to_float(row.get("intraday_low_rebound_pct")),
        "intraday_range_position_pct": _to_float(row.get("intraday_range_position_pct")),
        "rvol20": round(rvol20, 6),
        "trading_value": _to_float(row.get("trading_value")),
        "lob_status": row.get("lob_status", ""),
        "spread_bps": round(spread_bps, 6),
        "ask_depth_levels": _to_float(row.get("ask_depth_levels"), -1.0),
        "resolved_blockers": "NO_LOB_BLOCK",
        "remaining_hard_blockers": "",
        "risk_budget_profile": "research_shadow_only_no_order",
        "suggested_position_risk": "not_applicable_no_order_route",
        "suggested_stop": "simulation_only_no_order_route",
        "suggested_time_limit": "simulation_only_no_order_route",
        "entry_approval_changed": False,
        "policy_change": False,
        "entry_signal": False,
        "live_order_allowed": False,
        "research_only": True,
        "must_not_dispatch": True,
        "paper_order_route": False,
        "broker_order_route": False,
        "trading_route": False,
        "note": "Research shadow candidate only; not paper/live entry approval.",
    }


def _candidate_from_micro_probe_watch(row: Dict[str, Any]) -> Dict[str, Any] | None:
    keys = _reason_keys(row.get("exclude_reasons"), row.get("entry_reason"), row.get("paper_probe_block_reasons"))
    watch_blockers = sorted(k for k in keys if k in MICRO_PROBE_WATCH_BLOCKERS)
    if not watch_blockers:
        return None
    if _remaining_policy_hard(row):
        return None
    if not _to_bool(row.get("lob_available")):
        return None
    if str(row.get("lob_status") or "").upper() != "OK":
        return None
    spread_bps = _to_float(row.get("spread_bps"), 999999.0)
    if spread_bps > 40.0:
        return None
    score = _to_float(row.get("surge_score_final"))
    rvol20 = _to_float(row.get("rvol20"))
    change_pct = _to_float(row.get("change_pct"))
    return {
        "tier": "EV_MICRO_PROBE_WATCH_ONLY",
        "source": "SURGE_BLOCKER_LOB_CONFIRMED_WATCH",
        "code": str(row.get("code") or "").zfill(6),
        "ts": row.get("ts", ""),
        "first_no_lob_ts": row.get("first_no_lob_ts", row.get("ts", "")),
        "lob_confirm_ts": row.get("lob_confirm_ts", ""),
        "lob_confirm_delay_sec": _to_float(row.get("lob_confirm_delay_sec")),
        "detected_surge_type": row.get("detected_surge_type", ""),
        "surge_score_final": round(score, 6),
        "change_pct": round(change_pct, 6),
        "day_range_pct": _to_float(row.get("day_range_pct")),
        "intraday_high_drawdown_pct": _to_float(row.get("intraday_high_drawdown_pct")),
        "intraday_low_rebound_pct": _to_float(row.get("intraday_low_rebound_pct")),
        "intraday_range_position_pct": _to_float(row.get("intraday_range_position_pct")),
        "rvol20": round(rvol20, 6),
        "trading_value": _to_float(row.get("trading_value")),
        "lob_status": row.get("lob_status", ""),
        "spread_bps": round(spread_bps, 6),
        "ask_depth_levels": _to_float(row.get("ask_depth_levels"), -1.0),
        "resolved_blockers": "NO_LOB_BLOCK",
        "remaining_hard_blockers": "|".join(watch_blockers),
        "risk_budget_profile": "research_micro_probe_watch_no_order",
        "suggested_position_risk": "readiness_contract_only_no_order_route",
        "suggested_stop": "readiness_contract_only_no_order_route",
        "suggested_time_limit": "readiness_contract_only_no_order_route",
        "entry_approval_changed": False,
        "policy_change": False,
        "entry_signal": False,
        "live_order_allowed": False,
        "research_only": True,
        "must_not_dispatch": True,
        "paper_order_route": False,
        "broker_order_route": False,
        "trading_route": False,
        "note": "Research micro-probe watch only; not paper/live entry approval.",
    }


def _candidate_from_score_rvol(row: Dict[str, Any]) -> Dict[str, Any] | None:
    remaining = str(row.get("remaining_hard_blockers") or "").strip()
    if remaining:
        return None
    if not _to_bool(row.get("lob_available")):
        return None
    if str(row.get("lob_status") or "").upper() != "OK":
        return None
    return {
        "tier": "EV_SCORE_RVOL_SHADOW_CANDIDATE_ONLY",
        "source": "SCORE_RVOL_LOB_CONFIRMED_CLEAN",
        "code": str(row.get("code") or "").zfill(6),
        "ts": row.get("ts", ""),
        "first_no_lob_ts": row.get("first_no_lob_ts", row.get("ts", "")),
        "lob_confirm_ts": row.get("lob_confirm_ts", ""),
        "lob_confirm_delay_sec": _to_float(row.get("lob_confirm_delay_sec")),
        "detected_surge_type": row.get("detected_surge_type", ""),
        "surge_score_final": _to_float(row.get("surge_score_final")),
        "change_pct": _to_float(row.get("change_pct")),
        "day_range_pct": _to_float(row.get("day_range_pct")),
        "intraday_high_drawdown_pct": _to_float(row.get("intraday_high_drawdown_pct")),
        "intraday_low_rebound_pct": _to_float(row.get("intraday_low_rebound_pct")),
        "intraday_range_position_pct": _to_float(row.get("intraday_range_position_pct")),
        "rvol20": _to_float(row.get("rvol20")),
        "trading_value": _to_float(row.get("trading_value")),
        "lob_status": row.get("lob_status", ""),
        "spread_bps": _to_float(row.get("spread_bps")),
        "ask_depth_levels": _to_float(row.get("ask_depth_levels"), -1.0),
        "resolved_blockers": row.get("resolved_blockers", ""),
        "remaining_hard_blockers": "",
        "risk_budget_profile": "research_shadow_only_no_order",
        "suggested_position_risk": "not_applicable_no_order_route",
        "suggested_stop": "simulation_only_no_order_route",
        "suggested_time_limit": "simulation_only_no_order_route",
        "entry_approval_changed": False,
        "policy_change": False,
        "entry_signal": False,
        "live_order_allowed": False,
        "research_only": True,
        "must_not_dispatch": True,
        "paper_order_route": False,
        "broker_order_route": False,
        "trading_route": False,
        "note": "Research shadow candidate only; not paper/live entry approval.",
    }


def _dedupe(candidates: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best: Dict[str, Dict[str, Any]] = {}
    tier_rank = {
        "EV_SHADOW_CANDIDATE_ONLY": 1,
        "EV_SCORE_RVOL_SHADOW_CANDIDATE_ONLY": 2,
        "EV_MICRO_PROBE_WATCH_ONLY": 3,
    }
    for item in candidates:
        code = str(item.get("code") or "")
        old = best.get(code)
        if old is None or tier_rank.get(str(item.get("tier")), 99) < tier_rank.get(str(old.get("tier")), 99):
            best[code] = item
    return sorted(best.values(), key=lambda x: (-_to_float(x.get("surge_score_final")), str(x.get("code"))))


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "tier",
        "source",
        "code",
        "ts",
        "first_no_lob_ts",
        "lob_confirm_ts",
        "lob_confirm_delay_sec",
        "detected_surge_type",
        "surge_score_final",
        "change_pct",
        "day_range_pct",
        "intraday_high_drawdown_pct",
        "intraday_low_rebound_pct",
        "intraday_range_position_pct",
        "rvol20",
        "trading_value",
        "lob_status",
        "spread_bps",
        "ask_depth_levels",
        "resolved_blockers",
        "remaining_hard_blockers",
        "risk_budget_profile",
        "suggested_position_risk",
        "suggested_stop",
        "suggested_time_limit",
        "entry_approval_changed",
        "policy_change",
        "entry_signal",
        "live_order_allowed",
        "research_only",
        "must_not_dispatch",
        "paper_order_route",
        "broker_order_route",
        "trading_route",
        "note",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fields})


def _history_fields() -> List[str]:
    return [
        "snapshot_ts",
        "tier",
        "source",
        "code",
        "ts",
        "first_no_lob_ts",
        "lob_confirm_ts",
        "lob_confirm_delay_sec",
        "detected_surge_type",
        "surge_score_final",
        "change_pct",
        "day_range_pct",
        "intraday_high_drawdown_pct",
        "intraday_low_rebound_pct",
        "intraday_range_position_pct",
        "rvol20",
        "trading_value",
        "lob_status",
        "spread_bps",
        "ask_depth_levels",
        "resolved_blockers",
        "remaining_hard_blockers",
        "risk_budget_profile",
        "suggested_position_risk",
        "suggested_stop",
        "suggested_time_limit",
        "entry_approval_changed",
        "policy_change",
        "entry_signal",
        "live_order_allowed",
        "research_only",
        "must_not_dispatch",
        "paper_order_route",
        "broker_order_route",
        "trading_route",
        "note",
    ]


def _ensure_history_schema(path: Path, fields: List[str]) -> None:
    if not path.exists() or path.stat().st_size <= 0:
        return
    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        reader = csv.reader(fp)
        try:
            old_fields = next(reader)
        except StopIteration:
            return
        if old_fields == fields:
            return
        raw_rows = list(reader)
    migrated: List[Dict[str, Any]] = []
    for raw in raw_rows:
        if len(raw) == len(fields):
            migrated.append({fields[idx]: raw[idx] for idx in range(len(fields))})
        else:
            row = {old_fields[idx]: raw[idx] for idx in range(min(len(old_fields), len(raw)))}
            for field in fields:
                row.setdefault(field, "")
            migrated.append({field: row.get(field, "") for field in fields})
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        writer.writerows(migrated)


def _append_history_csv(path: Path, snapshot_ts: str, rows: List[Dict[str, Any]]) -> None:
    fields = _history_fields()
    _ensure_history_schema(path, fields)
    exists = path.exists() and path.stat().st_size > 0
    with path.open("a", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        if not exists:
            writer.writeheader()
        for row in rows:
            out = {k: row.get(k, "") for k in fields}
            out["snapshot_ts"] = snapshot_ts
            writer.writerow(out)


def main() -> int:
    ts = datetime.now().isoformat(timespec="seconds")
    observe = _load_observe()
    no_lob_rows = _read_csv(RECHECK_CSV)
    score_rows = _read_csv(SCORE_RVOL_CSV)

    candidates: List[Dict[str, Any]] = []
    blocked_reasons: Counter[str] = Counter()
    for row in no_lob_rows:
        candidate = _candidate_from_no_lob(row)
        if candidate is not None:
            if _is_regular_session_signal(candidate.get("ts")):
                candidates.append(candidate)
            else:
                blocked_reasons["AFTER_MARKET_SIGNAL_SKIPPED"] += 1
        else:
            watch_candidate = _candidate_from_micro_probe_watch(row)
            if watch_candidate is not None:
                if _is_regular_session_signal(watch_candidate.get("ts")):
                    candidates.append(watch_candidate)
                else:
                    blocked_reasons["AFTER_MARKET_SIGNAL_SKIPPED"] += 1
                continue
            remaining = _remaining_hard(row)
            if remaining:
                for token in remaining:
                    blocked_reasons[token] += 1
            elif str(row.get("lob_status") or "").upper() != "OK":
                blocked_reasons["LOB_STATUS_NOT_OK"] += 1
            elif _to_float(row.get("spread_bps"), 999999.0) > 40.0:
                blocked_reasons["SPREAD_OVER_40BPS"] += 1
            else:
                blocked_reasons["NOT_ELIGIBLE_UNKNOWN"] += 1

    for row in score_rows:
        candidate = _candidate_from_score_rvol(row)
        if candidate is not None:
            if _is_regular_session_signal(candidate.get("ts")):
                candidates.append(candidate)
            else:
                blocked_reasons["AFTER_MARKET_SIGNAL_SKIPPED"] += 1

    candidates = _dedupe(candidates)
    payload = {
        "ts": ts,
        "status": "OK",
        "scope": "surge_expected_value_shadow_candidates",
        "source_no_lob_recheck_csv": str(RECHECK_CSV),
        "source_score_rvol_recheck_csv": str(SCORE_RVOL_CSV),
        "source_observe_json": str(OBSERVE_JSON),
        "observe_ts": observe.get("ts", ""),
        "candidate_rows": len(candidates),
        "tier_counts": [{"tier": k, "count": int(v)} for k, v in Counter(c["tier"] for c in candidates).most_common()],
        "blocked_reason_counts": [{"reason": k, "count": int(v)} for k, v in blocked_reasons.most_common()],
        "risk_contract": {
            "policy_change": False,
            "entry_approval_changed": False,
            "entry_signal": False,
            "live_order_allowed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_route": False,
            "research_only": True,
            "must_not_dispatch": True,
            "purpose": "expected-value research shadow candidates only; no order route",
        },
        "candidates": candidates,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, candidates)
    _append_history_csv(HISTORY_CSV, ts, candidates)
    LEGACY_OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(LEGACY_OUT_CSV, candidates)
    print(json.dumps({"status": "OK", "candidate_rows": len(candidates), "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
