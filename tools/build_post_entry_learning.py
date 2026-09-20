"""Build a read-only post-entry learning snapshot for paper BUY fills.

The artifact connects executed paper entries back to the action pipeline and
current prices. It does not place orders, change scores, or alter policy.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER_DIR = ROOT / "paper"

FILLS_CSV = PAPER_DIR / "fills.csv"
PAPER_STATE_JSON = PAPER_DIR / "paper_state.json"
ACTION_PLAN_JSON = LOG_DIR / "candidate_action_plan_latest.json"
PROMOTED_JSON = LOG_DIR / "promoted_recheck_candidates_latest.json"
CONFIG_PATH = PAPER_DIR / "paper_engine_config.json"
CANDIDATES_FINAL = LOG_DIR / "candidates_latest_data.with_final_score.csv"

OUTPUT_JSON = LOG_DIR / "post_entry_learning_latest.json"
OUTPUT_CSV = LOG_DIR / "post_entry_learning_latest.csv"

KST = dt.timezone(dt.timedelta(hours=9))


def _now() -> dt.datetime:
    return dt.datetime.now(tz=KST)


def _today() -> str:
    return _now().strftime("%Y%m%d")


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return [{str(k): str(v) for k, v in row.items()} for row in csv.DictReader(f)]
    except Exception:
        return []


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "entry_ts",
        "code",
        "name",
        "entry_price",
        "buy_qty",
        "open_qty",
        "current_price_available",
        "current_price",
        "unrealized_return_pct",
        "current_change_pct",
        "current_trading_value",
        "current_price_source",
        "entry_action",
        "entry_origin",
        "learning_bucket",
        "learning_cause",
        "learning_policy_action",
        "exit_reason",
        "exit_qty",
        "exit_count",
        "partial_exit_count",
        "order_id",
        "entry_trace_id",
        "replay_chain_id",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _code(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    if not text:
        return ""
    return text.zfill(6)[-6:]


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value).strip()
        if not text:
            return float(default)
        return float(text)
    except Exception:
        return float(default)


def _i(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return int(default)


def _note_map(note: Any) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for part in str(note or "").split(";"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on", "t"}


def _load_price_map() -> Dict[str, Dict[str, Any]]:
    price_map: Dict[str, Dict[str, Any]] = {}

    for row in _read_csv(LOG_DIR / "intraday_prices_latest.csv"):
        code = _code(row.get("code"))
        price = _f(row.get("current_price"), 0.0)
        if code and price > 0.0:
            price_map[code] = {
                "price": price,
                "source": "intraday_prices",
                "price_ts": str(row.get("ts") or ""),
                "change_pct": None,
                "trading_value": _f(row.get("trading_value"), 0.0),
            }

    for row in _read_csv(LOG_DIR / "market_rising_latest.csv"):
        code = _code(row.get("code"))
        price = _f(row.get("current_price"), 0.0)
        if code and price > 0.0:
            price_map[code] = {
                "price": price,
                "source": "market_rising",
                "price_ts": "",
                "change_pct": _f(row.get("change_pct"), 0.0),
                "trading_value": _f(row.get("trading_value"), 0.0),
            }

    for row in _read_csv(LOG_DIR / "surge_realtime_latest.csv"):
        code = _code(row.get("code"))
        price = _f(row.get("current_price"), 0.0)
        if code and price > 0.0:
            price_map[code] = {
                "price": price,
                "source": "surge_realtime",
                "price_ts": str(row.get("ts") or ""),
                "change_pct": _f(row.get("change_pct"), 0.0) * 100.0,
                "trading_value": _f(row.get("trading_value"), 0.0),
            }

    return price_map


def _load_plan_map() -> Dict[str, Dict[str, Any]]:
    doc = _read_json(ACTION_PLAN_JSON)
    rows = doc.get("rows") if isinstance(doc.get("rows"), list) else []
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = _code(row.get("code"))
        if not code or str(row.get("next_action") or "") != "EXECUTED_BUY":
            continue
        existing = out.get(code)
        if existing and str(existing.get("source") or "") == "action_recheck_due":
            continue
        if existing and str(row.get("source") or "") != "action_recheck_due":
            continue
        if code:
            out[code] = row
    return out


def _load_promoted_codes() -> Dict[str, Dict[str, Any]]:
    doc = _read_json(PROMOTED_JSON)
    rows = doc.get("rows") if isinstance(doc.get("rows"), list) else []
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = _code(row.get("code"))
        if code:
            out[code] = row
    return out


def _load_candidate_map() -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in _read_csv(CANDIDATES_FINAL):
        code = _code(row.get("code"))
        if code:
            out[code] = row
    return out


def _load_open_positions() -> Dict[str, Dict[str, Any]]:
    doc = _read_json(PAPER_STATE_JSON)
    rows = doc.get("open_positions") if isinstance(doc.get("open_positions"), list) else []
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = _code(row.get("code"))
        if code:
            out[code] = row
    return out


def _sector_union_ok(candidate: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    pol = cfg.get("positive_entry_criteria", {}) if isinstance(cfg.get("positive_entry_criteria"), dict) else {}
    try:
        union_strength_min = float(cfg.get("union_entry_strength_min", 0.65) or 0.65)
    except Exception:
        union_strength_min = 0.65
    return bool(
        _f(candidate.get("final_score"), _f(candidate.get("score"), 0.0)) > _f(pol.get("min_score"), 0.0)
        and bool(pol.get("allow_sector_union", True))
        and str(candidate.get("candidate_origin") or "").strip().upper() == "SECTOR_PREFILTER_UNION"
        and str(candidate.get("sector_action") or "").strip().upper() == "BUY"
        and _truthy(candidate.get("sector_entry_allowed"))
        and _f(candidate.get("sector_strength"), 0.0) >= union_strength_min
    )


def _fresh_sector_fallback_selected(
    code: str,
    candidates: Dict[str, Dict[str, Any]],
    cfg: Dict[str, Any],
    prior_buy_codes: set[str],
) -> bool:
    pol = cfg.get("positive_entry_criteria", {}) if isinstance(cfg.get("positive_entry_criteria"), dict) else {}
    fallback = pol.get("fresh_sector_allowed_fallback", {}) if isinstance(pol.get("fresh_sector_allowed_fallback"), dict) else {}
    if not bool(fallback.get("enabled", False)):
        return False
    active = {c: r for c, r in candidates.items() if c not in prior_buy_codes}
    if any(_sector_union_ok(row, cfg) for row in active.values()):
        return False
    origin_required = str(fallback.get("candidate_origin", "SECTOR_PREFILTER_UNION") or "").strip().upper()
    actions_raw = fallback.get("allowed_sector_actions", ["BUY", "WAIT"])
    if not isinstance(actions_raw, list):
        actions_raw = ["BUY", "WAIT"]
    allowed_actions = {str(x).strip().upper() for x in actions_raw if str(x).strip()}
    min_score = _f(fallback.get("min_final_score"), 0.10)
    min_strength = _f(fallback.get("min_sector_strength"), 0.40)
    max_candidates = max(1, _i(fallback.get("max_candidates"), 1))
    eligible: List[tuple[str, float]] = []
    for cand_code, row in active.items():
        origin_ok = not origin_required or str(row.get("candidate_origin") or "").strip().upper() == origin_required
        action_ok = str(row.get("sector_action") or "").strip().upper() in allowed_actions
        if (
            origin_ok
            and action_ok
            and _truthy(row.get("sector_entry_allowed"))
            and _f(row.get("sector_strength"), 0.0) >= min_strength
            and _f(row.get("final_score"), _f(row.get("score"), 0.0)) >= min_score
        ):
            eligible.append((cand_code, _f(row.get("final_score"), _f(row.get("score"), 0.0))))
    eligible.sort(key=lambda x: (-x[1], x[0]))
    return code in {cand_code for cand_code, _ in eligible[:max_candidates]}


def _entry_origin_from_candidate_path(
    code: str,
    candidates: Dict[str, Dict[str, Any]],
    cfg: Dict[str, Any],
    prior_buy_codes: set[str],
) -> str:
    candidate = candidates.get(code)
    if not candidate:
        return ""
    if _sector_union_ok(candidate, cfg):
        return "SECTOR_UNION_CONDITIONAL"
    if _fresh_sector_fallback_selected(code, candidates, cfg, prior_buy_codes):
        return "FRESH_SECTOR_ALLOWED_FALLBACK"
    return ""


def _current_day_buy_fills(rows: Iterable[Dict[str, str]], ymd: str) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for row in rows:
        code = _code(row.get("code"))
        if not code:
            continue
        if str(row.get("side") or "").upper() != "BUY":
            continue
        if str(row.get("datetime") or "")[:8] != ymd:
            continue
        out.append(row)
    return out


def _sell_events_by_entry(rows: Iterable[Dict[str, str]], ymd: str) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if str(row.get("side") or "").upper() != "SELL":
            continue
        if str(row.get("datetime") or "")[:8] != ymd:
            continue
        note = _note_map(row.get("note"))
        entry_order_id = str(note.get("entry_order_id") or "")
        if not entry_order_id:
            continue
        event = out.setdefault(entry_order_id, {
            "exit_count": 0,
            "exit_qty": 0,
            "partial_exit_count": 0,
            "exit_reasons": [],
            "last_exit_ts": "",
            "last_exit_price": 0.0,
        })
        event["exit_count"] = int(event.get("exit_count") or 0) + 1
        event["exit_qty"] = int(event.get("exit_qty") or 0) + _i(row.get("qty"), 0)
        if str(note.get("partial_exit") or "") == "1":
            event["partial_exit_count"] = int(event.get("partial_exit_count") or 0) + 1
        exit_reason = str(note.get("exit_reason") or "")
        if exit_reason and exit_reason not in event["exit_reasons"]:
            event["exit_reasons"].append(exit_reason)
        event["last_exit_ts"] = str(row.get("datetime") or "")
        event["last_exit_price"] = _f(row.get("price"), 0.0)
    return out


def _origin_from_plan_or_reason(
    plan: Dict[str, Any],
    promoted: bool,
    reason: str,
    fill_note: Any = "",
    candidate_path_origin: str = "",
) -> str:
    note = _note_map(fill_note)
    if str(note.get("surge_immediate") or "").strip().lower() in {"1", "true", "yes", "y"}:
        return "SURGE_IMMEDIATE"
    if candidate_path_origin:
        return candidate_path_origin
    if str(plan.get("fill_provenance") or "") == "FILL_OBSERVED_NO_CURRENT_TRADABLE_ROW":
        return "FILL_OBSERVED_UNRESOLVED"
    explicit = str(plan.get("candidate_origin") or "")
    if explicit:
        return explicit
    source = str(plan.get("source") or "")
    if (
        promoted
        or source == "action_recheck_due"
        or "RECHECK_MISSED_MOVE_CANDIDATE" in reason
        or "WATCH_MISSED_MOVE_CANDIDATE" in reason
    ):
        return "ACTIVE_RECHECK_PROMOTION"
    return "NATURAL_OR_UNKNOWN"


def _entry_bucket_prefix(entry_origin: str, promoted: bool) -> str:
    if entry_origin == "SURGE_IMMEDIATE":
        return "SURGE"
    if entry_origin == "SECTOR_UNION_CONDITIONAL":
        return "SECTOR_UNION"
    if entry_origin == "FRESH_SECTOR_ALLOWED_FALLBACK":
        return "FRESH_SECTOR_FALLBACK"
    if entry_origin == "FILL_OBSERVED_UNRESOLVED":
        return "UNRESOLVED"
    return "PROMOTED" if promoted else "NATURAL"


def _bucket(
    *,
    prefix: str,
    open_qty: int,
    current_price_available: bool,
    return_pct: float,
    exit_count: int,
    exit_reason: str,
    partial_exit_count: int,
) -> str:
    if exit_count > 0:
        if open_qty <= 0:
            if "DDM_LIQUIDATE" in exit_reason:
                return f"{prefix}_ENTRY_FULL_DDM_LIQUIDATED"
            if exit_reason:
                return f"{prefix}_ENTRY_FULL_EXIT_{exit_reason}"
            return f"{prefix}_ENTRY_FULL_EXIT"
        if partial_exit_count > 0:
            if "DDM_LIQUIDATE" in exit_reason:
                return f"{prefix}_ENTRY_PARTIAL_DDM_LIQUIDATED"
            if exit_reason:
                return f"{prefix}_ENTRY_PARTIAL_EXIT_{exit_reason}"
            return f"{prefix}_ENTRY_PARTIAL_EXIT"
    if not current_price_available:
        return "NO_CURRENT_PRICE"
    if return_pct >= 1.0:
        return f"{prefix}_ENTRY_POSITIVE"
    if return_pct <= -1.0:
        return f"{prefix}_ENTRY_NEGATIVE"
    return f"{prefix}_ENTRY_FLAT"


def _learning_cause(bucket: str, return_pct: float, exit_reason: str) -> str:
    if "FULL_DDM_LIQUIDATED" in bucket:
        return "ddm_full_liquidation"
    if "PARTIAL_DDM_LIQUIDATED" in bucket:
        return "ddm_partial_liquidation"
    if "FULL_EXIT" in bucket:
        return f"full_exit:{exit_reason or 'unknown'}"
    if "PARTIAL_EXIT" in bucket:
        return f"partial_exit:{exit_reason or 'unknown'}"
    if bucket.endswith("_NEGATIVE"):
        return f"mark_to_market_negative:{return_pct:.4f}"
    if bucket.endswith("_POSITIVE"):
        return f"mark_to_market_positive:{return_pct:.4f}"
    if bucket.endswith("_FLAT"):
        return f"mark_to_market_flat:{return_pct:.4f}"
    return "no_current_price" if bucket == "NO_CURRENT_PRICE" else "unknown"


def _learning_policy_action(bucket: str) -> str:
    if "FULL_DDM_LIQUIDATED" in bucket or "FULL_EXIT" in bucket:
        return "NO_SAME_DAY_REENTRY_WITHOUT_NEW_SIGNAL"
    if "PARTIAL_DDM_LIQUIDATED" in bucket or "PARTIAL_EXIT" in bucket:
        return "TIGHTEN_CONTINUATION_RECHECK"
    if bucket.endswith("_NEGATIVE"):
        return "TIGHTEN_RECHECK_PROMOTION"
    if bucket.endswith("_POSITIVE"):
        return "ALLOW_CONTINUATION_RECHECK"
    if bucket.endswith("_FLAT"):
        return "WATCH_CONTINUATION_ONLY"
    return "OBSERVE_ONLY"


def _summarize(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    buckets: Dict[str, int] = {}
    origins: Dict[str, int] = {}
    observed = 0
    positive = 0
    negative = 0
    for row in rows:
        bucket = str(row.get("learning_bucket") or "")
        origin = str(row.get("entry_origin") or "")
        buckets[bucket] = buckets.get(bucket, 0) + 1
        origins[origin] = origins.get(origin, 0) + 1
        if row.get("current_price_available"):
            observed += 1
            ret = _f(row.get("unrealized_return_pct"), 0.0)
            if ret > 0.0:
                positive += 1
            elif ret < 0.0:
                negative += 1
    return {
        "rows": len(rows),
        "observed_price_rows": observed,
        "positive_rows": positive,
        "negative_rows": negative,
        "bucket_counts": buckets,
        "origin_counts": origins,
    }


def main() -> int:
    ymd = _today()
    fill_rows = _read_csv(FILLS_CSV)
    fills = _current_day_buy_fills(fill_rows, ymd)
    sell_events = _sell_events_by_entry(fill_rows, ymd)
    price_map = _load_price_map()
    plan_map = _load_plan_map()
    promoted_map = _load_promoted_codes()
    cfg = _read_json(CONFIG_PATH)
    candidate_map = _load_candidate_map()
    positions = _load_open_positions()
    rows: List[Dict[str, Any]] = []
    prior_buy_codes: set[str] = set()

    for fill in fills:
        code = _code(fill.get("code"))
        note = _note_map(fill.get("note"))
        plan = plan_map.get(code, {})
        action_reason = str(plan.get("action_reason") or "")
        origin_reason = "|".join([
            action_reason,
            str(plan.get("reason") or ""),
            str(plan.get("review_bucket") or ""),
        ])
        promoted = code in promoted_map or str(plan.get("candidate_origin") or "") == "ACTIVE_RECHECK_PROMOTION"
        price_info = price_map.get(code, {})
        current_price = _f(price_info.get("price"), 0.0)
        entry_price = _f(fill.get("price"), 0.0)
        current_price_available = bool(current_price > 0.0)
        ret_pct = 0.0
        if entry_price > 0.0 and current_price > 0.0:
            ret_pct = (current_price / entry_price - 1.0) * 100.0
        pos = positions.get(code, {})
        open_qty = _i(pos.get("qty"), 0)
        buy_qty = _i(fill.get("qty"), 0)
        name = str(pos.get("name") or plan.get("name") or "")
        candidate_path_origin = _entry_origin_from_candidate_path(code, candidate_map, cfg, prior_buy_codes)
        entry_origin = _origin_from_plan_or_reason(
            plan,
            promoted,
            origin_reason or str(fill.get("note") or ""),
            fill.get("note"),
            candidate_path_origin,
        )
        promoted = entry_origin == "ACTIVE_RECHECK_PROMOTION"
        order_id = str(fill.get("order_id") or "")
        exits = sell_events.get(order_id, {})
        exit_reasons = exits.get("exit_reasons") if isinstance(exits.get("exit_reasons"), list) else []
        exit_reason = "|".join(str(x) for x in exit_reasons if str(x))
        exit_count = int(_f(exits.get("exit_count"), 0.0))
        partial_exit_count = int(_f(exits.get("partial_exit_count"), 0.0))
        bucket = _bucket(
            prefix=_entry_bucket_prefix(entry_origin, promoted),
            open_qty=open_qty,
            current_price_available=current_price_available,
            return_pct=ret_pct,
            exit_count=exit_count,
            exit_reason=exit_reason,
            partial_exit_count=partial_exit_count,
        )
        rows.append({
            "entry_ts": str(fill.get("datetime") or ""),
            "code": code,
            "name": name,
            "entry_price": entry_price,
            "buy_qty": buy_qty,
            "open_qty": open_qty,
            "current_price_available": current_price_available,
            "current_price": current_price,
            "unrealized_return_pct": round(ret_pct, 4),
            "current_change_pct": price_info.get("change_pct"),
            "current_trading_value": _f(price_info.get("trading_value"), 0.0),
            "current_price_source": str(price_info.get("source") or ""),
            "current_price_ts": str(price_info.get("price_ts") or ""),
            "entry_action": str(plan.get("next_action") or "BUY_FILL_OBSERVED"),
            "entry_origin": entry_origin,
            "action_reason": action_reason,
            "learning_bucket": bucket,
            "learning_cause": _learning_cause(bucket, ret_pct, exit_reason),
            "learning_policy_action": _learning_policy_action(bucket),
            "exit_reason": exit_reason,
            "exit_qty": int(_f(exits.get("exit_qty"), 0.0)),
            "exit_count": exit_count,
            "partial_exit_count": partial_exit_count,
            "last_exit_ts": str(exits.get("last_exit_ts") or ""),
            "last_exit_price": _f(exits.get("last_exit_price"), 0.0),
            "order_id": order_id,
            "entry_order_id": note.get("entry_order_id", ""),
            "entry_intent_id": note.get("entry_intent_id", ""),
            "entry_trace_id": note.get("entry_trace_id", ""),
            "replay_chain_id": note.get("replay_chain_id", ""),
            "replay_depth": _i(note.get("replay_depth"), 0),
            "policy_effect": False,
            "trading_effect": False,
        })
        if code:
            prior_buy_codes.add(code)

    rows.sort(key=lambda row: str(row.get("entry_ts") or ""))
    payload = {
        "schema_version": "post_entry_learning_v1",
        "generated_at": _now().isoformat(timespec="seconds"),
        "trading_effect": False,
        "policy_effect": False,
        "source": "paper_fills_current_day",
        "ymd": ymd,
        "summary": _summarize(rows),
        "artifacts": {
            "fills": str(FILLS_CSV),
            "paper_state": str(PAPER_STATE_JSON),
            "action_plan": str(ACTION_PLAN_JSON),
            "promoted_recheck_candidates": str(PROMOTED_JSON),
            "json": str(OUTPUT_JSON),
            "csv": str(OUTPUT_CSV),
        },
        "rows": rows,
    }
    _write_json(OUTPUT_JSON, payload)
    _write_csv(OUTPUT_CSV, rows)
    print(f"[POST_ENTRY_LEARNING] rows={len(rows)} json={OUTPUT_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
