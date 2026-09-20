from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

MARKET_RISING_CSV = LOG_DIR / "market_rising_latest.csv"
EVENT_THEME_CSV = LOG_DIR / "event_theme_candidate_layer_latest.csv"
ACTION_QUEUE_CSV = LOG_DIR / "candidate_action_queue_latest.csv"
ACTION_PLAN_CSV = LOG_DIR / "candidate_action_plan_latest.csv"
LIQUIDITY_JSON = LOG_DIR / "liquidity_filter_daily_last.json"
NORMAL_BOTTLENECK_JSON = LOG_DIR / "normal_entry_path_bottleneck_audit_latest.json"

OUT_JSON = LOG_DIR / "market_theme_watch_preservation_audit_latest.json"
OUT_CSV = LOG_DIR / "market_theme_watch_preservation_audit_latest.csv"


STRONG_CHANGE_PCT = 5.0
LEADER_RANK_MAX = 20
MIN_TRADING_VALUE_KRW = 1_000_000_000.0


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_csv(path: Path, rows: List[Dict[str, Any]], fields: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _code(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text.zfill(6) if text else ""


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value in ("", None):
            return default
        return float(value)
    except Exception:
        return default


def _index_first(rows: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code and code not in out:
            out[code] = row
    return out


def _index_many(rows: Iterable[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        code = _code(row.get("code"))
        if code:
            out[code].append(row)
    return dict(out)


def _liquidity_removed_codes(payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    removed = payload.get("removed") if isinstance(payload, dict) else []
    out: Dict[str, Dict[str, Any]] = {}
    if not isinstance(removed, list):
        return out
    for row in removed:
        if not isinstance(row, dict):
            continue
        code = _code(row.get("code"))
        if code:
            out[code] = row
    return out


def _strong_market_rows(rows: List[Dict[str, str]], liquidity_removed: Dict[str, Dict[str, Any]]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        code = _code(row.get("code"))
        if not code:
            continue
        change_pct = _float(row.get("change_pct"))
        trading_value = _float(row.get("trading_value"))
        rank = _float(row.get("rank"), 999999.0)
        if (
            change_pct >= STRONG_CHANGE_PCT
            and trading_value >= MIN_TRADING_VALUE_KRW
            and rank <= LEADER_RANK_MAX
        ):
            out.append(row)
            seen.add(code)
    for code, row in liquidity_removed.items():
        if code in seen:
            continue
        day_ret = _float(row.get("day_ret_pct"))
        trading_value = _float(row.get("trading_value"))
        if day_ret >= 28.0 and trading_value >= MIN_TRADING_VALUE_KRW:
            out.append(
                {
                    "code": code,
                    "name": "",
                    "rank": "",
                    "change_pct": str(day_ret),
                    "trading_value": str(trading_value),
                    "_source": "liquidity_removed_limit_up",
                }
            )
            seen.add(code)
    return out


def _preservation_status(queue_rows: List[Dict[str, Any]], plan_rows: List[Dict[str, Any]]) -> tuple[str, str, str]:
    queue_states = {str(row.get("action_state") or "").strip().upper() for row in queue_rows}
    plan_actions = {str(row.get("next_action") or "").strip().upper() for row in plan_rows}
    queue_sources = {str(row.get("source") or "").strip() for row in queue_rows if str(row.get("source") or "").strip()}

    if "EXECUTED_BUY" in plan_actions:
        return "EXECUTED_ALREADY", "executed_buy_in_plan", "|".join(sorted(queue_sources))
    if plan_actions & {"PROMOTE_TO_RECHECK", "RECHECK_15M"}:
        return "PRESERVED_RECHECK", "recheck_or_promote_present", "|".join(sorted(queue_sources))
    if "WATCH" in queue_states or "WATCH_ONLY" in plan_actions:
        return "PRESERVED_WATCH_ONLY", "watch_present", "|".join(sorted(queue_sources))
    if queue_states == {"BLOCKED"} or plan_actions == {"BLOCK_HARD_OR_POLICY"}:
        return "PRESERVED_BLOCKED_ONLY", "blocked_without_watch_recheck", "|".join(sorted(queue_sources))
    return "MISSING_FROM_WATCH_CHAIN", "no_queue_or_plan_preservation", "|".join(sorted(queue_sources))


def build() -> Dict[str, Any]:
    market_rising = _read_csv(MARKET_RISING_CSV)
    event_theme = _index_first(_read_csv(EVENT_THEME_CSV))
    queue = _index_many(_read_csv(ACTION_QUEUE_CSV))
    plan = _index_many(_read_csv(ACTION_PLAN_CSV))
    liquidity = _read_json(LIQUIDITY_JSON)
    normal_bottleneck = _read_json(NORMAL_BOTTLENECK_JSON)
    liquidity_removed = _liquidity_removed_codes(liquidity)

    rows: List[Dict[str, Any]] = []
    for src in _strong_market_rows(market_rising, liquidity_removed):
        code = _code(src.get("code"))
        event = event_theme.get(code, {})
        queue_rows = queue.get(code, [])
        plan_rows = plan.get(code, [])
        status, reason, sources = _preservation_status(queue_rows, plan_rows)
        liquidity_removed_row = liquidity_removed.get(code, {})
        event_decision = str(event.get("event_theme_decision") or "")
        theme_type = str(event.get("theme_type") or "")
        route_fit = "THEME_OR_SURGE_WATCH"
        if liquidity_removed_row:
            route_fit = "NORMAL_ENTRY_EXCLUDED_LIMIT_UP_WATCH_PRESERVED"
        elif event_decision in {"DATA_LIMITED", ""} and theme_type in {"UNCLASSIFIED", ""}:
            route_fit = "WATCH_PRESERVED_THEME_MAPPING_WEAK"
        rows.append(
            {
                "code": code,
                "name": src.get("name") or event.get("name") or "",
                "rank": src.get("rank", ""),
                "change_pct": src.get("change_pct", ""),
                "trading_value": src.get("trading_value", ""),
                "event_theme_decision": event_decision,
                "event_theme_reason": event.get("decision_reason", ""),
                "theme_type": theme_type,
                "theme_hits": event.get("theme_hits", ""),
                "theme_scarcity_label": event.get("theme_scarcity_label", ""),
                "candidate_sources": event.get("candidate_sources", ""),
                "liquidity_removed": bool(liquidity_removed_row),
                "liquidity_removed_reason": "|".join(str(x) for x in liquidity_removed_row.get("reasons", []))
                if isinstance(liquidity_removed_row.get("reasons"), list)
                else str(liquidity_removed_row.get("reasons") or ""),
                "watch_preservation_status": status,
                "watch_preservation_reason": reason,
                "watch_sources": sources,
                "route_fit": route_fit,
                "normal_entry_route": "unchanged",
                "research_only": True,
                "trading_effect": False,
                "policy_effect": False,
                "policy_change_applied": False,
            }
        )

    return {
        "generated_at": _now_ts(),
        "status": "PASS",
        "scope": "read_only_market_theme_watch_preservation_audit",
        "source_files": {
            "market_rising": str(MARKET_RISING_CSV),
            "event_theme": str(EVENT_THEME_CSV),
            "candidate_action_queue": str(ACTION_QUEUE_CSV),
            "candidate_action_plan": str(ACTION_PLAN_CSV),
            "liquidity_filter": str(LIQUIDITY_JSON),
            "normal_bottleneck": str(NORMAL_BOTTLENECK_JSON),
        },
        "criteria": {
            "strong_change_pct_min": STRONG_CHANGE_PCT,
            "leader_rank_max": LEADER_RANK_MAX,
            "min_trading_value_krw": MIN_TRADING_VALUE_KRW,
            "liquidity_removed_day_ret_pct_min": 28.0,
        },
        "summary": {
            "rows": len(rows),
            "status_counts": dict(Counter(str(row["watch_preservation_status"]) for row in rows)),
            "route_fit_counts": dict(Counter(str(row["route_fit"]) for row in rows)),
            "event_theme_decision_counts": dict(Counter(str(row["event_theme_decision"]) for row in rows)),
            "theme_type_counts": dict(Counter(str(row["theme_type"]) for row in rows)),
            "normal_primary_bottleneck": normal_bottleneck.get("primary_bottleneck"),
            "normal_primary_bottleneck_type": normal_bottleneck.get("primary_bottleneck_type"),
        },
        "rows": rows,
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }


def main() -> int:
    payload = build()
    fields = [
        "code",
        "name",
        "rank",
        "change_pct",
        "trading_value",
        "event_theme_decision",
        "event_theme_reason",
        "theme_type",
        "theme_hits",
        "theme_scarcity_label",
        "candidate_sources",
        "liquidity_removed",
        "liquidity_removed_reason",
        "watch_preservation_status",
        "watch_preservation_reason",
        "watch_sources",
        "route_fit",
        "normal_entry_route",
        "research_only",
        "trading_effect",
        "policy_effect",
        "policy_change_applied",
    ]
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, payload["rows"], fields)
    print(
        f"[FINAL] market theme watch preservation audit -> {OUT_JSON} "
        f"rows={payload['summary']['rows']} status_counts={payload['summary']['status_counts']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
