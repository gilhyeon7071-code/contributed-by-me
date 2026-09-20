from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

OBSERVATION_LAYER_CSV = LOG_DIR / "surge_event_money_pullback_combined_observation_layer_latest.csv"
INTRADAY_PRICES_CSV = LOG_DIR / "intraday_prices_latest.csv"
SURGE_REALTIME_CSV = LOG_DIR / "surge_realtime_latest.csv"
PENDING_STATUS_JSON = LOG_DIR / "pending_entry_status_latest.json"

OUT_JSON = LOG_DIR / "surge_event_money_pullback_combined_paper_shadow_latest.json"
OUT_CSV = LOG_DIR / "surge_event_money_pullback_combined_paper_shadow_latest.csv"


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def _code(row: dict[str, Any]) -> str:
    value = str(row.get("code") or "").strip()
    return value.zfill(6) if value.isdigit() else value


def _index(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {_code(row): row for row in rows if _code(row)}


def _float(value: Any) -> float:
    try:
        text = str(value).strip()
        if not text or text.lower() in {"nan", "<na>", "none"}:
            return 0.0
        return float(text)
    except Exception:
        return 0.0


def _pct(num: float, den: float) -> float:
    if den == 0:
        return 0.0
    return round(num / den * 100.0, 4)


def _shadow_state(obs: dict[str, str], surge: dict[str, str]) -> tuple[str, str, str]:
    priority = obs.get("observation_priority_label", "")
    combined_state = obs.get("combined_shadow_state", "")
    missing = obs.get("missing_confirmations", "")
    surge_entry = surge.get("entry_decision", "")

    if priority == "BLOCKED" or combined_state == "COMBINED_BLOCKED":
        return "PAPER_SHADOW_BLOCKED", "combined_layer_keeps_block", "do_not_track_as_shadow_candidate"

    if priority == "HIGH_OBSERVATION" and combined_state == "COMBINED_RECOVERY_WATCH":
        return "PAPER_SHADOW_WATCH", f"track_recovery_watch_missing={missing}", "track_markout_only"

    if surge_entry == "ENTRY_ALLOWED":
        return "PAPER_SHADOW_CURRENT_LOGIC_ALLOWED", "current_surge_path_allowed", "track_current_logic_reference"

    return "PAPER_SHADOW_NO_CHANGE", "combined_layer_watch_unchanged", "no_shadow_action"


def main() -> int:
    observation_rows = _read_csv(OBSERVATION_LAYER_CSV)
    price_by_code = _index(_read_csv(INTRADAY_PRICES_CSV))
    surge_by_code = _index(_read_csv(SURGE_REALTIME_CSV))
    pending_status = _read_json(PENDING_STATUS_JSON)

    out_rows: list[dict[str, Any]] = []
    for obs in observation_rows:
        code = _code(obs)
        price = price_by_code.get(code, {})
        surge = surge_by_code.get(code, {})
        shadow_state, shadow_reason, shadow_action = _shadow_state(obs, surge)

        current = _float(price.get("current_price"))
        open_price = _float(price.get("open"))
        high = _float(price.get("high"))
        low = _float(price.get("low"))
        range_position = _pct(current - low, high - low)

        out_rows.append(
            {
                "code": code,
                "name": obs.get("name", ""),
                "paper_shadow_state": shadow_state,
                "paper_shadow_reason": shadow_reason,
                "paper_shadow_action": shadow_action,
                "combined_status": obs.get("combined_status", ""),
                "combined_shadow_state": obs.get("combined_shadow_state", ""),
                "observation_priority_label": obs.get("observation_priority_label", ""),
                "missing_confirmations": obs.get("missing_confirmations", ""),
                "current_price": current,
                "open": open_price,
                "high": high,
                "low": low,
                "range_position_pct": range_position,
                "latest_vs_open_pct": _pct(current - open_price, open_price),
                "latest_vs_high_pct": _pct(current - high, high),
                "latest_vs_low_pct": _pct(current - low, low),
                "trading_value": _float(price.get("trading_value")),
                "surge_entry_decision": surge.get("entry_decision", ""),
                "surge_entry_allowed": surge.get("entry_allowed", ""),
                "surge_entry_blocked": surge.get("entry_blocked", ""),
                "surge_entry_reason": surge.get("entry_reason", ""),
                "surge_score": surge.get("surge_score", ""),
                "rvol20": surge.get("rvol20", ""),
                "paper_order_route": False,
                "broker_order_route": False,
                "trading_route": False,
                "connection_level": "SIGNAL_QUALITY_ONLY",
                "trading_connection": False,
                "signal_connection": False,
                "execution_connection": False,
                "connection_note": "Signal quality or observation only; not entry approval, order, fill, or ledger.",
                "entry_approval_changed": False,
                "policy_change": False,
                "research_only": True,
            }
        )

    state_counts = Counter(str(row["paper_shadow_state"]) for row in out_rows)
    action_counts = Counter(str(row["paper_shadow_action"]) for row in out_rows)
    paper_shadow_watch_rows = sum(1 for row in out_rows if row["paper_shadow_state"] == "PAPER_SHADOW_WATCH")

    summary = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
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
        "inputs": {
            "observation_layer_csv": str(OBSERVATION_LAYER_CSV),
            "intraday_prices_csv": str(INTRADAY_PRICES_CSV),
            "surge_realtime_csv": str(SURGE_REALTIME_CSV),
            "pending_status_json": str(PENDING_STATUS_JSON),
        },
        "input_rows": {
            "observation_rows": len(observation_rows),
        },
        "pending_status_snapshot": {
            "generated_at": pending_status.get("generated_at", ""),
            "status": pending_status.get("status", ""),
            "entry_ready": pending_status.get("entry_ready", ""),
            "filled": pending_status.get("filled", ""),
            "candidates_after_caps": pending_status.get("candidates_after_caps", ""),
            "pending_queue_len_raw": pending_status.get("pending_queue_len_raw", ""),
        },
        "paper_shadow_state_counts": dict(state_counts),
        "paper_shadow_action_counts": dict(action_counts),
        "paper_shadow_watch_rows": paper_shadow_watch_rows,
        "paper_order_rows": 0,
        "broker_order_rows": 0,
        "trade_route_changes": 0,
        "rows": out_rows,
    }

    OUT_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    fieldnames = list(out_rows[0].keys()) if out_rows else ["code", "name", "paper_shadow_state"]
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    print(
        json.dumps(
            {
                "paper_shadow_state_counts": dict(state_counts),
                "paper_shadow_watch_rows": paper_shadow_watch_rows,
                "trade_route_changes": 0,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
