from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

BRIDGE_REVIEW_CSV = LOG_DIR / "surge_event_money_pullback_bridge_review_latest.csv"
INTRADAY_HISTORY_CSV = LOG_DIR / "arl_intraday_history_returns_latest.csv"
INTRADAY_LATEST_CSV = LOG_DIR / "intraday_prices_latest.csv"
SURGE_REALTIME_CSV = LOG_DIR / "surge_realtime_latest.csv"
SURGE_LOB_CSV = LOG_DIR / "surge_lob_latest.csv"

OUT_JSON = LOG_DIR / "surge_event_money_pullback_bridge_reaction_check_latest.json"
OUT_CSV = LOG_DIR / "surge_event_money_pullback_bridge_reaction_check_latest.csv"


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


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


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = "" if value is None else str(value).strip()
        if text and text.lower() not in {"nan", "<na>", "none"}:
            return text
    return ""


def _pct(num: float, den: float) -> float:
    if den == 0:
        return 0.0
    return round(num / den * 100.0, 4)


def _reaction_label(metrics: dict[str, float], surge_row: dict[str, str], lob_row: dict[str, str]) -> tuple[str, str, str, str]:
    range_pos = metrics["range_position_pct"]
    from_peak = metrics["latest_vs_observed_peak_pct"]
    from_bridge = metrics["latest_vs_bridge_reference_pct"]
    value_growth = metrics["value_growth_after_bridge_pct"]
    rvol20 = _float(surge_row.get("rvol20"))
    surge_detected = str(surge_row.get("detected_surge_flag", "")).lower() == "true"
    surge_allowed = str(surge_row.get("entry_allowed", "")).lower() == "true"
    lob_available = str(lob_row.get("lob_available", "")).lower() == "true"

    missing: list[str] = []
    if metrics["latest_vs_open_pct"] < -1.5 and metrics["latest_vs_high_pct"] < -1.5:
        missing.append("price_reclaim")
    if rvol20 < 1.0:
        missing.append("rvol_recovery")
    if not surge_detected and not surge_allowed:
        missing.append("realtime_surge_detection")
    if not lob_available:
        missing.append("lob_confirmation")

    if from_peak <= -4.0 and range_pos < 50.0:
        return "BRIDGE_REJECT", "failed_upper_half_and_peak_pullback_deepened", "|".join(missing), "keep_blocked_or_drop_watch"
    if range_pos >= 60.0 and from_bridge >= 0.0 and value_growth >= 0.0 and not missing:
        return "BRIDGE_CONFIRM", "price_volume_surge_lob_confirmed", "", "eligible_for_policy_review_only"
    if range_pos >= 55.0 and from_bridge >= -0.5 and value_growth >= 0.0:
        return "BRIDGE_RECOVERING", "low_defense_and_midrange_recovery_visible", "|".join(missing), "continue_intraday_observation"
    return "BRIDGE_WAIT", "needs_reclaim_or_volume_confirmation", "|".join(missing), "wait_for_next_tick"


def main() -> int:
    bridge_rows = [
        row
        for row in _read_csv(BRIDGE_REVIEW_CSV)
        if row.get("bridge_decision") == "BRIDGE_SHADOW_CANDIDATE"
    ]
    history_rows = _read_csv(INTRADAY_HISTORY_CSV)
    latest_by_code = _index(_read_csv(INTRADAY_LATEST_CSV))
    surge_by_code = _index(_read_csv(SURGE_REALTIME_CSV))
    lob_by_code = _index(_read_csv(SURGE_LOB_CSV))

    history_by_code: dict[str, list[dict[str, str]]] = {}
    for row in history_rows:
        code = _code(row)
        if code:
            history_by_code.setdefault(code, []).append(row)

    out_rows: list[dict[str, Any]] = []
    for bridge in bridge_rows:
        code = _code(bridge)
        rows = history_by_code.get(code, [])
        latest = latest_by_code.get(code, {})
        surge = surge_by_code.get(code, {})
        lob = lob_by_code.get(code, {})

        current = _float(latest.get("current_price") or (rows[-1].get("current_price") if rows else "0"))
        open_price = _float(latest.get("open") or (rows[-1].get("open") if rows else "0"))
        high = _float(latest.get("high") or (rows[-1].get("high") if rows else "0"))
        low = _float(latest.get("low") or (rows[-1].get("low") if rows else "0"))
        value = _float(latest.get("trading_value") or (rows[-1].get("trading_value") if rows else "0"))

        bridge_reference_value = _float(bridge.get("blocker_detail"))  # stays 0; reference comes from history fallback below
        prices = [_float(row.get("current_price")) for row in rows if _float(row.get("current_price")) > 0]
        observed_peak = max(prices) if prices else high
        observed_trough = min(prices) if prices else low
        bridge_reference_price = _float(rows[-2].get("current_price")) if len(rows) >= 2 else current
        bridge_reference_value = _float(rows[-2].get("trading_value")) if len(rows) >= 2 else value

        metrics = {
            "latest_vs_open_pct": _pct(current - open_price, open_price),
            "latest_vs_high_pct": _pct(current - high, high),
            "latest_vs_low_pct": _pct(current - low, low),
            "latest_vs_observed_peak_pct": _pct(current - observed_peak, observed_peak),
            "latest_vs_observed_trough_pct": _pct(current - observed_trough, observed_trough),
            "range_position_pct": _pct(current - low, high - low),
            "latest_vs_bridge_reference_pct": _pct(current - bridge_reference_price, bridge_reference_price),
            "value_growth_after_bridge_pct": _pct(value - bridge_reference_value, bridge_reference_value),
        }
        label, reason, missing_confirmations, next_action = _reaction_label(metrics, surge, lob)

        out_rows.append(
            {
                "code": code,
                "name": bridge.get("name", ""),
                "reaction_label": label,
                "reaction_reason": reason,
                "missing_confirmations": missing_confirmations,
                "next_observation_action": next_action,
                "bridge_decision": bridge.get("bridge_decision", ""),
                "criteria_first_bucket": bridge.get("criteria_first_bucket", ""),
                "criteria_first_bucket_display": bridge.get("criteria_first_bucket_display", ""),
                "bucket_contract": bridge.get("bucket_contract", ""),
                "current_price": current,
                "open": open_price,
                "high": high,
                "low": low,
                "trading_value": value,
                "history_points": len(rows),
                "first_history_ts": rows[0].get("ts", "") if rows else "",
                "latest_history_ts": rows[-1].get("ts", "") if rows else "",
                "latest_vs_open_pct": metrics["latest_vs_open_pct"],
                "latest_vs_high_pct": metrics["latest_vs_high_pct"],
                "latest_vs_low_pct": metrics["latest_vs_low_pct"],
                "latest_vs_observed_peak_pct": metrics["latest_vs_observed_peak_pct"],
                "latest_vs_observed_trough_pct": metrics["latest_vs_observed_trough_pct"],
                "range_position_pct": metrics["range_position_pct"],
                "latest_vs_bridge_reference_pct": metrics["latest_vs_bridge_reference_pct"],
                "value_growth_after_bridge_pct": metrics["value_growth_after_bridge_pct"],
                "detected_surge_flag": surge.get("detected_surge_flag", ""),
                "surge_entry_allowed": surge.get("entry_allowed", ""),
                "surge_entry_decision": surge.get("entry_decision", ""),
                "surge_entry_reason": surge.get("entry_reason", ""),
                "surge_score": surge.get("surge_score", ""),
                "rvol20": surge.get("rvol20", ""),
                "lob_available": lob.get("lob_available", ""),
                "lob_status": lob.get("lob_status", ""),
                "lob_evidence_reason": lob.get("lob_evidence_reason", ""),
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
                "research_only": True,
            }
        )

    label_counts = Counter(str(row["reaction_label"]) for row in out_rows)
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
            "bridge_review_csv": str(BRIDGE_REVIEW_CSV),
            "intraday_history_csv": str(INTRADAY_HISTORY_CSV),
            "intraday_latest_csv": str(INTRADAY_LATEST_CSV),
            "surge_realtime_csv": str(SURGE_REALTIME_CSV),
            "surge_lob_csv": str(SURGE_LOB_CSV),
        },
        "input_rows": {
            "bridge_shadow_rows": len(bridge_rows),
            "intraday_history_rows": len(history_rows),
        },
        "reaction_label_counts": dict(label_counts),
        "rows": out_rows,
    }

    OUT_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    fieldnames = list(out_rows[0].keys()) if out_rows else ["code", "name", "reaction_label", "reaction_reason"]
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    print(json.dumps({"reaction_label_counts": dict(label_counts), "rows": len(out_rows)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
