from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"
IN_SHADOW = LOGS / "news_signal_shadow_stage_latest.csv"
IN_STATUS = LOGS / "news_signal_shadow_stage_latest.json"
OUT_CSV = LOGS / "news_signal_shadow_diagnostic_latest.csv"
OUT_JSON = LOGS / "news_signal_shadow_diagnostic_latest.json"

VERSION = "news_signal_shadow_diagnostic_v1"
CONFIRM_MARKET_THRESHOLD = 0.50
CONFIRM_TEXT_THRESHOLD = 0.15


def _now_kst() -> str:
    return datetime.now(timezone(timedelta(hours=9))).isoformat(timespec="seconds")


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
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [dict(r) for r in csv.DictReader(f)]


def _f(row: Dict[str, str], key: str, default: float = 0.0) -> float:
    try:
        raw = str(row.get(key, "") or "").strip()
        if not raw:
            return default
        return float(raw)
    except Exception:
        return default


def _s(row: Dict[str, str], key: str) -> str:
    return str(row.get(key, "") or "").strip()


def _diagnose(row: Dict[str, str]) -> Dict[str, Any]:
    stage = _s(row, "news_signal_stage")
    text_strength = _f(row, "news_text_strength", 0.0)
    market_score = _f(row, "news_market_corroboration_score", 0.0)
    reason = _s(row, "news_signal_reason")
    publisher_gate_state = _s(row, "publisher_gate_state").upper()
    publisher_gate_applied = _s(row, "publisher_gate_applied").lower() == "true"
    publisher_verified = publisher_gate_state == "PUBLISHER_VERIFIED"
    publisher_corroboration_allowed = publisher_gate_state in {
        "PUBLISHER_VERIFIED",
        "PUBLISHER_AUTHENTICATED_PROVISIONAL",
    }

    blockers: List[str] = []
    if text_strength < CONFIRM_TEXT_THRESHOLD:
        blockers.append("TEXT_STRENGTH_LT_CONFIRM")
    if market_score < CONFIRM_MARKET_THRESHOLD:
        blockers.append("MARKET_CORROBORATION_LT_CONFIRM")
    if "lob_missing" in reason:
        blockers.append("LOB_MISSING")
    if "spread_wide=" in reason:
        blockers.append("SPREAD_WIDE")
    if "orderflow=PAUSE" in reason or "orderflow=CAUTION" in reason:
        blockers.append("ORDERFLOW_CAUTION")
    if stage in {"NO_TEXT_SIGNAL", "STALE_TEXT_SIGNAL", "SOURCE_FRESH_OBSERVED_SHADOW"}:
        blockers.append(stage)
    if stage == "CONFIRMED_SHADOW" and not publisher_verified:
        blockers.append("PUBLISHER_NOT_VERIFIED")
    elif stage in {"PRE_SIGNAL", "PUBLISHER_UNVERIFIED_SHADOW"} and not publisher_corroboration_allowed:
        blockers.append("PUBLISHER_NOT_VERIFIED")

    market_gap = max(0.0, CONFIRM_MARKET_THRESHOLD - market_score)
    text_gap = max(0.0, CONFIRM_TEXT_THRESHOLD - text_strength)

    if stage == "CONFIRMED_SHADOW" and not publisher_verified:
        next_action = "WAIT_FOR_PUBLISHER_VERIFICATION"
    elif stage in {"PRE_SIGNAL", "PUBLISHER_UNVERIFIED_SHADOW"} and not publisher_corroboration_allowed:
        next_action = "WAIT_FOR_PUBLISHER_VERIFICATION"
    elif stage == "PRE_SIGNAL" and market_gap > 0:
        next_action = "WAIT_FOR_MARKET_CORROBORATION"
    elif stage == "PRE_SIGNAL":
        next_action = "REVIEW_CONFIRM_RULE"
    elif stage == "NO_TEXT_SIGNAL":
        next_action = "WAIT_FOR_TEXT_SOURCE"
    elif stage == "STALE_TEXT_SIGNAL":
        next_action = "REFRESH_TEXT_SOURCE"
    elif stage == "SOURCE_FRESH_OBSERVED_SHADOW":
        next_action = "REVIEW_OBSERVED_FRESH_SOURCE"
    elif stage == "BLOCK_SHADOW":
        next_action = "KEEP_BLOCK_SHADOW"
    else:
        next_action = "KEEP_OBSERVING"

    return {
        "code": _s(row, "code"),
        "name": _s(row, "name"),
        "news_signal_stage": stage,
        "news_signal_direction": _s(row, "news_signal_direction"),
        "news_text_strength": round(text_strength, 6),
        "news_market_corroboration_score": round(market_score, 6),
        "confirm_text_threshold": CONFIRM_TEXT_THRESHOLD,
        "confirm_market_threshold": CONFIRM_MARKET_THRESHOLD,
        "text_gap_to_confirm": round(text_gap, 6),
        "market_gap_to_confirm": round(market_gap, 6),
        "diagnostic_blockers": "|".join(blockers) if blockers else "NONE",
        "diagnostic_next_action": next_action,
        "publisher_gate_state": publisher_gate_state,
        "publisher_trust": _s(row, "publisher_trust"),
        "publisher_crypto_auth_status": _s(row, "publisher_crypto_auth_status"),
        "publisher_gate_applied": str(publisher_gate_applied).lower(),
        "shadow_only": "true",
        "trading_effect": "false",
    }


def main() -> int:
    rows = _read_csv(IN_SHADOW)
    diagnostics = [_diagnose(r) for r in rows]
    LOGS.mkdir(parents=True, exist_ok=True)

    fields = [
        "code",
        "name",
        "news_signal_stage",
        "news_signal_direction",
        "news_text_strength",
        "news_market_corroboration_score",
        "confirm_text_threshold",
        "confirm_market_threshold",
        "text_gap_to_confirm",
        "market_gap_to_confirm",
        "diagnostic_blockers",
        "diagnostic_next_action",
        "publisher_gate_state",
        "publisher_trust",
        "publisher_crypto_auth_status",
        "publisher_gate_applied",
        "shadow_only",
        "trading_effect",
    ]
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(diagnostics)

    blocker_counts: Counter[str] = Counter()
    action_counts: Counter[str] = Counter()
    for row in diagnostics:
        action_counts[str(row["diagnostic_next_action"])] += 1
        for item in str(row["diagnostic_blockers"]).split("|"):
            if item and item != "NONE":
                blocker_counts[item] += 1

    pre_rows = [r for r in diagnostics if r["news_signal_stage"] == "PRE_SIGNAL"]
    avg_pre_market_gap = (
        sum(float(r["market_gap_to_confirm"]) for r in pre_rows) / len(pre_rows)
        if pre_rows
        else 0.0
    )
    status = {
        "generated_at": _now_kst(),
        "version": VERSION,
        "status": "PASS",
        "quality": "PASS" if rows else "WARN",
        "reason": "ok" if rows else "shadow_input_empty",
        "input_csv": str(IN_SHADOW),
        "input_status": str(IN_STATUS),
        "output_csv": str(OUT_CSV),
        "rows": len(diagnostics),
        "pre_signal_rows": len(pre_rows),
        "avg_pre_signal_market_gap_to_confirm": round(avg_pre_market_gap, 6),
        "blocker_counts": dict(blocker_counts),
        "next_action_counts": dict(action_counts),
        "shadow_only": True,
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "consumed_by_order_path": False,
        "source_stage_status": _read_json(IN_STATUS),
    }
    OUT_JSON.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "status": status["status"],
                "rows": status["rows"],
                "pre_signal_rows": status["pre_signal_rows"],
                "blocker_counts": status["blocker_counts"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
