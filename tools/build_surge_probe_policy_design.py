"""Build a shadow-only policy design for future surge micro-probe entries.

This is a design artifact, not an activation. It converts current shadow probe
candidates into a proposed sizing, loss-cap, kill-rule, and evidence checklist
for later review.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SHADOW_CSV = LOG_DIR / "surge_shadow_probe_candidate_report_latest.csv"
OUT_JSON = LOG_DIR / "surge_probe_policy_design_latest.json"
OUT_CSV = LOG_DIR / "surge_probe_policy_design_latest.csv"

KST = dt.timezone(dt.timedelta(hours=9))


def _now() -> str:
    return dt.datetime.now(tz=KST).isoformat(timespec="seconds")


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return [{str(k): str(v) for k, v in row.items()} for row in csv.DictReader(f)]
    except Exception:
        return []


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value).strip()
        if not text:
            return float(default)
        return float(text)
    except Exception:
        return float(default)


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on", "t"}


def _design_for(row: Dict[str, str]) -> Dict[str, Any]:
    spread = _f(row.get("spread_bps"))
    rvol20 = _f(row.get("rvol20"))
    follow = _f(row.get("return_pct_since_first_seen"))
    trading_value = _f(row.get("trading_value"))
    score = _f(row.get("surge_score_final"))

    # Intentionally conservative: this is only a future policy review envelope.
    size_cap_pct_of_equity = 0.25
    max_daily_probe_slots = 1
    max_loss_pct_of_probe = 1.2
    hard_time_stop_minutes = 10
    stale_lob_max_seconds = 30
    spread_kill_bps = 35.0
    rvol_kill = 3.0
    min_followthrough_confirm_pct = 0.5

    return {
        "code": row.get("code", ""),
        "design_class": "FUTURE_MICRO_PROBE_POLICY_REVIEW",
        "activation_status": "NOT_ACTIVE_SHADOW_ONLY",
        "shadow_probe_candidate": _truthy(row.get("shadow_probe_candidate")),
        "candidate_basis": row.get("shadow_class", ""),
        "detected_surge_type": row.get("detected_surge_type", ""),
        "current_followthrough_pct": round(follow, 6),
        "current_spread_bps": round(spread, 6),
        "current_rvol20": round(rvol20, 6),
        "current_trading_value": round(trading_value, 2),
        "current_surge_score_final": round(score, 6),
        "proposed_size_cap_pct_of_equity": size_cap_pct_of_equity,
        "proposed_max_daily_probe_slots": max_daily_probe_slots,
        "proposed_max_loss_pct_of_probe": max_loss_pct_of_probe,
        "proposed_hard_time_stop_minutes": hard_time_stop_minutes,
        "kill_if_lob_stale_seconds_gt": stale_lob_max_seconds,
        "kill_if_spread_bps_gt": spread_kill_bps,
        "kill_if_rvol20_gt": rvol_kill,
        "kill_if_followthrough_pct_lt": min_followthrough_confirm_pct,
        "kill_if_any_hard_blocker_returns": True,
        "kill_if_orderflow_risk_or_pause": True,
        "reentry_same_day_allowed": False,
        "requires_lob_ok_at_decision_time": True,
        "requires_positive_followthrough_at_decision_time": True,
        "requires_no_existing_position": True,
        "required_evidence_before_live_activation": (
            "shadow_candidate_repeat_or_forward_sample;"
            "entry_to_10m_markout_positive;"
            "no_hard_blocker_reappears;"
            "official_batch_wiring_verified;"
            "manual_policy_approval"
        ),
        "live_order_route_enabled": False,
        "entry_approval_changed": False,
        "trading_allowed": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys()) if rows else ["code"]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    candidates = [row for row in _read_csv(SHADOW_CSV) if _truthy(row.get("shadow_probe_candidate"))]
    rows = [_design_for(row) for row in candidates]
    payload = {
        "generated_at": _now(),
        "scope": "read_only_future_surge_probe_policy_design",
        "policy_note": "Design only. No live entry, order route, threshold, or gate behavior is changed.",
        "source_files": {"shadow_csv": str(SHADOW_CSV)},
        "summary": {
            "rows": len(rows),
            "candidate_codes": [row.get("code") for row in rows],
            "live_order_route_enabled": False,
            "trading_effect": False,
            "policy_effect": False,
            "policy_change_applied": False,
        },
        "rows": rows,
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"out_json": str(OUT_JSON), "out_csv": str(OUT_CSV), "summary": payload["summary"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
