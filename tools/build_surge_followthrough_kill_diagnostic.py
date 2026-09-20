"""Diagnose whether surge shadow probe kills are follow-through specific.

This report is read-only. It checks whether lowering the follow-through floor
would actually rescue a candidate, or whether other hard markout failures still
dominate the decision.
"""
from __future__ import annotations

import csv
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

TRACKER_CSV = LOG_DIR / "surge_shadow_probe_markout_tracker_latest.csv"
POLICY_CSV = LOG_DIR / "surge_probe_policy_design_latest.csv"

OUT_JSON = LOG_DIR / "surge_followthrough_kill_diagnostic_latest.json"
OUT_CSV = LOG_DIR / "surge_followthrough_kill_diagnostic_latest.csv"

KST = timezone(timedelta(hours=9))
THRESHOLD_GRID = [-1.0, -0.5, 0.0, 0.25, 0.5, 1.0]


def _now() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: List[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _code(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text.isdigit() else text


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _by_code(rows: Iterable[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    return {_code(row.get("code")): row for row in rows if _code(row.get("code"))}


def _classify(current_return: float, delta_pp: float, min_follow: float, max_loss_pct: float, price_available: bool) -> List[str]:
    reasons: List[str] = []
    if not price_available:
        reasons.append("CURRENT_PRICE_UNAVAILABLE")
    if current_return < min_follow:
        reasons.append("FOLLOWTHROUGH_BELOW_MIN")
    if current_return < 0:
        reasons.append("NEGATIVE_MARKOUT")
    if delta_pp <= -max_loss_pct:
        reasons.append("MARKOUT_DRAWDOWN_EXCEEDS_PROBE_LOSS_CAP")
    return reasons


def _dominant_cause(reasons: List[str], would_pass_without_follow: bool) -> str:
    if not reasons:
        return "NO_KILL"
    if "CURRENT_PRICE_UNAVAILABLE" in reasons:
        return "DATA_UNAVAILABLE_DOMINANT"
    if not would_pass_without_follow:
        if "NEGATIVE_MARKOUT" in reasons and "MARKOUT_DRAWDOWN_EXCEEDS_PROBE_LOSS_CAP" in reasons:
            return "NEGATIVE_MARKOUT_AND_DRAWDOWN_DOMINANT"
        if "MARKOUT_DRAWDOWN_EXCEEDS_PROBE_LOSS_CAP" in reasons:
            return "DRAWDOWN_DOMINANT"
        if "NEGATIVE_MARKOUT" in reasons:
            return "NEGATIVE_MARKOUT_DOMINANT"
    if reasons == ["FOLLOWTHROUGH_BELOW_MIN"]:
        return "FOLLOWTHROUGH_ONLY"
    return "MIXED_KILL"


def _row(row: Dict[str, str], policy: Dict[str, str]) -> Dict[str, Any]:
    code = _code(row.get("code"))
    candidate_return = _f(row.get("candidate_return_pct"))
    current_return = _f(row.get("current_return_pct"))
    delta_pp = _f(row.get("markout_delta_pct_points"))
    min_follow = _f(row.get("min_followthrough_pct"), _f(policy.get("kill_if_followthrough_pct_lt"), 0.5))
    max_loss_pct = _f(row.get("max_loss_pct_of_probe"), _f(policy.get("proposed_max_loss_pct_of_probe"), 1.2))
    price_available = _truthy(row.get("current_price_available"))
    current_reasons = _classify(current_return, delta_pp, min_follow, max_loss_pct, price_available)
    reasons_without_follow = [reason for reason in current_reasons if reason != "FOLLOWTHROUGH_BELOW_MIN"]
    would_pass_without_follow = len(reasons_without_follow) == 0

    grid_status: List[str] = []
    pass_thresholds: List[str] = []
    for threshold in THRESHOLD_GRID:
        reasons = _classify(current_return, delta_pp, threshold, max_loss_pct, price_available)
        status = "PASS" if not reasons else "KILL"
        grid_status.append(f"{threshold:g}:{status}({'+'.join(reasons) if reasons else 'NONE'})")
        if status == "PASS":
            pass_thresholds.append(f"{threshold:g}")

    followthrough_relaxation_effective = bool(pass_thresholds)
    return {
        "code": code,
        "tracker_status": row.get("tracker_status", ""),
        "dominant_cause": _dominant_cause(current_reasons, would_pass_without_follow),
        "current_kill_reasons": "|".join(current_reasons),
        "candidate_return_pct": round(candidate_return, 6),
        "current_return_pct": round(current_return, 6),
        "markout_delta_pct_points": round(delta_pp, 6),
        "current_min_followthrough_pct": round(min_follow, 6),
        "max_loss_pct_of_probe": round(max_loss_pct, 6),
        "would_pass_if_followthrough_floor_removed": would_pass_without_follow,
        "followthrough_relaxation_effective_in_grid": followthrough_relaxation_effective,
        "passing_followthrough_thresholds_in_grid": "|".join(pass_thresholds),
        "threshold_grid_result": "|".join(grid_status),
        "observed_elapsed_minutes": round(_f(row.get("followup_elapsed_minutes")), 6),
        "current_price_available": price_available,
        "live_order_route_enabled": False,
        "entry_approval_changed": False,
        "trading_allowed": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }


def main() -> int:
    generated_at = _now()
    policy_by_code = _by_code(_read_csv(POLICY_CSV))
    tracker_rows = _read_csv(TRACKER_CSV)
    rows = [_row(row, policy_by_code.get(_code(row.get("code")), {})) for row in tracker_rows]
    payload = {
        "generated_at": generated_at,
        "scope": "read_only_surge_followthrough_kill_diagnostic",
        "policy_note": "Diagnostic only. No live entry, order route, threshold, or gate behavior is changed.",
        "threshold_grid": THRESHOLD_GRID,
        "source_files": {
            "tracker_csv": str(TRACKER_CSV),
            "policy_csv": str(POLICY_CSV),
        },
        "summary": {
            "rows": len(rows),
            "dominant_cause_counts": {key: sum(1 for row in rows if row.get("dominant_cause") == key) for key in sorted({str(row.get("dominant_cause")) for row in rows})},
            "followthrough_relaxation_effective_rows": sum(1 for row in rows if row.get("followthrough_relaxation_effective_in_grid")),
            "live_order_route_enabled": False,
            "trading_effect": False,
            "policy_effect": False,
            "policy_change_applied": False,
        },
        "rows": rows,
    }
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"out_json": str(OUT_JSON), "out_csv": str(OUT_CSV), "summary": payload["summary"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
