"""Track forward markout for shadow-only surge probe candidates.

This tracker is read-only. It records whether a shadow candidate keeps or loses
follow-through after candidate creation. It never opens an entry route.
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
POLICY_CSV = LOG_DIR / "surge_probe_policy_design_latest.csv"
FOLLOWUP_CSV = LOG_DIR / "candidate_action_followup_latest.csv"

OUT_JSON = LOG_DIR / "surge_shadow_probe_markout_tracker_latest.json"
OUT_CSV = LOG_DIR / "surge_shadow_probe_markout_tracker_latest.csv"
HISTORY_CSV = LOG_DIR / "surge_shadow_probe_markout_tracker_history.csv"
HISTORY_JSONL = LOG_DIR / "surge_shadow_probe_markout_tracker_history.jsonl"

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


def _code(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text.zfill(6)[-6:] if text else ""


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


def _by_code(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code:
            out[code] = row
    return out


def _followup_by_code(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    selected: Dict[str, Dict[str, str]] = {}
    fallback: Dict[str, Dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if not code:
            continue
        if code not in fallback:
            fallback[code] = row
        if str(row.get("source") or "") == "no_lob_recheck":
            selected[code] = row
    for code, row in fallback.items():
        selected.setdefault(code, row)
    return selected


def _row_for(candidate: Dict[str, str], policy: Dict[str, str], follow: Dict[str, str], generated_at: str) -> Dict[str, Any]:
    code = _code(candidate.get("code"))
    candidate_return = _f(candidate.get("return_pct_since_first_seen"))
    current_return = _f(follow.get("return_pct_since_first_seen"))
    delta_pp = current_return - candidate_return
    min_follow = _f(policy.get("kill_if_followthrough_pct_lt"), _f(candidate.get("min_followthrough_pct"), 0.5))
    max_loss_pct = _f(policy.get("proposed_max_loss_pct_of_probe"), 1.2)
    current_price_available = _truthy(follow.get("current_price_available"))

    kill_reasons: List[str] = []
    if not current_price_available:
        kill_reasons.append("CURRENT_PRICE_UNAVAILABLE")
    if current_return < min_follow:
        kill_reasons.append("FOLLOWTHROUGH_BELOW_MIN")
    if current_return < 0:
        kill_reasons.append("NEGATIVE_MARKOUT")
    if delta_pp <= -max_loss_pct:
        kill_reasons.append("MARKOUT_DRAWDOWN_EXCEEDS_PROBE_LOSS_CAP")

    if kill_reasons:
        tracker_status = "SHADOW_KILL_CONDITION_TRIGGERED"
    elif delta_pp > 0:
        tracker_status = "SHADOW_MARKOUT_IMPROVING"
    else:
        tracker_status = "SHADOW_MARKOUT_HOLDING"

    return {
        "observed_at": generated_at,
        "code": code,
        "shadow_probe_candidate": _truthy(candidate.get("shadow_probe_candidate")),
        "tracker_status": tracker_status,
        "kill_reasons": "|".join(kill_reasons),
        "candidate_return_pct": round(candidate_return, 6),
        "current_return_pct": round(current_return, 6),
        "markout_delta_pct_points": round(delta_pp, 6),
        "min_followthrough_pct": round(min_follow, 6),
        "max_loss_pct_of_probe": round(max_loss_pct, 6),
        "followup_source": follow.get("source", ""),
        "followup_first_seen_at": follow.get("first_seen_at", ""),
        "followup_elapsed_minutes": round(_f(follow.get("elapsed_minutes")), 6),
        "baseline_price": round(_f(follow.get("baseline_price")), 6),
        "current_price": round(_f(follow.get("current_price")), 6),
        "current_price_available": current_price_available,
        "current_change_pct": round(_f(follow.get("current_change_pct")), 6),
        "current_trading_value": round(_f(follow.get("current_trading_value")), 2),
        "live_order_route_enabled": False,
        "entry_approval_changed": False,
        "trading_allowed": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys()) if rows else ["observed_at", "code"]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _append_history(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    HISTORY_CSV.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys())
    write_header = not HISTORY_CSV.exists() or HISTORY_CSV.stat().st_size == 0
    with HISTORY_CSV.open("a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)
    with HISTORY_JSONL.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> int:
    generated_at = _now()
    candidates = [row for row in _read_csv(SHADOW_CSV) if _truthy(row.get("shadow_probe_candidate"))]
    policy_by_code = _by_code(_read_csv(POLICY_CSV))
    follow_by_code = _followup_by_code(_read_csv(FOLLOWUP_CSV))
    rows = [
        _row_for(candidate, policy_by_code.get(_code(candidate.get("code")), {}), follow_by_code.get(_code(candidate.get("code")), {}), generated_at)
        for candidate in candidates
    ]
    payload = {
        "generated_at": generated_at,
        "scope": "read_only_surge_shadow_probe_markout_tracker",
        "policy_note": "Tracker only. No live entry, order route, threshold, or gate behavior is changed.",
        "source_files": {
            "shadow_csv": str(SHADOW_CSV),
            "policy_csv": str(POLICY_CSV),
            "followup_csv": str(FOLLOWUP_CSV),
        },
        "summary": {
            "rows": len(rows),
            "candidate_codes": [row.get("code") for row in rows],
            "kill_condition_rows": sum(1 for row in rows if row.get("tracker_status") == "SHADOW_KILL_CONDITION_TRIGGERED"),
            "live_order_route_enabled": False,
            "trading_effect": False,
            "policy_effect": False,
            "policy_change_applied": False,
        },
        "rows": rows,
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, rows)
    _append_history(rows)
    print(json.dumps({"out_json": str(OUT_JSON), "out_csv": str(OUT_CSV), "history_csv": str(HISTORY_CSV), "summary": payload["summary"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
