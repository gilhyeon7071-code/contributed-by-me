"""Build timepoint markout rows for shadow-only surge probe candidates.

The report maps accumulated shadow-probe markout observations to 3/5/10/15
minute checkpoints. It is read-only and never enables trading.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

HISTORY_CSV = LOG_DIR / "surge_shadow_probe_markout_tracker_history.csv"
OUT_JSON = LOG_DIR / "surge_shadow_probe_timepoint_markout_latest.json"
OUT_CSV = LOG_DIR / "surge_shadow_probe_timepoint_markout_latest.csv"

KST = dt.timezone(dt.timedelta(hours=9))
TARGET_MINUTES = [3, 5, 10, 15]
ON_TIME_LAG_MAX_MIN = 2.0


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


def _dedupe_key(row: Dict[str, str]) -> Tuple[str, str, str, str, str]:
    return (
        _code(row.get("code")),
        str(row.get("followup_first_seen_at") or ""),
        str(row.get("current_price") or ""),
        str(row.get("current_return_pct") or ""),
        str(row.get("tracker_status") or ""),
    )


def _deduped(rows: Iterable[Dict[str, str]]) -> List[Dict[str, str]]:
    seen: set[Tuple[str, str, str, str, str]] = set()
    out: List[Dict[str, str]] = []
    for row in rows:
        key = _dedupe_key(row)
        if not key[0] or key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _select_observation(rows: List[Dict[str, str]], target: int) -> Dict[str, str] | None:
    reached = [row for row in rows if _f(row.get("followup_elapsed_minutes")) >= float(target)]
    if not reached:
        return None
    return sorted(reached, key=lambda row: _f(row.get("followup_elapsed_minutes")))[0]


def _timepoint_status(row: Dict[str, str] | None) -> Tuple[str, str]:
    if row is None:
        return "MISSING_TIMEPOINT", ""
    status = str(row.get("tracker_status") or "")
    current_return = _f(row.get("current_return_pct"))
    if status == "SHADOW_KILL_CONDITION_TRIGGERED":
        return "KILL_TRIGGERED", str(row.get("kill_reasons") or "")
    if current_return > 0:
        return "POSITIVE_MARKOUT", ""
    if current_return < 0:
        return "NEGATIVE_MARKOUT", "NEGATIVE_MARKOUT"
    return "FLAT_MARKOUT", ""


def _row(code: str, target: int, obs: Dict[str, str] | None) -> Dict[str, Any]:
    status, reason = _timepoint_status(obs)
    elapsed = _f(obs.get("followup_elapsed_minutes")) if obs else 0.0
    lag = elapsed - float(target) if obs else None
    if obs is None:
        quality = "NO_SAMPLE"
    elif lag is not None and lag <= ON_TIME_LAG_MAX_MIN:
        quality = "ON_TIME_SAMPLE"
    else:
        quality = "LATE_SAMPLE"
    return {
        "code": code,
        "target_minutes": target,
        "timepoint_status": status,
        "kill_reasons": reason,
        "sample_quality": quality,
        "observed_at": obs.get("observed_at", "") if obs else "",
        "followup_first_seen_at": obs.get("followup_first_seen_at", "") if obs else "",
        "observed_elapsed_minutes": round(elapsed, 6) if obs else None,
        "observation_lag_minutes": round(float(lag), 6) if lag is not None else None,
        "candidate_return_pct": round(_f(obs.get("candidate_return_pct")), 6) if obs else None,
        "current_return_pct": round(_f(obs.get("current_return_pct")), 6) if obs else None,
        "markout_delta_pct_points": round(_f(obs.get("markout_delta_pct_points")), 6) if obs else None,
        "baseline_price": round(_f(obs.get("baseline_price")), 6) if obs else None,
        "current_price": round(_f(obs.get("current_price")), 6) if obs else None,
        "live_order_route_enabled": False,
        "entry_approval_changed": False,
        "trading_allowed": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys()) if rows else ["code", "target_minutes"]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    raw = _read_csv(HISTORY_CSV)
    history = _deduped(raw)
    grouped: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in history:
        grouped[_code(row.get("code"))].append(row)

    rows: List[Dict[str, Any]] = []
    for code, code_rows in sorted(grouped.items()):
        code_rows = sorted(code_rows, key=lambda row: _f(row.get("followup_elapsed_minutes")))
        for target in TARGET_MINUTES:
            rows.append(_row(code, target, _select_observation(code_rows, target)))

    status_counts = Counter(str(row.get("timepoint_status") or "") for row in rows)
    quality_counts = Counter(str(row.get("sample_quality") or "") for row in rows)
    payload = {
        "generated_at": _now(),
        "scope": "read_only_surge_shadow_probe_timepoint_markout",
        "policy_note": "Timepoint review only. No live entry, order route, threshold, or gate behavior is changed.",
        "target_minutes": TARGET_MINUTES,
        "source_files": {"history_csv": str(HISTORY_CSV)},
        "summary": {
            "raw_history_rows": len(raw),
            "distinct_history_rows": len(history),
            "codes": sorted(grouped.keys()),
            "rows": len(rows),
            "timepoint_status_counts": dict(sorted(status_counts.items())),
            "sample_quality_counts": dict(sorted(quality_counts.items())),
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
