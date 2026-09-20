"""Summarize repeated shadow-probe markout evidence.

This report gates future live-probe review on repeated forward evidence rather
than a single candidate snapshot. It is read-only and does not activate trading.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

HISTORY_CSV = LOG_DIR / "surge_shadow_probe_markout_tracker_history.csv"
LATEST_CSV = LOG_DIR / "surge_shadow_probe_markout_tracker_latest.csv"
OUT_JSON = LOG_DIR / "surge_shadow_probe_markout_summary_latest.json"
OUT_CSV = LOG_DIR / "surge_shadow_probe_markout_summary_latest.csv"

KST = dt.timezone(dt.timedelta(hours=9))

# Review-only activation prerequisites. These do not enable orders.
MIN_DISTINCT_OBSERVATIONS = 5
MIN_POSITIVE_MARKOUT_RATE = 0.60
MAX_KILL_RATE = 0.20
MIN_AVG_MARKOUT_DELTA_PP = 0.30


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


def _dedupe_key(row: Dict[str, str]) -> Tuple[str, str, str, str, str]:
    return (
        _code(row.get("code")),
        str(row.get("followup_first_seen_at") or ""),
        str(row.get("current_price") or ""),
        str(row.get("current_return_pct") or ""),
        str(row.get("tracker_status") or ""),
    )


def _deduped_history(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen: set[Tuple[str, str, str, str, str]] = set()
    out: List[Dict[str, str]] = []
    for row in rows:
        key = _dedupe_key(row)
        if not key[0] or key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _latest_by_code(rows: Iterable[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    latest: Dict[str, Dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code:
            latest[code] = row
    return latest


def _summarize_code(code: str, rows: List[Dict[str, str]], latest: Dict[str, str]) -> Dict[str, Any]:
    n = len(rows)
    deltas = [_f(row.get("markout_delta_pct_points")) for row in rows]
    current_returns = [_f(row.get("current_return_pct")) for row in rows]
    kill_n = sum(1 for row in rows if str(row.get("tracker_status") or "") == "SHADOW_KILL_CONDITION_TRIGGERED")
    positive_n = sum(1 for row in rows if _f(row.get("current_return_pct")) > 0)
    latest_status = str(latest.get("tracker_status") or "")
    latest_kill = latest_status == "SHADOW_KILL_CONDITION_TRIGGERED"
    avg_delta = mean(deltas) if deltas else 0.0
    avg_current = mean(current_returns) if current_returns else 0.0
    positive_rate = positive_n / n if n else 0.0
    kill_rate = kill_n / n if n else 0.0

    checks = {
        "enough_distinct_observations": n >= MIN_DISTINCT_OBSERVATIONS,
        "positive_markout_rate_ok": positive_rate >= MIN_POSITIVE_MARKOUT_RATE,
        "kill_rate_ok": kill_rate <= MAX_KILL_RATE,
        "avg_markout_delta_ok": avg_delta >= MIN_AVG_MARKOUT_DELTA_PP,
        "latest_not_kill": not latest_kill,
    }
    failed = [key for key, ok in checks.items() if not ok]
    if not failed:
        review_status = "LIVE_PROBE_REVIEW_ELIGIBLE_SHADOW_ONLY"
    elif n < MIN_DISTINCT_OBSERVATIONS:
        review_status = "INSUFFICIENT_REPEATED_EVIDENCE"
    else:
        review_status = "NOT_ELIGIBLE_BY_MARKOUT"

    kill_reasons = Counter()
    for row in rows:
        for token in str(row.get("kill_reasons") or "").split("|"):
            token = token.strip()
            if token:
                kill_reasons[token] += 1

    return {
        "code": code,
        "review_status": review_status,
        "failed_review_checks": "|".join(failed),
        "distinct_observation_n": n,
        "min_distinct_observations": MIN_DISTINCT_OBSERVATIONS,
        "positive_markout_n": positive_n,
        "positive_markout_rate": round(positive_rate, 6),
        "min_positive_markout_rate": MIN_POSITIVE_MARKOUT_RATE,
        "kill_n": kill_n,
        "kill_rate": round(kill_rate, 6),
        "max_kill_rate": MAX_KILL_RATE,
        "avg_markout_delta_pct_points": round(avg_delta, 6),
        "min_avg_markout_delta_pct_points": MIN_AVG_MARKOUT_DELTA_PP,
        "avg_current_return_pct": round(avg_current, 6),
        "latest_tracker_status": latest_status,
        "latest_kill_reasons": latest.get("kill_reasons", ""),
        "latest_current_return_pct": round(_f(latest.get("current_return_pct")), 6),
        "latest_markout_delta_pct_points": round(_f(latest.get("markout_delta_pct_points")), 6),
        "kill_reason_counts_json": json.dumps(dict(sorted(kill_reasons.items())), ensure_ascii=False, sort_keys=True),
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
    raw_history = _read_csv(HISTORY_CSV)
    history = _deduped_history(raw_history)
    latest_by_code = _latest_by_code(_read_csv(LATEST_CSV))
    grouped: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in history:
        grouped[_code(row.get("code"))].append(row)
    rows = [
        _summarize_code(code, code_rows, latest_by_code.get(code, code_rows[-1] if code_rows else {}))
        for code, code_rows in sorted(grouped.items())
        if code
    ]
    payload = {
        "generated_at": _now(),
        "scope": "read_only_surge_shadow_probe_markout_summary",
        "policy_note": "Repeated-evidence summary only. No live entry, order route, threshold, or gate behavior is changed.",
        "review_thresholds": {
            "min_distinct_observations": MIN_DISTINCT_OBSERVATIONS,
            "min_positive_markout_rate": MIN_POSITIVE_MARKOUT_RATE,
            "max_kill_rate": MAX_KILL_RATE,
            "min_avg_markout_delta_pct_points": MIN_AVG_MARKOUT_DELTA_PP,
        },
        "source_files": {
            "history_csv": str(HISTORY_CSV),
            "latest_csv": str(LATEST_CSV),
        },
        "summary": {
            "raw_history_rows": len(raw_history),
            "distinct_history_rows": len(history),
            "codes": [row.get("code") for row in rows],
            "eligible_codes": [row.get("code") for row in rows if row.get("review_status") == "LIVE_PROBE_REVIEW_ELIGIBLE_SHADOW_ONLY"],
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
