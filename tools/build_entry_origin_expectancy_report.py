"""Build a read-only expectancy report by realized entry origin.

The report is intentionally observational: it summarizes current paper fills and
nearby action-plan cohorts without changing entry thresholds, gates, orders, or
policy.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

LEDGER_HISTORY_CSV = LOG_DIR / "candidate_decision_outcome_ledger_history.csv"
TIMING_JSON = LOG_DIR / "entry_timing_outcome_report_latest.json"
PLAN_CSV = LOG_DIR / "candidate_action_plan_latest.csv"

OUT_JSON = LOG_DIR / "entry_origin_expectancy_report_latest.json"
OUT_CSV = LOG_DIR / "entry_origin_expectancy_report_latest.csv"

KST = dt.timezone(dt.timedelta(hours=9))
MIN_DECISION_N = 5


def _now() -> str:
    return dt.datetime.now(tz=KST).isoformat(timespec="seconds")


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


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


def _stats(vals: Iterable[float]) -> Dict[str, Any]:
    xs = [float(v) for v in vals]
    if not xs:
        return {
            "observed_n": 0,
            "avg_return_pct": None,
            "median_return_pct": None,
            "win_rate": None,
            "positive_n": 0,
            "negative_n": 0,
            "min_return_pct": None,
            "max_return_pct": None,
        }
    pos = sum(1 for x in xs if x > 0)
    neg = sum(1 for x in xs if x < 0)
    return {
        "observed_n": len(xs),
        "avg_return_pct": round(sum(xs) / len(xs), 6),
        "median_return_pct": round(float(median(xs)), 6),
        "win_rate": round(pos / len(xs), 6),
        "positive_n": pos,
        "negative_n": neg,
        "min_return_pct": round(min(xs), 6),
        "max_return_pct": round(max(xs), 6),
    }


def _sample_status(n: int) -> str:
    if n <= 0:
        return "NO_SAMPLE"
    if n < MIN_DECISION_N:
        return "PROVISIONAL_SMALL_N"
    return "EVALUABLE"


def _direction(avg: Any, win_rate: Any) -> str:
    if avg is None or win_rate is None:
        return "NO_SIGNAL"
    if float(avg) > 0 and float(win_rate) >= 0.5:
        return "FAVORABLE_CURRENT_MARK"
    if float(avg) < 0 and float(win_rate) <= 0.5:
        return "UNFAVORABLE_CURRENT_MARK"
    return "MIXED_CURRENT_MARK"


def _action_hint(origin: str, sample_status: str, direction: str) -> str:
    if sample_status != "EVALUABLE":
        if direction == "FAVORABLE_CURRENT_MARK":
            return "OBSERVE_AND_ACCUMULATE_FAST"
        if direction == "UNFAVORABLE_CURRENT_MARK":
            return "KEEP_SEPARATE_AND_ACCUMULATE"
        return "ACCUMULATE_SAMPLE"
    if direction == "FAVORABLE_CURRENT_MARK":
        return "CANDIDATE_FOR_POLICY_REVIEW"
    if direction == "UNFAVORABLE_CURRENT_MARK":
        return "CANDIDATE_FOR_TIGHTEN_OR_SIZE_LIMIT"
    return "REQUIRES_CONDITIONAL_SPLIT"


def _post_entry_origin_rows() -> List[Dict[str, Any]]:
    rows = _read_csv(LEDGER_HISTORY_CSV)
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        
        # Only consider executed trades (ENTRY decisions or actually filled trades)
        if row.get("decision_type") != "ENTRY" and not row.get("actual_trade_id"):
            continue
            
        origin = str(row.get("decision_reason") or "UNKNOWN")
        groups.setdefault(origin, []).append(row)

    out: List[Dict[str, Any]] = []
    for origin, items in sorted(groups.items()):
        vals = []
        for r in items:
            # Prefer actual PnL for closed/filled trades
            if r.get("actual_pnl_pct"):
                vals.append(_f(r["actual_pnl_pct"]))
            # Fallback to current return for open/pending positions
            elif r.get("ret_close_or_latest_pct"):
                vals.append(_f(r["ret_close_or_latest_pct"]))
                
        stat = _stats(vals)
        sample_status = _sample_status(len(items))
        direction = _direction(stat["avg_return_pct"], stat["win_rate"])
        out.append(
            {
                "section": "realized_entry_origin_current_mark",
                "group": origin,
                "n": len(items),
                **stat,
                "sample_status": sample_status,
                "direction": direction,
                "action_hint": _action_hint(origin, sample_status, direction),
                "codes": ",".join(str(r.get("code") or "") for r in items),
            }
        )
    return out


def _timing_section_rows(section: str, label: str) -> List[Dict[str, Any]]:
    doc = _read_json(TIMING_JSON)
    rows = ((doc.get("summary_sections") or {}).get(section) or []) if isinstance(doc.get("summary_sections"), dict) else []
    out: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        n = int(_f(row.get("n"), 0.0))
        sample_status = _sample_status(n)
        direction = _direction(row.get("avg_return_pct"), row.get("win_rate"))
        out.append(
            {
                "section": label,
                "group": str(row.get("group") or "UNKNOWN"),
                "n": n,
                "observed_n": int(_f(row.get("observed_n"), 0.0)),
                "avg_return_pct": row.get("avg_return_pct"),
                "median_return_pct": row.get("median_return_pct"),
                "win_rate": row.get("win_rate"),
                "positive_n": int(_f(row.get("positive_n"), 0.0)),
                "negative_n": int(_f(row.get("negative_n"), 0.0)),
                "min_return_pct": row.get("min_return_pct"),
                "max_return_pct": row.get("max_return_pct"),
                "sample_status": sample_status,
                "direction": direction,
                "action_hint": _action_hint(str(row.get("group") or "UNKNOWN"), sample_status, direction),
                "codes": "",
            }
        )
    return out


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "section",
        "group",
        "n",
        "observed_n",
        "avg_return_pct",
        "median_return_pct",
        "win_rate",
        "positive_n",
        "negative_n",
        "min_return_pct",
        "max_return_pct",
        "sample_status",
        "direction",
        "action_hint",
        "codes",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    realized = _post_entry_origin_rows()
    timing_post = _timing_section_rows("post_entry_origin", "timing_report_post_entry_origin")
    plan_next = _timing_section_rows("plan_next_action", "action_plan_next_action")
    rows = realized + timing_post + plan_next
    payload = {
        "generated_at": _now(),
        "schema_version": "entry_origin_expectancy_report_v1",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "sample_policy": {
            "min_decision_n": MIN_DECISION_N,
            "small_n_is_not_stop": True,
            "small_n_use": "provisional_direction_and_next_sampling_priority",
        },
        "artifacts": {
            "candidate_decision_outcome_ledger_history": str(LEDGER_HISTORY_CSV),
            "entry_timing_outcome_report": str(TIMING_JSON),
            "candidate_action_plan": str(PLAN_CSV),
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
        },
        "rows": rows,
        "interpretation": {
            "return_basis": "ledger history for realized entries; action-plan rows use return since first seen",
            "decision_boundary": "this report does not approve entries or change gates; it separates expectancy cohorts for review",
        },
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"status": "OK", "rows": len(rows), "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
