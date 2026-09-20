from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
VERDICT_JSON = LOG_DIR / "surge_ev_conditional_verdict_latest.json"
READINESS_JSON = LOG_DIR / "surge_ev_probe_readiness_latest.json"
OUT_JSON = LOG_DIR / "surge_outcome_promotion_report_latest.json"
OUT_CSV = LOG_DIR / "surge_outcome_promotion_report_latest.csv"


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except Exception:
        return default


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields: List[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    if not fields:
        fields = ["rule"]
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def _promotion_state(row: Dict[str, Any]) -> str:
    n = int(_to_float(row.get("n"), 0.0))
    avg_ret = _to_float(row.get("avg_primary_ret_pct"))
    med_ret = _to_float(row.get("median_primary_ret_pct"))
    stop_rate = _to_float(row.get("stop_rate"), 1.0)
    if n >= 30 and avg_ret > 0 and med_ret > 0 and stop_rate <= 0.30:
        return "POLICY_REVIEW_CANDIDATE_RESEARCH_ONLY"
    if n >= 5 and avg_ret > 0 and med_ret > 0 and stop_rate <= 0.0:
        return "VIRTUAL_PROBE_CANDIDATE"
    if n > 0:
        return "OBSERVE_MORE"
    return "NO_SAMPLE"


def main() -> int:
    now = datetime.now().isoformat(timespec="seconds")
    verdict = _read_json(VERDICT_JSON)
    readiness = _read_json(READINESS_JSON)
    ready_rules = {str(row.get("rule")): row for row in readiness.get("rows") or []}
    rows: List[Dict[str, Any]] = []
    for row in verdict.get("verdicts") or []:
        rule = str(row.get("rule") or "")
        ready = ready_rules.get(rule, {})
        rows.append({
            "rule": rule,
            "n": row.get("n", 0),
            "avg_primary_ret_pct": row.get("avg_primary_ret_pct", ""),
            "median_primary_ret_pct": row.get("median_primary_ret_pct", ""),
            "positive_rows": row.get("positive_rows", ""),
            "stop_rate": row.get("stop_rate", ""),
            "verdict": row.get("verdict", ""),
            "readiness_status": ready.get("readiness_status", ""),
            "promotion_state": _promotion_state(row),
            "next_allowed_layer": "virtual_paper_probe_only" if _promotion_state(row) == "VIRTUAL_PROBE_CANDIDATE" else "observation_only",
            "policy_change": False,
            "entry_approval_changed": False,
            "broker_order_route": False,
            "research_only": True,
        })
    payload = {
        "ts": now,
        "status": "OK",
        "scope": "surge_outcome_promotion_report",
        "source_verdict_json": str(VERDICT_JSON),
        "source_readiness_json": str(READINESS_JSON),
        "summary": {
            "rows": len(rows),
            "virtual_probe_candidates": sum(1 for row in rows if row.get("promotion_state") == "VIRTUAL_PROBE_CANDIDATE"),
            "policy_review_candidates": sum(1 for row in rows if row.get("promotion_state") == "POLICY_REVIEW_CANDIDATE_RESEARCH_ONLY"),
            "policy_change_ready": False,
            "entry_approval_ready": False,
        },
        "risk_contract": {
            "policy_change": False,
            "entry_approval_changed": False,
            "live_order_allowed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "research_only": True,
            "must_not_dispatch": True,
        },
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"status": "OK", **payload["summary"], "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
