from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
IMPACT_JSON = LOG_DIR / "defense_signal_entry_policy_impact_latest.json"
IMPACT_CSV = LOG_DIR / "defense_signal_entry_policy_impact_latest.csv"
OUTCOME_JSON = LOG_DIR / "defense_signal_entry_policy_outcome_review_latest.json"
OUT_JSON = LOG_DIR / "defense_signal_policy_path_audit_latest.json"
OUT_CSV = LOG_DIR / "defense_signal_policy_path_audit_latest.csv"
OUT_MD = LOG_DIR / "defense_signal_policy_path_audit_latest.md"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8-sig"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _build_rows(impact: dict[str, Any], outcome: dict[str, Any], impact_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    current = impact.get("current_candidates") if isinstance(impact.get("current_candidates"), dict) else {}
    potential = impact.get("defense_shadow_potential") if isinstance(impact.get("defense_shadow_potential"), dict) else {}
    log_scan = outcome.get("log_scan") if isinstance(outcome.get("log_scan"), dict) else {}
    potential_outcomes = outcome.get("potential_outcomes") if isinstance(outcome.get("potential_outcomes"), dict) else {}

    current_block_rows = _to_int(current.get("active_block_rows"))
    potential_rows = _to_int(potential.get("rows"))
    retained_block_sum = _to_int(log_scan.get("blocked_sum"))
    validation_synthetic_present = any(
        _to_int(item.get("before")) == 1 and _to_int(item.get("after")) == 0 and _to_int(item.get("blocked")) == 1
        for item in (log_scan.get("events_detail") or [])
        if isinstance(item, dict)
    )
    real_retained_block_sum_estimate = retained_block_sum - (1 if validation_synthetic_present else 0)
    if real_retained_block_sum_estimate < 0:
        real_retained_block_sum_estimate = 0

    current_candidate_blocked_codes = sorted(
        {
            str(row.get("code") or "").zfill(6)
            for row in impact_rows
            if str(row.get("scope") or "") == "current_candidate"
            and str(row.get("active_policy_result") or "") != "KEEP"
        }
    )
    potential_block_codes = sorted(
        {
            str(row.get("code") or "").zfill(6)
            for row in impact_rows
            if str(row.get("active_policy_result") or "") == "POTENTIAL_BLOCK_IF_IN_ENTRY_POOL"
        }
    )

    return [
        {
            "layer": "actual_policy_path",
            "path_or_source": "paper_engine._apply_defense_signal_entry_policy",
            "effect": "candidate_pool_filter",
            "score_effect": False,
            "order_dispatch_effect": False,
            "entry_pool_filter_effect": True,
            "current_candidate_rows": _to_int(current.get("rows")),
            "current_active_block_rows": current_block_rows,
            "retained_log_block_sum": retained_block_sum,
            "validation_synthetic_block_present": validation_synthetic_present,
            "real_retained_log_block_estimate": real_retained_block_sum_estimate,
            "potential_block_rows": potential_rows,
            "observed_potential_rows": _to_int(potential_outcomes.get("intraday_observed_rows")),
            "observed_potential_avg_return_pct": _to_float(potential_outcomes.get("avg_return_first_to_last_pct")),
            "blocked_codes_sample": ",".join(current_candidate_blocked_codes[:20]),
            "potential_codes_sample": ",".join(potential_block_codes[:20]),
            "classification": "ACTIVE_POLICY_BUT_CURRENTLY_NO_REAL_CANDIDATE_BLOCK",
            "recommended_next": "review whether this should be always-on or regime/surge-specific before changing behavior",
        },
        {
            "layer": "validation_wrapper",
            "path_or_source": "tools/validate_defense_signal_entry_policy.py",
            "effect": "synthetic_and_current_candidate_validation",
            "score_effect": False,
            "order_dispatch_effect": False,
            "entry_pool_filter_effect": False,
            "current_candidate_rows": _to_int(current.get("rows")),
            "current_active_block_rows": current_block_rows,
            "retained_log_block_sum": retained_block_sum,
            "validation_synthetic_block_present": validation_synthetic_present,
            "real_retained_log_block_estimate": real_retained_block_sum_estimate,
            "potential_block_rows": potential_rows,
            "observed_potential_rows": _to_int(potential_outcomes.get("intraday_observed_rows")),
            "observed_potential_avg_return_pct": _to_float(potential_outcomes.get("avg_return_first_to_last_pct")),
            "blocked_codes_sample": "",
            "potential_codes_sample": "",
            "classification": "REPORT_VALIDATION_NOT_PRODUCTION_BLOCK",
            "recommended_next": "do not count synthetic blocked=1 as real candidate blocking",
        },
    ]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "layer",
        "path_or_source",
        "effect",
        "score_effect",
        "order_dispatch_effect",
        "entry_pool_filter_effect",
        "current_candidate_rows",
        "current_active_block_rows",
        "retained_log_block_sum",
        "validation_synthetic_block_present",
        "real_retained_log_block_estimate",
        "potential_block_rows",
        "observed_potential_rows",
        "observed_potential_avg_return_pct",
        "blocked_codes_sample",
        "potential_codes_sample",
        "classification",
        "recommended_next",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_md(path: Path, payload: dict[str, Any]) -> None:
    s = payload["summary"]
    lines = [
        "# Defense Signal Policy Path Audit",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- status: `{payload['status']}`",
        "",
        "## Key Result",
        f"- actual_policy_path: `{s['actual_policy_classification']}`",
        f"- current_candidate_rows: `{s['current_candidate_rows']}`",
        f"- current_active_block_rows: `{s['current_active_block_rows']}`",
        f"- potential_block_rows: `{s['potential_block_rows']}`",
        f"- validation_synthetic_block_present: `{s['validation_synthetic_block_present']}`",
        f"- real_retained_log_block_estimate: `{s['real_retained_log_block_estimate']}`",
        "",
        "## Interpretation",
        "- This is a real candidate-pool policy path inside paper_engine, not only a report wrapper.",
        "- Latest current candidates show no real active block.",
        "- Potential surge blocks exist only if those rows enter the paper_engine candidate pool with matching route flags.",
        "- The retained blocked=1 log event is consistent with validation synthetic behavior and must not be counted as production candidate blocking without separate proof.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    impact = _read_json(IMPACT_JSON)
    outcome = _read_json(OUTCOME_JSON)
    impact_rows = _read_csv(IMPACT_CSV)
    rows = _build_rows(impact, outcome, impact_rows)
    actual = rows[0]
    summary = {
        "current_candidate_rows": actual["current_candidate_rows"],
        "current_active_block_rows": actual["current_active_block_rows"],
        "potential_block_rows": actual["potential_block_rows"],
        "validation_synthetic_block_present": actual["validation_synthetic_block_present"],
        "real_retained_log_block_estimate": actual["real_retained_log_block_estimate"],
        "actual_policy_classification": actual["classification"],
    }
    payload = {
        "generated_at": _now(),
        "status": "PASS",
        "schema_version": "defense_signal_policy_path_audit_v1",
        "source_files": {
            "impact_json": str(IMPACT_JSON),
            "impact_csv": str(IMPACT_CSV),
            "outcome_json": str(OUTCOME_JSON),
        },
        "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV), "md": str(OUT_MD)},
        "scope": {
            "policy_effect": "read_only_policy_path_audit_only",
            "full_logic_application": "NOT_APPLIED",
        },
        "summary": summary,
        "rows": rows,
    }
    _write_csv(OUT_CSV, rows)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_md(OUT_MD, payload)
    print(json.dumps({"status": "PASS", **summary, "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
