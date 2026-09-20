from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
DEFENSE_OUTCOME_CSV = LOG_DIR / "defense_signal_entry_policy_outcome_review_latest.csv"
SURGE_ACTIVE_CSV = LOG_DIR / "surge_active_response_layer_latest.csv"
OUT_JSON = LOG_DIR / "defense_surge_intent_review_latest.json"
OUT_CSV = LOG_DIR / "defense_surge_intent_review_latest.csv"
OUT_MD = LOG_DIR / "defense_surge_intent_review_latest.md"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _code(v: Any) -> str:
    return str(v or "").strip().zfill(6)


def _float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _bool(v: Any) -> bool:
    return str(v).strip().lower() in {"1", "true", "yes", "y"}


def _classify(def_row: dict[str, str], surge_row: dict[str, str] | None) -> tuple[str, str, str]:
    score = _float(def_row.get("defense_signal_score"))
    observed_status = str(def_row.get("observation_status") or "")
    ret = _float(def_row.get("return_first_to_last_pct"))
    reasons = str(def_row.get("defense_reasons") or "")

    if surge_row is None:
        if observed_status == "INTRADAY_PRICE_OBSERVED" and ret <= -2.0:
            return (
                "HARD_BLOCK_SUPPORTED",
                "defense_block_aligned_with_negative_intraday_without_active_surge_row",
                "keep_block; no promotion until active surge row and reclaim evidence exist",
            )
        return (
            "OBSERVE_ONLY_NO_ACTIVE_SURGE_ROW",
            "not_present_in_latest_surge_active_response_layer",
            "keep as observe-only review sample; do not create trading exception",
        )

    label = str(surge_row.get("active_response_label") or "")
    entry_decision = str(surge_row.get("entry_decision") or "")
    entry_blocked = _bool(surge_row.get("entry_blocked"))
    lob_ok = str(surge_row.get("lob_status") or "") == "OK" or _bool(surge_row.get("lob_available"))
    rvol20 = _float(surge_row.get("rvol20"))
    change = _float(surge_row.get("change_pct"))
    exclude = str(surge_row.get("exclude_reasons") or "")

    if label == "HARD_EXCLUDE":
        return (
            "HARD_BLOCK_SUPPORTED",
            "surge_layer_hard_exclude",
            "keep_block; severe surge-layer risk already excludes",
        )
    if entry_blocked or entry_decision == "ENTRY_BLOCKED":
        return (
            "WAIT_RECLAIM_OBSERVE_ONLY",
            "surge_layer_entry_blocked_requires_reclaim",
            "do not promote; observe only until reclaim/lob/probe criteria are met",
        )
    if label == "WAIT_RECLAIM":
        if lob_ok and 0.05 <= change <= 0.35 and rvol20 > 8.0:
            return (
                "PROMOTION_REVIEW_CANDIDATE_OBSERVE_ONLY",
                "surge_layer_entry_allowed_but_overheated_wait_reclaim",
                "possible future rule: observe-only reclaim checklist, not immediate buy",
            )
        return (
            "WAIT_RECLAIM_OBSERVE_ONLY",
            "surge_layer_wait_reclaim",
            "observe until vwap/high reclaim evidence appears",
        )
    if "late_buy_risk_high" in reasons or score >= 120:
        return (
            "HARD_BLOCK_SUPPORTED",
            "high_defense_score_late_buy_chase_risk",
            "keep_block unless separate controlled-risk probe policy is approved",
        )
    if exclude:
        return (
            "WAIT_RECLAIM_OBSERVE_ONLY",
            "surge_layer_has_exclusion_reasons",
            "observe only; require exclusion reason clear",
        )
    return (
        "OBSERVE_ONLY_REVIEW",
        "insufficient_evidence_for_promotion_or_hard_block",
        "keep observation sample and collect follow-through",
    )


def _build_rows() -> list[dict[str, Any]]:
    defense_rows = _read_csv(DEFENSE_OUTCOME_CSV)
    surge_by_code = {_code(row.get("code")): row for row in _read_csv(SURGE_ACTIVE_CSV)}
    out: list[dict[str, Any]] = []
    for row in defense_rows:
        code = _code(row.get("code"))
        surge = surge_by_code.get(code)
        action, reason, handling = _classify(row, surge)
        out.append(
            {
                "code": code,
                "name": row.get("name"),
                "defense_signal_score": row.get("defense_signal_score"),
                "defense_observation_status": row.get("observation_status"),
                "defense_return_first_to_last_pct": row.get("return_first_to_last_pct"),
                "route_scope": row.get("route_scope"),
                "surge_active_present": surge is not None,
                "surge_active_response_label": (surge or {}).get("active_response_label", ""),
                "surge_active_response_reason": (surge or {}).get("active_response_reason", ""),
                "surge_active_response_next_check": (surge or {}).get("active_response_next_check", ""),
                "surge_entry_decision": (surge or {}).get("entry_decision", ""),
                "surge_entry_reason": (surge or {}).get("entry_reason", ""),
                "surge_entry_allowed": (surge or {}).get("entry_allowed", ""),
                "surge_entry_blocked": (surge or {}).get("entry_blocked", ""),
                "surge_change_pct": (surge or {}).get("change_pct", ""),
                "surge_rvol20": (surge or {}).get("rvol20", ""),
                "surge_lob_status": (surge or {}).get("lob_status", ""),
                "surge_exclude_reasons": (surge or {}).get("exclude_reasons", ""),
                "review_class": action,
                "review_reason": reason,
                "recommended_handling": handling,
            }
        )
    return out


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "code",
        "name",
        "defense_signal_score",
        "defense_observation_status",
        "defense_return_first_to_last_pct",
        "route_scope",
        "surge_active_present",
        "surge_active_response_label",
        "surge_active_response_reason",
        "surge_active_response_next_check",
        "surge_entry_decision",
        "surge_entry_reason",
        "surge_entry_allowed",
        "surge_entry_blocked",
        "surge_change_pct",
        "surge_rvol20",
        "surge_lob_status",
        "surge_exclude_reasons",
        "review_class",
        "review_reason",
        "recommended_handling",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Defense Surge Intent Review",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- rows: `{payload['summary']['rows']}`",
        "",
        "## Review Class Counts",
    ]
    for k, v in sorted(payload["summary"]["review_class_counts"].items()):
        lines.append(f"- {k}: `{v}`")
    lines.extend(["", "## Promotion Review Candidates"])
    for row in payload["summary"]["promotion_review_candidates"]:
        lines.append(f"- `{row['code']}`: {row['recommended_handling']}")
    lines.extend(["", "## Key Interpretation"])
    lines.append("- No row is classified as immediate buy or production promotion.")
    lines.append("- Promotion candidates are observe-only reclaim checklist candidates, not order approval.")
    lines.append("- Defense signal remains aligned with chase/late-buy risk for most rows.")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    rows = _build_rows()
    counts = Counter(row["review_class"] for row in rows)
    payload = {
        "generated_at": _now(),
        "status": "PASS",
        "schema_version": "defense_surge_intent_review_v1",
        "source_files": {
            "defense_outcome_csv": str(DEFENSE_OUTCOME_CSV),
            "surge_active_csv": str(SURGE_ACTIVE_CSV),
        },
        "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV), "md": str(OUT_MD)},
        "scope": {
            "policy_effect": "read_only_surge_intent_review_only",
            "full_logic_application": "NOT_APPLIED",
        },
        "summary": {
            "rows": len(rows),
            "surge_active_present_rows": sum(1 for row in rows if row["surge_active_present"]),
            "review_class_counts": dict(counts),
            "promotion_review_candidates": [row for row in rows if row["review_class"] == "PROMOTION_REVIEW_CANDIDATE_OBSERVE_ONLY"],
        },
        "rows": rows,
    }
    _write_csv(OUT_CSV, rows)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_md(OUT_MD, payload)
    print(json.dumps({"status": "PASS", "rows": len(rows), "review_class_counts": dict(counts), "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
