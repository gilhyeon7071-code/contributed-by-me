from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

COMPARE_CSV = LOG_DIR / "surge_event_money_pullback_vs_current_logic_latest.csv"
SCREENER_CSV = LOG_DIR / "surge_event_money_pullback_screener_latest.csv"

OUT_JSON = LOG_DIR / "surge_event_money_pullback_discovery_refine_latest.json"
OUT_CSV = LOG_DIR / "surge_event_money_pullback_discovery_refine_latest.csv"


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _code(row: dict[str, Any]) -> str:
    value = str(row.get("code") or "").strip()
    return value.zfill(6) if value.isdigit() else value


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = "" if value is None else str(value).strip()
        if text and text.lower() not in {"nan", "<na>", "none"}:
            return text
    return ""


def _float(value: Any) -> float:
    try:
        text = str(value).strip()
        if not text or text.lower() in {"nan", "<na>", "none"}:
            return 0.0
        return float(text)
    except Exception:
        return 0.0


def _confirmations(row: dict[str, str]) -> list[str]:
    checks: list[str] = ["not_detected_by_current_logic"]
    event = _float(row.get("event_score"))
    pullback = _float(row.get("pullback_score"))
    risk = _float(row.get("risk_score"))
    risk_evidence = _first_nonempty(row.get("risk_evidence"))

    if event <= 0:
        checks.append("event_axis_missing")
    elif event < 2:
        checks.append("event_axis_weak")

    if pullback <= 0:
        checks.append("pullback_missing")
    elif pullback < 2:
        checks.append("pullback_needs_confirmation")

    if risk >= 2:
        checks.append("hard_risk_review")
    elif risk > 0:
        checks.append(f"risk_review:{risk_evidence}")

    return checks


def _grade(row: dict[str, str]) -> tuple[str, str, float]:
    event = _float(row.get("event_score"))
    money = _float(row.get("money_score"))
    pullback = _float(row.get("pullback_score"))
    risk = _float(row.get("risk_score"))
    total = _float(row.get("total_structure_score"))

    discovery_score = round(money + pullback + min(event, 2.0) - risk, 4)

    if risk >= 2.0:
        return "D4_EXCLUDE_RISK", "risk_score_ge_2", discovery_score

    if money >= 3.0 and pullback >= 2.25 and risk <= 0.75 and total >= 5.25:
        return "D1_HIGH_VALUE_DISCOVERY", "money_and_pullback_confirmed_with_low_risk", discovery_score

    if money >= 3.0 and pullback >= 2.0 and risk <= 1.5:
        return "D2_CONDITIONAL_DISCOVERY", "money_and_pullback_confirmed_but_event_or_risk_needs_check", discovery_score

    if money >= 3.0 and pullback >= 1.5 and risk <= 1.5:
        return "D3_WAIT_FOR_CONFIRMATION", "money_positive_but_pullback_quality_is_not_enough", discovery_score

    if money >= 3.0 and risk <= 1.5:
        return "D3_WAIT_FOR_CONFIRMATION", "money_positive_but_pullback_missing_or_weak", discovery_score

    return "D4_EXCLUDE_WEAK_STRUCTURE", "second_stage_structure_not_confirmed", discovery_score


def main() -> int:
    compare_rows = _read_csv(COMPARE_CSV)
    screener_rows = _read_csv(SCREENER_CSV)
    screener_by_code = {_code(row): row for row in screener_rows if _code(row)}

    discovery_rows = [row for row in compare_rows if row.get("overlap_type") == "SCREENER_ONLY"]

    output_rows: list[dict[str, Any]] = []
    for row in discovery_rows:
        code = _code(row)
        screener = screener_by_code.get(code, {})
        grade, grade_reason, discovery_score = _grade(row)
        confirmations = _confirmations(row)

        out = {
            "code": code,
            "name": _first_nonempty(row.get("name"), screener.get("name")),
            "second_stage_grade": grade,
            "second_stage_reason": grade_reason,
            "discovery_score": discovery_score,
            "confirmations_needed": "|".join(confirmations),
            "watch_subtype": row.get("watch_subtype", ""),
            "quality_label": row.get("quality_label", ""),
            "total_structure_score": row.get("total_structure_score", ""),
            "event_score": row.get("event_score", ""),
            "event_evidence": screener.get("event_evidence", ""),
            "money_score": row.get("money_score", ""),
            "money_evidence": screener.get("money_evidence", ""),
            "pullback_score": row.get("pullback_score", ""),
            "pullback_evidence": screener.get("pullback_evidence", ""),
            "risk_score": row.get("risk_score", ""),
            "risk_evidence": row.get("risk_evidence", ""),
            "difference_reason": row.get("difference_reason", ""),
            "research_only": True,
            "policy_change": False,
            "entry_approval_changed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_route": False,
            "connection_level": "SIGNAL_QUALITY_ONLY",
            "trading_connection": False,
            "signal_connection": False,
            "execution_connection": False,
            "connection_note": "Signal quality or observation only; not entry approval, order, fill, or ledger.",
        }
        output_rows.append(out)

    output_rows.sort(
        key=lambda r: (
            str(r["second_stage_grade"]),
            -float(r["discovery_score"]),
            str(r["code"]),
        )
    )

    grade_counts = Counter(str(row["second_stage_grade"]) for row in output_rows)
    confirmation_counts = Counter()
    for row in output_rows:
        for item in str(row["confirmations_needed"]).split("|"):
            if item:
                confirmation_counts[item] += 1

    summary = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "research_only": True,
        "policy_change": False,
        "entry_approval_changed": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "trading_route": False,
        "connection_level": "SIGNAL_QUALITY_ONLY",
        "trading_connection": False,
        "signal_connection": False,
        "execution_connection": False,
        "connection_note": "Signal quality or observation only; not entry approval, order, fill, or ledger.",
        "inputs": {
            "compare_csv": str(COMPARE_CSV),
            "screener_csv": str(SCREENER_CSV),
        },
        "input_rows": {
            "compare_rows": len(compare_rows),
            "screener_rows": len(screener_rows),
            "screener_only_rows": len(discovery_rows),
        },
        "grade_counts": dict(grade_counts),
        "confirmation_counts": dict(confirmation_counts),
        "rows": output_rows,
    }

    OUT_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    fieldnames = [
        "code",
        "name",
        "second_stage_grade",
        "second_stage_reason",
        "discovery_score",
        "confirmations_needed",
        "watch_subtype",
        "quality_label",
        "total_structure_score",
        "event_score",
        "event_evidence",
        "money_score",
        "money_evidence",
        "pullback_score",
        "pullback_evidence",
        "risk_score",
        "risk_evidence",
        "difference_reason",
        "research_only",
        "policy_change",
        "entry_approval_changed",
        "paper_order_route",
        "broker_order_route",
        "trading_route",
        "connection_level",
        "trading_connection",
        "signal_connection",
        "execution_connection",
        "connection_note",
    ]
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    print(json.dumps({"grade_counts": dict(grade_counts), "confirmation_counts": dict(confirmation_counts)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
