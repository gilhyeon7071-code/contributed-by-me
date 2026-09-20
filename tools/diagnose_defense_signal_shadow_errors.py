from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

VALIDATION_CSV = LOG_DIR / "defense_signal_shadow_outcome_validation_latest.csv"
DEFENSE_CSV = LOG_DIR / "defense_signal_shadow_latest.csv"

OUT_JSON = LOG_DIR / "defense_signal_shadow_error_diagnostic_latest.json"
OUT_CSV = LOG_DIR / "defense_signal_shadow_error_diagnostic_latest.csv"

KST = timezone(timedelta(hours=9))


def _now_kst() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as fh:
                return [{str(k): str(v or "") for k, v in row.items()} for row in csv.DictReader(fh)]
        except UnicodeDecodeError:
            continue
    return []


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fields})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _code(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6)[-6:] if digits else ""


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value or "").strip()
        if text == "":
            return float(default)
        return float(text)
    except Exception:
        return float(default)


def _b(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _index(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code and code not in out:
            out[code] = row
    return out


def _split_reasons(value: Any) -> list[str]:
    out: list[str] = []
    for part in str(value or "").split("|"):
        label = part.split(":", 1)[0].strip()
        if label:
            out.append(label)
    return out


def _contains(value: Any, token: str) -> bool:
    return token in str(value or "")


def _missed_risk_causes(row: dict[str, str], defense: dict[str, str]) -> tuple[list[str], str]:
    causes: list[str] = []
    neg = str(row.get("negative_evidence") or "")
    score = _f(row.get("defense_signal_score"), 0.0)
    reasons = set(_split_reasons(row.get("defense_reasons")))
    route = str(row.get("route_scope") or "")

    if score < 20:
        causes.append("threshold_too_low_to_trigger_watch")
    if _contains(neg, "four_question_intraday_negative") and not {
        "late_buy_risk_high",
        "news_prepriced_high",
        "four_question_chase_block",
    }.intersection(reasons):
        causes.append("intraday_negative_not_mapped_to_defense_reason")
    if _contains(neg, "historical_trade_avg_negative"):
        causes.append("historical_loss_reference_not_weighted")
    if _contains(neg, "surge_path_negative") and not _b(row.get("would_block_surge")):
        causes.append("surge_path_negative_not_escalated")
    if _contains(neg, "surge_path_adverse_gt_3pct") and not _b(row.get("would_block_surge")):
        causes.append("adverse_move_not_escalated")
    if _contains(neg, "surge_markout_kill_rate_high") and not _b(row.get("would_block_surge")):
        causes.append("markout_kill_not_escalated")
    if route in {"SURGE_ONLY", "BOTH"} and "lob_status" in defense and str(defense.get("lob_status") or "").upper() == "NO_LOB":
        causes.append("no_lob_should_force_recheck")
    if not causes:
        causes.append("mixed_or_unmapped_negative_evidence")

    suggestion = "raise_to_watch_or_recheck_shadow_only"
    if any(x in causes for x in ("surge_path_negative_not_escalated", "adverse_move_not_escalated", "markout_kill_not_escalated")):
        suggestion = "raise_surge_defense_weight_shadow_only"
    elif "historical_loss_reference_not_weighted" in causes:
        suggestion = "add_history_reference_as_weak_weight_shadow_only"
    return causes, suggestion


def _overblock_causes(row: dict[str, str], defense: dict[str, str]) -> tuple[list[str], str]:
    causes: list[str] = []
    pos = str(row.get("positive_evidence") or "")
    reasons = set(_split_reasons(row.get("defense_reasons")))
    score = _f(row.get("defense_signal_score"), 0.0)

    if _contains(pos, "four_question_intraday_positive"):
        causes.append("intraday_positive_conflicts_with_block")
    if _contains(pos, "historical_trade_avg_positive"):
        causes.append("positive_history_conflicts_with_block")
    if _contains(pos, "surge_path_positive"):
        causes.append("surge_path_positive_conflicts_with_block")
    if "late_buy_risk_high" in reasons or "four_question_chase_block" in reasons:
        causes.append("chase_risk_weight_may_be_too_strong")
    if "news_prepriced_high" in reasons:
        causes.append("prepriced_weight_may_be_too_strong")
    if score >= 100 and not str(row.get("negative_evidence") or "").strip():
        causes.append("high_score_without_negative_confirmation")
    if not causes:
        causes.append("mixed_or_unmapped_positive_evidence")

    suggestion = "downgrade_block_to_recheck_when_positive_confirmation_exists"
    if "high_score_without_negative_confirmation" in causes:
        suggestion = "require_negative_confirmation_for_hard_shadow_block"
    return causes, suggestion


def _supported_causes(row: dict[str, str]) -> tuple[list[str], str]:
    causes: list[str] = []
    neg = str(row.get("negative_evidence") or "")
    if _contains(neg, "four_question_intraday_negative"):
        causes.append("four_question_negative_supported")
    if _contains(neg, "surge_markout_not_eligible_or_kill"):
        causes.append("surge_markout_kill_supported")
    if _contains(neg, "historical_trade_avg_negative"):
        causes.append("historical_loss_reference_supported")
    if _contains(neg, "surge_path_negative"):
        causes.append("surge_path_negative_supported")
    if not causes:
        causes.append("negative_evidence_supported")
    return causes, "keep_as_shadow_candidate"


def _diagnose_row(row: dict[str, str], defense: dict[str, str]) -> dict[str, Any]:
    label = str(row.get("defense_validation_label") or "")
    if label == "MISSED_RISK":
        causes, suggestion = _missed_risk_causes(row, defense)
    elif label == "OVERBLOCK_RISK":
        causes, suggestion = _overblock_causes(row, defense)
    elif label in {"DEFENSE_SUPPORTED", "WATCH_OR_RECHECK_SUPPORTED"}:
        causes, suggestion = _supported_causes(row)
    else:
        causes, suggestion = (["not_actionable_for_error_diagnostic"], "no_change")

    needs_followup = label in {"MISSED_RISK", "OVERBLOCK_RISK"}
    route = str(row.get("route_scope") or "")
    if label == "MISSED_RISK" and route in {"SURGE_ONLY", "BOTH"}:
        priority = "HIGH"
    elif label == "OVERBLOCK_RISK":
        priority = "HIGH" if _f(row.get("defense_signal_score"), 0.0) >= 100 else "MEDIUM"
    elif label in {"DEFENSE_SUPPORTED", "WATCH_OR_RECHECK_SUPPORTED"}:
        priority = "MEDIUM"
    else:
        priority = "LOW"

    return {
        "code": _code(row.get("code")),
        "name": row.get("name", ""),
        "route_scope": route,
        "defense_signal_score": row.get("defense_signal_score", ""),
        "defense_action_shadow": row.get("defense_action_shadow", ""),
        "defense_validation_label": label,
        "diagnostic_priority": priority,
        "needs_followup": needs_followup,
        "diagnostic_causes": "|".join(causes),
        "shadow_adjustment_suggestion": suggestion,
        "negative_evidence": row.get("negative_evidence", ""),
        "positive_evidence": row.get("positive_evidence", ""),
        "defense_reasons": row.get("defense_reasons", ""),
        "would_block_general": row.get("would_block_general", ""),
        "would_block_surge": row.get("would_block_surge", ""),
        "score_effect": False,
        "trading_effect": False,
        "no_order_effect": True,
    }


def main() -> int:
    status: dict[str, Any] = {
        "generated_at": _now_kst(),
        "mode": "read_only_defense_signal_shadow_error_diagnostic",
        "score_effect": False,
        "trading_effect": False,
        "policy_effect": False,
        "entry_approval_changed": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "no_order_effect": True,
        "quality": "FAIL",
        "reason": "",
        "inputs": {
            "outcome_validation": str(VALIDATION_CSV),
            "defense_shadow": str(DEFENSE_CSV),
        },
        "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV)},
    }

    validation_rows = _read_csv(VALIDATION_CSV)
    defense_by_code = _index(_read_csv(DEFENSE_CSV))
    if not validation_rows:
        status["reason"] = "validation_input_missing_or_empty"
        _write_json(OUT_JSON, status)
        return 1

    diagnostic_rows = [
        _diagnose_row(row, defense_by_code.get(_code(row.get("code")), {}))
        for row in validation_rows
    ]
    actionable = [r for r in diagnostic_rows if r["needs_followup"]]
    diagnostic_rows.sort(
        key=lambda r: (
            str(r["diagnostic_priority"]) != "HIGH",
            str(r["defense_validation_label"]),
            -int(float(r.get("defense_signal_score") or 0)),
            str(r["code"]),
        )
    )

    fields = [
        "code",
        "name",
        "route_scope",
        "defense_signal_score",
        "defense_action_shadow",
        "defense_validation_label",
        "diagnostic_priority",
        "needs_followup",
        "diagnostic_causes",
        "shadow_adjustment_suggestion",
        "negative_evidence",
        "positive_evidence",
        "defense_reasons",
        "would_block_general",
        "would_block_surge",
        "score_effect",
        "trading_effect",
        "no_order_effect",
    ]
    _write_csv(OUT_CSV, diagnostic_rows, fields)

    label_counts = Counter(str(r["defense_validation_label"]) for r in diagnostic_rows)
    cause_counts = Counter(
        cause
        for row in diagnostic_rows
        if row["needs_followup"]
        for cause in str(row["diagnostic_causes"]).split("|")
        if cause
    )
    suggestion_counts = Counter(
        str(row["shadow_adjustment_suggestion"]) for row in diagnostic_rows if row["needs_followup"]
    )
    priority_counts = Counter(str(row["diagnostic_priority"]) for row in diagnostic_rows)
    by_route: dict[str, dict[str, int]] = defaultdict(dict)
    for row in diagnostic_rows:
        route = str(row["route_scope"] or "UNKNOWN")
        label = str(row["defense_validation_label"])
        by_route[route][label] = by_route[route].get(label, 0) + 1

    status.update(
        {
            "quality": "PASS",
            "reason": "ok",
            "rows": len(diagnostic_rows),
            "actionable_error_rows": len(actionable),
            "defense_validation_label_counts": dict(label_counts),
            "diagnostic_priority_counts": dict(priority_counts),
            "actionable_cause_counts": dict(cause_counts),
            "actionable_suggestion_counts": dict(suggestion_counts),
            "route_label_counts": dict(by_route),
            "top_actionable": actionable[:30],
            "notes": [
                "diagnostic only; suggestions are not applied to score, gate, order route, fills, ledger, or policy",
                "MISSED_RISK and OVERBLOCK_RISK must be reduced before minimum operating connection",
                "positive historical trade evidence is code-level reference only, not same-signal proof",
            ],
        }
    )
    _write_json(OUT_JSON, status)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
