from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"

AUTO_STATUS = LOGS / "auto_news_implications_status_latest.json"
TOPIC_STATUS = LOGS / "news_topic_judgment_status_latest.json"
CANDIDATE_STATUS = LOGS / "news_topic_candidate_impact_status_latest.json"
L3_STATUS = LOGS / "news_judgment_l3_layer_status_latest.json"
SCHEMA_GAP = LOGS / "news_judgment_l3_schema_gap_audit_latest.json"
PRECISION_AUDIT = LOGS / "news_judgment_precision_comparison_latest.json"
CONTENT_STATUS = LOGS / "news_article_content_extract_status_latest.json"

TOPIC_HISTORY = LOGS / "news_topic_judgment_history.csv"
CANDIDATE_IMPACT = LOGS / "news_topic_candidate_impact_latest.csv"
L3_LAYER = LOGS / "news_judgment_l3_layer_latest.csv"

OUT_JSON = LOGS / "news_judgment_approval_review_latest.json"
OUT_CSV = LOGS / "news_judgment_approval_review_latest.csv"

KST = timezone(timedelta(hours=9))

MIN_HISTORY_ROWS = 100
MIN_HISTORY_DAYS = 20
MIN_EFFECT_ROWS = 30
MIN_BODY_EVIDENCE_ROWS = 20


def _now() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as fp:
                return [dict(row) for row in csv.DictReader(fp)]
        except UnicodeDecodeError:
            continue
    with path.open("r", newline="") as fp:
        return [dict(row) for row in csv.DictReader(fp)]


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return default


def _field_count(rows: Iterable[Dict[str, str]], field: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows:
        value = str(row.get(field) or "").strip() or "<blank>"
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _distinct_values(rows: Iterable[Dict[str, str]], fields: Tuple[str, ...]) -> List[str]:
    values = set()
    for row in rows:
        for field in fields:
            value = str(row.get(field) or "").strip()
            if value:
                values.add(value)
    return sorted(values)


def _criterion(
    key: str,
    label: str,
    status: str,
    actual: Any,
    required: Any,
    evidence: str,
    approval_scope: str,
    note: str,
) -> Dict[str, Any]:
    return {
        "key": key,
        "label": label,
        "status": status,
        "actual": actual,
        "required": required,
        "evidence": evidence,
        "approval_scope": approval_scope,
        "note": note,
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "key",
        "label",
        "status",
        "actual",
        "required",
        "approval_scope",
        "evidence",
        "note",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def main() -> int:
    auto_status = _read_json(AUTO_STATUS)
    topic_status = _read_json(TOPIC_STATUS)
    candidate_status = _read_json(CANDIDATE_STATUS)
    l3_status = _read_json(L3_STATUS)
    schema_gap = _read_json(SCHEMA_GAP)
    precision = _read_json(PRECISION_AUDIT)
    content_status = _read_json(CONTENT_STATUS)

    topic_history = _read_csv(TOPIC_HISTORY)
    candidate_rows = _read_csv(CANDIDATE_IMPACT)
    l3_rows = _read_csv(L3_LAYER)

    history_days = _distinct_values(topic_history, ("asof_ymd", "date8"))
    history_effect_counts = _field_count(topic_history, "candidate_effect")
    current_effect_counts = _field_count(candidate_rows, "candidate_effect")
    l3_allowed_rows = sum(1 for row in l3_rows if _truthy(row.get("execution_allowed")))
    reviewed_allowed_rows = sum(1 for row in l3_rows if _truthy(row.get("reviewed_execution_allowed")))
    missing_override_audit = sum(
        1
        for row in l3_rows
        if _truthy(row.get("reviewed_execution_allowed"))
        and (
            not str(row.get("reviewer") or "").strip()
            or not str(row.get("reviewed_at") or "").strip()
            or not str(row.get("override_reason") or "").strip()
        )
    )

    body_rows = _as_int(auto_status.get("rows_with_body_evidence"))
    content_quality = str(content_status.get("quality") or "")
    schema_ready = str(schema_gap.get("status") or "") == "PASS"
    precision_ready = str(precision.get("status") or "") == "PASS"
    topic_readonly = topic_status.get("trading_effect") is False
    candidate_readonly = candidate_status.get("trading_effect") is False and candidate_status.get("candidate_append") is False
    l3_readonly = l3_status.get("trading_effect") is False and l3_allowed_rows == 0

    min_effect_actual = min(history_effect_counts.values()) if history_effect_counts else 0
    outcome_artifacts = sorted(LOGS.glob("*news*markout*")) + sorted(LOGS.glob("*news*outcome*"))
    outcome_available = bool(outcome_artifacts)

    criteria = [
        _criterion(
            "schema_ready",
            "L3 fields are normalized and auditable",
            "PASS" if schema_ready else "FAIL",
            schema_gap.get("status", "missing"),
            "PASS",
            str(SCHEMA_GAP),
            "reference_or_higher",
            "Approval cannot start if the rows do not carry confirm/confidence/effect/permission fields.",
        ),
        _criterion(
            "execution_closed_before_approval",
            "No news judgment is already changing execution",
            "PASS" if l3_readonly else "FAIL",
            {"l3_allowed_rows": l3_allowed_rows, "trading_effect": l3_status.get("trading_effect")},
            {"l3_allowed_rows": 0, "trading_effect": False},
            str(L3_STATUS),
            "reference_or_higher",
            "Pre-approval state must remain closed so review evidence is not mixed with live behavior.",
        ),
        _criterion(
            "current_body_evidence",
            "Enough current article-body evidence exists",
            "PASS" if content_quality == "PASS" and body_rows >= MIN_BODY_EVIDENCE_ROWS else "FAIL",
            {"quality": content_quality, "rows_with_body_evidence": body_rows},
            {"quality": "PASS", "min_rows_with_body_evidence": MIN_BODY_EVIDENCE_ROWS},
            str(AUTO_STATUS),
            "reference",
            "Current body evidence is useful, but today's sample alone is not enough for execution approval.",
        ),
        _criterion(
            "history_sample_size",
            "Enough historical judgment rows exist",
            "PASS" if len(topic_history) >= MIN_HISTORY_ROWS and len(history_days) >= MIN_HISTORY_DAYS else "FAIL",
            {"history_rows": len(topic_history), "history_days": len(history_days)},
            {"min_history_rows": MIN_HISTORY_ROWS, "min_history_days": MIN_HISTORY_DAYS},
            str(TOPIC_HISTORY),
            "candidate_or_score_review",
            "This checks judgment-volume only, not whether the judgment made money or reduced risk.",
        ),
        _criterion(
            "effect_bucket_sample_size",
            "Each effect bucket has enough observations",
            "PASS" if min_effect_actual >= MIN_EFFECT_ROWS else "FAIL",
            {"min_effect_rows": min_effect_actual, "history_effect_counts": history_effect_counts},
            {"min_rows_per_effect": MIN_EFFECT_ROWS},
            str(TOPIC_HISTORY),
            "candidate_or_score_review",
            "Sparse effect buckets should stay read-only because one or two examples can mislead approval.",
        ),
        _criterion(
            "outcome_counterfactual_evidence",
            "Post-judgment outcome and counterfactual evidence exists",
            "PASS" if outcome_available else "FAIL",
            [str(path) for path in outcome_artifacts[:10]],
            "news markout/outcome artifact with after-return and no-judgment baseline",
            str(LOGS),
            "score_or_execution",
            "Execution approval needs after-return, risk avoided, opportunity cost, and baseline comparison.",
        ),
        _criterion(
            "manual_override_audit",
            "Manual override rows have reviewer/time/reason",
            "PASS" if missing_override_audit == 0 else "FAIL",
            {"reviewed_allowed_rows": reviewed_allowed_rows, "missing_override_audit": missing_override_audit},
            {"missing_override_audit": 0},
            str(L3_LAYER),
            "execution",
            "If a person approves execution, the approval must say who, when, and why.",
        ),
        _criterion(
            "precision_audit_ready",
            "L3 precision audit exists",
            "PASS" if precision_ready else "FAIL",
            precision.get("status", "missing"),
            "PASS",
            str(PRECISION_AUDIT),
            "candidate_or_score_review",
            "This proves structure/auditability, not predictive accuracy.",
        ),
        _criterion(
            "downstream_read_only_boundary",
            "Topic and candidate layers remain non-trading",
            "PASS" if topic_readonly and candidate_readonly else "FAIL",
            {
                "topic_trading_effect": topic_status.get("trading_effect"),
                "candidate_trading_effect": candidate_status.get("trading_effect"),
                "candidate_append": candidate_status.get("candidate_append"),
            },
            {"topic_trading_effect": False, "candidate_trading_effect": False, "candidate_append": False},
            f"{TOPIC_STATUS}; {CANDIDATE_STATUS}",
            "reference_or_higher",
            "This keeps news as evidence until a separate approval changes the boundary.",
        ),
    ]

    failed = [row for row in criteria if row["status"] != "PASS"]
    approval_readiness = "REFERENCE_ONLY" if not failed else "NOT_APPROVABLE_FOR_EXECUTION"
    allowed_scopes = ["reference_material"]
    if not failed:
        allowed_scopes.append("candidate_or_score_review")
    if not failed and outcome_available and l3_allowed_rows == 0:
        allowed_scopes.append("manual_execution_review")

    payload = {
        "generated_at": _now(),
        "status": "PASS",
        "reason": "approval_review_artifact_created",
        "approval_readiness": approval_readiness,
        "allowed_scopes_now": allowed_scopes,
        "not_allowed_now": [
            "direct_buy_sell",
            "score_effect",
            "candidate_append",
            "order_block",
            "reduce_size",
            "gate_or_risk_lock_change",
        ],
        "thresholds": {
            "min_history_rows": MIN_HISTORY_ROWS,
            "min_history_days": MIN_HISTORY_DAYS,
            "min_effect_rows": MIN_EFFECT_ROWS,
            "min_body_evidence_rows": MIN_BODY_EVIDENCE_ROWS,
        },
        "current_summary": {
            "auto_rows_promoted": auto_status.get("rows_promoted"),
            "rows_with_body_evidence": body_rows,
            "topic_rows": topic_status.get("topics"),
            "candidate_impact_rows": candidate_status.get("rows_output"),
            "current_effect_counts": current_effect_counts,
            "history_rows": len(topic_history),
            "history_days": len(history_days),
            "history_effect_counts": history_effect_counts,
            "l3_execution_allowed_rows": l3_allowed_rows,
            "reviewed_execution_allowed_rows": reviewed_allowed_rows,
            "outcome_artifact_count": len(outcome_artifacts),
        },
        "criteria": criteria,
        "failed_criteria": [row["key"] for row in failed],
        "user_approval_rule": {
            "who_approves": "user",
            "codex_role": "prepare evidence, identify failed criteria, and record the requested approval scope",
            "approval_must_include": ["scope", "duration_or_review_date", "target_effects", "override_reason"],
            "approval_must_not_do": "turn news directly into buy/sell/order effects without outcome evidence",
        },
        "inputs": {
            "auto_status": str(AUTO_STATUS),
            "topic_status": str(TOPIC_STATUS),
            "candidate_status": str(CANDIDATE_STATUS),
            "l3_status": str(L3_STATUS),
            "schema_gap": str(SCHEMA_GAP),
            "precision_audit": str(PRECISION_AUDIT),
            "topic_history": str(TOPIC_HISTORY),
            "candidate_impact": str(CANDIDATE_IMPACT),
            "l3_layer": str(L3_LAYER),
        },
        "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV)},
        "trading_effect": False,
        "live_broker_effect": False,
        "gate_effect": False,
        "score_effect": False,
        "risk_lock_effect": False,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, criteria)
    print(
        json.dumps(
            {
                "status": payload["status"],
                "approval_readiness": approval_readiness,
                "failed_criteria": payload["failed_criteria"],
                "outputs": payload["outputs"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
