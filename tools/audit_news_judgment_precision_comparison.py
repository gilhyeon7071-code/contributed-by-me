from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
IMPACT_CSV = LOGS / "news_topic_candidate_impact_latest.csv"
L3_CSV = LOGS / "news_judgment_l3_layer_latest.csv"
SCHEMA_GAP_JSON = LOGS / "news_judgment_l3_schema_gap_audit_latest.json"
BLOCK_AUDIT_JSON = LOGS / "news_topic_block_order_path_audit_latest.json"
OUT_JSON = LOGS / "news_judgment_precision_comparison_latest.json"
OUT_CSV = LOGS / "news_judgment_precision_comparison_latest.csv"
KST = timezone(timedelta(hours=9))


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


def _legacy_execution_effect(candidate_effect: str) -> str:
    effect = str(candidate_effect or "").strip()
    if effect in {"block_review", "avoid_chase"}:
        return "block_order"
    if effect == "reduce_size_context":
        return "reduce_size"
    return ""


def _l3_by_key(rows: List[Dict[str, str]]) -> Dict[tuple[str, str], Dict[str, str]]:
    out: Dict[tuple[str, str], Dict[str, str]] = {}
    for row in rows:
        key = (str(row.get("code") or "").strip().zfill(6), str(row.get("topic_key") or "").strip())
        if key[0].strip("0") and key[1]:
            out[key] = row
    return out


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "code",
        "name",
        "topic_key",
        "topic_label",
        "candidate_effect",
        "confirm_level",
        "avg_confidence",
        "legacy_execution_effect",
        "current_l3_execution_effect",
        "execution_allowed",
        "execution_denied_reason",
        "scope_separated",
        "has_confirm_level",
        "has_confidence",
        "has_denied_reason",
        "precision_gain_type",
        "representative_titles",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def main() -> int:
    impact_rows = _read_csv(IMPACT_CSV)
    l3_rows = _read_csv(L3_CSV)
    schema_gap = _read_json(SCHEMA_GAP_JSON)
    block_audit = _read_json(BLOCK_AUDIT_JSON)
    l3_map = _l3_by_key(l3_rows)

    out_rows: List[Dict[str, Any]] = []
    counts: Dict[str, int] = {
        "legacy_block_order_rows": 0,
        "legacy_reduce_size_rows": 0,
        "current_block_order_rows": 0,
        "current_reduce_size_rows": 0,
        "suppressed_execution_rows": 0,
        "rows_with_denied_reason": 0,
        "rows_with_confirm_level": 0,
        "rows_with_confidence": 0,
        "rows_scope_separated": 0,
    }
    denied_counts: Dict[str, int] = {}
    gain_counts: Dict[str, int] = {}
    for row in impact_rows:
        code = str(row.get("code") or "").strip().zfill(6)
        topic_key = str(row.get("topic_key") or "").strip()
        l3 = l3_map.get((code, topic_key), {})
        candidate_effect = str(row.get("candidate_effect") or "").strip()
        legacy_effect = _legacy_execution_effect(candidate_effect)
        current_effect = str(l3.get("l3_execution_effect") or "").strip()
        execution_allowed = _truthy(l3.get("execution_allowed"))
        denied = str(l3.get("execution_denied_reason") or row.get("execution_denied_reason") or "").strip()
        confirm = str(row.get("confirm_level") or l3.get("confirm_level") or "").strip()
        confidence = str(row.get("avg_confidence") or l3.get("avg_confidence") or "").strip()
        scope = str(row.get("scope") or "").strip()

        if legacy_effect == "block_order":
            counts["legacy_block_order_rows"] += 1
        if legacy_effect == "reduce_size":
            counts["legacy_reduce_size_rows"] += 1
        if current_effect == "block_order":
            counts["current_block_order_rows"] += 1
        if current_effect == "reduce_size":
            counts["current_reduce_size_rows"] += 1
        if legacy_effect and not current_effect:
            counts["suppressed_execution_rows"] += 1
        if denied:
            counts["rows_with_denied_reason"] += 1
            for reason in denied.split("|"):
                reason = reason.strip()
                if reason:
                    denied_counts[reason] = denied_counts.get(reason, 0) + 1
        if confirm:
            counts["rows_with_confirm_level"] += 1
        if confidence:
            counts["rows_with_confidence"] += 1
        if scope:
            counts["rows_scope_separated"] += 1

        gain_types: List[str] = []
        if legacy_effect and not current_effect:
            gain_types.append("execution_suppressed_until_l3")
        if denied:
            gain_types.append("denied_reason_recorded")
        if confirm and confidence:
            gain_types.append("confidence_confirm_explicit")
        if scope:
            gain_types.append("scope_explicit")
        if not gain_types:
            gain_types.append("record_only")
        for item in gain_types:
            gain_counts[item] = gain_counts.get(item, 0) + 1

        out_rows.append(
            {
                "code": code,
                "name": str(row.get("name") or "").strip(),
                "topic_key": topic_key,
                "topic_label": str(row.get("topic_label") or "").strip(),
                "candidate_effect": candidate_effect,
                "confirm_level": confirm,
                "avg_confidence": confidence,
                "legacy_execution_effect": legacy_effect,
                "current_l3_execution_effect": current_effect,
                "execution_allowed": str(execution_allowed),
                "execution_denied_reason": denied,
                "scope_separated": str(bool(scope)),
                "has_confirm_level": str(bool(confirm)),
                "has_confidence": str(bool(confidence)),
                "has_denied_reason": str(bool(denied)),
                "precision_gain_type": "|".join(gain_types),
                "representative_titles": str(row.get("representative_titles") or "").strip()[:250],
            }
        )

    _write_csv(OUT_CSV, out_rows)
    schema_summary = schema_gap.get("summary") if isinstance(schema_gap.get("summary"), dict) else {}
    block_summary = block_audit.get("summary") if isinstance(block_audit.get("summary"), dict) else {}
    payload = {
        "generated_at": _now(),
        "status": "PASS",
        "reason": "comparison_artifact_created",
        "inputs": {
            "candidate_impact": str(IMPACT_CSV),
            "l3_layer": str(L3_CSV),
            "schema_gap_audit": str(SCHEMA_GAP_JSON),
            "block_order_audit": str(BLOCK_AUDIT_JSON),
        },
        "summary": {
            "rows": len(out_rows),
            **counts,
            "schema_gap_status": str(schema_gap.get("status") or ""),
            "schema_ready_layers": int(schema_summary.get("schema_ready_layers") or 0),
            "schema_ready_layer_total": int(schema_summary.get("schema_ready_layer_total") or 0),
            "block_order_policy_gap_rows": int(block_summary.get("policy_gap_rows") or 0),
            "l3_execution_allowed_rows": sum(1 for row in l3_rows if _truthy(row.get("execution_allowed"))),
        },
        "precision_gain_counts": dict(sorted(gain_counts.items())),
        "denied_reason_counts": dict(sorted(denied_counts.items())),
        "interpretation": {
            "what_improved": [
                "legacy candidate_effect-derived execution is now compared against L3 permission",
                "suppressed rows keep explicit denied reasons instead of silently becoming order effects",
                "scope, confirm level, confidence, and execution permission are separately auditable",
            ],
            "what_not_proven": [
                "future return prediction accuracy is not proven by this audit",
                "official full daily batch reflection is not proven by this audit",
            ],
        },
        "outputs": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
        },
        "trading_effect": False,
        "live_broker_effect": False,
        "gate_effect": False,
        "score_effect": False,
        "risk_lock_effect": False,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": payload["status"], "summary": payload["summary"], "outputs": payload["outputs"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
