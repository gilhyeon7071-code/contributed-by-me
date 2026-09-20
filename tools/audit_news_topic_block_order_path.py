from __future__ import annotations

import csv
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
FINAL_CANDIDATES = LOGS / "candidates_latest_data.with_final_score.csv"
SCHEMA_GAP = LOGS / "news_judgment_l3_schema_gap_audit_latest.json"
OUT_JSON = LOGS / "news_topic_block_order_path_audit_latest.json"
OUT_CSV = LOGS / "news_topic_block_order_path_audit_latest.csv"
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


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return default


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "code",
        "name",
        "news_topic_candidate_effect",
        "news_topic_execution_effect",
        "news_topic_confirm_level",
        "news_topic_avg_confidence",
        "news_topic_source_trading_effect",
        "defined_l3_allowed",
        "current_path_blocks",
        "policy_gap_reason",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def main() -> int:
    rows = _read_csv(FINAL_CANDIDATES)
    gap = _read_json(SCHEMA_GAP)
    l3_rule = {
        "effects": {"block_review", "avoid_chase", "reduce_size_context"},
        "min_confirm_level": 2,
        "min_confidence": 0.60,
    }

    audit_rows: List[Dict[str, Any]] = []
    block_rows = []
    defined_l3_allowed = []
    for row in rows:
        effect = str(row.get("news_topic_candidate_effect") or "").strip()
        execution_effect = str(row.get("news_topic_execution_effect") or "").strip()
        if not effect and not execution_effect:
            continue
        confirm = _to_int(row.get("news_topic_confirm_level"), 0)
        conf = _to_float(row.get("news_topic_avg_confidence"), 0.0)
        source_trading_effect = _truthy(row.get("news_topic_source_trading_effect"))
        current_path_blocks = execution_effect == "block_order"
        allowed = (
            effect in l3_rule["effects"]
            and confirm >= l3_rule["min_confirm_level"]
            and conf >= l3_rule["min_confidence"]
            and source_trading_effect
        )
        reasons = []
        if effect not in l3_rule["effects"]:
            reasons.append("candidate_effect_not_l3")
        if confirm < l3_rule["min_confirm_level"]:
            reasons.append("confirm_level_below_min")
        if conf < l3_rule["min_confidence"]:
            reasons.append("confidence_below_min")
        if not source_trading_effect:
            reasons.append("source_trading_effect_false")
        if current_path_blocks and not allowed:
            reasons.append("current_path_blocks_without_defined_l3_permission")
        out = {
            "code": str(row.get("code") or "").strip(),
            "name": str(row.get("name") or "").strip(),
            "news_topic_candidate_effect": effect,
            "news_topic_execution_effect": execution_effect,
            "news_topic_confirm_level": str(confirm),
            "news_topic_avg_confidence": f"{conf:.6f}",
            "news_topic_source_trading_effect": str(source_trading_effect),
            "defined_l3_allowed": str(allowed),
            "current_path_blocks": str(current_path_blocks),
            "policy_gap_reason": "|".join(reasons),
        }
        audit_rows.append(out)
        if current_path_blocks:
            block_rows.append(out)
        if allowed:
            defined_l3_allowed.append(out)

    policy_gap_rows = [
        row for row in audit_rows
        if "current_path_blocks_without_defined_l3_permission" in str(row.get("policy_gap_reason") or "")
    ]
    status = "FAIL" if policy_gap_rows else "PASS"
    payload = {
        "generated_at": _now(),
        "status": status,
        "reason": "current_block_order_path_bypasses_defined_l3_permission" if policy_gap_rows else "ok",
        "input": str(FINAL_CANDIDATES),
        "schema_gap_audit": str(SCHEMA_GAP),
        "code_path": [
            {
                "file": "tools/final_score_merge_daily.py",
                "function": "_merge_news_topic_candidate_impact",
                "behavior": "news_topic_execution_effect is copied from the L3 layer l3_execution_effect, not directly from candidate_effect",
            },
            {
                "file": "paper_engine.py",
                "function": "_prepare_entry_candidate_pool",
                "behavior": "news_topic_execution_effect=block_order removes rows only when news_topic_l3_execution_allowed is true",
            },
            {
                "file": "paper_engine.py",
                "function": "_process_entry_rows",
                "behavior": "news_topic_execution_effect=reduce_size can reduce order quantity only when L3 allowed rows reach this path",
            },
        ],
        "summary": {
            "candidate_rows": len(rows),
            "news_topic_rows_in_final_candidates": len(audit_rows),
            "current_block_order_rows": len(block_rows),
            "defined_l3_allowed_rows": len(defined_l3_allowed),
            "policy_gap_rows": len(policy_gap_rows),
            "schema_gap_status": str(gap.get("status") or ""),
            "schema_gap_execution_allowed_by_defined_rule_rows": int((gap.get("summary") or {}).get("execution_allowed_by_defined_rule_rows") or 0),
        },
        "current_block_order_rows": block_rows,
        "defined_l3_allowed_rows": defined_l3_allowed,
        "policy_gap_rows": policy_gap_rows,
        "recommendation": "keep_news_topic_execution_effect_consumption_behind_l3_permission_gate",
        "trading_effect": False,
        "live_broker_effect": False,
        "gate_effect": False,
        "score_effect": False,
        "risk_lock_effect": False,
        "outputs": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
        },
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, audit_rows)
    print(json.dumps({"status": status, "summary": payload["summary"], "outputs": payload["outputs"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
