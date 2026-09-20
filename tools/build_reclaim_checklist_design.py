from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SOURCE_CSV = LOG_DIR / "defense_surge_intent_review_latest.csv"
OUT_JSON = LOG_DIR / "reclaim_checklist_design_latest.json"
OUT_CSV = LOG_DIR / "reclaim_checklist_design_latest.csv"
OUT_MD = LOG_DIR / "reclaim_checklist_design_latest.md"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _design_rows() -> list[dict[str, Any]]:
    return [
        {
            "stage": "HARD_BLOCK_SUPPORTED",
            "purpose": "keep defense block",
            "input_class": "HARD_BLOCK_SUPPORTED",
            "minimum_conditions": "severe microstructure risk, negative observed follow-through, KRX warning, insufficient value, or active surge hard-exclude",
            "required_evidence": "existing defense and surge risk evidence is enough to keep blocked",
            "next_state_if_met": "HARD_BLOCK_SUPPORTED",
            "next_state_if_not_met": "OBSERVE_ONLY_REVIEW",
            "order_connection": "NONE",
            "policy_change_allowed": False,
        },
        {
            "stage": "OBSERVE_ONLY_REVIEW",
            "purpose": "retain sample without trading exception",
            "input_class": "OBSERVE_ONLY_NO_ACTIVE_SURGE_ROW",
            "minimum_conditions": "defense potential block exists but latest surge active-response row is missing",
            "required_evidence": "future active-response row, intraday price observation, and reason lineage",
            "next_state_if_met": "WAIT_RECLAIM_OBSERVE_ONLY or HARD_BLOCK_SUPPORTED",
            "next_state_if_not_met": "OBSERVE_ONLY_REVIEW",
            "order_connection": "NONE",
            "policy_change_allowed": False,
        },
        {
            "stage": "WAIT_RECLAIM_OBSERVE_ONLY",
            "purpose": "wait for price or structure recovery",
            "input_class": "WAIT_RECLAIM_OBSERVE_ONLY",
            "minimum_conditions": "entry blocked or overheated surge with reclaim requirement",
            "required_evidence": "VWAP reclaim or high reclaim, LOB available, no severe microstructure risk, overheat reason improving",
            "next_state_if_met": "RECLAIM_REVIEW_OBSERVE_ONLY",
            "next_state_if_not_met": "WAIT_RECLAIM_OBSERVE_ONLY or HARD_BLOCK_SUPPORTED",
            "order_connection": "NONE",
            "policy_change_allowed": False,
        },
        {
            "stage": "RECLAIM_REVIEW_OBSERVE_ONLY",
            "purpose": "observe-only promotion review after recovery evidence",
            "input_class": "PROMOTION_REVIEW_CANDIDATE_OBSERVE_ONLY",
            "minimum_conditions": "surge layer entry_allowed, LOB OK, reclaim evidence present, no hard exclude, no severe microstructure risk",
            "required_evidence": "reclaim timestamp, price basis, LOB status, RVOL trend, orderflow tag or explicit no-history flag",
            "next_state_if_met": "PROMOTION_REVIEW_OBSERVE_ONLY",
            "next_state_if_not_met": "WAIT_RECLAIM_OBSERVE_ONLY",
            "order_connection": "NONE",
            "policy_change_allowed": False,
        },
        {
            "stage": "PROMOTION_REVIEW_OBSERVE_ONLY",
            "purpose": "future policy candidate, not production approval",
            "input_class": "PROMOTION_REVIEW_CANDIDATE_OBSERVE_ONLY",
            "minimum_conditions": "repeatable positive follow-through after reclaim over accumulated samples",
            "required_evidence": "sample count, forward returns, max adverse excursion, liquidity and orderflow quality",
            "next_state_if_met": "FUTURE_POLICY_REVIEW",
            "next_state_if_not_met": "OBSERVE_ONLY_REVIEW",
            "order_connection": "NONE",
            "policy_change_allowed": False,
        },
    ]


def _candidate_rows(source_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in source_rows:
        cls = str(row.get("review_class") or "")
        if cls == "PROMOTION_REVIEW_CANDIDATE_OBSERVE_ONLY":
            checklist_stage = "RECLAIM_REVIEW_OBSERVE_ONLY"
            checklist_reason = "only observe reclaim evidence; no order connection"
        elif cls == "WAIT_RECLAIM_OBSERVE_ONLY":
            checklist_stage = "WAIT_RECLAIM_OBSERVE_ONLY"
            checklist_reason = "entry is blocked or requires reclaim; observe only"
        elif cls == "HARD_BLOCK_SUPPORTED":
            checklist_stage = "HARD_BLOCK_SUPPORTED"
            checklist_reason = "existing defense/surge evidence supports block"
        else:
            checklist_stage = "OBSERVE_ONLY_REVIEW"
            checklist_reason = "not present in active surge layer or insufficient evidence"
        out.append(
            {
                "code": str(row.get("code") or "").zfill(6),
                "source_review_class": cls,
                "checklist_stage": checklist_stage,
                "checklist_reason": checklist_reason,
                "surge_active_response_label": row.get("surge_active_response_label"),
                "surge_entry_decision": row.get("surge_entry_decision"),
                "defense_signal_score": row.get("defense_signal_score"),
                "order_connection": "NONE",
                "policy_change_allowed": False,
            }
        )
    return out


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "stage",
        "purpose",
        "input_class",
        "minimum_conditions",
        "required_evidence",
        "next_state_if_met",
        "next_state_if_not_met",
        "order_connection",
        "policy_change_allowed",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Reclaim Checklist Design",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- design_rows: `{len(payload['design_rows'])}`",
        f"- candidate_rows: `{len(payload['candidate_rows'])}`",
        "",
        "## Guardrails",
        "- This is design only, not implementation.",
        "- Order connection is NONE for every stage.",
        "- Defense block policy is not changed.",
        "- Promotion means observe-only review, not buy approval.",
        "",
        "## Stages",
    ]
    for row in payload["design_rows"]:
        lines.append(f"- `{row['stage']}`: {row['purpose']} -> {row['next_state_if_met']}")
    lines.extend(["", "## Current Candidate Mapping"])
    for row in payload["candidate_rows"]:
        if row["source_review_class"] == "PROMOTION_REVIEW_CANDIDATE_OBSERVE_ONLY":
            lines.append(f"- `{row['code']}` -> `{row['checklist_stage']}`: {row['checklist_reason']}")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    source_rows = _read_csv(SOURCE_CSV)
    design_rows = _design_rows()
    candidate_rows = _candidate_rows(source_rows)
    payload = {
        "generated_at": _now(),
        "status": "PASS",
        "schema_version": "reclaim_checklist_design_v1",
        "source_files": {"defense_surge_intent_review_csv": str(SOURCE_CSV)},
        "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV), "md": str(OUT_MD)},
        "scope": {
            "policy_effect": "read_only_design_only",
            "full_logic_application": "NOT_APPLIED",
            "order_connection": "NONE",
        },
        "design_rows": design_rows,
        "candidate_rows": candidate_rows,
        "summary": {
            "design_rows": len(design_rows),
            "candidate_rows": len(candidate_rows),
            "promotion_observe_only_rows": sum(1 for row in candidate_rows if row["source_review_class"] == "PROMOTION_REVIEW_CANDIDATE_OBSERVE_ONLY"),
            "policy_change_applied": False,
            "order_connection": "NONE",
        },
    }
    _write_csv(OUT_CSV, design_rows)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_md(OUT_MD, payload)
    print(json.dumps({"status": "PASS", "design_rows": len(design_rows), "candidate_rows": len(candidate_rows), "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
