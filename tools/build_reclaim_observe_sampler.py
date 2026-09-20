from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SOURCE_CSV = LOG_DIR / "defense_surge_intent_review_latest.csv"
DESIGN_JSON = LOG_DIR / "reclaim_checklist_design_latest.json"
OUT_JSON = LOG_DIR / "reclaim_observe_sampler_latest.json"
OUT_CSV = LOG_DIR / "reclaim_observe_sampler_latest.csv"
OUT_MD = LOG_DIR / "reclaim_observe_sampler_latest.md"
HISTORY_CSV = LOG_DIR / "reclaim_observe_sampler_history.csv"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8-sig"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _bool(v: Any) -> bool:
    return str(v).strip().lower() in {"1", "true", "yes", "y"}


def _float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _stage_for(row: dict[str, str]) -> tuple[str, str]:
    cls = str(row.get("review_class") or "")
    if cls == "PROMOTION_REVIEW_CANDIDATE_OBSERVE_ONLY":
        return "RECLAIM_REVIEW_OBSERVE_ONLY", "observe reclaim evidence only"
    if cls == "WAIT_RECLAIM_OBSERVE_ONLY":
        return "WAIT_RECLAIM_OBSERVE_ONLY", "wait for reclaim evidence"
    if cls == "HARD_BLOCK_SUPPORTED":
        return "HARD_BLOCK_SUPPORTED", "keep defense block"
    return "OBSERVE_ONLY_REVIEW", "retain observation sample"


def _checklist(row: dict[str, str]) -> dict[str, Any]:
    stage, stage_reason = _stage_for(row)
    label = str(row.get("surge_active_response_label") or "")
    entry_decision = str(row.get("surge_entry_decision") or "")
    entry_allowed = _bool(row.get("surge_entry_allowed"))
    entry_blocked = _bool(row.get("surge_entry_blocked"))
    lob_ok = str(row.get("surge_lob_status") or "") == "OK"
    change = _float(row.get("surge_change_pct"))
    rvol20 = _float(row.get("surge_rvol20"))
    exclude = str(row.get("surge_exclude_reasons") or "")
    severe_micro = "SEVERE_MICROSTRUCTURE" in str(row.get("review_reason") or "") or "MARKOUT_NEGATIVE_BLOCK" in exclude
    hard_exclude = label == "HARD_EXCLUDE"
    reclaim_required = "RECLAIM" in label or "reclaim" in str(row.get("recommended_handling") or "").lower()

    evidence_flags = {
        "surge_active_present": _bool(row.get("surge_active_present")),
        "entry_allowed": entry_allowed,
        "entry_blocked": entry_blocked,
        "lob_ok": lob_ok,
        "hard_exclude": hard_exclude,
        "severe_microstructure_risk": severe_micro,
        "change_in_reclaim_band": 0.05 <= change <= 0.35,
        "rvol_overheated": rvol20 > 8.0,
        "reclaim_required": reclaim_required,
    }

    if stage == "RECLAIM_REVIEW_OBSERVE_ONLY":
        checklist_status = "WATCH_RECLAIM"
        if not evidence_flags["entry_allowed"] or evidence_flags["hard_exclude"] or evidence_flags["severe_microstructure_risk"]:
            checklist_status = "WAIT_RECLAIM"
    elif stage == "WAIT_RECLAIM_OBSERVE_ONLY":
        checklist_status = "WAIT_RECLAIM"
    elif stage == "HARD_BLOCK_SUPPORTED":
        checklist_status = "KEEP_BLOCK"
    else:
        checklist_status = "OBSERVE_ONLY"

    return {
        "checklist_stage": stage,
        "checklist_stage_reason": stage_reason,
        "checklist_status": checklist_status,
        **evidence_flags,
    }


def _build_rows() -> list[dict[str, Any]]:
    now = _now()
    out: list[dict[str, Any]] = []
    for row in _read_csv(SOURCE_CSV):
        code = str(row.get("code") or "").zfill(6)
        check = _checklist(row)
        out.append(
            {
                "sampled_at": now,
                "code": code,
                "source_review_class": row.get("review_class"),
                "defense_signal_score": row.get("defense_signal_score"),
                "defense_return_first_to_last_pct": row.get("defense_return_first_to_last_pct"),
                "surge_active_response_label": row.get("surge_active_response_label"),
                "surge_active_response_reason": row.get("surge_active_response_reason"),
                "surge_entry_decision": row.get("surge_entry_decision"),
                "surge_entry_reason": row.get("surge_entry_reason"),
                "surge_change_pct": row.get("surge_change_pct"),
                "surge_rvol20": row.get("surge_rvol20"),
                "surge_lob_status": row.get("surge_lob_status"),
                "surge_exclude_reasons": row.get("surge_exclude_reasons"),
                **check,
                "order_connection": "NONE",
                "paper_order_route": False,
                "broker_order_route": False,
                "policy_change_allowed": False,
            }
        )
    return out


FIELDS = [
    "sampled_at",
    "code",
    "source_review_class",
    "defense_signal_score",
    "defense_return_first_to_last_pct",
    "surge_active_response_label",
    "surge_active_response_reason",
    "surge_entry_decision",
    "surge_entry_reason",
    "surge_change_pct",
    "surge_rvol20",
    "surge_lob_status",
    "surge_exclude_reasons",
    "checklist_stage",
    "checklist_stage_reason",
    "checklist_status",
    "surge_active_present",
    "entry_allowed",
    "entry_blocked",
    "lob_ok",
    "hard_exclude",
    "severe_microstructure_risk",
    "change_in_reclaim_band",
    "rvol_overheated",
    "reclaim_required",
    "order_connection",
    "paper_order_route",
    "broker_order_route",
    "policy_change_allowed",
]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _append_history(path: Path, rows: list[dict[str, Any]]) -> None:
    exists = path.exists()
    with path.open("a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Reclaim Observe Sampler",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- rows: `{payload['summary']['rows']}`",
        f"- order_connection: `{payload['scope']['order_connection']}`",
        "",
        "## Checklist Status Counts",
    ]
    for k, v in sorted(payload["summary"]["checklist_status_counts"].items()):
        lines.append(f"- {k}: `{v}`")
    lines.extend(["", "## Watch Reclaim Rows"])
    for row in payload["summary"]["watch_reclaim_rows"]:
        lines.append(f"- `{row['code']}`: {row['checklist_stage']} / {row['checklist_status']}")
    lines.extend(["", "## Guardrails"])
    lines.append("- This sampler is observe-only.")
    lines.append("- It writes latest/history observation files only.")
    lines.append("- It does not approve orders, alter gates, alter defense policy, or route paper/broker orders.")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    rows = _build_rows()
    status_counts = Counter(row["checklist_status"] for row in rows)
    stage_counts = Counter(row["checklist_stage"] for row in rows)
    design = _read_json(DESIGN_JSON)
    payload = {
        "generated_at": _now(),
        "status": "PASS",
        "schema_version": "reclaim_observe_sampler_v1",
        "source_files": {
            "defense_surge_intent_review_csv": str(SOURCE_CSV),
            "reclaim_checklist_design_json": str(DESIGN_JSON),
        },
        "outputs": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
            "history_csv": str(HISTORY_CSV),
            "md": str(OUT_MD),
        },
        "scope": {
            "policy_effect": "observe_only_sampler",
            "full_logic_application": "NOT_APPLIED",
            "order_connection": "NONE",
        },
        "summary": {
            "rows": len(rows),
            "design_schema_version": design.get("schema_version", ""),
            "checklist_status_counts": dict(status_counts),
            "checklist_stage_counts": dict(stage_counts),
            "watch_reclaim_rows": [row for row in rows if row["checklist_status"] == "WATCH_RECLAIM"],
            "policy_change_applied": False,
            "paper_order_route": False,
            "broker_order_route": False,
        },
        "rows": rows,
    }
    _write_csv(OUT_CSV, rows)
    _append_history(HISTORY_CSV, rows)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_md(OUT_MD, payload)
    print(json.dumps({"status": "PASS", "rows": len(rows), "checklist_status_counts": dict(status_counts), "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
