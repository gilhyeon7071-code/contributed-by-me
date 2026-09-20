from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SOURCE_CSV = LOG_DIR / "logic_role_batch_focus_audit_latest.csv"
OUT_JSON = LOG_DIR / "active_policy_layer_audit_latest.json"
OUT_CSV = LOG_DIR / "active_policy_layer_audit_latest.csv"
OUT_MD = LOG_DIR / "active_policy_layer_audit_latest.md"

TARGET_ROLES = {
    "ACTIVE_POLICY_CORE_ENGINE",
    "ACTIVE_POLICY_ENTRY_FILTER",
    "ACTIVE_POLICY_GATE",
    "ACTIVE_POLICY_REVIEW",
    "ACTIVE_POLICY_RISK",
    "ACTIVE_POLICY_SURGE_RESPONSE",
    "VALIDATION_GUARD",
    "FAIL_CLOSED_UNCLASSIFIED",
}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _norm_name(name: str) -> str:
    return str(name or "").replace("\\\\", "\\").strip()


def _classify(row: dict[str, str]) -> tuple[str, str, str, str]:
    name = _norm_name(row.get("name", ""))
    lower = name.lower()
    role = str(row.get("refined_role") or "")
    fail_mode = str(row.get("fail_mode") or "")

    if role == "ACTIVE_POLICY_CORE_ENGINE" or lower.endswith("paper_engine.py"):
        return (
            "CORE_EXECUTION_POLICY",
            "KEEP_ALWAYS_ON",
            "paper_engine_entry_order_decision_path",
            "do_not_change_without_e2e_order_fills_ledger_proof",
        )
    if role == "ACTIVE_POLICY_GATE" or "p0_daily_check" in lower or "gate_daily" in lower:
        return (
            "HARD_SAFETY_GATE",
            "KEEP_ALWAYS_ON",
            "p0_gate_and_daily_risk_off_controls_entry_permission",
            "only adjust specific hard-vs-soft reasons after evidence review",
        )
    if role == "ACTIVE_POLICY_RISK" or "production_risk_playbook" in lower:
        return (
            "PORTFOLIO_RISK_POLICY",
            "ADAPTIVE_REVIEW_CANDIDATE",
            "risk_policy_can_block_or_scale_new_orders",
            "review sizing/blocking split by risk source before policy change",
        )
    if role == "ACTIVE_POLICY_ENTRY_FILTER" or "defense_signal" in lower:
        return (
            "ENTRY_FILTER_POLICY",
            "ADAPTIVE_REVIEW_CANDIDATE",
            "entry_pool_filter_can_remove_candidates_before_order_path",
            "review always-on versus regime_or_signal_specific use",
        )
    if role == "ACTIVE_POLICY_SURGE_RESPONSE" or "surge_active_response_layer" in lower:
        return (
            "SURGE_STRATEGY_POLICY",
            "ADAPTIVE_REVIEW_CANDIDATE",
            "surge_response_classifies_active_entry_wait_or_exclude",
            "review separately from normal strategy defense layers",
        )
    if role == "VALIDATION_GUARD" or "validate" in lower or "integrity" in lower or "calibration" in lower:
        return (
            "VALIDATION_GUARD",
            "KEEP_BUT_SCOPE_FAIL_CLOSED",
            "validation_or_contract_guard_may_fail_closed_batch",
            "keep core-contract guards fail-closed; make non-core diagnostics warn-only after proof",
        )
    if role == "FAIL_CLOSED_UNCLASSIFIED" or fail_mode == "FAIL_CLOSED":
        return (
            "UNMAPPED_FAIL_CLOSED",
            "DEPENDENCY_REVIEW_REQUIRED",
            "fail_closed_step_not_yet_mapped_to_core_or_policy_role",
            "map downstream dependency before changing behavior",
        )
    return (
        "POLICY_REVIEW_OTHER",
        "DEPENDENCY_REVIEW_REQUIRED",
        "policy_like_step_needs_manual_scope_check",
        "manual review before any batch or trading policy change",
    )


def _group_rows(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        name = _norm_name(row.get("name", ""))
        role = str(row.get("refined_role") or "")
        key = (name, role)
        layer, action, reason, handling = _classify(row)
        item = grouped.setdefault(
            key,
            {
                "name": name,
                "refined_role": role,
                "lines": [],
                "policy_layer": layer,
                "review_action": action,
                "layer_reason": reason,
                "recommended_handling": handling,
                "fail_modes": set(),
                "trading_effect": row.get("trading_effect"),
                "entry_pool_filter_effect": row.get("entry_pool_filter_effect"),
                "order_dispatch_effect": row.get("order_dispatch_effect"),
            },
        )
        line = str(row.get("line") or "").strip()
        if line:
            item["lines"].append(line)
        if row.get("fail_mode"):
            item["fail_modes"].add(row["fail_mode"])

    out: list[dict[str, Any]] = []
    for item in grouped.values():
        out.append(
            {
                **{k: v for k, v in item.items() if k != "fail_modes"},
                "lines": ",".join(sorted(set(item["lines"]), key=lambda x: int(x) if x.isdigit() else x)),
                "source_row_count": len(item["lines"]),
                "fail_modes": ",".join(sorted(item["fail_modes"])),
            }
        )
    return sorted(out, key=lambda row: (row["review_action"], row["policy_layer"], row["name"]))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "name",
        "refined_role",
        "lines",
        "source_row_count",
        "policy_layer",
        "review_action",
        "layer_reason",
        "recommended_handling",
        "fail_modes",
        "trading_effect",
        "entry_pool_filter_effect",
        "order_dispatch_effect",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Active Policy Layer Audit",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- source_rows: `{payload['summary']['source_rows']}`",
        f"- grouped_items: `{payload['summary']['grouped_items']}`",
        "",
        "## Review Action Counts",
    ]
    for action, count in sorted(payload["summary"]["review_action_counts"].items()):
        lines.append(f"- {action}: `{count}`")
    lines.extend(["", "## Policy Layer Counts"])
    for layer, count in sorted(payload["summary"]["policy_layer_counts"].items()):
        lines.append(f"- {layer}: `{count}`")
    lines.extend(["", "## Adaptive Review Candidates"])
    for row in payload["summary"]["adaptive_review_candidates"]:
        lines.append(f"- `{row['name']}` -> `{row['policy_layer']}`: {row['recommended_handling']}")
    lines.extend(["", "## Keep Always On"])
    for row in payload["summary"]["keep_always_on"]:
        lines.append(f"- `{row['name']}` -> `{row['policy_layer']}`")
    lines.extend(["", "## Dependency Review Required"])
    for row in payload["summary"]["dependency_review_required"]:
        lines.append(f"- `{row['name']}` -> `{row['policy_layer']}`: {row['recommended_handling']}")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    source_rows = [
        row for row in _read_csv(SOURCE_CSV)
        if row.get("refined_role") in TARGET_ROLES
    ]
    rows = _group_rows(source_rows)
    action_counts = Counter(row["review_action"] for row in rows)
    layer_counts = Counter(row["policy_layer"] for row in rows)
    payload = {
        "generated_at": _now(),
        "status": "PASS",
        "schema_version": "active_policy_layer_audit_v1",
        "source_files": {"batch_focus_csv": str(SOURCE_CSV)},
        "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV), "md": str(OUT_MD)},
        "scope": {
            "policy_effect": "read_only_active_policy_layer_audit_only",
            "full_logic_application": "NOT_APPLIED",
        },
        "summary": {
            "source_rows": len(source_rows),
            "grouped_items": len(rows),
            "review_action_counts": dict(action_counts),
            "policy_layer_counts": dict(layer_counts),
            "adaptive_review_candidates": [r for r in rows if r["review_action"] == "ADAPTIVE_REVIEW_CANDIDATE"],
            "keep_always_on": [r for r in rows if r["review_action"] == "KEEP_ALWAYS_ON"],
            "dependency_review_required": [r for r in rows if r["review_action"] == "DEPENDENCY_REVIEW_REQUIRED"],
            "notes": [
                "This audit does not change trading logic, batch order, thresholds, gates, orders, fills, ledger, or stats.",
                "KEEP_ALWAYS_ON means do not loosen without E2E proof.",
                "ADAPTIVE_REVIEW_CANDIDATE means candidate for future regime/strategy/signal-specific design review, not approved change.",
            ],
        },
        "rows": rows,
    }
    _write_csv(OUT_CSV, rows)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_md(OUT_MD, payload)
    print(json.dumps({"status": "PASS", "source_rows": len(source_rows), "grouped_items": len(rows), "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
