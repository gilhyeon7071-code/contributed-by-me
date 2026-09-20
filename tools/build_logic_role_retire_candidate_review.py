from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SOURCE_CSV = LOG_DIR / "logic_role_batch_focus_audit_latest.csv"
OUT_JSON = LOG_DIR / "logic_role_retire_candidate_review_latest.json"
OUT_CSV = LOG_DIR / "logic_role_retire_candidate_review_latest.csv"
OUT_MD = LOG_DIR / "logic_role_retire_candidate_review_latest.md"

TARGET_REFINED_ROLES = {"REPORT_ONLY", "LOW_PRIORITY_UNCLASSIFIED"}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _norm_name(name: str) -> str:
    normalized = str(name or "").replace("\\\\", "\\").strip()
    lower = normalized.lower()
    if lower.startswith(str(ROOT).lower()):
        try:
            return str(Path(normalized).relative_to(ROOT))
        except ValueError:
            return normalized
    if "\\" not in normalized and "/" not in normalized and (ROOT / "tools" / normalized).exists():
        return str(Path("tools") / normalized)
    return normalized


def _classify(name: str) -> tuple[str, str, str]:
    lower = name.replace("\\\\", "\\").lower()

    if lower in {"run_paper_daily.bat", "tools\\python_exec_proxy.cmd"}:
        return (
            "KEEP_DAILY_BATCH",
            "batch_entry_or_python_proxy_infrastructure",
            "do_not_move_without_replacing_batch_infra",
        )
    if "decide_candidate_refresh_flags" in lower or "needs_post_candidate_price_refresh" in lower:
        return (
            "KEEP_DAILY_BATCH",
            "candidate_refresh_control_helper",
            "keep_until_refresh_contract_is_separately_mapped",
        )
    if "build_candidate_price_history" in lower or "build_forward_estimate_snapshot" in lower:
        return (
            "KEEP_DAILY_SUMMARY",
            "candidate_context_or_forward_snapshot",
            "keep_daily_unless downstream consumers are proven optional",
        )
    if "paper_pending_verdict" in lower or "build_integrated_ops_snapshot" in lower:
        return (
            "KEEP_DAILY_SUMMARY",
            "daily_ops_status_or_pending_verdict_context",
            "keep_daily_summary_path",
        )
    if "market_rising_snapshot" in lower:
        return (
            "KEEP_DAILY_SUMMARY",
            "market_context_snapshot_for_dashboard_or_ops",
            "keep_daily_summary_path_unless_rootb_dependency_removed",
        )
    if "orderflow_hawkes_glr" in lower:
        return (
            "DEPENDENCY_REVIEW",
            "microstructure_model_name_needs_intraday_dependency_check",
            "do_not_remove_until surge_realtime_consumer_dependency_is_checked",
        )
    if (
        "indicator_diag_and_recommend" in lower
        or "auto_signal_tuner" in lower
        or "surge_param_validator" in lower
        or "build_strategy_optimization_analyzer" in lower
        or "build_drift_monitor" in lower
    ):
        return (
            "MOVE_TO_MANUAL_OR_WEEKLY_CANDIDATE",
            "diagnostic_tuning_or_strategy_analysis_not_core_daily_execution",
            "candidate_for_manual_or_weekly_schedule_after_dependency_check",
        )
    return (
        "DEPENDENCY_REVIEW",
        "low_priority_name_not_enough_to_move",
        "manual_dependency_check_required_before_schedule_change",
    )


def _group_rows(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = _norm_name(row.get("name", ""))
        item = grouped.setdefault(
            name,
            {
                "name": name,
                "source_type": row.get("source_type"),
                "lines": [],
                "refined_roles": set(),
                "fail_modes": set(),
                "trading_effect": row.get("trading_effect"),
                "entry_pool_filter_effect": row.get("entry_pool_filter_effect"),
                "order_dispatch_effect": row.get("order_dispatch_effect"),
            },
        )
        line = str(row.get("line") or "").strip()
        if line:
            item["lines"].append(line)
        if row.get("refined_role"):
            item["refined_roles"].add(row["refined_role"])
        if row.get("fail_mode"):
            item["fail_modes"].add(row["fail_mode"])

    out: list[dict[str, Any]] = []
    for item in grouped.values():
        action, reason, handling = _classify(str(item["name"]))
        out.append(
            {
                "name": item["name"],
                "source_type": item["source_type"],
                "lines": ",".join(sorted(set(item["lines"]), key=lambda x: int(x) if x.isdigit() else x)),
                "source_row_count": len(item["lines"]),
                "refined_roles": ",".join(sorted(item["refined_roles"])),
                "fail_modes": ",".join(sorted(item["fail_modes"])),
                "retire_review_action": action,
                "review_reason": reason,
                "recommended_handling": handling,
                "trading_effect": item["trading_effect"],
                "entry_pool_filter_effect": item["entry_pool_filter_effect"],
                "order_dispatch_effect": item["order_dispatch_effect"],
            }
        )
    return sorted(out, key=lambda row: (row["retire_review_action"], row["name"]))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "name",
        "source_type",
        "lines",
        "source_row_count",
        "refined_roles",
        "fail_modes",
        "retire_review_action",
        "review_reason",
        "recommended_handling",
        "trading_effect",
        "entry_pool_filter_effect",
        "order_dispatch_effect",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Logic Role Retire Candidate Review",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- source_rows: `{payload['summary']['source_rows']}`",
        f"- grouped_items: `{payload['summary']['grouped_items']}`",
        "",
        "## Action Counts",
    ]
    for action, count in sorted(payload["summary"]["action_counts"].items()):
        lines.append(f"- {action}: `{count}`")
    lines.extend(["", "## Move To Manual Or Weekly Candidates"])
    for row in payload["summary"]["move_to_manual_or_weekly_candidates"]:
        lines.append(f"- `{row['name']}`: {row['review_reason']}")
    lines.extend(["", "## Dependency Review"])
    for row in payload["summary"]["dependency_review_items"]:
        lines.append(f"- `{row['name']}`: {row['review_reason']}")
    lines.extend(["", "## Keep In Daily Path"])
    for row in payload["summary"]["keep_daily_items"]:
        lines.append(f"- `{row['name']}`: {row['retire_review_action']}")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    source_rows = [
        row for row in _read_csv(SOURCE_CSV)
        if row.get("source_type") == "batch_step"
        and row.get("refined_role") in TARGET_REFINED_ROLES
    ]
    grouped_rows = _group_rows(source_rows)
    action_counts = Counter(row["retire_review_action"] for row in grouped_rows)
    by_action: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in grouped_rows:
        by_action[row["retire_review_action"]].append(row)

    payload = {
        "generated_at": _now(),
        "status": "PASS",
        "schema_version": "logic_role_retire_candidate_review_v1",
        "source_files": {"batch_focus_csv": str(SOURCE_CSV)},
        "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV), "md": str(OUT_MD)},
        "scope": {
            "input_refined_roles": sorted(TARGET_REFINED_ROLES),
            "policy_effect": "read_only_candidate_schedule_review_only",
            "full_logic_application": "NOT_APPLIED",
        },
        "summary": {
            "source_rows": len(source_rows),
            "grouped_items": len(grouped_rows),
            "action_counts": dict(action_counts),
            "move_to_manual_or_weekly_candidates": by_action["MOVE_TO_MANUAL_OR_WEEKLY_CANDIDATE"],
            "dependency_review_items": by_action["DEPENDENCY_REVIEW"],
            "keep_daily_items": by_action["KEEP_DAILY_BATCH"] + by_action["KEEP_DAILY_SUMMARY"],
            "notes": [
                "This report does not remove or move any batch step.",
                "MOVE_TO_MANUAL_OR_WEEKLY_CANDIDATE means schedule-reduction candidate only after dependency check.",
                "KEEP_DAILY_BATCH and KEEP_DAILY_SUMMARY are not cleanup targets in this pass.",
            ],
        },
        "rows": grouped_rows,
    }
    _write_csv(OUT_CSV, grouped_rows)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_md(OUT_MD, payload)
    print(json.dumps({"status": "PASS", "source_rows": len(source_rows), "grouped_items": len(grouped_rows), "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
