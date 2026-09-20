from __future__ import annotations

import csv
import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
RUN_DAILY = ROOT / "run_paper_daily.bat"
TOOLS_DIR = ROOT / "tools"

OUT_JSON = LOG_DIR / "logic_role_inventory_audit_latest.json"
OUT_CSV = LOG_DIR / "logic_role_inventory_audit_latest.csv"
OUT_MD = LOG_DIR / "logic_role_inventory_audit_latest.md"

ROLE_CORE = "CORE"
ROLE_ACTIVE = "ACTIVE_POLICY"
ROLE_OBSERVE = "OBSERVE_ONLY"
ROLE_REPORT = "REPORT"
ROLE_UNCLASSIFIED = "UNCLASSIFIED_REVIEW"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _mtime(path: Path) -> str:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")
    except Exception:
        return ""


def _classify(name: str, context: str = "") -> tuple[str, str, str]:
    text = f"{name} {context}".lower()

    explicit_active = [
        "defense_signal_shadow_latest",
        "entry_decision_layers_runtime_latest",
        "pending_entry_status_latest",
        "risk_orchestration_latest",
        "production_risk_playbook",
        "disclosure_risk_latest",
        "paper_engine.py",
        "gate_daily.py",
        "p0_daily_check.py",
    ]
    report_override_terms = [
        "impact_report",
        "outcome_review",
        "validation",
        "diagnostic",
        "audit",
        "report",
        "review",
        "summary",
    ]
    if any(term in text for term in explicit_active):
        return ROLE_ACTIVE, "explicit_active_runtime_policy_or_decision_artifact", "requires_explicit_policy_tracking"
    if "shadow" in text:
        return ROLE_OBSERVE, "shadow_output_not_active_unless_explicitly_consumed", "must_not_block_or_change_orders"
    if "impact" in text:
        return ROLE_REPORT, "impact_analysis_output_not_policy_by_itself", "compress_into_summary_where_possible"
    if any(term in text for term in report_override_terms):
        return ROLE_REPORT, "report_or_validation_output_not_policy_by_itself", "compress_into_summary_where_possible"

    core_terms = [
        "freshness",
        "prices_update",
        "ohlcv",
        "krx_update",
        "sync_krx",
        "generate_candidates",
        "p0_daily_check",
        "gate_daily",
        "paper_validate",
        "reconcile_paper_state",
        "ledger_append",
        "orders_exec",
        "fills",
        "execution_safety_contract",
        "check_entry_room",
        "survivorship_policy",
        "liquidity_filter",
        "paper_state",
        "run_backtest_validation_real",
    ]
    active_terms = [
        "defense_signal_entry_policy",
        "defense_signal_shadow_latest",
        "surge_active_response_layer",
        "kill",
        "risk",
        "entry_policy",
        "pending_entry",
        "entry_decision_layers",
    ]
    observe_terms = [
        "shadow",
        "observe",
        "simulation",
        "candidate_bridge",
        "diagnostic",
        "probe",
        "markout",
        "outcome",
        "learning",
        "followthrough",
        "recheck",
        "four_question",
        "multisource",
        "pullback",
        "event_theme",
        "no_lob",
        "latency",
        "quality",
    ]
    report_terms = [
        "report",
        "review",
        "summary",
        "dashboard",
        "status",
        "audit",
        "card",
        "pnl",
        "live_vs_bt",
        "after_close",
        "runtime_chain",
        "health",
    ]

    if any(term in text for term in core_terms):
        return ROLE_CORE, "core_runtime_or_data_chain", "fail_closed_or_required"
    if any(term in text for term in active_terms):
        return ROLE_ACTIVE, "can_affect_candidate_policy_or_risk", "requires_explicit_policy_tracking"
    if any(term in text for term in observe_terms):
        return ROLE_OBSERVE, "read_only_or_shadow_validation_candidate", "must_not_block_or_change_orders"
    if any(term in text for term in report_terms):
        return ROLE_REPORT, "human_or_dashboard_reporting", "compress_into_summary_where_possible"
    return ROLE_UNCLASSIFIED, "name_not_enough_to_classify", "manual_review_required"


def _impact_flags(role: str, name: str, context: str = "") -> dict[str, bool]:
    text = f"{name} {context}".lower()
    return {
        "trading_effect": role in {ROLE_CORE, ROLE_ACTIVE} and any(
            x in text for x in ["paper_engine", "entry", "risk", "gate", "order", "ledger", "fill", "defense_signal"]
        ),
        "entry_pool_filter_effect": "defense_signal" in text or "entry_policy" in text,
        "order_dispatch_effect": any(x in text for x in ["order", "orders_exec", "broker", "dispatch"]) and role == ROLE_CORE,
        "policy_change_applied": False,
    }


def _batch_rows() -> list[dict[str, Any]]:
    if not RUN_DAILY.exists():
        return []
    rows: list[dict[str, Any]] = []
    lines = RUN_DAILY.read_text(encoding="utf-8", errors="replace").splitlines()
    for i, line in enumerate(lines, start=1):
        if not re.search(r"\b[\w\\./:-]+\.(py|bat|cmd)\b", line, flags=re.IGNORECASE):
            continue
        if "Select-String" in line:
            continue
        m = re.search(r"([\w:\\./-]+(?:\\|/)?[\w.-]+\.(?:py|bat|cmd))", line, flags=re.IGNORECASE)
        item = m.group(1).replace('"', "") if m else line.strip()
        role, reason, action = _classify(item, line)
        flags = _impact_flags(role, item, line)
        fail_mode = "FAIL_CLOSED" if "goto :FAILED" in "\n".join(lines[i : min(i + 4, len(lines))]) else "FAIL_SOFT_OR_UNKNOWN"
        if "WARN" in "\n".join(lines[i : min(i + 6, len(lines))]) or "continuing" in "\n".join(lines[i : min(i + 6, len(lines))]).lower():
            fail_mode = "FAIL_SOFT"
        rows.append(
            {
                "source_type": "batch_step",
                "name": item,
                "path": str(RUN_DAILY),
                "line": i,
                "role": role,
                "classification_reason": reason,
                "recommended_handling": action,
                "fail_mode": fail_mode,
                **flags,
            }
        )
    return rows


def _artifact_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not LOG_DIR.exists():
        return rows
    candidates: list[tuple[float, Path, int]] = []
    for entry in os.scandir(LOG_DIR):
        if not entry.is_file():
            continue
        name = entry.name
        if not (
            "latest" in name
            or "last" in name
            or name.endswith("_status.json")
            or name.endswith("_summary.json")
        ):
            continue
        try:
            st = entry.stat()
        except OSError:
            continue
        candidates.append((st.st_mtime, Path(entry.path), st.st_size))
    for _, path, size in sorted(candidates, reverse=True)[:500]:
        name = path.name
        role, reason, action = _classify(name)
        flags = _impact_flags(role, name)
        rows.append(
            {
                "source_type": "artifact",
                "name": name,
                "path": str(path),
                "line": "",
                "role": role,
                "classification_reason": reason,
                "recommended_handling": action,
                "fail_mode": "ARTIFACT",
                "last_write_time": _mtime(path),
                "size_bytes": size,
                **flags,
            }
        )
    return rows


def _tool_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(TOOLS_DIR.glob("*.py")):
        role, reason, action = _classify(path.name)
        flags = _impact_flags(role, path.name)
        rows.append(
            {
                "source_type": "tool_file",
                "name": path.name,
                "path": str(path),
                "line": "",
                "role": role,
                "classification_reason": reason,
                "recommended_handling": action,
                "fail_mode": "TOOL_FILE",
                "last_write_time": _mtime(path),
                "size_bytes": path.stat().st_size,
                **flags,
            }
        )
    return rows


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "source_type",
        "name",
        "path",
        "line",
        "role",
        "classification_reason",
        "recommended_handling",
        "fail_mode",
        "trading_effect",
        "entry_pool_filter_effect",
        "order_dispatch_effect",
        "policy_change_applied",
        "last_write_time",
        "size_bytes",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_role = Counter(row["role"] for row in rows)
    by_source_role: dict[str, dict[str, int]] = defaultdict(dict)
    for (source, role), count in Counter((row["source_type"], row["role"]) for row in rows).items():
        by_source_role[source][role] = count
    active = [row for row in rows if row["role"] == ROLE_ACTIVE]
    observe = [row for row in rows if row["role"] == ROLE_OBSERVE]
    core = [row for row in rows if row["role"] == ROLE_CORE]
    unclassified = [row for row in rows if row["role"] == ROLE_UNCLASSIFIED]
    return {
        "total_rows": len(rows),
        "role_counts": dict(by_role),
        "source_role_counts": by_source_role,
        "active_policy_rows": len(active),
        "observe_only_rows": len(observe),
        "core_rows": len(core),
        "unclassified_rows": len(unclassified),
        "active_policy_sample": active[:20],
        "unclassified_sample": unclassified[:30],
        "recommendations": [
            "Do not add new ACTIVE_POLICY rows without replacing or retiring an existing strategy-quality policy.",
            "Keep CORE fail-closed and small; move diagnostics that do not change execution into OBSERVE_ONLY or REPORT.",
            "OBSERVE_ONLY and REPORT should be grouped into one daily status view instead of expanding dashboard noise.",
            "Every ACTIVE_POLICY artifact should expose trading_effect, entry_pool_filter_effect, order_dispatch_effect, and policy_change_applied.",
        ],
    }


def _write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Logic Role Inventory Audit",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- total_rows: `{payload['summary']['total_rows']}`",
        "",
        "## Role Counts",
    ]
    for role, count in sorted(payload["summary"]["role_counts"].items()):
        lines.append(f"- {role}: `{count}`")
    lines.extend(["", "## Source Role Counts"])
    for source, counts in sorted(payload["summary"]["source_role_counts"].items()):
        parts = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
        lines.append(f"- {source}: {parts}")
    lines.extend(["", "## Active Policy Sample"])
    for row in payload["summary"]["active_policy_sample"][:12]:
        lines.append(f"- `{row['source_type']}` `{row['name']}`: {row['classification_reason']}")
    lines.extend(["", "## Recommendations"])
    for rec in payload["summary"]["recommendations"]:
        lines.append(f"- {rec}")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    rows = _batch_rows() + _artifact_rows() + _tool_rows()
    payload = {
        "generated_at": _now(),
        "status": "PASS",
        "schema_version": "logic_role_inventory_audit_v1",
        "source_files": {
            "run_paper_daily": str(RUN_DAILY),
            "tools_dir": str(TOOLS_DIR),
            "logs_dir": str(LOG_DIR),
        },
        "outputs": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
            "md": str(OUT_MD),
        },
        "classification_rules": {
            "roles": [ROLE_CORE, ROLE_ACTIVE, ROLE_OBSERVE, ROLE_REPORT, ROLE_UNCLASSIFIED],
            "note": "Name-based first-pass audit. UNCLASSIFIED_REVIEW rows require manual follow-up before policy decisions.",
        },
        "summary": _summary(rows),
    }
    _write_csv(OUT_CSV, rows)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_md(OUT_MD, payload)
    print(json.dumps({"status": "PASS", "rows": len(rows), "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
