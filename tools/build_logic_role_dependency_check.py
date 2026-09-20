from __future__ import annotations

import csv
import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SOURCE_CSV = LOG_DIR / "logic_role_retire_candidate_review_latest.csv"
OUT_JSON = LOG_DIR / "logic_role_dependency_check_latest.json"
OUT_CSV = LOG_DIR / "logic_role_dependency_check_latest.csv"
OUT_MD = LOG_DIR / "logic_role_dependency_check_latest.md"


STATIC_DEPENDENCIES: dict[str, dict[str, Any]] = {
    "tools\\indicator_diag_and_recommend.py": {
        "batch_mode": "toggle_default_on_fail_soft_warn",
        "writes_policy_or_params": False,
        "batch_toggle": "INDICATOR_DIAG_AUTO",
        "default_behavior": "run",
        "known_outputs": [
            "2_Logs/indicator_diag_recommend_meta_latest.json",
            "2_Logs/indicator_diag_summary_latest_h*.json",
            "2_Logs/indicator_diag_binary_latest_h*.csv",
            "2_Logs/indicator_diag_continuous_latest_h*.csv",
            "2_Logs/indicator_diag_daily_combo_latest_h*.csv",
            "12_Risk_Controlled/param_candidates_v41_1_latest.json",
            "12_Risk_Controlled/param_candidates_v41_1_latest.md",
        ],
        "dependency_verdict": "TOGGLE_PRESENT_DEFAULT_ON",
        "reason": "diagnostic recommendation output already has INDICATOR_DIAG_AUTO toggle with default run behavior",
    },
    "tools\\auto_signal_tuner.py": {
        "batch_mode": "toggle_default_on_post_chain_fail_on_error_propose_only",
        "writes_policy_or_params": False,
        "batch_toggle": "AUTO_SIGNAL_TUNER_AUTO",
        "default_behavior": "run",
        "known_outputs": ["2_Logs/auto_signal_tune_candidate_latest.json"],
        "dependency_verdict": "TOGGLE_PRESENT_DEFAULT_ON",
        "reason": "daily path runs propose only and now has AUTO_SIGNAL_TUNER_AUTO toggle with default run behavior",
    },
    "tools\\build_drift_monitor.py": {
        "batch_mode": "post_chain_fail_on_error_report_only",
        "writes_policy_or_params": False,
        "known_outputs": [
            "2_Logs/drift_monitor_latest.json",
            "2_Logs/drift_monitor_YYYYMMDD.json",
            "2_Logs/latency_heatmap_YYYYMMDD.json",
            "2_Logs/slippage_by_venue_YYYYMMDD.json",
            "2_Logs/recon_diff_YYYYMMDD.json",
            "2_Logs/account_pnl_drift_YYYYMMDD.json",
        ],
        "dependency_verdict": "KEEP_OR_DEGRADE_TO_WARN_ONLY_REVIEW",
        "reason": "report-only by comment, but wired to POST_CHAIN_STEP_FAIL and referenced by policy config",
    },
    "tools\\build_strategy_optimization_analyzer.py": {
        "batch_mode": "batch_label_only_not_executed_as_roota_script",
        "writes_policy_or_params": False,
        "known_outputs": ["2_Logs/strategy_optimization_latest.json"],
        "dependency_verdict": "NO_BATCH_EDIT_LABEL_DUPLICATE",
        "reason": "RootA script exists, but run_paper_daily only calls the RootB script; this row came from the error label",
    },
    "E:\\vibe\\buffett\\tools\\build_strategy_optimization_analyzer.py": {
        "batch_mode": "post_chain_fail_on_error_rootb_report_only",
        "writes_policy_or_params": False,
        "known_outputs": ["RootB strategy optimization artifact"],
        "dependency_verdict": "ROOTB_DEPENDENCY_REVIEW_BEFORE_MOVE",
        "reason": "RootB script is called from RootA batch; RootB dashboard dependency must be checked separately",
    },
    "tools\\surge_param_validator.py": {
        "batch_mode": "post_chain_fail_on_error_propose; apply_only_when_SURGE_PARAM_AUTO_APPLY_1",
        "writes_policy_or_params": "apply_mode_only",
        "known_outputs": [
            "2_Logs/surge_param_proposal_latest.json",
            "2_Logs/surge_param_apply_latest.json",
            "2_Logs/surge_ml_retrain_latest.json",
            "paper/surge_params.json",
        ],
        "dependency_verdict": "SPLIT_PROPOSE_WEEKLY_KEEP_APPLY_OPT_IN",
        "reason": "propose is diagnostic; apply can change surge_params and must remain explicitly opt-in",
    },
}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _path_for_name(name: str) -> Path | None:
    n = str(name or "").replace("\\\\", "\\")
    if re.match(r"^[A-Za-z]:\\", n):
        return Path(n)
    return ROOT / n


def _extract_literal_outputs(path: Path | None) -> list[str]:
    if path is None or not path.exists() or path.suffix.lower() != ".py":
        return []
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return []
    hits: set[str] = set()
    for m in re.finditer(r"['\"]([^'\"]*(?:latest|2_Logs|12_Risk_Controlled|surge_params)[^'\"]*)['\"]", text):
        val = m.group(1).strip()
        if len(val) <= 160:
            hits.add(val)
    return sorted(hits)[:30]


def _artifact_exists(pattern: str) -> str:
    p = pattern.replace("/", "\\")
    if "*" in p or "YYYYMMDD" in p or "RootB " in p:
        return "PATTERN"
    full = Path(p) if re.match(r"^[A-Za-z]:\\", p) else ROOT / p
    return "YES" if full.exists() else "NO"


def _build_rows() -> list[dict[str, Any]]:
    rows = [
        row for row in _read_csv(SOURCE_CSV)
        if row.get("retire_review_action") == "MOVE_TO_MANUAL_OR_WEEKLY_CANDIDATE"
    ]
    out: list[dict[str, Any]] = []
    for row in rows:
        name = str(row.get("name") or "").replace("\\\\", "\\")
        meta = STATIC_DEPENDENCIES.get(name, {})
        path = _path_for_name(name)
        known_outputs = list(meta.get("known_outputs") or [])
        literal_outputs = _extract_literal_outputs(path)
        output_status = [
            {"artifact": item, "exists": _artifact_exists(item)}
            for item in known_outputs
        ]
        out.append(
            {
                "name": name,
                "script_exists": bool(path and path.exists()),
                "batch_lines": row.get("lines"),
                "source_row_count": row.get("source_row_count"),
                "batch_mode": meta.get("batch_mode", "UNKNOWN"),
                "writes_policy_or_params": meta.get("writes_policy_or_params", "UNKNOWN"),
                "batch_toggle": meta.get("batch_toggle", ""),
                "default_behavior": meta.get("default_behavior", ""),
                "dependency_verdict": meta.get("dependency_verdict", "DEPENDENCY_REVIEW"),
                "reason": meta.get("reason", "not enough static metadata"),
                "known_outputs": "; ".join(known_outputs),
                "known_output_status": json.dumps(output_status, ensure_ascii=False),
                "literal_output_hints": "; ".join(literal_outputs),
            }
        )
    return out


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "name",
        "script_exists",
        "batch_lines",
        "source_row_count",
        "batch_mode",
        "writes_policy_or_params",
        "batch_toggle",
        "default_behavior",
        "dependency_verdict",
        "reason",
        "known_outputs",
        "known_output_status",
        "literal_output_hints",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Logic Role Dependency Check",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- rows: `{payload['summary']['rows']}`",
        "",
        "## Verdict Counts",
    ]
    for verdict, count in sorted(payload["summary"]["verdict_counts"].items()):
        lines.append(f"- {verdict}: `{count}`")
    lines.extend(["", "## Rows"])
    for row in payload["rows"]:
        lines.append(f"- `{row['name']}` -> `{row['dependency_verdict']}`: {row['reason']}")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    rows = _build_rows()
    counts = Counter(row["dependency_verdict"] for row in rows)
    payload = {
        "generated_at": _now(),
        "status": "PASS",
        "schema_version": "logic_role_dependency_check_v1",
        "source_files": {"retire_candidate_review_csv": str(SOURCE_CSV)},
        "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV), "md": str(OUT_MD)},
        "scope": {
            "policy_effect": "read_only_dependency_check_only",
            "full_logic_application": "NOT_APPLIED",
        },
        "summary": {
            "rows": len(rows),
            "verdict_counts": dict(counts),
            "notes": [
                "This report does not remove, disable, reschedule, or edit run_paper_daily.bat.",
                "POST_CHAIN_STEP_FAIL wiring is treated as operational dependency even when a step is report-only.",
                "Surge apply mode is separate from propose mode because it can write paper/surge_params.json.",
            ],
        },
        "rows": rows,
    }
    _write_csv(OUT_CSV, rows)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_md(OUT_MD, payload)
    print(json.dumps({"status": "PASS", "rows": len(rows), "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
