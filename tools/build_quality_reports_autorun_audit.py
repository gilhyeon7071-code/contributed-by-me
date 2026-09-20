from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

RUN_PAPER_DAILY = ROOT / "run_paper_daily.bat"
RUN_NEWS_PIPELINE = ROOT / "run_news_pipeline_once.bat"

REPORT_TOOLS = [
    "tools\\build_entry_policy_group_report.py",
    "tools\\build_pure_normal_loss_driver_audit.py",
    "tools\\build_runtime_normal_quality_separation_report.py",
    "tools\\build_policy_group_quality_link_report.py",
    "tools\\build_strict_normal_policy_proof_audit.py",
    "tools\\build_pure_normal_entry_sample_audit.py",
    "tools\\build_cluster_candidate_quality_audit.py",
]

OUT_JSON = LOG_DIR / "quality_reports_autorun_audit_latest.json"
OUT_CSV = LOG_DIR / "quality_reports_autorun_audit_latest.csv"


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_text(path: Path) -> str:
    if not path.exists():
        return ""
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return path.read_text(encoding=enc)
        except UnicodeDecodeError:
            continue
    return path.read_text(errors="replace")


def _contains_tool(batch_text: str, tool: str) -> bool:
    norm = batch_text.replace("/", "\\").lower()
    return tool.lower() in norm


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    import csv

    fields = [
        "entrypoint",
        "tool",
        "included",
        "recommended_position",
        "recommendation",
        "trading_effect",
        "policy_effect",
        "policy_change_applied",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def main() -> int:
    paper_text = _read_text(RUN_PAPER_DAILY)
    news_text = _read_text(RUN_NEWS_PIPELINE)
    entrypoints = {
        "run_paper_daily.bat": (RUN_PAPER_DAILY, paper_text),
        "run_news_pipeline_once.bat": (RUN_NEWS_PIPELINE, news_text),
    }

    rows: List[Dict[str, Any]] = []
    for name, (_path, text) in entrypoints.items():
        for tool in REPORT_TOOLS:
            included = _contains_tool(text, tool)
            if name == "run_paper_daily.bat":
                recommended_position = "after [7.08a/9] shadow_promotion_report and before optional shadow_collect"
            else:
                recommended_position = "not recommended; quality reports need paper_engine/runtime artifacts"
            rows.append(
                {
                    "entrypoint": name,
                    "tool": tool,
                    "included": str(included).lower(),
                    "recommended_position": recommended_position,
                    "recommendation": "KEEP_READ_ONLY_FAIL_SOFT_IF_ADDED" if not included else "ALREADY_INCLUDED",
                    "trading_effect": "false",
                    "policy_effect": "false",
                    "policy_change_applied": "false",
                }
            )

    missing_from_paper = [
        row["tool"]
        for row in rows
        if row["entrypoint"] == "run_paper_daily.bat" and row["included"] == "false"
    ]
    included_any = [row for row in rows if row["included"] == "true"]
    all_paper_included = len(missing_from_paper) == 0

    out = {
        "generated_at": _now_ts(),
        "status": "PASS",
        "schema_version": "quality_reports_autorun_audit_v1",
        "source_files": {
            "run_paper_daily": str(RUN_PAPER_DAILY),
            "run_news_pipeline_once": str(RUN_NEWS_PIPELINE),
        },
        "report_tools_checked": REPORT_TOOLS,
        "included_any_count": len(included_any),
        "missing_from_run_paper_daily": missing_from_paper,
        "recommended_entrypoint": "run_paper_daily.bat",
        "recommended_position": "after [7.08a/9] shadow_promotion_report and before optional shadow_collect",
        "recommendation": (
            "AUTORUN_INCLUDED_READ_ONLY_FAIL_SOFT"
            if all_paper_included
            else "NOT_AUTORUN_YET_ADD_ONLY_WITH_EXPLICIT_APPROVAL"
        ),
        "reason": (
            "These reports depend on current candidate/runtime artifacts and should be fail-soft read-only if added. "
            "This audit only checks autorun inclusion and does not change trading policy."
        ),
        "decision": "AUTORUN_INCLUDED_READ_ONLY" if all_paper_included else "NO_AUTORUN_CHANGE_APPLIED",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    print(
        "[FINAL] quality reports autorun audit -> "
        f"{OUT_JSON} missing_from_run_paper_daily={len(missing_from_paper)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
