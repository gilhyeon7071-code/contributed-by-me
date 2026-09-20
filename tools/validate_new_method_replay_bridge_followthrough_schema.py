from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
BRIDGE_CSV = LOG_DIR / "new_method_candidates_for_replay_latest.csv"
BRIDGE_META = LOG_DIR / "new_method_candidates_for_replay_meta.json"
FOLLOWTHROUGH_SCRIPT = ROOT / "tools" / "followthrough_realtime.py"

OUT_JSON = LOG_DIR / "new_method_replay_bridge_followthrough_schema_latest.json"
OUT_ISSUES = LOG_DIR / "new_method_replay_bridge_followthrough_schema_issues_latest.csv"
OUT_MD = LOG_DIR / "new_method_replay_bridge_followthrough_schema_latest.md"


REQUIRED_COLUMNS = [
    "date",
    "code",
    "name",
    "market",
    "market_regime",
    "close",
    "value",
    "score",
    "final_score",
    "stretch",
    "atr14_pct",
    "high_52w_gap",
    "relax_level",
    "candidate_origin",
    "method_branch",
    "horizon",
    "observe_status",
    "promotion_blocker",
    "forward_return_status",
]

FOLLOWTHROUGH_KEEP_COLUMNS = [
    "code",
    "name",
    "market",
    "close",
    "value",
    "score",
    "final_score",
    "stretch",
    "atr14_pct",
    "high_52w_gap",
    "relax_level",
    "candidate_origin",
]

NUMERIC_COLUMNS = [
    "close",
    "value",
    "score",
    "final_score",
    "stretch",
    "atr14_pct",
    "high_52w_gap",
]

OPERATIONAL_CANDIDATE_FILES = [
    LOG_DIR / "candidates_latest_data.csv",
    LOG_DIR / "candidates_latest_data.with_final_score.csv",
    LOG_DIR / "candidates_latest.csv",
    LOG_DIR / "candidates_latest_meta.json",
]


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _mtime(path: Path) -> str | None:
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")


def _issue(issues: list[dict[str, Any]], severity: str, code: str, detail: str, impact: str) -> None:
    issues.append(
        {
            "severity": severity,
            "issue_code": code,
            "detail": detail,
            "impact": impact,
        }
    )


def _read_meta() -> dict[str, Any]:
    if not BRIDGE_META.exists():
        return {}
    return json.loads(BRIDGE_META.read_text(encoding="utf-8"))


def _followthrough_static_evidence() -> dict[str, Any]:
    text = FOLLOWTHROUGH_SCRIPT.read_text(encoding="utf-8", errors="replace")
    return {
        "script": str(FOLLOWTHROUGH_SCRIPT),
        "default_candidates_present": "DEFAULT_CANDIDATES" in text,
        "build_signals_present": "def build_signals(" in text,
        "code_join_present": 'on="code"' in text or "on='code'" in text,
        "fills_missing_candidate_numeric_columns_with_zero": "if c not in cand.columns" in text
        and "cand[c] = 0.0" in text,
        "keep_candidate_columns_checked": all(col in text for col in FOLLOWTHROUGH_KEEP_COLUMNS),
    }


def main() -> int:
    issues: list[dict[str, Any]] = []
    if not BRIDGE_CSV.exists():
        _issue(
            issues,
            "HIGH",
            "missing_bridge_candidate_file",
            str(BRIDGE_CSV),
            "schema validation cannot run",
        )
        df = pd.DataFrame()
    else:
        df = pd.read_csv(BRIDGE_CSV, dtype={"code": "string"})

    source_columns = list(df.columns)
    missing_required = [c for c in REQUIRED_COLUMNS if c not in source_columns]
    missing_keep = [c for c in FOLLOWTHROUGH_KEEP_COLUMNS if c not in source_columns]
    if missing_required:
        _issue(
            issues,
            "HIGH",
            "missing_required_columns",
            ", ".join(missing_required),
            "new-method replay bridge is not self-describing enough for validation",
        )
    if missing_keep:
        _issue(
            issues,
            "HIGH",
            "missing_followthrough_keep_columns",
            ", ".join(missing_keep),
            "followthrough candidate merge would silently fill or lose candidate fields",
        )

    row_count = int(len(df))
    date_count = int(df["date"].nunique()) if "date" in df.columns else 0
    branch_count = int(df["method_branch"].nunique()) if "method_branch" in df.columns else 0
    duplicate_code_rows = int(df.duplicated(["code"], keep=False).sum()) if "code" in df.columns else 0
    duplicate_date_code_rows = (
        int(df.duplicated(["date", "code"], keep=False).sum())
        if {"date", "code"}.issubset(df.columns)
        else 0
    )

    if "code" in df.columns:
        bad_code_rows = int((df["code"].astype("string").str.len() != 6).sum())
        if bad_code_rows:
            _issue(
                issues,
                "HIGH",
                "invalid_code_length",
                f"rows={bad_code_rows}",
                "followthrough removes non-6-digit codes before joining",
            )

    for col in NUMERIC_COLUMNS:
        if col not in df.columns:
            continue
        parsed = pd.to_numeric(df[col], errors="coerce")
        bad = int(parsed.isna().sum())
        if bad:
            _issue(
                issues,
                "HIGH",
                "non_numeric_candidate_field",
                f"{col}: bad_rows={bad}",
                "followthrough risk and score calculations require numeric candidate fields",
            )

    if "candidate_origin" in df.columns:
        unexpected_origin = sorted(
            str(v)
            for v in df.loc[df["candidate_origin"].astype(str) != "NEW_METHOD_OBSERVE_ONLY", "candidate_origin"]
            .dropna()
            .unique()
        )
        if unexpected_origin:
            _issue(
                issues,
                "MEDIUM",
                "unexpected_candidate_origin",
                ", ".join(unexpected_origin),
                "candidate provenance is not isolated from the observe-only layer",
            )

    if date_count > 1:
        _issue(
            issues,
            "HIGH",
            "multi_date_file_not_followthrough_ready",
            f"date_count={date_count}",
            "followthrough joins a current intraday snapshot by code; mixed historical dates can create misleading review rows",
        )
    if duplicate_code_rows:
        _issue(
            issues,
            "HIGH",
            "duplicate_code_would_multiply_join",
            f"duplicate_code_rows={duplicate_code_rows}",
            "a code-only intraday join can multiply rows when the candidate file contains the same code in multiple branches or dates",
        )
    if duplicate_date_code_rows:
        _issue(
            issues,
            "MEDIUM",
            "duplicate_date_code_rows",
            f"duplicate_date_code_rows={duplicate_date_code_rows}",
            "candidate grain is not unique even within the same date",
        )

    high_issue_count = sum(1 for item in issues if item["severity"] == "HIGH")
    schema_column_compatible = not missing_keep and all(c in source_columns for c in FOLLOWTHROUGH_KEEP_COLUMNS)
    if schema_column_compatible and high_issue_count == 0:
        conclusion = "FOLLOWTHROUGH_SCHEMA_READY"
        next_action = "can run isolated followthrough replay with an explicit candidate path"
    elif schema_column_compatible:
        conclusion = "SCHEMA_COLUMNS_COMPATIBLE_BUT_NOT_FOLLOWTHROUGH_READY"
        next_action = "build a single-date single-branch followthrough candidate slice before runtime replay"
    else:
        conclusion = "FOLLOWTHROUGH_SCHEMA_NOT_READY"
        next_action = "fix missing bridge columns before any followthrough replay"

    branch_summary: list[dict[str, Any]] = []
    if {"method_branch", "date", "code"}.issubset(df.columns):
        grouped = df.groupby("method_branch", dropna=False)
        for branch, group in grouped:
            branch_summary.append(
                {
                    "method_branch": str(branch),
                    "rows": int(len(group)),
                    "date_count": int(group["date"].nunique()),
                    "duplicate_code_rows": int(group.duplicated(["code"], keep=False).sum()),
                    "closed_rows": int((group.get("forward_return_status", "") == "CLOSED").sum())
                    if "forward_return_status" in group.columns
                    else 0,
                    "pending_rows": int((group.get("forward_return_status", "") != "CLOSED").sum())
                    if "forward_return_status" in group.columns
                    else 0,
                }
            )

    operational_file_state = {
        path.name: {
            "path": str(path),
            "exists": path.exists(),
            "mtime": _mtime(path),
        }
        for path in OPERATIONAL_CANDIDATE_FILES
    }

    payload = {
        "generated_at": _now(),
        "classification": "NEW_METHOD_REPLAY_BRIDGE_FOLLOWTHROUGH_SCHEMA_VALIDATION",
        "bridge_file": str(BRIDGE_CSV),
        "bridge_meta_file": str(BRIDGE_META),
        "followthrough_script": str(FOLLOWTHROUGH_SCRIPT),
        "operation_effect": "READ_ONLY_VALIDATION_ONLY",
        "operational_candidate_files_written": False,
        "conclusion": conclusion,
        "next_action": next_action,
        "row_count": row_count,
        "date_count": date_count,
        "branch_count": branch_count,
        "duplicate_code_rows": duplicate_code_rows,
        "duplicate_date_code_rows": duplicate_date_code_rows,
        "schema_column_compatible": schema_column_compatible,
        "missing_required_columns": missing_required,
        "missing_followthrough_keep_columns": missing_keep,
        "issues": issues,
        "branch_summary": branch_summary,
        "bridge_meta": _read_meta(),
        "followthrough_static_evidence": _followthrough_static_evidence(),
        "operational_file_state": operational_file_state,
    }

    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(issues).to_csv(OUT_ISSUES, index=False, encoding="utf-8-sig")

    md_lines = [
        "# New Method Replay Bridge Followthrough Schema Validation",
        "",
        f"- classification: {payload['classification']}",
        f"- operation_effect: {payload['operation_effect']}",
        f"- operational_candidate_files_written: {payload['operational_candidate_files_written']}",
        f"- conclusion: {conclusion}",
        f"- next_action: {next_action}",
        f"- row_count: {row_count}",
        f"- date_count: {date_count}",
        f"- branch_count: {branch_count}",
        f"- duplicate_code_rows: {duplicate_code_rows}",
        f"- duplicate_date_code_rows: {duplicate_date_code_rows}",
        f"- schema_column_compatible: {schema_column_compatible}",
        f"- high_issue_count: {high_issue_count}",
        "",
        "## Issues",
    ]
    if issues:
        for item in issues:
            md_lines.append(
                f"- {item['severity']} / {item['issue_code']}: {item['detail']} / impact={item['impact']}"
            )
    else:
        md_lines.append("- none")
    md_lines.extend(["", "## Branch Summary"])
    for item in branch_summary:
        md_lines.append(
            "- {method_branch}: rows={rows}, date_count={date_count}, duplicate_code_rows={duplicate_code_rows}, "
            "closed_rows={closed_rows}, pending_rows={pending_rows}".format(**item)
        )
    OUT_MD.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
