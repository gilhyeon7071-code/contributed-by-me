from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SOURCES = {
    "exit_quality": LOG_DIR / "mdd_exit_quality_diagnostic_latest.json",
    "late_case": LOG_DIR / "mdd_exit_late_case_diagnostic_latest.json",
    "counterfactual": LOG_DIR / "mdd_exit_counterfactual_diagnostic_latest.json",
    "rule_axis": LOG_DIR / "mdd_exit_rule_axis_diagnostic_latest.json",
    "ddm_stop_stacking": LOG_DIR / "mdd_ddm_stop_stacking_diagnostic_latest.json",
    "ddm_stop_defer": LOG_DIR / "mdd_ddm_stop_defer_counterfactual_latest.json",
    "stop_residual": LOG_DIR / "mdd_stop_family_residual_diagnostic_latest.json",
    "candidate_quality": LOG_DIR / "paper_mdd_candidate_quality_entry_timing_latest.json",
}

OUT_JSON = LOG_DIR / "mdd_stop_ddm_policy_value_review_latest.json"
OUT_CSV = LOG_DIR / "mdd_stop_ddm_policy_value_review_latest.csv"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"_missing": True, "path": str(path)}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except UnicodeDecodeError:
            continue
    return {}


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(str(value).replace(",", ""))
    except Exception:
        return default


def _dict_get(d: dict[str, Any], path: str, default: Any = None) -> Any:
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["axis", "status", "evidence", "implication"]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def build() -> dict[str, Any]:
    data = {name: _read_json(path) for name, path in SOURCES.items()}
    missing_sources = [name for name, obj in data.items() if obj.get("_missing")]

    quality_counts = data["exit_quality"].get("quality_counts", {}) if isinstance(data["exit_quality"].get("quality_counts"), dict) else {}
    protective = int(_num(quality_counts.get("PROTECTIVE_CONTINUED_DOWNSIDE")))
    late_or_over = int(_num(quality_counts.get("POTENTIALLY_LATE_OR_OVER_EXIT")))
    ambiguous = int(_num(quality_counts.get("AMBIGUOUS_FLAT_AFTER_EXIT")))
    unobservable = int(_num(quality_counts.get("UNOBSERVABLE")))
    target_rows = int(_num(_dict_get(data["exit_quality"], "filter_rows.target_exit_rows")))

    candidate_scope = data["candidate_quality"].get("scope", {}) if isinstance(data["candidate_quality"].get("scope"), dict) else {}
    candidate_breakdown = data["candidate_quality"].get("breakdown", {}) if isinstance(data["candidate_quality"].get("breakdown"), dict) else {}
    candidate_missing = data["candidate_quality"].get("candidate_missing_diagnosis", {}) if isinstance(data["candidate_quality"].get("candidate_missing_diagnosis"), dict) else {}

    axis_counts = data["rule_axis"].get("axis_counts", {}) if isinstance(data["rule_axis"].get("axis_counts"), dict) else {}
    stacking_labels = data["ddm_stop_stacking"].get("label_counts", {}) if isinstance(data["ddm_stop_stacking"].get("label_counts"), dict) else {}
    defer_variants = data["ddm_stop_defer"].get("variants", []) if isinstance(data["ddm_stop_defer"].get("variants"), list) else []
    stop_summary = data["stop_residual"].get("summary", {}) if isinstance(data["stop_residual"].get("summary"), dict) else {}
    stop_flags = stop_summary.get("quality_flags", {}) if isinstance(stop_summary.get("quality_flags"), dict) else {}

    protective_ratio = (protective / target_rows) if target_rows else None
    late_ratio = (late_or_over / target_rows) if target_rows else None
    broad_relaxation_supported = bool(target_rows and late_or_over > protective)
    targeted_review_required = bool(late_or_over or axis_counts or stacking_labels or stop_flags)

    rows = [
        {
            "axis": "exit_quality_population",
            "status": "BROAD_RELAXATION_NOT_SUPPORTED" if not broad_relaxation_supported else "BROAD_RELAXATION_REVIEW_REQUIRED",
            "evidence": f"target_rows={target_rows}; protective={protective}; late_or_over={late_or_over}; ambiguous={ambiguous}; unobservable={unobservable}",
            "implication": "Most observed STOP/DDM target exits were protective, so broad STOP/DDM loosening is not supported by this evidence.",
        },
        {
            "axis": "late_or_over_cases",
            "status": "TARGETED_REVIEW_REQUIRED" if late_or_over else "NO_LATE_OVER_CASES",
            "evidence": f"case_count={data['late_case'].get('case_count')}; axis_counts={axis_counts}",
            "implication": "Weak cases are concentrated in specific axes rather than proving global threshold failure.",
        },
        {
            "axis": "same_day_ddm_stop_stacking",
            "status": "SEQUENCING_REVIEW_REQUIRED" if stacking_labels else "NO_STACKING_GROUPS",
            "evidence": f"group_count={data['ddm_stop_stacking'].get('group_count')}; oversell_count={data['ddm_stop_stacking'].get('oversell_count')}; labels={stacking_labels}",
            "implication": "DDM plus STOP stacking is a sequencing/lineage issue before it is a threshold-value proof.",
        },
        {
            "axis": "candidate_entry_quality",
            "status": "ENTRY_QUALITY_REVIEW_REQUIRED" if int(_num(candidate_scope.get('focus_rows'))) else "NO_FOCUS_ROWS",
            "evidence": f"focus_rows={candidate_scope.get('focus_rows')}; missing={_dict_get(data['candidate_quality'], 'candidate_join.candidate_missing_rows')}; quality={candidate_breakdown.get('quality_bucket_counts')}; missing_root={candidate_missing.get('root_cause_counts')}",
            "implication": "MDD remains heavily tied to intraday realtime entry quality and lineage coverage before STOP/DDM exits.",
        },
        {
            "axis": "stop_family_residual",
            "status": "STOP_METADATA_AND_REPEAT_REVIEW_REQUIRED" if stop_flags else "NO_STOP_RESIDUAL_FLAGS",
            "evidence": f"top_loss_count={stop_summary.get('stop_family_top_loss_trade_count')}; sum_net_ret={stop_summary.get('stop_family_top_loss_sum_net_ret')}; flags={stop_flags}",
            "implication": "Residual STOP losses need metadata/repeated-lineage review before changing STOP thresholds.",
        },
    ]

    recommendation = "KEEP_CURRENT_POLICY_VALUES_UNTIL_TARGETED_SEQUENCE_AND_ENTRY_QUALITY_PROOF"
    if broad_relaxation_supported:
        recommendation = "BROAD_POLICY_REVIEW_REQUIRED_BEFORE_CHANGE"

    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "status": "FAIL" if missing_sources or targeted_review_required else "PASS",
        "policy_change": False,
        "trading_effect": False,
        "source": {name: str(path) for name, path in SOURCES.items()},
        "missing_sources": missing_sources,
        "summary": {
            "target_rows": target_rows,
            "protective_continued_downside": protective,
            "potentially_late_or_over_exit": late_or_over,
            "ambiguous_flat_after_exit": ambiguous,
            "unobservable": unobservable,
            "protective_ratio": protective_ratio,
            "late_or_over_ratio": late_ratio,
            "broad_stop_ddm_relaxation_supported": broad_relaxation_supported,
            "targeted_review_required": targeted_review_required,
            "recommendation": recommendation,
        },
        "axis_rows": rows,
        "artifacts": {"csv": str(OUT_CSV)},
        "interpretation": [
            "This is a read-only synthesis of existing MDD STOP/DDM diagnostics.",
            "It does not prove the current STOP/DDM policy values are profit-optimal.",
            "It does reject broad STOP/DDM relaxation when protective exits dominate late/over-exit cases.",
            "Remaining work is targeted: same-day DDM/STOP sequencing, entry quality, and residual STOP metadata/repeat-lineage review.",
        ],
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    return payload


def main() -> int:
    payload = build()
    print(json.dumps({
        "status": payload["status"],
        "summary": payload["summary"],
        "missing_sources": payload["missing_sources"],
        "artifacts": payload["artifacts"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
