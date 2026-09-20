from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

POLICY_GROUP_JSON = LOG_DIR / "entry_policy_group_report_latest.json"
POLICY_GROUP_CSV = LOG_DIR / "entry_policy_group_report_latest.csv"
RUNTIME_QUALITY_JSON = LOG_DIR / "runtime_normal_quality_separation_report_latest.json"
RUNTIME_QUALITY_CSV = LOG_DIR / "runtime_normal_quality_separation_report_latest.csv"
STRICT_PROOF_JSON = LOG_DIR / "strict_normal_policy_proof_audit_latest.json"

OUT_JSON = LOG_DIR / "policy_group_quality_link_report_latest.json"
OUT_CSV = LOG_DIR / "policy_group_quality_link_report_latest.csv"


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except UnicodeDecodeError:
            continue
    return {}


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    with path.open("r", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: List[Dict[str, Any]], fields: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _runtime_quality_bucket(row: Dict[str, str]) -> str:
    label = str(row.get("runtime_quality_label") or "").strip()
    usable = str(row.get("strict_normal_proof_usable") or "").strip().lower()
    if usable == "false":
        return "runtime_not_strict_proof"
    if label in {"L3_RELAXED", "RISK_FLAGGED", "NON_NATURAL_PASS"}:
        return "runtime_marked_not_clean"
    return "runtime_potential_strict_proof"


def main() -> int:
    policy_group = _read_json(POLICY_GROUP_JSON)
    policy_rows = _read_csv(POLICY_GROUP_CSV)
    runtime_quality = _read_json(RUNTIME_QUALITY_JSON)
    runtime_rows = _read_csv(RUNTIME_QUALITY_CSV)
    strict_proof = _read_json(STRICT_PROOF_JSON)

    normal_rows = [row for row in policy_rows if str(row.get("entry_policy_group") or "") == "normal_entry"]
    normal_realized = [
        row for row in normal_rows if str(row.get("source") or "") == "trades_calc" and str(row.get("net_ret") or "").strip()
    ]
    runtime_bucket_counts = Counter(_runtime_quality_bucket(row) for row in runtime_rows)
    runtime_label_counts = Counter(str(row.get("runtime_quality_label") or "") for row in runtime_rows)

    rows: List[Dict[str, Any]] = [
        {
            "scope": "historical_policy_group",
            "metric": "entry_policy_group_report_rows",
            "value": policy_group.get("rows", 0),
            "decision": "REFERENCE_ONLY",
            "reason": "All labelled historical rows, not strict proof",
            "trading_effect": "false",
            "policy_effect": "false",
            "policy_change_applied": "false",
        },
        {
            "scope": "historical_policy_group",
            "metric": "normal_entry_rows",
            "value": len(normal_rows),
            "decision": "DO_NOT_USE_AS_STRICT_POLICY_PROOF",
            "reason": "normal_entry contains fills without realized returns and what-if rows",
            "trading_effect": "false",
            "policy_effect": "false",
            "policy_change_applied": "false",
        },
        {
            "scope": "historical_policy_group",
            "metric": "normal_entry_realized_rows",
            "value": len(normal_realized),
            "decision": "REQUIRE_STRICT_PROOF_FILTER",
            "reason": "realized normal rows still include historical fallback and weak-quality rows",
            "trading_effect": "false",
            "policy_effect": "false",
            "policy_change_applied": "false",
        },
        {
            "scope": "strict_policy_proof",
            "metric": "strict_policy_proof_rows",
            "value": strict_proof.get("strict_policy_proof_rows", 0),
            "decision": "TOO_SMALL_FOR_POLICY_VALUE_CHANGE",
            "reason": str(strict_proof.get("sample_policy_decision") or ""),
            "trading_effect": "false",
            "policy_effect": "false",
            "policy_change_applied": "false",
        },
        {
            "scope": "runtime_quality",
            "metric": "candidate_rows",
            "value": runtime_quality.get("candidate_rows", 0),
            "decision": "LABEL_ONLY",
            "reason": "current runtime candidates are labelled without changing entry behavior",
            "trading_effect": "false",
            "policy_effect": "false",
            "policy_change_applied": "false",
        },
        {
            "scope": "runtime_quality",
            "metric": "runtime_not_strict_proof_rows",
            "value": runtime_bucket_counts.get("runtime_not_strict_proof", 0),
            "decision": "EXCLUDE_FROM_NORMAL_ENTRY_STATS_FOR_POLICY_VALUE_REVIEW",
            "reason": "runtime quality label says strict_normal_proof_usable=false",
            "trading_effect": "false",
            "policy_effect": "false",
            "policy_change_applied": "false",
        },
    ]

    reporting_rules = [
        "Do not use entry_policy_group=normal_entry alone as policy-value proof.",
        "Use strict_normal_proof_usable=false to exclude weak-quality runtime rows from normal-entry policy statistics.",
        "Keep SECTOR_UNION_NOT_EXECUTION_POOL outside strict normal-entry proof.",
        "Keep L3_WITH_RISK_FLAGS outside strict normal-entry proof.",
        "Treat remaining strict proof as review evidence only when sample size is small.",
    ]

    out = {
        "generated_at": _now_ts(),
        "status": "PASS",
        "schema_version": "policy_group_quality_link_report_v1",
        "source_files": {
            "entry_policy_group_report": str(POLICY_GROUP_JSON),
            "entry_policy_group_csv": str(POLICY_GROUP_CSV),
            "runtime_quality_report": str(RUNTIME_QUALITY_JSON),
            "runtime_quality_csv": str(RUNTIME_QUALITY_CSV),
            "strict_proof": str(STRICT_PROOF_JSON),
        },
        "entry_policy_group_counts": policy_group.get("entry_policy_group_counts", {}),
        "normal_entry_rows": len(normal_rows),
        "normal_entry_realized_rows": len(normal_realized),
        "strict_policy_proof_rows": strict_proof.get("strict_policy_proof_rows", 0),
        "runtime_candidate_rows": runtime_quality.get("candidate_rows", 0),
        "runtime_quality_label_counts": dict(sorted(runtime_label_counts.items())),
        "runtime_quality_bucket_counts": dict(sorted(runtime_bucket_counts.items())),
        "reporting_rules": reporting_rules,
        "sample_policy_decision": "LINK_POLICY_GROUP_TO_QUALITY_LABELS_NO_POLICY_VALUE_CHANGE",
        "interpretation": (
            "Policy-group labels and runtime quality labels must be used together. "
            "normal_entry is a broad label, while strict_normal_proof_usable controls whether a row can be used for policy-value review."
        ),
        "decision": "NO_ALTERNATIVE_PATH_NO_POLICY_RELAXATION",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }

    fields = [
        "scope",
        "metric",
        "value",
        "decision",
        "reason",
        "trading_effect",
        "policy_effect",
        "policy_change_applied",
    ]
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows, fields)
    print(
        "[FINAL] policy group quality link report -> "
        f"{OUT_JSON} strict_rows={out['strict_policy_proof_rows']} runtime_rows={out['runtime_candidate_rows']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
