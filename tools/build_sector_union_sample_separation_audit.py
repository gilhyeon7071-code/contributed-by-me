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
SECTOR_UNION_JSON = LOG_DIR / "sector_union_entry_path_audit_latest.json"
SECTOR_UNION_CSV = LOG_DIR / "sector_union_entry_path_audit_latest.csv"

OUT_JSON = LOG_DIR / "sector_union_sample_separation_audit_latest.json"
OUT_CSV = LOG_DIR / "sector_union_sample_separation_audit_latest.csv"


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


def _bool_text(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def _classify_union_sample(row: Dict[str, Any]) -> Dict[str, Any]:
    code = str(row.get("code") or "").zfill(6)
    raw_execution_pool = _bool_text(row.get("raw_execution_pool"))
    union_ok = _bool_text(row.get("union_ok"))
    sector_action = str(row.get("sector_action") or "").strip().upper()
    sector_entry_allowed = _bool_text(row.get("sector_entry_allowed"))
    audit_status = str(row.get("audit_status") or "").strip()

    if raw_execution_pool:
        decision = "NORMAL_EXECUTION_POOL_SAMPLE"
        mix_with_normal_entry = True
        reason = "raw execution_pool is true"
    elif union_ok:
        decision = "SEPARATE_SECTOR_UNION_SAMPLE"
        mix_with_normal_entry = False
        reason = "raw execution_pool is false and inclusion depends on sector union criteria"
    else:
        decision = "NOT_ENTRY_SAMPLE"
        mix_with_normal_entry = False
        reason = "sector union criteria did not pass"

    return {
        "code": code,
        "name": row.get("name", ""),
        "candidate_origin": row.get("candidate_origin", ""),
        "final_score": row.get("final_score", ""),
        "raw_execution_pool": str(raw_execution_pool).lower(),
        "sector_action": sector_action,
        "sector_entry_allowed": str(sector_entry_allowed).lower(),
        "sector_strength": row.get("sector_strength", ""),
        "union_ok": str(union_ok).lower(),
        "source_audit_status": audit_status,
        "sample_separation_decision": decision,
        "mix_with_normal_entry_proof": str(mix_with_normal_entry).lower(),
        "reason": reason,
        "trading_effect": "false",
        "policy_effect": "false",
        "policy_change_applied": "false",
    }


def main() -> int:
    policy_group = _read_json(POLICY_GROUP_JSON)
    sector_union = _read_json(SECTOR_UNION_JSON)
    policy_rows = _read_csv(POLICY_GROUP_CSV)
    union_rows = _read_csv(SECTOR_UNION_CSV)

    classified = [_classify_union_sample(row) for row in union_rows]
    decision_counts = Counter(row["sample_separation_decision"] for row in classified)
    mix_counts = Counter(row["mix_with_normal_entry_proof"] for row in classified)
    normal_entry_rows = int(policy_group.get("entry_policy_group_counts", {}).get("normal_entry", 0) or 0)
    normal_return_rows = sum(
        1
        for row in policy_rows
        if str(row.get("entry_policy_group") or "") == "normal_entry" and str(row.get("net_ret") or "").strip()
    )
    sector_union_return_rows = sum(1 for row in classified if str(row.get("net_ret") or "").strip())

    separate_needed = any(row["sample_separation_decision"] == "SEPARATE_SECTOR_UNION_SAMPLE" for row in classified)
    out = {
        "generated_at": _now_ts(),
        "status": "PASS",
        "schema_version": "sector_union_sample_separation_audit_v1",
        "source_files": {
            "entry_policy_group_report": str(POLICY_GROUP_JSON),
            "entry_policy_group_csv": str(POLICY_GROUP_CSV),
            "sector_union_entry_path_audit": str(SECTOR_UNION_JSON),
            "sector_union_entry_path_csv": str(SECTOR_UNION_CSV),
        },
        "policy_group_rows": int(policy_group.get("rows", 0) or 0),
        "normal_entry_rows_in_policy_group_report": normal_entry_rows,
        "normal_entry_return_rows_in_policy_group_report": normal_return_rows,
        "sector_union_rows": len(classified),
        "sector_union_return_rows": sector_union_return_rows,
        "decision_counts": dict(sorted(decision_counts.items())),
        "mix_with_normal_entry_proof_counts": dict(sorted(mix_counts.items())),
        "sample_policy_decision": "SEPARATE_SECTOR_UNION_FROM_NORMAL_ENTRY_PROOF" if separate_needed else "NO_SEPARATION_NEEDED",
        "normal_entry_mixing_allowed": False if separate_needed else True,
        "reason": (
            "SECTOR_PREFILTER_UNION has different inclusion semantics from raw execution_pool normal entry. "
            "It should remain a separate evidence group until realized return rows exist under that exact path."
        )
        if separate_needed
        else "No sector-union-only samples were found.",
        "decision": "NO_ALTERNATIVE_PATH_NO_POLICY_RELAXATION",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }

    fields = [
        "code",
        "name",
        "candidate_origin",
        "final_score",
        "raw_execution_pool",
        "sector_action",
        "sector_entry_allowed",
        "sector_strength",
        "union_ok",
        "source_audit_status",
        "sample_separation_decision",
        "mix_with_normal_entry_proof",
        "reason",
        "trading_effect",
        "policy_effect",
        "policy_change_applied",
    ]
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, classified, fields)
    print(
        "[FINAL] sector union sample separation audit -> "
        f"{OUT_JSON} rows={len(classified)} decision={out['sample_policy_decision']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
