from __future__ import annotations

import csv
import json
import math
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

CANDIDATES_CSV = LOG_DIR / "candidates_latest_data.with_final_score.csv"
ENTRY_LAYER_CSV = LOG_DIR / "entry_decision_layers_runtime_latest.csv"
STRICT_PROOF_JSON = LOG_DIR / "strict_normal_policy_proof_audit_latest.json"

OUT_JSON = LOG_DIR / "runtime_normal_quality_separation_report_latest.json"
OUT_CSV = LOG_DIR / "runtime_normal_quality_separation_report_latest.csv"


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


def _float(value: Any) -> float | None:
    try:
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except Exception:
        return None


def _bool_text(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def _runtime_quality_label(row: Dict[str, str]) -> tuple[str, str, bool]:
    origin = str(row.get("candidate_origin") or "").strip().upper()
    execution_pool = _bool_text(row.get("execution_pool"))
    relax_level = str(row.get("relax_level") or "").strip().upper()
    junk_flags = str(row.get("junk_flags") or "").strip()
    junk_grade = str(row.get("junk_risk_grade") or "").strip().upper()
    natural_pass = _bool_text(row.get("natural_pass"))
    final_score = _float(row.get("final_score"))

    if origin == "SECTOR_PREFILTER_UNION" and not execution_pool:
        return "SECTOR_UNION_NOT_EXECUTION_POOL", "Separate from strict normal-entry proof", False
    if relax_level == "L3" and junk_flags:
        return "L3_WITH_RISK_FLAGS", "Separate from strict normal-entry proof", False
    if relax_level == "L3":
        return "L3_RELAXED", "Mark as relaxed; not clean proof without more evidence", True
    if junk_flags or junk_grade in {"MID", "WARN", "HIGH"}:
        return "RISK_FLAGGED", "Mark as risk flagged", True
    if not natural_pass:
        return "NON_NATURAL_PASS", "Mark as non-natural-pass candidate", True
    if final_score is None:
        return "MISSING_SCORE", "Not usable as scored proof", False
    return "STRICT_NORMAL_QUALITY_CANDIDATE", "Candidate has no weak-quality separation label", True


def main() -> int:
    candidates = _read_csv(CANDIDATES_CSV)
    entry_layer_rows = _read_csv(ENTRY_LAYER_CSV)
    strict_proof = _read_json(STRICT_PROOF_JSON)
    rows: List[Dict[str, Any]] = []
    for row in candidates:
        code = str(row.get("code") or "").zfill(6) if row.get("code") else ""
        if not code:
            continue
        label, reason, usable = _runtime_quality_label(row)
        rows.append(
            {
                "date": row.get("date") or row.get("date_yyyymmdd") or "",
                "code": code,
                "name": row.get("name", ""),
                "candidate_origin": row.get("candidate_origin", ""),
                "execution_pool": str(_bool_text(row.get("execution_pool"))).lower(),
                "natural_pass": str(_bool_text(row.get("natural_pass"))).lower(),
                "relax_level": row.get("relax_level", ""),
                "final_score": row.get("final_score", ""),
                "score": row.get("score", ""),
                "junk_risk_grade": row.get("junk_risk_grade", ""),
                "junk_flags": row.get("junk_flags", ""),
                "sector_action": row.get("sector_action", ""),
                "sector_entry_allowed": row.get("sector_entry_allowed", ""),
                "sector_strength": row.get("sector_strength", ""),
                "runtime_quality_label": label,
                "strict_normal_proof_usable": str(bool(usable)).lower(),
                "label_reason": reason,
                "trading_effect": "false",
                "policy_effect": "false",
                "policy_change_applied": "false",
            }
        )

    label_counts = Counter(row["runtime_quality_label"] for row in rows)
    usable_counts = Counter(row["strict_normal_proof_usable"] for row in rows)
    entry_layer_data_rows = sum(1 for row in entry_layer_rows if any(str(v or "").strip() for v in row.values()))
    out = {
        "generated_at": _now_ts(),
        "status": "PASS",
        "schema_version": "runtime_normal_quality_separation_report_v1",
        "source_files": {
            "candidates": str(CANDIDATES_CSV),
            "entry_layer": str(ENTRY_LAYER_CSV),
            "strict_proof": str(STRICT_PROOF_JSON),
        },
        "candidate_rows": len(rows),
        "entry_layer_rows": entry_layer_data_rows,
        "entry_layer_status": "EMPTY" if entry_layer_data_rows == 0 else "HAS_ROWS",
        "runtime_quality_label_counts": dict(sorted(label_counts.items())),
        "strict_normal_proof_usable_counts": dict(sorted(usable_counts.items())),
        "strict_proof_reference": {
            "strict_policy_proof_rows": strict_proof.get("strict_policy_proof_rows"),
            "excluded_weak_quality_rows": strict_proof.get("excluded_weak_quality_rows"),
            "sample_policy_decision": strict_proof.get("sample_policy_decision"),
        },
        "sample_policy_decision": "RUNTIME_SEPARATION_LABELS_ONLY_NO_POLICY_VALUE_CHANGE",
        "interpretation": (
            "Current runtime candidates are labelled for weak-quality separation. "
            "This does not change entry behavior; it prevents sector-union non-execution-pool and L3 risk-flag rows from being treated as strict normal-entry proof."
        ),
        "decision": "NO_ALTERNATIVE_PATH_NO_POLICY_RELAXATION",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }

    fields = [
        "date",
        "code",
        "name",
        "candidate_origin",
        "execution_pool",
        "natural_pass",
        "relax_level",
        "final_score",
        "score",
        "junk_risk_grade",
        "junk_flags",
        "sector_action",
        "sector_entry_allowed",
        "sector_strength",
        "runtime_quality_label",
        "strict_normal_proof_usable",
        "label_reason",
        "trading_effect",
        "policy_effect",
        "policy_change_applied",
    ]
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows, fields)
    print(
        "[FINAL] runtime normal quality separation report -> "
        f"{OUT_JSON} rows={len(rows)} entry_layer={out['entry_layer_status']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
