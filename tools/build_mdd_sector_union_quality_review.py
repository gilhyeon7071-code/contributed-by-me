from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SOURCE_CSV = LOG_DIR / "paper_mdd_candidate_quality_entry_timing_latest.csv"
OUT_JSON = LOG_DIR / "mdd_sector_union_quality_review_latest.json"
OUT_CSV = LOG_DIR / "mdd_sector_union_quality_review_latest.csv"


SECTOR_FIELDS = (
    "sector_action",
    "sector_entry_allowed",
    "sector_strength",
    "sector_reason",
    "sector_policy_reason",
)


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    return []


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value in ("", None):
            return default
        return float(value)
    except Exception:
        return default


def _sum_by(rows: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = defaultdict(lambda: {"rows": 0, "sum_net_ret": 0.0})
    for row in rows:
        bucket = str(row.get(key) or "blank")
        grouped[bucket]["rows"] += 1
        grouped[bucket]["sum_net_ret"] += _float(row.get("net_ret"))
    return dict(sorted(grouped.items(), key=lambda item: (-item[1]["rows"], item[0])))


def _load_candidate_row(candidate_file: str, code: str) -> dict[str, str] | None:
    if not candidate_file:
        return None
    path = LOG_DIR / candidate_file
    for row in _read_csv(path):
        if str(row.get("code") or "").zfill(6) == code:
            return row
    return None


def main() -> int:
    source_rows = _read_csv(SOURCE_CSV)
    target_rows = [
        row
        for row in source_rows
        if str(row.get("quality_bucket") or "") == "SECTOR_UNION_NOT_EXECUTION_POOL"
    ]

    rows: list[dict[str, Any]] = []
    field_presence = Counter()
    candidate_row_hits = 0

    for row in target_rows:
        code = str(row.get("code") or "").zfill(6)
        cand = _load_candidate_row(str(row.get("candidate_file") or ""), code)
        if cand:
            candidate_row_hits += 1
        present_fields = [field for field in SECTOR_FIELDS if cand and field in cand]
        if present_fields:
            field_presence["has_sector_decision_fields"] += 1
        else:
            field_presence["missing_sector_decision_fields"] += 1

        evidence_gap = (
            "SECTOR_UNION_DECISION_FIELDS_PRESENT"
            if present_fields
            else "SECTOR_UNION_ORIGIN_WITHOUT_DECISION_FIELDS"
        )
        rows.append(
            {
                "trade_id": row.get("trade_id", ""),
                "code": code,
                "signal_date": row.get("signal_date", ""),
                "entry_ts": row.get("entry_ts", ""),
                "exit_reason": row.get("exit_reason", ""),
                "net_ret": row.get("net_ret", ""),
                "candidate_file": row.get("candidate_file", ""),
                "candidate_join_mode": row.get("candidate_join_mode", ""),
                "candidate_row_hit": str(bool(cand)).lower(),
                "candidate_origin": row.get("candidate_origin", ""),
                "execution_pool": row.get("execution_pool", ""),
                "natural_pass": row.get("natural_pass", ""),
                "relax_level": row.get("relax_level", ""),
                "horizon": row.get("horizon", ""),
                "junk_risk_grade": row.get("junk_risk_grade", ""),
                "junk_flags": row.get("junk_flags", ""),
                "final_score": row.get("final_score", ""),
                "sector_action": cand.get("sector_action", "") if cand else "",
                "sector_entry_allowed": cand.get("sector_entry_allowed", "") if cand else "",
                "sector_strength": cand.get("sector_strength", "") if cand else "",
                "sector_reason": cand.get("sector_reason", "") if cand else "",
                "sector_policy_reason": cand.get("sector_policy_reason", "") if cand else "",
                "sector_evidence_gap": evidence_gap,
                "policy_change_applied": "false",
            }
        )

    total_ret = sum(_float(row.get("net_ret")) for row in rows)
    top_losses = sorted(rows, key=lambda row: _float(row.get("net_ret")))[:10]
    out = {
        "generated_at": _now_ts(),
        "status": "FAIL",
        "schema_version": "mdd_sector_union_quality_review_v1",
        "source_files": {
            "paper_mdd_candidate_quality_entry_timing": str(SOURCE_CSV),
        },
        "scope": "read_only_mdd_rows_with_quality_bucket_SECTOR_UNION_NOT_EXECUTION_POOL",
        "rows": len(rows),
        "candidate_row_hits": candidate_row_hits,
        "sum_net_ret": total_ret,
        "counts": {
            "by_exit_reason": _sum_by(rows, "exit_reason"),
            "by_signal_date": _sum_by(rows, "signal_date"),
            "by_code": _sum_by(rows, "code"),
            "by_junk_risk_grade": _sum_by(rows, "junk_risk_grade"),
            "by_junk_flags": _sum_by(rows, "junk_flags"),
            "by_relax_level": _sum_by(rows, "relax_level"),
            "by_sector_evidence_gap": _sum_by(rows, "sector_evidence_gap"),
        },
        "sector_decision_field_presence": dict(sorted(field_presence.items())),
        "top_losses": [
            {
                "trade_id": row.get("trade_id"),
                "code": row.get("code"),
                "signal_date": row.get("signal_date"),
                "exit_reason": row.get("exit_reason"),
                "net_ret": _float(row.get("net_ret")),
                "junk_risk_grade": row.get("junk_risk_grade"),
                "junk_flags": row.get("junk_flags"),
                "relax_level": row.get("relax_level"),
                "sector_evidence_gap": row.get("sector_evidence_gap"),
            }
            for row in top_losses
        ],
        "interpretation": (
            "MDD sector-union rows are mostly recoverable to candidate backup rows, "
            "but historical backups do not retain the sector decision fields needed "
            "to prove why the non-execution-pool rows were admitted."
        ),
        "decision": "TARGETED_SECTOR_UNION_LINEAGE_AND_SHADOW_RULE_REVIEW_REQUIRED",
        "policy_effect": False,
        "policy_change_applied": False,
    }

    fields = [
        "trade_id",
        "code",
        "signal_date",
        "entry_ts",
        "exit_reason",
        "net_ret",
        "candidate_file",
        "candidate_join_mode",
        "candidate_row_hit",
        "candidate_origin",
        "execution_pool",
        "natural_pass",
        "relax_level",
        "horizon",
        "junk_risk_grade",
        "junk_flags",
        "final_score",
        "sector_action",
        "sector_entry_allowed",
        "sector_strength",
        "sector_reason",
        "sector_policy_reason",
        "sector_evidence_gap",
        "policy_change_applied",
    ]
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows, fields)
    print(f"[FINAL] mdd sector union quality review -> {OUT_JSON} rows={len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
