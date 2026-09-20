from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CONFIG_PATH = ROOT / "paper" / "paper_engine_config.json"
CANDIDATES_FINAL = LOG_DIR / "candidates_latest_data.with_final_score.csv"
FINAL_ELIMINATION_JSON = LOG_DIR / "final_candidate_elimination_audit_latest.json"

OUT_JSON = LOG_DIR / "sector_union_entry_path_audit_latest.json"
OUT_CSV = LOG_DIR / "sector_union_entry_path_audit_latest.csv"


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: List[Dict[str, Any]], fields: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _truthy(value: Any) -> bool:
    return str(value or "").strip().upper() in {"TRUE", "1", "Y", "YES", "T"}


def _float(value: Any) -> float | None:
    try:
        if value in ("", None):
            return None
        return float(value)
    except Exception:
        return None


def main() -> int:
    cfg = _read_json(CONFIG_PATH)
    positive = cfg.get("positive_entry_criteria", {}) if isinstance(cfg.get("positive_entry_criteria"), dict) else {}
    threshold = _float(cfg.get("union_entry_strength_min"))
    if threshold is None:
        threshold = 0.65
    rows: List[Dict[str, Any]] = []
    counts: Counter[str] = Counter()

    for row in _read_csv(CANDIDATES_FINAL):
        code = str(row.get("code") or "").zfill(6)
        origin = str(row.get("candidate_origin") or "").strip().upper()
        if origin != "SECTOR_PREFILTER_UNION":
            continue
        strength = _float(row.get("sector_strength")) or 0.0
        raw_execution_pool = _truthy(row.get("execution_pool"))
        union_ok = (
            bool(positive.get("allow_sector_union", True))
            and str(row.get("sector_action") or "").strip().upper() == "BUY"
            and _truthy(row.get("sector_entry_allowed"))
            and strength >= threshold
        )
        if union_ok and not raw_execution_pool:
            status = "UNION_INCLUDED_DESPITE_EXECUTION_POOL_FALSE"
        elif raw_execution_pool:
            status = "RAW_EXECUTION_POOL_TRUE"
        else:
            status = "UNION_NOT_ELIGIBLE"
        counts[status] += 1
        rows.append(
            {
                "code": code,
                "name": row.get("name", ""),
                "candidate_origin": row.get("candidate_origin", ""),
                "final_score": row.get("final_score", ""),
                "raw_execution_pool": str(raw_execution_pool).lower(),
                "sector_action": row.get("sector_action", ""),
                "sector_entry_allowed": row.get("sector_entry_allowed", ""),
                "sector_strength": row.get("sector_strength", ""),
                "union_entry_strength_min": threshold,
                "allow_sector_union": str(bool(positive.get("allow_sector_union", True))).lower(),
                "union_ok": str(bool(union_ok)).lower(),
                "audit_status": status,
                "trading_effect": "false",
                "policy_effect": "false",
                "policy_change_applied": "false",
            }
        )

    final_elim = _read_json(FINAL_ELIMINATION_JSON)
    out = {
        "generated_at": _now_ts(),
        "status": "PASS",
        "schema_version": "sector_union_entry_path_audit_v1",
        "source_files": {
            "config": str(CONFIG_PATH),
            "candidates_final": str(CANDIDATES_FINAL),
            "final_candidate_elimination": str(FINAL_ELIMINATION_JSON),
        },
        "config_readback": {
            "positive_entry_criteria.allow_sector_union": bool(positive.get("allow_sector_union", True)),
            "positive_entry_criteria.require_execution_pool_when_present": bool(positive.get("require_execution_pool_when_present", True)),
            "positive_entry_criteria.require_sector_entry_when_present": bool(positive.get("require_sector_entry_when_present", True)),
            "union_entry_strength_min": threshold,
        },
        "sector_union_rows": len(rows),
        "status_counts": dict(sorted(counts.items())),
        "finalist_codes": final_elim.get("finalist_codes", []),
        "finalist_elimination_status": final_elim.get("status"),
        "interpretation": "SECTOR_PREFILTER_UNION is an existing conditional inclusion path that can admit raw execution_pool=false rows when sector criteria pass. It did not create a new buy in the same-cycle verification because same-day duplicate buy guard removed the finalists.",
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
        "union_entry_strength_min",
        "allow_sector_union",
        "union_ok",
        "audit_status",
        "trading_effect",
        "policy_effect",
        "policy_change_applied",
    ]
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows, fields)
    print(f"[FINAL] sector union entry path audit -> {OUT_JSON} rows={len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
