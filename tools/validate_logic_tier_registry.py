from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(r"E:\1_Data")
LOG_DIR = ROOT / "2_Logs"
REGISTRY = ROOT / "config" / "logic_tier_registry.json"
OUT_JSON = LOG_DIR / "logic_tier_registry_validation_latest.json"
OUT_CSV = LOG_DIR / "logic_tier_registry_validation_latest.csv"

ALLOWED_TIERS = {"stable", "candidate", "research", "shadow", "hotfix", "deprecated"}
REQUIRED_ENTRY_FIELDS = [
    "logic_id",
    "logic_tier",
    "status",
    "owner_path",
    "source_artifacts",
    "output_artifacts",
    "trading_effect",
    "policy_effect",
    "order_path_effect",
    "score_effect",
    "stable_comparison_required",
    "promotion_status",
    "promotion_blockers",
    "validation_evidence",
    "notes",
]
BOOL_FIELDS = [
    "trading_effect",
    "policy_effect",
    "order_path_effect",
    "score_effect",
    "stable_comparison_required",
]


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("registry root must be a JSON object")
    return data


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _is_glob_path(text: str) -> bool:
    return "*" in text or "?" in text


def _path_exists(text: str) -> bool | None:
    if not text or _is_glob_path(text):
        return None
    try:
        return Path(text).exists()
    except OSError:
        return None


def _add(
    rows: list[dict[str, Any]],
    severity: str,
    logic_id: str,
    check: str,
    detail: str,
) -> None:
    rows.append(
        {
            "severity": severity,
            "logic_id": logic_id,
            "check": check,
            "detail": detail,
        }
    )


def _validate_entry(entry: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    logic_id = str(entry.get("logic_id") or "<missing>")
    tier = str(entry.get("logic_tier") or "")

    for field in REQUIRED_ENTRY_FIELDS:
        if field not in entry:
            _add(rows, "ERROR", logic_id, "required_field", f"missing field: {field}")

    if tier not in ALLOWED_TIERS:
        _add(rows, "ERROR", logic_id, "logic_tier", f"invalid tier: {tier}")

    for field in BOOL_FIELDS:
        if field in entry and not isinstance(entry.get(field), bool):
            _add(rows, "ERROR", logic_id, "bool_field", f"{field} must be boolean")

    if tier in {"research", "shadow"} and entry.get("trading_effect") is True:
        _add(rows, "ERROR", logic_id, "tier_effect", f"{tier} cannot have trading_effect=true")

    if tier == "research" and entry.get("policy_effect") is True:
        _add(rows, "ERROR", logic_id, "tier_effect", "research cannot have policy_effect=true")

    if tier == "candidate" and entry.get("trading_effect") is True:
        _add(rows, "ERROR", logic_id, "tier_effect", "candidate cannot have trading_effect=true before promotion")

    if tier == "stable" and not _as_list(entry.get("validation_evidence")):
        _add(rows, "ERROR", logic_id, "validation_evidence", "stable entry requires validation evidence")

    if tier == "candidate" and entry.get("stable_comparison_required") is not True:
        _add(rows, "ERROR", logic_id, "candidate_comparison", "candidate must require stable comparison")

    if entry.get("order_path_effect") is True and entry.get("trading_effect") is False:
        _add(
            rows,
            "WARN",
            logic_id,
            "order_path_without_trading",
            "order path effect with trading_effect=false must stay mock, dry-run, or read-only",
        )

    for field in ("source_artifacts", "output_artifacts", "validation_evidence", "promotion_blockers"):
        if field in entry and not isinstance(entry.get(field), list):
            _add(rows, "ERROR", logic_id, "list_field", f"{field} must be a list")

    for evidence in _as_list(entry.get("validation_evidence")):
        text = str(evidence)
        exists = _path_exists(text)
        if exists is False:
            _add(rows, "WARN", logic_id, "validation_evidence_path", f"evidence path missing: {text}")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = ["severity", "logic_id", "check", "detail"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    rows: list[dict[str, Any]] = []
    data = _read_json(REGISTRY)
    entries = data.get("entries")
    if not isinstance(entries, list):
        _add(rows, "ERROR", "<registry>", "entries", "entries must be a list")
        entries = []

    effect = data.get("registry_effect") or {}
    if not isinstance(effect, dict):
        _add(rows, "ERROR", "<registry>", "registry_effect", "registry_effect must be an object")
        effect = {}
    for field in ("trading_effect", "policy_effect", "order_path_effect", "score_effect"):
        if effect.get(field) is not False:
            _add(rows, "ERROR", "<registry>", "registry_effect", f"{field} must be false")

    ids: list[str] = []
    for entry in entries:
        if not isinstance(entry, dict):
            _add(rows, "ERROR", "<registry>", "entry_type", "entry must be an object")
            continue
        logic_id = str(entry.get("logic_id") or "")
        ids.append(logic_id)
        _validate_entry(entry, rows)

    for logic_id in sorted({x for x in ids if x and ids.count(x) > 1}):
        _add(rows, "ERROR", logic_id, "duplicate_logic_id", "logic_id must be unique")

    error_count = sum(1 for row in rows if row["severity"] == "ERROR")
    warn_count = sum(1 for row in rows if row["severity"] == "WARN")
    status = "FAIL" if error_count else ("PASS_WITH_WARNINGS" if warn_count else "PASS")

    result = {
        "generated_at": _now_ts(),
        "status": status,
        "schema_version": "logic_tier_registry_validation_v1",
        "registry": str(REGISTRY),
        "entry_count": len(entries),
        "error_count": error_count,
        "warn_count": warn_count,
        "checks": rows,
        "trading_effect": False,
        "policy_effect": False,
        "order_path_effect": False,
        "score_effect": False,
        "policy_change_applied": False,
    }

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    print(
        "[LOGIC_TIER_REGISTRY] "
        f"status={status} entries={len(entries)} errors={error_count} warnings={warn_count} "
        f"json={OUT_JSON}"
    )
    return 0 if error_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

