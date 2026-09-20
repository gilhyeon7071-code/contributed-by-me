from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SOURCE_JSON = LOG_DIR / "paper_mdd_candidate_quality_entry_timing_latest.json"
SOURCE_CSV = LOG_DIR / "paper_mdd_candidate_quality_entry_timing_latest.csv"
MISSING_CSV = LOG_DIR / "paper_mdd_candidate_missing_split_latest.csv"
OUT_JSON = LOG_DIR / "mdd_candidate_entry_quality_review_latest.json"
OUT_CSV = LOG_DIR / "mdd_candidate_entry_quality_review_latest.csv"


def _read_json(path: Path) -> dict[str, Any]:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except UnicodeDecodeError:
            continue
    return {}


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


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        out = float(str(value).replace(",", ""))
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except Exception:
        return default


def _stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    vals = [_f(row.get("net_ret")) for row in rows]
    if not vals:
        return {"n": 0, "sum_net_ret": 0.0, "avg_net_ret": None, "loss_rate": None, "worst_net_ret": None}
    return {
        "n": len(vals),
        "sum_net_ret": round(sum(vals), 6),
        "avg_net_ret": round(sum(vals) / len(vals), 6),
        "loss_rate": round(sum(1 for v in vals if v < 0) / len(vals), 6),
        "worst_net_ret": round(min(vals), 6),
    }


def _group(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        buckets[str(row.get(key) or "UNKNOWN")].append(row)
    out = []
    for bucket, bucket_rows in sorted(buckets.items(), key=lambda kv: (sum(_f(r.get("net_ret")) for r in kv[1]), kv[0])):
        out.append({"axis": key, "bucket": bucket, **_stats(bucket_rows)})
    return out


def _top_losses(rows: list[dict[str, Any]], n: int = 10) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: _f(row.get("net_ret")))[:n]
    fields = [
        "trade_id", "code", "entry_ts", "exit_ts", "exit_reason", "net_ret",
        "quality_bucket", "missing_root_cause", "candidate_origin", "surge_flag_bucket",
        "entry_timing", "horizon", "entry_order_id", "candidate_file",
        "entry_archive_matched",
    ]
    return [{field: row.get(field, "") for field in fields} for row in ordered]


def _review_rows(rows: list[dict[str, Any]], missing_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_quality = {row["bucket"]: row for row in _group(rows, "quality_bucket")}
    by_missing = {row["bucket"]: row for row in _group(missing_rows, "missing_root_cause")}
    by_surge = {row["bucket"]: row for row in _group(rows, "surge_flag_bucket")}
    return [
        {
            "axis": "candidate_match_coverage",
            "status": "LINEAGE_COVERAGE_REPAIR_FIRST",
            "evidence": json.dumps(by_quality.get("NO_CANDIDATE_MATCH", {}), ensure_ascii=False),
            "next_action": "Do not tune STOP/DDM from unmatched rows; repair or preserve source/candidate lineage first.",
        },
        {
            "axis": "surge_runtime_archive_gap",
            "status": "SURGE_RUNTIME_ARCHIVE_GAP",
            "evidence": json.dumps(by_missing.get("SURGE_RUNTIME_ONLY_NO_CANDIDATE_ARCHIVE", {}), ensure_ascii=False),
            "next_action": "Verify future surge runtime entries are covered by entry_source_archive_history and runtime source snapshots.",
        },
        {
            "axis": "latest_backup_selection_gap",
            "status": "JOIN_SEMANTICS_REVIEW_REQUIRED",
            "evidence": json.dumps(by_missing.get("LATEST_BACKUP_SELECTION_GAP", {}), ensure_ascii=False),
            "next_action": "Review whether MDD diagnostics should use closest/earliest same-date candidate backup instead of latest-only backup.",
        },
        {
            "axis": "intraday_realtime_fallback_gap",
            "status": "GENERAL_RUNTIME_SOURCE_GAP",
            "evidence": json.dumps(by_missing.get("INTRADAY_REALTIME_FALLBACK_OR_LINEAGE_GAP", {}), ensure_ascii=False),
            "next_action": "Use entry source archive for future rows; keep historical rows as unresolved provenance gaps.",
        },
        {
            "axis": "sector_union_non_execution_pool",
            "status": "ENTRY_ELIGIBILITY_REVIEW_REQUIRED",
            "evidence": json.dumps(by_quality.get("SECTOR_UNION_NOT_EXECUTION_POOL", {}), ensure_ascii=False),
            "next_action": "Review why sector prefilter union rows entered despite not being execution_pool, before changing exit thresholds.",
        },
        {
            "axis": "surge_immediate_loss_cluster",
            "status": "SURGE_ENTRY_QUALITY_REVIEW_REQUIRED",
            "evidence": json.dumps(by_surge.get("surge_immediate_1_with_type", {}), ensure_ascii=False),
            "next_action": "Review surge runtime entry criteria and LOB/source evidence; do not attribute this cluster to STOP/DDM thresholds alone.",
        },
    ]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = ["axis", "status", "evidence", "next_action"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def build() -> dict[str, Any]:
    source = _read_json(SOURCE_JSON)
    rows = _read_csv(SOURCE_CSV)
    missing_rows = _read_csv(MISSING_CSV)
    group_axes = {
        "quality_bucket": _group(rows, "quality_bucket"),
        "missing_root_cause": _group(missing_rows, "missing_root_cause"),
        "candidate_origin": _group(rows, "candidate_origin"),
        "surge_flag_bucket": _group(rows, "surge_flag_bucket"),
        "exit_reason": _group(rows, "exit_reason"),
        "horizon": _group(rows, "horizon"),
    }
    review_rows = _review_rows(rows, missing_rows)
    top_losses = _top_losses(rows, 10)
    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "status": "FAIL",
        "policy_change": False,
        "trading_effect": False,
        "source": {
            "candidate_quality_json": str(SOURCE_JSON),
            "candidate_quality_csv": str(SOURCE_CSV),
            "missing_split_csv": str(MISSING_CSV),
        },
        "scope": source.get("scope", {}),
        "candidate_join": source.get("candidate_join", {}),
        "summary": {
            "focus": _stats(rows),
            "missing": _stats([row for row in rows if row.get("quality_bucket") == "NO_CANDIDATE_MATCH"]),
            "matched_or_weak_quality": _stats([row for row in rows if row.get("quality_bucket") != "NO_CANDIDATE_MATCH"]),
            "top_loss_count": len(top_losses),
        },
        "group_axes": group_axes,
        "review_rows": review_rows,
        "top_losses": top_losses,
        "artifacts": {"csv": str(OUT_CSV)},
        "interpretation": [
            "This is a read-only review of candidate-entry quality inside the latest MDD STOP/DDM focus rows.",
            "The dominant issue is candidate/source lineage and runtime entry quality before STOP/DDM exits, not a broad STOP/DDM threshold proof.",
            "No trading policy value is changed by this diagnostic.",
        ],
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, review_rows)
    return payload


def main() -> int:
    payload = build()
    print(json.dumps({
        "status": payload["status"],
        "scope": payload["scope"],
        "summary": payload["summary"],
        "artifacts": payload["artifacts"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
