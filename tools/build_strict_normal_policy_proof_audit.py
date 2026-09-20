from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

PURE_NORMAL_CSV = LOG_DIR / "pure_normal_entry_sample_audit_latest.csv"
LOSS_DRIVER_CSV = LOG_DIR / "pure_normal_loss_driver_audit_latest.csv"
QUALITY_AUDIT_CSV = LOG_DIR / "cluster_candidate_quality_audit_latest.csv"

OUT_JSON = LOG_DIR / "strict_normal_policy_proof_audit_latest.json"
OUT_CSV = LOG_DIR / "strict_normal_policy_proof_audit_latest.csv"
OUT_SUMMARY_CSV = LOG_DIR / "strict_normal_policy_proof_audit_summary_latest.csv"

EXCLUDED_QUALITY_BUCKETS = {"L3_WITH_RISK_FLAGS", "SECTOR_UNION_NOT_EXECUTION_POOL"}


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


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


def _stats(values: Iterable[float]) -> Dict[str, Any]:
    xs = [float(v) for v in values if v is not None and not math.isnan(float(v))]
    if not xs:
        return {"n": 0, "avg": None, "median": None, "win_rate": None, "min": None, "max": None}
    return {
        "n": len(xs),
        "avg": sum(xs) / len(xs),
        "median": median(xs),
        "win_rate": sum(1 for x in xs if x > 0) / len(xs),
        "min": min(xs),
        "max": max(xs),
    }


def _key(row: Dict[str, str]) -> Tuple[str, str]:
    return str(row.get("row_id") or row.get("trade_id") or ""), str(row.get("code") or "").zfill(6)


def _summary_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    for row in rows:
        net = _float(row.get("net_ret"))
        if net is None:
            continue
        for kind, key in (
            ("strict_policy_proof_decision", str(row.get("strict_policy_proof_decision") or "")),
            ("exclusion_reason", str(row.get("exclusion_reason") or "")),
            ("quality_bucket", str(row.get("quality_bucket") or "")),
            ("entry_ymd", str(row.get("entry_ymd") or row.get("ymd") or "")),
            ("candidate_origin", str(row.get("candidate_origin") or "")),
            ("relax_level", str(row.get("relax_level") or "")),
        ):
            buckets[(kind, key)].append(net)
    return [{"kind": kind, "bucket": key, **_stats(vals)} for (kind, key), vals in sorted(buckets.items())]


def main() -> int:
    pure_rows = [
        row for row in _read_csv(PURE_NORMAL_CSV)
        if str(row.get("sample_audit_decision") or "") == "PURE_CURRENT_NORMAL_PROOF"
    ]
    loss_by_key = {_key(row): row for row in _read_csv(LOSS_DRIVER_CSV)}
    quality_by_key = {_key(row): row for row in _read_csv(QUALITY_AUDIT_CSV)}

    audited: List[Dict[str, Any]] = []
    for row in pure_rows:
        key = _key(row)
        loss = loss_by_key.get(key, {})
        quality = quality_by_key.get(key, {})
        quality_bucket = str(quality.get("quality_bucket") or "")
        if quality_bucket in EXCLUDED_QUALITY_BUCKETS:
            decision = "EXCLUDE_FROM_STRICT_NORMAL_POLICY_PROOF"
            reason = quality_bucket
        elif quality_bucket == "L3_RELAXED":
            decision = "KEEP_BUT_MARK_RELAXED"
            reason = "L3_RELAXED_WITHOUT_RISK_FLAGS"
        elif quality_bucket == "RISK_FLAGGED_NATURAL_PASS":
            decision = "KEEP_BUT_MARK_RISK_FLAGGED"
            reason = "NATURAL_PASS_WITH_RISK_FLAGS"
        elif quality_bucket:
            decision = "KEEP_STRICT_NORMAL_POLICY_PROOF"
            reason = "QUALITY_BUCKET_NOT_EXCLUDED"
        else:
            decision = "KEEP_STRICT_NORMAL_POLICY_PROOF"
            reason = "NO_CLUSTER_WEAK_QUALITY_FLAG"

        audited.append(
            {
                "trade_id": row.get("row_id", ""),
                "ymd": row.get("ymd", ""),
                "entry_ymd": loss.get("entry_ymd", row.get("ymd", "")),
                "code": row.get("code", ""),
                "order_id": row.get("order_id", ""),
                "entry_timing": row.get("entry_timing", ""),
                "fallback_stage": row.get("fallback_stage", ""),
                "net_ret": row.get("net_ret", ""),
                "loss_bucket": loss.get("loss_bucket", ""),
                "candidate_origin": quality.get("candidate_origin", ""),
                "execution_pool": quality.get("execution_pool", ""),
                "natural_pass": quality.get("natural_pass", ""),
                "relax_level": quality.get("relax_level", ""),
                "junk_risk_grade": quality.get("junk_risk_grade", ""),
                "junk_flags": quality.get("junk_flags", ""),
                "quality_bucket": quality_bucket,
                "strict_policy_proof_decision": decision,
                "exclusion_reason": reason,
                "trading_effect": "false",
                "policy_effect": "false",
                "policy_change_applied": "false",
            }
        )

    keep_rows = [row for row in audited if not str(row["strict_policy_proof_decision"]).startswith("EXCLUDE")]
    excluded_rows = [row for row in audited if str(row["strict_policy_proof_decision"]).startswith("EXCLUDE")]
    decision_counts = Counter(row["strict_policy_proof_decision"] for row in audited)
    exclusion_counts = Counter(row["exclusion_reason"] for row in excluded_rows)

    out = {
        "generated_at": _now_ts(),
        "status": "PASS",
        "schema_version": "strict_normal_policy_proof_audit_v1",
        "source_files": {
            "pure_normal": str(PURE_NORMAL_CSV),
            "loss_driver": str(LOSS_DRIVER_CSV),
            "cluster_candidate_quality": str(QUALITY_AUDIT_CSV),
        },
        "pure_current_normal_rows": len(audited),
        "excluded_weak_quality_rows": len(excluded_rows),
        "strict_policy_proof_rows": len(keep_rows),
        "decision_counts": dict(sorted(decision_counts.items())),
        "exclusion_counts": dict(sorted(exclusion_counts.items())),
        "excluded_quality_buckets": sorted(EXCLUDED_QUALITY_BUCKETS),
        "strict_policy_proof_stats": _stats(_float(row.get("net_ret")) for row in keep_rows),
        "pure_current_normal_stats": _stats(_float(row.get("net_ret")) for row in audited),
        "sample_policy_decision": "STRICT_NORMAL_POLICY_PROOF_TOO_SMALL_FOR_POLICY_VALUE_CHANGE",
        "interpretation": (
            "After excluding sector-union non-execution-pool rows and L3 rows with risk flags, "
            "the remaining strict normal-policy proof sample is too small for a live policy value change."
        ),
        "decision": "NO_ALTERNATIVE_PATH_NO_POLICY_RELAXATION",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }

    fields = [
        "trade_id",
        "ymd",
        "entry_ymd",
        "code",
        "order_id",
        "entry_timing",
        "fallback_stage",
        "net_ret",
        "loss_bucket",
        "candidate_origin",
        "execution_pool",
        "natural_pass",
        "relax_level",
        "junk_risk_grade",
        "junk_flags",
        "quality_bucket",
        "strict_policy_proof_decision",
        "exclusion_reason",
        "trading_effect",
        "policy_effect",
        "policy_change_applied",
    ]
    summary_fields = ["kind", "bucket", "n", "avg", "median", "win_rate", "min", "max"]
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, audited, fields)
    _write_csv(OUT_SUMMARY_CSV, _summary_rows(audited), summary_fields)
    print(
        "[FINAL] strict normal policy proof audit -> "
        f"{OUT_JSON} strict_rows={len(keep_rows)} excluded={len(excluded_rows)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
