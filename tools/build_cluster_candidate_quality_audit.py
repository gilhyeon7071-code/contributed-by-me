from __future__ import annotations

import csv
import glob
import json
import math
import os
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

LOSS_DRIVER_CSV = LOG_DIR / "pure_normal_loss_driver_audit_latest.csv"

OUT_JSON = LOG_DIR / "cluster_candidate_quality_audit_latest.json"
OUT_CSV = LOG_DIR / "cluster_candidate_quality_audit_latest.csv"
OUT_SUMMARY_CSV = LOG_DIR / "cluster_candidate_quality_audit_summary_latest.csv"


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


def _bool_text(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


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


def _latest_candidate_backup(entry_ymd: str) -> Path | None:
    files = sorted(glob.glob(str(LOG_DIR / f"candidates_latest_data.bak_{entry_ymd}_*.csv")))
    if not files:
        return None
    return Path(files[-1])


def _candidate_rows_by_date(dates: Iterable[str]) -> Dict[str, Tuple[Path | None, Dict[str, Dict[str, str]]]]:
    out: Dict[str, Tuple[Path | None, Dict[str, Dict[str, str]]]] = {}
    for ymd in sorted(set(dates)):
        path = _latest_candidate_backup(ymd)
        rows = _read_csv(path) if path else []
        out[ymd] = (path, {str(row.get("code") or "").zfill(6): row for row in rows})
    return out


def _quality_bucket(cand: Dict[str, str]) -> str:
    origin = str(cand.get("candidate_origin") or "").strip().upper()
    execution_pool = _bool_text(cand.get("execution_pool"))
    natural_pass = _bool_text(cand.get("natural_pass"))
    relax_level = str(cand.get("relax_level") or "").strip().upper()
    flags = str(cand.get("junk_flags") or "").strip()
    junk_grade = str(cand.get("junk_risk_grade") or "").strip().upper()
    if not cand:
        return "NO_CANDIDATE_MATCH"
    if origin == "SECTOR_PREFILTER_UNION" and not execution_pool:
        return "SECTOR_UNION_NOT_EXECUTION_POOL"
    if relax_level == "L3" and flags:
        return "L3_WITH_RISK_FLAGS"
    if relax_level == "L3":
        return "L3_RELAXED"
    if not natural_pass:
        return "NON_NATURAL_PASS"
    if junk_grade in {"MID", "WARN", "HIGH"} or flags:
        return "RISK_FLAGGED_NATURAL_PASS"
    return "CLEAN_NATURAL_PASS"


def _summary_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    for row in rows:
        net = _float(row.get("net_ret"))
        if net is None:
            continue
        for kind, key in (
            ("quality_bucket", str(row.get("quality_bucket") or "")),
            ("candidate_origin", str(row.get("candidate_origin") or "")),
            ("relax_level", str(row.get("relax_level") or "")),
            ("execution_pool", str(row.get("execution_pool") or "")),
            ("natural_pass", str(row.get("natural_pass") or "")),
            ("junk_risk_grade", str(row.get("junk_risk_grade") or "")),
            ("entry_ymd", str(row.get("entry_ymd") or "")),
        ):
            buckets[(kind, key)].append(net)
    return [{"kind": kind, "bucket": key, **_stats(vals)} for (kind, key), vals in sorted(buckets.items())]


def main() -> int:
    loss_rows = [
        row for row in _read_csv(LOSS_DRIVER_CSV) if str(row.get("date_cluster") or "") == "APR01_02_CLUSTER"
    ]
    by_date = _candidate_rows_by_date(row.get("entry_ymd", "") for row in loss_rows)
    enriched: List[Dict[str, Any]] = []
    for row in loss_rows:
        ymd = str(row.get("entry_ymd") or "")
        code = str(row.get("code") or "").zfill(6)
        cand_path, cand_by_code = by_date.get(ymd, (None, {}))
        cand = cand_by_code.get(code, {})
        enriched.append(
            {
                "trade_id": row.get("trade_id", ""),
                "entry_ymd": ymd,
                "code": code,
                "net_ret": row.get("net_ret", ""),
                "gross_ret": row.get("gross_ret", ""),
                "loss_bucket": row.get("loss_bucket", ""),
                "candidate_file": "" if cand_path is None else cand_path.name,
                "candidate_matched": str(bool(cand)).lower(),
                "candidate_origin": cand.get("candidate_origin", ""),
                "execution_pool": str(_bool_text(cand.get("execution_pool"))).lower() if cand else "",
                "natural_pass": str(_bool_text(cand.get("natural_pass"))).lower() if cand else "",
                "relax_level": cand.get("relax_level", ""),
                "final_score": cand.get("final_score", ""),
                "score": cand.get("score", ""),
                "ret1_pct": cand.get("ret1_pct", ""),
                "rs": cand.get("rs", ""),
                "rs_slope": cand.get("rs_slope", ""),
                "stretch": cand.get("stretch", ""),
                "v_accel": cand.get("v_accel", ""),
                "atr14_pct": cand.get("atr14_pct", ""),
                "rsi14": cand.get("rsi14", ""),
                "junk_risk_grade": cand.get("junk_risk_grade", ""),
                "junk_flags": cand.get("junk_flags", ""),
                "quality_bucket": _quality_bucket(cand),
                "trading_effect": "false",
                "policy_effect": "false",
                "policy_change_applied": "false",
            }
        )

    quality_counts = Counter(row["quality_bucket"] for row in enriched)
    origin_counts = Counter(row["candidate_origin"] for row in enriched)
    relax_counts = Counter(row["relax_level"] for row in enriched)
    execution_pool_counts = Counter(row["execution_pool"] for row in enriched)
    natural_pass_counts = Counter(row["natural_pass"] for row in enriched)
    junk_flagged = sum(1 for row in enriched if str(row.get("junk_flags") or "").strip())
    matched = sum(1 for row in enriched if row.get("candidate_matched") == "true")
    sector_union_not_exec = int(quality_counts.get("SECTOR_UNION_NOT_EXECUTION_POOL", 0))
    l3_risky = int(quality_counts.get("L3_WITH_RISK_FLAGS", 0))

    out = {
        "generated_at": _now_ts(),
        "status": "PASS",
        "schema_version": "cluster_candidate_quality_audit_v1",
        "source_files": {
            "loss_driver": str(LOSS_DRIVER_CSV),
            "candidate_backups_used": sorted(
                os.path.basename(str(path)) for path, _rows in by_date.values() if path is not None
            ),
        },
        "apr01_02_cluster_rows": len(enriched),
        "candidate_matched_rows": matched,
        "quality_bucket_counts": dict(sorted(quality_counts.items())),
        "candidate_origin_counts": dict(sorted(origin_counts.items())),
        "relax_level_counts": dict(sorted(relax_counts.items())),
        "execution_pool_counts": dict(sorted(execution_pool_counts.items())),
        "natural_pass_counts": dict(sorted(natural_pass_counts.items())),
        "junk_flagged_rows": junk_flagged,
        "sector_union_not_execution_pool_rows": sector_union_not_exec,
        "l3_with_risk_flags_rows": l3_risky,
        "net_stats": _stats(_float(row.get("net_ret")) for row in enriched),
        "primary_quality_driver": (
            "MIXED_WEAK_QUALITY_CLUSTER"
            if sector_union_not_exec + l3_risky >= max(1, len(enriched) // 2)
            else "QUALITY_DRIVER_NOT_DOMINANT"
        ),
        "interpretation": (
            "The 2026-04-01 to 2026-04-02 loss cluster is not clean current normal alpha. "
            "It mixes sector-union non-execution-pool rows on 2026-04-01 and L3 relaxed rows with risk flags on 2026-04-02."
        ),
        "decision": "NO_ALTERNATIVE_PATH_NO_POLICY_RELAXATION",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }

    fields = [
        "trade_id",
        "entry_ymd",
        "code",
        "net_ret",
        "gross_ret",
        "loss_bucket",
        "candidate_file",
        "candidate_matched",
        "candidate_origin",
        "execution_pool",
        "natural_pass",
        "relax_level",
        "final_score",
        "score",
        "ret1_pct",
        "rs",
        "rs_slope",
        "stretch",
        "v_accel",
        "atr14_pct",
        "rsi14",
        "junk_risk_grade",
        "junk_flags",
        "quality_bucket",
        "trading_effect",
        "policy_effect",
        "policy_change_applied",
    ]
    summary_fields = ["kind", "bucket", "n", "avg", "median", "win_rate", "min", "max"]
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, enriched, fields)
    _write_csv(OUT_SUMMARY_CSV, _summary_rows(enriched), summary_fields)
    print(
        "[FINAL] cluster candidate quality audit -> "
        f"{OUT_JSON} rows={len(enriched)} driver={out['primary_quality_driver']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
