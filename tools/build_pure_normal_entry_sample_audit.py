from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
PAPER_DIR = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"

CONFIG_PATH = PAPER_DIR / "paper_engine_config.json"
POLICY_GROUP_CSV = LOG_DIR / "entry_policy_group_report_latest.csv"

OUT_JSON = LOG_DIR / "pure_normal_entry_sample_audit_latest.json"
OUT_CSV = LOG_DIR / "pure_normal_entry_sample_audit_latest.csv"
OUT_SUMMARY_CSV = LOG_DIR / "pure_normal_entry_sample_audit_summary_latest.csv"


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


def _classify(row: Dict[str, str], strict_same_day_only: bool, max_stage: int) -> Dict[str, Any]:
    source = str(row.get("source") or "")
    group = str(row.get("entry_policy_group") or "")
    eligibility = str(row.get("sample_eligibility") or "")
    entry_timing = str(row.get("entry_timing") or "")
    fallback_stage = str(row.get("fallback_stage") or "")
    net_ret = str(row.get("net_ret") or "").strip()
    ret = _float(net_ret)

    if group != "normal_entry":
        decision = "NOT_NORMAL_ENTRY"
        reason = "entry_policy_group is not normal_entry"
    elif source != "trades_calc" or ret is None:
        decision = "NOT_REALIZED_RETURN_PROOF"
        reason = "row has no realized trade return"
    elif eligibility != "normal_policy_candidate":
        decision = "NOT_CLEAN_NORMAL_POLICY_CANDIDATE"
        reason = "sample_eligibility is not normal_policy_candidate"
    elif strict_same_day_only and fallback_stage in {"1(next_open_limit)", "2(intraday_limit)"}:
        decision = "HISTORICAL_FALLBACK_SEPARATE"
        reason = "current strict_same_day_only and max_stage=0 do not allow stage 1/2 as current-policy proof"
    elif max_stage <= 0 and fallback_stage in {"1(next_open_limit)", "2(intraday_limit)"}:
        decision = "HISTORICAL_FALLBACK_SEPARATE"
        reason = "current entry_fallback_policy.max_stage is 0"
    elif entry_timing == "same_close" and fallback_stage in {"", "0(close_auction)"}:
        decision = "PURE_CURRENT_NORMAL_PROOF"
        reason = "realized normal same-day row compatible with current stage-0 policy"
    else:
        decision = "NORMAL_BUT_POLICY_COMPATIBILITY_UNCLEAR"
        reason = "normal_entry row but timing/stage compatibility is unclear"

    return {
        "source": source,
        "row_id": row.get("row_id", ""),
        "ymd": row.get("ymd", ""),
        "code": row.get("code", ""),
        "order_id": row.get("order_id", ""),
        "entry_timing": entry_timing,
        "fallback_stage": fallback_stage,
        "entry_policy_group": group,
        "sample_eligibility": eligibility,
        "net_ret": net_ret,
        "sample_audit_decision": decision,
        "reason": reason,
        "trading_effect": "false",
        "policy_effect": "false",
        "policy_change_applied": "false",
    }


def main() -> int:
    cfg = _read_json(CONFIG_PATH)
    ops = cfg.get("market_ops_policy", {}) if isinstance(cfg.get("market_ops_policy"), dict) else {}
    fallback = ops.get("entry_fallback_policy", {}) if isinstance(ops.get("entry_fallback_policy"), dict) else {}
    strict_same_day_only = bool(ops.get("strict_same_day_only", False))
    max_stage = int(fallback.get("max_stage", 0) or 0)

    rows = _read_csv(POLICY_GROUP_CSV)
    audited = [_classify(row, strict_same_day_only, max_stage) for row in rows]
    normal_rows = [row for row in audited if row["entry_policy_group"] == "normal_entry"]
    realized_normal = [row for row in normal_rows if row["source"] == "trades_calc" and _float(row["net_ret"]) is not None]
    pure_current = [row for row in audited if row["sample_audit_decision"] == "PURE_CURRENT_NORMAL_PROOF"]
    fallback_separate = [row for row in audited if row["sample_audit_decision"] == "HISTORICAL_FALLBACK_SEPARATE"]

    decision_counts = Counter(row["sample_audit_decision"] for row in audited)
    pure_by_stage = Counter(row["fallback_stage"] for row in pure_current)
    realized_by_stage = Counter(row["fallback_stage"] for row in realized_normal)

    summary_rows: List[Dict[str, Any]] = []
    returns_by_decision: Dict[str, List[float]] = defaultdict(list)
    for row in audited:
        val = _float(row.get("net_ret"))
        if val is not None:
            returns_by_decision[str(row.get("sample_audit_decision"))].append(val)
    for decision, values in sorted(returns_by_decision.items()):
        summary_rows.append({"sample_audit_decision": decision, **_stats(values)})

    out = {
        "generated_at": _now_ts(),
        "status": "PASS",
        "schema_version": "pure_normal_entry_sample_audit_v1",
        "source_files": {
            "config": str(CONFIG_PATH),
            "entry_policy_group_csv": str(POLICY_GROUP_CSV),
        },
        "config_readback": {
            "market_ops_policy.strict_same_day_only": strict_same_day_only,
            "market_ops_policy.entry_fallback_policy.max_stage": max_stage,
        },
        "policy_group_rows": len(rows),
        "normal_entry_rows": len(normal_rows),
        "realized_normal_entry_rows": len(realized_normal),
        "pure_current_normal_proof_rows": len(pure_current),
        "historical_fallback_separate_rows": len(fallback_separate),
        "decision_counts": dict(sorted(decision_counts.items())),
        "realized_normal_by_fallback_stage": dict(sorted(realized_by_stage.items())),
        "pure_current_by_fallback_stage": dict(sorted(pure_by_stage.items())),
        "pure_current_stats": _stats(_float(row.get("net_ret")) for row in pure_current),
        "realized_normal_stats": _stats(_float(row.get("net_ret")) for row in realized_normal),
        "sample_policy_decision": "USE_ONLY_PURE_CURRENT_NORMAL_PROOF_FOR_CURRENT_POLICY_VALUE_REVIEW",
        "reason": "Historical stage 1/2 normal rows are separated because current config is strict_same_day_only=true and entry_fallback_policy.max_stage=0.",
        "decision": "NO_ALTERNATIVE_PATH_NO_POLICY_RELAXATION",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }

    fields = [
        "source",
        "row_id",
        "ymd",
        "code",
        "order_id",
        "entry_timing",
        "fallback_stage",
        "entry_policy_group",
        "sample_eligibility",
        "net_ret",
        "sample_audit_decision",
        "reason",
        "trading_effect",
        "policy_effect",
        "policy_change_applied",
    ]
    summary_fields = ["sample_audit_decision", "n", "avg", "median", "win_rate", "min", "max"]

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, audited, fields)
    _write_csv(OUT_SUMMARY_CSV, summary_rows, summary_fields)
    print(
        "[FINAL] pure normal entry sample audit -> "
        f"{OUT_JSON} pure_current={len(pure_current)} realized_normal={len(realized_normal)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
