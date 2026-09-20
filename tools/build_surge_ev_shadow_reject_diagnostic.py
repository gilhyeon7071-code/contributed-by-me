import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
RECHECK_CSV = LOG_DIR / "surge_no_lob_recheck_queue_latest.csv"
SCORE_RVOL_CSV = LOG_DIR / "surge_score_rvol_conditional_recheck_queue_latest.csv"
SURGE_CSV = LOG_DIR / "surge_realtime_latest.csv"
OUT_JSON = LOG_DIR / "surge_ev_shadow_reject_diagnostic_latest.json"
OUT_CSV = LOG_DIR / "surge_ev_shadow_reject_diagnostic_latest.csv"

RESOLVABLE_BLOCKERS = {"NO_LOB_BLOCK"}
HARD_BLOCKERS = {
    "ENTRY_CHANGE_BLOCK",
    "ENTRY_ATR_CAP",
    "HIGH_REJECTION_ENTRY_BLOCK",
    "RVOL_OVERHEAT_BLOCK",
    "SCORE_RVOL_OVERHEAT_BLOCK",
    "SPREAD_BLOCK",
    "ORDERFLOW_RISK_BLOCK",
    "ORDERFLOW_PAUSE",
    "ORDER_IMBALANCE_EXTREME",
    "KYLE_LAMBDA_Z_BLOCK",
    "MARKOUT_NEGATIVE_BLOCK",
    "OFI_NORM_EXTREME",
    "KRX_ADMIN",
    "KRX_WARNING",
    "KRX_RISK",
    "KRX_CAUTION",
    "TRADING_VALUE_BLOCK",
    "TRADING_VALUE_FLOOR",
    "LOW_TRADING_VALUE",
    "NEWS_NEGATIVE",
    "NEWS_IMPLICATION_BLOCK",
    "FEATURE_BLOCK",
    "FEATURE_MISSING_BLOCK",
    "INSUFFICIENT_FEATURES",
}


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except Exception:
        return default


def _to_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists() or path.stat().st_size <= 5:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        return list(csv.DictReader(fp))


def _reason_keys(*values: Any) -> Set[str]:
    keys: Set[str] = set()
    for value in values:
        for part in str(value or "").split("|"):
            token = part.strip()
            if token:
                keys.add(token.split(":", 1)[0].strip().upper())
    return keys


def _remaining_hard(row: Dict[str, Any]) -> List[str]:
    keys = _reason_keys(row.get("exclude_reasons"), row.get("entry_reason"), row.get("paper_probe_block_reasons"))
    return sorted(k for k in keys if k in HARD_BLOCKERS and k not in RESOLVABLE_BLOCKERS)


def _primary_class(row: Dict[str, Any], remaining: List[str]) -> str:
    if not _to_bool(row.get("lob_available")) or str(row.get("lob_status") or "").upper() != "OK":
        return "LOB_NOT_RESOLVED"
    if _to_float(row.get("spread_bps"), 999999.0) > 40.0:
        return "SPREAD_OVER_40BPS"
    if not remaining:
        return "EV_CLEAN_CANDIDATE"
    if remaining == ["ENTRY_CHANGE_BLOCK"]:
        return "ENTRY_CHANGE_ONLY_NEAR_MISS"
    if remaining == ["KRX_WARNING"]:
        return "KRX_WARNING_ONLY_NEAR_MISS"
    if remaining == ["MARKOUT_NEGATIVE_BLOCK"]:
        return "MARKOUT_NEGATIVE_ONLY"
    if set(remaining).issubset({"RVOL_OVERHEAT_BLOCK", "SCORE_RVOL_OVERHEAT_BLOCK"}):
        return "RVOL_OVERHEAT_ONLY"
    if "HIGH_REJECTION_ENTRY_BLOCK" in remaining and len(remaining) == 1:
        return "HIGH_REJECTION_ONLY"
    return "MULTI_HARD_BLOCKER"


def _source_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for source_name, source_path in [
        ("NO_LOB_RECHECK_QUEUE", RECHECK_CSV),
        ("SCORE_RVOL_QUEUE", SCORE_RVOL_CSV),
    ]:
        for row in _read_csv(source_path):
            item = dict(row)
            item["_diagnostic_source"] = source_name
            rows.append(item)
    return rows


def _surge_summary() -> Dict[str, Any]:
    rows = _read_csv(SURGE_CSV)
    return {
        "surge_rows": len(rows),
        "entry_decision_counts": Counter(str(r.get("entry_decision") or "") for r in rows),
        "entry_reason_counts": Counter(str(r.get("entry_reason") or "") for r in rows),
        "lob_ok_rows": sum(1 for r in rows if _to_bool(r.get("lob_available")) and str(r.get("lob_status") or "").upper() == "OK"),
    }


def main() -> int:
    ts = datetime.now().isoformat(timespec="seconds")
    source_rows = _source_rows()
    rows: List[Dict[str, Any]] = []
    class_counts: Counter[str] = Counter()
    blocker_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()

    for row in source_rows:
        remaining = _remaining_hard(row)
        diagnostic_class = _primary_class(row, remaining)
        class_counts[diagnostic_class] += 1
        source_counts[str(row.get("_diagnostic_source") or "")] += 1
        for blocker in remaining:
            blocker_counts[blocker] += 1
        rows.append(
            {
                "code": str(row.get("code") or "").zfill(6),
                "ts": row.get("ts", ""),
                "diagnostic_source": row.get("_diagnostic_source", ""),
                "diagnostic_class": diagnostic_class,
                "remaining_hard_blockers": "|".join(remaining),
                "entry_reason": row.get("entry_reason", ""),
                "exclude_reasons": row.get("exclude_reasons", ""),
                "detected_surge_type": row.get("detected_surge_type", ""),
                "surge_score_final": _to_float(row.get("surge_score_final")),
                "change_pct": _to_float(row.get("change_pct")),
                "rvol20": _to_float(row.get("rvol20")),
                "trading_value": _to_float(row.get("trading_value")),
                "lob_available": _to_bool(row.get("lob_available")),
                "lob_status": row.get("lob_status", ""),
                "spread_bps": _to_float(row.get("spread_bps")),
                "entry_approval_changed": False,
                "policy_change": False,
                "entry_signal": False,
                "live_order_allowed": False,
                "paper_order_route": False,
                "broker_order_route": False,
                "trading_route": False,
                "research_only": True,
                "must_not_dispatch": True,
            }
        )

    rows.sort(key=lambda r: (-_to_float(r.get("surge_score_final")), str(r.get("code"))))
    surge = _surge_summary()
    payload = {
        "ts": ts,
        "status": "OK",
        "scope": "read_only_surge_ev_shadow_reject_diagnostic",
        "source_files": {
            "surge_realtime_csv": str(SURGE_CSV),
            "no_lob_recheck_csv": str(RECHECK_CSV),
            "score_rvol_recheck_csv": str(SCORE_RVOL_CSV),
        },
        "summary": {
            "review_rows": len(rows),
            "diagnostic_class_counts": [{"class": k, "count": int(v)} for k, v in class_counts.most_common()],
            "remaining_hard_blocker_counts": [{"blocker": k, "count": int(v)} for k, v in blocker_counts.most_common()],
            "source_counts": [{"source": k, "count": int(v)} for k, v in source_counts.most_common()],
            "surge_rows": int(surge["surge_rows"]),
            "surge_lob_ok_rows": int(surge["lob_ok_rows"]),
            "surge_entry_decision_counts": [
                {"decision": k, "count": int(v)}
                for k, v in surge["entry_decision_counts"].most_common()
                if k
            ],
            "surge_entry_reason_counts": [
                {"reason": k, "count": int(v)}
                for k, v in surge["entry_reason_counts"].most_common()
                if k
            ],
            "entry_approval_changed": False,
            "policy_change": False,
            "trading_effect": False,
        },
        "risk_contract": {
            "policy_change": False,
            "entry_approval_changed": False,
            "entry_signal": False,
            "live_order_allowed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_route": False,
            "research_only": True,
            "must_not_dispatch": True,
            "purpose": "reject classification for expected-value research only; no order route",
        },
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    fields = [
        "code",
        "ts",
        "diagnostic_source",
        "diagnostic_class",
        "remaining_hard_blockers",
        "entry_reason",
        "exclude_reasons",
        "detected_surge_type",
        "surge_score_final",
        "change_pct",
        "rvol20",
        "trading_value",
        "lob_available",
        "lob_status",
        "spread_bps",
        "entry_approval_changed",
        "policy_change",
        "entry_signal",
        "live_order_allowed",
        "paper_order_route",
        "broker_order_route",
        "trading_route",
        "research_only",
        "must_not_dispatch",
    ]
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fields})
    print(json.dumps({"status": "OK", "review_rows": len(rows), "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
