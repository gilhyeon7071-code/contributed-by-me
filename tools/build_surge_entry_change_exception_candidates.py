import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SURGE_CSV = LOG_DIR / "surge_realtime_latest.csv"
OBSERVE_CSV = LOG_DIR / "surge_wait_lob_hoga_observe_latest.csv"
OUT_JSON = LOG_DIR / "surge_entry_change_exception_candidates_latest.json"
OUT_CSV = LOG_DIR / "surge_entry_change_exception_candidates_latest.csv"

MAX_SPREAD_BPS = 20.0
HIGH_REJECTION_DRAWDOWN_LIMIT = -0.025
MAX_RVOL20 = 5.0
OVERHEAT_SCORE_MIN = 90.0
OVERHEAT_RVOL20_MIN = 3.0

ENTRY_CHANGE = "ENTRY_CHANGE_BLOCK"
HARD_BLOCKERS = {
    "ENTRY_ATR_CAP",
    "HIGH_REJECTION_ENTRY_BLOCK",
    "RVOL_OVERHEAT_BLOCK",
    "SCORE_RVOL_OVERHEAT_BLOCK",
    "SPREAD_BLOCK",
    "NO_LOB_BLOCK",
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
    "JUNK_SCORE",
    "JUNK_GRADE",
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


def _row_key(row: Dict[str, Any]) -> Tuple[str, str, str]:
    return (str(row.get("date") or ""), str(row.get("code") or "").zfill(6), str(row.get("ts") or ""))


def _merged_rows(surge_rows: Iterable[Dict[str, Any]], observe_rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    observe_by_key = {_row_key(row): row for row in observe_rows}
    merged: List[Dict[str, Any]] = []
    for row in surge_rows:
        out = dict(row)
        obs = observe_by_key.get(_row_key(row))
        if obs:
            for key in (
                "lob_available",
                "lob_status",
                "spread_bps",
                "order_imbalance_l1",
                "order_imbalance_l1_y",
                "orderflow_tag",
                "orderflow_risk_score",
                "hoga_fetch_status",
                "hoga_fetch_error",
                "probe_class",
            ):
                if key in obs:
                    out[key] = obs.get(key)
            out["observe_matched"] = True
        else:
            out["observe_matched"] = False
        merged.append(out)
    return merged


def _classify(row: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    failed: List[str] = []
    reasons = _reason_keys(row.get("entry_reason"), row.get("exclude_reasons"), row.get("paper_probe_block_reasons"))
    if ENTRY_CHANGE not in reasons:
        failed.append("NO_ENTRY_CHANGE_BLOCK")
    remaining_hard = sorted(k for k in reasons if k in HARD_BLOCKERS and k != ENTRY_CHANGE)
    if remaining_hard:
        failed.append("OTHER_HARD_BLOCKER:" + ",".join(remaining_hard))
    if not _to_bool(row.get("is_realtime_surge")):
        failed.append("NOT_REALTIME_SURGE")
    if not (_to_bool(row.get("lob_available")) and str(row.get("lob_status") or "").upper() == "OK"):
        failed.append("LOB_NOT_OK")
    spread_bps = _to_float(row.get("spread_bps"), 999999.0)
    if spread_bps > MAX_SPREAD_BPS:
        failed.append("SPREAD_GT_20BPS")
    drawdown = _to_float(row.get("intraday_high_drawdown_pct"), 0.0)
    if drawdown <= HIGH_REJECTION_DRAWDOWN_LIMIT:
        failed.append("HIGH_REJECTION_DRAWDOWN")
    rvol20 = _to_float(row.get("rvol20"), 0.0)
    if rvol20 > MAX_RVOL20:
        failed.append("RVOL_GT_5")
    score = _to_float(row.get("surge_score_final"), _to_float(row.get("surge_score"), 0.0))
    if score >= OVERHEAT_SCORE_MIN and rvol20 >= OVERHEAT_RVOL20_MIN:
        failed.append("SCORE_RVOL_OVERHEAT")
    return (not failed), failed, remaining_hard


def _candidate(row: Dict[str, Any], failed: List[str], remaining_hard: List[str]) -> Dict[str, Any]:
    passed = not failed
    return {
        "ts": row.get("ts", ""),
        "date": row.get("date", ""),
        "code": str(row.get("code") or "").zfill(6),
        "entry_change_exception_candidate": passed,
        "candidate_tier": "ENTRY_CHANGE_EXCEPTION_REVIEW_ONLY" if passed else "NOT_CANDIDATE",
        "failed_conditions": "|".join(failed),
        "remaining_hard_blockers": "|".join(remaining_hard),
        "detected_surge_type": row.get("detected_surge_type", "") or row.get("surge_type", ""),
        "entry_reason": row.get("entry_reason", ""),
        "exclude_reasons": row.get("exclude_reasons", ""),
        "change_pct": _to_float(row.get("change_pct")),
        "surge_score_final": _to_float(row.get("surge_score_final"), _to_float(row.get("surge_score"))),
        "rvol20": _to_float(row.get("rvol20")),
        "intraday_high_drawdown_pct": _to_float(row.get("intraday_high_drawdown_pct")),
        "lob_available": _to_bool(row.get("lob_available")),
        "lob_status": row.get("lob_status", ""),
        "spread_bps": _to_float(row.get("spread_bps"), 999999.0),
        "observe_matched": bool(row.get("observe_matched")),
        "entry_allowed": False,
        "entry_approval_changed": False,
        "policy_change": False,
        "entry_signal": False,
        "live_order_allowed": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "trading_route": False,
        "research_only": True,
        "must_not_dispatch": True,
        "note": "Read-only ENTRY_CHANGE_BLOCK exception candidate; not entry approval.",
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "ts",
        "date",
        "code",
        "entry_change_exception_candidate",
        "candidate_tier",
        "failed_conditions",
        "remaining_hard_blockers",
        "detected_surge_type",
        "entry_reason",
        "exclude_reasons",
        "change_pct",
        "surge_score_final",
        "rvol20",
        "intraday_high_drawdown_pct",
        "lob_available",
        "lob_status",
        "spread_bps",
        "observe_matched",
        "entry_allowed",
        "entry_approval_changed",
        "policy_change",
        "entry_signal",
        "live_order_allowed",
        "paper_order_route",
        "broker_order_route",
        "trading_route",
        "research_only",
        "must_not_dispatch",
        "note",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fields})


def main() -> int:
    now = datetime.now().isoformat(timespec="seconds")
    surge_rows = _read_csv(SURGE_CSV)
    observe_rows = _read_csv(OBSERVE_CSV)
    rows: List[Dict[str, Any]] = []
    failed_counter: Counter[str] = Counter()
    for row in _merged_rows(surge_rows, observe_rows):
        reasons = _reason_keys(row.get("entry_reason"), row.get("exclude_reasons"), row.get("paper_probe_block_reasons"))
        if ENTRY_CHANGE not in reasons:
            continue
        passed, failed, remaining_hard = _classify(row)
        for item in failed:
            failed_counter[item.split(":", 1)[0]] += 1
        rows.append(_candidate(row, failed, remaining_hard))
    candidates = [r for r in rows if r.get("entry_change_exception_candidate") is True]
    payload = {
        "ts": now,
        "status": "OK",
        "scope": "surge_entry_change_exception_candidates",
        "source_surge_csv": str(SURGE_CSV),
        "source_observe_csv": str(OBSERVE_CSV),
        "total_review_rows": len(rows),
        "candidate_rows": len(candidates),
        "failed_condition_counts": [{"reason": k, "count": int(v)} for k, v in failed_counter.most_common()],
        "rule": {
            "max_spread_bps": MAX_SPREAD_BPS,
            "high_rejection_drawdown_must_be_gt": HIGH_REJECTION_DRAWDOWN_LIMIT,
            "max_rvol20": MAX_RVOL20,
            "score_rvol_overheat": f"score >= {OVERHEAT_SCORE_MIN} and rvol20 >= {OVERHEAT_RVOL20_MIN}",
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
            "purpose": "read-only ENTRY_CHANGE_BLOCK conditional exception candidate emission only",
        },
        "rows": rows,
        "candidates": candidates,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"status": "OK", "review_rows": len(rows), "candidate_rows": len(candidates), "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
