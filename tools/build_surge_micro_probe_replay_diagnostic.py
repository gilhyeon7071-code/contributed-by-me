import argparse
import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
OUT_JSON = LOG_DIR / "surge_micro_probe_replay_diagnostic_latest.json"
OUT_CSV = LOG_DIR / "surge_micro_probe_replay_diagnostic_latest.csv"

REGULAR_MARKET_CLOSE_HOUR = 15
REGULAR_MARKET_CLOSE_MINUTE = 30

MICRO_PROBE_WATCH_BLOCKERS = {
    "ENTRY_CHANGE_BLOCK",
    "ENTRY_ATR_CAP",
    "HIGH_REJECTION_ENTRY_BLOCK",
    "RVOL_OVERHEAT_BLOCK",
    "SCORE_RVOL_OVERHEAT_BLOCK",
}

POLICY_HARD_BLOCKERS = {
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


def _to_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except Exception:
        return default


def _parse_ts(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value or "").strip())
    except Exception:
        return None


def _is_regular_session_signal(value: Any) -> bool:
    ts = _parse_ts(value)
    if ts is None:
        return False
    close_ts = ts.replace(hour=REGULAR_MARKET_CLOSE_HOUR, minute=REGULAR_MARKET_CLOSE_MINUTE, second=0, microsecond=0)
    return ts <= close_ts


def _reason_keys(*values: Any) -> Set[str]:
    keys: Set[str] = set()
    for value in values:
        for part in str(value or "").split("|"):
            part = part.strip()
            if part:
                keys.add(part.split(":", 1)[0].strip())
    return keys


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists() or path.stat().st_size <= 5:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        return list(csv.DictReader(fp))


def _queue_files(date_filter: str, all_history: bool) -> List[Path]:
    if all_history:
        return sorted(LOG_DIR.glob("surge_no_lob_recheck_queue_*.csv"))
    return sorted(LOG_DIR.glob(f"surge_no_lob_recheck_queue_{date_filter}_*.csv"))


def _is_candidate(row: Dict[str, Any]) -> tuple[bool, List[str], List[str]]:
    keys = _reason_keys(row.get("exclude_reasons"), row.get("entry_reason"), row.get("paper_probe_block_reasons"))
    watch_blockers = sorted(k for k in keys if k in MICRO_PROBE_WATCH_BLOCKERS)
    policy_hard = sorted(k for k in keys if k in POLICY_HARD_BLOCKERS)
    if not watch_blockers or policy_hard:
        return False, watch_blockers, policy_hard
    if not _to_bool(row.get("lob_available")):
        return False, watch_blockers, policy_hard
    if str(row.get("lob_status") or "").upper() != "OK":
        return False, watch_blockers, policy_hard
    if _to_float(row.get("spread_bps"), 999999.0) > 40.0:
        return False, watch_blockers, policy_hard
    return True, watch_blockers, policy_hard


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay surge micro-probe watch candidates from archived recheck queues.")
    parser.add_argument("--date", default=datetime.now().strftime("%Y%m%d"), help="Date filter in YYYYMMDD format.")
    parser.add_argument("--all-history", action="store_true", help="Scan all archived recheck queue files.")
    args = parser.parse_args()

    generated_at = datetime.now().isoformat(timespec="seconds")
    file_summaries: List[Dict[str, Any]] = []
    candidate_rows: List[Dict[str, Any]] = []
    after_cutoff_rows: List[Dict[str, Any]] = []
    date_summaries: Dict[str, Counter[str]] = {}
    watch_blockers = Counter()
    policy_hard_blockers = Counter()

    for path in _queue_files(str(args.date), bool(args.all_history)):
        rows = _read_csv(path)
        if not rows:
            continue
        eligible_before = 0
        eligible_after = 0
        before_codes: List[str] = []
        after_codes: List[str] = []
        for row in rows:
            is_candidate, row_watch, row_hard = _is_candidate(row)
            for token in row_watch:
                watch_blockers[token] += 1
            for token in row_hard:
                policy_hard_blockers[token] += 1
            if not is_candidate:
                continue
            out = {
                "source_file": path.name,
                "ts": row.get("ts", ""),
                "code": str(row.get("code", "")).zfill(6),
                "detected_surge_type": row.get("detected_surge_type", ""),
                "surge_score_final": _to_float(row.get("surge_score_final")),
                "change_pct": _to_float(row.get("change_pct")),
                "rvol20": _to_float(row.get("rvol20")),
                "trading_value": _to_float(row.get("trading_value")),
                "lob_status": row.get("lob_status", ""),
                "spread_bps": _to_float(row.get("spread_bps")),
                "watch_blockers": "|".join(row_watch),
                "regular_session_signal": _is_regular_session_signal(row.get("ts")),
                "entry_approval_changed": False,
                "policy_change": False,
                "paper_order_route": False,
                "broker_order_route": False,
                "trading_route": False,
                "research_only": True,
                "must_not_dispatch": True,
            }
            if out["regular_session_signal"]:
                eligible_before += 1
                before_codes.append(out["code"])
                candidate_rows.append(out)
                date_summaries.setdefault(str(row.get("date") or "")[:8], Counter())["before_cutoff"] += 1
            else:
                eligible_after += 1
                after_codes.append(out["code"])
                after_cutoff_rows.append(out)
                date_summaries.setdefault(str(row.get("date") or "")[:8], Counter())["after_cutoff"] += 1
        file_summaries.append(
            {
                "file": path.name,
                "rows": len(rows),
                "eligible_before_cutoff": eligible_before,
                "eligible_after_cutoff": eligible_after,
                "codes_before": "|".join(before_codes),
                "codes_after": "|".join(after_codes),
            }
        )

    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as fp:
        fields = [
            "source_file",
            "ts",
            "code",
            "detected_surge_type",
            "surge_score_final",
            "change_pct",
            "rvol20",
            "trading_value",
            "lob_status",
            "spread_bps",
            "watch_blockers",
            "regular_session_signal",
            "entry_approval_changed",
            "policy_change",
            "paper_order_route",
            "broker_order_route",
            "trading_route",
            "research_only",
            "must_not_dispatch",
        ]
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in candidate_rows + after_cutoff_rows:
            writer.writerow({field: row.get(field, "") for field in fields})

    payload = {
        "ts": generated_at,
        "status": "OK",
        "scope": "surge_micro_probe_replay_diagnostic",
        "date_filter": str(args.date),
        "all_history": bool(args.all_history),
        "source_glob": str(LOG_DIR / ("surge_no_lob_recheck_queue_*.csv" if args.all_history else f"surge_no_lob_recheck_queue_{args.date}_*.csv")),
        "files_scanned": len(file_summaries),
        "candidate_rows_before_cutoff": len(candidate_rows),
        "candidate_rows_after_cutoff": len(after_cutoff_rows),
        "file_summaries": file_summaries,
        "date_summaries": [
            {
                "date": date,
                "candidate_rows_before_cutoff": int(counts.get("before_cutoff", 0)),
                "candidate_rows_after_cutoff": int(counts.get("after_cutoff", 0)),
            }
            for date, counts in sorted(date_summaries.items())
        ],
        "watch_blocker_counts": [{"blocker": k, "count": int(v)} for k, v in watch_blockers.most_common()],
        "policy_hard_blocker_counts": [{"blocker": k, "count": int(v)} for k, v in policy_hard_blockers.most_common()],
        "risk_contract": {
            "policy_change": False,
            "entry_approval_changed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_route": False,
            "research_only": True,
            "must_not_dispatch": True,
            "purpose": "historical replay diagnostic only; no order route",
        },
        "out_csv": str(OUT_CSV),
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": "OK", "candidate_rows_before_cutoff": len(candidate_rows), "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
