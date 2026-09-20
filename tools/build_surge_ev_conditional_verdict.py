import csv
import json
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Callable, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
BUCKET_CSV = LOG_DIR / "surge_ev_bucket_decomposition_latest.csv"
BUCKET_JSON = LOG_DIR / "surge_ev_bucket_decomposition_latest.json"
OUT_JSON = LOG_DIR / "surge_ev_conditional_verdict_latest.json"
OUT_CSV = LOG_DIR / "surge_ev_conditional_verdict_latest.csv"

MIN_POLICY_N = 30
MIN_WATCH_N = 5
MAX_STOP_RATE = 0.30


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except Exception:
        return default


def _load_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists() or path.stat().st_size <= 5:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        return list(csv.DictReader(fp))


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _summarize(name: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    returns = [_to_float(row.get("primary_ret_pct")) for row in rows]
    if not returns:
        return {
            "rule": name,
            "n": 0,
            "verdict": "NO_SAMPLE",
            "policy_change": False,
            "entry_approval_changed": False,
        }
    stop_hits = sum(1 for row in rows if str(row.get("exit_reason") or "") == "STOP_LOSS_HIT")
    positives = sum(1 for value in returns if value > 0.0)
    avg_ret = round(sum(returns) / len(returns), 6)
    median_ret = round(median(returns), 6)
    stop_rate = round(stop_hits / len(returns), 6)
    if len(returns) >= MIN_POLICY_N and avg_ret > 0.0 and median_ret > 0.0 and stop_rate <= MAX_STOP_RATE:
        verdict = "POLICY_REVIEW_READY_RESEARCH_ONLY"
    elif len(returns) >= MIN_WATCH_N and avg_ret > 0.0 and median_ret > 0.0 and stop_hits == 0:
        verdict = "WATCH_PROBE_EXPAND"
    elif avg_ret > 0.0 or median_ret > 0.0:
        verdict = "WATCH_ONLY_WEAK_SAMPLE"
    else:
        verdict = "REJECT_POLICY_CHANGE"
    return {
        "rule": name,
        "n": len(returns),
        "avg_primary_ret_pct": avg_ret,
        "median_primary_ret_pct": median_ret,
        "positive_rows": positives,
        "stop_hits": stop_hits,
        "stop_rate": stop_rate,
        "codes": ",".join(sorted({str(row.get("code") or "") for row in rows})),
        "verdict": verdict,
        "policy_change": False,
        "entry_approval_changed": False,
    }


def _regular_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [row for row in rows if row.get("session_bucket") == "REGULAR_PRIMARY_EVALUATED"]


def main() -> int:
    now = datetime.now().isoformat(timespec="seconds")
    payload = _load_json(BUCKET_JSON)
    rows = _regular_rows(_load_rows(BUCKET_CSV))

    def source(name: str) -> Callable[[Dict[str, Any]], bool]:
        return lambda row: row.get("source") == name

    rules: Dict[str, Callable[[Dict[str, Any]], bool]] = {
        "ALL_REGULAR_PRIMARY": lambda row: True,
        "NO_LOB_ALL": source("NO_LOB_RECHECK_CLEAN"),
        "SCORE_RVOL_ALL": source("SCORE_RVOL_LOB_CONFIRMED_CLEAN"),
        "MICRO_PROBE_WATCH_ALL": source("SURGE_BLOCKER_LOB_CONFIRMED_WATCH"),
        "MICRO_PROBE_WATCH_12_16_ALL": lambda row: (
            row.get("source") == "SURGE_BLOCKER_LOB_CONFIRMED_WATCH"
            and row.get("change_bucket") == "12-16%"
        ),
        "MICRO_PROBE_WATCH_SPREAD_LE40": lambda row: (
            row.get("source") == "SURGE_BLOCKER_LOB_CONFIRMED_WATCH"
            and _to_float(row.get("spread_bps")) <= 40.0
        ),
        "SCORE_RVOL_12_16_ALL": lambda row: (
            row.get("source") == "SCORE_RVOL_LOB_CONFIRMED_CLEAN"
            and row.get("change_bucket") == "12-16%"
        ),
        "WATCH_SCORE_RVOL_12_16_SPREAD_LE15_RVOL_3_4_2": lambda row: (
            row.get("source") == "SCORE_RVOL_LOB_CONFIRMED_CLEAN"
            and row.get("change_bucket") == "12-16%"
            and _to_float(row.get("spread_bps")) <= 15.0
            and 3.0 <= _to_float(row.get("rvol20")) <= 4.2
        ),
        "NO_LOB_12_16_ALL": lambda row: (
            row.get("source") == "NO_LOB_RECHECK_CLEAN"
            and row.get("change_bucket") == "12-16%"
        ),
        "NO_NEAR_HIGH": lambda row: "NEAR_HIGH_CHASE" not in str(row.get("cause_flags") or ""),
        "NO_WEAK_RVOL": lambda row: "WEAK_RVOL" not in str(row.get("cause_flags") or ""),
        "NO_LOB_NO_NEAR_HIGH_NO_WEAK_RVOL": lambda row: (
            row.get("source") == "NO_LOB_RECHECK_CLEAN"
            and "NEAR_HIGH_CHASE" not in str(row.get("cause_flags") or "")
            and "WEAK_RVOL" not in str(row.get("cause_flags") or "")
        ),
    }
    verdicts = [_summarize(name, [row for row in rows if fn(row)]) for name, fn in rules.items()]
    final_verdict = {
        "policy_change_ready": False,
        "entry_approval_ready": False,
        "production_entry_change_relaxation": "REJECT_BY_EVIDENCE",
        "watch_probe_expand_rules": [
            row["rule"] for row in verdicts if row.get("verdict") == "WATCH_PROBE_EXPAND"
        ],
        "reject_rules": [
            row["rule"] for row in verdicts if row.get("verdict") == "REJECT_POLICY_CHANGE"
        ],
        "basis": "regular-session primary 15-minute EV only; late-session and coverage-gap rows excluded",
    }
    out = {
        "ts": now,
        "status": "OK",
        "scope": "surge_ev_conditional_verdict",
        "source_bucket_json": str(BUCKET_JSON),
        "source_bucket_csv": str(BUCKET_CSV),
        "source_bucket_ts": payload.get("ts", ""),
        "regular_primary_rows": len(rows),
        "criteria": {
            "min_policy_n": MIN_POLICY_N,
            "min_watch_n": MIN_WATCH_N,
            "max_stop_rate": MAX_STOP_RATE,
        },
        "risk_contract": {
            "policy_change": False,
            "entry_approval_changed": False,
            "live_order_allowed": False,
            "entry_signal": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_route": False,
            "research_only": True,
            "must_not_dispatch": True,
        },
        "final_verdict": final_verdict,
        "verdicts": verdicts,
    }
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    fields = [
        "rule",
        "n",
        "avg_primary_ret_pct",
        "median_primary_ret_pct",
        "positive_rows",
        "stop_hits",
        "stop_rate",
        "codes",
        "verdict",
        "policy_change",
        "entry_approval_changed",
    ]
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in verdicts:
            writer.writerow({field: row.get(field, "") for field in fields})
    print(json.dumps({
        "status": "OK",
        "regular_primary_rows": len(rows),
        "watch_probe_expand_rules": final_verdict["watch_probe_expand_rules"],
        "production_entry_change_relaxation": final_verdict["production_entry_change_relaxation"],
        "out_json": str(OUT_JSON),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
