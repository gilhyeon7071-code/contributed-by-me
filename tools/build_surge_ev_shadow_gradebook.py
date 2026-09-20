import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CANDIDATES_JSON = LOG_DIR / "surge_ev_shadow_candidates_latest.json"
SIMULATION_JSON = LOG_DIR / "surge_ev_shadow_simulation_latest.json"
OUT_JSON = LOG_DIR / "surge_ev_shadow_gradebook_latest.json"
OUT_CSV = LOG_DIR / "surge_ev_shadow_gradebook_latest.csv"

REQUIRED_FALSE = {
    "policy_change",
    "entry_approval_changed",
    "live_order_allowed",
    "entry_signal",
    "paper_order_route",
    "broker_order_route",
    "trading_route",
}
REQUIRED_TRUE = {"research_only", "must_not_dispatch"}

MIN_EVALUATED_FOR_PROMOTION_REVIEW = 30
MIN_EVALUABLE_RATIO = 0.70
MAX_STOP_HIT_RATE = 0.30


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except Exception:
        return default


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _candidate_key(row: Dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row.get("code") or "").zfill(6),
        str(row.get("ts") or row.get("signal_ts") or ""),
        str(row.get("tier") or row.get("candidate_tier") or ""),
        str(row.get("source") or ""),
    )


def _session_bucket(simulation: Dict[str, Any]) -> str:
    status = str(simulation.get("status") or "")
    if status == "EVALUATED":
        return "REGULAR_PRIMARY_EVALUATED"
    if status == "NOT_EVALUABLE_MARKET_CLOSE_BEFORE_PRIMARY_TIMEBOX":
        return "LATE_SESSION_MARKET_CLOSE_BEFORE_PRIMARY_TIMEBOX"
    if status == "NOT_EVALUABLE_NO_PRICE_AT_OR_AFTER_SIGNAL":
        return "PRICE_HISTORY_COVERAGE_GAP"
    if status == "NOT_EVALUABLE_WAITING_PRIMARY_TIMEBOX":
        return "REGULAR_WAITING_PRIMARY_TIMEBOX"
    return "OTHER_NOT_EVALUABLE"


def _contract_ok(payload: Dict[str, Any]) -> bool:
    contract = payload.get("risk_contract") or {}
    for key in REQUIRED_FALSE:
        if contract.get(key) is not False:
            return False
    for key in REQUIRED_TRUE:
        if contract.get(key) is not True:
            return False
    return True


def _row_contract_ok(row: Dict[str, Any]) -> bool:
    for key in REQUIRED_FALSE:
        if row.get(key) is not False:
            return False
    for key in REQUIRED_TRUE:
        if row.get(key) is not True:
            return False
    return True


def _grade_row(candidate: Dict[str, Any], simulation: Dict[str, Any], artifact_contract_ok: bool) -> Dict[str, Any]:
    code = str(candidate.get("code") or simulation.get("code") or "").zfill(6)
    remaining = str(candidate.get("remaining_hard_blockers") or "").strip()
    sim_status = str(simulation.get("status") or "NOT_EVALUATED")
    score = _to_float(candidate.get("surge_score_final"))
    spread_bps = _to_float(candidate.get("spread_bps"), 999999.0)
    rvol20 = _to_float(candidate.get("rvol20"))
    primary_ret_pct = _to_float(simulation.get("primary_ret_pct"))
    stop_hit = str(simulation.get("exit_reason") or "") == "STOP_LOSS_HIT"

    if not artifact_contract_ok or not _row_contract_ok(candidate) or (simulation and not _row_contract_ok(simulation)):
        grade = "REJECT_CONTRACT_FAIL"
        reason = "NO_BYPASS_CONTRACT_NOT_SATISFIED"
    elif remaining:
        grade = "REJECT_HARD_BLOCKER"
        reason = f"remaining_hard_blockers={remaining}"
    elif sim_status != "EVALUATED":
        grade = "PENDING_EVALUATION"
        reason = sim_status
    elif stop_hit:
        grade = "REJECT_STOP_HIT"
        reason = "simulation_stop_loss_hit"
    elif score >= 90.0 and spread_bps <= 20.0 and 1.0 <= rvol20 <= 5.0 and primary_ret_pct > 0.0:
        grade = "A_RESEARCH_ONLY"
        reason = "strong_score_tight_spread_positive_primary_return"
    elif score >= 80.0 and spread_bps <= 40.0 and primary_ret_pct > 0.0:
        grade = "B_RESEARCH_ONLY"
        reason = "positive_primary_return_but_not_a_grade"
    elif primary_ret_pct > 0.0:
        grade = "C_RESEARCH_ONLY"
        reason = "positive_return_low_quality_inputs"
    else:
        grade = "REJECT_NEGATIVE_EV"
        reason = "non_positive_primary_return"

    return {
        "code": code,
        "grade": grade,
        "reason": reason,
        "candidate_tier": candidate.get("tier", ""),
        "source": candidate.get("source", ""),
        "signal_ts": candidate.get("ts", ""),
        "simulation_status": sim_status,
        "session_bucket": _session_bucket(simulation),
        "surge_score_final": score,
        "rvol20": rvol20,
        "change_pct": _to_float(candidate.get("change_pct")),
        "spread_bps": spread_bps,
        "primary_ret_pct": simulation.get("primary_ret_pct", ""),
        "exit_reason": simulation.get("exit_reason", ""),
        "micro_position_ret_pct": simulation.get("micro_position_ret_pct", ""),
        "remaining_hard_blockers": remaining,
        "promotion_allowed": False,
        "policy_change": False,
        "entry_approval_changed": False,
        "live_order_allowed": False,
        "entry_signal": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "trading_route": False,
        "research_only": True,
        "must_not_dispatch": True,
    }


def _promotion_summary(rows: List[Dict[str, Any]], artifact_contract_ok: bool) -> Dict[str, Any]:
    evaluated = [r for r in rows if r.get("simulation_status") == "EVALUATED"]
    candidate_count = len(rows)
    evaluated_count = len(evaluated)
    evaluable_ratio = (evaluated_count / candidate_count) if candidate_count else 0.0
    primary_returns = [_to_float(r.get("primary_ret_pct")) for r in evaluated]
    stop_hits = [r for r in evaluated if str(r.get("exit_reason") or "") == "STOP_LOSS_HIT"]
    avg_primary = round(sum(primary_returns) / len(primary_returns), 6) if primary_returns else None
    median_primary = round(median(primary_returns), 6) if primary_returns else None
    stop_hit_rate = round(len(stop_hits) / len(evaluated), 6) if evaluated else None

    criteria = {
        "artifact_contract_ok": artifact_contract_ok,
        "min_evaluated_n": MIN_EVALUATED_FOR_PROMOTION_REVIEW,
        "actual_evaluated_n": evaluated_count,
        "min_evaluable_ratio": MIN_EVALUABLE_RATIO,
        "actual_evaluable_ratio": round(evaluable_ratio, 6),
        "median_primary_ret_pct_gt_0": bool(median_primary is not None and median_primary > 0.0),
        "avg_primary_ret_pct_gt_0": bool(avg_primary is not None and avg_primary > 0.0),
        "max_stop_hit_rate": MAX_STOP_HIT_RATE,
        "actual_stop_hit_rate": stop_hit_rate,
    }
    enough_sample = evaluated_count >= MIN_EVALUATED_FOR_PROMOTION_REVIEW and evaluable_ratio >= MIN_EVALUABLE_RATIO
    positive_ev = bool(median_primary is not None and median_primary > 0.0 and avg_primary is not None and avg_primary > 0.0)
    stop_ok = bool(stop_hit_rate is not None and stop_hit_rate <= MAX_STOP_HIT_RATE)
    review_ready = bool(artifact_contract_ok and enough_sample and positive_ev and stop_ok)
    if not artifact_contract_ok:
        status = "PROMOTION_REVIEW_BLOCKED_CONTRACT_FAIL"
    elif not enough_sample:
        status = "PROMOTION_NOT_EVALUABLE_YET"
    elif review_ready:
        status = "PROMOTION_REVIEW_READY_RESEARCH_ONLY"
    else:
        status = "PROMOTION_REVIEW_REJECTED_BY_EVIDENCE"
    return {
        "status": status,
        "promotion_allowed": False,
        "policy_change": False,
        "review_ready_for_policy_proposal": review_ready,
        "candidate_rows": candidate_count,
        "evaluated_rows": evaluated_count,
        "not_evaluable_rows": candidate_count - evaluated_count,
        "avg_primary_ret_pct": avg_primary,
        "median_primary_ret_pct": median_primary,
        "stop_hit_rate": stop_hit_rate,
        "criteria": criteria,
    }


def _session_summary(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for bucket, items in sorted(_group_by(rows, "session_bucket").items()):
        evaluated = [row for row in items if row.get("simulation_status") == "EVALUATED"]
        returns = [_to_float(row.get("primary_ret_pct")) for row in evaluated]
        stop_hits = [row for row in evaluated if str(row.get("exit_reason") or "") == "STOP_LOSS_HIT"]
        out.append({
            "session_bucket": bucket,
            "rows": len(items),
            "evaluated_rows": len(evaluated),
            "avg_primary_ret_pct": round(sum(returns) / len(returns), 6) if returns else None,
            "median_primary_ret_pct": round(median(returns), 6) if returns else None,
            "stop_hits": len(stop_hits),
            "codes": ",".join(sorted({str(row.get("code") or "") for row in items})),
        })
    return out


def _group_by(rows: List[Dict[str, Any]], key: str) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get(key) or ""), []).append(row)
    return grouped


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "code",
        "grade",
        "reason",
        "candidate_tier",
        "source",
        "signal_ts",
        "simulation_status",
        "session_bucket",
        "surge_score_final",
        "rvol20",
        "change_pct",
        "spread_bps",
        "primary_ret_pct",
        "exit_reason",
        "micro_position_ret_pct",
        "remaining_hard_blockers",
        "promotion_allowed",
        "policy_change",
        "entry_approval_changed",
        "live_order_allowed",
        "entry_signal",
        "paper_order_route",
        "broker_order_route",
        "trading_route",
        "research_only",
        "must_not_dispatch",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def main() -> int:
    now = datetime.now().isoformat(timespec="seconds")
    candidates_payload = _load_json(CANDIDATES_JSON)
    simulation_payload = _load_json(SIMULATION_JSON)
    current_candidates = list(candidates_payload.get("candidates") or [])
    simulation_rows = list(simulation_payload.get("rows") or [])
    candidates_by_key = {_candidate_key(candidate): candidate for candidate in current_candidates}
    for sim in simulation_rows:
        key = _candidate_key(sim)
        if key in candidates_by_key:
            continue
        candidates_by_key[key] = {
            "code": sim.get("code", ""),
            "tier": sim.get("tier", ""),
            "source": sim.get("source", ""),
            "ts": sim.get("signal_ts", ""),
            "detected_surge_type": sim.get("detected_surge_type", ""),
            "surge_score_final": sim.get("surge_score_final", ""),
            "rvol20": sim.get("rvol20", ""),
            "change_pct": sim.get("change_pct", ""),
            "spread_bps": sim.get("spread_bps", ""),
            "remaining_hard_blockers": "",
            "policy_change": False,
            "entry_approval_changed": False,
            "live_order_allowed": False,
            "entry_signal": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_route": False,
            "research_only": True,
            "must_not_dispatch": True,
        }
    candidates = sorted(candidates_by_key.values(), key=lambda x: (str(x.get("ts") or ""), str(x.get("code") or "")))
    sim_by_key = {_candidate_key(row): row for row in simulation_rows}
    artifact_contract_ok = _contract_ok(candidates_payload) and _contract_ok(simulation_payload)
    rows = [_grade_row(candidate, sim_by_key.get(_candidate_key(candidate), {}), artifact_contract_ok) for candidate in candidates]
    summary = _promotion_summary(rows, artifact_contract_ok)
    payload = {
        "ts": now,
        "status": "OK",
        "scope": "surge_ev_shadow_gradebook",
        "source_candidates_json": str(CANDIDATES_JSON),
        "source_simulation_json": str(SIMULATION_JSON),
        "source_candidates_ts": candidates_payload.get("ts", ""),
        "source_simulation_ts": simulation_payload.get("ts", ""),
        "current_candidate_rows": len(current_candidates),
        "simulation_rows": len(simulation_rows),
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
            "purpose": "research shadow gradebook only; no order route and no entry approval",
        },
        "artifact_contract_ok": artifact_contract_ok,
        "grade_counts": [{"grade": k, "count": int(v)} for k, v in Counter(row["grade"] for row in rows).most_common()],
        "session_summary": _session_summary(rows),
        "regular_primary_promotion_summary": _promotion_summary(
            [row for row in rows if row.get("session_bucket") == "REGULAR_PRIMARY_EVALUATED"],
            artifact_contract_ok,
        ),
        "promotion_summary": summary,
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    print(json.dumps({
        "status": "OK",
        "candidate_rows": len(rows),
        "evaluated_rows": summary["evaluated_rows"],
        "promotion_status": summary["status"],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
