from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

CONFIG_PATH = ROOT / "paper" / "paper_engine_config.json"
CANDIDATES_FINAL_CSV = LOG_DIR / "candidates_latest_data.with_final_score.csv"
ENTRY_LAYER_JSON = LOG_DIR / "entry_decision_layers_runtime_latest.json"
ENTRY_LAYER_CSV = LOG_DIR / "entry_decision_layers_runtime_latest.csv"
PENDING_JSON = LOG_DIR / "pending_entry_status_latest.json"
P1_JSON = LOG_DIR / "p1_entry_gate_status_latest.json"
RISK_JSON = LOG_DIR / "risk_orchestration_latest.json"
PRODUCTION_RISK_JSON = LOG_DIR / "production_risk_playbook_latest.json"
POLICY_GROUP_JSON = LOG_DIR / "entry_policy_group_report_latest.json"
POLICY_REVIEW_JSON = LOG_DIR / "entry_policy_change_candidate_review_latest.json"
ENTRY_TRACE_JSON = LOG_DIR / "entry_layer_input_trace_audit_latest.json"
LIQUIDITY_FILTER_JSON = LOG_DIR / "liquidity_filter_daily_last.json"

OUT_JSON = LOG_DIR / "normal_entry_path_bottleneck_audit_latest.json"
OUT_CSV = LOG_DIR / "normal_entry_path_bottleneck_audit_latest.csv"


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: List[Dict[str, Any]], fields: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def _float(value: Any) -> float | None:
    try:
        if value in ("", None):
            return None
        return float(value)
    except Exception:
        return None


def _stage(name: str, status: str, count: Any, blocker: str, interpretation: str) -> Dict[str, Any]:
    return {
        "stage": name,
        "status": status,
        "count": count,
        "blocker": blocker,
        "interpretation": interpretation,
        "trading_effect": "false",
        "policy_effect": "false",
        "policy_change_applied": "false",
    }


def _cfg_float(cfg: Dict[str, Any], keys: List[str], default: float) -> float:
    cur: Any = cfg
    for key in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    try:
        return float(cur)
    except Exception:
        return default


def _classify_normal_candidate(row: Dict[str, str], cfg: Dict[str, Any]) -> Dict[str, Any]:
    code = str(row.get("code") or "").zfill(6)
    reason = str(row.get("execution_reason") or "").strip()
    reason_u = reason.upper()
    positive_entry = _truthy(row.get("positive_entry_ok"))
    normal_reason = reason_u.startswith("NORMAL_")
    normal_meta = any(str(row.get(k) or "").strip() for k in row if str(k).startswith("normal_"))
    is_normal_scope = bool(positive_entry or normal_reason or normal_meta)

    max_spread_bps = _cfg_float(cfg, ["normal_entry_execution_quality", "max_spread_bps"], 30.0)
    block_v_accel_min = _cfg_float(
        cfg,
        ["normal_realtime_gap_policy", "intraday_momentum_recheck", "block_v_accel_min"],
        1.0,
    )
    reduce_v_accel_min = _cfg_float(
        cfg,
        ["normal_realtime_gap_policy", "intraday_momentum_recheck", "reduce_v_accel_min"],
        1.5,
    )
    min_value_ratio = _cfg_float(
        cfg,
        ["normal_realtime_gap_policy", "intraday_momentum_recheck", "min_value_ratio"],
        0.7,
    )

    spread_bps = _float(row.get("normal_spread_bps"))
    value_ratio = _float(row.get("normal_intraday_value_ratio"))
    rechecked_v_accel = _float(row.get("normal_intraday_rechecked_v_accel"))
    spread_excess_bps = (
        round(float(spread_bps) - float(max_spread_bps), 6)
        if spread_bps is not None
        else ""
    )

    if not is_normal_scope:
        bucket = "OUT_OF_NORMAL_SCOPE"
        followup = "none"
        interpretation = "Not a current normal/general candidate row."
    elif reason_u == "BUY_EXECUTED":
        bucket = "EXECUTED"
        followup = "track_position"
        interpretation = "Already executed."
    elif "NORMAL_SPREAD_BLOCK" in reason_u:
        bucket = "RECHECK_SPREAD"
        followup = "recheck_lob_spread"
        interpretation = "LOB exists, but spread is wider than the normal entry limit."
    elif "NORMAL_INTRADAY_MOMENTUM_BLOCK" in reason_u:
        bucket = "RECHECK_MOMENTUM"
        followup = "recheck_intraday_value_and_v_accel"
        interpretation = "Intraday value ratio reduced v_accel below the block threshold."
    elif "NORMAL_LOB_" in reason_u:
        bucket = "RECHECK_LOB"
        followup = "recheck_lob_collection"
        interpretation = "Normal path needs fresh usable LOB before entry can be evaluated."
    elif "NORMAL_MARKOUT" in reason_u:
        bucket = "RECHECK_MARKOUT"
        followup = "recheck_markout_after_next_lob_snapshot"
        interpretation = "LOB is not enough; markout evidence is missing or adverse."
    elif "CLOSE_CUTOFF" in reason_u:
        bucket = "RECHECK_NEXT_SESSION"
        followup = "recheck_when_market_reopens"
        interpretation = "Entry evaluation was deferred by the close cutoff fail-closed guard."
    elif reason_u:
        bucket = "OTHER_BLOCK"
        followup = "review_non_normal_blocker"
        interpretation = "Blocked by a non-normal execution-quality reason."
    else:
        bucket = "NOT_EVALUATED"
        followup = "wait_for_next_decision_layer"
        interpretation = "No execution decision reason is available."

    return {
        "code": code,
        "name": str(row.get("name") or ""),
        "rank_score": row.get("rank_score", ""),
        "execution_layer": row.get("execution_layer", ""),
        "execution_reason": reason,
        "opportunity_state": row.get("opportunity_state", ""),
        "next_action": row.get("next_action", ""),
        "normal_recheck_bucket": bucket,
        "recommended_followup": followup,
        "interpretation": interpretation,
        "normal_lob_status": row.get("normal_lob_status", ""),
        "normal_lob_reason": row.get("normal_lob_reason", ""),
        "normal_spread_bps": spread_bps if spread_bps is not None else "",
        "normal_spread_limit_bps": max_spread_bps,
        "normal_spread_excess_bps": spread_excess_bps,
        "normal_intraday_value_ratio": value_ratio if value_ratio is not None else "",
        "normal_intraday_value_ratio_min": min_value_ratio,
        "normal_intraday_rechecked_v_accel": rechecked_v_accel if rechecked_v_accel is not None else "",
        "normal_intraday_block_v_accel_min": block_v_accel_min,
        "normal_intraday_reduce_v_accel_min": reduce_v_accel_min,
        "trading_effect": "false",
        "policy_effect": "false",
        "policy_change_applied": "false",
    }


def _candidate_quality(rows: List[Dict[str, str]]) -> Dict[str, Any]:
    dated_rows = [r for r in rows if str(r.get("date") or "").strip()]
    score_rows = [r for r in rows if _float(r.get("final_score")) is not None]
    date_counts = Counter(str(r.get("date") or "").strip() for r in rows)
    return {
        "rows": len(rows),
        "dated_rows": len(dated_rows),
        "score_rows": len(score_rows),
        "date_counts": dict(sorted(date_counts.items())),
        "codes_sample": [str(r.get("code") or "").zfill(6) for r in rows[:10]],
    }


def _liquidity_filter_trace(payload: Dict[str, Any]) -> Dict[str, Any]:
    removed = payload.get("removed") if isinstance(payload, dict) else []
    if not isinstance(removed, list):
        removed = []
    removed_rows: List[Dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()
    limit_up_removed_codes: List[str] = []
    for row in removed:
        if not isinstance(row, dict):
            continue
        code = str(row.get("code") or "").zfill(6)
        reasons = row.get("reasons")
        if not isinstance(reasons, list):
            reasons = [str(reasons or "")]
        reason_text = "|".join(str(x) for x in reasons if str(x).strip())
        for reason in reasons:
            reason_s = str(reason or "").strip()
            if reason_s:
                reason_counts[reason_s.split("(", 1)[0]] += 1
                if reason_s.startswith("limit_up_near"):
                    limit_up_removed_codes.append(code)
        removed_rows.append(
            {
                "code": code,
                "date": row.get("date"),
                "trading_value": row.get("trading_value"),
                "day_ret_pct": row.get("day_ret_pct"),
                "reasons": reason_text,
            }
        )
    rows_summary = payload.get("rows_summary") if isinstance(payload.get("rows_summary"), dict) else {}
    return {
        "ts": payload.get("ts"),
        "status": payload.get("status"),
        "reason": payload.get("reason"),
        "input_rows_raw": rows_summary.get("input_rows_raw"),
        "rows_after_rule_filter": rows_summary.get("rows_after_rule_filter"),
        "dropped_by_rule_filter": rows_summary.get("dropped_by_rule_filter"),
        "removed_rows": removed_rows,
        "removed_reason_counts": dict(reason_counts),
        "limit_up_removed_codes": sorted(set(limit_up_removed_codes)),
    }


def main() -> int:
    cfg = _read_json(CONFIG_PATH)
    candidates = _read_csv(CANDIDATES_FINAL_CSV)
    entry_layer = _read_json(ENTRY_LAYER_JSON)
    entry_layer_rows = _read_csv(ENTRY_LAYER_CSV)
    pending = _read_json(PENDING_JSON)
    p1 = _read_json(P1_JSON)
    risk = _read_json(RISK_JSON)
    production = _read_json(PRODUCTION_RISK_JSON)
    policy_group = _read_json(POLICY_GROUP_JSON)
    policy_review = _read_json(POLICY_REVIEW_JSON)
    entry_trace = _read_json(ENTRY_TRACE_JSON)
    liquidity_filter = _read_json(LIQUIDITY_FILTER_JSON)
    liquidity_trace = _liquidity_filter_trace(liquidity_filter)

    risk_detail = risk.get("risk_orchestration", {})
    market_ops = cfg.get("market_ops_policy", {})
    fallback_policy = market_ops.get("entry_fallback_policy", {})

    candidate_quality = _candidate_quality(candidates)
    entry_layer_candidate_rows = int(entry_layer.get("candidate_rows") or 0)
    entry_layer_decision_rows = int(entry_layer.get("decision_rows") or 0)
    pending_ready = int(pending.get("entry_ready") or 0)
    pending_candidates_after_caps = int(pending.get("candidates_after_caps") or 0)
    p1_before = int(p1.get("entry_candidates_before") or 0)
    p1_after = int(p1.get("entry_candidates_after") or 0)

    stages: List[Dict[str, Any]] = []
    stages.append(
        _stage(
            "candidate_file",
            "HAS_ROWS" if candidates else "NO_ROWS",
            len(candidates),
            "",
            "Candidate source file exists, but this alone is not normal-entry readiness.",
        )
    )
    stages.append(
        _stage(
            "entry_source_trace",
            "NO_VIABLE_ROWS" if int(entry_trace.get("current_candidate_viable_rows") or 0) == 0 else "HAS_VIABLE_ROWS",
            entry_trace.get("current_candidate_viable_rows", ""),
            json.dumps(entry_trace.get("current_candidate_trace_reason_counts", {}), ensure_ascii=False, sort_keys=True),
            "Current candidate source rows are traced before the entry decision layer; zero viable rows means the issue is source eligibility, not LOB/order dispatch.",
        )
    )
    stages.append(
        _stage(
            "liquidity_filter_trace",
            "REMOVED_ROWS" if int(liquidity_trace.get("dropped_by_rule_filter") or 0) > 0 else "NO_REMOVED_ROWS",
            liquidity_trace.get("dropped_by_rule_filter", ""),
            json.dumps(liquidity_trace.get("removed_reason_counts", {}), ensure_ascii=False, sort_keys=True),
            "Liquidity filter runs before normal entry readiness; removed rows should be separated from missing candidate generation.",
        )
    )
    stages.append(
        _stage(
            "p1_gate_pre_entry",
            "REDUCE" if str(p1.get("entry_gate_decision_before_p1") or "") == "REDUCE" else str(p1.get("entry_gate_decision_before_p1") or "UNKNOWN"),
            p1_before,
            str(p1.get("entry_gate_reason_before_p1") or ""),
            "P1 sees entry candidates, but they are in reduced defensive state before normal order readiness.",
        )
    )
    stages.append(
        _stage(
            "entry_decision_layer",
            "EMPTY" if entry_layer_candidate_rows == 0 else "HAS_ROWS",
            entry_layer_candidate_rows,
            "entry_decision_layers_runtime candidate_rows=0" if entry_layer_candidate_rows == 0 else "",
            "Current decision layer has no candidate rows, so alpha/risk/execution layer counts cannot evaluate normal_entry today.",
        )
    )
    stages.append(
        _stage(
            "pending_entry_state",
            "EMPTY" if pending_ready == 0 and pending_candidates_after_caps == 0 else "HAS_READY",
            pending_ready,
            f"entry_ready={pending_ready}; candidates_after_caps={pending_candidates_after_caps}; pending_queue_len={pending.get('pending_queue_len')}",
            "No normal pending-entry row is ready for order path.",
        )
    )
    stages.append(
        _stage(
            "risk_orchestration",
            "BLOCKING_DEFENSIVE_STATE" if risk_detail.get("dd_stop_triggered") or risk.get("position_size_multiplier") == 0 else "NOT_BLOCKING",
            risk.get("position_size_multiplier"),
            f"dd_stop_triggered={risk_detail.get('dd_stop_triggered')}; scale_zero_causes={risk_detail.get('scale_zero_causes')}",
            "Risk state is defensive; this is not a normal-entry policy proof sample.",
        )
    )
    stages.append(
        _stage(
            "production_risk",
            "HARD_BLOCK" if production.get("blocked") else "NOT_BLOCKED",
            production.get("action"),
            str(production.get("reason") or ""),
            "Production risk is HARD/blocked, so normal-entry readiness should not be inferred from missing orders.",
        )
    )

    if entry_layer_candidate_rows == 0:
        if int(entry_trace.get("current_candidate_viable_rows") or 0) == 0:
            if int(liquidity_trace.get("dropped_by_rule_filter") or 0) > 0 and liquidity_trace.get("limit_up_removed_codes"):
                bottleneck = "ENTRY_SOURCE_MULTIFACTOR_NO_VIABLE_NORMAL_ROWS"
                bottleneck_type = "source_candidate_eligibility_and_quality_filter"
            else:
                bottleneck = "ENTRY_SOURCE_NO_VIABLE_NORMAL_ROWS"
                bottleneck_type = "source_candidate_eligibility_filter"
        else:
            bottleneck = "ENTRY_DECISION_LAYER_EMPTY"
            bottleneck_type = "runtime_artifact_gap_or_input_filter"
    elif pending_ready == 0:
        bottleneck = "PENDING_ENTRY_EMPTY"
        bottleneck_type = "normal_order_path_no_ready_rows"
    elif risk_detail.get("dd_stop_triggered") or production.get("blocked"):
        bottleneck = "DEFENSIVE_RISK_STATE"
        bottleneck_type = "state_block_not_normal_policy_proof"
    else:
        bottleneck = "UNKNOWN_REQUIRES_DEEPER_CODE_TRACE"
        bottleneck_type = "unknown"

    normal_candidate_rows = [
        _classify_normal_candidate(row, cfg)
        for row in entry_layer_rows
    ]
    normal_candidate_rows = [
        row for row in normal_candidate_rows if row.get("normal_recheck_bucket") != "OUT_OF_NORMAL_SCOPE"
    ]
    normal_bucket_counts = dict(Counter(str(row.get("normal_recheck_bucket") or "") for row in normal_candidate_rows))
    if normal_candidate_rows:
        if any(str(row.get("normal_recheck_bucket")) == "RECHECK_SPREAD" for row in normal_candidate_rows):
            bottleneck = "NORMAL_SPREAD_RECHECK_REQUIRED"
            bottleneck_type = "normal_execution_quality_recheck"
        elif any(str(row.get("normal_recheck_bucket")) == "RECHECK_MOMENTUM" for row in normal_candidate_rows):
            bottleneck = "NORMAL_MOMENTUM_RECHECK_REQUIRED"
            bottleneck_type = "normal_intraday_momentum_recheck"
        else:
            bottleneck = "NORMAL_RECHECK_CLASSIFIED"
            bottleneck_type = "normal_candidate_recheck_classification"

    out = {
        "generated_at": _now_ts(),
        "status": "PASS",
        "schema_version": "normal_entry_path_bottleneck_audit_v1",
        "source_files": {
            "config": str(CONFIG_PATH),
            "candidates_final": str(CANDIDATES_FINAL_CSV),
            "entry_decision_layers_json": str(ENTRY_LAYER_JSON),
            "entry_decision_layers_csv": str(ENTRY_LAYER_CSV),
            "pending_entry_status": str(PENDING_JSON),
            "p1_entry_gate_status": str(P1_JSON),
            "risk_orchestration": str(RISK_JSON),
            "production_risk_playbook": str(PRODUCTION_RISK_JSON),
            "entry_policy_group_report": str(POLICY_GROUP_JSON),
            "entry_policy_change_candidate_review": str(POLICY_REVIEW_JSON),
            "entry_layer_input_trace": str(ENTRY_TRACE_JSON),
            "liquidity_filter": str(LIQUIDITY_FILTER_JSON),
        },
        "entry_source_trace": {
            "generated_at": entry_trace.get("generated_at"),
            "status": entry_trace.get("status"),
            "same_cycle_comparable": entry_trace.get("same_cycle_comparable"),
            "candidate_newer_than_entry_layer": entry_trace.get("candidate_newer_than_entry_layer"),
            "candidate_rows_now": entry_trace.get("candidate_rows_now"),
            "entry_layer_candidate_rows": entry_trace.get("entry_layer_candidate_rows"),
            "entry_layer_decision_rows": entry_trace.get("entry_layer_decision_rows"),
            "current_candidate_viable_rows": entry_trace.get("current_candidate_viable_rows"),
            "current_candidate_trace_reason_counts": entry_trace.get("current_candidate_trace_reason_counts", {}),
            "primary_trace": entry_trace.get("primary_trace"),
        },
        "liquidity_filter_trace": liquidity_trace,
        "candidate_quality": candidate_quality,
        "current_policy_readback": {
            "entry_fallback_policy": fallback_policy,
            "strict_same_day_only": market_ops.get("strict_same_day_only"),
            "normal_policy_review_final_decision": policy_review.get("final_decision"),
            "approved_next_step": policy_review.get("approved_next_step"),
        },
        "policy_group_counts": policy_group.get("entry_policy_group_counts", {}),
        "stage_audit": stages,
        "normal_candidate_recheck_rows": len(normal_candidate_rows),
        "normal_recheck_bucket_counts": normal_bucket_counts,
        "normal_candidate_recheck_classification": normal_candidate_rows,
        "primary_bottleneck": bottleneck,
        "primary_bottleneck_type": bottleneck_type,
        "classification": "NORMAL_PATH_BOTTLENECK_AUDIT_ONLY",
        "decision": "NO_ALTERNATIVE_PATH_NO_POLICY_RELAXATION",
        "next_step": "Separate liquidity/limit-up removals from remaining execution_pool, sector_entry_allowed, and candidate-date eligibility failures before changing policy.",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }

    stage_fields = ["stage", "status", "count", "blocker", "interpretation", "trading_effect", "policy_effect", "policy_change_applied"]
    row_fields = [
        "code",
        "name",
        "rank_score",
        "execution_layer",
        "execution_reason",
        "opportunity_state",
        "next_action",
        "normal_recheck_bucket",
        "recommended_followup",
        "interpretation",
        "normal_lob_status",
        "normal_lob_reason",
        "normal_spread_bps",
        "normal_spread_limit_bps",
        "normal_spread_excess_bps",
        "normal_intraday_value_ratio",
        "normal_intraday_value_ratio_min",
        "normal_intraday_rechecked_v_accel",
        "normal_intraday_block_v_accel_min",
        "normal_intraday_reduce_v_accel_min",
        "trading_effect",
        "policy_effect",
        "policy_change_applied",
    ]
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, normal_candidate_rows, row_fields)
    stage_csv = LOG_DIR / "normal_entry_path_bottleneck_stage_audit_latest.csv"
    _write_csv(stage_csv, stages, stage_fields)
    print(
        f"[FINAL] normal entry path bottleneck audit -> {OUT_JSON} "
        f"bottleneck={bottleneck} normal_buckets={normal_bucket_counts}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
