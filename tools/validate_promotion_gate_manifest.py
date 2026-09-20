from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


KST = timezone.utc
ROOT = Path(r"E:\1_Data")
LOG_DIR = ROOT / "2_Logs"

MANIFEST_DRAFT = ROOT / "docs" / "exec-plans" / "active" / "20260619_promotion_gate_manifest_draft.md"
TRADING_STAGE = LOG_DIR / "trading_stage_validation_latest.json"
PAPER_PNL = LOG_DIR / "paper_pnl_summary_last.json"
LIVE_VS_BT = LOG_DIR / "live_vs_bt_feedback_latest.json"
RUNTIME_CHAIN = LOG_DIR / "runtime_chain_status_latest.json"
LIVE_CANARY = LOG_DIR / "kis_live_canary_first_latest.json"
LEDGER_LIVE_FILLS = LOG_DIR / "ledger_live_fills_dry_run_latest.json"
FILLS = ROOT / "paper" / "fills.csv"
FUTURE_SURGE_STOP_GAP_VALIDATION_GLOB = "future_surge_stop_gap_candidate_evidence_validation_*.json"

OUT_JSON = LOG_DIR / "promotion_gate_validation_latest.json"
OUT_CSV = LOG_DIR / "promotion_gate_validation_latest.csv"
HISTORY_DIR = LOG_DIR / "promotion_gate_validation_history"

STAGES = ["shadow", "small-canary", "stage1", "full-promote"]
STAGE_CAPITAL = {
    "shadow": 0,
    "small-canary": 0.5,
    "stage1": 2,
    "full-promote": 10,
}


def _now() -> datetime:
    return datetime.now().astimezone()


def _iso_now() -> str:
    return _now().replace(microsecond=0).isoformat()


def _read_json(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        with path.open("r", encoding="utf-8-sig") as f:
            data = json.load(f)
    except Exception as exc:
        return None, str(exc)
    if not isinstance(data, dict):
        return None, "json root is not an object"
    return data, None


def _latest_log_path(pattern: str) -> Path | None:
    files = sorted(LOG_DIR.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _nested(data: dict[str, Any] | None, dotted: str, default: Any = None) -> Any:
    if data is None:
        return default
    cur: Any = data
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def _string(value: Any) -> str:
    if value is None:
        return "UNKNOWN"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _boolish(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "y", "ok", "pass"}:
            return True
        if text in {"false", "0", "no", "n", "fail"}:
            return False
    return None


def _mtime_age_seconds(path: Path, now: datetime) -> float | None:
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=now.tzinfo)
    except OSError:
        return None
    return max(0.0, (now - mtime).total_seconds())


def _freshness_status(path: Path, now: datetime, max_seconds: float) -> tuple[str, str]:
    if not path.exists():
        return "UNKNOWN", "file missing"
    age = _mtime_age_seconds(path, now)
    if age is None:
        return "UNKNOWN", "mtime unavailable"
    if age <= max_seconds:
        return "PASS", f"age_seconds={age:.1f} <= {max_seconds:.1f}"
    return "UNKNOWN", f"stale age_seconds={age:.1f} > {max_seconds:.1f}"


def _check(
    checks: list[dict[str, Any]],
    *,
    name: str,
    status: str,
    required_for: list[str],
    evidence_path: Path | str,
    observed_value: Any,
    expected_value: Any,
    reason: str,
) -> None:
    checks.append(
        {
            "name": name,
            "status": status,
            "required_for": required_for,
            "evidence_path": str(evidence_path),
            "observed_value": _string(observed_value),
            "expected_value": _string(expected_value),
            "reason": reason,
        }
    )


def _read_fills_d(path: Path) -> tuple[str, str, int]:
    if not path.exists():
        return "UNKNOWN", "fills.csv missing", 0

    last_buy = ""
    last_any = ""
    rows = 0
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows += 1
                side = str(row.get("side") or "").strip().upper()
                ymd = str(row.get("ymd") or "").strip()
                if not ymd:
                    dt_text = str(row.get("datetime") or "").strip()
                    digits = "".join(ch for ch in dt_text if ch.isdigit())
                    ymd = digits[:8] if len(digits) >= 8 else ""
                if len(ymd) == 8 and ymd.isdigit():
                    if ymd > last_any:
                        last_any = ymd
                    if side == "BUY" and ymd > last_buy:
                        last_buy = ymd
    except Exception as exc:
        return "UNKNOWN", f"fills.csv unreadable: {exc}", rows

    if last_buy:
        return last_buy, "latest BUY ymd", rows
    if last_any:
        return last_any, "latest datetime ymd because BUY absent", rows
    return "UNKNOWN", "no usable ymd in fills.csv", rows


def _items_by_name(section: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if isinstance(section, list):
        for item in section:
            if isinstance(item, dict):
                name = str(item.get("name") or "")
                if name:
                    out[name] = item
    return out


def _transition_checks(trading: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    checks = _nested(trading, "transition_gate.paper_to_live.checks", [])
    return _items_by_name(checks)


def _stage_status(checks: list[dict[str, Any]], stage: str) -> str:
    relevant = [c for c in checks if stage in c.get("required_for", [])]
    if not relevant:
        return "NA"
    statuses = {str(c.get("status")) for c in relevant}
    if "FAIL" in statuses:
        return "FAIL"
    if "UNKNOWN" in statuses:
        return "UNKNOWN"
    if statuses == {"NA"}:
        return "NA"
    if all(status == "PASS" for status in statuses):
        return "PASS"
    return "UNKNOWN"


def _stage_reason(stage: str, status: str, checks: list[dict[str, Any]]) -> str:
    if status == "PASS":
        return "all required checks passed"
    if status == "NA":
        return "stage is not implemented in current RootA gate"
    blockers = [
        f"{c['name']}={c['status']}"
        for c in checks
        if stage in c.get("required_for", []) and c.get("status") in {"FAIL", "UNKNOWN"}
    ]
    return "; ".join(blockers[:8]) if blockers else "not approved"


def build_result(ci_fail_on_block: bool = False) -> dict[str, Any]:
    now = _now()
    checks: list[dict[str, Any]] = []
    untested: list[str] = []

    trading, trading_err = _read_json(TRADING_STAGE)
    paper_pnl, paper_err = _read_json(PAPER_PNL)
    live_vs_bt, feedback_err = _read_json(LIVE_VS_BT)
    runtime_chain, runtime_err = _read_json(RUNTIME_CHAIN)
    live_canary, canary_err = _read_json(LIVE_CANARY)
    ledger_live_fills, ledger_err = _read_json(LEDGER_LIVE_FILLS)
    future_surge_stop_gap_path = _latest_log_path(FUTURE_SURGE_STOP_GAP_VALIDATION_GLOB)
    if future_surge_stop_gap_path is None:
        future_surge_stop_gap, future_surge_stop_gap_err = None, f"no file matching {FUTURE_SURGE_STOP_GAP_VALIDATION_GLOB}"
        future_surge_stop_gap_evidence_path: Path | str = LOG_DIR / FUTURE_SURGE_STOP_GAP_VALIDATION_GLOB
    else:
        future_surge_stop_gap, future_surge_stop_gap_err = _read_json(future_surge_stop_gap_path)
        future_surge_stop_gap_evidence_path = future_surge_stop_gap_path

    for name, path, data, err, stages in [
        ("manifest_draft_exists", MANIFEST_DRAFT, {}, None if MANIFEST_DRAFT.exists() else "missing", STAGES),
        ("trading_stage_json_readable", TRADING_STAGE, trading, trading_err, STAGES),
        ("paper_pnl_json_readable", PAPER_PNL, paper_pnl, paper_err, STAGES),
        ("live_vs_bt_json_readable", LIVE_VS_BT, live_vs_bt, feedback_err, STAGES),
        ("runtime_chain_json_readable", RUNTIME_CHAIN, runtime_chain, runtime_err, STAGES),
        ("live_canary_json_readable", LIVE_CANARY, live_canary, canary_err, ["small-canary", "stage1", "full-promote"]),
        ("ledger_live_fills_json_readable", LEDGER_LIVE_FILLS, ledger_live_fills, ledger_err, ["small-canary", "stage1", "full-promote"]),
        (
            "future_surge_stop_gap_evidence_json_readable",
            Path(str(future_surge_stop_gap_evidence_path)),
            future_surge_stop_gap,
            future_surge_stop_gap_err,
            ["small-canary", "stage1", "full-promote"],
        ),
    ]:
        status = "PASS" if err is None and (data is not None or path == MANIFEST_DRAFT) else "UNKNOWN"
        _check(
            checks,
            name=name,
            status=status,
            required_for=stages,
            evidence_path=path,
            observed_value="readable" if status == "PASS" else err,
            expected_value="readable",
            reason="required evidence readable" if status == "PASS" else "required evidence missing or unreadable",
        )

    for name, path, max_seconds, stages in [
        ("trading_stage_freshness", TRADING_STAGE, 86400.0, STAGES),
        ("paper_pnl_freshness", PAPER_PNL, 86400.0, STAGES),
        ("live_vs_bt_freshness", LIVE_VS_BT, 86400.0, STAGES),
        ("runtime_chain_freshness", RUNTIME_CHAIN, 10800.0, STAGES),
        ("live_canary_freshness", LIVE_CANARY, 86400.0, ["small-canary", "stage1", "full-promote"]),
        ("ledger_live_fills_freshness", LEDGER_LIVE_FILLS, 86400.0, ["small-canary", "stage1", "full-promote"]),
        (
            "future_surge_stop_gap_evidence_freshness",
            Path(str(future_surge_stop_gap_evidence_path)),
            86400.0,
            ["small-canary", "stage1", "full-promote"],
        ),
    ]:
        status, reason = _freshness_status(path, now, max_seconds)
        _check(
            checks,
            name=name,
            status=status,
            required_for=stages,
            evidence_path=path,
            observed_value=reason,
            expected_value=f"age_seconds <= {max_seconds:.1f}",
            reason=reason,
        )

    d_rule_ymd, d_reason, fills_rows = _read_fills_d(FILLS)
    _check(
        checks,
        name="d_rule_ymd",
        status="PASS" if d_rule_ymd != "UNKNOWN" else "UNKNOWN",
        required_for=STAGES,
        evidence_path=FILLS,
        observed_value=d_rule_ymd,
        expected_value="latest BUY ymd or latest datetime ymd",
        reason=f"{d_reason}; rows={fills_rows}",
    )

    orders_path = ROOT / "paper" / f"orders_{d_rule_ymd}_exec.xlsx" if d_rule_ymd != "UNKNOWN" else ROOT / "paper" / "orders_UNKNOWN_exec.xlsx"
    _check(
        checks,
        name="orders_exec_for_d_exists",
        status="PASS" if d_rule_ymd != "UNKNOWN" and orders_path.exists() else "UNKNOWN",
        required_for=["small-canary", "stage1", "full-promote"],
        evidence_path=orders_path,
        observed_value=orders_path.exists(),
        expected_value=True,
        reason="orders_exec exists for D" if orders_path.exists() else "orders_exec missing or D unknown",
    )

    paper_run_id = _nested(paper_pnl, "run_id", "UNKNOWN")
    feedback_run_id = _nested(live_vs_bt, "run_id", "UNKNOWN")
    _check(
        checks,
        name="run_id_match_paper_vs_bt",
        status="PASS" if paper_run_id != "UNKNOWN" and paper_run_id == feedback_run_id else "FAIL",
        required_for=STAGES,
        evidence_path=f"{PAPER_PNL}; {LIVE_VS_BT}",
        observed_value=f"paper={paper_run_id}; feedback={feedback_run_id}",
        expected_value="matching non-empty run_id",
        reason="run_id matches" if paper_run_id == feedback_run_id and paper_run_id != "UNKNOWN" else "run_id mismatch or missing",
    )

    paper_as_of = _nested(paper_pnl, "as_of", _nested(paper_pnl, "as_of_ymd", "UNKNOWN"))
    feedback_as_of = _nested(live_vs_bt, "as_of", "UNKNOWN")
    _check(
        checks,
        name="as_of_match_paper_vs_bt",
        status="PASS" if paper_as_of != "UNKNOWN" and paper_as_of == feedback_as_of else "FAIL",
        required_for=STAGES,
        evidence_path=f"{PAPER_PNL}; {LIVE_VS_BT}",
        observed_value=f"paper={paper_as_of}; feedback={feedback_as_of}",
        expected_value="matching non-empty as_of",
        reason="as_of matches" if paper_as_of == feedback_as_of and paper_as_of != "UNKNOWN" else "as_of mismatch or missing",
    )

    transition_status = _nested(trading, "transition_gate.paper_to_live.status", "UNKNOWN")
    _check(
        checks,
        name="transition_gate_ready",
        status="PASS" if transition_status == "READY" else "FAIL",
        required_for=["small-canary", "stage1", "full-promote"],
        evidence_path=TRADING_STAGE,
        observed_value=transition_status,
        expected_value="READY",
        reason="paper_to_live ready" if transition_status == "READY" else "paper_to_live is not READY",
    )

    paper_judgment = _nested(trading, "paper.judgment", "UNKNOWN")
    _check(
        checks,
        name="paper_stage_not_failed",
        status="FAIL" if str(paper_judgment).upper() in {"FAIL", "FAILED", "NO_GO", "BLOCK"} else ("UNKNOWN" if paper_judgment == "UNKNOWN" else "PASS"),
        required_for=STAGES,
        evidence_path=TRADING_STAGE,
        observed_value=paper_judgment,
        expected_value="not failed",
        reason="paper stage is not failed" if paper_judgment != "UNKNOWN" else "paper judgment missing",
    )

    transition_by_name = _transition_checks(trading)
    for gate_name, required_for in [
        ("paper_quality_gate", ["small-canary", "stage1", "full-promote"]),
        ("live_canary_gate", ["small-canary", "stage1", "full-promote"]),
        ("live_preflight_health", ["small-canary", "stage1", "full-promote"]),
        ("canary_execute_mode", ["small-canary", "stage1", "full-promote"]),
    ]:
        gate = transition_by_name.get(gate_name)
        status_value = str(gate.get("status")) if gate else "UNKNOWN"
        _check(
            checks,
            name=gate_name,
            status="PASS" if status_value == "PASS" else ("UNKNOWN" if status_value == "UNKNOWN" else "FAIL"),
            required_for=required_for,
            evidence_path=TRADING_STAGE,
            observed_value=status_value,
            expected_value="PASS",
            reason=f"{gate_name} status from transition checks",
        )

    runtime_overall = _nested(runtime_chain, "overall", "UNKNOWN")
    _check(
        checks,
        name="runtime_chain_not_fail_for_shadow",
        status="FAIL" if runtime_overall == "FAIL" else ("UNKNOWN" if runtime_overall == "UNKNOWN" else "PASS"),
        required_for=["shadow"],
        evidence_path=RUNTIME_CHAIN,
        observed_value=runtime_overall,
        expected_value="not FAIL",
        reason="runtime chain is not FAIL" if runtime_overall != "FAIL" else "runtime chain is FAIL",
    )
    _check(
        checks,
        name="runtime_chain_ok",
        status="PASS" if runtime_overall == "OK" else ("UNKNOWN" if runtime_overall == "UNKNOWN" else "FAIL"),
        required_for=["small-canary", "stage1", "full-promote"],
        evidence_path=RUNTIME_CHAIN,
        observed_value=runtime_overall,
        expected_value="OK",
        reason="runtime chain OK" if runtime_overall == "OK" else "runtime chain is not OK",
    )

    canary_ok = _boolish(_nested(live_canary, "ok", None))
    canary_execute = _boolish(_nested(live_canary, "execute", None))
    for name, observed in [("live_canary_ok_true", canary_ok), ("live_canary_execute_true", canary_execute)]:
        _check(
            checks,
            name=name,
            status="PASS" if observed is True else ("UNKNOWN" if observed is None else "FAIL"),
            required_for=["small-canary", "stage1", "full-promote"],
            evidence_path=LIVE_CANARY,
            observed_value=observed,
            expected_value=True,
            reason=f"{name} from live canary artifact",
        )

    ledger_status = _nested(ledger_live_fills, "status", "UNKNOWN")
    missing_rows = _nested(ledger_live_fills, "missing_rows", "UNKNOWN")
    _check(
        checks,
        name="ledger_live_fills_status_pass",
        status="PASS" if ledger_status == "PASS" else ("UNKNOWN" if ledger_status == "UNKNOWN" else "FAIL"),
        required_for=["small-canary", "stage1", "full-promote"],
        evidence_path=LEDGER_LIVE_FILLS,
        observed_value=ledger_status,
        expected_value="PASS",
        reason="ledger/live fills dry-run status",
    )
    _check(
        checks,
        name="ledger_live_fills_missing_rows_zero",
        status="PASS" if missing_rows == 0 else ("UNKNOWN" if missing_rows == "UNKNOWN" else "FAIL"),
        required_for=["small-canary", "stage1", "full-promote"],
        evidence_path=LEDGER_LIVE_FILLS,
        observed_value=missing_rows,
        expected_value=0,
        reason="ledger/live fills missing row count",
    )

    future_status = _nested(future_surge_stop_gap, "overall_status", "UNKNOWN")
    future_ready_count = _nested(future_surge_stop_gap, "ready_candidate_count", "UNKNOWN")
    future_promotion_allowed = _boolish(_nested(future_surge_stop_gap, "promotion_allowed", None))
    _check(
        checks,
        name="future_surge_stop_gap_evidence_overall_pass",
        status="PASS" if future_status == "PASS" else ("UNKNOWN" if future_status == "UNKNOWN" else "FAIL"),
        required_for=["small-canary", "stage1", "full-promote"],
        evidence_path=future_surge_stop_gap_evidence_path,
        observed_value=future_status,
        expected_value="PASS",
        reason="future surge STOP_GAP evidence validation must pass before canary-or-above promotion",
    )
    _check(
        checks,
        name="future_surge_stop_gap_ready_candidate_count_positive",
        status=(
            "PASS"
            if isinstance(future_ready_count, int) and future_ready_count > 0
            else ("FAIL" if isinstance(future_ready_count, int) else "UNKNOWN")
        ),
        required_for=["small-canary", "stage1", "full-promote"],
        evidence_path=future_surge_stop_gap_evidence_path,
        observed_value=future_ready_count,
        expected_value="> 0",
        reason="at least one non-routing evidence row must be ready for the promotion gate",
    )
    _check(
        checks,
        name="future_surge_stop_gap_promotion_allowed_true",
        status="PASS" if future_promotion_allowed is True else ("FAIL" if future_promotion_allowed is False else "UNKNOWN"),
        required_for=["small-canary", "stage1", "full-promote"],
        evidence_path=future_surge_stop_gap_evidence_path,
        observed_value=future_promotion_allowed,
        expected_value=True,
        reason="promotion_allowed must be true in the future surge STOP_GAP evidence validator",
    )

    non_routing_expected = {
        "trading_effect": False,
        "policy_change": False,
        "entry_approval_changed": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "trading_allowed": False,
        "must_not_dispatch": True,
    }
    non_routing_observed = {key: _nested(future_surge_stop_gap, key, "UNKNOWN") for key in non_routing_expected}
    non_routing_status = "UNKNOWN" if future_surge_stop_gap is None else "PASS"
    for key, expected in non_routing_expected.items():
        if non_routing_observed[key] != expected:
            non_routing_status = "FAIL"
            break
    _check(
        checks,
        name="future_surge_stop_gap_non_routing_controls",
        status=non_routing_status,
        required_for=["small-canary", "stage1", "full-promote"],
        evidence_path=future_surge_stop_gap_evidence_path,
        observed_value=non_routing_observed,
        expected_value=non_routing_expected,
        reason="future surge STOP_GAP evidence must remain read-only and non-routing",
    )

    for name, stage in [
        ("stage1_enforceable_capital_config", "stage1"),
        ("stage1_ci_enforcement", "stage1"),
        ("full_promote_enforceable_capital_config", "full-promote"),
        ("full_promote_ci_tag_gate", "full-promote"),
        ("full_promote_deploy_annotation_gate", "full-promote"),
    ]:
        _check(
            checks,
            name=name,
            status="FAIL",
            required_for=[stage],
            evidence_path="not confirmed",
            observed_value="not implemented",
            expected_value="implemented and enforceable",
            reason="not implemented in current RootA promotion gate",
        )

    untested.extend(
        [
            "ENBPI/MSPRT dedicated promotion checks",
            "signed bundle verification in CI",
            "promotion tag creation",
            "deploy annotation patching",
            "dashboard gate status card",
        ]
    )

    stage_results = []
    for stage in STAGES:
        status = _stage_status(checks, stage)
        if stage == "shadow" and status == "FAIL":
            allowed_stage = "none"
        stage_results.append(
            {
                "stage": stage,
                "status": status,
                "capital_pct": STAGE_CAPITAL[stage],
                "reason": _stage_reason(stage, status, checks),
                "checks": [c["name"] for c in checks if stage in c.get("required_for", [])],
            }
        )

    shadow_status = next((r["status"] for r in stage_results if r["stage"] == "shadow"), "UNKNOWN")
    allowed_stage = "shadow" if shadow_status == "PASS" else "none"
    blocked_stages = [r["stage"] for r in stage_results if r["stage"] != allowed_stage and r["status"] != "PASS"]
    overall_status = "PASS" if allowed_stage == "full-promote" else "FAIL"

    result = {
        "generated_at": _iso_now(),
        "schema_version": "promotion_gate_validation_v1",
        "trading_effect": False,
        "policy_effect": False,
        "order_path_effect": False,
        "score_effect": False,
        "ci_fail_on_block": bool(ci_fail_on_block),
        "overall_status": overall_status,
        "allowed_stage": allowed_stage,
        "blocked_stages": blocked_stages,
        "facts": {
            "d_rule_ymd": d_rule_ymd,
            "d_rule_reason": d_reason,
            "orders_exec_path": str(orders_path),
            "run_id": paper_run_id if paper_run_id == feedback_run_id else "UNKNOWN",
            "as_of": paper_as_of if paper_as_of == feedback_as_of else "UNKNOWN",
            "transition_status": transition_status,
            "runtime_chain_overall": runtime_overall,
            "paper_judgment": paper_judgment,
            "future_surge_stop_gap_evidence_path": str(future_surge_stop_gap_evidence_path),
            "future_surge_stop_gap_evidence_status": future_status,
            "future_surge_stop_gap_ready_candidate_count": future_ready_count,
            "future_surge_stop_gap_promotion_allowed": future_promotion_allowed,
        },
        "stage_results": stage_results,
        "checks": checks,
        "untested": untested,
        "next_action": "fix current blockers before any canary-or-above promotion validation",
    }
    return result


def _write_outputs(result: dict[str, Any]) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    fields = ["stage", "check", "status", "evidence_path", "observed_value", "expected_value", "reason"]
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for check in result["checks"]:
            for stage in check.get("required_for", []):
                writer.writerow(
                    {
                        "stage": stage,
                        "check": check.get("name", ""),
                        "status": check.get("status", ""),
                        "evidence_path": check.get("evidence_path", ""),
                        "observed_value": check.get("observed_value", ""),
                        "expected_value": check.get("expected_value", ""),
                        "reason": check.get("reason", ""),
                    }
                )

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    history_path = HISTORY_DIR / f"promotion_gate_validation_{stamp}.json"
    history_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return history_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate RootA promotion gate evidence in read-only mode.")
    parser.add_argument(
        "--ci-fail-on-block",
        action="store_true",
        help="return exit code 1 when the validation result is not PASS",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="print result summary without writing latest JSON/CSV outputs",
    )
    args = parser.parse_args()

    result = build_result(ci_fail_on_block=args.ci_fail_on_block)
    history_path = None
    if not args.no_write:
        history_path = _write_outputs(result)

    print(
        "[PROMOTION_GATE] "
        f"overall={result['overall_status']} allowed_stage={result['allowed_stage']} "
        f"blocked={','.join(result['blocked_stages'])} json={OUT_JSON if not args.no_write else 'NO_WRITE'}"
    )
    if history_path is not None:
        print(f"[PROMOTION_GATE] history={history_path}")

    if args.ci_fail_on_block and result["overall_status"] != "PASS":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
