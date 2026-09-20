from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"
PAPER = ROOT / "paper"
PAPER_CONFIG = PAPER / "paper_engine_config.json"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as exc:
        return {"_read_error": f"json_decode_error:{exc}"}


def _ws_status_path() -> Path:
    # kis_realtime_ws.py writes kis_ws_status_latest{_worker}.json, and
    # kis_ws_multiplexer.py always assigns a worker id, so the unsuffixed
    # name stops being updated. Pick whichever worker file is newest.
    files = [p for p in LOGS.glob("kis_ws_status_latest*.json") if p.is_file()]
    if not files:
        return LOGS / "kis_ws_status_latest.json"
    return max(files, key=lambda p: p.stat().st_mtime)


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        return []


def _as_float(value: Any) -> float | None:
    try:
        if value in ("", None):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _artifact_newer_than_loop(path: Path, loop_ts: Any) -> bool:
    if not path.exists():
        return False
    try:
        loop_dt = datetime.fromisoformat(str(loop_ts).replace("Z", "+00:00"))
        if loop_dt.tzinfo is not None:
            loop_dt = loop_dt.replace(tzinfo=None)
        artifact_dt = datetime.fromtimestamp(path.stat().st_mtime)
        return artifact_dt >= loop_dt
    except Exception:
        return False


def _fmt_pct(value: Any) -> str:
    num = _as_float(value)
    if num is None:
        return "NA"
    return f"{num * 100:.1f}%"


def _file_meta(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "size": 0, "mtime": ""}
    stat = path.stat()
    return {
        "path": str(path),
        "exists": True,
        "size": int(stat.st_size),
        "mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
    }


def _csv_count(path: Path) -> int:
    return len(_read_csv_rows(path))


def _top_codes_from_rows(rows: list[dict[str, str]], limit: int = 3) -> list[str]:
    ranked = []
    for row in rows:
        code = (row.get("code") or "").strip()
        score = _as_float(row.get("rank_score"))
        if code and score is not None:
            ranked.append((score, code))
    ranked.sort(reverse=True)
    return [code for _, code in ranked[:limit]]


def _top_signal() -> dict[str, Any]:
    rows = _read_csv_rows(LOGS / "entry_signal_snapshot_latest.csv")
    gate = _read_json(LOGS / "p1_entry_gate_status_latest.json")
    risk_runtime = gate.get("risk_gate_runtime") or {}
    kill_switch = risk_runtime.get("kill_switch") or {}
    top_codes = _top_codes_from_rows(rows)
    scores = []
    for row in rows:
        if row.get("code") in top_codes:
            scores.append(
                {
                    "code": row.get("code"),
                    "score": _as_float(row.get("rank_score")),
                    "decision": row.get("entry_gate_decision") or row.get("signal") or "unknown",
                    "reason": row.get("final_decision_reason") or row.get("reason") or "",
                }
            )
    entry_gate = str(gate.get("entry_gate_decision_before_p1") or "").upper()
    entry_allowed = (
        risk_runtime.get("risk_off_enabled") is False
        and (kill_switch.get("triggered") is False or kill_switch.get("triggered") is None)
        and entry_gate in {"PASS", "ALLOW", "ALLOWED", "BUY"}
    )
    return {
        "source": str(LOGS / "entry_signal_snapshot_latest.csv"),
        "top_codes": top_codes,
        "scores": scores,
        "market_regime": gate.get("market_regime") or "unknown",
        "entry_gate": entry_gate or "unknown",
        "risk_off": risk_runtime.get("risk_off_enabled", "unknown"),
        "kill_switch_triggered": (kill_switch.get("triggered") if kill_switch else "unknown"),
        "entry_allowed_observed": entry_allowed,
    }


def _fills_status() -> dict[str, Any]:
    report = _read_json(LOGS / "paper_order_validation_report_latest.json")
    lifecycle = report.get("lifecycle") or {}
    counts = lifecycle.get("counts") or {}
    rows = lifecycle.get("rows") or []
    filled = int(counts.get("buy_fill_rows") or 0) + int(counts.get("sell_fill_rows") or 0)
    unfilled = 0
    for row in rows:
        state = str(row.get("state") or "")
        if not state.startswith("FILLED"):
            unfilled += 1
    loop = _read_json(LOGS / "intraday_loop_status_latest.json")
    ws = _read_json(_ws_status_path())
    anomalies = []
    advisories = []
    for step in loop.get("steps") or []:
        if step.get("returncode") not in (0, None) and not step.get("skipped"):
            label = step.get("label") or "unknown_step"
            rc = step.get("returncode")
            if step.get("ok") is True or step.get("advisory_only"):
                reason = step.get("fallback_reason") or step.get("fallback_used") or "non_blocking"
                if (
                    label == "price_snapshot"
                    and reason == "price_snapshot_failed"
                    and _artifact_newer_than_loop(LOGS / "intraday_prices_latest.csv", loop.get("ts"))
                ):
                    continue
                advisories.append(f"{label}:rc={rc}:{reason}")
            else:
                anomalies.append(f"{label}:rc={rc}")
    reconnect = ws.get("reconnect")
    if isinstance(reconnect, int) and reconnect > 0:
        if str(ws.get("status") or "").upper() == "STREAMING" and ws.get("last_market_data_at"):
            advisories.append(f"KIS_WS reconnect={reconnect}:streaming")
        else:
            anomalies.append(f"KIS_WS reconnect={reconnect}")
    latency = _latency_status()
    return {
        "source": str(LOGS / "paper_order_validation_report_latest.json"),
        "fills": filled,
        "unfilled": unfilled,
        "avg_fill_latency_ms": latency["latency_ms"],
        "latency_delta_ms": latency["latency_delta_ms"],
        "latency_metric": latency["metric"],
        "latency_source": latency["source"],
        "latency_status": latency["status"],
        "anomalies": anomalies[:5],
        "advisories": advisories[:5],
    }


def _latency_status() -> dict[str, Any]:
    heatmap = _read_json(LOGS / f"latency_heatmap_{datetime.now().strftime('%Y%m%d')}.json")
    if not heatmap:
        drift = _read_json(LOGS / "drift_monitor_latest.json")
        latency_check = (drift.get("checks") or {}).get("latency_heatmap") or {}
        heatmap = latency_check if isinstance(latency_check, dict) else {}
    rows = list(heatmap.get("rows") or [])
    usable = []
    for row in rows:
        p95 = _as_float(row.get("p95_latency_ms"))
        hour = _as_float(row.get("hour"))
        if p95 is not None:
            usable.append((hour if hour is not None else -1.0, p95, row))
    if not usable:
        return {
            "latency_ms": "NA",
            "latency_delta_ms": "NA",
            "metric": "p95_latency_ms",
            "source": str(LOGS / f"latency_heatmap_{datetime.now().strftime('%Y%m%d')}.json"),
            "status": heatmap.get("status") or "NA",
        }
    usable.sort(key=lambda item: item[0])
    _, p95, row = usable[-1]
    mean_7d = _as_float(row.get("mean_7d"))
    delta = "NA" if mean_7d is None else round(float(p95) - float(mean_7d), 2)
    return {
        "latency_ms": round(float(p95), 2),
        "latency_delta_ms": delta,
        "metric": "p95_latency_ms",
        "source": str(LOGS / f"latency_heatmap_{datetime.now().strftime('%Y%m%d')}.json"),
        "status": heatmap.get("status") or "unknown",
    }


def _exposure() -> dict[str, Any]:
    state = _read_json(PAPER / "paper_state.json")
    cfg = _read_json(PAPER_CONFIG)
    capital_total = _as_float(cfg.get("capital_total")) or 0.0
    cap_policy = cfg.get("capital_budget_policy") if isinstance(cfg.get("capital_budget_policy"), dict) else {}
    gross_limit_pct = _as_float(cap_policy.get("gross_exposure_pct")) or _as_float(cfg.get("max_gross_exposure_pct")) or 0.0
    positions = state.get("open_positions") or []
    sector_notional: dict[str, float] = {}
    total = 0.0
    max_single = 0.0
    for pos in positions:
        qty = _as_float(pos.get("qty")) or 0.0
        price = _as_float(pos.get("entry_price")) or 0.0
        notional = qty * price
        sector = str(pos.get("sector") or "unknown").strip()
        if not sector or sector.lower() == "nan":
            sector = "unknown"
        sector_notional[sector] = sector_notional.get(sector, 0.0) + notional
        total += notional
        max_single = max(max_single, notional)
    sector_pct = {
        sector: (notional / capital_total if capital_total else 0.0)
        for sector, notional in sorted(sector_notional.items(), key=lambda item: item[1], reverse=True)
    }
    total_exposure_pct = total / capital_total if capital_total else 0.0
    max_single_pct = max_single / capital_total if capital_total else 0.0
    return {
        "source": str(PAPER / "paper_state.json"),
        "config_source": str(PAPER_CONFIG),
        "open_positions": len(positions),
        "capital_total": capital_total,
        "total_entry_notional": total,
        "total_exposure_pct": total_exposure_pct,
        "sector_pct": sector_pct,
        "max_sector_pct": max(sector_pct.values()) if sector_pct else 0.0,
        "max_single_pct": max_single_pct,
        "gross_limit_pct": gross_limit_pct,
        "sector_limit_pct": 0.40,
        "single_limit_pct": 0.08,
        "limit_check_observed": (
            (total_exposure_pct <= gross_limit_pct if gross_limit_pct > 0 else True)
            and ((max(sector_pct.values()) if sector_pct else 0.0) <= 0.40)
            and (max_single_pct <= 0.08)
        ),
    }


def _spikes() -> dict[str, Any]:
    surge = _read_json(LOGS / "surge_realtime_latest.json")
    alerts = surge.get("alerts") or []
    ranked = sorted(alerts, key=lambda row: _as_float(row.get("surge_score_final")) or -1.0, reverse=True)
    top = []
    for row in ranked[:3]:
        top.append(
            {
                "code": row.get("code"),
                "score": row.get("surge_score_final"),
                "change_pct": row.get("change_pct"),
                "rvol20": row.get("rvol20"),
                "news_score": row.get("news_score"),
                "exclude_reasons": row.get("exclude_reasons") or "",
            }
        )
    return {
        "source": str(LOGS / "surge_realtime_latest.json"),
        "status": surge.get("status") or "unknown",
        "alerts_count": surge.get("alerts_count", len(alerts)),
        "top": top,
        "thresholds": {
            "pct_min": (surge.get("thresholds") or {}).get("pct_min"),
            "rvol20_min": (surge.get("thresholds") or {}).get("rvol20_min"),
        },
    }


def _surge_readiness() -> dict[str, Any]:
    universe_path = PAPER / "surge_universe.csv"
    params_path = PAPER / "surge_params.json"
    proposal_path = LOGS / "surge_param_proposal_latest.json"
    realtime_path = LOGS / "surge_realtime_latest.json"
    ml_path = LOGS / "surge_ml_score_latest.json"
    lob_path = LOGS / "surge_lob_latest.json"
    readiness_path = LOGS / "surge_ev_probe_readiness_latest.json"
    staged_path = LOGS / "surge_ev_paper_probe_staged_latest.json"
    consumer_path = LOGS / "surge_ev_paper_probe_consumer_latest.json"
    freshness_audit_path = LOGS / "surge_freshness_audit_latest.json"
    state_machine_path = LOGS / "surge_state_machine_shadow_latest.json"
    preopen_readiness_path = LOGS / "surge_preopen_precursor_readiness_latest.json"

    universe_rows = _csv_count(universe_path)
    realtime = _read_json(realtime_path)
    ml_score = _read_json(ml_path)
    lob = _read_json(lob_path)
    readiness = _read_json(readiness_path)
    staged = _read_json(staged_path)
    consumer = _read_json(consumer_path)
    freshness_audit = _read_json(freshness_audit_path)
    state_machine = _read_json(state_machine_path)
    preopen_readiness = _read_json(preopen_readiness_path)

    staged_summary = staged.get("summary") or {}
    consumer_summary = consumer.get("summary") or {}
    state_summary = state_machine.get("summary") or {}
    state_contract = state_machine.get("risk_contract") or {}
    preopen_summary = preopen_readiness.get("summary") or {}
    preopen_calendar = preopen_readiness.get("calendar") or {}
    preopen_warnings = preopen_readiness.get("warnings") or {}
    transition_outcome = preopen_readiness.get("transition_outcome_readiness") or {}
    route_zero = (
        int(staged_summary.get("dispatch_enabled_rows") or 0) == 0
        and int(staged_summary.get("broker_order_route_rows") or 0) == 0
        and int(staged_summary.get("trading_allowed_rows") or 0) == 0
        and int(consumer_summary.get("orders_exec_write_rows") or 0) == 0
        and int(consumer_summary.get("fills_write_rows") or 0) == 0
        and int(consumer_summary.get("broker_order_route_rows") or 0) == 0
        and int(consumer_summary.get("dispatch_enabled_rows") or 0) == 0
        and int(consumer_summary.get("trading_allowed_rows") or 0) == 0
    )
    consumer_off = consumer.get("enabled") is False or consumer.get("enabled") in (None, "", "false", "False")

    checks = []
    checks.append({
        "name": "surge_universe",
        "status": "PASS" if universe_rows > 0 else "WARN",
        "rows": universe_rows,
        "source": str(universe_path),
    })
    checks.append({
        "name": "surge_params",
        "status": "PASS" if params_path.exists() else "WARN",
        "source": str(params_path),
        "proposal_source": str(proposal_path),
        "proposal_exists": proposal_path.exists(),
    })
    checks.append({
        "name": "surge_realtime_reference",
        "status": "INFO" if realtime_path.exists() else "WARN",
        "artifact_ts": realtime.get("ts") or realtime.get("generated_at") or "",
        "alerts_count": realtime.get("alerts_count", 0),
        "note": "preopen reference only; realtime surge signal is intraday-driven",
        "source": str(realtime_path),
    })
    checks.append({
        "name": "surge_ml_score_reference",
        "status": "INFO" if ml_path.exists() else "WARN",
        "artifact_ts": ml_score.get("ts") or ml_score.get("generated_at") or "",
        "rows": ml_score.get("rows", 0),
        "source": str(ml_path),
    })
    checks.append({
        "name": "surge_lob_reference",
        "status": "INFO" if lob_path.exists() else "WARN",
        "artifact_ts": lob.get("ts") or lob.get("generated_at") or "",
        "rows": lob.get("rows", 0),
        "note": "preopen reference only; LOB quality must be verified intraday",
        "source": str(lob_path),
    })
    checks.append({
        "name": "ev_probe_readiness",
        "status": "PASS" if readiness.get("status") == "OK" else "WARN",
        "ready_rows": (readiness.get("summary") or {}).get("ready_rows", 0),
        "paper_order_route": (readiness.get("summary") or {}).get("paper_order_route"),
        "broker_order_route": (readiness.get("summary") or {}).get("broker_order_route"),
        "trading_allowed": (readiness.get("summary") or {}).get("trading_allowed"),
        "source": str(readiness_path),
    })
    checks.append({
        "name": "ev_probe_staged_routes",
        "status": "PASS" if route_zero else "WARN",
        "staged_rows": staged_summary.get("rows", 0),
        "dispatch_enabled_rows": staged_summary.get("dispatch_enabled_rows", 0),
        "broker_order_route_rows": staged_summary.get("broker_order_route_rows", 0),
        "trading_allowed_rows": staged_summary.get("trading_allowed_rows", 0),
        "consumer_enabled": consumer.get("enabled"),
        "consumer_blocked_rows": consumer_summary.get("blocked_rows", 0),
        "source": str(staged_path),
        "consumer_source": str(consumer_path),
    })
    checks.append({
        "name": "surge_freshness_audit",
        "status": freshness_audit.get("status") or ("WARN" if not freshness_audit_path.exists() else "INFO"),
        "warnings": freshness_audit.get("warnings", []),
        "source": str(freshness_audit_path),
    })
    checks.append({
        "name": "surge_state_machine_shadow",
        "status": "PASS" if state_machine.get("status") == "OK" and state_contract.get("must_not_dispatch") is True else "WARN",
        "rows": state_summary.get("rows", 0),
        "state_counts": state_summary.get("state_counts", {}),
        "ohlcv_context_rows": state_summary.get("ohlcv_context_rows", 0),
        "must_not_dispatch": state_contract.get("must_not_dispatch"),
        "trading_allowed": state_contract.get("trading_allowed"),
        "source": str(state_machine_path),
    })
    checks.append({
        "name": "surge_preopen_precursor_readiness",
        "status": preopen_readiness.get("status") or ("WARN" if not preopen_readiness_path.exists() else "INFO"),
        "preopen_ready_items": preopen_summary.get("preopen_ready_items", 0),
        "total_items": preopen_summary.get("total_items", 0),
        "preopen_ready_ratio": preopen_summary.get("preopen_ready_ratio", 0.0),
        "preopen_quality_score": preopen_summary.get("preopen_quality_score", 0.0),
        "preopen_source_not_implemented_items": preopen_summary.get("preopen_source_not_implemented_items", 0),
        "intraday_required_items": preopen_summary.get("intraday_required_items", 0),
        "external_or_missing_items": preopen_summary.get("external_or_missing_items", 0),
        "transition_outcome_status": transition_outcome.get("status"),
        "state_transition_history_ready": transition_outcome.get("state_transition_history_ready"),
        "outcome_markout_ready": transition_outcome.get("outcome_markout_ready"),
        "is_weekend": preopen_calendar.get("is_weekend"),
        "intraday_validation_available": preopen_calendar.get("intraday_validation_available"),
        "source": str(preopen_readiness_path),
    })

    hard_warns = [c["name"] for c in checks if c["status"] == "WARN"]
    status = "PASS" if not hard_warns and consumer_off and route_zero else "WARN"
    if not consumer_off:
        hard_warns.append("consumer_enabled")
        status = "WARN"
    if not route_zero and "route_nonzero" not in hard_warns:
        hard_warns.append("route_nonzero")
        status = "WARN"

    return {
        "status": status,
        "mode": "preopen_read_only_observation",
        "warnings": hard_warns,
        "universe_rows": universe_rows,
        "ready_rows": (readiness.get("summary") or {}).get("ready_rows", 0),
        "staged_rows": staged_summary.get("rows", 0),
        "state_machine_rows": state_summary.get("rows", 0),
        "state_machine_counts": state_summary.get("state_counts", {}),
        "state_machine_must_not_dispatch": state_contract.get("must_not_dispatch"),
        "preopen_ready_items": preopen_summary.get("preopen_ready_items", 0),
        "preopen_total_items": preopen_summary.get("total_items", 0),
        "preopen_ready_ratio": preopen_summary.get("preopen_ready_ratio", 0.0),
        "preopen_quality_score": preopen_summary.get("preopen_quality_score", 0.0),
        "preopen_source_not_implemented_items": preopen_summary.get("preopen_source_not_implemented_items", 0),
        "preopen_intraday_required_items": preopen_summary.get("intraday_required_items", 0),
        "preopen_external_or_missing_items": preopen_summary.get("external_or_missing_items", 0),
        "preopen_warning_top": preopen_warnings.get("preopen_source_not_implemented_top", []),
        "preopen_intraday_required_top": preopen_warnings.get("intraday_required_top", []),
        "preopen_external_or_missing_top": preopen_warnings.get("external_or_missing_top", []),
        "preopen_intraday_validation_available": preopen_calendar.get("intraday_validation_available"),
        "preopen_calendar_note": preopen_calendar.get("note", ""),
        "transition_outcome_status": transition_outcome.get("status"),
        "state_transition_history_ready": transition_outcome.get("state_transition_history_ready"),
        "outcome_markout_ready": transition_outcome.get("outcome_markout_ready"),
        "consumer_enabled": consumer.get("enabled"),
        "route_zero_observed": route_zero,
        "files": {
            "surge_universe": _file_meta(universe_path),
            "surge_params": _file_meta(params_path),
            "surge_param_proposal": _file_meta(proposal_path),
            "surge_realtime": _file_meta(realtime_path),
            "surge_ml_score": _file_meta(ml_path),
            "surge_lob": _file_meta(lob_path),
            "ev_readiness": _file_meta(readiness_path),
            "ev_staged": _file_meta(staged_path),
            "ev_consumer": _file_meta(consumer_path),
            "surge_freshness_audit": _file_meta(freshness_audit_path),
            "surge_state_machine_shadow": _file_meta(state_machine_path),
            "surge_preopen_precursor_readiness": _file_meta(preopen_readiness_path),
        },
        "checks": checks,
    }


def _entry_idea() -> dict[str, str]:
    return {
        "mode": "observe_only",
        "text": "ATR14 high bucket: do not auto-apply; keep as validation candidate only.",
    }


def build_snapshot() -> dict[str, Any]:
    snapshot = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "as_of_ymd": datetime.now().strftime("%Y%m%d"),
        "top_signal": _top_signal(),
        "fills_status": _fills_status(),
        "exposure": _exposure(),
        "spikes": _spikes(),
        "surge_readiness": _surge_readiness(),
        "entry_idea": _entry_idea(),
    }
    snapshot["one_line"] = render_one_line(snapshot)
    return snapshot


def render_one_line(snapshot: dict[str, Any]) -> str:
    top = snapshot["top_signal"]
    fills = snapshot["fills_status"]
    exposure = snapshot["exposure"]
    spikes = snapshot["spikes"]
    surge_ready = snapshot["surge_readiness"]
    idea = snapshot["entry_idea"]
    top_sig = ",".join(top["top_codes"]) if top["top_codes"] else "NA"
    spike_codes = ",".join(str(row.get("code")) for row in spikes["top"] if row.get("code")) or "NA"
    exp = ",".join(f"{sector} {_fmt_pct(pct)}" for sector, pct in exposure["sector_pct"].items()) or "NA"
    anomaly = ";".join(fills["anomalies"]) if fills["anomalies"] else "none"
    advisory = ";".join(fills.get("advisories") or []) if fills.get("advisories") else "none"
    state_counts = surge_ready.get("state_machine_counts") or {}
    state_summary = ",".join(f"{key}={value}" for key, value in state_counts.items()) or "NA"
    preopen_ready = surge_ready.get("preopen_ready_items", 0)
    preopen_total = surge_ready.get("preopen_total_items", 0)
    preopen_quality = surge_ready.get("preopen_quality_score", 0.0)
    preopen_missing = surge_ready.get("preopen_source_not_implemented_items", 0)
    preopen_intraday = surge_ready.get("preopen_intraday_required_items", 0)
    transition_status = surge_ready.get("transition_outcome_status") or "NA"
    intraday_ok = surge_ready.get("preopen_intraday_validation_available")
    return (
        f"TOP_SIG={top_sig} "
        f"| REGIME={top['market_regime']} "
        f"| ENTRY_GATE={top['entry_gate']} "
        f"| ENTRY_OK={str(top['entry_allowed_observed']).lower()} "
        f"| risk_off={str(top['risk_off']).lower()} "
        f"| kill_switch={str(top['kill_switch_triggered']).lower()} "
        f"| FILLS={fills['fills']} "
        f"| UNFILLED={fills['unfilled']} "
        f"| LAT(ms)={fills['avg_fill_latency_ms']} "
        f"| LAT_D(ms)={fills['latency_delta_ms']} "
        f"| ANOM={anomaly} "
        f"| ADV={advisory} "
        f"| EXP=[{exp}] "
        f"| EXP_LIMIT_OK={str(exposure['limit_check_observed']).lower()} "
        f"| SPIKE=[{spike_codes}] "
        f"| SURGE_READY={surge_ready['status']}"
        f"(univ={surge_ready['universe_rows']},ready={surge_ready['ready_rows']},"
        f"staged={surge_ready['staged_rows']},consumer={str(surge_ready['consumer_enabled']).lower()}) "
        f"| SURGE_STATE=[{state_summary}] "
        f"| SURGE_PREOPEN={preopen_ready}/{preopen_total}"
        f"(quality={preopen_quality},missing={preopen_missing},intraday_need={preopen_intraday},"
        f"intraday_ok={str(intraday_ok).lower()},transition={transition_status}) "
        f"| ENTRY_IDEA={idea['mode']}:{idea['text']}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Build pre-open 5-minute one-line check from current SSOT artifacts.")
    parser.add_argument("--json-out", default=str(LOGS / "preopen_5min_check_latest.json"))
    parser.add_argument("--txt-out", default=str(LOGS / "preopen_5min_check_latest.txt"))
    args = parser.parse_args()

    snapshot = build_snapshot()
    json_out = Path(args.json_out)
    txt_out = Path(args.txt_out)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    txt_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    txt_out.write_text(snapshot["one_line"] + "\n", encoding="utf-8")
    print(snapshot["one_line"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
