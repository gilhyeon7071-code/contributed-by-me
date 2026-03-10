"""Adapters to build verifier evidence from runtime artifacts."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


def _load_json(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists() or not p.is_file():
        return {}

    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return json.loads(p.read_text(encoding=enc))
        except Exception:
            continue
    return {}


def _load_latest_json(directory: Optional[str], pattern: str) -> Dict[str, Any]:
    if not directory:
        return {}
    d = Path(directory)
    if not d.exists() or not d.is_dir():
        return {}
    files = sorted(d.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    for f in files:
        payload = _load_json(str(f))
        if payload:
            return payload
    return {}



def _resolve_logs_dir(
    pending_status_path: Optional[str],
    runtime_evidence_path: Optional[str],
) -> Optional[str]:
    candidates: List[Optional[str]] = [
        pending_status_path,
        runtime_evidence_path,
    ]
    for raw in candidates:
        if not raw:
            continue
        p = Path(raw)
        parent = p.parent
        if parent.exists() and parent.is_dir():
            return str(parent)
    fallback = Path(r"E:\1_Data\2_Logs")
    if fallback.exists() and fallback.is_dir():
        return str(fallback)
    return None


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _to_float(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        txt = value.strip().lower().replace("ms", "").replace("%", "")
        try:
            return float(txt)
        except Exception:
            return None
    return None


def _to_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        txt = value.strip().replace(",", "")
        try:
            return int(float(txt))
        except Exception:
            return None
    return None


def _to_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        txt = value.strip().lower()
        if txt in {"true", "t", "1", "yes", "y", "on"}:
            return True
        if txt in {"false", "f", "0", "no", "n", "off"}:
            return False
    return None


def _to_str_list(value: Any) -> Optional[List[str]]:
    if isinstance(value, list):
        out = [str(v).strip() for v in value if str(v).strip()]
        return out if out else None
    if isinstance(value, str):
        parts = [p.strip() for p in value.split(",") if p.strip()]
        return parts if parts else None
    return None


def _to_dt(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        txt = value.strip()
        if not txt:
            return None
        for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(txt, fmt)
            except Exception:
                continue
        try:
            return datetime.fromisoformat(txt.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


def _first_float(candidates: List[Any]) -> Optional[float]:
    for c in candidates:
        v = _to_float(c)
        if v is not None:
            return v
    return None


def _first_int(candidates: List[Any]) -> Optional[int]:
    for c in candidates:
        v = _to_int(c)
        if v is not None:
            return v
    return None


def _first_str_list(candidates: List[Any]) -> Optional[List[str]]:
    for c in candidates:
        v = _to_str_list(c)
        if v:
            return v
    return None


def _merge_phase_evidence(
    target_phase: Dict[str, Dict[str, Any]],
    src_phase: Dict[str, Dict[str, Any]],
) -> None:
    for method_name, kwargs in src_phase.items():
        if not isinstance(kwargs, dict):
            continue
        current = target_phase.get(method_name)
        if not isinstance(current, dict):
            target_phase[method_name] = dict(kwargs)
            continue
        merged = dict(current)
        merged.update(kwargs)
        target_phase[method_name] = merged


def _collect_design_evidence(
    state: Dict[str, Any],
    design_payload: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    evidence: Dict[str, Dict[str, Any]] = {}
    # Accept only explicit design-evidence payloads.
    # This avoids treating dashboard display fields as implementation evidence.
    sources: List[Dict[str, Any]] = [
        _as_dict(design_payload),
        _as_dict(state.get("design_evidence")),
    ]

    source_labels = [
        "design_payload",
        "dashboard_design_evidence",
    ]

    process_blocks = [_as_dict(s.get("process_isolation")) for s in sources]
    queue_blocks = [_as_dict(s.get("message_queue")) for s in sources]
    db_blocks = [_as_dict(s.get("db_schema")) for s in sources]
    module_blocks = [_as_dict(s.get("module_separation")) for s in sources]

    def _first_float_with_source(blocks: List[Dict[str, Any]], keys: List[str]):
        for idx, block in enumerate(blocks):
            for key in keys:
                v = _to_float(block.get(key))
                if v is None:
                    continue
                src = str(
                    block.get("evidence_source")
                    or block.get("_source")
                    or source_labels[idx]
                ).strip()
                return v, src
        return None, None

    def _first_int_with_source(blocks: List[Dict[str, Any]], keys: List[str]):
        for idx, block in enumerate(blocks):
            for key in keys:
                v = _to_int(block.get(key))
                if v is None:
                    continue
                src = str(
                    block.get("evidence_source")
                    or block.get("_source")
                    or source_labels[idx]
                ).strip()
                return v, src
        return None, None

    def _first_list_with_source(blocks: List[Dict[str, Any]], keys: List[str]):
        for idx, block in enumerate(blocks):
            for key in keys:
                v = _to_str_list(block.get(key))
                if not v:
                    continue
                src = str(
                    block.get("evidence_source")
                    or block.get("_source")
                    or source_labels[idx]
                ).strip()
                return v, src
        return None, None

    watchdog_ms, watchdog_src = _first_float_with_source(
        process_blocks,
        ["watchdog_detect_time_ms", "actual_ms"],
    )
    if watchdog_ms is not None:
        evidence["verify_process_isolation"] = {
            "watchdog_detect_time_ms": watchdog_ms,
            "evidence_source": watchdog_src,
        }

    lag_ms, lag_src = _first_float_with_source(
        queue_blocks,
        ["measured_lag_ms", "lag_ms", "actual_ms"],
    )
    dropped, dropped_src = _first_int_with_source(
        queue_blocks,
        ["dropped_messages", "drop_count"],
    )
    if lag_ms is not None and dropped is not None:
        evidence["verify_message_queue_performance"] = {
            "measured_lag_ms": lag_ms,
            "dropped_messages": dropped,
            "evidence_source": lag_src or dropped_src,
        }

    db_ms, db_src = _first_float_with_source(
        db_blocks,
        ["query_response_ms", "actual_ms"],
    )
    if db_ms is not None:
        evidence["verify_db_schema_efficiency"] = {
            "query_response_ms": db_ms,
            "evidence_source": db_src,
        }

    modules, modules_src = _first_list_with_source(
        module_blocks,
        ["designed_modules", "implemented_modules"],
    )
    if modules:
        evidence["verify_module_separation"] = {
            "designed_modules": modules,
            "evidence_source": modules_src,
        }

    return evidence

def _phase_alias_to_name(raw: str) -> Optional[str]:
    key = str(raw or "").strip().upper().replace("-", "_")
    aliases = {
        "1": "PLANNING",
        "PHASE1": "PLANNING",
        "PHASE_1": "PLANNING",
        "PLANNING": "PLANNING",
        "2": "DESIGN",
        "PHASE2": "DESIGN",
        "PHASE_2": "DESIGN",
        "DESIGN": "DESIGN",
        "3": "DATA",
        "PHASE3": "DATA",
        "PHASE_3": "DATA",
        "DATA": "DATA",
        "4": "STRATEGY",
        "PHASE4": "STRATEGY",
        "PHASE_4": "STRATEGY",
        "STRATEGY": "STRATEGY",
        "5": "EXECUTION",
        "PHASE5": "EXECUTION",
        "PHASE_5": "EXECUTION",
        "EXECUTION": "EXECUTION",
        "6": "RISK",
        "PHASE6": "RISK",
        "PHASE_6": "RISK",
        "RISK": "RISK",
        "7": "TESTING",
        "PHASE7": "TESTING",
        "PHASE_7": "TESTING",
        "TESTING": "TESTING",
        "8": "OPERATIONS",
        "PHASE8": "OPERATIONS",
        "PHASE_8": "OPERATIONS",
        "OPERATIONS": "OPERATIONS",
    }
    return aliases.get(key)


def _build_survivorship_v2(payload: Dict[str, Any]):
    from .phase_3_4_verifiers import SurvivorshipBiasCheckResultV2

    period_cov_raw = _as_dict(payload.get("period_coverage"))
    period_cov: Dict[str, float] = {}
    for k, v in period_cov_raw.items():
        fv = _to_float(v)
        if fv is not None:
            period_cov[str(k)] = fv

    return SurvivorshipBiasCheckResultV2(
        critical_stocks_required=_to_str_list(payload.get("critical_stocks_required")) or [],
        critical_stocks_included=_to_str_list(payload.get("critical_stocks_included")) or [],
        normal_stocks_required=_to_str_list(payload.get("normal_stocks_required")) or [],
        normal_stocks_included=_to_str_list(payload.get("normal_stocks_included")) or [],
        period_coverage=period_cov,
        critical_threshold=_to_float(payload.get("critical_threshold")) or 0.9,
        normal_threshold=_to_float(payload.get("normal_threshold")) or 0.7,
        period_min_threshold=_to_float(payload.get("period_min_threshold")) or 0.5,
        min_universe_size_required=_to_int(payload.get("min_universe_size_required")) or 500,
        universe_size=_to_int(payload.get("universe_size")) or 0,
        input_scope=str(payload.get("input_scope") or "UNKNOWN"),
        required_input_scope=str(payload.get("required_input_scope") or "FULL_UNIVERSE"),
        enforcement_mode=str(payload.get("enforcement_mode") or "STRICT"),
    )


def _build_backtest_result(payload: Dict[str, Any], is_in_sample_default: bool):
    from .phase_3_4_verifiers import BacktestResult

    return BacktestResult(
        total_return=_to_float(payload.get("total_return")) or 0.0,
        sharpe_ratio=_to_float(payload.get("sharpe_ratio") or payload.get("sharpe")) or 0.0,
        max_drawdown=_to_float(payload.get("max_drawdown")) or 0.0,
        win_rate=_to_float(payload.get("win_rate")) or 0.0,
        total_trades=_to_int(payload.get("total_trades")) or 0,
        avg_holding_period=_to_float(payload.get("avg_holding_period")) or 0.0,
        is_in_sample=bool(payload.get("is_in_sample", is_in_sample_default)),
    )


def _build_ohlcv_rows(rows: Sequence[Any]):
    from .phase_3_4_verifiers import OHLCVData

    out: List[Any] = []
    for row in rows:
        d = _as_dict(row)
        if not d:
            continue
        ts = _to_dt(d.get("timestamp") or d.get("time") or d.get("dt"))
        o = _to_float(d.get("open"))
        h = _to_float(d.get("high"))
        l = _to_float(d.get("low"))
        c = _to_float(d.get("close"))
        v = _to_int(d.get("volume"))
        if ts is None or o is None or h is None or l is None or c is None or v is None:
            continue
        out.append(
            OHLCVData(
                timestamp=ts,
                open=o,
                high=h,
                low=l,
                close=c,
                volume=v,
                symbol=str(d.get("symbol", "")),
            )
        )
    return out


def _to_order_status(raw: Any):
    from .phase_5_6_verifiers import OrderStatus

    if isinstance(raw, OrderStatus):
        return raw
    key = str(raw or "").strip().upper()
    for item in OrderStatus:
        if item.value == key or item.name == key:
            return item
    return None


def _to_order_type(raw: Any):
    from .phase_5_6_verifiers import OrderType

    if isinstance(raw, OrderType):
        return raw
    key = str(raw or "").strip().upper()
    for item in OrderType:
        if item.value == key or item.name == key:
            return item
    return None


def _build_order(payload: Dict[str, Any]):
    from .phase_5_6_verifiers import Order

    order_type = _to_order_type(payload.get("order_type")) or _to_order_type("LIMIT")
    status = _to_order_status(payload.get("status")) or _to_order_status("PENDING")
    if order_type is None or status is None:
        return None

    submitted_at = _to_dt(payload.get("submitted_at"))
    filled_at = _to_dt(payload.get("filled_at"))
    return Order(
        order_id=str(payload.get("order_id", "EVIDENCE_ORDER")),
        symbol=str(payload.get("symbol", "UNKNOWN")),
        side=str(payload.get("side", "BUY")),
        order_type=order_type,
        quantity=_to_int(payload.get("quantity")) or 0,
        price=_to_float(payload.get("price")),
        filled_quantity=_to_int(payload.get("filled_quantity")) or 0,
        avg_fill_price=_to_float(payload.get("avg_fill_price")) or 0.0,
        status=status,
        submitted_at=submitted_at,
        filled_at=filled_at,
        error_code=(str(payload.get("error_code")) if payload.get("error_code") is not None else None),
        error_message=(str(payload.get("error_message")) if payload.get("error_message") is not None else None),
    )


def _build_execution_test_case(payload: Dict[str, Any]):
    from .phase_5_6_verifiers import ExecutionTestCase

    input_order = _build_order(_as_dict(payload.get("input_order")))
    if input_order is None:
        return None

    expected_status = _to_order_status(payload.get("expected_status"))
    if expected_status is None:
        expected_status = input_order.status

    actual_status = _to_order_status(payload.get("actual_status"))
    return ExecutionTestCase(
        name=str(payload.get("name", "runtime_case")),
        input_order=input_order,
        expected_status=expected_status,
        expected_filled_qty=_to_int(payload.get("expected_filled_qty")) or 0,
        expected_error_code=(str(payload.get("expected_error_code")) if payload.get("expected_error_code") is not None else None),
        actual_status=actual_status,
        actual_filled_qty=_to_int(payload.get("actual_filled_qty")),
        passed=bool(payload.get("passed", False)),
    )


def _build_transitions(rows: Sequence[Any]) -> List[Tuple[Any, Any]]:
    transitions: List[Tuple[Any, Any]] = []
    for row in rows:
        if isinstance(row, str) and "->" in row:
            frm, to = row.split("->", 1)
            transitions.append((frm.strip(), to.strip()))
            continue
        if isinstance(row, (list, tuple)) and len(row) == 2:
            transitions.append((row[0], row[1]))
            continue
        d = _as_dict(row)
        if d:
            frm = d.get("from") or d.get("src")
            to = d.get("to") or d.get("dst")
            if frm is not None and to is not None:
                transitions.append((frm, to))
    return transitions


def _build_connection_logs(rows: Sequence[Any]):
    from .phase_7_8_verifiers import ConnectionLog

    out = []
    for row in rows:
        d = _as_dict(row)
        if not d:
            continue
        event = str(d.get("event_type") or d.get("type") or "").strip()
        if not event:
            continue
        success = _to_bool(d.get("success"))
        out.append(ConnectionLog(event_type=event, success=bool(success)))
    return out


def _build_trade_records(rows: Sequence[Any]):
    from .phase_7_8_verifiers import TradeRecord

    out = []
    for row in rows:
        d = _as_dict(row)
        if not d:
            continue
        trade_id = str(d.get("trade_id") or d.get("id") or "").strip()
        symbol = str(d.get("symbol") or "").strip()
        qty = _to_int(d.get("quantity"))
        price = _to_float(d.get("price"))
        if not trade_id or not symbol or qty is None or price is None:
            continue
        out.append(TradeRecord(trade_id=trade_id, symbol=symbol, quantity=qty, price=price))
    return out


def _normalize_runtime_method(
    phase_name: str,
    method_name: str,
    payload: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    if phase_name == "PLANNING":
        if method_name == "verify_os_compatibility":
            return {
                "os_name": payload.get("os_name"),
                "os_version": payload.get("os_version"),
                "arch": payload.get("arch"),
                "windows_major_version": _to_int(payload.get("windows_major_version")),
            }
        if method_name == "verify_cost_structure":
            return {
                "market": payload.get("market"),
                "slippage_bp": _to_float(payload.get("slippage_bp")),
                "bep_percent_override": _to_float(payload.get("bep_percent_override") or payload.get("bep_percent")),
            }
        if method_name == "verify_architecture_suitability":
            return {
                "architecture_type": payload.get("architecture_type"),
                "api_provider": payload.get("api_provider"),
            }
        if method_name == "verify_api_constraints":
            return {
                "api_provider": payload.get("api_provider"),
                "target_tps": _to_float(payload.get("target_tps")),
                "api_limit_override": _to_float(payload.get("api_limit_override")),
            }
        if method_name == "verify_market_calendar_alignment":
            checklist = _as_dict(payload.get("session_checklist"))
            return {
                "trading_day": _to_bool(payload.get("trading_day")),
                "session_checklist": ({str(k): bool(v) for k, v in checklist.items()} if checklist else None),
            }

    if phase_name == "DESIGN":
        if method_name == "verify_exactly_once_idempotency":
            return {
                "total_submit_events": _to_int(payload.get("total_submit_events")),
                "unique_idempotency_keys": _to_int(payload.get("unique_idempotency_keys")),
                "duplicate_replays_blocked": _to_int(payload.get("duplicate_replays_blocked")),
                "persistence_ok": _to_bool(payload.get("persistence_ok")),
                "evidence_source": str(payload.get("evidence_source") or "runtime_evidence"),
            }

        if method_name == "verify_draft_check":
            return {
                "logic_doc_present": _to_bool(payload.get("logic_doc_present")),
                "requirements_total": _to_int(payload.get("requirements_total")),
                "requirements_covered": _to_int(payload.get("requirements_covered")),
                "logical_gap_count": _to_int(payload.get("logical_gap_count")),
                "evidence_source": str(payload.get("evidence_source") or "runtime_evidence"),
                "gate_mode": str(payload.get("gate_mode") or "STRICT"),
            }
        if method_name == "verify_flowcharting":
            return {
                "flowchart_present": _to_bool(payload.get("flowchart_present")),
                "flowchart_artifact_count": _to_int(payload.get("flowchart_artifact_count")),
                "loop_violation_count": _to_int(payload.get("loop_violation_count")),
                "dead_end_count": _to_int(payload.get("dead_end_count")),
                "evidence_source": str(payload.get("evidence_source") or "runtime_evidence"),
                "gate_mode": str(payload.get("gate_mode") or "STRICT"),
            }
        if method_name == "verify_dry_run":
            return {
                "sample_case_count": _to_int(payload.get("sample_case_count")),
                "matched_case_count": _to_int(payload.get("matched_case_count")),
                "calculation_error_count": _to_int(payload.get("calculation_error_count")),
                "min_sample_cases": _to_int(payload.get("min_sample_cases")) or 10,
                "evidence_source": str(payload.get("evidence_source") or "runtime_evidence"),
                "gate_mode": str(payload.get("gate_mode") or "STRICT"),
            }
        if method_name == "verify_edge_case_test":
            return {
                "tested_case_count": _to_int(payload.get("tested_case_count")),
                "passed_case_count": _to_int(payload.get("passed_case_count")),
                "critical_fail_count": _to_int(payload.get("critical_fail_count")),
                "has_null_case": _to_bool(payload.get("has_null_case")),
                "has_extreme_case": _to_bool(payload.get("has_extreme_case")),
                "evidence_source": str(payload.get("evidence_source") or "runtime_evidence"),
                "gate_mode": str(payload.get("gate_mode") or "STRICT"),
            }
        if method_name == "verify_cross_check":
            return {
                "sources_compared": _to_int(payload.get("sources_compared")),
                "mismatch_count": _to_int(payload.get("mismatch_count")),
                "tolerance_breach_count": _to_int(payload.get("tolerance_breach_count")),
                "evidence_source": str(payload.get("evidence_source") or "runtime_evidence"),
                "gate_mode": str(payload.get("gate_mode") or "STRICT"),
            }
        if method_name == "verify_stress_test":
            return {
                "records_tested": _to_int(payload.get("records_tested")),
                "p95_latency_ms": _to_float(payload.get("p95_latency_ms")),
                "error_rate_percent": _to_float(payload.get("error_rate_percent")),
                "min_records": _to_int(payload.get("min_records")) or 100,
                "max_p95_latency_ms": _to_float(payload.get("max_p95_latency_ms")) or 500.0,
                "max_error_rate_percent": _to_float(payload.get("max_error_rate_percent")) or 1.0,
                "evidence_source": str(payload.get("evidence_source") or "runtime_evidence"),
                "gate_mode": str(payload.get("gate_mode") or "STRICT"),
            }

    if phase_name == "DATA":
        if method_name == "verify_survivorship_bias_v2":
            src = _as_dict(payload.get("result")) or payload
            return {"result": _build_survivorship_v2(src)}
        if method_name == "verify_lookahead_bias":
            signal_src = payload.get("signal_timestamps") or []
            data_src = payload.get("data_timestamps") or []
            signal = [dt for dt in (_to_dt(v) for v in signal_src) if dt is not None]
            data = [dt for dt in (_to_dt(v) for v in data_src) if dt is not None]
            return {"signal_timestamps": signal, "data_timestamps": data}
        if method_name == "verify_point_in_time_snapshot":
            return {
                "snapshot_timestamp": _to_dt(payload.get("snapshot_timestamp")),
                "latest_data_timestamp": _to_dt(payload.get("latest_data_timestamp")),
                "asof_match": _to_bool(payload.get("asof_match")),
                "universe_fixed": _to_bool(payload.get("universe_fixed")),
            }
        if method_name == "verify_data_integrity":
            rows = payload.get("ohlcv_data") or []
            return {"ohlcv_data": _build_ohlcv_rows(rows)}
        if method_name == "verify_adjusted_price_handling":
            checklist = _as_dict(payload.get("checklist"))
            if checklist:
                return {
                    "checklist": {str(k): bool(v) for k, v in checklist.items()},
                }
            return {}

    if phase_name == "STRATEGY":
        if method_name == "verify_overfitting":
            in_src = _as_dict(payload.get("in_sample_result"))
            out_src = _as_dict(payload.get("out_sample_result"))
            if not in_src:
                in_src = {
                    "sharpe_ratio": payload.get("in_sample_sharpe"),
                    "is_in_sample": True,
                }
            if not out_src:
                out_src = {
                    "sharpe_ratio": payload.get("out_sample_sharpe"),
                    "is_in_sample": False,
                }
            return {
                "in_sample_result": _build_backtest_result(in_src, True),
                "out_sample_result": _build_backtest_result(out_src, False),
                "deflated_sharpe_ratio": _to_float(payload.get("deflated_sharpe_ratio")),
                "pbo_proxy": _to_float(payload.get("pbo_proxy")),
                "spa_pvalue_proxy": _to_float(payload.get("spa_pvalue_proxy")),
                "min_dsr": _to_float(payload.get("min_dsr")) or 0.10,
                "max_pbo": _to_float(payload.get("max_pbo")) or 0.80,
                "max_spa_pvalue": _to_float(payload.get("max_spa_pvalue")) or 0.50,
                "overfit_metric_sample_size": _to_int(payload.get("overfit_metric_sample_size")),
                "min_overfit_metric_sample_size": _to_int(payload.get("min_overfit_metric_sample_size")) or 30,
                "performance_gate_mode": str(payload.get("performance_gate_mode") or "STRICT"),
            }
        if method_name == "verify_walkforward_regime_robustness":
            raw = _as_dict(payload.get("regime_results"))
            regimes: Dict[str, Dict[str, float]] = {}
            for name, stats in raw.items():
                d = _as_dict(stats)
                regimes[str(name)] = {
                    "sharpe": _to_float(d.get("sharpe")) or 0.0,
                    "mdd": _to_float(d.get("mdd")) or 0.0,
                }
            return {
                "regime_results": regimes,
                "min_regimes_required": _to_int(payload.get("min_regimes_required")) or 3,
                "min_regime_sharpe": _to_float(payload.get("min_regime_sharpe")) or 0.5,
                "max_regime_mdd_abs": _to_float(payload.get("max_regime_mdd_abs")) or 35.0,
                "overall_sortino": _to_float(payload.get("overall_sortino")),
                "overall_calmar": _to_float(payload.get("overall_calmar")),
                "overall_metric_sample_size": _to_int(payload.get("overall_metric_sample_size")),
                "min_metric_sample_size": _to_int(payload.get("min_metric_sample_size")) or 30,
                "min_overall_sortino": _to_float(payload.get("min_overall_sortino")) or 0.20,
                "min_overall_calmar": _to_float(payload.get("min_overall_calmar")) or 0.05,
                "performance_gate_mode": str(payload.get("performance_gate_mode") or "STRICT"),
            }
        if method_name == "verify_randomness_test":
            rnd = payload.get("random_returns") or []
            random_returns = [x for x in (_to_float(v) for v in rnd) if x is not None]
            strategy_return = _to_float(payload.get("strategy_return"))
            return {
                "strategy_return": strategy_return,
                "random_returns": random_returns,
            }
        if method_name == "verify_max_drawdown":
            historical = _to_float(payload.get("historical_mdd"))
            crisis_raw = _as_dict(payload.get("crisis_periods"))
            crisis_periods: Dict[str, float] = {}
            for k, v in crisis_raw.items():
                fv = _to_float(v)
                if fv is not None:
                    crisis_periods[str(k)] = fv
            return {
                "historical_mdd": historical,
                "crisis_periods": crisis_periods if crisis_periods else None,
            }
        if method_name == "verify_liquidity_constraints":
            ratio = _to_float(payload.get("max_order_volume_ratio"))
            return {"max_order_volume_ratio": ratio}

    if phase_name == "EXECUTION":
        if method_name == "verify_partial_fill_handling":
            case = _build_execution_test_case(payload)
            return {"test_result": case} if case is not None else {}
        if method_name == "verify_order_rejection_handling":
            events = payload.get("events") if isinstance(payload.get("events"), list) else payload.get("rejections")
            if not isinstance(events, list):
                return {}
            return {"events": [e for e in events if isinstance(e, dict)]}
        if method_name == "verify_network_disconnection":
            ms = _to_float(payload.get("reconnect_time_ms"))
            checklist = _as_dict(payload.get("emergency_checklist"))
            return {
                "reconnect_time_ms": ms,
                "emergency_checklist": ({str(k): bool(v) for k, v in checklist.items()} if checklist else None),
            }
        if method_name == "verify_order_state_machine":
            rows = payload.get("transitions")
            if not isinstance(rows, list):
                return {}
            return {"transitions": _build_transitions(rows)}
        if method_name == "verify_event_sequence_integrity":
            rows = payload.get("order_event_sequences")
            if not isinstance(rows, list):
                return {}
            norm: List[Dict[str, Any]] = []
            for row in rows:
                d = _as_dict(row)
                if not d:
                    continue
                events = d.get("events") if isinstance(d.get("events"), list) else []
                norm.append({
                    "order_id": str(d.get("order_id") or "UNKNOWN"),
                    "events": [str(x) for x in events],
                })
            return {"order_event_sequences": norm}
        if method_name == "verify_error_code_handling":
            handled = _as_dict(payload.get("handled_codes"))
            backoff = _as_dict(payload.get("backoff_config"))
            return {
                "handled_codes": {str(k): bool(v) for k, v in handled.items()} if handled else None,
                "backoff_config": backoff or None,
                "rate_limit_429_events": _to_int(payload.get("rate_limit_429_events")),
                "rate_limit_429_warn_threshold": _to_int(payload.get("rate_limit_429_warn_threshold")) or 0,
            }

    if phase_name == "RISK":
        if method_name == "verify_order_limits":
            return {
                "test_order_amount": _to_float(payload.get("test_order_amount")),
                "total_assets": _to_float(payload.get("total_assets")),
                "rejected": _to_bool(payload.get("rejected")),
            }
        if method_name == "verify_portfolio_exposure_limits":
            return {
                "gross_exposure_ratio": _to_float(payload.get("gross_exposure_ratio")),
                "single_name_max_ratio": _to_float(payload.get("single_name_max_ratio")),
                "sector_max_ratio": _to_float(payload.get("sector_max_ratio")),
                "leverage_ratio": _to_float(payload.get("leverage_ratio")),
            }
        if method_name == "verify_kill_switch":
            actions = _as_dict(payload.get("actions_triggered"))
            return {
                "simulated_loss_ratio": _to_float(payload.get("simulated_loss_ratio")),
                "actions_triggered": {str(k): bool(v) for k, v in actions.items()} if actions else {},
                "mode": (str(payload.get("mode")) if payload.get("mode") is not None else None),
            }
        if method_name == "verify_duplicate_prevention":
            return {
                "total_attempts": _to_int(payload.get("total_attempts")),
                "duplicate_orders_blocked": _to_int(payload.get("duplicate_orders_blocked")),
            }
        if method_name == "verify_price_deviation":
            rows = payload.get("order_checks")
            return {"order_checks": rows if isinstance(rows, list) else None}
        if method_name == "verify_cost_optimization":
            rows = payload.get("trades")
            stats = _as_dict(payload.get("trade_stats"))
            return {
                "trades": rows if isinstance(rows, list) else None,
                "has_ev_module": _to_bool(payload.get("has_ev_module")),
                "trade_stats": stats if stats else None,
                "min_profit_factor": _to_float(payload.get("min_profit_factor")) or 1.05,
                "min_expectancy": _to_float(payload.get("min_expectancy")) or 0.0,
                "min_sample_trades": _to_int(payload.get("min_sample_trades")) or 30,
                "performance_gate_mode": str(payload.get("performance_gate_mode") or "STRICT"),
            }

    if phase_name == "TESTING":
        if method_name == "verify_api_connection_stability":
            rows = payload.get("connection_logs")
            duration = _to_float(payload.get("test_duration_hours"))
            common = {
                "test_duration_hours": duration or 24.0,
                "data_staleness_p95_ms": _to_float(payload.get("data_staleness_p95_ms")),
                "data_staleness_p99_ms": _to_float(payload.get("data_staleness_p99_ms")),
                "ack_latency_p95_ms": _to_float(payload.get("ack_latency_p95_ms")),
                "fill_latency_p95_ms": _to_float(payload.get("fill_latency_p95_ms")),
            }
            if isinstance(rows, list):
                logs = _build_connection_logs(rows)
                return {"connection_logs": logs, **common}
            return common
        if method_name == "verify_execution_consistency":
            match_rate = _to_float(payload.get("match_rate_override"))
            total_override = _to_int(payload.get("total_trades_override"))
            if match_rate is not None:
                return {"match_rate_override": match_rate, "total_trades_override": total_override, "is_drift_bps": _to_float(payload.get("is_drift_bps"))}
            program = payload.get("program_trades")
            hts = payload.get("hts_trades")
            if isinstance(program, list) and isinstance(hts, list):
                return {
                    "program_trades": _build_trade_records(program),
                    "hts_trades": _build_trade_records(hts),
                    "is_drift_bps": _to_float(payload.get("is_drift_bps")),
                }
            return {}
        if method_name == "verify_broker_statement_reconciliation":
            return {
                "statement_total_trades": _to_int(payload.get("statement_total_trades")),
                "internal_total_trades": _to_int(payload.get("internal_total_trades")),
                "statement_net_pnl": _to_float(payload.get("statement_net_pnl")),
                "internal_net_pnl": _to_float(payload.get("internal_net_pnl")),
                "pnl_tolerance_krw": _to_float(payload.get("pnl_tolerance_krw")) or 1.0,
            }
        if method_name == "verify_tax_fee_calculation":
            return {
                "trade_price": _to_float(payload.get("trade_price")) or 70000,
                "quantity": _to_int(payload.get("quantity")) or 1,
                "market": payload.get("market"),
                "actual_total_cost": _to_float(payload.get("actual_total_cost")),
            }
        if method_name == "verify_paper_trading_awareness":
            limits = _as_dict(payload.get("limitations_acknowledged"))
            guides = _as_dict(payload.get("usage_guidelines"))
            return {
                "limitations_acknowledged": ({str(k): bool(v) for k, v in limits.items()} if limits else None),
                "usage_guidelines": ({str(k): bool(v) for k, v in guides.items()} if guides else None),
            }
        if method_name == "verify_canary_deployment_readiness":
            checklist = _as_dict(payload.get("canary_checklist"))
            return {
                "canary_checklist": ({str(k): bool(v) for k, v in checklist.items()} if checklist else None),
            }

    if phase_name == "OPERATIONS":
        if method_name == "verify_auto_reconnection":
            steps = _as_dict(payload.get("reconnection_steps"))
            return {
                "reconnection_time_ms": _to_float(payload.get("reconnection_time_ms")),
                "reconnection_steps": ({str(k): bool(v) for k, v in steps.items()} if steps else None),
            }
        if method_name == "verify_log_integrity":
            mgmt = _as_dict(payload.get("log_management"))
            return {
                "error_logs_with_alerts": _to_int(payload.get("error_logs_with_alerts")),
                "total_error_logs": _to_int(payload.get("total_error_logs")),
                "log_management": ({str(k): bool(v) for k, v in mgmt.items()} if mgmt else None),
            }
        if method_name == "verify_data_backup":
            checklist = _as_dict(payload.get("backup_checklist"))
            return {
                "recovery_time_minutes": _to_float(payload.get("recovery_time_minutes")),
                "backup_checklist": ({str(k): bool(v) for k, v in checklist.items()} if checklist else None),
            }
        if method_name == "verify_backup_drill_runbook":
            checklist = _as_dict(payload.get("runbook_checklist"))
            return {
                "last_drill_days_ago": _to_int(payload.get("last_drill_days_ago")),
                "rpo_minutes": _to_float(payload.get("rpo_minutes")),
                "rto_minutes": _to_float(payload.get("rto_minutes")),
                "runbook_checklist": ({str(k): bool(v) for k, v in checklist.items()} if checklist else None),
            }
        if method_name == "verify_scheduler":
            schedule = _as_dict(payload.get("schedule_items"))
            login = _as_dict(payload.get("auto_login_config"))
            return {
                "schedule_items": ({str(k): bool(v) for k, v in schedule.items()} if schedule else None),
                "auto_login_config": ({str(k): bool(v) for k, v in login.items()} if login else None),
            }
        if method_name == "verify_monitoring_alerts":
            channels = _as_dict(payload.get("alert_channels"))
            required = _as_dict(payload.get("required_alerts"))
            return {
                "alert_channels": ({str(k): bool(v) for k, v in channels.items()} if channels else None),
                "required_alerts": ({str(k): bool(v) for k, v in required.items()} if required else None),
                "best_execution_review_days_ago": _to_int(payload.get("best_execution_review_days_ago")),
                "best_execution_review_cycle_days": _to_int(payload.get("best_execution_review_cycle_days")) or 92,
            }

    return payload if isinstance(payload, dict) else None

def _normalize_runtime_payload(runtime_payload: Dict[str, Any]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    normalized: Dict[str, Dict[str, Dict[str, Any]]] = {}
    if not runtime_payload:
        return normalized

    phase_blocks = runtime_payload.get("phases") if isinstance(runtime_payload.get("phases"), dict) else runtime_payload
    if not isinstance(phase_blocks, dict):
        return normalized

    for raw_phase_name, phase_payload in phase_blocks.items():
        phase_name = _phase_alias_to_name(str(raw_phase_name))
        if phase_name is None:
            continue
        phase_dict = _as_dict(phase_payload)
        if not phase_dict:
            continue

        method_map: Dict[str, Dict[str, Any]] = {}
        for method_name, method_payload in phase_dict.items():
            if not isinstance(method_payload, dict):
                continue
            kwargs = _normalize_runtime_method(phase_name, method_name, method_payload)
            if kwargs is None:
                continue
            method_map[method_name] = kwargs

        if method_map:
            normalized[phase_name] = method_map

    return normalized

def build_evidence_by_phase(
    dashboard_state_path: Optional[str] = None,
    pending_status_path: Optional[str] = None,
    design_evidence_path: Optional[str] = None,
    runtime_evidence_path: Optional[str] = None,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    evidence: Dict[str, Dict[str, Dict[str, Any]]] = {}

    def phase(name: str) -> Dict[str, Dict[str, Any]]:
        return evidence.setdefault(name, {})

    state = _load_json(dashboard_state_path)
    pending = _load_json(pending_status_path)
    design_payload = _load_json(design_evidence_path)
    runtime_payload = _load_json(runtime_evidence_path)
    logs_dir = _resolve_logs_dir(pending_status_path, runtime_evidence_path)

    runtime_meta = runtime_payload.get("_meta") if isinstance(runtime_payload, dict) else None
    if isinstance(runtime_meta, dict) and runtime_meta:
        evidence["_meta"] = dict(runtime_meta)

    # DESIGN: optional evidence from explicit design payload (or dashboard extension).
    design_evidence = _collect_design_evidence(state, design_payload)
    if design_evidence:
        phase("DESIGN").update(design_evidence)

    # STRATEGY: Max drawdown from dashboard risk snapshot.
    risk = state.get("risk", {}) if isinstance(state.get("risk"), dict) else {}
    mdd_pct = risk.get("mdd_pct")
    if isinstance(mdd_pct, (int, float)):
        phase("STRATEGY")["verify_max_drawdown"] = {
            "historical_mdd": -abs(float(mdd_pct)),
        }

    # TESTING: execution consistency from live_vs_bt summary.
    lvb = state.get("live_vs_bt", {}) if isinstance(state.get("live_vs_bt"), dict) else {}
    match_rate = lvb.get("match_rate")
    executions_total = lvb.get("executions_total")
    if isinstance(match_rate, (int, float)):
        pct = float(match_rate)
        if pct <= 1.0:
            pct *= 100.0
        phase("TESTING")["verify_execution_consistency"] = {
            "match_rate_override": pct,
            "total_trades_override": int(executions_total or 0),
        }

    # TESTING: canary readiness derived from source meta existence.
    source_meta = state.get("source_meta", {}) if isinstance(state.get("source_meta"), dict) else {}
    if source_meta:
        keys = [
            "ssot_today_final",
            "config_yaml",
            "orders_exec",
            "live_fills",
            "portfolio",
            "risk_stats",
            "live_vs_bt",
        ]
        checklist = {
            f"{k}_exists": bool((source_meta.get(k) or {}).get("exists", False))
            for k in keys
        }
        phase("TESTING")["verify_canary_deployment_readiness"] = {
            "canary_checklist": checklist,
        }

    # RISK: map pending status into kill-switch evidence when explicit emergency status appears.
    status = str(pending.get("status", "")).upper()
    if status.startswith("DEFERRED") or status.startswith("BLOCK"):
        phase("RISK")["verify_kill_switch"] = {
            "simulated_loss_ratio": 0.03,
            "actions_triggered": {
                "block_new_orders": True,
                "cancel_open_orders": True,
                "notify_operator": True,
            },
            "mode": "BLOCK",
        }

    # RISK: optional automatic bridge from p0/gate artifacts.
    p0 = _load_latest_json(logs_dir, "p0_daily_check_*.json")
    gate = _load_latest_json(logs_dir, "gate_daily_*.json")
    kill = _as_dict(p0.get("kill_switch"))
    engine_action = str(_as_dict(gate.get("engine_action")).get("action", "")).upper()
    if kill:
        triggered = bool(kill.get("triggered", False))
        mode = str(_as_dict(kill.get("limits")).get("mode", "")).upper() or None

        if not triggered:
            phase("RISK")["verify_kill_switch"] = {
                "simulated_loss_ratio": 0.0,
                "actions_triggered": {
                    "block_new_orders": False,
                    "cancel_open_orders": False,
                    "notify_operator": False,
                },
                "mode": mode,
            }
        elif engine_action:
            phase("RISK")["verify_kill_switch"] = {
                "simulated_loss_ratio": 0.03,
                "actions_triggered": {
                    "block_new_orders": engine_action in {"BLOCK", "REDUCE"},
                    "cancel_open_orders": engine_action == "BLOCK",
                    "notify_operator": engine_action in {"BLOCK", "REDUCE"},
                },
                "mode": mode or engine_action,
            }

    # Runtime evidence: direct bridge for phase 3~8 verifier inputs.
    normalized_runtime = _normalize_runtime_payload(runtime_payload)
    for phase_name, methods in normalized_runtime.items():
        _merge_phase_evidence(phase(phase_name), methods)

    return evidence


















