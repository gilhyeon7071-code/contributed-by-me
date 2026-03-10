"""Phase 1 and 2 verifiers (Planning, Design)."""

from __future__ import annotations

import platform
import time
from typing import Any, Dict, List, Optional, Tuple

from .base import (
    BaseVerifier,
    KoreanMarketConstants,
    PhaseReport,
    TradingSystemConfig,
    VerificationResult,
    VerificationPhase,
)


class PlanningVerifier(BaseVerifier):
    def __init__(self, config: TradingSystemConfig):
        super().__init__(VerificationPhase.PLANNING)
        self.config = config

    def verify_all(self) -> PhaseReport:
        self.results.clear()
        self.run_with_evidence("verify_os_compatibility")
        self.run_with_evidence("verify_cost_structure")
        self.run_with_evidence("verify_architecture_suitability")
        self.run_with_evidence("verify_api_constraints")
        self.run_with_evidence("verify_market_calendar_alignment")
        return self.generate_report()

    def verify_os_compatibility(
        self,
        os_name: Optional[str] = None,
        os_version: Optional[str] = None,
        arch: Optional[str] = None,
        windows_major_version: Optional[int] = None,
    ) -> VerificationResult:
        start = time.time()
        resolved_os_name = os_name or platform.system()
        resolved_os_version = os_version or platform.version()
        resolved_arch = arch or platform.architecture()[0]

        is_windows = resolved_os_name == "Windows"
        is_64bit = "64" in resolved_arch

        windows_version_ok = True
        if is_windows:
            major: Optional[int] = windows_major_version
            if major is None:
                try:
                    major = int(platform.win32_ver()[1].split(".")[0])
                except Exception:
                    major = None
            windows_version_ok = (major is not None and major >= 10)

        passed = is_windows and is_64bit and windows_version_ok
        warn = is_windows and is_64bit and not windows_version_ok

        result = self.create_result(
            item_name="OS compatibility",
            passed=passed,
            description="Check if runtime environment supports broker/API stack.",
            criteria="Windows 10+ 64-bit",
            actual_value=f"{resolved_os_name} {resolved_os_version} ({resolved_arch})",
            expected_value="Windows 10+ (64-bit)",
            message=(
                "Compatible runtime" if passed else
                "Partial compatibility (Windows version unknown)" if warn else
                "Incompatible runtime for Windows-only API stack"
            ),
            warn_condition=warn,
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "pre_open", "critical": True},
        )
        self.add_result(result)
        return result
    def verify_cost_structure(
        self,
        market: Optional[str] = None,
        slippage_bp: Optional[float] = None,
        bep_percent_override: Optional[float] = None,
    ) -> VerificationResult:
        start = time.time()
        resolved_market = str(market or self.config.market).upper()
        resolved_slippage_bp = float(self.config.slippage_bp if slippage_bp is None else slippage_bp)

        if resolved_market == "KOSPI":
            tax_rate = KoreanMarketConstants.KOSPI_TOTAL_TAX_RATE
            total_cost = KoreanMarketConstants.TOTAL_COST_KOSPI
        else:
            tax_rate = KoreanMarketConstants.KOSDAQ_TOTAL_TAX_RATE
            total_cost = KoreanMarketConstants.TOTAL_COST_KOSDAQ

        round_trip = total_cost * 2
        if bep_percent_override is not None:
            bep_pct = float(bep_percent_override)
            total_with_slippage = bep_pct / 100.0
        else:
            slip = resolved_slippage_bp / 10000
            total_with_slippage = round_trip + (slip * 2)
            bep_pct = total_with_slippage * 100

        passed = bep_pct < 1.0
        warn = 0.5 <= bep_pct < 1.0

        result = self.create_result(
            item_name="Cost structure",
            passed=passed,
            description="Validate break-even expectation with taxes/fees/slippage.",
            criteria="BEP < 1.0%",
            actual_value=f"BEP={bep_pct:.3f}%",
            expected_value="< 1.0%",
            message=f"Round-trip with slippage={total_with_slippage*100:.3f}%",
            warn_condition=warn,
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "signal_to_order", "critical": False},
        )
        self.add_result(result)
        return result
    def verify_architecture_suitability(
        self,
        architecture_type: Optional[str] = None,
        api_provider: Optional[str] = None,
    ) -> VerificationResult:
        start = time.time()
        arch_type = str(architecture_type or self.config.architecture_type).upper()
        api_provider = str(api_provider or self.config.api_provider).upper()

        scores = {"SINGLE_32BIT": 40, "REST_64BIT": 70, "DUAL_PROCESS": 90}
        rec = {"KIWOOM": "DUAL_PROCESS", "EBEST": "DUAL_PROCESS", "KIS": "REST_64BIT"}

        score = scores.get(arch_type, 0)
        recommended = rec.get(api_provider, "DUAL_PROCESS")
        is_optimal = arch_type == recommended

        result = self.create_result(
            item_name="Architecture suitability",
            passed=score >= 60,
            description="Check architecture choice against API/provider constraints.",
            criteria="Suitability score >= 60",
            actual_value=f"{arch_type} ({score}/100)",
            expected_value=recommended,
            message=("Optimal architecture" if is_optimal else "Viable but not optimal"),
            warn_condition=not is_optimal and score >= 60,
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "pre_open", "critical": False},
        )
        self.add_result(result)
        return result
    def verify_api_constraints(
        self,
        api_provider: Optional[str] = None,
        target_tps: Optional[float] = None,
        api_limit_override: Optional[float] = None,
    ) -> VerificationResult:
        start = time.time()
        provider = str(api_provider or self.config.api_provider or "").upper().strip()
        resolved_target_tps = float(self.config.target_tps if target_tps is None else target_tps)
        limits = {
            "KIWOOM": KoreanMarketConstants.KIWOOM_TPS_LIMIT,
            "EBEST": KoreanMarketConstants.EBEST_TPM_LIMIT / 60,
            "KIS": KoreanMarketConstants.KIS_TPS_LIMIT,
        }

        if provider not in limits and api_limit_override is None:
            result = self.create_skipped_result(
                item_name="API rate-limit compliance",
                description="Check target order/query frequency against provider limits.",
                criteria="Known api_provider required (KIWOOM/KIS/EBEST)",
                reason=f"Unknown API provider: {provider or 'UNSPECIFIED'}",
                metadata={"checkpoint": "pre_open", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        api_limit = float(api_limit_override) if api_limit_override is not None else float(limits[provider])
        util = (resolved_target_tps / api_limit) * 100 if api_limit > 0 else 100.0
        passed = resolved_target_tps <= api_limit

        result = self.create_result(
            item_name="API rate-limit compliance",
            passed=passed,
            description="Check target order/query frequency against provider limits.",
            criteria=f"Target TPS <= provider TPS ({api_limit:.2f})",
            actual_value=f"target_tps={resolved_target_tps}",
            expected_value=f"<= {api_limit:.2f}",
            message=f"utilization={util:.1f}%",
            warn_condition=passed and util >= 70,
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "pre_open", "critical": True, "api_provider": provider},
        )
        self.add_result(result)
        return result
    def verify_market_calendar_alignment(
        self,
        trading_day: Optional[bool] = None,
        session_checklist: Optional[Dict[str, bool]] = None,
    ) -> VerificationResult:
        start = time.time()
        if trading_day is None or session_checklist is None:
            result = self.create_skipped_result(
                item_name="Market calendar alignment",
                description="Validate market session calendar/holiday/early-close controls.",
                criteria="trading_day=True and all session controls true",
                reason="No market-calendar evidence provided.",
                metadata={"checkpoint": "pre_open", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        controls_ok = all(bool(v) for v in session_checklist.values())
        passed = bool(trading_day) and controls_ok
        result = self.create_result(
            item_name="Market calendar alignment",
            passed=passed,
            description="Validate market session calendar/holiday/early-close controls.",
            criteria="trading_day=True and all session controls true",
            actual_value=f"trading_day={bool(trading_day)}, controls={sum(bool(v) for v in session_checklist.values())}/{len(session_checklist)}",
            expected_value="trading_day=True and controls all true",
            message="Session calendar controls validated" if passed else "Session calendar controls mismatch",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "pre_open", "critical": True},
        )
        self.add_result(result)
        return result
class DesignVerifier(BaseVerifier):
    def __init__(self, config: TradingSystemConfig):
        super().__init__(VerificationPhase.DESIGN)
        self.config = config
        self._watchdog_threshold_ms = 1000.0
        self._queue_lag_threshold_ms = 10.0
        self._db_query_threshold_ms = 200.0

    def verify_all(self) -> PhaseReport:
        self.results.clear()
        self.run_with_evidence("verify_process_isolation")
        self.run_with_evidence("verify_message_queue_performance")
        self.run_with_evidence("verify_db_schema_efficiency")
        self.run_with_evidence("verify_module_separation")
        self.run_with_evidence("verify_exactly_once_idempotency")
        self.run_with_evidence("verify_draft_check")
        self.run_with_evidence("verify_flowcharting")
        self.run_with_evidence("verify_dry_run")
        self.run_with_evidence("verify_edge_case_test")
        self.run_with_evidence("verify_cross_check")
        self.run_with_evidence("verify_stress_test")
        return self.generate_report()

    def _normalize_source(self, evidence_source: Optional[str]) -> str:
        src = str(evidence_source or "").strip().lower()
        return src if src else "unknown"

    def _reject_untrusted_source(
        self,
        *,
        item_name: str,
        description: str,
        criteria: str,
        evidence_source: Optional[str],
        start: float,
    ) -> Optional[VerificationResult]:
        src = self._normalize_source(evidence_source)
        untrusted = {"template", "manual", "manual_seed", "unknown", "missing", ""}
        if src not in untrusted:
            return None

        result = self.create_skipped_result(
            item_name=item_name,
            description=description,
            criteria=criteria,
            reason=f"Untrusted evidence source: {src}",
            metadata={
                "checkpoint": "pre_open",
                "critical": False,
                "evidence_source": src,
                "evidence_integrity": "untrusted",
            },
            execution_time_ms=(time.time() - start) * 1000,
        )
        return result

    def verify_process_isolation(
        self,
        watchdog_detect_time_ms: Optional[float] = None,
        evidence_source: Optional[str] = None,
    ) -> VerificationResult:
        start = time.time()
        src = self._normalize_source(evidence_source)

        if watchdog_detect_time_ms is None:
            result = self.create_skipped_result(
                item_name="Process isolation",
                description="Validate watchdog detection and isolation behavior.",
                criteria=f"watchdog_detect_time_ms < {self._watchdog_threshold_ms}",
                reason="No watchdog measurement provided.",
                metadata={"checkpoint": "pre_open", "critical": False, "evidence_source": src},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        blocked = self._reject_untrusted_source(
            item_name="Process isolation",
            description="Validate watchdog detection and isolation behavior.",
            criteria=f"watchdog_detect_time_ms < {self._watchdog_threshold_ms}",
            evidence_source=src,
            start=start,
        )
        if blocked is not None:
            self.add_result(blocked)
            return blocked

        passed = watchdog_detect_time_ms < self._watchdog_threshold_ms
        result = self.create_result(
            item_name="Process isolation",
            passed=passed,
            description="Validate watchdog detection and isolation behavior.",
            criteria=f"watchdog_detect_time_ms < {self._watchdog_threshold_ms}",
            actual_value=f"{watchdog_detect_time_ms:.1f}ms",
            expected_value=f"< {int(self._watchdog_threshold_ms)}ms",
            message="Detection within threshold" if passed else "Detection delay exceeded threshold",
            warn_condition=passed and watchdog_detect_time_ms > self._watchdog_threshold_ms * 0.7,
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "pre_open", "critical": False, "evidence_source": src, "evidence_integrity": "trusted"},
        )
        self.add_result(result)
        return result

    def verify_message_queue_performance(
        self,
        measured_lag_ms: Optional[float] = None,
        dropped_messages: Optional[int] = None,
        evidence_source: Optional[str] = None,
    ) -> VerificationResult:
        start = time.time()
        src = self._normalize_source(evidence_source)

        if measured_lag_ms is None or dropped_messages is None:
            result = self.create_skipped_result(
                item_name="Message queue performance",
                description="Validate market-data messaging latency.",
                criteria=f"lag_ms < {self._queue_lag_threshold_ms} and dropped_messages == 0",
                reason="No queue lag/drop measurements provided.",
                metadata={"checkpoint": "pre_open", "critical": False, "evidence_source": src},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        blocked = self._reject_untrusted_source(
            item_name="Message queue performance",
            description="Validate market-data messaging latency.",
            criteria=f"lag_ms < {self._queue_lag_threshold_ms} and dropped_messages == 0",
            evidence_source=src,
            start=start,
        )
        if blocked is not None:
            self.add_result(blocked)
            return blocked

        drops = int(dropped_messages or 0)
        passed = measured_lag_ms < self._queue_lag_threshold_ms and drops == 0
        result = self.create_result(
            item_name="Message queue performance",
            passed=passed,
            description="Validate market-data messaging latency.",
            criteria=f"lag_ms < {self._queue_lag_threshold_ms} and dropped_messages == 0",
            actual_value=f"{measured_lag_ms:.2f}ms (drops: {drops})",
            expected_value=f"< {int(self._queue_lag_threshold_ms)}ms, drops 0",
            message="Queue latency within threshold" if passed else "Queue lag or drops detected",
            warn_condition=passed and measured_lag_ms > self._queue_lag_threshold_ms * 0.7,
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "pre_open", "critical": False, "evidence_source": src, "evidence_integrity": "trusted"},
        )
        self.add_result(result)
        return result

    def verify_db_schema_efficiency(
        self,
        query_response_ms: Optional[float] = None,
        evidence_source: Optional[str] = None,
    ) -> VerificationResult:
        start = time.time()
        src = self._normalize_source(evidence_source)

        if query_response_ms is None:
            result = self.create_skipped_result(
                item_name="DB schema efficiency",
                description="Validate query performance for historical slices.",
                criteria=f"query_response_ms < {self._db_query_threshold_ms}",
                reason="No DB query benchmark provided.",
                metadata={"checkpoint": "pre_open", "critical": False, "evidence_source": src},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        blocked = self._reject_untrusted_source(
            item_name="DB schema efficiency",
            description="Validate query performance for historical slices.",
            criteria=f"query_response_ms < {self._db_query_threshold_ms}",
            evidence_source=src,
            start=start,
        )
        if blocked is not None:
            self.add_result(blocked)
            return blocked

        passed = query_response_ms < self._db_query_threshold_ms
        result = self.create_result(
            item_name="DB schema efficiency",
            passed=passed,
            description="Validate query performance for historical slices.",
            criteria=f"query_response_ms < {self._db_query_threshold_ms}",
            actual_value=f"{query_response_ms:.1f}ms",
            expected_value=f"< {int(self._db_query_threshold_ms)}ms",
            message="Query performance healthy" if passed else "Query latency too high",
            warn_condition=passed and query_response_ms > self._db_query_threshold_ms * 0.7,
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "pre_open", "critical": False, "evidence_source": src, "evidence_integrity": "trusted"},
        )
        self.add_result(result)
        return result

    def verify_module_separation(
        self,
        designed_modules: Optional[List[str]] = None,
        evidence_source: Optional[str] = None,
    ) -> VerificationResult:
        start = time.time()
        src = self._normalize_source(evidence_source)
        required_modules = [
            "Ingestion Agent",
            "Strategy Engine",
            "Order Manager",
            "Risk Controller",
        ]

        if designed_modules is None:
            result = self.create_skipped_result(
                item_name="Module separation",
                description="Validate architectural module decomposition.",
                criteria="All required modules explicitly defined",
                reason="No module-definition evidence provided.",
                metadata={"checkpoint": "pre_open", "critical": False, "evidence_source": src},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        blocked = self._reject_untrusted_source(
            item_name="Module separation",
            description="Validate architectural module decomposition.",
            criteria="All required modules explicitly defined",
            evidence_source=src,
            start=start,
        )
        if blocked is not None:
            self.add_result(blocked)
            return blocked

        missing = [m for m in required_modules if m not in designed_modules]
        passed = len(missing) == 0
        result = self.create_result(
            item_name="Module separation",
            passed=passed,
            description="Validate architectural module decomposition.",
            criteria="All required modules explicitly defined",
            actual_value=f"{len(designed_modules)} modules defined",
            expected_value=f"{len(required_modules)} required modules",
            message="All required modules are defined" if passed else f"Missing modules: {', '.join(missing)}",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "pre_open", "critical": False, "evidence_source": src, "evidence_integrity": "trusted"},
        )
        self.add_result(result)
        return result

    def verify_exactly_once_idempotency(
        self,
        total_submit_events: Optional[int] = None,
        unique_idempotency_keys: Optional[int] = None,
        duplicate_replays_blocked: Optional[int] = None,
        persistence_ok: Optional[bool] = None,
        evidence_source: Optional[str] = None,
    ) -> VerificationResult:
        start = time.time()
        src = self._normalize_source(evidence_source)

        if (
            total_submit_events is None
            or unique_idempotency_keys is None
            or duplicate_replays_blocked is None
            or persistence_ok is None
        ):
            result = self.create_skipped_result(
                item_name="Exactly-once idempotency",
                description="Validate idempotency-key based exactly-once order submission.",
                criteria="duplicates blocked and idempotency store persistence verified",
                reason="No idempotency evidence provided.",
                metadata={"checkpoint": "pre_order_send", "critical": True, "evidence_source": src},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        blocked = self._reject_untrusted_source(
            item_name="Exactly-once idempotency",
            description="Validate idempotency-key based exactly-once order submission.",
            criteria="duplicates blocked and idempotency store persistence verified",
            evidence_source=src,
            start=start,
        )
        if blocked is not None:
            self.add_result(blocked)
            return blocked

        total = max(0, int(total_submit_events))
        unique = max(0, int(unique_idempotency_keys))
        blocked_dup = max(0, int(duplicate_replays_blocked))
        expected_duplicates = max(total - unique, 0)
        passed = (
            unique <= total
            and blocked_dup >= expected_duplicates
            and bool(persistence_ok)
        )

        result = self.create_result(
            item_name="Exactly-once idempotency",
            passed=passed,
            description="Validate idempotency-key based exactly-once order submission.",
            criteria="duplicates blocked and idempotency store persistence verified",
            actual_value=f"submit={total}, unique_keys={unique}, blocked_dup={blocked_dup}, persistence={bool(persistence_ok)}",
            expected_value=f"blocked_dup>={expected_duplicates}, persistence=True",
            message="Idempotency controls validated" if passed else "Idempotency controls mismatch",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={
                "checkpoint": "pre_order_send",
                "critical": True,
                "evidence_source": src,
                "evidence_integrity": "trusted",
            },
        )
        self.add_result(result)
        return result
    def _apply_onboarding_gate(self, passed: bool, gate_mode: Optional[str]) -> Tuple[bool, bool, str]:
        mode = str(gate_mode or "STRICT").upper()
        if mode not in {"STRICT", "ONBOARDING"}:
            mode = "STRICT"
        bypassed = bool(mode == "ONBOARDING" and not passed)
        return (True if bypassed else bool(passed)), bypassed, mode

    def verify_draft_check(
        self,
        logic_doc_present: Optional[bool] = None,
        requirements_total: Optional[int] = None,
        requirements_covered: Optional[int] = None,
        logical_gap_count: Optional[int] = None,
        evidence_source: Optional[str] = None,
        gate_mode: str = "STRICT",
    ) -> VerificationResult:
        start = time.time()
        src = self._normalize_source(evidence_source)

        if (
            logic_doc_present is None
            or requirements_total is None
            or requirements_covered is None
            or logical_gap_count is None
        ):
            result = self.create_skipped_result(
                item_name="Draft check",
                description="Read and validate written logic for missing assumptions or logical jumps.",
                criteria="logic_doc_present=True, requirements_covered==requirements_total, logical_gap_count==0",
                reason="No draft-check evidence provided.",
                metadata={"checkpoint": "pre_open", "critical": False, "evidence_source": src},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        passed = bool(logic_doc_present) and int(requirements_covered) >= int(requirements_total) and int(logical_gap_count) == 0
        final_passed, bypassed, mode = self._apply_onboarding_gate(passed, gate_mode)
        coverage = (int(requirements_covered) / max(1, int(requirements_total))) * 100.0

        if bypassed:
            message = "Onboarding mode: draft gate bypassed"
        elif final_passed:
            message = "Draft logic validated"
        else:
            message = "Draft logic has uncovered requirements or logical gaps"

        result = self.create_result(
            item_name="Draft check",
            passed=final_passed,
            description="Read and validate written logic for missing assumptions or logical jumps.",
            criteria="logic_doc_present=True, requirements_covered==requirements_total, logical_gap_count==0",
            actual_value=(
                f"doc={bool(logic_doc_present)}, coverage={int(requirements_covered)}/{int(requirements_total)} ({coverage:.1f}%), "
                f"gaps={int(logical_gap_count)}"
            ),
            expected_value="doc=True, coverage=100%, gaps=0",
            message=message,
            warn_condition=bypassed or (final_passed and coverage < 100.0),
            execution_time_ms=(time.time() - start) * 1000,
            metadata={
                "checkpoint": "pre_open",
                "critical": False,
                "evidence_source": src,
                "evidence_integrity": "trusted",
                "gate_mode": mode,
                "gate_bypassed": bypassed,
            },
        )
        self.add_result(result)
        return result

    def verify_flowcharting(
        self,
        flowchart_present: Optional[bool] = None,
        flowchart_artifact_count: Optional[int] = None,
        loop_violation_count: Optional[int] = None,
        dead_end_count: Optional[int] = None,
        evidence_source: Optional[str] = None,
        gate_mode: str = "STRICT",
    ) -> VerificationResult:
        start = time.time()
        src = self._normalize_source(evidence_source)

        if (
            flowchart_present is None
            or flowchart_artifact_count is None
            or loop_violation_count is None
            or dead_end_count is None
        ):
            result = self.create_skipped_result(
                item_name="Flowcharting",
                description="Visualize logic flow to detect loops, dead-ends, and bottlenecks.",
                criteria="flowchart_present=True, loop_violation_count==0, dead_end_count==0",
                reason="No flowchart evidence provided.",
                metadata={"checkpoint": "pre_open", "critical": False, "evidence_source": src},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        passed = bool(flowchart_present) and int(loop_violation_count) == 0 and int(dead_end_count) == 0
        final_passed, bypassed, mode = self._apply_onboarding_gate(passed, gate_mode)

        if bypassed:
            message = "Onboarding mode: flowchart gate bypassed"
        elif final_passed:
            message = "Flowchart integrity validated"
        else:
            message = "Flowchart has loop/dead-end or missing artifact"

        result = self.create_result(
            item_name="Flowcharting",
            passed=final_passed,
            description="Visualize logic flow to detect loops, dead-ends, and bottlenecks.",
            criteria="flowchart_present=True, loop_violation_count==0, dead_end_count==0",
            actual_value=(
                f"present={bool(flowchart_present)}, artifacts={int(flowchart_artifact_count)}, "
                f"loop_violations={int(loop_violation_count)}, dead_ends={int(dead_end_count)}"
            ),
            expected_value="present=True, artifacts>=1, loop_violations=0, dead_ends=0",
            message=message,
            warn_condition=bypassed or (final_passed and int(flowchart_artifact_count) < 2),
            execution_time_ms=(time.time() - start) * 1000,
            metadata={
                "checkpoint": "pre_open",
                "critical": False,
                "evidence_source": src,
                "evidence_integrity": "trusted",
                "gate_mode": mode,
                "gate_bypassed": bypassed,
            },
        )
        self.add_result(result)
        return result

    def verify_dry_run(
        self,
        sample_case_count: Optional[int] = None,
        matched_case_count: Optional[int] = None,
        calculation_error_count: Optional[int] = None,
        min_sample_cases: int = 10,
        evidence_source: Optional[str] = None,
        gate_mode: str = "STRICT",
    ) -> VerificationResult:
        start = time.time()
        src = self._normalize_source(evidence_source)

        if sample_case_count is None or matched_case_count is None or calculation_error_count is None:
            result = self.create_skipped_result(
                item_name="Dry run",
                description="Run hand-checkable sample data before full-scale execution.",
                criteria="sample_case_count>=min_sample_cases and matched_case_count==sample_case_count and errors==0",
                reason="No dry-run evidence provided.",
                metadata={"checkpoint": "pre_open", "critical": False, "evidence_source": src},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        total = max(0, int(sample_case_count))
        matched = max(0, int(matched_case_count))
        errors = max(0, int(calculation_error_count))
        passed = total >= int(min_sample_cases) and matched == total and errors == 0
        final_passed, bypassed, mode = self._apply_onboarding_gate(passed, gate_mode)

        if bypassed:
            message = "Onboarding mode: dry-run gate bypassed"
        elif final_passed:
            message = "Dry-run consistency validated"
        else:
            message = "Dry-run mismatch or insufficient sample"

        result = self.create_result(
            item_name="Dry run",
            passed=final_passed,
            description="Run hand-checkable sample data before full-scale execution.",
            criteria="sample_case_count>=min_sample_cases and matched_case_count==sample_case_count and errors==0",
            actual_value=f"sample={total}, matched={matched}, errors={errors}",
            expected_value=f"sample>={int(min_sample_cases)}, matched=sample, errors=0",
            message=message,
            warn_condition=bypassed or (final_passed and total < int(min_sample_cases) + 5),
            execution_time_ms=(time.time() - start) * 1000,
            metadata={
                "checkpoint": "pre_open",
                "critical": False,
                "evidence_source": src,
                "evidence_integrity": "trusted",
                "gate_mode": mode,
                "gate_bypassed": bypassed,
            },
        )
        self.add_result(result)
        return result

    def verify_edge_case_test(
        self,
        tested_case_count: Optional[int] = None,
        passed_case_count: Optional[int] = None,
        critical_fail_count: Optional[int] = None,
        has_null_case: Optional[bool] = None,
        has_extreme_case: Optional[bool] = None,
        evidence_source: Optional[str] = None,
        gate_mode: str = "STRICT",
    ) -> VerificationResult:
        start = time.time()
        src = self._normalize_source(evidence_source)

        if (
            tested_case_count is None
            or passed_case_count is None
            or critical_fail_count is None
            or has_null_case is None
            or has_extreme_case is None
        ):
            result = self.create_skipped_result(
                item_name="Edge case test",
                description="Validate behavior on null, extreme, and pathological inputs.",
                criteria="critical_fail_count==0, has_null_case=True, has_extreme_case=True",
                reason="No edge-case evidence provided.",
                metadata={"checkpoint": "pre_open", "critical": False, "evidence_source": src},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        tested = max(0, int(tested_case_count))
        passed_cases = max(0, int(passed_case_count))
        critical_fail = max(0, int(critical_fail_count))
        passed = (
            tested > 0
            and passed_cases == tested
            and critical_fail == 0
            and bool(has_null_case)
            and bool(has_extreme_case)
        )
        final_passed, bypassed, mode = self._apply_onboarding_gate(passed, gate_mode)

        if bypassed:
            message = "Onboarding mode: edge-case gate bypassed"
        elif final_passed:
            message = "Edge-case robustness validated"
        else:
            message = "Edge-case coverage or outcome insufficient"

        result = self.create_result(
            item_name="Edge case test",
            passed=final_passed,
            description="Validate behavior on null, extreme, and pathological inputs.",
            criteria="critical_fail_count==0, has_null_case=True, has_extreme_case=True",
            actual_value=(
                f"tested={tested}, passed={passed_cases}, critical_fail={critical_fail}, "
                f"null_case={bool(has_null_case)}, extreme_case={bool(has_extreme_case)}"
            ),
            expected_value="tested>0, passed=tested, critical_fail=0, null_case=True, extreme_case=True",
            message=message,
            warn_condition=bypassed or (final_passed and tested < 8),
            execution_time_ms=(time.time() - start) * 1000,
            metadata={
                "checkpoint": "pre_open",
                "critical": False,
                "evidence_source": src,
                "evidence_integrity": "trusted",
                "gate_mode": mode,
                "gate_bypassed": bypassed,
            },
        )
        self.add_result(result)
        return result

    def verify_cross_check(
        self,
        sources_compared: Optional[int] = None,
        mismatch_count: Optional[int] = None,
        tolerance_breach_count: Optional[int] = None,
        evidence_source: Optional[str] = None,
        gate_mode: str = "STRICT",
    ) -> VerificationResult:
        start = time.time()
        src = self._normalize_source(evidence_source)

        if sources_compared is None or mismatch_count is None or tolerance_breach_count is None:
            result = self.create_skipped_result(
                item_name="Cross-check",
                description="Reconcile independent outputs to detect accounting/execution divergence.",
                criteria="sources_compared>=2 and mismatch_count==0 and tolerance_breach_count==0",
                reason="No cross-check evidence provided.",
                metadata={"checkpoint": "post_close", "critical": True, "evidence_source": src},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        sources = max(0, int(sources_compared))
        mismatch = max(0, int(mismatch_count))
        tol = max(0, int(tolerance_breach_count))
        passed = sources >= 2 and mismatch == 0 and tol == 0
        final_passed, bypassed, mode = self._apply_onboarding_gate(passed, gate_mode)

        if bypassed:
            message = "Onboarding mode: cross-check gate bypassed"
        elif final_passed:
            message = "Cross-check reconciliation validated"
        else:
            message = "Cross-check mismatch detected"

        result = self.create_result(
            item_name="Cross-check",
            passed=final_passed,
            description="Reconcile independent outputs to detect accounting/execution divergence.",
            criteria="sources_compared>=2 and mismatch_count==0 and tolerance_breach_count==0",
            actual_value=f"sources={sources}, mismatch={mismatch}, tol_breach={tol}",
            expected_value="sources>=2, mismatch=0, tol_breach=0",
            message=message,
            warn_condition=bypassed or (final_passed and sources == 2),
            execution_time_ms=(time.time() - start) * 1000,
            metadata={
                "checkpoint": "post_close",
                "critical": True,
                "evidence_source": src,
                "evidence_integrity": "trusted",
                "gate_mode": mode,
                "gate_bypassed": bypassed,
            },
        )
        self.add_result(result)
        return result

    def verify_stress_test(
        self,
        records_tested: Optional[int] = None,
        p95_latency_ms: Optional[float] = None,
        error_rate_percent: Optional[float] = None,
        min_records: int = 100,
        max_p95_latency_ms: float = 500.0,
        max_error_rate_percent: float = 1.0,
        evidence_source: Optional[str] = None,
        gate_mode: str = "STRICT",
    ) -> VerificationResult:
        start = time.time()
        src = self._normalize_source(evidence_source)

        if records_tested is None or p95_latency_ms is None or error_rate_percent is None:
            result = self.create_skipped_result(
                item_name="Stress test",
                description="Validate throughput and resource behavior under high-load conditions.",
                criteria="records_tested>=min_records, p95_latency_ms<=max_p95_latency_ms, error_rate_percent<=max_error_rate_percent",
                reason="No stress-test evidence provided.",
                metadata={"checkpoint": "pre_open", "critical": False, "evidence_source": src},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        records = max(0, int(records_tested))
        p95 = float(p95_latency_ms)
        error_rate = float(error_rate_percent)
        passed = (
            records >= int(min_records)
            and p95 <= float(max_p95_latency_ms)
            and error_rate <= float(max_error_rate_percent)
        )
        final_passed, bypassed, mode = self._apply_onboarding_gate(passed, gate_mode)

        if bypassed:
            message = "Onboarding mode: stress-test gate bypassed"
        elif final_passed:
            message = "Stress-test performance validated"
        else:
            message = "Stress-test threshold breached"

        result = self.create_result(
            item_name="Stress test",
            passed=final_passed,
            description="Validate throughput and resource behavior under high-load conditions.",
            criteria="records_tested>=min_records, p95_latency_ms<=max_p95_latency_ms, error_rate_percent<=max_error_rate_percent",
            actual_value=f"records={records}, p95={p95:.1f}ms, error_rate={error_rate:.2f}%",
            expected_value=(
                f"records>={int(min_records)}, p95<={float(max_p95_latency_ms):.1f}ms, "
                f"error_rate<={float(max_error_rate_percent):.2f}%"
            ),
            message=message,
            warn_condition=bypassed or (
                final_passed and (
                    records < int(min_records) * 2
                    or p95 > float(max_p95_latency_ms) * 0.8
                    or error_rate > float(max_error_rate_percent) * 0.7
                )
            ),
            execution_time_ms=(time.time() - start) * 1000,
            metadata={
                "checkpoint": "pre_open",
                "critical": False,
                "evidence_source": src,
                "evidence_integrity": "trusted",
                "gate_mode": mode,
                "gate_bypassed": bypassed,
            },
        )
        self.add_result(result)
        return result
