"""Phase 7 and 8 verifiers (Testing, Operations)."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .base import (
    BaseVerifier,
    KoreanMarketConstants,
    PhaseReport,
    TradingSystemConfig,
    VerificationResult,
    VerificationPhase,
)


@dataclass
class TradeRecord:
    trade_id: str
    symbol: str
    quantity: int
    price: float


@dataclass
class ConnectionLog:
    event_type: str
    success: bool


class TestingVerifier(BaseVerifier):
    def __init__(self, config: TradingSystemConfig):
        super().__init__(VerificationPhase.TESTING)
        self.config = config

    def verify_all(self) -> PhaseReport:
        self.results.clear()
        self.run_with_evidence("verify_api_connection_stability")
        self.run_with_evidence("verify_execution_consistency")
        self.run_with_evidence("verify_broker_statement_reconciliation")
        self.run_with_evidence("verify_tax_fee_calculation")
        self.run_with_evidence("verify_paper_trading_awareness")
        self.run_with_evidence("verify_canary_deployment_readiness")
        self.run_with_evidence("verify_realtime_websocket_pipeline")
        self.run_with_evidence("verify_emergency_full_liquidation")
        self.run_with_evidence("verify_alert_channel_delivery")
        self.run_with_evidence("verify_soak_test_automation")
        return self.generate_report()

    def verify_api_connection_stability(
        self,
        connection_logs: Optional[List[ConnectionLog]] = None,
        test_duration_hours: float = 24.0,
        data_staleness_p95_ms: Optional[float] = None,
        data_staleness_p99_ms: Optional[float] = None,
        ack_latency_p95_ms: Optional[float] = None,
        fill_latency_p95_ms: Optional[float] = None,
    ) -> VerificationResult:
        start = time.time()
        if connection_logs is None:
            result = self.create_skipped_result(
                item_name="API connection stability",
                description="Validate disconnect/reconnect behavior during market operation.",
                criteria="if disconnects occur, each must have successful reconnect evidence",
                reason="No connection log evidence provided.",
                metadata={"checkpoint": "pre_open", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        disconnect_count = sum(1 for log in connection_logs if log.event_type.upper() == "DISCONNECT")
        reconnect_logs = [log for log in connection_logs if log.event_type.upper() == "RECONNECT"]
        reconnect_count = len(reconnect_logs)
        reconnect_success = all(log.success for log in reconnect_logs) if reconnect_logs else False

        if disconnect_count == 0:
            passed = True
        else:
            passed = reconnect_count >= disconnect_count and reconnect_success

        observability_parts = []
        if data_staleness_p95_ms is not None:
            observability_parts.append(f"staleness_p95={float(data_staleness_p95_ms):.1f}ms")
        if data_staleness_p99_ms is not None:
            observability_parts.append(f"staleness_p99={float(data_staleness_p99_ms):.1f}ms")
        if ack_latency_p95_ms is not None:
            observability_parts.append(f"ack_p95={float(ack_latency_p95_ms):.1f}ms")
        if fill_latency_p95_ms is not None:
            observability_parts.append(f"fill_p95={float(fill_latency_p95_ms):.1f}ms")
        obs_suffix = f", {', '.join(observability_parts)}" if observability_parts else ""

        result = self.create_result(
            item_name="API connection stability",
            passed=passed,
            description="Validate disconnect/reconnect behavior during market operation.",
            criteria="if disconnects occur, each must have successful reconnect evidence",
            actual_value=(
                f"disconnects={disconnect_count}, reconnects={reconnect_count}, reconnect_success={reconnect_success}"
                f"{obs_suffix}"
            ),
            expected_value="disconnect=0 or reconnect>=disconnect and success",
            message="Connection recovery validated" if passed else "Reconnect evidence insufficient",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={
                "duration_hours": test_duration_hours,
                "checkpoint": "pre_open",
                "critical": True,
                "data_staleness_p95_ms": data_staleness_p95_ms,
                "data_staleness_p99_ms": data_staleness_p99_ms,
                "ack_latency_p95_ms": ack_latency_p95_ms,
                "fill_latency_p95_ms": fill_latency_p95_ms,
            },
        )
        self.add_result(result)
        return result

    def verify_execution_consistency(
        self,
        program_trades: Optional[List[TradeRecord]] = None,
        hts_trades: Optional[List[TradeRecord]] = None,
        match_rate_override: Optional[float] = None,
        total_trades_override: Optional[int] = None,
        is_drift_bps: Optional[float] = None,
        max_is_drift_warn_bps: float = 30.0,
    ) -> VerificationResult:
        start = time.time()

        if match_rate_override is not None:
            total = int(total_trades_override or 0)
            passed = abs(float(match_rate_override) - 100.0) < 1e-9
            drift_warn = is_drift_bps is not None and abs(float(is_drift_bps)) > float(max_is_drift_warn_bps)
            drift_text = f", is_drift={float(is_drift_bps):.2f}bps" if is_drift_bps is not None else ""
            result = self.create_result(
                item_name="Execution consistency",
                passed=passed,
                description="Validate fill consistency between program DB and broker/HTS records.",
                criteria="match rate must be 100% for compared trades",
                actual_value=f"match_rate={float(match_rate_override):.1f}% ({total} trades){drift_text}",
                expected_value="100%",
                message="Execution records fully consistent" if passed else "Execution records mismatch",
                warn_condition=passed and drift_warn,
                execution_time_ms=(time.time() - start) * 1000,
                metadata={
                    "checkpoint": "post_close",
                    "critical": True,
                    "is_drift_bps": is_drift_bps,
                    "max_is_drift_warn_bps": max_is_drift_warn_bps,
                },
            )
            self.add_result(result)
            return result

        if program_trades is None or hts_trades is None:
            result = self.create_skipped_result(
                item_name="Execution consistency",
                description="Validate fill consistency between program DB and broker/HTS records.",
                criteria="match rate must be 100% for compared trades",
                reason="Program/HTS trade evidence missing.",
                metadata={"checkpoint": "post_close", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        hts_by_id = {t.trade_id: t for t in hts_trades}
        mismatches: List[str] = []

        for trade in program_trades:
            peer = hts_by_id.get(trade.trade_id)
            if peer is None:
                mismatches.append(f"missing:{trade.trade_id}")
                continue
            if trade.quantity != peer.quantity or abs(trade.price - peer.price) > 1e-6:
                mismatches.append(f"diff:{trade.trade_id}")

        total = len(program_trades)
        matched = total - len(mismatches)
        match_rate = (matched / total * 100) if total > 0 else 0.0
        passed = len(mismatches) == 0
        drift_warn = is_drift_bps is not None and abs(float(is_drift_bps)) > float(max_is_drift_warn_bps)
        drift_text = f", is_drift={float(is_drift_bps):.2f}bps" if is_drift_bps is not None else ""

        result = self.create_result(
            item_name="Execution consistency",
            passed=passed,
            description="Validate fill consistency between program DB and broker/HTS records.",
            criteria="match rate must be 100% for compared trades",
            actual_value=f"match_rate={match_rate:.1f}% ({matched}/{total}){drift_text}",
            expected_value="100%",
            message="Execution records fully consistent" if passed else ", ".join(mismatches),
            warn_condition=passed and drift_warn,
            execution_time_ms=(time.time() - start) * 1000,
            metadata={
                "checkpoint": "post_close",
                "critical": True,
                "is_drift_bps": is_drift_bps,
                "max_is_drift_warn_bps": max_is_drift_warn_bps,
            },
        )
        self.add_result(result)
        return result
    def verify_broker_statement_reconciliation(
        self,
        statement_total_trades: Optional[int] = None,
        internal_total_trades: Optional[int] = None,
        statement_net_pnl: Optional[float] = None,
        internal_net_pnl: Optional[float] = None,
        pnl_tolerance_krw: float = 1.0,
    ) -> VerificationResult:
        start = time.time()
        if (
            statement_total_trades is None
            or internal_total_trades is None
            or statement_net_pnl is None
            or internal_net_pnl is None
        ):
            result = self.create_skipped_result(
                item_name="Broker statement reconciliation",
                description="Validate T+1 broker statement reconciliation with internal ledger.",
                criteria="trade count exact match and net PnL diff within tolerance",
                reason="No broker-statement reconciliation evidence provided.",
                metadata={"checkpoint": "post_close", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        trades_match = int(statement_total_trades) == int(internal_total_trades)
        pnl_diff = abs(float(statement_net_pnl) - float(internal_net_pnl))
        passed = trades_match and pnl_diff <= float(pnl_tolerance_krw)

        result = self.create_result(
            item_name="Broker statement reconciliation",
            passed=passed,
            description="Validate T+1 broker statement reconciliation with internal ledger.",
            criteria="trade count exact match and net PnL diff within tolerance",
            actual_value=(
                f"stmt_trades={int(statement_total_trades)}, internal_trades={int(internal_total_trades)}, "
                f"stmt_pnl={float(statement_net_pnl):.2f}, internal_pnl={float(internal_net_pnl):.2f}, diff={pnl_diff:.2f}"
            ),
            expected_value=f"trades_match=True, pnl_diff<={float(pnl_tolerance_krw):.2f}",
            message="Broker statement reconciliation validated" if passed else "Broker statement reconciliation mismatch",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "post_close", "critical": True},
        )
        self.add_result(result)
        return result
    def verify_tax_fee_calculation(
        self,
        trade_price: float = 70000,
        quantity: int = 1,
        market: Optional[str] = None,
        actual_total_cost: Optional[float] = None,
    ) -> VerificationResult:
        start = time.time()
        if actual_total_cost is None:
            result = self.create_skipped_result(
                item_name="Tax/fee calculation",
                description="Validate settlement cost calculation against actual statement data.",
                criteria="absolute error <= 1.0 KRW",
                reason="No actual total-cost evidence provided.",
                metadata={"checkpoint": "post_close", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        mkt = market or self.config.market
        tax_rate = (
            KoreanMarketConstants.KOSPI_TOTAL_TAX_RATE
            if mkt == "KOSPI"
            else KoreanMarketConstants.KOSDAQ_TOTAL_TAX_RATE
        )
        trade_amount = trade_price * quantity
        expected_tax = trade_amount * tax_rate
        expected_fee = trade_amount * KoreanMarketConstants.KRX_FEE_RATE
        expected_broker = trade_amount * KoreanMarketConstants.TYPICAL_BROKER_FEE
        expected_total = expected_tax + expected_fee + expected_broker

        diff = abs(actual_total_cost - expected_total)
        passed = diff <= 1.0

        result = self.create_result(
            item_name="Tax/fee calculation",
            passed=passed,
            description="Validate settlement cost calculation against actual statement data.",
            criteria="absolute error <= 1.0 KRW",
            actual_value=f"actual={actual_total_cost:.2f}, expected={expected_total:.2f}, diff={diff:.2f}",
            expected_value="diff <= 1.00",
            message="Cost calculation accurate" if passed else "Cost calculation mismatch",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "post_close", "critical": True},
        )
        self.add_result(result)
        return result

    def verify_paper_trading_awareness(
        self,
        limitations_acknowledged: Optional[Dict[str, bool]] = None,
        usage_guidelines: Optional[Dict[str, bool]] = None,
    ) -> VerificationResult:
        start = time.time()
        if limitations_acknowledged is None or usage_guidelines is None:
            result = self.create_skipped_result(
                item_name="Paper-trading awareness",
                description="Confirm known limits of mock trading and intended usage scope.",
                criteria="all limitation/guideline flags acknowledged",
                reason="No awareness checklist evidence provided.",
                metadata={"checkpoint": "post_close", "critical": False},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        passed = all(limitations_acknowledged.values()) and all(usage_guidelines.values())
        result = self.create_result(
            item_name="Paper-trading awareness",
            passed=passed,
            description="Confirm known limits of mock trading and intended usage scope.",
            criteria="all limitation/guideline flags acknowledged",
            actual_value=f"limits={sum(limitations_acknowledged.values())}/{len(limitations_acknowledged)}, guides={sum(usage_guidelines.values())}/{len(usage_guidelines)}",
            expected_value="all true",
            message="Awareness checklist complete" if passed else "Awareness checklist incomplete",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "post_close", "critical": False},
        )
        self.add_result(result)
        return result

    def verify_canary_deployment_readiness(self, canary_checklist: Optional[Dict[str, bool]] = None) -> VerificationResult:
        start = time.time()
        if canary_checklist is None:
            result = self.create_skipped_result(
                item_name="Canary readiness",
                description="Validate minimal-size deployment readiness checklist.",
                criteria="all canary checklist items are true",
                reason="No canary-checklist evidence provided.",
                metadata={"checkpoint": "pre_open", "critical": False},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        passed = all(canary_checklist.values())
        result = self.create_result(
            item_name="Canary readiness",
            passed=passed,
            description="Validate minimal-size deployment readiness checklist.",
            criteria="all canary checklist items are true",
            actual_value=f"{sum(canary_checklist.values())}/{len(canary_checklist)}",
            expected_value="all passed",
            message="Canary checklist complete" if passed else "Canary checklist has gaps",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "pre_open", "critical": False},
        )
        self.add_result(result)
        return result


    def verify_realtime_websocket_pipeline(
        self,
        ws_status: Optional[Dict[str, Any]] = None,
        min_total_messages: int = 1,
    ) -> VerificationResult:
        start = time.time()
        if ws_status is None:
            result = self.create_skipped_result(
                item_name="Realtime websocket pipeline",
                description="Validate websocket stream is connected and receiving messages.",
                criteria="status is STREAMING/DONE and total messages >= minimum",
                reason="No websocket status evidence provided.",
                metadata={"checkpoint": "pre_open", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        status = str(ws_status.get("status", "")).upper()
        total_msgs = int(ws_status.get("total_msgs", ws_status.get("total_messages", 0)) or 0)
        reconnects = int(ws_status.get("reconnects", ws_status.get("reconnect", 0)) or 0)

        passed = status in {"STREAMING", "DONE", "RUN_FOREVER"} and total_msgs >= int(min_total_messages)
        result = self.create_result(
            item_name="Realtime websocket pipeline",
            passed=passed,
            description="Validate websocket stream is connected and receiving messages.",
            criteria="status is STREAMING/DONE and total messages >= minimum",
            actual_value=f"status={status}, total_msgs={total_msgs}, reconnects={reconnects}",
            expected_value=f"status in STREAMING/DONE/RUN_FOREVER and total_msgs>={int(min_total_messages)}",
            message="Realtime websocket stream healthy" if passed else "Realtime websocket stream evidence insufficient",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "pre_open", "critical": True},
        )
        self.add_result(result)
        return result

    def verify_emergency_full_liquidation(
        self,
        emergency_checklist: Optional[Dict[str, bool]] = None,
        last_dry_run_ok: Optional[bool] = None,
    ) -> VerificationResult:
        start = time.time()
        if emergency_checklist is None or last_dry_run_ok is None:
            result = self.create_skipped_result(
                item_name="Emergency full liquidation",
                description="Validate emergency all-sell path and prerequisite controls.",
                criteria="all checklist items true and latest dry-run successful",
                reason="No emergency liquidation evidence provided.",
                metadata={"checkpoint": "pre_open", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        passed = bool(last_dry_run_ok) and all(bool(v) for v in emergency_checklist.values())
        result = self.create_result(
            item_name="Emergency full liquidation",
            passed=passed,
            description="Validate emergency all-sell path and prerequisite controls.",
            criteria="all checklist items true and latest dry-run successful",
            actual_value=f"checklist={sum(bool(v) for v in emergency_checklist.values())}/{len(emergency_checklist)}, dry_run_ok={bool(last_dry_run_ok)}",
            expected_value="all true + dry_run_ok=True",
            message="Emergency liquidation readiness validated" if passed else "Emergency liquidation readiness has gaps",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "pre_open", "critical": True},
        )
        self.add_result(result)
        return result

    def verify_alert_channel_delivery(
        self,
        alert_channels: Optional[Dict[str, bool]] = None,
        delivery_check: Optional[Dict[str, bool]] = None,
    ) -> VerificationResult:
        start = time.time()
        if alert_channels is None or delivery_check is None:
            result = self.create_skipped_result(
                item_name="Alert channel delivery",
                description="Validate alert channels are enabled and delivery smoke test passed.",
                criteria="at least one channel active and all delivery checks true",
                reason="No alert delivery evidence provided.",
                metadata={"checkpoint": "post_close", "critical": False},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        has_channel = any(bool(v) for v in alert_channels.values())
        delivery_ok = all(bool(v) for v in delivery_check.values())
        passed = has_channel and delivery_ok

        result = self.create_result(
            item_name="Alert channel delivery",
            passed=passed,
            description="Validate alert channels are enabled and delivery smoke test passed.",
            criteria="at least one channel active and all delivery checks true",
            actual_value=f"channels={sum(bool(v) for v in alert_channels.values())}, delivery={sum(bool(v) for v in delivery_check.values())}/{len(delivery_check)}",
            expected_value=">=1 active channel and all delivery checks true",
            message="Alert channel delivery validated" if passed else "Alert delivery coverage incomplete",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "post_close", "critical": False},
        )
        self.add_result(result)
        return result

    def verify_soak_test_automation(
        self,
        soak_summary: Optional[Dict[str, Any]] = None,
        min_duration_hours: float = 24.0,
        max_fail_ratio: float = 0.05,
    ) -> VerificationResult:
        start = time.time()
        if soak_summary is None:
            result = self.create_skipped_result(
                item_name="Soak test automation",
                description="Validate long-run soak automation and failure thresholds.",
                criteria="duration >= minimum, fail_ratio <= threshold, and summary ok=true",
                reason="No soak-test summary evidence provided.",
                metadata={"checkpoint": "post_close", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        duration_h = float(soak_summary.get("duration_hours", 0.0) or 0.0)
        fail_ratio = float(soak_summary.get("fail_ratio", 1.0) or 1.0)
        ok_flag = bool(soak_summary.get("ok", False))
        iterations = int(soak_summary.get("iterations", 0) or 0)

        passed = ok_flag and duration_h >= float(min_duration_hours) and fail_ratio <= float(max_fail_ratio)
        result = self.create_result(
            item_name="Soak test automation",
            passed=passed,
            description="Validate long-run soak automation and failure thresholds.",
            criteria="duration >= minimum, fail_ratio <= threshold, and summary ok=true",
            actual_value=f"duration_h={duration_h:.2f}, fail_ratio={fail_ratio:.4f}, iterations={iterations}, ok={ok_flag}",
            expected_value=f"duration_h>={float(min_duration_hours):.2f}, fail_ratio<={float(max_fail_ratio):.4f}, ok=True",
            message="Soak test automation validated" if passed else "Soak test automation criteria not met",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "post_close", "critical": True},
        )
        self.add_result(result)
        return result

class OperationsVerifier(BaseVerifier):
    def __init__(self, config: TradingSystemConfig):
        super().__init__(VerificationPhase.OPERATIONS)
        self.config = config

    def verify_all(self) -> PhaseReport:
        self.results.clear()
        self.run_with_evidence("verify_auto_reconnection")
        self.run_with_evidence("verify_log_integrity")
        self.run_with_evidence("verify_data_backup")
        self.run_with_evidence("verify_backup_drill_runbook")
        self.run_with_evidence("verify_scheduler")
        self.run_with_evidence("verify_monitoring_alerts")
        return self.generate_report()

    def verify_auto_reconnection(
        self,
        reconnection_time_ms: Optional[float] = None,
        reconnection_steps: Optional[Dict[str, bool]] = None,
    ) -> VerificationResult:
        start = time.time()
        max_ms = 10000.0
        if reconnection_time_ms is None or reconnection_steps is None:
            result = self.create_skipped_result(
                item_name="Auto reconnection",
                description="Validate automatic reconnect runbook and timing.",
                criteria=f"reconnection <= {max_ms}ms and all steps true",
                reason="No reconnection evidence provided.",
                metadata={"checkpoint": "pre_open", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        passed = reconnection_time_ms <= max_ms and all(reconnection_steps.values())
        result = self.create_result(
            item_name="Auto reconnection",
            passed=passed,
            description="Validate automatic reconnect runbook and timing.",
            criteria=f"reconnection <= {max_ms}ms and all steps true",
            actual_value=f"time={reconnection_time_ms:.1f}ms, steps={sum(reconnection_steps.values())}/{len(reconnection_steps)}",
            expected_value="within threshold and all true",
            message="Auto reconnection acceptable" if passed else "Auto reconnection gaps detected",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "pre_open", "critical": True},
        )
        self.add_result(result)
        return result

    def verify_log_integrity(
        self,
        error_logs_with_alerts: Optional[int] = None,
        total_error_logs: Optional[int] = None,
        log_management: Optional[Dict[str, bool]] = None,
    ) -> VerificationResult:
        start = time.time()
        if error_logs_with_alerts is None or total_error_logs is None or log_management is None:
            result = self.create_skipped_result(
                item_name="Log integrity",
                description="Validate alerting coverage for error/critical logs.",
                criteria="alert rate = 100% and log-management checklist all true",
                reason="No log integrity evidence provided.",
                metadata={"checkpoint": "post_close", "critical": False},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        alert_rate = (error_logs_with_alerts / total_error_logs * 100) if total_error_logs > 0 else 0.0
        passed = alert_rate == 100.0 and all(log_management.values())

        result = self.create_result(
            item_name="Log integrity",
            passed=passed,
            description="Validate alerting coverage for error/critical logs.",
            criteria="alert rate = 100% and log-management checklist all true",
            actual_value=f"alert_rate={alert_rate:.1f}%",
            expected_value="100.0%",
            message="Log integrity checks passed" if passed else "Log integrity checks failed",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "post_close", "critical": False},
        )
        self.add_result(result)
        return result

    def verify_data_backup(
        self,
        recovery_time_minutes: Optional[float] = None,
        backup_checklist: Optional[Dict[str, bool]] = None,
    ) -> VerificationResult:
        start = time.time()
        max_minutes = 60.0
        if recovery_time_minutes is None or backup_checklist is None:
            result = self.create_skipped_result(
                item_name="Data backup",
                description="Validate backup and restore readiness.",
                criteria=f"restore <= {max_minutes} minutes and checklist all true",
                reason="No backup/restore evidence provided.",
                metadata={"checkpoint": "post_close", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        passed = recovery_time_minutes <= max_minutes and all(backup_checklist.values())
        result = self.create_result(
            item_name="Data backup",
            passed=passed,
            description="Validate backup and restore readiness.",
            criteria=f"restore <= {max_minutes} minutes and checklist all true",
            actual_value=f"restore={recovery_time_minutes:.1f}m, checklist={sum(backup_checklist.values())}/{len(backup_checklist)}",
            expected_value="within threshold and all true",
            message="Backup and recovery acceptable" if passed else "Backup/recovery gaps detected",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "post_close", "critical": True},
        )
        self.add_result(result)
        return result
    def verify_backup_drill_runbook(
        self,
        last_drill_days_ago: Optional[int] = None,
        rpo_minutes: Optional[float] = None,
        rto_minutes: Optional[float] = None,
        runbook_checklist: Optional[Dict[str, bool]] = None,
    ) -> VerificationResult:
        start = time.time()
        if (
            last_drill_days_ago is None
            or rpo_minutes is None
            or rto_minutes is None
            or runbook_checklist is None
        ):
            result = self.create_skipped_result(
                item_name="Backup drill runbook",
                description="Validate periodic backup-restore drill and operational runbook execution.",
                criteria="drill recency, RPO/RTO thresholds, and runbook checklist all satisfied",
                reason="No backup-drill/runbook evidence provided.",
                metadata={"checkpoint": "post_close", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        max_drill_days = 30
        max_rpo = 15.0
        max_rto = 60.0

        passed = (
            int(last_drill_days_ago) <= max_drill_days
            and float(rpo_minutes) <= max_rpo
            and float(rto_minutes) <= max_rto
            and all(bool(v) for v in runbook_checklist.values())
        )

        result = self.create_result(
            item_name="Backup drill runbook",
            passed=passed,
            description="Validate periodic backup-restore drill and operational runbook execution.",
            criteria="drill recency, RPO/RTO thresholds, and runbook checklist all satisfied",
            actual_value=(
                f"drill_days={int(last_drill_days_ago)}, rpo={float(rpo_minutes):.1f}m, "
                f"rto={float(rto_minutes):.1f}m, checklist={sum(bool(v) for v in runbook_checklist.values())}/{len(runbook_checklist)}"
            ),
            expected_value=f"drill<=30d, rpo<=15.0m, rto<=60.0m, checklist=all true",
            message="Backup drill/runbook validated" if passed else "Backup drill/runbook mismatch",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "post_close", "critical": True},
        )
        self.add_result(result)
        return result
    def verify_scheduler(
        self,
        schedule_items: Optional[Dict[str, bool]] = None,
        auto_login_config: Optional[Dict[str, bool]] = None,
    ) -> VerificationResult:
        start = time.time()
        if schedule_items is None or auto_login_config is None:
            result = self.create_skipped_result(
                item_name="Operations scheduler",
                description="Validate automated market-session schedule and secure startup config.",
                criteria="all schedule and auto-login controls true",
                reason="No scheduling evidence provided.",
                metadata={"checkpoint": "pre_open", "critical": False},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        passed = all(schedule_items.values()) and all(auto_login_config.values())
        result = self.create_result(
            item_name="Operations scheduler",
            passed=passed,
            description="Validate automated market-session schedule and secure startup config.",
            criteria="all schedule and auto-login controls true",
            actual_value=f"schedule={sum(schedule_items.values())}/{len(schedule_items)}, login={sum(auto_login_config.values())}/{len(auto_login_config)}",
            expected_value="all true",
            message="Scheduler configuration complete" if passed else "Scheduler configuration incomplete",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "pre_open", "critical": False},
        )
        self.add_result(result)
        return result

    def verify_monitoring_alerts(
        self,
        alert_channels: Optional[Dict[str, bool]] = None,
        required_alerts: Optional[Dict[str, bool]] = None,
        best_execution_review_days_ago: Optional[int] = None,
        best_execution_review_cycle_days: int = 92,
    ) -> VerificationResult:
        start = time.time()
        if alert_channels is None or required_alerts is None:
            result = self.create_skipped_result(
                item_name="Monitoring alerts",
                description="Validate alert channel activation and required event coverage.",
                criteria="at least one channel active and all required alerts enabled",
                reason="No monitoring/alert evidence provided.",
                metadata={"checkpoint": "post_close", "critical": False},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        has_active_channel = any(alert_channels.values())
        all_required = all(required_alerts.values())
        passed = has_active_channel and all_required
        review_warn = (
            best_execution_review_days_ago is not None
            and int(best_execution_review_days_ago) > int(best_execution_review_cycle_days)
        )
        review_text = (
            f", best_exec_review={int(best_execution_review_days_ago)}d"
            if best_execution_review_days_ago is not None
            else ""
        )

        result = self.create_result(
            item_name="Monitoring alerts",
            passed=passed,
            description="Validate alert channel activation and required event coverage.",
            criteria="at least one channel active and all required alerts enabled",
            actual_value=(
                f"channels={sum(alert_channels.values())}, "
                f"required={sum(required_alerts.values())}/{len(required_alerts)}"
                f"{review_text}"
            ),
            expected_value=">=1 active channel and all required alerts",
            message="Monitoring alerts configured" if passed else "Monitoring alerts incomplete",
            warn_condition=passed and review_warn,
            execution_time_ms=(time.time() - start) * 1000,
            metadata={
                "checkpoint": "post_close",
                "critical": False,
                "best_execution_review_days_ago": best_execution_review_days_ago,
                "best_execution_review_cycle_days": best_execution_review_cycle_days,
            },
        )
        self.add_result(result)
        return result

















