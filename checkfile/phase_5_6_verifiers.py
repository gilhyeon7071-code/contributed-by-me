"""Phase 5 and 6 verifiers (Execution, Risk)."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .base import (
    BaseVerifier,
    KoreanMarketConstants,
    PhaseReport,
    TradingSystemConfig,
    VerificationResult,
    VerificationPhase,
)


class OrderStatus(Enum):
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    PARTIAL_FILLED = "PARTIAL_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"


class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    CONDITIONAL = "CONDITIONAL"


@dataclass
class Order:
    order_id: str
    symbol: str
    side: str
    order_type: OrderType
    quantity: int
    price: Optional[float] = None
    filled_quantity: int = 0
    avg_fill_price: float = 0.0
    status: OrderStatus = OrderStatus.PENDING
    submitted_at: Optional[datetime] = None
    filled_at: Optional[datetime] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None

    @property
    def unfilled_quantity(self) -> int:
        return self.quantity - self.filled_quantity

    @property
    def is_complete(self) -> bool:
        return self.status in {
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.FAILED,
        }


@dataclass
class ExecutionTestCase:
    name: str
    input_order: Order
    expected_status: OrderStatus
    expected_filled_qty: int
    expected_error_code: Optional[str] = None
    actual_status: Optional[OrderStatus] = None
    actual_filled_qty: Optional[int] = None
    passed: bool = False


class ExecutionVerifier(BaseVerifier):
    def __init__(self, config: TradingSystemConfig):
        super().__init__(VerificationPhase.EXECUTION)
        self.config = config

    def verify_all(self) -> PhaseReport:
        self.results.clear()
        self.run_with_evidence("verify_partial_fill_handling")
        self.run_with_evidence("verify_order_rejection_handling")
        self.run_with_evidence("verify_network_disconnection")
        self.run_with_evidence("verify_order_state_machine")
        self.run_with_evidence("verify_event_sequence_integrity")
        self.run_with_evidence("verify_error_code_handling")
        return self.generate_report()

    def verify_partial_fill_handling(self, test_result: Optional[ExecutionTestCase] = None) -> VerificationResult:
        start = time.time()
        if test_result is None:
            result = self.create_skipped_result(
                item_name="Partial-fill handling",
                description="Validate partial fill state/accounting synchronization.",
                criteria="actual filled qty/status must match expected",
                reason="No execution test case evidence provided.",
                metadata={"checkpoint": "post_close", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        actual_status = test_result.actual_status
        actual_qty = test_result.actual_filled_qty
        status_ok = actual_status == test_result.expected_status
        qty_ok = actual_qty == test_result.expected_filled_qty
        passed = bool(status_ok and qty_ok and test_result.passed)

        result = self.create_result(
            item_name="Partial-fill handling",
            passed=passed,
            description="Validate partial fill state/accounting synchronization.",
            criteria="actual filled qty/status must match expected",
            actual_value=f"status={actual_status}, qty={actual_qty}",
            expected_value=f"status={test_result.expected_status}, qty={test_result.expected_filled_qty}",
            message="Partial fill handling correct" if passed else "Partial fill handling mismatch",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "post_close", "critical": True},
        )
        self.add_result(result)
        return result

    def verify_order_rejection_handling(self, events: Optional[List[Dict[str, Any]]] = None) -> VerificationResult:
        start = time.time()
        required = {
            "-308": {"name": "price out of range", "allowed": {"FAILED", "REJECTED"}},
            "-302": {"name": "insufficient buying power", "allowed": {"REJECTED"}},
            "-304": {"name": "invalid symbol", "allowed": {"FAILED", "REJECTED"}},
        }

        if events is None:
            result = self.create_skipped_result(
                item_name="Order rejection handling",
                description="Validate broker rejection scenarios and state updates.",
                criteria="required rejection codes are handled with expected final status",
                reason="No rejection event evidence provided.",
                metadata={"checkpoint": "pre_order_send", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        by_code: Dict[str, Dict[str, Any]] = {}
        for event in events:
            code = str(event.get("error_code", ""))
            if code and code not in by_code:
                by_code[code] = event

        issues: List[str] = []
        for code, rule in required.items():
            event = by_code.get(code)
            if event is None:
                issues.append(f"missing:{code}")
                continue

            handled = bool(event.get("handled", False))
            status = str(event.get("final_status", "")).upper()
            if not handled:
                issues.append(f"unhandled:{code}")
            if status not in rule["allowed"]:
                issues.append(f"bad_status:{code}:{status}")

        passed = len(issues) == 0
        result = self.create_result(
            item_name="Order rejection handling",
            passed=passed,
            description="Validate broker rejection scenarios and state updates.",
            criteria="required rejection codes are handled with expected final status",
            actual_value=f"issues={len(issues)}",
            expected_value="issues=0",
            message="Rejection handling complete" if passed else "; ".join(issues),
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"issues": issues, "checkpoint": "pre_order_send", "critical": True},
        )
        self.add_result(result)
        return result

    def verify_network_disconnection(
        self,
        reconnect_time_ms: Optional[float] = None,
        emergency_checklist: Optional[Dict[str, bool]] = None,
    ) -> VerificationResult:
        start = time.time()
        max_allowed_ms = 5000.0

        if reconnect_time_ms is None or emergency_checklist is None:
            result = self.create_skipped_result(
                item_name="Network disconnection handling",
                description="Validate fail-safe behavior under network outage.",
                criteria=f"reconnect <= {max_allowed_ms}ms and all emergency actions true",
                reason="Reconnect timing and/or emergency checklist evidence missing.",
                metadata={"checkpoint": "pre_order_send", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        passed = reconnect_time_ms <= max_allowed_ms and all(emergency_checklist.values())

        result = self.create_result(
            item_name="Network disconnection handling",
            passed=passed,
            description="Validate fail-safe behavior under network outage.",
            criteria=f"reconnect <= {max_allowed_ms}ms and all emergency actions true",
            actual_value=f"reconnect={reconnect_time_ms:.1f}ms, checklist={sum(emergency_checklist.values())}/{len(emergency_checklist)}",
            expected_value="within threshold and all true",
            message="Outage handling acceptable" if passed else "Outage handling insufficient",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "pre_order_send", "critical": True},
        )
        self.add_result(result)
        return result

    def verify_order_state_machine(
        self,
        transitions: Optional[Sequence[Tuple[Any, Any]]] = None,
    ) -> VerificationResult:
        start = time.time()
        valid = {
            "PENDING": {"SUBMITTED", "REJECTED", "FAILED"},
            "SUBMITTED": {"PARTIAL_FILLED", "FILLED", "CANCELLED", "REJECTED", "FAILED"},
            "PARTIAL_FILLED": {"PARTIAL_FILLED", "FILLED", "CANCELLED", "FAILED"},
            "FILLED": set(),
            "CANCELLED": set(),
            "REJECTED": set(),
            "FAILED": set(),
        }

        if transitions is None:
            result = self.create_skipped_result(
                item_name="Order state machine",
                description="Validate state-transition logs for order lifecycle.",
                criteria="all observed transitions must be in allowed transition map",
                reason="No transition log evidence provided.",
                metadata={"checkpoint": "pre_order_send", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        invalid: List[str] = []
        for frm, to in transitions:
            f = frm.value if isinstance(frm, OrderStatus) else str(frm).upper()
            t = to.value if isinstance(to, OrderStatus) else str(to).upper()
            if t not in valid.get(f, set()):
                invalid.append(f"{f}->{t}")

        passed = len(invalid) == 0
        result = self.create_result(
            item_name="Order state machine",
            passed=passed,
            description="Validate state-transition logs for order lifecycle.",
            criteria="all observed transitions must be in allowed transition map",
            actual_value=f"invalid_transitions={len(invalid)}",
            expected_value="0",
            message="State transitions valid" if passed else ", ".join(invalid),
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"invalid_transitions": invalid, "checkpoint": "pre_order_send", "critical": True},
        )
        self.add_result(result)
        return result
    def verify_event_sequence_integrity(
        self,
        order_event_sequences: Optional[List[Dict[str, Any]]] = None,
    ) -> VerificationResult:
        start = time.time()
        if order_event_sequences is None:
            result = self.create_skipped_result(
                item_name="Event sequence integrity",
                description="Validate ACK/REJECT/FILL event ordering integrity.",
                criteria="all sequences must follow allowed transitions and end in terminal state",
                reason="No order-event sequence evidence provided.",
                metadata={"checkpoint": "pre_order_send", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        allowed = {
            "SUBMIT": {"ACK", "REJECT", "FAIL"},
            "ACK": {"PARTIAL_FILL", "FILL", "CANCEL_REQ", "REJECT", "FAIL"},
            "PARTIAL_FILL": {"PARTIAL_FILL", "FILL", "CANCEL_REQ", "FAIL"},
            "CANCEL_REQ": {"CANCEL_ACK", "FILL", "FAIL"},
            "REJECT": set(),
            "FAIL": set(),
            "FILL": set(),
            "CANCEL_ACK": set(),
        }
        terminal = {"REJECT", "FAIL", "FILL", "CANCEL_ACK"}

        issues: List[str] = []
        for row in order_event_sequences:
            order_id = str((row or {}).get("order_id", "UNKNOWN"))
            events = [str(x).strip().upper() for x in ((row or {}).get("events") or []) if str(x).strip()]
            if len(events) < 2:
                issues.append(f"{order_id}:too_short")
                continue
            for frm, to in zip(events[:-1], events[1:]):
                if to not in allowed.get(frm, set()):
                    issues.append(f"{order_id}:{frm}->{to}")
                    break
            if events[-1] not in terminal:
                issues.append(f"{order_id}:non_terminal={events[-1]}")

        passed = len(issues) == 0
        result = self.create_result(
            item_name="Event sequence integrity",
            passed=passed,
            description="Validate ACK/REJECT/FILL event ordering integrity.",
            criteria="all sequences must follow allowed transitions and end in terminal state",
            actual_value=f"sequences={len(order_event_sequences)}, issues={len(issues)}",
            expected_value="issues=0",
            message="Event sequence integrity validated" if passed else "; ".join(issues[:5]),
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "pre_order_send", "critical": True, "issues": issues},
        )
        self.add_result(result)
        return result
    def verify_error_code_handling(
        self,
        handled_codes: Optional[Dict[str, bool]] = None,
        backoff_config: Optional[Dict[str, Any]] = None,
        rate_limit_429_events: Optional[int] = None,
        rate_limit_429_warn_threshold: int = 0,
    ) -> VerificationResult:
        start = time.time()
        required_codes = ["-200", "-302", "-308", "-310"]

        if handled_codes is None or backoff_config is None:
            result = self.create_skipped_result(
                item_name="API error-code handling",
                description="Validate mandatory broker error handling and retry strategy.",
                criteria="all required codes handled + valid exponential-backoff config",
                reason="Handled-code matrix and/or backoff config evidence missing.",
                metadata={"checkpoint": "pre_order_send", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        missing = [code for code in required_codes if not handled_codes.get(code, False)]
        backoff_ok = (
            backoff_config.get("initial_delay_ms", 0) > 0
            and backoff_config.get("max_delay_ms", 0) >= backoff_config.get("initial_delay_ms", 0)
            and backoff_config.get("multiplier", 0) >= 1
            and backoff_config.get("max_retries", 0) >= 1
        )

        passed = len(missing) == 0 and backoff_ok
        issues = []
        if missing:
            issues.append(f"missing_codes={missing}")
        if not backoff_ok:
            issues.append("invalid_backoff")

        rate_events = int(rate_limit_429_events or 0)
        rate_warn = rate_events > int(rate_limit_429_warn_threshold)

        result = self.create_result(
            item_name="API error-code handling",
            passed=passed,
            description="Validate mandatory broker error handling and retry strategy.",
            criteria="all required codes handled + valid exponential-backoff config",
            actual_value=f"missing={len(missing)}, backoff_ok={backoff_ok}, rate_limit_429={rate_events}",
            expected_value="missing=0, backoff_ok=True",
            message="Error handling complete" if passed else "; ".join(issues),
            warn_condition=passed and rate_warn,
            execution_time_ms=(time.time() - start) * 1000,
            metadata={
                "issues": issues,
                "checkpoint": "pre_order_send",
                "critical": True,
                "rate_limit_429_events": rate_events,
                "rate_limit_429_warn_threshold": int(rate_limit_429_warn_threshold),
            },
        )
        self.add_result(result)
        return result


class RiskVerifier(BaseVerifier):
    def __init__(self, config: TradingSystemConfig):
        super().__init__(VerificationPhase.RISK)
        self.config = config

    def verify_all(self) -> PhaseReport:
        self.results.clear()
        self.run_with_evidence("verify_order_limits")
        self.run_with_evidence("verify_portfolio_exposure_limits")
        self.run_with_evidence("verify_kill_switch")
        self.run_with_evidence("verify_duplicate_prevention")
        self.run_with_evidence("verify_price_deviation")
        self.run_with_evidence("verify_cost_optimization")
        return self.generate_report()

    def verify_order_limits(
        self,
        test_order_amount: Optional[float] = None,
        total_assets: Optional[float] = None,
        rejected: Optional[bool] = None,
    ) -> VerificationResult:
        start = time.time()
        limit = self.config.max_single_order_ratio

        if test_order_amount is None or total_assets is None or rejected is None:
            result = self.create_skipped_result(
                item_name="Order limits",
                description="Validate single-order size limit enforcement.",
                criteria="exceeded orders must be rejected; valid orders must pass",
                reason="Order amount/assets/rejection evidence missing.",
                metadata={"checkpoint": "pre_order_send", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        ratio = test_order_amount / total_assets if total_assets > 0 else float("inf")
        exceeds = ratio > limit
        passed = rejected if exceeds else (not rejected)

        result = self.create_result(
            item_name="Order limits",
            passed=passed,
            description="Validate single-order size limit enforcement.",
            criteria="exceeded orders must be rejected; valid orders must pass",
            actual_value=f"ratio={ratio*100:.2f}%, rejected={rejected}",
            expected_value=f"limit={limit*100:.2f}%",
            message="Order-limit handling correct" if passed else "Order-limit handling mismatch",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "pre_order_send", "critical": True},
        )
        self.add_result(result)
        return result
    def verify_portfolio_exposure_limits(
        self,
        gross_exposure_ratio: Optional[float] = None,
        single_name_max_ratio: Optional[float] = None,
        sector_max_ratio: Optional[float] = None,
        leverage_ratio: Optional[float] = None,
    ) -> VerificationResult:
        start = time.time()
        if (
            gross_exposure_ratio is None
            or single_name_max_ratio is None
            or sector_max_ratio is None
            or leverage_ratio is None
        ):
            result = self.create_skipped_result(
                item_name="Portfolio exposure limits",
                description="Validate gross/single-name/sector/leverage exposure limits.",
                criteria="all exposure ratios must stay within configured limits",
                reason="No portfolio-exposure evidence provided.",
                metadata={"checkpoint": "pre_order_send", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        gross_max = 1.0
        single_max = float(self.config.max_position_ratio)
        sector_max = 0.35
        leverage_max = 1.0

        checks = {
            "gross": float(gross_exposure_ratio) <= gross_max,
            "single": float(single_name_max_ratio) <= single_max,
            "sector": float(sector_max_ratio) <= sector_max,
            "leverage": float(leverage_ratio) <= leverage_max,
        }
        passed = all(checks.values())
        result = self.create_result(
            item_name="Portfolio exposure limits",
            passed=passed,
            description="Validate gross/single-name/sector/leverage exposure limits.",
            criteria="all exposure ratios must stay within configured limits",
            actual_value=(
                f"gross={float(gross_exposure_ratio):.3f}, single={float(single_name_max_ratio):.3f}, "
                f"sector={float(sector_max_ratio):.3f}, leverage={float(leverage_ratio):.3f}"
            ),
            expected_value=(
                f"gross<={gross_max:.3f}, single<={single_max:.3f}, sector<={sector_max:.3f}, leverage<={leverage_max:.3f}"
            ),
            message="Exposure limits validated" if passed else f"failed={','.join(k for k,v in checks.items() if not v)}",
            warn_condition=passed and any(
                [
                    float(gross_exposure_ratio) >= gross_max * 0.9,
                    float(single_name_max_ratio) >= single_max * 0.9,
                    float(sector_max_ratio) >= sector_max * 0.9,
                    float(leverage_ratio) >= leverage_max * 0.9,
                ]
            ),
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "pre_order_send", "critical": True, "checks": checks},
        )
        self.add_result(result)
        return result
    def verify_kill_switch(
        self,
        simulated_loss_ratio: Optional[float] = None,
        actions_triggered: Optional[Dict[str, bool]] = None,
        mode: Optional[str] = None,
    ) -> VerificationResult:
        start = time.time()
        max_daily_loss = self.config.max_daily_loss_ratio

        if simulated_loss_ratio is None or actions_triggered is None:
            result = self.create_skipped_result(
                item_name="Kill switch",
                description="Validate emergency shutdown behavior on loss-limit breach.",
                criteria="on breach, all mandatory kill-switch actions are true",
                reason="Loss ratio and/or kill-switch action evidence missing.",
                metadata={"checkpoint": "pre_order_send", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        exceeds = simulated_loss_ratio > max_daily_loss
        mode_key = str(mode or "").upper()
        if mode_key == "REDUCE":
            required_actions = ["block_new_orders", "notify_operator"]
        else:
            required_actions = [
                "block_new_orders",
                "cancel_open_orders",
                "notify_operator",
            ]

        if exceeds:
            passed = all(bool(actions_triggered.get(k, False)) for k in required_actions)
        else:
            passed = True

        result = self.create_result(
            item_name="Kill switch",
            passed=passed,
            description="Validate emergency shutdown behavior on loss-limit breach.",
            criteria="on breach, all mandatory kill-switch actions are true",
            actual_value=f"loss={simulated_loss_ratio*100:.2f}%, exceeded={exceeds}",
            expected_value=f"threshold={max_daily_loss*100:.2f}%",
            message="Kill-switch behavior correct" if passed else "Kill-switch actions missing",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "pre_order_send", "critical": True, "mode": mode_key or "DEFAULT"},
        )
        self.add_result(result)
        return result

    def verify_duplicate_prevention(
        self,
        total_attempts: Optional[int] = None,
        duplicate_orders_blocked: Optional[int] = None,
    ) -> VerificationResult:
        start = time.time()
        if total_attempts is None or duplicate_orders_blocked is None:
            result = self.create_skipped_result(
                item_name="Duplicate-order prevention",
                description="Validate duplicate-order throttle and deduplication logic.",
                criteria="for N rapid identical attempts, at least N-1 must be blocked",
                reason="Attempt/block evidence missing.",
                metadata={"checkpoint": "pre_order_send", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        expected_blocked = max(total_attempts - 1, 0)
        passed = duplicate_orders_blocked >= expected_blocked

        result = self.create_result(
            item_name="Duplicate-order prevention",
            passed=passed,
            description="Validate duplicate-order throttle and deduplication logic.",
            criteria="for N rapid identical attempts, at least N-1 must be blocked",
            actual_value=f"blocked={duplicate_orders_blocked}/{expected_blocked}",
            expected_value=f">= {expected_blocked}",
            message="Duplicate-order protection active" if passed else "Duplicate orders not sufficiently blocked",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "pre_order_send", "critical": True},
        )
        self.add_result(result)
        return result

    def verify_price_deviation(self, order_checks: Optional[List[Dict[str, Any]]] = None) -> VerificationResult:
        start = time.time()
        limit = self.config.price_deviation_limit

        if order_checks is None:
            result = self.create_skipped_result(
                item_name="Price deviation guard",
                description="Validate fat-finger protection against excessive price deviation.",
                criteria=f"abs(order-current)/current > {limit:.4f} must be blocked",
                reason="No order-check evidence provided.",
                metadata={"checkpoint": "pre_order_send", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        mismatches: List[str] = []
        for idx, row in enumerate(order_checks):
            current = float(row.get("current_price", 0.0))
            order = float(row.get("order_price", 0.0))
            blocked = bool(row.get("blocked", False))
            if current <= 0:
                mismatches.append(f"row{idx}:invalid_current")
                continue
            should_block = abs(order - current) / current > limit
            if should_block != blocked:
                mismatches.append(f"row{idx}:expected_block={should_block}")

        passed = len(mismatches) == 0
        result = self.create_result(
            item_name="Price deviation guard",
            passed=passed,
            description="Validate fat-finger protection against excessive price deviation.",
            criteria=f"abs(order-current)/current > {limit:.4f} must be blocked",
            actual_value=f"checks={len(order_checks)}, mismatches={len(mismatches)}",
            expected_value="mismatches=0",
            message="Price-deviation guard correct" if passed else ", ".join(mismatches),
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "pre_order_send", "critical": True},
        )
        self.add_result(result)
        return result

    def verify_cost_optimization(
        self,
        trades: Optional[List[Dict[str, Any]]] = None,
        has_ev_module: Optional[bool] = None,
        trade_stats: Optional[Dict[str, Any]] = None,
        min_profit_factor: float = 1.05,
        min_expectancy: float = 0.0,
        min_sample_trades: int = 30,
        min_deploy_sample_trades: int = 60,
        performance_gate_mode: str = "STRICT",
    ) -> VerificationResult:
        start = time.time()
        market = self.config.market
        tax_rate = (
            KoreanMarketConstants.KOSPI_TOTAL_TAX_RATE
            if market == "KOSPI"
            else KoreanMarketConstants.KOSDAQ_TOTAL_TAX_RATE
        )
        total_cost = tax_rate + KoreanMarketConstants.KRX_FEE_RATE + KoreanMarketConstants.TYPICAL_BROKER_FEE + (
            self.config.slippage_bp / 10000
        )

        if (trades is None or has_ev_module is None) and not isinstance(trade_stats, dict):
            result = self.create_skipped_result(
                item_name="Cost optimization",
                description="Validate EV filtering under 2025 cost policy.",
                criteria="negative-EV trades must not be executed",
                reason="Trade evidence/EV-module and trade_stats evidence missing.",
                metadata={"checkpoint": "signal_to_order", "critical": False},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        negative_executed = 0
        core_checked = trades is not None and has_ev_module is not None
        if trades is not None:
            for trade in trades:
                expected_return = float(trade.get("expected_return", 0.0))
                executed = bool(trade.get("executed", False))
                ev_positive = expected_return > total_cost
                if executed and not ev_positive:
                    negative_executed += 1

        core_pass = (bool(has_ev_module) and negative_executed == 0) if core_checked else True

        stats = trade_stats if isinstance(trade_stats, dict) else {}
        sample_trades = int(stats.get("sample_trades") or 0)
        profit_factor = stats.get("profit_factor")
        expectancy = stats.get("expectancy")
        if isinstance(trade_stats, dict) and sample_trades < int(max(1, min_deploy_sample_trades)):
            result = self.create_skipped_result(
                item_name="Cost optimization",
                description="Validate EV filtering and realized expectancy quality.",
                criteria=f"sample_trades >= {int(max(1, min_deploy_sample_trades))}",
                reason=f"Insufficient realized-trade sample: sample_trades={sample_trades}",
                metadata={
                    "checkpoint": "signal_to_order",
                    "critical": False,
                    "sample_trades": sample_trades,
                    "min_deploy_sample_trades": int(max(1, min_deploy_sample_trades)),
                },
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        stats_ready = (
            sample_trades >= int(min_sample_trades)
            and profit_factor is not None
            and expectancy is not None
        )

        stats_pass = True
        if stats_ready:
            stats_pass = float(profit_factor) >= float(min_profit_factor) and float(expectancy) >= float(min_expectancy)

        gate_mode = str(performance_gate_mode or "STRICT").upper()
        if gate_mode not in {"STRICT", "ONBOARDING"}:
            gate_mode = "STRICT"

        gate_bypassed = bool(gate_mode == "ONBOARDING" and bool(stats) and not stats_pass)
        if gate_bypassed:
            passed = core_pass
        else:
            passed = core_pass and stats_pass

        core_text = f"negative_ev_executed={negative_executed}" if core_checked else "negative_ev_executed=N/A"
        pf_text = f", pf={float(profit_factor):.4f}" if profit_factor is not None else ""
        exp_text = f", expectancy={float(expectancy):.6f}" if expectancy is not None else ""
        sample_text = f", sample={sample_trades}"

        if passed:
            if gate_bypassed:
                message = "Onboarding mode: performance gate bypassed"
            elif stats and not stats_ready:
                message = "EV filtering correct (expectancy sample below minimum)"
            else:
                message = "EV filtering behavior correct"
        else:
            reasons: List[str] = []
            if not core_pass:
                reasons.append("negative-EV execution or EV module missing")
            if not stats_pass:
                reasons.append("expectancy/profit-factor below threshold")
            message = "; ".join(reasons)

        result = self.create_result(
            item_name="Cost optimization",
            passed=passed,
            description="Validate EV filtering and realized expectancy quality.",
            criteria="negative-EV trades=0 AND (if sample sufficient) PF/expectancy above thresholds",
            actual_value=f"{core_text}, cost_floor={total_cost:.6f}{sample_text}{pf_text}{exp_text}",
            expected_value=(
                f"negative_ev_executed=0, pf>={float(min_profit_factor):.2f}, "
                f"expectancy>={float(min_expectancy):.6f}"
            ),
            message=message,
            warn_condition=passed and bool(stats) and (not stats_ready or gate_bypassed),
            execution_time_ms=(time.time() - start) * 1000,
            metadata={
                "checkpoint": "signal_to_order",
                "critical": False,
                "sample_trades": sample_trades,
                "min_sample_trades": int(min_sample_trades),
                "min_profit_factor": float(min_profit_factor),
                "min_expectancy": float(min_expectancy),
                "stats_ready": bool(stats_ready),
                "performance_gate_mode": gate_mode,
                "gate_bypassed": bool(gate_bypassed),
            },
        )
        self.add_result(result)
        return result














