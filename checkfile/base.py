"""Core types and utilities for the trading system verification framework."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


class VerificationStatus(Enum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    WARNING = "WARNING"
    SKIPPED = "SKIPPED"
    ERROR = "ERROR"
    NOT_APPLICABLE = "N/A"


class VerificationPhase(Enum):
    PLANNING = "Phase 1: Planning"
    DESIGN = "Phase 2: Design"
    DATA = "Phase 3: Data"
    STRATEGY = "Phase 4: Strategy"
    EXECUTION = "Phase 5: Execution"
    RISK = "Phase 6: Risk"
    TESTING = "Phase 7: Testing"
    OPERATIONS = "Phase 8: Operations"


@dataclass
class VerificationResult:
    item_name: str
    status: VerificationStatus
    phase: VerificationPhase
    description: str
    criteria: str
    actual_value: Any = None
    expected_value: Any = None
    message: str = ""
    timestamp: datetime = field(default_factory=datetime.now)
    execution_time_ms: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "item_name": self.item_name,
            "status": self.status.value,
            "phase": self.phase.value,
            "description": self.description,
            "criteria": self.criteria,
            "actual_value": self.actual_value,
            "expected_value": self.expected_value,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "execution_time_ms": self.execution_time_ms,
            "metadata": self.metadata,
        }


@dataclass
class PhaseReport:
    phase: VerificationPhase
    results: List[VerificationResult] = field(default_factory=list)
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None

    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.results if r.status == VerificationStatus.PASSED)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if r.status == VerificationStatus.FAILED)

    @property
    def warning_count(self) -> int:
        return sum(1 for r in self.results if r.status == VerificationStatus.WARNING)

    @property
    def skipped_count(self) -> int:
        return sum(1 for r in self.results if r.status == VerificationStatus.SKIPPED)

    @property
    def total_count(self) -> int:
        return len(self.results)

    @property
    def pass_rate(self) -> float:
        if self.total_count == 0:
            return 0.0
        return (self.passed_count / self.total_count) * 100

    @property
    def is_phase_passed(self) -> bool:
        # Conservative policy: SKIPPED means not production-ready.
        return self.failed_count == 0 and self.skipped_count == 0


class BaseVerifier(ABC):
    def __init__(self, phase: VerificationPhase):
        self.phase = phase
        self.results: List[VerificationResult] = []
        self.evidence: Dict[str, Dict[str, Any]] = {}
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def verify_all(self) -> PhaseReport:
        raise NotImplementedError

    def add_result(self, result: VerificationResult) -> None:
        self.results.append(result)
        self._log_result(result)

    def set_evidence(self, evidence: Optional[Dict[str, Dict[str, Any]]]) -> None:
        self.evidence = evidence or {}

    def run_with_evidence(self, method_name: str):
        method = getattr(self, method_name)
        kwargs = self.evidence.get(method_name, {})
        if kwargs is None:
            kwargs = {}
        if not isinstance(kwargs, dict):
            raise TypeError(f"Evidence for {method_name} must be a dict.")
        return method(**kwargs)

    def _log_result(self, result: VerificationResult) -> None:
        icon = {
            VerificationStatus.PASSED: "OK",
            VerificationStatus.FAILED: "FAIL",
            VerificationStatus.WARNING: "WARN",
            VerificationStatus.SKIPPED: "SKIP",
            VerificationStatus.ERROR: "ERR",
            VerificationStatus.NOT_APPLICABLE: "NA",
        }.get(result.status, "UNK")
        self.logger.info("[%s] %s: %s", icon, result.item_name, result.message)

    def create_result(
        self,
        item_name: str,
        passed: bool,
        description: str,
        criteria: str,
        actual_value: Any = None,
        expected_value: Any = None,
        message: str = "",
        warn_condition: bool = False,
        execution_time_ms: float = 0.0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> VerificationResult:
        if warn_condition and passed:
            status = VerificationStatus.WARNING
        elif passed:
            status = VerificationStatus.PASSED
        else:
            status = VerificationStatus.FAILED

        return VerificationResult(
            item_name=item_name,
            status=status,
            phase=self.phase,
            description=description,
            criteria=criteria,
            actual_value=actual_value,
            expected_value=expected_value,
            message=message,
            execution_time_ms=execution_time_ms,
            metadata=metadata or {},
        )

    def create_skipped_result(
        self,
        item_name: str,
        description: str,
        criteria: str,
        reason: str,
        metadata: Optional[Dict[str, Any]] = None,
        execution_time_ms: float = 0.0,
    ) -> VerificationResult:
        return VerificationResult(
            item_name=item_name,
            status=VerificationStatus.SKIPPED,
            phase=self.phase,
            description=description,
            criteria=criteria,
            actual_value="N/A",
            expected_value="Input evidence required",
            message=reason,
            execution_time_ms=execution_time_ms,
            metadata=metadata or {},
        )

    def generate_report(self) -> PhaseReport:
        return PhaseReport(
            phase=self.phase,
            results=self.results.copy(),
            end_time=datetime.now(),
        )


class KoreanMarketConstants:
    KOSPI_STT_RATE = 0.0005
    KOSPI_RURAL_TAX_RATE = 0.0015
    KOSDAQ_STT_RATE = 0.0020

    KOSPI_TOTAL_TAX_RATE = KOSPI_STT_RATE + KOSPI_RURAL_TAX_RATE
    KOSDAQ_TOTAL_TAX_RATE = KOSDAQ_STT_RATE

    KRX_FEE_RATE = 0.000036
    TYPICAL_BROKER_FEE = 0.00015

    TOTAL_COST_KOSPI = KOSPI_TOTAL_TAX_RATE + KRX_FEE_RATE + TYPICAL_BROKER_FEE
    TOTAL_COST_KOSDAQ = KOSDAQ_TOTAL_TAX_RATE + KRX_FEE_RATE + TYPICAL_BROKER_FEE

    ROUND_TRIP_COST_KOSPI = TOTAL_COST_KOSPI * 2
    ROUND_TRIP_COST_KOSDAQ = TOTAL_COST_KOSDAQ * 2

    KIWOOM_TPS_LIMIT = 5
    KIS_TPS_LIMIT = 20
    EBEST_TPM_LIMIT = 60

    MARKET_OPEN_TIME = "09:00:00"
    MARKET_CLOSE_TIME = "15:30:00"
    PRE_MARKET_START = "08:30:00"
    POST_MARKET_END = "18:00:00"

    SLIPPAGE_KOSPI_LARGE_CAP_BP = 3
    SLIPPAGE_KOSDAQ_SMALL_CAP_BP = 10

    MAX_VOLUME_RATIO = 0.05


@dataclass
class TradingSystemConfig:
    system_name: str = "AlgoTrading System"
    version: str = "1.0.0"
    market: str = "KOSPI"

    architecture_type: str = "DUAL_PROCESS"
    api_provider: str = "KIS"

    target_latency_ms: float = 100.0
    target_tps: int = 10

    max_single_order_ratio: float = 0.05
    max_daily_loss_ratio: float = 0.02
    max_position_ratio: float = 0.20
    price_deviation_limit: float = 0.03

    slippage_bp: int = 5

    backtest_start_date: str = "2015-01-01"
    backtest_end_date: str = "2024-12-31"
    include_delisted: bool = True
    max_drawdown_limit: float = 0.30

    def to_dict(self) -> Dict[str, Any]:
        return {
            "system_name": self.system_name,
            "version": self.version,
            "market": self.market,
            "architecture_type": self.architecture_type,
            "api_provider": self.api_provider,
            "target_latency_ms": self.target_latency_ms,
            "target_tps": self.target_tps,
            "max_single_order_ratio": self.max_single_order_ratio,
            "max_daily_loss_ratio": self.max_daily_loss_ratio,
            "max_position_ratio": self.max_position_ratio,
            "price_deviation_limit": self.price_deviation_limit,
            "slippage_bp": self.slippage_bp,
            "backtest_start_date": self.backtest_start_date,
            "backtest_end_date": self.backtest_end_date,
            "include_delisted": self.include_delisted,
            "max_drawdown_limit": self.max_drawdown_limit,
        }

