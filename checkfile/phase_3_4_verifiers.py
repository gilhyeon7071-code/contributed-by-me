"""Phase 3 and 4 verifiers (Data, Strategy)."""

from __future__ import annotations

import statistics
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from .base import (
    BaseVerifier,
    KoreanMarketConstants,
    PhaseReport,
    TradingSystemConfig,
    VerificationResult,
    VerificationPhase,
)


@dataclass
class OHLCVData:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    symbol: str = ""

    def is_valid(self) -> Tuple[bool, List[str]]:
        errors: List[str] = []
        if self.high < self.low:
            errors.append("high < low")
        if self.high < self.open:
            errors.append("high < open")
        if self.high < self.close:
            errors.append("high < close")
        if self.low > self.open:
            errors.append("low > open")
        if self.low > self.close:
            errors.append("low > close")
        if self.volume < 0:
            errors.append("volume < 0")
        if any(v <= 0 for v in [self.open, self.high, self.low, self.close]):
            errors.append("price <= 0")
        return len(errors) == 0, errors


@dataclass
class BacktestResult:
    total_return: float
    sharpe_ratio: float
    max_drawdown: float
    win_rate: float
    total_trades: int
    avg_holding_period: float
    is_in_sample: bool



@dataclass
class SurvivorshipBiasCheckResultV2:
    """Improved survivorship-bias evidence payload."""

    critical_stocks_required: List[str]
    critical_stocks_included: List[str]
    normal_stocks_required: List[str]
    normal_stocks_included: List[str]
    period_coverage: Dict[str, float]

    critical_threshold: float = 0.9
    normal_threshold: float = 0.7
    period_min_threshold: float = 0.5
    min_universe_size_required: int = 500
    universe_size: int = 0

    input_scope: str = "UNKNOWN"
    required_input_scope: str = "FULL_UNIVERSE"
    enforcement_mode: str = "STRICT"

class DataVerifier(BaseVerifier):
    def __init__(self, config: TradingSystemConfig):
        super().__init__(VerificationPhase.DATA)
        self.config = config
        self.known_delisted_stocks = [
            {"symbol": "067830", "name": "STX Offshore", "reason": "delisted"},
            {"symbol": "004020", "name": "Hyundai Steel old line", "reason": "merged"},
            {"symbol": "000547", "name": "Sample Delisted A", "reason": "delisted"},
            {"symbol": "047040", "name": "Sample Delisted B", "reason": "merged"},
            {"symbol": "000270", "name": "Sample Delisted C", "reason": "renamed"},
        ]

    def verify_all(self) -> PhaseReport:
        self.results.clear()
        self.run_with_evidence("verify_survivorship_bias_v2")
        self.run_with_evidence("verify_lookahead_bias")
        self.run_with_evidence("verify_point_in_time_snapshot")
        self.run_with_evidence("verify_data_integrity")
        self.run_with_evidence("verify_adjusted_price_handling")
        return self.generate_report()

    def verify_survivorship_bias_v2(
        self,
        result: Optional[SurvivorshipBiasCheckResultV2] = None,
    ) -> VerificationResult:
        start = time.time()
        if result is None:
            skipped = self.create_skipped_result(
                item_name="Survivorship bias",
                description="Weighted survivorship-bias check with critical/normal/period gates.",
                criteria="critical >= 90%, normal >= 70%, every period >= 50%",
                reason="No v2 survivorship evidence provided.",
                metadata={"checkpoint": "signal_to_order", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(skipped)
            return skipped

        required_critical = set(result.critical_stocks_required or [])
        included_critical = set(result.critical_stocks_included or [])
        required_normal = set(result.normal_stocks_required or [])
        included_normal = set(result.normal_stocks_included or [])

        critical_threshold = min(1.0, max(0.0, float(result.critical_threshold or 0.9)))
        normal_threshold = min(1.0, max(0.0, float(result.normal_threshold or 0.7)))
        period_min_threshold = min(1.0, max(0.0, float(result.period_min_threshold or 0.5)))
        min_universe = max(1, int(result.min_universe_size_required or 500))
        universe_size = max(0, int(result.universe_size or 0))
        input_scope = str(result.input_scope or "UNKNOWN").upper()
        required_scope = str(result.required_input_scope or "FULL_UNIVERSE").upper()
        enforcement_mode = str(result.enforcement_mode or "STRICT").upper()

        if universe_size > 0 and universe_size < min_universe:
            skipped = self.create_skipped_result(
                item_name="Survivorship bias",
                description="Weighted survivorship-bias check with critical/normal/period gates.",
                criteria=f"universe_size >= {min_universe} and scope={required_scope}",
                reason=f"Evidence scope insufficient: universe_size={universe_size} (<{min_universe})",
                metadata={
                    "checkpoint": "signal_to_order",
                    "critical": True,
                    "universe_size": universe_size,
                    "min_universe_size_required": min_universe,
                    "input_scope": input_scope,
                    "required_input_scope": required_scope,
                },
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(skipped)
            return skipped

        if required_scope and input_scope and input_scope != required_scope:
            skipped = self.create_skipped_result(
                item_name="Survivorship bias",
                description="Weighted survivorship-bias check with critical/normal/period gates.",
                criteria=f"scope must be {required_scope}",
                reason=f"Evidence scope mismatch: input_scope={input_scope}, required_scope={required_scope}",
                metadata={
                    "checkpoint": "signal_to_order",
                    "critical": True,
                    "universe_size": universe_size,
                    "input_scope": input_scope,
                    "required_input_scope": required_scope,
                },
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(skipped)
            return skipped

        critical_hit = len(included_critical & required_critical)
        normal_hit = len(included_normal & required_normal)
        critical_rate = critical_hit / len(required_critical) if len(required_critical) > 0 else 1.0
        normal_rate = normal_hit / len(required_normal) if len(required_normal) > 0 else 1.0

        period_cov = result.period_coverage or {}
        failed_periods = [k for k, v in period_cov.items() if float(v) < period_min_threshold]
        period_ok = bool(period_cov) and len(failed_periods) == 0

        base_pass = (
            critical_rate >= critical_threshold
            and normal_rate >= normal_threshold
            and period_ok
        )

        missing_critical = sorted(required_critical - included_critical)
        missing_normal = sorted(required_normal - included_normal)

        if base_pass:
            passed = True
            warn_condition = (
                critical_rate < min(1.0, critical_threshold + 0.05)
                or normal_rate < min(1.0, normal_threshold + 0.10)
            )
            message = "Coverage policy satisfied"
        elif enforcement_mode == "WARN":
            passed = True
            warn_condition = True
            message = "Coverage policy violated (warn mode)"
        else:
            passed = False
            warn_condition = False
            message = "Coverage policy violated"

        out = self.create_result(
            item_name="Survivorship bias",
            passed=passed,
            description="Weighted survivorship-bias check with critical/normal/period gates.",
            criteria=(
                f"critical >= {critical_threshold*100:.0f}%, "
                f"normal >= {normal_threshold*100:.0f}%, "
                f"every period >= {period_min_threshold*100:.0f}%"
            ),
            actual_value=(
                f"critical={critical_rate * 100:.1f}% ({critical_hit}/{len(required_critical)}), "
                f"normal={normal_rate * 100:.1f}% ({normal_hit}/{len(required_normal)}), "
                f"failed_periods={len(failed_periods)}"
            ),
            expected_value=(
                f"critical>={critical_threshold*100:.0f}%, "
                f"normal>={normal_threshold*100:.0f}%, "
                f"periods>={period_min_threshold*100:.0f}%"
            ),
            message=(
                f"{message}; "
                f"missing_critical={",".join(missing_critical[:3]) or "-"}; "
                f"missing_normal={",".join(missing_normal[:3]) or "-"}; "
                f"failed_periods={",".join(failed_periods[:3]) or "-"}"
            ),
            warn_condition=warn_condition,
            execution_time_ms=(time.time() - start) * 1000,
            metadata={
                "critical_required": len(required_critical),
                "critical_included": critical_hit,
                "normal_required": len(required_normal),
                "normal_included": normal_hit,
                "missing_critical": missing_critical,
                "missing_normal": missing_normal,
                "failed_periods": failed_periods,
                "period_min_threshold": period_min_threshold,
                "checkpoint": "signal_to_order",
                "critical": True,
                "enforcement_mode": enforcement_mode,
                "input_scope": input_scope,
                "required_input_scope": required_scope,
                "universe_size": universe_size,
            },
        )
        self.add_result(out)
        return out
    def verify_survivorship_bias(
        self,
        dataset_symbols: Optional[List[str]] = None,
        reference_date: str = "2010-01-01",
    ) -> VerificationResult:
        start = time.time()
        if dataset_symbols is None:
            result = self.create_skipped_result(
                item_name="Survivorship bias",
                description="Ensure delisted instruments are represented in backtest universe.",
                criteria="Coverage of known delisted names >= 70%",
                reason="No dataset symbol list provided.",
                metadata={"checkpoint": "signal_to_order", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        delisted_symbols = {s["symbol"] for s in self.known_delisted_stocks}
        included = [s for s in dataset_symbols if s in delisted_symbols]
        coverage = (len(included) / len(delisted_symbols)) * 100 if delisted_symbols else 0.0
        passed = coverage >= 70.0

        result = self.create_result(
            item_name="Survivorship bias",
            passed=passed,
            description="Ensure delisted instruments are represented in backtest universe.",
            criteria="Coverage of known delisted names >= 70%",
            actual_value=f"coverage={coverage:.1f}% ({len(included)}/{len(delisted_symbols)})",
            expected_value=">= 70%",
            message="Coverage adequate" if passed else "Insufficient delisted coverage",
            warn_condition=passed and coverage < 90.0,
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"reference_date": reference_date, "checkpoint": "signal_to_order", "critical": True},
        )
        self.add_result(result)
        return result

    def verify_lookahead_bias(
        self,
        signal_timestamps: Optional[List[datetime]] = None,
        data_timestamps: Optional[List[datetime]] = None,
    ) -> VerificationResult:
        start = time.time()
        if signal_timestamps is None or data_timestamps is None:
            result = self.create_skipped_result(
                item_name="Look-ahead bias",
                description="Verify signal time never precedes data availability time.",
                criteria="violations == 0",
                reason="Signal/data timestamp evidence not provided.",
                metadata={"checkpoint": "signal_to_order", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        if len(signal_timestamps) != len(data_timestamps):
            result = self.create_result(
                item_name="Look-ahead bias",
                passed=False,
                description="Verify signal time never precedes data availability time.",
                criteria="equal-length aligned timestamp pairs",
                actual_value=f"signal={len(signal_timestamps)} data={len(data_timestamps)}",
                expected_value="signal length == data length",
                message="Input alignment mismatch",
                execution_time_ms=(time.time() - start) * 1000,
                metadata={"checkpoint": "signal_to_order", "critical": True},
            )
            self.add_result(result)
            return result

        violations = 0
        for signal_ts, data_ts in zip(signal_timestamps, data_timestamps):
            if signal_ts < data_ts:
                violations += 1

        passed = violations == 0
        total = len(signal_timestamps)
        rate = (violations / total * 100) if total > 0 else 0.0

        result = self.create_result(
            item_name="Look-ahead bias",
            passed=passed,
            description="Verify signal time never precedes data availability time.",
            criteria="violations == 0",
            actual_value=f"violations={violations}/{total} ({rate:.2f}%)",
            expected_value="0 violations",
            message="No look-ahead detected" if passed else "Look-ahead violations found",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "signal_to_order", "critical": True},
        )
        self.add_result(result)
        return result
    def verify_point_in_time_snapshot(
        self,
        snapshot_timestamp: Optional[datetime] = None,
        latest_data_timestamp: Optional[datetime] = None,
        asof_match: Optional[bool] = None,
        universe_fixed: Optional[bool] = None,
    ) -> VerificationResult:
        start = time.time()
        if (
            snapshot_timestamp is None
            or latest_data_timestamp is None
            or asof_match is None
            or universe_fixed is None
        ):
            result = self.create_skipped_result(
                item_name="Point-in-time snapshot",
                description="Validate point-in-time snapshot integrity for backtest/live inputs.",
                criteria="latest_data_timestamp <= snapshot_timestamp and asof/universe flags true",
                reason="No point-in-time snapshot evidence provided.",
                metadata={"checkpoint": "signal_to_order", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        chronological_ok = latest_data_timestamp <= snapshot_timestamp
        passed = chronological_ok and bool(asof_match) and bool(universe_fixed)
        result = self.create_result(
            item_name="Point-in-time snapshot",
            passed=passed,
            description="Validate point-in-time snapshot integrity for backtest/live inputs.",
            criteria="latest_data_timestamp <= snapshot_timestamp and asof/universe flags true",
            actual_value=(
                f"snapshot={snapshot_timestamp.isoformat()}, latest_data={latest_data_timestamp.isoformat()}, "
                f"asof_match={bool(asof_match)}, universe_fixed={bool(universe_fixed)}"
            ),
            expected_value="chronology_ok=True, asof_match=True, universe_fixed=True",
            message="Point-in-time snapshot validated" if passed else "Point-in-time snapshot mismatch",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "signal_to_order", "critical": True},
        )
        self.add_result(result)
        return result
    def verify_data_integrity(self, ohlcv_data: Optional[List[OHLCVData]] = None) -> VerificationResult:
        start = time.time()
        if ohlcv_data is None:
            result = self.create_skipped_result(
                item_name="Data integrity",
                description="Validate OHLCV consistency rules.",
                criteria="100% records valid",
                reason="No OHLCV dataset provided.",
                metadata={"checkpoint": "signal_to_order", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        total = len(ohlcv_data)
        invalid = 0
        error_types: Dict[str, int] = {}
        for row in ohlcv_data:
            ok, errors = row.is_valid()
            if not ok:
                invalid += 1
                for e in errors:
                    error_types[e] = error_types.get(e, 0) + 1

        valid_rate = ((total - invalid) / total * 100) if total > 0 else 0.0
        passed = invalid == 0

        result = self.create_result(
            item_name="Data integrity",
            passed=passed,
            description="Validate OHLCV consistency rules.",
            criteria="100% records valid",
            actual_value=f"valid_rate={valid_rate:.2f}% invalid={invalid}",
            expected_value="invalid=0",
            message="OHLCV integrity clean" if passed else f"Integrity errors: {error_types}",
            warn_condition=not passed and valid_rate >= 99.0,
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"error_types": error_types, "checkpoint": "signal_to_order", "critical": True},
        )
        self.add_result(result)
        return result

    def verify_adjusted_price_handling(self, checklist: Optional[Dict[str, bool]] = None) -> VerificationResult:
        start = time.time()
        if checklist is None:
            result = self.create_skipped_result(
                item_name="Adjusted price handling",
                description="Validate adjustment pipeline for splits/dividends/corporate actions.",
                criteria="all required adjustment checks are true",
                reason="No adjustment checklist evidence provided.",
                metadata={"checkpoint": "signal_to_order", "critical": False},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        passed = all(checklist.values())
        result = self.create_result(
            item_name="Adjusted price handling",
            passed=passed,
            description="Validate adjustment pipeline for splits/dividends/corporate actions.",
            criteria="all required adjustment checks are true",
            actual_value=f"{sum(checklist.values())}/{len(checklist)} passed",
            expected_value="all passed",
            message="Adjustment handling complete" if passed else "Missing adjustment checks",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checklist": checklist, "checkpoint": "signal_to_order", "critical": False},
        )
        self.add_result(result)
        return result


class StrategyVerifier(BaseVerifier):
    def __init__(self, config: TradingSystemConfig):
        super().__init__(VerificationPhase.STRATEGY)
        self.config = config

    def verify_all(self) -> PhaseReport:
        self.results.clear()
        self.run_with_evidence("verify_overfitting")
        self.run_with_evidence("verify_walkforward_regime_robustness")
        self.run_with_evidence("verify_randomness_test")
        self.run_with_evidence("verify_max_drawdown")
        self.run_with_evidence("verify_slippage_modeling")
        self.run_with_evidence("verify_liquidity_constraints")
        return self.generate_report()

    def verify_overfitting(
        self,
        in_sample_result: Optional[BacktestResult] = None,
        out_sample_result: Optional[BacktestResult] = None,
        deflated_sharpe_ratio: Optional[float] = None,
        pbo_proxy: Optional[float] = None,
        spa_pvalue_proxy: Optional[float] = None,
        min_dsr: float = 0.10,
        max_pbo: float = 0.80,
        max_spa_pvalue: float = 0.50,
        overfit_metric_sample_size: Optional[int] = None,
        min_overfit_metric_sample_size: int = 30,
        min_deploy_oos_trades: int = 60,
        performance_gate_mode: str = "STRICT",
    ) -> VerificationResult:
        start = time.time()
        if in_sample_result is None or out_sample_result is None:
            result = self.create_skipped_result(
                item_name="Overfitting",
                description="Compare in-sample and out-of-sample robustness.",
                criteria=(
                    "Sharpe delta <= 35% and OOS Sharpe >= 1.0; "
                    "DSR >= min, PBO <= max, SPA p-value <= max (if stats available)"
                ),
                reason="In-sample/out-of-sample backtest evidence not provided.",
                metadata={"checkpoint": "signal_to_order", "critical": False},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        in_trades = int(getattr(in_sample_result, "total_trades", 0) or 0)
        out_trades = int(getattr(out_sample_result, "total_trades", 0) or 0)
        if out_trades < int(max(1, min_deploy_oos_trades)):
            result = self.create_skipped_result(
                item_name="Overfitting",
                description="Compare in-sample and out-of-sample robustness.",
                criteria=f"OOS trades >= {int(max(1, min_deploy_oos_trades))} for deployment-grade overfitting gate",
                reason=f"Insufficient OOS trade sample: out_trades={out_trades}, in_trades={in_trades}",
                metadata={
                    "checkpoint": "signal_to_order",
                    "critical": False,
                    "out_trades": out_trades,
                    "in_trades": in_trades,
                    "min_deploy_oos_trades": int(max(1, min_deploy_oos_trades)),
                },
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        in_sharpe = in_sample_result.sharpe_ratio
        out_sharpe = out_sample_result.sharpe_ratio

        threshold = 35.0
        oos_min_threshold = 1.0
        sharpe_diff_percent = abs(in_sharpe - out_sharpe) / abs(in_sharpe) * 100 if in_sharpe != 0 else 100.0

        base_pass = (sharpe_diff_percent <= threshold and out_sharpe >= oos_min_threshold)

        if sharpe_diff_percent <= 20.0 and out_sharpe >= oos_min_threshold:
            base_message = "Generalization acceptable"
            severity = "ACCEPTABLE"
        elif sharpe_diff_percent <= threshold and out_sharpe >= oos_min_threshold:
            base_message = "Caution: overfitting suspicion"
            severity = "CAUTION"
        elif out_sharpe < oos_min_threshold:
            base_message = "OOS Sharpe below minimum threshold"
            severity = "LOW_OOS_SHARPE"
        else:
            base_message = "Overfitting risk too high"
            severity = "HIGH_RISK"

        metric_sample = int(overfit_metric_sample_size or 0)
        metric_candidate = any(x is not None for x in [deflated_sharpe_ratio, pbo_proxy, spa_pvalue_proxy])
        metric_gate_active = metric_candidate and metric_sample >= int(max(1, min_overfit_metric_sample_size))

        stat_failures: List[str] = []
        if metric_gate_active:
            if deflated_sharpe_ratio is not None and float(deflated_sharpe_ratio) < float(min_dsr):
                stat_failures.append(f"dsr<{float(min_dsr):.2f}")
            if pbo_proxy is not None and float(pbo_proxy) > float(max_pbo):
                stat_failures.append(f"pbo>{float(max_pbo):.2f}")
            if spa_pvalue_proxy is not None and float(spa_pvalue_proxy) > float(max_spa_pvalue):
                stat_failures.append(f"spa_p>{float(max_spa_pvalue):.2f}")

        stat_pass = len(stat_failures) == 0
        gate_mode = str(performance_gate_mode or "STRICT").upper()
        if gate_mode not in {"STRICT", "ONBOARDING"}:
            gate_mode = "STRICT"
        base_gate_bypassed = bool(gate_mode == "ONBOARDING" and not base_pass)
        stat_gate_bypassed = bool(gate_mode == "ONBOARDING" and metric_candidate and not stat_pass)
        if stat_gate_bypassed:
            stat_pass = True

        effective_base_pass = bool(base_pass or base_gate_bypassed)
        passed = effective_base_pass and stat_pass

        if base_gate_bypassed and stat_gate_bypassed:
            message = "Onboarding mode: overfitting baseline/stat gates bypassed"
        elif base_gate_bypassed:
            message = "Onboarding mode: overfitting baseline gate bypassed"
        elif not base_pass:
            message = base_message
        elif stat_gate_bypassed:
            message = "Onboarding mode: overfitting statistic gate bypassed"
        elif not stat_pass:
            message = f"Overfitting statistic gate failed: {','.join(stat_failures)}"
        elif metric_candidate and not metric_gate_active:
            message = "Overfitting check passed (stat sample below minimum)"
        else:
            message = base_message

        dsr_txt = f"{float(deflated_sharpe_ratio):.3f}" if deflated_sharpe_ratio is not None else "-"
        pbo_txt = f"{float(pbo_proxy):.3f}" if pbo_proxy is not None else "-"
        spa_txt = f"{float(spa_pvalue_proxy):.3f}" if spa_pvalue_proxy is not None else "-"

        warn_near = (
            passed
            and metric_gate_active
            and (
                (deflated_sharpe_ratio is not None and float(deflated_sharpe_ratio) < float(min_dsr) * 1.25)
                or (pbo_proxy is not None and float(pbo_proxy) > float(max_pbo) * 0.85)
                or (spa_pvalue_proxy is not None and float(spa_pvalue_proxy) > float(max_spa_pvalue) * 0.85)
            )
        )

        result = self.create_result(
            item_name="Overfitting",
            passed=passed,
            description="Compare in-sample and out-of-sample robustness.",
            criteria=(
                "Sharpe delta <= 35% and OOS Sharpe >= 1.0; "
                f"DSR>={float(min_dsr):.2f}, PBO<={float(max_pbo):.2f}, SPA p<={float(max_spa_pvalue):.2f}"
            ),
            actual_value=(
                f"IS={in_sharpe:.3f}, OOS={out_sharpe:.3f}, delta={sharpe_diff_percent:.2f}%, "
                f"DSR={dsr_txt}, PBO={pbo_txt}, SPAp={spa_txt}, sample={metric_sample}"
            ),
            expected_value=(
                "delta <= 35%, OOS >= 1.0, "
                f"DSR>={float(min_dsr):.2f}, PBO<={float(max_pbo):.2f}, SPAp<={float(max_spa_pvalue):.2f}"
            ),
            message=message,
            warn_condition=(25.0 <= sharpe_diff_percent <= threshold) or base_gate_bypassed or stat_gate_bypassed or warn_near,
            execution_time_ms=(time.time() - start) * 1000,
            metadata={
                "checkpoint": "signal_to_order",
                "critical": False,
                "overfit_severity": severity,
                "deflated_sharpe_ratio": deflated_sharpe_ratio,
                "pbo_proxy": pbo_proxy,
                "spa_pvalue_proxy": spa_pvalue_proxy,
                "metric_gate_active": bool(metric_gate_active),
                "metric_sample": metric_sample,
                "min_metric_sample_size": int(min_overfit_metric_sample_size),
                "metric_failures": stat_failures,
                "performance_gate_mode": gate_mode,
                "base_gate_bypassed": bool(base_gate_bypassed),
                "metric_gate_bypassed": bool(stat_gate_bypassed),
            },
        )
        self.add_result(result)
        return result

    def verify_walkforward_regime_robustness(
        self,
        regime_results: Optional[Dict[str, Dict[str, float]]] = None,
        min_regimes_required: int = 3,
        min_regime_sharpe: float = 0.5,
        max_regime_mdd_abs: float = 35.0,
        overall_sortino: Optional[float] = None,
        overall_calmar: Optional[float] = None,
        overall_metric_sample_size: Optional[int] = None,
        min_metric_sample_size: int = 30,
        min_deploy_metric_sample_size: int = 60,
        min_overall_sortino: float = 0.20,
        min_overall_calmar: float = 0.05,
        performance_gate_mode: str = "STRICT",
    ) -> VerificationResult:
        start = time.time()
        if regime_results is None or len(regime_results) == 0:
            result = self.create_skipped_result(
                item_name="Walk-forward regime robustness",
                description="Validate walk-forward robustness across distinct market regimes.",
                criteria="min regime count and each regime passes Sharpe/MDD thresholds",
                reason="No walk-forward regime evidence provided.",
                metadata={"checkpoint": "signal_to_order", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        metric_sample = int(overall_metric_sample_size or 0)
        if metric_sample < int(max(1, min_deploy_metric_sample_size)):
            result = self.create_skipped_result(
                item_name="Walk-forward regime robustness",
                description="Validate walk-forward robustness across distinct market regimes.",
                criteria=f"overall metric sample >= {int(max(1, min_deploy_metric_sample_size))}",
                reason=f"Insufficient risk-adjusted sample: sample={metric_sample}",
                metadata={
                    "checkpoint": "signal_to_order",
                    "critical": True,
                    "metric_sample": metric_sample,
                    "min_deploy_metric_sample_size": int(max(1, min_deploy_metric_sample_size)),
                },
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        regimes = list(regime_results.items())
        failed: List[str] = []
        for regime_name, stats in regimes:
            s = float((stats or {}).get("sharpe", 0.0))
            mdd = abs(float((stats or {}).get("mdd", 100.0)))
            if s < float(min_regime_sharpe) or mdd > float(max_regime_mdd_abs):
                failed.append(str(regime_name))

        enough_regimes = len(regimes) >= int(max(1, min_regimes_required))
        regime_pass = enough_regimes and len(failed) == 0

        metric_sample = int(overall_metric_sample_size or 0)
        metric_candidate = (overall_sortino is not None) or (overall_calmar is not None)
        metric_gate_active = metric_candidate and metric_sample >= int(max(1, min_metric_sample_size))

        metric_failures: List[str] = []
        if metric_gate_active:
            if overall_sortino is not None and float(overall_sortino) < float(min_overall_sortino):
                metric_failures.append(f"sortino<{float(min_overall_sortino):.2f}")
            if overall_calmar is not None and float(overall_calmar) < float(min_overall_calmar):
                metric_failures.append(f"calmar<{float(min_overall_calmar):.2f}")

        metric_pass = len(metric_failures) == 0
        gate_mode = str(performance_gate_mode or "STRICT").upper()
        if gate_mode not in {"STRICT", "ONBOARDING"}:
            gate_mode = "STRICT"
        metric_gate_bypassed = bool(gate_mode == "ONBOARDING" and metric_candidate and not metric_pass)
        if metric_gate_bypassed:
            metric_pass = True
        passed = regime_pass and metric_pass

        sortino_txt = f", sortino={float(overall_sortino):.3f}" if overall_sortino is not None else ""
        calmar_txt = f", calmar={float(overall_calmar):.3f}" if overall_calmar is not None else ""
        sample_txt = f", sample={metric_sample}" if metric_candidate else ""

        if not regime_pass:
            message = f"failed_regimes={','.join(failed[:5])}"
        elif metric_gate_bypassed:
            message = "Onboarding mode: risk-adjusted gate bypassed"
        elif not metric_pass:
            message = f"risk-adjusted metric gate failed: {','.join(metric_failures)}"
        elif metric_candidate and not metric_gate_active:
            message = "Regime robustness validated (risk-adjusted sample below minimum)"
        else:
            message = "Regime robustness validated"

        warn_near = (
            passed
            and metric_gate_active
            and (
                (overall_sortino is not None and float(overall_sortino) < float(min_overall_sortino) * 1.25)
                or (overall_calmar is not None and float(overall_calmar) < float(min_overall_calmar) * 1.25)
            )
        )

        result = self.create_result(
            item_name="Walk-forward regime robustness",
            passed=passed,
            description="Validate walk-forward robustness across distinct market regimes.",
            criteria="min regime count and each regime passes Sharpe/MDD thresholds",
            actual_value=(
                f"regimes={len(regimes)}, failed={len(failed)}{sample_txt}{sortino_txt}{calmar_txt}"
            ),
            expected_value=(
                f"regimes>={int(max(1, min_regimes_required))}, failed=0, "
                f"sortino>={float(min_overall_sortino):.2f}, calmar>={float(min_overall_calmar):.2f}"
            ),
            message=message,
            warn_condition=(passed and len(regimes) == int(max(1, min_regimes_required))) or warn_near or metric_gate_bypassed,
            execution_time_ms=(time.time() - start) * 1000,
            metadata={
                "checkpoint": "signal_to_order",
                "critical": True,
                "failed_regimes": failed,
                "metric_gate_active": bool(metric_gate_active),
                "metric_sample": metric_sample,
                "min_metric_sample_size": int(min_metric_sample_size),
                "metric_failures": metric_failures,
                "performance_gate_mode": gate_mode,
                "metric_gate_bypassed": bool(metric_gate_bypassed),
            },
        )
        self.add_result(result)
        return result
    def verify_randomness_test(
        self,
        strategy_return: Optional[float] = None,
        random_returns: Optional[List[float]] = None,
    ) -> VerificationResult:
        start = time.time()
        if strategy_return is None or random_returns is None or len(random_returns) < 2:
            result = self.create_skipped_result(
                item_name="Randomness test",
                description="Check whether strategy outperforms random baseline.",
                criteria="z_score > 1.96",
                reason="Strategy return and/or random baseline sample missing.",
                metadata={"checkpoint": "signal_to_order", "critical": False},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        mean = statistics.mean(random_returns)
        stdev = statistics.stdev(random_returns)
        z = (strategy_return - mean) / stdev if stdev > 0 else 0.0
        passed = z > 1.96

        result = self.create_result(
            item_name="Randomness test",
            passed=passed,
            description="Check whether strategy outperforms random baseline.",
            criteria="z_score > 1.96",
            actual_value=f"strategy={strategy_return:.4f}, z={z:.3f}",
            expected_value="> 1.96",
            message="Statistically significant edge" if passed else "No significant edge",
            warn_condition=passed and z <= 2.3,
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "signal_to_order", "critical": False},
        )
        self.add_result(result)
        return result

    def verify_max_drawdown(
        self,
        historical_mdd: Optional[float] = None,
        crisis_periods: Optional[Dict[str, float]] = None,
    ) -> VerificationResult:
        start = time.time()
        if historical_mdd is None and not crisis_periods:
            result = self.create_skipped_result(
                item_name="Max drawdown",
                description="Validate worst drawdown against configured risk limit.",
                criteria=f"|MDD| <= {self.config.max_drawdown_limit * 100:.1f}%",
                reason="No drawdown evidence provided.",
                metadata={"checkpoint": "signal_to_order", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        observed = historical_mdd
        if crisis_periods:
            worst_crisis = min(crisis_periods.values())
            observed = worst_crisis if observed is None else min(observed, worst_crisis)

        assert observed is not None
        limit_pct = self.config.max_drawdown_limit * 100
        passed = abs(observed) <= limit_pct

        result = self.create_result(
            item_name="Max drawdown",
            passed=passed,
            description="Validate worst drawdown against configured risk limit.",
            criteria=f"|MDD| <= {limit_pct:.1f}%",
            actual_value=f"observed_mdd={observed:.2f}%",
            expected_value=f"<= {limit_pct:.2f}%",
            message="Drawdown within limit" if passed else "Drawdown limit breached",
            warn_condition=passed and abs(observed) >= limit_pct * 0.8,
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "signal_to_order", "critical": True},
        )
        self.add_result(result)
        return result

    def verify_slippage_modeling(self) -> VerificationResult:
        start = time.time()
        market = self.config.market
        configured = self.config.slippage_bp
        recommended = (
            KoreanMarketConstants.SLIPPAGE_KOSPI_LARGE_CAP_BP
            if market == "KOSPI"
            else KoreanMarketConstants.SLIPPAGE_KOSDAQ_SMALL_CAP_BP
        )
        passed = configured >= recommended

        result = self.create_result(
            item_name="Slippage modeling",
            passed=passed,
            description="Check configured slippage against market minimum recommendation.",
            criteria=f"slippage_bp >= {recommended}",
            actual_value=f"configured={configured}bp",
            expected_value=f">= {recommended}bp",
            message="Slippage assumption acceptable" if passed else "Slippage likely underestimated",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "signal_to_order", "critical": False},
        )
        self.add_result(result)
        return result

    def verify_liquidity_constraints(self, max_order_volume_ratio: Optional[float] = None) -> VerificationResult:
        start = time.time()
        if max_order_volume_ratio is None:
            result = self.create_skipped_result(
                item_name="Liquidity constraints",
                description="Validate order sizing relative to traded volume.",
                criteria="max_order_volume_ratio <= 5%",
                reason="No liquidity ratio evidence provided.",
                metadata={"checkpoint": "pre_order_send", "critical": True},
                execution_time_ms=(time.time() - start) * 1000,
            )
            self.add_result(result)
            return result

        recommended_max = 0.05
        passed = max_order_volume_ratio <= recommended_max

        result = self.create_result(
            item_name="Liquidity constraints",
            passed=passed,
            description="Validate order sizing relative to traded volume.",
            criteria="max_order_volume_ratio <= 5%",
            actual_value=f"{max_order_volume_ratio * 100:.2f}%",
            expected_value="<= 5.00%",
            message="Liquidity constraint respected" if passed else "Order size exceeds liquidity constraint",
            execution_time_ms=(time.time() - start) * 1000,
            metadata={"checkpoint": "pre_order_send", "critical": True},
        )
        self.add_result(result)
        return result







