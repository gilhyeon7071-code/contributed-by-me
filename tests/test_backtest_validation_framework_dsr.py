import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools"
for path in (ROOT, TOOLS):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from backtest_validation_framework import BacktestResult, CostModel, DeflatedSharpeValidator, StatisticalPowerMDEValidator, ValidationPipeline, make_demo_market, reference_backtest, sma_cross_strategy


class DeflatedSharpeValidatorTests(unittest.TestCase):
    def test_deflated_sharpe_validator_fail_closed_on_insufficient_returns(self) -> None:
        validator = DeflatedSharpeValidator()
        bt = BacktestResult(
            returns=pd.Series([0.01]),
            equity=pd.Series([1.01]),
            trades=pd.DataFrame(),
            metrics={},
            meta={},
        )
        result = validator.run(bt, min_dsr=0.10, n_trials=5)
        self.assertEqual(result.name, "deflated_sharpe_ratio")
        self.assertFalse(result.passed)
        self.assertEqual(result.details.get("reason"), "insufficient_returns")

    def test_validation_pipeline_emits_deflated_sharpe_gate(self) -> None:
        market = make_demo_market(periods=252 * 4)
        params = {"fast": 10, "slow": 100, "allow_short": False, "position_scale": 0.7}
        grid = [
            {"fast": 8, "slow": 90, "allow_short": False, "position_scale": 0.6},
            {"fast": 10, "slow": 100, "allow_short": False, "position_scale": 0.7},
            {"fast": 12, "slow": 110, "allow_short": False, "position_scale": 0.8},
        ]
        pipe = ValidationPipeline(strategy_fn=sma_cross_strategy, backtest_fn=reference_backtest)
        report = pipe.run(
            market_df=market,
            params=params,
            param_grid=grid,
            cost_model=CostModel(),
            min_dsr=0.10,
        )

        gate_names = {g.name for g in report.gate_results}
        self.assertIn("deflated_sharpe_ratio", gate_names)
        self.assertIn("deflated_sharpe_ratio", report.artifacts)

        dsr_gate = next(g for g in report.gate_results if g.name == "deflated_sharpe_ratio")
        self.assertIn("min_dsr", dsr_gate.details)
        self.assertIn("n_trials", dsr_gate.details)

    def test_statistical_power_mde_passes_when_target_is_detectable(self) -> None:
        validator = StatisticalPowerMDEValidator()
        returns = pd.Series([0.001, -0.0005, 0.0008, 0.0012, -0.0004] * 120)
        bt = BacktestResult(
            returns=returns,
            equity=(1.0 + returns).cumprod(),
            trades=pd.DataFrame(),
            metrics={},
            meta={},
        )
        result = validator.run(
            bt,
            economic_mde_sharpe=10.0,
            economic_mde_return=None,
            sharpe_scale="annual",
            n_boot=100,
            block_size=5,
        )
        self.assertEqual(result.name, "statistical_power_mde")
        self.assertTrue(result.passed)
        self.assertGreater(result.details.get("n_eff"), 0)
        self.assertLessEqual(result.details.get("mde_sharpe"), result.details.get("economic_mde_sharpe"))

    def test_validation_pipeline_records_power_artifact_without_gate_by_default(self) -> None:
        market = make_demo_market(periods=252 * 3)
        params = {"fast": 10, "slow": 100, "allow_short": False, "position_scale": 0.7}
        grid = [{"fast": 10, "slow": 100, "allow_short": False, "position_scale": 0.7}]
        pipe = ValidationPipeline(strategy_fn=sma_cross_strategy, backtest_fn=reference_backtest)
        report = pipe.run(
            market_df=market,
            params=params,
            param_grid=grid,
            cost_model=CostModel(),
            economic_mde_sharpe=0.15,
            mde_bootstrap_runs=100,
        )

        gate_names = {g.name for g in report.gate_results}
        self.assertNotIn("statistical_power_mde", gate_names)
        self.assertIn("statistical_power_mde", report.artifacts)
        self.assertFalse(report.artifacts["statistical_power_mde"]["enabled_as_gate"])

    def test_statistical_power_mde_fails_closed_without_economic_target(self) -> None:
        market = make_demo_market(periods=252 * 3)
        params = {"fast": 10, "slow": 100, "allow_short": False, "position_scale": 0.7}
        grid = [{"fast": 10, "slow": 100, "allow_short": False, "position_scale": 0.7}]
        pipe = ValidationPipeline(strategy_fn=sma_cross_strategy, backtest_fn=reference_backtest)
        report = pipe.run(
            market_df=market,
            params=params,
            param_grid=grid,
            cost_model=CostModel(),
            enable_power_prereg=True,
            mde_bootstrap_runs=100,
        )

        gate = next(g for g in report.gate_results if g.name == "statistical_power_mde")
        self.assertFalse(gate.passed)
        self.assertEqual(gate.details.get("reason"), "missing_economic_mde")


if __name__ == "__main__":
    unittest.main()
