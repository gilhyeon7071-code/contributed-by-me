import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools"
for path in (ROOT, TOOLS):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import build_paper_parameter_review as review_mod


class PaperParameterReviewTests(unittest.TestCase):
    def _write_json(self, path: Path, payload: dict) -> None:
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    def _write_trades(self, path: Path) -> None:
        with path.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["exit_reason"])
            writer.writeheader()
            writer.writerows(
                [
                    {"exit_reason": "STOP_GAP"},
                    {"exit_reason": "STOP_GAP"},
                    {"exit_reason": "TIME"},
                    {"exit_reason": "TIME"},
                    {"exit_reason": "TIME"},
                    {"exit_reason": "STOP"},
                ]
            )

    def test_build_review_blocks_candidates_when_sample_is_insufficient(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            paper_pnl = base / "paper_pnl_summary_last.json"
            lvb = base / "live_vs_bt_feedback_latest.json"
            trval = base / "trading_stage_validation_latest.json"
            backtest_final = base / "backtest_final_output_latest.json"
            stable = base / "stable_params_v41_1.json"
            trades = base / "trades.csv"

            self._write_json(
                paper_pnl,
                {
                    "trades_used": 16,
                    "equity": {"max_drawdown_pct": -0.11, "last_day_ret": -0.02},
                    "avg_ret": 0.01,
                    "gross_pf": 1.3,
                },
            )
            self._write_json(lvb, {"comparison": {"alignment_ready": False}, "quality_gate": {"ok": False}, "divergence": {"abs_diff": 0.2}})
            self._write_json(trval, {"paper": {"judgment": "조건부", "items": []}})
            self._write_json(backtest_final, {"operating_parameters": {"params": {"stop_loss": -0.05, "hold": 13, "max_pos": 3}}})
            self._write_json(stable, {"stop_loss": -0.05, "hold": 13, "max_pos": 3})
            self._write_trades(trades)

            with patch.object(review_mod, "PAPER_PNL_PATH", paper_pnl), patch.object(review_mod, "LVB_PATH", lvb), patch.object(
                review_mod, "TRVAL_PATH", trval
            ), patch.object(review_mod, "BACKTEST_FINAL_PATH", backtest_final), patch.object(review_mod, "STABLE_PATH", stable), patch.object(
                review_mod, "PAPER_TRADES_PATH", trades
            ):
                review = review_mod.build_review(warmup_trades=50)

        by_name = {row["name"]: row for row in review["parameters"]}
        self.assertFalse(review["sample_gate"]["enough_for_feedback"])
        self.assertEqual(by_name["stop_loss"]["paper_review"]["status"], "blocked")
        self.assertIsNone(by_name["stop_loss"]["candidate_update"]["proposed_value"])

    def test_build_review_generates_candidates_when_sample_is_sufficient(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            paper_pnl = base / "paper_pnl_summary_last.json"
            lvb = base / "live_vs_bt_feedback_latest.json"
            trval = base / "trading_stage_validation_latest.json"
            backtest_final = base / "backtest_final_output_latest.json"
            stable = base / "stable_params_v41_1.json"
            trades = base / "trades.csv"

            self._write_json(
                paper_pnl,
                {
                    "trades_used": 64,
                    "equity": {"max_drawdown_pct": -0.18, "last_day_ret": -0.08},
                    "avg_ret": 0.004,
                    "gross_pf": 1.08,
                },
            )
            self._write_json(
                lvb,
                {
                    "comparison": {"alignment_ready": False},
                    "quality_gate": {"ok": False},
                    "divergence": {"abs_diff": 0.22},
                },
            )
            self._write_json(trval, {"paper": {"judgment": "조건부", "items": []}})
            self._write_json(
                backtest_final,
                {
                    "operating_parameters": {
                        "params": {
                            "stop_loss": -0.05,
                            "hold": 13,
                            "gap_up_max_pct": 0.03,
                            "entry_gap_down_stop_pct": 0.03,
                            "value_min": 30000000000.0,
                            "atr_max": 4.0,
                            "max_pos": 3,
                        }
                    }
                },
            )
            self._write_json(
                stable,
                {
                    "stop_loss": -0.05,
                    "hold": 13,
                    "gap_up_max_pct": 0.03,
                    "entry_gap_down_stop_pct": 0.03,
                    "value_min": 30000000000.0,
                    "atr_max": 4.0,
                    "max_pos": 3,
                },
            )
            self._write_trades(trades)

            with patch.object(review_mod, "PAPER_PNL_PATH", paper_pnl), patch.object(review_mod, "LVB_PATH", lvb), patch.object(
                review_mod, "TRVAL_PATH", trval
            ), patch.object(review_mod, "BACKTEST_FINAL_PATH", backtest_final), patch.object(review_mod, "STABLE_PATH", stable), patch.object(
                review_mod, "PAPER_TRADES_PATH", trades
            ):
                review = review_mod.build_review(warmup_trades=50)

        by_name = {row["name"]: row for row in review["parameters"]}
        self.assertTrue(review["sample_gate"]["enough_for_feedback"])
        self.assertEqual(by_name["stop_loss"]["paper_review"]["status"], "candidate")
        self.assertEqual(by_name["hold"]["paper_review"]["status"], "candidate")
        self.assertEqual(by_name["value_min"]["paper_review"]["status"], "candidate")
        self.assertEqual(by_name["max_pos"]["paper_review"]["status"], "candidate")
        self.assertIsNotNone(by_name["stop_loss"]["candidate_update"]["proposed_value"])
        self.assertIsNotNone(by_name["stop_loss"]["candidate_update"]["proposed_zone"])
        self.assertLess(by_name["hold"]["candidate_update"]["proposed_value"], 13)


if __name__ == "__main__":
    unittest.main()
