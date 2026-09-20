import unittest

import pandas as pd

from paper_engine import _revalidate_pending_signals


class CarryoverRevalidateTests(unittest.TestCase):
    def test_market_gate_failed_marks_row_as_dropped(self):
        pending = pd.DataFrame(
            [
                {
                    "signal_date": "20260327",
                    "code": "062040",
                    "final_score": 1.00,
                    "score": 1.00,
                    "carry_reason": "REVALIDATE_PENDING",
                }
            ]
        )
        current_df = pd.DataFrame(
            [
                {
                    "signal_date": "20260331",
                    "code": "062040",
                    "final_score": 1.00,
                    "score": 1.00,
                    "sector_entry_allowed": True,
                }
            ]
        )

        out, summary = _revalidate_pending_signals(
            pending,
            current_df,
            market_gate_block=True,
            sector_gate_enabled=True,
        )

        self.assertTrue(out.empty)
        self.assertEqual(summary["loaded"], 1)
        self.assertEqual(summary["revalidated"], 0)
        self.assertEqual(summary["dropped"], 1)
        self.assertEqual(summary["failed_codes"], ["062040"])
        self.assertEqual(summary["failed_reason_counts"].get("MARKET_GATE_FAILED"), 1)

    def test_sector_gate_failed_marks_row_as_dropped(self):
        pending = pd.DataFrame(
            [
                {
                    "signal_date": "20260327",
                    "code": "098460",
                    "final_score": 1.00,
                    "score": 1.00,
                    "carry_reason": "REVALIDATE_PENDING",
                }
            ]
        )
        current_df = pd.DataFrame(
            [
                {
                    "signal_date": "20260331",
                    "code": "098460",
                    "final_score": 1.00,
                    "score": 1.00,
                    "sector_entry_allowed": False,
                }
            ]
        )

        out, summary = _revalidate_pending_signals(
            pending,
            current_df,
            market_gate_block=False,
            sector_gate_enabled=True,
        )

        self.assertTrue(out.empty)
        self.assertEqual(summary["loaded"], 1)
        self.assertEqual(summary["revalidated"], 0)
        self.assertEqual(summary["dropped"], 1)
        self.assertEqual(summary["failed_codes"], ["098460"])
        self.assertEqual(summary["failed_reason_counts"].get("SECTOR_GATE_FAILED"), 1)


if __name__ == "__main__":
    unittest.main()
