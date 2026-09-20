import unittest

from tools.safe_exploration_review import build_review


class SafeExplorationReviewTests(unittest.TestCase):
    def _candidate(self):
        return {
            "context": {"entry_decision": "PASS", "pending_max_new": 1},
            "signals": [
                {
                    "signal": "entry_pool",
                    "current": 0.63,
                    "proposed": 0.61,
                    "reason": "test",
                }
            ],
            "patch": {"union_entry_strength_min": 0.61},
        }

    def test_waits_for_approval_even_when_safe_pass(self):
        review = build_review(
            self._candidate(),
            {"status": "PASS", "metrics": {"rows": 10000}},
            {"status": "PASS", "history": {"rows": 10000}},
            {"max_new": 1},
            beta=3.0,
            gamma=0.6,
            eta0=1.0,
            min_k=50,
            max_safe_pct=0.10,
            safe_threshold=0.0,
            bandit_dual_limit=1.0,
        )
        self.assertEqual(review["status"], "SAFE_PASS")
        self.assertEqual(review["apply_status"], "WAITING_APPROVAL")
        self.assertFalse(review["orders_modified"])
        self.assertFalse(review["runtime_config_modified"])
        self.assertEqual(review["live_pct"], 0.0)

    def test_fails_closed_when_samples_are_low(self):
        review = build_review(
            self._candidate(),
            {"status": "PASS", "metrics": {"rows": 10}},
            {"status": "PASS", "history": {"rows": 20}},
            {"max_new": 1},
            beta=3.0,
            gamma=0.6,
            eta0=1.0,
            min_k=50,
            max_safe_pct=0.10,
            safe_threshold=0.0,
            bandit_dual_limit=1.0,
        )
        self.assertEqual(review["status"], "SAFE_FAIL")
        self.assertIn("K_BELOW_MIN(20<50)", review["reasons"])

    def test_fails_closed_when_current_gate_is_blocked(self):
        candidate = self._candidate()
        candidate["context"]["entry_decision"] = "BLOCK"
        candidate["context"]["pending_max_new"] = 0
        review = build_review(
            candidate,
            {"status": "PASS", "metrics": {"rows": 10000}},
            {"status": "PASS", "history": {"rows": 10000}},
            {"max_new": 0},
            beta=3.0,
            gamma=0.6,
            eta0=1.0,
            min_k=50,
            max_safe_pct=0.10,
            safe_threshold=0.0,
            bandit_dual_limit=1.0,
        )
        self.assertEqual(review["status"], "SAFE_FAIL")
        self.assertIn("CURRENT_GATE_NOT_OPEN", review["reasons"])


if __name__ == "__main__":
    unittest.main()
