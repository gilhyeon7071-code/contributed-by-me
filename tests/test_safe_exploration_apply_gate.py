import unittest

from tools.safe_exploration_apply_gate import build_apply_gate


class SafeExplorationApplyGateTests(unittest.TestCase):
    def test_blocks_without_approval_even_when_safe_pass(self):
        payload = build_apply_gate(
            {"status": "SAFE_PASS", "apply_status": "WAITING_APPROVAL", "live_pct": 0.0},
            approve=False,
        )
        self.assertEqual(payload["status"], "APPLY_GATE_BLOCK")
        self.assertIn("APPROVAL_FLAG_MISSING", payload["reasons"])
        self.assertFalse(payload["runtime_config_modified"])

    def test_blocks_when_review_is_safe_fail_even_with_approval(self):
        payload = build_apply_gate(
            {"status": "SAFE_FAIL", "apply_status": "WAITING_APPROVAL", "live_pct": 0.0},
            approve=True,
        )
        self.assertEqual(payload["status"], "APPLY_GATE_BLOCK")
        self.assertIn("REVIEW_NOT_SAFE_PASS(SAFE_FAIL)", payload["reasons"])
        self.assertFalse(payload["orders_modified"])

    def test_pass_is_still_evidence_only(self):
        payload = build_apply_gate(
            {"status": "SAFE_PASS", "apply_status": "WAITING_APPROVAL", "live_pct": 0.0},
            approve=True,
        )
        self.assertEqual(payload["status"], "APPLY_GATE_PASS")
        self.assertEqual(payload["action"], "WOULD_APPLY_AFTER_SEPARATE_IMPLEMENTATION")
        self.assertFalse(payload["gate_modified"])
        self.assertFalse(payload["risk_lock_modified"])


if __name__ == "__main__":
    unittest.main()
