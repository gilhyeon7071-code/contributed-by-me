import json
import os
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(r"E:\1_Data")
TOOLS = ROOT / "tools"
for path in (ROOT, TOOLS):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import notify_channels  # noqa: E402


class NotifyChannelsPolicyTests(unittest.TestCase):
    def test_duplicate_alert_is_suppressed_inside_cooldown(self):
        with tempfile.TemporaryDirectory() as td:
            alert_dir = Path(td)
            orig_alert_dir = notify_channels.ALERT_DIR
            orig_state_path = notify_channels.ALERT_STATE_PATH
            prev_env = dict(os.environ)
            try:
                notify_channels.ALERT_DIR = alert_dir
                notify_channels.ALERT_STATE_PATH = alert_dir / "alert_policy_state_latest.json"
                os.environ["ALERT_CHANNELS_INFO"] = "file"
                os.environ["ALERT_COOLDOWN_INFO_SEC"] = "300"

                first = notify_channels.send_alert("same-message", level="info")
                second = notify_channels.send_alert("same-message", level="info")

                self.assertTrue(first["ok"])
                self.assertFalse(bool(first.get("suppressed", False)))
                self.assertTrue(bool(second.get("suppressed", False)))

                saved = json.loads((alert_dir / "alert_policy_state_latest.json").read_text(encoding="utf-8"))
                self.assertIn("fingerprints", saved)
            finally:
                notify_channels.ALERT_DIR = orig_alert_dir
                notify_channels.ALERT_STATE_PATH = orig_state_path
                os.environ.clear()
                os.environ.update(prev_env)


if __name__ == "__main__":
    unittest.main()
