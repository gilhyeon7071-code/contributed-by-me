import json
import shutil
import unittest
import uuid
from pathlib import Path

from tools import restore_drill_vss_scripts as vss


WORK_TMP = Path(r"E:\1_Data\backup\restore_drill_vss_unit_tmp")
WORK_TMP.mkdir(parents=True, exist_ok=True)


def case_dir():
    path = WORK_TMP / uuid.uuid4().hex
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)
    return path


class RestoreDrillVssScriptTests(unittest.TestCase):
    def write_config(self, base, source="C:", expose="X:", target=None):
        if target is None:
            target = str(base / "restore_target")
        config = {
            "windows_vss": {
                "enabled": False,
                "source_volume": source,
                "expose_drive": expose,
                "restore_target": target,
            }
        }
        path = base / "config.json"
        path.write_text(json.dumps(config), encoding="utf-8")
        return path

    def test_generates_alias_safe_scripts_without_deletion(self):
        base = case_dir()
        try:
            config = self.write_config(base)
            rc = vss.main(["--config", str(config), "--out-dir", str(base / "out")])
            self.assertEqual(rc, 0)
            latest = base / "out" / "vss_dryrun_plan_latest.json"
            self.assertTrue(latest.exists())
            data = json.loads(latest.read_text(encoding="utf-8"))
            create = Path(data["create_script"]).read_text(encoding="ascii")
            cleanup = Path(data["cleanup_script"]).read_text(encoding="ascii")
            self.assertIn("EXPOSE %os_snap% X:", create)
            self.assertNotIn("%%os_snap%%", create)
            self.assertNotIn("DELETE SHADOWS", cleanup.upper())
            self.assertEqual(data["execution"], "NOT_EXECUTED_DRY_RUN_ONLY")
        finally:
            shutil.rmtree(base, ignore_errors=True)

    def test_fails_closed_when_restore_target_escapes_root(self):
        base = case_dir()
        try:
            config = self.write_config(base, target=r"C:\outside_restore_target")
            rc = vss.main(["--config", str(config), "--out-dir", str(base / "out")])
            self.assertEqual(rc, 2)
            self.assertFalse((base / "out").exists())
        finally:
            shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
