import json
import shutil
import unittest
import uuid
from pathlib import Path

from tests.test_restore_drill_verify import deterministic_rsa_key, pem_rsa_public_key, rsa_sign_sha256
from tools import restore_drill_preflight as preflight


WORK_TMP = Path(r"E:\1_Data\backup\restore_drill_preflight_unit_tmp")
WORK_TMP.mkdir(parents=True, exist_ok=True)


def case_dir():
    path = WORK_TMP / uuid.uuid4().hex
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)
    return path


class RestoreDrillPreflightTests(unittest.TestCase):
    def build_config(self, base):
        restore = base / "restore"
        restore.mkdir()
        target = restore / "important.db"
        target.write_text("preflight", encoding="utf-8")
        manifest = base / "manifest.json"
        manifest_raw = b'{"files":[{"path":"important.db","sha256":"dummy"}]}'
        manifest.write_bytes(manifest_raw)
        n, e, d = deterministic_rsa_key()
        sig = base / "manifest.sig"
        sig.write_bytes(rsa_sign_sha256(manifest_raw, n, d))
        pub = base / "public.pem"
        pub.write_text(pem_rsa_public_key(n, e), encoding="ascii")
        proof = base / "object_lock_proof.json"
        proof.write_text(
            json.dumps(
                {
                    "objects": [
                        {
                            "key": "unit/eor.json",
                            "version_id": "v1",
                            "object_lock_mode": "COMPLIANCE",
                            "retain_until_date": "2031-01-01T00:00:00Z",
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        config = {
            "restore_verify": {
                "restore_root": str(restore),
                "manifest": str(manifest),
                "manifest_sig": str(sig),
                "public_key": str(pub),
                "object_lock_proof": str(proof),
                "allow_local_eor": False,
                "eor_dir": str(base / "eor"),
            },
            "windows_vss": {"enabled": False},
            "immutable_upload": {"enabled": False},
        }
        config_path = base / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        return config_path

    def test_preflight_passes_and_writes_result(self):
        base = case_dir()
        try:
            config = self.build_config(base)
            rc = preflight.main(["--config", str(config), "--out-dir", str(base / "out")])
            self.assertEqual(rc, 0)
            latest = base / "out" / "restore_drill_preflight_latest.json"
            self.assertTrue(latest.exists())
            data = json.loads(latest.read_text(encoding="utf-8"))
            self.assertEqual(data["status"], "PASS")
            self.assertEqual(data["windows_vss"]["status"], "NA")
            self.assertIn("run_restore_drill_verify.bat", data["verify_command"][0])
        finally:
            shutil.rmtree(base, ignore_errors=True)

    def test_preflight_fails_closed_without_object_lock_or_local_override(self):
        base = case_dir()
        try:
            config = self.build_config(base)
            data = json.loads(config.read_text(encoding="utf-8"))
            data["restore_verify"]["object_lock_proof"] = ""
            data["restore_verify"]["allow_local_eor"] = False
            config.write_text(json.dumps(data), encoding="utf-8")
            rc = preflight.main(["--config", str(config), "--out-dir", str(base / "out")])
            self.assertEqual(rc, 2)
            self.assertFalse((base / "out").exists())
        finally:
            shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
