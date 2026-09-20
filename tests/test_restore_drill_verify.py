import base64
import hashlib
import json
import random
import shutil
import unittest
import uuid
from pathlib import Path

from tools import restore_drill_verify as rdv


WORK_TMP = Path(r"E:\1_Data\backup\restore_drill_unit_tmp")
WORK_TMP.mkdir(parents=True, exist_ok=True)


def case_dir():
    path = WORK_TMP / uuid.uuid4().hex
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)
    return path


def der_len(n):
    if n < 0x80:
        return bytes([n])
    raw = n.to_bytes((n.bit_length() + 7) // 8, "big")
    return bytes([0x80 | len(raw)]) + raw


def der_tlv(tag, value):
    return bytes([tag]) + der_len(len(value)) + value


def der_int(n):
    raw = n.to_bytes((n.bit_length() + 7) // 8, "big")
    if raw[0] & 0x80:
        raw = b"\x00" + raw
    return der_tlv(0x02, raw)


def pem_rsa_public_key(n, e):
    der = der_tlv(0x30, der_int(n) + der_int(e))
    body = base64.encodebytes(der).decode("ascii").replace("\n", "")
    lines = [body[i : i + 64] for i in range(0, len(body), 64)]
    return "-----BEGIN RSA PUBLIC KEY-----\n" + "\n".join(lines) + "\n-----END RSA PUBLIC KEY-----\n"


def rsa_sign_sha256(data, n, d):
    k = (n.bit_length() + 7) // 8
    digest_info = rdv.SHA256_DIGESTINFO_PREFIX + hashlib.sha256(data).digest()
    padding_len = k - len(digest_info) - 3
    encoded = b"\x00\x01" + (b"\xff" * padding_len) + b"\x00" + digest_info
    return pow(int.from_bytes(encoded, "big"), d, n).to_bytes(k, "big")


def is_probable_prime(n):
    if n < 2:
        return False
    small_primes = [3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37]
    for p in small_primes:
        if n == p:
            return True
        if n % p == 0:
            return False
    d = n - 1
    s = 0
    while d % 2 == 0:
        s += 1
        d //= 2
    for a in [2, 3, 5, 7, 11, 13, 17]:
        if a >= n:
            continue
        x = pow(a, d, n)
        if x == 1 or x == n - 1:
            continue
        for _ in range(s - 1):
            x = pow(x, 2, n)
            if x == n - 1:
                break
        else:
            return False
    return True


def deterministic_prime(bits, seed):
    rnd = random.Random(seed)
    while True:
        n = rnd.getrandbits(bits) | (1 << (bits - 1)) | 1
        if is_probable_prime(n):
            return n


def deterministic_rsa_key():
    e = 65537
    p = deterministic_prime(384, 20260429)
    q = deterministic_prime(384, 20260430)
    phi = (p - 1) * (q - 1)
    d = pow(e, -1, phi)
    return p * q, e, d


class RestoreDrillVerifyTests(unittest.TestCase):
    def test_verifies_manifest_signature_and_hashes_local_eor(self):
        n, e, d = deterministic_rsa_key()
        base = case_dir()
        try:
            restore = base / "restore"
            restore.mkdir()
            target = restore / "important.db"
            target.write_bytes(b"restore-drill-data")
            manifest = {
                "snapshot_id": "unit-test",
                "created_utc": "2026-04-29T00:00:00Z",
                "files": [{"path": "important.db", "sha256": hashlib.sha256(target.read_bytes()).hexdigest()}],
            }
            manifest_path = base / "manifest.json"
            manifest_raw = json.dumps(manifest, sort_keys=True).encode("utf-8")
            manifest_path.write_bytes(manifest_raw)
            sig_path = base / "manifest.sig"
            sig_path.write_bytes(rsa_sign_sha256(manifest_raw, n, d))
            pub_path = base / "public.pem"
            pub_path.write_text(pem_rsa_public_key(n, e), encoding="ascii")

            rc = rdv.main(
                [
                    "--restore-root",
                    str(restore),
                    "--manifest",
                    str(manifest_path),
                    "--manifest-sig",
                    str(sig_path),
                    "--public-key",
                    str(pub_path),
                    "--allow-local-eor",
                    "--eor-dir",
                    str(base / "eor"),
                ]
            )
            self.assertEqual(rc, 0)
            eor_files = list((base / "eor").glob("restore_drill_eor_*.json"))
            self.assertEqual(len(eor_files), 1)
            evidence = json.loads(eor_files[0].read_text(encoding="utf-8"))
            self.assertEqual(evidence["status"], "PASS_LOCAL_EOR_ONLY")
            self.assertEqual(evidence["files"][0]["status"], "PASS")
        finally:
            shutil.rmtree(base, ignore_errors=True)

    def test_missing_object_lock_fails_closed_without_local_override(self):
        base = case_dir()
        try:
            restore = base / "restore"
            restore.mkdir()
            (restore / "important.db").write_bytes(b"x")
            manifest = base / "manifest.json"
            manifest.write_text(
                json.dumps(
                    {
                        "files": [
                            {
                                "path": "important.db",
                                "sha256": hashlib.sha256(b"x").hexdigest(),
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            sig = base / "missing.sig"
            pub = base / "missing.pem"
            rc = rdv.main(
                [
                    "--restore-root",
                    str(restore),
                    "--manifest",
                    str(manifest),
                    "--manifest-sig",
                    str(sig),
                    "--public-key",
                    str(pub),
                    "--eor-dir",
                    str(base / "eor"),
                ]
            )
            self.assertEqual(rc, 2)
            self.assertFalse((base / "eor").exists())
        finally:
            shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    WORK_TMP.mkdir(parents=True, exist_ok=True)
    unittest.main()
