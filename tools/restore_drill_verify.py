import argparse
import base64
import datetime as dt
import hashlib
import json
import os
import socket
import sys
from pathlib import Path


SHA256_DIGESTINFO_PREFIX = bytes.fromhex(
    "3031300d060960864801650304020105000420"
)


class DrillFail(Exception):
    pass


def _read_len(data, offset):
    first = data[offset]
    offset += 1
    if first < 0x80:
        return first, offset
    n = first & 0x7F
    if n == 0 or n > 4:
        raise DrillFail("unsupported DER length")
    return int.from_bytes(data[offset : offset + n], "big"), offset + n


def _read_tlv(data, offset, expected_tag):
    if offset >= len(data) or data[offset] != expected_tag:
        raise DrillFail("invalid DER public key")
    offset += 1
    length, offset = _read_len(data, offset)
    end = offset + length
    if end > len(data):
        raise DrillFail("truncated DER public key")
    return data[offset:end], end


def _read_integer(data, offset):
    value, offset = _read_tlv(data, offset, 0x02)
    return int.from_bytes(value.lstrip(b"\x00"), "big"), offset


def _parse_pem_public_key(path):
    text = Path(path).read_text(encoding="ascii").strip()
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    header = lines[0]
    footer = lines[-1]
    body = "".join(lines[1:-1])
    der = base64.b64decode(body)

    if header == "-----BEGIN RSA PUBLIC KEY-----" and footer == "-----END RSA PUBLIC KEY-----":
        seq, end = _read_tlv(der, 0, 0x30)
        if end != len(der):
            raise DrillFail("trailing data in RSA public key")
        n, off = _read_integer(seq, 0)
        e, off = _read_integer(seq, off)
        if off != len(seq):
            raise DrillFail("trailing data in RSA public key sequence")
        return n, e

    if header == "-----BEGIN PUBLIC KEY-----" and footer == "-----END PUBLIC KEY-----":
        outer, end = _read_tlv(der, 0, 0x30)
        if end != len(der):
            raise DrillFail("trailing data in public key")
        _alg, off = _read_tlv(outer, 0, 0x30)
        bit_string, off = _read_tlv(outer, off, 0x03)
        if off != len(outer) or not bit_string or bit_string[0] != 0:
            raise DrillFail("invalid subject public key info")
        inner, inner_end = _read_tlv(bit_string[1:], 0, 0x30)
        if inner_end != len(bit_string) - 1:
            raise DrillFail("trailing data in subject public key")
        n, off = _read_integer(inner, 0)
        e, off = _read_integer(inner, off)
        if off != len(inner):
            raise DrillFail("trailing data in subject public key sequence")
        return n, e

    raise DrillFail("public key must be PEM RSA PUBLIC KEY or PUBLIC KEY")


def verify_rsa_sha256_signature(data, signature, public_key_path):
    n, e = _parse_pem_public_key(public_key_path)
    k = (n.bit_length() + 7) // 8
    if len(signature) != k:
        return False
    encoded = pow(int.from_bytes(signature, "big"), e, n).to_bytes(k, "big")
    digest = hashlib.sha256(data).digest()
    expected_tail = SHA256_DIGESTINFO_PREFIX + digest
    if not encoded.startswith(b"\x00\x01"):
        return False
    try:
        sep = encoded.index(b"\x00", 2)
    except ValueError:
        return False
    padding = encoded[2:sep]
    return len(padding) >= 8 and set(padding) == {0xFF} and encoded[sep + 1 :] == expected_tail


def sha256_file(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _safe_restore_path(root, rel):
    if Path(rel).is_absolute() or ".." in Path(rel).parts:
        raise DrillFail(f"unsafe manifest path: {rel}")
    full = (root / rel).resolve()
    root_resolved = root.resolve()
    if root_resolved != full and root_resolved not in full.parents:
        raise DrillFail(f"path escapes restore root: {rel}")
    return full


def load_manifest(path):
    raw = Path(path).read_bytes()
    try:
        data = json.loads(raw.decode("utf-8-sig"))
    except Exception as exc:
        raise DrillFail(f"manifest JSON parse failed: {exc}") from exc
    if not isinstance(data, dict):
        raise DrillFail("manifest must be a JSON object")
    files = data.get("files")
    if not isinstance(files, list) or not files:
        raise DrillFail("manifest.files must be a non-empty list")
    return raw, data


def verify_files(restore_root, files):
    results = []
    for item in files:
        rel = item.get("path")
        expected = str(item.get("sha256", "")).lower()
        if not rel or not expected:
            raise DrillFail("each manifest file needs path and sha256")
        if len(expected) != 64 or any(c not in "0123456789abcdef" for c in expected):
            raise DrillFail(f"invalid sha256 for {rel}")
        full = _safe_restore_path(restore_root, rel)
        if not full.exists() or not full.is_file():
            raise DrillFail(f"restored file missing: {rel}")
        actual = sha256_file(full).lower()
        if actual != expected:
            raise DrillFail(f"sha256 mismatch: {rel}")
        results.append({"path": rel, "sha256": actual, "status": "PASS"})
    return results


def _parse_utc(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(dt.timezone.utc)


def verify_object_lock_proof(path, now_utc):
    if not path:
        return {"status": "NA", "reason": "object_lock_proof_not_provided"}
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except Exception as exc:
        raise DrillFail(f"object lock proof parse failed: {exc}") from exc
    objects = data.get("objects")
    if not isinstance(objects, list) or not objects:
        raise DrillFail("object lock proof needs non-empty objects list")
    checked = []
    for obj in objects:
        key = obj.get("key")
        mode = obj.get("object_lock_mode") or obj.get("mode")
        retain = obj.get("retain_until_date") or obj.get("retain_until")
        version_id = obj.get("version_id")
        if not key or not retain or not version_id:
            raise DrillFail("object lock proof objects need key, version_id, retain_until_date")
        if mode != "COMPLIANCE":
            raise DrillFail(f"object lock mode is not COMPLIANCE for {key}")
        if _parse_utc(retain) <= now_utc:
            raise DrillFail(f"object lock retain date is not future for {key}")
        checked.append(
            {
                "key": key,
                "version_id": version_id,
                "object_lock_mode": mode,
                "retain_until_date": retain,
                "status": "PASS",
            }
        )
    return {"status": "PASS", "objects": checked}


def write_eor(eor_dir, evidence):
    eor_dir.mkdir(parents=True, exist_ok=True)
    out = eor_dir / f"restore_drill_eor_{evidence['run_id']}.json"
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(evidence, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, out)
    return out


def run(args):
    restore_root = Path(args.restore_root)
    if not restore_root.exists() or not restore_root.is_dir():
        raise DrillFail(f"restore root not found: {restore_root}")

    manifest_raw, manifest = load_manifest(args.manifest)
    try:
        sig = Path(args.manifest_sig).read_bytes()
    except FileNotFoundError as exc:
        raise DrillFail(f"manifest signature not found: {args.manifest_sig}") from exc
    if not verify_rsa_sha256_signature(manifest_raw, sig, args.public_key):
        raise DrillFail("manifest detached signature verification failed")

    file_results = verify_files(restore_root, manifest["files"])
    now_utc = dt.datetime.now(dt.timezone.utc)
    object_lock = verify_object_lock_proof(args.object_lock_proof, now_utc)
    if object_lock["status"] != "PASS" and not args.allow_local_eor:
        raise DrillFail("object lock proof is required unless --allow-local-eor is set")

    run_id = now_utc.strftime("%Y%m%dT%H%M%SZ")
    evidence = {
        "run_id": run_id,
        "status": "PASS" if object_lock["status"] == "PASS" else "PASS_LOCAL_EOR_ONLY",
        "host": socket.gethostname(),
        "restore_root": str(restore_root.resolve()),
        "manifest": str(Path(args.manifest).resolve()),
        "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
        "manifest_signature": str(Path(args.manifest_sig).resolve()),
        "snapshot_id": manifest.get("snapshot_id", "unknown"),
        "source_created_utc": manifest.get("created_utc", "unknown"),
        "files": file_results,
        "object_lock": object_lock,
        "generated_utc": now_utc.isoformat().replace("+00:00", "Z"),
    }
    eor_path = write_eor(Path(args.eor_dir), evidence)
    print(f"[RESTORE_DRILL] status={evidence['status']}")
    print(f"[RESTORE_DRILL] eor={eor_path}")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description="Verify an isolated restore drill and write EoR JSON.")
    parser.add_argument("--restore-root", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--manifest-sig", required=True)
    parser.add_argument("--public-key", required=True)
    parser.add_argument("--object-lock-proof")
    parser.add_argument("--allow-local-eor", action="store_true")
    parser.add_argument("--eor-dir", default=r"E:\1_Data\2_Logs\restore_drill_eor")
    args = parser.parse_args(argv)
    try:
        return run(args)
    except DrillFail as exc:
        print(f"[RESTORE_DRILL] FAILED {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
