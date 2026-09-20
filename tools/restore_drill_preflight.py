import argparse
import datetime as dt
import json
import os
import shutil
import socket
import sys
from pathlib import Path


ROOT = Path(r"E:\1_Data")
DEFAULT_OUT_DIR = ROOT / "2_Logs" / "restore_drill_preflight"


class PreflightFail(Exception):
    pass


def load_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except Exception as exc:
        raise PreflightFail(f"config parse failed: {exc}") from exc


def require_obj(data, key):
    value = data.get(key)
    if not isinstance(value, dict):
        raise PreflightFail(f"{key} must be an object")
    return value


def require_str(data, key):
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise PreflightFail(f"{key} must be a non-empty string")
    return value


def resolve_existing_file(value, label):
    path = Path(value)
    if not path.exists() or not path.is_file():
        raise PreflightFail(f"{label} file not found: {value}")
    return str(path.resolve())


def resolve_existing_dir(value, label):
    path = Path(value)
    if not path.exists() or not path.is_dir():
        raise PreflightFail(f"{label} directory not found: {value}")
    return str(path.resolve())


def ensure_under_root(value, label):
    path = Path(value).resolve()
    root = ROOT.resolve()
    if path != root and root not in path.parents:
        raise PreflightFail(f"{label} must be under {ROOT}: {value}")
    return str(path)


def validate_restore_verify(config):
    section = require_obj(config, "restore_verify")
    result = {
        "restore_root": resolve_existing_dir(require_str(section, "restore_root"), "restore_root"),
        "manifest": resolve_existing_file(require_str(section, "manifest"), "manifest"),
        "manifest_sig": resolve_existing_file(require_str(section, "manifest_sig"), "manifest_sig"),
        "public_key": resolve_existing_file(require_str(section, "public_key"), "public_key"),
        "eor_dir": ensure_under_root(require_str(section, "eor_dir"), "eor_dir"),
        "allow_local_eor": bool(section.get("allow_local_eor", False)),
    }
    object_lock_proof = section.get("object_lock_proof")
    if object_lock_proof:
        result["object_lock_proof"] = resolve_existing_file(object_lock_proof, "object_lock_proof")
    else:
        result["object_lock_proof"] = None
    return result


def validate_windows_vss(config):
    section = config.get("windows_vss", {})
    if not isinstance(section, dict):
        raise PreflightFail("windows_vss must be an object")
    enabled = bool(section.get("enabled", False))
    result = {"enabled": enabled}
    if not enabled:
        result["status"] = "NA"
        return result
    if os.name != "nt":
        raise PreflightFail("windows_vss enabled but OS is not Windows")
    diskshadow = shutil.which("diskshadow")
    if not diskshadow:
        raise PreflightFail("windows_vss enabled but diskshadow was not found")
    source_volume = require_str(section, "source_volume")
    expose_drive = require_str(section, "expose_drive")
    restore_target = ensure_under_root(require_str(section, "restore_target"), "restore_target")
    if not source_volume.endswith(":"):
        raise PreflightFail("windows_vss.source_volume must look like C:")
    if not expose_drive.endswith(":"):
        raise PreflightFail("windows_vss.expose_drive must look like X:")
    result.update(
        {
            "status": "PASS",
            "diskshadow": diskshadow,
            "source_volume": source_volume,
            "expose_drive": expose_drive,
            "restore_target": restore_target,
        }
    )
    return result


def validate_immutable(config):
    section = config.get("immutable_upload", {})
    if not isinstance(section, dict):
        raise PreflightFail("immutable_upload must be an object")
    enabled = bool(section.get("enabled", False))
    result = {"enabled": enabled}
    if not enabled:
        result["status"] = "NA"
        return result
    bucket = require_str(section, "bucket")
    key_prefix = require_str(section, "key_prefix")
    mode = require_str(section, "object_lock_mode")
    retain = require_str(section, "retain_until_date")
    if mode != "COMPLIANCE":
        raise PreflightFail("immutable_upload.object_lock_mode must be COMPLIANCE")
    retain_dt = dt.datetime.fromisoformat(retain.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
    if retain_dt <= dt.datetime.now(dt.timezone.utc):
        raise PreflightFail("immutable_upload.retain_until_date must be in the future")
    if not shutil.which("aws"):
        raise PreflightFail("immutable_upload enabled but aws CLI was not found")
    result.update(
        {
            "status": "PASS",
            "bucket": bucket,
            "key_prefix": key_prefix,
            "object_lock_mode": mode,
            "retain_until_date": retain,
        }
    )
    return result


def build_verify_command(restore):
    cmd = [
        "run_restore_drill_verify.bat",
        "--restore-root",
        restore["restore_root"],
        "--manifest",
        restore["manifest"],
        "--manifest-sig",
        restore["manifest_sig"],
        "--public-key",
        restore["public_key"],
        "--eor-dir",
        restore["eor_dir"],
    ]
    if restore.get("object_lock_proof"):
        cmd.extend(["--object-lock-proof", restore["object_lock_proof"]])
    if restore.get("allow_local_eor"):
        cmd.append("--allow-local-eor")
    return cmd


def write_result(out_dir, result):
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"restore_drill_preflight_{result['run_id']}.json"
    latest = out_dir / "restore_drill_preflight_latest.json"
    payload = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    out.write_text(payload, encoding="utf-8")
    latest.write_text(payload, encoding="utf-8")
    return out, latest


def run(args):
    config_path = Path(args.config)
    config = load_json(config_path)
    now = dt.datetime.now(dt.timezone.utc)
    restore = validate_restore_verify(config)
    vss = validate_windows_vss(config)
    immutable = validate_immutable(config)
    if restore["object_lock_proof"] is None and not restore["allow_local_eor"]:
        raise PreflightFail("object_lock_proof is required unless allow_local_eor is true")
    result = {
        "run_id": now.strftime("%Y%m%dT%H%M%SZ"),
        "status": "PASS",
        "generated_utc": now.isoformat().replace("+00:00", "Z"),
        "host": socket.gethostname(),
        "config": str(config_path.resolve()),
        "restore_verify": restore,
        "windows_vss": vss,
        "immutable_upload": immutable,
        "verify_command": build_verify_command(restore),
    }
    out, latest = write_result(Path(args.out_dir), result)
    print("[RESTORE_DRILL_PREFLIGHT] status=PASS")
    print(f"[RESTORE_DRILL_PREFLIGHT] result={out}")
    print(f"[RESTORE_DRILL_PREFLIGHT] latest={latest}")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description="Fail-closed preflight for restore drill inputs.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    args = parser.parse_args(argv)
    try:
        return run(args)
    except PreflightFail as exc:
        print(f"[RESTORE_DRILL_PREFLIGHT] FAILED {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
