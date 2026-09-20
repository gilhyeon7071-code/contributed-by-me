import argparse
import datetime as dt
import json
import re
import socket
import sys
from pathlib import Path


ROOT = Path(r"E:\1_Data")
DEFAULT_OUT_DIR = ROOT / "2_Logs" / "restore_drill_vss_dryrun"
ALIAS = "os_snap"


class VssPlanFail(Exception):
    pass


def load_config(path):
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except Exception as exc:
        raise VssPlanFail(f"config parse failed: {exc}") from exc
    section = data.get("windows_vss")
    if not isinstance(section, dict):
        raise VssPlanFail("windows_vss must be an object")
    return section


def require_drive(value, label):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z]:", value):
        raise VssPlanFail(f"{label} must look like C:")
    return value.upper()


def require_target(value):
    if not isinstance(value, str) or not value.strip():
        raise VssPlanFail("windows_vss.restore_target must be a non-empty string")
    path = Path(value).resolve()
    root = ROOT.resolve()
    if path != root and root not in path.parents:
        raise VssPlanFail(f"restore_target must be under {ROOT}: {value}")
    return path


def build_create_script(source_volume, expose_drive):
    return "\r\n".join(
        [
            "SET CONTEXT PERSISTENT",
            "BEGIN BACKUP",
            f"ADD VOLUME {source_volume} ALIAS {ALIAS}",
            "CREATE",
            f"EXPOSE %{ALIAS}% {expose_drive}",
            "END BACKUP",
            "",
        ]
    )


def build_cleanup_script(expose_drive):
    return "\r\n".join(
        [
            f"UNEXPOSE {expose_drive}",
            "",
        ]
    )


def validate_scripts(create_text, cleanup_text):
    if f"EXPOSE %{ALIAS}%" not in create_text:
        raise VssPlanFail("create script lost DiskShadow alias marker")
    if f"%%{ALIAS}%%" in create_text:
        raise VssPlanFail("create script contains batch-escaped alias; .dsh must use single percent")
    if "DELETE SHADOWS" in cleanup_text.upper():
        raise VssPlanFail("cleanup script must not delete snapshots")


def write_outputs(out_dir, source_volume, expose_drive, restore_target):
    now = dt.datetime.now(dt.timezone.utc)
    run_id = now.strftime("%Y%m%dT%H%M%SZ")
    target_dir = out_dir / run_id
    target_dir.mkdir(parents=True, exist_ok=True)
    create_text = build_create_script(source_volume, expose_drive)
    cleanup_text = build_cleanup_script(expose_drive)
    validate_scripts(create_text, cleanup_text)
    create_path = target_dir / "diskshadow_create.dsh"
    cleanup_path = target_dir / "diskshadow_cleanup_success_only.dsh"
    create_path.write_text(create_text, encoding="ascii")
    cleanup_path.write_text(cleanup_text, encoding="ascii")
    plan = {
        "run_id": run_id,
        "status": "PASS",
        "generated_utc": now.isoformat().replace("+00:00", "Z"),
        "host": socket.gethostname(),
        "source_volume": source_volume,
        "expose_drive": expose_drive,
        "restore_target": str(restore_target),
        "create_script": str(create_path),
        "cleanup_script": str(cleanup_path),
        "execution": "NOT_EXECUTED_DRY_RUN_ONLY",
        "failure_policy": "preserve_snapshot_do_not_cleanup",
    }
    plan_path = target_dir / "vss_dryrun_plan.json"
    latest_path = out_dir / "vss_dryrun_plan_latest.json"
    payload = json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True)
    plan_path.write_text(payload, encoding="utf-8")
    latest_path.write_text(payload, encoding="utf-8")
    return plan_path, latest_path


def run(args):
    section = load_config(args.config)
    source_volume = require_drive(section.get("source_volume"), "windows_vss.source_volume")
    expose_drive = require_drive(section.get("expose_drive"), "windows_vss.expose_drive")
    if source_volume == expose_drive:
        raise VssPlanFail("source_volume and expose_drive must differ")
    restore_target = require_target(section.get("restore_target"))
    out, latest = write_outputs(Path(args.out_dir), source_volume, expose_drive, restore_target)
    print("[RESTORE_DRILL_VSS_DRYRUN] status=PASS")
    print(f"[RESTORE_DRILL_VSS_DRYRUN] plan={out}")
    print(f"[RESTORE_DRILL_VSS_DRYRUN] latest={latest}")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description="Generate DiskShadow scripts for restore drill dry-run.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    args = parser.parse_args(argv)
    try:
        return run(args)
    except VssPlanFail as exc:
        print(f"[RESTORE_DRILL_VSS_DRYRUN] FAILED {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
