# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SCAN_TOOL = ROOT / "tools" / "scan_mojibake_text.py"


def _resolve_path(raw: str) -> Path:
    p = Path(str(raw or "").strip().strip('"'))
    if not p.is_absolute():
        p = ROOT / p
    return p.resolve()


def _target_files(paths: list[Path]) -> list[Path]:
    out: list[Path] = []
    for p in paths:
        if p.is_file():
            out.append(p)
    return sorted(set(out), key=lambda x: str(x).lower())


def _run(cmd: list[str]) -> dict[str, Any]:
    proc = subprocess.run(cmd, cwd=str(ROOT), text=True, capture_output=True)
    return {
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "stdout_tail": proc.stdout.splitlines()[-20:],
        "stderr_tail": proc.stderr.splitlines()[-20:],
    }


def _py_compile(py_files: list[Path]) -> dict[str, Any]:
    if not py_files:
        return {"status": "SKIP", "returncode": 0, "files": []}
    result = _run([sys.executable, "-m", "py_compile", *[str(p) for p in py_files]])
    result["status"] = "PASS" if result["returncode"] == 0 else "FAIL"
    result["files"] = [str(p) for p in py_files]
    return result


def _mojibake_scan(files: list[Path], out_json: Path) -> dict[str, Any]:
    cmd = [sys.executable, str(SCAN_TOOL)]
    for p in files:
        cmd.extend(["--path", str(p)])
    cmd.extend(["--out-json", str(out_json)])
    result = _run(cmd)
    result["status"] = "PASS" if result["returncode"] == 0 else "FAIL"
    result["out_json"] = str(out_json)
    try:
        payload = json.loads(out_json.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        payload = {"parse_error": str(exc)}
    result["summary"] = {
        "files_scanned": payload.get("files_scanned"),
        "issue_file_count": payload.get("issue_file_count"),
        "issue_count": payload.get("issue_count"),
        "repairable_count": payload.get("repairable_count"),
        "parse_error": payload.get("parse_error"),
    }
    return result


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Development text preflight for explicit changed files only."
    )
    ap.add_argument("--path", action="append", default=[], help="Changed file to check. Repeatable.")
    ap.add_argument("--out-json", default="", help="Preflight report path.")
    args = ap.parse_args()

    if not args.path:
        print("[DEV_TEXT_PREFLIGHT] --path is required; pass explicit changed files only.")
        return 2

    requested = [_resolve_path(x) for x in args.path]
    missing = [str(p) for p in requested if not p.exists()]
    files = _target_files(requested)
    if not files:
        print("[DEV_TEXT_PREFLIGHT] no existing files to check.")
        return 2

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = Path(args.out_json) if args.out_json else LOG_DIR / f"dev_text_preflight_{stamp}.json"
    scan_json = LOG_DIR / f"mojibake_dev_text_preflight_{stamp}.json"
    latest_json = LOG_DIR / "dev_text_preflight_latest.json"

    py_files = [p for p in files if p.suffix.lower() == ".py"]
    syntax = _py_compile(py_files)
    mojibake = _mojibake_scan(files, scan_json)

    status = "PASS"
    reasons: list[str] = []
    if missing:
        status = "FAIL"
        reasons.append("missing_path")
    if syntax.get("returncode") != 0:
        status = "FAIL"
        reasons.append("py_compile_failed")
    if mojibake.get("returncode") != 0:
        status = "FAIL"
        reasons.append("mojibake_issue")

    payload = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "reasons": reasons,
        "root": str(ROOT),
        "requested_paths": [str(p) for p in requested],
        "checked_files": [str(p) for p in files],
        "missing_paths": missing,
        "syntax": syntax,
        "mojibake": mojibake,
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    out_json.write_text(text, encoding="utf-8")
    latest_json.write_text(text, encoding="utf-8")

    print(f"[DEV_TEXT_PREFLIGHT] status={status} reasons={','.join(reasons) if reasons else '-'}")
    print(f"[DEV_TEXT_PREFLIGHT] report={out_json}")
    print(f"[DEV_TEXT_PREFLIGHT] latest={latest_json}")
    print(f"[DEV_TEXT_PREFLIGHT] mojibake_report={scan_json}")
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
