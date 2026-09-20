from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]


def _rel_or_abs(path_text: str) -> Path:
    path = Path(path_text)
    return path if path.is_absolute() else ROOT / path


def _write_status(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Run a subprocess with a hard timeout and JSON evidence.")
    ap.add_argument("--name", default="step")
    ap.add_argument("--timeout-sec", type=float, required=True)
    ap.add_argument("--status-json", required=True)
    ap.add_argument("cmd", nargs=argparse.REMAINDER)
    args = ap.parse_args(argv)

    cmd = list(args.cmd or [])
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]
    status_path = _rel_or_abs(str(args.status_json))
    started_at = datetime.now().isoformat(timespec="seconds")
    payload: Dict[str, Any] = {
        "generated_at": started_at,
        "name": str(args.name),
        "status": "RUNNING",
        "timeout_sec": float(args.timeout_sec),
        "cmd": cmd,
        "returncode": None,
        "timed_out": False,
    }
    _write_status(status_path, payload)

    if not cmd:
        payload.update(
            {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "status": "FAIL",
                "reason": "empty_command",
                "returncode": 87,
            }
        )
        _write_status(status_path, payload)
        print(json.dumps({"status": "FAIL", "reason": "empty_command", "status_json": str(status_path)}, ensure_ascii=False))
        return 87

    try:
        cp = subprocess.run(
            cmd,
            cwd=str(ROOT),
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=max(1.0, float(args.timeout_sec)),
            check=False,
        )
        rc = int(cp.returncode)
        payload.update(
            {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "status": "PASS" if rc == 0 else "FAIL",
                "reason": "ok" if rc == 0 else "subprocess_nonzero",
                "returncode": rc,
                "timed_out": False,
            }
        )
        _write_status(status_path, payload)
        print(json.dumps({"status": payload["status"], "returncode": rc, "status_json": str(status_path)}, ensure_ascii=False))
        return rc
    except subprocess.TimeoutExpired as exc:
        payload.update(
            {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "status": "FAIL",
                "reason": "timeout",
                "returncode": 124,
                "timed_out": True,
                "timeout_sec": float(args.timeout_sec),
                "partial_stdout_len": len(exc.stdout or ""),
                "partial_stderr_len": len(exc.stderr or ""),
            }
        )
        _write_status(status_path, payload)
        print(
            json.dumps(
                {
                    "status": "FAIL",
                    "reason": "timeout",
                    "timeout_sec": float(args.timeout_sec),
                    "status_json": str(status_path),
                },
                ensure_ascii=False,
            )
        )
        return 124


if __name__ == "__main__":
    raise SystemExit(main())
