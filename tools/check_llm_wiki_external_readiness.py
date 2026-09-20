from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
WIKI_ROOT = ROOT / "docs" / "llm_wiki"
REPORT_PATH = WIKI_ROOT / "05_Logs" / "external_readiness_latest.json"
TASK_NAME = "VIBE_LLM_Wiki_Pipeline"


def _scheduled_task_exists() -> dict[str, Any]:
    try:
        result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                f"Get-ScheduledTask -TaskName '{TASK_NAME}' -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty TaskName",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except Exception as exc:  # noqa: BLE001 - report-only probe
        return {"checked": False, "exists": False, "error": f"{type(exc).__name__}: {exc}"}
    return {
        "checked": True,
        "exists": TASK_NAME in result.stdout,
        "returncode": result.returncode,
        "stderr": result.stderr.strip(),
    }


def _env_present(name: str) -> bool:
    return bool(os.environ.get(name, "").strip())


def main() -> int:
    required_paths = {
        "pipeline_runner": ROOT / "run_llm_wiki_pipeline.bat",
        "collect_script": ROOT / "tools" / "collect_llm_wiki_exports.py",
        "contract_validator": ROOT / "tools" / "validate_llm_wiki_source_contract.py",
        "ingest_script": ROOT / "tools" / "ingest_llm_wiki_inbox.py",
        "snapshot_script": ROOT / "tools" / "build_llm_wiki_snapshot.py",
        "slack_import_dir": WIKI_ROOT / "00_Inbox" / "import_exports" / "slack_json",
        "meeting_import_dir": WIKI_ROOT / "00_Inbox" / "import_exports" / "meeting_text",
        "doc_import_dir": WIKI_ROOT / "00_Inbox" / "import_exports" / "doc_text",
    }
    env = {
        "LLM_WIKI_SLACK_BOT_TOKEN": _env_present("LLM_WIKI_SLACK_BOT_TOKEN"),
        "LLM_WIKI_SLACK_CHANNELS": _env_present("LLM_WIKI_SLACK_CHANNELS"),
        "LLM_WIKI_MEETING_SOURCE_DIR": _env_present("LLM_WIKI_MEETING_SOURCE_DIR"),
    }
    path_status = {name: path.exists() for name, path in required_paths.items()}
    scheduler = _scheduled_task_exists()
    local_blockers = []
    external_blockers = []
    if not all(path_status.values()):
        local_blockers.append("LOCAL_PATH_MISSING")
    if not scheduler.get("exists"):
        local_blockers.append("SCHEDULER_NOT_REGISTERED")
    if not env["LLM_WIKI_SLACK_BOT_TOKEN"]:
        external_blockers.append("SLACK_TOKEN_NOT_CONFIGURED")
    if not env["LLM_WIKI_SLACK_CHANNELS"]:
        external_blockers.append("SLACK_CHANNELS_NOT_CONFIGURED")
    if not env["LLM_WIKI_MEETING_SOURCE_DIR"]:
        external_blockers.append("MEETING_SOURCE_DIR_NOT_CONFIGURED")

    local_status = "READY" if not local_blockers else "BLOCKED"
    external_status = "READY" if not external_blockers else "BLOCKED"

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "root": str(ROOT),
        "scope": "read_only_external_readiness_probe",
        "policy_effect": False,
        "trading_effect": False,
        "task_name": TASK_NAME,
        "status": local_status if external_status == "READY" else "PARTIAL",
        "blockers": local_blockers + external_blockers,
        "local_pipeline": {
            "status": local_status,
            "blockers": local_blockers,
        },
        "external_api": {
            "status": external_status,
            "blockers": external_blockers,
        },
        "paths": {name: {"path": str(path), "exists": path_status[name]} for name, path in required_paths.items()},
        "environment": env,
        "scheduler": scheduler,
        "notes": [
            "This report does not create credentials, register scheduler tasks, call Slack, call meeting APIs, or change trading runtime.",
            "External API or scheduler apply requires separate explicit approval.",
        ],
    }
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(REPORT_PATH))
    print(f"status={payload['status']}")
    print(f"local_pipeline={local_status}")
    print(f"external_api={external_status}")
    print("blockers=" + ",".join(payload["blockers"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
