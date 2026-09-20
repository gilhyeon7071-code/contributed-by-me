from __future__ import annotations

"""Control Center V2 Forensic 'Evidence 검증 액션' wrapper — evidence_id=chain_hash_replay.

Read-only. Reuses build_canonical_replay_compare.build_report() (the existing
validated canonical fills replay/hash comparison) as the single source of truth.
Does not write any files, does not touch orders/fills/ledger/Gate/LOCK. Always
exits 0 — PASS/FAIL is carried in the JSON body's "status" field so the calling
Node route can always JSON.parse(stdout).
"""

import importlib.util
import json
from pathlib import Path

TOOLS = Path(__file__).resolve().parent


def _load_module(name: str):
    spec = importlib.util.spec_from_file_location(name, TOOLS / f"{name}.py")
    if spec is None or spec.loader is None:
        raise RuntimeError(f"{name} load failed")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


compare = _load_module("build_canonical_replay_compare")


def main() -> int:
    try:
        _rc, report = compare.build_report("")
    except Exception as exc:
        print(json.dumps({
            "status": "FAIL",
            "evidence_id": "chain_hash_replay",
            "reason": f"build_report_error: {exc}",
        }, ensure_ascii=False))
        return 0

    result = {
        "status": report.get("status", "FAIL"),
        "evidence_id": "chain_hash_replay",
        "D": report.get("D"),
        "checks": report.get("checks", {}),
        "counts": report.get("counts", {}),
        "report_path": report.get("paths", {}).get("report", ""),
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
