#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Validate that intraday_price_snapshot.py can be used for isolated new-method probes.

This is a read-only validator. It does not call KIS and does not rewrite canonical
intraday snapshot outputs.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools"
LOG_DIR = ROOT / "2_Logs"
TARGET = TOOLS / "intraday_price_snapshot.py"

OUT_JSON = LOG_DIR / "intraday_snapshot_isolated_options_validation_latest.json"
OUT_MD = LOG_DIR / "intraday_snapshot_isolated_options_validation_latest.md"


def _mtime(path: Path) -> str | None:
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")


def main() -> int:
    sys.path.insert(0, str(TOOLS))
    import intraday_price_snapshot as snap  # type: ignore

    source = TARGET.read_text(encoding="utf-8")
    canonical_csv = LOG_DIR / "intraday_prices_latest.csv"
    canonical_status = LOG_DIR / "intraday_prices_status.json"
    canonical_latest = LOG_DIR / "intraday_prices_status_latest.json"
    canonical_dated = LOG_DIR / f"intraday_prices_status_{datetime.now().strftime('%Y%m%d')}.json"

    custom_status = LOG_DIR / "probe_status.json"
    custom_csv = LOG_DIR / "probe.csv"
    default_paths = {k: str(v) for k, v in snap._intraday_status_output_paths(snap.OUT_CSV).items()}
    custom_paths = {
        k: str(v)
        for k, v in snap._intraday_status_output_paths(custom_csv, status_path=custom_status).items()
    }

    checks = {
        "arg_out_status_json_present": "--out-status-json" in source,
        "arg_codes_only_present": "--codes-only" in source,
        "codes_only_initial_source_skip_present": "if args.codes_only:" in source
        and "realtime_surge_codes, realtime_surge_selection = [], {\"status\": \"SKIPPED_CODES_ONLY\"}" in source
        and "recheck_observation_codes, recheck_observation_selection = [], {\"status\": \"SKIPPED_CODES_ONLY\"}" in source,
        "codes_only_ev_source_skip_present": "ev_source_queue_observation_codes, ev_source_queue_observation_selection = [], {\"status\": \"SKIPPED_CODES_ONLY\"}" in source,
        "codes_only_ev_pending_future_skip_present": "skip_status = \"SKIPPED_CODES_ONLY\" if args.codes_only else \"SKIPPED_EV_SOURCE_QUEUES_ONLY\"" in source
        and "future_signal_codes = []" in source,
        "custom_status_outputs_only_custom_status": custom_paths == {"status": str(custom_status)},
        "default_status_outputs_keep_canonical": set(default_paths.keys()) == {"status", "latest", "dated"},
    }

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "target": str(TARGET),
        "status": "PASS" if all(checks.values()) else "FAIL",
        "checks": checks,
        "default_paths": default_paths,
        "custom_paths": custom_paths,
        "canonical_outputs_observed": {
            "csv": {"path": str(canonical_csv), "mtime": _mtime(canonical_csv), "size": canonical_csv.stat().st_size if canonical_csv.exists() else None},
            "status": {"path": str(canonical_status), "mtime": _mtime(canonical_status), "size": canonical_status.stat().st_size if canonical_status.exists() else None},
            "latest": {"path": str(canonical_latest), "mtime": _mtime(canonical_latest), "size": canonical_latest.stat().st_size if canonical_latest.exists() else None},
            "dated": {"path": str(canonical_dated), "mtime": _mtime(canonical_dated), "size": canonical_dated.stat().st_size if canonical_dated.exists() else None},
        },
    }

    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# Intraday Snapshot Isolated Options Validation",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- status: {payload['status']}",
        f"- target: {TARGET}",
        "",
        "## Checks",
    ]
    for key, value in checks.items():
        lines.append(f"- {key}: {'PASS' if value else 'FAIL'}")
    lines.extend(
        [
            "",
            "## Canonical outputs observed only",
        ]
    )
    for key, value in payload["canonical_outputs_observed"].items():
        lines.append(f"- {key}: mtime={value['mtime']} size={value['size']} path={value['path']}")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": payload["status"], "json": str(OUT_JSON), "md": str(OUT_MD)}, ensure_ascii=False))
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
