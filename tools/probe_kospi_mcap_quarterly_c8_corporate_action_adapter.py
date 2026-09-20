from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_corporate_action_adapter import (
    run_corporate_action_connectivity_probe,
    write_corporate_action_probe_report,
)


def _api_key(path: Path) -> str:
    value = str(os.getenv("DART_API_KEY", "")).strip()
    if value:
        return value
    if path.is_file():
        return path.read_text(encoding="utf-8-sig").strip()
    return ""


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run an isolated, non-persisting OpenDART C8 adapter connectivity probe."
    )
    parser.add_argument("--live", action="store_true", help="Explicitly allow read-only API requests.")
    parser.add_argument("--selection-as-of", required=True)
    parser.add_argument("--query-start", required=True)
    parser.add_argument("--code", default="005930")
    parser.add_argument("--api-key-file", type=Path, default=ROOT / "_cache/dart_api_key.txt")
    parser.add_argument("--timeout", type=float, default=40.0)
    parser.add_argument("--write-report", action="store_true")
    args = parser.parse_args()

    if not args.live:
        report = {
            "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "strategy_id": "KOSPI_MCAP_QUARTERLY_V1",
            "scope": "C8_CORPORATE_ACTION_ADAPTER_CONNECTIVITY_PROBE",
            "status": "FAIL",
            "verdict": "FAIL_C8_CORPORATE_ACTION_ADAPTER_CONNECTIVITY",
            "reason_codes": ["LIVE_PROBE_NOT_EXPLICITLY_ENABLED"],
            "full_capture_evidence": False,
            "raw_payload_persisted": False,
            "canonical_publish_allowed": False,
            "m2_allowed": False,
            "candidate_selection_allowed": False,
            "orders_allowed": False,
            "operational_change": False,
        }
    else:
        report = run_corporate_action_connectivity_probe(
            selection_as_of=args.selection_as_of,
            query_start=args.query_start,
            stock_code=args.code,
            api_key=_api_key(args.api_key_file),
            timeout=args.timeout,
        )
        report["generated_at"] = datetime.now().astimezone().isoformat(timespec="seconds")

    if args.write_report:
        versioned, latest = write_corporate_action_probe_report(report)
        report["outputs"] = [
            versioned.relative_to(ROOT).as_posix(),
            latest.relative_to(ROOT).as_posix(),
        ]
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
