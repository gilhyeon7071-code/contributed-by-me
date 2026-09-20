from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_same_day_snapshot_authority import (
    assess_same_day_snapshot_authority,
    write_same_day_authority_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read-only preflight for same-day KRX current-snapshot date authority."
    )
    parser.add_argument("--selection-as-of", required=True, help="YYYYMMDD selection date.")
    parser.add_argument(
        "--captured-at",
        default="",
        help="Timezone-aware ISO timestamp. Defaults to the current local timestamp.",
    )
    parser.add_argument("--authenticated-session-available", action="store_true")
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument(
        "--expect-verdict",
        choices=[
            "C8_SAME_DAY_AUTHORITY_PREFLIGHT_BLOCKED",
            "C8_SAME_DAY_AUTHORITY_CANDIDATE_REQUIRES_POLICY_APPROVAL",
            "C8_SAME_DAY_AUTHORITY_PREFLIGHT_PASS",
        ],
        default="",
    )
    args = parser.parse_args()
    captured_at = args.captured_at or datetime.now().astimezone().isoformat(timespec="seconds")
    report = assess_same_day_snapshot_authority(
        selection_as_of=args.selection_as_of,
        captured_at=captured_at,
        authenticated_session_available=args.authenticated_session_available,
    )
    outputs: list[str] = []
    if not args.no_write:
        outputs = [str(path) for path in write_same_day_authority_report(report)]
    print(
        json.dumps(
            {
                "status": report["status"],
                "verdict": report["verdict"],
                "reason_codes": report["reason_codes"],
                "same_day": report["same_day"],
                "post_close": report["post_close"],
                "authenticated_session_available": report["authenticated_session_available"],
                "candidate_ready_for_policy_review": report["candidate_ready_for_policy_review"],
                "existing_acquisition_contract_supported": report[
                    "existing_acquisition_contract_supported"
                ],
                "m2_allowed": report["m2_allowed"],
                "outputs": outputs,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if args.expect_verdict:
        return 0 if report["verdict"] == args.expect_verdict else 3
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
