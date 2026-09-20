from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_status_traversal import (
    build_contract_status_report,
    evaluate_status_traversal,
    write_status_traversal_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit the restartable C8 status traversal contract without fetching KRX data."
    )
    parser.add_argument("--manifest", type=Path, help="Existing traversal manifest to validate.")
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument(
        "--expect-verdict",
        choices=[
            "C8_STATUS_TRAVERSAL_CONTRACT_BLOCKED",
            "C8_STATUS_TRAVERSAL_CONTRACT_PASS",
            "FAIL_C8_STATUS_TRAVERSAL_CONTRACT",
            "FAIL_C8_STATUS_TRAVERSAL",
            "C8_STATUS_HISTORY_TRAVERSAL_COMPLETE_ROLE_BLOCKED",
            "C8_STATUS_ROLE_COVERAGE_PASS",
        ],
        default="",
    )
    args = parser.parse_args()

    if args.manifest:
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        report = evaluate_status_traversal(manifest)
    else:
        report = build_contract_status_report()
    outputs: list[str] = []
    if not args.no_write:
        outputs = [str(path) for path in write_status_traversal_report(report)]
    print(
        json.dumps(
            {
                "status": report["status"],
                "verdict": report["verdict"],
                "reason_codes": report["reason_codes"],
                "role_ready": report["role_ready"],
                "selection_evidence_eligible": report["selection_evidence_eligible"],
                "m2_allowed": report["m2_allowed"],
                "bulk_requests_executed": report.get("bulk_requests_executed", 0),
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
