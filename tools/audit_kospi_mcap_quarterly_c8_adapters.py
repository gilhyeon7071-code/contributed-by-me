from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_adapter_audit import (
    build_adapter_capability_report,
    write_adapter_capability_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit current KRX files for C8 adapter readiness.")
    parser.add_argument(
        "--expect-verdict",
        choices=["C8_ADAPTERS_READY", "C8_ADAPTERS_BLOCKED"],
        default="",
        help="A matching expected blocked verdict returns zero without weakening the result.",
    )
    parser.add_argument("--no-write", action="store_true", help="Do not publish capability reports.")
    args = parser.parse_args()

    report = build_adapter_capability_report()
    outputs: list[str] = []
    if not args.no_write:
        outputs = [str(path) for path in write_adapter_capability_report(report)]
    print(
        json.dumps(
            {
                "audit_execution_status": report["audit_execution_status"],
                "status": report["status"],
                "verdict": report["verdict"],
                "root_cause": report["root_cause"],
                "mapping_only_resolution_possible": report["mapping_only_resolution_possible"],
                "contract_fit_status": report["contract_fit"]["status"],
                "adapter_ready_roles": report["adapter_ready_roles"],
                "adapter_blocked_roles": report["adapter_blocked_roles"],
                "build_request_possible": report["build_request_possible"],
                "adapter_files_generated": report["adapter_files_generated"],
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
