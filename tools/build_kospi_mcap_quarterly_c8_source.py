from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_source_builder import (
    build_c8_source_bundle,
    write_build_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the validated C8 canonical source for the isolated strategy.")
    parser.add_argument("--request", default="", help="Repo-relative or absolute C8 build request JSON path.")
    parser.add_argument(
        "--expect-verdict",
        choices=["C8_SOURCE_BUILD_READY", "FAIL_C8_SOURCE_BUILD"],
        default="",
        help="A matching expected failure returns zero without weakening the verdict.",
    )
    parser.add_argument("--no-report", action="store_true", help="Do not publish build status reports.")
    args = parser.parse_args()

    request_path = Path(args.request) if args.request else None
    if request_path is not None and not request_path.is_absolute():
        request_path = ROOT / request_path
    report = build_c8_source_bundle(request_path=request_path)
    report_outputs: list[str] = []
    if not args.no_report:
        report_outputs = [str(path) for path in write_build_report(report)]

    print(
        json.dumps(
            {
                "status": report["status"],
                "verdict": report["verdict"],
                "reason_codes": report["reason_codes"],
                "published": report["published"],
                "m2_allowed": report["m2_allowed"],
                "candidate_selection_calculated": report["candidate_selection_calculated"],
                "orders_generated": report["orders_generated"],
                "outputs": report["outputs"],
                "report_outputs": report_outputs,
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
