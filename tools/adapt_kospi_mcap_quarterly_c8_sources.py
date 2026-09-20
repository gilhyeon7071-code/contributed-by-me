from __future__ import annotations

import argparse
import json
from pathlib import Path

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_source_adapter import (
    adapt_c8_sources,
    write_adapter_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize evidence-backed C8 raw sources.")
    parser.add_argument("--request", type=Path, default=None)
    parser.add_argument(
        "--expect-verdict",
        choices=["C8_SOURCE_ADAPTER_READY", "FAIL_C8_SOURCE_ADAPTER"],
        default=None,
    )
    parser.add_argument("--no-write-report", action="store_true")
    args = parser.parse_args()

    report = adapt_c8_sources(request_path=args.request)
    outputs: list[str] = []
    if not args.no_write_report:
        outputs = [str(path) for path in write_adapter_report(report)]
    print(
        json.dumps(
            {
                "status": report["status"],
                "verdict": report["verdict"],
                "reason_codes": report["reason_codes"],
                "evidence_revalidated": report["evidence_revalidated"],
                "adapter_files_generated": report["adapter_files_generated"],
                "build_request_generated": report["build_request_generated"],
                "canonical_generated": report["canonical_generated"],
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
