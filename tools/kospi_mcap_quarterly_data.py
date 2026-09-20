from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_source_audit import build_report, write_report


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit C8 data-source readiness for the isolated quarterly strategy.")
    parser.add_argument("--selection-as-of", default="", help="Optional YYYYMMDD snapshot date to require.")
    parser.add_argument(
        "--expect-verdict",
        choices=["C8_UNIVERSE_SOURCE_READY", "FAIL_C8_UNIVERSE_SOURCE"],
        default="",
        help="Validation assertion. A matching expected fail returns zero without weakening the verdict.",
    )
    parser.add_argument("--no-write", action="store_true", help="Build the audit in memory without publishing reports.")
    args = parser.parse_args()

    report = build_report(selection_as_of=args.selection_as_of or None)
    outputs: list[str] = []
    if not args.no_write:
        outputs = [str(path) for path in write_report(report)]

    summary = {
        "audit_execution_status": report["audit_execution_status"],
        "status": report["status"],
        "verdict": report["verdict"],
        "m2_allowed": report["m2_allowed"],
        "candidate_selection_calculated": report["candidate_selection_calculated"],
        "orders_generated": report["orders_generated"],
        "outputs": outputs,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if args.expect_verdict:
        return 0 if report["verdict"] == args.expect_verdict else 3
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
