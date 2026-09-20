from __future__ import annotations

import argparse
import json
from pathlib import Path

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_acquisition_session import (
    finalize_acquisition_session,
    inspect_acquisition_session,
    start_acquisition_session,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare and seal an isolated C8 acquisition session.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    start = subparsers.add_parser("start")
    start.add_argument("--selection-as-of", required=True)
    finalize = subparsers.add_parser("finalize")
    finalize.add_argument("--descriptor", type=Path, required=True)
    subparsers.add_parser("status")
    parser.add_argument("--expect-verdict", default=None)
    args = parser.parse_args()

    if args.command == "start":
        result = start_acquisition_session(selection_as_of=args.selection_as_of)
    elif args.command == "finalize":
        result = finalize_acquisition_session(descriptor_path=args.descriptor)
    else:
        result = inspect_acquisition_session()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if args.expect_verdict:
        return 0 if result["verdict"] == args.expect_verdict else 3
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
