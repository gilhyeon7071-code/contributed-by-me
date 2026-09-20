from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_manual_collection_readiness import (
    evaluate_manual_collection_readiness,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Check C8 manual collection inputs without network access.")
    parser.add_argument(
        "--expect-verdict",
        choices=["BLOCKED_C8_MANUAL_COLLECTION", "C8_MANUAL_COLLECTION_READY"],
        default=None,
    )
    args = parser.parse_args()
    report = evaluate_manual_collection_readiness()
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if args.expect_verdict:
        return 0 if report["verdict"] == args.expect_verdict else 3
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
