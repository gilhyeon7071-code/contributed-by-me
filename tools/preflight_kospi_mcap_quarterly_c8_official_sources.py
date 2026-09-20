from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_official_source_preflight import (
    run_official_source_preflight,
    write_official_source_preflight_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only preflight for official KRX C8 API routes.")
    parser.add_argument("--selection-as-of", required=True, help="YYYYMMDD date to query.")
    parser.add_argument(
        "--key-file",
        type=Path,
        default=ROOT / "_cache" / "krx_api_key.txt",
        help="Local AUTH_KEY file. Its value is never printed or persisted in the report.",
    )
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument(
        "--expect-verdict",
        choices=["C8_OFFICIAL_API_PREFLIGHT_PASS", "FAIL_C8_OFFICIAL_SOURCE_PREFLIGHT"],
        default="",
    )
    args = parser.parse_args()

    try:
        auth_key = args.key_file.read_text(encoding="utf-8").strip()
    except OSError:
        auth_key = ""
    report = run_official_source_preflight(
        selection_as_of=args.selection_as_of,
        auth_key=auth_key,
        timeout=max(1.0, args.timeout),
    )
    outputs: list[str] = []
    if not args.no_write:
        outputs = [str(path) for path in write_official_source_preflight_report(report)]
    print(
        json.dumps(
            {
                "status": report["status"],
                "verdict": report["verdict"],
                "reason_codes": report["reason_codes"],
                "auth_key_present": report["auth_key_present"],
                "auth_key_persisted": report["auth_key_persisted"],
                "api_routes_pass": report.get("api_routes_pass", False),
                "all_required_roles_ready": report["all_required_roles_ready"],
                "selection_evidence_eligible": report["selection_evidence_eligible"],
                "route_checks": [
                    {
                        "role": item["role"],
                        "source_id": item["source_id"],
                        "status": item["status"],
                        "reason_codes": item["reason_codes"],
                        "http_status": item["http_status"],
                        "row_count": item["row_count"],
                        "output_as_of_values": item["output_as_of_values"],
                    }
                    for item in report["route_checks"]
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
