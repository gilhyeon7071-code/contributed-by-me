from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_corporate_action_capture import (
    assess_listing_capture,
    build_contract_status_report,
    create_capture_manifest,
    evaluate_capture_manifest,
    write_capture_manifest,
    write_capture_report,
)


DEFAULT_MANIFEST_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/data/acquisition/checkpoints/"
    "c8_corporate_action_capture_manifest_latest.json"
)


def _failure_report(assessment: dict) -> dict:
    return {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "strategy_id": "KOSPI_MCAP_QUARTERLY_V1",
        "scope": "M1_C8_CORPORATE_ACTION_CAPTURE_PREFLIGHT",
        "status": "FAIL",
        "verdict": "FAIL_C8_CORPORATE_ACTION_CAPTURE_BASE_PREFLIGHT",
        "reason_codes": assessment["reason_codes"],
        "manifest_created": False,
        "base_universe_authorized": False,
        "base_universe_code_count": assessment["selected_code_count"],
        "bulk_requests_executed": 0,
        "network_execution_authorized": False,
        "full_capture_complete": False,
        "acquisition_evidence_published": False,
        "adapter_components_generated": False,
        "canonical_generated": False,
        "m2_allowed": False,
        "candidate_selection_calculated": False,
        "target_portfolio_calculated": False,
        "orders_generated": False,
        "operational_change": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the exact same-day KRX base universe and prepare a restartable "
            "OpenDART corporate-action capture manifest without making network requests."
        )
    )
    parser.add_argument("--listing-evidence", type=Path)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--manifest-output", type=Path, default=DEFAULT_MANIFEST_RELATIVE)
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument(
        "--expect-verdict",
        choices=[
            "C8_CORPORATE_ACTION_CAPTURE_BASE_BLOCKED",
            "FAIL_C8_CORPORATE_ACTION_CAPTURE_CONTRACT",
            "FAIL_C8_CORPORATE_ACTION_CAPTURE_BASE_PREFLIGHT",
            "FAIL_C8_CORPORATE_ACTION_CAPTURE_MANIFEST",
            "C8_CORPORATE_ACTION_CAPTURE_PENDING",
            "C8_CORPORATE_ACTION_FULL_CAPTURE_PASS",
        ],
        default="",
    )
    args = parser.parse_args()

    manifest = None
    if args.listing_evidence and args.manifest:
        parser.error("use only one of --listing-evidence or --manifest")
    if args.listing_evidence:
        evidence = json.loads(args.listing_evidence.read_text(encoding="utf-8"))
        assessment = assess_listing_capture(evidence, repo_root=ROOT)
        if assessment["status"] == "PASS":
            manifest = create_capture_manifest(evidence, repo_root=ROOT)
            report = evaluate_capture_manifest(manifest, repo_root=ROOT)
        else:
            report = _failure_report(assessment)
    elif args.manifest:
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        report = evaluate_capture_manifest(manifest, repo_root=ROOT)
    else:
        report = build_contract_status_report(repo_root=ROOT)

    outputs: list[str] = []
    if not args.no_write:
        if manifest is not None and args.listing_evidence:
            outputs.append(str(write_capture_manifest(manifest, args.manifest_output, repo_root=ROOT)))
        outputs.extend(str(path) for path in write_capture_report(report, repo_root=ROOT))
    print(
        json.dumps(
            {
                "status": report["status"],
                "verdict": report["verdict"],
                "reason_codes": report["reason_codes"],
                "manifest_created": report["manifest_created"],
                "base_universe_authorized": report["base_universe_authorized"],
                "base_universe_code_count": report.get("base_universe_code_count", 0),
                "minimum_request_count": report.get("minimum_request_count"),
                "bulk_requests_executed": report["bulk_requests_executed"],
                "network_execution_authorized": report["network_execution_authorized"],
                "full_capture_complete": report["full_capture_complete"],
                "m2_allowed": report["m2_allowed"],
                "orders_generated": report["orders_generated"],
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
