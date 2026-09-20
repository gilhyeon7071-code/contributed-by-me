from __future__ import annotations

import copy
import json
import shutil
import sys
import tempfile
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src import contracts
from paper.strategies.kospi_mcap_quarterly_v1.src.c8_status_traversal import (
    CONTRACT_RELATIVE,
    HISTORY_SOURCE_IDS,
    LIQUIDATION_SOURCE_ID,
    ROUTE_CONTRACT_RELATIVE,
    build_contract_status_report,
    create_status_traversal_manifest,
    evaluate_status_traversal,
    load_status_traversal_contract,
    next_pending_codes,
    record_history_result,
    record_liquidation_snapshot,
    write_status_traversal_manifest,
)


def _repo() -> Path:
    root = Path(tempfile.mkdtemp(prefix="kospi_mcap_c8_status_"))
    for relative in (CONTRACT_RELATIVE, ROUTE_CONTRACT_RELATIVE):
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(contracts.REPO_ROOT / relative, destination)
    return root


def _manifest(root: Path) -> dict:
    return create_status_traversal_manifest(
        selection_as_of="20260914",
        query_start="19900101",
        earliest_listed_date="19950101",
        universe_codes=["000003", "000001", "000002"],
        universe_source_sha256="a" * 64,
        generated_at="2026-09-15T10:20:00+09:00",
        repo_root=root,
    )


def _query(source_id: str, code: str) -> dict[str, str]:
    query = {"isuCd": code, "strtDd": "19900101", "endDd": "20260914"}
    if source_id == "KRX_MDCSTAT215_MANAGEMENT_HISTORY":
        query["designDdYn"] = "N"
    return query


def _complete_histories(manifest: dict) -> dict:
    current = manifest
    for source_id in HISTORY_SOURCE_IDS:
        for code in manifest["universe"]["codes"]:
            current = record_history_result(
                current,
                source_id=source_id,
                code=code,
                query_parameters=_query(source_id, code),
                status="PASS",
                payload_sha256=code[-1] * 64,
                row_count=0,
                captured_at="2026-09-15T10:21:00+09:00",
            )
    return current


def test_manifest_is_exact_and_restart_order_is_deterministic():
    root = _repo()
    try:
        manifest = _manifest(root)
        assert manifest["universe"]["codes"] == ["000001", "000002", "000003"]
        assert next_pending_codes(manifest, HISTORY_SOURCE_IDS[0], 2) == ["000001", "000002"]
        updated = record_history_result(
            manifest,
            source_id=HISTORY_SOURCE_IDS[0],
            code="000001",
            query_parameters=_query(HISTORY_SOURCE_IDS[0], "000001"),
            status="PASS",
            payload_sha256="1" * 64,
            row_count=0,
            captured_at="2026-09-15T10:21:00+09:00",
        )
        assert next_pending_codes(updated, HISTORY_SOURCE_IDS[0], 3) == ["000002", "000003"]
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_failed_attempt_can_retry_but_success_conflict_is_rejected():
    root = _repo()
    try:
        manifest = _manifest(root)
        failed = record_history_result(
            manifest,
            source_id=HISTORY_SOURCE_IDS[0],
            code="000001",
            query_parameters=_query(HISTORY_SOURCE_IDS[0], "000001"),
            status="FAIL",
            payload_sha256="1" * 64,
            row_count=0,
            captured_at="2026-09-15T10:21:00+09:00",
            error_code="HTTP_500",
        )
        passed = record_history_result(
            failed,
            source_id=HISTORY_SOURCE_IDS[0],
            code="000001",
            query_parameters=_query(HISTORY_SOURCE_IDS[0], "000001"),
            status="PASS",
            payload_sha256="2" * 64,
            row_count=0,
            captured_at="2026-09-15T10:22:00+09:00",
        )
        replay = record_history_result(
            passed,
            source_id=HISTORY_SOURCE_IDS[0],
            code="000001",
            query_parameters=_query(HISTORY_SOURCE_IDS[0], "000001"),
            status="PASS",
            payload_sha256="2" * 64,
            row_count=0,
            captured_at="2026-09-15T10:22:00+09:00",
        )
        assert replay == passed
        assert len(passed["sources"][HISTORY_SOURCE_IDS[0]]["records"]["000001"]["attempts"]) == 2
        with pytest.raises(contracts.ContractError, match="conflicting replay"):
            record_history_result(
                passed,
                source_id=HISTORY_SOURCE_IDS[0],
                code="000001",
                query_parameters=_query(HISTORY_SOURCE_IDS[0], "000001"),
                status="PASS",
                payload_sha256="3" * 64,
                row_count=1,
                captured_at="2026-09-15T10:23:00+09:00",
            )
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_wrong_range_and_unknown_code_are_rejected():
    root = _repo()
    try:
        manifest = _manifest(root)
        wrong = _query(HISTORY_SOURCE_IDS[0], "000001")
        wrong["endDd"] = "20260913"
        with pytest.raises(contracts.ContractError, match="selection_as_of"):
            record_history_result(
                manifest,
                source_id=HISTORY_SOURCE_IDS[0],
                code="000001",
                query_parameters=wrong,
                status="PASS",
                payload_sha256="1" * 64,
                row_count=0,
                captured_at="2026-09-15T10:21:00+09:00",
            )
        with pytest.raises(contracts.ContractError, match="outside the exact base universe"):
            record_history_result(
                manifest,
                source_id=HISTORY_SOURCE_IDS[0],
                code="999999",
                query_parameters=_query(HISTORY_SOURCE_IDS[0], "999999"),
                status="PASS",
                payload_sha256="1" * 64,
                row_count=0,
                captured_at="2026-09-15T10:21:00+09:00",
            )
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_complete_histories_still_require_liquidation_acquisition_evidence():
    root = _repo()
    try:
        manifest = _complete_histories(_manifest(root))
        manifest = record_liquidation_snapshot(
            manifest,
            query_parameters={"mktId": "STK"},
            payload_sha256="f" * 64,
            row_count=0,
            captured_at="2026-09-15T10:22:00+09:00",
            claimed_as_of="20260914",
            claimed_as_of_authority="OFFICIAL_RESPONSE_FIELD",
        )
        report = evaluate_status_traversal(
            manifest, repo_root=root, generated_at="2026-09-15T10:23:00+09:00"
        )
        assert report["status"] == "BLOCKED"
        assert report["verdict"] == "C8_STATUS_HISTORY_TRAVERSAL_COMPLETE_ROLE_BLOCKED"
        assert report["history_traversal_complete"] is True
        assert report["liquidation_snapshot_captured"] is True
        assert report["liquidation_date_authorized"] is False
        assert report["blocked_fields"] == ["delisting_procedure_status"]
        assert "LIQUIDATION_RETRIEVAL_EVIDENCE_REQUIRED" in report["reason_codes"]
        assert "LIQUIDATION_ASOF_AUTHORITY_UNRESOLVED" not in report["reason_codes"]
        assert report["role_ready"] is False
        assert report["selection_evidence_eligible"] is False
        assert report["m2_allowed"] is False
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_manifest_tampering_fails_closed_and_writes_only_inside_strategy_namespace():
    root = _repo()
    try:
        manifest = _manifest(root)
        tampered = copy.deepcopy(manifest)
        tampered["universe"]["codes"].append("000004")
        report = evaluate_status_traversal(
            tampered, repo_root=root, generated_at="2026-09-15T10:23:00+09:00"
        )
        assert report["status"] == "FAIL"
        assert "UNIVERSE_CODE_SET_HASH_MISMATCH" in report["reason_codes"]
        output = root / "paper/strategies/kospi_mcap_quarterly_v1/data/inbox/status/manifest.json"
        assert write_status_traversal_manifest(manifest, output, repo_root=root).is_file()
        with pytest.raises(ValueError, match="outside strategy namespace"):
            write_status_traversal_manifest(manifest, root / "outside.json", repo_root=root)
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_success_record_query_or_fingerprint_tampering_fails_closed():
    root = _repo()
    try:
        manifest = _complete_histories(_manifest(root))
        tampered = copy.deepcopy(manifest)
        latest = tampered["sources"][HISTORY_SOURCE_IDS[0]]["records"]["000001"]["attempts"][-1]
        latest["query_parameters"]["endDd"] = "20260913"
        report = evaluate_status_traversal(
            tampered, repo_root=root, generated_at="2026-09-15T10:23:00+09:00"
        )
        assert report["status"] == "FAIL"
        assert f"{HISTORY_SOURCE_IDS[0]}_RESULT_CONTRACT_INVALID" in report["reason_codes"]
        assert report["history_checks"][0]["invalid_record_count"] == 1
        assert report["history_checks"][0]["next_pending_codes"] == ["000001"]
        assert report["role_ready"] is False
        assert report["m2_allowed"] is False
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_contract_status_executes_no_bulk_requests_and_opens_nothing():
    root = _repo()
    try:
        contract = load_status_traversal_contract(root)
        liquidation = next(
            source
            for source in contract["sources"]
            if source["source_id"] == LIQUIDATION_SOURCE_ID
        )
        assert liquidation["temporal_mode"] == "RETRIEVAL_DATE_SNAPSHOT"
        assert liquidation["date_authority"] == (
            "APPROVED_SAME_DAY_RETRIEVAL_TIMESTAMP_VIA_ACQUISITION_EVIDENCE"
        )
        report = build_contract_status_report(
            repo_root=root, generated_at="2026-09-15T10:23:00+09:00"
        )
        assert report["status"] == "BLOCKED"
        assert report["reason_codes"] == ["LIQUIDATION_RETRIEVAL_EVIDENCE_REQUIRED"]
        assert report["bulk_requests_executed"] == 0
        assert report["manifest_created"] is False
        assert report["selection_evidence_eligible"] is False
        assert report["m2_allowed"] is False
        assert report["orders_generated"] is False
    finally:
        shutil.rmtree(root, ignore_errors=True)
