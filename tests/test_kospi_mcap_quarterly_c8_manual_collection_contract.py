from __future__ import annotations

import json
from copy import deepcopy

from paper.strategies.kospi_mcap_quarterly_v1.src import contracts
from paper.strategies.kospi_mcap_quarterly_v1.src.c8_manual_collection_readiness import (
    CORPORATE_ACTION_ADAPTER_CONTRACT_RELATIVE,
    LIFECYCLE_CONTRACT_RELATIVE,
    MANUAL_CONTRACT_RELATIVE,
    evaluate_manual_collection_readiness,
)


def test_current_manual_collection_contract_exposes_remaining_known_blocker():
    report = evaluate_manual_collection_readiness()
    assert report["verdict"] == "BLOCKED_C8_MANUAL_COLLECTION"
    assert report["reason_codes"] == ["CORPORATE_ACTION_ADAPTER_FULL_CAPTURE_EVIDENCE_MISSING"]
    assert len(report["source_checks"]) == 6
    resolved_profiles = {
        item["source_id"]: item["status"]
        for item in report["source_checks"]
        if item["source_id"] in {"LISTING_CURRENT", "STATUS_LIQUIDATION_CURRENT"}
    }
    assert resolved_profiles == {
        "LISTING_CURRENT": "PASS",
        "STATUS_LIQUIDATION_CURRENT": "PASS",
    }
    assert report["network_collection_allowed"] is False
    assert report["descriptor_finalize_allowed"] is False
    assert report["canonical_publish_allowed"] is False
    assert report["m2_allowed"] is False
    assert report["candidate_selection_allowed"] is False
    assert report["orders_allowed"] is False


def test_status_input_ownership_is_exact_and_non_overlapping():
    manual = json.loads((contracts.REPO_ROOT / MANUAL_CONTRACT_RELATIVE).read_text(encoding="utf-8"))
    status_sources = [
        item for item in manual["source_inventory"]
        if item["role"] == "trade_management_status_as_of"
    ]
    counts: dict[str, int] = {}
    for source in status_sources:
        for field in source["covered_fields"]:
            counts[field] = counts.get(field, 0) + 1
    assert counts == {
        "trade_status": 1,
        "management_status": 1,
        "delisting_procedure_status": 1,
    }
    assert set(manual["status_role_coverage"]["source_ids"]) == {
        item["source_id"] for item in status_sources
    }


def test_readiness_evaluator_detects_contract_drift(tmp_path, monkeypatch):
    relative_paths = [
        MANUAL_CONTRACT_RELATIVE,
        "paper/strategies/kospi_mcap_quarterly_v1/config/c8_acquisition_evidence_contract_v1.json",
        "paper/strategies/kospi_mcap_quarterly_v1/config/c8_official_source_route_contract_v1.json",
        "paper/strategies/kospi_mcap_quarterly_v1/config/c8_source_adapter_contract_v1.json",
        "paper/strategies/kospi_mcap_quarterly_v1/config/c8_status_traversal_contract_v1.json",
        LIFECYCLE_CONTRACT_RELATIVE,
        CORPORATE_ACTION_ADAPTER_CONTRACT_RELATIVE,
    ]
    for relative in relative_paths:
        source = contracts.REPO_ROOT / relative
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(source.read_bytes())
    manual_path = tmp_path / MANUAL_CONTRACT_RELATIVE
    manual = json.loads(manual_path.read_text(encoding="utf-8"))
    changed = deepcopy(manual)
    changed["source_inventory"][0]["query_parameters"] = ["wrong"]
    manual_path.write_text(json.dumps(changed), encoding="utf-8")
    adapter_path = (
        tmp_path
        / "paper/strategies/kospi_mcap_quarterly_v1/config/c8_source_adapter_contract_v1.json"
    )
    adapter = json.loads(adapter_path.read_text(encoding="utf-8"))
    adapter["retrieval_date_transform_contract"]["approved_sources"][
        "KRX_MDCSTAT019_CURRENT_BASIC_INFO"
    ]["transform_profile_id"] = "BROKEN_PROFILE"
    adapter_path.write_text(json.dumps(adapter), encoding="utf-8")
    acquisition_path = (
        tmp_path
        / "paper/strategies/kospi_mcap_quarterly_v1/config/c8_acquisition_evidence_contract_v1.json"
    )
    acquisition = json.loads(acquisition_path.read_text(encoding="utf-8"))
    acquisition["allowed_methods"]["KRX_OPEN_API"]["download_hosts"] = [
        "openapi.krx.co.kr"
    ]
    acquisition_path.write_text(json.dumps(acquisition), encoding="utf-8")
    lifecycle_path = tmp_path / LIFECYCLE_CONTRACT_RELATIVE
    lifecycle = json.loads(lifecycle_path.read_text(encoding="utf-8"))
    lifecycle["scope"] = "BROKEN_SCOPE"
    lifecycle_path.write_text(json.dumps(lifecycle), encoding="utf-8")
    corporate_adapter_path = tmp_path / CORPORATE_ACTION_ADAPTER_CONTRACT_RELATIVE
    corporate_adapter = json.loads(corporate_adapter_path.read_text(encoding="utf-8"))
    corporate_adapter["source_id"] = "BROKEN_SOURCE"
    corporate_adapter_path.write_text(json.dumps(corporate_adapter), encoding="utf-8")
    report = evaluate_manual_collection_readiness(repo_root=tmp_path)
    listing = next(item for item in report["source_checks"] if item["source_id"] == "LISTING_CURRENT")
    close = next(item for item in report["source_checks"] if item["source_id"] == "CLOSE_MARKET_CAP_DAILY")
    assert "OFFICIAL_ROUTE_QUERY_PARAMETERS_MISMATCH" in listing["reason_codes"]
    assert "LISTING_STATUS_TRANSFORM_UNRESOLVED" in listing["reason_codes"]
    assert "OPENAPI_ACQUISITION_HOST_CONTRACT_MISMATCH" in close["reason_codes"]
    corporate = next(
        item
        for item in report["source_checks"]
        if item["source_id"] == "CORPORATE_ACTIONS_DART_COMPOSITE"
    )
    assert "CORPORATE_ACTION_LIFECYCLE_CONTRACT_MISMATCH" in corporate["reason_codes"]
    assert "CORPORATE_ACTION_ADAPTER_CONTRACT_MISMATCH" in corporate["reason_codes"]
    assert report["descriptor_finalize_allowed"] is False
