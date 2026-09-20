from __future__ import annotations

from tools import validate_kospi_mcap_quarterly_c8_completion_authority_design as design


def _candidate(**overrides):
    item = {
        "classification": "SCHEDULE_ROW_EXACT_DATE_CANDIDATE",
        "source": "OPENDART_PASS",
        "document_sha256": "a" * 64,
        "promotion_allowed": False,
        "schedule_row_dates": ["20221227"],
        "candidate_actual_effective_date": "20221227",
    }
    item.update(overrides)
    return item


def test_current_adapter_interface_does_not_claim_structured_authority():
    source = """
def build_corporate_action_adapter_output(*, document_payloads):
    text, reasons = _document_text(payload)
    return _date_after_label(text, [], [])
"""
    report = design.inspect_current_adapter_interface(source)
    assert report["flattens_document_before_date_parse"] is True
    assert report["has_completion_source_context_input"] is False
    assert report["interface_supports_proposed_authority"] is False


def test_schedule_row_candidate_requires_opendart_unique_full_date():
    assert design.validate_candidate_receipt(_candidate()) == []
    assert "SCHEDULE_ROW_DATE_NOT_UNIQUE" in design.validate_candidate_receipt(
        _candidate(schedule_row_dates=["20221227", "20221228"])
    )
    assert "SCHEDULE_ROW_SOURCE_NOT_OPENDART" in design.validate_candidate_receipt(
        _candidate(source="VIEWER_FALLBACK")
    )


def test_viewer_candidate_requires_landing_hash_exact_node_and_linked_member():
    item = _candidate(
        classification="OFFICIAL_SCHEDULE_NODE_EXACT_DATE_CANDIDATE",
        source="VIEWER_FALLBACK",
        landing_sha256="b" * 64,
        schedule_node_ele_ids=["3"],
        schedule_node_dates=["20180901"],
        schedule_row_evidence=[
            {"member_ele_id": "3", "full_dates": ["20180901"]}
        ],
        candidate_actual_effective_date="20180901",
    )
    assert design.validate_candidate_receipt(item) == []
    assert "VIEWER_LANDING_HASH_INVALID" in design.validate_candidate_receipt(
        {**item, "landing_sha256": "tampered"}
    )
    assert "VIEWER_SCHEDULE_NODE_NOT_UNIQUE" in design.validate_candidate_receipt(
        {**item, "schedule_node_ele_ids": []}
    )


def test_viewer_candidate_blocks_node_member_date_mismatch():
    item = _candidate(
        classification="OFFICIAL_SCHEDULE_NODE_EXACT_DATE_CANDIDATE",
        source="VIEWER_FALLBACK",
        landing_sha256="b" * 64,
        schedule_node_ele_ids=["3"],
        schedule_node_dates=["20180901"],
        schedule_row_evidence=[
            {"member_ele_id": "1", "full_dates": ["20180901"]}
        ],
        candidate_actual_effective_date="20180901",
    )
    assert "VIEWER_NODE_MEMBER_DATE_MISMATCH" in design.validate_candidate_receipt(item)


def test_partial_date_remains_blocked_without_year_inference():
    item = {
        "classification": "PARTIAL_DATE_YEAR_REQUIRED_BLOCKED",
        "candidate_actual_effective_date": "",
        "partial_dates": ["06-01"],
        "schedule_row_dates": [],
    }
    assert design.validate_blocked_receipt(item) == []
    assert "BLOCKED_RECEIPT_HAS_CANDIDATE_DATE" in design.validate_blocked_receipt(
        {**item, "candidate_actual_effective_date": "20180601"}
    )


def test_non_merger_content_remains_blocked():
    item = {
        "classification": "NON_MERGER_CONTENT_BLOCKED",
        "candidate_actual_effective_date": "",
        "non_merger_content": True,
    }
    assert design.validate_blocked_receipt(item) == []
    assert "NON_MERGER_BLOCK_CONTRACT_MISMATCH" in design.validate_blocked_receipt(
        {**item, "non_merger_content": False}
    )


def test_design_contract_is_validation_only_and_disables_every_permission():
    contract = design.load_design_contract()
    assert contract["mode"] == "VALIDATION_ONLY"
    assert all(value is False for value in contract["execution_permissions"].values())
