from tools.analyze_kospi_mcap_quarterly_c8_lineage_landing_scope import (
    classify_family_authority,
)


def _diag(kind, code="000001", series="MERGER_DECISION_HISTORY", date="20200101", merger=True):
    return {
        "event_kind": kind,
        "stock_code": code,
        "series": series,
        "receipt_date": date,
        "contains_merger": merger,
    }


def _family(current="20200202000002", root="20200101000001"):
    return [
        {"receipt_no": current, "selected": True, "is_correction": True},
        {"receipt_no": root, "selected": False, "is_correction": False},
    ]


def test_exact_same_stock_initial_root_resolves_revision():
    current_receipt = "20200202000002"
    root_receipt = "20200101000001"
    current = _diag("REVISION", date="20200202")
    diagnostics = {
        current_receipt: current,
        root_receipt: _diag("INITIAL_DECISION", date="20200101"),
    }
    result = classify_family_authority(
        receipt=current_receipt,
        current=current,
        family=_family(current_receipt, root_receipt),
        diagnostics=diagnostics,
    )
    assert result["authority_resolved"] is True
    assert result["initial_receipt"] == root_receipt


def test_completion_family_root_does_not_establish_initial_decision():
    current_receipt = "20200202000002"
    root_receipt = "20200101000001"
    current = _diag("COMPLETION", series="MERGER_COMPLETION_HISTORY", date="20200202")
    diagnostics = {
        current_receipt: current,
        root_receipt: _diag(
            "COMPLETION", series="MERGER_COMPLETION_HISTORY", date="20200101"
        ),
    }
    result = classify_family_authority(
        receipt=current_receipt,
        current=current,
        family=_family(current_receipt, root_receipt),
        diagnostics=diagnostics,
    )
    assert result["authority_resolved"] is False
    assert "EVENT_KIND_NOT_AUTHORITY_ELIGIBLE" in result["reason_codes"]
    assert "FAMILY_ROOT_NOT_INITIAL_DECISION" in result["reason_codes"]


def test_cross_stock_and_future_root_fail_closed():
    current_receipt = "20200202000002"
    root_receipt = "20200303000003"
    current = _diag("REVISION", date="20200202")
    diagnostics = {
        current_receipt: current,
        root_receipt: _diag("INITIAL_DECISION", code="999999", date="20200303"),
    }
    result = classify_family_authority(
        receipt=current_receipt,
        current=current,
        family=_family(current_receipt, root_receipt),
        diagnostics=diagnostics,
    )
    assert result["authority_resolved"] is False
    assert "FAMILY_ROOT_NOT_ACCEPTED_SAME_STOCK_SERIES" in result["reason_codes"]


def test_ambiguous_originals_and_selected_mismatch_fail_closed():
    current_receipt = "20200202000002"
    current = _diag("REVISION", date="20200202")
    family = _family("20209999000000", "20200101000001") + [
        {"receipt_no": "20191212000012", "selected": False, "is_correction": False}
    ]
    diagnostics = {
        current_receipt: current,
        "20200101000001": _diag("INITIAL_DECISION", date="20200101"),
        "20191212000012": _diag("INITIAL_DECISION", date="20191212"),
    }
    result = classify_family_authority(
        receipt=current_receipt,
        current=current,
        family=family,
        diagnostics=diagnostics,
    )
    assert result["authority_resolved"] is False
    assert "FAMILY_SELECTED_RECEIPT_NOT_CURRENT" in result["reason_codes"]
    assert "FAMILY_ORIGINAL_OPTION_NOT_EXACT_ONE" in result["reason_codes"]


def test_attachment_wrapper_official_original_becomes_initial_decision():
    receipt = "20200101000001"
    current = _diag("REVISION", date="20200101")
    diagnostics = {receipt: current}
    result = classify_family_authority(
        receipt=receipt,
        current=current,
        family=[
            {
                "receipt_no": receipt,
                "selected": True,
                "is_correction": False,
            }
        ],
        diagnostics=diagnostics,
        attachment_original_receipts=[receipt],
    )
    assert result["authority_resolved"] is True
    assert result["initial_receipt"] == receipt
    assert result["event_kind_override"] == "INITIAL_DECISION"
    assert result["classification"] == "DART_OFFICIAL_ATTACHMENT_ORIGINAL_INITIAL"


def test_revision_can_link_to_attachment_wrapper_original():
    receipt = "20200202000002"
    root = "20200101000001"
    current = _diag("REVISION", date="20200202")
    diagnostics = {
        receipt: current,
        root: _diag("REVISION", date="20200101"),
    }
    result = classify_family_authority(
        receipt=receipt,
        current=current,
        family=_family(receipt, root),
        diagnostics=diagnostics,
        attachment_original_receipts=[root],
    )
    assert result["authority_resolved"] is True
    assert result["initial_receipt"] == root
    assert result["event_kind_override"] == ""
