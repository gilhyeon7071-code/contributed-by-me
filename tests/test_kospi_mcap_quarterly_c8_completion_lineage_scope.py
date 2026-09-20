from tools.analyze_kospi_mcap_quarterly_c8_completion_lineage_scope import (
    match_completion_to_family,
)


def _event(receipt, date, kind, planned=""):
    return {
        "receipt_no": receipt,
        "receipt_date": date,
        "event_kind": kind,
        "planned_effective_date": planned,
    }


def test_unique_active_exact_date_family_resolves():
    result = match_completion_to_family(
        completion_receipt_date="20200310",
        actual_effective_date="20200301",
        family_events={
            "20200101000001": [
                _event("20200101000001", "20200101", "INITIAL_DECISION", "20200201"),
                _event("20200201000002", "20200201", "REVISION", "20200301"),
            ],
            "20190101000001": [
                _event("20190101000001", "20190101", "INITIAL_DECISION", "20190301")
            ],
        },
    )
    assert result["authority_resolved"] is True
    assert result["initial_receipt"] == "20200101000001"


def test_cancellation_disables_matching_family():
    result = match_completion_to_family(
        completion_receipt_date="20200310",
        actual_effective_date="20200301",
        family_events={
            "20200101000001": [
                _event("20200101000001", "20200101", "INITIAL_DECISION", "20200301"),
                _event("20200201000002", "20200201", "CANCELLATION"),
            ]
        },
    )
    assert result["authority_resolved"] is False
    assert result["classification"] == "NO_ACTIVE_FAMILY_DATE_MATCH"


def test_multiple_exact_date_families_fail_closed():
    result = match_completion_to_family(
        completion_receipt_date="20200310",
        actual_effective_date="20200301",
        family_events={
            "20200101000001": [
                _event("20200101000001", "20200101", "INITIAL_DECISION", "20200301")
            ],
            "20200102000002": [
                _event("20200102000002", "20200102", "INITIAL_DECISION", "20200301")
            ],
        },
    )
    assert result["authority_resolved"] is False
    assert result["classification"] == "AMBIGUOUS_ACTIVE_FAMILY_DATE_MATCH"


def test_unresolved_prior_decision_state_blocks_even_unique_match():
    result = match_completion_to_family(
        completion_receipt_date="20200310",
        actual_effective_date="20200301",
        family_events={
            "20200101000001": [
                _event("20200101000001", "20200101", "INITIAL_DECISION", "20200301")
            ]
        },
        unresolved_decision_dates=["20200201"],
    )
    assert result["authority_resolved"] is False
    assert result["classification"] == "UNRESOLVED_DECISION_STATE_PRESENT"


def test_missing_actual_date_fails_closed():
    result = match_completion_to_family(
        completion_receipt_date="20200310",
        actual_effective_date="",
        family_events={},
    )
    assert result["authority_resolved"] is False
    assert result["classification"] == "COMPLETION_ACTUAL_DATE_MISSING"
