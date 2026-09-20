from __future__ import annotations

from copy import deepcopy

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_state_reconstruction import (
    load_reconstruction_contract,
    reconstruct_point_in_time_components,
)


AS_OF = "20260930"
CODES = [f"{index:06d}" for index in range(1, 101)]


def _listing_rows() -> list[dict[str, str]]:
    return [
        {
            "as_of_date": AS_OF,
            "code": code,
            "name": f"TEST{code}",
            "market": "KOSPI",
            "security_type": "COMMON",
            "security_type_rule": "OFFICIAL_FIELD",
            "listed_date": "20200102",
            "delisted_date": "",
            "listing_status": "ACTIVE",
        }
        for code in CODES
    ]


def _price_rows() -> list[dict[str, str]]:
    return [
        {"as_of_date": AS_OF, "code": code, "close": "10000", "market_cap": str(1_000_000_000 - index)}
        for index, code in enumerate(CODES)
    ]


def _coverage(basis: str, *, query_start: str = AS_OF) -> dict[str, object]:
    return {
        "basis": basis,
        "scope": "ALL_BASE_UNIVERSE_CODES",
        "query_start": query_start,
        "query_end": AS_OF,
        "provenance_status": "PASS",
        "official_source_ids": ["KRX_TEST_SOURCE"],
        "source_sha256s": ["a" * 64],
    }


def _state_inputs() -> dict[str, object]:
    return {
        "trade_management_status_as_of": {
            "mode": "EXHAUSTIVE_POINT_IN_TIME_EXCEPTIONS",
            "coverage": _coverage("EXHAUSTIVE_OFFICIAL_POINT_IN_TIME_EXCEPTIONS"),
            "default_state": {
                "trade_status": "TRADING",
                "management_status": "NORMAL",
                "delisting_procedure_status": "NONE",
            },
            "rows": [
                {
                    "as_of_date": AS_OF,
                    "code": "000001",
                    "trade_status": "SUSPENDED",
                    "management_status": "NORMAL",
                    "delisting_procedure_status": "NONE",
                }
            ],
        },
        "corporate_actions_as_of": {
            "mode": "EXHAUSTIVE_INTERVAL_HISTORY",
            "coverage": _coverage("EXHAUSTIVE_OFFICIAL_EVENT_HISTORY", query_start="20200102"),
            "default_state": {
                "merger_status": "NONE",
                "event_type": "NONE",
                "event_effective_date": "",
            },
            "rows": [
                {
                    "code": "000002",
                    "effective_from": "20260920",
                    "effective_to": "",
                    "merger_status": "PENDING",
                    "event_type": "MERGER",
                    "event_effective_date": "",
                }
            ],
        },
    }


def _mixed_state_inputs() -> dict[str, object]:
    state_inputs = _state_inputs()
    state_inputs["trade_management_status_as_of"] = {
        "mode": "MIXED_APPROVED_TEMPORAL_COMPONENTS",
        "coverage": {
            **_coverage("EXACT_APPROVED_MIXED_TEMPORAL_COMPONENTS", query_start="20200102"),
            "official_source_ids": ["KRX_STATUS_HISTORY", "KRX_MDCSTAT237_LIQUIDATION_CURRENT"],
            "source_sha256s": ["a" * 64, "b" * 64],
            "source_covered_fields": {
                "KRX_STATUS_HISTORY": ["trade_status", "management_status"],
                "KRX_MDCSTAT237_LIQUIDATION_CURRENT": ["delisting_procedure_status"],
            },
            "source_temporal_modes": {
                "KRX_STATUS_HISTORY": "EXHAUSTIVE_INTERVAL_HISTORY",
                "KRX_MDCSTAT237_LIQUIDATION_CURRENT": "RETRIEVAL_DATE_SNAPSHOT",
            },
        },
        "default_state": {
            "trade_status": "TRADING",
            "management_status": "NORMAL",
            "delisting_procedure_status": "NONE",
        },
        "rows": [
            {
                "code": "000001",
                "effective_from": "20260920",
                "effective_to": "",
                "trade_status": "SUSPENDED",
                "management_status": "MANAGEMENT",
                "covered_fields": ["trade_status", "management_status"],
                "temporal_mode": "EXHAUSTIVE_INTERVAL_HISTORY",
                "official_source_id": "KRX_STATUS_HISTORY",
            },
            {
                "as_of_date": AS_OF,
                "code": "000002",
                "delisting_procedure_status": "PROCEDURE",
                "covered_fields": ["delisting_procedure_status"],
                "temporal_mode": "RETRIEVAL_DATE_SNAPSHOT",
                "official_source_id": "KRX_MDCSTAT237_LIQUIDATION_CURRENT",
            },
        ],
    }
    return state_inputs


def test_sparse_official_states_expand_only_after_complete_coverage_passes():
    report = reconstruct_point_in_time_components(
        selection_as_of=AS_OF,
        listing_rows=_listing_rows(),
        price_rows=_price_rows(),
        state_inputs=_state_inputs(),
    )
    assert report["verdict"] == "C8_STATE_RECONSTRUCTION_READY"
    assert report["base_code_count"] == 100
    assert report["component_code_sets_exact"] is True
    assert report["publication_allowed"] is False
    status = {row["code"]: row for row in report["components"]["trade_management_status_as_of"]}
    actions = {row["code"]: row for row in report["components"]["corporate_actions_as_of"]}
    assert status["000001"]["trade_status"] == "SUSPENDED"
    assert status["000003"]["trade_status"] == "TRADING"
    assert actions["000002"]["merger_status"] == "PENDING"
    assert actions["000003"]["merger_status"] == "NONE"


def test_sparse_default_is_blocked_without_verified_provenance():
    state_inputs = _state_inputs()
    state_inputs["trade_management_status_as_of"]["coverage"]["provenance_status"] = "UNVERIFIED"
    report = reconstruct_point_in_time_components(
        selection_as_of=AS_OF,
        listing_rows=_listing_rows(),
        price_rows=_price_rows(),
        state_inputs=state_inputs,
    )
    assert report["status"] == "FAIL"
    assert "SPARSE_PROVENANCE_NOT_VERIFIED" in report["reason_codes"]
    assert report["components"] == {}
    assert report["m2_allowed"] is False


def test_incomplete_history_and_post_selection_event_fail_closed():
    state_inputs = _state_inputs()
    corporate = state_inputs["corporate_actions_as_of"]
    corporate["coverage"]["query_start"] = "20210101"
    corporate["rows"].append(
        {
            "code": "000003",
            "effective_from": "20261001",
            "effective_to": "",
            "merger_status": "PENDING",
            "event_type": "MERGER",
            "event_effective_date": "",
        }
    )
    report = reconstruct_point_in_time_components(
        selection_as_of=AS_OF,
        listing_rows=_listing_rows(),
        price_rows=_price_rows(),
        state_inputs=state_inputs,
    )
    assert "EVENT_HISTORY_WINDOW_INCOMPLETE" in report["reason_codes"]
    assert "POST_SELECTION_EVENT_PRESENT" in report["reason_codes"]
    assert report["components"] == {}


def test_base_price_code_mismatch_blocks_reconstruction():
    report = reconstruct_point_in_time_components(
        selection_as_of=AS_OF,
        listing_rows=_listing_rows(),
        price_rows=_price_rows()[:-1],
        state_inputs=deepcopy(_state_inputs()),
        contract=load_reconstruction_contract(),
    )
    assert "CLOSE_MARKET_CAP_AS_OF_CODE_SET_MISMATCH" in report["reason_codes"]
    assert report["component_code_sets_exact"] is False
    assert report["components"] == {}


def test_overlapping_active_intervals_do_not_choose_an_arbitrary_state():
    state_inputs = _state_inputs()
    state_inputs["corporate_actions_as_of"]["rows"].append(
        {
            "code": "000002",
            "effective_from": "20260925",
            "effective_to": "20261020",
            "merger_status": "PENDING",
            "event_type": "MERGER",
            "event_effective_date": "",
        }
    )
    report = reconstruct_point_in_time_components(
        selection_as_of=AS_OF,
        listing_rows=_listing_rows(),
        price_rows=_price_rows(),
        state_inputs=state_inputs,
    )
    assert "CORPORATE_ACTIONS_AS_OF_OVERLAPPING_ACTIVE_INTERVALS" in report["reason_codes"]
    assert report["components"] == {}


def test_pending_state_cannot_promote_planned_date_to_actual_date():
    state_inputs = _state_inputs()
    state_inputs["corporate_actions_as_of"]["rows"][0]["event_effective_date"] = "20261015"
    report = reconstruct_point_in_time_components(
        selection_as_of=AS_OF,
        listing_rows=_listing_rows(),
        price_rows=_price_rows(),
        state_inputs=state_inputs,
    )
    assert "CORPORATE_ACTION_STATE_COMBINATION_INVALID" in report["reason_codes"]
    assert report["components"] == {}


def test_effective_state_requires_one_day_terminal_interval():
    state_inputs = _state_inputs()
    state_inputs["corporate_actions_as_of"]["rows"] = [
        {
            "code": "000002",
            "effective_from": AS_OF,
            "effective_to": "",
            "merger_status": "EFFECTIVE",
            "event_type": "MERGER",
            "event_effective_date": "20260920",
        }
    ]
    report = reconstruct_point_in_time_components(
        selection_as_of=AS_OF,
        listing_rows=_listing_rows(),
        price_rows=_price_rows(),
        state_inputs=state_inputs,
    )
    assert "CORPORATE_ACTION_EFFECTIVE_INTERVAL_INVALID" in report["reason_codes"]
    assert report["components"] == {}


def test_overlapping_intervals_for_different_state_fields_are_merged():
    state_inputs = _state_inputs()
    state_inputs["trade_management_status_as_of"] = {
        "mode": "EXHAUSTIVE_INTERVAL_HISTORY",
        "coverage": _coverage("EXHAUSTIVE_OFFICIAL_EVENT_HISTORY", query_start="20200102"),
        "default_state": {
            "trade_status": "TRADING",
            "management_status": "NORMAL",
            "delisting_procedure_status": "NONE",
        },
        "rows": [
            {
                "code": "000001",
                "effective_from": "20260920",
                "effective_to": "",
                "trade_status": "SUSPENDED",
                "covered_fields": ["trade_status"],
            },
            {
                "code": "000001",
                "effective_from": "20260925",
                "effective_to": "",
                "management_status": "MANAGEMENT",
                "covered_fields": ["management_status"],
            },
        ],
    }
    report = reconstruct_point_in_time_components(
        selection_as_of=AS_OF,
        listing_rows=_listing_rows(),
        price_rows=_price_rows(),
        state_inputs=state_inputs,
    )
    assert report["status"] == "PASS"
    status = {row["code"]: row for row in report["components"]["trade_management_status_as_of"]}
    assert status["000001"]["trade_status"] == "SUSPENDED"
    assert status["000001"]["management_status"] == "MANAGEMENT"
    assert status["000001"]["delisting_procedure_status"] == "NONE"


def test_approved_mixed_temporal_sources_reconstruct_field_by_field():
    report = reconstruct_point_in_time_components(
        selection_as_of=AS_OF,
        listing_rows=_listing_rows(),
        price_rows=_price_rows(),
        state_inputs=_mixed_state_inputs(),
    )
    assert report["status"] == "PASS"
    status = {row["code"]: row for row in report["components"]["trade_management_status_as_of"]}
    assert status["000001"]["trade_status"] == "SUSPENDED"
    assert status["000001"]["management_status"] == "MANAGEMENT"
    assert status["000001"]["delisting_procedure_status"] == "NONE"
    assert status["000002"]["delisting_procedure_status"] == "PROCEDURE"


def test_mixed_coverage_overlap_is_blocked_before_default_expansion():
    state_inputs = _mixed_state_inputs()
    coverage = state_inputs["trade_management_status_as_of"]["coverage"]
    coverage["source_covered_fields"]["KRX_STATUS_HISTORY"].append("delisting_procedure_status")
    report = reconstruct_point_in_time_components(
        selection_as_of=AS_OF,
        listing_rows=_listing_rows(),
        price_rows=_price_rows(),
        state_inputs=state_inputs,
    )
    assert "MIXED_SOURCE_FIELD_ASSIGNMENT_INVALID" in report["reason_codes"]
    assert "MIXED_SOURCE_COVERAGE_FIELDS_OVERLAP" in report["reason_codes"]
    assert report["components"] == {}


def test_mixed_retrieval_future_date_is_blocked():
    state_inputs = _mixed_state_inputs()
    state_inputs["trade_management_status_as_of"]["rows"][1]["as_of_date"] = "20261001"
    report = reconstruct_point_in_time_components(
        selection_as_of=AS_OF,
        listing_rows=_listing_rows(),
        price_rows=_price_rows(),
        state_inputs=state_inputs,
    )
    assert "TRADE_MANAGEMENT_STATUS_AS_OF_ASOF_MISMATCH" in report["reason_codes"]
    assert report["components"] == {}


def test_mixed_row_source_lineage_must_match_coverage():
    state_inputs = _mixed_state_inputs()
    state_inputs["trade_management_status_as_of"]["rows"][1]["official_source_id"] = "KRX_OTHER"
    report = reconstruct_point_in_time_components(
        selection_as_of=AS_OF,
        listing_rows=_listing_rows(),
        price_rows=_price_rows(),
        state_inputs=state_inputs,
    )
    assert "TRADE_MANAGEMENT_STATUS_AS_OF_ROW_SOURCE_LINEAGE_INVALID" in report["reason_codes"]
    assert report["components"] == {}
