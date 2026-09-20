from __future__ import annotations

from copy import deepcopy

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_corporate_action_lifecycle import (
    build_corporate_action_intervals,
    evaluate_corporate_action_contract,
)


AS_OF = "20260930"
EARLIEST = "20200102"
MAPPING = {"005930": "00126380", "000660": "00164779"}


def _coverage() -> dict[str, object]:
    return {
        "scope": "ALL_BASE_UNIVERSE_CODES",
        "query_start": EARLIEST,
        "query_end": AS_OF,
        "provenance_status": "PASS",
        "official_source_ids": ["DART_CORPORATE_ACTIONS_COMPOSITE"],
        "source_sha256s": ["a" * 64, "b" * 64, "c" * 64],
        "covered_corp_codes": sorted(MAPPING.values()),
        "pagination_complete_by_corp_code": {
            corp_code: True for corp_code in MAPPING.values()
        },
        "last_reprt_at": "N",
    }


def _event(
    *,
    rcept_no: str,
    initial_rcept_no: str,
    rcept_dt: str,
    event_kind: str,
    planned: str = "",
    actual: str = "",
    lineage: str = "ORIGINAL_DOCUMENT_EXPLICIT_REFERENCE",
) -> dict[str, str]:
    return {
        "stock_code": "005930",
        "corp_code": "00126380",
        "rcept_no": rcept_no,
        "initial_rcept_no": initial_rcept_no,
        "rcept_dt": rcept_dt,
        "event_kind": event_kind,
        "lineage_authority": lineage,
        "actual_effective_date": actual,
        "planned_effective_date": planned,
        "document_sha256": "d" * 64,
    }


def _build(events: list[dict[str, str]], *, coverage: object | None = None):
    return build_corporate_action_intervals(
        events=events,
        selection_as_of=AS_OF,
        earliest_listed_date=EARLIEST,
        base_code_to_corp_code=MAPPING,
        coverage=_coverage() if coverage is None else coverage,
    )


def test_contract_and_adapter_are_consistent_but_full_capture_remains_blocked():
    report = evaluate_corporate_action_contract()
    assert report["status"] == "PASS"
    assert report["verdict"] == "C8_CORPORATE_ACTION_ADAPTER_IMPLEMENTED_FULL_CAPTURE_PENDING"
    assert report["adapter_implemented"] is True
    assert report["full_capture_evidence"] is False
    assert report["network_collection_allowed"] is False
    assert report["orders_allowed"] is False


def test_revision_then_cancellation_returns_to_default_none():
    initial = "20260102000001"
    report = _build(
        [
            _event(
                rcept_no=initial,
                initial_rcept_no=initial,
                rcept_dt="20260102",
                event_kind="INITIAL_DECISION",
                planned="20260701",
                lineage="",
            ),
            _event(
                rcept_no="20260303000002",
                initial_rcept_no=initial,
                rcept_dt="20260303",
                event_kind="REVISION",
                planned="20260801",
            ),
            _event(
                rcept_no="20260404000003",
                initial_rcept_no=initial,
                rcept_dt="20260404",
                event_kind="CANCELLATION",
            ),
        ]
    )
    assert report["status"] == "PASS"
    assert report["interval_rows"] == [
        {
            "code": "005930",
            "effective_from": "20260102",
            "effective_to": "20260404",
            "merger_status": "PENDING",
            "event_type": "MERGER",
            "event_effective_date": "",
        }
    ]


def test_completion_uses_actual_date_and_emits_one_day_terminal_marker():
    initial = "20260102000001"
    report = _build(
        [
            _event(
                rcept_no=initial,
                initial_rcept_no=initial,
                rcept_dt="20260102",
                event_kind="INITIAL_DECISION",
                planned="20260701",
                lineage="",
            ),
            _event(
                rcept_no="20260715000002",
                initial_rcept_no=initial,
                rcept_dt="20260715",
                event_kind="COMPLETION",
                planned="20260701",
                actual="20260701",
            ),
        ]
    )
    assert report["status"] == "PASS"
    assert report["interval_rows"] == [
        {
            "code": "005930",
            "effective_from": "20260102",
            "effective_to": "20260715",
            "merger_status": "PENDING",
            "event_type": "MERGER",
            "event_effective_date": "",
        },
        {
            "code": "005930",
            "effective_from": "20260715",
            "effective_to": "20260716",
            "merger_status": "EFFECTIVE",
            "event_type": "MERGER",
            "event_effective_date": "20260701",
        },
    ]
    assert all(row["event_effective_date"] != "20260701" for row in report["interval_rows"][:1])


def test_missing_explicit_lineage_fails_closed():
    initial = "20260102000001"
    report = _build(
        [
            _event(
                rcept_no=initial,
                initial_rcept_no=initial,
                rcept_dt="20260102",
                event_kind="INITIAL_DECISION",
                lineage="",
            ),
            _event(
                rcept_no="20260303000002",
                initial_rcept_no=initial,
                rcept_dt="20260303",
                event_kind="REVISION",
                lineage="",
            ),
        ]
    )
    assert report["status"] == "FAIL"
    assert "EVENT_IDENTITY_UNRESOLVED" in report["reason_codes"]
    assert report["interval_rows"] == []


def test_same_day_event_order_fails_closed():
    initial = "20260102000001"
    report = _build(
        [
            _event(
                rcept_no=initial,
                initial_rcept_no=initial,
                rcept_dt="20260102",
                event_kind="INITIAL_DECISION",
                lineage="",
            ),
            _event(
                rcept_no="20260102000002",
                initial_rcept_no=initial,
                rcept_dt="20260102",
                event_kind="REVISION",
            ),
        ]
    )
    assert report["status"] == "FAIL"
    assert "SAME_DAY_EVENT_ORDER_UNRESOLVED" in report["reason_codes"]
    assert report["interval_rows"] == []


def test_incomplete_per_company_pagination_fails_closed():
    coverage = deepcopy(_coverage())
    coverage["pagination_complete_by_corp_code"]["00164779"] = False
    report = _build([], coverage=coverage)
    assert report["status"] == "FAIL"
    assert "EVENT_HISTORY_PAGINATION_INCOMPLETE" in report["reason_codes"]
    assert report["interval_rows"] == []


def test_completion_actual_date_before_initial_decision_fails_closed():
    initial = "20260102000001"
    report = _build(
        [
            _event(
                rcept_no=initial,
                initial_rcept_no=initial,
                rcept_dt="20260102",
                event_kind="INITIAL_DECISION",
                lineage="",
            ),
            _event(
                rcept_no="20260715000002",
                initial_rcept_no=initial,
                rcept_dt="20260715",
                event_kind="COMPLETION",
                actual="20251231",
            ),
        ]
    )
    assert report["status"] == "FAIL"
    assert "COMPLETION_ACTUAL_DATE_BEFORE_DECISION" in report["reason_codes"]
    assert report["interval_rows"] == []
