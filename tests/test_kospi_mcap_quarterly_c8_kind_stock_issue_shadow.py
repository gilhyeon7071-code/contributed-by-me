from __future__ import annotations

import io
import zipfile

import pytest

from tools import analyze_kospi_mcap_quarterly_c8_kind_stock_issue_shadow as analyzer
from tools import capture_kospi_mcap_quarterly_c8_kind_stock_issues as capture


def _kind_row(date: str, reason: str = "타법인흡수합병") -> dict[str, str]:
    return {
        "stock_code": "000001",
        "listing_date": date,
        "issue_reason": reason,
        "process_key": "20251001000001",
    }


def test_kind_html_parser_extracts_official_row() -> None:
    payload = b"""<table><tr onclick="fnDetailView('20251001000001')">
    <td>Test Corp</td><td>2025-10-10</td><td>Additional</td>
    <td>1,000</td><td>500</td><td>Merger</td></tr></table>"""
    rows = capture.parse_issue_rows(payload)
    assert rows == [
        {
            "process_key": "20251001000001",
            "company_name": "Test Corp",
            "listing_date": "20251010",
            "listing_type": "Additional",
            "issued_shares": "1,000",
            "par_value": "500",
            "issue_reason": "Merger",
        }
    ]


def test_unique_kind_and_initial_date_match_resolves() -> None:
    result = analyzer.select_kind_candidate(
        event_receipt_no="20251001000001",
        stock_code="000001",
        candidate_receipts=["20250901000001", "20250902000001"],
        candidate_listing_dates={
            "20250901000001": "20251010",
            "20250902000001": "20251020",
        },
        kind_rows=[_kind_row("20251010")],
        max_days=45,
        merger_substring="합병",
    )
    assert result["classification"] == "UNIQUE_KIND_LISTING_MATCH"
    assert result["selected_initial_receipt"] == "20250901000001"


def test_multiple_kind_rows_fail_closed() -> None:
    result = analyzer.select_kind_candidate(
        event_receipt_no="20251001000001",
        stock_code="000001",
        candidate_receipts=["20250901000001"],
        candidate_listing_dates={"20250901000001": "20251010"},
        kind_rows=[_kind_row("20251010"), _kind_row("20251011")],
        max_days=45,
        merger_substring="합병",
    )
    assert result["classification"] == "AMBIGUOUS_KIND_MERGER_ROWS"
    assert result["selected_initial_receipt"] == ""


def test_multiple_initial_decisions_fail_closed() -> None:
    result = analyzer.select_kind_candidate(
        event_receipt_no="20251001000001",
        stock_code="000001",
        candidate_receipts=["20250901000001", "20250902000001"],
        candidate_listing_dates={
            "20250901000001": "20251010",
            "20250902000001": "20251010",
        },
        kind_rows=[_kind_row("20251010")],
        max_days=45,
        merger_substring="합병",
    )
    assert result["classification"] == "AMBIGUOUS_INITIAL_DECISIONS"
    assert result["selected_initial_receipt"] == ""


def test_non_merger_and_out_of_window_rows_are_ignored() -> None:
    result = analyzer.select_kind_candidate(
        event_receipt_no="20251001000001",
        stock_code="000001",
        candidate_receipts=["20250901000001"],
        candidate_listing_dates={"20250901000001": "20251010"},
        kind_rows=[_kind_row("20251010", "유상증자"), _kind_row("20251201")],
        max_days=45,
        merger_substring="합병",
    )
    assert result["classification"] == "NO_KIND_MERGER_ROW"


def test_cross_stock_row_is_ignored() -> None:
    row = _kind_row("20251010")
    row["stock_code"] = "999999"
    result = analyzer.select_kind_candidate(
        event_receipt_no="20251001000001",
        stock_code="000001",
        candidate_receipts=["20250901000001"],
        candidate_listing_dates={"20250901000001": "20251010"},
        kind_rows=[row],
        max_days=45,
        merger_substring="합병",
    )
    assert result["classification"] == "NO_KIND_MERGER_ROW"


def test_kind_availability_distinguishes_absent_and_outside_window() -> None:
    absent = analyzer.diagnose_kind_merger_availability(
        event_receipt_no="20251001000001",
        stock_code="000001",
        kind_rows=[_kind_row("20251010", "유상증자")],
        max_days=45,
        merger_substring="합병",
    )
    outside = analyzer.diagnose_kind_merger_availability(
        event_receipt_no="20251001000001",
        stock_code="000001",
        kind_rows=[_kind_row("20250801"), _kind_row("20251201")],
        max_days=45,
        merger_substring="합병",
    )

    assert absent["classification"] == "NO_MERGER_ROW_ANYWHERE"
    assert outside["classification"] == "NO_MERGER_ROW_WITHIN_ABSOLUTE_WINDOW"


def test_kind_availability_identifies_precompletion_near_miss() -> None:
    result = analyzer.diagnose_kind_merger_availability(
        event_receipt_no="20251001000001",
        stock_code="000001",
        kind_rows=[_kind_row("20250920")],
        max_days=45,
        merger_substring="합병",
    )

    assert result["classification"] == "MERGER_ROW_BEFORE_COMPLETION_WITHIN_WINDOW"
    assert result["nearest_signed_days"] == -11


def test_kind_availability_identifies_forward_window_row() -> None:
    result = analyzer.diagnose_kind_merger_availability(
        event_receipt_no="20251001000001",
        stock_code="000001",
        kind_rows=[_kind_row("20251010")],
        max_days=45,
        merger_substring="합병",
    )

    assert result["classification"] == "FORWARD_WINDOW_MERGER_ROW_PRESENT"
    assert result["nearest_signed_days"] == 9


def test_extracts_only_frozen_scheduled_listing_label() -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "document.xml",
            "합병기일 2025년 10월 01일 신주의 상장예정일 2025년 10월 10일",
        )
    assert analyzer.extract_scheduled_listing_date(
        buffer.getvalue(),
        labels=["신주의 상장예정일"],
        date_patterns=["([0-9]{4})[.년/-][ ]*([0-9]{1,2})[.월/-][ ]*([0-9]{1,2})"],
    ) == "20251010"


def test_source_ceiling_below_required_correct_count_fails_preflight() -> None:
    result = analyzer.assess_source_ceiling(
        population_count=272,
        source_ceiling_count=38,
        minimum_correct_link_count=136,
    )
    assert result["status"] == "FAIL"
    assert result["shortfall_count"] == 98
    assert result["reason_codes"] == [
        "DART_INITIAL_LISTING_DATE_CEILING_BELOW_MINIMUM_CORRECT_LINK_COUNT"
    ]


def test_source_ceiling_at_required_correct_count_passes_preflight() -> None:
    result = analyzer.assess_source_ceiling(
        population_count=272,
        source_ceiling_count=136,
        minimum_correct_link_count=136,
    )
    assert result["status"] == "PASS"
    assert result["shortfall_count"] == 0
    assert result["reason_codes"] == []


@pytest.mark.parametrize(
    ("dart_count", "kind_count", "expected"),
    [
        (38, 35, "DUAL_REQUIRED_INPUT_SHORTFALL"),
        (38, 136, "DART_REQUIRED_INPUT_SHORTFALL"),
        (136, 35, "KIND_REQUIRED_INPUT_SHORTFALL"),
        (136, 136, "NO_REQUIRED_INPUT_SHORTFALL"),
    ],
)
def test_required_input_shortfall_attribution_is_data_derived(
    dart_count: int, kind_count: int, expected: str
) -> None:
    assert analyzer.required_input_shortfall_attribution(
        dart_available_count=dart_count,
        kind_available_count=kind_count,
        minimum_correct_count=136,
    ) == expected


def test_capture_blocks_before_network_when_source_ceiling_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setattr(
        capture,
        "load_contract",
        lambda root: {"acquisition": {"maximum_requests": 1}},
    )
    monkeypatch.setattr(capture, "frozen_stock_codes", lambda root, contract: ["000001"])
    monkeypatch.setattr(
        analyzer,
        "evaluate_pre_capture_feasibility",
        lambda root: {
            "status": "FAIL",
            "verdict": "KIND_PRE_CAPTURE_SOURCE_CEILING_BELOW_ACCEPTANCE_MINIMUM",
            "network_requests": 0,
            "artifact_writes": 0,
        },
    )
    network_calls: list[str] = []
    monkeypatch.setattr(
        capture,
        "_capture_one",
        lambda **kwargs: network_calls.append(str(kwargs["stock_code"])),
    )

    with pytest.raises(capture.KindFeasibilityBlocked) as exc_info:
        capture.capture(root=tmp_path)

    assert exc_info.value.report["network_requests"] == 0
    assert network_calls == []
    assert list(tmp_path.rglob("*")) == []
