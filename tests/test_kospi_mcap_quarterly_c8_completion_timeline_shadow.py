from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pytest

from tools import analyze_kospi_mcap_quarterly_c8_completion_timeline_shadow as analyzer


def _fields(**overrides: str) -> dict[str, str]:
    values = {
        "board_resolution_date": "20251001",
        "merger_contract_date": "20251002",
    }
    values.update(overrides)
    return values


def _candidate(receipt: str, **overrides: str) -> dict[str, object]:
    return {
        "receipt_no": receipt,
        "stock_code": "000001",
        "receipt_date": "20251003",
        "fields": _fields(**overrides),
    }


def _zip_document(text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("document.xml", text.encode("utf-8"))
    return buffer.getvalue()


def test_extracts_both_frozen_timeline_dates() -> None:
    payload = _zip_document(
        "합병 이사회 결의일 2025년 10월 01일 "
        "합병계약 체결일 2025년 10월 02일 합병기일 2026년 01월 31일"
    )
    fields, reasons = analyzer.extract_timeline_fields(
        payload,
        date_patterns=["([0-9]{4})[.년/-][ ]*([0-9]{1,2})[.월/-][ ]*([0-9]{1,2})"],
        board_labels=["합병 이사회 결의일"],
        contract_labels=["합병계약 체결일"],
    )
    assert reasons == []
    assert fields == {
        "board_resolution_date": "20251001",
        "merger_contract_date": "20251002",
    }


def test_unique_exact_two_field_candidate_is_selected() -> None:
    result = analyzer.select_timeline_candidate(
        event_receipt_no="20251201000001",
        event_stock_code="000001",
        event_receipt_date="20251201",
        event_fields=_fields(),
        candidates=[
            _candidate("20251003000001"),
            _candidate("20251003000002", merger_contract_date="20251004"),
        ],
    )
    assert result["classification"] == "UNIQUE_TIMELINE_MATCH"
    assert result["selected_initial_receipt"] == "20251003000001"


def test_missing_event_field_fails_closed() -> None:
    result = analyzer.select_timeline_candidate(
        event_receipt_no="20251201000001",
        event_stock_code="000001",
        event_receipt_date="20251201",
        event_fields=_fields(merger_contract_date=""),
        candidates=[_candidate("20251003000001")],
    )
    assert result["classification"] == "EVENT_TIMELINE_FIELDS_MISSING"
    assert result["selected_initial_receipt"] == ""


def test_conflicting_field_rejects_candidate() -> None:
    result = analyzer.select_timeline_candidate(
        event_receipt_no="20251201000001",
        event_stock_code="000001",
        event_receipt_date="20251201",
        event_fields=_fields(),
        candidates=[_candidate("20251003000001", board_resolution_date="20251004")],
    )
    assert result["classification"] == "NO_TIMELINE_MATCH"
    assert result["candidate_evaluations"][0]["conflict_fields"] == [
        "board_resolution_date"
    ]


def test_multiple_exact_candidates_remain_ambiguous() -> None:
    result = analyzer.select_timeline_candidate(
        event_receipt_no="20251201000001",
        event_stock_code="000001",
        event_receipt_date="20251201",
        event_fields=_fields(),
        candidates=[
            _candidate("20251003000001"),
            _candidate("20251003000002"),
        ],
    )
    assert result["classification"] == "AMBIGUOUS_TIMELINE_MATCH"
    assert result["selected_initial_receipt"] == ""


def test_cross_stock_and_future_candidates_are_not_evaluated() -> None:
    cross_stock = _candidate("20251003000001")
    cross_stock["stock_code"] = "999999"
    future = _candidate("20260101000001")
    future["receipt_date"] = "20260101"
    result = analyzer.select_timeline_candidate(
        event_receipt_no="20251201000001",
        event_stock_code="000001",
        event_receipt_date="20251201",
        event_fields=_fields(),
        candidates=[cross_stock, future],
    )
    assert result["classification"] == "NO_TIMELINE_MATCH"
    assert result["candidate_count"] == 0


def test_current_and_later_same_day_receipts_are_not_candidates() -> None:
    current = _candidate("20251201000010")
    current["receipt_date"] = "20251201"
    later = _candidate("20251201000011")
    later["receipt_date"] = "20251201"
    earlier = _candidate("20251201000009")
    earlier["receipt_date"] = "20251201"
    result = analyzer.select_timeline_candidate(
        event_receipt_no="20251201000010",
        event_stock_code="000001",
        event_receipt_date="20251201",
        event_fields=_fields(),
        candidates=[current, later, earlier],
    )
    assert result["classification"] == "UNIQUE_TIMELINE_MATCH"
    assert result["selected_initial_receipt"] == "20251201000009"


def _minimal_contract() -> dict[str, object]:
    return {
        "strategy_id": analyzer.STRATEGY_ID,
        "mode": "VALIDATION_ONLY",
        "matcher": {
            "fields": ["board_resolution_date", "merger_contract_date"],
            "required_exact_fields": 2,
            "actual_effective_date_input_allowed": False,
            "planned_effective_date_input_allowed": False,
            "receipt_link_input_allowed": False,
            "official_family_input_allowed": False,
            "list_order_input_allowed": False,
            "nearest_date_input_allowed": False,
            "same_stock_only": True,
            "prior_receipt_only": True,
            "both_fields_required": True,
            "exact_one_candidate_required": True,
        },
        "execution_permissions": {"orders_allowed": False},
    }


def test_contract_rejects_enabled_permission(tmp_path: Path) -> None:
    contract = _minimal_contract()
    contract["execution_permissions"] = {"orders_allowed": True}
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(analyzer.CompletionTimelineShadowError) as error:
        analyzer.load_contract(tmp_path, path)
    assert error.value.code == "COMPLETION_TIMELINE_PERMISSIONS_INVALID"


def test_contract_rejects_effective_date_input(tmp_path: Path) -> None:
    contract = _minimal_contract()
    contract["matcher"]["actual_effective_date_input_allowed"] = True
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(analyzer.CompletionTimelineShadowError) as error:
        analyzer.load_contract(tmp_path, path)
    assert error.value.code == "FORBIDDEN_MATCHER_INPUT_ENABLED"
