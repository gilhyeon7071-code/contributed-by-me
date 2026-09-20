from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pytest

from tools import analyze_kospi_mcap_quarterly_c8_event_identity_shadow as analyzer


MATCHER = {
    "identity_fields": ["counterparty_company", "merger_method", "merger_ratio"],
    "conflict_fields": [
        "counterparty_company",
        "merger_method",
        "merger_ratio",
        "planned_effective_date",
    ],
    "minimum_exact_matches": 2,
    "minimum_identity_matches": 1,
}


def _fields(**overrides: str) -> dict[str, str]:
    values = {
        "counterparty_company": "targetco",
        "merger_method": "absorption",
        "merger_ratio": "1005",
        "planned_effective_date": "20260131",
        "board_resolution_date": "20251001",
    }
    values.update(overrides)
    return values


def _candidate(receipt: str, **overrides: str) -> dict[str, object]:
    return {
        "receipt_no": receipt,
        "stock_code": "000001",
        "receipt_date": "20251001",
        "fields": _fields(**overrides),
    }


def _zip_document(text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("document.html", text.encode("utf-8"))
    return buffer.getvalue()


def test_normalize_value_is_nfkc_and_punctuation_insensitive() -> None:
    assert analyzer._normalize_value("ＣＪ㈜ : 1.0") == "cj주1①0".replace("①", "")


def test_extract_event_fields_from_flat_document() -> None:
    payload = _zip_document(
        "1. 합병상대회사 가. 회사의 개요 회사명 대상식품(주) 주요사업 음식료 제조업 "
        "합병방법 대상(주)가 대상식품(주)를 흡수합병 2. 합병목적 경쟁력 강화 "
        "합병비율 대상 1 : 대상식품 0.25 4. 합병비율 산출근거 기준주가 "
        "합병기일 2026년 01월 31일 이사회결의일(결정일) 2025년 10월 01일"
    )
    fields, reasons = analyzer.extract_event_fields(
        payload,
        date_patterns=["([0-9]{4})[.년/-][ ]*([0-9]{1,2})[.월/-][ ]*([0-9]{1,2})"],
    )
    assert reasons == []
    assert fields["counterparty_company"] == "대상식품주"
    assert fields["merger_method"] == "대상주가대상식품주를흡수합병"
    assert fields["merger_ratio"] == "대상1대상식품025"
    assert fields["planned_effective_date"] == "20260131"
    assert fields["board_resolution_date"] == "20251001"


def test_candidate_passes_with_two_matches_including_identity() -> None:
    result = analyzer.evaluate_candidate(
        _fields(board_resolution_date=""),
        _fields(board_resolution_date="20251002"),
        **MATCHER,
    )
    assert result["passed"] is True
    assert "merger_method" in result["identity_matched_fields"]


def test_nonempty_identity_conflict_rejects_candidate() -> None:
    result = analyzer.evaluate_candidate(
        _fields(merger_ratio="1005"),
        _fields(merger_ratio="1006"),
        **MATCHER,
    )
    assert result["passed"] is False
    assert result["conflict_fields"] == ["merger_ratio"]


def test_unique_candidate_is_selected() -> None:
    result = analyzer.select_shadow_candidate(
        event_receipt_no="20251201000001",
        event_stock_code="000001",
        event_receipt_date="20251201",
        event_fields=_fields(),
        candidates=[
            _candidate("20251001000001"),
            _candidate("20251001000002", merger_ratio="9999"),
        ],
        matcher=MATCHER,
    )
    assert result["classification"] == "UNIQUE_STRUCTURED_MATCH"
    assert result["selected_initial_receipt"] == "20251001000001"


def test_multiple_passing_candidates_fail_closed() -> None:
    result = analyzer.select_shadow_candidate(
        event_receipt_no="20251201000001",
        event_stock_code="000001",
        event_receipt_date="20251201",
        event_fields=_fields(),
        candidates=[
            _candidate("20251001000001"),
            _candidate("20251001000002"),
        ],
        matcher=MATCHER,
    )
    assert result["classification"] == "AMBIGUOUS_STRUCTURED_MATCH"
    assert result["selected_initial_receipt"] == ""


def test_cross_stock_and_future_candidates_are_not_evaluated() -> None:
    cross_stock = _candidate("20251001000001")
    cross_stock["stock_code"] = "999999"
    future = _candidate("20260101000001")
    future["receipt_date"] = "20260101"
    result = analyzer.select_shadow_candidate(
        event_receipt_no="20251201000001",
        event_stock_code="000001",
        event_receipt_date="20251201",
        event_fields=_fields(),
        candidates=[cross_stock, future],
        matcher=MATCHER,
    )
    assert result["classification"] == "NO_STRUCTURED_MATCH"
    assert result["candidate_count"] == 0


def test_current_and_later_same_day_receipts_are_not_candidates() -> None:
    current = _candidate("20251201000010")
    current["receipt_date"] = "20251201"
    later = _candidate("20251201000011")
    later["receipt_date"] = "20251201"
    earlier = _candidate("20251201000009")
    earlier["receipt_date"] = "20251201"
    result = analyzer.select_shadow_candidate(
        event_receipt_no="20251201000010",
        event_stock_code="000001",
        event_receipt_date="20251201",
        event_fields=_fields(),
        candidates=[current, later, earlier],
        matcher=MATCHER,
    )
    assert result["classification"] == "UNIQUE_STRUCTURED_MATCH"
    assert result["selected_initial_receipt"] == "20251201000009"
    assert result["candidate_count"] == 1


def test_contract_rejects_enabled_permission(tmp_path: Path) -> None:
    contract = {
        "strategy_id": analyzer.STRATEGY_ID,
        "mode": "VALIDATION_ONLY",
        "matcher": {
            "receipt_link_input_allowed": False,
            "official_family_input_allowed": False,
            "list_order_input_allowed": False,
            "nearest_date_input_allowed": False,
            "same_stock_only": True,
            "prior_receipt_only": True,
            "exact_one_candidate_required": True,
        },
        "execution_permissions": {"orders_allowed": True},
    }
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(analyzer.EventIdentityShadowError) as error:
        analyzer.load_contract(tmp_path, path)
    assert error.value.code == "EVENT_IDENTITY_PERMISSIONS_INVALID"


def test_contract_rejects_forbidden_matcher_input(tmp_path: Path) -> None:
    contract = {
        "strategy_id": analyzer.STRATEGY_ID,
        "mode": "VALIDATION_ONLY",
        "matcher": {
            "receipt_link_input_allowed": True,
            "official_family_input_allowed": False,
            "list_order_input_allowed": False,
            "nearest_date_input_allowed": False,
            "same_stock_only": True,
            "prior_receipt_only": True,
            "exact_one_candidate_required": True,
        },
        "execution_permissions": {"orders_allowed": False},
    }
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(analyzer.EventIdentityShadowError) as error:
        analyzer.load_contract(tmp_path, path)
    assert error.value.code == "FORBIDDEN_MATCHER_INPUT_ENABLED"
