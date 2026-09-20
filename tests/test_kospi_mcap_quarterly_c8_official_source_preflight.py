from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path

from paper.strategies.kospi_mcap_quarterly_v1.src import contracts
from paper.strategies.kospi_mcap_quarterly_v1.src.c8_official_source_preflight import (
    CONTRACT_RELATIVE,
    run_official_source_preflight,
    write_official_source_preflight_report,
)


def _repo_with_contract() -> Path:
    root = contracts.STRATEGY_ROOT / "state" / f"test_c8_official_preflight_{uuid.uuid4().hex}"
    target = root / CONTRACT_RELATIVE
    target.parent.mkdir(parents=True, exist_ok=True)
    source = contracts.REPO_ROOT / CONTRACT_RELATIVE
    target.write_bytes(source.read_bytes())
    return root


def _listing_rows() -> list[dict[str, str]]:
    return [
        {
            "ISU_SRT_CD": f"{index:06d}",
            "ISU_ABBRV": f"TEST{index}",
            "LIST_DD": "20000101",
            "MKT_TP_NM": "KOSPI",
            "SECUGRP_NM": "주권",
            "KIND_STKCERT_TP_NM": "보통주",
        }
        for index in range(100)
    ]


def _price_rows(as_of: str) -> list[dict[str, str]]:
    return [
        {
            "BAS_DD": as_of,
            "ISU_CD": f"{index:06d}",
            "ISU_NM": f"TEST{index}",
            "MKT_NM": "KOSPI",
            "TDD_CLSPRC": "1000",
            "MKTCAP": "1000000",
        }
        for index in range(100)
    ]


def _fetch_success(route: dict, as_of: str, auth_key: str, timeout: float):
    assert auth_key == "secret-not-persisted"
    assert timeout > 0
    rows = _listing_rows() if route["api_id"] == "stk_isu_base_info" else _price_rows(as_of)
    payload = {"OutBlock_1": rows}
    content = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    return 200, content, payload


def test_two_official_api_routes_pass_without_opening_c8_or_persisting_key():
    root = _repo_with_contract()
    try:
        report = run_official_source_preflight(
            selection_as_of="20260914",
            auth_key="secret-not-persisted",
            repo_root=root,
            generated_at="2026-09-15T10:00:00+09:00",
            fetcher=_fetch_success,
        )
        assert report["status"] == "PASS"
        assert report["verdict"] == "C8_OFFICIAL_API_PREFLIGHT_PASS"
        assert report["api_routes_pass"] is True
        assert report["all_required_roles_ready"] is False
        assert report["selection_evidence_eligible"] is False
        assert report["auth_key_present"] is True
        assert report["auth_key_persisted"] is False
        assert report["m2_allowed"] is False
        assert report["raw_sources_published"] is False
        assert "secret-not-persisted" not in json.dumps(report, ensure_ascii=False)
        versioned, latest = write_official_source_preflight_report(report, repo_root=root)
        assert versioned.is_file()
        assert latest.is_file()
        assert "secret-not-persisted" not in latest.read_text(encoding="utf-8")
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_unauthorized_listing_route_blocks_even_when_price_route_passes():
    root = _repo_with_contract()
    try:
        def fetch(route: dict, as_of: str, auth_key: str, timeout: float):
            if route["api_id"] == "stk_isu_base_info":
                payload = {"respCode": "401", "respMsg": "Unauthorized API Call"}
                return 401, json.dumps(payload).encode(), payload
            payload = {"OutBlock_1": _price_rows(as_of)}
            return 200, json.dumps(payload).encode(), payload

        report = run_official_source_preflight(
            selection_as_of="20260914",
            auth_key="secret-not-persisted",
            repo_root=root,
            generated_at="2026-09-15T10:00:00+09:00",
            fetcher=fetch,
        )
        checks = {item["role"]: item for item in report["route_checks"]}
        assert report["status"] == "FAIL"
        assert report["verdict"] == "FAIL_C8_OFFICIAL_SOURCE_PREFLIGHT"
        assert report["all_required_roles_ready"] is False
        assert "KRX_OPENAPI_STK_ISU_BASE_INFO_HTTP_STATUS_401" in report["reason_codes"]
        assert checks["listing_security_as_of"]["status"] == "FAIL"
        assert checks["close_market_cap_as_of"]["status"] == "PASS"
        assert report["adapter_request_generated"] is False
        assert report["m2_allowed"] is False
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_empty_or_wrong_date_price_response_is_blocked():
    root = _repo_with_contract()
    try:
        for mode, expected_reason in (
            ("empty", "RESPONSE_ROW_COUNT_BELOW_MINIMUM"),
            ("wrong_date", "RESPONSE_ASOF_MISMATCH"),
        ):
            def fetch(route: dict, as_of: str, auth_key: str, timeout: float):
                if route["api_id"] == "stk_isu_base_info":
                    payload = {"OutBlock_1": _listing_rows()}
                elif mode == "empty":
                    payload = {"OutBlock_1": []}
                else:
                    payload = {"OutBlock_1": _price_rows("20260913")}
                return 200, json.dumps(payload).encode(), payload

            report = run_official_source_preflight(
                selection_as_of="20260914",
                auth_key="secret-not-persisted",
                repo_root=root,
                generated_at="2026-09-15T10:00:00+09:00",
                fetcher=fetch,
            )
            price = next(item for item in report["route_checks"] if item["role"] == "close_market_cap_as_of")
            assert expected_reason in price["reason_codes"]
            assert report["m2_allowed"] is False
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_missing_key_or_future_date_fails_before_fetch():
    root = _repo_with_contract()
    calls = 0
    try:
        def fetch(route: dict, as_of: str, auth_key: str, timeout: float):
            nonlocal calls
            calls += 1
            raise AssertionError("fetch must not run")

        missing = run_official_source_preflight(
            selection_as_of="20260914",
            auth_key="",
            repo_root=root,
            generated_at="2026-09-15T10:00:00+09:00",
            fetcher=fetch,
        )
        future = run_official_source_preflight(
            selection_as_of="20260916",
            auth_key="secret-not-persisted",
            repo_root=root,
            generated_at="2026-09-15T10:00:00+09:00",
            fetcher=fetch,
        )
        assert missing["reason_codes"] == ["KRX_AUTH_KEY_MISSING"]
        assert future["reason_codes"] == ["SELECTION_ASOF_IN_FUTURE"]
        assert calls == 0
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_route_contract_preserves_verified_query_shapes():
    contract = json.loads((contracts.REPO_ROOT / CONTRACT_RELATIVE).read_text(encoding="utf-8"))
    apis = {item["api_id"]: item for item in contract["official_api_routes"]}
    screens = {item["screen_id"]: item for item in contract["official_manual_history_routes"]}
    assert apis["stk_isu_base_info"]["date_parameter"] == "basDd"
    assert apis["stk_bydd_trd"]["output_as_of_field"] == "BAS_DD"
    assert screens["MDCSTAT213"]["coverage_shape"] == "PER_SECURITY_DATE_RANGE"
    assert screens["MDCSTAT215"]["query_parameters"] == ["isuCd", "strtDd", "endDd", "designDdYn"]
    assert screens["MDCSTAT237"]["coverage_shape"] == "CURRENT_ALL_MARKET_EXCEPTIONS_WITHOUT_QUERY_DATE"
    disclosure = {
        item["source_id"]: item for item in contract["official_disclosure_routes"]
    }
    corporate = disclosure["DART_CORPORATE_ACTIONS_COMPOSITE"]
    assert corporate["role"] == "corporate_actions_as_of"
    assert corporate["implementation_status"] == "ADAPTER_IMPLEMENTED_FULL_CAPTURE_PENDING"
    assert corporate["adapter_contract_id"] == (
        "KOSPI_MCAP_QUARTERLY_C8_CORPORATE_ACTION_ADAPTER"
    )
