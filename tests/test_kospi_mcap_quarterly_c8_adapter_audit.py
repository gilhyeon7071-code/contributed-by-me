from __future__ import annotations

import shutil
import uuid
from pathlib import Path

from paper.strategies.kospi_mcap_quarterly_v1.src import contracts
from paper.strategies.kospi_mcap_quarterly_v1.src.c8_adapter_audit import (
    build_adapter_capability_report,
    write_adapter_capability_report,
)


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_current_like_sources(repo_root: Path) -> None:
    static = repo_root / "_cache" / "manual_static_population_source"
    inbox = repo_root / "_krx_manual" / "_inbox"
    _write(
        static / "data_4425_20260715.csv",
        "표준코드,단축코드,한글 종목명,한글 종목약명,영문 종목명,상장일,시장구분,증권구분,소속부,주식종류,액면가,상장주식수\n"
        "KR7000000001,000001,테스트1,테스트1,TEST1,2000/01/01,KOSPI,주권,,보통주,5000,1000\n"
        "KR7000000002,000002,테스트2,테스트2,TEST2,2000/01/01,KOSPI,주권,,보통주,5000,1000\n"
        "KR7000000003,000003,테스트리츠,테스트리츠,TESTREIT,2000/01/01,KOSPI,부동산투자회사,,보통주,5000,1000\n",
    )
    _write(
        static / "data_4446_20260715.csv",
        "종목코드,종목명,매매거래정지,정리매매 종목,관리종목\n"
        "000001,테스트1,X,X,X\n"
        "000002,테스트2,X,X,X\n"
        "000003,테스트리츠,X,X,X\n",
    )
    _write(
        static / "data_1052_20260715.csv",
        "종목코드,종목명,시장구분,정지일,정지사유\n000001,테스트1,KOSPI,2026/07/15,투자자 보호\n",
    )
    _write(
        static / "data_5456_20260715.csv",
        "종목코드,종목명,시장구분,증권구분,주식종류,상장일,폐지일,폐지사유\n"
        "999999,과거종목,KOSPI,주권,보통주,2000/01/01,2020/01/01,피흡수합병\n",
    )
    price = (
        "종목코드,종목명,시장구분,종가,시가총액,상장주식수\n"
        "000001,테스트1,KOSPI,10000,100000000,1000\n"
        "000003,테스트리츠,KOSPI,10000,100000000,1000\n"
    )
    _write(inbox / "krx_price_20260821.csv", price)
    _write(inbox / "data_1628_20260822.csv", price)


def test_current_like_manual_sources_are_blocked_without_adapter_generation():
    repo_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_adapter_{uuid.uuid4().hex}"
    try:
        _write_current_like_sources(repo_root)
        report = build_adapter_capability_report(
            repo_root=repo_root,
            generated_at="2026-09-15T07:10:00+09:00",
        )
        roles = {item["role"]: item for item in report["role_checks"]}
        assert report["verdict"] == "C8_ADAPTERS_BLOCKED"
        assert report["root_cause"] == "FULL_SNAPSHOT_CONTRACT_MISMATCH_WITH_OFFICIAL_SOURCE_TEMPORAL_SHAPES"
        assert report["mapping_only_resolution_possible"] is False
        assert report["contract_fit"]["status"] == "FAIL_SOURCE_SHAPE_MISMATCH"
        assert report["contract_fit"]["exact_code_set_valid_stage"] == "AFTER_POINT_IN_TIME_STATE_RECONSTRUCTION"
        assert report["temporal_source_summary"]["mapping_only_roles"] == ["close_market_cap_as_of"]
        assert report["temporal_source_summary"]["state_reconstruction_roles"] == [
            "listing_security_as_of",
            "trade_management_status_as_of",
            "corporate_actions_as_of",
        ]
        assert report["temporal_source_summary"]["unconfirmed_complete_source_roles"] == []
        assert report["adapter_ready_roles"] == []
        assert report["build_request_possible"] is False
        assert report["adapter_files_generated"] is False
        assert roles["trade_management_status_as_of"]["coverage_status"] == "PASS"
        assert roles["trade_management_status_as_of"]["semantic_status"] == "PARTIAL"
        assert roles["trade_management_status_as_of"]["temporal_source_fit"]["mapping_only_sufficient"] is False
        assert roles["corporate_actions_as_of"]["temporal_source_fit"]["official_route_status"] == (
            "ADAPTER_IMPLEMENTED_FULL_CAPTURE_PENDING"
        )
        assert roles["close_market_cap_as_of"]["temporal_source_fit"]["historical_as_of_capability"] == (
            "CONFIRMED"
        )
        listing_evidence = roles["listing_security_as_of"]["evidence"]
        assert listing_evidence["kospi_rows"] == 3
        assert listing_evidence["kospi_common_codes"] == 2
        assert listing_evidence["excluded_kospi_non_common_codes"] == 1
        assert listing_evidence["excluded_kospi_by_security_type"] == {"부동산투자회사": 1}
        assert report["field_mapping_readiness"]["corporate_actions_as_of"]["status"] == "BLOCKED"
        assert report["field_mapping_readiness"]["corporate_actions_as_of"]["blocked_fields"] == [
            "full_capture_evidence"
        ]
        assert roles["close_market_cap_as_of"]["evidence"]["missing_kospi_common_codes"] == ["000002"]
        assert len(report["identical_bytes_conflicting_file_dates"]) == 1
        assert report["worst_case"]["verdict"] == "PASS_FAIL_CLOSED"
        versioned, latest = write_adapter_capability_report(report, repo_root=repo_root)
        assert versioned.is_file()
        assert latest.is_file()
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_missing_manual_sources_remain_blocked():
    repo_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_adapter_empty_{uuid.uuid4().hex}"
    try:
        report = build_adapter_capability_report(
            repo_root=repo_root,
            generated_at="2026-09-15T07:10:00+09:00",
        )
        assert report["verdict"] == "C8_ADAPTERS_BLOCKED"
        assert report["adapter_ready_roles"] == []
        assert report["build_request_possible"] is False
        assert report["mapping_only_resolution_possible"] is False
        assert report["contract_fit"]["status"] == "FAIL_SOURCE_SHAPE_MISMATCH"
        assert all(item["adapter_status"] == "BLOCKED" for item in report["role_checks"])
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)
