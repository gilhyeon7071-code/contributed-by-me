from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from datetime import datetime
from pathlib import Path

from paper.strategies.kospi_mcap_quarterly_v1.src import contracts
from paper.strategies.kospi_mcap_quarterly_v1.src.c8_acquisition_evidence import (
    capture_acquisition_evidence,
)


ROLES = (
    "listing_security_as_of",
    "trade_management_status_as_of",
    "corporate_actions_as_of",
    "close_market_cap_as_of",
)
STRATEGY_RELATIVE = contracts.STRATEGY_ROOT.relative_to(contracts.REPO_ROOT)


def _write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _request_fixture(repo_root: Path, *, as_of: str = "20260914") -> Path:
    capture_started = "2026-09-15T07:19:00+09:00"
    retrieved_at = "2026-09-15T07:20:00+09:00"
    captured_at = "2026-09-15T07:21:00+09:00"
    mtime = datetime.fromisoformat(retrieved_at).timestamp()
    sources = []
    for index, role in enumerate(ROLES, start=1):
        relative = STRATEGY_RELATIVE / "data" / "inbox" / "raw" / f"source_{index}.csv"
        raw_path = repo_root / relative
        content = f"code,value\n000001,{index}\n".encode("utf-8")
        _write(raw_path, content)
        os.utime(raw_path, (mtime, mtime))
        source = {
            "role": role,
            "method": "KRX_DATA_MARKETPLACE_MANUAL",
            "official_source_id": f"MDCSTAT_TEST_{index}",
            "source_page_url": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
            "download_url": "https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd",
            "query_parameters": {"trdDd": as_of, "mktId": "STK"},
            "date_parameter": "trdDd",
            "as_of_date": as_of,
            "retrieved_at": retrieved_at,
            "raw_path": relative.as_posix(),
            "raw_sha256": hashlib.sha256(content).hexdigest(),
        }
        if role == ROLES[3]:
            source.update(
                {
                    "method": "KRX_OPEN_API",
                    "official_source_id": "KRX_OPENAPI_STK_BYDD_TRD",
                    "source_page_url": "https://openapi.krx.co.kr/contents/OPP/USES/service/OPPUSES002_S2.cmd",
                    "download_url": "https://data-dbg.krx.co.kr/svc/apis/sto/stk_bydd_trd",
                    "query_parameters": {"basDd": as_of},
                    "date_parameter": "basDd",
                }
            )
        sources.append(source)
    payload = {
        "request_version": "1.0.0",
        "strategy_id": contracts.STRATEGY_ID,
        "capture_mode": "AT_RETRIEVAL",
        "selection_as_of": as_of,
        "capture_started_at": capture_started,
        "captured_at": captured_at,
        "sources": sources,
    }
    request = STRATEGY_RELATIVE / "data" / "inbox" / "acquisition" / "c8_acquisition_request_latest.json"
    request_path = repo_root / request
    _write(request_path, (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8"))
    return request_path


def _add_interval_status_source(request_path: Path, repo_root: Path) -> None:
    payload = json.loads(request_path.read_text(encoding="utf-8"))
    status = next(item for item in payload["sources"] if item["role"] == ROLES[1])
    status.update(
        {
            "source_id": "STATUS_SUSPENSION_HISTORY",
            "temporal_mode": "EXHAUSTIVE_INTERVAL_HISTORY",
            "query_parameters": {"strtDd": "20200102", "endDd": payload["selection_as_of"], "mktId": "STK"},
            "query_start_parameter": "strtDd",
            "query_end_parameter": "endDd",
        }
    )
    relative = STRATEGY_RELATIVE / "data" / "inbox" / "raw" / "status_management_history.csv"
    raw_path = repo_root / relative
    content = b"code,start_date,end_date\n000001,20200102,\n"
    _write(raw_path, content)
    retrieved_at = datetime.fromisoformat(status["retrieved_at"]).timestamp()
    os.utime(raw_path, (retrieved_at, retrieved_at))
    payload["sources"].append(
        {
            **status,
            "source_id": "STATUS_MANAGEMENT_HISTORY",
            "official_source_id": "MDCSTAT_TEST_STATUS_MANAGEMENT",
            "raw_path": relative.as_posix(),
            "raw_sha256": hashlib.sha256(content).hexdigest(),
        }
    )
    payload["role_coverage"] = [
        {
            "role": ROLES[1],
            "mode": "EXHAUSTIVE_INTERVAL_HISTORY",
            "basis": "EXHAUSTIVE_OFFICIAL_EVENT_HISTORY",
            "scope": "ALL_BASE_UNIVERSE_CODES",
            "query_start": "20200102",
            "query_end": payload["selection_as_of"],
            "source_ids": ["STATUS_SUSPENSION_HISTORY", "STATUS_MANAGEMENT_HISTORY"],
            "covered_fields": ["trade_status", "management_status", "delisting_procedure_status"],
        }
    ]
    request_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _same_day_retrieval_fixture(
    repo_root: Path,
    *,
    as_of: str = "20260915",
    retrieved_at: str = "2026-09-15T16:00:00+09:00",
) -> Path:
    request_path = _request_fixture(repo_root, as_of=as_of)
    payload = json.loads(request_path.read_text(encoding="utf-8"))
    payload["capture_started_at"] = "2026-09-15T15:59:00+09:00"
    payload["captured_at"] = "2026-09-15T16:02:00+09:00"
    mtime = datetime.fromisoformat(retrieved_at).timestamp()
    for source in payload["sources"]:
        source["retrieved_at"] = retrieved_at
        raw_path = repo_root / source["raw_path"]
        os.utime(raw_path, (mtime, mtime))

    listing = payload["sources"][0]
    listing.update(
        {
            "source_id": "LISTING_CURRENT",
            "temporal_mode": "RETRIEVAL_DATE_SNAPSHOT",
            "official_source_id": "KRX_MDCSTAT019_CURRENT_BASIC_INFO",
            "query_parameters": {"mktId": "ALL"},
            "date_parameter": None,
            "covered_fields": [
                "market",
                "security_type",
                "listed_date",
                "delisted_date",
                "listing_status",
            ],
            "authenticated_session_verified": True,
            "http_status": 200,
            "http_response_timestamp": retrieved_at,
            "historical_replay": False,
        }
    )

    status = payload["sources"][1]
    status.update(
        {
            "source_id": "STATUS_SUSPENSION_HISTORY",
            "temporal_mode": "EXHAUSTIVE_INTERVAL_HISTORY",
            "query_parameters": {"strtDd": "20200102", "endDd": as_of, "mktId": "STK"},
            "query_start_parameter": "strtDd",
            "query_end_parameter": "endDd",
            "covered_fields": ["trade_status"],
        }
    )
    management_relative = (
        STRATEGY_RELATIVE / "data" / "inbox" / "raw" / "status_management_history.csv"
    )
    management_content = b"code,start_date,end_date\n000001,20200102,\n"
    _write(repo_root / management_relative, management_content)
    os.utime(repo_root / management_relative, (mtime, mtime))
    payload["sources"].append(
        {
            **status,
            "source_id": "STATUS_MANAGEMENT_HISTORY",
            "official_source_id": "MDCSTAT_TEST_STATUS_MANAGEMENT",
            "covered_fields": ["management_status"],
            "raw_path": management_relative.as_posix(),
            "raw_sha256": hashlib.sha256(management_content).hexdigest(),
        }
    )

    liquidation_relative = (
        STRATEGY_RELATIVE / "data" / "inbox" / "raw" / "status_liquidation_current.csv"
    )
    liquidation_content = b"code,start_date,end_date\n000001,20260915,\n"
    _write(repo_root / liquidation_relative, liquidation_content)
    os.utime(repo_root / liquidation_relative, (mtime, mtime))
    payload["sources"].append(
        {
            "role": ROLES[1],
            "source_id": "STATUS_LIQUIDATION_CURRENT",
            "temporal_mode": "RETRIEVAL_DATE_SNAPSHOT",
            "method": "KRX_DATA_MARKETPLACE_MANUAL",
            "official_source_id": "KRX_MDCSTAT237_LIQUIDATION_CURRENT",
            "source_page_url": "https://data.krx.co.kr/contents/MDC/STAT/issue/MDCSTAT237.jsp",
            "download_url": "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
            "query_parameters": {"mktId": "ALL"},
            "date_parameter": None,
            "as_of_date": as_of,
            "retrieved_at": retrieved_at,
            "raw_path": liquidation_relative.as_posix(),
            "raw_sha256": hashlib.sha256(liquidation_content).hexdigest(),
            "covered_fields": ["delisting_procedure_status"],
            "authenticated_session_verified": True,
            "http_status": 200,
            "http_response_timestamp": retrieved_at,
            "historical_replay": False,
        }
    )
    payload["role_coverage"] = [
        {
            "role": ROLES[1],
            "mode": "MIXED_APPROVED_TEMPORAL_COMPONENTS",
            "basis": "EXACT_APPROVED_MIXED_TEMPORAL_COMPONENTS",
            "scope": "ALL_BASE_UNIVERSE_CODES",
            "query_start": "20200102",
            "query_end": as_of,
            "source_ids": [
                "STATUS_SUSPENSION_HISTORY",
                "STATUS_MANAGEMENT_HISTORY",
                "STATUS_LIQUIDATION_CURRENT",
            ],
            "covered_fields": [
                "trade_status",
                "management_status",
                "delisting_procedure_status",
            ],
        }
    ]
    request_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return request_path


def test_four_official_sources_publish_immutable_capture_only():
    repo_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_acquisition_{uuid.uuid4().hex}"
    try:
        request = _request_fixture(repo_root)
        report = capture_acquisition_evidence(
            request_path=request,
            repo_root=repo_root,
            generated_at="2026-09-15T07:22:00+09:00",
        )
        assert report["verdict"] == "C8_ACQUISITION_CAPTURE_READY"
        assert report["evidence_published"] is True
        assert len(report["source_checks"]) == 4
        assert all(item["status"] == "PASS" for item in report["source_checks"])
        assert all(
            item["query_parameters"]["trdDd"] == "20260914"
            for item in report["source_checks"]
            if item["role"] != ROLES[3]
        )
        close = next(item for item in report["source_checks"] if item["role"] == ROLES[3])
        assert close["query_parameters"]["basDd"] == "20260914"
        assert close["source_page_url"].startswith("https://openapi.krx.co.kr/")
        assert close["download_url"].startswith("https://data-dbg.krx.co.kr/")
        assert report["adapter_files_generated"] is False
        assert report["build_request_generated"] is False
        assert report["canonical_generated"] is False
        assert report["m2_allowed"] is False
        assert (repo_root / report["outputs"]["evidence_versioned"]).is_file()
        assert (repo_root / report["outputs"]["evidence_latest"]).is_file()
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_openapi_cross_host_and_lookalike_host_are_blocked_before_publication():
    repo_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_acquisition_host_{uuid.uuid4().hex}"
    try:
        request = _request_fixture(repo_root)
        payload = json.loads(request.read_text(encoding="utf-8"))
        close = next(item for item in payload["sources"] if item["role"] == ROLES[3])
        close["source_page_url"] = "https://data-dbg.krx.co.kr/svc/apis/sto/stk_bydd_trd"
        close["download_url"] = "https://data-dbg.krx.co.kr.evil.example/svc/apis/sto/stk_bydd_trd"
        request.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        report = capture_acquisition_evidence(
            request_path=request,
            repo_root=repo_root,
            generated_at="2026-09-15T07:22:00+09:00",
        )
        close_check = next(item for item in report["source_checks"] if item["role"] == ROLES[3])
        assert report["verdict"] == "FAIL_C8_ACQUISITION_CAPTURE"
        assert "SOURCE_PAGE_URL_INVALID" in close_check["reason_codes"]
        assert "DOWNLOAD_URL_INVALID" in close_check["reason_codes"]
        assert report["evidence_published"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_hash_tamper_is_blocked_before_evidence_publication():
    repo_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_acquisition_hash_{uuid.uuid4().hex}"
    try:
        request = _request_fixture(repo_root)
        payload = json.loads(request.read_text(encoding="utf-8"))
        raw_path = repo_root / payload["sources"][0]["raw_path"]
        raw_path.write_bytes(raw_path.read_bytes() + b"tampered\n")
        report = capture_acquisition_evidence(
            request_path=request,
            repo_root=repo_root,
            generated_at="2026-09-15T07:22:00+09:00",
        )
        assert report["verdict"] == "FAIL_C8_ACQUISITION_CAPTURE"
        assert "RAW_FILE_HASH_MISMATCH" in report["reason_codes"]
        assert report["evidence_published"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_multiple_official_status_histories_publish_with_explicit_coverage():
    repo_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_acquisition_multi_{uuid.uuid4().hex}"
    try:
        request = _request_fixture(repo_root)
        _add_interval_status_source(request, repo_root)
        report = capture_acquisition_evidence(
            request_path=request,
            repo_root=repo_root,
            generated_at="2026-09-15T07:22:00+09:00",
        )
        assert report["verdict"] == "C8_ACQUISITION_CAPTURE_READY"
        assert report["evidence_published"] is True
        assert len(report["source_checks"]) == 5
        status_coverage = next(item for item in report["role_coverage_checks"] if item["role"] == ROLES[1])
        assert status_coverage["status"] == "PASS"
        assert status_coverage["mode"] == "EXHAUSTIVE_INTERVAL_HISTORY"
        assert set(status_coverage["source_ids"]) == {
            "STATUS_SUSPENSION_HISTORY",
            "STATUS_MANAGEMENT_HISTORY",
        }
        assert len(status_coverage["source_sha256s"]) == 2
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_incomplete_sparse_role_coverage_is_blocked_before_publication():
    repo_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_acquisition_coverage_{uuid.uuid4().hex}"
    try:
        request = _request_fixture(repo_root)
        _add_interval_status_source(request, repo_root)
        payload = json.loads(request.read_text(encoding="utf-8"))
        payload["role_coverage"][0]["covered_fields"] = ["trade_status", "management_status"]
        request.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        report = capture_acquisition_evidence(
            request_path=request,
            repo_root=repo_root,
            generated_at="2026-09-15T07:22:00+09:00",
        )
        assert report["verdict"] == "FAIL_C8_ACQUISITION_CAPTURE"
        assert "ROLE_COVERAGE_FIELDS_INCOMPLETE" in report["reason_codes"]
        assert report["evidence_published"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_secret_or_future_asof_cannot_be_persisted_as_evidence():
    repo_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_acquisition_secret_{uuid.uuid4().hex}"
    try:
        request = _request_fixture(repo_root, as_of="20260930")
        payload = json.loads(request.read_text(encoding="utf-8"))
        payload["sources"][0]["query_parameters"]["AUTH_KEY"] = "must-not-persist"
        request.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        report = capture_acquisition_evidence(
            request_path=request,
            repo_root=repo_root,
            generated_at="2026-09-15T07:22:00+09:00",
        )
        assert report["verdict"] == "FAIL_C8_ACQUISITION_CAPTURE"
        assert "SECRET_MATERIAL_PERSISTED" in report["reason_codes"]
        assert "SELECTION_ASOF_IN_FUTURE" in report["reason_codes"]
        assert report["evidence_published"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_same_day_retrieval_sources_publish_v1_2_evidence_without_opening_m2():
    repo_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_retrieval_{uuid.uuid4().hex}"
    try:
        request = _same_day_retrieval_fixture(repo_root)
        report = capture_acquisition_evidence(
            request_path=request,
            repo_root=repo_root,
            generated_at="2026-09-15T16:03:00+09:00",
        )
        assert report["verdict"] == "C8_ACQUISITION_CAPTURE_READY"
        assert report["evidence_published"] is True
        assert report["m2_allowed"] is False
        retrieval_checks = [
            item for item in report["source_checks"]
            if item["temporal_mode"] == "RETRIEVAL_DATE_SNAPSHOT"
        ]
        assert {item["official_source_id"] for item in retrieval_checks} == {
            "KRX_MDCSTAT019_CURRENT_BASIC_INFO",
            "KRX_MDCSTAT237_LIQUIDATION_CURRENT",
        }
        assert all(item["retrieval_date_authority"]["authenticated_session_verified"] for item in retrieval_checks)
        status = next(item for item in report["role_coverage_checks"] if item["role"] == ROLES[1])
        assert status["mode"] == "MIXED_APPROVED_TEMPORAL_COMPONENTS"
        assert status["status"] == "PASS"
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_previous_day_retrieval_snapshot_is_blocked():
    repo_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_retrieval_old_{uuid.uuid4().hex}"
    try:
        request = _same_day_retrieval_fixture(repo_root, as_of="20260914")
        report = capture_acquisition_evidence(
            request_path=request,
            repo_root=repo_root,
            generated_at="2026-09-15T16:03:00+09:00",
        )
        assert report["evidence_published"] is False
        assert any(reason.endswith("_DATE_MISMATCH") for reason in report["reason_codes"])
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_unapproved_or_unauthenticated_retrieval_source_is_blocked():
    repo_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_retrieval_auth_{uuid.uuid4().hex}"
    try:
        request = _same_day_retrieval_fixture(repo_root)
        payload = json.loads(request.read_text(encoding="utf-8"))
        listing = payload["sources"][0]
        listing["official_source_id"] = "KRX_UNAPPROVED_CURRENT_SOURCE"
        listing["authenticated_session_verified"] = False
        request.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        report = capture_acquisition_evidence(
            request_path=request,
            repo_root=repo_root,
            generated_at="2026-09-15T16:03:00+09:00",
        )
        assert report["evidence_published"] is False
        assert "SOURCE_RETRIEVAL_ID_NOT_APPROVED" in report["reason_codes"]
        assert "SOURCE_RETRIEVAL_AUTHENTICATION_NOT_VERIFIED" in report["reason_codes"]
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_retrieval_snapshot_with_date_query_is_blocked():
    repo_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_retrieval_query_{uuid.uuid4().hex}"
    try:
        request = _same_day_retrieval_fixture(repo_root)
        payload = json.loads(request.read_text(encoding="utf-8"))
        listing = payload["sources"][0]
        listing["date_parameter"] = "trdDd"
        listing["query_parameters"]["trdDd"] = payload["selection_as_of"]
        request.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        report = capture_acquisition_evidence(
            request_path=request,
            repo_root=repo_root,
            generated_at="2026-09-15T16:03:00+09:00",
        )
        assert report["evidence_published"] is False
        assert "SOURCE_RETRIEVAL_DATE_PARAMETER_MUST_BE_EMPTY" in report["reason_codes"]
        assert "SOURCE_RETRIEVAL_QUERY_CONTAINS_DATE_PARAMETER" in report["reason_codes"]
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_mixed_status_field_gap_is_blocked():
    repo_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_retrieval_gap_{uuid.uuid4().hex}"
    try:
        request = _same_day_retrieval_fixture(repo_root)
        payload = json.loads(request.read_text(encoding="utf-8"))
        management = next(item for item in payload["sources"] if item.get("source_id") == "STATUS_MANAGEMENT_HISTORY")
        management["covered_fields"] = []
        request.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        report = capture_acquisition_evidence(
            request_path=request,
            repo_root=repo_root,
            generated_at="2026-09-15T16:03:00+09:00",
        )
        assert report["evidence_published"] is False
        assert "ROLE_SOURCE_COVERAGE_FIELDS_INCOMPLETE" in report["reason_codes"]
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)
