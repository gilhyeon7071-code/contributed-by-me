from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src import contracts
from paper.strategies.kospi_mcap_quarterly_v1.src.c8_corporate_action_capture import (
    ADAPTER_CONTRACT_RELATIVE,
    CONTRACT_RELATIVE,
    assess_listing_capture,
    build_contract_status_report,
    create_capture_manifest,
    evaluate_capture_manifest,
    next_pending_requests,
    record_corp_code_package,
    record_document,
    record_list_page,
    write_capture_manifest,
)
from paper.strategies.kospi_mcap_quarterly_v1.src.contracts import ContractError


KST = timezone(timedelta(hours=9))
RAW_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/data/inbox/raw/"
    "mdcstat019_20260915.csv"
)


def _repo(tmp_path: Path, *, minimum_code_count: int = 100) -> Path:
    root = tmp_path / "repo"
    for relative in (CONTRACT_RELATIVE, ADAPTER_CONTRACT_RELATIVE):
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(contracts.REPO_ROOT / relative, destination)
    if minimum_code_count != 100:
        path = root / CONTRACT_RELATIVE
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["base_universe"]["minimum_code_count"] = minimum_code_count
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return root


def _rows(count: int = 100) -> list[str]:
    rows = [
        "표준코드,단축코드,한글 종목명,한글 종목약명,영문 종목명,상장일,시장구분,증권구분,소속부,주식종류,액면가,상장주식수"
    ]
    for index in range(1, count + 1):
        code = f"{index:06d}"
        rows.append(
            f"KR{code},{code},종목{index},종목{index},NAME{index},20000101,KOSPI,주권,,보통주,5000,1000000"
        )
    rows.extend(
        [
            "KR900001,900001,우선주,우선주,PREFERRED,20000101,KOSPI,주권,,신형우선주,5000,1000000",
            "KR900002,900002,코스닥,코스닥,KOSDAQ,20000101,KOSDAQ,주권,,보통주,5000,1000000",
        ]
    )
    return rows


def _evidence(
    root: Path,
    *,
    rows: list[str] | None = None,
    mtime: datetime | None = None,
) -> dict:
    raw_path = root / RAW_RELATIVE
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    payload = ("\n".join(rows or _rows()) + "\n").encode("utf-8-sig")
    raw_path.write_bytes(payload)
    raw_mtime = mtime or datetime(2026, 9, 15, 15, 41, 2, tzinfo=KST)
    mtime_ns = int(raw_mtime.timestamp() * 1_000_000_000)
    os.utime(raw_path, ns=(mtime_ns, mtime_ns))
    actual_mtime_ns = raw_path.stat().st_mtime_ns
    return {
        "strategy_id": "KOSPI_MCAP_QUARTERLY_V1",
        "role": "listing_security_as_of",
        "official_source_id": "KRX_MDCSTAT019_CURRENT_BASIC_INFO",
        "method": "KRX_DATA_MARKETPLACE_MANUAL",
        "temporal_mode": "RETRIEVAL_DATE_SNAPSHOT",
        "transform_profile_id": "MDCSTAT019_ACTIVE_LISTING_MEMBERSHIP_V1",
        "selection_as_of": "20260915",
        "capture_started_at": "2026-09-15T15:40:00+09:00",
        "retrieved_at": "2026-09-15T15:41:00+09:00",
        "http_response_timestamp": "2026-09-15T15:41:01+09:00",
        "captured_at": "2026-09-15T15:42:00+09:00",
        "authenticated_session_verified": True,
        "http_status": 200,
        "historical_replay": False,
        "source_page_url": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd?screenId=MDCSTAT019",
        "download_url": "https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd",
        "query_parameters": {"mktId": "ALL"},
        "raw_path": RAW_RELATIVE.as_posix(),
        "raw_sha256": hashlib.sha256(payload).hexdigest(),
        "raw_size_bytes": len(payload),
        "raw_file_mtime_ns": actual_mtime_ns,
    }


def _manifest(root: Path, count: int = 100) -> dict:
    return create_capture_manifest(
        _evidence(root, rows=_rows(count)),
        generated_at="2026-09-15T15:43:00+09:00",
        repo_root=root,
    )


def _mapping(manifest: dict) -> dict[str, str]:
    return {
        code: f"{index:08d}"
        for index, code in enumerate(manifest["universe"]["codes"], start=1)
    }


def test_valid_same_day_post_close_source_builds_exact_deterministic_manifest(tmp_path: Path):
    root = _repo(tmp_path)
    evidence = _evidence(root)
    assessment = assess_listing_capture(evidence, repo_root=root)
    assert assessment["status"] == "PASS"
    assert assessment["selected_code_count"] == 100
    assert assessment["earliest_listed_date"] == "20000101"
    first = create_capture_manifest(
        evidence, generated_at="2026-09-15T15:43:00+09:00", repo_root=root
    )
    second = create_capture_manifest(
        evidence, generated_at="2026-09-15T15:43:00+09:00", repo_root=root
    )
    assert first == second
    assert first["universe"]["codes"][0] == "000001"
    assert first["universe"]["codes"][-1] == "000100"
    assert first["request_plan"]["minimum_request_count"] == 201
    assert first["request_plan"]["additional_page_requests"] == "DYNAMIC_FROM_TOTAL_PAGE"
    assert first["manifest_version"] == "1.0.1"
    assert len(first["accepted_report_filter_sha256"]) == 64


def test_valid_krx_alphanumeric_short_codes_remain_in_exact_universe(tmp_path: Path):
    root = _repo(tmp_path)
    rows = _rows()
    rows.extend(
        [
            "KR70126Z0002,0126Z0,삼성에피스홀딩스보통주,삼성에피스홀딩스,SAMSUNG EPIS HOLDINGS,20251124,KOSPI,주권,,보통주,5000,1000000",
            "KR70120G0001,0120G0,삼양바이오팜보통주,삼양바이오팜,SAMYANG BIOPHARM,20251124,KOSPI,주권,,보통주,5000,1000000",
            "KR70220W0000,0220W0,한화머시너리앤서비스홀딩스보통주,한화머시너리앤서비스홀딩스,HANWHA MACHINERY AND SERVICE HOLDINGS,20260825,KOSPI,주권,,보통주,5000,1000000",
        ]
    )
    assessment = assess_listing_capture(_evidence(root, rows=rows), repo_root=root)
    assert assessment["status"] == "PASS"
    assert assessment["selected_code_count"] == 103
    assert {"0126Z0", "0120G0", "0220W0"}.issubset(assessment["codes"])


def test_pre_close_capture_is_rejected(tmp_path: Path):
    root = _repo(tmp_path)
    evidence = _evidence(
        root, mtime=datetime(2026, 9, 15, 15, 39, 2, tzinfo=KST)
    )
    evidence.update(
        {
            "capture_started_at": "2026-09-15T15:38:00+09:00",
            "retrieved_at": "2026-09-15T15:39:00+09:00",
            "http_response_timestamp": "2026-09-15T15:39:01+09:00",
            "captured_at": "2026-09-15T15:39:30+09:00",
        }
    )
    result = assess_listing_capture(evidence, repo_root=root)
    assert result["status"] == "FAIL"
    assert "BASE_CAPTURE_BEFORE_APPROVED_CLOSE_TIME" in result["reason_codes"]
    assert "BASE_RAW_MTIME_BEFORE_APPROVED_CLOSE_TIME" in result["reason_codes"]


def test_stale_file_or_asof_mismatch_is_rejected(tmp_path: Path):
    root = _repo(tmp_path)
    stale = _evidence(root, mtime=datetime(2026, 7, 15, 15, 41, 2, tzinfo=KST))
    stale_result = assess_listing_capture(stale, repo_root=root)
    assert "BASE_RAW_MTIME_OUTSIDE_CAPTURE_WINDOW" in stale_result["reason_codes"]
    assert "BASE_RAW_MTIME_DATE_MISMATCH" in stale_result["reason_codes"]

    current = _evidence(root)
    current["selection_as_of"] = "20260914"
    asof_result = assess_listing_capture(current, repo_root=root)
    assert "BASE_CAPTURE_DATE_AUTHORITY_MISMATCH" in asof_result["reason_codes"]
    assert "BASE_RAW_MTIME_DATE_MISMATCH" in asof_result["reason_codes"]


def test_hash_mtime_and_raw_path_tampering_are_rejected(tmp_path: Path):
    root = _repo(tmp_path)
    evidence = _evidence(root)
    evidence["raw_sha256"] = "f" * 64
    evidence["raw_file_mtime_ns"] += 1
    result = assess_listing_capture(evidence, repo_root=root)
    assert "BASE_RAW_SHA256_MISMATCH" in result["reason_codes"]
    assert "BASE_RAW_MTIME_NS_MISMATCH" in result["reason_codes"]

    outside = _evidence(root)
    outside["raw_path"] = "outside.csv"
    result = assess_listing_capture(outside, repo_root=root)
    assert "BASE_RAW_PATH_OUTSIDE_STRATEGY_RAW" in result["reason_codes"]


def test_duplicate_invalid_and_too_small_universe_are_rejected(tmp_path: Path):
    root = _repo(tmp_path)
    rows = _rows(99)
    rows.append(rows[1])
    rows.append(
        "KRBAD,BAD-01,오류,오류,BAD,20000101,KOSPI,주권,,보통주,5000,1000000"
    )
    result = assess_listing_capture(_evidence(root, rows=rows), repo_root=root)
    assert "BASE_UNIVERSE_CODE_DUPLICATE" in result["reason_codes"]
    assert "BASE_UNIVERSE_CODE_INVALID" in result["reason_codes"]
    assert "BASE_UNIVERSE_CODE_COUNT_BELOW_MINIMUM" in result["reason_codes"]


def test_contract_status_opens_nothing_without_authoritative_base_evidence(tmp_path: Path):
    root = _repo(tmp_path)
    report = build_contract_status_report(
        repo_root=root, generated_at="2026-09-15T14:30:00+09:00"
    )
    assert report["status"] == "BLOCKED"
    assert report["verdict"] == "C8_CORPORATE_ACTION_CAPTURE_BASE_BLOCKED"
    assert report["reason_codes"] == ["BASE_UNIVERSE_ACQUISITION_EVIDENCE_MISSING"]
    assert report["bulk_requests_executed"] == 0
    assert report["network_execution_authorized"] is False
    assert report["m2_allowed"] is False
    assert report["orders_generated"] is False


def test_checkpoint_order_retry_and_conflicting_success_rules(tmp_path: Path):
    root = _repo(tmp_path, minimum_code_count=2)
    manifest = _manifest(root, count=2)
    assert next_pending_requests(manifest) == [{"kind": "CORP_CODE_PACKAGE"}]
    failed = record_corp_code_package(
        manifest,
        status="FAIL",
        payload_sha256="a" * 64,
        row_count=0,
        captured_at="2026-09-15T15:44:00+09:00",
        error_code="TEMPORARY",
    )
    passed = record_corp_code_package(
        failed,
        status="PASS",
        payload_sha256="b" * 64,
        row_count=2,
        captured_at="2026-09-15T15:45:00+09:00",
        mapping=_mapping(manifest),
    )
    replay = record_corp_code_package(
        passed,
        status="PASS",
        payload_sha256="b" * 64,
        row_count=2,
        captured_at="2026-09-15T15:45:00+09:00",
        mapping=_mapping(manifest),
    )
    assert len(replay["checkpoint"]["corp_code_package"]["attempts"]) == 2
    assert next_pending_requests(replay, 1)[0] == {
        "kind": "LIST_PAGE",
        "code": "000001",
        "corp_code": "00000001",
        "series": "MERGER_DECISION_HISTORY",
        "page_no": 1,
    }
    with pytest.raises(ContractError, match="conflicting replay"):
        record_corp_code_package(
            replay,
            status="PASS",
            payload_sha256="c" * 64,
            row_count=2,
            captured_at="2026-09-15T15:46:00+09:00",
            mapping=_mapping(manifest),
        )


def test_dynamic_pages_documents_and_full_capture_completion(tmp_path: Path):
    root = _repo(tmp_path, minimum_code_count=2)
    manifest = _manifest(root, count=2)
    current = record_corp_code_package(
        manifest,
        status="PASS",
        payload_sha256="1" * 64,
        row_count=2,
        captured_at="2026-09-15T15:44:00+09:00",
        mapping=_mapping(manifest),
    )
    for code in current["universe"]["codes"]:
        for series in ("MERGER_DECISION_HISTORY", "MERGER_COMPLETION_HISTORY"):
            receipts = ["20260915000001"] if code == "000001" and series == "MERGER_DECISION_HISTORY" else []
            total_page = 2 if receipts else 0
            current = record_list_page(
                current,
                code=code,
                series=series,
                page_no=1,
                status="PASS",
                dart_status="000" if total_page else "013",
                payload_sha256=code[-1] * 64,
                row_count=len(receipts),
                total_page=total_page,
                accepted_receipts=receipts,
                captured_at="2026-09-15T15:45:00+09:00",
                repo_root=root,
            )
            if total_page == 2:
                current = record_list_page(
                    current,
                    code=code,
                    series=series,
                    page_no=2,
                    status="PASS",
                    dart_status="000",
                    payload_sha256="2" * 64,
                    row_count=0,
                    total_page=2,
                    accepted_receipts=[],
                    captured_at="2026-09-15T15:46:00+09:00",
                    repo_root=root,
                )
    assert next_pending_requests(current) == [
        {"kind": "DOCUMENT", "receipt_no": "20260915000001"}
    ]
    current = record_document(
        current,
        receipt_no="20260915000001",
        status="PASS",
        payload_sha256="d" * 64,
        package_member_count=1,
        captured_at="2026-09-15T15:47:00+09:00",
    )
    report = evaluate_capture_manifest(current, repo_root=root)
    assert report["status"] == "PASS"
    assert report["full_capture_complete"] is True
    assert report["acquisition_evidence_published"] is False
    assert report["m2_allowed"] is False


def test_unavailable_document_is_terminal_for_traversal_but_blocks_full_capture(
    tmp_path: Path,
):
    root = _repo(tmp_path, minimum_code_count=1)
    manifest = _manifest(root, count=1)
    current = record_corp_code_package(
        manifest,
        status="PASS",
        payload_sha256="1" * 64,
        row_count=1,
        captured_at="2026-09-15T15:44:00+09:00",
        mapping=_mapping(manifest),
    )
    current = record_list_page(
        current,
        code="000001",
        series="MERGER_DECISION_HISTORY",
        page_no=1,
        status="PASS",
        dart_status="000",
        payload_sha256="2" * 64,
        row_count=1,
        total_page=1,
        accepted_receipts=["20260915000001"],
        captured_at="2026-09-15T15:45:00+09:00",
        repo_root=root,
    )
    current = record_list_page(
        current,
        code="000001",
        series="MERGER_COMPLETION_HISTORY",
        page_no=1,
        status="PASS",
        dart_status="013",
        payload_sha256="3" * 64,
        row_count=0,
        total_page=0,
        accepted_receipts=[],
        captured_at="2026-09-15T15:46:00+09:00",
        repo_root=root,
    )
    current = record_document(
        current,
        receipt_no="20260915000001",
        status="UNAVAILABLE",
        payload_sha256="4" * 64,
        package_member_count=0,
        captured_at="2026-09-15T15:47:00+09:00",
        error_code="DART_DOCUMENT_STATUS_014",
    )
    assert next_pending_requests(current, repo_root=root) == []
    report = evaluate_capture_manifest(current, repo_root=root)
    assert report["status"] == "BLOCKED"
    assert report["verdict"] == "C8_CORPORATE_ACTION_CAPTURE_DOCUMENTS_UNAVAILABLE"
    assert report["reason_codes"] == ["CORPORATE_ACTION_DOCUMENTS_UNAVAILABLE"]
    assert report["manifest_integrity_pass"] is True
    assert report["unavailable_document_count"] == 1
    assert report["unavailable_receipts"] == ["20260915000001"]
    assert report["full_capture_complete"] is False
    with pytest.raises(ContractError, match="terminal result"):
        record_document(
            current,
            receipt_no="20260915000001",
            status="PASS",
            payload_sha256="5" * 64,
            package_member_count=1,
            captured_at="2026-09-15T15:48:00+09:00",
        )


def test_unavailable_document_requires_exact_014_contract(tmp_path: Path):
    root = _repo(tmp_path, minimum_code_count=1)
    manifest = _manifest(root, count=1)
    current = record_corp_code_package(
        manifest,
        status="PASS",
        payload_sha256="1" * 64,
        row_count=1,
        captured_at="2026-09-15T15:44:00+09:00",
        mapping=_mapping(manifest),
    )
    current = record_list_page(
        current,
        code="000001",
        series="MERGER_DECISION_HISTORY",
        page_no=1,
        status="PASS",
        dart_status="000",
        payload_sha256="2" * 64,
        row_count=1,
        total_page=1,
        accepted_receipts=["20260915000001"],
        captured_at="2026-09-15T15:45:00+09:00",
        repo_root=root,
    )
    with pytest.raises(ContractError, match="exact OpenDART status 014"):
        record_document(
            current,
            receipt_no="20260915000001",
            status="UNAVAILABLE",
            payload_sha256="4" * 64,
            package_member_count=0,
            captured_at="2026-09-15T15:47:00+09:00",
            error_code="DART_DOCUMENT_STATUS_020",
        )


def test_list_status_cannot_turn_invalid_empty_page_into_complete_history(tmp_path: Path):
    root = _repo(tmp_path, minimum_code_count=2)
    manifest = _manifest(root, count=2)
    current = record_corp_code_package(
        manifest,
        status="PASS",
        payload_sha256="1" * 64,
        row_count=2,
        captured_at="2026-09-15T15:44:00+09:00",
        mapping=_mapping(manifest),
    )
    with pytest.raises(ContractError, match="at least one total page"):
        record_list_page(
            current,
            code="000001",
            series="MERGER_DECISION_HISTORY",
            page_no=1,
            status="PASS",
            dart_status="000",
            payload_sha256="2" * 64,
            row_count=0,
            total_page=0,
            accepted_receipts=[],
            captured_at="2026-09-15T15:45:00+09:00",
            repo_root=root,
        )


def test_manifest_tamper_fails_and_writer_is_strategy_scoped(tmp_path: Path):
    root = _repo(tmp_path)
    manifest = _manifest(root)
    tampered = copy.deepcopy(manifest)
    tampered["universe"]["codes"][0] = "999999"
    report = evaluate_capture_manifest(tampered, repo_root=root)
    assert report["status"] == "FAIL"
    assert "CAPTURE_MANIFEST_CODE_SET_HASH_MISMATCH" in report["reason_codes"]

    filter_tamper = copy.deepcopy(manifest)
    filter_tamper["accepted_report_filter_sha256"] = "0" * 64
    report = evaluate_capture_manifest(filter_tamper, repo_root=root)
    assert report["status"] == "FAIL"
    assert "CAPTURE_MANIFEST_REPORT_FILTER_HASH_MISMATCH" in report["reason_codes"]

    checkpoint_tamper = record_corp_code_package(
        manifest,
        status="PASS",
        payload_sha256="1" * 64,
        row_count=100,
        captured_at="2026-09-15T15:44:00+09:00",
        mapping=_mapping(manifest),
    )
    checkpoint_tamper["checkpoint"]["list_pages"][
        "000001:MERGER_DECISION_HISTORY:1"
    ] = {
        "attempts": [
            {
                "status": "PASS",
                "dart_status": "000",
                "corp_code": "00000001",
                "code": "000001",
                "series": "MERGER_DECISION_HISTORY",
                "page_no": 1,
                "payload_sha256": "2" * 64,
                "row_count": 0,
                "total_page": 0,
                "accepted_receipts": [],
                "captured_at": "2026-09-15T15:45:00+09:00",
                "error_code": "",
            }
        ]
    }
    report = evaluate_capture_manifest(checkpoint_tamper, repo_root=root)
    assert report["status"] == "FAIL"
    assert "CAPTURE_LIST_SUCCESS_PAGINATION_INVALID" in report["reason_codes"]
    with pytest.raises(ValueError, match="outside strategy namespace"):
        write_capture_manifest(manifest, root / "outside.json", repo_root=root)
