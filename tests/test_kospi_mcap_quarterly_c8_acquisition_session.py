from __future__ import annotations

import json
import os
import shutil
import uuid
from datetime import datetime
from pathlib import Path

from paper.strategies.kospi_mcap_quarterly_v1.src import contracts
from paper.strategies.kospi_mcap_quarterly_v1.src.c8_acquisition_session import (
    finalize_acquisition_session,
    start_acquisition_session,
)


ROLES = (
    "listing_security_as_of",
    "trade_management_status_as_of",
    "corporate_actions_as_of",
    "close_market_cap_as_of",
)


def _descriptor(repo_root: Path, *, as_of: str = "20260914", mtime: str = "2026-09-15T07:31:00+09:00") -> Path:
    source_dir = repo_root / "downloads"
    source_dir.mkdir(parents=True, exist_ok=True)
    sources = []
    timestamp = datetime.fromisoformat(mtime).timestamp()
    for index, role in enumerate(ROLES, start=1):
        path = source_dir / f"source_{index}.csv"
        path.write_text(f"code,value\n000001,{index}\n", encoding="utf-8")
        os.utime(path, (timestamp, timestamp))
        source = {
            "role": role,
            "source_path": str(path),
            "method": "KRX_DATA_MARKETPLACE_MANUAL",
            "official_source_id": f"MDCSTAT_TEST_{index}",
            "source_page_url": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
            "download_url": "https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd",
            "query_parameters": {"trdDd": as_of, "mktId": "STK"},
            "date_parameter": "trdDd",
            "as_of_date": as_of,
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
        "descriptor_version": "1.0.0",
        "strategy_id": contracts.STRATEGY_ID,
        "selection_as_of": as_of,
        "sources": sources,
    }
    path = repo_root / "descriptor.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _add_interval_status_source(descriptor_path: Path, repo_root: Path) -> None:
    payload = json.loads(descriptor_path.read_text(encoding="utf-8"))
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
    source_path = repo_root / "downloads" / "status_management_history.csv"
    source_path.write_text("code,start_date,end_date\n000001,20200102,\n", encoding="utf-8")
    os.utime(source_path, (Path(status["source_path"]).stat().st_mtime,) * 2)
    payload["sources"].append(
        {
            **status,
            "source_id": "STATUS_MANAGEMENT_HISTORY",
            "source_path": str(source_path),
            "official_source_id": "MDCSTAT_TEST_STATUS_MANAGEMENT",
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
    descriptor_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _same_day_descriptor(
    repo_root: Path,
    *,
    mtime: str = "2026-09-15T16:00:00+09:00",
) -> Path:
    descriptor = _descriptor(repo_root, as_of="20260915", mtime=mtime)
    _add_interval_status_source(descriptor, repo_root)
    payload = json.loads(descriptor.read_text(encoding="utf-8"))
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
            "http_response_timestamp": mtime,
            "historical_replay": False,
        }
    )
    status_sources = [item for item in payload["sources"] if item["role"] == ROLES[1]]
    status_sources[0]["covered_fields"] = ["trade_status"]
    status_sources[1]["covered_fields"] = ["management_status"]

    source_path = repo_root / "downloads" / "status_liquidation_current.csv"
    source_path.write_text("code,start_date,end_date\n000001,20260915,\n", encoding="utf-8")
    timestamp = datetime.fromisoformat(mtime).timestamp()
    os.utime(source_path, (timestamp, timestamp))
    payload["sources"].append(
        {
            "role": ROLES[1],
            "source_id": "STATUS_LIQUIDATION_CURRENT",
            "temporal_mode": "RETRIEVAL_DATE_SNAPSHOT",
            "source_path": str(source_path),
            "method": "KRX_DATA_MARKETPLACE_MANUAL",
            "official_source_id": "KRX_MDCSTAT237_LIQUIDATION_CURRENT",
            "source_page_url": "https://data.krx.co.kr/contents/MDC/STAT/issue/MDCSTAT237.jsp",
            "download_url": "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
            "query_parameters": {"mktId": "ALL"},
            "date_parameter": None,
            "as_of_date": "20260915",
            "covered_fields": ["delisting_procedure_status"],
            "authenticated_session_verified": True,
            "http_status": 200,
            "http_response_timestamp": mtime,
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
            "query_end": "20260915",
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
    descriptor.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return descriptor


def _repo(name: str) -> Path:
    return contracts.STRATEGY_ROOT / "state" / f"test_c8_session_{name}_{uuid.uuid4().hex}"


def test_start_then_finalize_seals_exact_four_sources_without_opening_m2():
    repo_root = _repo("positive")
    try:
        started = start_acquisition_session(
            selection_as_of="20260914",
            repo_root=repo_root,
            generated_at="2026-09-15T07:30:00+09:00",
        )
        assert started["verdict"] == "C8_ACQUISITION_SESSION_ACTIVE"
        descriptor = _descriptor(repo_root)
        sealed = finalize_acquisition_session(
            descriptor_path=descriptor,
            repo_root=repo_root,
            generated_at="2026-09-15T07:32:00+09:00",
        )
        assert sealed["verdict"] == "C8_ACQUISITION_SESSION_SEALED"
        assert sealed["source_count"] == 4
        assert sealed["raw_snapshots_published"] is True
        assert sealed["request_published"] is True
        assert sealed["evidence_published"] is True
        assert sealed["adapter_files_generated"] is False
        assert sealed["build_request_generated"] is False
        assert sealed["canonical_generated"] is False
        assert sealed["m2_allowed"] is False
        assert all((repo_root / path).is_file() for path in sealed["outputs"]["raw_snapshots"])
        assert (repo_root / sealed["outputs"]["request_latest"]).is_file()
        assert (repo_root / sealed["outputs"]["evidence_latest"]).is_file()
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_openapi_cross_host_and_lookalike_host_block_session_before_raw_copy():
    repo_root = _repo("host")
    try:
        start_acquisition_session(
            selection_as_of="20260914",
            repo_root=repo_root,
            generated_at="2026-09-15T07:30:00+09:00",
        )
        descriptor = _descriptor(repo_root)
        payload = json.loads(descriptor.read_text(encoding="utf-8"))
        close = next(item for item in payload["sources"] if item["role"] == ROLES[3])
        close["source_page_url"] = "https://data-dbg.krx.co.kr/svc/apis/sto/stk_bydd_trd"
        close["download_url"] = "https://data-dbg.krx.co.kr.evil.example/svc/apis/sto/stk_bydd_trd"
        descriptor.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        blocked = finalize_acquisition_session(
            descriptor_path=descriptor,
            repo_root=repo_root,
            generated_at="2026-09-15T07:32:00+09:00",
        )
        close_check = next(item for item in blocked["source_checks"] if item["role"] == ROLES[3])
        assert "SOURCE_PAGE_URL_INVALID" in close_check["reason_codes"]
        assert "DOWNLOAD_URL_INVALID" in close_check["reason_codes"]
        assert blocked["raw_snapshots_published"] is False
        assert blocked["request_published"] is False
        assert blocked["evidence_published"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_file_created_before_session_is_blocked_without_request_or_evidence():
    repo_root = _repo("predates")
    try:
        start_acquisition_session(
            selection_as_of="20260914",
            repo_root=repo_root,
            generated_at="2026-09-15T07:30:00+09:00",
        )
        descriptor = _descriptor(repo_root, mtime="2026-09-15T07:29:59+09:00")
        blocked = finalize_acquisition_session(
            descriptor_path=descriptor,
            repo_root=repo_root,
            generated_at="2026-09-15T07:32:00+09:00",
        )
        assert "SOURCE_PREDATES_CAPTURE_SESSION" in blocked["reason_codes"]
        assert blocked["raw_snapshots_published"] is False
        assert blocked["request_published"] is False
        assert blocked["evidence_published"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_session_seals_multiple_status_histories_to_unique_raw_snapshots():
    repo_root = _repo("multi")
    try:
        start_acquisition_session(
            selection_as_of="20260914",
            repo_root=repo_root,
            generated_at="2026-09-15T07:30:00+09:00",
        )
        descriptor = _descriptor(repo_root)
        _add_interval_status_source(descriptor, repo_root)
        sealed = finalize_acquisition_session(
            descriptor_path=descriptor,
            repo_root=repo_root,
            generated_at="2026-09-15T07:32:00+09:00",
        )
        assert sealed["verdict"] == "C8_ACQUISITION_SESSION_SEALED"
        assert sealed["source_count"] == 5
        assert len(set(sealed["outputs"]["raw_snapshots"])) == 5
        assert sealed["evidence_published"] is True
        evidence = json.loads((repo_root / sealed["outputs"]["evidence_latest"]).read_text(encoding="utf-8"))
        status_coverage = next(item for item in evidence["role_coverage_checks"] if item["role"] == ROLES[1])
        assert status_coverage["status"] == "PASS"
        assert len(status_coverage["source_sha256s"]) == 2
        assert sealed["m2_allowed"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_secret_query_key_is_blocked_before_raw_copy():
    repo_root = _repo("secret")
    try:
        start_acquisition_session(
            selection_as_of="20260914",
            repo_root=repo_root,
            generated_at="2026-09-15T07:30:00+09:00",
        )
        descriptor = _descriptor(repo_root)
        payload = json.loads(descriptor.read_text(encoding="utf-8"))
        payload["sources"][0]["query_parameters"]["AUTH_KEY"] = "do-not-store"
        descriptor.write_text(json.dumps(payload), encoding="utf-8")
        blocked = finalize_acquisition_session(
            descriptor_path=descriptor,
            repo_root=repo_root,
            generated_at="2026-09-15T07:32:00+09:00",
        )
        assert "SECRET_MATERIAL_PRESENT" in blocked["reason_codes"]
        assert blocked["raw_snapshots_published"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_duplicate_role_and_future_session_are_blocked():
    repo_root = _repo("boundaries")
    future_root = _repo("future")
    try:
        future = start_acquisition_session(
            selection_as_of="20260916",
            repo_root=future_root,
            generated_at="2026-09-15T07:30:00+09:00",
        )
        assert "SELECTION_ASOF_IN_FUTURE" in future["reason_codes"]
        assert not (future_root / "paper/strategies/kospi_mcap_quarterly_v1/data/inbox/acquisition/c8_acquisition_session_latest.json").exists()

        start_acquisition_session(
            selection_as_of="20260914",
            repo_root=repo_root,
            generated_at="2026-09-15T07:30:00+09:00",
        )
        descriptor = _descriptor(repo_root)
        payload = json.loads(descriptor.read_text(encoding="utf-8"))
        payload["sources"][3]["role"] = payload["sources"][0]["role"]
        descriptor.write_text(json.dumps(payload), encoding="utf-8")
        blocked = finalize_acquisition_session(
            descriptor_path=descriptor,
            repo_root=repo_root,
            generated_at="2026-09-15T07:32:00+09:00",
        )
        assert "DESCRIPTOR_SOURCE_ROLES_MISSING_DUPLICATE_OR_EXTRA" in blocked["reason_codes"]
        assert blocked["request_published"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)
        shutil.rmtree(future_root, ignore_errors=True)


def test_same_day_retrieval_session_seals_evidence_but_keeps_m2_closed():
    repo_root = _repo("retrieval")
    try:
        start_acquisition_session(
            selection_as_of="20260915",
            repo_root=repo_root,
            generated_at="2026-09-15T15:59:00+09:00",
        )
        descriptor = _same_day_descriptor(repo_root)
        sealed = finalize_acquisition_session(
            descriptor_path=descriptor,
            repo_root=repo_root,
            generated_at="2026-09-15T16:02:00+09:00",
        )
        assert sealed["verdict"] == "C8_ACQUISITION_SESSION_SEALED"
        assert sealed["source_count"] == 6
        assert sealed["evidence_published"] is True
        assert sealed["m2_allowed"] is False
        evidence = json.loads((repo_root / sealed["outputs"]["evidence_latest"]).read_text(encoding="utf-8"))
        assert evidence["evidence_schema_version"] == "1.4.0"
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_before_close_retrieval_session_is_blocked_before_raw_copy():
    repo_root = _repo("retrieval_early")
    try:
        start_acquisition_session(
            selection_as_of="20260915",
            repo_root=repo_root,
            generated_at="2026-09-15T15:29:00+09:00",
        )
        descriptor = _same_day_descriptor(repo_root, mtime="2026-09-15T15:30:00+09:00")
        blocked = finalize_acquisition_session(
            descriptor_path=descriptor,
            repo_root=repo_root,
            generated_at="2026-09-15T15:32:00+09:00",
        )
        assert any(reason.endswith("_BEFORE_MINIMUM") for reason in blocked["reason_codes"])
        assert blocked["raw_snapshots_published"] is False
        assert blocked["evidence_published"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)
