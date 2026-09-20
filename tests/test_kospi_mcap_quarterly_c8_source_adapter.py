from __future__ import annotations

import csv
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
from paper.strategies.kospi_mcap_quarterly_v1.src.c8_source_adapter import (
    _validate_retrieval_transform,
    adapt_c8_sources,
    load_adapter_contract,
)
from paper.strategies.kospi_mcap_quarterly_v1.src.c8_source_builder import build_c8_source_bundle


ROLES = (
    "listing_security_as_of",
    "trade_management_status_as_of",
    "corporate_actions_as_of",
    "close_market_cap_as_of",
)
STRATEGY_RELATIVE = contracts.STRATEGY_ROOT.relative_to(contracts.REPO_ROOT)


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, str]], mtime: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    timestamp = datetime.fromisoformat(mtime).timestamp()
    os.utime(path, (timestamp, timestamp))


def _role_rows(role: str, count: int) -> tuple[list[str], list[dict[str, str]]]:
    codes = [f"{index:06d}" for index in range(1, count + 1)]
    if role == "listing_security_as_of":
        columns = ["code", "name", "market", "security_type", "listed_date", "delisted_date", "listing_status"]
        rows = [{
            "code": code, "name": f"TEST{code}", "market": "KOSPI", "security_type": "COMMON",
            "listed_date": "20200102", "delisted_date": "", "listing_status": "ACTIVE",
        } for code in codes]
    elif role == "trade_management_status_as_of":
        columns = ["code", "trade_status", "management_status", "delisting_procedure_status"]
        rows = [{
            "code": code, "trade_status": "TRADING", "management_status": "NORMAL",
            "delisting_procedure_status": "NONE",
        } for code in codes]
    elif role == "corporate_actions_as_of":
        columns = ["code", "merger_status", "event_type", "event_effective_date"]
        rows = [{
            "code": code, "merger_status": "NONE", "event_type": "NONE", "event_effective_date": "",
        } for code in codes]
    else:
        columns = ["code", "close", "market_cap"]
        rows = [{"code": code, "close": "10000", "market_cap": str(1_000_000_000 - index)} for index, code in enumerate(codes)]
    return columns, rows


def _prepare_evidence(repo_root: Path, *, short_role: str | None = None) -> dict:
    as_of = "20260914"
    started = "2026-09-15T07:40:00+09:00"
    mtime = "2026-09-15T07:41:00+09:00"
    start_acquisition_session(selection_as_of=as_of, repo_root=repo_root, generated_at=started)
    sources = []
    download_dir = repo_root / "downloads"
    for index, role in enumerate(ROLES, start=1):
        columns, rows = _role_rows(role, 99 if role == short_role else 100)
        path = download_dir / f"{role}.csv"
        _write_csv(path, columns, rows, mtime)
        sources.append(
            {
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
        )
    descriptor = repo_root / "descriptor.json"
    descriptor.write_text(
        json.dumps(
            {
                "descriptor_version": "1.0.0",
                "strategy_id": contracts.STRATEGY_ID,
                "selection_as_of": as_of,
                "sources": sources,
            }
        ),
        encoding="utf-8",
    )
    sealed = finalize_acquisition_session(
        descriptor_path=descriptor,
        repo_root=repo_root,
        generated_at="2026-09-15T07:42:00+09:00",
    )
    assert sealed["status"] == "PASS"
    return sealed


def _prepare_sparse_status_evidence(repo_root: Path) -> dict:
    as_of = "20260914"
    started = "2026-09-15T07:40:00+09:00"
    mtime = "2026-09-15T07:41:00+09:00"
    start_acquisition_session(selection_as_of=as_of, repo_root=repo_root, generated_at=started)
    sources = []
    download_dir = repo_root / "downloads"
    for index, role in enumerate((ROLES[0], ROLES[2], ROLES[3]), start=1):
        columns, rows = _role_rows(role, 102)
        path = download_dir / f"{role}.csv"
        _write_csv(path, columns, rows, mtime)
        sources.append(
            {
                "role": role,
                "source_path": str(path),
                "method": "KRX_DATA_MARKETPLACE_MANUAL",
                "official_source_id": f"MDCSTAT_FULL_{index}",
                "source_page_url": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
                "download_url": "https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd",
                "query_parameters": {"trdDd": as_of, "mktId": "STK"},
                "date_parameter": "trdDd",
                "as_of_date": as_of,
            }
        )
    sparse_sources = (
        (
            "STATUS_SUSPENSION_HISTORY",
            "MDCSTAT_STATUS_SUSPENSION",
            ["code", "effective_from", "effective_to", "trade_status"],
            [{"code": "000001", "effective_from": "20200102", "effective_to": "", "trade_status": "SUSPENDED"}],
        ),
        (
            "STATUS_MANAGEMENT_HISTORY",
            "MDCSTAT_STATUS_MANAGEMENT",
            ["code", "effective_from", "effective_to", "management_status", "delisting_procedure_status"],
            [{
                "code": "000002",
                "effective_from": "20200102",
                "effective_to": "",
                "management_status": "MANAGEMENT",
                "delisting_procedure_status": "PROCEDURE",
            }],
        ),
    )
    for source_id, official_source_id, columns, rows in sparse_sources:
        path = download_dir / f"{source_id}.csv"
        _write_csv(path, columns, rows, mtime)
        sources.append(
            {
                "role": ROLES[1],
                "source_id": source_id,
                "temporal_mode": "EXHAUSTIVE_INTERVAL_HISTORY",
                "source_path": str(path),
                "method": "KRX_DATA_MARKETPLACE_MANUAL",
                "official_source_id": official_source_id,
                "source_page_url": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
                "download_url": "https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd",
                "query_parameters": {"strtDd": "20200102", "endDd": as_of, "mktId": "STK"},
                "query_start_parameter": "strtDd",
                "query_end_parameter": "endDd",
                "date_parameter": "endDd",
                "as_of_date": as_of,
            }
        )
    descriptor = repo_root / "descriptor_sparse.json"
    descriptor.write_text(
        json.dumps(
            {
                "descriptor_version": "1.0.0",
                "strategy_id": contracts.STRATEGY_ID,
                "selection_as_of": as_of,
                "sources": sources,
                "role_coverage": [
                    {
                        "role": ROLES[1],
                        "mode": "EXHAUSTIVE_INTERVAL_HISTORY",
                        "basis": "EXHAUSTIVE_OFFICIAL_EVENT_HISTORY",
                        "scope": "ALL_BASE_UNIVERSE_CODES",
                        "query_start": "20200102",
                        "query_end": as_of,
                        "source_ids": ["STATUS_SUSPENSION_HISTORY", "STATUS_MANAGEMENT_HISTORY"],
                        "covered_fields": ["trade_status", "management_status", "delisting_procedure_status"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    sealed = finalize_acquisition_session(
        descriptor_path=descriptor,
        repo_root=repo_root,
        generated_at="2026-09-15T07:42:00+09:00",
    )
    assert sealed["status"] == "PASS"
    return sealed


def _prepare_retrieval_status_evidence(
    repo_root: Path, *, listing_security_type: str = "보통주"
) -> dict:
    as_of = "20260915"
    started = "2026-09-15T15:59:00+09:00"
    mtime = "2026-09-15T16:00:00+09:00"
    start_acquisition_session(selection_as_of=as_of, repo_root=repo_root, generated_at=started)
    download_dir = repo_root / "downloads"
    sources = []

    for index, role in enumerate((ROLES[0], ROLES[2], ROLES[3]), start=1):
        columns, rows = _role_rows(role, 103)
        if role == ROLES[0]:
            columns = ["ISU_SRT_CD", "ISU_NM", "MKT_TP_NM", "KIND_STKCERT_TP_NM", "LIST_DD"]
            rows = [
                {
                    "ISU_SRT_CD": f"{code:06d}",
                    "ISU_NM": f"TEST{code:06d}",
                    "MKT_TP_NM": "KOSPI",
                    "KIND_STKCERT_TP_NM": listing_security_type,
                    "LIST_DD": "20200102",
                }
                for code in range(1, 104)
            ]
        path = download_dir / f"{role}.csv"
        _write_csv(path, columns, rows, mtime)
        source = {
            "role": role,
            "source_path": str(path),
            "method": "KRX_DATA_MARKETPLACE_MANUAL",
            "official_source_id": f"MDCSTAT_FULL_{index}",
            "source_page_url": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
            "download_url": "https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd",
            "query_parameters": {"trdDd": as_of, "mktId": "STK"},
            "date_parameter": "trdDd",
            "as_of_date": as_of,
        }
        if role == ROLES[0]:
            source.update(
                {
                    "source_id": "LISTING_CURRENT",
                    "temporal_mode": "RETRIEVAL_DATE_SNAPSHOT",
                    "official_source_id": "KRX_MDCSTAT019_CURRENT_BASIC_INFO",
                    "query_parameters": {"mktId": "STK"},
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
        sources.append(source)

    sparse_sources = (
        (
            "STATUS_SUSPENSION_HISTORY",
            "MDCSTAT_STATUS_SUSPENSION",
            ["trade_status"],
            [{
                "code": "000001",
                "effective_from": "20200102",
                "effective_to": "",
                "trade_status": "SUSPENDED",
            }],
        ),
        (
            "STATUS_MANAGEMENT_HISTORY",
            "MDCSTAT_STATUS_MANAGEMENT",
            ["management_status"],
            [{
                "code": "000002",
                "effective_from": "20200102",
                "effective_to": "",
                "management_status": "MANAGEMENT",
            }],
        ),
    )
    for source_id, official_source_id, covered_fields, rows in sparse_sources:
        columns = ["code", "effective_from", "effective_to", *covered_fields]
        path = download_dir / f"{source_id}.csv"
        _write_csv(path, columns, rows, mtime)
        sources.append(
            {
                "role": ROLES[1],
                "source_id": source_id,
                "temporal_mode": "EXHAUSTIVE_INTERVAL_HISTORY",
                "source_path": str(path),
                "method": "KRX_DATA_MARKETPLACE_MANUAL",
                "official_source_id": official_source_id,
                "source_page_url": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
                "download_url": "https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd",
                "query_parameters": {"strtDd": "20200102", "endDd": as_of, "mktId": "STK"},
                "query_start_parameter": "strtDd",
                "query_end_parameter": "endDd",
                "date_parameter": "endDd",
                "as_of_date": as_of,
                "covered_fields": covered_fields,
            }
        )

    liquidation_path = download_dir / "STATUS_LIQUIDATION_CURRENT.csv"
    _write_csv(
        liquidation_path,
        [
            "ISU_CD",
            "ISU_NM",
            "MKT_NM",
            "SECUGRP_NM",
            "STRT_DD",
            "END_DD",
            "DELIST_SCHDL_DD",
            "DELIST_RSN_DSC",
        ],
        [{
            "ISU_CD": "000003",
            "ISU_NM": "TEST000003",
            "MKT_NM": "KOSPI",
            "SECUGRP_NM": "주권",
            "STRT_DD": "20260915",
            "END_DD": "20260919",
            "DELIST_SCHDL_DD": "20260920",
            "DELIST_RSN_DSC": "TEST",
        }],
        mtime,
    )
    sources.append(
        {
            "role": ROLES[1],
            "source_id": "STATUS_LIQUIDATION_CURRENT",
            "temporal_mode": "RETRIEVAL_DATE_SNAPSHOT",
            "source_path": str(liquidation_path),
            "method": "KRX_DATA_MARKETPLACE_MANUAL",
            "official_source_id": "KRX_MDCSTAT237_LIQUIDATION_CURRENT",
            "source_page_url": "https://data.krx.co.kr/contents/MDC/STAT/issue/MDCSTAT237.jsp",
            "download_url": "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
            "query_parameters": {"mktId": "STK"},
            "date_parameter": None,
            "as_of_date": as_of,
            "covered_fields": ["delisting_procedure_status"],
            "authenticated_session_verified": True,
            "http_status": 200,
            "http_response_timestamp": mtime,
            "historical_replay": False,
        }
    )
    descriptor = repo_root / "descriptor_retrieval.json"
    descriptor.write_text(
        json.dumps(
            {
                "descriptor_version": "1.0.0",
                "strategy_id": contracts.STRATEGY_ID,
                "selection_as_of": as_of,
                "sources": sources,
                "role_coverage": [
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
                ],
            }
        ),
        encoding="utf-8",
    )
    sealed = finalize_acquisition_session(
        descriptor_path=descriptor,
        repo_root=repo_root,
        generated_at="2026-09-15T16:02:00+09:00",
    )
    assert sealed["status"] == "PASS"
    return sealed


def _transform(role: str) -> dict:
    role_columns = {
        "listing_security_as_of": [
            "as_of_date", "code", "name", "market", "security_type", "security_type_rule",
            "listed_date", "delisted_date", "listing_status",
        ],
        "trade_management_status_as_of": [
            "as_of_date", "code", "trade_status", "management_status", "delisting_procedure_status",
        ],
        "corporate_actions_as_of": [
            "as_of_date", "code", "merger_status", "event_type", "event_effective_date",
        ],
        "close_market_cap_as_of": ["as_of_date", "code", "close", "market_cap"],
    }
    constants = {"as_of_date": "$SELECTION_AS_OF"}
    if role == "listing_security_as_of":
        constants["security_type_rule"] = "OFFICIAL_FIELD"
    column_map = {field: field for field in role_columns[role] if field not in constants}
    return {
        "role": role,
        "format": "CSV",
        "encoding": "utf-8-sig",
        "column_map": column_map,
        "constants": constants,
        "value_maps": {},
    }


def _sparse_status_transform(source_id: str, covered_fields: list[str]) -> dict:
    columns = ["code", "effective_from", "effective_to", *covered_fields]
    return {
        "role": ROLES[1],
        "source_id": source_id,
        "covered_fields": covered_fields,
        "format": "CSV",
        "encoding": "utf-8-sig",
        "column_map": {field: field for field in columns},
        "constants": {},
        "value_maps": {},
    }


def _retrieval_listing_transform() -> dict:
    return {
        "role": ROLES[0],
        "source_id": "LISTING_CURRENT",
        "covered_fields": [
            "market",
            "security_type",
            "listed_date",
            "delisted_date",
            "listing_status",
        ],
        "transform_profile_id": "MDCSTAT019_ACTIVE_LISTING_MEMBERSHIP_V1",
        "format": "CSV",
        "encoding": "utf-8-sig",
        "column_map": {
            "code": "ISU_SRT_CD",
            "name": "ISU_NM",
            "market": "MKT_TP_NM",
            "security_type": "KIND_STKCERT_TP_NM",
            "listed_date": "LIST_DD",
        },
        "constants": {
            "as_of_date": "$SELECTION_AS_OF",
            "security_type_rule": "OFFICIAL_FIELD",
            "delisted_date": "",
            "listing_status": "ACTIVE",
        },
        "value_maps": {
            "market": {"KOSPI": "KOSPI"},
            "security_type": {"보통주": "COMMON"},
        },
    }


def _retrieval_liquidation_transform() -> dict:
    return {
        "role": ROLES[1],
        "source_id": "STATUS_LIQUIDATION_CURRENT",
        "covered_fields": ["delisting_procedure_status"],
        "transform_profile_id": "MDCSTAT237_LIQUIDATION_MEMBERSHIP_V1",
        "format": "CSV",
        "encoding": "utf-8-sig",
        "column_map": {
            "code": "ISU_CD",
        },
        "constants": {
            "as_of_date": "$SELECTION_AS_OF",
            "delisting_procedure_status": "PROCEDURE",
        },
        "value_maps": {},
    }


def _adapter_request(
    repo_root: Path,
    sealed: dict,
    *,
    transforms: list[dict] | None = None,
    as_of: str = "20260914",
) -> Path:
    evidence_path = repo_root / sealed["outputs"]["evidence_latest"]
    payload = {
        "request_version": "1.0.0",
        "strategy_id": contracts.STRATEGY_ID,
        "selection_as_of": as_of,
        "evidence_path": evidence_path.relative_to(repo_root).as_posix(),
        "evidence_sha256": contracts.sha256_file(evidence_path),
        "transforms": transforms or [_transform(role) for role in ROLES],
    }
    path = repo_root / STRATEGY_RELATIVE / "data" / "inbox" / "acquisition" / "c8_adapter_request_latest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _repo(name: str) -> Path:
    return contracts.STRATEGY_ROOT / "state" / f"test_c8_source_adapter_{name}_{uuid.uuid4().hex}"


def test_evidence_backed_adapters_generate_build_request_but_not_canonical():
    repo_root = _repo("positive")
    try:
        sealed = _prepare_evidence(repo_root)
        request = _adapter_request(repo_root, sealed)
        report = adapt_c8_sources(
            request_path=request,
            repo_root=repo_root,
            generated_at="2026-09-15T07:43:00+09:00",
        )
        assert report["verdict"] == "C8_SOURCE_ADAPTER_READY"
        assert report["evidence_revalidated"] is True
        assert report["adapter_files_generated"] is True
        assert report["build_request_generated"] is True
        assert report["canonical_generated"] is False
        assert report["m2_allowed"] is False
        assert report["code_count"] == 100
        built = build_c8_source_bundle(
            request_path=repo_root / report["outputs"]["build_request_latest"],
            repo_root=repo_root,
            generated_at="2026-09-15T07:44:00+09:00",
        )
        assert built["status"] == "PASS"
        assert built["published"] is True
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_missing_evidence_blocks_before_component_publication():
    repo_root = _repo("missing")
    try:
        request = repo_root / STRATEGY_RELATIVE / "data" / "inbox" / "acquisition" / "c8_adapter_request_latest.json"
        request.parent.mkdir(parents=True, exist_ok=True)
        request.write_text(json.dumps({
            "request_version": "1.0.0", "strategy_id": contracts.STRATEGY_ID,
            "selection_as_of": "20260914", "evidence_path": "missing.json",
            "evidence_sha256": "0" * 64, "transforms": [_transform(role) for role in ROLES],
        }), encoding="utf-8")
        report = adapt_c8_sources(request_path=request, repo_root=repo_root)
        assert "ACQUISITION_EVIDENCE_MISSING" in report["reason_codes"]
        assert report["adapter_files_generated"] is False
        assert report["build_request_generated"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_sparse_multisource_status_is_reconstructed_before_component_publication():
    repo_root = _repo("sparse")
    try:
        sealed = _prepare_sparse_status_evidence(repo_root)
        transforms = [
            _transform(ROLES[0]),
            _sparse_status_transform("STATUS_SUSPENSION_HISTORY", ["trade_status"]),
            _sparse_status_transform(
                "STATUS_MANAGEMENT_HISTORY",
                ["management_status", "delisting_procedure_status"],
            ),
            _transform(ROLES[2]),
            _transform(ROLES[3]),
        ]
        request = _adapter_request(repo_root, sealed, transforms=transforms)
        report = adapt_c8_sources(
            request_path=request,
            repo_root=repo_root,
            generated_at="2026-09-15T07:43:00+09:00",
        )
        assert report["verdict"] == "C8_SOURCE_ADAPTER_READY"
        assert report["state_reconstruction"]["verdict"] == "C8_STATE_RECONSTRUCTION_READY"
        assert report["code_count"] == 102
        status_path = next(
            repo_root / path
            for path in report["outputs"]["components"]
            if "trade_management_status_as_of" in path
        )
        with status_path.open("r", encoding="utf-8", newline="") as handle:
            status_rows = {row["code"]: row for row in csv.DictReader(handle)}
        assert status_rows["000001"] == {
            "as_of_date": "20260914",
            "code": "000001",
            "trade_status": "SUSPENDED",
            "management_status": "NORMAL",
            "delisting_procedure_status": "NONE",
        }
        assert status_rows["000002"]["trade_status"] == "TRADING"
        assert status_rows["000002"]["management_status"] == "MANAGEMENT"
        assert status_rows["000002"]["delisting_procedure_status"] == "PROCEDURE"
        build_request = json.loads(
            (repo_root / report["outputs"]["build_request_latest"]).read_text(encoding="utf-8")
        )
        status_component = next(item for item in build_request["components"] if item["role"] == ROLES[1])
        assert status_component["official_source_ids"] == [
            "MDCSTAT_STATUS_SUSPENSION",
            "MDCSTAT_STATUS_MANAGEMENT",
        ]
        assert len(status_component["source_sha256s"]) == 2
        built = build_c8_source_bundle(
            request_path=repo_root / report["outputs"]["build_request_latest"],
            repo_root=repo_root,
            generated_at="2026-09-15T07:44:00+09:00",
        )
        assert built["status"] == "PASS"
        assert report["m2_allowed"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_sparse_transform_field_union_must_match_evidence_coverage():
    repo_root = _repo("sparse_coverage")
    try:
        sealed = _prepare_sparse_status_evidence(repo_root)
        transforms = [
            _transform(ROLES[0]),
            _sparse_status_transform("STATUS_SUSPENSION_HISTORY", ["trade_status"]),
            _sparse_status_transform("STATUS_MANAGEMENT_HISTORY", ["management_status"]),
            _transform(ROLES[2]),
            _transform(ROLES[3]),
        ]
        request = _adapter_request(repo_root, sealed, transforms=transforms)
        report = adapt_c8_sources(request_path=request, repo_root=repo_root)
        assert "SPARSE_TRANSFORM_COVERAGE_MISMATCH" in report["reason_codes"]
        assert report["adapter_files_generated"] is False
        assert report["build_request_generated"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_multisource_role_without_explicit_transform_source_ids_is_blocked():
    repo_root = _repo("source_identity")
    try:
        sealed = _prepare_sparse_status_evidence(repo_root)
        transforms = [
            _transform(ROLES[0]),
            _transform(ROLES[1]),
            _transform(ROLES[1]),
            _transform(ROLES[2]),
            _transform(ROLES[3]),
        ]
        request = _adapter_request(repo_root, sealed, transforms=transforms)
        report = adapt_c8_sources(request_path=request, repo_root=repo_root)
        assert "TRANSFORM_SOURCE_MAPPING_MISSING_DUPLICATE_OR_EXTRA" in report["reason_codes"]
        assert report["adapter_files_generated"] is False
        assert report["build_request_generated"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_raw_tamper_after_evidence_is_blocked_by_revalidation():
    repo_root = _repo("tamper")
    try:
        sealed = _prepare_evidence(repo_root)
        request = _adapter_request(repo_root, sealed)
        evidence = json.loads((repo_root / sealed["outputs"]["evidence_latest"]).read_text(encoding="utf-8"))
        raw = repo_root / evidence["source_checks"][0]["raw_path"]
        raw.write_bytes(raw.read_bytes() + b"tampered\n")
        report = adapt_c8_sources(request_path=request, repo_root=repo_root)
        assert "ACQUISITION_EVIDENCE_REVALIDATION_FAILED" in report["reason_codes"]
        assert report["adapter_files_generated"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_mismatched_role_code_sets_do_not_publish_build_request():
    repo_root = _repo("codes")
    try:
        sealed = _prepare_evidence(repo_root, short_role="corporate_actions_as_of")
        request = _adapter_request(repo_root, sealed)
        report = adapt_c8_sources(request_path=request, repo_root=repo_root)
        assert "TRANSFORMED_CODE_SET_MISMATCH" in report["reason_codes"]
        assert report["evidence_revalidated"] is True
        assert report["adapter_files_generated"] is False
        assert report["build_request_generated"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_retrieval_sources_pass_approved_mixed_reconstruction_with_lineage():
    repo_root = _repo("retrieval")
    try:
        sealed = _prepare_retrieval_status_evidence(repo_root)
        transforms = [
            _retrieval_listing_transform(),
            _sparse_status_transform("STATUS_SUSPENSION_HISTORY", ["trade_status"]),
            _sparse_status_transform("STATUS_MANAGEMENT_HISTORY", ["management_status"]),
            _retrieval_liquidation_transform(),
            _transform(ROLES[2]),
            _transform(ROLES[3]),
        ]
        request = _adapter_request(repo_root, sealed, transforms=transforms, as_of="20260915")
        report = adapt_c8_sources(
            request_path=request,
            repo_root=repo_root,
            generated_at="2026-09-15T16:03:00+09:00",
        )
        assert report["verdict"] == "C8_SOURCE_ADAPTER_READY"
        assert report["state_reconstruction"]["verdict"] == "C8_STATE_RECONSTRUCTION_READY"
        assert report["evidence_revalidated"] is True
        assert all(item["status"] == "PASS" for item in report["role_checks"])
        retrieval_checks = [
            item for item in report["role_checks"]
            if item["temporal_mode"] == "RETRIEVAL_DATE_SNAPSHOT"
        ]
        assert {item["retrieval_shape"] for item in retrieval_checks} == {
            "FULL_ROLE_SNAPSHOT",
            "POINT_IN_TIME_EXCEPTIONS",
        }
        assert report["adapter_files_generated"] is True
        assert report["build_request_generated"] is True
        assert report["m2_allowed"] is False
        build_request_path = repo_root / report["outputs"]["build_request_latest"]
        build_request = json.loads(build_request_path.read_text(encoding="utf-8"))
        listing_component = next(item for item in build_request["components"] if item["role"] == ROLES[0])
        status_component = next(item for item in build_request["components"] if item["role"] == ROLES[1])
        with (repo_root / listing_component["path"]).open(encoding="utf-8", newline="") as handle:
            listing_rows = {row["code"]: row for row in csv.DictReader(handle)}
        with (repo_root / status_component["path"]).open(encoding="utf-8", newline="") as handle:
            status_rows = {row["code"]: row for row in csv.DictReader(handle)}
        assert listing_rows["000001"]["listing_status"] == "ACTIVE"
        assert listing_rows["000001"]["security_type"] == "COMMON"
        assert status_rows["000003"]["delisting_procedure_status"] == "PROCEDURE"
        assert status_rows["000004"]["delisting_procedure_status"] == "NONE"
        assert listing_component["date_authority"] == "APPROVED_SAME_DAY_RETRIEVAL_TIMESTAMP"
        assert status_component["date_authority"] == "MIXED_VERIFIED_SOURCE_AUTHORITIES"
        assert [item["temporal_mode"] for item in status_component["source_date_authorities"]] == [
            "EXHAUSTIVE_INTERVAL_HISTORY",
            "EXHAUSTIVE_INTERVAL_HISTORY",
            "RETRIEVAL_DATE_SNAPSHOT",
        ]
        built = build_c8_source_bundle(
            request_path=build_request_path,
            repo_root=repo_root,
            generated_at="2026-09-15T16:04:00+09:00",
        )
        assert built["status"] == "PASS", (
            built.get("reason_codes"),
            built.get("canonical_validation"),
        )
        assert built["m2_allowed"] is True
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_retrieval_transform_rejects_unapproved_source_and_field_claim():
    adapter_contract = load_adapter_contract()
    source = {
        "role": ROLES[1],
        "official_source_id": "KRX_UNAPPROVED_CURRENT",
        "covered_fields": ["trade_status"],
        "retrieval_date_authority": {
            "approved_retrieval_source": True,
            "authenticated_session_verified": True,
            "http_status": 200,
            "historical_replay": False,
        },
    }
    transform = {
        "source_id": "STATUS_CURRENT",
        "covered_fields": ["trade_status"],
    }
    _, reasons = _validate_retrieval_transform(
        source=source,
        transform=transform,
        adapter_contract=adapter_contract,
    )
    assert reasons == ["RETRIEVAL_TRANSFORM_SOURCE_NOT_APPROVED"]

    source["official_source_id"] = "KRX_MDCSTAT237_LIQUIDATION_CURRENT"
    _, reasons = _validate_retrieval_transform(
        source=source,
        transform=transform,
        adapter_contract=adapter_contract,
    )
    assert "RETRIEVAL_EVIDENCE_COVERED_FIELDS_MISMATCH" in reasons
    assert "RETRIEVAL_TRANSFORM_COVERED_FIELDS_MISMATCH" in reasons


def test_retrieval_transform_blocks_unmapped_official_listing_value():
    repo_root = _repo("retrieval_unknown")
    try:
        sealed = _prepare_retrieval_status_evidence(
            repo_root, listing_security_type="확인되지않은종류"
        )
        transforms = [
            _retrieval_listing_transform(),
            _sparse_status_transform("STATUS_SUSPENSION_HISTORY", ["trade_status"]),
            _sparse_status_transform("STATUS_MANAGEMENT_HISTORY", ["management_status"]),
            _retrieval_liquidation_transform(),
            _transform(ROLES[2]),
            _transform(ROLES[3]),
        ]
        request = _adapter_request(repo_root, sealed, transforms=transforms, as_of="20260915")
        report = adapt_c8_sources(request_path=request, repo_root=repo_root)
        assert "UNMAPPED_SOURCE_VALUES" in report["reason_codes"]
        assert "TRANSFORMED_ROWS_INVALID" in report["reason_codes"]
        assert report["adapter_files_generated"] is False
        assert report["build_request_generated"] is False
        assert report["m2_allowed"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_retrieval_transform_blocks_membership_semantics_tamper():
    repo_root = _repo("retrieval_profile_tamper")
    try:
        sealed = _prepare_retrieval_status_evidence(repo_root)
        listing_transform = _retrieval_listing_transform()
        listing_transform["constants"]["listing_status"] = "DELISTED"
        transforms = [
            listing_transform,
            _sparse_status_transform("STATUS_SUSPENSION_HISTORY", ["trade_status"]),
            _sparse_status_transform("STATUS_MANAGEMENT_HISTORY", ["management_status"]),
            _retrieval_liquidation_transform(),
            _transform(ROLES[2]),
            _transform(ROLES[3]),
        ]
        request = _adapter_request(repo_root, sealed, transforms=transforms, as_of="20260915")
        report = adapt_c8_sources(request_path=request, repo_root=repo_root)
        assert "RETRIEVAL_TRANSFORM_CONSTANTS_MISMATCH" in report["reason_codes"]
        assert report["adapter_files_generated"] is False
        assert report["build_request_generated"] is False
        assert report["m2_allowed"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)
