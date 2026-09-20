from __future__ import annotations

import csv
import json
import shutil
import uuid
from pathlib import Path

from paper.strategies.kospi_mcap_quarterly_v1.src import contracts
from paper.strategies.kospi_mcap_quarterly_v1.src.c8_source_audit import build_report, load_c8_source_contract
from paper.strategies.kospi_mcap_quarterly_v1.src.c8_source_builder import (
    build_c8_source_bundle,
    write_build_report,
)


STRATEGY_RELATIVE = Path("paper") / "strategies" / "kospi_mcap_quarterly_v1"


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _build_request(
    repo_root: Path,
    *,
    as_of: str = "20260930",
    invalid_authority_role: str = "",
    short_role: str = "",
) -> Path:
    contract = load_c8_source_contract()
    request_contract = contract["builder_request_contract"]
    component_dir = repo_root / STRATEGY_RELATIVE / "data" / "inbox" / "components"
    codes = [f"{index:06d}" for index in range(100)]
    components = []
    for role, fieldnames in request_contract["role_required_columns"].items():
        role_codes = codes[:-1] if role == short_role else codes
        rows: list[dict[str, str]] = []
        for index, code in enumerate(role_codes):
            base = {"as_of_date": as_of, "code": code}
            if role == "listing_security_as_of":
                base.update(
                    {
                        "name": f"TEST{index}",
                        "market": "KOSPI",
                        "security_type": "COMMON",
                        "security_type_rule": "OFFICIAL_FIELD",
                        "listed_date": "20000101",
                        "delisted_date": "",
                        "listing_status": "ACTIVE",
                    }
                )
            elif role == "trade_management_status_as_of":
                base.update(
                    {
                        "trade_status": "TRADING",
                        "management_status": "NORMAL",
                        "delisting_procedure_status": "NONE",
                    }
                )
            elif role == "corporate_actions_as_of":
                base.update({"merger_status": "NONE", "event_type": "NONE", "event_effective_date": ""})
            elif role == "close_market_cap_as_of":
                base.update({"close": "10000", "market_cap": str(100000000000 + index)})
            rows.append(base)
        component_path = component_dir / f"{role}_{as_of}.csv"
        _write_csv(component_path, list(fieldnames), rows)
        components.append(
            {
                "role": role,
                "path": component_path.relative_to(repo_root).as_posix(),
                "sha256": contracts.sha256_file(component_path),
                "official_source_id": f"KRX_TEST_{role.upper()}",
                "as_of_date": as_of,
                "date_authority": (
                    "FILENAME_ONLY_UNVERIFIED_ASOF"
                    if role == invalid_authority_role
                    else "OFFICIAL_QUERY_PARAMETER"
                ),
            }
        )
    request_path = repo_root / STRATEGY_RELATIVE / "data" / "inbox" / "c8_build_request_latest.json"
    request_path.write_text(
        json.dumps(
            {
                "request_version": request_contract["request_version"],
                "strategy_id": contracts.STRATEGY_ID,
                "selection_as_of": as_of,
                "components": components,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return request_path


def _set_mixed_status_authority_lineage(request_path: Path, *, tamper: str = "") -> None:
    payload = json.loads(request_path.read_text(encoding="utf-8"))
    component = next(
        item for item in payload["components"]
        if item["role"] == "trade_management_status_as_of"
    )
    as_of = payload["selection_as_of"]
    component.update(
        {
            "official_source_id": "KRX_STATUS_HISTORY+KRX_MDCSTAT237_LIQUIDATION_CURRENT",
            "official_source_ids": ["KRX_STATUS_HISTORY", "KRX_MDCSTAT237_LIQUIDATION_CURRENT"],
            "source_sha256s": ["a" * 64, "b" * 64],
            "date_authority": "MIXED_VERIFIED_SOURCE_AUTHORITIES",
            "source_date_authorities": [
                {
                    "source_id": "STATUS_HISTORY",
                    "official_source_id": "KRX_STATUS_HISTORY",
                    "temporal_mode": "EXHAUSTIVE_INTERVAL_HISTORY",
                    "date_authority": "OFFICIAL_QUERY_PARAMETER",
                    "as_of_date": as_of,
                    "raw_sha256": "a" * 64,
                    "query_as_of": None,
                    "query_start": "20000101",
                    "query_end": as_of,
                    "retrieval_date_authority": None,
                },
                {
                    "source_id": "LIQUIDATION_CURRENT",
                    "official_source_id": "KRX_MDCSTAT237_LIQUIDATION_CURRENT",
                    "temporal_mode": "RETRIEVAL_DATE_SNAPSHOT",
                    "date_authority": "APPROVED_SAME_DAY_RETRIEVAL_TIMESTAMP",
                    "as_of_date": as_of,
                    "raw_sha256": "b" * 64,
                    "query_as_of": None,
                    "query_start": None,
                    "query_end": None,
                    "retrieval_date_authority": {
                        "approved_retrieval_source": True,
                        "authenticated_session_verified": True,
                        "http_status": 200,
                        "http_response_timestamp": "2026-09-30T16:00:00+09:00",
                        "historical_replay": False,
                    },
                },
            ],
        }
    )
    if tamper == "retrieval_evidence":
        component["source_date_authorities"][1]["retrieval_date_authority"]["http_status"] = 500
    elif tamper == "source_hash_summary":
        component["source_sha256s"][1] = "c" * 64
    elif tamper == "retrieval_role":
        wrong_id = "KRX_MDCSTAT019_CURRENT_BASIC_INFO"
        component["official_source_id"] = f"KRX_STATUS_HISTORY+{wrong_id}"
        component["official_source_ids"][1] = wrong_id
        component["source_date_authorities"][1]["official_source_id"] = wrong_id
    elif tamper == "retrieval_timezone":
        component["source_date_authorities"][1]["retrieval_date_authority"][
            "http_response_timestamp"
        ] = "2026-09-30T16:00:00"
    request_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_valid_four_component_request_publishes_canonical_latest():
    repo_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_builder_{uuid.uuid4().hex}"
    try:
        request = _build_request(repo_root)
        report = build_c8_source_bundle(
            request_path=request,
            repo_root=repo_root,
            generated_at="2026-09-15T07:00:00+09:00",
        )
        assert report["status"] == "PASS"
        assert report["verdict"] == "C8_SOURCE_BUILD_READY"
        assert report["published"] is True
        assert report["m2_allowed"] is True
        assert report["candidate_selection_calculated"] is False
        assert report["orders_generated"] is False
        latest = repo_root / report["outputs"]["canonical_latest"]
        assert latest.is_file()
        audit = build_report(
            repo_root=repo_root,
            canonical_source=latest,
            selection_as_of="20260930",
            generated_at="2026-09-15T07:00:00+09:00",
        )
        assert audit["verdict"] == "C8_UNIVERSE_SOURCE_READY"
        assert audit["canonical_source_validation"]["eligible_rows"] == 100
        versioned_report, latest_report = write_build_report(report, repo_root=repo_root)
        assert versioned_report.is_file()
        assert latest_report.is_file()
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_filename_only_component_authority_does_not_publish():
    repo_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_builder_auth_{uuid.uuid4().hex}"
    try:
        request = _build_request(repo_root, invalid_authority_role="trade_management_status_as_of")
        report = build_c8_source_bundle(
            request_path=request,
            repo_root=repo_root,
            generated_at="2026-09-15T07:00:00+09:00",
        )
        assert "COMPONENT_DATE_AUTHORITY_INVALID" in report["reason_codes"]
        assert report["published"] is False
        assert report["m2_allowed"] is False
        assert report["outputs"] == {}
        assert not (repo_root / STRATEGY_RELATIVE / "data" / "source" / "c8_universe_source_latest.csv").exists()
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_component_code_set_mismatch_does_not_publish():
    repo_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_builder_codes_{uuid.uuid4().hex}"
    try:
        request = _build_request(repo_root, short_role="close_market_cap_as_of")
        report = build_c8_source_bundle(
            request_path=request,
            repo_root=repo_root,
            generated_at="2026-09-15T07:00:00+09:00",
        )
        assert "COMPONENT_CODE_SET_MISMATCH" in report["reason_codes"]
        assert report["published"] is False
        assert report["m2_allowed"] is False
        assert report["outputs"] == {}
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_mixed_source_date_authority_lineage_is_preserved_in_manifest():
    repo_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_builder_mixed_{uuid.uuid4().hex}"
    try:
        request = _build_request(repo_root)
        _set_mixed_status_authority_lineage(request)
        report = build_c8_source_bundle(
            request_path=request,
            repo_root=repo_root,
            generated_at="2026-09-30T16:05:00+09:00",
        )
        assert report["status"] == "PASS"
        status_check = next(
            item for item in report["component_checks"]
            if item["role"] == "trade_management_status_as_of"
        )
        assert len(status_check["source_date_authorities"]) == 2
        manifest = json.loads((repo_root / report["outputs"]["manifest"]).read_text(encoding="utf-8"))
        status_manifest = next(
            item for item in manifest["source_components"]
            if item["role"] == "trade_management_status_as_of"
        )
        assert status_manifest["date_authority"] == "MIXED_VERIFIED_SOURCE_AUTHORITIES"
        assert len(status_manifest["source_date_authorities"]) == 2
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_invalid_retrieval_authority_lineage_does_not_publish():
    repo_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_builder_mixed_bad_{uuid.uuid4().hex}"
    try:
        request = _build_request(repo_root)
        _set_mixed_status_authority_lineage(request, tamper="retrieval_evidence")
        report = build_c8_source_bundle(request_path=request, repo_root=repo_root)
        assert "SOURCE_DATE_AUTHORITY_RETRIEVAL_EVIDENCE_INVALID" in report["reason_codes"]
        assert report["published"] is False
        assert report["m2_allowed"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_source_hash_lineage_mismatch_does_not_publish():
    repo_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_builder_mixed_hash_{uuid.uuid4().hex}"
    try:
        request = _build_request(repo_root)
        _set_mixed_status_authority_lineage(request, tamper="source_hash_summary")
        report = build_c8_source_bundle(request_path=request, repo_root=repo_root)
        assert "COMPONENT_SOURCE_HASHES_LINEAGE_MISMATCH" in report["reason_codes"]
        assert report["published"] is False
        assert report["m2_allowed"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_approved_retrieval_source_for_wrong_role_does_not_publish():
    repo_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_builder_mixed_role_{uuid.uuid4().hex}"
    try:
        request = _build_request(repo_root)
        _set_mixed_status_authority_lineage(request, tamper="retrieval_role")
        report = build_c8_source_bundle(request_path=request, repo_root=repo_root)
        assert "SOURCE_DATE_AUTHORITY_RETRIEVAL_ROLE_MISMATCH" in report["reason_codes"]
        assert report["published"] is False
        assert report["m2_allowed"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)


def test_retrieval_timestamp_without_timezone_does_not_publish():
    repo_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_builder_mixed_tz_{uuid.uuid4().hex}"
    try:
        request = _build_request(repo_root)
        _set_mixed_status_authority_lineage(request, tamper="retrieval_timezone")
        report = build_c8_source_bundle(request_path=request, repo_root=repo_root)
        assert "SOURCE_DATE_AUTHORITY_RETRIEVAL_TIMESTAMP_INVALID" in report["reason_codes"]
        assert report["published"] is False
        assert report["m2_allowed"] is False
    finally:
        shutil.rmtree(repo_root, ignore_errors=True)
