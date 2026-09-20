from __future__ import annotations

import hashlib
import json
import zipfile
from io import BytesIO
from pathlib import Path

import pytest

from tools import validate_kospi_mcap_quarterly_c8_dart_viewer_fallback as validator


RECEIPT = "20251024000420"


def _document(root: Path, index: int, dcm_no: str, ele_id: str, payload: bytes) -> dict:
    path = root / f"raw/document_{index:03d}.html"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return {
        "bytes": len(payload),
        "http_status": 200,
        "index": index,
        "kind": "VIEWER_DOCUMENT",
        "path": path.relative_to(root).as_posix(),
        "request": {
            "rcpNo": RECEIPT,
            "dcmNo": dcm_no,
            "eleId": ele_id,
        },
        "sha256": hashlib.sha256(payload).hexdigest(),
    }


def _journal(root: Path) -> dict:
    first = _document(root, 2, "200", "3", b"<html>second</html>")
    second = _document(root, 1, "100", "1", b"<html>first</html>")
    landing = root / "raw/landing.html"
    landing.write_bytes(b"landing-must-not-enter-package")
    landing_record = {
        "kind": "LANDING",
        "path": landing.relative_to(root).as_posix(),
        "bytes": landing.stat().st_size,
        "http_status": 200,
        "sha256": hashlib.sha256(landing.read_bytes()).hexdigest(),
    }
    return {
        "receipt_no": RECEIPT,
        "status": "SHADOW_PASS",
        "landing": landing_record,
        "documents": [first, second],
        "artifacts": [
            landing_record,
            first,
            second,
        ],
    }


def _contract(root: Path) -> Path:
    path = root / "contract.json"
    path.write_text(
        json.dumps(
            {
                "strategy_id": validator.STRATEGY_ID,
                "mode": "VALIDATION_ONLY",
                "output": {
                    "latest_report_path": "reports/latest.json",
                    "versioned_report_prefix": "validation_",
                },
                "execution_permissions": {
                    "network_allowed": False,
                    "capture_status_change_allowed": False,
                },
            }
        ),
        encoding="utf-8",
    )
    return path


def test_deterministic_package_is_byte_stable_and_sorted(tmp_path: Path):
    journal = _journal(tmp_path)
    first, evidence_a = validator.build_deterministic_viewer_package(
        root=tmp_path, receipt=RECEIPT, shadow_journal=journal
    )
    second, evidence_b = validator.build_deterministic_viewer_package(
        root=tmp_path, receipt=RECEIPT, shadow_journal=journal
    )
    assert first == second
    assert evidence_a["package_sha256"] == evidence_b["package_sha256"]
    with zipfile.ZipFile(BytesIO(first)) as archive:
        assert archive.namelist() == ["001_100_1.html", "002_200_3.html"]
        assert archive.read("001_100_1.html") == b"<html>first</html>"


def test_landing_artifact_is_not_included(tmp_path: Path):
    package, evidence = validator.build_deterministic_viewer_package(
        root=tmp_path, receipt=RECEIPT, shadow_journal=_journal(tmp_path)
    )
    assert b"landing-must-not-enter-package" not in package
    assert evidence["landing_artifacts_included"] is False
    assert evidence["member_count"] == 2


def test_tampered_document_is_rejected(tmp_path: Path):
    journal = _journal(tmp_path)
    (tmp_path / journal["documents"][0]["path"]).write_bytes(b"tampered")
    with pytest.raises(validator.FallbackValidationError, match="RAW_ARTIFACT_LENGTH_MISMATCH"):
        validator.build_deterministic_viewer_package(
            root=tmp_path, receipt=RECEIPT, shadow_journal=journal
        )


def test_document_receipt_mismatch_is_rejected(tmp_path: Path):
    journal = _journal(tmp_path)
    journal["documents"][0]["request"]["rcpNo"] = "20250101000001"
    with pytest.raises(validator.FallbackValidationError, match="SHADOW_DOCUMENT_RECEIPT_MISMATCH"):
        validator.build_deterministic_viewer_package(
            root=tmp_path, receipt=RECEIPT, shadow_journal=journal
        )


def test_completion_authority_context_loads_hash_verified_viewer_landing(tmp_path: Path):
    journal_dir = tmp_path / "journals"
    journal_dir.mkdir()
    journal = _journal(tmp_path)
    (journal_dir / f"{RECEIPT}.json").write_text(
        json.dumps(journal), encoding="utf-8"
    )
    source_contexts, landings, evidence = validator._load_completion_authority_contexts(
        root=tmp_path,
        contract={
            "input": {
                "shadow_pass_journal_dir": "journals",
                "required_shadow_status": "SHADOW_PASS",
            }
        },
        status_by_receipt={"20250101000001": "PASS", RECEIPT: "UNAVAILABLE"},
    )
    assert source_contexts == {
        "20250101000001": "OPENDART_PASS",
        RECEIPT: "VIEWER_FALLBACK",
    }
    assert landings[RECEIPT] == b"landing-must-not-enter-package"
    assert evidence["completion_source_context_count"] == 2
    assert evidence["viewer_landing_context_count"] == 1


def test_completion_authority_context_rejects_tampered_viewer_landing(tmp_path: Path):
    journal_dir = tmp_path / "journals"
    journal_dir.mkdir()
    journal = _journal(tmp_path)
    (journal_dir / f"{RECEIPT}.json").write_text(
        json.dumps(journal), encoding="utf-8"
    )
    (tmp_path / journal["landing"]["path"]).write_bytes(b"tampered")
    with pytest.raises(validator.FallbackValidationError, match="RAW_ARTIFACT_LENGTH_MISMATCH"):
        validator._load_completion_authority_contexts(
            root=tmp_path,
            contract={
                "input": {
                    "shadow_pass_journal_dir": "journals",
                    "required_shadow_status": "SHADOW_PASS",
                }
            },
            status_by_receipt={RECEIPT: "UNAVAILABLE"},
        )


def test_report_write_is_explicit_and_writes_versioned_and_latest(tmp_path: Path):
    contract = _contract(tmp_path)
    report = {
        "generated_at": "2026-09-16T10:11:12+09:00",
        "status": "FAIL",
        "policy_change_allowed": False,
    }
    assert not (tmp_path / "reports").exists()
    versioned, latest = validator.write_validation_report(
        report, root=tmp_path, contract_path=contract
    )
    assert versioned.name == "validation_20260916101112.json"
    assert latest == tmp_path / "reports/latest.json"
    assert versioned.read_bytes() == latest.read_bytes()


def test_contract_rejects_any_enabled_permission(tmp_path: Path):
    contract = _contract(tmp_path)
    payload = json.loads(contract.read_text(encoding="utf-8"))
    payload["execution_permissions"]["network_allowed"] = True
    contract.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(
        validator.FallbackValidationError, match="FALLBACK_VALIDATION_PERMISSIONS_INVALID"
    ):
        validator.load_validation_contract(tmp_path, contract)
