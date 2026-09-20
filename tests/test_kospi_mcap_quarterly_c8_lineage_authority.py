from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools import analyze_kospi_mcap_quarterly_c8_lineage_authority as analyzer


def _classify(**overrides):
    values = {
        "source": "OPENDART_PASS",
        "event_kind": "REVISION",
        "direct_initial_receipts": [],
        "prior_initial_receipts": [],
    }
    values.update(overrides)
    return analyzer.classify_lineage_authority(**values)


def _contract(root: Path) -> Path:
    path = root / "contract.json"
    path.write_text(
        json.dumps(
            {
                "strategy_id": analyzer.STRATEGY_ID,
                "mode": "VALIDATION_ONLY",
                "authority_rules": {
                    "list_history_is_authority": False,
                    "completion_family_establishes_initial_decision_lineage": False,
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


def test_document_explicit_reference_is_authority():
    result = _classify(direct_initial_receipts=["20260102000001"])
    assert result == {
        "classification": "DOCUMENT_EXPLICIT_INITIAL_RECEIPT",
        "authority_resolved": True,
        "initial_receipt": "20260102000001",
    }


def test_viewer_official_family_resolves_revision_to_initial_decision():
    result = _classify(
        source="VIEWER_FALLBACK",
        viewer_family_root_receipt="20260102000001",
        viewer_family_root_is_initial_decision=True,
        viewer_family_valid=True,
    )
    assert result["classification"] == "DART_VIEWER_OFFICIAL_FAMILY"
    assert result["authority_resolved"] is True


def test_opendart_official_family_resolves_revision_to_initial_decision():
    result = _classify(
        source="OPENDART_PASS",
        opendart_family_initial_receipt="20260102000001",
        opendart_family_valid=True,
    )
    assert result["classification"] == "DART_OPENDART_OFFICIAL_FAMILY"
    assert result["authority_resolved"] is True


def test_opendart_completion_family_remains_blocked():
    result = _classify(
        source="OPENDART_PASS",
        event_kind="COMPLETION",
        opendart_family_initial_receipt="20260102000001",
        opendart_family_valid=True,
        prior_initial_receipts=["20251201000001"],
    )
    assert result["classification"] == "LIST_HISTORY_UNIQUE_INFERENCE_ONLY"
    assert result["authority_resolved"] is False


def test_completion_family_does_not_replace_initial_decision_lineage():
    result = _classify(
        source="VIEWER_FALLBACK",
        event_kind="COMPLETION",
        viewer_family_root_receipt="20260102000001",
        viewer_family_root_is_initial_decision=True,
        viewer_family_valid=True,
        prior_initial_receipts=["20251201000001"],
    )
    assert result["classification"] == "LIST_HISTORY_UNIQUE_INFERENCE_ONLY"
    assert result["authority_resolved"] is False


def test_viewer_family_root_that_is_itself_revision_fails_closed():
    result = _classify(
        source="VIEWER_FALLBACK",
        viewer_family_root_receipt="20260102000001",
        viewer_family_root_is_initial_decision=False,
        viewer_family_valid=True,
        prior_initial_receipts=["20251201000001"],
    )
    assert result["classification"] == "LIST_HISTORY_UNIQUE_INFERENCE_ONLY"
    assert result["authority_resolved"] is False


def test_unique_list_history_candidate_remains_inference_only():
    result = _classify(prior_initial_receipts=["20260102000001"])
    assert result["classification"] == "LIST_HISTORY_UNIQUE_INFERENCE_ONLY"
    assert result["authority_resolved"] is False
    assert result["initial_receipt"] == ""


def test_multiple_same_day_initials_fail_closed():
    result = _classify(
        prior_initial_receipts=["20260102000001", "20260102000002"]
    )
    assert result["classification"] == "LIST_HISTORY_AMBIGUOUS_MULTIPLE"
    assert result["authority_resolved"] is False


def test_contract_rejects_enabled_permission(tmp_path: Path):
    contract = _contract(tmp_path)
    payload = json.loads(contract.read_text(encoding="utf-8"))
    payload["execution_permissions"]["network_allowed"] = True
    contract.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(
        analyzer.LineageAuthorityError,
        match="LINEAGE_AUTHORITY_PERMISSIONS_INVALID",
    ):
        analyzer.load_lineage_authority_contract(tmp_path, contract)


def test_contract_rejects_list_history_as_authority(tmp_path: Path):
    contract = _contract(tmp_path)
    payload = json.loads(contract.read_text(encoding="utf-8"))
    payload["authority_rules"]["list_history_is_authority"] = True
    contract.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(
        analyzer.LineageAuthorityError,
        match="LIST_HISTORY_AUTHORITY_FORBIDDEN",
    ):
        analyzer.load_lineage_authority_contract(tmp_path, contract)
