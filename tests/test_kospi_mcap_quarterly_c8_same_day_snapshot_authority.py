from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src import contracts
from paper.strategies.kospi_mcap_quarterly_v1.src.c8_same_day_snapshot_authority import (
    ACQUISITION_CONTRACT_RELATIVE,
    CONTRACT_RELATIVE,
    ROUTE_CONTRACT_RELATIVE,
    assess_same_day_snapshot_authority,
    write_same_day_authority_report,
)


def _repo() -> Path:
    root = Path(tempfile.mkdtemp(prefix="kospi_mcap_c8_same_day_"))
    for relative in (
        CONTRACT_RELATIVE,
        ROUTE_CONTRACT_RELATIVE,
        ACQUISITION_CONTRACT_RELATIVE,
    ):
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(contracts.REPO_ROOT / relative, destination)
    return root


def test_route_contract_has_one_current_snapshot_key_and_exact_source_set():
    raw = (contracts.REPO_ROOT / ROUTE_CONTRACT_RELATIVE).read_text(encoding="utf-8")
    assert raw.count('"official_current_snapshot_routes"') == 1
    payload = json.loads(raw)
    source_ids = {item["source_id"] for item in payload["official_current_snapshot_routes"]}
    assert source_ids == {
        "KRX_MDCSTAT019_CURRENT_BASIC_INFO",
        "KRX_MDCSTAT237_LIQUIDATION_CURRENT",
    }


def test_previous_day_capture_is_blocked_even_with_authenticated_session():
    root = _repo()
    try:
        report = assess_same_day_snapshot_authority(
            selection_as_of="20260914",
            captured_at="2026-09-15T16:00:00+09:00",
            authenticated_session_available=True,
            repo_root=root,
            generated_at="2026-09-15T16:01:00+09:00",
        )
        assert report["status"] == "BLOCKED"
        assert "SELECTION_ASOF_NOT_CAPTURE_DATE" in report["reason_codes"]
        assert report["historical_replay_allowed"] is False
        assert report["m2_allowed"] is False
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_same_day_before_close_is_blocked():
    root = _repo()
    try:
        report = assess_same_day_snapshot_authority(
            selection_as_of="20260915",
            captured_at="2026-09-15T10:00:00+09:00",
            authenticated_session_available=True,
            repo_root=root,
            generated_at="2026-09-15T10:01:00+09:00",
        )
        assert report["same_day"] is True
        assert report["post_close"] is False
        assert "CAPTURE_BEFORE_POST_CLOSE_MINIMUM" in report["reason_codes"]
        assert report["candidate_ready_for_policy_review"] is False
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_same_day_post_close_passes_preflight_only_without_evidence_or_m2():
    root = _repo()
    try:
        report = assess_same_day_snapshot_authority(
            selection_as_of="20260915",
            captured_at="2026-09-15T16:00:00+09:00",
            authenticated_session_available=True,
            repo_root=root,
            generated_at="2026-09-15T16:01:00+09:00",
        )
        assert report["status"] == "PASS"
        assert report["verdict"] == "C8_SAME_DAY_AUTHORITY_PREFLIGHT_PASS"
        assert report["candidate_ready_for_policy_review"] is True
        assert report["policy_approved"] is True
        assert report["existing_acquisition_contract_supported"] is True
        assert report["capture_ready_for_evidence_session"] is True
        assert report["raw_sources_published"] is False
        assert report["acquisition_evidence_published"] is False
        assert report["m2_allowed"] is False
        assert report["orders_generated"] is False
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_missing_authenticated_session_blocks_current_routes():
    root = _repo()
    try:
        report = assess_same_day_snapshot_authority(
            selection_as_of="20260915",
            captured_at="2026-09-15T16:00:00+09:00",
            authenticated_session_available=False,
            repo_root=root,
            generated_at="2026-09-15T16:01:00+09:00",
        )
        assert report["status"] == "BLOCKED"
        assert "AUTHENTICATED_KRX_SESSION_UNAVAILABLE" in report["reason_codes"]
        assert report["candidate_ready_for_policy_review"] is False
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_route_contract_mismatch_fails_structural_candidate():
    root = _repo()
    try:
        path = root / ROUTE_CONTRACT_RELATIVE
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["official_current_snapshot_routes"][0]["bld"] = ""
        path.write_text(json.dumps(payload), encoding="utf-8")
        report = assess_same_day_snapshot_authority(
            selection_as_of="20260915",
            captured_at="2026-09-15T16:00:00+09:00",
            authenticated_session_available=True,
            repo_root=root,
            generated_at="2026-09-15T16:01:00+09:00",
        )
        assert report["status"] == "BLOCKED"
        assert report["structural_candidate"] is False
        assert any(reason.endswith("OFFICIAL_ROUTE_IDENTITY_INCOMPLETE") for reason in report["reason_codes"])
        assert report["m2_allowed"] is False
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_report_writer_stays_in_strategy_namespace():
    root = _repo()
    try:
        report = assess_same_day_snapshot_authority(
            selection_as_of="20260914",
            captured_at="2026-09-15T10:00:00+09:00",
            authenticated_session_available=False,
            repo_root=root,
            generated_at="2026-09-15T10:01:00+09:00",
        )
        versioned, latest = write_same_day_authority_report(report, repo_root=root)
        assert versioned.is_file()
        assert latest.is_file()
        assert json.loads(latest.read_text(encoding="utf-8"))["m2_allowed"] is False
    finally:
        shutil.rmtree(root, ignore_errors=True)
