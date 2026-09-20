from __future__ import annotations

import copy
import hashlib
import io
import json
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path


from tools.apply_kospi_mcap_quarterly_c8_corp_code_checkpoint import run_checkpoint


KST = timezone(timedelta(hours=9))
CODES = ["000001", "0126Z0"]


def _package(*codes: str) -> bytes:
    rows = "".join(
        f"<list><corp_code>{index:08d}</corp_code><corp_name>N{index}</corp_name>"
        f"<stock_code>{code}</stock_code><modify_date>20260915</modify_date></list>"
        for index, code in enumerate(codes, start=1)
    )
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(
            "CORPCODE.xml",
            f"<?xml version='1.0' encoding='UTF-8'?><result>{rows}</result>".encode(),
        )
    return output.getvalue()


def _inputs(root: Path, *, package_codes: tuple[str, ...] = tuple(CODES)) -> tuple[Path, Path]:
    raw = root / "paper/strategies/kospi_mcap_quarterly_v1/data/inbox/raw/corp.zip"
    raw.parent.mkdir(parents=True, exist_ok=True)
    payload = _package(*package_codes)
    raw.write_bytes(payload)
    manifest = {
        "strategy_id": "KOSPI_MCAP_QUARTERLY_V1",
        "scope": "M1_C8_CORPORATE_ACTION_FULL_CAPTURE_CHECKPOINT",
        "selection_as_of": "20260915",
        "universe": {"codes": CODES, "code_count": len(CODES)},
        "request_plan": {"minimum_request_count": 5},
        "checkpoint": {"corp_code_package": {"attempts": []}},
    }
    manifest_path = root / (
        "paper/strategies/kospi_mcap_quarterly_v1/data/acquisition/checkpoints/manifest.json"
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    report = {
        "strategy_id": "KOSPI_MCAP_QUARTERLY_V1",
        "scope": "M1_C8_CORP_CODE_PACKAGE_ONE_CALL_CAPTURE",
        "status": "PASS",
        "verdict": "C8_CORP_CODE_PACKAGE_CAPTURED_COVERAGE_PASS",
        "http_status": 200,
        "request_count": 1,
        "package_parse_status": "PASS",
        "package_mapping_count": len(package_codes),
        "manifest_code_count": len(CODES),
        "matched_code_count": len(CODES),
        "missing_code_count": 0,
        "manifest_updated": False,
        "raw_path": raw.relative_to(root).as_posix(),
        "raw_size_bytes": len(payload),
        "raw_sha256": hashlib.sha256(payload).hexdigest(),
        "captured_at": "2026-09-15T16:08:50.029377+09:00",
        "full_capture_complete": False,
        "acquisition_evidence_published": False,
        "canonical_generated": False,
        "m2_allowed": False,
        "candidate_selection_calculated": False,
        "target_portfolio_calculated": False,
        "orders_generated": False,
        "operational_change": False,
    }
    report_path = root / "capture.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    return manifest_path, report_path


def _evaluator(manifest, **_kwargs):
    attempts = manifest["checkpoint"]["corp_code_package"]["attempts"]
    mapped = bool(attempts and attempts[-1]["status"] == "PASS")
    return {
        "generated_at": "2026-09-15T16:30:00+09:00",
        "manifest_integrity_pass": True,
        "base_universe_authorized": True,
        "verdict": "C8_CORPORATE_ACTION_CAPTURE_PENDING",
        "reason_codes": [
            "CORPORATE_ACTION_REQUESTS_PENDING" if mapped else "CORP_CODE_PACKAGE_PENDING"
        ],
        "bulk_requests_executed": 1 if mapped else 0,
        "next_pending_requests": (
            [{"kind": "LIST_PAGE", "code": CODES[0], "series": "B001", "page_no": 1}]
            if mapped
            else [{"kind": "CORP_CODE_PACKAGE"}]
        ),
        "full_capture_complete": False,
        "m2_allowed": False,
        "orders_generated": False,
    }


def _run(root: Path, manifest: Path, report: Path, *, apply=False, confirmed=False):
    return run_checkpoint(
        repo_root=root,
        manifest_path=manifest,
        capture_report_path=report,
        apply=apply,
        confirmed=confirmed,
        now=lambda: datetime(2026, 9, 15, 16, 30, tzinfo=KST),
        evaluator=_evaluator,
    )


def test_dry_run_builds_exact_checkpoint_without_writing(tmp_path: Path):
    manifest, report = _inputs(tmp_path)
    before = manifest.read_bytes()
    result = _run(tmp_path, manifest, report)
    assert result["verdict"] == "C8_CORP_CODE_CHECKPOINT_READY"
    assert result["network_request_count"] == 0
    assert result["checkpoint_attempt_count_after"] == 1
    assert result["minimum_requests_remaining"] == 4
    assert manifest.read_bytes() == before


def test_apply_writes_one_attempt_and_replay_is_noop(tmp_path: Path):
    manifest, report = _inputs(tmp_path)
    result = _run(tmp_path, manifest, report, apply=True, confirmed=True)
    assert result["verdict"] == "C8_CORP_CODE_CHECKPOINT_APPLIED"
    assert result["manifest_updated"] is True
    saved = json.loads(manifest.read_text(encoding="utf-8"))
    attempts = saved["checkpoint"]["corp_code_package"]["attempts"]
    assert len(attempts) == 1
    assert attempts[0]["mapping"] == {"000001": "00000001", "0126Z0": "00000002"}
    replay = _run(tmp_path, manifest, report, apply=True, confirmed=True)
    assert replay["verdict"] == "C8_CORP_CODE_CHECKPOINT_ALREADY_APPLIED"
    saved_again = json.loads(manifest.read_text(encoding="utf-8"))
    assert saved_again == saved
    assert replay["network_request_count"] == 0


def test_apply_requires_explicit_confirmation(tmp_path: Path):
    manifest, report = _inputs(tmp_path)
    before = manifest.read_bytes()
    result = _run(tmp_path, manifest, report, apply=True, confirmed=False)
    assert result["verdict"] == "C8_CORP_CODE_CHECKPOINT_BLOCKED"
    assert "CORP_CODE_CHECKPOINT_CONFIRMATION_MISSING" in result["reason_codes"]
    assert manifest.read_bytes() == before


def test_raw_hash_tamper_is_fail_closed(tmp_path: Path):
    manifest, report = _inputs(tmp_path)
    raw = tmp_path / json.loads(report.read_text(encoding="utf-8"))["raw_path"]
    raw.write_bytes(raw.read_bytes() + b"tamper")
    result = _run(tmp_path, manifest, report, apply=True, confirmed=True)
    assert result["verdict"] == "C8_CORP_CODE_CHECKPOINT_BLOCKED"
    assert "CORP_CODE_CHECKPOINT_RAW_SHA256_MISMATCH" in result["reason_codes"]
    assert "CORP_CODE_CHECKPOINT_RAW_SIZE_MISMATCH" in result["reason_codes"]


def test_current_manifest_coverage_gap_is_fail_closed(tmp_path: Path):
    manifest, report = _inputs(tmp_path, package_codes=("000001",))
    capture = json.loads(report.read_text(encoding="utf-8"))
    capture["package_mapping_count"] = 1
    report.write_text(json.dumps(capture), encoding="utf-8")
    result = _run(tmp_path, manifest, report, apply=True, confirmed=True)
    assert result["verdict"] == "C8_CORP_CODE_CHECKPOINT_BLOCKED"
    assert "CORP_CODE_CHECKPOINT_EXACT_COVERAGE_INCOMPLETE" in result["reason_codes"]
    assert result["network_request_count"] == 0


def test_capture_report_downstream_open_is_rejected(tmp_path: Path):
    manifest, report = _inputs(tmp_path)
    capture = json.loads(report.read_text(encoding="utf-8"))
    capture["m2_allowed"] = True
    report.write_text(json.dumps(capture), encoding="utf-8")
    result = _run(tmp_path, manifest, report)
    assert result["verdict"] == "C8_CORP_CODE_CHECKPOINT_BLOCKED"
    assert "CORP_CODE_CHECKPOINT_CAPTURE_REPORT_DOWNSTREAM_OPEN" in result["reason_codes"]


def test_capture_timestamp_without_timezone_is_rejected(tmp_path: Path):
    manifest, report = _inputs(tmp_path)
    capture = json.loads(report.read_text(encoding="utf-8"))
    capture["captured_at"] = "2026-09-15T16:08:50"
    report.write_text(json.dumps(capture), encoding="utf-8")
    result = _run(tmp_path, manifest, report)
    assert result["verdict"] == "C8_CORP_CODE_CHECKPOINT_BLOCKED"
    assert "CORP_CODE_CHECKPOINT_CAPTURE_TIMESTAMP_INVALID" in result["reason_codes"]
