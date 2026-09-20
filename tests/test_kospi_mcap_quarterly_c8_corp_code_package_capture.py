from __future__ import annotations

import io
import json
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

from tools.collect_kospi_mcap_quarterly_c8_corp_code_package import run_capture


KST = timezone(timedelta(hours=9))


def _manifest(path: Path, *, attempts: list[dict] | None = None) -> Path:
    payload = {
        "strategy_id": "KOSPI_MCAP_QUARTERLY_V1",
        "scope": "M1_C8_CORPORATE_ACTION_FULL_CAPTURE_CHECKPOINT",
        "selection_as_of": "20260915",
        "universe": {
            "code_count": 2,
            "codes": ["000001", "0126Z0"],
        },
        "request_plan": {"corp_code_package_requests": 1},
        "checkpoint": {"corp_code_package": {"attempts": attempts or []}},
        "full_capture_complete": False,
        "acquisition_evidence_published": False,
        "adapter_components_generated": False,
        "canonical_generated": False,
        "m2_allowed": False,
        "candidate_selection_calculated": False,
        "target_portfolio_calculated": False,
        "orders_generated": False,
        "operational_change": False,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _key(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x" * 40, encoding="utf-8")
    return path


def _package(*codes: str) -> bytes:
    rows = "".join(
        f"<list><corp_code>{index:08d}</corp_code><corp_name>N{index}</corp_name>"
        f"<stock_code>{code}</stock_code><modify_date>20260915</modify_date></list>"
        for index, code in enumerate(codes, start=1)
    )
    xml = f"<?xml version='1.0' encoding='UTF-8'?><result>{rows}</result>".encode()
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("CORPCODE.xml", xml)
    return output.getvalue()


def _clock():
    values = iter(
        [
            datetime(2026, 9, 15, 16, 10, 0, tzinfo=KST),
            datetime(2026, 9, 15, 16, 10, 1, tzinfo=KST),
            datetime(2026, 9, 15, 16, 10, 2, tzinfo=KST),
        ]
    )
    return lambda: next(values)


def test_dry_run_never_calls_network_or_writes(tmp_path: Path):
    called = 0

    def fetcher(*_args):
        nonlocal called
        called += 1
        raise AssertionError("network must stay closed")

    report = run_capture(
        repo_root=tmp_path,
        manifest_path=_manifest(tmp_path / "manifest.json"),
        api_key_file=_key(tmp_path / "key.txt"),
        apply=False,
        confirmed=False,
        timeout=1,
        fetcher=fetcher,
    )
    assert report["verdict"] == "C8_CORP_CODE_PACKAGE_ONE_CALL_READY"
    assert report["request_count"] == 0
    assert called == 0
    assert not (tmp_path / "paper").exists()


def test_apply_makes_one_call_and_keeps_manifest_unchanged(tmp_path: Path):
    manifest = _manifest(tmp_path / "manifest.json")
    before = manifest.read_bytes()
    called = 0

    def fetcher(_endpoint, params, _timeout):
        nonlocal called
        called += 1
        assert set(params) == {"crtfc_key"}
        return 200, _package("000001", "0126Z0"), {"Date": "Tue, 15 Sep 2026 07:10:01 GMT"}

    report = run_capture(
        repo_root=tmp_path,
        manifest_path=manifest,
        api_key_file=_key(tmp_path / "key.txt"),
        apply=True,
        confirmed=True,
        timeout=1,
        fetcher=fetcher,
        now=_clock(),
    )
    assert report["verdict"] == "C8_CORP_CODE_PACKAGE_CAPTURED_COVERAGE_PASS"
    assert report["request_count"] == called == 1
    assert report["matched_code_count"] == 2
    assert report["missing_code_count"] == 0
    assert manifest.read_bytes() == before
    assert Path(report["outputs"][0]).is_file()
    report_text = Path(report["outputs"][1]).read_text(encoding="utf-8")
    assert "x" * 40 not in report_text
    second = run_capture(
        repo_root=tmp_path,
        manifest_path=manifest,
        api_key_file=tmp_path / "key.txt",
        apply=True,
        confirmed=True,
        timeout=1,
        fetcher=fetcher,
        now=_clock(),
    )
    assert second["verdict"] == "C8_CORP_CODE_PACKAGE_CAPTURE_BLOCKED"
    assert "CORP_CODE_CAPTURE_ALREADY_EXECUTED_FOR_SELECTION" in second["reason_codes"]
    assert called == 1


def test_mapping_gap_is_blocked_without_additional_call(tmp_path: Path):
    called = 0

    def fetcher(_endpoint, _params, _timeout):
        nonlocal called
        called += 1
        return 200, _package("000001"), {}

    report = run_capture(
        repo_root=tmp_path,
        manifest_path=_manifest(tmp_path / "manifest.json"),
        api_key_file=_key(tmp_path / "key.txt"),
        apply=True,
        confirmed=True,
        timeout=1,
        fetcher=fetcher,
        now=_clock(),
    )
    assert report["verdict"] == "C8_CORP_CODE_PACKAGE_CAPTURED_MAPPING_GAP"
    assert report["missing_codes"] == ["0126Z0"]
    assert report["request_count"] == called == 1
    assert report["manifest_updated"] is False


def test_existing_attempt_blocks_before_network(tmp_path: Path):
    called = 0

    def fetcher(*_args):
        nonlocal called
        called += 1
        return 200, b"", {}

    report = run_capture(
        repo_root=tmp_path,
        manifest_path=_manifest(tmp_path / "manifest.json", attempts=[{"status": "PASS"}]),
        api_key_file=_key(tmp_path / "key.txt"),
        apply=True,
        confirmed=True,
        timeout=1,
        fetcher=fetcher,
    )
    assert report["verdict"] == "C8_CORP_CODE_PACKAGE_CAPTURE_BLOCKED"
    assert "CORP_CODE_CAPTURE_ALREADY_ATTEMPTED" in report["reason_codes"]
    assert called == 0
