from __future__ import annotations

import io
import json
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from tools import collect_kospi_mcap_quarterly_c8_documents as collector


KST = timezone(timedelta(hours=9))
RECEIPTS = ["20260915000001", "20260915000002"]


def _package(text: str = "<doc>회사합병</doc>") -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("document.xml", text.encode("utf-8"))
    return buffer.getvalue()


def _manifest(root: Path) -> Path:
    payload = {
        "strategy_id": "KOSPI_MCAP_QUARTERLY_V1",
        "selection_as_of": "20260915",
        "universe": {"codes": ["000001"], "code_count": 1},
        "request_plan": {"minimum_request_count": 3},
        "checkpoint": {
            "corp_code_package": {
                "attempts": [
                    {
                        "status": "PASS",
                        "payload_sha256": "a" * 64,
                        "row_count": 1,
                        "mapping": {"000001": "00000001"},
                        "captured_at": "2026-09-15T16:00:00+09:00",
                        "error_code": "",
                    }
                ]
            },
            "list_pages": {
                "000001:MERGER_DECISION_HISTORY:1": {
                    "attempts": [
                        {
                            "status": "PASS",
                            "code": "000001",
                            "corp_code": "00000001",
                            "series": "MERGER_DECISION_HISTORY",
                            "page_no": 1,
                            "dart_status": "000",
                            "payload_sha256": "b" * 64,
                            "row_count": 2,
                            "total_page": 1,
                            "accepted_receipts": RECEIPTS,
                            "captured_at": "2026-09-15T17:00:00+09:00",
                            "error_code": "",
                        }
                    ]
                },
                "000001:MERGER_COMPLETION_HISTORY:1": {
                    "attempts": [
                        {
                            "status": "PASS",
                            "code": "000001",
                            "corp_code": "00000001",
                            "series": "MERGER_COMPLETION_HISTORY",
                            "page_no": 1,
                            "dart_status": "013",
                            "payload_sha256": "c" * 64,
                            "row_count": 0,
                            "total_page": 0,
                            "accepted_receipts": [],
                            "captured_at": "2026-09-15T17:00:01+09:00",
                            "error_code": "",
                        }
                    ]
                },
            },
            "documents": {},
        },
    }
    path = root / collector.MANIFEST_RELATIVE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _key(root: Path) -> Path:
    path = root / "key.txt"
    path.write_text("x" * 40, encoding="utf-8")
    return path


def _assessment(manifest, *, generated_at, **_kwargs):
    documents = manifest["checkpoint"]["documents"]
    complete = all(
        (documents.get(receipt, {}).get("attempts") or [{}])[-1].get("status") == "PASS"
        for receipt in RECEIPTS
    )
    return {
        "generated_at": generated_at,
        "strategy_id": "KOSPI_MCAP_QUARTERLY_V1",
        "scope": "M1_C8_CORPORATE_ACTION_CAPTURE_STATUS",
        "status": "PASS" if complete else "BLOCKED",
        "verdict": (
            "C8_CORPORATE_ACTION_FULL_CAPTURE_PASS"
            if complete
            else "C8_CORPORATE_ACTION_CAPTURE_PENDING"
        ),
        "reason_codes": [] if complete else ["CORPORATE_ACTION_REQUESTS_PENDING"],
        "manifest_integrity_pass": True,
        "base_universe_authorized": True,
        "bulk_requests_executed": 3 + sum(
            len(record.get("attempts") or []) for record in documents.values()
        ),
        "next_pending_requests": [],
        "full_capture_complete": complete,
        "network_execution_authorized": False,
        "acquisition_evidence_published": False,
        "adapter_components_generated": False,
        "canonical_generated": False,
        "m2_allowed": False,
        "candidate_selection_calculated": False,
        "target_portfolio_calculated": False,
        "orders_generated": False,
        "operational_change": False,
    }


def _clock():
    current = datetime(2026, 9, 15, 20, 0, 0, tzinfo=KST)

    def tick():
        nonlocal current
        current += timedelta(microseconds=1)
        return current

    return tick


def _run(root: Path, fetcher, *, max_requests=1, apply=False, confirmed=False):
    return collector.run_collection(
        repo_root=root,
        manifest_path=_manifest(root),
        api_key_file=_key(root),
        max_requests=max_requests,
        apply=apply,
        confirmed=confirmed,
        timeout=1,
        fetcher=fetcher,
        now=_clock(),
        evaluator=_assessment,
    )


def test_dry_run_never_calls_network_or_writes(tmp_path: Path):
    called = 0

    def fetcher(*_args):
        nonlocal called
        called += 1
        raise AssertionError("network must stay closed")

    result = _run(tmp_path, fetcher)
    assert result["status"] == "READY"
    assert result["verdict"] == "C8_DOCUMENT_COLLECTION_READY"
    assert result["first_pending_request"] == {
        "kind": "DOCUMENT",
        "receipt_no": RECEIPTS[0],
    }
    assert result["network_request_count"] == called == 0
    assert not (tmp_path / collector.REPORT_LATEST_RELATIVE).exists()


def test_apply_collects_package_and_checkpoints(tmp_path: Path):
    seen = []

    def fetcher(endpoint, params, _timeout):
        seen.append((endpoint, params))
        return 200, _package(), {"Date": "Tue, 15 Sep 2026 11:00:00 GMT"}

    result = _run(tmp_path, fetcher, apply=True, confirmed=True)
    assert result["status"] == "PASS"
    assert result["verdict"] == "C8_DOCUMENT_BUDGET_EXHAUSTED_CHECKPOINT_SAVED"
    assert result["network_request_count"] == result["pass_count"] == 1
    assert seen == [
        (
            "https://opendart.fss.or.kr/api/document.xml",
            {"crtfc_key": "x" * 40, "rcept_no": RECEIPTS[0]},
        )
    ]
    saved = json.loads((tmp_path / collector.MANIFEST_RELATIVE).read_text(encoding="utf-8"))
    assert saved["checkpoint"]["documents"][RECEIPTS[0]]["attempts"][-1]["status"] == "PASS"
    journal = json.loads(
        (tmp_path / collector.JOURNAL_DIR_RELATIVE / f"{RECEIPTS[0]}.json").read_text(
            encoding="utf-8"
        )
    )
    assert journal["package_member_count"] == 1
    assert len(list((tmp_path / collector.RAW_DIR_RELATIVE).glob("opendart_document_*.zip"))) == 1


def test_apply_requires_confirmation_and_contract_budget(tmp_path: Path):
    def fetcher(*_args):
        raise AssertionError("network must stay closed")

    missing = _run(tmp_path, fetcher, apply=True, confirmed=False)
    assert "C8_DOCUMENT_COLLECTION_CONFIRMATION_MISSING" in missing["reason_codes"]
    excessive = _run(tmp_path, fetcher, max_requests=1001)
    assert "C8_DOCUMENT_COLLECTION_REQUEST_BUDGET_INVALID" in excessive["reason_codes"]
    assert excessive["network_request_count"] == 0


def test_http_error_records_failure_and_hard_stops(tmp_path: Path):
    def fetcher(_endpoint, _params, _timeout):
        return 503, b"unavailable", {}

    result = _run(tmp_path, fetcher, max_requests=10, apply=True, confirmed=True)
    assert result["status"] == "FAIL"
    assert result["network_request_count"] == result["fail_count"] == 1
    assert result["hard_stop_triggered"] is True
    assert result["stop_reason"] == "DOCUMENT_REQUEST_OR_RESPONSE_FAILED"
    assert result["reason_codes"] == ["DART_DOCUMENT_HTTP_503"]


def test_dart_status_020_is_request_limit_hard_stop(tmp_path: Path):
    def fetcher(_endpoint, _params, _timeout):
        return 200, b"<result><status>020</status><message>limit</message></result>", {}

    result = _run(tmp_path, fetcher, max_requests=10, apply=True, confirmed=True)
    assert result["status"] == "FAIL"
    assert result["reason_codes"] == ["DART_DOCUMENT_STATUS_020"]
    assert result["stop_reason"] == "DART_REQUEST_LIMIT_EXCEEDED"
    assert result["network_request_count"] == 1


def test_dart_status_014_checkpoints_unavailable_and_same_run_progresses(tmp_path: Path):
    manifest = _manifest(tmp_path)
    key = _key(tmp_path)
    clock = _clock()
    seen = []

    def fetcher(_endpoint, params, _timeout):
        receipt_no = params["rcept_no"]
        seen.append(receipt_no)
        if receipt_no == RECEIPTS[0]:
            return 200, b"<result><status>014</status><message>missing</message></result>", {}
        return 200, _package(), {}

    first = collector.run_collection(
        repo_root=tmp_path,
        manifest_path=manifest,
        api_key_file=key,
        max_requests=10,
        apply=True,
        confirmed=True,
        timeout=1,
        fetcher=fetcher,
        now=clock,
        evaluator=_assessment,
    )
    assert first["status"] == "PASS"
    assert first["verdict"] == "C8_DOCUMENT_COLLECTION_NO_PENDING_INCOMPLETE"
    assert first["network_request_count"] == 2
    assert first["pass_count"] == 1
    assert first["fail_count"] == 0
    assert first["unavailable_count"] == 1
    assert first["hard_stop_triggered"] is False
    assert first["stop_reason"] == "NO_PENDING_REQUESTS"
    assert seen == RECEIPTS
    assert first["request_results"][0]["status"] == "UNAVAILABLE"
    assert first["request_results"][1]["status"] == "PASS"
    assert first["next_pending_request"] == {}
    assert first["full_capture_complete"] is False
    saved = json.loads(manifest.read_text(encoding="utf-8"))
    assert saved["checkpoint"]["documents"][RECEIPTS[0]]["attempts"][-1]["status"] == "UNAVAILABLE"
    assert saved["checkpoint"]["documents"][RECEIPTS[1]]["attempts"][-1]["status"] == "PASS"
    assert (
        tmp_path / collector.UNAVAILABLE_JOURNAL_DIR_RELATIVE / f"{RECEIPTS[0]}.json"
    ).is_file()

    def no_replay_fetcher(*_args):
        raise AssertionError("terminal checkpoints must not replay")

    second = collector.run_collection(
        repo_root=tmp_path,
        manifest_path=manifest,
        api_key_file=key,
        max_requests=1,
        apply=True,
        confirmed=True,
        timeout=1,
        fetcher=no_replay_fetcher,
        now=clock,
        evaluator=_assessment,
    )
    assert second["status"] == "PASS"
    assert second["network_request_count"] == 0
    assert second["full_capture_complete"] is False


def test_dart_status_014_continues_but_next_020_still_hard_stops(tmp_path: Path):
    seen = []

    def fetcher(_endpoint, params, _timeout):
        receipt_no = params["rcept_no"]
        seen.append(receipt_no)
        status = "014" if receipt_no == RECEIPTS[0] else "020"
        payload = f"<result><status>{status}</status><message>x</message></result>".encode()
        return 200, payload, {}

    result = _run(tmp_path, fetcher, max_requests=10, apply=True, confirmed=True)
    assert result["status"] == "FAIL"
    assert result["network_request_count"] == 2
    assert result["unavailable_count"] == 1
    assert result["fail_count"] == 1
    assert result["hard_stop_triggered"] is True
    assert result["reason_codes"] == ["DART_DOCUMENT_STATUS_020"]
    assert result["stop_reason"] == "DART_REQUEST_LIMIT_EXCEEDED"
    assert seen == RECEIPTS


def test_persisted_fail_014_recovers_without_replaying_request(tmp_path: Path):
    manifest = _manifest(tmp_path)
    key = _key(tmp_path)
    clock = _clock()
    payload = b"<result><status>014</status><message>missing</message></result>"
    captured = clock()
    raw_relative = collector._raw_relative(RECEIPTS[0], payload, captured)
    raw_path = tmp_path / raw_relative
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_bytes(payload)
    current = json.loads(manifest.read_text(encoding="utf-8"))
    current = collector.record_document(
        current,
        receipt_no=RECEIPTS[0],
        status="FAIL",
        payload_sha256=collector._sha256(payload),
        package_member_count=0,
        captured_at=captured.isoformat(timespec="microseconds"),
        error_code="DART_DOCUMENT_STATUS_014",
    )
    manifest.write_text(json.dumps(current), encoding="utf-8")
    seen = []

    def fetcher(_endpoint, params, _timeout):
        seen.append(params["rcept_no"])
        return 200, _package(), {}

    result = collector.run_collection(
        repo_root=tmp_path,
        manifest_path=manifest,
        api_key_file=key,
        max_requests=1,
        apply=True,
        confirmed=True,
        timeout=1,
        fetcher=fetcher,
        now=clock,
        evaluator=_assessment,
    )
    assert result["recovered_unavailable_count"] == 1
    assert result["unavailable_count"] == 1
    assert result["network_request_count"] == 1
    assert seen == [RECEIPTS[1]]
    saved = json.loads(manifest.read_text(encoding="utf-8"))
    attempts = saved["checkpoint"]["documents"][RECEIPTS[0]]["attempts"]
    assert [attempt["status"] for attempt in attempts] == ["FAIL", "UNAVAILABLE"]


def test_empty_or_invalid_package_fails_closed(tmp_path: Path):
    def empty_fetcher(_endpoint, _params, _timeout):
        return 200, b"", {}

    empty = _run(tmp_path / "empty", empty_fetcher, apply=True, confirmed=True)
    assert empty["reason_codes"] == ["DART_DOCUMENT_RESPONSE_EMPTY"]

    def invalid_fetcher(_endpoint, _params, _timeout):
        return 200, b"not-a-package", {}

    invalid = _run(tmp_path / "invalid", invalid_fetcher, apply=True, confirmed=True)
    assert invalid["reason_codes"] == ["DART_DOCUMENT_PACKAGE_INVALID"]


def test_second_run_skips_successful_checkpoint(tmp_path: Path):
    manifest = _manifest(tmp_path)
    key = _key(tmp_path)
    seen = []
    clock = _clock()

    def fetcher(_endpoint, params, _timeout):
        seen.append(params["rcept_no"])
        return 200, _package(), {}

    first = collector.run_collection(
        repo_root=tmp_path,
        manifest_path=manifest,
        api_key_file=key,
        max_requests=1,
        apply=True,
        confirmed=True,
        timeout=1,
        fetcher=fetcher,
        now=clock,
        evaluator=_assessment,
    )
    second = collector.run_collection(
        repo_root=tmp_path,
        manifest_path=manifest,
        api_key_file=key,
        max_requests=1,
        apply=True,
        confirmed=True,
        timeout=1,
        fetcher=fetcher,
        now=clock,
        evaluator=_assessment,
    )
    assert first["status"] == second["status"] == "PASS"
    assert seen == RECEIPTS
    assert second["next_pending_request"] == {}
    assert second["full_capture_complete"] is True


def test_pass_journal_recovers_without_replaying_request(tmp_path: Path):
    manifest = _manifest(tmp_path)
    initial = manifest.read_bytes()
    key = _key(tmp_path)
    clock = _clock()

    def first_fetcher(_endpoint, _params, _timeout):
        return 200, _package(), {}

    first = collector.run_collection(
        repo_root=tmp_path,
        manifest_path=manifest,
        api_key_file=key,
        max_requests=1,
        apply=True,
        confirmed=True,
        timeout=1,
        fetcher=first_fetcher,
        now=clock,
        evaluator=_assessment,
    )
    assert first["pass_count"] == 1
    manifest.write_bytes(initial)
    seen = []

    def next_fetcher(_endpoint, params, _timeout):
        seen.append(params["rcept_no"])
        return 200, _package("<doc>합병등종료보고서</doc>"), {}

    recovered = collector.run_collection(
        repo_root=tmp_path,
        manifest_path=manifest,
        api_key_file=key,
        max_requests=1,
        apply=True,
        confirmed=True,
        timeout=1,
        fetcher=next_fetcher,
        now=clock,
        evaluator=_assessment,
    )
    assert recovered["recovered_pass_journal_count"] == 1
    assert recovered["network_request_count"] == 1
    assert seen == [RECEIPTS[1]]


def test_manifest_write_failure_returns_fail_report_without_replay(tmp_path: Path, monkeypatch):
    def fetcher(_endpoint, _params, _timeout):
        return 200, _package(), {}

    def fail_write(*_args, **_kwargs):
        raise PermissionError("locked")

    monkeypatch.setattr(collector, "write_capture_manifest", fail_write)
    result = _run(tmp_path, fetcher, apply=True, confirmed=True)
    assert result["status"] == "FAIL"
    assert result["reason_codes"] == ["C8_DOCUMENT_MANIFEST_WRITE_FAILED"]
    assert result["stop_reason"] == "MANIFEST_WRITE_FAILED"
    assert result["network_request_count"] == 1
    assert result["next_pending_request"]["receipt_no"] == RECEIPTS[0]
    assert (tmp_path / collector.REPORT_LATEST_RELATIVE).is_file()
