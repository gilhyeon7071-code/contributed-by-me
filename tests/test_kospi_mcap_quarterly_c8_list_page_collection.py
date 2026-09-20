from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


from tools.collect_kospi_mcap_quarterly_c8_list_pages import run_collection


KST = timezone(timedelta(hours=9))
CODES = ["000001", "0126Z0"]
MAPPING = {"000001": "00000001", "0126Z0": "00000002"}


def _manifest(root: Path) -> Path:
    payload = {
        "strategy_id": "KOSPI_MCAP_QUARTERLY_V1",
        "scope": "M1_C8_CORPORATE_ACTION_FULL_CAPTURE_CHECKPOINT",
        "selection_as_of": "20260915",
        "query_start": "20200101",
        "query_end": "20260915",
        "universe": {"codes": CODES, "code_count": len(CODES)},
        "request_plan": {"minimum_request_count": 5},
        "checkpoint": {
            "corp_code_package": {
                "attempts": [
                    {
                        "status": "PASS",
                        "payload_sha256": "a" * 64,
                        "row_count": 2,
                        "mapping": MAPPING,
                        "captured_at": "2026-09-15T16:00:00+09:00",
                        "error_code": "",
                    }
                ]
            },
            "list_pages": {},
            "documents": {},
        },
    }
    path = root / (
        "paper/strategies/kospi_mcap_quarterly_v1/data/acquisition/checkpoints/manifest.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _key(root: Path) -> Path:
    path = root / "key.txt"
    path.write_text("x" * 40, encoding="utf-8")
    return path


def _assessment(manifest, *, generated_at, **_kwargs):
    attempts = sum(
        len(record.get("attempts") or [])
        for record in manifest["checkpoint"]["list_pages"].values()
    )
    return {
        "generated_at": generated_at,
        "strategy_id": "KOSPI_MCAP_QUARTERLY_V1",
        "scope": "M1_C8_CORPORATE_ACTION_CAPTURE_STATUS",
        "status": "BLOCKED",
        "verdict": "C8_CORPORATE_ACTION_CAPTURE_PENDING",
        "reason_codes": ["CORPORATE_ACTION_REQUESTS_PENDING"],
        "manifest_integrity_pass": True,
        "base_universe_authorized": True,
        "bulk_requests_executed": 1 + attempts,
        "next_pending_requests": [],
        "full_capture_complete": False,
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
    current = datetime(2026, 9, 15, 17, 0, 0, tzinfo=KST)

    def tick():
        nonlocal current
        current += timedelta(microseconds=1)
        return current

    return tick


def _response(*, status="013", page_no=1, total_page=0, rows=None):
    return json.dumps(
        {
            "status": status,
            "message": "OK",
            "page_no": page_no,
            "page_count": 100,
            "total_count": len(rows or []),
            "total_page": total_page,
            "list": rows or [],
        }
    ).encode()


def _run(root: Path, fetcher, *, max_requests=1, apply=False, confirmed=False):
    return run_collection(
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
    assert result["verdict"] == "C8_LIST_PAGE_COLLECTION_READY"
    assert result["first_pending_request"]["kind"] == "LIST_PAGE"
    assert result["max_requests_this_run"] == 1
    assert result["network_request_count"] == called == 0


def test_apply_collects_two_pages_in_contract_order_and_checkpoints(tmp_path: Path):
    seen = []

    def fetcher(_endpoint, params, _timeout):
        seen.append((params["corp_code"], params["pblntf_detail_ty"], params["page_no"]))
        return 200, _response(), {"Date": "Tue, 15 Sep 2026 08:00:00 GMT"}

    result = _run(tmp_path, fetcher, max_requests=2, apply=True, confirmed=True)
    assert result["verdict"] == "C8_LIST_PAGE_BUDGET_EXHAUSTED_CHECKPOINT_SAVED"
    assert seen == [("00000001", "B001", "1"), ("00000001", "E003", "1")]
    assert result["network_request_count"] == result["pass_count"] == 2
    saved = json.loads(
        (tmp_path / "paper/strategies/kospi_mcap_quarterly_v1/data/acquisition/checkpoints/manifest.json")
        .read_text(encoding="utf-8")
    )
    assert len(saved["checkpoint"]["list_pages"]) == 2
    assert len(list((tmp_path / "paper/strategies/kospi_mcap_quarterly_v1/data/inbox/raw").glob("*"))) == 2


def test_additional_page_precedes_next_series(tmp_path: Path):
    seen = []

    def fetcher(_endpoint, params, _timeout):
        seen.append((params["pblntf_detail_ty"], params["page_no"]))
        if len(seen) == 1:
            return 200, _response(status="000", page_no=1, total_page=2), {}
        return 200, _response(status="000", page_no=2, total_page=2), {}

    result = _run(tmp_path, fetcher, max_requests=2, apply=True, confirmed=True)
    assert result["status"] == "PASS"
    assert seen == [("B001", "1"), ("B001", "2")]


def test_status_020_records_failure_and_hard_stops(tmp_path: Path):
    def fetcher(_endpoint, _params, _timeout):
        return 200, _response(status="020"), {}

    result = _run(tmp_path, fetcher, max_requests=10, apply=True, confirmed=True)
    assert result["verdict"] == "FAIL_C8_LIST_PAGE_COLLECTION"
    assert result["network_request_count"] == 1
    assert result["fail_count"] == 1
    assert result["hard_stop_triggered"] is True
    assert result["stop_reason"] == "DART_REQUEST_LIMIT_EXCEEDED"


def test_identity_mismatch_is_fail_closed(tmp_path: Path):
    bad_row = {
        "corp_code": "99999999",
        "stock_code": "000001",
        "report_nm": "회사합병결정",
        "rcept_no": "20260915000001",
        "rcept_dt": "20260915",
        "rm": "",
    }

    def fetcher(_endpoint, _params, _timeout):
        return 200, _response(status="000", page_no=1, total_page=1, rows=[bad_row]), {}

    result = _run(tmp_path, fetcher, apply=True, confirmed=True)
    assert result["verdict"] == "FAIL_C8_LIST_PAGE_COLLECTION"
    assert "DART_LIST_ROW_IDENTITY_MISMATCH" in result["reason_codes"]


def test_apply_requires_confirmation_and_budget_cap(tmp_path: Path):
    def fetcher(*_args):
        raise AssertionError("network must stay closed")

    missing = _run(tmp_path, fetcher, apply=True, confirmed=False)
    assert "C8_LIST_COLLECTION_CONFIRMATION_MISSING" in missing["reason_codes"]
    excessive = _run(tmp_path, fetcher, max_requests=1001)
    assert "C8_LIST_COLLECTION_REQUEST_BUDGET_INVALID" in excessive["reason_codes"]
    assert excessive["network_request_count"] == 0


def test_successful_checkpoint_is_skipped_on_next_run(tmp_path: Path):
    root_manifest = _manifest(tmp_path)
    key = _key(tmp_path)
    seen = []
    clock = _clock()

    def fetcher(_endpoint, params, _timeout):
        seen.append((params["corp_code"], params["pblntf_detail_ty"]))
        return 200, _response(), {}

    first = run_collection(
        repo_root=tmp_path,
        manifest_path=root_manifest,
        api_key_file=key,
        max_requests=1,
        apply=True,
        confirmed=True,
        timeout=1,
        fetcher=fetcher,
        now=clock,
        evaluator=_assessment,
    )
    second = run_collection(
        repo_root=tmp_path,
        manifest_path=root_manifest,
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
    assert seen == [("00000001", "B001"), ("00000001", "E003")]
    assert second["network_request_count"] == 1


def test_pass_journal_recovers_before_requesting_next_page(tmp_path: Path):
    root_manifest = _manifest(tmp_path)
    initial_manifest = root_manifest.read_bytes()
    key = _key(tmp_path)
    clock = _clock()

    def first_fetcher(_endpoint, _params, _timeout):
        return 200, _response(), {}

    first = run_collection(
        repo_root=tmp_path,
        manifest_path=root_manifest,
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
    root_manifest.write_bytes(initial_manifest)
    seen = []

    def next_fetcher(_endpoint, params, _timeout):
        seen.append(params["pblntf_detail_ty"])
        return 200, _response(), {}

    recovered = run_collection(
        repo_root=tmp_path,
        manifest_path=root_manifest,
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
    assert seen == ["E003"]
