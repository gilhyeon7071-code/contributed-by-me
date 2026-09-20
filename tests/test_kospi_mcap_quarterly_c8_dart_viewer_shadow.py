from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from tools import collect_kospi_mcap_quarterly_c8_dart_viewer_shadow as collector


KST = timezone(timedelta(hours=9))
RECEIPTS = ["20251024000420", "20251111000001"]


def _clock():
    current = datetime(2026, 9, 16, 9, 0, 0, tzinfo=KST)

    def tick():
        nonlocal current
        current += timedelta(microseconds=1)
        return current

    return tick


def _context(root: Path, receipts: list[str] | None = None) -> Path:
    receipts = receipts or RECEIPTS
    status_relative = Path("reports/capture.json")
    status_path = root / status_relative
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(
        json.dumps(
            {
                "status": "BLOCKED",
                "reason_codes": ["CORPORATE_ACTION_DOCUMENTS_UNAVAILABLE"],
                "unavailable_document_count": len(receipts),
                "unavailable_receipts": receipts,
            }
        ),
        encoding="utf-8",
    )
    contract = {
        "contract_version": "test",
        "strategy_id": collector.STRATEGY_ID,
        "mode": "SHADOW_ONLY",
        "input": {
            "capture_status_path": status_relative.as_posix(),
            "required_capture_status": "BLOCKED",
            "required_reason_code": "CORPORATE_ACTION_DOCUMENTS_UNAVAILABLE",
            "receipt_field": "unavailable_receipts",
            "expected_receipt_count": len(receipts),
        },
        "official_route": {
            "landing_endpoint": "https://dart.fss.or.kr/dsaf001/main.do",
            "viewer_endpoint": "https://dart.fss.or.kr/report/viewer.do",
            "landing_query_key": "rcpNo",
        },
        "execution": {
            "default_max_receipts": 1,
            "default_max_network_requests": 10,
            "default_delay_seconds": 0,
            "default_timeout_seconds": 1,
            "default_retries": 0,
            "retry_backoff_seconds": 0,
        },
        "output": {
            "root": "shadow",
            "raw_dir": "raw",
            "pass_journal_dir": "journals/pass",
            "failure_journal_dir": "journals/fail",
            "checkpoint_path": "checkpoints/latest.json",
            "report_path": "reports/shadow_latest.json",
        },
    }
    contract_path = root / "contract.json"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    return contract_path


def _landing(receipt: str) -> bytes:
    return (
        "<html><head><title>아이에스동서 - 주요사항보고서(회사합병결정)</title></head>"
        "<body><a href=\"javascript:viewDoc('"
        + receipt
        + "','100','1','0','120','dart3.xsd')\">본문</a>"
        "<a href=\"/report/viewer.do?rcpNo="
        + receipt
        + "&amp;dcmNo=100&amp;eleId=2&amp;offset=120&amp;length=50&amp;dtd=dart3.xsd\">첨부</a>"
        "</body></html>"
    ).encode("utf-8")


def _actual_shape_landing(receipt: str, dcm_no: str = "4883965") -> bytes:
    return f"""
    <html><head><title>영흥/주요사항보고서(회사합병결정)/2015.12.14</title></head><body>
    <select id="att"><option value="null">+첨부선택+</option>
      <option value="rcpNo={receipt}&amp;dcmNo=4883966">대표이사등의확인</option>
      <option value="rcpNo=20151217000199&amp;dcmNo=4888529">[정정] 대표이사등의확인</option>
    </select>
    <script>
    var node1 = {{}};
    node1['text'] = "회사합병 결정";
    node1['rcpNo'] = "{receipt}";
    node1['dcmNo'] = "{dcm_no}";
    node1['eleId'] = "3";
    node1['offset'] = "5923";
    node1['length'] = "27166";
    node1['dtd'] = "dart3.xsd";
    treeData.push(node1);
    </script></body></html>
    """.encode("utf-8")


def _fetcher(calls: list[tuple[str, dict[str, str]]]):
    def fetch(endpoint, params, _headers, _timeout):
        calls.append((endpoint, dict(params)))
        if endpoint.endswith("main.do"):
            return 200, _landing(params["rcpNo"]), {"Date": "Wed, 16 Sep 2026 00:00:00 GMT"}
        return 200, f"<html>document-{params['eleId']}</html>".encode(), {"Date": "x"}

    return fetch


def test_parse_viewdoc_and_encoded_viewer_url_are_deduplicated():
    refs = collector.parse_viewer_references(_landing(RECEIPTS[0]))
    assert [item["eleId"] for item in refs] == ["1", "2"]
    assert all(item["rcpNo"] == RECEIPTS[0] for item in refs)


def test_parse_actual_node_assignments_attachments_and_related_receipts():
    payload = _actual_shape_landing(RECEIPTS[0])
    refs = collector.parse_viewer_references(payload)
    attachments = collector.parse_attachment_references(payload, RECEIPTS[0])
    related = collector.parse_related_receipt_links(payload)
    assert refs == [
        {
            "rcpNo": RECEIPTS[0],
            "dcmNo": "4883965",
            "eleId": "3",
            "offset": "5923",
            "length": "27166",
            "dtd": "dart3.xsd",
        }
    ]
    assert [(item["rcpNo"], item["dcmNo"]) for item in attachments] == [
        (RECEIPTS[0], "4883966")
    ]
    assert {item["rcpNo"] for item in related} == {RECEIPTS[0], "20151217000199"}


def test_dry_run_has_no_network_and_no_shadow_write(tmp_path: Path):
    contract = _context(tmp_path)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("network must not run")

    result = collector.run_collection(
        root=tmp_path, contract_path=contract, fetcher=forbidden, clock=_clock()
    )
    assert result["mode"] == "DRY_RUN"
    assert result["network_requests"] == 0
    assert result["pending_receipt_count"] == 2
    assert not (tmp_path / "shadow").exists()
    assert not (tmp_path / "reports/shadow_latest.json").exists()


def test_apply_requires_explicit_shadow_confirmation(tmp_path: Path):
    contract = _context(tmp_path)
    with pytest.raises(collector.ShadowCollectionError, match="SHADOW_CONFIRMATION_REQUIRED"):
        collector.run_collection(root=tmp_path, contract_path=contract, apply=True)


def test_apply_captures_every_reference_and_preserves_capture_status(tmp_path: Path):
    contract = _context(tmp_path)
    capture_path = tmp_path / "reports/capture.json"
    before = hashlib.sha256(capture_path.read_bytes()).hexdigest()
    calls: list[tuple[str, dict[str, str]]] = []
    result = collector.run_collection(
        root=tmp_path,
        contract_path=contract,
        apply=True,
        confirm_shadow_only=True,
        max_receipts=1,
        max_network_requests=10,
        fetcher=_fetcher(calls),
        sleep_fn=lambda _seconds: None,
        clock=_clock(),
    )
    assert result["completed_receipt_count"] == 1
    assert result["pending_receipt_count"] == 1
    assert result["promotion_allowed"] is False
    assert len(calls) == 3
    journal_path = tmp_path / f"shadow/journals/pass/{RECEIPTS[0]}.json"
    journal = json.loads(journal_path.read_text(encoding="utf-8"))
    assert journal["status"] == "SHADOW_PASS"
    assert journal["document_count"] == 2
    assert len(journal["artifacts"]) == 3
    for artifact in journal["artifacts"]:
        raw = tmp_path / artifact["path"]
        assert raw.is_file()
        assert hashlib.sha256(raw.read_bytes()).hexdigest() == artifact["sha256"]
    assert hashlib.sha256(capture_path.read_bytes()).hexdigest() == before


def test_completed_receipt_is_not_requested_again(tmp_path: Path):
    contract = _context(tmp_path)
    first_calls: list[tuple[str, dict[str, str]]] = []
    collector.run_collection(
        root=tmp_path,
        contract_path=contract,
        apply=True,
        confirm_shadow_only=True,
        fetcher=_fetcher(first_calls),
        sleep_fn=lambda _seconds: None,
        clock=_clock(),
    )
    second_calls: list[tuple[str, dict[str, str]]] = []
    result = collector.run_collection(
        root=tmp_path,
        contract_path=contract,
        apply=True,
        confirm_shadow_only=True,
        fetcher=_fetcher(second_calls),
        sleep_fn=lambda _seconds: None,
        clock=_clock(),
    )
    assert second_calls[0][1]["rcpNo"] == RECEIPTS[1]
    assert result["completed_receipt_count"] == 2


def test_connection_limit_is_checkpointed_without_pass(tmp_path: Path):
    contract = _context(tmp_path)

    def limited(*_args, **_kwargs):
        return 503, b"", {}

    result = collector.run_collection(
        root=tmp_path,
        contract_path=contract,
        apply=True,
        confirm_shadow_only=True,
        fetcher=limited,
        sleep_fn=lambda _seconds: None,
        clock=_clock(),
    )
    assert result["stop_reason"] == "NA_CONNECTION_LIMIT"
    assert result["completed_receipt_count"] == 0
    assert not (tmp_path / f"shadow/journals/pass/{RECEIPTS[0]}.json").exists()
    failures = list((tmp_path / f"shadow/journals/fail/{RECEIPTS[0]}").glob("*.json"))
    assert len(failures) == 1


def test_missing_exact_reference_fails_closed(tmp_path: Path):
    contract = _context(tmp_path)

    def wrong(endpoint, params, _headers, _timeout):
        assert endpoint.endswith("main.do")
        return 200, _landing("20250101000001"), {}

    result = collector.run_collection(
        root=tmp_path,
        contract_path=contract,
        apply=True,
        confirm_shadow_only=True,
        fetcher=wrong,
        sleep_fn=lambda _seconds: None,
        clock=_clock(),
    )
    assert result["stop_reason"] == "EXACT_RECEIPT_REFERENCE_MISSING"
    assert result["completed_receipt_count"] == 0


def test_receipt_count_drift_blocks_before_network(tmp_path: Path):
    contract_path = _context(tmp_path)
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    contract["input"]["expected_receipt_count"] = 292
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(collector.ShadowCollectionError, match="UNAVAILABLE_RECEIPT_COUNT_MISMATCH"):
        collector.run_collection(root=tmp_path, contract_path=contract_path)
