from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pytest

from tools.collect_kospi_mcap_quarterly_c8_broad_b_legacy_supplement import (
    BroadBSupplementError,
    _document_status,
    _is_merger_decision,
    _parse_list_payload,
)


def _contract() -> dict:
    return {
        "decision_filter": {
            "required_normalized_fragment": "합병결정",
            "forbidden_normalized_fragments": ["종료보고서"],
            "revision_prefixes": ["[기재정정]", "[첨부정정]"],
        }
    }


def _target() -> dict:
    return {
        "stock_code": "000680",
        "corp_code": "00104698",
        "query_start": "19560303",
        "query_end": "20130909",
    }


def _payload(rows: list[dict], *, page_no: int = 1, total_page: int = 1) -> bytes:
    return json.dumps(
        {
            "status": "000",
            "message": "정상",
            "page_no": page_no,
            "page_count": 100,
            "total_count": len(rows),
            "total_page": total_page,
            "list": rows,
        },
        ensure_ascii=False,
    ).encode("utf-8")


def _row(report_name: str = "주요사항보고서(합병결정)") -> dict:
    return {
        "corp_code": "00104698",
        "corp_name": "대상",
        "stock_code": "000680",
        "corp_cls": "Y",
        "report_nm": report_name,
        "rcept_no": "20090403001135",
        "flr_nm": "대상",
        "rcept_dt": "20090403",
        "rm": "",
    }


def test_filter_accepts_legacy_merger_decision_and_revision() -> None:
    contract = _contract()
    assert _is_merger_decision("주요사항보고서(합병결정)", contract)
    assert _is_merger_decision("[기재정정] 회사합병결정", contract)


def test_filter_rejects_unrelated_and_completion() -> None:
    contract = _contract()
    assert not _is_merger_decision("타법인주식및출자증권취득결정", contract)
    assert not _is_merger_decision("합병등종료보고서(합병결정)", contract)


def test_parse_list_keeps_only_merger_decisions() -> None:
    rows = [_row(), _row("영업양수결정")]
    rows[1]["rcept_no"] = "20090403009999"
    parsed = _parse_list_payload(_payload(rows), target=_target(), page_no=1, contract=_contract())
    assert parsed["dart_status"] == "000"
    assert [row["rcept_no"] for row in parsed["rows"]] == ["20090403001135"]


def test_parse_list_blocks_cross_stock_identity() -> None:
    row = _row()
    row["stock_code"] = "999999"
    with pytest.raises(BroadBSupplementError, match="BROAD_B_LIST_ROW_IDENTITY_MISMATCH"):
        _parse_list_payload(_payload([row]), target=_target(), page_no=1, contract=_contract())


def test_parse_list_accepts_no_data_status() -> None:
    payload = json.dumps({"status": "013", "message": "조회된 데이타가 없습니다."}).encode()
    parsed = _parse_list_payload(payload, target=_target(), page_no=1, contract=_contract())
    assert parsed == {"dart_status": "013", "page_no": 1, "total_page": 0, "rows": []}


def test_document_status_requires_nonempty_package() -> None:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("report.xml", "<DOCUMENT>합병</DOCUMENT>")
    assert _document_status(stream.getvalue()) == "PASS"
    assert _document_status(b"not-a-package") == "INVALID"


def test_document_status_preserves_dart_error() -> None:
    assert _document_status(b"<result><status>014</status></result>") == "014"
