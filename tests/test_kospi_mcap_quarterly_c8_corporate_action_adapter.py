from __future__ import annotations

import io
import json
import zipfile

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_corporate_action_adapter import (
    _date_after_label,
    _document_receipt_references,
    build_corporate_action_adapter_output,
    build_list_request_spec,
    list_page_key,
    parse_disclosure_list_page,
    parse_corp_code_package,
    run_corporate_action_connectivity_probe,
)


CODE = "005930"
CORP = "00126380"
START = "20260101"
AS_OF = "20260915"
DATE_PATTERNS = [r"([0-9]{4})[.년/-][ ]*([0-9]{1,2})[.월/-][ ]*([0-9]{1,2})"]


def _zip(name: str, text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(name, text.encode("utf-8"))
    return buffer.getvalue()


def test_document_receipt_reference_preserves_html_href_authority():
    payload = _zip(
        "document.html",
        '<a href="/dsaf001/main.do?rcpNo=20260102000001">관련 공시</a>',
    )
    references = _document_receipt_references(
        payload, [r"rcpNo[^0-9]{0,8}([0-9]{14})"]
    )
    assert references == {"20260102000001"}


def _zip_members(members: dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, text in members.items():
            archive.writestr(name, text.encode("utf-8"))
    return buffer.getvalue()


def _viewer_landing(schedule_ele_id: str = "3") -> bytes:
    return f"""
    <script>
      var node1 = {{}};
      node1['text'] = "I. placeholder";
      node1['eleId'] = "1";
      treeData.push(node1);
      var node2 = {{}};
      node2['text'] = "Ⅰ. 일정";
      node2['eleId'] = "{schedule_ele_id}";
      treeData.push(node2);
    </script>
    """.encode("utf-8")


def _corp_payload() -> bytes:
    return _zip(
        "CORPCODE.xml",
        "<result><list><corp_code>00126380</corp_code><corp_name>TEST</corp_name>"
        "<stock_code>005930</stock_code><modify_date>20260915</modify_date></list></result>",
    )


def _no_data() -> bytes:
    return json.dumps({"status": "013", "message": "no data"}).encode("utf-8")


def _list_page(rows: list[dict[str, str]], *, page_no: int = 1, total_page: int = 1) -> bytes:
    return json.dumps(
        {
            "status": "000",
            "message": "normal",
            "page_no": page_no,
            "page_count": 100,
            "total_count": len(rows),
            "total_page": total_page,
            "list": rows,
        },
        ensure_ascii=False,
    ).encode("utf-8")


def _row(receipt: str, date: str, report_name: str) -> dict[str, str]:
    return {
        "corp_code": CORP,
        "stock_code": CODE,
        "report_nm": report_name,
        "rcept_no": receipt,
        "rcept_dt": date,
        "rm": "",
    }


def _pages(decision: bytes, completion: bytes | None = None) -> dict[str, bytes]:
    return {
        list_page_key(CORP, "MERGER_DECISION_HISTORY", 1): decision,
        list_page_key(CORP, "MERGER_COMPLETION_HISTORY", 1): completion or _no_data(),
    }


def _build(
    pages: dict[str, bytes],
    documents: dict[str, bytes] | None = None,
    source_contexts: dict[str, str] | None = None,
    viewer_landings: dict[str, bytes] | None = None,
    supplemental_rows: list[dict[str, str]] | None = None,
    supplemental_documents: dict[str, bytes] | None = None,
):
    return build_corporate_action_adapter_output(
        selection_as_of=AS_OF,
        earliest_listed_date=START,
        base_codes=[CODE],
        corp_code_payload=_corp_payload(),
        list_page_payloads=pages,
        document_payloads=documents or {},
        supplemental_decision_rows=supplemental_rows,
        supplemental_document_payloads=supplemental_documents,
        completion_source_context_by_receipt=source_contexts,
        viewer_landing_payload_by_receipt=viewer_landings,
    )


def test_targeted_broad_b_supplement_can_supply_missing_initial_decision():
    initial = "20260102000001"
    completion = "20260912000649"
    completion_page = _list_page(
        [_row(completion, "20260912", "합병등종료보고서(합병)")]
    )
    supplemental_row = {
        **_row(initial, "20260102", "주요사항보고서(합병결정)"),
        "series": "MERGER_DECISION_HISTORY",
        "source_series": "BROAD_B_LEGACY_SUPPLEMENT",
    }
    report = _build(
        _pages(_no_data(), completion_page),
        {
            completion: _zip(
                "completion.xml",
                f"<doc>회사합병 접수번호 {initial} 합병기일 2026.09.01</doc>",
            )
        },
        {completion: "OPENDART_PASS"},
        supplemental_rows=[supplemental_row],
        supplemental_documents={
            initial: _zip("initial.xml", "<doc>회사합병 합병기일 2026.09.01</doc>")
        },
    )
    assert report["status"] == "PASS"
    assert report["supplemental_decision_count"] == 1
    assert len(report["interval_rows"]) == 2


def test_corp_code_zip_is_parsed_without_persisting_raw_payload():
    report = parse_corp_code_package(_corp_payload())
    assert report["status"] == "PASS"
    assert report["mapping"] == {CODE: CORP}
    assert len(report["payload_sha256"]) == 64


def test_date_after_label_uses_nearest_date_when_later_dates_share_window():
    text = "합병기일 2016.01.01 2016.01.01 합병종료보고일 2016.01.04 합병등기일 2016.01.05"
    assert _date_after_label(text, ["합병기일"], DATE_PATTERNS) == "20160101"


def test_date_after_label_fails_closed_when_nearest_dates_conflict():
    text = "합병기일 2016.01.01 다른 표 합병기일 2016.01.02"
    assert _date_after_label(text, ["합병기일"], DATE_PATTERNS) == ""


def test_structured_opendart_fallback_accepts_unique_schedule_row_only():
    initial = "20260102000001"
    completion = "20260912000649"
    decision_page = _list_page(
        [_row(initial, "20260102", "주요사항보고서(회사합병결정)")]
    )
    completion_page = _list_page(
        [_row(completion, "20260912", "합병등종료보고서(합병)")]
    )
    documents = {
        initial: _zip("initial.xml", "<doc>회사합병 합병기일 2026년 9월 1일</doc>"),
        completion: _zip(
            "completion.xml",
            f"<doc>회사합병 접수번호 {initial}"
            "<table><tr><td>합병기일</td><td>2026.09.01</td></tr></table>"
            "<p>재무주석 합병기일 2026.09.02</p></doc>",
        ),
    }
    report = _build(
        _pages(decision_page, completion_page),
        documents,
        {initial: "OPENDART_PASS", completion: "OPENDART_PASS"},
    )
    assert report["status"] == "PASS"
    assert report["completion_actual_date_count"] == 1
    assert report["completion_actual_date_authority_counts"] == {
        "DART_COMPLETION_SCHEDULE_TABLE_ROW": 1
    }
    assert report["completion_structured_fallback_rows"] == [
        {
            "receipt_no": completion,
            "source": "OPENDART_PASS",
            "actual_effective_date": "20260901",
            "authority": "DART_COMPLETION_SCHEDULE_TABLE_ROW",
        }
    ]
    assert report["interval_rows"][1]["event_effective_date"] == "20260901"


def test_structured_viewer_fallback_uses_only_official_schedule_node():
    initial = "20260102000001"
    completion = "20260912000367"
    decision_page = _list_page(
        [_row(initial, "20260102", "주요사항보고서(회사합병결정)")]
    )
    completion_page = _list_page(
        [_row(completion, "20260912", "합병등종료보고서(합병)")]
    )
    completion_package = _zip_members(
        {
                "001_6317926_1.html": (
                    f"<p>회사합병 접수번호 {initial}</p>"
                    "<table><tr><td>합병기일</td><td>2026.07.01</td></tr>"
                    "<tr><td>합병기일</td><td>2026.09.01</td></tr></table>"
                ),
                "003_6317926_3.html": (
                    "<table><tr><td>합병기일</td><td>2026.09.01</td></tr></table>"
                ),
        }
    )
    report = _build(
        _pages(decision_page, completion_page),
        {
            initial: _zip("initial.xml", "<doc>회사합병 합병기일 2026년 7월 1일</doc>"),
            completion: completion_package,
        },
        {initial: "OPENDART_PASS", completion: "VIEWER_FALLBACK"},
        {completion: _viewer_landing()},
    )
    assert report["status"] == "PASS"
    assert report["completion_actual_date_authority_counts"] == {
        "DART_VIEWER_TREE_SCHEDULE_NODE_TABLE_ROW": 1
    }
    assert report["completion_structured_fallback_rows"][0]["receipt_no"] == completion
    assert report["interval_rows"][1]["event_effective_date"] == "20260901"


def test_existing_nonempty_completion_date_is_never_overridden():
    initial = "20260102000001"
    completion = "20260715000002"
    report = _build(
        _pages(
            _list_page([_row(initial, "20260102", "주요사항보고서(회사합병결정)")]),
            _list_page([_row(completion, "20260715", "합병등종료보고서(합병)")]),
        ),
        {
            initial: _zip("initial.xml", "<doc>회사합병 합병기일 2026년 7월 1일</doc>"),
            completion: _zip(
                "completion.xml",
                f"<doc>회사합병 접수번호 {initial}<p>합병기일 2026.07.01</p>"
                "<table><tr><td>합병 기일</td><td>2026.08.01</td></tr></table></doc>",
            ),
        },
        {initial: "OPENDART_PASS", completion: "OPENDART_PASS"},
    )
    assert report["status"] == "PASS"
    assert report["completion_actual_date_authority_counts"] == {
        "DART_LABEL_NEAREST_DATE": 1
    }
    assert report["interval_rows"][1]["event_effective_date"] == "20260701"


def test_viewer_fallback_without_landing_context_fails_closed():
    initial = "20260102000001"
    completion = "20260912000367"
    report = _build(
        _pages(
                _list_page([_row(initial, "20260102", "주요사항보고서(회사합병결정)")]),
                _list_page([_row(completion, "20260912", "합병등종료보고서(합병)")]),
        ),
        {
                initial: _zip("initial.xml", "<doc>회사합병 합병기일 2026년 7월 1일</doc>"),
                completion: _zip_members(
                    {
                        "001_6317926_1.html": (
                            f"<p>회사합병 접수번호 {initial}</p>"
                            "<table><tr><td>합병기일</td><td>2026.07.01</td></tr></table>"
                        ),
                    "003_6317926_3.html": (
                            "<table><tr><td>합병기일</td><td>2026.09.01</td></tr></table>"
                    ),
                }
            ),
        },
        {initial: "OPENDART_PASS", completion: "VIEWER_FALLBACK"},
    )
    assert report["status"] == "FAIL"
    assert "VIEWER_LANDING_CONTEXT_MISSING" in report["reason_codes"]
    assert report["completion_actual_date_count"] == 0
    assert report["interval_rows"] == []


def test_partial_completion_date_is_not_inferred_from_receipt_year():
    initial = "20260102000001"
    completion = "20260605000425"
    report = _build(
        _pages(
            _list_page([_row(initial, "20260102", "주요사항보고서(회사합병결정)")]),
            _list_page([_row(completion, "20260605", "합병등종료보고서(합병)")]),
        ),
        {
            initial: _zip("initial.xml", "<doc>회사합병 합병기일 2026년 6월 1일</doc>"),
            completion: _zip(
                "completion.xml",
                f"<doc>회사합병 접수번호 {initial}"
                "<table><tr><td>합병 기일</td><td>6/1</td></tr></table></doc>",
            ),
        },
        {initial: "OPENDART_PASS", completion: "OPENDART_PASS"},
    )
    assert report["status"] == "FAIL"
    assert report["completion_actual_date_count"] == 0
    assert report["completion_missing_receipts"] == [completion]
    assert report["interval_rows"] == []


def test_exhaustive_hashed_no_data_pages_prove_empty_history_for_one_code():
    report = _build(_pages(_no_data()))
    assert report["status"] == "PASS"
    assert report["accepted_disclosure_count"] == 0
    assert report["interval_rows"] == []
    assert report["coverage"]["pagination_complete_by_corp_code"] == {CORP: True}
    assert report["raw_payload_persisted"] is False
    assert report["orders_allowed"] is False


def test_revision_and_cancellation_build_one_closed_pending_interval():
    initial = "20260102000001"
    revision = "20260303000002"
    cancellation = "20260404000003"
    decision = _list_page(
        [
            _row(initial, "20260102", "주요사항보고서(회사합병결정)"),
            _row(revision, "20260303", "[기재정정] 주요사항보고서(회사합병결정)"),
            _row(cancellation, "20260404", "[기재정정] 주요사항보고서(회사합병결정)"),
        ]
    )
    documents = {
        initial: _zip("initial.xml", "<doc>회사합병 합병기일 2026년 7월 1일</doc>"),
        revision: _zip(
            "revision.xml",
            f"<doc>회사합병 접수번호 {initial} 합병기일 2026년 8월 1일</doc>",
        ),
        cancellation: _zip(
            "cancel.xml",
            f"<doc>회사합병 합병결정 취소 rcpNo={initial}</doc>",
        ),
    }
    report = _build(_pages(decision), documents)
    assert report["status"] == "PASS"
    assert report["event_count"] == 3
    assert report["interval_rows"] == [
        {
            "code": CODE,
            "effective_from": "20260102",
            "effective_to": "20260404",
            "merger_status": "PENDING",
            "event_type": "MERGER",
            "event_effective_date": "",
        }
    ]


def test_completion_document_supplies_actual_date_and_terminal_marker():
    initial = "20260102000001"
    completion = "20260715000002"
    decision_page = _list_page(
        [_row(initial, "20260102", "주요사항보고서(회사합병결정)")]
    )
    completion_page = _list_page(
        [_row(completion, "20260715", "합병등종료보고서(합병)")]
    )
    documents = {
        initial: _zip("initial.xml", "<doc>회사합병 합병기일 2026년 7월 1일</doc>"),
        completion: _zip(
            "completion.xml",
            f"<doc>회사합병 접수번호 {initial} 합병기일 2026년 7월 1일</doc>",
        ),
    }
    report = _build(_pages(decision_page, completion_page), documents)
    assert report["status"] == "PASS"
    assert report["interval_rows"][1] == {
        "code": CODE,
        "effective_from": "20260715",
        "effective_to": "20260716",
        "merger_status": "EFFECTIVE",
        "event_type": "MERGER",
        "event_effective_date": "20260701",
    }


def test_completion_list_accepts_only_merger_subtype_after_revision_prefix_strip():
    merger = "20260715000002"
    split = "20260715000003"
    asset_transfer = "20260715000004"
    spec = build_list_request_spec(
        corp_code=CORP,
        query_start=START,
        query_end=AS_OF,
        series="MERGER_COMPLETION_HISTORY",
    )
    report = parse_disclosure_list_page(
        _list_page(
            [
                _row(merger, "20260715", "[기재정정] 합병등종료보고서(합병)"),
                _row(split, "20260715", "합병등종료보고서(분할)"),
                _row(asset_transfer, "20260715", "합병등종료보고서(자산양수도)"),
            ]
        ),
        request_spec=spec,
        expected_stock_code=CODE,
    )
    assert report["status"] == "PASS"
    assert [row["rcept_no"] for row in report["accepted_rows"]] == [merger]


def test_missing_second_page_fails_closed_with_no_intervals():
    first_page = _list_page([], page_no=1, total_page=2)
    report = _build(_pages(first_page))
    assert report["status"] == "FAIL"
    assert "DART_LIST_PAGE_MISSING" in report["reason_codes"]
    assert report["interval_rows"] == []


def test_revision_without_explicit_initial_receipt_fails_closed():
    initial = "20260102000001"
    revision = "20260303000002"
    decision = _list_page(
        [
            _row(initial, "20260102", "주요사항보고서(회사합병결정)"),
            _row(revision, "20260303", "[기재정정] 주요사항보고서(회사합병결정)"),
        ]
    )
    report = _build(
        _pages(decision),
        {
            initial: _zip("initial.xml", "<doc>회사합병 합병기일 2026년 7월 1일</doc>"),
            revision: _zip("revision.xml", "<doc>회사합병 합병기일 2026년 8월 1일</doc>"),
        },
    )
    assert report["status"] == "FAIL"
    assert "EVENT_IDENTITY_UNRESOLVED" in report["reason_codes"]
    assert report["interval_rows"] == []


def test_connectivity_probe_with_injected_responses_is_non_persisting():
    def fetcher(endpoint: str, params: dict[str, str], timeout: float):
        assert timeout == 5.0
        assert params["crtfc_key"] == "test-key"
        if endpoint.endswith("corpCode.xml"):
            return 200, _corp_payload()
        assert endpoint.endswith("list.json")
        return 200, _no_data()

    report = run_corporate_action_connectivity_probe(
        selection_as_of=AS_OF,
        query_start="20260901",
        stock_code=CODE,
        api_key="test-key",
        timeout=5.0,
        fetcher=fetcher,
    )
    assert report["status"] == "PASS"
    assert report["sample_adapter_status"] == "PASS"
    assert report["full_capture_evidence"] is False
    assert report["raw_payload_persisted"] is False
    assert report["auth_key_persisted"] is False


def test_connectivity_exception_fails_closed_without_leaking_exception_or_key():
    def fetcher(endpoint: str, params: dict[str, str], timeout: float):
        raise RuntimeError(f"secret={params['crtfc_key']}")

    report = run_corporate_action_connectivity_probe(
        selection_as_of=AS_OF,
        query_start="20260901",
        stock_code=CODE,
        api_key="must-not-leak",
        timeout=5.0,
        fetcher=fetcher,
    )
    serialized = json.dumps(report)
    assert report["status"] == "FAIL"
    assert report["reason_codes"] == ["DART_CORP_CODE_REQUEST_FAILED"]
    assert report["request_checks"][0]["http_status"] == 0
    assert "must-not-leak" not in serialized
    assert "secret=" not in serialized
    assert report["orders_allowed"] is False


def test_request_spec_never_contains_api_key():
    spec = build_list_request_spec(
        corp_code=CORP,
        query_start=START,
        query_end=AS_OF,
        series="MERGER_DECISION_HISTORY",
    )
    assert "crtfc_key" not in spec["params"]
    assert spec["params"]["last_reprt_at"] == "N"
    assert spec["params"]["page_count"] == "100"
