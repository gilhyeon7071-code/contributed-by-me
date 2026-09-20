from __future__ import annotations

import io
import zipfile

from tools import analyze_kospi_mcap_quarterly_c8_date_residuals as analyzer


DATE_PATTERNS = [r"([0-9]{4})[.년/-][ ]*([0-9]{1,2})[.월/-][ ]*([0-9]{1,2})"]
PARTIAL_PATTERN = r"(?<![0-9])([0-9]{1,2})[ ]*/[ ]*([0-9]{1,2})(?![0-9])"


def _zip(name: str, text: str) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(name, text.encode("utf-8"))
    return output.getvalue()


def test_table_parser_preserves_schedule_row_cells():
    rows = analyzer.parse_table_rows(
        "<table><tr><td>합병 기일</td><td>2014. 12. 29.</td></tr></table>".encode()
    )
    assert rows == [["합병 기일", "2014. 12. 29."]]


def test_schedule_row_evidence_accepts_space_variant_full_date():
    evidence = analyzer.extract_schedule_row_evidence(
        _zip(
            "receipt.xml",
            "<table><tr><td>합병 기일</td><td>2014. 12. 29.</td><td>2014. 12. 29.</td></tr></table>",
        ),
        label_prefix="합병기일",
        date_patterns=DATE_PATTERNS,
        partial_date_pattern=PARTIAL_PATTERN,
    )
    assert evidence[0]["full_dates"] == ["20141229"]


def test_schedule_row_evidence_keeps_yearless_date_blocked():
    evidence = analyzer.extract_schedule_row_evidence(
        _zip("receipt.xml", "<table><tr><td>합병기일</td><td>6/1</td></tr></table>"),
        label_prefix="합병기일",
        date_patterns=DATE_PATTERNS,
        partial_date_pattern=PARTIAL_PATTERN,
    )
    assert evidence[0]["full_dates"] == []
    assert evidence[0]["partial_dates"] == ["06-01"]


def test_viewer_node_titles_identify_official_schedule_node():
    landing = b"""
    <script>
      var node1 = {};
      node1['text'] = "I. placeholder";
      node1['eleId'] = "1";
      treeData.push(node1);
      var node2 = {};
      node2['text'] = "\xe2\x85\xa0. \xec\x9d\xbc\xec\xa0\x95";
      node2['eleId'] = "3";
      treeData.push(node2);
    </script>
    """
    assert analyzer.parse_viewer_node_titles(landing)["3"] == "Ⅰ. 일정"


def test_schedule_row_candidate_ignores_non_schedule_footnote_conflict():
    classification = analyzer.classify_residual(
        source="OPENDART_PASS",
        schedule_row_dates=["20221227"],
        schedule_node_dates=[],
        partial_dates=[],
        non_merger_content=False,
    )
    assert classification == (
        "SCHEDULE_ROW_EXACT_DATE_CANDIDATE",
        "20221227",
        "DART_COMPLETION_SCHEDULE_TABLE_ROW",
    )


def test_partial_and_non_merger_cases_remain_blocked():
    assert analyzer.classify_residual(
        source="OPENDART_PASS",
        schedule_row_dates=[],
        schedule_node_dates=[],
        partial_dates=["06-01"],
        non_merger_content=False,
    )[0] == "PARTIAL_DATE_YEAR_REQUIRED_BLOCKED"
    assert analyzer.classify_residual(
        source="OPENDART_PASS",
        schedule_row_dates=[],
        schedule_node_dates=[],
        partial_dates=[],
        non_merger_content=True,
    )[0] == "NON_MERGER_CONTENT_BLOCKED"
