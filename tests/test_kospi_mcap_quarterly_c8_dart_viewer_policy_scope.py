from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools import analyze_kospi_mcap_quarterly_c8_dart_viewer_policy_scope as analyzer


def _family_html() -> bytes:
    return """
    <html><body>
      <select id="att">
        <option value="rcpNo=20260102000001&amp;dcmNo=10">attachment</option>
      </select>
      <select class="w01" id="family" onchange="changeFamily(this.value)">
        <option value="null">+본문선택+</option>
        <option value="rcpNo=20260303000002" selected title="주요사항보고서(회사합병결정)">
          2026.03.03 [정정] 주요사항보고서(회사합병결정)
        </option>
        <option value="rcpNo=20260102000001" title="주요사항보고서(회사합병결정)">
          2026.01.02 주요사항보고서(회사합병결정)
        </option>
      </select>
    </body></html>
    """.encode("utf-8")


def _contract(root: Path) -> Path:
    path = root / "contract.json"
    path.write_text(
        json.dumps(
            {
                "strategy_id": analyzer.STRATEGY_ID,
                "mode": "VALIDATION_ONLY",
                "execution_permissions": {
                    "network_allowed": False,
                    "capture_status_change_allowed": False,
                },
            }
        ),
        encoding="utf-8",
    )
    return path


def test_family_parser_separates_selected_correction_and_original():
    refs = analyzer.parse_official_family_references(_family_html())
    assert [item["receipt_no"] for item in refs] == [
        "20260303000002",
        "20260102000001",
    ]
    assert refs[0]["selected"] is True
    assert refs[0]["is_correction"] is True
    assert refs[1]["selected"] is False
    assert refs[1]["is_correction"] is False


def test_family_parser_does_not_mix_attachment_select():
    refs = analyzer.parse_official_family_references(_family_html())
    assert len(refs) == 2
    assert all("dcmNo" not in item for item in refs)


def test_family_parser_returns_empty_when_official_select_is_absent():
    assert analyzer.parse_official_family_references(b"<html><body>none</body></html>") == []


def test_nearest_date_uses_label_adjacent_date_not_later_table_dates():
    text = "합병기일 2016.01.01 2016.01.01 합병종료보고일 2016.01.04 합병등기일 2016.01.05"
    patterns = [r"([0-9]{4})[.년/-][ ]*([0-9]{1,2})[.월/-][ ]*([0-9]{1,2})"]
    assert analyzer.nearest_date_after_label(text, ["합병기일"], patterns) == "20160101"


def test_nearest_date_rejects_ambiguous_same_distance():
    text = "합병기일 2016.01.01"
    patterns = [
        r"([0-9]{4})[.]([0-9]{1,2})[.]([0-9]{1,2})",
        r"([0-9]{4})[.]([0-9]{1,2})[.]([0-9]{1,2})",
    ]
    assert analyzer.nearest_date_after_label(text, ["합병기일"], patterns) == "20160101"


def test_nested_attachment_correction_prefix_is_detected():
    assert analyzer.is_attachment_correction(
        "[정정명령부과][첨부정정]주요사항보고서(회사합병결정)"
    )
    assert not analyzer.is_attachment_correction(
        "[기재정정]주요사항보고서(회사합병결정)"
    )


def test_contract_rejects_enabled_permission(tmp_path: Path):
    contract = _contract(tmp_path)
    payload = json.loads(contract.read_text(encoding="utf-8"))
    payload["execution_permissions"]["network_allowed"] = True
    contract.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(analyzer.PolicyScopeError, match="POLICY_SCOPE_PERMISSIONS_INVALID"):
        analyzer.load_policy_scope_contract(tmp_path, contract)
