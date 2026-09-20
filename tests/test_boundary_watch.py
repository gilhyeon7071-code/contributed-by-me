# -*- coding: utf-8 -*-
"""불가침 경계 감시가 **실제로 잡는지** 고정한다.

[2026-09-13] 왜 있나: 2026-09-12 에 내가 `VIBE_Investor_Flow_Daily` 를
16:30 -> 20:30 으로 옮겼다. `OBJECTIVE_LEDGER.md` 의 O5 불가침 경계가 명시적으로
금지한 것인데 그 문서를 읽지 않았다.

**스킬로는 안 막힌다** - 스킬은 불러야 뜨고, 실패는 "안 불렀다" 가 아니라
"아무것도 안 부르고 시작했다" 였다. 그래서 매일 기계적으로 재는 장치를 둔다.
이 시험이 지키는 것은 **그 장치가 조용해지지 않는 것**이다.
"""
from __future__ import annotations

import pytest

bw = pytest.importorskip("tools.boundary_watch")


def test_boundaries_are_read_from_the_document():
    """하드코딩이 아니라 문서에서 읽는다. 문서가 바뀌면 이 도구도 따라가야 한다."""
    b = bw.parse_boundaries()
    assert b["ok"], b["reason"]
    assert "VIBE_Investor_Flow_Daily" in b["tasks"], b["tasks"]
    assert "VIBE_ETF_Daily_1640" in b["tasks"], b["tasks"]
    assert "tools/load_merged_panel.py" in b["files"], b["files"]
    assert "tools/fetch_investor_flow.py" in b["files"], b["files"]
    assert "RD_20260831_flow_h10" in (b.get("rounds") or []), b.get("rounds")


def test_schedule_move_is_detected():
    """2026-09-12 에 실제로 일어난 위반을 잡는가."""
    prev = {"tasks": {"VIBE_Investor_Flow_Daily": {"exists": True, "start_hhmm": "16:30"}},
            "files": {}}
    cur = {"tasks": {"VIBE_Investor_Flow_Daily": {"exists": True, "start_hhmm": "20:30"}},
           "files": {}}
    ch = bw.diff(prev, cur)
    assert len(ch) == 1 and "16:30" in ch[0] and "20:30" in ch[0], ch


def test_baseline_file_change_is_detected():
    prev = {"tasks": {}, "files": {"tools/load_merged_panel.py": {"sha256_16": "b816aa287dde"}}}
    cur = {"tasks": {}, "files": {"tools/load_merged_panel.py": {"sha256_16": "2a36ccfd181f"}}}
    assert len(bw.diff(prev, cur)) == 1


def test_no_change_is_silent():
    """매일 울리면 무시된다."""
    cur = {"tasks": {"X": {"a": 1}}, "files": {"y": {"b": 2}}}
    assert bw.diff(cur, cur) == []


def test_first_record_does_not_alarm():
    assert bw.diff(None, {"tasks": {"X": {}}, "files": {}}) == []


def test_disappearing_target_is_detected():
    """경계 대상이 **사라지는 것**도 변화다. 조용히 없어지면 안 된다."""
    prev = {"tasks": {"VIBE_ETF_Daily_1640": {"exists": True}}, "files": {}}
    cur = {"tasks": {"VIBE_ETF_Daily_1640": {"exists": False}}, "files": {}}
    assert len(bw.diff(prev, cur)) == 1


def test_unparseable_document_is_not_treated_as_no_boundary(monkeypatch, tmp_path):
    """**경계를 못 읽은 것을 '경계 없음' 으로 흡수하지 않는다.**"""
    empty = tmp_path / "OBJECTIVE_LEDGER.md"
    empty.write_text("# 아무 내용 없음\n", encoding="utf-8")
    monkeypatch.setattr(bw, "OBJ", empty, raising=False)
    b = bw.parse_boundaries()
    assert b["ok"] is False and b["reason"], b
