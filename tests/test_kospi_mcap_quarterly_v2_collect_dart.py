"""H1 수집기 1 (DART 공시) — 가짜 응답으로. 네트워크 없음."""
from __future__ import annotations

import json
from datetime import datetime

import pytest

from paper.strategies.kospi_mcap_quarterly_v2.src import collect_dart_events as C

RULES = json.loads((C.DEFAULT_RULES).read_text(encoding="utf-8"))
NOW = datetime(2026, 9, 18, 18, 0, 0)


def _row(rc, nm, code="005930", dt="20260918"):
    return {"rcept_no": rc, "rcept_dt": dt, "corp_code": "0012345", "corp_name": "테스트", "stock_code": code,
            "corp_cls": "Y", "report_nm": nm, "flr_nm": "테스트", "rm": ""}


class FakeSession:
    def __init__(self, pages):
        self.pages = pages
        self.calls = 0

    def get(self, url, params, timeout):
        self.calls += 1
        body = self.pages[params["page_no"] - 1]

        class R:
            def json(self_inner):
                return body
        return R()


def _page(rows, total, total_page=1, status="000"):
    return {"status": status, "message": "정상", "total_count": total, "total_page": total_page, "list": rows}


@pytest.mark.parametrize("name,expect", [
    ("주요사항보고서(자기주식취득결정)", ["BUYBACK_DIRECT"]),
    ("자기주식취득신탁계약체결결정", ["BUYBACK_TRUST"]),
    ("자기주식취득신탁계약해지결정", ["BUYBACK_TRUST_END"]),
    ("자기주식처분결정", ["BUYBACK_DISPOSAL"]),
    ("유상증자결정", ["RIGHTS_OFFERING"]),
    ("단일판매ㆍ공급계약체결", []),
])
def test_classify(name, expect):
    assert [h["event_type"] for h in C.classify(name, RULES["rules"])] == expect


def test_trust_is_not_counted_as_direct_buyback():
    hits = [h["event_type"] for h in C.classify("주요사항보고서(자기주식취득신탁계약체결결정)", RULES["rules"])]
    assert "BUYBACK_DIRECT" not in hits and "BUYBACK_TRUST" in hits


def test_collect_writes_raw_and_events_and_is_idempotent(tmp_path, monkeypatch):
    pages = [_page([_row("1", "주요사항보고서(자기주식취득결정)"), _row("2", "단일판매ㆍ공급계약체결")], 2)]
    monkeypatch.setattr(C.requests, "Session", lambda: FakeSession(pages))
    r1 = C.collect("k", ["20260918"], tmp_path, RULES, NOW)
    assert r1["status"] == "OK" and r1["new_raw"] == 2 and r1["new_events"] == 1
    r2 = C.collect("k", ["20260918"], tmp_path, RULES, NOW)
    assert r2["new_raw"] == 0 and r2["new_events"] == 0  # 같은 날 다시 돌려도 안 늘어남
    ev = [json.loads(x) for x in (tmp_path / "h1_events.jsonl").read_text(encoding="utf-8").splitlines()]
    assert ev[0]["event_type"] == "BUYBACK_DIRECT" and ev[0]["direction"] == "BUY_PRESSURE"
    assert ev[0]["rules_version"] == RULES["version"]


def test_paging_collects_all_rows(tmp_path, monkeypatch):
    p1 = _page([_row(str(i), "유상증자결정") for i in range(100)], 150, total_page=2)
    p2 = _page([_row(str(100 + i), "유상증자결정") for i in range(50)], 150, total_page=2)
    monkeypatch.setattr(C.requests, "Session", lambda: FakeSession([p1, p2]))
    r = C.collect("k", ["20260918"], tmp_path, RULES, NOW)
    assert r["new_raw"] == 150 and r["status"] == "OK"


def test_partial_page_is_reported_incomplete_not_zero(tmp_path, monkeypatch):
    monkeypatch.setattr(C.requests, "Session", lambda: FakeSession([_page([_row("1", "유상증자결정")], 5)]))
    r = C.collect("k", ["20260918"], tmp_path, RULES, NOW)
    assert r["status"] == "STOP" and r["days"][0]["status"] == "INCOMPLETE"


def test_api_error_is_stop_not_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(C.requests, "Session", lambda: FakeSession([_page([], 0, status="020")]))
    r = C.collect("k", ["20260918"], tmp_path, RULES, NOW)
    assert r["status"] == "STOP" and "API_ERROR" in r["reasons"][0]


def test_no_rows_day_is_ok(tmp_path, monkeypatch):
    monkeypatch.setattr(C.requests, "Session", lambda: FakeSession([_page([], 0, status="013")]))
    r = C.collect("k", ["20260918"], tmp_path, RULES, NOW)
    assert r["status"] == "OK" and r["new_raw"] == 0


def test_rule_change_can_reclassify_from_raw(tmp_path, monkeypatch):
    # 원본을 따로 쌓는 이유: 규칙을 바꾸면 과거를 다시 분류할 수 있어야 한다
    monkeypatch.setattr(C.requests, "Session", lambda: FakeSession([_page([_row("1", "특수한신규사건공시")], 1)]))
    C.collect("k", ["20260918"], tmp_path, RULES, NOW)
    raw = [json.loads(x) for x in (tmp_path / "dart_disclosures.jsonl").read_text(encoding="utf-8").splitlines()]
    new_rules = {**RULES, "version": "1.1.0",
                 "rules": RULES["rules"] + [{"event_type": "NEW_KIND", "direction": "BUY_PRESSURE",
                                             "any": ["특수한신규사건"], "none": []}]}
    hits = [C.classify(r["report_nm"], new_rules["rules"]) for r in raw]
    assert hits[0][0]["event_type"] == "NEW_KIND"


# ---------------- 규칙 v1.1 (09-18): 종속회사 제외 / 정정 표시 / 원본에서 재분류

RULES_11 = json.loads((C.DEFAULT_RULES.parent / "h1_event_rules_v1_1.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("name,n", [
    ("주요사항보고서(유상증자결정)", 1),
    ("유상증자결정(종속회사의주요경영사항)", 0),
    ("주요사항보고서(유상증자결정)(자회사의 주요경영사항)", 0),
])
def test_subsidiary_disclosures_are_not_our_events(name, n):
    assert len(C.classify(name, RULES_11["rules"], RULES_11)) == n


def test_amendment_is_flagged_not_dropped():
    hits = C.classify("[기재정정]주요사항보고서(유상증자결정)", RULES_11["rules"], RULES_11)
    assert len(hits) == 1 and hits[0]["is_amendment"] is True
    assert C.classify("주요사항보고서(유상증자결정)", RULES_11["rules"], RULES_11)[0]["is_amendment"] is False


def test_block_sale_rule_removed_in_v1_1():
    assert C.classify("주식등의대량보유상황보고서", RULES_11["rules"], RULES_11) == []


def test_rebuild_reclassifies_from_raw_and_keeps_old_file(tmp_path, monkeypatch):
    pages = [_page([_row("1", "주요사항보고서(자기주식취득결정)"),
                    _row("2", "주식등의대량보유상황보고서"),
                    _row("3", "유상증자결정(종속회사의주요경영사항)")], 3)]
    monkeypatch.setattr(C.requests, "Session", lambda: FakeSession(pages))
    C.collect("k", ["20260918"], tmp_path, RULES, NOW)          # v1.0 로 수집
    before = len((tmp_path / "h1_events.jsonl").read_text(encoding="utf-8").strip().splitlines())
    r = C.rebuild(tmp_path, RULES_11, NOW)                       # v1.1 로 재분류
    after = [json.loads(x) for x in (tmp_path / "h1_events.jsonl").read_text(encoding="utf-8").splitlines()]
    assert before == 3 and r["events"] == 1 and len(after) == 1  # 대량보유·종속회사 빠짐
    assert after[0]["event_type"] == "BUYBACK_DIRECT" and after[0]["rules_version"] == "1.1.0"
    assert list(tmp_path.glob("h1_events_before_1.1.0_*.jsonl")), "옛 분류 파일이 보존돼야 한다"
    assert len([json.loads(x) for x in (tmp_path / "dart_disclosures.jsonl").read_text(encoding="utf-8").splitlines()]) == 3
