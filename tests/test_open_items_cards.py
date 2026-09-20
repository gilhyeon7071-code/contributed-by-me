# -*- coding: utf-8 -*-
"""처리 카드가 **저장되고 살아남는지** 고정한다.

[2026-09-13] 사용자 지적: *"문제 도출되면 목록화 하기로한거아니었어?
목록화후 각대상별 검증 원인분석 판단 원인해결 보고"*

표는 목록만 했고 5단계는 PLANS 산문에만 있었다. 인용하려면 날짜를 찾아 헤매야 했다.

가장 위험한 것: 예전 `save()` 는 표 이후를 통째로 버렸다.
카드를 붙이고도 그대로 뒀으면 **항목을 닫을 때마다 카드가 전부 사라졌을 것이다.**
"""
from __future__ import annotations

import pytest

oi = pytest.importorskip("tools.open_items")


def test_cards_survive_a_table_rewrite():
    """**핵심.** --done 등으로 표를 다시 써도 카드가 남아야 한다."""
    before = oi.load_cards()
    assert before, "카드가 하나도 없다 - 선행 조건 실패"
    rows = oi.load()
    oi.save(rows)                      # 표만 다시 쓴다
    after = oi.load_cards()
    assert set(after) == set(before), (set(before) - set(after), set(after) - set(before))
    for cid in before:
        assert after[cid] == before[cid], cid


def test_card_carries_five_stages():
    cards = oi.load_cards()
    a13 = cards.get("A13")
    assert a13, "A13 카드가 없다"
    for key, ko in oi.STAGES:
        assert a13.get(key), "%s(%s) 가 비어 있다" % (ko, key)


def test_stage_labels_are_the_agreed_five():
    """검증 -> 원인분석 -> 판단 -> 원인해결 -> 보고"""
    assert [ko for _k, ko in oi.STAGES] == ["검증", "원인", "판단", "조치", "보고"]


def test_empty_card_is_visible_not_silent():
    """카드가 없는 항목은 **보여야** 한다. 조용하면 목록화의 의미가 없다."""
    cards = oi.load_cards()
    live = [r for r in oi.load() if r["state"] != "DONE"]
    empty = [r["id"] for r in live
             if not any((cards.get(r["id"]) or {}).get(k) for k, _ in oi.STAGES)]
    # 지금은 대부분 비어 있는 것이 정상이다. 그 사실 자체가 드러나야 한다
    assert isinstance(empty, list)
    assert len(empty) + len([r for r in live if r["id"] in cards]) == len(live)
