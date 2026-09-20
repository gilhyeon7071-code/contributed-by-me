# -*- coding: utf-8 -*-
"""뉴스 시간대 라벨이 애프터마켓(2026-09-14~, 16:00~20:00)을 장중으로 보는지 고정한다.

[2026-09-12] 미결 대장 C21. 라벨이 수집 예산(심볼수·TTL·타임아웃)을 정한다.
15:30~20:00 을 통째로 evening(장외)으로 부르면, 09-14 부터는 **장이 열려 있는 동안**
장외 예산으로 수집한다. 그 시간의 뉴스가 다른 밀도로 들어오고 나중에 비교가 안 된다.

개설일 **전**의 판정이 안 바뀌는 것까지 같이 본다 - 과거 기록의 의미를 소급해서
바꾸면 안 되기 때문이다.
"""
from __future__ import annotations

import datetime as dt
import pytest

loop = pytest.importorskip("intraday_paper_loop")

KST = loop.KST


def _at(ymd: str, hhmm: str) -> dt.datetime:
    return dt.datetime.strptime(ymd + hhmm, "%Y%m%d%H%M").replace(tzinfo=KST)


def test_aftermarket_window_is_intraday_from_opening_day():
    # 2026-09-14 는 월요일이고 holidays.json 에 없다 (실측)
    assert loop._offhours_news_session(_at("20260914", "1605")) == "intraday"
    assert loop._offhours_news_session(_at("20260914", "1930")) == "intraday"


def test_before_opening_day_the_label_is_unchanged():
    """09-11(금) 16:05 는 예전대로 evening 이어야 한다. 과거를 소급하지 않는다."""
    assert loop._offhours_news_session(_at("20260911", "1605")) == "evening"
    assert loop._offhours_news_session(_at("20260911", "1930")) == "evening"


def test_boundaries():
    # 15:30~16:00 은 애프터마켓 전이라 장외다 (정규장 마감 ~ 애프터마켓 개시 사이)
    assert loop._offhours_news_session(_at("20260914", "1545")) == "evening"
    # 20:00 부터는 다시 장외
    assert loop._offhours_news_session(_at("20260914", "2000")) == "night"


def test_weekend_and_holiday_are_not_intraday():
    """애프터마켓도 거래일에만 열린다. 09-19 는 토요일."""
    assert loop._offhours_news_session(_at("20260919", "1700")) != "intraday"


def test_other_labels_are_untouched():
    assert loop._offhours_news_session(_at("20260914", "0230")) == "night"
    assert loop._offhours_news_session(_at("20260914", "0630")) == "dawn"
    assert loop._offhours_news_session(_at("20260914", "1000")) == "intraday"
