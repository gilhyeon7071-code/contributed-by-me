# -*- coding: utf-8 -*-
"""KRX 호가가격단위 - 이 저장소의 단일 출처.

[2026-09-08] 왜 만들었나. 같은 표가 두 벌 있었고 한 벌이 틀려 있었다.
  audit_daily.py:_krx_tick        1/5/10/50/100/500/1000   7단계, 맞았다
  tools/topn_build_orders.py      1/5/10/50/100            5단계, 5만원 이상이 전부 100원
  후자가 발주 지정가를 만들고 있었다. 실측 2026-09-08: 003230 지정가 1,336,700 이
  "모의투자 주문처리가 안되었습니다(호가단위 오류)" 로 거래소에서 거절됐다.
  정답이 저장소 안에 이미 있었는데 공유되지 않은 것이 원인이라, 두 곳을 여기로 모은다.

기준. 유가증권시장 7단계 (2023-01-25 개편).
  코스닥은 5단계(5만원 이상 전부 100원)로 다르지만 시장 구분을 쓰지 않는다 -
  패널의 market 컬럼이 52% UNKNOWN 이라(topn_candidates.py:132) 절반을 틀리게 판정하느니
  **항상 유효한 격자**를 쓴다. 유가증권 격자(500/1,000)는 코스닥 격자(100)의 배수라
  코스닥 종목에 써도 주문은 언제나 접수된다. 대가는 코스닥 20만원 이상 종목의 지정가가
  최대 0.25% 거칠어지는 것이다.

바꾸려면. 거래소가 호가단위를 다시 개편했을 때만 바꾼다. 바꾸면 그날 이후 발주 가격이
  전부 달라지므로 PLANS 에 기준선 이동으로 적는다.
"""
from __future__ import annotations

import math

# (상한 미만, 호가단위). 마지막 칸은 상한 없음.
_BANDS = (
    (2_000, 1),
    (5_000, 5),
    (20_000, 10),
    (50_000, 50),
    (200_000, 100),
    (500_000, 500),
)
_TOP_TICK = 1_000


def tick_size(px: float) -> int:
    """가격 px 구간의 호가단위."""
    p = float(px)
    for upper, tick in _BANDS:
        if p < upper:
            return tick
    return _TOP_TICK


def round_tick(px: float, up: bool) -> int:
    """px 를 자기 구간의 격자로 올림(up=True) 또는 내림.

    올림이 구간 경계를 넘을 수 있다(예: 199,990 -> 200,000). 넘어간 값도 새 구간의
    격자 위에 있어야 하므로 결과를 한 번 더 검사한다. 유가증권 격자는 위로 갈수록
    거칠고 아래 칸의 배수라(500=5x100, 1000=10x100) 한 번의 재조정이면 닫힌다.
    """
    t = tick_size(px)
    v = int((math.ceil(float(px) / t) if up else math.floor(float(px) / t)) * t)
    t2 = tick_size(v)
    if t2 != t and v % t2 != 0:
        v = int((math.ceil(v / t2) if up else math.floor(v / t2)) * t2)
    return v
