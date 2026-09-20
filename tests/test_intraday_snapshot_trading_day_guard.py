# -*- coding: utf-8 -*-
"""비거래일에 '장중' 스냅샷을 찍지 않는지 고정한다.

[2026-09-13] 2026-09-12(토) 배치가 장중 스냅샷을 찍고 `date=20260912` 로 도장했다.
그날은 장이 없었으므로 그 값은 09-11 종가다. 다음날 `surge_detector_realtime` 이
'기대 20260911 / 실제 20260912' 로 거부하고 **rc=1, 출력 0바이트**로 죽었다.
배치 실패 `[16.957c/16]` 의 진짜 출처였고, 화면의 `STALE_INTRADAY_DATE` 도 여기서 왔다.

가드는 **생산자**에 둔다. 소비자(탐지기)의 날짜 검사는 원래 맞았다.
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

snap = pytest.importorskip("tools.intraday_price_snapshot")


def test_weekend_is_non_trading():
    assert snap._non_trading_day_reason(dt.datetime(2026, 9, 12, 9, 0)).startswith("주말")
    assert snap._non_trading_day_reason(dt.datetime(2026, 9, 13, 9, 0)).startswith("주말")


def test_weekday_holiday_is_non_trading():
    """holidays.json 이 정본이다."""
    hs = json.loads((Path(snap.ROOT) / "holidays.json").read_text(encoding="utf-8-sig"))
    vals = hs.get("holidays") if isinstance(hs, dict) else hs
    wk = [h for h in sorted(vals) if dt.datetime.strptime(h, "%Y%m%d").weekday() < 5]
    assert wk, "평일 공휴일 표본이 없다"
    d = dt.datetime.strptime(wk[0], "%Y%m%d")
    assert snap._non_trading_day_reason(d) == "휴장일"


def test_trading_day_passes():
    """2026-09-14 는 월요일이고 holidays.json 에 없다 (실측)."""
    assert snap._non_trading_day_reason(dt.datetime(2026, 9, 14, 9, 0)) == ""


def test_holidays_unreadable_still_blocks_weekend(monkeypatch, tmp_path):
    """holidays.json 을 못 읽어도 **주말 검사는 남는다.** 못 읽었다고 통과시키지 않는다."""
    monkeypatch.setattr(snap, "ROOT", tmp_path, raising=False)
    assert snap._non_trading_day_reason(dt.datetime(2026, 9, 12, 9, 0)).startswith("주말")
    # 평일은 휴장 목록을 모르므로 통과시킨다(주말 검사만 적용)
    assert snap._non_trading_day_reason(dt.datetime(2026, 9, 14, 9, 0)) == ""


def test_guard_is_wired_into_main_and_can_be_overridden():
    import inspect
    src = inspect.getsource(snap.main)
    assert "_non_trading_day_reason()" in src, "가드가 main 에 배선돼 있지 않다"
    assert "allow_non_trading_day" in src, "복구용 우회 스위치가 없다"
    assert "return 0" in src, "비거래일은 결함이 아니므로 rc=0 이어야 한다"


def test_default_argument_path_works():
    """**인자 없이** 부르는 경로를 반드시 덮는다.

    2026-09-13 실제 사고: `now or datetime.now()` 라고 썼는데 이 모듈은
    `import datetime as dt` 다. 인자를 넘기는 시험만 있어서 그 줄을 한 번도 안 탔고,
    08:30 배치가 `NameError` 로 rc=1 이 됐다. **배치를 내가 깨뜨렸다.**

    기본 인자 경로는 배치가 실제로 쓰는 경로다. 시험이 그것을 안 덮으면
    시험이 통과해도 운영은 죽는다.
    """
    r = snap._non_trading_day_reason()
    assert isinstance(r, str)


def test_main_guard_returns_zero_on_non_trading_day(monkeypatch):
    """비거래일은 결함이 아니다 - rc=0 이어야 배치가 안 죽는다."""
    import sys
    monkeypatch.setattr(sys, "argv", ["intraday_price_snapshot.py", "--mock", "auto"])
    monkeypatch.setattr(snap, "_non_trading_day_reason", lambda *a, **k: "주말(일)")
    assert snap.main() == 0
