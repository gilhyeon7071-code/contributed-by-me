# -*- coding: utf-8 -*-
"""수량 축소 사슬이 **초기 산정부터** 기록되는지 고정한다.

[2026-09-12] 미결 대장 C20. `_qty_trail` 은 2026-08-21 에 만들어졌는데 추적이
`_resolve_initial_entry_qty()` 가 끝난 뒤부터 시작했다. 예산상한·분할·적응 축소가
그 함수 **안**에 있어서 09-11 에 qty_initial=1 로 차단된 4종목의 사슬이 전부 빈 값이었다.
붕괴가 끝난 자리에서 관찰을 시작하면 아무것도 안 보인다.

이 시험은 (1) 사슬이 그 세 단계를 담는지 (2) 수량 산식이 안 바뀌었는지를 본다.
"""
from __future__ import annotations

import pytest

entry = pytest.importorskip("paper_engine.entry")


def _stages(trail):
    return [x["stage"] for x in trail]


def test_track_qty_records_only_real_changes():
    trail = []
    entry._track_qty(trail, "budget_cap", 139, 18)
    entry._track_qty(trail, "no_change", 18, 18)      # 변화 없으면 기록하지 않는다
    entry._track_qty(trail, "split_first_ratio", 18, 5)
    assert _stages(trail) == ["budget_cap", "split_first_ratio"]
    assert trail[0]["before"] == 139 and trail[0]["after"] == 18


def test_initial_sizing_stages_are_instrumented():
    """세 단계가 실제로 계측 지점에 있다 - 소스 고정."""
    import inspect
    src = inspect.getsource(entry._resolve_initial_entry_qty)
    for stage in ("budget_cap", "split_first_ratio", "adaptive_good_stock"):
        assert '_track_qty(_trail, "%s"' % stage in src, "%s 계측이 사라졌다" % stage
    assert 'row["_qty_raw_calc"]' in src, "사슬의 출발점(날 수량) 기록이 사라졌다"


def test_arithmetic_is_unchanged():
    """계측은 기록만 한다. 산식에 _trail 이 끼어들면 안 된다."""
    import inspect, re
    src = inspect.getsource(entry._resolve_initial_entry_qty)
    for line in src.splitlines():
        st = line.strip()
        if re.match(r"^qty = ", st) or re.match(r"^qty = max\(1,", st):
            assert "_trail" not in st, "수량 산식에 계측이 섞였다: %s" % st


def test_trail_text_roundtrip():
    trail = [{"stage": "budget_cap", "before": 139, "after": 18},
             {"stage": "split_first_ratio", "before": 18, "after": 5}]
    assert entry._qty_trail_text(trail) == "budget_cap:139>18|split_first_ratio:18>5"
    assert entry._qty_trail_text([]) == ""
    assert entry._qty_trail_text(None) == ""
