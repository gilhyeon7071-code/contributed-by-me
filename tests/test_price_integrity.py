from __future__ import annotations

import pandas as pd

from tools.price_integrity import (
    build_prev_close_map,
    decide_corporate_action_repair,
    detect_day_price_issues,
)


def test_detect_day_price_issues_blocks_invalid_ohlc() -> None:
    prev = {"005930": {"date": "20260418", "close": 70000.0}}
    day = pd.DataFrame(
        [
            {"date": "20260421", "code": "005930", "open": 69000, "high": 68000, "low": 67000, "close": 67500, "volume": 100},
            {"date": "20260421", "code": "000660", "open": 120000, "high": 123000, "low": 119000, "close": 121000, "volume": 200},
        ]
    )
    clean, blocked, spikes = detect_day_price_issues(day, prev)
    assert len(clean) == 1
    assert len(blocked) == 1
    assert blocked.iloc[0]["code"] == "005930"
    assert spikes == []


def test_detect_day_price_issues_marks_spike_candidate() -> None:
    prev = {"005930": {"date": "20260418", "close": 100.0}}
    day = pd.DataFrame(
        [
            {"date": "20260421", "code": "005930", "open": 155, "high": 160, "low": 150, "close": 158, "volume": 1000},
        ]
    )
    clean, blocked, spikes = detect_day_price_issues(day, prev, spike_threshold_abs=0.45)
    assert len(clean) == 1
    assert blocked.empty
    assert len(spikes) == 1
    assert spikes[0]["code"] == "005930"


def test_decide_corporate_action_repair_applies_when_adjusted_window_reduces_jump() -> None:
    decision = decide_corporate_action_repair(
        prev_close_raw=100.0,
        close_raw=45.0,
        prev_close_adjusted=50.0,
        close_adjusted=45.0,
        raw_move_trigger_abs=0.45,
        adjusted_move_accept_abs=0.30,
    )
    assert decision.apply_adjusted is True
    assert decision.reason == "adjusted_window_repair"


def test_build_prev_close_map_picks_latest_row_per_code() -> None:
    df = pd.DataFrame(
        [
            {"date": "20260418", "code": "005930", "close": 100},
            {"date": "20260421", "code": "005930", "close": 110},
            {"date": "20260417", "code": "000660", "close": 200},
        ]
    )
    out = build_prev_close_map(df)
    assert out["005930"]["date"] == "20260421"
    assert out["005930"]["close"] == 110.0
    assert out["000660"]["date"] == "20260417"
