import pandas as pd

from utils.price_history_contract import (
    KEY_COL,
    SEGMENT_COL,
    add_exact_session_forward_returns,
    add_trade_count_forward_returns,
    apply_price_history_contract,
)


def _row(date, code, close, *, high=None, low=None):
    high = close if high is None else high
    low = close if low is None else low
    return {
        "date": date, "code": code, "open": close, "high": high, "low": low,
        "close": close, "volume": 100, "value": close * 100,
    }


def test_forward_horizons_use_actual_trading_sessions_and_stop_at_gap():
    rows = []
    sessions = ["2026-07-01", "2026-07-02", "2026-07-03", "2026-07-06", "2026-07-07", "2026-07-08"]
    rows.extend(_row(d, "000001", 100 + i) for i, d in enumerate(sessions))
    rows.extend(_row(d, "000002", 200 + i) for i, d in enumerate(sessions) if d != "2026-07-03")
    clean, audit = apply_price_history_contract(pd.DataFrame(rows))
    out = add_exact_session_forward_returns(clean, (1, 2, 5))

    a = out[out["code"] == "000001"].reset_index(drop=True)
    assert a.loc[0, "fwd_date_1d"] == pd.Timestamp("2026-07-02")
    assert a.loc[0, "fwd_date_5d"] == pd.Timestamp("2026-07-08")

    b = out[out["code"] == "000002"].reset_index(drop=True)
    assert b[SEGMENT_COL].nunique() == 2
    assert pd.isna(b.loc[1, "fwd_ret_1d"])
    assert audit["session_gap_breaks"] == 1


def test_invalid_ohlc_is_removed_and_extreme_return_starts_new_segment():
    df = pd.DataFrame([
        _row("2026-07-01", "000001", 100),
        _row("2026-07-02", "000001", 101),
        _row("2026-07-03", "000001", 250),
        _row("2026-07-06", "000001", 251),
        _row("2026-07-07", "000003", 10, high=9, low=10),
    ])
    clean, audit = apply_price_history_contract(df)
    assert "000003" not in set(clean["code"])
    assert clean[clean["code"] == "000001"][KEY_COL].nunique() == 2
    assert audit["invalid_rows"] == 1
    assert audit["extreme_return_breaks"] == 1


def test_valid_krx_alphanumeric_code_is_not_blocked():
    clean, audit = apply_price_history_contract(pd.DataFrame([_row("2026-07-01", "00104K", 100)]))
    assert clean.iloc[0]["code"] == "00104K"
    assert audit["invalid_rows"] == 0


def test_default_max_gap_sessions_preserves_strict_legacy_behavior():
    rows = []
    sessions = ["2026-07-01", "2026-07-02", "2026-07-03", "2026-07-06", "2026-07-07", "2026-07-08"]
    rows.extend(_row(d, "000001", 100 + i) for i, d in enumerate(sessions))
    rows.extend(_row(d, "000002", 200 + i) for i, d in enumerate(sessions) if d != "2026-07-03")
    clean, audit = apply_price_history_contract(pd.DataFrame(rows))
    assert audit["max_gap_sessions"] == 0
    assert audit["soft_gap_bridges"] == 0
    assert audit["session_gap_breaks"] == 1


def test_soft_gap_bridges_segment_and_trade_count_forward_return_crosses_it():
    rows = []
    sessions = ["2026-07-01", "2026-07-02", "2026-07-03", "2026-07-06", "2026-07-07", "2026-07-08"]
    rows.extend(_row(d, "000001", 100 + i) for i, d in enumerate(sessions))
    # 000002 misses a single session (2026-07-03), e.g. a 1-day trading halt.
    rows.extend(_row(d, "000002", 200 + i) for i, d in enumerate(sessions) if d != "2026-07-03")
    clean, audit = apply_price_history_contract(pd.DataFrame(rows), max_gap_sessions=1)

    b = clean[clean["code"] == "000002"].reset_index(drop=True)
    assert b[SEGMENT_COL].nunique() == 1
    assert audit["session_gap_breaks"] == 0
    assert audit["soft_gap_bridges"] == 1

    out = add_trade_count_forward_returns(clean, (1, 2))
    ob = out[out["code"] == "000002"].reset_index(drop=True)
    assert ob.loc[1, "fwd_gap_sessions_1d"] == 2
    assert pd.notna(ob.loc[1, "fwd_ret_1d"])


def test_gap_longer_than_tolerance_still_breaks_segment():
    rows = []
    sessions = ["2026-07-01", "2026-07-02", "2026-07-03", "2026-07-06", "2026-07-07", "2026-07-08"]
    rows.extend(_row(d, "000001", 100 + i) for i, d in enumerate(sessions))
    # 000002 misses two consecutive sessions (07-03, 07-06) -- longer than tolerance.
    rows.extend(_row(d, "000002", 200 + i) for i, d in enumerate(sessions) if d not in ("2026-07-03", "2026-07-06"))
    clean, audit = apply_price_history_contract(pd.DataFrame(rows), max_gap_sessions=1)
    b = clean[clean["code"] == "000002"].reset_index(drop=True)
    assert b[SEGMENT_COL].nunique() == 2
    assert audit["session_gap_breaks"] == 1


def test_extreme_return_still_breaks_segment_even_when_gap_is_within_tolerance():
    rows = []
    sessions = ["2026-07-01", "2026-07-02", "2026-07-03", "2026-07-06", "2026-07-07", "2026-07-08"]
    rows.extend(_row(d, "000001", 100 + i) for i, d in enumerate(sessions))
    prices = {"2026-07-01": 200, "2026-07-02": 201, "2026-07-06": 500, "2026-07-07": 501, "2026-07-08": 502}
    rows.extend(_row(d, "000002", prices[d]) for d in sessions if d in prices)
    clean, audit = apply_price_history_contract(pd.DataFrame(rows), max_gap_sessions=2)
    b = clean[clean["code"] == "000002"].reset_index(drop=True)
    assert b[SEGMENT_COL].nunique() == 2
    assert audit["extreme_return_breaks"] == 1
    assert audit["soft_gap_bridges"] == 0
