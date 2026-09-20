# Strategy Execution V4: Volume Breakout, Next-Open

This research-only validation tests a volume-confirmed 20-session breakout. Signal-day close is observed first; a position enters at the next exact global-session open and exits at the exact h1, h2, or h5 close counted from the signal session.

The frozen signal is a close above the preceding 20-session close high combined with daily volume acceleration in the highest cross-sectional quintile. Signals rank by volume acceleration, descending. The fixed grid is Top 1, Top 3, and Top 5 across h1, h2, and h5, with 0.5% fee and 0.1% slippage. Splits are Train 2021-02 to 2023-12, Validation 2024, and historic OOS 2025-01 to 2025-05.

The legacy unique KOSPI/KOSDAQ mapping is for research coverage only and may create survivorship bias. OHLC rows do not prove auction priority, limit-price fill, depth, or partial-fill feasibility. No result changes operational selection, Gate, LOCK, orders, broker dispatch, or paper runtime.
