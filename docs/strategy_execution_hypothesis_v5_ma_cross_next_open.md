# Strategy Execution V5: MA20/MA60 Cross, Next-Open

This is a research-only execution validation of the base MA20/MA60 bullish cross. Its signal is MA20 above MA60 while the immediately preceding MA20 was at or below MA60. The original grammar does not define a cross-sectional rank, so every qualifying signal is retained. No RS, volume, or other confirmation is added.

Entry is the next exact global-session open. Exit is the h1, h2, or h5 close counted from the signal session. Costs are 0.5% fee and 0.1% slippage. Splits are Train 2021-02 to 2023-12, Validation 2024, and historic OOS 2025-01 to 2025-05.

Historical KOSPI/KOSDAQ coverage uses only uniquely mapped later labels and may contain survivorship bias. OHLC cannot prove auction priority, limit-price fill, depth, or partial fill. This does not modify operating candidates, Gate, LOCK, orders, broker dispatch, or paper runtime.
