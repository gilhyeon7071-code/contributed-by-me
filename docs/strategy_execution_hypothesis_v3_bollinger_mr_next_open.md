# Strategy Execution V3: Bollinger Mean Reversion, Next-Open

## Purpose

Test a distinct mean-reversion family under an executable historical proxy. This is research-only and does not alter candidate generation, Gate, LOCK, orders, broker dispatch, or paper runtime.

## Frozen rule

- Signal: 20-session close z-score less than or equal to -1.5 and RSI14 less than or equal to 30.
- Ranking: lower z-score first within each signal date.
- Selection grid: Top 1, Top 3, and Top 5 for h1, h2, and h5.
- Entry: next exact global trading-session open after the signal close.
- Exit: close at the signal session plus the stated horizon, only when that exact global session exists.
- Cost: fee 0.5% and slippage 0.1%.
- Splits: Train 2021-02 to 2023-12; Validation 2024; historic OOS 2025-01 to 2025-05.

## Scope limits

Legacy rows receive a later unique KOSPI/KOSDAQ code mapping only for research coverage. Ambiguous and unmapped codes are excluded. This can introduce survivorship bias. OHLC data proves neither opening-auction priority nor limit-price, depth, and partial-fill feasibility. Therefore, a positive historical result can only become a paper-validation candidate, never an operating-policy promotion.
