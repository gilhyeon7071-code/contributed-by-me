# Strategy Execution Hypothesis V2: Next-Open Breakout

## Purpose

V1 used same-close entry and showed that 252-day breakout selection can have relative stock-selection value but cannot establish executable performance when a small number of near-limit outcomes dominate returns. V2 is a separate execution hypothesis, not a V1 parameter adjustment.

## Research universe

- Use KOSPI and KOSDAQ where the historical source supplies a market label.
- Treat KRX as a legacy missing-market label only: the loader creates it when a source parquet has no market column.
- This preserves historical coverage but does not assert the legacy KRX composition is identical to the labeled KOSPI/KOSDAQ universe. It is research-only and never changes the operating universe.

## Legacy market mapping

- Historical blank or `KRX` market labels are mapped only when the same code has one later KOSPI or KOSDAQ label from 2025-06-01 onward.
- Codes with conflicting later labels or no later label are excluded, never guessed.
- The mapping restores 94%+ of the historical rows but can introduce survivorship bias because later-listed evidence is unavailable for delisted codes. Treat every V2 result as research-only historical replication.

## Fixed grammar

- Signal: close above the prior 252-session high, with at least 60 historical sessions.
- Ranking: breakout strength (`close / prior_252_session_high`) descending on the signal date.
- Grid: h1/h2 paired with Top 1, Top 3, and Top 5. The grid is fixed before V2 results are read.
- Entry: next exact global trading-session open, never the signal-day close.
- Exit: exact global session close at the signal session plus h1 or h2.
- Cost: fee 0.5% and slippage 0.1%.

## Time order

- Train: 2021-02-01 through 2023-12-29.
- Validation: 2024-01-02 through 2024-12-30.
- Historical OOS: 2025-01-02 through 2025-05-30.

The historical OOS ends before the V1 research window that began on 2025-06-01. It is not used to choose a V2 grid member.

## Fill limitation

The next-open price is an OHLC execution proxy. It does not prove auction queue position, order-book depth, partial fill, or limit-price fill. Therefore a positive historical result can become a historical candidate only; Stage 4 paper portfolio entry still requires later paper-fill evidence.

## Required reporting

For every grid member and split, record absolute cost-after return, same-date baseline excess, median net return, and OOS top-five-trimmed mean. Do not promote an average-only result that is concentrated in a few outcomes.
