# Strategy Execution V6: Simple Momentum, Next-Open

Research-only test of the existing simple-momentum grammar: same-date relative strength in the highest quintile. The top-quintile condition already selects the cross-section, so all qualifying rows are retained without a new Top-N selector. Entry is next exact global-session open; exits are exact h1/h2/h5 closes. Fee is 0.5%, slippage is 0.1%, and splits are Train 2021-02 to 2023-12, Validation 2024, OOS 2025-01 to 2025-05.

The later unique KOSPI/KOSDAQ mapping is historical-coverage only and may cause survivorship bias. No operating candidate, Gate, LOCK, order, broker, or paper-runtime behavior changes.
