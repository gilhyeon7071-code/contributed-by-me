# System Architecture (RootA)

Last updated: 2026-03-05

## 1. Scope
This document defines the runtime architecture of `E:\1_Data` paper-trading pipeline.
It covers data ingestion, candidate generation, risk gates, paper execution, and signal integration.

## 2. Runtime Layers
1. Market Data Layer
- KRX clean update -> parquet price update.
- Primary artifacts:
  - `E:\1_Data\_krx_manual\*.parquet`
  - `E:\1_Data\paper\prices\ohlcv_paper.parquet`

2. Signal Generation Layer
- `generate_candidates_v41_1.py` produces base candidates and relax metadata.
- `liquidity_filter_daily.py` filters executable universe.

3. Enrichment Layer (Phase 2/3)
- Sector: `tools\sector_score_daily.py`
- News collect: `tools\news_collect_naver_daily.py`
- News score: `tools\news_score_daily.py`
- Final merge: `tools\final_score_merge_daily.py`

4. Risk/Gate Layer
- Macro signal: `tools\macro_signal_daily.py`
- Daily risk check: `p0_daily_check.py`
- Gate decision: `gate_daily.py`

5. Execution Layer
- `paper_engine.py`
- Uses config lock (`paper\paper_engine_config.lock.json`) and fail-closed checks.

6. Integration/Analytics Layer
- `tools\signal_integration_daily.py` joins trades with phase2/3 stubs and final score.
- `live_vs_bt_paper_daily.py` computes drift and can auto-trigger optimize.

## 3. Control Principles
- Fail-soft for new optional signals (score=0, source marked) when data is missing.
- Fail-closed for critical risk controls (kill switch and hard block reasons).
- Config lock must match approved sha256 before pipeline continuation.

## 4. Key Design Constraints
- Existing core order/fill/ledger flow is minimally changed.
- New signal stages run after liquidity filter.
- Artifacts are generated as sidecars; originals are not overwritten where possible.

## 5. Known Operational Risks
- External market APIs can intermittently fail (pykrx/OECD); fallback logic is active.
- Historical trade window and latest candidate date can diverge by design; must be interpreted carefully in reports.
- Log growth is high; retention/cleanup must be enforced periodically.
