# Indicator Remaining Issues - Deferred

Date: 2026-05-08
Scope: stock indicator implementation status
Status: deferred

## Source Evidence

- `E:\1_Data\2_Logs\macro_feature_external_latest.json`
- `E:\1_Data\2_Logs\candidates_latest_data.with_final_score.csv`
- `E:\1_Data\2_Logs\final_score_merge_status_20260507.json`
- `E:\1_Data\2_Logs\forecast_score_validation_20260507.json`

## Deferred Items

| No | Area | Indicator | Current Status | Evidence | Deferred Reason |
|---:|---|---|---|---|---|
| 1 | Market sentiment | Put/Call Ratio | Not implemented | `put_call_ratio=null`, `put_call_source_status=OFFICIAL_SOURCE_NO_ROWS` | Official-source connection exists, but no usable current row was collected. |
| 2 | Market sentiment | CNN Fear & Greed | Not implemented | `cnn_fear_greed=null`, `cnn_fear_greed_source_status=OFFICIAL_SOURCE_NOT_CONFIGURED` | Official source is not configured. |
| 3 | Sector/linked indicator | BDI | Partially implemented | `bdi=2991.0`, `bdi_source_status=WEB_FALLBACK`, `score_role=reference_only` | Web fallback source only; not used as direct score input. |
| 4 | Korea supply/demand | Short balance | Partially implemented | `short_balance_status=PYKRX_NO_ROWS`, `short_balance_source_tier=MISSING`, values 0/11 rows | Collection path exists, but no usable short-balance rows were returned. |
| 5 | Korea supply/demand | Credit balance | Partially implemented | `credit_balance_amount` 11/11 rows, `credit_balance_source_tier=MARKET_LEVEL`, `credit_balance_as_of=20260506` | Market-level amount only; not stock-level quantity/ratio. |
| 6 | Earnings/valuation | Operating profit consensus change rate | Partially implemented | `op_consensus_change` 10/11 rows, `op_consensus_source=forward_estimate_snapshot_ttm_proxy` | TTM/forward snapshot proxy, not analyst consensus. |

## Current Applied Controls

- Macro tier weight factor is applied in `final_score_merge_status_20260507.json`.
- Current macro tier factor: `0.7260000000000001`.
- `regime`, `fx`, and `forecast` weights are reduced by source-tier confidence.
- Forecast proxy validation is currently `PASS`.
- `forecast_proxy_rate=1.0`.

## Deferred Boundary

These items are not treated as current blockers.

They must not be marked as implemented unless a fresh source/path validation confirms:

- source file exists,
- row coverage is non-zero,
- as-of date is aligned,
- output JSON/CSV reflects the value,
- score or reference role is explicitly documented.

