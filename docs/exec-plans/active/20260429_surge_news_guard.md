## Goal
- Change realtime surge news usage from a score booster into a negative-news guard with a small positive bonus.

## Non-goals
- Do not change order generation, fills, ledger, stats, D rule, or SSOT chain.
- Do not implement T-1 news timing in this step.
- Do not add LLM scoring in this step.
- Do not backfill historical scores.

## Scope
- `E:\1_Data\tools\surge_detector_realtime.py`
- `E:\1_Data\paper\surge_params.json`
- `E:\1_Data\.agent\PLANS.md`

## Current Behavior
- `surge_score_final` combines rule, news, and ML weights.
- News can act as a full weighted score component.
- Runtime artifact before this change showed `weight_rule=0.60`, `weight_news=0.15`, `weight_ml=0.25`.

## Risk
- Negative news can newly block surge alerts.
- Positive news no longer gives a full weighted boost; it only adds a small bonus.

## Design
- Add `news_block_threshold=-0.30`.
- Add `news_bonus_threshold=0.30`.
- Add `news_bonus_pts=5.0`.
- Default weights become `rule=0.65`, `news=0.08`, `ml=0.27`.
- The news weight is assigned to rule unless news crosses the positive bonus threshold.
- Unknown/no-news score `0.0` is neutral and does not block.

## Implementation Steps
1. Back up the surge detector and PLANS file.
2. Add configurable news guard thresholds.
3. Add negative-news policy block reason.
4. Replace full news weighted component with small positive bonus.
5. Update runtime surge params for `0.65/0.08/0.27`.
6. Record thresholds in surge output JSON.
7. Validate syntax, direct runtime, output JSON/CSV, and policy boundaries.

## Validation Plan
1. Functional validation: run `surge_detector_realtime.py`.
2. Consistency validation: inspect latest JSON thresholds and CSV columns.
3. Operational reflection validation: confirm latest `surge_realtime_latest.json` regenerated.
4. Policy validation: confirm only surge news scoring policy changed.
5. FAIL-CLOSED validation: direct negative-news fixture or function-level check should produce `NEWS_NEGATIVE`.
6. Regression validation: compile the changed Python file and run mojibake scan.

## Rollback
- Restore from `E:\1_Data\backup\20260429_surge_news_guard\20260429_172123`.
