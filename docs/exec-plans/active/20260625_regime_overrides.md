# 2026-06-25 Regime Overrides ExecPlan

## Scope
- Add a runtime `regime_overrides` config merge after `market_regime` is resolved in `paper_engine.py`.
- Add default `regime_overrides` values to `paper/paper_engine_config.json`.
- Recompute local variables that are bound before regime resolution and are affected by overrides.

## Non-Scope
- No live/KIS approval change.
- No hardcoded market regime forcing in production code.
- No score/status uplift by hardcoding.
- No change to order/fill/ledger/stats SSOT semantics.

## Implementation Notes
- Use existing `_deep_merge_dict`.
- Apply override only when `cfg["regime_overrides"]` has a dict for the resolved `market_regime`.
- Recompute `stop_loss`, `take_profit`, `trail_pct`, `sell_rules`, `sell_rules_enabled`, `fundamentals_db`, `budget_policy_cfg`, `budget_target_positions`, `regime_policy`, `surge_policy`, and cost profile values after the merge.
- Recompute `max_new_surge` after `surge_policy` is refreshed.
- Keep CRASH fail-closed evidence separate from config presence.

## Validation Plan
- Syntax: `python -m py_compile paper_engine.py`.
- Config parse: load `paper/paper_engine_config.json` as JSON.
- Runtime: isolated direct execution or official batch path with separate output paths.
- Result artifacts: inspect log lines for `[REGIME_OVERRIDE]`, `[BUDGET_POLICY]`, `[PAPER_ENGINE] stop_loss=...`, `max_new`, `max_new_surge`, and fills count.

## Completion Criteria
- Feature validation: override merge and recomputed values are visible in runtime logs/artifacts.
- Consistency validation: config JSON parses and lock status is explicitly reported.
- Operational reflection validation: official or isolated runtime evidence exists.
- Policy validation: no live approval or FAIL-CLOSED bypass is introduced.
- FAIL-CLOSED validation: CRASH path proves `max_new=0`, `max_new_surge=0`, and `filled=0`.
- Regression validation: existing NORMAL path still runs without syntax/config failure.
