## Goal
- Include professional investment media in the Naver news collector press allowlist.

## Non-goals
- Do not change news scoring formulas.
- Do not change surge weights, gate policy, order policy, D rule, or SSOT chain.
- Do not infer or backfill historical news scores.

## Scope
- `E:\1_Data\tools\news_collect_naver_daily.py`
- `E:\1_Data\.agent\PLANS.md`

## Current Behavior
- `PRESS_SOURCES` allows only tier1, tier2, and tier3 domains.
- Any host outside `PRESS_SOURCES` is classified as `excluded`.
- Latest evidence: `E:\1_Data\2_Logs\news_collect_status_20260429.json`
  - `tier1=2`
  - `tier2=3`
  - `tier3=8`
  - `excluded=68`

## Risk
- Wider press coverage can admit lower-quality articles.
- This change affects article collection coverage only; it does not change scoring weights or gate semantics.

## Design
- Add a `tier4` list for professional investment and market media.
- Keep the existing tier classification function unchanged.
- Update status metadata so `press_allowed_tiers` and `source_counts` include `tier4`.

## Implementation Steps
1. Back up the collector and PLANS file.
2. Add tier4 domains to `PRESS_SOURCES`.
3. Add `tier4` to tier counts and status metadata.
4. Run syntax validation.
5. Run a no-network classifier validation using representative URLs.
6. Validate generated status shape without changing production outputs.

## Validation Plan
1. Functional validation: direct `_classify_press_tier` calls return `tier4` for added domains.
2. Consistency validation: existing tier domains still map to their original tiers; unknown domains still map to `excluded`.
3. Operational reflection validation: status template includes `tier4`.
4. Policy validation: no surge/gate/order/D rule change.
5. FAIL-CLOSED validation: empty or unknown host still returns `excluded`.
6. Regression validation: compile the changed Python file.

## Rollback
- Restore from `E:\1_Data\backup\20260429_news_press_tier4\20260429_171450`.
