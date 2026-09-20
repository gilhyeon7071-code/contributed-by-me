# Codex LLM Review API Integration Decision

## 1. Current State

The advisory scaffold is verified.

Confirmed:

- manual `workflow_dispatch` workflow exists on `main`
- GitHub runner completed successfully
- artifact `codex-review-advisory` was generated
- artifact contains `codex_review_context.md`
- artifact contains `codex_review_sample.md`
- parser accepts `Machine result` JSON

Not implemented:

- direct LLM API call
- pull request comment writer
- pull request automatic trigger
- hard-fail blocking in GitHub Actions

## 2. Decision

Do not connect the LLM API yet.

Use one more controlled phase first:

1. keep the workflow manual
2. keep `permissions: contents: read`
3. keep generated context as artifact
4. run external LLM review manually from the artifact
5. paste or save the review output outside operational files
6. parse the review output with the existing parser

Move to API integration only after this manual phase is stable.

## 3. Required Conditions Before API Integration

All conditions below must be true before adding an API call.

- API provider selected explicitly
- model name configured through workflow input or environment, not hardcoded in logic
- secret configured in GitHub repository settings
- no secret printed to logs
- no generated operational JSON, log, ledger, fills, stats, or dashboard state is written
- LLM failure exits as review failure, not operational success
- timeout behavior is explicit
- oversized context behavior is explicit
- parser still requires `Machine result` JSON
- advisory mode remains non-blocking by default

## 4. Proposed API Phase Shape

The first API phase should add only one job step after context generation.

Inputs:

- `codex_review_prompt.md`
- `codex_review_context.md`

Output:

- `codex_review_output.md`

The API step should not:

- modify repository files
- commit changes
- post PR comments
- execute trading batches
- execute live order paths
- change Gate, STOP, LOCK, or FAIL-CLOSED state

## 5. Failure Handling

Use these rules.

| Failure | Expected behavior |
| --- | --- |
| API key missing | fail the workflow step |
| API timeout | fail the workflow step |
| API returns invalid format | parser exits `3` |
| `Machine result` missing | parser exits `3` |
| `HARD_FAIL` in advisory mode | workflow remains success, result is recorded |
| `HARD_FAIL` in hard-fail mode | workflow exits non-zero |

## 6. PR Comment Phase

Do not add PR comments in the same step as API integration.

Add PR comments only after API output is stable.

Required conditions:

- workflow uses `pull_request` only after explicit approval
- permissions are widened only to the minimum needed for comments
- comment body includes findings, validation matrix, and untested areas
- comment body does not include secrets
- comment writer does not edit repository files
- comment writer does not mark operational readiness

## 7. Current Recommendation

Next action should be one of these:

- keep manual artifact review for one more run
- add an API integration draft behind `workflow_dispatch` only
- defer PR comments until API output quality is verified

Do not enable automatic pull request review yet.
