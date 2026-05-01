# Codex LLM Code Review CI Design

## 1. Boundary

This design describes a read-only advisory review path.

It does not create a workflow, does not call an LLM API, does not change trading logic, and does not write operational outputs.

## 2. Goal

The review should catch risky changes before they affect operations.

Primary targets:

- Gate, STOP, LOCK, and FAIL-CLOSED meaning changes
- `orders(D) -> fills(D) -> ledger -> stats` chain breaks
- Date mismatches around `D`, `exec_date`, `as_of`, and `run_id`
- Hardcoded upward score, status, PASS, ready, or risk state
- Operational output edits without backup evidence
- Claims of validation without evidence paths

## 3. Initial CI Shape

Use advisory mode first.

Recommended trigger:

- Pull request or manual dispatch only
- Read-only checkout
- No batch execution
- No live order path
- No formatter or auto-fix step
- No write-back to repository files

Recommended review inputs:

- Changed file list
- Unified diff
- `AGENTS.md`
- `docs/03-operations/codex_llm_code_review_ci_checklist.md`
- `E:\1_Data\.agent\PLANS.md` when RootA behavior changes
- `E:\vibe\buffett\PLANS.md` when RootB behavior changes

## 4. Review Prompt Contract

The prompt should force evidence-only output.

Minimum instruction block:

```text
You are reviewing an operational trading-system diff.
Report findings only from the provided diff and context.
Do not propose unrelated improvements.
Do not say complete, ready, fixed, or problem-free unless every required validation item has PASS evidence.
Separate facts from interpretation.
Classify each finding as HARD_FAIL, WARN, or INFO.
```

Required output:

```text
Findings
- Severity:
- File/line:
- Fact:
- Risk:
- Required action:

Validation matrix
- Functional validation:
- Consistency validation:
- Operational reflection validation:
- Policy validation:
- FAIL-CLOSED validation:
- Regression validation:

Untested areas
- ...
```

## 5. Hard-Fail Gate Rule

In phase 1, hard-fail findings should be comments only.

In phase 2, the CI may block only when the LLM reports `HARD_FAIL`.

Blocking should not depend on natural-language confidence alone. The blocking wrapper should require an explicit machine-readable marker, for example:

```json
{
  "result": "HARD_FAIL",
  "findings_count": 1
}
```

## 6. Local Wrapper Design

A local wrapper can be added later under `tools/ci/`.

Recommended files:

- `tools/ci/build_codex_review_context.py`
- `tools/ci/codex_review_prompt.md`
- `tools/ci/parse_codex_review_result.py`

Responsibilities:

- Collect changed file list
- Build a bounded diff context
- Attach checklist and relevant plan files
- Reject oversized context before API call
- Parse the result marker
- Exit `0` for advisory findings in phase 1
- Exit non-zero only for phase 2 hard-fail mode

## 7. Workflow Design

A workflow can be added later under `.github/workflows/`.

Recommended file:

- `.github/workflows/codex-llm-review.yml`

Recommended defaults:

- `pull_request` and `workflow_dispatch`
- `permissions: contents: read, pull-requests: write`
- advisory mode by default
- no repository write token
- no scheduled execution at first

## 8. Rollout Plan

1. Keep this design and checklist as documentation only.
2. Add local wrapper in read-only mode.
3. Run wrapper manually against a small diff.
4. Add GitHub Actions workflow in advisory mode.
5. After stable review quality, allow phase 2 blocking for `HARD_FAIL` only.

## 9. Explicit Non-Goals

This CI must not:

- auto-fix code
- run live trading or order paths
- rewrite generated operational outputs
- modify latest JSON, logs, ledger, fills, or stats
- change policy thresholds
- declare operational readiness without runtime evidence
