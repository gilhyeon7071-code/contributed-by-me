# Codex LLM Code Review CI Checklist

## 1. Purpose

This checklist defines what an LLM code review CI should check before operational trading changes are merged or applied.

The CI review is advisory first. It must not auto-fix files, rewrite policy, change gate behavior, or create operational outputs.

## 2. Scope Classification

Classify every change before review.

- RootA trading logic: `E:\1_Data`
- RootB dashboard/UI: `E:\vibe\buffett`
- Cross-root work: changes touching both RootA and RootB
- Docs-only work: documentation without executable behavior changes
- Generated output work: latest JSON, logs, CSV, ledger, stats, or dashboard state files

## 3. Required Review Inputs

The review prompt should include these inputs when available.

- Changed file list
- Unified diff
- Relevant `AGENTS.md` instructions
- RootA plan file: `E:\1_Data\.agent\PLANS.md`
- RootB plan file when dashboard files changed: `E:\vibe\buffett\PLANS.md`
- Relevant final report template: `E:\1_Data\docs\references\FINAL_REPORT_TEMPLATE.md`
- Evidence paths from the run, if the change claims validation

## 4. Hard Fail Findings

The CI should block or require manual approval when any item below appears in a diff.

- Gate, STOP, LOCK, or FAIL-CLOSED meaning changed without explicit request
- Fail-open behavior introduced on missing data, stale data, parse failure, or exception
- `orders(D) -> fills(D) -> ledger -> stats` chain broken or bypassed
- `orders_exec` missing, `exec_date != D`, `as_of/run_id` mismatch, or paper/broker date mixing ignored
- Score, state, PASS, ready, risk, gate, or completion status hardcoded upward
- Latest JSON, operational logs, history CSV, ledger, or stats modified without backup evidence
- Destructive action, scheduler change, live order behavior, `DOIT`, `--apply`, or external backup write introduced without explicit approval
- Secrets, API keys, credentials, or private tokens added to repository files
- Policy thresholds changed without separating policy change from bug fix
- Runtime validation claimed without evidence path

## 5. Warning Findings

The CI should warn and request human review for these patterns.

- Score-weighted sizing added without shadow validation and entry-time score evidence
- New fallback path added, especially `fallback_top1`, without explicit policy approval
- Dashboard display changed while root cause remains unresolved
- Config default changed without migration or old-row handling
- New generated field added without schema compatibility rule
- CI or script uses network, external service, or write access during review
- Review output says complete, fixed, ready, or problem-free while required validation is missing
- Batch path and standalone execution path are not reported separately

## 6. Info Findings

The CI may report these as non-blocking context.

- Docs-only wording changes
- Comments or type hints without runtime behavior change
- Observability additions that do not alter gate, order, fill, ledger, or stats behavior
- Refactors with identical input/output evidence

## 7. Required Review Output

The LLM review should report findings first.

Each finding should include:

- Severity: `HARD_FAIL`, `WARN`, or `INFO`
- File and line
- Fact found in the diff
- Operational risk
- Required action
- Whether code change is required

If there are no findings, the review must still list untested areas instead of saying the change is fully safe.

## 8. Validation Matrix

The review should include this matrix. Use `NA` only when the item is outside the requested scope.

| Item | Status | Evidence |
| --- | --- | --- |
| Functional validation | PASS / FAIL / NA | Path or reason |
| Consistency validation | PASS / FAIL / NA | Path or reason |
| Operational reflection validation | PASS / FAIL / NA | Path or reason |
| Policy validation | PASS / FAIL / NA | Path or reason |
| FAIL-CLOSED validation | PASS / FAIL / NA | Path or reason |
| Regression validation | PASS / FAIL / NA | Path or reason |

Sub-checks should be mapped into the six required validation items instead of creating new top-level completion categories.

### Functional Validation Sub-Checks

- The changed function, script, or UI path was exercised.
- Batch path and standalone path are separated when both exist.
- The review distinguishes code presence from actual runtime behavior.
- The review lists what was not executed.

### Consistency Validation Sub-Checks

- `D`, `exec_date`, `as_of`, and `run_id` are aligned when relevant.
- Paper and broker dates are not mixed.
- `orders(D) -> fills(D) -> ledger -> stats` remains the source-of-truth chain.
- New fields preserve schema compatibility or include a migration rule.

### Operational Reflection Validation Sub-Checks

- Claimed results appear in the expected latest JSON, log, CSV, dashboard state, or report.
- Evidence paths are included for each operational claim.
- Generated outputs are not edited without backup evidence.
- The review separates displayed status from root-cause resolution.

### Policy Validation Sub-Checks

- Gate, STOP, LOCK, threshold, fallback, and risk policy meaning are unchanged unless explicitly requested.
- Policy change and bug fix are reported separately.
- Score, state, PASS, ready, risk, gate, or completion status is not hardcoded upward.
- Any fallback path is explicit and approved.

### FAIL-CLOSED Validation Sub-Checks

- Missing input, stale input, parse failure, exception, and empty output keep the safer blocked state.
- Review or CI failure does not mark operational state as safe.
- External service, network, or API failure does not bypass review.
- Live order, scheduler, cleanup, and `--apply` paths are not executed by advisory review.

### Regression Validation Sub-Checks

- Existing output format remains compatible.
- Existing batch entrypoint behavior is not changed unintentionally.
- Historical rows remain readable after schema changes.
- A narrow representative old path is checked, or the review marks regression validation as missing.

## 9. Recommended Rollout

Start in three phases.

1. Advisory mode: comment-only review, no blocking.
2. Hard-fail mode: block only `HARD_FAIL` findings.
3. Required-evidence mode: block missing evidence for operational claims.

The first implementation should be read-only. It should not run fixers, formatters, migrations, batch jobs, live order paths, or cleanup scripts.

## 10. Current Boundary

This document is only the review-rule checklist.

It does not create a GitHub Actions workflow, does not call an LLM API, and does not change any trading, gate, dashboard, or batch behavior.
