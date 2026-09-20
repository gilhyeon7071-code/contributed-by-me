# Code Logic Audit

Use this lane after the target contract is established, or state which contract is unresolved.

## Trace implementation

Trace each stage from producer to consumer. Verify actual call sites and batch entrypoints rather than matching names or comments. Search active sources first and exclude backups, archives, generated outputs, virtual environments, logs, and temporary trees unless a specific artifact is evidence.

For each rule, record:

- definition source
- implementing file, function, and line
- caller and consumer
- input/output schema
- configuration source and fallback
- runtime artifact that can prove execution

Classify implementation as `FULL`, `PARTIAL`, `MISSING`, `CONFLICTING`, or `UNREACHABLE`.

## Logic checks

Check:

- condition inversion and precedence
- null, empty, NaN, zero-row, duplicate, and stale-input handling
- date/session boundaries and holiday behavior
- partial fills, cancellation, retry, and idempotency
- silent exceptions and permissive fallbacks
- unbounded loops, repeated work, and material time/space complexity
- hardcoded state, scores, thresholds, capital, or dates
- writer/reader schema drift
- separation of research-only and operational paths

At least one worst-case test must cover unintended order risk, duplicate processing, silent stale reuse, or false success.

## Tests

Validation is read-only unless fixes were requested. Run existing focused tests first. When a defect is found, specify a minimal reproducer and the unit or integration test that would prove the correction. Only add the test when the user authorized changes.

Passing tests for a legacy component do not prove conformance to a different target strategy. Report that distinction explicitly.
