# Contract and Scope Audit

Use this lane before judging whether implementation is suitable.

## Establish the target contract

Read and compare:

- `E:\1_Data\docs\references\SYSTEM_DEFINITION.md`
- `E:\1_Data\docs\references\OBJECTIVE_LEDGER.md`
- `E:\1_Data\.agent\PLANS.md`
- the user's latest explicit decision
- current runtime artifacts that identify the active strategy or generator version

Do not choose a target merely because a section is newer or says `final`. Record:

- approved operational target
- proposed or research-only target
- retired logic
- preserved infrastructure
- unresolved policy decisions

If the sources disagree, stop implementation-fit conclusions at `TARGET_CONTRACT_CONFLICT`. Continue only with facts that do not depend on choosing one side.

## Stage contract

For each applicable stage, identify its inputs, decision rule, output, consumer, failure behavior, and evidence:

1. market/regime context
2. eligible universe and candidate discovery
3. candidate classification and scoring
4. entry decision and timing
5. sizing and exposure
6. order construction and dispatch
7. fill handling
8. holding management
9. exit
10. risk block and kill switch
11. ledger and statistics
12. post-trade evaluation

Check missing definitions and contradictions, including market scope, as-of timing, rebalance calendar, data gaps, threshold equality, precedence between risk rules, partial fills, retries, and recovery.

## Required output

Use a table with:

```text
stage | declared policy | active evidence | conflict or omission | worst case | verdict
```

Do not propose a code replacement until the target contract is explicit.
