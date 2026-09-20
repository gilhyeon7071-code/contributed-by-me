# RootA Logic Tier Procedure

## Scope
- Root: `E:\1_Data`
- Purpose: classify trading logic, diagnostics, and artifacts by operating impact before any change is treated as usable.
- This procedure adapts `D:\개발절차.txt` to the RootA runtime. It is not a direct copy of that text.
- This procedure does not change Gate, LOCK, STOP, risk, score, order, fill, ledger, broker, or dashboard behavior by itself.

## Core Principle
Every logic change or recurring artifact must declare its operating impact before it is interpreted:

- `logic_tier`
- `trading_effect`
- `policy_effect`
- `order_path_effect`
- `score_effect`
- `source_artifacts`
- `output_artifacts`
- `validation_status`
- `promotion_status`
- `promotion_blockers`

If these fields are missing, the item cannot be treated as Stable or as evidence for live policy relaxation.

## Tiers

### Stable
Current operating logic or artifacts that can affect candidate generation, scoring, entry, exit, orders, fills, ledger, statistics, Gate, LOCK, or risk behavior.

Rules:
- Must have a clear version or current owner artifact.
- Must preserve RootA STOP conditions and FAIL-CLOSED behavior.
- Any change requires backup, PLANS entry, validation evidence, and explicit policy classification.
- Code presence alone is not Stable evidence. Runtime artifact evidence is required.

Examples:
- `paper_engine.py`
- `run_paper_daily.bat`
- `paper\paper_engine_config.json`
- `paper\orders_*_exec.xlsx`
- `paper\fills.csv`
- ledger/stat chain artifacts

### Candidate
A proposed operating change that may later become Stable, but is not allowed to change live behavior yet.

Rules:
- Must run in parallel or read-only form first.
- Must compare against Stable on the same date, same universe, same cost and slippage assumptions, and same D interpretation.
- Must produce validation artifacts with candidate count, BUY count, common symbols, candidate-only symbols, Stable-only symbols, MFE, MAE, realized return, MDD, PF or equivalent metrics.
- Cannot be promoted while official batch, D consistency, policy, FAIL-CLOSED, or regression checks are unresolved.

Examples:
- entry exception candidates
- score or threshold changes
- new candidate pool rules
- surge or normal-entry policy candidates

### Research
An idea, experiment, analysis, or detector that is not yet a Candidate.

Rules:
- Must not feed order generation or Stable score behavior.
- Must declare `trading_effect=false`.
- Must not be used as direct proof for policy promotion.
- May become Candidate only after the target behavior, comparison method, and acceptance metrics are defined.

Examples:
- idea probes
- exploratory markout
- microstructure or news quality research

### Shadow
A read-only or observe-only runtime artifact that mirrors current behavior, explains missed opportunities, or collects future evidence.

Rules:
- Must declare `trading_effect=false`.
- Must declare whether `policy_effect` is false or whether it can affect candidate input without order dispatch.
- Must not be interpreted as buy approval.
- Missing inputs must fail closed into `WATCH`, `NO_SIGNAL`, `BLOCKED`, `NOT_EVALUABLE`, or equivalent non-approval states.

Examples:
- news shadow stage
- surge probe reports
- follow-through markout
- candidate action review

### Hotfix
A narrow fix for execution failure, data corruption, schema breakage, encoding breakage, logging, validation, or fail-closed enforcement.

Rules:
- Must not change strategy intent or policy thresholds unless explicitly approved as a policy change.
- Does not need Candidate performance promotion when the issue is operational correctness.
- Still requires backup, minimal patch, and validation.
- Must report display/status changes separately from root-cause fixes.

Examples:
- batch encoding failure fix
- broken data source fallback guard
- validator crash fix
- fail-closed missing input guard

### Deprecated
Old logic retained for audit or rollback reference.

Rules:
- Must not be used as current Stable proof.
- Must not feed current order or score paths unless explicitly re-promoted through Candidate validation.

## Promotion Rules

Candidate can become Stable only when all of these are true:

1. Functional validation: PASS.
2. Consistency validation: PASS.
3. Operational reflection validation: PASS.
4. Policy validation: PASS.
5. FAIL-CLOSED validation: PASS.
6. Regression validation: PASS.
7. D rule and STOP conditions are clean for the affected path.
8. `orders(D) -> fills(D) -> ledger -> stats` is verified when the change touches that chain.
9. Promotion evidence is recorded in PLANS and linked from the registry.

If any item is missing, `promotion_status` must remain `blocked`, `deferred`, `research_only`, or `candidate_only`.

## Required Registry Fields

Use `config\logic_tier_registry.json` as the current RootA registry.

Each entry should include:

- `logic_id`: stable unique identifier.
- `logic_tier`: `stable`, `candidate`, `research`, `shadow`, `hotfix`, or `deprecated`.
- `status`: current status such as `active`, `blocked`, `plan_only`, `read_only`, or `deprecated`.
- `owner_path`: main code or document path.
- `source_artifacts`: input artifacts.
- `output_artifacts`: output artifacts.
- `trading_effect`: boolean.
- `policy_effect`: boolean.
- `order_path_effect`: boolean.
- `score_effect`: boolean.
- `stable_comparison_required`: boolean.
- `promotion_status`: `stable_current`, `candidate_only`, `research_only`, `shadow_only`, `hotfix_applied`, `blocked`, or `deprecated`.
- `promotion_blockers`: list of unresolved blockers.
- `validation_evidence`: paths to latest evidence.
- `notes`: short plain-language note.

## Interpretation Rules

- `PASS` in a shadow or diagnostic artifact means the artifact ran, not that the strategy is approved.
- `policy_effect=false` means it cannot change policy by itself.
- `trading_effect=false` means it cannot submit or approve orders by itself.
- `policy_effect=true` with `trading_effect=false` means the artifact may affect candidate input or policy review, but still cannot dispatch orders.
- `Stable` cannot be inferred from file age, code existence, or old documentation.
- Official runtime artifacts and batch logs override older summaries.

