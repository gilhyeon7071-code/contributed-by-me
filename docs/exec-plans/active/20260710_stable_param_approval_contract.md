# 2026-07-10 Stable Parameter Approval Contract

## Scope

- Prevent unapproved HPO stable artifacts from creating natural tradable candidates.
- Preserve the existing stable artifact when an HPO search does not promote a candidate.
- Reuse one quality-gate calculation in candidate generation and backtest reporting.
- Do not substitute fallback parameters into live candidate generation.

## Backup

- `E:\1_Data\backup\20260710_stable_param_approval_contract\20260710_094000\`

## Validation

1. Syntax compile all changed Python files.
2. Unit-check gate PASS/FAIL and non-promotion stable preservation.
3. Run candidate generator with the current rejected stable artifact.
4. Confirm meta records gate failure and all resulting fallback rows remain observe-only.
5. Confirm stable JSON hash and mtime are unchanged.
## Result
- Current rejected stable gate FAIL is reflected in candidate meta.
- All generated fallback rows remain `NONE` observe-only.
- Non-promoted stable persistence and HPO unit tests passed.
- Full HPO rerun was not executed.