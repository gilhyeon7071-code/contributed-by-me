# Retention and Archive Policy

Last updated: 2026-05-06

## 1. Purpose
Unify cleanup, archive, and backup-retention decisions without changing the existing safety model.

This policy is a classification layer. It does not authorize automatic deletion or movement.

## 2. Decision Classes
- `keep`: operational source, current evidence, locks, state, config, recent backup, or otherwise protected data.
- `archive_candidate`: old or misplaced artifact that may be moved to an archive after dry-run review.
- `delete_candidate`: disposable cache or constrained temporary artifact that may be deleted only through the approved cleanup tool and explicit approval.
- `review_required`: ambiguous, referenced, credential-like, or cross-root item that must be inspected before any action.

## 3. Retention Defaults
- Backup retention: 90 days.
- RootB runs retention: 90 days.
- Temporary file retention: 7 days.
- Diagnostic temporary retention: 30 days.
- Cache directories: disposable, but still reported before apply.

## 4. Safety Rules
- Default mode is dry-run.
- No tool in this policy should hard-delete or move files unless a separate apply command and user approval are explicitly provided.
- Root-level cleanup remains archive-first and non-destructive.
- Backup cleanup remains review-first. Expired backups are candidates, not automatic deletes.
- Operational evidence must not be removed from the default path when it is the latest, referenced, or needed for audit.
- Credential-like files are always `review_required`.

## 5. Tooling
Read-only unified classification:
- `E:\1_Data\run_retention_policy.bat`
- `E:\1_Data\tools\maintenance\retention_policy.py`

Existing action tools remain separate:
- Backup/cache/temp cleanup dry-run: `E:\1_Data\run_temp_cleanup_policy.bat`
- Root artifact archive dry-run/apply: `E:\1_Data\tools\cleanup_root_artifacts.ps1`

## 6. Apply Rule
The unified classifier does not apply changes.

If an action is needed:
1. Run the unified classifier and inspect the JSON report.
2. Run the specific existing tool in dry-run mode.
3. Confirm candidate paths and safety class.
4. Proceed only after explicit user approval for that specific apply operation.

## 7. Evidence
Unified classification writes:
- dated report: `E:\1_Data\2_Logs\retention_policy_status_YYYYMMDD_HHMMSS.json`
- latest report: `E:\1_Data\2_Logs\retention_policy_status_latest.json`
