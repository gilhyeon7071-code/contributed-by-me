# Root Cleanup Policy

Last updated: 2026-03-05

## 1. Purpose
Prevent root-level artifact pollution in `E:\1_Data` without destructive deletion.

## 2. Tool
Script:
- `E:\1_Data\tools\cleanup_root_artifacts.ps1`

Behavior:
- default: dry-run (plan only)
- `-Apply`: move suspicious root artifacts to archive under `2_Logs`

## 3. Usage
Dry-run:
- `powershell -NoProfile -ExecutionPolicy Bypass -File E:\1_Data\tools\cleanup_root_artifacts.ps1`

Apply:
- `powershell -NoProfile -ExecutionPolicy Bypass -File E:\1_Data\tools\cleanup_root_artifacts.ps1 -Apply`

## 4. Safety Model
- Uses explicit bad-name list + whitelist.
- Moves files/directories to archive; does not hard delete.
- Writes plan/manifest JSON under archive path.

## 5. Recovery
To restore an item:
1. Open archive folder in `E:\1_Data\2_Logs\root_artifacts_archive\cleanup_*`.
2. Move selected item back to `E:\1_Data`.
3. Re-run dry-run to verify no unintended candidates remain.
