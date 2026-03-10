param(
  [switch]$Apply,
  [switch]$BuildDashboard,
  [switch]$DoHousekeeping,
  [switch]$DoMove,
  [switch]$DoDelete,
  [switch]$HousekeepingMove,
  [switch]$HousekeepingDelete,
  [switch]$SkipHousekeeping
)

$ErrorActionPreference = "Stop"

$Canonical = "E:\vibe\tools\run_daily_guard.ps1"
Write-Host "[HARD_FAIL] Deprecated launcher: E:\1_Data\tools\run_daily_guard.ps1" -ForegroundColor Red
Write-Host "[HINT] Use canonical guard script: $Canonical" -ForegroundColor Yellow
Write-Host "[HINT] Example: pwsh -NoProfile -File $Canonical -Apply" -ForegroundColor Yellow

exit 2
