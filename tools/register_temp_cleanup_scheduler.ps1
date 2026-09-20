$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$runner = Join-Path $root "run_temp_cleanup_policy.bat"
if (-not (Test-Path $runner)) {
    throw "missing runner: $runner"
}

$taskName = "Buffett-Temp-Cleanup-Daily"
$taskCommand = "cmd.exe /c call `"$runner`" DRY"

& schtasks.exe /Create `
    /TN $taskName `
    /TR $taskCommand `
    /SC DAILY `
    /ST "18:30" `
    /F | Out-Null

Write-Host "[OK] registered $taskName"
