param(
    [switch]$Apply,
    [string]$TaskName = "Buffett-Ops-Sanity-Quick",
    [string]$At = "08:45"
)

$ErrorActionPreference = "Stop"

$root = "E:\1_Data"
$runner = Join-Path $root "run_ops_sanity_ci.bat"
$logDir = Join-Path $root "2_Logs"
$dryRunPath = Join-Path $logDir "ops_sanity_scheduler_plan_latest.json"

if (-not (Test-Path -LiteralPath $runner)) {
    throw "runner not found: $runner"
}
if (-not (Test-Path -LiteralPath $logDir)) {
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
}

$taskCommand = "cmd.exe /c call `"$runner`""
$plan = [ordered]@{
    status = if ($Apply) { "APPLY" } else { "DRY_RUN" }
    task_name = $TaskName
    schedule = "DAILY"
    at = $At
    command = $taskCommand
    root = $root
    generated_at = (Get-Date).ToUniversalTime().ToString("s") + "Z"
}
$plan | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $dryRunPath -Encoding UTF8

if (-not $Apply) {
    Write-Output "[DRY_RUN] wrote $dryRunPath"
    Write-Output "[DRY_RUN] task=$TaskName schedule=DAILY at=$At command=$taskCommand"
    exit 0
}

& schtasks.exe /Create `
    /TN $TaskName `
    /TR $taskCommand `
    /SC DAILY `
    /ST $At `
    /F | Out-Null

Write-Output "[OK] registered $TaskName"
