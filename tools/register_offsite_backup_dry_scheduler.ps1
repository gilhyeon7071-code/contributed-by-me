$ErrorActionPreference = "Stop"

$taskName = "Buffett-Offsite-Backup-Dry-Daily"
$root = "E:\1_Data"
$runner = Join-Path $root "run_offsite_backup_dry_hidden.ps1"

if (-not (Test-Path -LiteralPath $runner)) {
    throw "runner not found: $runner"
}

$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$runner`""
$trigger = New-ScheduledTaskTrigger -Daily -At "18:40"
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 30)
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null
Write-Output "[OK] registered $taskName"
