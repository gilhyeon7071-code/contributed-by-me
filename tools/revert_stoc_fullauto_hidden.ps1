# Revert STOC_FullAuto to the original launch (full_auto.bat directly).
# Requires Administrator. Apply with apply_stoc_fullauto_hidden.ps1
#
# Original task definition XML:
#   E:\1_Data\backup\20260821_stoc_fullauto_hidden\20260821_171241\STOC_FullAuto.xml
# Note: ASCII only on purpose (Windows PowerShell 5.1 codepage issue).

$ErrorActionPreference = 'Stop'
$task = 'STOC_FullAuto'
$bat  = 'E:\1_Data\full_auto.bat'

Write-Host "=== BEFORE"
(Get-ScheduledTask -TaskName $task).Actions | ForEach-Object {
    Write-Host ("   exec: " + $_.Execute + "   args: [" + $_.Arguments + "]")
}

try {
    $action = New-ScheduledTaskAction -Execute $bat
    Set-ScheduledTask -TaskName $task -Action $action | Out-Null
    Write-Host ""
    Write-Host "[OK] reverted to direct launch" -ForegroundColor Green
} catch {
    Write-Host ""
    Write-Host ("[FAILED] " + $_.Exception.Message) -ForegroundColor Red
    Read-Host "Press Enter to close"
    exit 1
}

Write-Host ""
Write-Host "=== AFTER"
$t = Get-ScheduledTask -TaskName $task
$t.Actions | ForEach-Object {
    Write-Host ("   exec: " + $_.Execute + "   args: [" + $_.Arguments + "]")
}

Write-Host ""
Read-Host "Press Enter to close"
