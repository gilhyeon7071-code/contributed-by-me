# Switch STOC_FullAuto to a hidden (no console window) launch.
# Requires Administrator. Revert with revert_stoc_fullauto_hidden.ps1
#
# Reference: PLANS 2026-08-21 (33)
# Why: the morning task ran full_auto.bat directly, so a console window stayed
#      on the desktop for 16-75 minutes. On 2026-08-21 08:40:47 the batch died
#      with STATUS_CONTROL_C_EXIT. The evening task (VIBE_Paper_Daily) already
#      uses a hidden wscript wrapper. This makes the morning task match it.
# Note: ASCII only on purpose. Windows PowerShell 5.1 reads a BOM-less UTF-8
#       .ps1 as the ANSI codepage, which corrupts non-ASCII text.

$ErrorActionPreference = 'Stop'
$task = 'STOC_FullAuto'
$vbs  = 'E:\1_Data\full_auto_hidden.vbs'

Write-Host "=== BEFORE"
(Get-ScheduledTask -TaskName $task).Actions | ForEach-Object {
    Write-Host ("   exec: " + $_.Execute + "   args: [" + $_.Arguments + "]")
}

if (-not (Test-Path $vbs)) {
    Write-Host "[FAILED] wrapper not found: $vbs" -ForegroundColor Red
    Read-Host "Press Enter to close"
    exit 2
}

try {
    $action = New-ScheduledTaskAction -Execute 'wscript.exe' -Argument $vbs
    Set-ScheduledTask -TaskName $task -Action $action | Out-Null
    Write-Host ""
    Write-Host "[OK] switched to hidden launch" -ForegroundColor Green
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
$i = $t | Get-ScheduledTaskInfo
Write-Host ("   state=" + $t.State + "  next=" + $i.NextRunTime + "  lastRc=" + $i.LastTaskResult)

Write-Host ""
Read-Host "Press Enter to close"
