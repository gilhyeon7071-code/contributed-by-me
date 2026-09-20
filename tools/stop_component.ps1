# stop_component.ps1 - 예약작업을 **되돌릴 수 있게** 멈춘다.
#
# 왜 이렇게 하나 (2026-09-10, 사용자 방침):
#   "결정 후 검증이 맞을 수도 있다" - 멈춰보고 뭐가 깨지는지 보는 것이
#   정적 의존성 분석보다 강한 검증이다.
#   **단 그것이 성립하려면 되돌릴 수 있어야 한다.** 그래서:
#     1) 정의(XML)를 먼저 백업한다  - 나중에 삭제됐어도 복원 가능
#     2) 삭제가 아니라 **비활성화** 한다 - 복구가 한 줄
#     3) 무엇을 왜 멈췄는지 append-only 원장에 남긴다
#        (이름에 날짜가 없어 log_cleanup_30d 대상이 아니다)
#
# 사용:
#   powershell -File E:/1_Data/tools/stop_component.ps1 -Task 'NAME' -Reason '왜'
#   powershell -File E:/1_Data/tools/stop_component.ps1 -Task 'NAME' -Restore
#
# 복구:
#   -Restore 를 붙이면 다시 켠다. 원장에도 복구 기록이 남는다.

param(
    [Parameter(Mandatory = $true)][string]$Task,
    [string]$Reason = '',
    [switch]$Restore
)

$ErrorActionPreference = 'Stop'
$bakDir = 'E:\1_Data\2_Logs\stopped_tasks'
$ledger = 'E:\1_Data\2_Logs\stopped_components_ledger.jsonl'
if (-not (Test-Path $bakDir)) { New-Item -ItemType Directory -Path $bakDir | Out-Null }

$t = Get-ScheduledTask -TaskName $Task -ErrorAction SilentlyContinue
if (-not $t) { Write-Host "[ERR] 예약작업 없음: $Task"; exit 1 }

$info = Get-ScheduledTaskInfo -TaskName $Task -ErrorAction SilentlyContinue
$before = $t.State

if ($Restore) {
    if ($before -ne 'Disabled') {
        Write-Host "[SKIP] $Task 는 이미 켜져 있습니다 (state=$before)"
    } else {
        Enable-ScheduledTask -TaskName $Task | Out-Null
        Write-Host "[RESTORE] $Task 다시 켬"
    }
    $act = 'restore'
} else {
    # 정의 백업 (삭제된 뒤에도 복원할 수 있게)
    $xmlPath = Join-Path $bakDir ($Task -replace '[\\/:*?"<>|]', '_')
    $xmlPath = "$xmlPath.xml"
    Export-ScheduledTask -TaskName $Task | Set-Content -Path $xmlPath -Encoding UTF8
    Write-Host "[BACKUP] $xmlPath"

    if ($before -eq 'Disabled') {
        Write-Host "[SKIP] $Task 는 이미 꺼져 있습니다"
    } else {
        Disable-ScheduledTask -TaskName $Task | Out-Null
        Write-Host "[STOP] $Task 비활성화"
    }
    $act = 'stop'
}

$after = (Get-ScheduledTask -TaskName $Task).State
Write-Host "        state: $before -> $after"

$rec = [ordered]@{
    ts       = (Get-Date).ToString('s')
    action   = $act
    task     = $Task
    before   = "$before"
    after    = "$after"
    reason   = $Reason
    last_run = if ($info -and $info.LastRunTime.Year -gt 1999) { $info.LastRunTime.ToString('s') } else { $null }
    last_rc  = if ($info) { ('0x{0:X}' -f $info.LastTaskResult) } else { $null }
    next_run = if ($info -and $info.NextRunTime) { $info.NextRunTime.ToString('s') } else { $null }
    restore  = "powershell -File E:/1_Data/tools/stop_component.ps1 -Task '$Task' -Restore"
}
($rec | ConvertTo-Json -Compress) | Add-Content -Path $ledger -Encoding UTF8
Write-Host "[LEDGER] $ledger"
