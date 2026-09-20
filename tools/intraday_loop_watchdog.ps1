param(
  [double]$StaleMin = 12,
  [int]$RestartCooldownSec = 180,
  [switch]$DryRun
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$logDir = Join-Path $root '2_Logs'
$loopStatusPath = Join-Path $logDir 'intraday_loop_status_latest.json'
$watchdogStatusPath = Join-Path $logDir 'intraday_watchdog_status_latest.json'
$loopLockDir = Join-Path $logDir 'run_intraday_paper.lock'
$dailyBatchLockDir = Join-Path $logDir 'run_paper_daily.lock'
$runBatPath = Join-Path $root 'run_intraday_paper.bat'

if (-not (Test-Path $logDir)) {
  New-Item -Path $logDir -ItemType Directory -Force | Out-Null
}

$now = Get-Date

$isMarketOpen = $true
try {
  $pyExe = Join-Path $root "_runtime\python312-embed\python.exe"
  $res = & $pyExe -c "import sys; sys.path.insert(0, r'$root'); from holiday_manager import HolidayManager; from datetime import datetime; print('OPEN' if HolidayManager().is_market_open(datetime.now().strftime('%Y%m%d')) else 'CLOSED')"
  if ($res -like "*CLOSED*") {
    $isMarketOpen = $false
  }
} catch {}

if (-not $isMarketOpen) {
  $statusObj = @{
    ts = $now.ToString("s")
    status = "STANDBY"
    action = "none"
    reason = "market closed (holiday/weekend)"
  }
  $statusObj | ConvertTo-Json -Depth 10 | Set-Content $watchdogStatusPath -Encoding UTF8
  exit 0
}
$hhmm = [int]$now.ToString('HHmm')
# 2026-08-20: 재시작 창을 1530 -> 2130 으로 넓힌다.
# 루프는 마감 후에도 offhours_* 단계로 상태/손익/신선도 산출물을 유지한다(60초 주기).
# 그것이 멈추면 다음 기동 시 run_intraday_paper.bat PREFLIGHT 가 freshness FAIL 을 내고
# run_paper_daily.bat(2026-08-20 실측 53분)를 장전에 돌린다. 대가를 다음 날 아침에 치른다.
# 21:30 부터는 VIBE_Paper_Daily 배치가 같은 산출물을 갱신하므로 루프의 유지 역할이 중복이다.
# 배치와의 충돌은 daily_batch_skip 분기와 루프 쪽 배치 락 인지(2026-08-20)가 이중으로 막는다.
$inRestartWindow = ($hhmm -ge 830 -and $hhmm -lt 2130)
$staleThresholdSec = [Math]::Max(60.0, $StaleMin * 60.0)
$ageSec = $null
if (Test-Path $loopStatusPath) {
  $ageSec = [Math]::Max(0.0, ($now - (Get-Item $loopStatusPath).LastWriteTime).TotalSeconds)
}
$stale = ($null -eq $ageSec) -or ($ageSec -gt $staleThresholdSec)
$lockExists = Test-Path $loopLockDir
$dailyBatchLockExists = Test-Path $dailyBatchLockDir
$lockAgeSec = $null
if ($lockExists) {
  $lockAgeSec = [Math]::Max(0.0, ($now - (Get-Item $loopLockDir).LastWriteTime).TotalSeconds)
}
$dailyBatchLockAgeSec = $null
if ($dailyBatchLockExists) {
  $dailyBatchLockAgeSec = [Math]::Max(0.0, ($now - (Get-Item $dailyBatchLockDir).LastWriteTime).TotalSeconds)
}

$hardBlockFlagPath = Join-Path $logDir 'paper_intraday_hard_blocked.flag'
$hardBlockFlagExists = Test-Path $hardBlockFlagPath

$lastRestartAt = $null
if (Test-Path $watchdogStatusPath) {
  try {
    $prev = Get-Content -Path $watchdogStatusPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($prev.last_restart_at) {
      $lastRestartAt = [datetime]::Parse($prev.last_restart_at)
    }
  } catch {}
}

$cooldownOk = $true
if ($lastRestartAt) {
  $cooldownOk = (($now - $lastRestartAt).TotalSeconds -ge $RestartCooldownSec)
}

$pidsBefore = @()
$loopPids = @()
try {
  # [2026-08-25] Name 필터가 없어 명령줄에 문자열만 들어 있으면 무엇이든 셌다.
  #   실측: 실제 루프 0개인데 프로세스를 조회하던 pwsh.exe 가 1개로 잡혔다.
  #   조회하는 행위 자체가 판정을 바꾸면 안 된다. 실행 파일과 자기 자신으로 거른다.
  $procs = Get-CimInstance Win32_Process | Where-Object {
    ($_.Name -in @('cmd.exe','python.exe','pythonw.exe')) -and
    ($_.ProcessId -ne $PID) -and
    (($_.CommandLine -like '*intraday_paper_loop.py*') -or ($_.CommandLine -like '*run_intraday_paper.bat*'))
  }
  $pidsBefore = @($procs | ForEach-Object { [int]$_.ProcessId } | Sort-Object -Unique)
  # 2026-08-20: 중복 판정을 PID 개수로 하면 안 된다.
  # 정상 상태가 이미 2개다 - run_intraday_paper.bat(cmd.exe)가 부모, intraday_paper_loop.py 가 자식.
  # 이전 코드는 $pidsBefore.Count -gt 1 을 중복으로 보아 duplicate_restart 로 갔고,
  # 그 분기는 락 삭제 + Stop-Process -Force + 재시작을 한다.
  # 즉 stale 이 나고 alive_skip(락 나이 < 임계x2)이 풀리는 순간 정상 루프를 죽인다.
  # 루프 본체(intraday_paper_loop.py)만 세어 실제 중복일 때만 발동시킨다.
  $loopPids = @($procs | Where-Object { $_.CommandLine -like '*intraday_paper_loop.py*' } |
                ForEach-Object { [int]$_.ProcessId } | Sort-Object -Unique)
} catch {}

$action = 'none'
$started = $false
$startReason = ''
$killedPids = @()

if ($stale) {
  if ($dailyBatchLockExists) {
    $action = 'daily_batch_skip'
    $startReason = 'run_paper_daily_lock_active'
  } elseif ($hardBlockFlagExists -and $pidsBefore.Count -gt 0) {
    # [2026-08-25] 프로세스가 살아 있을 때만 건너뛴다.
    #   하드블록은 "신규 진입을 하지 마라" 이지 "루프를 돌리지 마라" 가 아니다.
    #   (86) 이후 루프는 블록 중에도 축소 사이클로 청산 관리를 한다.
    #   2026-08-24 19:01 루프가 죽은 뒤 13시간 동안 pidsBefore 가 비었는데도
    #   hard_blocked_skip 으로 넘겨 장 시작까지 루프가 없었다.
    $action = 'hard_blocked_skip'
    $startReason = 'hard_block_flag_active'
  } elseif (-not $inRestartWindow) {
    $action = 'session_closed_skip'
    $startReason = 'outside_intraday_restart_window'
  } elseif ($lockExists -and $lockAgeSec -lt ($staleThresholdSec * 2) -and $pidsBefore.Count -gt 0) {
    $action = 'alive_skip'
    $startReason = 'loop_lock_recent'
  } elseif ($loopPids.Count -gt 1) {
    $action = 'duplicate_restart'
    if (-not $DryRun) {
      if (Test-Path $loopLockDir) {
        try { Remove-Item -LiteralPath $loopLockDir -Recurse -Force -ErrorAction Stop } catch {}
      }
      foreach ($procId in $pidsBefore) {
        if ($procId -ne $PID) {
          try {
            Stop-Process -Id $procId -Force -ErrorAction Stop
            $killedPids += $procId
          } catch {}
        }
      }
      if (Test-Path $runBatPath) {
        try {
          Start-Process -FilePath 'cmd.exe' -ArgumentList '/c', $runBatPath -WorkingDirectory $root -WindowStyle Minimized
          $started = $true
          $startReason = 'started_after_duplicate_cleanup'
        } catch {
          $startReason = "start_failed: $($_.Exception.Message)"
        }
      } else {
        $startReason = "run_bat_missing: $runBatPath"
      }
    } else {
      $startReason = 'dry_run'
    }
  } elseif ($pidsBefore.Count -gt 0) {
    $action = 'alive_skip'
    $startReason = 'loop_process_alive'
  } elseif (-not $cooldownOk) {
    $action = 'cooldown_skip'
  } else {
    $action = 'restart'
    if (-not $DryRun) {
      if (Test-Path $loopLockDir) {
        try { Remove-Item -LiteralPath $loopLockDir -Recurse -Force -ErrorAction Stop } catch {}
      }
      foreach ($procId in $pidsBefore) {
        if ($procId -ne $PID) {
          try {
            Stop-Process -Id $procId -Force -ErrorAction Stop
            $killedPids += $procId
          } catch {}
        }
      }
      if (Test-Path $runBatPath) {
        try {
          Start-Process -FilePath 'cmd.exe' -ArgumentList '/c', $runBatPath -WorkingDirectory $root -WindowStyle Minimized
          $started = $true
          $startReason = 'started'
        } catch {
          $startReason = "start_failed: $($_.Exception.Message)"
        }
      } else {
        $startReason = "run_bat_missing: $runBatPath"
      }
    } else {
      $startReason = 'dry_run'
    }
  }
}

$out = [ordered]@{
  ts = $now.ToString('s')
  status = $(if ($stale) { 'STALE' } else { 'FRESH' })
  loop_status_path = $loopStatusPath
  loop_status_exists = (Test-Path $loopStatusPath)
  loop_lock_exists = (Test-Path $loopLockDir)
  loop_lock_age_sec = $(if ($null -eq $lockAgeSec) { $null } else { [Math]::Round([double]$lockAgeSec, 1) })
  daily_batch_lock_exists = [bool]$dailyBatchLockExists
  daily_batch_lock_age_sec = $(if ($null -eq $dailyBatchLockAgeSec) { $null } else { [Math]::Round([double]$dailyBatchLockAgeSec, 1) })
  loop_status_age_sec = $(if ($null -eq $ageSec) { $null } else { [Math]::Round([double]$ageSec, 1) })
  stale_threshold_sec = [Math]::Round([double]$staleThresholdSec, 1)
  restart_cooldown_sec = [int]$RestartCooldownSec
  restart_window = '0830-2130'
  in_restart_window = [bool]$inRestartWindow
  cooldown_ok = [bool]$cooldownOk
  action = $action
  dry_run = [bool]$DryRun
  loop_pids_before = @($pidsBefore)
  loop_body_pids = @($loopPids)
  killed_pids = @($killedPids)
  started = [bool]$started
  start_reason = $startReason
  last_restart_at = $(if (($action -eq 'restart' -or $action -eq 'duplicate_restart') -and ($DryRun -or $started)) { $now.ToString('s') } elseif ($lastRestartAt) { $lastRestartAt.ToString('s') } else { $null })
}

($out | ConvertTo-Json -Depth 8) | Set-Content -Path $watchdogStatusPath -Encoding UTF8

# 2026-08-20: 이력 보존.
# 상태 JSON 이 _latest 하나뿐이라 1분마다 덮어써졌고, "워치독이 오늘 무엇을 건너뛰었는가"를
# 사후에 알 수 없었다. action 이 none/FRESH 가 아닌 실행만 한 줄씩 누적한다.
# none 까지 남기면 하루 1440줄이 되므로 판단이 있었던 실행만 남긴다.
try {
  if ($out.action -ne 'none') {
    $histPath = Join-Path $logDir ('intraday_watchdog_history_{0}.csv' -f $now.ToString('yyyyMMdd'))
    if (-not (Test-Path $histPath)) {
      'ts,status,action,start_reason,loop_status_age_sec,loop_lock_age_sec,daily_batch_lock_age_sec,in_restart_window,cooldown_ok,loop_pids,killed_pids,started' |
        Set-Content -Path $histPath -Encoding UTF8
    }
    $line = '{0},{1},{2},"{3}",{4},{5},{6},{7},{8},"{9}","{10}",{11}' -f `
      $out.ts, $out.status, $out.action, $out.start_reason,
      $out.loop_status_age_sec, $out.loop_lock_age_sec, $out.daily_batch_lock_age_sec,
      $out.in_restart_window, $out.cooldown_ok,
      ($out.loop_pids_before -join '|'), ($out.killed_pids -join '|'), $out.started
    Add-Content -Path $histPath -Value $line -Encoding UTF8
  }
} catch {}

Write-Output ("[WATCHDOG] status={0} age={1}s action={2} started={3}" -f $out.status, $out.loop_status_age_sec, $out.action, $out.started)
