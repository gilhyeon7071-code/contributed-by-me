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
$inRestartWindow = ($hhmm -ge 830 -and $hhmm -lt 1530)
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
try {
  $procs = Get-CimInstance Win32_Process | Where-Object {
    ($_.CommandLine -like '*intraday_paper_loop.py*') -or ($_.CommandLine -like '*run_intraday_paper.bat*')
  }
  $pidsBefore = @($procs | ForEach-Object { [int]$_.ProcessId } | Sort-Object -Unique)
} catch {}

$action = 'none'
$started = $false
$startReason = ''
$killedPids = @()

if ($stale) {
  if ($dailyBatchLockExists) {
    $action = 'daily_batch_skip'
    $startReason = 'run_paper_daily_lock_active'
  } elseif ($hardBlockFlagExists) {
    $action = 'hard_blocked_skip'
    $startReason = 'hard_block_flag_active'
  } elseif (-not $inRestartWindow) {
    $action = 'session_closed_skip'
    $startReason = 'outside_intraday_restart_window'
  } elseif ($lockExists -and $lockAgeSec -lt ($staleThresholdSec * 2) -and $pidsBefore.Count -gt 0) {
    $action = 'alive_skip'
    $startReason = 'loop_lock_recent'
  } elseif ($pidsBefore.Count -gt 1) {
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
  restart_window = '0830-1530'
  in_restart_window = [bool]$inRestartWindow
  cooldown_ok = [bool]$cooldownOk
  action = $action
  dry_run = [bool]$DryRun
  loop_pids_before = @($pidsBefore)
  killed_pids = @($killedPids)
  started = [bool]$started
  start_reason = $startReason
  last_restart_at = $(if (($action -eq 'restart' -or $action -eq 'duplicate_restart') -and ($DryRun -or $started)) { $now.ToString('s') } elseif ($lastRestartAt) { $lastRestartAt.ToString('s') } else { $null })
}

($out | ConvertTo-Json -Depth 8) | Set-Content -Path $watchdogStatusPath -Encoding UTF8
Write-Output ("[WATCHDOG] status={0} age={1}s action={2} started={3}" -f $out.status, $out.loop_status_age_sec, $out.action, $out.started)
