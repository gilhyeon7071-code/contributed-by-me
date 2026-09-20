param(
  [string]$RootB = "E:\vibe\buffett",
  [int]$Port = 8501,
  [int]$IntervalMinutes = 60
)

$ErrorActionPreference = "Continue"

$officialPy = "E:\1_Data\_runtime\python312-embed\python.exe"
$fallbackPy = Join-Path $RootB ".venv\Scripts\python.exe"
$py = if (Test-Path $officialPy) { $officialPy } else { $fallbackPy }
$builder = Join-Path $RootB "tools\build_dashboard_state_v2.py"
$integratedBuilder = Join-Path $PSScriptRoot "build_integrated_ops_snapshot.py"
$reactDir = Join-Path $RootB "react_forensic_dashboard"
$logDir = Join-Path $RootB "runs\_scheduler_logs"
$lockPath = Join-Path $logDir "VIBE_Dashboard_State_Hourly.lock"
$policyPath = Join-Path (Split-Path $PSScriptRoot -Parent) "dashboard_refresh_policy.json"

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

function Write-LoopLog {
  param([string]$Message)
  $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  $line = "[$ts] $Message"
  $logFile = Join-Path $logDir "VIBE_Dashboard_State_Hourly.loop.log"
  Add-Content -Path $logFile -Value $line -Encoding UTF8
}

function Test-DashboardListening {
  param([int]$CheckPort)
  try {
    $rows = netstat -ano | Select-String -Pattern ":$CheckPort\s+.*LISTENING"
    return ($rows.Count -gt 0)
  } catch {
    return $false
  }
}

function Invoke-ProcessWithTimeout {
  param(
    [string]$FilePath,
    [string[]]$ArgumentList,
    [string]$WorkingDirectory,
    [int]$TimeoutSeconds,
    [string]$LogPath
  )

  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $FilePath
  $psi.Arguments = (($ArgumentList | ForEach-Object {
    if ($_ -match '^[/-][A-Za-z]+$') { $_ } else { '"' + ($_ -replace '"', '\"') + '"' }
  }) -join ' ')
  $psi.WorkingDirectory = $WorkingDirectory
  $psi.UseShellExecute = $false
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError = $true

  $proc = New-Object System.Diagnostics.Process
  $proc.StartInfo = $psi
  [void]$proc.Start()
  if (-not $proc.WaitForExit($TimeoutSeconds * 1000)) {
    try { $proc.Kill($true) } catch { try { $proc.Kill() } catch {} }
    Add-Content -Path $LogPath -Value "[TIMEOUT] $FilePath $($ArgumentList -join ' ') exceeded ${TimeoutSeconds}s" -Encoding UTF8
    return 124
  }

  $stdout = $proc.StandardOutput.ReadToEnd()
  $stderr = $proc.StandardError.ReadToEnd()
  if (-not [string]::IsNullOrWhiteSpace($stdout)) {
    Add-Content -Path $LogPath -Value $stdout -Encoding UTF8
  }
  if (-not [string]::IsNullOrWhiteSpace($stderr)) {
    Add-Content -Path $LogPath -Value $stderr -Encoding UTF8
  }
  return $proc.ExitCode
}

function Acquire-Lock {
  if (Test-Path $lockPath) {
    try {
      $prev = (Get-Content -Path $lockPath -Raw).Trim()
      $prevPid = 0
      [void][int]::TryParse($prev, [ref]$prevPid)
      if ($prevPid -gt 0) {
        $prevProc = Get-CimInstance Win32_Process -Filter "ProcessId=$prevPid" -ErrorAction SilentlyContinue
        $prevCmd = if ($prevProc) { [string]$prevProc.CommandLine } else { "" }
        if ($prevCmd -like "*vibe_dashboard_state_hourly_loop.ps1*") {
          Write-LoopLog "another loop is running (pid=$prevPid), exit."
          return $false
        }
        if ($prevProc) {
          Write-LoopLog "stale loop lock pid reused by non-loop process (pid=$prevPid); removing lock."
        }
      }
    } catch {}
    Remove-Item -Force $lockPath -ErrorAction SilentlyContinue
  }

  Set-Content -Path $lockPath -Value $PID -Encoding ascii
  return $true
}

function Get-RefreshPolicy {
  if (-not (Test-Path $policyPath)) { return $null }
  try {
    return Get-Content -Path $policyPath -Raw -Encoding UTF8 | ConvertFrom-Json
  } catch {
    Write-LoopLog "policy load failed: $($_.Exception.Message)"
    return $null
  }
}

function Convert-HhmmToMinutes {
  param([string]$Value, [int]$Fallback)
  try {
    if ([string]::IsNullOrWhiteSpace($Value)) { return $Fallback }
    $parts = $Value.Split(':')
    if ($parts.Count -ne 2) { return $Fallback }
    $hh = [int]$parts[0]
    $mm = [int]$parts[1]
    if ($hh -eq 24 -and $mm -eq 0) { return 1440 }
    if ($hh -lt 0 -or $hh -gt 23 -or $mm -lt 0 -or $mm -gt 59) { return $Fallback }
    return ($hh * 60 + $mm)
  } catch {
    return $Fallback
  }
}

function Get-StateRefreshWindow {
  param($Policy)
  $defaults = @{
    pre_market = @{ label = "장전"; start = "00:00"; end = "09:00"; state_refresh_minutes = 30 }
    intraday = @{ label = "장중"; start = "09:00"; end = "15:30"; state_refresh_minutes = 5 }
    after_close = @{ label = "장후"; start = "15:30"; end = "18:00"; state_refresh_minutes = 0 }
    night = @{ label = "야간"; start = "18:00"; end = "24:00"; state_refresh_minutes = 60 }
    weekend = @{ label = "주말/휴장"; state_refresh_minutes = 60 }
  }

  $windows = $null
  if ($Policy -and $Policy.windows) { $windows = $Policy.windows }

  $now = Get-Date
  if ($now.DayOfWeek -in @('Saturday', 'Sunday')) {
    $cfg = if ($windows -and $windows.weekend) { $windows.weekend } else { $defaults.weekend }
    return @{
      Name = "weekend"
      Label = [string]$cfg.label
      StateRefreshMinutes = [int]$cfg.state_refresh_minutes
    }
  }

  $curMin = $now.Hour * 60 + $now.Minute
  foreach ($name in @('pre_market', 'intraday', 'after_close', 'night')) {
    $cfg = if ($windows -and $windows.$name) { $windows.$name } else { $defaults[$name] }
    $startMin = Convert-HhmmToMinutes ([string]$cfg.start) 0
    $endMin = Convert-HhmmToMinutes ([string]$cfg.end) 1440
    if ($curMin -ge $startMin -and $curMin -lt $endMin) {
      return @{
        Name = $name
        Label = [string]$cfg.label
        StateRefreshMinutes = [int]$cfg.state_refresh_minutes
      }
    }
  }

  return @{
    Name = "night"
    Label = "야간"
    StateRefreshMinutes = 60
  }
}

if (-not (Acquire-Lock)) { exit 0 }

try {
  if (-not (Test-Path $py)) {
    Write-LoopLog "python not found: $py"
    exit 1
  }
  if (-not (Test-Path $builder)) {
    Write-LoopLog "builder not found: $builder"
    exit 1
  }
  if (-not (Test-Path $integratedBuilder)) {
    Write-LoopLog "integrated builder not found: $integratedBuilder"
    exit 1
  }
  if (-not (Test-Path $reactDir)) {
    Write-LoopLog "react dir not found: $reactDir"
    exit 1
  }

  $policy = Get-RefreshPolicy
  $lastRunAt = (Get-Date).AddMinutes(-1 * [Math]::Max(1, $IntervalMinutes))
  Write-LoopLog "hourly loop start (pid=$PID, default_interval=${IntervalMinutes}m, port=$Port)"

  while ($true) {
    if (-not (Test-DashboardListening -CheckPort $Port)) {
      Write-LoopLog "dashboard port $Port not listening; stop loop."
      break
    }

    $window = Get-StateRefreshWindow -Policy $policy
    $intervalNow = [int]$window.StateRefreshMinutes
    if ($intervalNow -le 0) {
      Start-Sleep -Seconds 60
      continue
    }

    $elapsed = ((Get-Date) - $lastRunAt).TotalMinutes
    if ($elapsed -lt $intervalNow) {
      Start-Sleep -Seconds 60
      continue
    }

    $ts = Get-Date -Format "yyyyMMdd_HHmmss"
    $runLog = Join-Path $logDir ("VIBE_Dashboard_State_Hourly_{0}.log" -f $ts)
    try {
      Set-Location $RootB
      & $py $integratedBuilder *> $runLog
      if ($LASTEXITCODE -ne 0) {
        Write-LoopLog "refresh failed: integrated_ops failed(exit=$LASTEXITCODE) [$($window.Label) ${intervalNow}m] -> $runLog"
        $lastRunAt = Get-Date
        continue
      }
      & $py $builder *>> $runLog
      if ($LASTEXITCODE -eq 0) {
        Push-Location $reactDir
        try {
          $opsExit = Invoke-ProcessWithTimeout -FilePath "cmd.exe" -ArgumentList @("/d", "/s", "/c", "npm.cmd run prepare-ops") -WorkingDirectory $reactDir -TimeoutSeconds 180 -LogPath $runLog
          if ($opsExit -eq 0) {
            Write-LoopLog "refresh ok [$($window.Label) ${intervalNow}m] + ops-data -> $runLog"
          } else {
            Write-LoopLog "refresh partial: state ok, ops-data failed(exit=$opsExit) [$($window.Label) ${intervalNow}m] -> $runLog"
          }
        } finally {
          Pop-Location
        }
      } else {
        Write-LoopLog "refresh failed(exit=$LASTEXITCODE) [$($window.Label) ${intervalNow}m] -> $runLog"
      }
      $lastRunAt = Get-Date
    } catch {
      Write-LoopLog "refresh exception: $($_.Exception.Message)"
      $lastRunAt = Get-Date
    }
  }
}
finally {
  Remove-Item -Force $lockPath -ErrorAction SilentlyContinue
}


