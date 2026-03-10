param(
  [string]$RootB = "E:\vibe\buffett",
  [int]$Port = 8501,
  [int]$IntervalMinutes = 60
)

$ErrorActionPreference = "Continue"

$py = Join-Path $RootB ".venv\Scripts\python.exe"
$builder = Join-Path $RootB "tools\build_dashboard_state_v2.py"
$logDir = Join-Path $RootB "runs\_scheduler_logs"
$lockPath = Join-Path $logDir "VIBE_Dashboard_State_Hourly.lock"

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

function Acquire-Lock {
  if (Test-Path $lockPath) {
    try {
      $prev = (Get-Content -Path $lockPath -Raw).Trim()
      $prevPid = 0
      [void][int]::TryParse($prev, [ref]$prevPid)
      if ($prevPid -gt 0 -and (Get-Process -Id $prevPid -ErrorAction SilentlyContinue)) {
        Write-LoopLog "another loop is running (pid=$prevPid), exit."
        return $false
      }
    } catch {}
    Remove-Item -Force $lockPath -ErrorAction SilentlyContinue
  }

  Set-Content -Path $lockPath -Value $PID -Encoding ascii
  return $true
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

  Write-LoopLog "hourly loop start (pid=$PID, interval=${IntervalMinutes}m, port=$Port)"

  # Launch path already performs initial build once. First refresh starts after one interval.
  Start-Sleep -Seconds ([Math]::Max(60, $IntervalMinutes * 60))

  while ($true) {
    if (-not (Test-DashboardListening -CheckPort $Port)) {
      Write-LoopLog "dashboard port $Port not listening; stop loop."
      break
    }

    $ts = Get-Date -Format "yyyyMMdd_HHmmss"
    $runLog = Join-Path $logDir ("VIBE_Dashboard_State_Hourly_{0}.log" -f $ts)
    try {
      Set-Location $RootB
      & $py $builder *> $runLog
      if ($LASTEXITCODE -eq 0) {
        Write-LoopLog "refresh ok -> $runLog"
      } else {
        Write-LoopLog "refresh failed(exit=$LASTEXITCODE) -> $runLog"
      }
    } catch {
      Write-LoopLog "refresh exception: $($_.Exception.Message)"
    }

    Start-Sleep -Seconds ([Math]::Max(60, $IntervalMinutes * 60))
  }
}
finally {
  Remove-Item -Force $lockPath -ErrorAction SilentlyContinue
}
