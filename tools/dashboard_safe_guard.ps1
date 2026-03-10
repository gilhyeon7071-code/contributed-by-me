param(
  [string]$RootB = "E:\vibe\buffett",
  [string]$DashboardRelPath = "dashboard.py",
  [string]$BackupDirRelPath = "runs\_safe_backups\dashboard",
  [int]$MinSizeBytes = 5000
)

$ErrorActionPreference = "Stop"

$dashboardPath = Join-Path $RootB $DashboardRelPath
$backupDir = Join-Path $RootB $BackupDirRelPath
$logDir = Join-Path $RootB "runs\_scheduler_logs"
$logFile = Join-Path $logDir "dashboard_safe_guard.log"
$venvPy = Join-Path $RootB ".venv\Scripts\python.exe"

New-Item -ItemType Directory -Force -Path $backupDir | Out-Null
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

function Write-GuardLog {
  param([string]$Message)
  $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  $line = "[$ts] $Message"
  Add-Content -Path $logFile -Value $line -Encoding UTF8
}

function Get-LatestBackup {
  $hit = Get-ChildItem -Path $backupDir -Filter "dashboard_*.py" -File -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1
  return $hit
}

function Compile-Ok {
  param([string]$PathToCompile)
  try {
    if (Test-Path $venvPy) {
      & $venvPy -m py_compile $PathToCompile | Out-Null
      return ($LASTEXITCODE -eq 0)
    }
    & python -m py_compile $PathToCompile | Out-Null
    return ($LASTEXITCODE -eq 0)
  } catch {
    return $false
  }
}

function Backup-CurrentIfChanged {
  if (-not (Test-Path $dashboardPath)) { return }

  $currentHash = (Get-FileHash -Path $dashboardPath -Algorithm SHA256).Hash
  $latest = Get-LatestBackup
  $needsBackup = $true
  if ($latest) {
    try {
      $latestHash = (Get-FileHash -Path $latest.FullName -Algorithm SHA256).Hash
      if ($latestHash -eq $currentHash) {
        $needsBackup = $false
      }
    } catch {}
  }

  if ($needsBackup) {
    $ts = Get-Date -Format "yyyyMMdd_HHmmss"
    $dst = Join-Path $backupDir ("dashboard_{0}.py" -f $ts)
    Copy-Item -Path $dashboardPath -Destination $dst -Force
    Write-GuardLog "backup created: $dst"
  }

  $all = Get-ChildItem -Path $backupDir -Filter "dashboard_*.py" -File -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending
  if ($all.Count -gt 50) {
    $all | Select-Object -Skip 50 | Remove-Item -Force -ErrorAction SilentlyContinue
  }
}

function Restore-FromLatestBackup {
  param([string]$Reason)
  $latest = Get-LatestBackup
  if (-not $latest) {
    Write-GuardLog "restore failed(no backup): $Reason"
    return $false
  }

  try {
    Copy-Item -Path $latest.FullName -Destination $dashboardPath -Force
    Write-GuardLog "restore ok: $Reason <- $($latest.FullName)"
    return $true
  } catch {
    Write-GuardLog "restore exception: $Reason : $($_.Exception.Message)"
    return $false
  }
}

try {
  $repaired = $false

  if (-not (Test-Path $dashboardPath)) {
    $ok = Restore-FromLatestBackup "missing dashboard.py"
    if (-not $ok) {
      Write-GuardLog "fatal: dashboard missing and no restorable backup"
      Write-Output "[SAFE_GUARD] FAIL missing dashboard and no backup"
      exit 1
    }
    $repaired = $true
  } else {
    $size = (Get-Item $dashboardPath).Length
    if ($size -lt $MinSizeBytes) {
      $ok = Restore-FromLatestBackup ("file too small({0}B)" -f $size)
      if (-not $ok) {
        Write-GuardLog "fatal: small file and restore failed"
        Write-Output "[SAFE_GUARD] FAIL small dashboard and no restore"
        exit 1
      }
      $repaired = $true
    }
  }

  if (-not (Compile-Ok -PathToCompile $dashboardPath)) {
    $ok = Restore-FromLatestBackup "py_compile failed"
    if (-not $ok) {
      Write-GuardLog "fatal: compile failed and restore failed"
      Write-Output "[SAFE_GUARD] FAIL compile and restore"
      exit 2
    }

    if (-not (Compile-Ok -PathToCompile $dashboardPath)) {
      Write-GuardLog "fatal: compile failed after restore"
      Write-Output "[SAFE_GUARD] FAIL compile after restore"
      exit 2
    }
    $repaired = $true
  }

  Backup-CurrentIfChanged
  if ($repaired) {
    Write-Output "[SAFE_GUARD] REPAIRED"
  } else {
    Write-Output "[SAFE_GUARD] OK"
  }
  exit 0
}
catch {
  Write-GuardLog "fatal exception: $($_.Exception.Message)"
  Write-Output "[SAFE_GUARD] FAIL exception"
  exit 9
}
