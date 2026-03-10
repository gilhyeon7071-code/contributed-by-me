param(
  [switch]$Apply,
  [switch]$BuildDashboard,
    [switch]$DoHousekeeping,
    [switch]$DoMove,
    [switch]$DoDelete,
    [switch]$HousekeepingMove,
    [switch]$HousekeepingDelete,
    [switch]$SkipHousekeeping
)
$ErrorActionPreference = "Stop"

$ROOTB   = "E:\vibe\buffett"
$LOCKDIR = Join-Path $ROOTB "runs\locks"
$RUNLOGS = Join-Path $ROOTB "runs\ops_runs"
New-Item -ItemType Directory -Force -Path $LOCKDIR | Out-Null
New-Item -ItemType Directory -Force -Path $RUNLOGS | Out-Null

$RunId = (Get-Date).ToString("yyyyMMdd_HHmmss")
$Log = Join-Path $RUNLOGS ("run_daily_guard_{0}.log" -f $RunId)

function Log($s) {
  $line = ("[{0}] {1}" -f (Get-Date).ToString("yyyy-MM-dd HH:mm:ss"), $s)
  # always persist
  Add-Content -LiteralPath $Log -Value $line

  # color routing (console only)
  if ($s -match "=== STEP START:")      { Write-Host $line -ForegroundColor Cyan; return }
  if ($s -match "=== STEP PASS:")       { Write-Host $line -ForegroundColor Green; return }
  if ($s -match "=== STEP HARD_FAIL:")  { Write-Host $line -ForegroundColor Red; return }
  if ($s -match "^\[SKIP\]")            { Write-Host $line -ForegroundColor DarkGray; return }
  if ($s -match "^START run_id=")       { Write-Host $line -ForegroundColor White; return }
  if ($s -match "^DONE run_id=")        { Write-Host $line -ForegroundColor Green; return }

  Write-Host $line
}


function Step($name, [scriptblock]$fn) {
  Log "=== STEP START: $name ==="
  try {
    $out = & $fn 2>&1
    $rc = $LASTEXITCODE

    foreach ($o in $out) {
      $line = ($o | Out-String).TrimEnd("`r","`n")
      if ($line -eq "") { continue }

      # persist raw line
      Add-Content -LiteralPath $Log -Value $line

      # highlight key patterns
      if ($line -like "[SUMMARY]*")        { Write-Host $line -ForegroundColor Magenta; continue }
      if ($line -like "[FRESHNESS]*")      { Write-Host $line -ForegroundColor DarkCyan; continue }
      if ($line -like "[REDTEAM_V2]*")     { Write-Host $line -ForegroundColor Cyan; continue }
      if ($line -like "[REDTEAM]*")        { Write-Host $line -ForegroundColor DarkYellow; continue }
      if ($line -like "[HARD_FAIL]*")      { Write-Host $line -ForegroundColor Red; continue }
      if ($line -like "[OK]*")             { Write-Host $line -ForegroundColor Green; continue }
      if ($line -like "[WARNING]*" -or $line -like "[WARN]*") { Write-Host $line -ForegroundColor Yellow; continue }

      Write-Host $line
    }

    if ($rc -ne 0) { throw "Non-zero exit code: $rc" }
    Log "=== STEP PASS:  $name ==="
  } catch {
    Log "=== STEP HARD_FAIL: $name :: $($_.Exception.Message) ==="
    throw
  }
}


# mutex
$Mutex = Join-Path $LOCKDIR "ops_mutex.lock"
if (Test-Path $Mutex) {
  Write-Host "[HARD_FAIL] Another run is in progress: $Mutex" -ForegroundColor Red
  exit 1
}
"run_ts=$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Set-Content -Encoding utf8 $Mutex

try {
  Log "START run_id=$RunId"
  Log "log=$Log"
  Log "rootb=$ROOTB"

  $Py = Join-Path $ROOTB ".venv\Scripts\python.exe"

  # 0) Update data inputs (KRX + Candidates). Prices updater will be added when available.
  Step "update krx_clean (incremental)" {
    & $Py "E:\1_Data\krx_update_clean_incremental.py"
  }

  Step "update candidates (v41_1)" {
    & $Py "E:\1_Data\generate_candidates_v41_1.py"
  }

  # 0.5) Freshness evidence (writes freshness_source_*.json)
  Step "freshness_check_v1" {
    & $Py "E:\1_Data\tools\freshness_check_v1.py"
  }

  # 0.7) Redteam v2 (includes freshness gate => fail-closed)
  Step "redteam_check_v2" {
    & $Py "E:\1_Data\tools\redteam_check_v2.py"
  }

  # 1) Apply pipeline
  Step "p0_paper_daily (-Apply)" {
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $ROOTB "tools\dev\task_p0_paper_daily.ps1") -Apply
  }

  # 2) Dashboard build (optional)
  if ($BuildDashboard) {
    Step "dashboard build (v18)" {
      & $Py (Join-Path $ROOTB "vibe_v18.py")
    }
  } else {
    Log "[SKIP] dashboard build (use -BuildDashboard to enable)"
  }
# 3) housekeeping policy (P1)
# - Default on -Apply: MOVE (archive) ON, DELETE OFF
# - DELETE only when explicitly requested via -HousekeepingDelete
# - DRYRUN when not -Apply unless requested via -DoHousekeeping / -HousekeepingMove
# - Back-compat: -DoHousekeeping implies MOVE
$HK_DO_MOVE   = (-not $SkipHousekeeping) -and ( $Apply -or $DoHousekeeping -or $HousekeepingMove )
$HK_DO_DELETE = (-not $SkipHousekeeping) -and ( $HousekeepingDelete )

$HK_MODE = if($HK_DO_DELETE){ "DELETE" } elseif($HK_DO_MOVE){ "MOVE" } else { "DRYRUN" }

Step ("housekeeping (" + $HK_MODE + ")") {
  # move_to_vibe_archive.ps1 supports -DoMove/-DoDelete (per prior output)
  & $Pwsh (Join-Path $RootB "tools\dev\move_to_vibe_archive.ps1") -DoMove:$HK_DO_MOVE -DoDelete:$HK_DO_DELETE
  # archive retention (keep last N)
  & $Pwsh (Join-Path $RootB "tools\dev\archive_retention.ps1") -DoDelete:$HK_DO_DELETE
}

  Log "DONE run_id=$RunId"
  Write-Host "DONE. run_id=$RunId`nlog=$Log" -ForegroundColor Green
}
catch {
  Write-Host "[HARD_FAIL] run_id=$RunId (see log) $Log" -ForegroundColor Red
  exit 1
}
finally {
  Remove-Item -Force $Mutex -ErrorAction SilentlyContinue
}
