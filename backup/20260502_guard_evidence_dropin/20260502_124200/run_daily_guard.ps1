param(
  [switch]$Apply,
  [switch]$BuildDashboard,
  [switch]$RunRootAPaper,
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
  $prevEap = $ErrorActionPreference
  try {
    # PowerShell 5.x: native stderr can be promoted to terminating error when EAP=Stop.
    # Keep stderr as log lines; fail only on explicit non-zero exit code.
    $ErrorActionPreference = "Continue"

    & $fn 2>&1 | ForEach-Object {
      $line = ($_ | Out-String).TrimEnd("`r","`n")
      if ($line -eq "") { return }

      # persist raw line
      Add-Content -LiteralPath $Log -Value $line

      # highlight key patterns
      if ($line -like "[SUMMARY]*")        { Write-Host $line -ForegroundColor Magenta; return }
      if ($line -like "[FRESHNESS]*")      { Write-Host $line -ForegroundColor DarkCyan; return }
      if ($line -like "[REDTEAM_V2]*")     { Write-Host $line -ForegroundColor Cyan; return }
      if ($line -like "[REDTEAM]*")        { Write-Host $line -ForegroundColor DarkYellow; return }
      if ($line -like "[HARD_FAIL]*")      { Write-Host $line -ForegroundColor Red; return }
      if ($line -like "[OK]*")             { Write-Host $line -ForegroundColor Green; return }
      if ($line -like "[WARNING]*" -or $line -like "[WARN]*") { Write-Host $line -ForegroundColor Yellow; return }

      Write-Host $line
    }

    $rc = $LASTEXITCODE
    if ($rc -ne 0) { throw "Non-zero exit code: $rc" }
    Log "=== STEP PASS:  $name ==="
  } catch {
    Log "=== STEP HARD_FAIL: $name :: $($_.Exception.Message) ==="
    throw
  } finally {
    $ErrorActionPreference = $prevEap
  }
}

function ResolvePSHost() {
  $cmd = Get-Command pwsh -ErrorAction SilentlyContinue
  if ($cmd -and $cmd.Source) { return $cmd.Source }
  $cmd2 = Get-Command powershell.exe -ErrorAction SilentlyContinue
  if ($cmd2 -and $cmd2.Source) { return $cmd2.Source }
  return $null
}

function ResolveRootAPython() {
  $candidates = @()
  $candidates += (Join-Path $env:LOCALAPPDATA "Programs\Python\Python312\python.exe")
  $candidates += (Join-Path $env:LOCALAPPDATA "Programs\Python\Python311\python.exe")
  $cmd = Get-Command python.exe -ErrorAction SilentlyContinue
  if ($cmd -and $cmd.Source) { $candidates += $cmd.Source }

  foreach ($c in $candidates) {
    if (-not $c) { continue }
    if (-not (Test-Path $c)) { continue }
    try {
      & $c -V 1>$null 2>$null
      if ($LASTEXITCODE -eq 0) { return $c }
    } catch {}
  }
  return $null
}


# --- FINAL SUMMARY: single SSOT function (PASS/HARD_FAIL both use this) ---
function WriteFinalSummary([string]$verdict_final) {
  $lines = @()
  try { $lines = Get-Content -LiteralPath $Log -ErrorAction Stop } catch { $lines = @() }

  $LastCap = {
    param([string]$pattern, [int]$group = 1)
    $last = $null
    foreach ($l in $lines) {
      $m = [regex]::Match($l, $pattern)
      if ($m.Success) { $last = $m }
    }
    if ($null -eq $last) { return "NA" }
    return $last.Groups[$group].Value
  }

  $LastLine = {
    param([string]$pattern)
    $last = $null
    foreach ($l in $lines) {
      if ([regex]::IsMatch($l, $pattern)) { $last = $l }
    }
    if ($null -eq $last) { return "NA" }
    return $last
  }

  # (parse from current SSOT log)
  $expected    = & $LastCap '^\[SUMMARY\]\s+expected_date=(\d{8})$'

  $candMeta    = & $LastCap '^\[SUMMARY\]\s+cand_meta=(.+)$'
  $candCsv     = & $LastCap '^\[SUMMARY\]\s+cand_csv=(.+)$'
  $candMax     = & $LastCap '^\[SUMMARY\]\s+cand\s+status=\w+\s+max_date=(\d{8})\s+lag=(-?\d+)$' 1
  $candLag     = & $LastCap '^\[SUMMARY\]\s+cand\s+status=\w+\s+max_date=(\d{8})\s+lag=(-?\d+)$' 2
  $krxMax      = & $LastCap '^\[SUMMARY\]\s+krx_clean\s+status=\w+\s+max_date=(\d{8})\s+lag=(-?\d+)$' 1
  $krxLag      = & $LastCap '^\[SUMMARY\]\s+krx_clean\s+status=\w+\s+max_date=(\d{8})\s+lag=(-?\d+)$' 2
  $pricesMax   = & $LastCap '^\[SUMMARY\]\s+prices\s+status=\w+\s+max_date=(\d{8})\s+lag=(-?\d+)$' 1
  $pricesLag   = & $LastCap '^\[SUMMARY\]\s+prices\s+status=\w+\s+max_date=(\d{8})\s+lag=(-?\d+)$' 2
  $rtVerdict   = & $LastCap '^\[REDTEAM_V2\]\s+verdict=(\w+)\s+HARD_FAIL=(\d+)\s+WARN=(\d+)$' 1
  $rtHard      = & $LastCap '^\[REDTEAM_V2\]\s+verdict=(\w+)\s+HARD_FAIL=(\d+)\s+WARN=(\d+)$' 2
  $rtWarn      = & $LastCap '^\[REDTEAM_V2\]\s+verdict=(\w+)\s+HARD_FAIL=(\d+)\s+WARN=(\d+)$' 3
  $rtEvidence  = & $LastCap '^\[REDTEAM_V2\]\s+wrote:\s+(.+)$'
  $ssotHealthOverall = & $LastCap '^\[FINAL\]\s+ssot_health\s+overall=(\w+)\s+new_orders=(\w+)$' 1
  $ssotHealthOrders  = & $LastCap '^\[FINAL\]\s+ssot_health\s+overall=(\w+)\s+new_orders=(\w+)$' 2

  $DbyRule     = & $LastCap 'D_by_rule=(\d{8})'
  $execDate    = & $LastCap "exec_date_unique\s+\['(\d{8})'\]"
  $stopOk      = & $LastCap '^STOP_OK\s+(True|False)$'

  # housekeeping policy line is timestamped (from Log), so allow optional prefix
  $hkPolicy    = & $LastCap '^(?:\[\d{4}-\d{2}-\d{2}[^\]]*\]\s*)?\[HOUSEKEEPING_POLICY\]\s+(.+)$'
  $hkArchive   = & $LastCap '^Archive=(.+)$'
  $hkManifest  = & $LastCap '^Manifest=(.+)$'
  $alignLast   = & $LastLine '^ALIGN_OK.*$'

  # fixed 10 lines
  Log "[FINAL] run_id=$RunId log=$Log"
  Log "[FINAL] verdict_final=$verdict_final"
  Log "[FINAL] expected_date=$expected"
  Log "[FINAL] cand max_date=$candMax lag=$candLag csv=$candCsv meta=$candMeta"
  Log "[FINAL] krx_clean max_date=$krxMax lag=$krxLag"
  Log "[FINAL] prices max_date=$pricesMax lag=$pricesLag"
  Log "[FINAL] redteam_v2 verdict=$rtVerdict hard_fail=$rtHard warn=$rtWarn evidence=$rtEvidence"
  Log "[FINAL] ssot_health overall=$ssotHealthOverall new_orders=$ssotHealthOrders"
  Log "[FINAL] p0 D_by_rule=$DbyRule exec_date=$execDate stop_ok=$stopOk"
  Log "[FINAL] housekeeping policy=$hkPolicy archive=$hkArchive manifest=$hkManifest"
  Log "[FINAL] align_last=$alignLast"
}

# mutex
$Mutex = Join-Path $LOCKDIR "ops_mutex.lock"
if (Test-Path $Mutex) {
  Write-Host "[HARD_FAIL] Another run is in progress: $Mutex" -ForegroundColor Red
  exit 1
}
"run_ts=$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Set-Content -Encoding utf8 $Mutex

try {
  $AllStepsOk = $false
  Log "START run_id=$RunId"
  Log "log=$Log"
  Log "rootb=$ROOTB"

  $PyRootB = Join-Path $ROOTB ".venv\Scripts\python.exe"
  if (-not (Test-Path $PyRootB)) {
    throw "RootB venv python missing: $PyRootB"
  }

  $PyRootA = ResolveRootAPython
  if (-not $PyRootA -or -not (Test-Path $PyRootA)) {
    throw "RootA python not found (preferred: LocalAppData Python312/311, fallback: PATH python.exe)"
  }

  $PSHostExe = ResolvePSHost
  if (-not $PSHostExe) {
    throw "PowerShell host not found (pwsh/powershell.exe)"
  }

  $GhostNames = @("0","int","str","list[str]","pd.DataFrame","Path","nul","D_by_rule")
  $GhostHits = @()
  foreach($g in $GhostNames){
    $gp = Join-Path $ROOTB $g
    if(Test-Path -LiteralPath $gp){ $GhostHits += $gp }
  }
  if($GhostHits.Count -gt 0){
    throw ("Ghost files found under RootB: " + ($GhostHits -join ", ") + ". Run E:\vibe\tools\cleanup_ghost_files.ps1 -Apply")
  }

  Log "py_rootA=$PyRootA"
  Log "py_rootB=$PyRootB"
  Log "ps_host=$PSHostExe"

  # 0) Update data inputs (KRX + Candidates). Prices updater will be added when available.
  Step "update krx_clean (incremental)" {
    & $PyRootA "E:\1_Data\krx_update_clean_incremental.py" --probe-cap 2000 --min-uni 2000 --coverage 0.90
  }

  Step "update prices (from krx_clean)" {
    & $PyRootA "E:\1_Data\tools\prices_update_from_krx_clean.py"
  }

  Step "update candidates (v41_1)" {
    & $PyRootA "E:\1_Data\generate_candidates_v41_1.py"
  }

  # 0.5) Freshness evidence (writes freshness_source_*.json)
  Step "freshness_check_v1" {
    & $PyRootA "E:\1_Data\tools\freshness_check_v1.py"
  }

  # 0.7) Redteam v2 (includes freshness gate => fail-closed)
  Step "redteam_check_v2" {
    & $PyRootA "E:\1_Data\tools\redteam_check_v2.py"
  }

  Step "ssot_health_card" {
    & cmd.exe /c "E:\1_Data\run_ssot_health_card.bat"
  }

  # 0.8) Optional RootA canonical daily pipeline run
  if($RunRootAPaper){
    Step "rootA run_paper_daily (optional)" {
      & cmd.exe /c "E:\1_Data\run_paper_daily.bat"
    }
  } else {
    Log "[SKIP] rootA run_paper_daily (use -RunRootAPaper to enable)"
  }

  # 0.9) SSOT pointer ensure (fail-closed):
  #      - try pointer update for today
  #      - if missing today snapshot, build FINAL snapshot once and retry
  Step "ssot_today_final_ensure" {
    & $PyRootB (Join-Path $ROOTB "tools\ssot_today_final_update.py")
    if ($LASTEXITCODE -ne 0) {
      Write-Host "[WARN] ssot_today_final_update failed -> try ssot_snapshot_final_build_v1" -ForegroundColor Yellow
      & $PyRootB (Join-Path $ROOTB "tools\ssot_snapshot_final_build_v1.py")
      if ($LASTEXITCODE -ne 0) {
        throw "ssot_snapshot_final_build_v1 failed: exit=$LASTEXITCODE"
      }
      & $PyRootB (Join-Path $ROOTB "tools\ssot_today_final_update.py")
      if ($LASTEXITCODE -ne 0) {
        throw "ssot_today_final_update failed after snapshot build: exit=$LASTEXITCODE"
      }
    }
  }

  # 1) Apply pipeline
  Step ("p0_paper_daily (" + $(if($Apply){"-Apply"}else{"DRYRUN"}) + ")") {
    $taskArgs = @("-NoProfile","-ExecutionPolicy","Bypass","-File",(Join-Path $ROOTB "tools\dev\task_p0_paper_daily.ps1"),"-Mode","paper")
    if($Apply){ $taskArgs += "-Apply" }
    & $PSHostExe @taskArgs
  }

  # 2) Dashboard build (optional)
  if ($BuildDashboard) {
    Step "dashboard build (v18)" {
      & $PyRootB (Join-Path $ROOTB "vibe_v18.py")
    }
  } else {
    Log "[SKIP] dashboard build (use -BuildDashboard to enable)"
  }

  # 3) housekeeping (P1): policy SSOT (MOVE default on -Apply, DELETE hard-disabled)
  # - Run default: enabled unless -SkipHousekeeping
  # - MOVE: forced by -DoMove or -HousekeepingMove; otherwise enabled when -Apply
  # - DELETE: disabled; if requested -> hard-fail (fail-closed)
  $ArchiveRoot = "E:\1_Data\2_Logs\vibe_archive"

  if($DoDelete -or $HousekeepingDelete){
    throw "HOUSEKEEPING_DELETE_DISABLED: delete is OFF by policy (fail-closed)."
  }

  $hkRun  = $true
  if($PSBoundParameters.ContainsKey("DoHousekeeping")){ $hkRun = [bool]$DoHousekeeping }
  if($SkipHousekeeping){ $hkRun = $false }

  $hkMove = $false
  if($DoMove -or $HousekeepingMove){ $hkMove = $true }
  elseif($Apply){ $hkMove = $true }  # policy: MOVE on Apply

  if(-not $hkRun){
    Log "[SKIP] housekeeping (SkipHousekeeping)"
  } else {
    $mode = $(if($hkMove){"MOVE"}else{"DRYRUN"})
    Log ("[HOUSEKEEPING_POLICY] mode=" + $mode + " DoMove=" + $hkMove + " DoDelete=False ArchiveRoot=" + $ArchiveRoot)

    Step ("housekeeping (" + $mode + ")") {
      $args = @("-ArchiveRoot", $ArchiveRoot)
      if($hkMove){ $args += "-DoMove" }

      & $PSHostExe -NoProfile -ExecutionPolicy Bypass -File "E:\vibe\tools\move_to_vibe_archive.ps1" @args
      & $PSHostExe -NoProfile -ExecutionPolicy Bypass -File "E:\vibe\tools\archive_retention.ps1" -ArchiveRoot $ArchiveRoot
    }
  }

  # FINAL (PASS)
  $AllStepsOk = $true
  if($AllStepsOk){ WriteFinalSummary "PASS" }
}
catch {
  # FINAL (HARD_FAIL) - fail-safe
  try { WriteFinalSummary "HARD_FAIL" } catch { }
  Write-Host "[HARD_FAIL] run_id=$RunId (see log) $Log" -ForegroundColor Red
  exit 1
}
finally {
  Remove-Item -Force $Mutex -ErrorAction SilentlyContinue
}


