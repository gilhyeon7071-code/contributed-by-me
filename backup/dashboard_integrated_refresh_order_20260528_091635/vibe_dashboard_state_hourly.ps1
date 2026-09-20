param(
  [string]$RootB = "E:\vibe\buffett"
)

$ErrorActionPreference = "Stop"
$officialPy = "E:\1_Data\_runtime\python312-embed\python.exe"
$venvPy = Join-Path $RootB ".venv\Scripts\python.exe"
$bundledPy = "C:\Users\jjtop\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
$pyCandidates = @($env:VIBE_PYTHON, $officialPy, $venvPy, $bundledPy) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
$py = ""
foreach($candidate in $pyCandidates){
  if(Test-Path $candidate){
    & $candidate -c "import sys; sys.exit(0)" *> $null
    if($LASTEXITCODE -eq 0){
      $py = $candidate
      break
    }
  }
}
$builder = Join-Path $RootB "tools\build_dashboard_state_v2.py"
$ledgerSync = Join-Path $RootB "tools\sync_ledger_missing_live_fills.py"
$logDir = Join-Path $RootB "runs\_scheduler_logs"

New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$log = Join-Path $logDir ("VIBE_Dashboard_State_Hourly_{0}.log" -f $ts)

Start-Transcript -Path $log | Out-Null
try {
  Write-Host "=== VIBE Dashboard State Hourly ==="
  Write-Host "rootb=$RootB"
  Write-Host "python=$py"
  Write-Host "builder=$builder"
  Write-Host "ledger_sync=$ledgerSync"

  if([string]::IsNullOrWhiteSpace($py)) { throw "usable python not found. checked: $($pyCandidates -join ', ')" }
  if(-not (Test-Path $builder)) { throw "builder not found: $builder" }
  if(-not (Test-Path $ledgerSync)) { throw "ledger sync not found: $ledgerSync" }

  Set-Location $RootB
  & $py $ledgerSync --apply
  if($LASTEXITCODE -ne 0){
    throw "sync_ledger_missing_live_fills.py failed (exit=$LASTEXITCODE)"
  }

  & $py $builder
  if($LASTEXITCODE -ne 0){
    throw "build_dashboard_state_v2.py failed (exit=$LASTEXITCODE)"
  }

  Write-Host "[OK] dashboard_state refresh done"
  exit 0
}
catch {
  Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
  Write-Host $_.ScriptStackTrace
  exit 1
}
finally {
  try { Stop-Transcript | Out-Null } catch {}
}
