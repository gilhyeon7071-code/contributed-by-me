#requires -Version 5
param(
  [string]$RunId = $env:RUN_ID,
  [string]$Seed = $env:SEED,
  [string]$OutDir = $env:OUT_DIR,
  [string]$PythonExe = "python"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$env:MKL_CBWR = "AVX2"
$env:MKL_NUM_THREADS = "1"
$env:OPENBLAS_NUM_THREADS = "1"
$env:OMP_NUM_THREADS = "1"

if ([string]::IsNullOrWhiteSpace($RunId)) {
  $RunId = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
}
if ([string]::IsNullOrWhiteSpace($Seed)) {
  $Seed = "20260301"
}
if ([string]::IsNullOrWhiteSpace($OutDir)) {
  $OutDir = Join-Path "E:\1_Data\2_Logs\proof" $RunId
}

$env:RUN_ID = $RunId
$env:SEED = $Seed
$env:OUT_DIR = $OutDir

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Helper = Join-Path $ScriptDir "evidence_bundle_dropin.py"
if (-not (Test-Path -LiteralPath $Helper)) {
  throw "helper not found: $Helper"
}

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

& $PythonExe $Helper --mode python-env --out-dir $OutDir --run-id $RunId --seed $Seed
if ($LASTEXITCODE -ne 0) { throw "python-env evidence failed: $LASTEXITCODE" }

try {
  & w32tm /query /status 2>&1 | Out-File -Encoding UTF8 (Join-Path $OutDir "time_status.txt")
} catch {
  $_.Exception.Message | Out-File -Encoding UTF8 (Join-Path $OutDir "time_status.txt")
}

try {
  Get-Item "$env:WINDIR\System32\kernel32.dll" |
    Format-List * |
    Out-String |
    Out-File -Encoding UTF8 (Join-Path $OutDir "kernel32_meta.txt")
} catch {
  $_.Exception.Message | Out-File -Encoding UTF8 (Join-Path $OutDir "kernel32_meta.txt")
}

& $PythonExe $Helper --mode events --out-dir $OutDir --run-id $RunId --seed $Seed
if ($LASTEXITCODE -ne 0) { throw "events evidence failed: $LASTEXITCODE" }

& $PythonExe $Helper --mode manifest --out-dir $OutDir --run-id $RunId --seed $Seed
if ($LASTEXITCODE -ne 0) { throw "manifest evidence failed: $LASTEXITCODE" }

$ZipPath = "$OutDir.zip"
if (Test-Path -LiteralPath $ZipPath) {
  Remove-Item -LiteralPath $ZipPath -Force
}
Compress-Archive -Force -Path $OutDir -DestinationPath $ZipPath

$Cosign = Get-Command cosign -ErrorAction SilentlyContinue
if ($Cosign -and -not [string]::IsNullOrWhiteSpace($env:COSIGN_KEY)) {
  & $Cosign.Source sign-blob --key $env:COSIGN_KEY --yes $ZipPath > "$ZipPath.sig"
  if ($LASTEXITCODE -ne 0) { throw "cosign sign-blob failed: $LASTEXITCODE" }
}

Write-Host "[OK] Evidence bundle: $OutDir"
Write-Host "[OK] Archive: $ZipPath"
