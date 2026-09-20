#requires -Version 5
param(
  [string]$RunId = $env:RUN_ID,
  [string]$Seed = $env:SEED,
  [string]$OutDir = $env:OUT_DIR,
  [string]$PythonExe = "python",
  [string]$AsOf = $env:AS_OF,
  [string[]]$ArtifactPath = @(),
  [switch]$RequireSignature
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
if ([string]::IsNullOrWhiteSpace($AsOf)) {
  $AsOf = (Get-Date).ToString("yyyyMMdd")
}

$env:RUN_ID = $RunId
$env:SEED = $Seed
$env:OUT_DIR = $OutDir
$env:AS_OF = $AsOf

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Helper = Join-Path $ScriptDir "evidence_bundle_dropin.py"
if (-not (Test-Path -LiteralPath $Helper)) {
  throw "helper not found: $Helper"
}

function ResolveEvidencePython([string]$Requested) {
  if (-not [string]::IsNullOrWhiteSpace($Requested)) {
    if ([System.IO.Path]::IsPathRooted($Requested) -and (Test-Path -LiteralPath $Requested)) {
      return $Requested
    }
    $cmd = Get-Command $Requested -ErrorAction SilentlyContinue
    if ($cmd -and $cmd.Source) {
      return $cmd.Source
    }
  }

  $candidates = @()
  $candidates += (Join-Path $env:LOCALAPPDATA "Programs\Python\Python312\python.exe")
  $candidates += (Join-Path $env:LOCALAPPDATA "Programs\Python\Python314\python.exe")
  try {
    $candidates += (& where.exe python 2>$null)
  } catch {}

  foreach ($candidate in $candidates) {
    if ([string]::IsNullOrWhiteSpace($candidate)) { continue }
    if (-not (Test-Path -LiteralPath $candidate)) { continue }
    & $candidate -V 1>$null 2>$null
    if ($LASTEXITCODE -eq 0) { return $candidate }
  }

  throw "python executable not found"
}

$PythonExe = ResolveEvidencePython $PythonExe
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

function GetGitCommit() {
  try {
    $commit = & git -C "E:\1_Data" rev-parse HEAD 2>$null
    if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($commit)) {
      return [string]$commit
    }
  } catch {}
  return "NA"
}

function ResolveArtifactPath([string]$PathText) {
  if (Test-Path -LiteralPath $PathText) {
    return $PathText
  }
  if ($PathText.Contains("*")) {
    $match = Get-ChildItem -Path $PathText -File -ErrorAction SilentlyContinue |
      Sort-Object LastWriteTime -Descending |
      Select-Object -First 1
    if ($match) {
      return $match.FullName
    }
  }
  return $PathText
}

function AddEvidenceArtifact([System.Collections.ArrayList]$Rows, [string]$Label, [string]$PathText) {
  $ResolvedPath = ResolveArtifactPath $PathText
  $exists = Test-Path -LiteralPath $ResolvedPath
  $row = [ordered]@{
    label = $Label
    path = $ResolvedPath
    requested_path = $PathText
    exists = [bool]$exists
    sha256 = "NA"
    bytes = $null
    mtime = $null
  }
  if ($exists) {
    $item = Get-Item -LiteralPath $ResolvedPath
    $hash = Get-FileHash -Algorithm SHA256 -LiteralPath $ResolvedPath
    $row.sha256 = $hash.Hash
    $row.bytes = $item.Length
    $row.mtime = $item.LastWriteTime.ToString("s")
  }
  [void]$Rows.Add([pscustomobject]$row)
}

function GetSignatureStatus() {
  $cosign = Get-Command cosign -ErrorAction SilentlyContinue
  $cosignFound = [bool]($cosign -and $cosign.Source)
  $keySet = -not [string]::IsNullOrWhiteSpace($env:COSIGN_KEY)
  $passwordSet = -not [string]::IsNullOrWhiteSpace($env:COSIGN_PASSWORD)
  $ready = $cosignFound -and $keySet
  $status = if ($ready) { "READY" } elseif ($RequireSignature) { "FAIL_CLOSED" } else { "UNSIGNED" }
  return [ordered]@{
    status = $status
    require_signature = [bool]$RequireSignature
    cosign_found = $cosignFound
    cosign_path = if ($cosignFound) { $cosign.Source } else { "" }
    cosign_key_set = $keySet
    cosign_password_set = $passwordSet
    reason = if ($ready) { "signing_path_available" } elseif (-not $cosignFound) { "cosign_not_found" } else { "cosign_key_not_set" }
  }
}

function GetDockerImageStatus() {
  $dockerfile = "E:\1_Data\Dockerfile"
  $compose = "E:\1_Data\docker-compose.yml"
  $composeSsot = "E:\1_Data\docker-compose.ssot.yml"
  $fromLines = @()
  if (Test-Path -LiteralPath $dockerfile) {
    $fromLines = @(Select-String -LiteralPath $dockerfile -Pattern '^\s*FROM\s+' |
      ForEach-Object { $_.Line.Trim() })
  }
  $basePinned = $true
  if ($fromLines.Count -eq 0) {
    $basePinned = $false
  } else {
    foreach ($line in $fromLines) {
      if ($line -notmatch '@sha256:') {
        $basePinned = $false
      }
    }
  }
  $composeUsesBuild = $false
  foreach ($path in @($compose, $composeSsot)) {
    if (-not (Test-Path -LiteralPath $path)) { continue }
    if (Select-String -LiteralPath $path -Pattern '^\s*build\s*:' -Quiet) {
      $composeUsesBuild = $true
    }
  }
  $docker = Get-Command docker -ErrorAction SilentlyContinue
  $dockerFound = [bool]($docker -and $docker.Source)
  return [ordered]@{
    status = if ($basePinned -and -not $composeUsesBuild -and $dockerFound) { "PINNED_OR_INSPECTABLE" } else { "INCOMPLETE" }
    docker_found = $dockerFound
    docker_path = if ($dockerFound) { $docker.Source } else { "" }
    dockerfile = $dockerfile
    dockerfile_from = $fromLines
    base_image_digest_pinned = [bool]$basePinned
    compose_uses_build = [bool]$composeUsesBuild
    compose_paths = @($compose, $composeSsot)
    reason = if (-not $basePinned) { "base_image_not_digest_pinned" } elseif ($composeUsesBuild) { "compose_uses_build_without_image_digest" } elseif (-not $dockerFound) { "docker_cli_not_found" } else { "ok" }
  }
}

$SignatureStatus = GetSignatureStatus
if ($RequireSignature -and $SignatureStatus.status -ne "READY") {
  throw "signature required but unavailable: $($SignatureStatus.reason)"
}

$OperationArtifactRows = [System.Collections.ArrayList]::new()
$DefaultArtifacts = [ordered]@{
  fills = "E:\1_Data\paper\fills.csv"
  orders_exec = "E:\1_Data\paper\orders_${AsOf}_exec.xlsx"
  paper_state = "E:\1_Data\paper\paper_state.json"
  paper_engine_config = "E:\1_Data\paper\paper_engine_config.json"
  paper_engine_config_lock = "E:\1_Data\paper\paper_engine_config.lock.json"
  ohlcv_paper = "E:\1_Data\paper\prices\ohlcv_paper.parquet"
  run_paper_daily_last = "E:\1_Data\2_Logs\run_paper_daily_last.txt"
  p0_daily_check = "E:\1_Data\2_Logs\p0_daily_check_*.json"
  canonical_replay_compare = "E:\1_Data\2_Logs\canonical_replay_compare_latest.json"
  ops_sanity_quick = "E:\1_Data\2_Logs\ops_sanity_quick_latest.json"
  paper_pnl_summary = "E:\1_Data\2_Logs\paper_pnl_summary_last.json"
  risk_orchestration = "E:\1_Data\2_Logs\risk_orchestration_latest.json"
  state_hash = "E:\1_Data\2_Logs\state_hash_${AsOf}.log"
}
foreach ($entry in $DefaultArtifacts.GetEnumerator()) {
  AddEvidenceArtifact $OperationArtifactRows $entry.Key $entry.Value
}
foreach ($extraPath in $ArtifactPath) {
  if ([string]::IsNullOrWhiteSpace($extraPath)) { continue }
  AddEvidenceArtifact $OperationArtifactRows "extra" $extraPath
}

$OperationArtifacts = [ordered]@{
  schema = "operation_artifacts_v1"
  root = "E:\1_Data"
  run_id = $RunId
  as_of = $AsOf
  seed = $Seed
  git_commit = GetGitCommit
  generated_at_utc = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
  signature = $SignatureStatus
  image_repro = GetDockerImageStatus
  artifacts = $OperationArtifactRows
}
$OperationArtifactsPath = Join-Path $OutDir "operation_artifacts.json"
$OperationArtifacts | ConvertTo-Json -Depth 6 | Out-File -Encoding UTF8 $OperationArtifactsPath

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

& $PythonExe $Helper --mode manifest --out-dir $OutDir --run-id $RunId --seed $Seed --operation-artifacts-json $OperationArtifactsPath
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
} elseif ($RequireSignature) {
  throw "signature required but not produced"
}

Write-Host "[OK] Evidence bundle: $OutDir"
Write-Host "[OK] Archive: $ZipPath"
