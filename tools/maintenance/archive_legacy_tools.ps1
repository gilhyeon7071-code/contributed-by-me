param(
  [switch]$Apply,
  [string]$ArchiveRoot = "E:\1_Data\2_Logs\tools_archive"
)

$ErrorActionPreference = "Stop"

$ToolsRoot = "E:\1_Data\tools"
if (-not (Test-Path -LiteralPath $ToolsRoot)) {
  Write-Host "[HARD_FAIL] tools root not found: $ToolsRoot" -ForegroundColor Red
  exit 2
}

$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$destBase = Join-Path $ArchiveRoot ("cleanup_{0}" -f $ts)
$manifest = Join-Path $destBase ("manifest_{0}.csv" -f $ts)

$all = Get-ChildItem -LiteralPath $ToolsRoot -Recurse -File
$targets = @()

foreach ($f in $all) {
  $nm = $f.Name
  $isLegacyPatch = $nm -match '^(apply_fix|patch_|_patch_)'
  $isBakLike = $nm -match '\.bak$|\.bak_'
  $isZero = ($f.Length -eq 0)

  if ($isLegacyPatch -or $isBakLike -or $isZero) {
    $reason = @()
    if ($isLegacyPatch) { $reason += "legacy_patch" }
    if ($isBakLike) { $reason += "backup_like" }
    if ($isZero) { $reason += "zero_byte" }

    $rel = $f.FullName.Substring($ToolsRoot.Length).TrimStart('\\')
    $dest = Join-Path $destBase $rel

    $targets += [PSCustomObject]@{
      source = $f.FullName
      dest = $dest
      size = [int64]$f.Length
      reason = ($reason -join ",")
    }
  }
}

if ($targets.Count -eq 0) {
  Write-Host "[OK] nothing to archive" -ForegroundColor Green
  exit 0
}

New-Item -ItemType Directory -Force -Path $destBase | Out-Null
$targets | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $manifest

$legacyCnt = ($targets | Where-Object { $_.reason -match 'legacy_patch' }).Count
$bakCnt = ($targets | Where-Object { $_.reason -match 'backup_like' }).Count
$zeroCnt = ($targets | Where-Object { $_.reason -match 'zero_byte' }).Count
$totalSize = ($targets | Measure-Object -Property size -Sum).Sum
if ($null -eq $totalSize) { $totalSize = 0 }

Write-Host "[PLAN] targets=$($targets.Count) legacy_patch=$legacyCnt backup_like=$bakCnt zero_byte=$zeroCnt bytes=$totalSize"
Write-Host "[PLAN] archive=$destBase"
Write-Host "[PLAN] manifest=$manifest"

if (-not $Apply) {
  $targets | Select-Object -First 20 source,reason,size | Format-Table -AutoSize
  Write-Host "[DRYRUN] pass -Apply to move files" -ForegroundColor Yellow
  exit 0
}

$moved = 0
foreach ($t in $targets) {
  $parent = Split-Path -Parent $t.dest
  New-Item -ItemType Directory -Force -Path $parent | Out-Null
  Move-Item -LiteralPath $t.source -Destination $t.dest -Force
  $moved++
}

Write-Host "[OK] moved=$moved archive=$destBase" -ForegroundColor Green
exit 0
