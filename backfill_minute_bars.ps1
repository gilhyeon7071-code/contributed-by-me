[CmdletBinding()]
param(
    [string]$LogDir = "E:\1_Data\2_Logs",
    [string]$OutDir = "E:\1_Data\2_Logs",
    [string]$Python = "E:\1_Data\_runtime\python312-embed\python.exe",
    [switch]$Force
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $Python)) {
    $Python = "python"
}

$scriptPath = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) "tools\build_intraday_minute_bars.py"
if (-not (Test-Path $scriptPath)) {
    Write-Error "[BACKFILL] parser script not found: $scriptPath"
    exit 2
}

# Collect distinct YYYYMMDD dates from every kis_ws_ticks*_{date}.jsonl file
# (both plain/worker-numbered trade files and hoga*-prefixed quote files).
$dates = @{}
Get-ChildItem -Path $LogDir -Filter "kis_ws_ticks*.jsonl" -File | ForEach-Object {
    if ($_.Name -match "_(\d{8})\.jsonl$") {
        $dates[$Matches[1]] = $true
    }
}

$sortedDates = $dates.Keys | Sort-Object

if ($sortedDates.Count -eq 0) {
    Write-Output "[BACKFILL] no kis_ws_ticks*_{date}.jsonl files found under $LogDir"
    exit 0
}

Write-Output "[BACKFILL] found $($sortedDates.Count) distinct dates: $($sortedDates -join ', ')"

$results = @()
foreach ($ymd in $sortedDates) {
    $marker = Join-Path $OutDir "intraday_minute_bars_$ymd.parquet"
    if ((Test-Path $marker) -and (-not $Force)) {
        Write-Output "[BACKFILL] $ymd already done (found $marker), skipping (use -Force to redo)"
        $results += [pscustomobject]@{ date = $ymd; status = "skipped_existing" }
        continue
    }

    Write-Output "[BACKFILL] processing $ymd ..."
    $args = @($scriptPath, "--date", $ymd, "--log-dir", $LogDir, "--out-dir", $OutDir)
    & $Python @args
    $rc = $LASTEXITCODE
    if ($rc -ne 0) {
        Write-Warning "[BACKFILL] $ymd FAILED rc=$rc"
        $results += [pscustomobject]@{ date = $ymd; status = "failed_rc_$rc" }
    } else {
        $results += [pscustomobject]@{ date = $ymd; status = "ok" }
    }
}

Write-Output "[BACKFILL] summary:"
$results | Format-Table -AutoSize | Out-String | Write-Output

$failCount = ($results | Where-Object { $_.status -like "failed_*" }).Count
if ($failCount -gt 0) {
    Write-Warning "[BACKFILL] $failCount date(s) failed"
    exit 1
}
exit 0
