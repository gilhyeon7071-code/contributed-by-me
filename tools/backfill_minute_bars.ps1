param(
    [string]$LogDir = "E:\1_Data\2_Logs",
    [string]$ToolsDir = "E:\1_Data\tools"
)

$ErrorActionPreference = "Stop"

Write-Host "[BACKFILL] Scanning for tick logs in $LogDir"

# Find all kis_ws_ticks*_*.jsonl files and extract YYYYMMDD
# Filename pattern: kis_ws_ticks_{worker}_{YYYYMMDD}.jsonl or kis_ws_ticks_hoga1_{YYYYMMDD}.jsonl
$files = Get-ChildItem -Path $LogDir -Filter "kis_ws_ticks*.jsonl"
$dates = @()

foreach ($f in $files) {
    if ($f.Name -match "kis_ws_ticks.*_(\d{8})\.jsonl") {
        $dates += $matches[1]
    }
}

$dates = $dates | Sort-Object -Unique

if ($dates.Count -eq 0) {
    Write-Host "[BACKFILL] No tick files found."
    exit 0
}

Write-Host "[BACKFILL] Found $($dates.Count) distinct dates."

foreach ($ymd in $dates) {
    Write-Host "`n[BACKFILL] Processing date: $ymd"
    $cmd = "python `"$ToolsDir\build_intraday_minute_bars.py`" --date $ymd --log-dir `"$LogDir`" --out-dir `"$LogDir`""
    Invoke-Expression $cmd
}

Write-Host "`n[BACKFILL] Complete."
