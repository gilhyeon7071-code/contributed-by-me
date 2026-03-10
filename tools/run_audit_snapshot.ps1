param()

$rid = Get-Date -Format "yyyyMMdd_HHmmss"
$od  = "E:\1_Data\2_Logs\audit_$rid"
New-Item -ItemType Directory -Force $od | Out-Null
New-Item -ItemType Directory -Force (Join-Path $od "evidence") | Out-Null

$sum = Join-Path $od "audit_summary.txt"
"RID=$rid`nOUTDIR=$od`n" | Out-File $sum -Encoding UTF8

function Add-Line([string]$s) { $s | Out-File $sum -Append -Encoding UTF8 }

Add-Line "=== [A] ROOT ZERO-BYTE (Ghost candidate: root 0-byte only) ==="
$gA = Get-ChildItem E:\1_Data -File -Force | Where-Object { $_.Length -eq 0 } | Select-Object FullName,CreationTime
$gB = Get-ChildItem E:\vibe\buffett -File -Force | Where-Object { $_.Length -eq 0 } | Select-Object FullName,CreationTime
Add-Line ("RootA_zero_bytes=" + ($gA | Measure-Object).Count)
Add-Line ("RootB_zero_bytes=" + ($gB | Measure-Object).Count)
$gA | Export-Csv (Join-Path $od "evidence\ghost_rootA_zero_bytes.csv") -NoTypeInformation -Encoding UTF8
$gB | Export-Csv (Join-Path $od "evidence\ghost_rootB_zero_bytes.csv") -NoTypeInformation -Encoding UTF8

Add-Line "`n=== [B] dist empty check (no delete) ==="
if (Test-Path E:\vibe\buffett\dist) {
  $m = Get-ChildItem E:\vibe\buffett\dist -Recurse -Force -File -ErrorAction SilentlyContinue | Measure-Object Length -Sum
  Add-Line ("dist exists: True")
  Add-Line ("dist files=" + $m.Count + " bytes=" + $m.Sum)
} else {
  Add-Line ("dist exists: False")
}

Add-Line "`n=== [C] _tmp_*.log list (no delete) ==="
$tmp = Get-ChildItem E:\vibe\buffett -Force -File -Filter "_tmp_*.log" -ErrorAction SilentlyContinue |
  Select-Object FullName,Length,CreationTime
Add-Line ("tmp_logs_count=" + ($tmp | Measure-Object).Count)
$tmp | Export-Csv (Join-Path $od "evidence\tmp_logs.csv") -NoTypeInformation -Encoding UTF8

Add-Line "`n=== [D] cleanup_improved + after_close_summary.cmd wiring ==="
Add-Line ("cleanup_improved_exists=" + (Test-Path E:\1_Data\cleanup_improved.py))
$cmd = Get-ChildItem E:\1_Data -Recurse -Force -File -Filter "after_close_summary.cmd" -ErrorAction SilentlyContinue | Select-Object -First 1
if ($cmd) {
  Add-Line ("after_close_summary_cmd=" + $cmd.FullName)
  Select-String -Path $cmd.FullName -Pattern "cleanup_improved\.py|log_cleanup_30d\.py" -CaseSensitive:$false |
    ForEach-Object { Add-Line ("cmd_match: " + $_.LineNumber + ": " + $_.Line) }
} else {
  Add-Line ("after_close_summary_cmd=NOT_FOUND")
}

Add-Line "`n=== [E] config.yaml paths (as-is) ==="
if (Test-Path E:\vibe\buffett\config.yaml) {
  Add-Line "config_yaml=E:\vibe\buffett\config.yaml"
  Select-String -Path E:\vibe\buffett\config.yaml -Pattern "stats_dir|orders_dir|ledger_dir|data_dir|ssot" -CaseSensitive:$false |
    ForEach-Object { Add-Line ("cfg_match: " + $_.LineNumber + ": " + $_.Line) }
} else {
  Add-Line "config_yaml=NOT_FOUND"
}

Add-Line "`n=== [F] live_fills candidates + hashes ==="
$lf1 = "E:\vibe\buffett\data\live_fills.csv"
$lf2 = "E:\vibe\buffett\data\live\live_fills.csv"
foreach ($p in @($lf1,$lf2)) {
  if (Test-Path $p) {
    $h = Get-FileHash $p -Algorithm SHA256
    $l = (Get-Content -LiteralPath $p | Measure-Object -Line).Lines
    Add-Line ("live_fills_file=" + $p)
    Add-Line ("  sha256=" + $h.Hash)
    Add-Line ("  lines=" + $l)
  } else {
    Add-Line ("live_fills_file=" + $p + " NOT_FOUND")
  }
}

Add-Line "`n=== [G] latest run_daily_guard log extract (by Name sort) ==="
$logdir = "E:\vibe\buffett\runs\ops_runs"
$logs = Get-ChildItem $logdir -Force -File -Filter "run_daily_guard_*.log" -ErrorAction SilentlyContinue | Sort-Object Name
if ($logs) {
  $last = $logs | Select-Object -Last 1
  Add-Line ("run_daily_guard_log=" + $last.FullName)
  Select-String -Path $last.FullName -Pattern "verdict_final=|expected_date=|orders_exec:|D_RULE_TRACE|SYNC_LVB WROTE=|SYNC_LVB BACKUP=" -CaseSensitive:$false |
    ForEach-Object { Add-Line ("log_hit: " + $_.LineNumber + ": " + $_.Line) } |
    Out-Null
  Select-String -Path $last.FullName -Pattern "verdict_final=|expected_date=|orders_exec:|D_RULE_TRACE|SYNC_LVB WROTE=|SYNC_LVB BACKUP=" -CaseSensitive:$false |
    Select-Object LineNumber,Line |
    Export-Csv (Join-Path $od "evidence\run_daily_guard_extract.csv") -NoTypeInformation -Encoding UTF8
} else {
  Add-Line "run_daily_guard_log=NOT_FOUND"
}

Add-Line "`n=== [H] live_vs_bt.json head (as-is) ==="
$lvb = "E:\vibe\buffett\data\stats\live_vs_bt.json"
if (Test-Path $lvb) {
  Add-Line ("live_vs_bt_json=" + $lvb)
  Get-Content $lvb -TotalCount 160 | Out-File (Join-Path $od "evidence\live_vs_bt_head.txt") -Encoding UTF8
} else {
  Add-Line "live_vs_bt_json=NOT_FOUND"
}

Add-Line "`nDONE"
$od
