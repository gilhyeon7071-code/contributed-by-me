$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$log = Join-Path $root '2_Logs'

$trval = Join-Path $log 'trading_stage_validation_latest.json'
$paperFix = Join-Path $log 'paper_fix_cycle_latest.json'
$pending = Join-Path $log 'pending_entry_status_latest.json'
$snap = Join-Path $log 'integrated_ops_snapshot_latest.json'
$runlog = Join-Path $log 'run_paper_daily_last.txt'

$outJson = Join-Path $log 'hold_diagnose_fast_latest.json'
$outMd = Join-Path $log 'hold_diagnose_fast_latest.md'

function Read-Json([string]$path) {
  if (-not (Test-Path $path)) { return $null }
  try { return (Get-Content -Raw -Encoding UTF8 $path | ConvertFrom-Json) } catch {}
  try { return (Get-Content -Raw -Encoding UTF8BOM $path | ConvertFrom-Json) } catch {}
  return $null
}

function Get-Val($obj, [string]$key) {
  if ($null -eq $obj) { return $null }
  if ($obj -is [hashtable]) { return $obj[$key] }
  $p = $obj.PSObject.Properties[$key]
  if ($null -ne $p) { return $p.Value }
  return $null
}

function Coalesce {
  param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Values)
  foreach ($v in $Values) { if ($null -ne $v -and [string]$v -ne '') { return $v } }
  return $null
}

function Required-Blockers($stage) {
  $rows = @()
  $items = @(Get-Val $stage 'items')
  foreach ($it in $items) {
    if ($null -eq $it) { continue }
    $required = Coalesce (Get-Val $it 'required') $true
    if (-not [bool]$required) { continue }
    $status = [string](Coalesce (Get-Val $it 'status') '')
    if ($status -in @('FAIL','NOT_EVALUABLE')) {
      $rows += [ordered]@{
        name = [string](Coalesce (Get-Val $it 'name') '-')
        status = $status
        issue = [string](Coalesce (Get-Val $it 'issue') '-')
        metric = [string](Coalesce (Get-Val $it 'metric') '-')
        action = [string](Coalesce (Get-Val $it 'action') '-')
      }
    }
  }
  return $rows
}

function Read-GateLines([string]$path) {
  if (-not (Test-Path $path)) { return @() }
  $lines = Get-Content -Encoding UTF8 $path -ErrorAction SilentlyContinue
  if (-not $lines) { return @() }
  $picked = @()
  foreach ($ln in $lines) {
    if ($ln -match 'OUTLIER_GATE|ENTRY_GATE|MACRO_NEWS_GUARD') { $picked += $ln.Trim() }
  }
  if ($picked.Count -le 12) { return $picked }
  return $picked[($picked.Count-12)..($picked.Count-1)]
}

$tr = Read-Json $trval
$pf = Read-Json $paperFix
$pd = Read-Json $pending
$sn = Read-Json $snap

$paper = Get-Val $tr 'paper'
$live = Get-Val $tr 'live'
$overall = Get-Val $tr 'overall'
$summary = Get-Val $sn 'summary'

$rep = [ordered]@{
  generated_at = (Get-Date).ToString('yyyy-MM-ddTHH:mm:ss')
  overall_judgment = [string](Coalesce (Get-Val $overall 'judgment') (Get-Val $pf 'overall_judgment') '-')
  next_step = [string](Coalesce (Get-Val $overall 'next_step') (Get-Val $pf 'next_step') '-')
  paper_judgment = [string](Coalesce (Get-Val $paper 'judgment') (Get-Val $pf 'paper_judgment') '-')
  live_judgment = [string](Coalesce (Get-Val $live 'judgment') (Get-Val $pf 'live_judgment') '-')
  paper_required_blockers = @(Required-Blockers $paper)
  live_required_blockers = @(Required-Blockers $live)
  pending_status_reason = [string](Coalesce (Get-Val $pd 'status_reason') '-')
  pending_max_new = (Get-Val $pd 'max_new')
  pending_queue_len = (Get-Val $pd 'pending_queue_len')
  top_blocker_display = [string](Coalesce (Get-Val $summary 'top_blocker') '-')
  top_blocker_effective = [string](Coalesce (Get-Val $summary 'top_blocker_effective') '-')
  runlog_gate_lines = @(Read-GateLines $runlog)
  sources = [ordered]@{
    trading_stage_validation = $trval
    paper_fix_cycle = $paperFix
    pending_entry_status = $pending
    integrated_ops_snapshot = $snap
    run_paper_daily_last = $runlog
  }
}

$rep | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 $outJson

$md = @()
$md += "# Hold Fast Diagnose ($($rep.generated_at))"
$md += ""
$md += "## Summary"
$md += "- overall: **$($rep.overall_judgment)**"
$md += "- next_step: $($rep.next_step)"
$md += "- paper: **$($rep.paper_judgment)**"
$md += "- live: **$($rep.live_judgment)**"
$md += "- pending_status_reason: $($rep.pending_status_reason)"
$md += "- pending_max_new: $($rep.pending_max_new)"
$md += "- top_blocker_display: $($rep.top_blocker_display)"
$md += "- top_blocker_effective: $($rep.top_blocker_effective)"
$md += ""
$md += "## Paper Required Blockers ($(@($rep.paper_required_blockers).Count))"
if (@($rep.paper_required_blockers).Count -eq 0) { $md += "- none" } else {
  foreach ($b in $rep.paper_required_blockers) { $md += "- $($b.name) [$($b.status)] $($b.issue) / $($b.metric)" }
}
$md += ""
$md += "## Live Required Blockers ($(@($rep.live_required_blockers).Count))"
if (@($rep.live_required_blockers).Count -eq 0) { $md += "- none" } else {
  foreach ($b in $rep.live_required_blockers) { $md += "- $($b.name) [$($b.status)] $($b.issue) / $($b.metric)" }
}
$md += ""
$md += "## Latest Gate Lines"
if (@($rep.runlog_gate_lines).Count -eq 0) { $md += "- none" } else {
  foreach ($ln in $rep.runlog_gate_lines) { $md += "- $ln" }
}
$md += ""

$md -join "`r`n" | Set-Content -Encoding UTF8 $outMd

Write-Output "[HOLD_FAST] wrote $outJson"
Write-Output "[HOLD_FAST] wrote $outMd"
Write-Output "[HOLD_FAST] overall=$($rep.overall_judgment) next=$($rep.next_step) paper=$($rep.paper_judgment) live=$($rep.live_judgment)"
