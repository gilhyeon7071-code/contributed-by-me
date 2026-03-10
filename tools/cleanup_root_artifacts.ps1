param(
    [string]$Root = (Split-Path -Parent $PSScriptRoot),
    [string]$ArchiveRoot = "",
    [switch]$Apply
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $Root)) {
    throw "Root not found: $Root"
}

if ([string]::IsNullOrWhiteSpace($ArchiveRoot)) {
    $ArchiveRoot = Join-Path $Root "2_Logs\root_artifacts_archive"
}

$explicitBadNames = @(
    "'",
    "''",
    "0",
    "0)",
    "0).mean()",
    "0).mean())",
    "0]",
    "0].sum()",
    "8]",
    "127",
    "Dict",
    "bool",
    "int",
    "type",
    "python",
    "paper_engine",
    "Params",
    "entry_date",
    "EUC-KR",
    "List[Path]",
    "Optional[pd.DataFrame]",
    "pd.DataFrame",
    "pd.Series",
    "params.rs_lim",
    "params.v_accel_lim",
    "params.value_min",
    "_safe_int(max_hold_days",
    "d].head(params.hold).copy()",
    "float(params.get(rs_lim",
    "float(params.get(v_accel_lim",
    "float(params.get(value_min",
    "tuple[str",
    "mx)",
    "best)",
    "cd",
    "Gate",
    "REDUCE",
    "risk_off",
    "%BAKCFG%",
    "collector",
    "nul",
    "pq.ParquetFile(str(p)).metadata",
    "CUsersjjtopAppDataLocalTempexcel_content.txt"
)

$explicitBadDirs = @(
    "or",
    "mx",
    "None",
    "(report['meta'].get('latest_date')",
    "'')",
    "'').replace('-'",
    "새 폴더",
    "pq.ParquetFile(str(p)).metadata"
)

$whitelistNames = @(
    ".git",
    ".claude",
    "_bak",
    "_cache",
    "_dev",
    "_diag",
    "_krx_manual",
    "_krx_seed_full",
    "2_Logs",
    "data",
    "docs",
    "docker_export",
    "Macro",
    "news_trading",
    "paper",
    "Raw",
    "tools",
    "utils",
    "run_paper_daily.bat",
    "requirements.txt",
    ".gitignore",
    ".dockerignore"
)

function Is-SuspiciousZeroByteFile {
    param([System.IO.FileInfo]$Item)

    if ($Item.Length -ne 0) { return $false }

    $name = $Item.Name
    $ext = [System.IO.Path]::GetExtension($name)
    if ([string]::IsNullOrWhiteSpace($ext)) {
        return $true
    }

    if ($name -match "[\[\]\(\)']" -and $ext -eq "") {
        return $true
    }

    return $false
}

$items = Get-ChildItem -LiteralPath $Root -Force
$candidates = New-Object System.Collections.Generic.List[object]

foreach ($it in $items) {
    if ($whitelistNames -contains $it.Name) {
        continue
    }

    $reason = $null

    if ($it.PSIsContainer) {
        if ($explicitBadDirs -contains $it.Name) {
            $reason = "explicit_bad_dir"
        }
    } else {
        if ($explicitBadNames -contains $it.Name) {
            $reason = "explicit_bad_file"
        } elseif (Is-SuspiciousZeroByteFile -Item $it) {
            $reason = "zero_byte_suspicious"
        }
    }

    if ($reason) {
        $candidates.Add([PSCustomObject]@{
            Name = $it.Name
            Type = if ($it.PSIsContainer) { "Directory" } else { "File" }
            Size = if ($it.PSIsContainer) { $null } else { $it.Length }
            Reason = $reason
            FullPath = $it.FullName
        })
    }
}

$candidates = $candidates | Sort-Object FullPath -Unique
$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$runDir = Join-Path $ArchiveRoot ("cleanup_" + $ts)

$summary = [ordered]@{
    generated_at = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    root = $Root
    archive_root = $ArchiveRoot
    apply = [bool]$Apply
    candidate_count = [int]@($candidates).Count
    moved_count = 0
    failed_count = 0
}

Write-Host "[ROOT_CLEANUP] root=$Root"
Write-Host "[ROOT_CLEANUP] apply=$Apply candidates=$($summary.candidate_count)"

if (@($candidates).Count -eq 0) {
    Write-Host "[ROOT_CLEANUP] nothing to do"
    return
}

$candidates | Select-Object Name,Type,Size,Reason | Format-Table -AutoSize

if (-not $Apply) {
    $planDir = Join-Path $ArchiveRoot "dryrun"
    New-Item -ItemType Directory -Force -Path $planDir | Out-Null
    $planPath = Join-Path $planDir ("cleanup_root_artifacts_plan_" + $ts + ".json")
    $obj = [PSCustomObject]@{
        summary = $summary
        candidates = $candidates
    }
    $obj | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $planPath -Encoding utf8
    Write-Host "[ROOT_CLEANUP] DRYRUN plan=$planPath"
    return
}

New-Item -ItemType Directory -Force -Path $runDir | Out-Null
$moved = New-Object System.Collections.Generic.List[object]
$failed = New-Object System.Collections.Generic.List[object]

foreach ($c in $candidates) {
    try {
        $safe = ($c.Name -replace "[^A-Za-z0-9._-]", "_")
        if ([string]::IsNullOrWhiteSpace($safe)) { $safe = "artifact" }
        $prefix = if ($c.Type -eq "Directory") { "dir" } else { "file" }
        $dest = Join-Path $runDir ("{0}_{1}" -f $prefix, $safe)
        $i = 1
        while (Test-Path -LiteralPath $dest) {
            $dest = Join-Path $runDir ("{0}_{1}_{2}" -f $prefix, $safe, $i)
            $i += 1
        }

        Move-Item -LiteralPath $c.FullPath -Destination $dest -Force
        $moved.Add([PSCustomObject]@{ src = $c.FullPath; dst = $dest; reason = $c.Reason })
    } catch {
        $failed.Add([PSCustomObject]@{ src = $c.FullPath; error = $_.Exception.Message })
    }
}

$summary.moved_count = @($moved).Count
$summary.failed_count = @($failed).Count

$manifest = [PSCustomObject]@{
    summary = $summary
    moved = $moved
    failed = $failed
}
$manifestPath = Join-Path $runDir "manifest.json"
$manifest | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $manifestPath -Encoding utf8

Write-Host "[ROOT_CLEANUP] moved=$($summary.moved_count) failed=$($summary.failed_count)"
Write-Host "[ROOT_CLEANUP] archive=$runDir"
Write-Host "[ROOT_CLEANUP] manifest=$manifestPath"



