param(
    [Parameter(Mandatory = $true)]
    [string]$CsvPath
)

if (-not (Test-Path -LiteralPath $CsvPath)) {
    Write-Output ""
    exit 0
}

try {
    $rows = Import-Csv -Path $CsvPath
} catch {
    Write-Output ""
    exit 0
}

function Get-Ymd8([string]$raw) {
    $s = (($raw | ForEach-Object { $_ }) -replace '\D', '')
    if ($s.Length -ge 8) { return $s.Substring(0, 8) }
    return ""
}

$ymdAny = @()
$ymdBuy = @()
foreach ($r in $rows) {
    $ymd = Get-Ymd8 ([string]$r.datetime)
    if ($ymd.Length -ne 8) { continue }
    $ymdAny += $ymd
    $side = ([string]$r.side).Trim().ToUpperInvariant()
    if ($side -eq "BUY") {
        $ymdBuy += $ymd
    }
}

$d = ""
if ($ymdBuy.Count -gt 0) {
    $d = ($ymdBuy | Sort-Object | Select-Object -Last 1)
} elseif ($ymdAny.Count -gt 0) {
    $d = ($ymdAny | Sort-Object | Select-Object -Last 1)
}

Write-Output $d
exit 0
