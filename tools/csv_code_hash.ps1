param(
    [Parameter(Mandatory = $true)]
    [string]$Path
)

if (-not (Test-Path -LiteralPath $Path)) {
    exit 0
}

$rows = Import-Csv -LiteralPath $Path
$codes = New-Object 'System.Collections.Generic.HashSet[string]'

foreach ($r in $rows) {
    $c = $null
    if ($r.PSObject.Properties['code']) {
        $c = $r.code
    } elseif ($r.PSObject.Properties['Code']) {
        $c = $r.Code
    } elseif ($r.PSObject.Properties['종목코드']) {
        $c = $r.'종목코드'
    }
    if ($null -ne $c -and [string]$c -ne '') {
        $null = $codes.Add([string]$c)
    }
}

if ($codes.Count -eq 0) {
    exit 0
}

$arr = New-Object string[] $codes.Count
$codes.CopyTo($arr)
[Array]::Sort($arr)
$joined = [string]::Join(',', $arr)

$sha = [System.Security.Cryptography.SHA256]::Create()
$bytes = [System.Text.Encoding]::UTF8.GetBytes($joined)
$hashBytes = $sha.ComputeHash($bytes)

$sb = New-Object System.Text.StringBuilder
foreach ($b in $hashBytes) {
    [void]$sb.AppendFormat('{0:x2}', $b)
}

Write-Output $sb.ToString().ToUpper()
