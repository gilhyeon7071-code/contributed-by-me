param(
    [Parameter(Mandatory=$false)]
    [string]$Date,

    [Parameter(Mandatory=$false)]
    [switch]$Apply,

    [Parameter(Mandatory=$false)]
    [string]$Root = "E:\1_Data",

    # vibe_broker_daily.ps1 경로를 명시하고 싶으면 사용(자동탐색보다 우선)
    [Parameter(Mandatory=$false)]
    [string]$ScriptPath = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Set-Location -LiteralPath $Root

# Date 결정: 인자 > env:D
if (-not $Date -or $Date.Trim() -eq "") { $Date = $env:D }
$Date = ($Date ?? "").Trim()

if ($Date -notmatch '^\d{8}$') {
    throw "[FATAL] Date must be YYYYMMDD. got=[$Date]. Usage: .\tools\run_broker_daily_B.ps1 -Date 20260107 [-Apply]"
}

# === 실전(B) 강제 스위치 ===
$env:VIBE_EXEC_MODE = "B"
$env:D = $Date

# 대상 스크립트 찾기
$target = $null

if ($ScriptPath -and $ScriptPath.Trim() -ne "") {
    if (-not (Test-Path -LiteralPath $ScriptPath)) {
        throw "[FATAL] ScriptPath not found: $ScriptPath"
    }
    $target = (Resolve-Path -LiteralPath $ScriptPath).Path
} else {
    $cands = @()

    $p0 = Join-Path $Root "vibe_broker_daily.ps1"
    if (Test-Path -LiteralPath $p0) { $cands += $p0 }

    $more = Get-ChildItem -LiteralPath $Root -Recurse -File -Filter "vibe_broker_daily.ps1" -ErrorAction SilentlyContinue |
        Select-Object -ExpandProperty FullName

    if ($more) { $cands += $more }

    $cands = $cands | Sort-Object -Unique

    if (-not $cands -or $cands.Count -eq 0) {
        throw "[FATAL] vibe_broker_daily.ps1 not found under Root=$Root. Specify -ScriptPath explicitly."
    }
    if ($cands.Count -gt 1) {
        $msg = "[FATAL] Multiple vibe_broker_daily.ps1 found. Specify -ScriptPath. Candidates:`n" + ($cands -join "`n")
        throw $msg
    }
    $target = $cands[0]
}

# ------------------------------------------------------------
# [FIX-1] BROKER 입력 CSV 선검증(누락이면 wrapper가 즉시 FAIL/exit 1)
# - 사고 패턴: 대상 스크립트가 ERROR 메시지만 찍고 exit 0처럼 보이는 케이스 방지
# - 기대 파일명/경로(관측): <broker_root>\data\inbox\broker_fills_from_norm_YYYYMMDD.csv
# ------------------------------------------------------------
$brokerRoot = Split-Path -Parent (Split-Path -Parent $target)   # ...\buffett
$inboxDir  = Join-Path $brokerRoot "data\inbox"
$expected  = Join-Path $inboxDir ("broker_fills_from_norm_{0}.csv" -f $Date)

if (-not (Test-Path -LiteralPath $inboxDir)) {
    throw "[FATAL] BROKER inbox dir missing: $inboxDir"
}
if (-not (Test-Path -LiteralPath $expected)) {
    $alts = @(Get-ChildItem -LiteralPath $inboxDir -File -Filter ("broker_fills_from_norm_{0}*.csv" -f $Date) -ErrorAction SilentlyContinue |
        Select-Object -ExpandProperty FullName)
    if ($alts.Count -gt 0) {
        $msg = "[FATAL] BROKER fills missing: $expected`n[HINT] Found other candidates:`n" + ($alts -join "`n")
        throw $msg
    }
    throw "[FATAL] BROKER fills missing: $expected"
}

# 로그
$logDir = Join-Path $Root "2_Logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$logPath = Join-Path $logDir ("broker_wrapper_{0}_{1}.log" -f $Date, $ts)

Write-Host ("[BROKER_WRAPPER] Root={0} Date={1} Apply={2} VIBE_EXEC_MODE={3}" -f $Root, $Date, [bool]$Apply, $env:VIBE_EXEC_MODE)
Write-Host ("[BROKER_WRAPPER] Target={0}" -f $target)
Write-Host ("[BROKER_WRAPPER] PrecheckOK={0}" -f $expected)
Write-Host ("[BROKER_WRAPPER] Log={0}" -f $logPath)

Start-Transcript -Path $logPath | Out-Null
$ok = $true
try {
    if ($Apply) {
        & $target -Date $Date -Apply
    } else {
        & $target -Date $Date
    }
} catch {
    $ok = $false
    Write-Error $_
} finally {
    try { Stop-Transcript | Out-Null } catch {}
}

if (-not $ok) {
    Write-Host "[BROKER_WRAPPER] EXIT_CODE=1"
    exit 1
}

Write-Host "[BROKER_WRAPPER] EXIT_CODE=0"
exit 0
