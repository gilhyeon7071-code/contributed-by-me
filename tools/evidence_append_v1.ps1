param(
  [Parameter(Mandatory=$true)]
  [string]$Payload,

  # close_check에서 현재 log_sha256을 payload에 덧붙이고 싶으면 사용
  [switch]$IncludeLogSha256,

  # 실제로 쓰지 않고, 비교 결과만 출력
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$log = "E:\1_Data\2_Logs\evidence_integrity.log"
if (-not (Test-Path $log)) {
  Write-Output "[HARD_FAIL] log not found: $log"
  exit 2
}

function Is-TimestampOnly([string]$line) {
  return ($line.TrimEnd() -match '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')
}

function Strip-Timestamp([string]$line) {
  return ($line -replace '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s+', '')
}

# normalize payload
$payloadNorm = ($Payload.Trim())
if ([string]::IsNullOrWhiteSpace($payloadNorm)) {
  Write-Output "[HARD_FAIL] empty payload (after Trim)"
  exit 2
}
if ($payloadNorm -match '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$') {
  Write-Output "[HARD_FAIL] payload is timestamp-only"
  exit 2
}

# optional: attach current log sha256
if ($IncludeLogSha256) {
  $logSha = (Get-FileHash -Algorithm SHA256 $log).Hash
  if ($payloadNorm -notmatch 'log_sha256=') {
    $payloadNorm = "$payloadNorm log_sha256=$logSha"
  }
}

# find last valid (non timestamp-only) line
$lines = Get-Content $log
$lastValid = $lines | Where-Object { -not (Is-TimestampOnly $_) } | Select-Object -Last 1

$prevPayload = ""
if ($null -ne $lastValid) {
  $prevPayload = (Strip-Timestamp $lastValid).Trim()
}

if ($prevPayload -eq $payloadNorm) {
  Write-Output "log_append_dup_payload: SKIP"
  exit 0
}

if ($DryRun) {
  Write-Output "DRYRUN: would_append"
  Write-Output "payload=$payloadNorm"
  Write-Output "prev_payload=$prevPayload"
  exit 0
}

# write with RO toggle (preserve Archive)
$attrs = (Get-Item $log).Attributes
$wasReadOnly = (($attrs -band [IO.FileAttributes]::ReadOnly) -ne 0)

if ($wasReadOnly) { attrib -R $log }
("{0} {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $payloadNorm) | Add-Content -Encoding UTF8 $log
if ($wasReadOnly) { attrib +R $log }

Write-Output "log_append_new: OK"
exit 0
