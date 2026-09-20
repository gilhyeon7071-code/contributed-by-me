param(
  [ValidateSet("check", "telegram", "kakao")]
  [string]$Channel = "check",
  [switch]$Apply,
  [switch]$Test
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RootA = "E:\1_Data"
$PythonCandidates = @(
  "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe",
  "C:\Users\jjtop\AppData\Local\Programs\Python\Python314\python.exe",
  (Join-Path $RootA "_runtime\python312-embed\python.exe"),
  "python"
)

function Get-EnvState {
  param([string]$Name)
  $process = [Environment]::GetEnvironmentVariable($Name, "Process")
  $user = [Environment]::GetEnvironmentVariable($Name, "User")
  $machine = [Environment]::GetEnvironmentVariable($Name, "Machine")
  [pscustomobject]@{
    Name = $Name
    ProcessSet = -not [string]::IsNullOrWhiteSpace($process)
    UserSet = -not [string]::IsNullOrWhiteSpace($user)
    MachineSet = -not [string]::IsNullOrWhiteSpace($machine)
  }
}

function Read-SecretPlainText {
  param([string]$Prompt)
  $secure = Read-Host -Prompt $Prompt -AsSecureString
  $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
  try {
    return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
  } finally {
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
  }
}

function Set-UserEnv {
  param(
    [string]$Name,
    [string]$Value
  )
  if ([string]::IsNullOrWhiteSpace($Value)) {
    throw "empty value for $Name"
  }
  [Environment]::SetEnvironmentVariable($Name, $Value, "User")
  [Environment]::SetEnvironmentVariable($Name, $Value, "Process")
}

function Resolve-AlertPython {
  foreach ($p in $PythonCandidates) {
    if ([string]::IsNullOrWhiteSpace($p)) { continue }
    if ($p -ne "python" -and -not (Test-Path $p)) { continue }
    try {
      $null = & $p -c "import sys; sys.path.insert(0, r'E:\1_Data\tools'); import notify_channels" 2>&1
      if ($LASTEXITCODE -eq 0) { return $p }
    } catch {
    }
  }
  throw "alert python runtime with notify_channels dependencies not found"
}

function Invoke-TestAlert {
  $py = Resolve-AlertPython
  $code = @"
import json
import sys
sys.path.insert(0, r"E:\1_Data\tools")
from notify_channels import send_alert
payload = send_alert(
    "[AUTO_DAILY_SYNC] phone alert channel test",
    level="error",
    channels=None,
    fail_silent=True,
    extra={"test": "phone_alert_channel_setup"},
)
print(json.dumps(payload, ensure_ascii=False))
raise SystemExit(0 if payload.get("ok") else 2)
"@
  $tmp = Join-Path (Join-Path $RootA "2_Logs") ("tmp_phone_alert_test_{0}.py" -f ([Guid]::NewGuid().ToString("N")))
  Set-Content -Path $tmp -Value $code -Encoding UTF8
  try {
    & $py $tmp
    return $LASTEXITCODE
  } finally {
    if (Test-Path $tmp) { Remove-Item $tmp -Force -ErrorAction SilentlyContinue }
  }
}

$names = @(
  "TELEGRAM_BOT_TOKEN",
  "TELEGRAM_CHAT_ID",
  "KAKAO_ACCESS_TOKEN",
  "ALERT_CHANNELS",
  "ALERT_CHANNELS_ERROR"
)

if (-not $Apply) {
  Write-Host "[CHECK] current alert environment"
  $names | ForEach-Object { Get-EnvState $_ } | Format-Table -AutoSize
  if ($Test) {
    exit (Invoke-TestAlert)
  }
  exit 0
}

if ($Channel -eq "check") {
  throw "Use -Channel telegram or -Channel kakao with -Apply."
}

if ($Channel -eq "telegram") {
  $botToken = Read-SecretPlainText "TELEGRAM_BOT_TOKEN"
  $chatId = Read-Host -Prompt "TELEGRAM_CHAT_ID"
  Set-UserEnv "TELEGRAM_BOT_TOKEN" $botToken
  Set-UserEnv "TELEGRAM_CHAT_ID" $chatId
  Set-UserEnv "ALERT_CHANNELS_ERROR" "telegram,file"
  Set-UserEnv "ALERT_CHANNELS" "telegram,file"
}

if ($Channel -eq "kakao") {
  $accessToken = Read-SecretPlainText "KAKAO_ACCESS_TOKEN"
  Set-UserEnv "KAKAO_ACCESS_TOKEN" $accessToken
  Set-UserEnv "ALERT_CHANNELS_ERROR" "kakao,file"
  Set-UserEnv "ALERT_CHANNELS" "kakao,file"
}

Write-Host "[APPLY] user environment updated for $Channel"
$names | ForEach-Object { Get-EnvState $_ } | Format-Table -AutoSize

if ($Test) {
  exit (Invoke-TestAlert)
}
