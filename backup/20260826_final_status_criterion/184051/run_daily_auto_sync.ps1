param(
  [string]$RootA = "E:\1_Data",
  [string]$RootB = "E:\vibe\buffett"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$logDir = Join-Path $RootA "2_Logs"
if (-not (Test-Path $logDir)) {
  New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}

$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$logPath = Join-Path $logDir ("auto_daily_sync_{0}.log" -f $ts)

function Write-Log {
  param([string]$Message)
  $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
  $line | Tee-Object -FilePath $logPath -Append
}

function Resolve-AlertPython {
  $candidates = @(
    (Join-Path $RootA "_runtime\python312-embed\python.exe"),
    (Join-Path $RootA ".venv\Scripts\python.exe"),
    (Join-Path $RootB ".venv\Scripts\python.exe"),
    "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe",
    "C:\Users\jjtop\AppData\Local\Programs\Python\Python314\python.exe",
    "python"
  )

  foreach ($p in $candidates) {
    if ([string]::IsNullOrWhiteSpace($p)) { continue }
    if ($p -ne "python" -and -not (Test-Path $p)) { continue }

    try {
      $null = & $p -c "import requests, certifi" 2>&1
      if ($LASTEXITCODE -eq 0) { return $p }
    } catch {
    }
  }

  throw "alert python runtime with requests/certifi not found"
}

function Send-AutoDailyFailureAlert {
  param(
    [string]$Stage,
    [int]$ExitCode = 1,
    [string]$Message = ""
  )

  try {
    $alertPy = Resolve-AlertPython
    $toolsDir = Join-Path $RootA "tools"
    $text = "[AUTO_DAILY_SYNC] FAIL stage={0} rc={1} msg={2} log={3}" -f $Stage, $ExitCode, $Message, $logPath
    $code = @"
import sys
sys.path.insert(0, r"$toolsDir")
from notify_channels import send_alert
send_alert(r"""$text""", level="error", extra={"stage": r"""$Stage""", "exit_code": $ExitCode, "log": r"""$logPath"""})
"@
    $tmpCode = Join-Path $logDir ("tmp_auto_daily_alert_{0}.py" -f ([Guid]::NewGuid().ToString("N")))
    $tmpAlertOut = Join-Path $logDir ("tmp_auto_daily_alert_{0}.out.log" -f ([Guid]::NewGuid().ToString("N")))
    $tmpAlertErr = Join-Path $logDir ("tmp_auto_daily_alert_{0}.err.log" -f ([Guid]::NewGuid().ToString("N")))
    Set-Content -Path $tmpCode -Value $code -Encoding UTF8
    try {
      $p = Start-Process -FilePath $alertPy -ArgumentList @($tmpCode) -NoNewWindow -Wait -PassThru -RedirectStandardOutput $tmpAlertOut -RedirectStandardError $tmpAlertErr -ErrorAction Stop
      if ([int]$p.ExitCode -eq 0) {
        Write-Log ("ALERT SENT/LOGGED: stage={0} alert_rc=0" -f $Stage)
      } else {
        $errText = ""
        if (Test-Path $tmpAlertErr) {
          $errText = ((Get-Content -Path $tmpAlertErr -Tail 3) -join " ")
        }
        Write-Log ("ALERT FAIL-SOFT: stage={0} alert_rc={1} error={2}" -f $Stage, $p.ExitCode, $errText)
      }
    } finally {
      if (Test-Path $tmpCode) { Remove-Item $tmpCode -Force -ErrorAction SilentlyContinue }
      if (Test-Path $tmpAlertOut) { Remove-Item $tmpAlertOut -Force -ErrorAction SilentlyContinue }
      if (Test-Path $tmpAlertErr) { Remove-Item $tmpAlertErr -Force -ErrorAction SilentlyContinue }
    }
  } catch {
    Write-Log ("ALERT FAIL-SOFT: stage={0} error={1}" -f $Stage, $_.Exception.Message)
  }
}

function Assert-AutoDailyFinalStatus {
  $dashboardPath = Join-Path $RootB "runs\dashboard_state_latest.json"
  if (-not (Test-Path $dashboardPath)) {
    Write-Log ("FINAL STATUS FAIL: dashboard_state_latest.json missing path={0}" -f $dashboardPath)
    Send-AutoDailyFailureAlert -Stage "final_status" -ExitCode 20 -Message "dashboard_state_latest.json missing"
    exit 20
  }

  try {
    $state = Get-Content -Path $dashboardPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $overall = [string]$state.status_overall
    $alerts = 0
    if ($null -ne $state.health -and $null -ne $state.health.alerts_count) {
      $alerts = [int]$state.health.alerts_count
    }
    Write-Log ("FINAL STATUS: dashboard_overall={0} alerts_count={1} path={2}" -f $overall, $alerts, $dashboardPath)
    if ($overall.ToUpperInvariant() -ne "PASS" -or $alerts -gt 0) {
      Send-AutoDailyFailureAlert -Stage "final_status" -ExitCode 20 -Message ("dashboard_overall={0} alerts_count={1}" -f $overall, $alerts)
      exit 20
    }
  } catch {
    Write-Log ("FINAL STATUS FAIL: dashboard_state parse/check error={0}" -f $_.Exception.Message)
    Send-AutoDailyFailureAlert -Stage "final_status" -ExitCode 20 -Message $_.Exception.Message
    exit 20
  }
}

function Resolve-Python {
  # 2026-08-20: Test-Path 만으로 고르면 "존재하지만 쓸 수 없는" 런타임을 집는다.
  # E:\1_Data\.venv 는 lint 전용이라 pandas 가 없고, 그것이 1순위였다.
  # 그 결과 vibe_onepass_run 이 2026-08-09 부터 8회 연속 ModuleNotFoundError 로 실패했다.
  # RequireModule 을 주면 실제로 import 되는지까지 확인하고, 안 되면 다음 후보로 넘어간다.
  param([string[]]$Candidates, [string]$RequireModule = "")
  foreach ($p in $Candidates) {
    if ([string]::IsNullOrWhiteSpace($p)) { continue }
    if ($p -eq "python") {
      try {
        $proc = Start-Process -FilePath "python" -ArgumentList @("-V") -NoNewWindow -Wait -PassThru -ErrorAction Stop
        if ($proc.ExitCode -eq 0) { return "python" }
      } catch {}
      continue
    }
    if (-not (Test-Path $p)) { continue }
    if ([string]::IsNullOrWhiteSpace($RequireModule)) { return $p }
    try {
      $pyArg = '-c "import ' + $RequireModule + '"'
      $chk = Start-Process -FilePath $p -ArgumentList $pyArg -NoNewWindow -Wait -PassThru -ErrorAction Stop
      if ($chk.ExitCode -eq 0) { return $p }
      Write-Log ("PYTHON SKIP: {0} (cannot import {1})" -f $p, $RequireModule)
    } catch {
      Write-Log ("PYTHON SKIP: {0} ({1})" -f $p, $_.Exception.Message)
    }
  }
  throw "python runtime not found"
}

function Invoke-Step {
  param(
    [string]$Name,
    [string]$FilePath,
    [string[]]$Arguments = @(),
    [string]$WorkingDirectory = "",
    # rc=0 이어도 이 시간보다 빨리 끝나면 실패로 본다 (PLANS 2026-08-21 (31)).
    # 08-11~08-20 아침 배치가 10~109초에 rc=0 으로 끝났는데 "성공"으로 기록돼
    # 8거래일간 알림이 한 번도 나가지 않았다. 0 이면 검사하지 않는다.
    [int]$MinDurationSec = 0
  )

  $stepStarted = Get-Date
  Write-Log ("STEP START: {0}" -f $Name)
  Write-Log ("CMD: {0} {1}" -f $FilePath, ($Arguments -join " "))

  $tmpOut = Join-Path $logDir ("tmp_{0}_{1}.out.log" -f $Name, ([Guid]::NewGuid().ToString("N")))
  $tmpErr = Join-Path $logDir ("tmp_{0}_{1}.err.log" -f $Name, ([Guid]::NewGuid().ToString("N")))

  try {
    $sp = @{
      FilePath = $FilePath
      ArgumentList = $Arguments
      NoNewWindow = $true
      Wait = $true
      PassThru = $true
      RedirectStandardOutput = $tmpOut
      RedirectStandardError = $tmpErr
      ErrorAction = "Stop"
    }
    if (-not [string]::IsNullOrWhiteSpace($WorkingDirectory)) {
      $sp.WorkingDirectory = $WorkingDirectory
    }

    $p = Start-Process @sp
    $rc = [int]$p.ExitCode

    if (Test-Path $tmpOut) {
      Get-Content -Path $tmpOut | Tee-Object -FilePath $logPath -Append
    }
    if (Test-Path $tmpErr) {
      Get-Content -Path $tmpErr | Tee-Object -FilePath $logPath -Append
    }
  } finally {
    if (Test-Path $tmpOut) { Remove-Item $tmpOut -Force -ErrorAction SilentlyContinue }
    if (Test-Path $tmpErr) { Remove-Item $tmpErr -Force -ErrorAction SilentlyContinue }
  }

  $elapsedSec = [int]((Get-Date) - $stepStarted).TotalSeconds

  if ($rc -ne 0) {
    Write-Log ("STEP FAIL: {0} rc={1} elapsed_s={2}" -f $Name, $rc, $elapsedSec)
    Send-AutoDailyFailureAlert -Stage $Name -ExitCode $rc -Message "step failed"
    exit $rc
  }

  if ($MinDurationSec -gt 0 -and $elapsedSec -lt $MinDurationSec) {
    Write-Log ("STEP FAIL: {0} rc=0 but elapsed_s={1} < min_s={2} (too fast to be a real run)" -f $Name, $elapsedSec, $MinDurationSec)
    Send-AutoDailyFailureAlert -Stage $Name -ExitCode 90 -Message ("rc=0 but elapsed_s={0} < min_s={1}" -f $elapsedSec, $MinDurationSec)
    exit 90
  }

  Write-Log ("STEP OK: {0} elapsed_s={1}" -f $Name, $elapsedSec)
}

try {
  Write-Log "AUTO DAILY SYNC START"
  Write-Log ("RootA={0}" -f $RootA)
  Write-Log ("RootB={0}" -f $RootB)

  # 2026-08-20: 공식 실행 런타임을 1순위로 둔다.
  # RootA의 .venv 는 lint 전용(pandas 없음)이라 후보에서 제외한다.
  $pyA = Resolve-Python -RequireModule "pandas" -Candidates @(
    (Join-Path $RootA "_runtime\python312-embed\python.exe"),
    (Join-Path $RootB ".venv\Scripts\python.exe"),
    "python"
  )

  $pyB = Resolve-Python -RequireModule "pandas" -Candidates @(
    (Join-Path $RootB ".venv\Scripts\python.exe"),
    (Join-Path $RootA "_runtime\python312-embed\python.exe"),
    "python"
  )

  Write-Log ("PY_A={0}" -f $pyA)
  Write-Log ("PY_B={0}" -f $pyB)

  # 아침 배치 정상 소요는 979~4501초(16~75분) 실측. 실패 사례는 10~109초였다.
  $minDaily = 300
  if ($env:RUN_DAILY_MIN_DURATION_SEC) { $minDaily = [int]$env:RUN_DAILY_MIN_DURATION_SEC }
  Invoke-Step -Name "run_paper_daily" -FilePath "cmd" -Arguments @("/c", (Join-Path $RootA "run_paper_daily.bat")) -WorkingDirectory $RootA -MinDurationSec $minDaily
  Invoke-Step -Name "vibe_onepass_run" -FilePath $pyA -Arguments @((Join-Path $RootA "tools\vibe_onepass_run.py"), "--mode", "paper", "--apply-ledger", "--force-snapshot") -WorkingDirectory $RootA
  Invoke-Step -Name "ssot_today_final_update" -FilePath $pyB -Arguments @((Join-Path $RootB "tools\ssot_today_final_update.py")) -WorkingDirectory $RootB
  Invoke-Step -Name "build_dashboard_state_v2" -FilePath $pyB -Arguments @((Join-Path $RootB "tools\build_dashboard_state_v2.py")) -WorkingDirectory $RootB
  Assert-AutoDailyFinalStatus

  Write-Log "AUTO DAILY SYNC DONE"
  exit 0
} catch {
  Write-Log ("AUTO DAILY SYNC EXCEPTION: {0}" -f $_.Exception.Message)
  Send-AutoDailyFailureAlert -Stage "exception" -ExitCode 1 -Message $_.Exception.Message
  exit 1
}
