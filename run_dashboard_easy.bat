@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "ROOT=E:\vibe\buffett"
set "STATE_PY=E:\1_Data\_runtime\python312-embed\python.exe"
set "PY=%STATE_PY%"
set "FALLBACK_PY=%ROOT%\.venv\Scripts\python.exe"
set "HOURLY_PS=E:\1_Data\tools\vibe_dashboard_state_hourly_loop.ps1"
set "SAFE_GUARD_PS=E:\1_Data\tools\dashboard_safe_guard.ps1"
set "POLICY_JSON=E:\1_Data\dashboard_refresh_policy.json"
set "MODE=%~1"
set "BOOT_SYNC_ENABLED=1"

if /I "%MODE%"=="integrated" (
  set "DASHBOARD_LABEL=Integrated Validation Dashboard"
  set "APP_SCRIPT=dashboard.py"
  set "URL=http://localhost:8501"
  set "PORT=8501"
  set "LOG=%ROOT%\_tmp_dashboard_integrated_launch_last.log"
  set "RUN_LOG=%ROOT%\_tmp_dashboard_integrated_launch_%RANDOM%.log"
  set "REFRESH_MINUTES=60"
) else (
  set "MODE=stock"
  set "DASHBOARD_LABEL=Stock Dashboard"
  set "APP_SCRIPT=dashboard_stock_v2.py"
  set "URL=http://localhost:8502"
  set "PORT=8502"
  set "LOG=%ROOT%\_tmp_dashboard_stock_launch_last.log"
  set "RUN_LOG=%ROOT%\_tmp_dashboard_stock_launch_%RANDOM%.log"
  set "REFRESH_MINUTES=10"
)

if exist "%SAFE_GUARD_PS%" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%SAFE_GUARD_PS%" -RootB "%ROOT%" -DashboardRelPath "%APP_SCRIPT%" -MinSizeBytes 5000 >nul 2>nul
  if errorlevel 1 (
    echo [ERROR] dashboard safe guard failed.
    exit /b 11
  )
)

call :is_listening
if "%LISTENING%"=="1" (
  echo [INFO] %DASHBOARD_LABEL% already running: %URL%
  echo [INFO] %DASHBOARD_LABEL% already running: %URL% > "%RUN_LOG%"
  if exist "%HOURLY_PS%" (
    echo [INFO] Ensure refresh loop %REFRESH_MINUTES%m...
    echo [INFO] Ensure refresh loop %REFRESH_MINUTES%m... >> "%RUN_LOG%"
    start "" /min powershell -NoProfile -ExecutionPolicy Bypass -File "%HOURLY_PS%" -RootB "%ROOT%" -Port %PORT% -IntervalMinutes %REFRESH_MINUTES%
  )
  call :publish_log
  exit /b 0
)

if not exist "%ROOT%\%APP_SCRIPT%" (
  echo [ERROR] Missing dashboard script: %ROOT%\%APP_SCRIPT%
  exit /b 2
)

if not exist "%PY%" (
  if exist "%FALLBACK_PY%" (
    echo [WARN] Missing official streamlit python: %PY%; fallback to %FALLBACK_PY%
    set "PY=%FALLBACK_PY%"
  ) else (
    echo [ERROR] Missing dashboard python: %PY%
    exit /b 3
  )
)
if not exist "%STATE_PY%" (
  echo [WARN] Missing official state python: %STATE_PY%
  set "STATE_PY=%PY%"
)

if exist "%POLICY_JSON%" (
  for /f "usebackq delims=" %%I in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='Stop'; $j=Get-Content -Path '%POLICY_JSON%' -Raw -Encoding UTF8 | ConvertFrom-Json; if($j.boot_sync -and $null -ne $j.boot_sync.enabled){ if([bool]$j.boot_sync.enabled){'1'} else {'0'} } else {'1'}" 2^>nul`) do set "BOOT_SYNC_ENABLED=%%I"
)
if not defined BOOT_SYNC_ENABLED set "BOOT_SYNC_ENABLED=1"

pushd "%ROOT%" >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Cannot enter directory: %ROOT%
  exit /b 4
)

if exist "%RUN_LOG%" del /q "%RUN_LOG%" >nul 2>nul

echo [INFO] Sync SSOT pointer...
call :run_retry "%STATE_PY%" "%ROOT%\tools\ssot_today_final_update.py" > "%RUN_LOG%" 2>&1
if errorlevel 1 (
  findstr /C:"NO_SNAPSHOT_TODAY" "%RUN_LOG%" >nul
  if errorlevel 1 findstr /C:"NO_SNAPSHOT_FOR_D:" "%RUN_LOG%" >nul
  if not errorlevel 1 (
    echo [INFO] Today's snapshot missing. Building snapshot...
    cmd /c "%ROOT%\tools\snapshot_and_package.cmd" >> "%RUN_LOG%" 2>&1
    if errorlevel 1 (
      echo [WARN] snapshot_and_package.cmd failed. Try stale SSOT fallback.
      call :allow_stale_ssot_or_fail 5
    ) else (
      call :run_retry "%STATE_PY%" "%ROOT%\tools\ssot_today_final_update.py" >> "%RUN_LOG%" 2>&1
      if errorlevel 1 (
        echo [WARN] SSOT pointer update failed after snapshot. Try stale SSOT fallback.
        call :allow_stale_ssot_or_fail 6
      )
    )
  ) else (
    echo [ERROR] ssot_today_final_update.py failed. See: %LOG%
    call :publish_log
    popd
    exit /b 7
  )
)
if "%BOOT_SYNC_ENABLED%"=="1" (
  echo [INFO] Build dashboard_state_latest.json... (boot_sync=on)
  if exist "%ROOT%\tools\build_dashboard_state_v2.py" (
    call :run_retry "%STATE_PY%" "%ROOT%\tools\build_dashboard_state_v2.py" >> "%RUN_LOG%" 2>&1
  ) else (
    call :run_retry "%STATE_PY%" "%ROOT%\tools\build_dashboard_state.py" >> "%RUN_LOG%" 2>&1
  )
  if errorlevel 1 (
    echo [WARN] build_dashboard_state failed. Continue launch.
  )
) else (
  echo [INFO] Skip boot sync state build by policy (boot_sync=off).
)

echo [INFO] Launch %DASHBOARD_LABEL% process...
start "" /min "%PY%" -m streamlit run "%ROOT%\%APP_SCRIPT%" --server.port %PORT% --server.headless true

set /a WAIT_SEC=0
:wait_loop
call :is_listening
if "%LISTENING%"=="1" goto :ready
if %WAIT_SEC% GEQ 60 (
  echo [ERROR] Dashboard did not open port %PORT% within 60s. See: %LOG%
  call :publish_log
  popd
  exit /b 8
)
set /a WAIT_SEC+=1
timeout /t 1 /nobreak >nul
goto :wait_loop

:ready
echo [INFO] %DASHBOARD_LABEL% ready: %URL%
if exist "%HOURLY_PS%" (
  echo [INFO] Start refresh loop %REFRESH_MINUTES%m...
  start "" /min powershell -NoProfile -ExecutionPolicy Bypass -File "%HOURLY_PS%" -RootB "%ROOT%" -Port %PORT% -IntervalMinutes %REFRESH_MINUTES%
)
call :publish_log
popd
exit /b 0

:allow_stale_ssot_or_fail
if exist "%ROOT%\runs\SSOT_TODAY_FINAL.json" (
  echo [WARN] Continue launch with existing SSOT_TODAY_FINAL.json
  call :publish_log
  exit /b 0
)
echo [ERROR] No fallback SSOT pointer file. See: %LOG%
call :publish_log
popd
exit /b %~1

:is_listening
set "LISTENING=0"
for /f "tokens=1,2,3,4,5" %%A in ('netstat -ano ^| findstr /I ":%PORT% " ^| findstr /I "LISTENING"') do (
  set "LISTENING=1"
  goto :eof
)
exit /b 0

:run_retry
set "RUN_RETRY_CMD=%~1"
set "RUN_RETRY_ARG=%~2"
set /a RUN_RETRY_I=0
:run_retry_loop
"%RUN_RETRY_CMD%" "%RUN_RETRY_ARG%"
set "RUN_RETRY_RC=%ERRORLEVEL%"
if "%RUN_RETRY_RC%"=="0" exit /b 0
set /a RUN_RETRY_I+=1
if %RUN_RETRY_I% GEQ 5 exit /b %RUN_RETRY_RC%
echo [WARN] command failed rc=%RUN_RETRY_RC%; retry %RUN_RETRY_I%/5 after file-lock backoff
timeout /t 2 /nobreak >nul
goto :run_retry_loop

:publish_log
if defined RUN_LOG if exist "%RUN_LOG%" copy /y "%RUN_LOG%" "%LOG%" >nul 2>nul
exit /b 0




