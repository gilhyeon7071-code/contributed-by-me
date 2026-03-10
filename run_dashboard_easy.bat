@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "ROOT=E:\vibe\buffett"
set "URL=http://localhost:8501"
set "PORT=8501"
set "LOG=%ROOT%\_tmp_dashboard_launch_last.log"
set "PY=%ROOT%\.venv\Scripts\python.exe"
set "HOURLY_PS=E:\1_Data\tools\vibe_dashboard_state_hourly_loop.ps1"
set "SAFE_GUARD_PS=E:\1_Data\tools\dashboard_safe_guard.ps1"

if exist "%SAFE_GUARD_PS%" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%SAFE_GUARD_PS%" -RootB "%ROOT%" -MinSizeBytes 5000 >nul 2>nul
  if errorlevel 1 (
    echo [ERROR] dashboard safe guard failed.
    exit /b 11
  )
)

call :is_listening
if "%LISTENING%"=="1" (
  echo [INFO] Dashboard already running: %URL%
  if exist "%HOURLY_PS%" (
    echo [INFO] Ensure hourly refresh loop 60m...
    start "" /min powershell -NoProfile -ExecutionPolicy Bypass -File "%HOURLY_PS%" -RootB "%ROOT%" -Port %PORT% -IntervalMinutes 60
  )
  start "" "%URL%" >nul 2>nul
  exit /b 0
)

if not exist "%ROOT%\dashboard.py" (
  echo [ERROR] Missing dashboard script: %ROOT%\dashboard.py
  exit /b 2
)

if not exist "%PY%" (
  echo [ERROR] Missing venv python: %PY%
  exit /b 3
)

pushd "%ROOT%" >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Cannot enter directory: %ROOT%
  exit /b 4
)

if exist "%LOG%" del /q "%LOG%" >nul 2>nul

echo [INFO] Sync SSOT pointer...
"%PY%" "%ROOT%\tools\ssot_today_final_update.py" > "%LOG%" 2>&1
if errorlevel 1 (
  findstr /C:"NO_SNAPSHOT_TODAY" "%LOG%" >nul
  if not errorlevel 1 (
    echo [INFO] Today's snapshot missing. Building snapshot...
    cmd /c "%ROOT%\tools\snapshot_and_package.cmd" >> "%LOG%" 2>&1
    if errorlevel 1 (
      echo [ERROR] snapshot_and_package.cmd failed. See: %LOG%
      popd
      exit /b 5
    )
    "%PY%" "%ROOT%\tools\ssot_today_final_update.py" >> "%LOG%" 2>&1
    if errorlevel 1 (
      echo [ERROR] SSOT pointer update failed after snapshot. See: %LOG%
      popd
      exit /b 6
    )
  ) else (
    echo [ERROR] ssot_today_final_update.py failed. See: %LOG%
    popd
    exit /b 7
  )
)

echo [INFO] Build dashboard_state_latest.json...
if exist "%ROOT%\tools\build_dashboard_state_v2.py" (
  "%PY%" "%ROOT%\tools\build_dashboard_state_v2.py" >> "%LOG%" 2>&1
) else (
  "%PY%" "%ROOT%\tools\build_dashboard_state.py" >> "%LOG%" 2>&1
)
if errorlevel 1 (
  echo [WARN] build_dashboard_state failed. Continue launch.
)

echo [INFO] Launch dashboard process...
start "" /min "%PY%" -m streamlit run "%ROOT%\dashboard.py" --server.port %PORT% --server.headless true

set /a WAIT_SEC=0
:wait_loop
call :is_listening
if "%LISTENING%"=="1" goto :ready
if %WAIT_SEC% GEQ 60 (
  echo [ERROR] Dashboard did not open port %PORT% within 60s. See: %LOG%
  popd
  exit /b 8
)
set /a WAIT_SEC+=1
timeout /t 1 /nobreak >nul
goto :wait_loop

:ready
echo [INFO] Dashboard ready: %URL%
if exist "%HOURLY_PS%" (
  echo [INFO] Start hourly refresh loop 60m...
  start "" /min powershell -NoProfile -ExecutionPolicy Bypass -File "%HOURLY_PS%" -RootB "%ROOT%" -Port %PORT% -IntervalMinutes 60
)
start "" "%URL%" >nul 2>nul
popd
exit /b 0

:is_listening
set "LISTENING=0"
for /f "tokens=1,2,3,4,5" %%A in ('netstat -ano ^| findstr /I ":%PORT% " ^| findstr /I "LISTENING"') do (
  set "LISTENING=1"
  goto :eof
)
exit /b 0
