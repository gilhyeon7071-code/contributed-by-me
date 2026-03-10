@echo off
setlocal EnableExtensions

set "ROOT=%~dp0"
pushd "%ROOT%" || exit /b 2

set "PY="
if exist "%ROOT%.venv\Scripts\python.exe" set "PY=%ROOT%.venv\Scripts\python.exe"
if not defined PY if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY if exist "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe" set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY (
  where python >nul 2>nul && set "PY=python"
)
if defined PY (
  %PY% -V >nul 2>nul
  if errorlevel 1 set "PY="
)
if not defined PY (
  where py >nul 2>nul && set "PY=py -3"
)
if defined PY (
  %PY% -V >nul 2>nul
  if errorlevel 1 set "PY="
)
if not defined PY (
  echo [FAILED] python runtime not found
  exit /b 9009
)

if "%SOAK_CODES%"=="" set "SOAK_CODES=005930,000660"
if "%SOAK_MOCK%"=="" set "SOAK_MOCK=auto"
if "%SOAK_DURATION_HOURS%"=="" set "SOAK_DURATION_HOURS=24"
if "%SOAK_INTERVAL_SEC%"=="" set "SOAK_INTERVAL_SEC=30"
if "%SOAK_NOTIFY%"=="" set "SOAK_NOTIFY=1"
if "%SOAK_FAIL_RATIO_MAX%"=="" set "SOAK_FAIL_RATIO_MAX=0.05"
if "%SOAK_MAX_CONSEC_FAIL%"=="" set "SOAK_MAX_CONSEC_FAIL=5"

set "ARGS=--codes %SOAK_CODES% --mock %SOAK_MOCK% --duration-hours %SOAK_DURATION_HOURS% --interval-sec %SOAK_INTERVAL_SEC% --fail-ratio-max %SOAK_FAIL_RATIO_MAX% --max-consecutive-fail %SOAK_MAX_CONSEC_FAIL%"
if "%SOAK_NOTIFY%"=="1" set "ARGS=%ARGS% --notify"

echo [SOAK] PY=%PY%
echo [SOAK] ARGS=%ARGS%

%PY% tools\kis_soak_test.py %ARGS%
set "RC=%ERRORLEVEL%"

echo [SOAK] exit=%RC%
exit /b %RC%
