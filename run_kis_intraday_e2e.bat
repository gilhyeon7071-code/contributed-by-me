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

if "%E2E_MOCK%"=="" set "E2E_MOCK=auto"
if "%E2E_CODES%"=="" set "E2E_CODES=005930,000660"
if "%E2E_HEALTH_CODE%"=="" set "E2E_HEALTH_CODE=005930"
if "%E2E_ITERATIONS%"=="" set "E2E_ITERATIONS=3"
if "%E2E_INTERVAL_SEC%"=="" set "E2E_INTERVAL_SEC=120"
if "%E2E_TIMEOUT_SEC%"=="" set "E2E_TIMEOUT_SEC=180"
if "%E2E_MAX_ORDERS%"=="" set "E2E_MAX_ORDERS=1"
if "%E2E_NOTIFY%"=="" set "E2E_NOTIFY=1"
if "%E2E_APPLY%"=="" set "E2E_APPLY=0"
if "%E2E_CONFIRM%"=="" set "E2E_CONFIRM="
if "%E2E_VALIDATION_MODE%"=="" set "E2E_VALIDATION_MODE=0"
if "%E2E_SKIP_CANCEL_OPEN%"=="" set "E2E_SKIP_CANCEL_OPEN=0"
if "%E2E_STOP_ON_FAIL%"=="" set "E2E_STOP_ON_FAIL=0"

set "ARGS=--mock %E2E_MOCK% --codes %E2E_CODES% --health-code %E2E_HEALTH_CODE% --iterations %E2E_ITERATIONS% --interval-sec %E2E_INTERVAL_SEC% --timeout-sec %E2E_TIMEOUT_SEC% --max-orders %E2E_MAX_ORDERS%"
if "%E2E_NOTIFY%"=="1" set "ARGS=%ARGS% --notify"
if "%E2E_VALIDATION_MODE%"=="1" set "ARGS=%ARGS% --validation-mode"
if "%E2E_SKIP_CANCEL_OPEN%"=="1" set "ARGS=%ARGS% --skip-cancel-open"
if "%E2E_STOP_ON_FAIL%"=="1" set "ARGS=%ARGS% --stop-on-fail"
if "%E2E_APPLY%"=="1" set "ARGS=%ARGS% --apply --confirm %E2E_CONFIRM%"

echo [E2E] PY=%PY%
echo [E2E] ARGS=%ARGS%

%PY% tools\kis_intraday_e2e_runner.py %ARGS%
set "RC=%ERRORLEVEL%"

echo [E2E] exit=%RC%
exit /b %RC%
