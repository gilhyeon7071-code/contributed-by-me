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

if "%CANARY_MOCK%"=="" set "CANARY_MOCK=false"
if "%CANARY_MAX_ORDERS%"=="" set "CANARY_MAX_ORDERS=1"
if "%CANARY_MAX_TOTAL_QTY%"=="" set "CANARY_MAX_TOTAL_QTY=3"
if "%CANARY_EXECUTE%"=="" set "CANARY_EXECUTE=0"
if "%CANARY_CONFIRM%"=="" set "CANARY_CONFIRM="
if "%CANARY_NOTIFY%"=="" set "CANARY_NOTIFY=1"
if "%CANARY_SKIP_CANCEL_OPEN%"=="" set "CANARY_SKIP_CANCEL_OPEN=0"
if "%CANARY_SKIP_VIRTUAL_GATE%"=="" set "CANARY_SKIP_VIRTUAL_GATE=0"
if "%CANARY_VIRTUAL_MAX_AGE_DAYS%"=="" set "CANARY_VIRTUAL_MAX_AGE_DAYS=7"

set "ARGS=--mock %CANARY_MOCK% --max-orders %CANARY_MAX_ORDERS% --max-total-qty %CANARY_MAX_TOTAL_QTY% --virtual-max-age-days %CANARY_VIRTUAL_MAX_AGE_DAYS%"
if "%CANARY_NOTIFY%"=="1" set "ARGS=%ARGS% --notify"
if "%CANARY_SKIP_CANCEL_OPEN%"=="1" set "ARGS=%ARGS% --skip-cancel-open"
if "%CANARY_SKIP_VIRTUAL_GATE%"=="1" set "ARGS=%ARGS% --skip-virtual-gate"
if "%CANARY_EXECUTE%"=="1" set "ARGS=%ARGS% --execute --confirm %CANARY_CONFIRM%"

echo [CANARY_FIRST] PY=%PY%
echo [CANARY_FIRST] ARGS=%ARGS%

%PY% tools\kis_live_canary_first_test.py %ARGS%
set "RC=%ERRORLEVEL%"

echo [CANARY_FIRST] exit=%RC%
exit /b %RC%
