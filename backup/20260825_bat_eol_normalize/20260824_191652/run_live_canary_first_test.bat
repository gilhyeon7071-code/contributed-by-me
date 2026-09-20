@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "ROOT=%~dp0"
pushd "%ROOT%" || exit /b 2

set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
set "PS=C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"
"%PS%" -NoProfile -ExecutionPolicy Bypass -Command "if (-not (Test-Path -LiteralPath '%PY%')) { exit 1 }; & '%PY%' -V | Out-Null"
if errorlevel 1 (
  echo [FAILED] python runtime not found
  exit /b 9009
)

if "%CANARY_MOCK%"=="" set "CANARY_MOCK=false"
if "%CANARY_MAX_ORDERS%"=="" set "CANARY_MAX_ORDERS=1"
if "%CANARY_MAX_TOTAL_QTY%"=="" set "CANARY_MAX_TOTAL_QTY=100"
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

"%PS%" -NoProfile -ExecutionPolicy Bypass -Command "& '%PY%' 'tools\kis_live_canary_first_test.py' %ARGS%"
set "RC=%ERRORLEVEL%"

echo [CANARY_FIRST] exit=%RC%
exit /b %RC%
