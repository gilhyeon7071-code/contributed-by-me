@echo off
setlocal EnableExtensions

set "ROOT=%~dp0"
pushd "%ROOT%" || exit /b 2

set "PY="
if exist "%ROOT%_runtime\python312-embed\python.exe" set "PY=%ROOT%_runtime\python312-embed\python.exe"
if not defined PY if exist "%ROOT%.venv\Scripts\python.exe" set "PY=%ROOT%.venv\Scripts\python.exe"
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

if "%WS_HOGA_MAX_CODES%"=="" set "WS_HOGA_MAX_CODES=36"
if "%WS_HOGA_CHANNELS%"=="" set "WS_HOGA_CHANNELS=hoga"
if "%WS_HOGA_MOCK%"=="" set "WS_HOGA_MOCK=auto"
if "%WS_HOGA_DURATION_SEC%"=="" set "WS_HOGA_DURATION_SEC=23400"
if "%WS_HOGA_MAX_SUBSCRIPTIONS%"=="" set "WS_HOGA_MAX_SUBSCRIPTIONS=40"
if "%WS_HOGA_NOTIFY_ON_ERROR%"=="" set "WS_HOGA_NOTIFY_ON_ERROR=1"
if "%WS_HOGA_EXTRA_SUBSCRIPTIONS%"=="" set "WS_HOGA_EXTRA_SUBSCRIPTIONS=H0UPCNT0:0001,H0UPCNT0:1001,H0UPCNT0:2001"

set "ARGS=--max-codes %WS_HOGA_MAX_CODES% --channels %WS_HOGA_CHANNELS% --mock %WS_HOGA_MOCK% --duration-sec %WS_HOGA_DURATION_SEC% --max-subscriptions %WS_HOGA_MAX_SUBSCRIPTIONS% --worker-id-prefix hoga --extra-subscriptions %WS_HOGA_EXTRA_SUBSCRIPTIONS%"
if "%WS_HOGA_NOTIFY_ON_ERROR%"=="1" set "ARGS=%ARGS% --notify-on-error"

echo [WS_HOGA] PY=%PY%
echo [WS_HOGA] ARGS=%ARGS%

"%PY%" tools\kis_ws_multiplexer.py %ARGS%
set "RC=%ERRORLEVEL%"

echo [WS_HOGA] exit=%RC%
exit /b %RC%
