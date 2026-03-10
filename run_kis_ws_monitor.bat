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

if "%WS_CODES%"=="" set "WS_CODES=005930"
if "%WS_CHANNELS%"=="" set "WS_CHANNELS=trade,hoga"
if "%WS_MOCK%"=="" set "WS_MOCK=auto"
if "%WS_DURATION_SEC%"=="" set "WS_DURATION_SEC=0"
if "%WS_NOTIFY_ON_ERROR%"=="" set "WS_NOTIFY_ON_ERROR=1"

set "ARGS=--codes %WS_CODES% --channels %WS_CHANNELS% --mock %WS_MOCK% --duration-sec %WS_DURATION_SEC%"
if "%WS_NOTIFY_ON_ERROR%"=="1" set "ARGS=%ARGS% --notify-on-error"

echo [WS] PY=%PY%
echo [WS] ARGS=%ARGS%

%PY% tools\kis_realtime_ws.py %ARGS%
set "RC=%ERRORLEVEL%"

echo [WS] exit=%RC%
exit /b %RC%
