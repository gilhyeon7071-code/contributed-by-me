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

if "%WS_INDEX_CODES%"=="" set "WS_INDEX_CODES=0001,1001,2001"
if "%WS_INDEX_CHANNELS%"=="" set "WS_INDEX_CHANNELS=trade"
if "%WS_INDEX_MOCK%"=="" set "WS_INDEX_MOCK=auto"
if "%WS_INDEX_DURATION_SEC%"=="" set "WS_INDEX_DURATION_SEC=23400"
if "%WS_INDEX_NOTIFY_ON_ERROR%"=="" set "WS_INDEX_NOTIFY_ON_ERROR=1"

set "ARGS=--codes %WS_INDEX_CODES% --channels %WS_INDEX_CHANNELS% --mock %WS_INDEX_MOCK% --duration-sec %WS_INDEX_DURATION_SEC% --worker-id index"
if "%WS_INDEX_NOTIFY_ON_ERROR%"=="1" set "ARGS=%ARGS% --notify-on-error"

echo [WS_INDEX] PY=%PY%
echo [WS_INDEX] ARGS=%ARGS%

"%PY%" tools\kis_ws_index_realtime.py %ARGS%
set "RC=%ERRORLEVEL%"

echo [WS_INDEX] exit=%RC%
exit /b %RC%
