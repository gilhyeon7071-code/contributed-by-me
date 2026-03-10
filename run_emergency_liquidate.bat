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

if "%EMERGENCY_MOCK%"=="" set "EMERGENCY_MOCK=auto"
if "%EMERGENCY_APPLY%"=="" set "EMERGENCY_APPLY=0"
if "%EMERGENCY_CANCEL_OPEN_FIRST%"=="" set "EMERGENCY_CANCEL_OPEN_FIRST=1"
if "%EMERGENCY_NOTIFY%"=="" set "EMERGENCY_NOTIFY=1"
if "%EMERGENCY_REASON%"=="" set "EMERGENCY_REASON=MANUAL_TRIGGER"

set "ARGS=--mock %EMERGENCY_MOCK% --reason %EMERGENCY_REASON%"
if "%EMERGENCY_CANCEL_OPEN_FIRST%"=="1" set "ARGS=%ARGS% --cancel-open-first"
if "%EMERGENCY_NOTIFY%"=="1" set "ARGS=%ARGS% --notify"
if "%EMERGENCY_APPLY%"=="1" set "ARGS=%ARGS% --apply"

echo [EMERGENCY] PY=%PY%
echo [EMERGENCY] ARGS=%ARGS%

%PY% tools\kis_emergency_liquidate.py %ARGS%
set "RC=%ERRORLEVEL%"

echo [EMERGENCY] exit=%RC%
exit /b %RC%
