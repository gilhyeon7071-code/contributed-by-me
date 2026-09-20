@echo off
setlocal EnableExtensions

set "ROOT=%~dp0"
pushd "%ROOT%" || (
  echo [FAILED] pushd failed: "%ROOT%"
  exit /b 2
)

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

set "MODE=%~1"
if /I "%MODE%"=="" set "MODE=DRY"

set "ARGS=--root E:\1_Data"
if /I "%MODE%"=="DOIT" (
  set "ARGS=%ARGS% --apply"
) else (
  set "ARGS=%ARGS% --dry-run"
)

echo [TEMP_CLEANUP] PY=%PY%
echo [TEMP_CLEANUP] MODE=%MODE%
echo [TEMP_CLEANUP] ARGS=%ARGS%

%PY% tools\maintenance\temp_cleanup_policy.py %ARGS%
set "RC=%ERRORLEVEL%"

echo [TEMP_CLEANUP] exit=%RC%
exit /b %RC%
