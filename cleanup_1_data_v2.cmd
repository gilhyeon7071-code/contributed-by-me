@echo off
setlocal EnableExtensions

set "ROOT=%~dp0"
set "PY="
if exist "%ROOT%_runtime\python312-embed\python.exe" set "PY=%ROOT%_runtime\python312-embed\python.exe"
if not defined PY if exist "%ROOT%.venv\Scripts\python.exe" set "PY=%ROOT%.venv\Scripts\python.exe"
if not defined PY (
  where python >nul 2>nul && set "PY=python"
)
if not defined PY (
  echo [CLEANUP_V2] FAILED python runtime not found
  exit /b 9009
)

set "MODE=%~1"
if /I "%MODE%"=="" set "MODE=DRY"

if /I "%MODE%"=="DRY" (
  "%PY%" "%ROOT%tools\maintenance\cleanup_1_data_v2.py"
  exit /b %ERRORLEVEL%
)

if /I "%MODE%"=="DOIT" (
  "%PY%" "%ROOT%tools\maintenance\cleanup_1_data_v2.py" --apply --confirm-d-drive
  exit /b %ERRORLEVEL%
)

if /I "%MODE%"=="ROLLBACK" (
  if "%~2"=="" (
    echo [CLEANUP_V2] Usage: cleanup_1_data_v2.cmd ROLLBACK ^<manifest_path^>
    exit /b 2
  )
  "%PY%" "%ROOT%tools\maintenance\cleanup_1_data_v2.py" --rollback "%~2"
  exit /b %ERRORLEVEL%
)

echo [CLEANUP_V2] Usage: cleanup_1_data_v2.cmd [DRY^|DOIT^|ROLLBACK ^<manifest_path^>]
exit /b 2
