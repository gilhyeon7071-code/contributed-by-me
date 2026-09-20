@echo off
setlocal EnableExtensions
cd /d %~dp0

set "PY="
if exist "%~dp0_runtime\python312-embed\python.exe" set "PY=%~dp0_runtime\python312-embed\python.exe"
if not defined PY if exist "%~dp0.venv\Scripts\python.exe" set "PY=%~dp0.venv\Scripts\python.exe"
if not defined PY if exist "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe" set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY (
  where python >nul 2>nul && set "PY=python"
)
if not defined PY (
  echo [FAILED] python runtime not found
  exit /b 9009
)

echo [RECLAIM_OBSERVE] PY=%PY%
"%PY%" "%~dp0tools\build_reclaim_observe_sampler.py" %*
set "RC=%ERRORLEVEL%"
echo [RECLAIM_OBSERVE] exit=%RC%
exit /b %RC%
