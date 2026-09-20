@echo off
setlocal EnableExtensions

set "ROOT=%~dp0"
set "PS1=%ROOT%tools\run_daily_auto_sync.ps1"

if not exist "%PS1%" (
  echo [FAILED] script not found: "%PS1%"
  exit /b 2
)

echo [AUTO] start full_auto at %DATE% %TIME%
powershell -NoProfile -ExecutionPolicy Bypass -File "%PS1%"
set "RC=%ERRORLEVEL%"

if "%RC%"=="0" (
  echo [AUTO] done full_auto rc=0
) else (
  echo [AUTO] failed full_auto rc=%RC%
)

exit /b %RC%
