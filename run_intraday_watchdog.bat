@echo off
setlocal EnableExtensions

set "ROOT=%~dp0"
pushd "%ROOT%" || exit /b 2

if not exist "%ROOT%2_Logs" mkdir "%ROOT%2_Logs" >nul 2>nul
set "LOG=%ROOT%2_Logs\run_intraday_watchdog_last.txt"

if "%INTRADAY_WATCHDOG_STALE_MIN%"=="" set "INTRADAY_WATCHDOG_STALE_MIN=12"
if "%INTRADAY_WATCHDOG_COOLDOWN_SEC%"=="" set "INTRADAY_WATCHDOG_COOLDOWN_SEC=180"

echo [START] %DATE% %TIME% > "%LOG%"
echo STALE_MIN=%INTRADAY_WATCHDOG_STALE_MIN% COOLDOWN_SEC=%INTRADAY_WATCHDOG_COOLDOWN_SEC% >> "%LOG%"
powershell -NoProfile -ExecutionPolicy Bypass -File "%ROOT%tools\intraday_loop_watchdog.ps1" -StaleMin %INTRADAY_WATCHDOG_STALE_MIN% -RestartCooldownSec %INTRADAY_WATCHDOG_COOLDOWN_SEC% >> "%LOG%" 2>&1
set "RC=%ERRORLEVEL%"
echo [END] rc=%RC% %DATE% %TIME% >> "%LOG%"

popd
endlocal & exit /b %RC%
