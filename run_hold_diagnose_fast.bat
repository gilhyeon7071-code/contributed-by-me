@echo off
setlocal EnableExtensions
cd /d %~dp0

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\diagnose_hold_fast.ps1" %*
set "RC=%ERRORLEVEL%"
echo [HOLD_FAST] exit=%RC%
exit /b %RC%
