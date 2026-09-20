@echo off
setlocal EnableExtensions
cd /d %~dp0

powershell -NoProfile -ExecutionPolicy Bypass -Command "& '%~dp0run_trading_stage_validation_report.bat'"
if errorlevel 1 (
  echo [FAST_VALIDATE] warn: trading_stage_validation refresh failed; continue with latest artifacts
)

call "%~dp0run_hold_diagnose_fast.bat"
if errorlevel 1 (
  echo [FAST_VALIDATE] failed: hold_diagnose
  exit /b 12
)

echo [FAST_VALIDATE] done
exit /b 0
