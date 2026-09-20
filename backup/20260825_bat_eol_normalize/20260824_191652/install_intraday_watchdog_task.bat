@echo off
setlocal EnableExtensions

set "TASK_NAME=VIBE_Intraday_Watchdog"
set "BAT=E:\1_Data\run_intraday_watchdog.bat"
set "HIDDEN=E:\1_Data\run_intraday_watchdog_hidden.vbs"
set "TR=wscript.exe \"E:\1_Data\run_intraday_watchdog_hidden.vbs\""

if not exist "%BAT%" (
  echo [FAILED] missing: %BAT%
  endlocal & exit /b 1
)
if not exist "%HIDDEN%" (
  echo [FAILED] missing: %HIDDEN%
  endlocal & exit /b 1
)

schtasks /Create /TN "%TASK_NAME%" /SC MINUTE /MO 1 /TR "%TR%" /F
set "RC=%ERRORLEVEL%"
if "%RC%"=="0" (
  echo [OK] task created: %TASK_NAME%
) else (
  echo [FAILED] schtasks rc=%RC%
)
endlocal & exit /b %RC%
