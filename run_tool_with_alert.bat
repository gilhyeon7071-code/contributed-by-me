@echo off
REM run_tool_with_alert.bat - run a tools/*.py under the official runtime, log it,
REM   and alert on failure.  2026-08-26: five scheduled tasks had no failure alert at all,
REM   and one of them (the 11:00 spread poll) died silently on its first run.
REM
REM   usage:  run_tool_with_alert.bat <label> <tools\script.py> [args...]
REM   log:    2_Logs\<label>_last.txt
setlocal EnableExtensions EnableDelayedExpansion

REM [2026-08-26] ROOT 는 shift 전에 잡아야 한다.
REM   shift 는 %0 도 옮기므로 shift 뒤의 %~dp0 는 인자 경로가 된다
REM   (실측: ROOT 가 tools 로 중복돼 경로가 어긋났다)
set "ROOT=%~dp0"

set "LABEL=%~1"
if "%LABEL%"=="" (
  echo [USAGE] run_tool_with_alert.bat ^<label^> ^<tools\script.py^> [args...]
  endlocal & exit /b 2
)
shift
set "SCRIPT=%~1"
if "%SCRIPT%"=="" (
  echo [USAGE] missing script
  endlocal & exit /b 2
)
shift

REM collect the remaining arguments
set "ARGS="
:COLLECT
if "%~1"=="" goto RUN
set "ARGS=!ARGS! %1"
shift
goto COLLECT

:RUN
set "LOG_DIR=%ROOT%2_Logs"
pushd "%ROOT%" || exit /b 2
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

set "PY="
if exist "%ROOT%_runtime\python312-embed\python.exe" set "PY=%ROOT%_runtime\python312-embed\python.exe"
if not defined PY if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY (
  where python >nul 2>nul && set "PY=python"
)
if not defined PY (
  echo [HARD_FAIL] python runtime not found
  popd
  endlocal & exit /b 9009
)

set "LOG=%LOG_DIR%\%LABEL%_last.txt"
if exist "%ROOT%tools\rotate_log.py" "%PY%" "%ROOT%tools\rotate_log.py" "%LOG%" >nul 2>nul
set "PYTHONIOENCODING=utf-8"

"%PY%" "%ROOT%%SCRIPT%"!ARGS! > "%LOG%" 2>&1
set "RC=%ERRORLEVEL%"
type "%LOG%"

REM rc=3 is a documented "waiting for data" state in these tools, not a failure.
if "%RC%"=="3" (
  echo [WAIT] %LABEL%: rc=3 waiting, not a failure
  popd
  endlocal & exit /b 0
)
if not "%RC%"=="0" (
  echo [FAIL] %LABEL% rc=%RC%
  if exist "%ROOT%tools\task_fail_alert.py" "%PY%" "%ROOT%tools\task_fail_alert.py" "%LABEL%" "%RC%" "%LOG%" >nul 2>nul
  popd
  endlocal & exit /b %RC%
)
echo [OK] %LABEL%
popd
endlocal & exit /b 0
