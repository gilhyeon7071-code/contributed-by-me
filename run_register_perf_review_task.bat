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

if "%PERF_TASK_NAME%"=="" set "PERF_TASK_NAME=Buffett_Perf_Weekly"
if "%PERF_TASK_DAY%"=="" set "PERF_TASK_DAY=MON"
if "%PERF_TASK_TIME%"=="" set "PERF_TASK_TIME=18:10"
if "%PERF_LOOKBACK_DAYS%"=="" set "PERF_LOOKBACK_DAYS=7"
if "%PERF_TASK_DRY_RUN%"=="" set "PERF_TASK_DRY_RUN=1"
if "%PERF_TASK_FORCE%"=="" set "PERF_TASK_FORCE=1"
if "%PERF_TASK_RUN_NOW%"=="" set "PERF_TASK_RUN_NOW=0"

set "ARGS=--task-name %PERF_TASK_NAME% --day %PERF_TASK_DAY% --time %PERF_TASK_TIME% --lookback-days %PERF_LOOKBACK_DAYS%"
if "%PERF_TASK_DRY_RUN%"=="1" set "ARGS=%ARGS% --dry-run"
if "%PERF_TASK_FORCE%"=="1" set "ARGS=%ARGS% --force"
if "%PERF_TASK_RUN_NOW%"=="1" set "ARGS=%ARGS% --run-now"

echo [PERF_TASK] PY=%PY%
echo [PERF_TASK] ARGS=%ARGS%

%PY% tools\register_perf_review_task.py %ARGS%
set "RC=%ERRORLEVEL%"

echo [PERF_TASK] exit=%RC%
exit /b %RC%
