@echo off
setlocal EnableExtensions
REM After close summary (CMD only)
cd /d %~dp0

set "PY="
if exist "%~dp0_runtime\python312-embed\python.exe" set "PY=%~dp0_runtime\python312-embed\python.exe"
if not defined PY if exist "%~dp0.venv\Scripts\python.exe" set "PY=%~dp0.venv\Scripts\python.exe"
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

"%PY%" after_close_summary.py
if "%LOG_CLEANUP_TIMEOUT_SEC%"=="" set "LOG_CLEANUP_TIMEOUT_SEC=120"
if errorlevel 1 (
  echo [FAILED] after_close_summary.py
  exit /b 1
)

REM [LOG_CLEANUP_V1] live cleanup (retention 30d, delete ON). Never fail pipeline. Bounded by timeout.
"%PY%" E:\1_Data\tools\run_step_with_timeout.py --name log_cleanup_rootA_2_Logs --timeout-sec %LOG_CLEANUP_TIMEOUT_SEC% --status-json 2_Logs\log_cleanup_rootA_timeout_status_latest.json -- "%PY%" E:\1_Data\tools\log_cleanup_30d.py --target E:\1_Data\2_Logs --retention-days 30 --enabled
if errorlevel 1 (
  echo [LOG_CLEANUP_V1] rootA_cleanup_failed_but_ignored
)

"%PY%" E:\1_Data\tools\run_step_with_timeout.py --name log_cleanup_rootB_runs --timeout-sec %LOG_CLEANUP_TIMEOUT_SEC% --status-json 2_Logs\log_cleanup_rootB_runs_timeout_status_latest.json -- "%PY%" E:\1_Data\tools\log_cleanup_30d.py --target E:\vibe\buffett\runs --retention-days 30 --include-ext .json,.txt,.log --enabled
if errorlevel 1 (
  echo [LOG_CLEANUP_V1] rootB_runs_cleanup_failed_but_ignored
)

"%PY%" E:\1_Data\tools\run_step_with_timeout.py --name log_cleanup_rootB_orders --timeout-sec %LOG_CLEANUP_TIMEOUT_SEC% --status-json 2_Logs\log_cleanup_rootB_orders_timeout_status_latest.json -- "%PY%" E:\1_Data\tools\log_cleanup_30d.py --target E:\vibe\buffett\data\orders --retention-days 30 --bak-only --all-ext --enabled
if errorlevel 1 (
  echo [LOG_CLEANUP_V1] rootB_orders_backup_cleanup_failed_but_ignored
)

exit /b 0




