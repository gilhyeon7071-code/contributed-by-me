@echo off
REM run_rebalance_daily.bat - rebalance book daily driver (morning | evening).
REM   2026-08-25. The book opened in PLANS 109 but nothing scheduled it:
REM   0 of 19 tasks and 0 of 84 bats referenced rebalance.
REM   morning = decide and write today's orders (uses yesterday's close, no lookahead)
REM   evening = fill at today's close and report.  rc=3 means "archive not in yet".
setlocal EnableExtensions

set "PHASE=%~1"
if "%PHASE%"=="" set "PHASE=evening"

set "ROOT=%~dp0"
set "LOG_DIR=%ROOT%2_Logs"
pushd "%ROOT%" || exit /b 2
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

set "PY="
if exist "%ROOT%_runtime\python312-embed\python.exe" set "PY=%ROOT%_runtime\python312-embed\python.exe"
if not defined PY if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY if exist "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe" set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY (
  where python >nul 2>nul && set "PY=python"
)
if not defined PY (
  echo [HARD_FAIL] python runtime not found
  popd
  endlocal & exit /b 9009
)

set "LOG=%LOG_DIR%\run_rebalance_daily_last.txt"
if exist "%ROOT%tools\rotate_log.py" "%PY%" "%ROOT%tools\rotate_log.py" "%LOG%" >nul 2>nul
set "PYTHONIOENCODING=utf-8"

"%PY%" "%ROOT%tools\rebalance_daily.py" --phase %PHASE% --apply > "%LOG%" 2>&1
set "RC=%ERRORLEVEL%"
type "%LOG%"

REM rc=3 is "waiting for the archive", not a failure. The next run picks it up,
REM and the morning phase catches up on anything the evening missed.
if "%RC%"=="3" (
  echo [WAIT] rebalance %PHASE%: archive not available yet
  popd
  endlocal & exit /b 0
)
if not "%RC%"=="0" (
  echo [FAIL] rebalance %PHASE% rc=%RC%
  if exist "%ROOT%tools\rebalance_fail_alert.py" "%PY%" "%ROOT%tools\rebalance_fail_alert.py" %PHASE% %RC% "%LOG%" >nul 2>nul
  popd
  endlocal & exit /b %RC%
)
echo [OK] rebalance %PHASE% done
popd
endlocal & exit /b 0
