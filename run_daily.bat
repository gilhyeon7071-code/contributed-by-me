@echo off
setlocal EnableExtensions
TITLE STOC Daily Runner
chcp 65001 > nul
set PYTHONIOENCODING=utf-8

set "BASE_DIR=%~dp0"
echo [SYSTEM] STOC runner start
echo [INFO] BASE_DIR=%BASE_DIR%

set "PY="
if exist "%BASE_DIR%.venv\Scripts\python.exe" set "PY=%BASE_DIR%.venv\Scripts\python.exe"
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
  goto :FAIL
)

echo [PY] %PY%

echo [STEP 0] Market Guard
"%PY%" "%BASE_DIR%market_guard.py"
set "MG=%ERRORLEVEL%"

echo [STEP 0.5] Paper Init
"%PY%" "%BASE_DIR%paper_init.py"
if errorlevel 1 goto :FAIL

echo [STEP A] Optimize-if-due
"%PY%" "%BASE_DIR%optimize_if_due_v41_1.py"
if errorlevel 1 goto :FAIL

echo [STEP B] Generate Candidates
"%PY%" "%BASE_DIR%generate_candidates_v41_1.py"
if errorlevel 1 goto :FAIL

echo [STEP D] Backtest Report-if-due
"%PY%" "%BASE_DIR%report_if_due_v41_1.py"
if errorlevel 1 goto :FAIL

echo [STEP C] Cleanup / Archive
"%PY%" "%BASE_DIR%cleanup_manager.py"
if errorlevel 1 goto :FAIL

if "%BROKER_MODE%"=="" set "BROKER_MODE=OFF"
if "%BROKER_VALIDATION_MODE%"=="" set "BROKER_VALIDATION_MODE=0"
if "%BROKER_BLOCK_PREFIXES%"=="" set "BROKER_BLOCK_PREFIXES=CAP_"
if "%BROKER_MIN_LIVE_ORDERS%"=="" set "BROKER_MIN_LIVE_ORDERS=3"
if "%BROKER_MOCK%"=="" set "BROKER_MOCK=auto"
if "%BROKER_CANCEL_OPEN%"=="" set "BROKER_CANCEL_OPEN=0"
if "%BROKER_CANCEL_MIN_AGE_MINUTES%"=="" set "BROKER_CANCEL_MIN_AGE_MINUTES=10"
if "%BROKER_CANCEL_MAX_ORDERS%"=="" set "BROKER_CANCEL_MAX_ORDERS=0"
if "%BROKER_PREFLIGHT_KIS%"=="" set "BROKER_PREFLIGHT_KIS=1"
if "%BROKER_PREFLIGHT_CODE%"=="" set "BROKER_PREFLIGHT_CODE=005930"
if "%BROKER_PREFLIGHT_CHECK_BALANCE%"=="" set "BROKER_PREFLIGHT_CHECK_BALANCE=1"
if "%BROKER_PREFLIGHT_CHECK_OPEN_ORDERS%"=="" set "BROKER_PREFLIGHT_CHECK_OPEN_ORDERS=0"
if "%BROKER_PREFLIGHT_NOTIFY%"=="" set "BROKER_PREFLIGHT_NOTIFY=1"
if /I "%BROKER_VALIDATION_MODE%"=="1" if /I "%BROKER_MOCK%"=="auto" set "BROKER_MOCK=true"
set "BROKER_VALID_ARGS="
if /I "%BROKER_VALIDATION_MODE%"=="1" set "BROKER_VALID_ARGS=--validation-mode --allow-block-prefixes %BROKER_BLOCK_PREFIXES% --min-live-orders %BROKER_MIN_LIVE_ORDERS%"
set "BROKER_CANCEL_ARGS=--mock %BROKER_MOCK% --min-age-minutes %BROKER_CANCEL_MIN_AGE_MINUTES% --max-cancels %BROKER_CANCEL_MAX_ORDERS%"
set "BROKER_PREFLIGHT_ARGS=--mock %BROKER_MOCK% --code %BROKER_PREFLIGHT_CODE%"
if "%BROKER_PREFLIGHT_CHECK_BALANCE%"=="1" set "BROKER_PREFLIGHT_ARGS=%BROKER_PREFLIGHT_ARGS% --check-balance"
if "%BROKER_PREFLIGHT_CHECK_OPEN_ORDERS%"=="1" set "BROKER_PREFLIGHT_ARGS=%BROKER_PREFLIGHT_ARGS% --check-open-orders"
if "%BROKER_PREFLIGHT_NOTIFY%"=="1" set "BROKER_PREFLIGHT_ARGS=%BROKER_PREFLIGHT_ARGS% --notify-on-fail"
echo [BROKER] mode=%BROKER_MODE% validation=%BROKER_VALIDATION_MODE% mock=%BROKER_MOCK% cancel_open=%BROKER_CANCEL_OPEN% preflight=%BROKER_PREFLIGHT_KIS%
if /I "%BROKER_MODE%"=="OFF" (
  echo [BROKER] skipped OFF
) else (
  if "%BROKER_PREFLIGHT_KIS%"=="1" (
    if /I "%BROKER_MODE%"=="DRY" (
      echo [BROKER] preflight skipped in DRY mode
    ) else (
      echo [BROKER] preflight KIS healthcheck
      "%PY%" "%BASE_DIR%tools\kis_healthcheck.py" %BROKER_PREFLIGHT_ARGS%
      if errorlevel 1 (
        echo [FAILED] broker preflight failed. blocking order dispatch.
        goto :FAIL
      )
    )
  )
  if /I "%BROKER_MODE%"=="DRY" (
    "%PY%" "%BASE_DIR%tools\kis_order_dispatch_from_exec.py" --mock %BROKER_MOCK% %BROKER_VALID_ARGS%
    if errorlevel 1 goto :FAIL
    if "%BROKER_CANCEL_OPEN%"=="1" (
      "%PY%" "%BASE_DIR%tools\kis_cancel_open_orders.py" %BROKER_CANCEL_ARGS%
      if errorlevel 1 goto :FAIL
    )
  ) else (
    if /I "%BROKER_MODE%"=="APPLY" (
      "%PY%" "%BASE_DIR%tools\kis_order_dispatch_from_exec.py" --mock %BROKER_MOCK% %BROKER_VALID_ARGS% --apply
      if errorlevel 1 goto :FAIL
      if "%BROKER_CANCEL_OPEN%"=="1" (
        "%PY%" "%BASE_DIR%tools\kis_cancel_open_orders.py" %BROKER_CANCEL_ARGS% --apply
        if errorlevel 1 goto :FAIL
      )
    ) else (
      if /I "%BROKER_MODE%"=="APPLY_SYNC" (
        if "%BROKER_LIVE_PATH%"=="" set "BROKER_LIVE_PATH=E:\vibe\buffett\data\live\live_fills.csv"
        "%PY%" "%BASE_DIR%tools\kis_order_dispatch_from_exec.py" --mock %BROKER_MOCK% %BROKER_VALID_ARGS% --apply
        if errorlevel 1 goto :FAIL
        if "%BROKER_CANCEL_OPEN%"=="1" (
          "%PY%" "%BASE_DIR%tools\kis_cancel_open_orders.py" %BROKER_CANCEL_ARGS% --apply
          if errorlevel 1 goto :FAIL
        )
        for /f "usebackq delims=" %%I in (`"%PY%" -c "import datetime as d; print(d.datetime.now().strftime('%%Y%%m%%d'))"`) do set "D_TODAY=%%I"
        if "%D_TODAY%"=="" goto :FAIL
        "%PY%" "%BASE_DIR%tools\kis_sync_fills_from_api.py" --date %D_TODAY% --bridge-write --bridge-live-path "%BROKER_LIVE_PATH%"
        if errorlevel 1 goto :FAIL
      ) else (
        echo [FAILED] invalid BROKER_MODE=%BROKER_MODE%
        goto :FAIL
      )
    )
  )
)
if "%MG%"=="0" goto :OPEN
echo [SUCCESS] Finished (Market Closed) at %TIME%.
goto :END_OK

:OPEN
echo [SUCCESS] Finished at %TIME%.
goto :END_OK

:FAIL
set "RC=%ERRORLEVEL%"
if "%RC%"=="0" set "RC=1"
echo [ERROR] A step failed at %TIME%. See messages above.
goto :END_FAIL

:END_OK
pause
exit /b 0

:END_FAIL
pause
exit /b %RC%




