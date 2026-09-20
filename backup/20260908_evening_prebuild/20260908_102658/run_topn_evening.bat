@echo off
REM ============================================================
REM  RD_20260901_topn stage-1 : EVENING leg (signal day D)
REM  Prereg : docs/references/PREREG_RD_20260901_TOPN.md
REM
REM  Does   : compute day-D candidates, then build the order file that the
REM           NEXT trading session will try to execute.
REM  Sends  : NOTHING. Dispatch lives in run_topn_intraday.bat.
REM
REM  Why split: the price panel refreshes at 21:32, so day-D candidates can only
REM           be computed after the close. Ordering at that hour is rejected by
REM           the broker - measured: 18 of 76 off-hours applies came back
REM           dispatch_status=SKIP_MARKET_CLOSED (broker: market closed), 0 fills.
REM           So signal day D and execution day D+1 are necessarily different.
REM
REM  Timing : 22:10 (after the 21:30 batch). exec_date is set by the intraday leg.
REM  Note   : ASCII comments only (cmd emits stderr on Hangul REM).
REM ============================================================
setlocal EnableExtensions

set "ROOT=%~dp0"
if "%PYTHONIOENCODING%"=="" set "PYTHONIOENCODING=utf-8"

set "LOGDIR=%ROOT%2_Logs\topn"
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
set "LOG=%LOGDIR%\run_topn_evening.log"

echo [START] evening %DATE% %TIME% >> "%LOG%"

REM Trading-day guard. The Daily trigger also fires on weekends and holidays;
REM mock apply is exempt from the dispatcher session guard, so nothing else stops it.
REM Without this, held_days would advance on non-trading days and HOLD_DAYS=13 is
REM counted in TRADING days.
REM rc is compared EXACTLY. `if errorlevel 3` means ">= 3", so a broken interpreter
REM path (9009) would read as "holiday" and silently disable the whole harness.
"%ROOT%_runtime\python312-embed\python.exe" "%ROOT%tools\topn_session_guard.py" >> "%LOG%" 2>&1
set "GRC=%ERRORLEVEL%"
if "%GRC%"=="3" goto :NOTRADE
if not "%GRC%"=="0" (
  echo [FAIL] trading-day guard rc=%GRC% >> "%LOG%"
  set "RC=%GRC%"
  goto :DONE
)
set "RC=0"

call "%ROOT%run_tool_with_alert.bat" topn_candidates tools\topn_candidates.py --forward >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [FAIL] step=candidates rc=%ERRORLEVEL% >> "%LOG%"
  set "RC=%ERRORLEVEL%"
  goto :DONE
)

REM reconcile brings the position ledger up to date before tomorrow's sizing.
call "%ROOT%run_tool_with_alert.bat" topn_reconcile tools\topn_reconcile.py >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] step=reconcile rc=%ERRORLEVEL% >> "%LOG%"
)

goto :DONE

:NOTRADE
echo [WAIT] not a trading day - skipping >> "%LOG%"
echo [END] skipped %DATE% %TIME% >> "%LOG%"
endlocal & exit /b 0

:DONE
echo [END] evening rc=%RC% %DATE% %TIME% >> "%LOG%"
endlocal & exit /b %RC%
