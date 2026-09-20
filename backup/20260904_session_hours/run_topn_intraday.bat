@echo off
REM ============================================================
REM  RD_20260901_topn stage-1 : INTRADAY leg (execution day D+1)
REM  Prereg : docs/references/PREREG_RD_20260901_TOPN.md
REM
REM  Does   : build today's order file from the last signal day, try to dispatch,
REM           then reconcile fills into the harness position ledger.
REM  Repeats: the scheduler re-runs this through the session. Execution time is
REM           NOT a fixed clock value - it is whenever the tradability checks
REM           (LOB availability, spread, risk gates, buying power) allow it.
REM           "when did it become tradable" is itself a stage-1 measurement.
REM
REM  Safe to repeat: kis_order_dispatch_from_exec dedups on the submit log.
REM           Only ACCEPTED / *_PENDING keys count as done, so a dispatched order
REM           is never re-sent while a blocked one is retried next cycle.
REM           topn_dispatch.py restores our previous submit log before running so
REM           that dedup history survives the artifact isolation.
REM
REM  TOPN_APPLY=1 -> real orders to the MOCK account. 0 -> dry-run.
REM  The crash guard (risk_off) is deliberately NOT bypassed. Full block returns
REM  rc=3, the repo's "waiting, not a failure" convention.
REM
REM  Note   : ASCII comments only (cmd emits stderr on Hangul REM).
REM ============================================================
setlocal EnableExtensions

set "ROOT=%~dp0"
if "%PYTHONIOENCODING%"=="" set "PYTHONIOENCODING=utf-8"
if "%TOPN_APPLY%"=="" set "TOPN_APPLY=1"

set "LOGDIR=%ROOT%2_Logs\topn"
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
set "LOG=%LOGDIR%\run_topn_intraday.log"

set "DISPATCH_ARGS=--mock true"
if "%TOPN_APPLY%"=="1" set "DISPATCH_ARGS=%DISPATCH_ARGS% --apply --confirm TOPN_APPLY"

echo [START] intraday %DATE% %TIME% TOPN_APPLY=%TOPN_APPLY% >> "%LOG%"

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

call "%ROOT%run_tool_with_alert.bat" topn_build_orders tools\topn_build_orders.py >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [FAIL] step=build_orders rc=%ERRORLEVEL% >> "%LOG%"
  set "RC=%ERRORLEVEL%"
  goto :DONE
)

call "%ROOT%run_tool_with_alert.bat" topn_dispatch tools\topn_dispatch.py %DISPATCH_ARGS% >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] step=dispatch rc=%ERRORLEVEL% - continuing to reconcile >> "%LOG%"
)

call "%ROOT%run_tool_with_alert.bat" topn_reconcile tools\topn_reconcile.py >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [FAIL] step=reconcile rc=%ERRORLEVEL% >> "%LOG%"
  set "RC=%ERRORLEVEL%"
  goto :DONE
)

goto :DONE

:NOTRADE
echo [WAIT] not a trading day - skipping >> "%LOG%"
echo [END] skipped %DATE% %TIME% >> "%LOG%"
endlocal & exit /b 0

:DONE
echo [END] intraday rc=%RC% %DATE% %TIME% >> "%LOG%"
endlocal & exit /b %RC%
