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

REM [2026-09-08] Pre-build tomorrow's order file. This step is what the header above
REM   has claimed since day one ("then build the order file that the NEXT trading
REM   session will try to execute") - it was documented but never implemented.
REM
REM   Why it matters, measured today: the morning leg's first cycle always does a
REM   full panel load (6.4M rows, ~1GB off a USB HDD). With the OS file cache warm
REM   it took 8m21s (09-07); with it cold it took 47m04s (09-08, because the 08:30
REM   batch was skipped and nothing had warmed the cache). The order file is
REM   identical either way - it is built from the signal-day close, which is final
REM   at 21:32. Building it tonight makes the morning leg hit [SKIP] and dispatch
REM   within seconds of 09:05.
REM
REM   MUST run last. The morning freshness check compares the order file's mtime
REM   against positions.csv and forward_ledger.csv; if either is newer it rebuilds.
REM
REM   Fail-open by design: if this step fails the morning leg simply builds the
REM   file itself, exactly as it does today. It can make the morning faster,
REM   never blocked. That is why rc is a WARN and never sets RC.
REM   The date lookup lives in tools\next_trading_day.py, not in a python -c one-liner:
REM   inside `for /f usebackq` a second pair of quotes makes cmd truncate the command
REM   (measured: 'python.exe" -c "import' is not recognized). The pushd below removes
REM   the need for those quotes - the scheduled task sets no working directory, so
REM   without it the relative paths would resolve against C:\Windows\System32.
set "EXECD="
pushd "%ROOT%"
for /f "usebackq delims=" %%D in (`_runtime\python312-embed\python.exe tools\next_trading_day.py`) do set "EXECD=%%D"
popd
if not defined EXECD (
  echo [WARN] step=prebuild next_trading_day empty - skipping. morning leg will build it >> "%LOG%"
  goto :DONE
)
echo [PREBUILD] exec=%EXECD% >> "%LOG%"
call "%ROOT%run_tool_with_alert.bat" topn_build_orders tools\topn_build_orders.py --date %EXECD% >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] step=prebuild rc=%ERRORLEVEL% - morning leg will build it >> "%LOG%"
)

REM [2026-09-13] Evaluate what the forward ledger actually picked.
REM   The ledger had 10 days x 60 rows and all four readers only counted days.
REM   Nothing joined the picks to prices, so nothing was ever visible.
REM   Observation only - never fails the batch over a report.
"%ROOT%_runtime\python312-embed\python.exe" "%ROOT%tools\topn_forward_eval.py" >> "%LOG%" 2>&1

goto :DONE

:NOTRADE
echo [WAIT] not a trading day - skipping >> "%LOG%"
echo [END] skipped %DATE% %TIME% >> "%LOG%"
endlocal & exit /b 0

:DONE
echo [END] evening rc=%RC% %DATE% %TIME% >> "%LOG%"
endlocal & exit /b %RC%
