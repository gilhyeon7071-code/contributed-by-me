@echo off
REM ============================================================
REM  [2026-09-09] Refresh the two [decision]-grade data sources that had
REM  NO automated refresh at all. Both had gone 13 trading days stale
REM  (limit 5) and nothing in any .bat / .ps1 / scheduled task rebuilt them.
REM
REM    market_master_latest.json      code -> KOSPI/KOSDAQ. Built from local
REM                                   clean parquets. No external dependency.
REM    pykrx_fundamental_latest.csv   market_cap only. pykrx is fully broken
REM                                   (KRX changed the response shape; every
REM                                   date fails), but market_cap = close x
REM                                   listed_shares reproduces the source
REM                                   exactly - verified 2,578/2,578 codes,
REM                                   median error 0.000000%.
REM
REM  Must run AFTER the daily clean parquet exists (~15:50) and after the
REM  16:05 index fetch. Avoids the 16:30/16:40 reserved boundary.
REM
REM  ASCII comments only (cmd emits stderr on Hangul REM).
REM  See .agent\PLANS.md (274).
REM ============================================================
setlocal EnableExtensions
set "ROOT=%~dp0"
if "%PYTHONIOENCODING%"=="" set "PYTHONIOENCODING=utf-8"
set "LOG=%ROOT%2_Logs\decision_sources_daily.log"
echo [START] %DATE% %TIME% >> "%LOG%"
set "RC=0"

REM Trading-day guard. Weekend runs would rebuild from a stale panel and
REM refresh the mtime, which would make the freshness guard read as PASS.
"%ROOT%_runtime\python312-embed\python.exe" "%ROOT%tools\topn_session_guard.py" >> "%LOG%" 2>&1
set "GRC=%ERRORLEVEL%"
if "%GRC%"=="3" goto :NOTRADE
if not "%GRC%"=="0" (
  echo [FAIL] trading-day guard rc=%GRC% >> "%LOG%"
  endlocal & exit /b %GRC%
)

call "%ROOT%run_tool_with_alert.bat" market_master tools\build_market_master.py >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [FAIL] step=market_master rc=%ERRORLEVEL% >> "%LOG%"
  set "RC=%ERRORLEVEL%"
)

call "%ROOT%run_tool_with_alert.bat" market_cap_refresh tools\refresh_market_cap_from_panel.py --apply >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [FAIL] step=market_cap_refresh rc=%ERRORLEVEL% >> "%LOG%"
  set "RC=%ERRORLEVEL%"
)

echo [END] rc=%RC% %DATE% %TIME% >> "%LOG%"
endlocal & exit /b %RC%

:NOTRADE
echo [WAIT] not a trading day - skipping >> "%LOG%"
echo [END] skipped %DATE% %TIME% >> "%LOG%"
endlocal & exit /b 0
