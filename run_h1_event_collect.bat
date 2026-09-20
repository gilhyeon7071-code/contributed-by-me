@echo off
REM H1 forced-flow event collection (DART). Read-only: no orders, no trading changes.
REM Manual run:  run_h1_event_collect.bat
REM Scheduling is NOT registered yet - needs user approval (see PLANS 2026-09-18).
setlocal
set PYTHONIOENCODING=utf-8
set ROOT=E:\1_Data
set PY=%ROOT%\_runtime\python312-embed\python.exe
set EVENTS=%ROOT%\paper\strategies\kospi_mcap_quarterly_v2\data\events
set RULES=%ROOT%\paper\strategies\kospi_mcap_quarterly_v2\config\h1_event_rules_v1_1.json
cd /d %ROOT%

REM 1) today's disclosures (run again next day: same-day receipts keep coming in)
"%PY%" -m paper.strategies.kospi_mcap_quarterly_v2.src.collect_dart_events --out-dir "%EVENTS%" --rules "%RULES%"
set RC1=%ERRORLEVEL%

REM 2) yesterday's, to fill late receipts
for /f %%d in ('powershell -NoProfile -Command "(Get-Date).AddDays(-1).ToString(\"yyyyMMdd\")"') do set YDAY=%%d
"%PY%" -m paper.strategies.kospi_mcap_quarterly_v2.src.collect_dart_events --out-dir "%EVENTS%" --rules "%RULES%" --start %YDAY% --end %YDAY%
set RC2=%ERRORLEVEL%

REM 3) detail figures (qty / amount / period) for events that still lack them
"%PY%" -m paper.strategies.kospi_mcap_quarterly_v2.src.collect_event_details --events-dir "%EVENTS%"
set RC3=%ERRORLEVEL%

echo rc collect=%RC1% backfill=%RC2% detail=%RC3%
if not "%RC1%"=="0" exit /b %RC1%
if not "%RC2%"=="0" exit /b %RC2%
exit /b 0
