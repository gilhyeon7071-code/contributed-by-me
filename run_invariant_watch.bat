@echo off
REM [2026-09-12] Invariant watch - things that must stay true, re-measured daily.
REM   Why one wrapper: the watchdog coverage audit found invariant monitoring
REM   absent, and that absence is the common root of this year's failures.
REM   New invariant checks go HERE rather than becoming another scheduled task.
REM   1) round_freeze_watch      - frozen rounds still match their baseline
REM   2) verify_20260820_changes - the 9 operational changes have not reverted
REM   3) verify_20260913_changes - the 09-12/13 repairs actually work on a session
REM   4) verify_20260914_changes - the aftermarket-day operational changes
REM   5) market_observation_report - cost of the fail-closed entry gate
REM   6) craft_scan               - repeat coding mistakes I actually made
REM   Rationale: PLANS (370)(371)(373) and docs\references\OPEN_ITEMS_REGISTER.md
REM   ASCII-only comments on purpose: cmd reads .bat with codepage 949 and
REM   Korean UTF-8 comments were parsed as commands on 2026-09-12.
setlocal EnableExtensions
cd /d E:\1_Data

set "PY=E:\1_Data\_runtime\python312-embed\python.exe"
if not exist "%PY%" set "PY=python"
set "LOG=E:\1_Data\2_Logs\invariant_watch_last.txt"

echo [START] %DATE% %TIME% >> "%LOG%"

REM boundary_watch runs FIRST. If a no-touch boundary moved, that is the most
REM   important line in this log and it must not scroll off the bottom.
REM   2026-09-12: a collection batch was moved 16:30 -> 20:30 against an explicit
REM   no-touch boundary, and nothing detected it. Intent did not hold; detection does.
echo [0/7] boundary_watch >> "%LOG%"
"%PY%" tools\boundary_watch.py >> "%LOG%" 2>&1
set "RC0=%ERRORLEVEL%"

echo [1/7] round_freeze_watch >> "%LOG%"
"%PY%" tools\round_freeze_watch.py >> "%LOG%" 2>&1
set "RC1=%ERRORLEVEL%"

echo [2/7] verify_20260820_changes >> "%LOG%"
"%PY%" tools\verify_20260820_changes.py >> "%LOG%" 2>&1
set "RC2=%ERRORLEVEL%"

echo [3/7] verify_20260913_changes >> "%LOG%"
"%PY%" tools\verify_20260913_changes.py >> "%LOG%" 2>&1
set "RC3=%ERRORLEVEL%"

echo [4/7] verify_20260914_changes >> "%LOG%"
"%PY%" tools\verify_20260914_changes.py >> "%LOG%" 2>&1
set "RC4=%ERRORLEVEL%"

REM [2026-09-13] Index-observation failure rate. This is the COST of the
REM   2026-09-12 fail-closed entry gate: each failure blocks new entries for that
REM   cycle, and the market may well be trading normally. Printed every night so
REM   nobody has to remember to ask - the answer is already on screen in a week.
echo [5/7] market_observation_report >> "%LOG%"
"%PY%" tools\market_observation_report.py >> "%LOG%" 2>&1
set "RC5=%ERRORLEVEL%"

REM [2026-09-13] Repeat-mistake scan. Every class here is one I actually shipped
REM   in this session: control chars eaten from backslash paths (5x in one day),
REM   an `or` falsy trap I re-introduced 7 weeks after fixing 10 of them, and a
REM   test writing into the real 2_Logs SSOT. A skill did not stop any of them -
REM   I have to invoke a skill, and the failure mode is starting without invoking
REM   anything. Detection beats intent, so it runs nightly whether I remember or not.
echo [6/7] craft_scan >> "%LOG%"
"%PY%" tools\craft_scan.py >> "%LOG%" 2>&1
set "RC6=%ERRORLEVEL%"

echo [END] rc0=%RC0% rc1=%RC1% rc2=%RC2% rc3=%RC3% rc4=%RC4% rc5=%RC5% rc6=%RC6% %DATE% %TIME% >> "%LOG%"
REM A non-zero rc from either check is a real FAIL. Do not swallow it.
REM Any non-zero rc is a real FAIL. Do not swallow it.
if not "%RC0%"=="0" endlocal & exit /b %RC0%
if not "%RC1%"=="0" endlocal & exit /b %RC1%
if not "%RC2%"=="0" endlocal & exit /b %RC2%
if not "%RC3%"=="0" endlocal & exit /b %RC3%
if not "%RC4%"=="0" endlocal & exit /b %RC4%
if not "%RC5%"=="0" endlocal & exit /b %RC5%
endlocal & exit /b %RC6%
