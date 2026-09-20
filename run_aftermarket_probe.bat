@echo off
REM [2026-09-12] KRX aftermarket (opens 2026-09-14, 16:00-20:00) first-day probe.
REM   Arg1 = label:      T1 = right after regular close / T2 = after aftermarket close
REM                      T3_FLOW = next-day re-snapshot of the flow ledger
REM   Arg2 = flow date:  YYYYMMDD, optional. Defaults to today inside the script.
REM                      T3 must pass 20260914 explicitly, otherwise it would
REM                      snapshot its own day and answer nothing.
REM   Compares accumulated volume/value at the two times to see whether the
REM   daily bar definition changes. If it does, value(close x volume) gate
REM   thresholds change meaning from that day.
REM   Why ASCII-only comments here: cmd reads .bat with the console codepage
REM   (949). Korean written as UTF-8 got mis-parsed as commands on 2026-09-12.
REM   Rationale lives in tools\aftermarket_probe.py docstring and PLANS (367)(369),
REM   docs\references\AFTERMARKET_20260914_OBSERVATION.md
setlocal EnableExtensions
cd /d E:\1_Data

set "PY=E:\1_Data\_runtime\python312-embed\python.exe"
if not exist "%PY%" set "PY=python"

set "LABEL=%~1"
if "%LABEL%"=="" set "LABEL=AUTO"

set "FLOWARG="
if not "%~2"=="" set "FLOWARG=--flow-date %~2"

set "LOG=E:\1_Data\2_Logs\aftermarket_probe_last.txt"
echo [START] %DATE% %TIME% label=%LABEL% flow=%~2 >> "%LOG%"
"%PY%" tools\aftermarket_probe.py --label %LABEL% %FLOWARG% >> "%LOG%" 2>&1
set "RC=%ERRORLEVEL%"
echo [END] rc=%RC% %DATE% %TIME% >> "%LOG%"
endlocal & exit /b %RC%
