@echo off
REM KOSPI_MCAP_QUARTERLY_V2 daily operation (exec plan 4-2, 2026-09-19).
REM   usage: run_v2_daily_ops.bat evening|morning|afternoon
REM   evening 20:20 / morning 10:00 / afternoon 15:25, Mon-Fri. Holidays end as STANDBY inside python.
REM   Orders are sent only if config\daily_ops_v1.json auto_submit=true (user approval item). Default false = shadow.
REM   rc: 0 OK/STANDBY, 3 STOP (python already sent the alert), other = crash -> task_fail_alert here.
REM   Not using run_tool_with_alert.bat: it maps rc=3 to 0, which would hide STOP from the scheduler.
setlocal
set "JOB=%~1"
if "%JOB%"=="" (
  echo [USAGE] run_v2_daily_ops.bat evening^|morning^|afternoon
  exit /b 2
)
set PYTHONIOENCODING=utf-8
set ROOT=E:\1_Data
set PY=%ROOT%\_runtime\python312-embed\python.exe
set STATE=%ROOT%\paper\strategies\kospi_mcap_quarterly_v2\data\state
set LOG=%ROOT%\2_Logs\v2_daily_%JOB%_last.txt
cd /d %ROOT%

"%PY%" -m paper.strategies.kospi_mcap_quarterly_v2.src.daily_ops %JOB% --state-dir "%STATE%" > "%LOG%" 2>&1
set RC=%ERRORLEVEL%
type "%LOG%"
if "%RC%"=="0" exit /b 0
if "%RC%"=="3" exit /b 3
"%PY%" "%ROOT%\tools\task_fail_alert.py" "V2_daily_%JOB%" "%RC%" "%LOG%" >nul 2>nul
exit /b %RC%
