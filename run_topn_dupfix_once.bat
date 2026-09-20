@echo off
REM [2026-09-08 ONE-SHOT] Correct the duplicate-dispatch overage. See PLANS (245).
REM   Sells only the excess over the intended qty, re-checking the broker at run time.
REM   Delete this file and the VIBE_TopN_DupFix_Once task once it has done its job.
REM   ASCII comments only (cmd emits stderr on Hangul REM).
setlocal EnableExtensions
set "ROOT=%~dp0"
if "%PYTHONIOENCODING%"=="" set "PYTHONIOENCODING=utf-8"
set "LOG=%ROOT%2_Logs\topn\dup_position_fix_20260908.log"
echo [START] %DATE% %TIME% >> "%LOG%"
"%ROOT%_runtime\python312-embed\python.exe" "%ROOT%tools\topn_correct_dup_position.py" --apply --mock true >> "%LOG%" 2>&1
set "RC=%ERRORLEVEL%"
echo [END] rc=%RC% %DATE% %TIME% >> "%LOG%"
endlocal & exit /b %RC%
