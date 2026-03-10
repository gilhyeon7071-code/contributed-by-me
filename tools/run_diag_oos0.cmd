@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM run_diag_oos0.cmd
REM - Resolve python runtime explicitly (fail-closed)
REM - Run diag script and write runlog
REM - Only print report path when exit code is 0

cd /d E:\1_Data || exit /b 1

if not exist _diag mkdir _diag

set "PY="
if exist "E:\1_Data\.venv\Scripts\python.exe" set "PY=E:\1_Data\.venv\Scripts\python.exe"
if not defined PY if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY if exist "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe" set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY (
  where python >nul 2>nul && set "PY=python"
)
if not defined PY (
  echo [ERROR] python runtime not found
  exit /b 9009
)

set "RUNLOG=E:\1_Data\_diag\oos0_diag_runlog.txt"
"%PY%" tools\diag_oos0.py > "%RUNLOG%" 2>&1
set "ERR=%ERRORLEVEL%"

echo.
echo [EXIT_CODE]=%ERR%
echo [RUNLOG]=%RUNLOG%
echo.

if not exist "%RUNLOG%" (
  echo [WARN] %RUNLOG% not found
  exit /b %ERR%
)

if not "%ERR%"=="0" (
  set "REPORT="
  set /p REPORT=< "%RUNLOG%"
  echo [RUNLOG_HEAD]=!REPORT!
  exit /b %ERR%
)

set "REPORT="
set /p REPORT=< "%RUNLOG%"
if not defined REPORT (
  echo [WARN] report path line missing in runlog
  exit /b %ERR%
)

echo [REPORT_PATH]=!REPORT!
if exist "!REPORT!" (
  echo.
  echo Open report:
  echo notepad "!REPORT!"
) else (
  echo [WARN] report path does not exist: !REPORT!
)

exit /b %ERR%
