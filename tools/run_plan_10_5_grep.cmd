@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM run_plan_10_5_grep.cmd
REM - Resolve python runtime explicitly (fail-closed)
REM - Run plan_10_5_grep.py and write a runlog under _diag

cd /d E:\1_Data || exit /b 1

if not exist _diag mkdir _diag

set "TS="
for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TS=%%I"
if not defined TS set "TS=manual"

set "OUT_FILE=E:\1_Data\_diag\plan_10_5_grep.txt"
set "RUNLOG=E:\1_Data\_diag\plan_10_5_grep_runlog_%TS%.txt"

set "PY="
if exist "E:\1_Data\.venv\Scripts\python.exe" set "PY=E:\1_Data\.venv\Scripts\python.exe"
if not defined PY if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY if exist "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe" set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY (
  where python >nul 2>nul && set "PY=python"
)
if not defined PY (
  echo EXIT_CODE=9009
  echo OUT_FILE=%OUT_FILE%
  echo RUNLOG=%RUNLOG%
  echo [ERROR] python runtime not found
  exit /b 9009
)

echo [RUN] "%PY%" tools\plan_10_5_grep.py > "%RUNLOG%"
"%PY%" tools\plan_10_5_grep.py >> "%RUNLOG%" 2>&1

set "EC=%ERRORLEVEL%"
echo EXIT_CODE=%EC%>> "%RUNLOG%"
echo OUT_FILE=%OUT_FILE%>> "%RUNLOG%"

echo EXIT_CODE=%EC%
echo OUT_FILE=%OUT_FILE%
echo RUNLOG=%RUNLOG%

if not "%EC%"=="0" (
  if exist "%RUNLOG%" (
    set "HEAD="
    set /p HEAD=< "%RUNLOG%"
    echo RUNLOG_HEAD=!HEAD!
  )
  exit /b %EC%
)

if exist "%OUT_FILE%" (
  for %%F in ("%OUT_FILE%") do set "OUT_SIZE=%%~zF"
  echo OUT_SIZE=%OUT_SIZE% bytes>> "%RUNLOG%"
  echo OUT_SIZE=%OUT_SIZE% bytes
) else (
  echo OUT_MISSING>> "%RUNLOG%"
  echo OUT_MISSING
)

exit /b %EC%
