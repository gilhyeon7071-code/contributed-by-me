@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM Determine ROOT as parent folder of this script's directory (...\tools -> ...\)
for %%I in ("%~dp0..") do set "ROOT=%%~fI"

if not exist "%ROOT%\_diag" mkdir "%ROOT%\_diag"

set "TS="
for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TS=%%I"
if not defined TS set "TS=manual"

set "OUT=%ROOT%\_diag\pf_calc_grep_py.txt"
set "RUNLOG=%ROOT%\_diag\pf_calc_grep_runlog_%TS%.txt"

set "PY="
if exist "%ROOT%\.venv\Scripts\python.exe" set "PY=%ROOT%\.venv\Scripts\python.exe"
if not defined PY if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY if exist "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe" set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY (
  where python >nul 2>nul && set "PY=python"
)
if not defined PY (
  echo EXIT_CODE=9009
  echo OUT_FILE=%OUT%
  echo RUNLOG=%RUNLOG%
  echo [ERROR] python runtime not found
  exit /b 9009
)

pushd "%ROOT%" >nul
"%PY%" "%ROOT%\tools\pf_calc_grep.py" --root "%ROOT%" > "%RUNLOG%" 2>&1
set "EC=%ERRORLEVEL%"
popd >nul

echo EXIT_CODE=!EC!
echo OUT_FILE=%OUT%
echo RUNLOG=%RUNLOG%

if not "!EC!"=="0" (
  if exist "%RUNLOG%" (
    set "HEAD="
    set /p HEAD=< "%RUNLOG%"
    echo RUNLOG_HEAD=!HEAD!
  )
  exit /b !EC!
)

if exist "%OUT%" (
  for %%A in ("%OUT%") do echo OUT_SIZE=%%~zA bytes
) else (
  echo OUT_MISSING
)

exit /b !EC!
