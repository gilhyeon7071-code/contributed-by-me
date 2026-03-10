@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM Usage:
REM   cleanup_1_data.cmd        -> DRY RUN (no moves)
REM   cleanup_1_data.cmd DOIT   -> actual move

cd /d "%~dp0"

set "PY="
if exist "%~dp0.venv\Scripts\python.exe" set "PY=%~dp0.venv\Scripts\python.exe"
if not defined PY if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY if exist "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe" set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY (
  where python >nul 2>nul && set "PY=python"
)
if defined PY (
  %PY% -V >nul 2>nul
  if errorlevel 1 set "PY="
)
if not defined PY (
  where py >nul 2>nul && set "PY=py -3"
)
if defined PY (
  %PY% -V >nul 2>nul
  if errorlevel 1 set "PY="
)
if not defined PY (
  echo [FAILED] python runtime not found
  exit /b 9009
)

set MODE=DRY
if /I "%~1"=="DOIT" set MODE=DOIT

"%PY%" "%~dp0cleanup_1_data.py" %MODE%
set ERR=%ERRORLEVEL%

if not "%ERR%"=="0" (
  echo [CLEANUP] FAILED. ERRORLEVEL=%ERR%
  exit /b %ERR%
)

echo [CLEANUP] OK.
exit /b 0

