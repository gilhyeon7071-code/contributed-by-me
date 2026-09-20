@echo off
setlocal EnableExtensions
cd /d %~dp0

set "PY="
if exist "%~dp0.venv\Scripts\python.exe" set "PY=%~dp0.venv\Scripts\python.exe"
if not defined PY if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY if exist "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe" set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY set "PY=python"

echo [RATE] PY=%PY%
"%PY%" "%~dp0tools\build_rate_series_seed_csv.py" %*
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" (
  where py >nul 2>nul
  if not errorlevel 1 (
    echo [RATE] fallback=py -3
    py -3 "%~dp0tools\build_rate_series_seed_csv.py" %*
    set "RC=%ERRORLEVEL%"
  )
)
echo [RATE] exit=%RC%
exit /b %RC%
