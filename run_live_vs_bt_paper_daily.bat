@echo off
setlocal EnableExtensions
cd /d %~dp0

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

"%PY%" live_vs_bt_paper_daily.py
set EC=%ERRORLEVEL%
if NOT "%EC%"=="0" (
  echo [FAILED] live_vs_bt_paper_daily.py ERRORLEVEL=%EC%
  exit /b %EC%
)

echo [OK] finished
endlocal

