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

set "BTCSV_USE_CORE6=%BTCSV_USE_CORE6%"
if "%BTCSV_USE_CORE6%"=="" set "BTCSV_USE_CORE6=1"

echo [BTCSV] PY=%PY%
if "%BTCSV_USE_CORE6%"=="1" (
  "%PY%" "%~dp0tools\build_backtest_market_csv.py" --use-core6 %*
) else (
  "%PY%" "%~dp0tools\build_backtest_market_csv.py" %*
)
set "RC=%ERRORLEVEL%"
echo [BTCSV] exit=%RC%
exit /b %RC%
