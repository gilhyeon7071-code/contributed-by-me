@echo off
setlocal EnableExtensions
cd /d %~dp0

set "PY="
if exist "%~dp0.venv\Scripts\python.exe" set "PY=%~dp0.venv\Scripts\python.exe"
if not defined PY if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY if exist "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe" set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY set "PY=python"

set "BTPANEL_USE_CORE6=%BTPANEL_USE_CORE6%"
if "%BTPANEL_USE_CORE6%"=="" set "BTPANEL_USE_CORE6=1"

echo [BTPANEL] PY=%PY%
if "%BTPANEL_USE_CORE6%"=="1" (
  "%PY%" "%~dp0tools\build_backtest_symbol_panel_csv.py" --use-core6 %*
) else (
  "%PY%" "%~dp0tools\build_backtest_symbol_panel_csv.py" %*
)
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" (
  where py >nul 2>nul
  if not errorlevel 1 (
    echo [BTPANEL] fallback=py -3
    if "%BTPANEL_USE_CORE6%"=="1" (
      py -3 "%~dp0tools\build_backtest_symbol_panel_csv.py" --use-core6 %*
    ) else (
      py -3 "%~dp0tools\build_backtest_symbol_panel_csv.py" %*
    )
    set "RC=%ERRORLEVEL%"
  )
)
echo [BTPANEL] exit=%RC%
exit /b %RC%
