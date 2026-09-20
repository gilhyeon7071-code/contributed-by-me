@echo off
setlocal EnableExtensions
cd /d %~dp0
if "%PAPER_OPER_START_YMD%"=="" set "PAPER_OPER_START_YMD=20260301"
if "%LVB_MIN_STABLE_SCORE%"=="" set "LVB_MIN_STABLE_SCORE=-20"

set "PY="
if exist "%~dp0.venv\Scripts\python.exe" set "PY=%~dp0.venv\Scripts\python.exe"
if not defined PY if exist "E:\1_Data\_runtime\python312-embed\python.exe" set "PY=E:\1_Data\_runtime\python312-embed\python.exe"
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

echo [SCOPE] PAPER_OPER_START_YMD=%PAPER_OPER_START_YMD% LVB_MIN_STABLE_SCORE=%LVB_MIN_STABLE_SCORE%
"%PY%" live_vs_bt_paper_daily.py --align-window-trades 30 --min-shared-trades 10 --max-backtest-age-days 7 --min-oos-trades 20 --min-oos-pf 0.75 --min-stable-score %LVB_MIN_STABLE_SCORE%
set EC=%ERRORLEVEL%
if NOT "%EC%"=="0" (
  echo [FAILED] live_vs_bt_paper_daily.py ERRORLEVEL=%EC%
  exit /b %EC%
)

echo [OK] finished
endlocal



