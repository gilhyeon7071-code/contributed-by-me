@echo off
setlocal EnableExtensions

set "ROOT=%~dp0"
cd /d "%ROOT%"

set "PY="
if exist "%ROOT%.venv\Scripts\python.exe" set "PY=%ROOT%.venv\Scripts\python.exe"
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

set "RUN_REPORT=%BACKTEST_RUN_REPORT%"
if "%RUN_REPORT%"=="" set "RUN_REPORT=1"

set "RUN_SENSI=%BACKTEST_RUN_SENSITIVITY%"
if "%RUN_SENSI%"=="" set "RUN_SENSI=1"

set "RUN_OOS_DIAG=%BACKTEST_RUN_OOS_DIAG%"
if "%RUN_OOS_DIAG%"=="" set "RUN_OOS_DIAG=1"

set "STRICT=%BACKTEST_STRICT%"
if "%STRICT%"=="" set "STRICT=1"

set "RC=0"

echo [BT] PY=%PY%
echo [BT] RUN_REPORT=%RUN_REPORT% RUN_SENSITIVITY=%RUN_SENSI% RUN_OOS_DIAG=%RUN_OOS_DIAG% STRICT=%STRICT%

if "%RUN_REPORT%"=="1" (
  if exist "%ROOT%report_backtest_v41_1.py" (
    echo [BT] running report_backtest_v41_1.py
    "%PY%" "%ROOT%report_backtest_v41_1.py"
    if errorlevel 1 (
      echo [BT][ERR] report_backtest_v41_1.py failed
      set "RC=21"
      if "%STRICT%"=="1" goto :end
    )
  ) else (
    echo [BT][WARN] missing report_backtest_v41_1.py
    if "%STRICT%"=="1" (
      set "RC=22"
      goto :end
    )
  )
)

if "%RUN_SENSI%"=="1" (
  if exist "%ROOT%sensitivity_report_v41_1.py" (
    echo [BT] running sensitivity_report_v41_1.py
    "%PY%" "%ROOT%sensitivity_report_v41_1.py"
    if errorlevel 1 (
      echo [BT][ERR] sensitivity_report_v41_1.py failed
      set "RC=31"
      if "%STRICT%"=="1" goto :end
    )
  ) else (
    echo [BT][WARN] missing sensitivity_report_v41_1.py
    if "%STRICT%"=="1" (
      set "RC=32"
      goto :end
    )
  )
)

if "%RUN_OOS_DIAG%"=="1" (
  if exist "%ROOT%tools\diag_oos0.py" (
    echo [BT] running tools\diag_oos0.py
    "%PY%" "%ROOT%tools\diag_oos0.py"
    if errorlevel 1 (
      echo [BT][ERR] tools\diag_oos0.py failed
      set "RC=41"
      if "%STRICT%"=="1" goto :end
    )
  ) else (
    echo [BT][WARN] missing tools\diag_oos0.py
    if "%STRICT%"=="1" (
      set "RC=42"
      goto :end
    )
  )
)

:end
echo [BT] exit=%RC%
exit /b %RC%
