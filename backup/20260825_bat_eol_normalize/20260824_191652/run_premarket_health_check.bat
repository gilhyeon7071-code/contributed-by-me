@echo off
setlocal EnableExtensions

set "ROOT=%~dp0"
pushd "%ROOT%" || exit /b 2

if not exist "%ROOT%2_Logs" mkdir "%ROOT%2_Logs" >nul 2>nul
set "LOG=%ROOT%2_Logs\run_premarket_health_last.txt"

set "PY="
if exist "%ROOT%_runtime\python312-embed\python.exe" set "PY=%ROOT%_runtime\python312-embed\python.exe"
if not defined PY if exist "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe" set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY if exist "%ROOT%.venv\Scripts\python.exe" set "PY=%ROOT%.venv\Scripts\python.exe"
if not defined PY if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY (
  where python >nul 2>nul && set "PY=python"
)
if defined PY (
  "%PY%" -V >nul 2>nul
  if errorlevel 1 set "PY="
)
if not defined PY (
  where py >nul 2>nul && set "PY=py -3"
)
if defined PY (
  "%PY%" -V >nul 2>nul
  if errorlevel 1 set "PY="
)
if not defined PY (
  echo [FAILED] python runtime not found > "%LOG%"
  popd
  endlocal & exit /b 9009
)

echo [START] %DATE% %TIME% > "%LOG%"
echo PY=%PY% >> "%LOG%"
"%PY%" "%ROOT%tools\build_premarket_health_check.py" %* >> "%LOG%" 2>&1
set "RC=%ERRORLEVEL%"
echo [END] rc=%RC% %DATE% %TIME% >> "%LOG%"

popd
endlocal & exit /b %RC%
