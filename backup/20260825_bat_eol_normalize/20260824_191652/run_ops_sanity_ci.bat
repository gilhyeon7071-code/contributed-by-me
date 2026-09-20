@echo off
REM run_ops_sanity_ci.bat - CI/scheduler wrapper for RootA quick sanity.
setlocal EnableExtensions

set "ROOT=%~dp0"
set "LOG_DIR=%ROOT%2_Logs"
pushd "%ROOT%" || exit /b 2
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

set "PY="
if exist "%ROOT%_runtime\python312-embed\python.exe" set "PY=%ROOT%_runtime\python312-embed\python.exe"
if not defined PY if exist "%ROOT%.venv\Scripts\python.exe" set "PY=%ROOT%.venv\Scripts\python.exe"
if not defined PY if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY if exist "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe" set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY (
  where python >nul 2>nul && set "PY=python"
)
if not defined PY (
  echo [HARD_FAIL] python runtime not found
  popd
  endlocal & exit /b 9009
)

call "%ROOT%run_ops_sanity_quick.bat"
set "RC=%ERRORLEVEL%"
if "%RC%"=="0" (
  echo [OK] ops sanity CI wrapper PASS
  popd
  endlocal & exit /b 0
)

echo [FAIL] ops sanity failed rc=%RC%; building incident report
"%PY%" "%ROOT%tools\build_ops_incident_report.py" ^
  --sanity-json "%LOG_DIR%\ops_sanity_quick_latest.json" ^
  --template "%ROOT%docs\references\ROOTA_INCIDENT_REPORT_TEMPLATE.md" ^
  --output-dir "%LOG_DIR%\incidents" ^
  --command "%ROOT%run_ops_sanity_quick.bat" ^
  --exit-code "%RC%"
set "IR_RC=%ERRORLEVEL%"
if not "%IR_RC%"=="0" (
  echo [WARN] incident report generation failed rc=%IR_RC%
)
popd
endlocal & exit /b %RC%
