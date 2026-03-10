@echo off
setlocal EnableExtensions EnableDelayedExpansion

if "%~1"=="" (
  echo Usage: %~nx0 PLANNING^|DESIGN^|DATA^|STRATEGY^|EXECUTION^|RISK^|TESTING^|OPERATIONS
  exit /b 2
)

set "PHASE=%~1"
set "ROOT=E:\1_Data"

set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
call :probe_python
if not defined PY (
  set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
  call :probe_python
)
if not defined PY (
  set "PY=python"
  call :probe_python
)
if not defined PY (
  set "PY=py -3"
  call :probe_python
)
if not defined PY (
  echo [ERROR] No runnable Python found.
  exit /b 9009
)

set "PROFILE=%VERIFY_PROFILE%"
if "%PROFILE%"=="" set "PROFILE=PROD"

set "RUNTIME_LATEST=%ROOT%\2_Logs\verification_runtime_evidence_latest.json"
set "RUNTIME_DEMO=%ROOT%\checkfile\runtime_evidence_demo.json"
set "RUNTIME_PROD_BASE=%ROOT%\checkfile\runtime_evidence_prod_base.json"
set "SURV_POLICY=%ROOT%\checkfile\survivorship_policy_kr.json"
set "SUPPLEMENT_CSV=%VERIFY_SUPPLEMENT_CSV%"
if "%SUPPLEMENT_CSV%"=="" set "SUPPLEMENT_CSV=%ROOT%\_cache\survivorship_delisted_seed.csv"
set "UNIVERSE_CSV=%VERIFY_UNIVERSE_CSV%"
if "%UNIVERSE_CSV%"=="" (
  if exist "%ROOT%\_cache\sector_ssot_plus_pref.csv" (
    set "UNIVERSE_CSV=%ROOT%\_cache\sector_ssot_plus_pref.csv"
  ) else (
    set "UNIVERSE_CSV=%ROOT%\2_Logs\candidates_latest_data.csv"
  )
)

set "DASHBOARD_STATE=E:\vibe\buffett\runs\dashboard_state_latest.json"
set "PENDING_STATUS=%ROOT%\2_Logs\pending_entry_status_latest.json"
set "DESIGN_EVIDENCE=%ROOT%\2_Logs\design_evidence_latest.json"
set "DESIGN_TEMPLATE=%ROOT%\checkfile\design_evidence_template.json"
set "DESIGN_EVIDENCE_EXTERNAL=%VERIFY_DESIGN_EVIDENCE_EXTERNAL%"
if "%DESIGN_EVIDENCE_EXTERNAL%"=="" set "DESIGN_EVIDENCE_EXTERNAL=%ROOT%\2_Logs\design_evidence_external_latest.json"

cd /d "%ROOT%"

echo [INFO] Using Python: %PY%
echo [INFO] Building design evidence...
%PY% -m checkfile.build_design_evidence --out "%DESIGN_EVIDENCE%" --template "%DESIGN_TEMPLATE%" --dashboard-state "%DASHBOARD_STATE%" --external "%DESIGN_EVIDENCE_EXTERNAL%"
if errorlevel 1 (
  echo [ERROR] design evidence build failed. stop.
  exit /b %errorlevel%
)

echo [INFO] Building runtime evidence... profile=%PROFILE%
if /I "%PROFILE%"=="DEMO" (
  %PY% -m checkfile.build_runtime_evidence --profile DEMO --demo-source "%RUNTIME_DEMO%" --out "%RUNTIME_LATEST%"
) else (
  %PY% -m checkfile.build_runtime_evidence --profile PROD --prod-base "%RUNTIME_PROD_BASE%" --policy "%SURV_POLICY%" --universe-csv "%UNIVERSE_CSV%" --supplement-csv "%SUPPLEMENT_CSV%" --out "%RUNTIME_LATEST%"
)

if errorlevel 1 (
  echo [ERROR] runtime evidence build failed. stop.
  exit /b %errorlevel%
)

echo [INFO] Running phase: %PHASE%
echo [INFO] Runtime evidence: %RUNTIME_LATEST%

%PY% -m checkfile.main --phase "%PHASE%" --dashboard-state "%DASHBOARD_STATE%" --pending-status "%PENDING_STATUS%" --design-evidence "%DESIGN_EVIDENCE%" --runtime-evidence "%RUNTIME_LATEST%" --quiet
set "RC=%ERRORLEVEL%"

echo [INFO] Exit code: %RC%
exit /b %RC%

:probe_python
%PY% -V >nul 2>nul
if errorlevel 1 set "PY="
exit /b 0
