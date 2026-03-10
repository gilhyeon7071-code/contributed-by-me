@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "ROOT=E:\1_Data"
set "OUTPUT_DIR=%ROOT%\checkfile\outputs"

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
set "PERF_GATE_MODE=%VERIFY_PERFORMANCE_GATE_MODE%"
if "%PERF_GATE_MODE%"=="" set "PERF_GATE_MODE=AUTO"
set "INTEGRITY_GATE=%VERIFY_INTEGRITY_GATE%"
if "%INTEGRITY_GATE%"=="" set "INTEGRITY_GATE=1"
set "INTEGRITY_GATE_MODE=%VERIFY_INTEGRITY_GATE_MODE%"
if "%INTEGRITY_GATE_MODE%"=="" set "INTEGRITY_GATE_MODE=STRICT"

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

if not exist "%OUTPUT_DIR%" mkdir "%OUTPUT_DIR%"

set "MAX_RECOVERY=%VERIFY_MAX_RECOVERY%"
if "%MAX_RECOVERY%"=="" set "MAX_RECOVERY=1"
set "RECOVERY_ATTEMPT=0"

cd /d "%ROOT%"

:run_cycle
echo [INFO] Using Python: %PY%
echo [INFO] Verification cycle: !RECOVERY_ATTEMPT!/%MAX_RECOVERY%

echo [INFO] Building design evidence...
%PY% -m checkfile.build_design_evidence --out "%DESIGN_EVIDENCE%" --template "%DESIGN_TEMPLATE%" --dashboard-state "%DASHBOARD_STATE%" --external "%DESIGN_EVIDENCE_EXTERNAL%"
if errorlevel 1 (
  echo [ERROR] design evidence build failed. stop.
  set "RC=%ERRORLEVEL%"
  goto :end
)

echo [INFO] Building runtime evidence... profile=%PROFILE%
if /I "%PROFILE%"=="DEMO" (
  %PY% -m checkfile.build_runtime_evidence --profile DEMO --demo-source "%RUNTIME_DEMO%" --performance-gate-mode "%PERF_GATE_MODE%" --out "%RUNTIME_LATEST%"
) else (
  %PY% -m checkfile.build_runtime_evidence --profile PROD --prod-base "%RUNTIME_PROD_BASE%" --policy "%SURV_POLICY%" --universe-csv "%UNIVERSE_CSV%" --supplement-csv "%SUPPLEMENT_CSV%" --performance-gate-mode "%PERF_GATE_MODE%" --out "%RUNTIME_LATEST%"
)

if errorlevel 1 (
  echo [ERROR] runtime evidence build failed. stop.
  set "RC=%ERRORLEVEL%"
  goto :end
)

set "RUNTIME_EVIDENCE=%RUNTIME_LATEST%"

echo [INFO] Running full verification...
echo [INFO] Runtime evidence: %RUNTIME_EVIDENCE%

%PY% -m checkfile.main ^
  --dashboard-state "%DASHBOARD_STATE%" ^
  --pending-status "%PENDING_STATUS%" ^
  --design-evidence "%DESIGN_EVIDENCE%" ^
  --runtime-evidence "%RUNTIME_EVIDENCE%" ^
  --report all ^
  --output "%OUTPUT_DIR%" ^
  --quiet ^
  %*

set "RC=%ERRORLEVEL%"

call :run_integrity_gate
set "IG_RC=%ERRORLEVEL%"
if "%IG_RC%"=="42" set "RC=42"
if "%IG_RC%"=="41" if "%RC%"=="0" set "RC=41"

if "%RC%"=="0" goto :open_html
if "%RC%"=="41" goto :open_html
if "%RC%"=="42" goto :open_html
if !RECOVERY_ATTEMPT! GEQ %MAX_RECOVERY% goto :open_html

call :detect_recoverable
if "%RECOVERABLE%"=="1" (
  set /a RECOVERY_ATTEMPT+=1
  echo [INFO] Self-recovery triggered. retrying...
  goto :run_cycle
)

goto :open_html

:detect_recoverable
set "RECOVERABLE=0"
for /f "usebackq delims=" %%V in (`powershell -NoProfile -Command "$latest = Get-ChildItem -Path \"%OUTPUT_DIR%\" -Filter \"verification_report_*.json\" | Sort-Object LastWriteTime -Descending | Select-Object -First 1; if (-not $latest) { '0'; exit }; try { $j = Get-Content -Raw -Encoding utf8 $latest.FullName | ConvertFrom-Json } catch { '0'; exit }; if (([int]$j.summary.failed -eq 0) -and ([int]$j.summary.skipped -gt 0)) { '1' } else { '0' }"`) do set "RECOVERABLE=%%V"
exit /b 0

:run_integrity_gate
if not "%INTEGRITY_GATE%"=="1" (
  echo [INFO] Integrity gate disabled.
  exit /b 0
)

echo [INFO] Building integrity overlay...
cmd /c "%ROOT%\run_integrity_overlay_report.bat"
if errorlevel 1 (
  echo [ERROR] integrity overlay build failed.
  exit /b 41
)

echo [INFO] Running integrity gate... mode=%INTEGRITY_GATE_MODE%
if /I "%INTEGRITY_GATE_MODE%"=="WARN" (
  cmd /c "%ROOT%\run_integrity_gate.bat" --warn-only
  if errorlevel 1 (
    echo [WARN] integrity gate runner returned non-zero in WARN mode.
  )
  exit /b 0
)

cmd /c "%ROOT%\run_integrity_gate.bat"
if errorlevel 1 (
  echo [ERROR] integrity gate failed.
  exit /b 42
)

exit /b 0

:open_html
set "LATEST_HTML="
for /f "delims=" %%F in ('dir /b /a-d /o-d "%OUTPUT_DIR%\verification_report_*.html" 2^>nul') do (
  set "LATEST_HTML=%OUTPUT_DIR%\%%F"
  goto :open_html_found
)

goto :end

:open_html_found
echo [INFO] Opening report: !LATEST_HTML!
start "" "!LATEST_HTML!"

goto :end

:probe_python
%PY% -V >nul 2>nul
if errorlevel 1 set "PY="
exit /b 0

:end
echo [INFO] Exit code: %RC%
exit /b %RC%

