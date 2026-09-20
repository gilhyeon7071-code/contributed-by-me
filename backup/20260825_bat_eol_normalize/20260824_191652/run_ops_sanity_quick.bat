@echo off
REM run_ops_sanity_quick.bat - RootA quick read-only sanity run
setlocal EnableExtensions EnableDelayedExpansion

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

for /f "delims=" %%T in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TS=%%T"
set "RUN_LOG=%LOG_DIR%\ops_sanity_quick_%TS%.log"
set "SUMMARY_TS=%LOG_DIR%\ops_sanity_quick_%TS%.json"
set "SUMMARY_LATEST=%LOG_DIR%\ops_sanity_quick_latest.json"

echo [START] ops_sanity_quick ts=%TS% root=%ROOT% > "%RUN_LOG%"
echo [INFO] PY=%PY% >> "%RUN_LOG%"

echo [1/5] gateway health snapshot
call "%ROOT%run_gateway_health_snapshot.bat" >> "%RUN_LOG%" 2>&1
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" (
  call :write_summary "FAIL" "gateway_health_script_failed" "%RC%" ""
  echo [HARD_FAIL] gateway health snapshot rc=%RC%
  popd
  endlocal & exit /b 3
)
powershell -NoProfile -ExecutionPolicy Bypass -Command "$files=Get-ChildItem -Path '%LOG_DIR%' -Filter 'gateway_health_*_latest.json' -ErrorAction SilentlyContinue; if(-not $files){exit 4}; $ok=0; foreach($f in $files){try{$j=Get-Content -Raw -Encoding UTF8 $f.FullName | ConvertFrom-Json; if($j.ok -eq $true){$ok=1}}catch{}}; if($ok -eq 1){exit 0}; exit 4" >> "%RUN_LOG%" 2>&1
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" (
  call :write_summary "FAIL" "gateway_health_no_ok_latest" "%RC%" ""
  echo [HARD_FAIL] no gateway latest json has ok=true
  popd
  endlocal & exit /b 4
)

echo [2/5] ssot health card
call "%ROOT%run_ssot_health_card.bat" >> "%RUN_LOG%" 2>&1
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" (
  call :write_summary "FAIL" "ssot_health_card_failed" "%RC%" ""
  echo [HARD_FAIL] ssot health card rc=%RC%
  popd
  endlocal & exit /b 5
)

echo [3/5] fills D summary
set "D="
"%PY%" "%ROOT%tools\derive_d_from_fills.py" --csv "%ROOT%paper\fills.csv" > "%LOG_DIR%\ops_sanity_d.tmp" 2>> "%RUN_LOG%"
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" (
  call :write_summary "FAIL" "derive_D_from_fills_script_failed" "%RC%" ""
  echo [HARD_FAIL] derive D script failed rc=%RC%
  popd
  endlocal & exit /b 6
)
set /p D=<"%LOG_DIR%\ops_sanity_d.tmp"
del "%LOG_DIR%\ops_sanity_d.tmp" >nul 2>nul
if not defined D (
  call :write_summary "FAIL" "cannot_derive_D_from_fills" "6" ""
  echo [HARD_FAIL] cannot derive D from paper\fills.csv
  popd
  endlocal & exit /b 6
)
powershell -NoProfile -ExecutionPolicy Bypass -Command "$d='%D%'; $p='%ROOT%paper\fills.csv'; $n=0; if(Test-Path $p){$rows=Import-Csv -Path $p -Encoding UTF8; foreach($r in $rows){$raw=[string]$r.datetime; $s=$raw -replace '[^0-9]',''; if($s.Length -ge 8 -and $s.Substring(0,8) -eq $d){$n++}}}; $o=[ordered]@{status=$(if($n -gt 0){'PASS'}else{'FAIL'}); D=$d; source_fills=$p; rows_for_D=$n; generated_at_utc=(Get-Date).ToUniversalTime().ToString('s')+'Z'}; $o | ConvertTo-Json -Depth 4 | Set-Content -Path '%LOG_DIR%\ops_sanity_fills_latest.json' -Encoding UTF8; if($n -gt 0){exit 0}else{exit 6}" >> "%RUN_LOG%" 2>&1
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" (
  call :write_summary "FAIL" "fills_rows_for_D_zero" "%RC%" "%D%"
  echo [HARD_FAIL] fills rows for D are zero D=%D%
  popd
  endlocal & exit /b 6
)

echo [4/5] fill idempotency smoke
"%PY%" "%ROOT%tools\fill_idempotency_smoke.py" > "%LOG_DIR%\fill_idempotency_smoke_latest.json" 2>> "%RUN_LOG%"
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" (
  call :write_summary "FAIL" "fill_idempotency_smoke_failed" "%RC%" "%D%"
  echo [HARD_FAIL] fill idempotency smoke rc=%RC%
  popd
  endlocal & exit /b 7
)

echo [5/5] canonical replay compare D=%D%
call "%ROOT%run_canonical_fills_shadow.bat" --date "%D%" >> "%RUN_LOG%" 2>&1
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" (
  call :write_summary "FAIL" "canonical_fills_shadow_failed" "%RC%" "%D%"
  echo [HARD_FAIL] canonical fills shadow rc=%RC%
  popd
  endlocal & exit /b 9
)

call "%ROOT%run_canonical_replay_compare.bat" --date "%D%" >> "%RUN_LOG%" 2>&1
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" (
  call :write_summary "FAIL" "canonical_replay_compare_failed" "%RC%" "%D%"
  echo [HARD_FAIL] canonical replay compare rc=%RC%
  popd
  endlocal & exit /b 10
)

call :write_summary "PASS" "all_checks_passed" "0" "%D%"
echo [OK] ops sanity quick PASS D=%D%
echo [EVIDENCE] %SUMMARY_LATEST%
echo [EVIDENCE] %RUN_LOG%
popd
endlocal & exit /b 0

:write_summary
set "SUMMARY_STATUS=%~1"
set "SUMMARY_REASON=%~2"
set "SUMMARY_RC=%~3"
set "SUMMARY_D=%~4"
powershell -NoProfile -ExecutionPolicy Bypass -Command "$o=[ordered]@{status='%SUMMARY_STATUS%'; reason='%SUMMARY_REASON%'; exit_code=[int]'%SUMMARY_RC%'; D='%SUMMARY_D%'; generated_at_utc=(Get-Date).ToUniversalTime().ToString('s')+'Z'; root='%ROOT%'; run_log='%RUN_LOG%'; evidence=[ordered]@{gateway_latest='%LOG_DIR%\gateway_health_*_latest.json'; ssot_health_json='%LOG_DIR%\ssot_health_card_latest.json'; ssot_health_text='%LOG_DIR%\ssot_health_card_latest.txt'; fills_summary='%LOG_DIR%\ops_sanity_fills_latest.json'; idempotency_smoke='%LOG_DIR%\fill_idempotency_smoke_latest.json'; replay_compare='%LOG_DIR%\canonical_replay_compare_latest.json'}}; $json=$o | ConvertTo-Json -Depth 5; $json | Set-Content -Path '%SUMMARY_TS%' -Encoding UTF8; $json | Set-Content -Path '%SUMMARY_LATEST%' -Encoding UTF8" >> "%RUN_LOG%" 2>&1
exit /b 0
