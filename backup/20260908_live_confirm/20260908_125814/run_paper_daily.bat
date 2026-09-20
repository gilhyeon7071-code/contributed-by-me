setlocal EnableExtensions
if /I "%~1"=="__run_daily_inner__" (
  set "RUN_DAILY_STDERR_CAPTURED=1"
  shift /1
  goto :RUN_DAILY_MAIN
)
if "%P0_DATA_HARD_LAG_DAYS%"=="" set "P0_DATA_HARD_LAG_DAYS=2"

REM ============================================================
REM run_paper_daily.bat  (RootA SSOT: E:\1_Data)
REM - Runs paper daily pipeline steps [1/9]..[9/9]
REM - Derives D from %ROOT%paper\fills.csv (latest BUY ymd else latest ymd)
REM - One-pass: orders(D) -> core -> ledger append
REM - Contract check: exec_date_unique must be D
REM - Post checks: paper_sync + pending_report JSON verdict
REM ============================================================

set "ROOT=%~dp0"
set "STDERR_LOG=%ROOT%2_Logs\run_paper_daily_last.stderr.txt"
set "STDOUT_LOG=%ROOT%2_Logs\run_paper_daily_last.stdout.txt"
if not defined RUN_DAILY_STDERR_CAPTURED (
  setlocal EnableDelayedExpansion
  set "STDOUT_TMP=%ROOT%2_Logs\run_paper_daily_last.stdout.tmp.txt"
  set "STDERR_TMP=%ROOT%2_Logs\run_paper_daily_last.stderr.tmp.txt"
  if not exist "%ROOT%2_Logs" mkdir "%ROOT%2_Logs" >nul 2>nul
  > "%STDOUT_LOG%" echo [START] run_paper_daily stdout capture ts=%DATE% %TIME%
  > "%STDERR_LOG%" echo [START] run_paper_daily stderr capture ts=%DATE% %TIME%
  > "%ROOT%2_Logs\run_paper_daily_wrapper_status.txt" echo [WRAPPER_START] ts=!DATE! !TIME!
  if exist "!STDOUT_TMP!" del /f /q "!STDOUT_TMP!" >nul 2>nul
  if exist "!STDERR_TMP!" del /f /q "!STDERR_TMP!" >nul 2>nul
  set "RUN_DAILY_STDERR_CAPTURED=1"
  call "%~f0" __run_daily_inner__ %* > "!STDOUT_TMP!" 2>> "!STDERR_TMP!"
  set "WRAP_RC=!ERRORLEVEL!"
  >> "%ROOT%2_Logs\run_paper_daily_wrapper_status.txt" echo [WRAPPER_EXIT] rc=!WRAP_RC! ts=!DATE! !TIME!
  if exist "%ROOT%2_Logs\run_paper_daily_last.txt" >> "%ROOT%2_Logs\run_paper_daily_last.txt" echo [WRAPPER_EXIT] rc=!WRAP_RC!
  if exist "!STDOUT_TMP!" (
    type "!STDOUT_TMP!" >> "%STDOUT_LOG%"
    del /f /q "!STDOUT_TMP!" >nul 2>nul
  )
  if exist "!STDERR_TMP!" (
    type "!STDERR_TMP!" >> "%STDERR_LOG%"
    del /f /q "!STDERR_TMP!" >nul 2>nul
  )
  REM ------------------------------------------------------------
  REM Per-run log archive (PLANS 2026-08-21 (31) / 2026-08-22 (45)).
  REM Lines :26 :27 :28 :148 truncate the four run logs on every run, so the
  REM previous run's evidence was destroyed. That is why the 8 trading days of
  REM 10-109s rc=0 deaths (08-11..08-20) could not be diagnosed afterwards.
  REM Copy only. Existing log paths and behavior are unchanged.
  REM ASCII only: cmd reads .bat using the OEM codepage; non-ASCII corrupts parsing.
  REM ------------------------------------------------------------
  set "RUN_ARCHIVE_DIR=%ROOT%2_Logs\run_paper_daily_archive"
  if not exist "!RUN_ARCHIVE_DIR!" mkdir "!RUN_ARCHIVE_DIR!" >nul 2>nul
  set "RUN_ARCHIVE_TS="
  for /f "usebackq delims=" %%T in (`powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"`) do set "RUN_ARCHIVE_TS=%%T"
  if not "!RUN_ARCHIVE_TS!"=="" (
    set "RUN_ARCHIVE_BASE=!RUN_ARCHIVE_DIR!\run_paper_daily_!RUN_ARCHIVE_TS!_rc!WRAP_RC!"
    if exist "%ROOT%2_Logs\run_paper_daily_last.txt" copy /y "%ROOT%2_Logs\run_paper_daily_last.txt" "!RUN_ARCHIVE_BASE!.step.txt" >nul 2>nul
    if exist "%STDOUT_LOG%" copy /y "%STDOUT_LOG%" "!RUN_ARCHIVE_BASE!.stdout.txt" >nul 2>nul
    if exist "%STDERR_LOG%" copy /y "%STDERR_LOG%" "!RUN_ARCHIVE_BASE!.stderr.txt" >nul 2>nul
    if exist "%ROOT%2_Logs\run_paper_daily_wrapper_status.txt" copy /y "%ROOT%2_Logs\run_paper_daily_wrapper_status.txt" "!RUN_ARCHIVE_BASE!.wrapper.txt" >nul 2>nul
  )
  REM Prune only files this feature created. Never touches pre-existing artifacts.
  if "%RUN_DAILY_ARCHIVE_KEEP_FILES%"=="" set "RUN_DAILY_ARCHIVE_KEEP_FILES=400"
  powershell -NoProfile -Command "$d='%ROOT%2_Logs\run_paper_daily_archive'; if (Test-Path $d) { Get-ChildItem -Path $d -Filter 'run_paper_daily_*' -File | Sort-Object Name -Descending | Select-Object -Skip %RUN_DAILY_ARCHIVE_KEEP_FILES% | Remove-Item -Force -ErrorAction SilentlyContinue }" >nul 2>nul
  if exist "%ROOT%2_Logs\run_paper_daily.lock" (
    if exist "%ROOT%2_Logs\run_paper_daily_last.txt" >> "%ROOT%2_Logs\run_paper_daily_last.txt" echo [WRAPPER_CLEANUP_SKIP] lock cleanup delegated to pid-aware stale check rc=!WRAP_RC!
  )
  if not "!WRAP_RC!"=="0" (
    endlocal & exit /b !WRAP_RC!
  )
  endlocal & exit /b 0
)
:RUN_DAILY_MAIN
pushd "%ROOT%" || (
  echo [FAILED] pushd failed: "%ROOT%"
  exit /b 2
)
if not exist "%ROOT%2_Logs" mkdir "%ROOT%2_Logs" >nul 2>nul
set "LAST_LOG=%ROOT%2_Logs\run_paper_daily_last.txt"
set "LOCK_DIR=%ROOT%2_Logs\run_paper_daily.lock"
set "LOCK_ACQUIRED="
if "%LOCK_STALE_MIN%"=="" set "LOCK_STALE_MIN=180"
if exist "%LOCK_DIR%" call :CHECK_AND_CLEAN_STALE_LOCK
2>nul mkdir "%LOCK_DIR%"
if errorlevel 1 (
  echo [FAILED] run_paper_daily already running or stale lock exists: "%LOCK_DIR%"
  popd
  endlocal & exit 9
)
set "LOCK_ACQUIRED=1"
> "%LOCK_DIR%\run.info" echo started=%DATE% %TIME%
for /f "usebackq delims=" %%P in (`powershell -NoProfile -Command "$self=Get-CimInstance Win32_Process -Filter ('ProcessId=' + $PID); if($self){ $self.ParentProcessId }"`) do set "LOCK_RUN_PID=%%P"
if not "%LOCK_RUN_PID%"=="" >> "%LOCK_DIR%\run.info" echo pid=%LOCK_RUN_PID%

if "%PAPER_OPER_START_YMD%"=="" set "PAPER_OPER_START_YMD=20260301"
if "%LVB_MIN_STABLE_SCORE%"=="" set "LVB_MIN_STABLE_SCORE=-20"
if "%KRX_MIN_UNI%"=="" set "KRX_MIN_UNI=1800"
if "%PYTHONIOENCODING%"=="" set "PYTHONIOENCODING=utf-8"
if "%SURGE_RT_PAPER_DATA_COLLECTION%"=="" set "SURGE_RT_PAPER_DATA_COLLECTION=1"
if "%SURGE_RT_PAPER_LIMIT_NEAR_IGNORE_ENTRY_CAPS%"=="" set "SURGE_RT_PAPER_LIMIT_NEAR_IGNORE_ENTRY_CAPS=1"
if "%SURGE_RT_PAPER_HIGH_REJECTION_ENTRY_BLOCK_PCT%"=="" set "SURGE_RT_PAPER_HIGH_REJECTION_ENTRY_BLOCK_PCT=0.025"
if "%SURGE_RT_PAPER_ALLOW_KRX_CAUTION%"=="" set "SURGE_RT_PAPER_ALLOW_KRX_CAUTION=1"
if "%POST_CHAIN_FAIL_SOFT%"=="" set "POST_CHAIN_FAIL_SOFT=0"
REM [2026-08-24] v41.1 new-entry stop. Step [7/9] paper_engine takes this path too.
REM   Basis: entry condition negative in 11.6y / all 12 years (.agent/PLANS.md 2026-08-24 (77)(84)).
REM   PAPER_EXIT_ONLY is the official switch at paper_engine.py:440/448/1255/1388;
REM   it blocks new entries only. Exit / stop-loss / trailing keep running as before.
REM   To revert, delete the single line below.
if "%PAPER_EXIT_ONLY%"=="" set "PAPER_EXIT_ONLY=1"
set "POST_CHAIN_FAILED=0"
set "POST_CHAIN_FAILED_STEP="
set "LAST_STEP_LABEL=[BOOT] init"
REM Python launcher (venv/absolute path first; fail-closed)
set "LAST_STEP_LABEL=[BOOT] python_resolve"
set "PY="
if not "%PY_FORCE%"=="" (
  set "LAST_STEP_LABEL=[BOOT] validate_py_force"
  call :VALIDATE_PY_FORCE "%PY_FORCE%"
  if errorlevel 1 goto :FAILED
  set "PY=%PY_FORCE%"
)
set "PY_PROBE_FILE=%ROOT%2_Logs\_py_probe.tmp"
REM Official runtime first: match intraday loop and dashboard-state verification.
if not defined PY call :TRY_PY_EXE "%ROOT%_runtime\python312-embed\python.exe"
if not defined PY call :TRY_PY_EXE "%ROOT%.venv\Scripts\python.exe"
if not defined PY call :TRY_PY_EXE "E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY call :TRY_PY_EXE "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY call :TRY_PY_EXE "C:\Users\jjtop\AppData\Local\Programs\Python\Python314\python.exe"
if not defined PY call :TRY_PY_EXE "%ROOT%tools\python_exec_proxy.cmd"
if not defined PY call :TRY_PY_FROM_WHERE
if not defined PY call :TRY_PY_FROM_POWERSHELL
if not defined PY call :TRY_PY_CMD python
if not defined PY call :TRY_PY_LAUNCHER
if defined PY (
  if /I not "%PY%"=="python" if /I not "%PY%"=="py -3" (
    echo %PY% | findstr /R /I "\\.exe$" >nul && set "PY=%PY%"
  )
)
set "PY_RAW=%PY%"
if not defined PY (
  echo [FAILED] python runtime not found
  echo         checked: "%ROOT%_runtime\python312-embed\python.exe"
  echo                  "%ROOT%.venv\Scripts\python.exe"
  echo                  "E:\vibe\buffett\.venv\Scripts\python.exe"
  echo                  "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
  echo                  "%ROOT%tools\python_exec_proxy.cmd"
  echo         install python or create venv and retry.
  goto :FAILED
)

echo [ROOT] %CD%
echo [PY] %PY%
echo [SCOPE] PAPER_OPER_START_YMD=%PAPER_OPER_START_YMD%
echo [KRX_MIN_UNI] %KRX_MIN_UNI%
if "%SSOT_HEALTH_CHECK%"=="" set "SSOT_HEALTH_CHECK=1"
if "%RESILIENCE_CHECK%"=="" set "RESILIENCE_CHECK=1"
if "%RESILIENCE_CHECK%"=="1" (
  set "LAST_STEP_LABEL=[PRECHECK] run_resilience_check.bat"
  echo [PRECHECK] run_resilience_check.bat
  call "%ROOT%run_resilience_check.bat"
  if errorlevel 1 (
    echo [FAILED] resilience precheck failed - fail closed
    goto :FAILED
  )
)
set "LAST_LOG=%ROOT%2_Logs\run_paper_daily_last.txt"
if not exist "%ROOT%2_Logs" mkdir "%ROOT%2_Logs" >nul 2>nul
set "RUN_TS="
for /f "delims=" %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-ddTHH:mm:ss"') do set "RUN_TS=%%I"
if "%RUN_TS%"=="" set "RUN_TS=%DATE%_%TIME%"
if "%RUN_ID%"=="" (
  for /f "delims=" %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "RUN_ID=%%I"
)
if "%RUN_ID%"=="" set "RUN_ID=%RUN_TS%"
> "%LAST_LOG%" echo ============================================================
>> "%LAST_LOG%" echo [START] run_paper_daily.bat ts=%RUN_TS%
>> "%LAST_LOG%" echo [RUN_ID] %RUN_ID%
>> "%LAST_LOG%" echo [ROOT] %CD%
>> "%LAST_LOG%" echo [PY] %PY%
>> "%LAST_LOG%" echo [SCOPE] PAPER_OPER_START_YMD=%PAPER_OPER_START_YMD%
>> "%LAST_LOG%" echo [KRX_MIN_UNI] %KRX_MIN_UNI%
>> "%LAST_LOG%" echo [SURGE_RT_PAPER_DATA_COLLECTION] %SURGE_RT_PAPER_DATA_COLLECTION%
>> "%LAST_LOG%" echo [SURGE_RT_PAPER_HIGH_REJECTION_ENTRY_BLOCK_PCT] %SURGE_RT_PAPER_HIGH_REJECTION_ENTRY_BLOCK_PCT%
>> "%LAST_LOG%" echo ============================================================
REM ------------------------------------------------------------
REM [0/14] snapshot + hash the config used for this run (reproducibility)
REM + LOCK-B: require config sha256 to match approved_sha256 in paper_engine_config.lock.json
REM ------------------------------------------------------------
echo [0/14] snapshot+hash+lockcheck paper\paper_engine_config.json
set "LAST_STEP_LABEL=[0/14] snapshot+hash+lockcheck paper_engine_config"
call "%ROOT%tasks\task_00_config_lock.bat"
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [0.5/14] pre-price KRX clean refresh
REM - prices_update_paper_incremental.py falls back to latest clean parquet when
REM   pykrx same-day fetch is unavailable, so ensure clean parquet is refreshed
REM   before [1/9].
REM ------------------------------------------------------------
set "LAST_STEP_LABEL=[0.5/14] sync_krx_reference_cache.py"
echo [0.5/14] tools\sync_krx_reference_cache.py (pre-prices)
"%PY%" tools\sync_krx_reference_cache.py
if errorlevel 1 goto :FAILED

set "LAST_STEP_LABEL=[0.55/14] krx_update_clean_incremental.py"
echo [0.55/14] krx_update_clean_incremental.py (pre-prices)
if "%KRX_UPDATE_CLEAN_TIMEOUT_SEC%"=="" set "KRX_UPDATE_CLEAN_TIMEOUT_SEC=1800"
"%PY%" tools\run_step_with_timeout.py --name krx_update_clean_pre_prices --timeout-sec %KRX_UPDATE_CLEAN_TIMEOUT_SEC% --status-json 2_Logs\krx_update_clean_pre_prices_timeout_status_latest.json -- "%PY%" krx_update_clean_incremental.py --base . --min-uni %KRX_MIN_UNI% --probe-cap 700
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [1/9] update prices parquet
REM ------------------------------------------------------------
call :LOG_STEP_BEGIN "[1/9] prices_update_paper_incremental.py"
"%PY%" prices_update_paper_incremental.py
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[1/9]" %RC%
if not "%RC%"=="0" goto :FAILED

if "%SSOT_HEALTH_CHECK%"=="1" (
  set "LAST_STEP_LABEL=[PRECHECK] run_ssot_health_card.bat"
  echo [PRECHECK] run_ssot_health_card.bat
  call "%ROOT%run_ssot_health_card.bat"
  if errorlevel 1 (
    echo [FAILED] ssot health precheck failed after daily data refresh - fail closed
    goto :FAILED
  )
)

REM ------------------------------------------------------------
REM [1.05/9] prebuild surge universe from refreshed OHLCV
REM ------------------------------------------------------------
call :LOG_STEP_BEGIN "[1.05/9] tools\\build_surge_universe.py"
"%PY%" tools\build_surge_universe.py
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[1.05/9]" %RC%
if not "%RC%"=="0" goto :FAILED

REM ------------------------------------------------------------
REM [2/9] sync candidates meta
REM ------------------------------------------------------------
set "LAST_STEP_LABEL=[2/9] sync_candidates_meta.py"
echo [2/9] sync_candidates_meta.py
"%PY%" sync_candidates_meta.py
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [3/9] macro prebuild
REM ------------------------------------------------------------
echo [PRE] build_rate_series_external.py
"%PY%" tools\build_rate_series_external.py
if errorlevel 1 (
  echo [WARN] build_rate_series_external failed - continue with local rate series
)

set "LAST_STEP_LABEL=[PRE] macro_signal_daily.py"
echo [PRE] macro_signal_daily.py
"%PY%" tools\run_step_with_timeout.py --name macro_signal_daily --timeout-sec 600 --status-json 2_Logs\macro_signal_daily_timeout_status_latest.json -- "%PY%" tools\macro_signal_daily.py
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [5/9] survivorship policy daily
REM ------------------------------------------------------------
call :LOG_STEP_BEGIN "[5/9] survivorship_policy_daily.py"
"%PY%" survivorship_policy_daily.py
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[5/9]" %RC%
if not "%RC%"=="0" goto :FAILED

REM ------------------------------------------------------------
REM [6/9] liquidity filter daily
REM ------------------------------------------------------------
call :LOG_STEP_BEGIN "[6/9] liquidity_filter_daily.py"
"%PY%" liquidity_filter_daily.py
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[6/9]" %RC%
if not "%RC%"=="0" goto :FAILED


REM ------------------------------------------------------------
REM [6.25/9] candidates refresh (DART + KRX watchlist)
REM ------------------------------------------------------------
set "LAST_STEP_LABEL=[6.2/9] sync_krx_reference_cache.py"
echo [6.2/9] tools\sync_krx_reference_cache.py
"%PY%" tools\sync_krx_reference_cache.py
if errorlevel 1 goto :FAILED

set "LAST_STEP_LABEL=[6.23/9] krx_update_clean_incremental.py"
echo [6.23/9] krx_update_clean_incremental.py
"%PY%" tools\run_step_with_timeout.py --name krx_update_clean_candidates --timeout-sec %KRX_UPDATE_CLEAN_TIMEOUT_SEC% --status-json 2_Logs\krx_update_clean_candidates_timeout_status_latest.json -- "%PY%" krx_update_clean_incremental.py --base . --min-uni %KRX_MIN_UNI% --probe-cap 700
if errorlevel 1 goto :FAILED

set "LAST_STEP_LABEL=[6.24/9] sync_krx_archive_from_sources.py"
echo [6.24/9] tools\sync_krx_archive_from_sources.py
"%PY%" tools\sync_krx_archive_from_sources.py
if errorlevel 1 goto :FAILED

set "CAND_FILE=%ROOT%2_Logs\candidates_latest_data.csv"
set "CAND_HASH_BEFORE="
set "CAND_CODE_HASH_BEFORE="
if exist "%CAND_FILE%" call :FILE_SHA "%CAND_FILE%" CAND_HASH_BEFORE
if exist "%CAND_FILE%" call :CSV_CODE_HASH "%CAND_FILE%" CAND_CODE_HASH_BEFORE
call :LOG_STEP_BEGIN "[6.25/9] generate_candidates_v41_1.py"
set "DART_KEY_FILE=%ROOT%_cache\dart_api_key.txt"
if "%DART_API_KEY%"=="" (
  if exist "%DART_KEY_FILE%" (
    set /p DART_API_KEY=<"%DART_KEY_FILE%"
    echo [INFO] DART_API_KEY loaded from key file
  ) else (
    echo [INFO] DART_API_KEY not set (DART refresh may skip)
  )
) else (
  echo [INFO] DART_API_KEY detected in environment
)
if "%FUND_DART_REFRESH%"=="" set "FUND_DART_REFRESH=1"
if "%FUND_KRX_WATCH_REFRESH%"=="" set "FUND_KRX_WATCH_REFRESH=1"
for /f "usebackq tokens=1,2 delims==" %%A in (`"%PY%" tools\decide_candidate_refresh_flags.py --root "%CD%"`) do (
  if /I "%%A"=="FUND_DART_REFRESH" set "FUND_DART_REFRESH=%%B"
  if /I "%%A"=="FUND_KRX_WATCH_REFRESH" set "FUND_KRX_WATCH_REFRESH=%%B"
)
if "%WATCH_CAUTION_LOOKBACK_DAYS%"=="" set "WATCH_CAUTION_LOOKBACK_DAYS=30"
if "%WATCH_SEARCH_LOOKBACK_DAYS%"=="" set "WATCH_SEARCH_LOOKBACK_DAYS=365"
if "%SUPPLY_PROVIDER%"=="" set "SUPPLY_PROVIDER=kis"
if "%SUPPLY_KIS_MOCK%"=="" set "SUPPLY_KIS_MOCK=1"
echo [INFO] FUND_DART_REFRESH=%FUND_DART_REFRESH% FUND_KRX_WATCH_REFRESH=%FUND_KRX_WATCH_REFRESH%
echo [INFO] SUPPLY_PROVIDER=%SUPPLY_PROVIDER% SUPPLY_KIS_MOCK=%SUPPLY_KIS_MOCK%

"%PY%" generate_candidates_v41_1.py
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[6.25/9]" %RC%
if not "%RC%"=="0" goto :FAILED
call :LOG_STEP_BEGIN "[6.251/9] tools\\build_general_logic_observe_companion.py"
"%PY%" tools\build_general_logic_observe_companion.py --root "%ROOT_NQ%"
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[6.251/9]" %RC%
if not "%RC%"=="0" (
  echo [WARN] general logic observe companion failed - continuing read-only observation step
)
call :LOG_STEP_BEGIN "[6.252/9] tools\\build_candidate_bridge_policy_simulation.py"
"%PY%" tools\build_candidate_bridge_policy_simulation.py --root "%ROOT_NQ%"
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[6.252/9]" %RC%
if not "%RC%"=="0" (
  echo [WARN] candidate bridge policy simulation failed - continuing read-only observation step
)
set "CAND_HASH_AFTER="
set "CAND_CODE_HASH_AFTER="
if exist "%CAND_FILE%" call :FILE_SHA "%CAND_FILE%" CAND_HASH_AFTER
if exist "%CAND_FILE%" call :CSV_CODE_HASH "%CAND_FILE%" CAND_CODE_HASH_AFTER
set "CANDIDATES_REFRESHED=1"
if /I "%CAND_HASH_BEFORE%"=="%CAND_HASH_AFTER%" set "CANDIDATES_REFRESHED=0"
set "CANDIDATE_CODES_CHANGED=1"
if /I "%CAND_CODE_HASH_BEFORE%"=="%CAND_CODE_HASH_AFTER%" set "CANDIDATE_CODES_CHANGED=0"
set "POST_REFRESH_NEEDED=1"
set "POST_PRICE_REFRESH_NEEDED=0"
set "POST_SURVIVORSHIP_REFRESH_NEEDED=0"
set "POST_LIQUIDITY_REFRESH_NEEDED=0"
if "%CANDIDATES_REFRESHED%"=="1" (
  set "LAST_STEP_LABEL=[6.26/9] CHECK_POST_REFRESH_NEEDED"
  call :CHECK_POST_REFRESH_NEEDED
  if errorlevel 1 goto :FAILED
)
if "%CANDIDATES_REFRESHED%"=="1" if "%POST_REFRESH_NEEDED%"=="1" (
  set "POST_PRICE_REFRESH_NEEDED=1"
)
if "%CANDIDATES_REFRESHED%"=="1" if "%CANDIDATE_CODES_CHANGED%"=="1" (
  set "POST_SURVIVORSHIP_REFRESH_NEEDED=1"
  set "POST_LIQUIDITY_REFRESH_NEEDED=1"
)

REM ------------------------------------------------------------
REM [6.26/9] refresh prices parquet again after today's candidates are rebuilt
REM - step [1/9] may still be using previous candidate universe
REM ------------------------------------------------------------
if "%CANDIDATES_REFRESHED%"=="1" (
  call :LOG_MSG "[6.26/9] post-refresh flags price=%POST_PRICE_REFRESH_NEEDED% survivorship=%POST_SURVIVORSHIP_REFRESH_NEEDED% liquidity=%POST_LIQUIDITY_REFRESH_NEEDED% (codes_changed=%CANDIDATE_CODES_CHANGED%)"
  if "%POST_PRICE_REFRESH_NEEDED%"=="1" (
    call :LOG_STEP_BEGIN "[6.26/9] prices_update_paper_incremental.py (post-candidates refresh)"
    "%PY%" prices_update_paper_incremental.py
    set "RC=%ERRORLEVEL%"
    call :LOG_STEP_END "[6.26/9]" %RC%
    if not "%RC%"=="0" goto :FAILED
  ) else (
    call :LOG_MSG "[6.26/9] skip prices_update_paper_incremental.py (prices up-to-date)"
  )

  if "%POST_SURVIVORSHIP_REFRESH_NEEDED%"=="1" (
    call :LOG_STEP_BEGIN "[6.3/9] survivorship_policy_daily.py (post-candidates refresh)"
    "%PY%" survivorship_policy_daily.py
    set "RC=%ERRORLEVEL%"
    call :LOG_STEP_END "[6.3/9]" %RC%
    if not "%RC%"=="0" goto :FAILED
  ) else (
    call :LOG_MSG "[6.3/9] skip survivorship_policy_daily.py (candidate universe unchanged)"
  )

  if "%POST_LIQUIDITY_REFRESH_NEEDED%"=="1" (
    call :LOG_STEP_BEGIN "[6.4/9] liquidity_filter_daily.py (post-candidates refresh)"
    "%PY%" liquidity_filter_daily.py
    set "RC=%ERRORLEVEL%"
    call :LOG_STEP_END "[6.4/9]" %RC%
    if not "%RC%"=="0" goto :FAILED
  ) else (
    call :LOG_MSG "[6.4/9] skip liquidity_filter_daily.py (candidate universe unchanged)"
  )
) else (
  call :LOG_MSG "[6.26/9] candidates unchanged -> skip post-candidates refresh steps"
)

call :LOG_STEP_BEGIN "[6.262/9] tools\\build_candidate_bridge_shadow_ledger.py"
"%PY%" tools\build_candidate_bridge_shadow_ledger.py --root "%ROOT_NQ%"
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[6.262/9]" %RC%
if not "%RC%"=="0" (
  echo [WARN] candidate bridge shadow ledger failed - continuing read-only validation step
)
call :LOG_STEP_BEGIN "[6.263/9] tools\\build_candidate_bridge_daily_status_report.py"
"%PY%" tools\build_candidate_bridge_daily_status_report.py --root "%ROOT_NQ%"
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[6.263/9]" %RC%
if not "%RC%"=="0" (
  echo [WARN] candidate bridge daily status report failed - continuing read-only validation step
)

REM ------------------------------------------------------------
REM [6.265/9] freshness precheck (fail-closed after official refresh steps)
REM - validate candidate/price/krx freshness only after this batch had a chance
REM   to refresh those artifacts in [1/9], [6.23/9], [6.25/9], [6.26/9].
REM ------------------------------------------------------------
if "%FRESHNESS_PRECHECK%"=="" set "FRESHNESS_PRECHECK=1"
if "%FRESHNESS_PRECHECK%"=="1" (
  set "LAST_STEP_LABEL=[6.265/9] freshness_check_v1.py"
  echo [6.265/9] freshness_check_v1.py
  "%PY%" "%ROOT%tools\freshness_check_v1.py"
  if errorlevel 1 (
    echo [FAILED] freshness precheck failed - fail closed
    goto :FAILED
  )
)

REM ------------------------------------------------------------
REM [6.45/9] refresh p0/gate after candidate pipeline finalized today's meta
REM - sector/news/final stages read latest p0/gate artifacts, so refresh here once.
REM ------------------------------------------------------------
set "LAST_STEP_LABEL=[6.45/9] p0_daily_check.py"
echo [6.45/9] p0_daily_check.py (post-candidates refresh)
set "P0_STDOUT=%ROOT%2_Logs\p0_daily_check_last.stdout.txt"
set "P0_STDERR=%ROOT%2_Logs\p0_daily_check_last.stderr.txt"
if "%P0_DAILY_CHECK_TIMEOUT_SEC%"=="" set "P0_DAILY_CHECK_TIMEOUT_SEC=600"
> "%P0_STDOUT%" echo [START] p0_daily_check stdout ts=%DATE% %TIME%
> "%P0_STDERR%" echo [START] p0_daily_check stderr ts=%DATE% %TIME%
"%PY%" tools\run_step_with_timeout.py --name p0_daily_check --timeout-sec %P0_DAILY_CHECK_TIMEOUT_SEC% --status-json 2_Logs\p0_daily_check_timeout_status_latest.json -- "%PY%" p0_daily_check.py >> "%P0_STDOUT%" 2>> "%P0_STDERR%"
set "P0_RC=%ERRORLEVEL%"
if not "%P0_RC%"=="0" (
  echo [FAILED] p0_daily_check rc=%P0_RC% stdout="%P0_STDOUT%" stderr="%P0_STDERR%"
  goto :FAILED
)

set "LAST_STEP_LABEL=[6.46/9] gate_daily.py"
echo [6.46/9] gate_daily.py (post-candidates refresh)
"%PY%" gate_daily.py
if errorlevel 1 goto :FAILED

set "LAST_STEP_LABEL=[6.47/9] build_production_risk_playbook.py"
echo [6.47/9] tools\build_production_risk_playbook.py
"%PY%" tools\build_production_risk_playbook.py
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [6.5/9] sector score daily (snapshot + history append)
REM ------------------------------------------------------------
set "SIGNAL_FAIL_SOFT=%SIGNAL_FAIL_SOFT%"
if "%SIGNAL_FAIL_SOFT%"=="" set "SIGNAL_FAIL_SOFT=0"
echo [SIGNAL_POLICY] SIGNAL_FAIL_SOFT=%SIGNAL_FAIL_SOFT%
echo [6.5/9] tools\sector_score_daily.py
if "%SECTOR_SCORE_TIMEOUT_SEC%"=="" set "SECTOR_SCORE_TIMEOUT_SEC=240"
"%PY%" tools\run_step_with_timeout.py --name sector_score_daily --timeout-sec %SECTOR_SCORE_TIMEOUT_SEC% --status-json 2_Logs\sector_score_timeout_status_latest.json -- "%PY%" tools\sector_score_daily.py
if errorlevel 1 (
  echo [ERROR] sector_score_daily failed
  if "%SIGNAL_FAIL_SOFT%"=="1" (
    echo [WARN] SIGNAL_FAIL_SOFT=1 - continuing
  ) else (
    goto :FAILED
  )
)
"%PY%" tools\check_signal_contract.py --stage sector --root "%ROOT_NQ%"
if errorlevel 1 (
  echo [ERROR] sector contract failed
  if "%SIGNAL_FAIL_SOFT%"=="1" (
    echo [WARN] SIGNAL_FAIL_SOFT=1 - continuing
  ) else (
    goto :FAILED
  )
)

echo [6.52/9] tools\build_candidate_price_history.py
"%PY%" tools\build_candidate_price_history.py --lookback 60
if errorlevel 1 (
  echo [ERROR] build_candidate_price_history failed
  if "%SIGNAL_FAIL_SOFT%"=="1" (
    echo [WARN] SIGNAL_FAIL_SOFT=1 - continuing
  ) else (
    goto :FAILED
  )
)

REM ------------------------------------------------------------
REM [6.55-6.7/9] unified news pipeline (collect -> score -> final)
REM ------------------------------------------------------------
echo [6.55-6.7/9] run_news_pipeline_once.bat
if "%NAVER_CLIENT_ID%"=="" (
  echo [INFO] NAVER_CLIENT_ID missing -> collector will skip fail-soft
) else (
  echo [INFO] NAVER_CLIENT_ID detected -> collector enabled
)
if "%NEWS_PIPELINE_PROFILE%"=="" set "NEWS_PIPELINE_PROFILE=POST_CLOSE_FULL"
if /I "%NEWS_PIPELINE_PROFILE%"=="INTRADAY_LIGHT" (
  if "%NEWS_PIPELINE_WINDOW_START%"=="" set "NEWS_PIPELINE_WINDOW_START=09:00"
  if "%NEWS_PIPELINE_WINDOW_END%"=="" set "NEWS_PIPELINE_WINDOW_END=15:30"
  if "%NEWS_PIPELINE_SKIP_FINAL%"=="" set "NEWS_PIPELINE_SKIP_FINAL=1"
) else (
  if "%NEWS_PIPELINE_WINDOW_START%"=="" set "NEWS_PIPELINE_WINDOW_START=00:00"
  if "%NEWS_PIPELINE_WINDOW_END%"=="" set "NEWS_PIPELINE_WINDOW_END=23:59"
  set "NEWS_PIPELINE_SKIP_FINAL=1"
)
echo [NEWS_PIPELINE] profile=%NEWS_PIPELINE_PROFILE% window=%NEWS_PIPELINE_WINDOW_START%-%NEWS_PIPELINE_WINDOW_END% skip_final=%NEWS_PIPELINE_SKIP_FINAL%
REM LLM cost guard: daily batch always blocks LLM unless caller explicitly sets NEWS_LLM_SKIP=0
if "%NEWS_LLM_SKIP%"=="" set "NEWS_LLM_SKIP=0"
if "%NEWS_LLM_ENABLE%"=="" set "NEWS_LLM_ENABLE=1"
echo [NEWS_LLM_GUARD] NEWS_LLM_SKIP=%NEWS_LLM_SKIP% NEWS_LLM_ENABLE=%NEWS_LLM_ENABLE%
set "NEWS_PIPELINE_SKIP_INTEGRATION=1"
call "%ROOT%run_news_pipeline_once.bat" daily %NEWS_PIPELINE_WINDOW_START% %NEWS_PIPELINE_WINDOW_END%
set "NEWS_PIPELINE_SKIP_INTEGRATION="
if errorlevel 1 (
  echo [ERROR] run_news_pipeline_once failed
  if "%SIGNAL_FAIL_SOFT%"=="1" (
    echo [WARN] SIGNAL_FAIL_SOFT=1 - continuing
  ) else (
    goto :FAILED
  )
)
set "LAST_STEP_LABEL=[6.55n/9] news_score_daily.py"
echo [6.55n/9] tools\news_score_daily.py
"%PY%" tools\news_score_daily.py
if errorlevel 1 (
  echo [ERROR] news_score_daily failed
  if "%SIGNAL_FAIL_SOFT%"=="1" (
    echo [WARN] SIGNAL_FAIL_SOFT=1 - continuing
  ) else (
    goto :FAILED
  )
)
set "LAST_STEP_LABEL=[6.55c/9] check_signal_contract.py --stage news"
"%PY%" tools\check_signal_contract.py --stage news --root "%ROOT_NQ%"
if errorlevel 1 (
  echo [ERROR] news contract failed
  if "%SIGNAL_FAIL_SOFT%"=="1" (
    echo [WARN] SIGNAL_FAIL_SOFT=1 - continuing
  ) else (
    goto :FAILED
  )
)

REM ------------------------------------------------------------
REM [6.95/9] P0 preflight guards (fail-closed)
REM ------------------------------------------------------------
call :LOG_STEP_BEGIN "[6.95a/9] paper_validate.py"
"%PY%" paper_validate.py
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[6.95a/9]" %RC%
if not "%RC%"=="0" goto :FAILED

call :LOG_STEP_BEGIN "[6.95b/9] tools\\reconcile_paper_state_from_fills.py"
"%PY%" tools\reconcile_paper_state_from_fills.py
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[6.95b/9]" %RC%
if not "%RC%"=="0" goto :FAILED

call :LOG_STEP_BEGIN "[6.95c/9] tools\\check_entry_room.py"
"%PY%" tools\check_entry_room.py
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[6.95c/9]" %RC%
if not "%RC%"=="0" goto :FAILED

call :LOG_STEP_BEGIN "[6.95d/9] prices_update_paper_incremental.py"
"%PY%" prices_update_paper_incremental.py
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[6.95d/9]" %RC%
if not "%RC%"=="0" goto :FAILED

call :LOG_STEP_BEGIN "[6.96/9] run_backtest_validation_real.bat"
set "BT_SCREEN_OPEN=0"
call :RUN_BATCH_ISOLATED "%ROOT%run_backtest_validation_real.bat"
set "RC=%ERRORLEVEL%"
set "BT_SCREEN_OPEN="
call :LOG_STEP_END "[6.96/9]" %RC%
if "%RC%"=="0" goto :BTVAL_OK
if "%RC%"=="2" (
  echo [WARN] backtest validation refreshed with NO_GO result (rc=2)
  goto :BTVAL_OK
)
goto :FAILED
:BTVAL_OK

call :LOG_STEP_BEGIN "[6.966/9] tools\\news_candidates_daily.py"
"%PY%" tools\news_candidates_daily.py
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[6.966/9]" %RC%
if not "%RC%"=="0" goto :FAILED

REM [6.24b/9] Forward Estimate Snapshot (estimated PER / TTM / PEG)
REM - final_score_merge_daily.py must read the same candidate chain date/source.
if "%FUND_DART_REFRESH%"=="1" (
  call :LOG_STEP_BEGIN "[6.24b/9] build_forward_estimate_snapshot.py"
  "%PY%" tools\build_forward_estimate_snapshot.py --max-codes 80 --sleep 0.05 --http-retry-max 1 --http-backoff-base 0.2 --http-backoff-cap 1.5
  if errorlevel 1 (
    call :LOG_STEP_END "[6.24b/9]" 1
    call :LOG_MSG "[WARN] forward_estimate_snapshot failed - non-fatal"
  ) else (
    call :LOG_STEP_END "[6.24b/9]" 0
  )
) else (
  call :LOG_MSG "[6.24b/9] skip build_forward_estimate_snapshot.py (FUND_DART_REFRESH=0)"
)

if "%FINAL_SCORE_MERGE_TIMEOUT_SEC%"=="" set "FINAL_SCORE_MERGE_TIMEOUT_SEC=180"
REM [2026-08-31] news pipeline OFF. Decision recorded in .agent\PLANS.md (148).
REM   Evidence: news_score vs final_score corr -0.639 (PLANS 17);
REM   52%% of candidates are a structural partition that can never enter
REM   (w_news cap 0.125 < tech floor 0.367); effective axis weight 0.0 at
REM   final_score_merge_daily.py:2309-2313; wiki theses 100%% blocked;
REM   5 news tasks failing rc=1. Ten days unresolved (backlog: 'keep or kill').
REM   NEWS_ONLY rows are appended at final_score_merge_daily.py:1642-1644 where
REM   append = (collect_mode==accumulate) OR switch. Both are turned off:
REM   state\news_collect_mode.txt -> production, and the switch below.
REM   Reversible: delete the set line and restore the mode file to accumulate.
REM   collect_mode 를 기본값에 맡기면 accumulate 로 나오는 것을 실측했다. 명시한다.
set "NEWS_COLLECT_MODE=production"
set "NEWS_CANDIDATES_APPEND_NEWS_ONLY=0"
call :LOG_STEP_BEGIN "[6.967/9] tools\\final_score_merge_daily.py"
"%PY%" tools\run_step_with_timeout.py --name final_score_merge_daily --timeout-sec %FINAL_SCORE_MERGE_TIMEOUT_SEC% --status-json 2_Logs\final_score_merge_timeout_status_latest.json -- "%PY%" tools\final_score_merge_daily.py
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[6.967/9]" %RC%
if not "%RC%"=="0" goto :FAILED

call :LOG_STEP_BEGIN "[6.967a/9] tools\\build_news_signal_shadow_stage.py"
"%PY%" tools\build_news_signal_shadow_stage.py
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[6.967a/9]" %RC%
if not "%RC%"=="0" goto :FAILED

call :LOG_STEP_BEGIN "[6.967b/9] tools\\build_news_signal_shadow_diagnostic.py"
"%PY%" tools\build_news_signal_shadow_diagnostic.py
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[6.967b/9]" %RC%
if not "%RC%"=="0" goto :FAILED

call :LOG_STEP_BEGIN "[6.967c/9] tools\\build_news_signal_market_corroboration_audit.py"
"%PY%" tools\build_news_signal_market_corroboration_audit.py
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[6.967c/9]" %RC%
if not "%RC%"=="0" goto :FAILED

"%PY%" tools\check_signal_contract.py --stage final --root "%ROOT_NQ%"
if errorlevel 1 (
  echo [ERROR] final contract failed
  if "%SIGNAL_FAIL_SOFT%"=="1" (
    echo [WARN] SIGNAL_FAIL_SOFT=1 - continuing
  ) else (
    goto :FAILED
  )
)

call :LOG_STEP_BEGIN "[6.968/9] tools\\run_future_signal_daily.py"
"%PY%" tools\run_step_with_timeout.py --name future_signal_daily --timeout-sec 600 --status-json 2_Logs\future_signal_daily_timeout_status_latest.json -- "%PY%" tools\run_future_signal_daily.py >> "%LAST_LOG%" 2>&1
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[6.968/9]" %RC%
if not "%RC%"=="0" goto :FAILED
call :LOG_STEP_BEGIN "[6.97/9] tools\\build_integrated_ops_snapshot.py"
"%PY%" tools\build_integrated_ops_snapshot.py
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[6.97/9]" %RC%
if not "%RC%"=="0" goto :FAILED

call :LOG_STEP_BEGIN "[6.971/9] E:\vibe\buffett\tools\build_dashboard_state_v2.py pre-engine"
if exist "E:\vibe\buffett\tools\build_dashboard_state_v2.py" (
  "%PY%" E:\vibe\buffett\tools\build_dashboard_state_v2.py
  set "RC=%ERRORLEVEL%"
  call :LOG_STEP_END "[6.971/9]" %RC%
  if not "%RC%"=="0" goto :FAILED
) else (
  call :LOG_MSG "[6.971/9] SKIP build_dashboard_state_v2.py pre-engine missing"
)

REM ------------------------------------------------------------
REM [6.99/9] refresh after_close before paper engine validation
REM ------------------------------------------------------------
echo [6.99/9] after_close_summary.py pre-engine
"%PY%" after_close_summary.py
if errorlevel 1 (
  echo [WARN] after_close_summary pre-engine failed - continuing to paper_engine
)

REM ------------------------------------------------------------
REM [7/9] paper engine
REM ------------------------------------------------------------
echo [6.95/9] tools\build_promoted_recheck_candidates.py
call :LOG_MSG "[6.95/9] promoted_recheck_candidates START"
"%PY%" tools\build_promoted_recheck_candidates.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] promoted_recheck_candidates failed - continuing
  call :LOG_MSG "[6.95/9] promoted_recheck_candidates END rc=1 advisory"
) else (
  call :LOG_MSG "[6.95/9] promoted_recheck_candidates END rc=0"
)

call :LOG_STEP_BEGIN "[6.995/9] gate_daily.py pre-engine latest P0"
"%PY%" gate_daily.py >> "%LAST_LOG%" 2>&1
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[6.995/9]" %RC%
if not "%RC%"=="0" goto :FAILED

echo [6.996/9] tools\build_surge_active_response_layer.py pre-engine
call :LOG_MSG "[6.996/9] surge_active_response_layer pre-engine START"
"%PY%" tools\build_surge_active_response_layer.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] surge_active_response_layer pre-engine failed - continuing
  call :LOG_MSG "[6.996/9] surge_active_response_layer pre-engine END rc=1 advisory"
) else (
  call :LOG_MSG "[6.996/9] surge_active_response_layer pre-engine END rc=0"
)

echo [6.997a/9] tools\build_surge_event_money_pullback_screener.py pre-engine
call :LOG_MSG "[6.997a/9] surge_event_money_pullback_screener pre-engine START"
"%PY%" tools\build_surge_event_money_pullback_screener.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] surge_event_money_pullback_screener pre-engine failed - continuing
  call :LOG_MSG "[6.997a/9] surge_event_money_pullback_screener pre-engine END rc=1 advisory"
) else (
  call :LOG_MSG "[6.997a/9] surge_event_money_pullback_screener pre-engine END rc=0"
)

echo [6.997b/9] tools\build_pullback_methodology_first_screener.py pre-engine
call :LOG_MSG "[6.997b/9] pullback_methodology_first_screener pre-engine START"
"%PY%" tools\build_pullback_methodology_first_screener.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] pullback_methodology_first_screener pre-engine failed - continuing
  call :LOG_MSG "[6.997b/9] pullback_methodology_first_screener pre-engine END rc=1 advisory"
) else (
  call :LOG_MSG "[6.997b/9] pullback_methodology_first_screener pre-engine END rc=0"
)

echo [6.997c/9] tools\build_event_theme_candidate_layer.py pre-engine
call :LOG_MSG "[6.997c/9] event_theme_candidate_layer pre-engine START"
"%PY%" tools\build_event_theme_candidate_layer.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] event_theme_candidate_layer pre-engine failed - continuing
  call :LOG_MSG "[6.997c/9] event_theme_candidate_layer pre-engine END rc=1 advisory"
) else (
  call :LOG_MSG "[6.997c/9] event_theme_candidate_layer pre-engine END rc=0"
)

echo [6.997c1/9] tools\validate_four_question_readonly_layer.py pre-engine
call :LOG_MSG "[6.997c1/9] four_question_readonly_validation pre-engine START"
"%PY%" tools\validate_four_question_readonly_layer.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] four_question_readonly_validation pre-engine failed - continuing
  call :LOG_MSG "[6.997c1/9] four_question_readonly_validation pre-engine END rc=1 advisory"
) else (
  call :LOG_MSG "[6.997c1/9] four_question_readonly_validation pre-engine END rc=0"
)

echo [6.997c2/9] tools\build_four_question_intraday_watch.py pre-engine
call :LOG_MSG "[6.997c2/9] four_question_intraday_watch pre-engine START"
"%PY%" tools\build_four_question_intraday_watch.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] four_question_intraday_watch pre-engine failed - continuing
  call :LOG_MSG "[6.997c2/9] four_question_intraday_watch pre-engine END rc=1 advisory"
) else (
  call :LOG_MSG "[6.997c2/9] four_question_intraday_watch pre-engine END rc=0"
)

echo [6.997c3/9] tools\validate_four_question_role_fit_forward.py pre-engine
call :LOG_MSG "[6.997c3/9] four_question_role_fit_forward pre-engine START"
"%PY%" tools\validate_four_question_role_fit_forward.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] four_question_role_fit_forward pre-engine failed - continuing
  call :LOG_MSG "[6.997c3/9] four_question_role_fit_forward pre-engine END rc=1 advisory"
) else (
  call :LOG_MSG "[6.997c3/9] four_question_role_fit_forward pre-engine END rc=0"
)

echo [6.997c4/9] tools\news_multisource_shadow_score_review.py pre-engine
call :LOG_MSG "[6.997c4/9] news_multisource_shadow_score_review pre-engine START"
"%PY%" tools\news_multisource_shadow_score_review.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] news_multisource_shadow_score_review pre-engine failed - continuing
  call :LOG_MSG "[6.997c4/9] news_multisource_shadow_score_review pre-engine END rc=1 advisory"
) else (
  call :LOG_MSG "[6.997c4/9] news_multisource_shadow_score_review pre-engine END rc=0"
)

echo [6.997c5/9] tools\news_multisource_shadow_score_impact_sim.py pre-engine
call :LOG_MSG "[6.997c5/9] news_multisource_shadow_score_impact_sim pre-engine START"
"%PY%" tools\news_multisource_shadow_score_impact_sim.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] news_multisource_shadow_score_impact_sim pre-engine failed - continuing
  call :LOG_MSG "[6.997c5/9] news_multisource_shadow_score_impact_sim pre-engine END rc=1 advisory"
) else (
  call :LOG_MSG "[6.997c5/9] news_multisource_shadow_score_impact_sim pre-engine END rc=0"
)

echo [6.997c6/9] tools\update_news_multisource_shadow_history.py pre-engine
call :LOG_MSG "[6.997c6/9] news_multisource_shadow_history pre-engine START"
"%PY%" tools\update_news_multisource_shadow_history.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] news_multisource_shadow_history pre-engine failed - continuing
  call :LOG_MSG "[6.997c6/9] news_multisource_shadow_history pre-engine END rc=1 advisory"
) else (
  call :LOG_MSG "[6.997c6/9] news_multisource_shadow_history pre-engine END rc=0"
)

echo [6.997d/9] tools\build_defense_signal_shadow.py pre-engine
call :LOG_MSG "[6.997d/9] defense_signal_shadow pre-engine START"
"%PY%" tools\build_defense_signal_shadow.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] defense_signal_shadow pre-engine failed - continuing
  call :LOG_MSG "[6.997d/9] defense_signal_shadow pre-engine END rc=1 advisory"
) else (
  call :LOG_MSG "[6.997d/9] defense_signal_shadow pre-engine END rc=0"
)

echo [6.997e/9] tools\validate_defense_signal_entry_policy.py pre-engine
call :LOG_MSG "[6.997e/9] defense_signal_entry_policy_validation pre-engine START"
"%PY%" tools\validate_defense_signal_entry_policy.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] defense_signal_entry_policy_validation pre-engine failed - continuing
  call :LOG_MSG "[6.997e/9] defense_signal_entry_policy_validation pre-engine END rc=1 advisory"
) else (
  call :LOG_MSG "[6.997e/9] defense_signal_entry_policy_validation pre-engine END rc=0"
)

set "LAST_STEP_LABEL=[7/9] paper_engine.py main"
echo [7/9] paper_engine.py (main)
call :LOG_MSG "[7/9] paper_engine main START run_label=main"
"%PY%" paper_engine.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 goto :FAILED
call :LOG_MSG "[7/9] paper_engine main END rc=0 run_label=main"
echo [7.00/9] tools\build_intraday_residual_overnight_risk_audit.py
call :LOG_MSG "[7.00/9] intraday_residual_overnight_risk_audit START"
"%PY%" tools\build_intraday_residual_overnight_risk_audit.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] intraday_residual_overnight_risk_audit failed - continuing
  call :LOG_MSG "[7.00/9] intraday_residual_overnight_risk_audit END rc=1 advisory"
) else (
  call :LOG_MSG "[7.00/9] intraday_residual_overnight_risk_audit END rc=0"
)
echo [7.00b/9] tools\build_no_trade_blocker_markout_archive.py
call :LOG_MSG "[7.00b/9] no_trade_blocker_markout_archive START"
"%PY%" tools\build_no_trade_blocker_markout_archive.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] no_trade_blocker_markout_archive failed - continuing
  call :LOG_MSG "[7.00b/9] no_trade_blocker_markout_archive END rc=1 advisory"
) else (
  call :LOG_MSG "[7.00b/9] no_trade_blocker_markout_archive END rc=0"
)

echo [7.00c/9] tools\build_no_trade_blocker_markout_summary.py
call :LOG_MSG "[7.00c/9] no_trade_blocker_markout_summary START"
"%PY%" tools\build_no_trade_blocker_markout_summary.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] no_trade_blocker_markout_summary failed - continuing
  call :LOG_MSG "[7.00c/9] no_trade_blocker_markout_summary END rc=1 advisory"
) else (
  call :LOG_MSG "[7.00c/9] no_trade_blocker_markout_summary END rc=0"
)
echo [7.005/9] tools\build_research_regime_live_companion.py
call :LOG_MSG "[7.005/9] research_regime_live_companion START"
"%PY%" tools\build_research_regime_live_companion.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] research_regime_live_companion failed - continuing
  call :LOG_MSG "[7.005/9] research_regime_live_companion END rc=1 advisory"
) else (
  call :LOG_MSG "[7.005/9] research_regime_live_companion END rc=0"
)

echo [7.01/9] tools\validate_intraday_residual_overnight_guard_activation.py
call :LOG_MSG "[7.01/9] intraday_residual_overnight_guard_activation_validation START"
"%PY%" tools\validate_intraday_residual_overnight_guard_activation.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] intraday_residual_overnight_guard_activation_validation failed - continuing
  call :LOG_MSG "[7.01/9] intraday_residual_overnight_guard_activation_validation END rc=1 advisory"
) else (
  call :LOG_MSG "[7.01/9] intraday_residual_overnight_guard_activation_validation END rc=0"
)

echo [7.05/9] tools\build_candidate_action_queue.py
call :LOG_MSG "[7.05/9] candidate_action_queue START"
"%PY%" tools\build_candidate_action_queue.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] candidate_action_queue failed - continuing
  call :LOG_MSG "[7.05/9] candidate_action_queue END rc=1 advisory"
) else (
  call :LOG_MSG "[7.05/9] candidate_action_queue END rc=0"
)

echo [7.06/9] tools\build_candidate_action_followup.py
call :LOG_MSG "[7.06/9] candidate_action_followup START"
"%PY%" tools\build_candidate_action_followup.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] candidate_action_followup failed - continuing
  call :LOG_MSG "[7.06/9] candidate_action_followup END rc=1 advisory"
) else (
  call :LOG_MSG "[7.06/9] candidate_action_followup END rc=0"
)

echo [7.07/9] tools\build_candidate_action_review.py
call :LOG_MSG "[7.07/9] candidate_action_review START"
"%PY%" tools\build_candidate_action_review.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] candidate_action_review failed - continuing
  call :LOG_MSG "[7.07/9] candidate_action_review END rc=1 advisory"
) else (
  call :LOG_MSG "[7.07/9] candidate_action_review END rc=0"
)

echo [7.08/9] tools\build_candidate_action_plan.py
call :LOG_MSG "[7.08/9] candidate_action_plan START"
"%PY%" tools\build_candidate_action_plan.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] candidate_action_plan failed - continuing
  call :LOG_MSG "[7.08/9] candidate_action_plan END rc=1 advisory"
) else (
  call :LOG_MSG "[7.08/9] candidate_action_plan END rc=0"
)

echo [7.08n/9] tools\build_no_lob_recheck_review_report.py
call :LOG_MSG "[7.08n/9] no_lob_recheck_review START"
"%PY%" tools\build_no_lob_recheck_review_report.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] no_lob_recheck_review failed - continuing
  call :LOG_MSG "[7.08n/9] no_lob_recheck_review END rc=1 advisory"
) else (
  call :LOG_MSG "[7.08n/9] no_lob_recheck_review END rc=0"
)

echo [7.08s/9] tools\build_surge_shadow_probe_candidate_report.py
call :LOG_MSG "[7.08s/9] surge_shadow_probe_candidate START"
"%PY%" tools\build_surge_shadow_probe_candidate_report.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] surge_shadow_probe_candidate failed - continuing
  call :LOG_MSG "[7.08s/9] surge_shadow_probe_candidate END rc=1 advisory"
) else (
  call :LOG_MSG "[7.08s/9] surge_shadow_probe_candidate END rc=0"
)

echo [7.08p/9] tools\build_surge_probe_policy_design.py
call :LOG_MSG "[7.08p/9] surge_probe_policy_design START"
"%PY%" tools\build_surge_probe_policy_design.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] surge_probe_policy_design failed - continuing
  call :LOG_MSG "[7.08p/9] surge_probe_policy_design END rc=1 advisory"
) else (
  call :LOG_MSG "[7.08p/9] surge_probe_policy_design END rc=0"
)

echo [7.08m/9] tools\build_surge_shadow_probe_markout_tracker.py
call :LOG_MSG "[7.08m/9] surge_shadow_probe_markout START"
"%PY%" tools\build_surge_shadow_probe_markout_tracker.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] surge_shadow_probe_markout failed - continuing
  call :LOG_MSG "[7.08m/9] surge_shadow_probe_markout END rc=1 advisory"
) else (
  call :LOG_MSG "[7.08m/9] surge_shadow_probe_markout END rc=0"
)

echo [7.08q/9] tools\build_surge_shadow_probe_markout_summary.py
call :LOG_MSG "[7.08q/9] surge_shadow_probe_markout_summary START"
"%PY%" tools\build_surge_shadow_probe_markout_summary.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] surge_shadow_probe_markout_summary failed - continuing
  call :LOG_MSG "[7.08q/9] surge_shadow_probe_markout_summary END rc=1 advisory"
) else (
  call :LOG_MSG "[7.08q/9] surge_shadow_probe_markout_summary END rc=0"
)

echo [7.08t/9] tools\build_surge_shadow_probe_timepoint_markout.py
call :LOG_MSG "[7.08t/9] surge_shadow_probe_timepoint_markout START"
"%PY%" tools\build_surge_shadow_probe_timepoint_markout.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] surge_shadow_probe_timepoint_markout failed - continuing
  call :LOG_MSG "[7.08t/9] surge_shadow_probe_timepoint_markout END rc=1 advisory"
) else (
  call :LOG_MSG "[7.08t/9] surge_shadow_probe_timepoint_markout END rc=0"
)

echo [7.08u/9] tools\build_surge_followthrough_kill_diagnostic.py
call :LOG_MSG "[7.08u/9] surge_followthrough_kill_diagnostic START"
"%PY%" tools\build_surge_followthrough_kill_diagnostic.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] surge_followthrough_kill_diagnostic failed - continuing
  call :LOG_MSG "[7.08u/9] surge_followthrough_kill_diagnostic END rc=1 advisory"
) else (
  call :LOG_MSG "[7.08u/9] surge_followthrough_kill_diagnostic END rc=0"
)

echo [7.08v/9] tools\build_surge_probe_latency_source_diagnostic.py
if defined RUN_DAILY_ADVISORY_DIAGNOSTICS_DONE goto :RUN_DAILY_AFTER_ADVISORY_DIAGNOSTICS
set "RUN_DAILY_ADVISORY_DIAGNOSTICS_DONE=1"
call :LOG_MSG "[7.08v/9] surge_probe_latency_source_diagnostic START"
"%PY%" tools\build_surge_probe_latency_source_diagnostic.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] surge_probe_latency_source_diagnostic failed - continuing
  call :LOG_MSG "[7.08v/9] surge_probe_latency_source_diagnostic END rc=1 advisory"
) else (
  call :LOG_MSG "[7.08v/9] surge_probe_latency_source_diagnostic END rc=0"
)

echo [7.08w/9] tools\build_lob_recheck_source_consistency_audit.py
call :LOG_MSG "[7.08w/9] lob_recheck_source_consistency_audit START"
"%PY%" tools\build_lob_recheck_source_consistency_audit.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] lob_recheck_source_consistency_audit failed - continuing
  call :LOG_MSG "[7.08w/9] lob_recheck_source_consistency_audit END rc=1 advisory"
) else (
  call :LOG_MSG "[7.08w/9] lob_recheck_source_consistency_audit END rc=0"
)

echo [7.08x/9] tools\build_lob_ingest_priority_diagnostic.py
call :LOG_MSG "[7.08x/9] lob_ingest_priority_diagnostic START"
"%PY%" tools\build_lob_ingest_priority_diagnostic.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] lob_ingest_priority_diagnostic failed - continuing
  call :LOG_MSG "[7.08x/9] lob_ingest_priority_diagnostic END rc=1 advisory"
) else (
  call :LOG_MSG "[7.08x/9] lob_ingest_priority_diagnostic END rc=0"
)

echo [7.08a/9] tools\build_shadow_promotion_report.py
call :LOG_MSG "[7.08a/9] shadow_promotion_report START"
"%PY%" tools\build_shadow_promotion_report.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] shadow_promotion_report failed - continuing
  call :LOG_MSG "[7.08a/9] shadow_promotion_report END rc=1 advisory"
) else (
  call :LOG_MSG "[7.08a/9] shadow_promotion_report END rc=0"
)

echo [7.08b/9] tools\build_entry_policy_group_report.py
call :LOG_MSG "[7.08b/9] entry_policy_group_report START"
"%PY%" tools\build_entry_policy_group_report.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] entry_policy_group_report failed - continuing
  call :LOG_MSG "[7.08b/9] entry_policy_group_report END rc=1 advisory"
) else (
  call :LOG_MSG "[7.08b/9] entry_policy_group_report END rc=0"
)

echo [7.08c/9] tools\build_pure_normal_entry_sample_audit.py
call :LOG_MSG "[7.08c/9] pure_normal_entry_sample_audit START"
"%PY%" tools\build_pure_normal_entry_sample_audit.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] pure_normal_entry_sample_audit failed - continuing
  call :LOG_MSG "[7.08c/9] pure_normal_entry_sample_audit END rc=1 advisory"
) else (
  call :LOG_MSG "[7.08c/9] pure_normal_entry_sample_audit END rc=0"
)

echo [7.08d/9] tools\build_pure_normal_loss_driver_audit.py
call :LOG_MSG "[7.08d/9] pure_normal_loss_driver_audit START"
"%PY%" tools\build_pure_normal_loss_driver_audit.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] pure_normal_loss_driver_audit failed - continuing
  call :LOG_MSG "[7.08d/9] pure_normal_loss_driver_audit END rc=1 advisory"
) else (
  call :LOG_MSG "[7.08d/9] pure_normal_loss_driver_audit END rc=0"
)

echo [7.08e/9] tools\build_cluster_candidate_quality_audit.py
call :LOG_MSG "[7.08e/9] cluster_candidate_quality_audit START"
"%PY%" tools\build_cluster_candidate_quality_audit.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] cluster_candidate_quality_audit failed - continuing
  call :LOG_MSG "[7.08e/9] cluster_candidate_quality_audit END rc=1 advisory"
) else (
  call :LOG_MSG "[7.08e/9] cluster_candidate_quality_audit END rc=0"
)

echo [7.08f/9] tools\build_strict_normal_policy_proof_audit.py
call :LOG_MSG "[7.08f/9] strict_normal_policy_proof_audit START"
"%PY%" tools\build_strict_normal_policy_proof_audit.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] strict_normal_policy_proof_audit failed - continuing
  call :LOG_MSG "[7.08f/9] strict_normal_policy_proof_audit END rc=1 advisory"
) else (
  call :LOG_MSG "[7.08f/9] strict_normal_policy_proof_audit END rc=0"
)

echo [7.08g/9] tools\build_runtime_normal_quality_separation_report.py
call :LOG_MSG "[7.08g/9] runtime_normal_quality_separation_report START"
"%PY%" tools\build_runtime_normal_quality_separation_report.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] runtime_normal_quality_separation_report failed - continuing
  call :LOG_MSG "[7.08g/9] runtime_normal_quality_separation_report END rc=1 advisory"
) else (
  call :LOG_MSG "[7.08g/9] runtime_normal_quality_separation_report END rc=0"
)

echo [7.08h/9] tools\build_policy_group_quality_link_report.py
call :LOG_MSG "[7.08h/9] policy_group_quality_link_report START"
"%PY%" tools\build_policy_group_quality_link_report.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] policy_group_quality_link_report failed - continuing
  call :LOG_MSG "[7.08h/9] policy_group_quality_link_report END rc=1 advisory"
) else (
  call :LOG_MSG "[7.08h/9] policy_group_quality_link_report END rc=0"
)

echo [7.09/9] tools\build_candidate_recheck_queue.py
call :LOG_MSG "[7.09/9] candidate_recheck_queue START"
"%PY%" tools\build_candidate_recheck_queue.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] candidate_recheck_queue failed - continuing
  call :LOG_MSG "[7.09/9] candidate_recheck_queue END rc=1 advisory"
) else (
  call :LOG_MSG "[7.09/9] candidate_recheck_queue END rc=0"
)

echo [7.10/9] tools\build_post_entry_learning.py
call :LOG_MSG "[7.10/9] post_entry_learning START"
"%PY%" tools\build_post_entry_learning.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] post_entry_learning failed - continuing
  call :LOG_MSG "[7.10/9] post_entry_learning END rc=1 advisory"
) else (
  call :LOG_MSG "[7.10/9] post_entry_learning END rc=0"
)

:RUN_DAILY_AFTER_ADVISORY_DIAGNOSTICS
REM [7.1/9] optional shadow collect (main path isolated)
REM [7.1/9] shadow_collect retired 2026-08-20.
REM   Lane ran a second paper_engine with 16 path overrides and 5 config overrides.
REM   Last shadow fill 20260721; last shadow BUY 20260715 - one month with zero fills,
REM   undetected because its status files kept refreshing daily.
REM   Artifacts frozen under backup\20260820_shadow_collect_retire\ (see README there).
REM   Reason it stopped is NOT known. Re-enable with PAPER_SHADOW_ENABLED=1.
REM   Review by 2026-11-20: remove the block entirely if still unused.
REM   Record: .agent\PLANS.md 2026-08-20
if "%PAPER_SHADOW_ENABLED%"=="" set "PAPER_SHADOW_ENABLED=0"
if "%PAPER_SHADOW_MAX_NEW%"=="" set "PAPER_SHADOW_MAX_NEW=6"
if "%PAPER_SHADOW_GAP_UP_MAX_PCT%"=="" set "PAPER_SHADOW_GAP_UP_MAX_PCT=0.07"
if "%PAPER_SHADOW_GAP_DOWN_STOP_PCT%"=="" set "PAPER_SHADOW_GAP_DOWN_STOP_PCT=0.05"
if "%PAPER_SHADOW_ENABLED%"=="1" (
  call :LOG_STEP_BEGIN "[7.099/9] gate_daily.py pre-shadow latest P0"
  "%PY%" gate_daily.py >> "%LAST_LOG%" 2>&1
  set "RC=%ERRORLEVEL%"
  call :LOG_STEP_END "[7.099/9]" %RC%
  if not "%RC%"=="0" (
    echo [WARN] gate_daily pre-shadow failed - continuing with paper_engine fail-closed alignment guard
  )
  echo [7.1/9] paper_engine.py shadow_collect
  call :LOG_MSG "[7.1/9] paper_engine shadow START run_label=shadow"
  set "PAPER_RUN_LABEL=shadow"
  set "PAPER_FILLS_PATH=%ROOT%paper\fills_shadow.csv"
  set "PAPER_TRADES_PATH=%ROOT%paper\trades_shadow.csv"
  set "PAPER_STATE_PATH=%ROOT%paper\paper_state_shadow.json"
  set "PAPER_PENDING_SIGNALS_PATH=%ROOT%2_Logs\pending_entry_signals_shadow_latest.csv"
  set "PAPER_ENTRY_SIGNAL_SNAPSHOT_PATH=%ROOT%2_Logs\entry_signal_snapshot_shadow_latest.csv"
  set "PAPER_ENTRY_DECISION_LAYERS_RUNTIME_CSV_PATH=%ROOT%2_Logs\entry_decision_layers_runtime_shadow_latest.csv"
  set "PAPER_ENTRY_DECISION_LAYERS_RUNTIME_JSON_PATH=%ROOT%2_Logs\entry_decision_layers_runtime_shadow_latest.json"
  set "PAPER_PENDING_STATUS_PATH=%ROOT%2_Logs\pending_entry_status_shadow_latest.json"
  set "PAPER_P1_GATE_STATUS_PATH=%ROOT%2_Logs\p1_entry_gate_status_shadow_latest.json"
  set "PAPER_P1_GATE_STATUS_HISTORY_PATH=%ROOT%2_Logs\p1_entry_gate_status_shadow_history.csv"
  set "PAPER_RECOVERY_STATUS_PATH=%ROOT%2_Logs\paper_recovery_status_shadow_latest.json"
  set "PAPER_SELL_VALIDATION_REPORT_PATH=%ROOT%2_Logs\sell_validation_report_shadow_latest.json"
  set "PAPER_ORDER_VALIDATION_REPORT_PATH=%ROOT%2_Logs\paper_order_validation_report_shadow_latest.json"
  set "PAPER_DDM_STATUS_PATH=%ROOT%2_Logs\paper_ddm_status_shadow_latest.json"
  set "PAPER_OPS_ALERT_PATH=%ROOT%2_Logs\market_ops_alert_shadow_latest.json"
  set "FILLS_IDEMPOTENCY_STATE=%ROOT%state\fills_idempotency_shadow.json"
  set "PAPER_CFG_OVERRIDES=max_new_trades_per_day=%PAPER_SHADOW_MAX_NEW%;gap_up_max_pct=%PAPER_SHADOW_GAP_UP_MAX_PCT%;entry_gap_down_stop_pct=%PAPER_SHADOW_GAP_DOWN_STOP_PCT%;max_per_sector=0;regime_entry_policy.crash_force_block=false"
  "%PY%" paper_engine.py >> "%LAST_LOG%" 2>&1
  if errorlevel 1 (
    echo [WARN] paper_engine shadow_collect failed - continuing
    call :LOG_MSG "[7.1/9] paper_engine shadow END rc=1 run_label=shadow"
  ) else (
    call :LOG_MSG "[7.1/9] paper_engine shadow END rc=0 run_label=shadow"
  )
  set "PAPER_RUN_LABEL="
  set "PAPER_FILLS_PATH="
  set "PAPER_TRADES_PATH="
  set "PAPER_STATE_PATH="
  set "PAPER_PENDING_SIGNALS_PATH="
  set "PAPER_ENTRY_SIGNAL_SNAPSHOT_PATH="
  set "PAPER_ENTRY_DECISION_LAYERS_RUNTIME_CSV_PATH="
  set "PAPER_ENTRY_DECISION_LAYERS_RUNTIME_JSON_PATH="
  set "PAPER_PENDING_STATUS_PATH="
  set "PAPER_P1_GATE_STATUS_PATH="
  set "PAPER_P1_GATE_STATUS_HISTORY_PATH="
  set "PAPER_RECOVERY_STATUS_PATH="
  set "PAPER_SELL_VALIDATION_REPORT_PATH="
  set "PAPER_ORDER_VALIDATION_REPORT_PATH="
  set "PAPER_DDM_STATUS_PATH="
  set "PAPER_OPS_ALERT_PATH="
  set "FILLS_IDEMPOTENCY_STATE="
  set "PAPER_CFG_OVERRIDES="
) else (
  echo [SKIP] shadow_collect disabled PAPER_SHADOW_ENABLED=%PAPER_SHADOW_ENABLED%
)
REM ------------------------------------------------------------
REM [8/9] audit daily
REM ------------------------------------------------------------
set "LAST_STEP_LABEL=[8/9] audit_daily.py"
echo [8/9] audit_daily.py
"%PY%" audit_daily.py
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [9/9] after close summary
REM ------------------------------------------------------------
set "LAST_STEP_LABEL=[9/9] after_close_summary.cmd"
echo [9/9] after_close_summary.cmd
if "%PAPER_DAILY_AFTER_CLOSE_LOG_CLEANUP_TIMEOUT_SEC%"=="" set "PAPER_DAILY_AFTER_CLOSE_LOG_CLEANUP_TIMEOUT_SEC=180"
if "%LOG_CLEANUP_TIMEOUT_SEC%"=="" set "LOG_CLEANUP_TIMEOUT_SEC=%PAPER_DAILY_AFTER_CLOSE_LOG_CLEANUP_TIMEOUT_SEC%"
echo [AFTER_CLOSE] LOG_CLEANUP_TIMEOUT_SEC=%LOG_CLEANUP_TIMEOUT_SEC%
call :LOG_STEP_BEGIN "[9/9] after_close_summary.cmd"
call :RUN_BATCH_ISOLATED "%ROOT%after_close_summary.cmd"
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[9/9]" %RC%
if not "%RC%"=="0" goto :FAILED

REM ------------------------------------------------------------
REM [10/14] derive D (execution date)
REM - TODAY  : use run date
REM - FILLS  : use latest fills ymd
REM - HYBRID : use max(TODAY, FILLS)
REM ------------------------------------------------------------
if "%PAPER_EXEC_DATE_MODE%"=="" set "PAPER_EXEC_DATE_MODE=FILLS"
set "D_TODAY=%RUN_TS:~0,4%%RUN_TS:~5,2%%RUN_TS:~8,2%"
set "D_FILLS="
if exist "%ROOT%paper\fills.csv" (
  for /f %%I in ('powershell -NoProfile -ExecutionPolicy Bypass -File "%ROOT%tools\derive_d_from_fills.ps1" -CsvPath "%ROOT%paper\fills.csv"') do set "D_FILLS=%%I"
)
if /I "%PAPER_EXEC_DATE_MODE%"=="FILLS" (
  if "%D_FILLS%"=="" (
    echo [FAILED] PAPER_EXEC_DATE_MODE=FILLS but cannot infer from fills.csv
    goto :FAILED
  )
  set "D=%D_FILLS%"
) else if /I "%PAPER_EXEC_DATE_MODE%"=="HYBRID" (
  set "D=%D_TODAY%"
  if not "%D_FILLS%"=="" (
    set "D=%D_FILLS%"
    findstr /R /C:"^%D_TODAY%.*,BUY," "%ROOT%paper\fills.csv" >nul 2>nul
    if not errorlevel 1 (
      set "D=%D_TODAY%"
    )
  )
) else (
  set "D=%D_TODAY%"
)
set "VIBE_EXEC_MODE=A"
set "LAST_STEP_LABEL=[10/14] derive D"
echo [10/14] derive D mode=%PAPER_EXEC_DATE_MODE% today=%D_TODAY% fills=%D_FILLS% -> D=%D%

REM ------------------------------------------------------------
REM [11/14] onepass + ledger append (A-mode)
REM ------------------------------------------------------------
echo [11/14] p0_onepass_from_fills.py + ledger_append D=%D% (VIBE_EXEC_MODE=%VIBE_EXEC_MODE%)
set "LAST_STEP_LABEL=[11/14] p0_onepass_from_fills.py"
"%PY%" tools\p0_onepass_from_fills.py %D%
if errorlevel 1 goto :FAILED

set "LAST_STEP_LABEL=[11/14] ledger_append_from_orders_exec.py"
"%PY%" tools\ledger_append_from_orders_exec.py %D% --apply
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [12/14] contract check: exec_date_unique must be D
REM ------------------------------------------------------------
echo [12/14] contract exec_date==D for orders_%D%_exec.xlsx
set "LAST_STEP_LABEL=[12/14] contract exec_date==D"
"%PY%" -c "import pandas as pd,sys; D=r'%D%'; p=rf'%ROOT%paper\orders_{D}_exec.xlsx'; df=pd.read_excel(p); u=sorted(df['exec_date'].astype(str).unique().tolist()); print('exec_date_unique',u); sys.exit(0 if u==[D] else 2)" >> "%LAST_LOG%" 2>&1
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [12.1/14] chain date contract: D == fills(D_by_rule) == orders_exec_date
REM ------------------------------------------------------------
echo [12.1/14] chain date contract (D/orders/fills)
set "LAST_STEP_LABEL=[12.1/14] chain date contract"
set "D_RULE_FILLS="
if exist "%ROOT%paper\fills.csv" (
  for /f %%I in ('powershell -NoProfile -ExecutionPolicy Bypass -File "%ROOT%tools\derive_d_from_fills.ps1" -CsvPath "%ROOT%paper\fills.csv"') do set "D_RULE_FILLS=%%I"
)
echo [CHAIN_D] D=%D% fills_D_by_rule=%D_RULE_FILLS%
if /I "%PAPER_EXEC_DATE_MODE%"=="FILLS" (
  if "%D_RULE_FILLS%"=="" goto :FAILED
  if /I not "%D_RULE_FILLS%"=="%D%" goto :FAILED
) else if /I "%PAPER_EXEC_DATE_MODE%"=="HYBRID" (
  setlocal EnableDelayedExpansion
  set "D_EXPECT=%D_TODAY%"
  if not "%D_RULE_FILLS%"=="" (
    set "D_EXPECT=%D_RULE_FILLS%"
    findstr /R /C:"^%D_TODAY%.*,BUY," "%ROOT%paper\fills.csv" >nul 2>nul
    if not errorlevel 1 (
      set "D_EXPECT=%D_TODAY%"
    )
  )
  echo [CHAIN_D] HYBRID expected_D=!D_EXPECT!
  if /I not "%D%"=="!D_EXPECT!" (
    endlocal
    goto :FAILED
  )
  endlocal
) else (
  if /I not "%D%"=="%D_TODAY%" goto :FAILED
)

REM ------------------------------------------------------------
REM [12.2/14] execution safety contract (shadow/read-only)
REM ------------------------------------------------------------
echo [12.2/14] tools\build_execution_safety_contract.py --date %D%
set "LAST_STEP_LABEL=[12.2/14] execution safety contract"
"%PY%" tools\build_execution_safety_contract.py --date %D%
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [12.5/14] optional broker dispatch/sync (default OFF)
REM BROKER_MODE: OFF | DRY | APPLY | APPLY_SYNC
REM ------------------------------------------------------------
if "%BROKER_MODE%"=="" set "BROKER_MODE=DRY"
if "%BROKER_VALIDATION_MODE%"=="" set "BROKER_VALIDATION_MODE=0"
if "%BROKER_BLOCK_PREFIXES%"=="" set "BROKER_BLOCK_PREFIXES=CAP_"
if "%BROKER_MIN_LIVE_ORDERS%"=="" set "BROKER_MIN_LIVE_ORDERS=3"
if "%BROKER_MOCK%"=="" set "BROKER_MOCK=true"
if "%BROKER_ORDER_TYPE%"=="" set "BROKER_ORDER_TYPE=limit"
if "%BROKER_CANCEL_OPEN%"=="" set "BROKER_CANCEL_OPEN=0"
if "%BROKER_SYNC_AFTER_DISPATCH%"=="" set "BROKER_SYNC_AFTER_DISPATCH=1"
if "%BROKER_CANCEL_MIN_AGE_MINUTES%"=="" set "BROKER_CANCEL_MIN_AGE_MINUTES=10"
if "%BROKER_CANCEL_MAX_ORDERS%"=="" set "BROKER_CANCEL_MAX_ORDERS=0"
if "%BROKER_PREFLIGHT_KIS%"=="" set "BROKER_PREFLIGHT_KIS=1"
if "%BROKER_PREFLIGHT_CODE%"=="" set "BROKER_PREFLIGHT_CODE=005930"
if "%BROKER_PREFLIGHT_CHECK_BALANCE%"=="" set "BROKER_PREFLIGHT_CHECK_BALANCE=1"
if "%BROKER_PREFLIGHT_CHECK_OPEN_ORDERS%"=="" set "BROKER_PREFLIGHT_CHECK_OPEN_ORDERS=0"
if "%BROKER_PREFLIGHT_NOTIFY%"=="" set "BROKER_PREFLIGHT_NOTIFY=1"
if "%BROKER_CONFIRM%"=="" set "BROKER_CONFIRM="
if /I "%BROKER_VALIDATION_MODE%"=="1" if /I "%BROKER_MOCK%"=="auto" set "BROKER_MOCK=true"
if /I "%BROKER_VALIDATION_MODE%"=="1" (
  if /I "%BROKER_MODE%"=="APPLY" (
    echo [FAILED] BROKER_VALIDATION_MODE=1 cannot be used with BROKER_MODE=APPLY
    goto :FAILED
  )
  if /I "%BROKER_MODE%"=="APPLY_SYNC" (
    echo [FAILED] BROKER_VALIDATION_MODE=1 cannot be used with BROKER_MODE=APPLY_SYNC
    goto :FAILED
  )
)
set "BROKER_VALID_ARGS="
if /I "%BROKER_VALIDATION_MODE%"=="1" set "BROKER_VALID_ARGS=--validation-mode --allow-block-prefixes %BROKER_BLOCK_PREFIXES% --min-live-orders %BROKER_MIN_LIVE_ORDERS%"
if "%BROKER_D%"=="" set "BROKER_D=%D%"
if "%BROKER_LIVE_PATH%"=="" set "BROKER_LIVE_PATH=E:\vibe\buffett\data\live\live_fills.csv"
set "BROKER_CANCEL_ARGS=--date %BROKER_D% --mock %BROKER_MOCK% --min-age-minutes %BROKER_CANCEL_MIN_AGE_MINUTES% --max-cancels %BROKER_CANCEL_MAX_ORDERS%"
set "BROKER_PREFLIGHT_ARGS=--date %BROKER_D% --mock %BROKER_MOCK% --code %BROKER_PREFLIGHT_CODE%"
if "%BROKER_PREFLIGHT_CHECK_BALANCE%"=="1" set "BROKER_PREFLIGHT_ARGS=%BROKER_PREFLIGHT_ARGS% --check-balance"
if "%BROKER_PREFLIGHT_CHECK_OPEN_ORDERS%"=="1" set "BROKER_PREFLIGHT_ARGS=%BROKER_PREFLIGHT_ARGS% --check-open-orders"
if "%BROKER_PREFLIGHT_NOTIFY%"=="1" set "BROKER_PREFLIGHT_ARGS=%BROKER_PREFLIGHT_ARGS% --notify-on-fail"
if not "%D%"=="%BROKER_D%" echo [INFO] replay D=%D% broker D=%BROKER_D%
set "LAST_STEP_LABEL=[12.5/14] broker dispatch/sync"
echo [12.5/14] broker mode=%BROKER_MODE% validation=%BROKER_VALIDATION_MODE% mock=%BROKER_MOCK% order_type=%BROKER_ORDER_TYPE% cancel_open=%BROKER_CANCEL_OPEN% preflight=%BROKER_PREFLIGHT_KIS%
if /I "%BROKER_MODE%"=="OFF" (
  echo [SKIP] broker dispatch BROKER_MODE=OFF
) else (
  if "%BROKER_PREFLIGHT_KIS%"=="1" (
    if /I "%BROKER_MODE%"=="DRY" (
      echo [BROKER] preflight skipped in DRY mode
    ) else (
      echo [BROKER] preflight KIS healthcheck
      "%PY%" tools\kis_healthcheck.py %BROKER_PREFLIGHT_ARGS%
      if errorlevel 1 (
        if /I "%BROKER_MOCK%"=="true" (
          echo [WARN] broker preflight failed in MOCK mode. continue.
        ) else (
          echo [FAILED] broker preflight failed. blocking order dispatch.
          goto :FAILED
        )
      )
    )
  )
  if /I "%BROKER_MODE%"=="DRY" (
    "%PY%" tools\kis_order_dispatch_from_exec.py --date %BROKER_D% --mock %BROKER_MOCK% --order-type %BROKER_ORDER_TYPE% %BROKER_VALID_ARGS%
    if errorlevel 1 goto :FAILED
    if "%BROKER_CANCEL_OPEN%"=="1" (
      "%PY%" tools\kis_cancel_open_orders.py %BROKER_CANCEL_ARGS%
      if errorlevel 1 goto :FAILED
    )
    if "%BROKER_SYNC_AFTER_DISPATCH%"=="1" (
      "%PY%" tools\kis_sync_fills_from_api.py --date %BROKER_D% --mock %BROKER_MOCK% --bridge-write --bridge-live-path "%BROKER_LIVE_PATH%"
      if errorlevel 1 goto :FAILED
      if not exist "%ROOT%2_Logs\kis_fills_sync_%BROKER_D%.json" (
        echo [FAILED] missing broker sync summary: %ROOT%2_Logs\kis_fills_sync_%BROKER_D%.json
        goto :FAILED
      )
      echo [BROKER] verify bridge-write summary=%ROOT%2_Logs\kis_fills_sync_%BROKER_D%.json
      "%PY%" -c "import json,sys; p=r'%ROOT%2_Logs\kis_fills_sync_%BROKER_D%.json'; j=json.load(open(p,'r',encoding='utf-8')); bw=bool(j.get('bridge_write')); b=j.get('bridge') or {}; ok=bw and isinstance(b,dict) and bool(b.get('ledger')); print('[BROKER_SYNC_CHECK] bridge_write=',bw,'has_bridge_ledger=',bool((b.get('ledger') if isinstance(b,dict) else None))); sys.exit(0 if ok else 3)"
      if errorlevel 1 (
        echo [FAILED] broker sync bridge-write verification failed
        goto :FAILED
      )
      echo [BROKER] verify ledger as_of=%BROKER_D% reflected when normalized fills exist
      "%PY%" -c "import csv,json,os,sys; d=str('%BROKER_D%'); p=r'%ROOT%2_Logs\kis_fills_sync_%BROKER_D%.json'; j=json.load(open(p,'r',encoding='utf-8')); rn=int(j.get('rows_normalized') or 0); b=j.get('bridge') or {}; lg=(b.get('ledger') or {}) if isinstance(b,dict) else {}; lp=str(lg.get('ledger_path') or r'E:\vibe\buffett\data\ledger\paper_fills_ledger.csv'); rows=0; rows=(sum(1 for r in csv.DictReader(open(lp,encoding='utf-8-sig',newline='')) if str(r.get('as_of',''))[:8]==d) if os.path.exists(lp) else 0); ok=(rn==0) or (rows>0); print('[BROKER_LEDGER_CHECK] D=',d,'rows_normalized=',rn,'ledger_asof_rows=',rows,'ledger_path=',lp); sys.exit(0 if ok else 4)"
      if errorlevel 1 (
        echo [FAILED] broker ledger as_of verification failed
        goto :FAILED
      )
    )
  ) else (
    if /I "%BROKER_MODE%"=="APPLY" (
      if /I not "%BROKER_CONFIRM%"=="LIVE_APPLY" (
        echo [FAILED] BROKER_MODE=APPLY requires BROKER_CONFIRM=LIVE_APPLY
        goto :FAILED
      )
      "%PY%" tools\kis_order_dispatch_from_exec.py --date %BROKER_D% --mock %BROKER_MOCK% --order-type %BROKER_ORDER_TYPE% %BROKER_VALID_ARGS% --apply
      if errorlevel 1 goto :FAILED
      if "%BROKER_CANCEL_OPEN%"=="1" (
        "%PY%" tools\kis_cancel_open_orders.py %BROKER_CANCEL_ARGS% --apply
        if errorlevel 1 goto :FAILED
      )
      if "%BROKER_SYNC_AFTER_DISPATCH%"=="1" (
        "%PY%" tools\kis_sync_fills_from_api.py --date %BROKER_D% --mock %BROKER_MOCK% --bridge-write --bridge-live-path "%BROKER_LIVE_PATH%"
        if errorlevel 1 goto :FAILED
        if not exist "%ROOT%2_Logs\kis_fills_sync_%BROKER_D%.json" (
          echo [FAILED] missing broker sync summary: %ROOT%2_Logs\kis_fills_sync_%BROKER_D%.json
          goto :FAILED
        )
        echo [BROKER] verify bridge-write summary=%ROOT%2_Logs\kis_fills_sync_%BROKER_D%.json
        "%PY%" -c "import json,sys; p=r'%ROOT%2_Logs\kis_fills_sync_%BROKER_D%.json'; j=json.load(open(p,'r',encoding='utf-8')); bw=bool(j.get('bridge_write')); b=j.get('bridge') or {}; ok=bw and isinstance(b,dict) and bool(b.get('ledger')); print('[BROKER_SYNC_CHECK] bridge_write=',bw,'has_bridge_ledger=',bool((b.get('ledger') if isinstance(b,dict) else None))); sys.exit(0 if ok else 3)"
        if errorlevel 1 (
          echo [FAILED] broker sync bridge-write verification failed
          goto :FAILED
        )
        echo [BROKER] verify ledger as_of=%BROKER_D% reflected when normalized fills exist
        "%PY%" -c "import csv,json,os,sys; d=str('%BROKER_D%'); p=r'%ROOT%2_Logs\kis_fills_sync_%BROKER_D%.json'; j=json.load(open(p,'r',encoding='utf-8')); rn=int(j.get('rows_normalized') or 0); b=j.get('bridge') or {}; lg=(b.get('ledger') or {}) if isinstance(b,dict) else {}; lp=str(lg.get('ledger_path') or r'E:\vibe\buffett\data\ledger\paper_fills_ledger.csv'); rows=0; rows=(sum(1 for r in csv.DictReader(open(lp,encoding='utf-8-sig',newline='')) if str(r.get('as_of',''))[:8]==d) if os.path.exists(lp) else 0); ok=(rn==0) or (rows>0); print('[BROKER_LEDGER_CHECK] D=',d,'rows_normalized=',rn,'ledger_asof_rows=',rows,'ledger_path=',lp); sys.exit(0 if ok else 4)"
        if errorlevel 1 (
          echo [FAILED] broker ledger as_of verification failed
          goto :FAILED
        )
      )
      ) else (
        if /I "%BROKER_MODE%"=="APPLY_SYNC" (
        if /I not "%BROKER_CONFIRM%"=="LIVE_APPLY" (
          echo [FAILED] BROKER_MODE=APPLY_SYNC requires BROKER_CONFIRM=LIVE_APPLY
          goto :FAILED
        )
        "%PY%" tools\kis_order_dispatch_from_exec.py --date %BROKER_D% --mock %BROKER_MOCK% --order-type %BROKER_ORDER_TYPE% %BROKER_VALID_ARGS% --apply
        if errorlevel 1 goto :FAILED
        if "%BROKER_CANCEL_OPEN%"=="1" (
          "%PY%" tools\kis_cancel_open_orders.py %BROKER_CANCEL_ARGS% --apply
          if errorlevel 1 goto :FAILED
        )
        "%PY%" tools\kis_sync_fills_from_api.py --date %BROKER_D% --mock %BROKER_MOCK% --bridge-write --bridge-live-path "%BROKER_LIVE_PATH%"
        if errorlevel 1 goto :FAILED
        if not exist "%ROOT%2_Logs\kis_fills_sync_%BROKER_D%.json" (
          echo [FAILED] missing broker sync summary: %ROOT%2_Logs\kis_fills_sync_%BROKER_D%.json
          goto :FAILED
        )
        echo [BROKER] verify bridge-write summary=%ROOT%2_Logs\kis_fills_sync_%BROKER_D%.json
        "%PY%" -c "import json,sys; p=r'%ROOT%2_Logs\kis_fills_sync_%BROKER_D%.json'; j=json.load(open(p,'r',encoding='utf-8')); bw=bool(j.get('bridge_write')); b=j.get('bridge') or {}; ok=bw and isinstance(b,dict) and bool(b.get('ledger')); print('[BROKER_SYNC_CHECK] bridge_write=',bw,'has_bridge_ledger=',bool((b.get('ledger') if isinstance(b,dict) else None))); sys.exit(0 if ok else 3)"
        if errorlevel 1 (
          echo [FAILED] broker sync bridge-write verification failed
          goto :FAILED
        )
        echo [BROKER] verify ledger as_of=%BROKER_D% reflected when normalized fills exist
        "%PY%" -c "import csv,json,os,sys; d=str('%BROKER_D%'); p=r'%ROOT%2_Logs\kis_fills_sync_%BROKER_D%.json'; j=json.load(open(p,'r',encoding='utf-8')); rn=int(j.get('rows_normalized') or 0); b=j.get('bridge') or {}; lg=(b.get('ledger') or {}) if isinstance(b,dict) else {}; lp=str(lg.get('ledger_path') or r'E:\vibe\buffett\data\ledger\paper_fills_ledger.csv'); rows=0; rows=(sum(1 for r in csv.DictReader(open(lp,encoding='utf-8-sig',newline='')) if str(r.get('as_of',''))[:8]==d) if os.path.exists(lp) else 0); ok=(rn==0) or (rows>0); print('[BROKER_LEDGER_CHECK] D=',d,'rows_normalized=',rn,'ledger_asof_rows=',rows,'ledger_path=',lp); sys.exit(0 if ok else 4)"
        if errorlevel 1 (
          echo [FAILED] broker ledger as_of verification failed
          goto :FAILED
        )
      ) else (
        echo [FAILED] invalid BROKER_MODE=%BROKER_MODE%
        goto :FAILED
      )
    )
  )
)

REM ------------------------------------------------------------
REM [12.6/14] append broker auto-dispatch fills to virtual ledger
REM - Only AUTO_DISPATCH rows (system-dispatched accepted orders) are reflected.
REM - Manual broker trades stay outside strategy P&L attribution.
REM - SELL rows close matching open positions; BUY rows append with LIVE_BROKER group.
REM ------------------------------------------------------------
if /I "%BROKER_MODE%"=="APPLY" (
  set "LAST_STEP_LABEL=[12.6/14] ledger_append_from_orders_exec.py --from-live-fills"
  echo [12.6/14] ledger_append_from_orders_exec.py --apply --from-live-fills %D%
  "%PY%" tools\ledger_append_from_orders_exec.py %D% --apply --from-live-fills "%BROKER_LIVE_PATH%"
  if errorlevel 1 goto :FAILED
)
if /I "%BROKER_MODE%"=="APPLY_SYNC" (
  set "LAST_STEP_LABEL=[12.6/14] ledger_append_from_orders_exec.py --from-live-fills"
  echo [12.6/14] ledger_append_from_orders_exec.py --apply --from-live-fills %D%
  "%PY%" tools\ledger_append_from_orders_exec.py %D% --apply --from-live-fills "%BROKER_LIVE_PATH%"
  if errorlevel 1 goto :FAILED
)

set "LAST_STEP_LABEL=[POST_CHAIN] RUN_POST_CHAIN"
call :RUN_POST_CHAIN
if errorlevel 1 goto :FAILED
goto :POST_CHAIN_DONE

:RUN_POST_CHAIN
REM ------------------------------------------------------------
REM [13/14] paper_sync (fills_norm/trades_calc/pnl)
REM ------------------------------------------------------------
echo [13/14] paper_sync.py
"%PY%" paper_sync.py
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[13/14] paper_sync.py"
)

REM ------------------------------------------------------------
REM [13.5/16] policy effect tracking report (fail-soft)
REM ------------------------------------------------------------
echo [13.5/16] tools\build_policy_effect_tracking_report.py
"%PY%" tools\build_policy_effect_tracking_report.py
if errorlevel 1 (
  echo [WARN] policy effect tracking report failed - continuing
)

REM ------------------------------------------------------------
REM [14/16] signal integration daily (phase2/3/final as-of join)
REM ------------------------------------------------------------
echo [14/16] tools\signal_integration_daily.py
"%PY%" tools\signal_integration_daily.py
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[14/16] tools\\signal_integration_daily.py"
)

REM ------------------------------------------------------------
REM [14.5/16] refresh backtest summary if due (freshness gate)
REM ------------------------------------------------------------
echo [14.5/16] report_if_due_v41_1.py
"%PY%" report_if_due_v41_1.py
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[14.5/16] report_if_due_v41_1.py"
)

REM ------------------------------------------------------------
REM [15/16] live-vs-backtest feedback + auto optimize trigger
REM ------------------------------------------------------------
echo [15/16] live_vs_bt_paper_daily.py --date %D% --auto-optimize (aligned+gated, min_stable_score=%LVB_MIN_STABLE_SCORE%)
"%PY%" live_vs_bt_paper_daily.py --date %D% --auto-optimize --align-window-trades 30 --min-shared-trades 10 --max-backtest-age-days 7 --min-oos-trades 20 --min-oos-pf 0.75 --min-stable-score %LVB_MIN_STABLE_SCORE%
if errorlevel 1 (
  echo [WARN] live_vs_bt_paper_daily failed - continuing
)

REM ------------------------------------------------------------
REM [15.5/16] indicator diag + param recommendation (fail-soft)
REM ------------------------------------------------------------
if "%INDICATOR_DIAG_AUTO%"=="" set "INDICATOR_DIAG_AUTO=1"
echo [INDICATOR_DIAG] auto=%INDICATOR_DIAG_AUTO%
if "%INDICATOR_DIAG_AUTO%"=="1" (
  echo [15.5/16] tools\indicator_diag_and_recommend.py
  "%PY%" tools\run_step_with_timeout.py --name indicator_diag_and_recommend --timeout-sec 300 --status-json 2_Logs\indicator_diag_timeout_status_latest.json -- "%PY%" tools\indicator_diag_and_recommend.py --lookback-days 120 --min-universe 2000 --horizons 1,2,5 >> "%LAST_LOG%" 2>&1
  if errorlevel 1 (
    echo [WARN] indicator_diag_and_recommend failed - continuing
  )
) else (
  echo [SKIP] indicator diag (INDICATOR_DIAG_AUTO=%INDICATOR_DIAG_AUTO%)
)

REM [16/16] pending report (JSON-based verdict; allow same-day pending)
echo [16/16] paper_pending_report.py + JSON verdict (pending==0)
"%PY%" tools\paper_pending_report.py
"%PY%" tools\paper_pending_verdict.py
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16/16] pending_report verdict"
)

echo.
REM [16.5/16] pending queue delta report (day-over-day)
echo [16.5/16] tools\pending_queue_delta_report.py
"%PY%" tools\pending_queue_delta_report.py
if errorlevel 1 (
  echo [WARN] pending_queue_delta_report failed - continuing
)

REM [16.6/16] kill-switch validation report (shadow score, fail-soft)
echo [16.6/16] tools\kill_switch_validation_report.py
"%PY%" tools\kill_switch_validation_report.py
if errorlevel 1 (
  echo [WARN] kill_switch_validation_report failed - continuing
)

REM [16.7/16] sync orders_exec to RootB, ensure today's SSOT FINAL pointer, then point config.stats_dir
echo [16.7/16] E:\vibe\buffett\tools\ssot_sync_orders_exec.py %D% --apply --force
"%PY%" E:\vibe\buffett\tools\ssot_sync_orders_exec.py %D% --apply --force
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.7/16] ssot_sync_orders_exec.py"
)
echo [16.7a/16] E:\vibe\buffett\tools\ssot_today_final_update.py
"%PY%" E:\vibe\buffett\tools\ssot_today_final_update.py
if errorlevel 1 (
  echo [WARN] SSOT_TODAY missing today's FINAL snapshot -> build snapshot
  echo [16.7a/16] refresh RootB data\stats before snapshot build
  call :RUN_ROOTB_STATS
  if errorlevel 1 (
    call :POST_CHAIN_STEP_FAIL "[16.7a/16] vibe_generate_stats_p0.py"
  )
  "%PY%" E:\vibe\buffett\tools\ssot_snapshot_final_build_v1.py
  if errorlevel 1 (
    call :POST_CHAIN_STEP_FAIL "[16.7a/16] ssot_snapshot_final_build_v1.py"
  )
  "%PY%" E:\vibe\buffett\tools\ssot_today_final_update.py
  if errorlevel 1 (
    call :POST_CHAIN_STEP_FAIL "[16.7a/16] ssot_today_final_update.py (retry)"
  )
)

REM [16.75/16] streaming calibration gate (fail-closed on FAIL)
REM ------------------------------------------------------------
REM [2026-08-31] DISABLED by user decision (B). nothing deleted, artifacts kept.
REM   full reasoning: .agent/PLANS.md (178)(179)
REM   why: this step ran daily but never worked.
REM     - input joined_trades stuck at 36 rows / 2026-02-05 (206 days).
REM       signal_integration_daily.py reads trades.csv but only to refresh pnl_krw;
REM       it never appends new trades (it rewrites the same file).
REM     - root cause: no final_score column exists anywhere.
REM       calibration_stream.py:51 defaults to 0.0 -> p = sigmoid(0) = 0.5 constant.
REM       a constant prediction leaves the calibration curve undefined.
REM   consumer: tools/safe_exploration_review.py only. penalty on FAIL only;
REM           current status is WARN so no effect. missing file -> UNKNOWN/0 (safe).
REM   revive when ALL three hold:
REM     (1) entry-time final_score is recorded in the trade ledger
REM     (2) a producer appends new trades into joined_trades
REM     (3) entries resume and a sample accumulates (2026-08 had 3 rows)
REM   how: set CALIB_STREAM_ENABLED=1 below. code and artifacts are untouched.
REM ------------------------------------------------------------
if not defined CALIB_STREAM_ENABLED set "CALIB_STREAM_ENABLED=0"
if "%CALIB_STREAM_ENABLED%"=="1" (
  echo [16.75/16] tools\calibration_stream.py
  "%PY%" tools\calibration_stream.py
  if errorlevel 1 (
    call :POST_CHAIN_STEP_FAIL "[16.75/16] tools\calibration_stream.py"
  )
) else (
  echo [16.75/16] skip calibration_stream ^(CALIB_STREAM_ENABLED=0, PLANS 179^)
)
REM [16.755/16] refresh after_close summary snapshot before dashboard build
echo [16.755/16] after_close_summary.py
"%PY%" after_close_summary.py
if errorlevel 1 (
  echo [WARN] after_close_summary failed - continuing with normalized dashboard fallback
)

REM [16.76/16] refresh market rising realtime snapshot before dashboard build
echo [16.76/16] E:\vibe\buffett\tools\market_rising_snapshot.py
if exist "E:\vibe\buffett\tools\market_rising_snapshot.py" (
  "%PY%" E:\vibe\buffett\tools\market_rising_snapshot.py
  if errorlevel 1 (
    echo [WARN] market_rising_snapshot failed - continuing with stale-safe dashboard fallback
  )
)

echo [16.7b/16] E:\vibe\buffett\tools\dashboard_point_today.py
"%PY%" E:\vibe\buffett\tools\dashboard_point_today.py
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.7b/16] dashboard_point_today.py"
)

REM [16.8/16] refresh dashboard runtime state after stats_dir repoint
echo [16.8/16] E:\vibe\buffett\tools\build_dashboard_state_v2.py
if exist "E:\vibe\buffett\tools\build_dashboard_state_v2.py" (
  "%PY%" E:\vibe\buffett\tools\build_dashboard_state_v2.py
  if errorlevel 1 (
    call :POST_CHAIN_STEP_FAIL "[16.8/16] build_dashboard_state_v2.py"
  )
) else (
  echo [16.8/16] E:\vibe\buffett\tools\build_dashboard_state.py
  "%PY%" E:\vibe\buffett\tools\build_dashboard_state.py
  if errorlevel 1 (
    call :POST_CHAIN_STEP_FAIL "[16.8/16] build_dashboard_state.py"
  )
)

REM [16.85/16] refresh trading stage validation right after successful daily pipeline
echo [16.85/16] run_trading_stage_validation_report.bat
set "TRVAL_OPEN=0"
call :LOG_STEP_BEGIN "[16.85/16] run_trading_stage_validation_report.bat"
call :RUN_BATCH_ISOLATED "%ROOT%run_trading_stage_validation_report.bat"
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[16.85/16]" %RC%
if not "%RC%"=="0" (
  call :POST_CHAIN_STEP_FAIL "[16.85/16] run_trading_stage_validation_report.bat"
)

REM [16.9/16] refresh paper fix cycle snapshot from latest trading stage validation
echo [16.9/16] tools\build_paper_fix_cycle_report.py
"%PY%" tools\build_paper_fix_cycle_report.py --cycle-index 0
set "PAPER_FIX_RC=%ERRORLEVEL%"
if "%PAPER_FIX_RC%"=="10" set "PAPER_FIX_RC=0"
if "%PAPER_FIX_RC%"=="11" set "PAPER_FIX_RC=0"
if not "%PAPER_FIX_RC%"=="0" (
  call :POST_CHAIN_STEP_FAIL "[16.9/16] build_paper_fix_cycle_report.py"
)

REM [16.92/16] refresh execution health observed snapshot (fills/trades based)
echo [16.92/16] tools\build_execution_health_observed.py
"%PY%" tools\build_execution_health_observed.py
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.92/16] build_execution_health_observed.py"
)

REM [16.922/16] repair live_fills rows missing from RootB ledger before monitor
echo [16.922/16] tools\repair_rootb_ledger_missing_live_fills.py --apply
"%PY%" tools\repair_rootb_ledger_missing_live_fills.py --start-ymd %PAPER_OPER_START_YMD% --apply
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.922/16] repair_rootb_ledger_missing_live_fills.py --apply"
)

REM [16.923/16] preserve RootA fill order_id in RootB live/ledger rows
echo [16.923/16] tools\repair_rootb_order_id_from_roota_fills.py --date %D% --apply
"%PY%" tools\repair_rootb_order_id_from_roota_fills.py --date %D% --apply
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.923/16] repair_rootb_order_id_from_roota_fills.py --apply"
)

REM [16.9230/16] align RootB live/ledger D rows from RootA fills.csv before stats
echo [16.9230/16] tools\repair_rootb_fills_from_roota_fills.py --date %D% --apply
"%PY%" tools\repair_rootb_fills_from_roota_fills.py --date %D% --apply
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.9230/16] repair_rootb_fills_from_roota_fills.py --apply"
)

REM [16.923a/16] refresh RootB stats and today's FINAL snapshot after ledger repairs
echo [16.923a/16] E:\vibe\buffett\vibe_generate_stats_p0.py after ledger repairs
call :RUN_ROOTB_STATS
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.923a/16] vibe_generate_stats_p0.py after ledger repairs"
)
echo [16.923b/16] E:\vibe\buffett\tools\ssot_snapshot_final_build_v1.py after ledger repairs
"%PY%" E:\vibe\buffett\tools\ssot_snapshot_final_build_v1.py
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.923b/16] ssot_snapshot_final_build_v1.py after ledger repairs"
)
echo [16.923c/16] E:\vibe\buffett\tools\ssot_today_final_update.py after ledger repairs
"%PY%" E:\vibe\buffett\tools\ssot_today_final_update.py
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.923c/16] ssot_today_final_update.py after ledger repairs"
)
echo [16.923d/16] E:\vibe\buffett\tools\dashboard_point_today.py after ledger repairs
"%PY%" E:\vibe\buffett\tools\dashboard_point_today.py
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.923d/16] dashboard_point_today.py after ledger repairs"
)
echo [16.923e/16] refresh dashboard state after ledger repairs
if exist "E:\vibe\buffett\tools\build_dashboard_state_v2.py" (
  "%PY%" E:\vibe\buffett\tools\build_dashboard_state_v2.py
  if errorlevel 1 (
    call :POST_CHAIN_STEP_FAIL "[16.923e/16] build_dashboard_state_v2.py after ledger repairs"
  )
) else (
  "%PY%" E:\vibe\buffett\tools\build_dashboard_state.py
  if errorlevel 1 (
    call :POST_CHAIN_STEP_FAIL "[16.923e/16] build_dashboard_state.py after ledger repairs"
  )
)

REM [16.924/16] refresh SSOT drift monitor snapshots (report-only)
echo [16.924/16] tools\build_drift_monitor.py --date %D%
"%PY%" tools\build_drift_monitor.py --date %D%
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.924/16] build_drift_monitor.py"
)

REM [16.925/16] dry-run check for live_fills rows missing from ledger
echo [16.925/16] tools\ledger_live_fills_dry_run_report.py
"%PY%" tools\ledger_live_fills_dry_run_report.py
if errorlevel 1 (
  echo [WARN] ledger live fills dry-run failed - run repair and recheck once
  "%PY%" tools\repair_rootb_ledger_missing_live_fills.py --start-ymd %PAPER_OPER_START_YMD% --apply
  if errorlevel 1 (
    call :POST_CHAIN_STEP_FAIL "[16.925/16] repair_rootb_ledger_missing_live_fills.py recheck"
  ) else (
    "%PY%" tools\ledger_live_fills_dry_run_report.py
    if errorlevel 1 (
      call :POST_CHAIN_STEP_FAIL "[16.925/16] ledger_live_fills_dry_run_report.py"
    )
  )
)

REM [16.94/16] refresh nightly data integrity latest in warn-only mode
echo [16.94/16] tools\nightly_data_integrity_check.py --warn-only
"%PY%" tools\nightly_data_integrity_check.py --warn-only
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.94/16] nightly_data_integrity_check.py --warn-only"
)

REM [16.945/16] validate KIS API paper-capital-100M start scope across operational reports
echo [16.945/16] tools\build_validation_scope_audit.py
"%PY%" tools\build_validation_scope_audit.py
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.945/16] build_validation_scope_audit.py"
)

REM [16.95/16] refresh integrated ops snapshot latest
echo [16.95/16] tools\build_integrated_ops_snapshot.py
set "INTEGRATED_OPS_UPDATE_PLANS=0"
"%PY%" tools\build_integrated_ops_snapshot.py
set "INTEGRATED_OPS_UPDATE_PLANS="
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.95/16] build_integrated_ops_snapshot.py"
)

REM [16.951/16] refresh dashboard state after integrated ops snapshot
echo [16.951/16] refresh dashboard state after integrated ops snapshot
if exist "E:\vibe\buffett\tools\build_dashboard_state_v2.py" (
  "%PY%" E:\vibe\buffett\tools\build_dashboard_state_v2.py
  if errorlevel 1 (
    call :POST_CHAIN_STEP_FAIL "[16.951/16] build_dashboard_state_v2.py after integrated ops snapshot"
  )
) else (
  "%PY%" E:\vibe\buffett\tools\build_dashboard_state.py
  if errorlevel 1 (
    call :POST_CHAIN_STEP_FAIL "[16.951/16] build_dashboard_state.py after integrated ops snapshot"
  )
)

REM [16.955/16] refresh paper parameter review (observation layer)
echo [16.955/16] tools\build_paper_parameter_review.py
"%PY%" tools\build_paper_parameter_review.py
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.955/16] build_paper_parameter_review.py"
)

REM [16.956/16] signal auto-tuner propose only (no apply)
if "%AUTO_SIGNAL_TUNER_AUTO%"=="" set "AUTO_SIGNAL_TUNER_AUTO=1"
echo [AUTO_SIGNAL_TUNER] auto=%AUTO_SIGNAL_TUNER_AUTO%
if "%AUTO_SIGNAL_TUNER_AUTO%"=="1" (
  echo [16.956/16] tools\auto_signal_tuner.py --mode propose
  "%PY%" tools\auto_signal_tuner.py --mode propose
  if errorlevel 1 (
    call :POST_CHAIN_STEP_FAIL "[16.956/16] auto_signal_tuner.py --mode propose"
  )
) else (
  echo [SKIP] auto_signal_tuner (AUTO_SIGNAL_TUNER_AUTO=%AUTO_SIGNAL_TUNER_AUTO%)
)

REM [16.957/16] surge param validator propose
echo [16.957/16] tools\surge_param_validator.py --mode propose
"%PY%" tools\surge_param_validator.py --mode propose
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.957/16] surge_param_validator.py --mode propose"
)

REM [16.957a/16] surge LOB ingest latest refresh
REM Refresh intraday price input before surge realtime consumers.
REM Without this, surge_detector_realtime.py can read a prior-session latest CSV
REM and fail the official post-chain with STALE_INTRADAY_DATE.
echo [16.956p/16] tools\intraday_price_snapshot.py
"%PY%" tools\intraday_price_snapshot.py --from-candidates --with-surge-universe --surge-universe-max 50 --max-total-codes 35 --mock auto --request-interval-sec 1.15
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.956p/16] intraday_price_snapshot.py"
)

REM [16.957a/16] surge LOB ingest latest refresh
echo [16.957a/16] tools\surge_lob_ingest.py
"%PY%" tools\surge_lob_ingest.py
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.957a/16] surge_lob_ingest.py"
)

REM [16.957a2/16] orderflow Hawkes-GLR observer latest refresh
echo [16.957a2/16] tools\orderflow_hawkes_glr.py
if "%ORDERFLOW_HAWKES_GLR_TIMEOUT_SEC%"=="" set "ORDERFLOW_HAWKES_GLR_TIMEOUT_SEC=180"
"%PY%" tools\run_step_with_timeout.py --name orderflow_hawkes_glr --timeout-sec %ORDERFLOW_HAWKES_GLR_TIMEOUT_SEC% --status-json 2_Logs\orderflow_hawkes_glr_timeout_status_latest.json -- "%PY%" tools\orderflow_hawkes_glr.py
if errorlevel 1 (
  echo [ORDERFLOW_HAWKES_GLR] observer_refresh_failed_but_ignored
)

REM [16.957b/16] surge ML realtime scoring latest refresh
echo [16.957b/16] tools\surge_ml_score_realtime.py
"%PY%" tools\surge_ml_score_realtime.py
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.957b/16] surge_ml_score_realtime.py"
)

REM [16.957c/16] surge realtime detector latest refresh
echo [16.957c/16] tools\surge_detector_realtime.py
"%PY%" tools\surge_detector_realtime.py
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.957c/16] surge_detector_realtime.py"
)

REM [16.957d/16] surge sanity shadow label latest refresh
echo [16.957d/16] tools\fast_surge_sanity_labeler.py
"%PY%" tools\fast_surge_sanity_labeler.py
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.957d/16] fast_surge_sanity_labeler.py"
)

REM [16.957d2/16] microstructure observe-only provisional/corroboration bundle
echo [16.957d2/16] tools\build_microstructure_observe_contract.py
"%PY%" tools\build_microstructure_observe_contract.py
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.957d2/16] build_microstructure_observe_contract.py"
)

REM [16.957e/16] final recovery SSOT chain report after late post-chain writers
echo [16.957e/16] tools\build_recovery_ssot_chain_report.py
"%PY%" tools\build_recovery_ssot_chain_report.py
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.957e/16] build_recovery_ssot_chain_report.py"
)

REM [16.957e2/16] strategy parameter optimization
echo [16.957e2/16] E:\vibe\buffett\tools\build_strategy_optimization_analyzer.py
if exist "E:\vibe\buffett\tools\build_strategy_optimization_analyzer.py" (
  "%PY%" E:\vibe\buffett\tools\build_strategy_optimization_analyzer.py
  if errorlevel 1 (
    call :POST_CHAIN_STEP_FAIL "[16.957e2/16] build_strategy_optimization_analyzer.py"
  )
)

REM [16.957f/16] final integrated ops snapshot after late post-chain writers
echo [16.957f/16] tools\build_integrated_ops_snapshot.py final
set "INTEGRATED_OPS_UPDATE_PLANS=0"
"%PY%" tools\build_integrated_ops_snapshot.py
set "INTEGRATED_OPS_UPDATE_PLANS="
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.957f/16] build_integrated_ops_snapshot.py final"
)

REM [16.957g/16] final dashboard state after final integrated ops snapshot
echo [16.957g/16] refresh dashboard state final
if exist "E:\vibe\buffett\tools\build_dashboard_state_v2.py" (
  "%PY%" E:\vibe\buffett\tools\build_dashboard_state_v2.py
  if errorlevel 1 (
    call :POST_CHAIN_STEP_FAIL "[16.957g/16] build_dashboard_state_v2.py final"
  )
) else (
  "%PY%" E:\vibe\buffett\tools\build_dashboard_state.py
  if errorlevel 1 (
    call :POST_CHAIN_STEP_FAIL "[16.957g/16] build_dashboard_state.py final"
  )
)

REM [16.958/16] surge param validator apply (opt-in)
if "%SURGE_PARAM_AUTO_APPLY%"=="1" (
  echo [16.958/16] tools\surge_param_validator.py --mode apply
  "%PY%" tools\surge_param_validator.py --mode apply
  if errorlevel 1 (
    call :POST_CHAIN_STEP_FAIL "[16.958/16] surge_param_validator.py --mode apply"
  )
)

REM [16.959/16] logic tier registry validation (read-only)
echo [16.959/16] tools\validate_logic_tier_registry.py
"%PY%" tools\validate_logic_tier_registry.py
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.959/16] validate_logic_tier_registry.py"
)

REM [16.96/16] JSON UTF8 encoding hygiene scan (fail-closed via post-chain policy)
echo [16.96/16] tools\scan_json_encoding_utf8.ps1 -FailOnFind
if "%JSON_ENCODING_SCAN_TIMEOUT_SEC%"=="" set "JSON_ENCODING_SCAN_TIMEOUT_SEC=600"
"%PY%" tools\run_step_with_timeout.py --name json_encoding_scan --timeout-sec %JSON_ENCODING_SCAN_TIMEOUT_SEC% --status-json 2_Logs\json_encoding_scan_timeout_status_latest.json -- powershell -NoProfile -ExecutionPolicy Bypass -File "%ROOT%tools\scan_json_encoding_utf8.ps1" -FailOnFind
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.96/16] scan_json_encoding_utf8.ps1"
)

if "%POST_CHAIN_FAILED%"=="1" (
  echo [WARN] post chain failed at %POST_CHAIN_FAILED_STEP%
  if "%POST_CHAIN_FAIL_SOFT%"=="1" (
    if /I "%BROKER_MODE%"=="DRY" (
      echo [WARN] POST_CHAIN_FAIL_SOFT=1 and BROKER_MODE=DRY -> continue main success
      exit /b 0
    )
    if /I "%BROKER_MODE%"=="OFF" (
      echo [WARN] POST_CHAIN_FAIL_SOFT=1 and BROKER_MODE=OFF -> continue main success
      exit /b 0
    )
    echo [FAILED] POST_CHAIN_FAIL_SOFT=1 is blocked for live broker mode=%BROKER_MODE%
    exit /b 1
  )
  echo [FAILED] post chain strict mode failure
  exit /b 1
)
exit /b 0

:POST_CHAIN_DONE

echo [OK] finished
if not defined RUN_TS set "RUN_TS=%DATE%_%TIME%"
>> "%LAST_LOG%" echo [OK] finished ts=%RUN_TS%
>> "%ROOT%2_Logs\run_paper_daily_wrapper_status.txt" echo [INNER_EXIT] rc=0 ts=%DATE% %TIME%
>> "%LAST_LOG%" echo [INNER_EXIT] rc=0
if defined LOCK_ACQUIRED del /f /q "%LOCK_DIR%\run.info" >nul 2>nul
if defined LOCK_ACQUIRED rmdir "%LOCK_DIR%" >nul 2>nul
popd
endlocal & exit /b 0

:TRY_PY_EXE
if defined PY goto :eof
if not exist "%~1" goto :eof
if exist "%PY_PROBE_FILE%" del /q "%PY_PROBE_FILE%" >nul 2>nul
"%~1" -c "import sys;print('PYOK')" > "%PY_PROBE_FILE%" 2>nul
set "PY_PROBE="
if exist "%PY_PROBE_FILE%" set /p PY_PROBE=<"%PY_PROBE_FILE%"
if /I "%PY_PROBE%"=="PYOK" set "PY=%~1"
if exist "%PY_PROBE_FILE%" del /q "%PY_PROBE_FILE%" >nul 2>nul
goto :eof

:TRY_PY_CMD
if defined PY goto :eof
where %~1 >nul 2>nul || goto :eof
if exist "%PY_PROBE_FILE%" del /q "%PY_PROBE_FILE%" >nul 2>nul
%~1 -c "import sys;print('PYOK')" > "%PY_PROBE_FILE%" 2>nul
set "PY_PROBE="
if exist "%PY_PROBE_FILE%" set /p PY_PROBE=<"%PY_PROBE_FILE%"
if /I "%PY_PROBE%"=="PYOK" set "PY=%~1"
if exist "%PY_PROBE_FILE%" del /q "%PY_PROBE_FILE%" >nul 2>nul
goto :eof

:TRY_PY_LAUNCHER
if defined PY goto :eof
where py >nul 2>nul || goto :eof
if exist "%PY_PROBE_FILE%" del /q "%PY_PROBE_FILE%" >nul 2>nul
py -3 -c "import sys;print('PYOK')" > "%PY_PROBE_FILE%" 2>nul
set "PY_PROBE="
if exist "%PY_PROBE_FILE%" set /p PY_PROBE=<"%PY_PROBE_FILE%"
if /I "%PY_PROBE%"=="PYOK" set "PY=py -3"
if exist "%PY_PROBE_FILE%" del /q "%PY_PROBE_FILE%" >nul 2>nul
goto :eof

:TRY_PY_FROM_POWERSHELL
if defined PY goto :eof
for /f "usebackq delims=" %%P in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "$cmd=Get-Command python -ErrorAction SilentlyContinue; if($cmd -and $cmd.Source){ $cmd.Source }"`) do (
  if not defined PY call :TRY_PY_EXE "%%P"
)
goto :eof

:TRY_PY_FROM_WHERE
if defined PY goto :eof
for /f "usebackq delims=" %%P in (`where.exe python 2^>nul`) do (
  if not defined PY call :TRY_PY_EXE "%%P"
)
goto :eof

:LOG_MSG
for /f "delims=" %%T in ('powershell -NoProfile -Command "Get-Date -Format HH:mm:ss"') do set "__LOG_TS=%%T"
echo [%__LOG_TS%] %~1
>> "%LAST_LOG%" echo [%__LOG_TS%] %~1
goto :eof

:LOG_STEP_BEGIN
for /f "delims=" %%T in ('powershell -NoProfile -Command "[DateTimeOffset]::Now.ToUnixTimeSeconds()"') do set "__STEP_START_TS=%%T"
set "LAST_STEP_LABEL=%~1"
call :LOG_MSG "%~1 START"
goto :eof

:LOG_STEP_END
set "__STEP_RC=%~2"
for /f "delims=" %%T in ('powershell -NoProfile -Command "[DateTimeOffset]::Now.ToUnixTimeSeconds()"') do set "__STEP_END_TS=%%T"
set /a __STEP_ELAPSED=__STEP_END_TS-__STEP_START_TS
if "%__STEP_ELAPSED%"=="" set "__STEP_ELAPSED=-1"
call :LOG_MSG "%~1 END rc=%__STEP_RC% elapsed_s=%__STEP_ELAPSED%"
goto :eof

:RUN_BATCH_ISOLATED
if "%~1"=="" exit /b 87
cmd /d /c call "%~1" >> "%LAST_LOG%" 2>&1
exit /b %ERRORLEVEL%

:RUN_ROOTB_STATS
set "VIBE_STATS_DIR=E:\vibe\buffett\data\stats"
pushd "E:\vibe\buffett" || (
  set "VIBE_STATS_DIR="
  exit /b 1
)
"%PY%" E:\vibe\buffett\vibe_generate_stats_p0.py
set "_ROOTB_STATS_RC=%ERRORLEVEL%"
popd
set "VIBE_STATS_DIR="
exit /b %_ROOTB_STATS_RC%

:FILE_SHA
set "__FILE_SHA="
for /f "tokens=1 delims= " %%I in ('certutil -hashfile "%~1" SHA256 ^| findstr /R /I "^[0-9A-F][0-9A-F]*$"') do (
  if not defined __FILE_SHA set "__FILE_SHA=%%I"
)
set "%~2=%__FILE_SHA%"
goto :eof

:CSV_CODE_HASH
set "__CSV_CODE_HASH="
for /f "usebackq delims=" %%H in (`powershell -NoProfile -ExecutionPolicy Bypass -File "%ROOT%tools\csv_code_hash.ps1" -Path "%~1"`) do (
  if not defined __CSV_CODE_HASH set "__CSV_CODE_HASH=%%H"
)
set "%~2=%__CSV_CODE_HASH%"
goto :eof

:VALIDATE_PY_FORCE
set "_PYF=%~1"
if /I "%_PYF%"=="python" exit /b 0
if /I "%_PYF%"=="py -3" exit /b 0
if /I "%_PYF%"=="%ROOT%_runtime\python312-embed\python.exe" exit /b 0
if /I "%_PYF%"=="%ROOT%.venv\Scripts\python.exe" exit /b 0
if /I "%_PYF%"=="E:\vibe\buffett\.venv\Scripts\python.exe" exit /b 0
if /I "%_PYF%"=="C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe" exit /b 0
if /I "%_PYF%"=="C:\Users\jjtop\AppData\Local\Programs\Python\Python314\python.exe" exit /b 0
if /I "%_PYF%"=="%ROOT%tools\python_exec_proxy.cmd" exit /b 0
echo [FAILED] PY_FORCE not allowlisted: "%_PYF%"
exit /b 1

:CHECK_POST_REFRESH_NEEDED
set "POST_REFRESH_RC="
"%PY%" tools\needs_post_candidate_price_refresh.py --root "%CD%"
set "POST_REFRESH_RC=%ERRORLEVEL%"
if "%POST_REFRESH_RC%"=="0" (
  set "POST_REFRESH_NEEDED=0"
  exit /b 0
)
if "%POST_REFRESH_RC%"=="10" (
  set "POST_REFRESH_NEEDED=1"
  exit /b 0
)
echo [ERROR] post refresh need-check failed rc=%POST_REFRESH_RC%
exit /b 1

:POST_CHAIN_STEP_FAIL
set "POST_CHAIN_FAILED=1"
if "%POST_CHAIN_FAILED_STEP%"=="" set "POST_CHAIN_FAILED_STEP=%~1"
echo [ERROR] post chain step failed: %~1
goto :eof

:CHECK_AND_CLEAN_STALE_LOCK
set "LOCK_STATE=ACTIVE"
set "LOCK_OWNER_PID="
if not exist "%LOCK_DIR%\run.info" (
  set "LOCK_STATE=UNKNOWN_NO_INFO"
  goto :CHECK_LOCK_STATE
)
for /f "usebackq tokens=1,* delims==" %%A in ("%LOCK_DIR%\run.info") do if /I "%%A"=="pid" set "LOCK_OWNER_PID=%%B"
for /f "usebackq delims=" %%S in (`powershell -NoProfile -Command "$p='%LOCK_DIR%\run.info'; $m=%LOCK_STALE_MIN%; $pidText='%LOCK_OWNER_PID%'; if(Test-Path $p){ $age=((Get-Date)-(Get-Item $p).LastWriteTime).TotalMinutes; $owner=0; if(-not [string]::IsNullOrWhiteSpace($pidText) -and [int]::TryParse($pidText, [ref]$owner) -and $owner -gt 0){ if(Get-Process -Id $owner -ErrorAction SilentlyContinue){ 'ACTIVE_PID' }else{ 'STALE_PID_DEAD' } }else{ if($age -lt $m){ 'ACTIVE' }else{ 'STALE_NO_PID' } } } else { 'UNKNOWN_NO_INFO' }"`) do set "LOCK_STATE=%%S"

:CHECK_LOCK_STATE
if /I "%LOCK_STATE%"=="STALE_PID_DEAD" (
  echo [WARN] stale lock with dead pid detected. cleaning "%LOCK_DIR%" threshold=%LOCK_STALE_MIN% min pid=%LOCK_OWNER_PID%
  del /f /q "%LOCK_DIR%\run.info" >nul 2>nul
  rmdir "%LOCK_DIR%" >nul 2>nul
)
if /I "%LOCK_STATE%"=="STALE_NO_PID" (
  echo [WARN] stale lock without pid detected. cleaning "%LOCK_DIR%" threshold=%LOCK_STALE_MIN% min
  del /f /q "%LOCK_DIR%\run.info" >nul 2>nul
  rmdir "%LOCK_DIR%" >nul 2>nul
)
if /I "%LOCK_STATE%"=="UNKNOWN_NO_INFO" (
  echo [WARN] lock without run.info detected. cleaning "%LOCK_DIR%"
  del /f /q "%LOCK_DIR%\run.info" >nul 2>nul
  rmdir "%LOCK_DIR%" >nul 2>nul
)
goto :eof

:FAILED
set "FAIL_RC=%ERRORLEVEL%"
if "%FAIL_RC%"=="0" set "FAIL_RC=1"
echo.
echo [FAILED] step failed. ERRORLEVEL=%FAIL_RC%
if not "%POST_CHAIN_FAILED_STEP%"=="" set "LAST_STEP_LABEL=%POST_CHAIN_FAILED_STEP%"
if "%LAST_STEP_LABEL%"=="" set "LAST_STEP_LABEL=UNKNOWN"
echo [FAILED] step=%LAST_STEP_LABEL%
if not defined RUN_TS set "RUN_TS=%DATE%_%TIME%"
if defined ROOT >> "%ROOT%2_Logs\run_paper_daily_wrapper_status.txt" echo [INNER_EXIT] rc=%FAIL_RC% ts=%DATE% %TIME% step=%LAST_STEP_LABEL%
if defined LAST_LOG if not "%LAST_LOG%"=="" >> "%LAST_LOG%" echo [FAILED] ts=%RUN_TS% ERRORLEVEL=%FAIL_RC% STEP=%LAST_STEP_LABEL%
if defined LAST_LOG if not "%LAST_LOG%"=="" >> "%LAST_LOG%" echo [INNER_EXIT] rc=%FAIL_RC% step=%LAST_STEP_LABEL%
if defined STDERR_LOG if exist "%STDERR_LOG%" (
  if defined LAST_LOG if not "%LAST_LOG%"=="" >> "%LAST_LOG%" echo [STDERR_TAIL] path=%STDERR_LOG%
  if defined LAST_LOG if not "%LAST_LOG%"=="" powershell -NoProfile -Command "Get-Content -Path '%STDERR_LOG%' -Tail 80" >> "%LAST_LOG%"
)
if defined LOCK_ACQUIRED del /f /q "%LOCK_DIR%\run.info" >nul 2>nul
if defined LOCK_ACQUIRED rmdir "%LOCK_DIR%" >nul 2>nul
popd
endlocal & exit /b 1


















