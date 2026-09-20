setlocal EnableExtensions
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
if not defined RUN_DAILY_STDERR_CAPTURED (
  if not exist "%ROOT%2_Logs" mkdir "%ROOT%2_Logs" >nul 2>nul
  > "%STDERR_LOG%" echo [START] run_paper_daily stderr capture ts=%DATE% %TIME%
  set "RUN_DAILY_STDERR_CAPTURED=1"
  call "%~f0" %* 2>> "%STDERR_LOG%"
  if errorlevel 1 exit /b 1
  exit /b 0
)
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
  endlocal & exit /b 9
)
set "LOCK_ACQUIRED=1"
> "%LOCK_DIR%\run.info" echo started=%DATE% %TIME%

if "%PAPER_OPER_START_YMD%"=="" set "PAPER_OPER_START_YMD=20260301"
if "%LVB_MIN_STABLE_SCORE%"=="" set "LVB_MIN_STABLE_SCORE=-20"
if "%KRX_MIN_UNI%"=="" set "KRX_MIN_UNI=1800"
if "%POST_CHAIN_FAIL_SOFT%"=="" set "POST_CHAIN_FAIL_SOFT=0"
set "POST_CHAIN_FAILED=0"
set "POST_CHAIN_FAILED_STEP="
set "LAST_STEP_LABEL="
REM Python launcher (venv/absolute path first; fail-closed)
set "PY="
if not "%PY_FORCE%"=="" (
  call :VALIDATE_PY_FORCE "%PY_FORCE%"
  if errorlevel 1 goto :FAILED
  set "PY=%PY_FORCE%"
)
set "PY_PROBE_FILE=%ROOT%2_Logs\_py_probe.tmp"
REM prioritize command-resolved python first (more stable than broken venv shims)
if not defined PY call :TRY_PY_CMD python
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
  echo         checked: "%ROOT%.venv\Scripts\python.exe"
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
if "%SSOT_HEALTH_CHECK%"=="1" (
  echo [PRECHECK] run_ssot_health_card.bat
  call "%ROOT%run_ssot_health_card.bat"
  if errorlevel 1 (
    echo [FAILED] ssot health precheck failed - fail closed
    goto :FAILED
  )
)
if "%RESILIENCE_CHECK%"=="" set "RESILIENCE_CHECK=1"
if "%RESILIENCE_CHECK%"=="1" (
  echo [PRECHECK] run_resilience_check.bat
  call "%ROOT%run_resilience_check.bat"
  if errorlevel 1 (
    echo [FAILED] resilience precheck failed - fail closed
    goto :FAILED
  )
)
if "%FRESHNESS_PRECHECK%"=="" set "FRESHNESS_PRECHECK=1"
if "%FRESHNESS_PRECHECK%"=="1" (
  echo [PRECHECK] freshness_check_v1.py
  "%PY%" "%ROOT%tools\freshness_check_v1.py"
  if errorlevel 1 (
    echo [FAILED] freshness precheck failed - fail closed
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
REM [1/9] update prices parquet
REM ------------------------------------------------------------
call :LOG_STEP_BEGIN "[1/9] paper_update_prices_parquet.py"
"%PY%" paper_update_prices_parquet.py
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[1/9]" %RC%
if not "%RC%"=="0" goto :FAILED

REM ------------------------------------------------------------
REM [2/9] sync candidates meta
REM ------------------------------------------------------------
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

echo [PRE] macro_signal_daily.py
"%PY%" tools\macro_signal_daily.py
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
echo [6.2/9] tools\sync_krx_reference_cache.py
"%PY%" tools\sync_krx_reference_cache.py
if errorlevel 1 goto :FAILED

echo [6.23/9] krx_update_clean_incremental.py
"%PY%" krx_update_clean_incremental.py --base . --min-uni %KRX_MIN_UNI%
if errorlevel 1 goto :FAILED

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

REM ── [6.24b/9] Forward Estimate Snapshot (추정PER / TTM / PEG) ──────────────
call :LOG_STEP_BEGIN "[6.24b/9] build_forward_estimate_snapshot.py"
if "%FUND_DART_REFRESH%"=="1" (
  "%PY%" tools\build_forward_estimate_snapshot.py --max-codes 150 --sleep 0.2
  set "RC_FWD=%ERRORLEVEL%"
  if not "%RC_FWD%"=="0" echo [WARN] forward_estimate_snapshot failed (non-fatal, RC=%RC_FWD%)
) else (
  echo [INFO] skip forward_estimate_snapshot (FUND_DART_REFRESH=0)
)
call :LOG_STEP_END "[6.24b/9]" 0

"%PY%" generate_candidates_v41_1.py
set "RC=%ERRORLEVEL%"
call :LOG_STEP_END "[6.25/9]" %RC%
if not "%RC%"=="0" goto :FAILED
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
  call :CHECK_POST_REFRESH_NEEDED
  if errorlevel 1 goto :FAILED
)
if "%CANDIDATES_REFRESHED%"=="1" if "%POST_REFRESH_NEEDED%"=="1" (
  set "POST_PRICE_REFRESH_NEEDED=1"
  set "POST_SURVIVORSHIP_REFRESH_NEEDED=1"
  set "POST_LIQUIDITY_REFRESH_NEEDED=1"
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
    call :LOG_STEP_BEGIN "[6.26/9] paper_update_prices_parquet.py (post-candidates refresh)"
    "%PY%" paper_update_prices_parquet.py
    set "RC=%ERRORLEVEL%"
    call :LOG_STEP_END "[6.26/9]" %RC%
    if not "%RC%"=="0" goto :FAILED
  ) else (
    call :LOG_MSG "[6.26/9] skip paper_update_prices_parquet.py (prices up-to-date)"
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

REM ------------------------------------------------------------
REM [6.45/9] refresh p0/gate after candidate pipeline finalized today's meta
REM - sector/news/final stages read latest p0/gate artifacts, so refresh here once.
REM ------------------------------------------------------------
echo [6.45/9] p0_daily_check.py (post-candidates refresh)
"%PY%" p0_daily_check.py
if errorlevel 1 goto :FAILED

echo [6.46/9] gate_daily.py (post-candidates refresh)
"%PY%" gate_daily.py
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [6.5/9] sector score daily (snapshot + history append)
REM ------------------------------------------------------------
set "SIGNAL_FAIL_SOFT=%SIGNAL_FAIL_SOFT%"
if "%SIGNAL_FAIL_SOFT%"=="" set "SIGNAL_FAIL_SOFT=0"
echo [SIGNAL_POLICY] SIGNAL_FAIL_SOFT=%SIGNAL_FAIL_SOFT%
echo [6.5/9] tools\sector_score_daily.py
"%PY%" tools\sector_score_daily.py
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

REM ------------------------------------------------------------
REM [6.55-6.7/9] unified news pipeline (collect -> score -> final)
REM ------------------------------------------------------------
echo [6.55-6.7/9] run_news_pipeline_once.bat
if "%NAVER_CLIENT_ID%"=="" (
  echo [INFO] NAVER_CLIENT_ID missing -> collector will skip fail-soft
) else (
  echo [INFO] NAVER_CLIENT_ID detected -> collector enabled
)
set "NEWS_PIPELINE_SKIP_FINAL="
set "NEWS_PIPELINE_SKIP_INTEGRATION=1"
call "%ROOT%run_news_pipeline_once.bat" daily 00:00 23:59
set "NEWS_PIPELINE_SKIP_INTEGRATION="
if errorlevel 1 (
  echo [ERROR] run_news_pipeline_once failed
  if "%SIGNAL_FAIL_SOFT%"=="1" (
    echo [WARN] SIGNAL_FAIL_SOFT=1 - continuing
  ) else (
    goto :FAILED
  )
)
"%PY%" tools\check_signal_contract.py --stage news --root "%ROOT_NQ%"
if errorlevel 1 (
  echo [ERROR] news contract failed
  if "%SIGNAL_FAIL_SOFT%"=="1" (
    echo [WARN] SIGNAL_FAIL_SOFT=1 - continuing
  ) else (
    goto :FAILED
  )
)

"%PY%" tools\check_signal_contract.py --stage final --root "%ROOT_NQ%"
if errorlevel 1 (
  echo [ERROR] final contract failed
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

REM ------------------------------------------------------------
REM [7/9] paper engine
REM ------------------------------------------------------------
echo [7/9] paper_engine.py (main)
call :LOG_MSG "[7/9] paper_engine main START run_label=main"
"%PY%" paper_engine.py >> "%LAST_LOG%" 2>&1
if errorlevel 1 goto :FAILED
call :LOG_MSG "[7/9] paper_engine main END rc=0 run_label=main"

REM [7.1/9] optional shadow collect (main path isolated)
if "%PAPER_SHADOW_ENABLED%"=="" set "PAPER_SHADOW_ENABLED=1"
if "%PAPER_SHADOW_MAX_NEW%"=="" set "PAPER_SHADOW_MAX_NEW=6"
if "%PAPER_SHADOW_GAP_UP_MAX_PCT%"=="" set "PAPER_SHADOW_GAP_UP_MAX_PCT=0.07"
if "%PAPER_SHADOW_GAP_DOWN_STOP_PCT%"=="" set "PAPER_SHADOW_GAP_DOWN_STOP_PCT=0.05"
if "%PAPER_SHADOW_ENABLED%"=="1" (
  echo [7.1/9] paper_engine.py shadow_collect
  call :LOG_MSG "[7.1/9] paper_engine shadow START run_label=shadow"
  set "PAPER_RUN_LABEL=shadow"
  set "PAPER_FILLS_PATH=%ROOT%paper\fills_shadow.csv"
  set "PAPER_TRADES_PATH=%ROOT%paper\trades_shadow.csv"
  set "PAPER_STATE_PATH=%ROOT%paper\paper_state_shadow.json"
  set "PAPER_PENDING_SIGNALS_PATH=%ROOT%2_Logs\pending_entry_signals_shadow_latest.csv"
  set "PAPER_PENDING_STATUS_PATH=%ROOT%2_Logs\pending_entry_status_shadow_latest.json"
  set "PAPER_RECOVERY_STATUS_PATH=%ROOT%2_Logs\paper_recovery_status_shadow_latest.json"
  set "PAPER_OPS_ALERT_PATH=%ROOT%2_Logs\market_ops_alert_shadow_latest.json"
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
  set "PAPER_PENDING_STATUS_PATH="
  set "PAPER_RECOVERY_STATUS_PATH="
  set "PAPER_OPS_ALERT_PATH="
  set "PAPER_CFG_OVERRIDES="
) else (
  echo [SKIP] shadow_collect disabled PAPER_SHADOW_ENABLED=%PAPER_SHADOW_ENABLED%
)
REM ------------------------------------------------------------
REM [8/9] audit daily
REM ------------------------------------------------------------
echo [8/9] audit_daily.py
"%PY%" audit_daily.py
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [9/9] after close summary
REM ------------------------------------------------------------
echo [9/9] after_close_summary.cmd
call after_close_summary.cmd
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [10/14] derive D (execution date)
REM - TODAY  : use run date (default, intraday-friendly)
REM - FILLS  : use latest fills ymd
REM - HYBRID : use max(TODAY, FILLS)
REM ------------------------------------------------------------
if "%PAPER_EXEC_DATE_MODE%"=="" set "PAPER_EXEC_DATE_MODE=FILLS"
set "D_TODAY=%RUN_TS:~0,4%%RUN_TS:~5,2%%RUN_TS:~8,2%"
set "D_FILLS="
if exist "%ROOT%paper\fills.csv" (
  set "PY_DERIVE=%ROOT%tools\python_exec_proxy.cmd"
  if not exist "%PY_DERIVE%" set "PY_DERIVE=%PY%"
  set "D_FILLS_TMP=%ROOT%2_Logs\_d_fills.tmp"
  if exist "%D_FILLS_TMP%" del /q "%D_FILLS_TMP%" 1>nul 2>nul
  "%PY_DERIVE%" tools\derive_d_from_fills.py --csv "%ROOT%paper\fills.csv" 1>"%D_FILLS_TMP%" 2>nul
  if exist "%D_FILLS_TMP%" set /p D_FILLS=<"%D_FILLS_TMP%"
  if exist "%D_FILLS_TMP%" del /q "%D_FILLS_TMP%" 1>nul 2>nul
)
if /I "%PAPER_EXEC_DATE_MODE%"=="FILLS" (
  if "%D_FILLS%"=="" (
    echo [FAILED] PAPER_EXEC_DATE_MODE=FILLS but cannot infer from fills.csv
    goto :FAILED
  )
  set "D=%D_FILLS%"
) else if /I "%PAPER_EXEC_DATE_MODE%"=="HYBRID" (
  if "%D_FILLS%"=="" (
    set "D=%D_TODAY%"
  ) else (
    set "D=%D_TODAY%"
    if "%D_FILLS%" GTR "%D%" set "D=%D_FILLS%"
  )
) else (
  set "D=%D_TODAY%"
)
set "VIBE_EXEC_MODE=A"
echo [10/14] derive D mode=%PAPER_EXEC_DATE_MODE% today=%D_TODAY% fills=%D_FILLS% -> D=%D%

REM ------------------------------------------------------------
REM [11/14] onepass + ledger append (A-mode)
REM ------------------------------------------------------------
echo [11/14] p0_onepass_from_fills.py + ledger_append D=%D% (VIBE_EXEC_MODE=%VIBE_EXEC_MODE%)
"%PY%" tools\p0_onepass_from_fills.py %D%
if errorlevel 1 goto :FAILED

"%PY%" tools\ledger_append_from_orders_exec.py %D%
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [12/14] contract check: exec_date_unique must be D
REM ------------------------------------------------------------
echo [12/14] contract exec_date==D for orders_%D%_exec.xlsx
"%PY%" -c "import pandas as pd,sys; D=r'%D%'; p=rf'%ROOT%paper\orders_{D}_exec.xlsx'; df=pd.read_excel(p); u=sorted(df['exec_date'].astype(str).unique().tolist()); print('exec_date_unique',u); sys.exit(0 if u==[D] else 2)"
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [12.1/14] chain date contract: D == fills(D_by_rule) == orders_exec_date
REM ------------------------------------------------------------
echo [12.1/14] chain date contract (D/orders/fills)
set "D_RULE_FILLS="
if exist "%ROOT%paper\fills.csv" (
  set "PY_DERIVE=%ROOT%tools\python_exec_proxy.cmd"
  if not exist "%PY_DERIVE%" set "PY_DERIVE=%PY%"
  set "D_RULE_TMP=%ROOT%2_Logs\_d_rule_fills.tmp"
  if exist "%D_RULE_TMP%" del /q "%D_RULE_TMP%" 1>nul 2>nul
  "%PY_DERIVE%" tools\derive_d_from_fills.py --csv "%ROOT%paper\fills.csv" 1>"%D_RULE_TMP%" 2>nul
  if exist "%D_RULE_TMP%" set /p D_RULE_FILLS=<"%D_RULE_TMP%"
  if exist "%D_RULE_TMP%" del /q "%D_RULE_TMP%" 1>nul 2>nul
)
echo [CHAIN_D] D=%D% fills_D_by_rule=%D_RULE_FILLS%
if "%D_RULE_FILLS%"=="" goto :FAILED
if /I not "%D_RULE_FILLS%"=="%D%" goto :FAILED

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
      set "BROKER_SYNC_SUMMARY=%ROOT%2_Logs\kis_fills_sync_%BROKER_D%.json"
      if not exist "%BROKER_SYNC_SUMMARY%" (
        echo [FAILED] missing broker sync summary: %BROKER_SYNC_SUMMARY%
        goto :FAILED
      )
      echo [BROKER] verify bridge-write summary=%BROKER_SYNC_SUMMARY%
      "%PY%" -c "import json,sys; p=r'%BROKER_SYNC_SUMMARY%'; j=json.load(open(p,'r',encoding='utf-8')); bw=bool(j.get('bridge_write')); b=j.get('bridge') or {}; ok=bw and isinstance(b,dict) and bool(b.get('ledger')); print('[BROKER_SYNC_CHECK] bridge_write=',bw,'has_bridge_ledger=',bool((b.get('ledger') if isinstance(b,dict) else None))); sys.exit(0 if ok else 3)"
      if errorlevel 1 (
        echo [FAILED] broker sync bridge-write verification failed
        goto :FAILED
      )
      echo [BROKER] verify ledger as_of=%BROKER_D% reflected when normalized fills exist
      "%PY%" -c "import csv,json,os,sys; d=str('%BROKER_D%'); p=r'%BROKER_SYNC_SUMMARY%'; j=json.load(open(p,'r',encoding='utf-8')); rn=int(j.get('rows_normalized') or 0); b=j.get('bridge') or {}; lg=(b.get('ledger') or {}) if isinstance(b,dict) else {}; lp=str(lg.get('ledger_path') or r'E:\vibe\buffett\data\ledger\paper_fills_ledger.csv'); rows=0; rows=(sum(1 for r in csv.DictReader(open(lp,encoding='utf-8-sig',newline='')) if str(r.get('as_of',''))[:8]==d) if os.path.exists(lp) else 0); ok=(rn==0) or (rows>0); print('[BROKER_LEDGER_CHECK] D=',d,'rows_normalized=',rn,'ledger_asof_rows=',rows,'ledger_path=',lp); sys.exit(0 if ok else 4)"
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
        set "BROKER_SYNC_SUMMARY=%ROOT%2_Logs\kis_fills_sync_%BROKER_D%.json"
        if not exist "%BROKER_SYNC_SUMMARY%" (
          echo [FAILED] missing broker sync summary: %BROKER_SYNC_SUMMARY%
          goto :FAILED
        )
        echo [BROKER] verify bridge-write summary=%BROKER_SYNC_SUMMARY%
        "%PY%" -c "import json,sys; p=r'%BROKER_SYNC_SUMMARY%'; j=json.load(open(p,'r',encoding='utf-8')); bw=bool(j.get('bridge_write')); b=j.get('bridge') or {}; ok=bw and isinstance(b,dict) and bool(b.get('ledger')); print('[BROKER_SYNC_CHECK] bridge_write=',bw,'has_bridge_ledger=',bool((b.get('ledger') if isinstance(b,dict) else None))); sys.exit(0 if ok else 3)"
        if errorlevel 1 (
          echo [FAILED] broker sync bridge-write verification failed
          goto :FAILED
        )
        echo [BROKER] verify ledger as_of=%BROKER_D% reflected when normalized fills exist
        "%PY%" -c "import csv,json,os,sys; d=str('%BROKER_D%'); p=r'%BROKER_SYNC_SUMMARY%'; j=json.load(open(p,'r',encoding='utf-8')); rn=int(j.get('rows_normalized') or 0); b=j.get('bridge') or {}; lg=(b.get('ledger') or {}) if isinstance(b,dict) else {}; lp=str(lg.get('ledger_path') or r'E:\vibe\buffett\data\ledger\paper_fills_ledger.csv'); rows=0; rows=(sum(1 for r in csv.DictReader(open(lp,encoding='utf-8-sig',newline='')) if str(r.get('as_of',''))[:8]==d) if os.path.exists(lp) else 0); ok=(rn==0) or (rows>0); print('[BROKER_LEDGER_CHECK] D=',d,'rows_normalized=',rn,'ledger_asof_rows=',rows,'ledger_path=',lp); sys.exit(0 if ok else 4)"
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
        set "BROKER_SYNC_SUMMARY=%ROOT%2_Logs\kis_fills_sync_%BROKER_D%.json"
        if not exist "%BROKER_SYNC_SUMMARY%" (
          echo [FAILED] missing broker sync summary: %BROKER_SYNC_SUMMARY%
          goto :FAILED
        )
        echo [BROKER] verify bridge-write summary=%BROKER_SYNC_SUMMARY%
        "%PY%" -c "import json,sys; p=r'%BROKER_SYNC_SUMMARY%'; j=json.load(open(p,'r',encoding='utf-8')); bw=bool(j.get('bridge_write')); b=j.get('bridge') or {}; ok=bw and isinstance(b,dict) and bool(b.get('ledger')); print('[BROKER_SYNC_CHECK] bridge_write=',bw,'has_bridge_ledger=',bool((b.get('ledger') if isinstance(b,dict) else None))); sys.exit(0 if ok else 3)"
        if errorlevel 1 (
          echo [FAILED] broker sync bridge-write verification failed
          goto :FAILED
        )
        echo [BROKER] verify ledger as_of=%BROKER_D% reflected when normalized fills exist
        "%PY%" -c "import csv,json,os,sys; d=str('%BROKER_D%'); p=r'%BROKER_SYNC_SUMMARY%'; j=json.load(open(p,'r',encoding='utf-8')); rn=int(j.get('rows_normalized') or 0); b=j.get('bridge') or {}; lg=(b.get('ledger') or {}) if isinstance(b,dict) else {}; lp=str(lg.get('ledger_path') or r'E:\vibe\buffett\data\ledger\paper_fills_ledger.csv'); rows=0; rows=(sum(1 for r in csv.DictReader(open(lp,encoding='utf-8-sig',newline='')) if str(r.get('as_of',''))[:8]==d) if os.path.exists(lp) else 0); ok=(rn==0) or (rows>0); print('[BROKER_LEDGER_CHECK] D=',d,'rows_normalized=',rn,'ledger_asof_rows=',rows,'ledger_path=',lp); sys.exit(0 if ok else 4)"
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
  "%PY%" tools\indicator_diag_and_recommend.py --lookback-days 120 --min-universe 2000 --horizons 1,2,5
  if errorlevel 1 (
    echo [WARN] indicator_diag_and_recommend failed - continuing
  )
) else (
  echo [SKIP] indicator diag (INDICATOR_DIAG_AUTO=%INDICATOR_DIAG_AUTO%)
)

REM [16/16] pending report (JSON-based verdict; allow same-day pending)
echo [16/16] paper_pending_report.py + JSON verdict (pending==0)
"%PY%" tools\paper_pending_report.py
"%PY%" -c "import glob,json,os,sys; fs=glob.glob(r'%ROOT%2_Logs\paper_pending_report_*.json'); f=max(fs,key=os.path.getmtime); j=json.load(open(f,'r',encoding='utf-8')); pend=j.get('pending',[]) or []; act=j.get('active',[]) or []; px=str(j.get('prices_date_max') or ''); ok=(len(pend)==0) or all((str(r.get('entry_date') or '')==px and str(r.get('last_price_date_for_code') or '')==px) for r in pend); print('LATEST',f,'prices_date_max',px,'pending',len(pend),'active',len(act),'ok_same_day_pending',ok); sys.exit(0 if ok else 3)"
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
echo [16.75/16] tools\calibration_stream.py
"%PY%" tools\calibration_stream.py
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.75/16] tools\\calibration_stream.py"
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
call "%ROOT%run_trading_stage_validation_report.bat"
if errorlevel 1 (
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

REM [16.95/16] refresh integrated ops snapshot latest
echo [16.95/16] tools\build_integrated_ops_snapshot.py
set "INTEGRATED_OPS_UPDATE_PLANS=0"
"%PY%" tools\build_integrated_ops_snapshot.py
set "INTEGRATED_OPS_UPDATE_PLANS="
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.95/16] build_integrated_ops_snapshot.py"
)

REM [16.955/16] refresh paper parameter review (observation layer)
echo [16.955/16] tools\build_paper_parameter_review.py
"%PY%" tools\build_paper_parameter_review.py
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.955/16] build_paper_parameter_review.py"
)

REM [16.956/16] signal auto-tuner propose only (no apply)
echo [16.956/16] tools\auto_signal_tuner.py --mode propose
"%PY%" tools\auto_signal_tuner.py --mode propose
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.956/16] auto_signal_tuner.py --mode propose"
)

REM [16.957/16] surge param validator propose
echo [16.957/16] tools\surge_param_validator.py --mode propose
"%PY%" tools\surge_param_validator.py --mode propose
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.957/16] surge_param_validator.py --mode propose"
)

REM [16.957a/16] surge LOB ingest latest refresh
echo [16.957a/16] tools\surge_lob_ingest.py
"%PY%" tools\surge_lob_ingest.py
if errorlevel 1 (
  call :POST_CHAIN_STEP_FAIL "[16.957a/16] surge_lob_ingest.py"
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

REM [16.958/16] surge param validator apply (opt-in)
if "%SURGE_PARAM_AUTO_APPLY%"=="1" (
  echo [16.958/16] tools\surge_param_validator.py --mode apply
  "%PY%" tools\surge_param_validator.py --mode apply
  if errorlevel 1 (
    call :POST_CHAIN_STEP_FAIL "[16.958/16] surge_param_validator.py --mode apply"
  )
)

REM [16.96/16] JSON UTF8 encoding hygiene scan (fail-closed via post-chain policy)
echo [16.96/16] tools\scan_json_encoding_utf8.ps1 -FailOnFind
powershell -NoProfile -ExecutionPolicy Bypass -File "%ROOT%tools\scan_json_encoding_utf8.ps1" -FailOnFind
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
if exist "%LOCK_DIR%\run.info" (
  for /f "usebackq delims=" %%S in (`powershell -NoProfile -Command "$p='%LOCK_DIR%\run.info'; $m=%LOCK_STALE_MIN%; if(Test-Path $p){ $age=((Get-Date)-(Get-Item $p).LastWriteTime).TotalMinutes; if($age -ge $m){'STALE'} else {'ACTIVE'} } else { 'UNKNOWN' }"`) do set "LOCK_STATE=%%S"
) else (
  set "LOCK_STATE=UNKNOWN"
)
if /I "%LOCK_STATE%"=="STALE" (
  echo [WARN] stale lock detected. cleaning "%LOCK_DIR%" (threshold=%LOCK_STALE_MIN% min)
  del /f /q "%LOCK_DIR%\run.info" >nul 2>nul
  rmdir "%LOCK_DIR%" >nul 2>nul
)
if /I "%LOCK_STATE%"=="UNKNOWN" (
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
if defined LAST_LOG if not "%LAST_LOG%"=="" >> "%LAST_LOG%" echo [FAILED] ts=%RUN_TS% ERRORLEVEL=%FAIL_RC% STEP=%LAST_STEP_LABEL%
if defined STDERR_LOG if exist "%STDERR_LOG%" (
  if defined LAST_LOG if not "%LAST_LOG%"=="" >> "%LAST_LOG%" echo [STDERR_TAIL] path=%STDERR_LOG%
  if defined LAST_LOG if not "%LAST_LOG%"=="" powershell -NoProfile -Command "Get-Content -Path '%STDERR_LOG%' -Tail 80" >> "%LAST_LOG%"
)
if defined LOCK_ACQUIRED del /f /q "%LOCK_DIR%\run.info" >nul 2>nul
if defined LOCK_ACQUIRED rmdir "%LOCK_DIR%" >nul 2>nul
popd
endlocal & exit /b 1














