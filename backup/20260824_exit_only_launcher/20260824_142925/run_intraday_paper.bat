@echo off
setlocal EnableExtensions

REM run_intraday_paper.bat
REM - Run intraday paper loop in mock-safe mode by default

set "ROOT=%~dp0"
pushd "%ROOT%" || (
  echo [FAILED] pushd: %ROOT%
  exit /b 2
)

if not exist "%ROOT%2_Logs" mkdir "%ROOT%2_Logs" >nul 2>nul
set "LOG=%ROOT%2_Logs\run_intraday_paper_last.txt"
if "%LOOP_ONCE%"=="1" set "LOG=%ROOT%2_Logs\run_intraday_paper_once_last.txt"
set "SKIP_LOG=%ROOT%2_Logs\run_intraday_paper_skip_last.txt"
set "LOCK_DIR=%ROOT%2_Logs\run_intraday_paper.lock"
set "LOCK_ACQUIRED=0"

2>nul mkdir "%LOCK_DIR%"
if errorlevel 1 (
  echo [SKIP] intraday loop already running. lock="%LOCK_DIR%"
  > "%SKIP_LOG%" echo [SKIP] duplicate start blocked by lock %DATE% %TIME%
  popd
  endlocal & exit /b 0
)
set "LOCK_ACQUIRED=1"
set "LOCK_OWNER=%LOCK_DIR%\owner.txt"
set "LOCK_HEARTBEAT=%LOCK_DIR%\heartbeat.json"
> "%LOCK_OWNER%" echo started=%DATE% %TIME%
>> "%LOCK_OWNER%" echo script=%~f0
>> "%LOCK_OWNER%" echo cwd=%CD%
>> "%LOCK_OWNER%" echo pid_env_unavailable=cmd_parent
>> "%LOCK_OWNER%" echo status_latest=%ROOT%2_Logs\intraday_loop_status_latest.json
>> "%LOCK_OWNER%" echo heartbeat=%LOCK_HEARTBEAT%
set "INTRADAY_LOCK_DIR=%LOCK_DIR%"
set "INTRADAY_LOCK_OWNER=%LOCK_OWNER%"
set "INTRADAY_LOCK_HEARTBEAT=%LOCK_HEARTBEAT%"

if "%KIS_MOCK%"=="" set "KIS_MOCK=0"
if "%LOOP_INTERVAL%"=="" set "LOOP_INTERVAL=2"
if "%LOOP_MAX_ORDERS%"=="" set "LOOP_MAX_ORDERS=5"
if "%LOOP_DISPATCH_APPLY%"=="" (
  if /I "%KIS_MOCK%"=="1" (
    set "LOOP_DISPATCH_APPLY=1"
  ) else (
    set "LOOP_DISPATCH_APPLY=0"
  )
)
if "%LOOP_MOCK_ALLOW_RISK_GUARDED_BUY%"=="" (
  if /I "%KIS_MOCK%"=="1" (
    set "LOOP_MOCK_ALLOW_RISK_GUARDED_BUY=1"
  ) else (
    set "LOOP_MOCK_ALLOW_RISK_GUARDED_BUY=0"
  )
)
if "%PAPER_INTRADAY_MAX_POSITIONS%"=="" set "PAPER_INTRADAY_MAX_POSITIONS=12"
if "%LOOP_WATCHDOG%"=="" set "LOOP_WATCHDOG=1"
if "%LOOP_RESTART_WAIT_SEC%"=="" set "LOOP_RESTART_WAIT_SEC=15"
if "%SURGE_LOB_INGEST_EVERY_N%"=="" set "SURGE_LOB_INGEST_EVERY_N=2"
if "%SURGE_LOB_HOGA_MAX_FETCH%"=="" set "SURGE_LOB_HOGA_MAX_FETCH=10"
if "%SURGE_LOB_HOGA_SOFT_TIMEOUT_SEC%"=="" set "SURGE_LOB_HOGA_SOFT_TIMEOUT_SEC=35"
if "%SURGE_LOB_KIS_TIMEOUT_SEC%"=="" set "SURGE_LOB_KIS_TIMEOUT_SEC=3.5"
if "%SURGE_LOB_KIS_SLEEP_SEC%"=="" set "SURGE_LOB_KIS_SLEEP_SEC=0.35"
if "%SURGE_LOB_INGEST_TIMEOUT_SEC%"=="" set "SURGE_LOB_INGEST_TIMEOUT_SEC=45"
if "%SURGE_LOB_INGEST_TIMEOUT_CAP_SEC%"=="" set "SURGE_LOB_INGEST_TIMEOUT_CAP_SEC=45"
if "%INTRADAY_PRICE_TIMEOUT_SEC%"=="" set "INTRADAY_PRICE_TIMEOUT_SEC=90"
if "%INTRADAY_PRICE_MAX_TOTAL_CODES%"=="" set "INTRADAY_PRICE_MAX_TOTAL_CODES=20"
if "%INTRADAY_PRICE_REQUEST_INTERVAL_SEC%"=="" set "INTRADAY_PRICE_REQUEST_INTERVAL_SEC=1.15"
if "%INTRADAY_PRICE_BROAD_EVERY_N%"=="" set "INTRADAY_PRICE_BROAD_EVERY_N=3"
if "%INTRADAY_PRICE_MOCK%"=="" set "INTRADAY_PRICE_MOCK=false"
if "%INTRADAY_PRICE_REST_CUTOFF_HHMM%"=="" set "INTRADAY_PRICE_REST_CUTOFF_HHMM=1520"
if "%INTRADAY_SURGE_UNIVERSE_MAX%"=="" set "INTRADAY_SURGE_UNIVERSE_MAX=150"
if "%INTRADAY_SURGE_UNIVERSE_SLICE_MAX%"=="" set "INTRADAY_SURGE_UNIVERSE_SLICE_MAX=50"
if "%MARKET_RISING_TOP_N%"=="" set "MARKET_RISING_TOP_N=50"
if "%MARKET_RISING_BUCKET_MAX%"=="" set "MARKET_RISING_BUCKET_MAX=5"
if "%MARKET_RISING_TIMEOUT_CAP_SEC%"=="" set "MARKET_RISING_TIMEOUT_CAP_SEC=60"
if "%LOKY_MAX_CPU_COUNT%"=="" set "LOKY_MAX_CPU_COUNT=2"
if "%SURGE_ML_SCORE_TIMEOUT_SEC%"=="" set "SURGE_ML_SCORE_TIMEOUT_SEC=90"
if "%SURGE_ML_SCORE_TIMEOUT_CAP_SEC%"=="" set "SURGE_ML_SCORE_TIMEOUT_CAP_SEC=90"
if "%SURGE_FRESHNESS_POINTER_MAX_AGE_SEC%"=="" set "SURGE_FRESHNESS_POINTER_MAX_AGE_SEC=3600"
if "%SURGE_BLOCKED_PATH_REVIEW_EVERY_N%"=="" set "SURGE_BLOCKED_PATH_REVIEW_EVERY_N=5"
if "%SURGE_SANITY_TIMEOUT_SEC%"=="" set "SURGE_SANITY_TIMEOUT_SEC=90"
if "%WAIT_LOB_HOGA_OBSERVE_EVERY_N%"=="" set "WAIT_LOB_HOGA_OBSERVE_EVERY_N=4"
if "%WAIT_LOB_HOGA_MAX_FETCH%"=="" set "WAIT_LOB_HOGA_MAX_FETCH=10"
if "%WAIT_LOB_HOGA_SOFT_TIMEOUT_SEC%"=="" set "WAIT_LOB_HOGA_SOFT_TIMEOUT_SEC=25"
if "%WAIT_LOB_HOGA_SLEEP_SEC%"=="" set "WAIT_LOB_HOGA_SLEEP_SEC=0.35"
if "%WAIT_LOB_HOGA_OBSERVE_TIMEOUT_SEC%"=="" set "WAIT_LOB_HOGA_OBSERVE_TIMEOUT_SEC=35"
if "%INTRADAY_DASHBOARD_REALTIME_EVERY_N%"=="" set "INTRADAY_DASHBOARD_REALTIME_EVERY_N=1"
if "%SURGE_FOLLOWTHROUGH_VALIDATION_EVERY_N%"=="" set "SURGE_FOLLOWTHROUGH_VALIDATION_EVERY_N=3"
if "%ORDERFLOW_GLR_EVERY_N%"=="" set "ORDERFLOW_GLR_EVERY_N=4"
if "%ORDERFLOW_OBSERVER_MAX_AGE_SEC%"=="" set "ORDERFLOW_OBSERVER_MAX_AGE_SEC=900"
if "%ORDERFLOW_GLR_MAX_WINDOWS_PER_CODE%"=="" set "ORDERFLOW_GLR_MAX_WINDOWS_PER_CODE=720"
if "%INTRADAY_NEWS_COLLECT_ENABLED%"=="" set "INTRADAY_NEWS_COLLECT_ENABLED=0"
if "%INTRADAY_NEWS_SCORE_EVERY_N%"=="" set "INTRADAY_NEWS_SCORE_EVERY_N=7"
if "%INTRADAY_NEWS_COLLECT_TIMEOUT_SEC%"=="" set "INTRADAY_NEWS_COLLECT_TIMEOUT_SEC=90"
if "%INTRADAY_NEWS_COLLECT_MAX_SYMBOLS%"=="" set "INTRADAY_NEWS_COLLECT_MAX_SYMBOLS=8"
if "%INTRADAY_NEWS_COLLECT_CACHE_TTL_SEC%"=="" set "INTRADAY_NEWS_COLLECT_CACHE_TTL_SEC=1800"
REM Paper data collection defaults: mock-only; PROD uses production thresholds from surge_params.json
if "%KIS_MOCK%"=="1" (
  if "%SURGE_RT_PAPER_DATA_COLLECTION%"=="" set "SURGE_RT_PAPER_DATA_COLLECTION=1"
  if "%SURGE_RT_PAPER_LIMIT_NEAR_IGNORE_ENTRY_CAPS%"=="" set "SURGE_RT_PAPER_LIMIT_NEAR_IGNORE_ENTRY_CAPS=1"
  if "%SURGE_RT_PAPER_HIGH_REJECTION_ENTRY_BLOCK_PCT%"=="" set "SURGE_RT_PAPER_HIGH_REJECTION_ENTRY_BLOCK_PCT=0.025"
  if "%SURGE_RT_PAPER_ALLOW_KRX_CAUTION%"=="" set "SURGE_RT_PAPER_ALLOW_KRX_CAUTION=1"
) else (
  if "%SURGE_RT_PAPER_DATA_COLLECTION%"=="" set "SURGE_RT_PAPER_DATA_COLLECTION=0"
  if "%SURGE_RT_PAPER_LIMIT_NEAR_IGNORE_ENTRY_CAPS%"=="" set "SURGE_RT_PAPER_LIMIT_NEAR_IGNORE_ENTRY_CAPS=0"
  if "%SURGE_RT_PAPER_HIGH_REJECTION_ENTRY_BLOCK_PCT%"=="" set "SURGE_RT_PAPER_HIGH_REJECTION_ENTRY_BLOCK_PCT=0.05"
  if "%SURGE_RT_PAPER_ALLOW_KRX_CAUTION%"=="" set "SURGE_RT_PAPER_ALLOW_KRX_CAUTION=0"
)
if "%INTRADAY_PREFLIGHT_FRESHNESS%"=="" set "INTRADAY_PREFLIGHT_FRESHNESS=1"
if "%INTRADAY_PREFLIGHT_DAILY_REFRESH%"=="" set "INTRADAY_PREFLIGHT_DAILY_REFRESH=1"
if "%INTRADAY_PREFLIGHT_DAILY_TIMEOUT_SEC%"=="" set "INTRADAY_PREFLIGHT_DAILY_TIMEOUT_SEC=5400"

set "PY="
if not defined PY call :TRY_PY_EXE "%ROOT%_runtime\python312-embed\python.exe"
if not defined PY if defined PAPER_PYTHON call :TRY_PY_EXE "%PAPER_PYTHON%"
if not defined PY if defined PYTHON_EXE call :TRY_PY_EXE "%PYTHON_EXE%"
if not defined PY call :TRY_PY_EXE "%ROOT%.venv\Scripts\python.exe"
if not defined PY call :TRY_PY_EXE "E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY call :TRY_PY_EXE "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY call :TRY_PY_EXE "C:\Users\jjtop\AppData\Local\Programs\Python\Python314\python.exe"
if not defined PY if defined LOCALAPPDATA call :TRY_PY_EXE "%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
if not defined PY if defined LOCALAPPDATA call :TRY_PY_EXE "%LOCALAPPDATA%\Programs\Python\Python314\python.exe"
if not defined PY call :TRY_PY_CMD_PATH "%ROOT%tools\python_exec_proxy.cmd"
if not defined PY call :TRY_PY_CMD python
if not defined PY (
  echo [FAILED] Python not found
  call :RELEASE_LOCK
  popd
  endlocal & exit /b 1
)

echo [START] %DATE% %TIME% > "%LOG%"
echo INTRADAY_PREFLIGHT_FRESHNESS=%INTRADAY_PREFLIGHT_FRESHNESS% >> "%LOG%"
echo INTRADAY_PREFLIGHT_DAILY_REFRESH=%INTRADAY_PREFLIGHT_DAILY_REFRESH% >> "%LOG%"
if "%INTRADAY_PREFLIGHT_FRESHNESS%"=="1" (
  call :RUN_PREFLIGHT_FRESHNESS
  if errorlevel 1 (
    echo [FAILED] intraday preflight freshness failed %DATE% %TIME% >> "%LOG%"
    call :RELEASE_LOCK
    popd
    endlocal & exit /b 24
  )
)

set "LOOP_ARGS=--mock auto --interval %LOOP_INTERVAL% --max-orders %LOOP_MAX_ORDERS%"
if "%LOOP_DRY_RUN%"=="1" set "LOOP_ARGS=%LOOP_ARGS% --dry-run"
if "%LOOP_ONCE%"=="1" set "LOOP_ARGS=%LOOP_ARGS% --once"
if "%LOOP_ALLOW_OFFHOURS%"=="1" set "LOOP_ARGS=%LOOP_ARGS% --allow-offhours"
if "%LOOP_DISPATCH_APPLY%"=="1" set "LOOP_ARGS=%LOOP_ARGS% --dispatch-apply"

echo [LOOP_START] %DATE% %TIME% >> "%LOG%"
echo KIS_MOCK=%KIS_MOCK% INTERVAL=%LOOP_INTERVAL%min >> "%LOG%"
echo LOOP_DISPATCH_APPLY=%LOOP_DISPATCH_APPLY% >> "%LOG%"
echo LOOP_MOCK_ALLOW_RISK_GUARDED_BUY=%LOOP_MOCK_ALLOW_RISK_GUARDED_BUY% >> "%LOG%"
echo SURGE_LOB_INGEST_EVERY_N=%SURGE_LOB_INGEST_EVERY_N% >> "%LOG%"
echo SURGE_LOB_HOGA_MAX_FETCH=%SURGE_LOB_HOGA_MAX_FETCH% >> "%LOG%"
echo SURGE_LOB_HOGA_SOFT_TIMEOUT_SEC=%SURGE_LOB_HOGA_SOFT_TIMEOUT_SEC% >> "%LOG%"
echo SURGE_LOB_KIS_SLEEP_SEC=%SURGE_LOB_KIS_SLEEP_SEC% >> "%LOG%"
echo SURGE_LOB_INGEST_TIMEOUT_SEC=%SURGE_LOB_INGEST_TIMEOUT_SEC% >> "%LOG%"
echo SURGE_LOB_INGEST_TIMEOUT_CAP_SEC=%SURGE_LOB_INGEST_TIMEOUT_CAP_SEC% >> "%LOG%"
echo INTRADAY_PRICE_REQUEST_INTERVAL_SEC=%INTRADAY_PRICE_REQUEST_INTERVAL_SEC% >> "%LOG%"
echo INTRADAY_PRICE_MAX_TOTAL_CODES=%INTRADAY_PRICE_MAX_TOTAL_CODES% >> "%LOG%"
echo INTRADAY_PRICE_BROAD_EVERY_N=%INTRADAY_PRICE_BROAD_EVERY_N% >> "%LOG%"
echo INTRADAY_PRICE_MOCK=%INTRADAY_PRICE_MOCK% >> "%LOG%"
echo INTRADAY_PRICE_REST_CUTOFF_HHMM=%INTRADAY_PRICE_REST_CUTOFF_HHMM% >> "%LOG%"
echo INTRADAY_SURGE_UNIVERSE_MAX=%INTRADAY_SURGE_UNIVERSE_MAX% >> "%LOG%"
echo INTRADAY_SURGE_UNIVERSE_SLICE_MAX=%INTRADAY_SURGE_UNIVERSE_SLICE_MAX% >> "%LOG%"
echo MARKET_RISING_TOP_N=%MARKET_RISING_TOP_N% >> "%LOG%"
echo MARKET_RISING_BUCKET_MAX=%MARKET_RISING_BUCKET_MAX% >> "%LOG%"
echo MARKET_RISING_TIMEOUT_CAP_SEC=%MARKET_RISING_TIMEOUT_CAP_SEC% >> "%LOG%"
echo LOKY_MAX_CPU_COUNT=%LOKY_MAX_CPU_COUNT% >> "%LOG%"
echo SURGE_ML_SCORE_TIMEOUT_SEC=%SURGE_ML_SCORE_TIMEOUT_SEC% >> "%LOG%"
echo SURGE_SANITY_TIMEOUT_SEC=%SURGE_SANITY_TIMEOUT_SEC% >> "%LOG%"
echo WAIT_LOB_HOGA_OBSERVE_EVERY_N=%WAIT_LOB_HOGA_OBSERVE_EVERY_N% >> "%LOG%"
echo WAIT_LOB_HOGA_MAX_FETCH=%WAIT_LOB_HOGA_MAX_FETCH% >> "%LOG%"
echo WAIT_LOB_HOGA_SOFT_TIMEOUT_SEC=%WAIT_LOB_HOGA_SOFT_TIMEOUT_SEC% >> "%LOG%"
echo WAIT_LOB_HOGA_SLEEP_SEC=%WAIT_LOB_HOGA_SLEEP_SEC% >> "%LOG%"
echo WAIT_LOB_HOGA_OBSERVE_TIMEOUT_SEC=%WAIT_LOB_HOGA_OBSERVE_TIMEOUT_SEC% >> "%LOG%"
echo INTRADAY_DASHBOARD_REALTIME_EVERY_N=%INTRADAY_DASHBOARD_REALTIME_EVERY_N% >> "%LOG%"
echo SURGE_FOLLOWTHROUGH_VALIDATION_EVERY_N=%SURGE_FOLLOWTHROUGH_VALIDATION_EVERY_N% >> "%LOG%"
echo ORDERFLOW_GLR_EVERY_N=%ORDERFLOW_GLR_EVERY_N% >> "%LOG%"
echo ORDERFLOW_OBSERVER_MAX_AGE_SEC=%ORDERFLOW_OBSERVER_MAX_AGE_SEC% >> "%LOG%"
echo ORDERFLOW_GLR_MAX_WINDOWS_PER_CODE=%ORDERFLOW_GLR_MAX_WINDOWS_PER_CODE% >> "%LOG%"
echo INTRADAY_NEWS_COLLECT_ENABLED=%INTRADAY_NEWS_COLLECT_ENABLED% >> "%LOG%"
echo INTRADAY_NEWS_COLLECT_TIMEOUT_SEC=%INTRADAY_NEWS_COLLECT_TIMEOUT_SEC% >> "%LOG%"
echo INTRADAY_NEWS_COLLECT_MAX_SYMBOLS=%INTRADAY_NEWS_COLLECT_MAX_SYMBOLS% >> "%LOG%"
echo INTRADAY_NEWS_COLLECT_CACHE_TTL_SEC=%INTRADAY_NEWS_COLLECT_CACHE_TTL_SEC% >> "%LOG%"
echo SURGE_RT_PAPER_DATA_COLLECTION=%SURGE_RT_PAPER_DATA_COLLECTION% >> "%LOG%"
echo SURGE_RT_PAPER_HIGH_REJECTION_ENTRY_BLOCK_PCT=%SURGE_RT_PAPER_HIGH_REJECTION_ENTRY_BLOCK_PCT% >> "%LOG%"
echo LOOP_WATCHDOG=%LOOP_WATCHDOG% RESTART_WAIT=%LOOP_RESTART_WAIT_SEC%s >> "%LOG%"
echo PY=%PY% >> "%LOG%"
echo ARGS=%LOOP_ARGS% >> "%LOG%"

echo [INFO] Starting intraday loop...
set "RESTART_COUNT=0"

REM ------------------------------------------------------------
REM [PRE] news_score T-1 premarket refresh (fail-soft)
REM - Runs once before the loop to ensure stable non-zero news scores
REM   from yesterday's articles are loaded before market open.
REM - NEWS_SCORE_T1_MODE=true caps allowed article date to reference_ymd-1
REM   so today's (potentially empty) articles are excluded.
REM - Skip if NEWS_SCORE_T1_SKIP=1 (e.g., post-close daily batch already ran)
REM ------------------------------------------------------------
if "%NEWS_SCORE_T1_SKIP%"=="" set "NEWS_SCORE_T1_SKIP=0"
for /f "usebackq delims=" %%T in (`powershell -NoProfile -Command "Get-Date -Format HHmm"`) do set "NOW_HHMM=%%T"
if "%NEWS_SCORE_T1_SKIP%"=="0" if "%NOW_HHMM%" GEQ "1530" (
  set "NEWS_SCORE_T1_SKIP=1"
  echo [PRE] skip news_score T-1 refresh after market close
  >> "%LOG%" echo [PRE] skip news_score T-1 refresh after market close
)
if "%NEWS_SCORE_T1_SKIP%"=="0" if "%NOW_HHMM%" GEQ "0900" if "%NOW_HHMM%" LSS "1530" (
  set "NEWS_SCORE_T1_SKIP=1"
  echo [PRE] skip news_score T-1 refresh during regular intraday session
  >> "%LOG%" echo [PRE] skip news_score T-1 refresh during regular intraday session
)
if "%NOW_HHMM%" GEQ "0900" if "%NOW_HHMM%" LSS "1530" goto :AFTER_NEWS_SCORE_T1
if not "%NEWS_SCORE_T1_SKIP%"=="1" (
  echo [PRE] news_score T-1 premarket refresh (NEWS_SCORE_T1_MODE=true)
  >> "%LOG%" echo [PRE] news_score T-1 premarket refresh (NEWS_SCORE_T1_MODE=true)
  set "NEWS_SCORE_T1_MODE=true"
  "%PY%" tools\news_score_daily.py >> "%LOG%" 2>&1
  set "NEWS_T1_RC=%ERRORLEVEL%"
  set "NEWS_SCORE_T1_MODE="
  if "%NEWS_T1_RC%"=="0" (
    echo [PRE] news_score T-1 refresh OK
    >> "%LOG%" echo [PRE] news_score T-1 refresh OK
  ) else (
    echo [WARN] news_score T-1 refresh failed rc=%NEWS_T1_RC% - continuing with stale scores
    >> "%LOG%" echo [WARN] news_score T-1 refresh failed rc=%NEWS_T1_RC%
  )
)
:AFTER_NEWS_SCORE_T1

echo [PRE] build_surge_universe.py --top-n %INTRADAY_SURGE_UNIVERSE_MAX%
>> "%LOG%" echo [PRE] build_surge_universe.py --top-n %INTRADAY_SURGE_UNIVERSE_MAX%
"%PY%" "%ROOT%tools\build_surge_universe.py" --top-n %INTRADAY_SURGE_UNIVERSE_MAX% >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [WARN] build_surge_universe failed - continuing with existing surge_universe.csv
  >> "%LOG%" echo [WARN] build_surge_universe failed - continuing with existing surge_universe.csv
)

:RUN_LOOP
>> "%LOG%" echo [CMD] "%PY%" "%ROOT%intraday_paper_loop.py" %LOOP_ARGS%
"%PY%" "%ROOT%intraday_paper_loop.py" %LOOP_ARGS% >> "%LOG%" 2>&1
set "EXITCODE=%ERRORLEVEL%"
echo [END] exitcode=%EXITCODE% %DATE% %TIME% >> "%LOG%"

if "%LOOP_ONCE%"=="1" goto :DONE
if "%LOOP_WATCHDOG%"=="0" goto :DONE
if "%EXITCODE%"=="0" goto :DONE
if "%EXITCODE%"=="2" (
  > "%SKIP_LOG%" echo [SKIP] singleton duplicate blocked by intraday_paper_loop.py; treating as already running %DATE% %TIME%
  set "EXITCODE=0"
  goto :DONE
)

set /a RESTART_COUNT+=1
echo [WATCHDOG] restart #%RESTART_COUNT% after %LOOP_RESTART_WAIT_SEC%s (exitcode=%EXITCODE%) %DATE% %TIME%>> "%LOG%"
powershell -NoProfile -Command "Start-Sleep -Seconds %LOOP_RESTART_WAIT_SEC%" >nul 2>nul
goto :RUN_LOOP

:DONE

call :RELEASE_LOCK
popd
endlocal & exit /b %EXITCODE%

:RUN_PREFLIGHT_FRESHNESS
echo [PREFLIGHT] freshness_check_v1 before loop %DATE% %TIME% >> "%LOG%"
"%PY%" tools\freshness_check_v1.py >> "%LOG%" 2>&1
if not errorlevel 1 (
  echo [PREFLIGHT] freshness PASS before loop %DATE% %TIME% >> "%LOG%"
  exit /b 0
)
echo [PREFLIGHT] freshness FAIL before loop rc=%ERRORLEVEL% %DATE% %TIME% >> "%LOG%"
if "%INTRADAY_PREFLIGHT_DAILY_REFRESH%"=="0" (
  echo [PREFLIGHT] daily refresh disabled; fail closed >> "%LOG%"
  exit /b 21
)
echo [PREFLIGHT] run_paper_daily.bat for daily data refresh timeout=%INTRADAY_PREFLIGHT_DAILY_TIMEOUT_SEC%s %DATE% %TIME% >> "%LOG%"
"%PY%" tools\run_step_with_timeout.py --name intraday_preflight_run_paper_daily --timeout-sec %INTRADAY_PREFLIGHT_DAILY_TIMEOUT_SEC% --status-json 2_Logs\intraday_preflight_run_paper_daily_status_latest.json -- cmd /c "%ROOT%run_paper_daily.bat" >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [PREFLIGHT] run_paper_daily.bat failed rc=%ERRORLEVEL% %DATE% %TIME% >> "%LOG%"
  exit /b 22
)
findstr /C:"[WRAPPER_EXIT] rc=0" "%ROOT%2_Logs\run_paper_daily_wrapper_status.txt" >nul 2>nul
if errorlevel 1 (
  echo [PREFLIGHT] run_paper_daily wrapper did not report rc=0 %DATE% %TIME% >> "%LOG%"
  type "%ROOT%2_Logs\run_paper_daily_wrapper_status.txt" >> "%LOG%" 2>nul
  exit /b 22
)
echo [PREFLIGHT] freshness_check_v1 after daily refresh %DATE% %TIME% >> "%LOG%"
"%PY%" tools\freshness_check_v1.py >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [PREFLIGHT] freshness still failed after daily refresh rc=%ERRORLEVEL% %DATE% %TIME% >> "%LOG%"
  exit /b 23
)
echo [PREFLIGHT] freshness PASS after daily refresh %DATE% %TIME% >> "%LOG%"
exit /b 0

:TRY_PY_CMD
set "__CAND=%~1"
if not defined __CAND goto :EOF
where %__CAND% >nul 2>nul || goto :EOF
%__CAND% -V >nul 2>nul
if errorlevel 1 goto :EOF
set "PY=%__CAND%"
goto :EOF

:TRY_PY_CMD_PATH
set "__CMD=%~1"
if not exist "%__CMD%" goto :EOF
"%__CMD%" -V >nul 2>nul
if errorlevel 1 goto :EOF
set "PY=%__CMD%"
goto :EOF

:TRY_PY_EXE
set "__EXE=%~1"
if not exist "%__EXE%" goto :EOF
"%__EXE%" -V >nul 2>nul
if errorlevel 1 goto :EOF
set "PY=%__EXE%"
goto :EOF

:RELEASE_LOCK
if "%LOCK_ACQUIRED%"=="1" (
  rmdir /s /q "%LOCK_DIR%" >nul 2>nul
  set "LOCK_ACQUIRED=0"
)
goto :EOF


