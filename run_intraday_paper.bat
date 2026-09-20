@echo off
setlocal EnableExtensions

REM run_intraday_paper.bat
REM - Run intraday paper loop in mock-safe mode by default

REM [2026-08-24] v41.1 new-entry stop.
REM   Basis: entry condition negative in 11.6y / all 12 years
REM   (.agent/PLANS.md 2026-08-24 (77)(84)).
REM   PAPER_EXIT_ONLY is the official switch at paper_engine.py:440/448/1255/1388;
REM   it blocks new entries only. Exit / stop-loss / trailing keep running.
REM   Set here so a manual restart and a watchdog restart end up identical
REM   (tools/intraday_loop_watchdog.ps1 revives the loop through this bat).
REM   To revert, delete the single line below and restart the loop.
REM [2026-09-19] 사용자 승인: v41.1 을 **청산 전용**으로. 새 로직(분기 시총 V2)과 같은 모의계좌를
REM   쓰는데(모의계좌는 ID 당 1개) 둘이 같이 사면 매수 가능 금액을 서로 잡아먹는다.
REM   topn 라운드 때 실제로 겪었다. 10-01 첫 재구성 전에 진입만 막는다. 되돌리려면 0 으로.
if not defined PAPER_EXIT_ONLY set "PAPER_EXIT_ONLY=1"

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

REM [2026-09-13] A killed loop leaves this lock behind and every later start backs
REM   off with [SKIP] already running - the loop then stays down all day, and the
REM   watchdog did NOT recover it (measured 2026-09-13 10:09-10:15). The owner pid
REM   was already inside the lock (heartbeat.json); nothing looked at it. Same
REM   defect was fixed this morning in the daily-batch lock - this is copy two.
REM   Fail-closed: if the owner cannot be judged, the lock is kept.
REM   PY is resolved much later in this file, so use the embedded runtime directly
REM   and skip the check when it is absent rather than guessing an interpreter.
set "LOCK_PY=%ROOT%_runtime\python312-embed\python.exe"
if exist "%LOCK_PY%" (
  REM %LOG% is truncated a few lines below, which erased this evidence on the
  REM   first run (2026-09-13). Keep it in its own append-only file.
  "%LOCK_PY%" "%ROOT%tools\stale_lock_check.py" --lock "%LOCK_DIR%" --remove >> "%ROOT%2_Logs\intraday_lock_check_log.txt" 2>&1
) else (
  >> "%ROOT%2_Logs\intraday_lock_check_log.txt" echo [LOCK] embedded python missing; check skipped %DATE% %TIME%
)

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

REM [2026-09-08 user decision] Default account switched from PROD to MOCK.
REM   The loop authenticated against the real account every 2 minutes all session
REM   just to read quotes and balances. It never ordered there (see below), but the
REM   standing state was "flip one env var and it is real money". Removed.
set "KIS_MOCK_PREV_DEFAULT=0"
if "%KIS_MOCK%"=="" set "KIS_MOCK=1"
if "%LOOP_INTERVAL%"=="" set "LOOP_INTERVAL=2"
if "%LOOP_MAX_ORDERS%"=="" set "LOOP_MAX_ORDERS=5"
REM [2026-09-08] Dispatch stays OFF by default even in mock, which is NOT what the
REM   old rule did (mock implied apply=1). Reason: the mock account 50204785 is the
REM   prereg round RD_20260901_topn stage-1 account. If the v41.1 loop also ordered
REM   there, it would eat the buying power and the max_pos slots that the round is
REM   measuring - the round would be measuring our own interference.
REM   v41.1 signal is deprecated (2026-09-07 direction), so it has nothing to send.
REM   Set LOOP_DISPATCH_APPLY=1 explicitly if mock ordering is ever wanted again.
if "%LOOP_DISPATCH_APPLY%"=="" set "LOOP_DISPATCH_APPLY=0"
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
REM [2026-08-24] Cap the parent restart loop. Without a cap the parent never
REM   exits, so the external watchdog sees it in pidsBefore and alive_skips
REM   forever while python crash-loops every 15s unnoticed.
if "%LOOP_RESTART_MAX%"=="" set "LOOP_RESTART_MAX=20"
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

REM [2026-08-24] Rotate the previous run log before truncating it.
REM   The next line uses ">" which wipes the file, so every restart erased
REM   the previous run. On 2026-08-24 that lost the 09:47-14:29 hard-block
REM   evidence. Consumers read *_last.txt, so keep the name and move the old
REM   file to 2_Logs\_archive first. Failure never blocks the launcher.
if exist "%LOG%" if defined PY "%PY%" "%ROOT%tools\rotate_log.py" "%LOG%" >nul 2>nul
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
if %RESTART_COUNT% GEQ %LOOP_RESTART_MAX% goto :RESTART_CAP
echo [WATCHDOG] restart #%RESTART_COUNT% after %LOOP_RESTART_WAIT_SEC%s (exitcode=%EXITCODE%) %DATE% %TIME%>> "%LOG%"
powershell -NoProfile -Command "Start-Sleep -Seconds %LOOP_RESTART_WAIT_SEC%" >nul 2>nul
goto :RUN_LOOP

:RESTART_CAP
echo [WATCHDOG] restart cap %LOOP_RESTART_MAX% reached; parent gives up so the external watchdog can take over %DATE% %TIME%>> "%LOG%"
if defined PY "%PY%" "%ROOT%tools\loop_restart_cap_alert.py" "%RESTART_COUNT%" "%EXITCODE%" >nul 2>nul
set "EXITCODE=3"
goto :DONE

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


